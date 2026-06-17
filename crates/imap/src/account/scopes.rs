use std::sync::Arc;

use bifrost_types::{Account, CursorScope, MembershipScope, SyncEvent, Warning};

use super::{ImapAccount, folder_scope, membership_scope};

pub(crate) fn discover_cursor_scopes(
    account: ImapAccount,
) -> bifrost_types::AccountStream<SyncEvent<CursorScope>> {
    let folder_scopes: Vec<CursorScope> = account
        .folders
        .entries()
        .into_iter()
        .filter(|entry| entry.selectable)
        .map(|entry| folder_scope(&entry.name))
        .collect();
    // The two composable sub-accounts contribute their own typed cursor
    // scopes (CardDAV `Type(Contact)`, CalDAV `Type(CalendarEvent)`).
    let subs: Vec<Arc<dyn Account>> = [account.contacts.clone(), account.calendars.clone()]
        .into_iter()
        .flatten()
        .collect();
    // Degraded-DAV warnings recorded at open (brick 5) surface here, on
    // the first discovery, so the engine observes the degradation.
    let degraded = account.take_dav_degraded_warnings();
    fan_in_discovery(folder_scopes, degraded, subs, |sub| {
        sub.discover_cursor_scopes()
    })
}

pub(crate) fn discover_memberships(
    account: ImapAccount,
) -> bifrost_types::AccountStream<SyncEvent<MembershipScope>> {
    let folder_memberships: Vec<MembershipScope> = account
        .folders
        .entries()
        .into_iter()
        .flat_map(|entry| memberships_for_entry(&entry))
        .collect();
    let subs: Vec<Arc<dyn Account>> = [account.contacts.clone(), account.calendars.clone()]
        .into_iter()
        .flatten()
        .collect();
    fan_in_discovery(folder_memberships, Vec::new(), subs, |sub| {
        sub.discover_memberships()
    })
}

/// Merge the IMAP-owned discovery items with each sub-account's
/// discovery stream. Delegates to the reusable
/// `bifrost_types::account_compose::merge_scope_streams` (brick 10): the
/// composition fan-in is protocol-agnostic, so IMAP is just its first
/// consumer.
fn fan_in_discovery<T, F>(
    items: Vec<T>,
    warnings: Vec<Warning>,
    subs: Vec<Arc<dyn Account>>,
    discover: F,
) -> bifrost_types::AccountStream<SyncEvent<T>>
where
    T: Send + 'static,
    F: Fn(&Arc<dyn Account>) -> bifrost_types::AccountStream<SyncEvent<T>> + Send + 'static,
{
    bifrost_types::account_compose::merge_scope_streams(items, warnings, subs, discover)
}

/// Memberships a single folder entry contributes to discovery. Every
/// folder keeps its `Folder` membership for folder-keyed routing. A
/// shared/other-user folder (`shared_owner.is_some()`) also emits its
/// owning `Mailbox(owner)` membership so the consumer maps the scope to a
/// shared mailbox identity (A5c). The owner tag does not form via the
/// engine's `scope_covers_membership` covering rule (the FolderId and
/// MailboxId strings differ for a shared folder), so it must be emitted
/// explicitly here.
fn memberships_for_entry(entry: &super::folder_registry::FolderEntry) -> Vec<MembershipScope> {
    let mut out = vec![membership_scope(&entry.name)];
    if let Some(owner) = &entry.shared_owner {
        out.push(MembershipScope::Mailbox(owner.clone()));
    }
    out
}

/// IMAP intentionally emits no `ScopeLifecycleEvent`s: this stream stays
/// open (so the engine's lifecycle worker does not treat an early close as
/// a fault) and yields nothing until shutdown.
///
/// This is a deliberate limitation, not a missing feature. IMAP discovers
/// folders only at open/reopen (NAMESPACE + LIST in `factory.rs`), and the
/// engine re-runs `discover_cursor_scopes` / `discover_memberships` on
/// every account reopen. Folder mutations observed mid-session via push
/// IDLE (`IdleEvent::MailboxEvent` -> `FolderRegistry::apply_mailbox_event`,
/// in `push.rs`) update the in-memory registry and surface as
/// `WatchEvent::Invalidated`, which drives a reconcile rather than a
/// per-scope `RestartScope`. A folder that *appears* after attach (a newly
/// shared/other-user mailbox) therefore stays invisible to the engine
/// until the next full account reopen.
///
/// Wiring true folder-lifecycle detection (RFC 5465 NOTIFY MAILBOXES, or
/// periodic LIST diffing) into a dedicated `Created`/`Renamed`/`Deleted`
/// feed is a feature, not a bug fix, and is deferred. See `reference/imap.md`
/// "Folder lifecycle" and `reference/sync.md` scope-lifecycle.
pub(crate) fn scope_lifecycle_stream(
    account: ImapAccount,
) -> bifrost_types::AccountStream<bifrost_types::ScopeLifecycleEvent> {
    let shutdown = account.shutdown.clone();
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        shutdown.cancelled().await;
        drop(tx);
    });
    super::boxed_receiver_stream(rx)
}

#[cfg(test)]
mod tests {
    use bifrost_types::{CursorScope, FolderId, MailboxId, ObjectType, SyncEvent};
    use futures::StreamExt;

    use super::super::folder_registry::FolderRegistry;
    use super::super::folder_scope;
    use super::super::test_support::{StubAccount, stub_arc};
    use super::{fan_in_discovery, memberships_for_entry};
    use crate::types::{MailboxInfo, MailboxName};
    use bifrost_types::MembershipScope;

    fn mailbox_info(name: &str) -> MailboxInfo {
        MailboxInfo {
            name: MailboxName::new(name).expect("valid mailbox name"),
            delimiter: Some('/'),
            ..Default::default()
        }
    }

    // A registry with one personal + one shared folder yields, for the
    // shared one, both `Folder(..)` and `Mailbox(owner)` memberships; the
    // personal one yields only `Folder`.
    #[test]
    fn discover_memberships_tags_shared_folder_with_mailbox() {
        let registry = FolderRegistry::from_lists(
            vec![mailbox_info("INBOX")],
            vec![(
                mailbox_info("Shared/alice/INBOX"),
                MailboxId("alice".to_string()),
            )],
        );

        let mut personal = None;
        let mut shared = None;
        for entry in registry.entries() {
            if entry.name.as_str() == "INBOX" {
                personal = Some(memberships_for_entry(&entry));
            } else {
                shared = Some(memberships_for_entry(&entry));
            }
        }

        let personal = personal.expect("personal entry present");
        assert_eq!(
            personal,
            vec![MembershipScope::Folder(FolderId("INBOX".to_string()))]
        );

        let shared = shared.expect("shared entry present");
        assert!(shared.contains(&MembershipScope::Folder(FolderId(
            "Shared/alice/INBOX".to_string()
        ))));
        assert!(shared.contains(&MembershipScope::Mailbox(MailboxId("alice".to_string()))));
        assert_eq!(shared.len(), 2);
    }

    // A shared selectable folder surfaces as an ordinary
    // `CursorScope::Folder` in the merged discovery batch, exactly like a
    // personal folder (decision 3: no `route_typed_scope` change).
    #[tokio::test]
    async fn discover_cursor_scopes_includes_shared_folder_scope() {
        let registry = FolderRegistry::from_lists(
            vec![mailbox_info("INBOX")],
            vec![(
                mailbox_info("Shared/alice/INBOX"),
                MailboxId("alice".to_string()),
            )],
        );
        let folder_scopes: Vec<CursorScope> = registry
            .entries()
            .into_iter()
            .filter(|entry| entry.selectable)
            .map(|entry| folder_scope(&entry.name))
            .collect();

        let mut stream = fan_in_discovery(folder_scopes, Vec::new(), Vec::new(), |sub| {
            sub.discover_cursor_scopes()
        });

        let mut items = Vec::new();
        while let Some(event) = stream.next().await {
            match event {
                SyncEvent::Batch(batch) => items.extend(batch.items),
                SyncEvent::Done(_) => break,
                other => panic!("unexpected event {other:?}"),
            }
        }
        assert!(items.contains(&CursorScope::Folder(FolderId("INBOX".to_string()))));
        assert!(items.contains(&CursorScope::Folder(FolderId(
            "Shared/alice/INBOX".to_string()
        ))));
    }

    // Brick 2: discovery fan-in. A populated folder set merged with a
    // stub contacts sub-account (whose discovery yields one
    // `Type(Contact)` scope) produces a single terminal `Done` and one
    // batch carrying both the folder scope and the contact scope.
    #[tokio::test]
    async fn discover_cursor_scopes_merges_folder_and_contact_scopes() {
        let folder = CursorScope::Folder(FolderId("INBOX".to_string()));
        let contacts = stub_arc(StubAccount::new(vec![CursorScope::Type(
            ObjectType::Contact,
        )]));

        let mut stream =
            fan_in_discovery(vec![folder.clone()], Vec::new(), vec![contacts], |sub| {
                sub.discover_cursor_scopes()
            });

        let mut items = Vec::new();
        let mut saw_done = false;
        while let Some(event) = stream.next().await {
            match event {
                SyncEvent::Batch(batch) => items.extend(batch.items),
                SyncEvent::Done(_) => {
                    saw_done = true;
                    assert!(stream.next().await.is_none(), "Done must be terminal");
                    break;
                }
                other => panic!("unexpected event {other:?}"),
            }
        }
        assert!(saw_done, "discovery must terminate with Done");
        assert!(
            items.contains(&folder),
            "folder scope must survive the merge"
        );
        assert!(
            items.contains(&CursorScope::Type(ObjectType::Contact)),
            "contact scope from the sub-account must be merged in",
        );
        assert_eq!(items.len(), 2);
    }

    // A sub-account discovery error becomes a Warning, not a fatal: the
    // IMAP folder scopes still flow and discovery still terminates.
    #[tokio::test]
    async fn discover_cursor_scopes_folds_sub_failure_into_warning() {
        use std::sync::Arc;

        use bifrost_types::{Account, AccountStream};

        let folder = CursorScope::Folder(FolderId("INBOX".to_string()));
        let calendars: Arc<dyn Account> = stub_arc(StubAccount::new(Vec::new()));

        // Drive the failure path with a closure that yields a Terminated
        // discovery stream for the sub.
        let discover = |_sub: &Arc<dyn Account>| -> AccountStream<SyncEvent<CursorScope>> {
            let op = bifrost_types::AccountOperation::DiscoverCursorScopes;
            let error = bifrost_types::AccountErrorBuilder::new(
                bifrost_types::AccountErrorKind::Transport(
                    bifrost_types::TransportErrorKind::Network,
                ),
                bifrost_types::Cause::Transport(bifrost_types::TransportCause::new(
                    bifrost_types::TransportKind::Network,
                    None,
                )),
            )
            .operation(op)
            .try_build()
            .expect("valid account error classification");
            Box::pin(futures::stream::iter([SyncEvent::Terminated(error)]))
        };

        let mut stream =
            fan_in_discovery(vec![folder.clone()], Vec::new(), vec![calendars], discover);

        let mut warnings = 0;
        let mut items = Vec::new();
        let mut saw_done = false;
        while let Some(event) = stream.next().await {
            match event {
                SyncEvent::Warning(_) => warnings += 1,
                SyncEvent::Batch(batch) => items.extend(batch.items),
                SyncEvent::Done(_) => {
                    saw_done = true;
                    break;
                }
                other => panic!("unexpected event {other:?}"),
            }
        }
        assert!(saw_done);
        assert_eq!(warnings, 1, "sub failure surfaces as exactly one warning");
        assert_eq!(items, vec![folder], "IMAP folder scopes still flow");
    }
}
