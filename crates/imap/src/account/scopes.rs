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
        .map(|entry| membership_scope(&entry.name))
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
    use bifrost_types::{CursorScope, FolderId, ObjectType, SyncEvent};
    use futures::StreamExt;

    use super::super::test_support::{StubAccount, stub_arc};
    use super::fan_in_discovery;

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
