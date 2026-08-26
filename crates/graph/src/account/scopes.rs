use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bifrost_types::{
    AccessErrorKind, AccountError, AccountErrorKind, AccountOperation, Batch, Checkpoint,
    CursorScope, FolderId, MembershipScope, ObjectType, PageBoundary, ScopeLifecycle, SyncEvent,
    Warning, WarningKind,
};

use super::GraphAccount;
use super::foreign::{encode_foreign, owner_tag, parse_folder};
use super::graph_error::{GraphErrorContext, into_account_error};

#[derive(Debug, Default, Clone)]
pub(crate) struct CursorIndex {
    scopes: Vec<CursorScope>,
}

impl CursorIndex {
    pub(crate) fn replace(&mut self, scopes: Vec<CursorScope>) {
        self.scopes = scopes;
    }

    pub(crate) fn scopes(&self) -> Vec<CursorScope> {
        self.scopes.clone()
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct FolderTree {
    parents: HashMap<String, Option<String>>,
}

impl FolderTree {
    pub(crate) fn replace_mail_folders<I>(&mut self, folders: I)
    where
        I: IntoIterator<Item = (String, Option<String>)>,
    {
        self.parents.clear();
        for (id, parent) in folders {
            self.parents.insert(id, parent);
        }
    }
}

pub(crate) async fn discover_cursor_scope_events(
    account: GraphAccount,
) -> Vec<SyncEvent<CursorScope>> {
    // Discovery walks the folder tree with an unbounded number of
    // requests - one per mailbox plus recursive child pages - all of
    // which land in this single Final batch.
    let (metered, tally) = account.metered();
    match discover_cursor_scopes_inner(&metered).await {
        Ok((scopes, warnings)) => {
            account.cursor_index.write().await.replace(scopes.clone());
            let mut events: Vec<SyncEvent<CursorScope>> =
                warnings.into_iter().map(SyncEvent::Warning).collect();
            events.push(batch(scopes, Some(PageBoundary::Final), tally.take()));
            events.push(SyncEvent::Done(None));
            events
        }
        Err(error) => vec![SyncEvent::Terminated(error), SyncEvent::Done(None)],
    }
}

pub(crate) async fn discover_membership_events(
    account: GraphAccount,
) -> Vec<SyncEvent<MembershipScope>> {
    let (metered, tally) = account.metered();
    match discover_memberships_inner(&metered).await {
        Ok(memberships) => vec![
            batch(memberships, Some(PageBoundary::Final), tally.take()),
            SyncEvent::Done(None),
        ],
        Err(error) => vec![SyncEvent::Terminated(error), SyncEvent::Done(None)],
    }
}

pub(crate) fn scope_lifecycle_events() -> Vec<ScopeLifecycle> {
    // Graph does not expose a change-notification resource for
    // mail-folder lifecycle. The eventual polling loop belongs here;
    // until the engine wires its adaptive cadence into protocol
    // lifecycle streams, a quiet stream is the least surprising
    // behavior and discovery can be re-run on account reopen.
    Vec::new()
}

pub(crate) fn batch<T>(
    items: Vec<T>,
    page_boundary: Option<PageBoundary>,
    bytes_in: u64,
) -> SyncEvent<T> {
    SyncEvent::Batch(Batch {
        items,
        page_boundary: page_boundary.unwrap_or(PageBoundary::Page),
        server_latency: Duration::default(),
        bytes_in,
        checkpoint: None::<Checkpoint>,
    })
}

async fn discover_cursor_scopes_inner(
    account: &GraphAccount,
) -> Result<(Vec<CursorScope>, Vec<Warning>), AccountError> {
    // Primary mailbox: a list failure here is account-fatal (the whole
    // account cannot be discovered), so propagate it.
    let mail_folders = account
        .client
        .list_mail_folders_recursive()
        .await
        .map_err(|error| {
            into_account_error(
                error,
                GraphErrorContext::graph(AccountOperation::DiscoverCursorScopes),
            )
        })?;
    account.folder_tree.write().await.replace_mail_folders(
        mail_folders
            .iter()
            .map(|folder| (folder.id.clone(), folder.parent_folder_id.clone())),
    );

    let mut scopes = Vec::new();
    for folder in mail_folders {
        scopes.push(CursorScope::FolderType {
            folder: FolderId(folder.id),
            ty: ObjectType::Email,
        });
    }

    // Foreign (shared/delegate) mailboxes: each is independent. A
    // per-mailbox permission denial means that shared mailbox is no
    // longer accessible - skip it with a scoped Warning rather than
    // failing the whole discovery (the primary and other shared
    // mailboxes still sync).
    let mut warnings = Vec::new();
    for (mailbox, client) in account.shared_clients.iter() {
        match client.list_mail_folders_recursive().await {
            Ok(folders) => {
                for folder in folders {
                    scopes.push(CursorScope::FolderType {
                        folder: encode_foreign(mailbox, &folder.id),
                        ty: ObjectType::Email,
                    });
                }
            }
            Err(error) => {
                let account_error = into_account_error(
                    error,
                    GraphErrorContext::graph(AccountOperation::DiscoverCursorScopes),
                );
                if matches!(
                    account_error.kind(),
                    AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
                ) {
                    warnings.push(Warning::support_only(
                        WarningKind::OperatorAttentionNeeded,
                        format!("shared mailbox {mailbox} skipped: access denied during discovery"),
                    ));
                } else {
                    return Err(account_error);
                }
            }
        }
    }

    // Public folders (opt-in via `with_public_folders`): browse the
    // Exchange public-folder hierarchy, seed each readable folder's
    // content-mailbox routing, and surface it as a `CursorScope::Folder`
    // synced by the no-delta-token poll strategy. Per-folder failures
    // skip with a scoped warning - the primary/shared mailboxes already
    // discovered above are unaffected.
    // Discovery seeds the whole readable hierarchy either way; only the
    // allowlisted folders come back as scopes.
    if let Some(policy) = account.public_folders.as_ref() {
        let (pf_scopes, pf_warnings) =
            super::public_folder::discover_public_folder_scopes(account, policy).await;
        scopes.extend(pf_scopes);
        warnings.extend(pf_warnings);
    }

    Ok((scopes, warnings))
}

async fn discover_memberships_inner(
    account: &GraphAccount,
) -> Result<Vec<MembershipScope>, AccountError> {
    let scopes = {
        let cached = account.cursor_index.read().await.scopes();
        if cached.is_empty() {
            discover_cursor_scopes_inner(account).await?.0
        } else {
            cached
        }
    };
    let mut seen_folders = HashSet::new();
    let mut seen_owners = HashSet::new();
    let mut memberships = Vec::new();
    for scope in scopes {
        match scope {
            CursorScope::FolderType { folder, .. } if seen_folders.insert(folder.clone()) => {
                // A foreign folder also contributes its owner tag (the
                // shared-mailbox identity), which the engine's covering
                // rule cannot form because the folder-id and mailbox-id
                // strings differ.
                if let Some(foreign) = parse_folder(&folder).foreign()
                    && seen_owners.insert(foreign.mailbox.clone())
                {
                    memberships.push(owner_tag(&foreign.mailbox));
                }
                memberships.push(MembershipScope::Folder(folder));
            }
            // A public folder contributes its content-mailbox owner tag
            // (the same A5a pattern) plus its folder membership.
            CursorScope::Folder(folder) if seen_folders.insert(folder.clone()) => {
                if let Some(routing) = account.public_folder_routing(&folder).await {
                    memberships.push(MembershipScope::Mailbox(bifrost_types::MailboxId(
                        routing.anchor_mailbox,
                    )));
                }
                memberships.push(MembershipScope::Folder(folder));
            }
            _ => {}
        }
    }
    Ok(memberships)
}

#[cfg(test)]
mod tests {
    use bifrost_types::{FolderId, MailboxId};

    use super::super::PushMode;
    use super::*;
    use crate::client::GraphClient;

    /// Each foreign folder contributes an owner tag the engine's covering
    /// rule cannot derive on its own (the folder-id and mailbox-id strings
    /// differ), but the tag is per-MAILBOX, not per-folder: a shared
    /// mailbox with forty discovered folders must still contribute exactly
    /// one `Mailbox` membership. The seed below therefore carries TWO
    /// folders in one shared mailbox, so the dedup is actually exercised
    /// rather than being satisfied trivially by a single foreign folder,
    /// plus a folder in a second mailbox so the dedup is proven to be
    /// keyed on the owner rather than collapsing all owners into one.
    #[tokio::test]
    async fn discover_emits_one_owner_membership_per_shared_mailbox() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &[
                "shared@contoso.com".to_string(),
                "ops@contoso.com".to_string(),
            ],
        );
        // Seed the cursor index directly so the membership pass does not
        // hit the network.
        let folder_scope = |folder: FolderId| CursorScope::FolderType {
            folder,
            ty: ObjectType::Email,
        };
        account.cursor_index.write().await.replace(vec![
            folder_scope(FolderId("inbox".to_string())),
            folder_scope(encode_foreign("shared@contoso.com", "AAMk")),
            folder_scope(encode_foreign("shared@contoso.com", "AAMkSent")),
            folder_scope(encode_foreign("ops@contoso.com", "AAMkOps")),
        ]);

        let memberships = discover_memberships_inner(&account)
            .await
            .expect("membership discovery succeeds");

        // Every folder is present, foreign ones in their encoded form.
        for folder in [
            FolderId("inbox".to_string()),
            encode_foreign("shared@contoso.com", "AAMk"),
            encode_foreign("shared@contoso.com", "AAMkSent"),
            encode_foreign("ops@contoso.com", "AAMkOps"),
        ] {
            assert!(
                memberships.contains(&MembershipScope::Folder(folder.clone())),
                "missing folder {folder:?} in {memberships:?}"
            );
        }

        // One owner tag per shared mailbox - not one per foreign folder,
        // and not one collapsed tag for both mailboxes. The primary
        // mailbox's folder contributes no owner tag at all.
        let owners: Vec<&MailboxId> = memberships
            .iter()
            .filter_map(|m| match m {
                MembershipScope::Mailbox(id) => Some(id),
                _ => None,
            })
            .collect();
        assert_eq!(owners.len(), 2, "{memberships:?}");
        assert!(owners.contains(&&MailboxId("shared@contoso.com".to_string())));
        assert!(owners.contains(&&MailboxId("ops@contoso.com".to_string())));
    }

    #[test]
    fn scope_batch_has_final_boundary() {
        let event = batch(
            vec![MembershipScope::Folder(FolderId("f1".to_string()))],
            Some(PageBoundary::Final),
            0,
        );
        assert!(matches!(
            event,
            SyncEvent::Batch(Batch {
                page_boundary: PageBoundary::Final,
                ..
            })
        ));
    }
}
