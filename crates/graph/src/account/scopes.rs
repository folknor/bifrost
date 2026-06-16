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
    match discover_cursor_scopes_inner(&account).await {
        Ok((scopes, warnings)) => {
            account.cursor_index.write().await.replace(scopes.clone());
            let mut events: Vec<SyncEvent<CursorScope>> =
                warnings.into_iter().map(SyncEvent::Warning).collect();
            events.push(batch(scopes, Some(PageBoundary::Final)));
            events.push(SyncEvent::Done(None));
            events
        }
        Err(error) => vec![SyncEvent::Terminated(error), SyncEvent::Done(None)],
    }
}

pub(crate) async fn discover_membership_events(
    account: GraphAccount,
) -> Vec<SyncEvent<MembershipScope>> {
    match discover_memberships_inner(&account).await {
        Ok(memberships) => vec![
            batch(memberships, Some(PageBoundary::Final)),
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

pub(crate) fn batch<T>(items: Vec<T>, page_boundary: Option<PageBoundary>) -> SyncEvent<T> {
    SyncEvent::Batch(Batch {
        items,
        page_boundary: page_boundary.unwrap_or(PageBoundary::Page),
        server_latency: Duration::default(),
        bytes_in: 0,
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
                GraphErrorContext::graph(AccountOperation::DiscoverMemberships),
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
                    GraphErrorContext::graph(AccountOperation::DiscoverMemberships),
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
    let mut seen = HashSet::new();
    let mut memberships = Vec::new();
    for scope in scopes {
        if let CursorScope::FolderType { folder, .. } = scope
            && seen.insert(folder.clone())
        {
            // A foreign folder also contributes its owner tag (the
            // shared-mailbox identity), which the engine's covering
            // rule cannot form because the folder-id and mailbox-id
            // strings differ.
            if let Some(foreign) = parse_folder(&folder).foreign() {
                memberships.push(owner_tag(&foreign.mailbox));
            }
            memberships.push(MembershipScope::Folder(folder));
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

    #[tokio::test]
    async fn discover_emits_foreign_owner_membership() {
        let account = GraphAccount::new_for_tests_with_shared(
            GraphClient::new("token"),
            PushMode::GraphSubscriptions,
            &["shared@contoso.com".to_string()],
        );
        // Seed the cursor index directly so the membership pass does not
        // hit the network: one primary folder, one foreign folder.
        let primary = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let foreign = CursorScope::FolderType {
            folder: encode_foreign("shared@contoso.com", "AAMk"),
            ty: ObjectType::Email,
        };
        account
            .cursor_index
            .write()
            .await
            .replace(vec![primary.clone(), foreign.clone()]);

        let memberships = discover_memberships_inner(&account)
            .await
            .expect("membership discovery succeeds");

        // The foreign folder contributes its owner tag (the shared
        // mailbox identity) in addition to its folder membership.
        assert!(memberships.contains(&MembershipScope::Mailbox(MailboxId(
            "shared@contoso.com".to_string()
        ))));
        assert!(
            memberships.contains(&MembershipScope::Folder(encode_foreign(
                "shared@contoso.com",
                "AAMk"
            )))
        );
        // The primary folder contributes only its folder membership - no
        // owner tag.
        assert!(memberships.contains(&MembershipScope::Folder(FolderId("inbox".to_string()))));
        assert_eq!(
            memberships
                .iter()
                .filter(|m| matches!(m, MembershipScope::Mailbox(_)))
                .count(),
            1
        );
    }

    #[test]
    fn scope_batch_has_final_boundary() {
        let event = batch(
            vec![MembershipScope::Folder(FolderId("f1".to_string()))],
            Some(PageBoundary::Final),
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
