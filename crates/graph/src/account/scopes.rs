use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bifrost_types::{
    Batch, Checkpoint, CursorScope, Fatal, FolderId, MembershipScope, ObjectType, PageBoundary,
    ScopeLifecycle, SyncEvent,
};

use super::GraphAccount;
use super::error::graph_error_to_fatal;

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
        Ok(scopes) => {
            account.cursor_index.write().await.replace(scopes.clone());
            vec![
                batch(scopes, Some(PageBoundary::Final)),
                SyncEvent::Done(None),
            ]
        }
        Err(fatal) => vec![SyncEvent::Fatal(fatal), SyncEvent::Done(None)],
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
        Err(fatal) => vec![SyncEvent::Fatal(fatal), SyncEvent::Done(None)],
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

async fn discover_cursor_scopes_inner(account: &GraphAccount) -> Result<Vec<CursorScope>, Fatal> {
    let mail_folders = account
        .client
        .list_mail_folders_recursive()
        .await
        .map_err(|error| graph_error_to_fatal(error, CursorScope::Account))?;
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

    Ok(scopes)
}

async fn discover_memberships_inner(account: &GraphAccount) -> Result<Vec<MembershipScope>, Fatal> {
    let scopes = {
        let cached = account.cursor_index.read().await.scopes();
        if cached.is_empty() {
            discover_cursor_scopes_inner(account).await?
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
            memberships.push(MembershipScope::Folder(folder));
        }
    }
    Ok(memberships)
}

#[cfg(test)]
mod tests {
    use bifrost_types::FolderId;

    use super::*;

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
