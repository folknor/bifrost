use bifrost_types::{Batch, Checkpoint, CursorScope, MembershipScope, PageBoundary, SyncEvent};

use super::{ImapAccount, folder_scope, iter_stream, membership_scope};

pub(crate) fn discover_cursor_scopes(
    account: ImapAccount,
) -> bifrost_types::AccountStream<SyncEvent<CursorScope>> {
    let items = account
        .folders
        .entries()
        .into_iter()
        .filter(|entry| entry.selectable)
        .map(|entry| folder_scope(&entry.name))
        .collect();
    discovery_stream(items)
}

pub(crate) fn discover_memberships(
    account: ImapAccount,
) -> bifrost_types::AccountStream<SyncEvent<MembershipScope>> {
    let items = account
        .folders
        .entries()
        .into_iter()
        .map(|entry| membership_scope(&entry.name))
        .collect();
    discovery_stream(items)
}

fn discovery_stream<T: Send + Unpin + 'static>(
    items: Vec<T>,
) -> bifrost_types::AccountStream<SyncEvent<T>> {
    iter_stream(vec![
        SyncEvent::Batch(Batch {
            items,
            page_boundary: PageBoundary::Final,
            server_latency: std::time::Duration::ZERO,
            bytes_in: 0,
            checkpoint: None::<Checkpoint>,
        }),
        SyncEvent::Done(None),
    ])
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
