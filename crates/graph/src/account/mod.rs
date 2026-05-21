mod blob;
mod capabilities;
mod changes;
mod cursor;
mod error;
mod ews_stream;
mod get;
mod inventory;
mod mutate;
mod push;
mod push_stream;
mod scopes;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use bifrost_types::{
    Account, AccountCapabilities, AccountFactory, AccountFuture, AccountStream, BlobHandle,
    ByteRange, Change, ChangeCursor, CostClass, CursorDescriptor, CursorEstablishment, CursorScope,
    Error, HydratedObject, IdempotencyKey, InventoryEntry, MembershipScope, MutationResult,
    ObjectId, Priority, Projection, ScopeLifecycle, SubscriptionHandle, SyncEvent, SyncStrategy,
    WatchEvent,
};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub use self::cursor::{GraphCursorKind, GraphCursorPayload, GraphPageMarker};
pub use self::ews_stream::{
    EwsStreamingEventType, EwsStreamingNotification, EwsStreamingSubscription,
    build_get_streaming_events_request, build_subscribe_request, parse_streaming_notifications,
    parse_subscribe_response,
};
pub use self::push::PushEndpoint;
pub use crate::client::GraphClient;

use self::push::{EwsSubscriptionState, GraphSubscriptionGroup};
use self::scopes::{CursorIndex, FolderTree};

const UNLIMITED_BANDWIDTH: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PushMode {
    GraphSubscriptions,
    EwsStreaming,
}

#[derive(Clone)]
pub struct GraphAccount {
    pub(crate) client: GraphClient,
    pub(crate) capabilities: AccountCapabilities,
    pub(crate) push_endpoint: Option<PushEndpoint>,
    pub(crate) push_mode: PushMode,
    pub(crate) push_tx: broadcast::Sender<WatchEvent>,
    pub(crate) cursor_index: Arc<RwLock<CursorIndex>>,
    pub(crate) folder_tree: Arc<RwLock<FolderTree>>,
    pub(crate) graph_subscriptions:
        Arc<RwLock<HashMap<SubscriptionHandle, GraphSubscriptionGroup>>>,
    pub(crate) ews_subscriptions: Arc<RwLock<HashMap<SubscriptionHandle, EwsSubscriptionState>>>,
    pub(crate) ews_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) priority: Arc<AtomicU8>,
    pub(crate) bandwidth_cap: Arc<AtomicU64>,
    pub(crate) etag_index: Arc<RwLock<HashMap<String, String>>>,
}

impl GraphAccount {
    pub fn new(
        client: GraphClient,
        push_mode: PushMode,
        push_endpoint: Option<PushEndpoint>,
    ) -> Self {
        let (push_tx, _) = broadcast::channel(256);
        Self {
            client,
            capabilities: capabilities::build_capabilities(push_mode),
            push_endpoint,
            push_mode,
            push_tx,
            cursor_index: Arc::new(RwLock::new(CursorIndex::default())),
            folder_tree: Arc::new(RwLock::new(FolderTree::default())),
            graph_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            ews_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            ews_worker: Arc::new(Mutex::new(None)),
            shutdown: CancellationToken::new(),
            priority: Arc::new(AtomicU8::new(priority_to_u8(Priority::Normal))),
            bandwidth_cap: Arc::new(AtomicU64::new(UNLIMITED_BANDWIDTH)),
            etag_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_tests(client: GraphClient, push_mode: PushMode) -> Self {
        Self::new(client, push_mode, None)
    }
}

pub struct GraphAccountFactory {
    client: GraphClient,
    push_mode: PushMode,
    push_endpoint: Option<PushEndpoint>,
}

impl GraphAccountFactory {
    pub fn new(client: GraphClient) -> Self {
        Self {
            client,
            push_mode: PushMode::GraphSubscriptions,
            push_endpoint: None,
        }
    }

    pub fn with_push_endpoint(mut self, webhook_url: impl Into<String>) -> Self {
        self.push_endpoint = Some(PushEndpoint {
            webhook_url: webhook_url.into(),
        });
        self.push_mode = PushMode::GraphSubscriptions;
        self
    }

    pub fn with_ews_streaming(mut self) -> Self {
        self.push_endpoint = None;
        self.push_mode = PushMode::EwsStreaming;
        self
    }
}

impl AccountFactory for GraphAccountFactory {
    fn open(&self) -> AccountFuture<Result<Arc<dyn Account>, Error>> {
        let client = self.client.clone();
        let push_mode = self.push_mode;
        let push_endpoint = self.push_endpoint.clone();
        Box::pin(async move {
            client.get_profile().await.map_err(Error::Auth)?;
            let account = GraphAccount::new(client.clone(), push_mode, push_endpoint);
            let folders = client
                .list_mail_folders_recursive()
                .await
                .map_err(Error::Transport)?;
            account.folder_tree.write().await.replace_mail_folders(
                folders
                    .into_iter()
                    .map(|folder| (folder.id, folder.parent_folder_id)),
            );
            Ok(Arc::new(account) as Arc<dyn Account>)
        })
    }
}

impl Account for GraphAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.capabilities
    }

    fn set_priority(&self, priority: Priority) {
        self.priority
            .store(priority_to_u8(priority), Ordering::Release);
    }

    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        self.bandwidth_cap
            .store(bps.unwrap_or(UNLIMITED_BANDWIDTH), Ordering::Release);
    }

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        CursorDescriptor {
            cost_class: CostClass::Cheap,
            strategy: SyncStrategy::ServerCursor,
            freshness: None,
        }
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        sync_event_stream(scopes::discover_cursor_scope_events(self.clone()))
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        sync_event_stream(scopes::discover_membership_events(self.clone()))
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycle> {
        Box::pin(stream::iter(scopes::scope_lifecycle_events()))
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, Error>> {
        Box::pin(async move {
            cursor::kind_for_scope(&scope)?;
            Ok(CursorEstablishment::EstablishViaInventory)
        })
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<SyncEvent<InventoryEntry>> {
        inventory::inventory_stream(self.clone(), scope)
    }

    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<HydratedObject>> {
        get::get_stream(self.clone(), ids, projection)
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        changes::changes_stream(self.clone(), cursor)
    }

    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<SubscriptionHandle, Error>> {
        let account = self.clone();
        let scopes = scopes.to_vec();
        Box::pin(async move { push::push_subscribe(account, scopes).await })
    }

    fn push_unsubscribe(&self, handle: SubscriptionHandle) -> AccountFuture<Result<(), Error>> {
        let account = self.clone();
        Box::pin(async move { push::push_unsubscribe(account, handle).await })
    }

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        push_stream::push_stream(self.clone())
    }

    fn open_blob(&self, handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        blob::open_blob_stream(self.clone(), handle)
    }

    fn open_blob_range(
        &self,
        handle: BlobHandle,
        range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        blob::open_blob_range_stream(self.clone(), handle, range)
    }

    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        op: bifrost_types::FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<MutationResult>> {
        mutate::bulk_set_flags_stream(self.clone(), targets, op, key)
    }

    fn bulk_move(
        &self,
        targets: AccountStream<ObjectId>,
        destination: MembershipScope,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<MutationResult>> {
        mutate::bulk_move_stream(self.clone(), targets, destination, key)
    }

    fn bulk_destroy(
        &self,
        targets: AccountStream<ObjectId>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<MutationResult>> {
        mutate::bulk_destroy_stream(self.clone(), targets, key)
    }

    fn close(&self) -> AccountFuture<Result<(), Error>> {
        let account = self.clone();
        Box::pin(async move {
            account.shutdown.cancel();
            if let Some(worker) = account.ews_worker.lock().await.take() {
                worker.abort();
            }
            Ok(())
        })
    }
}

fn sync_event_stream<T, F>(future: F) -> AccountStream<SyncEvent<T>>
where
    T: Send + 'static,
    F: Future<Output = Vec<SyncEvent<T>>> + Send + 'static,
{
    Box::pin(stream::once(future).flat_map(stream::iter))
}

fn priority_to_u8(priority: Priority) -> u8 {
    match priority {
        Priority::Foreground => 0,
        Priority::Normal => 1,
        Priority::Background => 2,
        Priority::Bulk => 3,
        _ => 1,
    }
}
