mod blob;
mod capabilities;
mod changes;
mod cursor;
mod error;
mod ews_stream;
mod filters;
mod get;
mod graph_error;
mod inventory;
mod mutate;
mod pim;
mod push;
mod push_stream;
mod scopes;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFactory, AccountFuture, AccountStream,
    AddressBook, AddressBookId, BlobHandle, ByteRange, Change, ChangeCursor, ContactCard,
    ContactCreate, ContactId, ContactPatch, ContactSearchRequest, CostClass, CursorDescriptor,
    CursorEstablishment, CursorScope, DraftHandle, DraftPatch, FilterValidation, HydratedObject,
    HydrationProjection, IdempotencyKey, InventoryEntry, ItemOutcome, MembershipScope, Message,
    MutationSuccess, MutationTarget, ObjectId, Page, Priority, Projection, ScopeLifecycleEvent,
    SearchRequest, SendRequest, ServerFilter, ServerFilterCreate, ServerFilterId,
    ServerFilterPatch, SubscriptionHandle, SyncEvent, SyncStrategy, ThreadHydration, ThreadId,
    VacationConfig, WatchEvent,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

// pub: consumers build a GraphClient before registering GraphAccountFactory with the engine.
pub use crate::client::GraphClient;

use self::push::{EwsSubscriptionState, GraphSubscriptionGroup, PushEndpoint};
use self::scopes::{CursorIndex, FolderTree};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum PushMode {
    GraphSubscriptions,
    EwsStreaming,
}

#[derive(Clone)]
pub(crate) struct GraphAccount {
    pub(crate) client: GraphClient,
    pub(crate) capabilities: AccountCapabilities,
    pub(crate) push_endpoint: Option<PushEndpoint>,
    pub(crate) push_mode: PushMode,
    pub(crate) push_tx: broadcast::Sender<WatchEvent>,
    pub(crate) cursor_index: Arc<RwLock<CursorIndex>>,
    pub(crate) folder_tree: Arc<RwLock<FolderTree>>,
    pub(crate) graph_subscriptions:
        Arc<RwLock<HashMap<SubscriptionHandle, GraphSubscriptionGroup>>>,
    pub(crate) graph_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(crate) ews_subscriptions: Arc<RwLock<HashMap<SubscriptionHandle, EwsSubscriptionState>>>,
    pub(crate) ews_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) etag_index: Arc<RwLock<HashMap<String, String>>>,
}

impl GraphAccount {
    pub(crate) fn new(
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
            graph_worker: Arc::new(Mutex::new(None)),
            ews_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            ews_worker: Arc::new(Mutex::new(None)),
            shutdown: CancellationToken::new(),
            etag_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_tests(client: GraphClient, push_mode: PushMode) -> Self {
        Self::new(client, push_mode, None)
    }
}

// pub: sync-engine consumers register this factory so reopen can mint fresh Graph accounts.
pub struct GraphAccountFactory {
    client: GraphClient,
    push_mode: PushMode,
    push_endpoint: Option<PushEndpoint>,
}

impl GraphAccountFactory {
    // pub: ergonomic factory construction from an already configured GraphClient.
    pub fn new(client: GraphClient) -> Self {
        Self {
            client,
            push_mode: PushMode::GraphSubscriptions,
            push_endpoint: None,
        }
    }

    // pub: webhook-mode consumers provide the public Graph subscription callback URL here.
    pub fn with_push_endpoint(mut self, webhook_url: impl Into<String>) -> Self {
        self.push_endpoint = Some(PushEndpoint {
            webhook_url: webhook_url.into(),
        });
        self.push_mode = PushMode::GraphSubscriptions;
        self
    }

    // pub: consumers without a reachable webhook endpoint can select in-process EWS streaming.
    pub fn with_ews_streaming(mut self) -> Self {
        self.push_endpoint = None;
        self.push_mode = PushMode::EwsStreaming;
        self
    }
}

impl AccountFactory for GraphAccountFactory {
    fn open(
        &self,
        account_id: bifrost_types::AccountId,
    ) -> AccountFuture<Result<Arc<dyn Account>, AccountError>> {
        let client = self.client.clone();
        let push_mode = self.push_mode;
        let push_endpoint = self.push_endpoint.clone();
        Box::pin(async move {
            client.attach_account(account_id);
            client.get_profile().await.map_err(|e| {
                super::account::graph_error::into_account_error(
                    e,
                    super::account::graph_error::GraphErrorContext::graph(
                        bifrost_types::AccountOperation::Discover,
                    ),
                )
            })?;
            let account = GraphAccount::new(client.clone(), push_mode, push_endpoint);
            let folders = client.list_mail_folders_recursive().await.map_err(|e| {
                super::account::graph_error::into_account_error(
                    e,
                    super::account::graph_error::GraphErrorContext::graph(
                        bifrost_types::AccountOperation::DiscoverMemberships,
                    ),
                )
            })?;
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
        if let Some(account_net) = self.client.account_net() {
            account_net.set_priority(priority);
        }
    }

    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        if let Some(account_net) = self.client.account_net() {
            account_net.set_bandwidth_cap(bps);
        }
    }

    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor {
        let valid = cursor::decode_cursor(cursor).is_ok();
        CursorDescriptor {
            cost_class: if valid {
                CostClass::Cheap
            } else {
                CostClass::Expensive
            },
            strategy: if valid {
                SyncStrategy::ServerCursor
            } else {
                SyncStrategy::None
            },
            freshness: valid.then(Instant::now),
        }
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        sync_event_stream(scopes::discover_cursor_scope_events(self.clone()))
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        sync_event_stream(scopes::discover_membership_events(self.clone()))
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        Box::pin(stream::iter(
            scopes::scope_lifecycle_events()
                .into_iter()
                .map(ScopeLifecycleEvent::Lifecycle),
        ))
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        Box::pin(async move {
            cursor::kind_for_scope(&scope).map_err(|e| {
                graph_error::cursor_error_to_account_error(
                    e,
                    graph_error::GraphErrorContext::graph(
                        bifrost_types::AccountOperation::EstablishCursor,
                    )
                    .with_scope(bifrost_types::ErrorScope::Cursor(scope.clone())),
                )
            })?;
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
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        get::get_stream(self.clone(), ids, projection)
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        changes::changes_stream(self.clone(), cursor)
    }

    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<SubscriptionHandle, AccountError>> {
        let account = self.clone();
        let scopes = scopes.to_vec();
        Box::pin(async move { push::push_subscribe(account, scopes).await })
    }

    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
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
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutate::bulk_set_flags_stream(self.clone(), targets, op, key)
    }

    fn bulk_move(
        &self,
        targets: AccountStream<ObjectId>,
        destination: MembershipScope,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutate::bulk_move_stream(self.clone(), targets, destination, key)
    }

    fn bulk_destroy(
        &self,
        targets: AccountStream<ObjectId>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutate::bulk_destroy_stream(self.clone(), targets, key)
    }

    fn add_to_container(
        &self,
        target: MutationTarget,
        container: bifrost_types::ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::add_to_container(account, target, container).await })
    }

    fn remove_from_container(
        &self,
        _target: MutationTarget,
        _container: bifrost_types::ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::RemoveFromContainer,
            ))
        })
    }

    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::SetKeyword,
            ))
        })
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: bifrost_types::ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::SetLabelMembership,
            ))
        })
    }

    fn set_category(
        &self,
        target: MutationTarget,
        category: String,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::set_category(account, target, category, value).await })
    }

    fn set_extended_property(
        &self,
        target: MutationTarget,
        property_id: String,
        value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(
            async move { pim::set_extended_property(account, target, property_id, value).await },
        )
    }

    fn set_is_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::set_is_read(account, target, is_read).await })
    }

    fn send_message(&self, request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::send_message(account, request).await })
    }

    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<bifrost_types::AttachmentHandle, AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::AttachmentUpload,
            ))
        })
    }

    fn draft_create(&self, patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::draft_create(account, patch).await })
    }

    fn draft_update(
        &self,
        draft: DraftHandle,
        patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::draft_update(account, draft, patch).await })
    }

    fn draft_discard(&self, draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::draft_discard(account, draft).await })
    }

    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::draft_send(account, draft).await })
    }

    fn search(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::search(account, request).await })
    }

    fn search_messages(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::search_messages(account, request).await })
    }

    fn containers_list(
        &self,
    ) -> AccountFuture<Result<Vec<bifrost_types::Container>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::containers_list(account).await })
    }

    fn container_create(
        &self,
        kind: bifrost_types::ContainerKind,
        name: String,
        parent: Option<bifrost_types::ContainerId>,
    ) -> AccountFuture<Result<bifrost_types::ContainerId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::container_create(account, kind, name, parent).await })
    }

    fn container_rename(
        &self,
        container: bifrost_types::ContainerId,
        name: String,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::container_rename(account, container, name).await })
    }

    fn container_move(
        &self,
        container: bifrost_types::ContainerId,
        new_parent: Option<bifrost_types::ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::container_move(account, container, new_parent).await })
    }

    fn container_delete(
        &self,
        container: bifrost_types::ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::container_delete(account, container).await })
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<bifrost_types::Identity>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::identities_list(account).await })
    }

    fn identity_update(
        &self,
        _identity: bifrost_types::IdentityId,
        _patch: bifrost_types::IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::IdentityUpdate,
            ))
        })
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::vacation_get(account).await })
    }

    fn vacation_set(&self, config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::vacation_set(account, config).await })
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<bifrost_types::QuotaInfo>, AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::QuotaGet,
            ))
        })
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { filters::list(account).await })
    }

    fn filter_create(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { filters::create(account, filter).await })
    }

    fn filter_update(
        &self,
        filter: ServerFilterId,
        patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { filters::update(account, filter, patch).await })
    }

    fn filter_delete(&self, filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { filters::delete(account, filter).await })
    }

    fn filter_validate(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        Box::pin(async move { filters::validate(filter) })
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::AddressBooksList,
            ))
        })
    }

    fn contacts_list(
        &self,
        _address_book: Option<AddressBookId>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::ContactsList,
            ))
        })
    }

    fn contact_get(&self, _contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::ContactGet,
            ))
        })
    }

    fn contact_create(
        &self,
        _contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::ContactCreate,
            ))
        })
    }

    fn contact_update(
        &self,
        _contact: ContactId,
        _patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::ContactUpdate,
            ))
        })
    }

    fn contact_delete(&self, _contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::ContactDelete,
            ))
        })
    }

    fn contact_search(
        &self,
        _request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        Box::pin(async {
            Err(graph_error::unsupported_account_error(
                bifrost_types::AccountOperation::ContactSearch,
            ))
        })
    }

    fn thread_hydrate(
        &self,
        thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::thread_hydrate(account, thread).await })
    }

    fn message_hydrate(
        &self,
        message: ObjectId,
        projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::message_hydrate(account, message, projection).await })
    }

    fn move_thread(
        &self,
        thread: ThreadId,
        target: bifrost_types::ContainerId,
        _source: Option<bifrost_types::ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::move_thread(account, thread, target).await })
    }

    // `apply_label` and `remove_label` rely on the trait's default
    // dispatch in `bifrost_types::Account`. The default routes
    // `(Label, Graph)` and `(Folder, Graph)` provenance through
    // `set_category`, `(Folder, non-Graph)` through
    // `add_to_container` / `remove_from_container`, and the remaining
    // shapes through `set_keyword` / `set_label_membership` - which
    // surface `Unsupported` on Graph if the consumer hands us a
    // cross-account label object. No override needed here.

    fn delete_thread(
        &self,
        thread: ThreadId,
        current: Option<bifrost_types::ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::delete_thread(account, thread, current).await })
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
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
