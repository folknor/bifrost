mod autodiscover;
mod blob;
mod calendar;
mod capabilities;
mod changes;
mod cloud;
mod contacts;
mod cursor;
mod error;
mod ews_stream;
mod filters;
mod foreign;
mod get;
mod graph_error;
mod inventory;
mod mutate;
mod pim;
mod public_folder;
mod push;
mod push_stream;
mod scopes;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFactory, AccountFuture, AccountStream,
    AddressBook, AddressBookId, BlobHandle, ByteRange, Calendar, CalendarEvent, Change,
    ChangeCursor, CloudUploadMeta, ContactCard, ContactCreate, ContactId, ContactPatch,
    ContactSearchRequest, CostClass, CursorDescriptor, CursorEstablishment, CursorScope,
    DirectoryCard, DraftHandle, DraftPatch, EventCreate, EventId, EventPatch, EventRange,
    EventSearchRequest, FilterValidation, HostedAttachment, HydratedObject, HydrationProjection,
    IdempotencyKey, Importance, InventoryEntry, ItemOutcome, MembershipScope, Message,
    MutationSuccess, MutationTarget, ObjectId, Page, Priority, Projection, RsvpStatus,
    ScopeLifecycleEvent, SearchRequest, SendRequest, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch, SubscriptionHandle, SyncEvent, SyncStrategy,
    ThreadHydration, ThreadId, VacationConfig, WatchEvent,
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
    /// Foreign (shared/delegate) mailbox clients, keyed by the
    /// `/users/{id}` routing key. Built once at `open` from the
    /// factory's configured shared mailboxes via
    /// `GraphClient::for_shared_mailbox`. Selecting the right client by
    /// scope (`client_for_scope`) keeps every existing call site - which
    /// reads `api_path_prefix()` off the client - mailbox-correct by
    /// construction.
    pub(crate) shared_clients: Arc<HashMap<String, GraphClient>>,
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
    /// Public-folder routing map, keyed by native EWS `FolderId`. A
    /// folder present here is a public folder: it establishes via the
    /// public-folder inventory pass and polls via the no-delta-token
    /// strategy. Seeded empty at open; spec-3's Autodiscover discovery
    /// fills it. The presence-in-map check (`public_folder_routing`) is
    /// the discriminator that keeps a bare `CursorScope::Folder` for a
    /// non-public folder on its existing (reject-on-delta) path.
    pub(crate) routing_map:
        Arc<RwLock<HashMap<bifrost_types::FolderId, cursor::PublicFolderRouting>>>,
    /// Whether public-folder discovery is enabled (opt-in via
    /// `with_public_folders`). Default off, so no existing Graph account
    /// pays the Autodiscover round-trips.
    pub(crate) public_folders_enabled: bool,
    /// The account's primary SMTP, captured at open from the Graph
    /// profile. Seeds the `GetUserSettings` Autodiscover lookups.
    pub(crate) user_email: Option<String>,
}

impl GraphAccount {
    pub(crate) fn new(
        client: GraphClient,
        push_mode: PushMode,
        push_endpoint: Option<PushEndpoint>,
        shared_mailboxes: &[String],
        public_folders_enabled: bool,
        user_email: Option<String>,
    ) -> Self {
        let (push_tx, _) = broadcast::channel(256);
        let shared_clients = shared_mailboxes
            .iter()
            .map(|mailbox| (mailbox.clone(), client.for_shared_mailbox(mailbox.clone())))
            .collect();
        Self {
            client,
            shared_clients: Arc::new(shared_clients),
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
            routing_map: Arc::new(RwLock::new(HashMap::new())),
            public_folders_enabled,
            user_email,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_tests(client: GraphClient, push_mode: PushMode) -> Self {
        Self::new(client, push_mode, None, &[], false, None)
    }

    #[cfg(test)]
    pub(crate) fn new_for_tests_with_shared(
        client: GraphClient,
        push_mode: PushMode,
        shared_mailboxes: &[String],
    ) -> Self {
        Self::new(client, push_mode, None, shared_mailboxes, false, None)
    }

    /// Seed a public-folder routing entry for tests. Mirrors what
    /// spec-3's Autodiscover discovery does at runtime.
    #[cfg(test)]
    pub(crate) async fn seed_public_folder_for_tests(
        &self,
        folder: bifrost_types::FolderId,
        routing: cursor::PublicFolderRouting,
    ) {
        self.routing_map.write().await.insert(folder, routing);
    }

    /// Select the `GraphClient` whose `/users/{mailbox}` prefix routes
    /// this scope. A `FolderType` scope whose `FolderId` parses as a
    /// foreign-mailbox folder and whose mailbox has a configured shared
    /// client routes there; everything else (primary folders, unknown
    /// mailboxes) routes through the primary client.
    pub(crate) fn client_for_scope(&self, scope: &CursorScope) -> &GraphClient {
        if let CursorScope::FolderType { folder, .. } = scope
            && let Some(foreign) = foreign::parse_folder(folder).foreign()
            && let Some(client) = self.shared_clients.get(&foreign.mailbox)
        {
            client
        } else {
            &self.client
        }
    }

    /// Select the `GraphClient` for an owning mailbox decoded from a
    /// foreign-encoded message/blob id. `None` (a primary-mailbox item)
    /// routes through `/me`; a configured shared mailbox routes through
    /// its `/users/{mailbox}` client. An unconfigured mailbox falls back
    /// to the primary client - the subsequent request will surface the
    /// real `/me` 404, which is more honest than a silent local error for
    /// an id this account never minted.
    pub(crate) fn client_for_owner(&self, owner: Option<&str>) -> &GraphClient {
        owner
            .and_then(|mailbox| self.shared_clients.get(mailbox))
            .unwrap_or(&self.client)
    }

    /// Look up the public-folder routing for a native EWS folder id, or
    /// `None` if the folder is not a public folder. Presence in the map
    /// is the discriminator that routes a `CursorScope::Folder` onto the
    /// public-folder sync path (vs the existing reject-on-delta path).
    pub(crate) async fn public_folder_routing(
        &self,
        folder: &bifrost_types::FolderId,
    ) -> Option<cursor::PublicFolderRouting> {
        self.routing_map.read().await.get(folder).cloned()
    }

    /// The owning shared-mailbox identity for a foreign scope, or `None`
    /// for a primary-mailbox scope. Used to decide whether a per-scope
    /// permission denial quarantines just this scope (foreign) or
    /// escalates account-wide (primary).
    pub(crate) fn owner_of_scope(&self, scope: &CursorScope) -> Option<bifrost_types::MailboxId> {
        if let CursorScope::FolderType { folder, .. } = scope {
            foreign::parse_folder(folder)
                .foreign()
                .map(|foreign| bifrost_types::MailboxId(foreign.mailbox.clone()))
        } else {
            None
        }
    }
}

// pub: sync-engine consumers register this factory so reopen can mint fresh Graph accounts.
pub struct GraphAccountFactory {
    client: GraphClient,
    push_mode: PushMode,
    push_endpoint: Option<PushEndpoint>,
    shared_mailboxes: Vec<String>,
    public_folders: bool,
}

impl GraphAccountFactory {
    // pub: ergonomic factory construction from an already configured GraphClient.
    pub fn new(client: GraphClient) -> Self {
        Self {
            client,
            push_mode: PushMode::GraphSubscriptions,
            push_endpoint: None,
            shared_mailboxes: Vec::new(),
            public_folders: false,
        }
    }

    /// Opt in to Exchange public-folder discovery and sync. Default off:
    /// no existing Graph account pays the Autodiscover round-trips.
    /// Discovered public folders surface as ordinary
    /// `CursorScope::Folder` scopes synced by the no-delta-token poll
    /// strategy. A per-folder Autodiscover/permission failure is skipped
    /// with a scoped warning, not a discovery-wide failure.
    // pub: public-folder consumers opt in before building the factory.
    pub fn with_public_folders(mut self) -> Self {
        self.public_folders = true;
        self
    }

    /// Register a delegate/shared mailbox by its routing key (the SMTP
    /// address or user id Graph accepts at `/users/{id}`). The mailbox's
    /// folders are discovered, established, and synced alongside the
    /// primary mailbox. Auto-discovery of delegated mailboxes is A5b
    /// (Autodiscover/EWS); until then the consumer names them here.
    // pub: shared-mailbox consumers register foreign mailboxes before building the factory.
    pub fn with_shared_mailbox(mut self, mailbox: impl Into<String>) -> Self {
        self.shared_mailboxes.push(mailbox.into());
        self
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
        let shared_mailboxes = self.shared_mailboxes.clone();
        let public_folders = self.public_folders;
        Box::pin(async move {
            client.attach_account(account_id);
            let profile = client.get_profile().await.map_err(|e| {
                super::account::graph_error::into_account_error(
                    e,
                    super::account::graph_error::GraphErrorContext::graph(
                        bifrost_types::AccountOperation::Discover,
                    ),
                )
            })?;
            let user_email = profile.mail.or(profile.user_principal_name);
            let account = GraphAccount::new(
                client.clone(),
                push_mode,
                push_endpoint,
                &shared_mailboxes,
                public_folders,
                user_email,
            );
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
        let decoded = cursor::decode_cursor(cursor).ok();
        // A public folder is a client-maintained watermark poll, not a
        // server-issued delta token; report `Poll` so the descriptor is
        // honest about its strategy. Delta kinds stay `ServerCursor`.
        let strategy = match decoded.as_ref().map(|p| &p.kind) {
            Some(cursor::GraphCursorKind::PublicFolder(_)) => SyncStrategy::Poll,
            Some(_) => SyncStrategy::ServerCursor,
            None => SyncStrategy::None,
        };
        CursorDescriptor {
            cost_class: if decoded.is_some() {
                CostClass::Cheap
            } else {
                CostClass::Expensive
            },
            strategy,
            freshness: decoded.is_some().then(Instant::now),
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
        let account = self.clone();
        Box::pin(async move {
            // Public-folder scope: discriminate by routing-map
            // membership, NOT by the `CursorScope::Folder` variant alone
            // (that variant is already live on the Graph delta path and
            // must keep rejecting non-public folders).
            if let CursorScope::Folder(folder) = &scope
                && account.public_folder_routing(folder).await.is_some()
            {
                return Ok(CursorEstablishment::EstablishViaInventory);
            }
            // Fall through: a bare Folder NOT in the routing map stays
            // Unsupported (preserving the reject-on-bare-Folder
            // invariant), as does any other unsupported scope.
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
        // Route a public-folder `CursorScope::Folder` (present in the
        // routing map) to the no-delta-token inventory pass; everything
        // else (including a bare Folder for a non-public folder) stays
        // on the existing delta inventory path, which still rejects it.
        if let CursorScope::Folder(folder) = &scope {
            let account = self.clone();
            let folder = folder.clone();
            let scope = scope.clone();
            return Box::pin(
                stream::once(async move {
                    if account.public_folder_routing(&folder).await.is_some() {
                        public_folder::public_folder_inventory_stream(account, scope)
                    } else {
                        inventory::inventory_stream(account, scope)
                    }
                })
                .flatten(),
            );
        }
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
        // A decoded `PublicFolder` payload routes to the no-delta-token
        // poll strategy; any other (or undecodable) cursor stays on the
        // existing delta changes path. The cursor kind is unambiguous,
        // so no routing-map check is needed - the routing rides in the
        // payload.
        if matches!(
            cursor::decode_cursor(&cursor).map(|p| p.kind),
            Ok(cursor::GraphCursorKind::PublicFolder(_))
        ) {
            return public_folder::public_folder_changes_stream(self.clone(), cursor);
        }
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

    fn open_raw_rfc822(&self, message: ObjectId) -> AccountStream<SyncEvent<Bytes>> {
        blob::open_raw_rfc822(self.clone(), message)
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

    fn set_importance(
        &self,
        target: MutationTarget,
        level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::set_importance(account, target, level).await })
    }

    fn send_message(&self, request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::send_message(account, request).await })
    }

    fn send_raw_message(
        &self,
        raw: Bytes,
        save_to_sent: Option<bool>,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::send_raw_message(account, raw, save_to_sent).await })
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

    fn host_attachment(
        &self,
        bytes: Bytes,
        meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        cloud::host_attachment(self.clone(), bytes, meta)
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

    fn cancel_scheduled_send(&self, handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::cancel_scheduled_send(account, handle).await })
    }

    fn reschedule_send(
        &self,
        handle: ObjectId,
        scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { pim::reschedule_send(account, handle, scheduled).await })
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
        let account = self.clone();
        Box::pin(async move { contacts::address_books_list(account).await })
    }

    fn contacts_list(
        &self,
        address_book: Option<AddressBookId>,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { contacts::list(account, address_book, page_cursor).await })
    }

    fn contact_get(&self, contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        let account = self.clone();
        Box::pin(async move { contacts::get(account, contact).await })
    }

    fn contact_create(
        &self,
        contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { contacts::create(account, contact).await })
    }

    fn contact_update(
        &self,
        contact: ContactId,
        patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { contacts::update(account, contact, patch).await })
    }

    fn contact_delete(&self, contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { contacts::delete(account, contact).await })
    }

    fn contact_search(
        &self,
        request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { contacts::search(account, request).await })
    }

    fn directory_search(
        &self,
        query: String,
        limit: Option<u32>,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        let account = self.clone();
        Box::pin(
            async move { contacts::directory_search(account, query, limit, page_cursor).await },
        )
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::calendars_list(account).await })
    }

    fn events_in_range(
        &self,
        range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::events_in_range(account, range).await })
    }

    fn event_get(&self, event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::get(account, event).await })
    }

    fn event_create(&self, event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::create(account, event).await })
    }

    fn event_update(
        &self,
        event: EventId,
        patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::update(account, event, patch).await })
    }

    fn event_delete(&self, event: EventId) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::delete(account, event).await })
    }

    fn event_rsvp(
        &self,
        event: EventId,
        status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::rsvp(account, event, status).await })
    }

    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        let account = self.clone();
        Box::pin(async move { calendar::search(account, request).await })
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use bifrost_types::{AccountErrorKind, AccountOperation, FolderId, ObjectType};

    use super::cursor::{
        GraphCursorPayload, PublicFolderCursor, PublicFolderRouting, encode_cursor, kind_for_scope,
    };
    use super::*;

    fn account() -> GraphAccount {
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions)
    }

    fn public_folder_routing() -> PublicFolderRouting {
        PublicFolderRouting {
            anchor_mailbox: "content@contoso.com".to_string(),
            public_folder_mailbox: Some("pf@contoso.com".to_string()),
        }
    }

    fn public_folder_cursor(folder: &str) -> ChangeCursor {
        let pf = PublicFolderCursor {
            folder_id: folder.to_string(),
            routing: public_folder_routing(),
            watermark: Some("2026-03-01T10:00:00Z".to_string()),
            last_full_scan_at: None,
            live_ids: Vec::new(),
            boundary_ids: Vec::new(),
            degraded: false,
        };
        encode_cursor(
            CursorScope::Folder(FolderId(folder.to_string())),
            GraphCursorPayload::public_folder(pf),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn bare_folder_in_routing_map_establishes_via_inventory() {
        let account = account();
        let folder = FolderId("AAMkPF=".to_string());
        account
            .seed_public_folder_for_tests(folder.clone(), public_folder_routing())
            .await;

        // A Folder scope present in the routing map establishes via
        // inventory.
        let in_map = account
            .establish_initial_cursor(CursorScope::Folder(folder))
            .await
            .expect("seeded public folder establishes");
        assert!(matches!(in_map, CursorEstablishment::EstablishViaInventory));

        // A Folder scope absent from the map is rejected exactly as
        // today (preserving initial_delta_url_rejects_bare_folder_scope).
        let absent = account
            .establish_initial_cursor(CursorScope::Folder(FolderId("other".to_string())))
            .await;
        let err = absent.expect_err("bare non-public folder rejected");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::EstablishCursor)
        ));
    }

    #[test]
    fn describe_cursor_reports_poll_for_public_folder() {
        let account = account();
        let pf = account.describe_cursor(&public_folder_cursor("AAMkPF="));
        assert_eq!(pf.strategy, SyncStrategy::Poll);

        let messages_scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Email,
        };
        let messages = encode_cursor(
            messages_scope.clone(),
            GraphCursorPayload::new(
                kind_for_scope(&messages_scope).unwrap(),
                "https://graph.example/delta".to_string(),
                None,
            ),
        )
        .unwrap();
        assert_eq!(
            account.describe_cursor(&messages).strategy,
            SyncStrategy::ServerCursor
        );
    }

    #[tokio::test]
    async fn public_folder_push_subscribe_unsupported() {
        let account = account();
        let scope = CursorScope::Folder(FolderId("AAMkPF=".to_string()));
        let err = account
            .push_subscribe(&[scope])
            .await
            .expect_err("public-folder push is unsupported");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::PushSubscribe)
        ));
    }
}
