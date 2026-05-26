mod blobs;
mod capabilities;
mod changes;
mod cursor;
mod flags;
mod inventory;
mod mutation;
mod pim;
mod push;
mod recovery;
mod scopes;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFactory, AccountFuture, AccountId,
    AccountOperation, AccountStream, AttachmentHandle, BlobHandle, ByteRange, Change, ChangeCursor,
    Container, ContainerId, ContainerKind, CostClass, CursorDescriptor, CursorEstablishment,
    CursorScope, DraftHandle, DraftPatch, FlagOp, HydratedObject, HydrationProjection,
    IdempotencyKey, Identity, IdentityId, IdentityPatch, InventoryEntry, ItemOutcome,
    MembershipScope, Message, MutationSuccess, MutationTarget, ObjectId, OpaqueChangeState, Page,
    Priority, Projection, QuotaInfo, ScopeLifecycle, SearchRequest, SendRequest,
    SubscriptionHandle, SyncEvent, SyncStrategy, ThreadHydration, ThreadId, VacationConfig,
    WatchEvent,
};
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::client::GmailClient;
use crate::types::GmailProfile;

pub use push::PubSubConfig;

use self::capabilities::gmail_capabilities;
use self::cursor::{
    GmailChangeState, cursor_from_state, decode_gmail_state_for_profile, encode_gmail_state,
};
use self::push::PubSubControl;
use self::scopes::{ScopeCache, ScopeSnapshot};

/// Factory for opening Gmail accounts through the shared `Account` API.
pub struct GmailAccountFactory {
    client: Arc<GmailClient>,
    pubsub: Option<PubSubConfig>,
}

impl GmailAccountFactory {
    fn from_client(client: GmailClient) -> Self {
        Self {
            client: Arc::new(client),
            pubsub: None,
        }
    }

    /// Construct a Gmail factory from a bearer access token.
    #[must_use]
    pub fn from_access_token(access_token: impl Into<String>) -> Self {
        Self::from_client(GmailClient::new(access_token))
    }

    /// Configure Gmail Cloud Pub/Sub watch ownership for opened accounts.
    #[must_use]
    pub fn with_pubsub_config(mut self, config: PubSubConfig) -> Self {
        self.pubsub = Some(config);
        self
    }

    /// Configure an account-wide Gmail Cloud Pub/Sub watch topic.
    #[must_use]
    pub fn with_pubsub_topic(self, topic: impl Into<String>) -> Self {
        self.with_pubsub_config(PubSubConfig::new(topic))
    }
}

impl AccountFactory for GmailAccountFactory {
    fn open(&self, account_id: AccountId) -> AccountFuture<Result<Arc<dyn Account>, AccountError>> {
        let client = Arc::new(self.client.for_account(account_id));
        let pubsub = self.pubsub.clone();
        Box::pin(async move {
            let account = GmailAccount::open(client, pubsub).await?;
            Ok(account as Arc<dyn Account>)
        })
    }
}

struct GmailAccount {
    client: Arc<GmailClient>,
    capabilities: AccountCapabilities,
    profile: GmailProfile,
    seed_state: OpaqueChangeState,
    pubsub: Arc<PubSubControl>,
    scope_cache: ScopeCache,
    shutdown: CancellationToken,
    closed: AtomicBool,
}

impl GmailAccount {
    async fn open(
        client: Arc<GmailClient>,
        pubsub: Option<PubSubConfig>,
    ) -> Result<Arc<Self>, AccountError> {
        let profile = client.get_profile().await.map_err(|error| {
            recovery::into_account_error(error, recovery::GmailErrorContext::open())
        })?;
        let history_id = profile.history_id.parse::<u64>().map_err(|error| {
            recovery::into_account_error(
                crate::error::Error::missing_field(
                    "historyId",
                    format!("gmail profile invalid history id: {error}"),
                ),
                recovery::GmailErrorContext::open(),
            )
        })?;
        let seed_state = encode_gmail_state(&GmailChangeState::new(
            history_id,
            profile.email_address.clone(),
        ));
        Ok(Arc::new(Self {
            client,
            capabilities: gmail_capabilities(),
            profile,
            seed_state,
            pubsub: Arc::new(PubSubControl::new(pubsub)),
            scope_cache: Arc::new(std::sync::RwLock::new(ScopeSnapshot::empty())),
            shutdown: CancellationToken::new(),
            closed: AtomicBool::new(false),
        }))
    }
}

impl Account for GmailAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.capabilities
    }

    fn set_priority(&self, priority: Priority) {
        self.client.account_net().set_priority(priority);
    }

    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        self.client.account_net().set_bandwidth_cap(bps);
    }

    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor {
        let valid =
            decode_gmail_state_for_profile(&cursor.server_state, &self.profile.email_address)
                .is_ok();
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
        scopes::discover_cursor_scopes()
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        scopes::discover_memberships(Arc::clone(&self.client), Arc::clone(&self.scope_cache))
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycle> {
        scopes::scope_lifecycle_stream(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            self.shutdown.clone(),
        )
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        let seed = self.seed_state.clone();
        Box::pin(async move {
            if !matches!(scope, CursorScope::Account) {
                return Err(recovery::into_account_error(
                    crate::error::Error::unsupported(AccountOperation::EstablishCursor),
                    recovery::GmailErrorContext::establish_cursor(),
                ));
            }
            Ok(CursorEstablishment::Ready(cursor_from_state(seed)))
        })
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<SyncEvent<InventoryEntry>> {
        inventory::inventory_stream(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            scope,
        )
    }

    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<HydratedObject>> {
        inventory::get_stream(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            ids,
            projection,
        )
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        changes::changes_stream(Arc::clone(&self.client), self.profile.clone(), cursor)
    }

    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<SubscriptionHandle, AccountError>> {
        push::push_subscribe(
            Arc::clone(&self.client),
            Arc::clone(&self.pubsub),
            self.shutdown.clone(),
            scopes.to_vec(),
        )
    }

    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        push::push_unsubscribe(Arc::clone(&self.client), Arc::clone(&self.pubsub), handle)
    }

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        push::push_stream(Arc::clone(&self.pubsub), self.shutdown.clone())
    }

    fn open_blob(&self, handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        blobs::open_blob(Arc::clone(&self.client), handle)
    }

    fn open_blob_range(
        &self,
        handle: BlobHandle,
        range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        blobs::open_blob_range(Arc::clone(&self.client), handle, range)
    }

    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        op: FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::bulk_set_flags(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            targets,
            op,
            key,
        )
    }

    fn bulk_move(
        &self,
        targets: AccountStream<ObjectId>,
        destination: MembershipScope,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::bulk_move(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            targets,
            destination,
            key,
        )
    }

    fn bulk_destroy(
        &self,
        targets: AccountStream<ObjectId>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::bulk_destroy(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            targets,
            key,
        )
    }

    fn add_to_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::add_to_container(Arc::clone(&self.client), target, container)
    }

    fn remove_from_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::remove_from_container(Arc::clone(&self.client), target, container)
    }

    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(recovery::into_account_error(
                crate::error::Error::unsupported(AccountOperation::SetKeyword),
                recovery::GmailErrorContext::mutation(AccountOperation::SetKeyword),
            ))
        })
    }

    fn set_label_membership(
        &self,
        target: MutationTarget,
        label: ContainerId,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::set_label_membership(Arc::clone(&self.client), target, label, value)
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(recovery::into_account_error(
                crate::error::Error::unsupported(AccountOperation::SetCategory),
                recovery::GmailErrorContext::mutation(AccountOperation::SetCategory),
            ))
        })
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(recovery::into_account_error(
                crate::error::Error::unsupported(AccountOperation::SetExtendedProperty),
                recovery::GmailErrorContext::mutation(AccountOperation::SetExtendedProperty),
            ))
        })
    }

    fn set_is_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::set_is_read(Arc::clone(&self.client), target, is_read)
    }

    fn send_message(&self, request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::send_message(
            Arc::clone(&self.client),
            self.profile.email_address.clone(),
            request,
        )
    }

    fn attachment_upload(
        &self,
        bytes: AccountStream<Result<Bytes, AccountError>>,
        mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        pim::attachment_upload(bytes, mime)
    }

    fn draft_create(&self, patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        pim::draft_create(
            Arc::clone(&self.client),
            self.profile.email_address.clone(),
            patch,
        )
    }

    fn draft_update(
        &self,
        draft: DraftHandle,
        patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::draft_update(
            Arc::clone(&self.client),
            self.profile.email_address.clone(),
            draft,
            patch,
        )
    }

    fn draft_discard(&self, draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        pim::draft_discard(Arc::clone(&self.client), draft)
    }

    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::draft_send(Arc::clone(&self.client), draft)
    }

    fn search(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        pim::search(Arc::clone(&self.client), request)
    }

    fn search_messages(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        pim::search_messages(Arc::clone(&self.client), request)
    }

    fn containers_list(&self) -> AccountFuture<Result<Vec<Container>, AccountError>> {
        pim::containers_list(Arc::clone(&self.client), Arc::clone(&self.scope_cache))
    }

    fn container_create(
        &self,
        kind: ContainerKind,
        name: String,
        parent: Option<ContainerId>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        pim::container_create(Arc::clone(&self.client), kind, name, parent)
    }

    fn container_rename(
        &self,
        container: ContainerId,
        name: String,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::container_rename(Arc::clone(&self.client), container, name)
    }

    fn container_move(
        &self,
        container: ContainerId,
        new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::container_move(container, new_parent)
    }

    fn container_delete(&self, container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        pim::container_delete(Arc::clone(&self.client), container)
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        pim::identities_list(Arc::clone(&self.client))
    }

    fn identity_update(
        &self,
        identity: IdentityId,
        patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::identity_update(Arc::clone(&self.client), identity, patch)
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        pim::vacation_get(Arc::clone(&self.client))
    }

    fn vacation_set(&self, config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        pim::vacation_set(Arc::clone(&self.client), config)
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        pim::quota_get()
    }

    fn thread_hydrate(
        &self,
        thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        pim::thread_hydrate(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            thread,
        )
    }

    fn message_hydrate(
        &self,
        message: ObjectId,
        projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        pim::message_hydrate(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            message,
            projection,
        )
    }

    fn move_thread(
        &self,
        thread: ThreadId,
        target: ContainerId,
        source: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::move_thread(Arc::clone(&self.client), thread, target, source)
    }

    fn delete_thread(
        &self,
        thread: ThreadId,
        current: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::delete_thread(Arc::clone(&self.client), thread, current)
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Box::pin(async { Ok(()) });
        }
        self.shutdown.cancel();
        let pubsub = Arc::clone(&self.pubsub);
        Box::pin(async move {
            pubsub.abort_renewer().await;
            Ok(())
        })
    }
}
