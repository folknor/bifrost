mod blobs;
mod capabilities;
mod changes;
mod cursor;
mod flags;
mod inventory;
mod mutation;
mod push;
mod recovery;
mod scopes;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Instant;

use bifrost_types::{
    Account, AccountCapabilities, AccountFactory, AccountFuture, AccountStream, BlobHandle,
    ByteRange, Change, ChangeCursor, CostClass, CursorDescriptor, CursorEstablishment, CursorScope,
    Error as AccountError, FlagOp, HydratedObject, IdempotencyKey, InventoryEntry, MembershipScope,
    MutationResult, ObjectId, OpaqueChangeState, Priority, Projection, ScopeLifecycle,
    SubscriptionHandle, SyncEvent, SyncStrategy, WatchEvent,
};
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::client::GmailClient;
use crate::types::GmailProfile;

// pub: downstream consumers configure Gmail Pub/Sub before registering the factory.
pub use push::PubSubConfig;

use self::capabilities::gmail_capabilities;
use self::cursor::{
    GmailChangeState, cursor_from_state, decode_gmail_state_for_profile, encode_gmail_state,
};
use self::push::PubSubControl;
use self::scopes::{ScopeCache, ScopeSnapshot};

const UNLIMITED_BANDWIDTH: u64 = u64::MAX;

// pub: the sync engine's cross-crate conformance test and downstream engines register this factory.
pub struct GmailAccountFactory {
    client: Arc<GmailClient>,
    pubsub: Option<PubSubConfig>,
}

impl GmailAccountFactory {
    // pub: direct callers with a preconfigured GmailClient can still register an AccountFactory.
    #[must_use]
    pub fn new(client: GmailClient) -> Self {
        Self {
            client: Arc::new(client),
            pubsub: None,
        }
    }

    // pub: ergonomic AccountFactory construction for callers that already hold a bearer token.
    #[must_use]
    pub fn from_access_token(access_token: impl Into<String>) -> Self {
        Self::new(GmailClient::new(access_token))
    }

    // pub: Gmail Pub/Sub subscription ownership is configured on the factory before open.
    #[must_use]
    pub fn with_pubsub_config(mut self, config: PubSubConfig) -> Self {
        self.pubsub = Some(config);
        self
    }

    // pub: common Pub/Sub setup needs only a topic and no label filter.
    #[must_use]
    pub fn with_pubsub_topic(self, topic: impl Into<String>) -> Self {
        self.with_pubsub_config(PubSubConfig::new(topic))
    }
}

impl AccountFactory for GmailAccountFactory {
    fn open(&self) -> AccountFuture<Result<Arc<dyn Account>, AccountError>> {
        let client = Arc::clone(&self.client);
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
    priority: AtomicU8,
    bandwidth_cap: AtomicU64,
}

impl GmailAccount {
    async fn open(
        client: Arc<GmailClient>,
        pubsub: Option<PubSubConfig>,
    ) -> Result<Arc<Self>, AccountError> {
        let profile = client
            .get_profile()
            .await
            .map_err(|error| recovery::account_error_from_gmail(&error))?;
        let history_id = profile.history_id.parse::<u64>().map_err(|error| {
            AccountError::Other(format!("gmail profile carried invalid history id: {error}"))
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
            priority: AtomicU8::new(priority_to_u8(Priority::Normal)),
            bandwidth_cap: AtomicU64::new(UNLIMITED_BANDWIDTH),
        }))
    }
}

impl Account for GmailAccount {
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
                return Err(AccountError::Unsupported);
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
    ) -> AccountStream<SyncEvent<MutationResult>> {
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
    ) -> AccountStream<SyncEvent<MutationResult>> {
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
    ) -> AccountStream<SyncEvent<MutationResult>> {
        mutation::bulk_destroy(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            targets,
            key,
        )
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

fn priority_to_u8(priority: Priority) -> u8 {
    match priority {
        Priority::Foreground => 0,
        Priority::Normal => 1,
        Priority::Background => 2,
        Priority::Bulk => 3,
        _ => 1,
    }
}
