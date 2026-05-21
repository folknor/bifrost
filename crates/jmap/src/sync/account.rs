use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use bifrost_types::{
    Account, AccountCapabilities, AccountFuture, AccountStream, BlobHandle, ByteRange,
    ChangeCursor, CostClass, CursorDescriptor, CursorEstablishment, CursorScope, Error,
    HydratedObject, IdempotencyKey, InventoryEntry, MembershipScope, MutationResult, ObjectId,
    Priority, Projection, ScopeLifecycle, SubscriptionHandle, SyncEvent, SyncStrategy, WatchEvent,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::client::Client;
use crate::transport_reqwest::ReqwestTransport;

use super::capabilities::CoreLimits;
use super::{blob, changes, discover, hydrate, inventory, mutation, push, state};

type MailAccount = crate::account::Account<ReqwestTransport>;

const BANDWIDTH_CAP_UNLIMITED: u64 = u64::MAX;

pub struct JmapAccount {
    pub(crate) client: Client,
    pub(crate) mail: MailAccount,
    pub(crate) caps: AccountCapabilities,
    pub(crate) core_limits: CoreLimits,
    pub(crate) seed_states: HashMap<CursorScope, bifrost_types::OpaqueChangeState>,
    pub(crate) ws: push::WsState,
    pub(crate) subscriptions: Arc<Mutex<HashMap<SubscriptionHandle, push::DataTypeSet>>>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) closed: AtomicBool,
    pub(crate) priority: AtomicU8,
    pub(crate) bandwidth_cap: AtomicU64,
    pub(crate) subscription_seq: AtomicU64,
    pub(crate) email_state: Arc<Mutex<Option<String>>>,
    pub(crate) mailbox_state: Arc<Mutex<Option<String>>>,
    pub(crate) thread_state: Arc<Mutex<Option<String>>>,
    pub(crate) mailbox_names: Arc<Mutex<HashMap<String, String>>>,
}

impl JmapAccount {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: Client,
        mail: MailAccount,
        caps: AccountCapabilities,
        core_limits: CoreLimits,
        seed_states: HashMap<CursorScope, bifrost_types::OpaqueChangeState>,
        ws: push::WsState,
        shutdown: CancellationToken,
        email_state: Option<String>,
        mailbox_state: Option<String>,
        thread_state: Option<String>,
        mailbox_names: HashMap<String, String>,
    ) -> Self {
        Self {
            client,
            mail,
            caps,
            core_limits,
            seed_states,
            ws,
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            shutdown,
            closed: AtomicBool::new(false),
            priority: AtomicU8::new(Priority::Normal as u8),
            bandwidth_cap: AtomicU64::new(BANDWIDTH_CAP_UNLIMITED),
            subscription_seq: AtomicU64::new(1),
            email_state: Arc::new(Mutex::new(email_state)),
            mailbox_state: Arc::new(Mutex::new(mailbox_state)),
            thread_state: Arc::new(Mutex::new(thread_state)),
            mailbox_names: Arc::new(Mutex::new(mailbox_names)),
        }
    }

    pub(crate) fn cursor_scopes(&self) -> Vec<CursorScope> {
        let ordered = [
            CursorScope::Type(bifrost_types::ObjectType::Email),
            CursorScope::Type(bifrost_types::ObjectType::Mailbox),
            CursorScope::Type(bifrost_types::ObjectType::Thread),
        ];

        ordered
            .into_iter()
            .filter(|scope| self.seed_states.contains_key(scope))
            .collect()
    }

    pub(crate) fn next_subscription_handle(&self) -> SubscriptionHandle {
        let next = self.subscription_seq.fetch_add(1, Ordering::AcqRel);
        SubscriptionHandle(format!("jmap-ws-{next}"))
    }
}

impl Account for JmapAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.caps
    }

    fn set_priority(&self, priority: Priority) {
        self.priority.store(priority as u8, Ordering::Release);
    }

    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        self.bandwidth_cap
            .store(bps.unwrap_or(BANDWIDTH_CAP_UNLIMITED), Ordering::Release);
    }

    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor {
        let valid = state::decode_cursor(cursor).is_ok();
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
            freshness: None,
        }
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        discover::cursor_scopes(self.cursor_scopes())
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        discover::memberships(self.mail.clone())
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycle> {
        discover::scope_lifecycle(
            self.mail.clone(),
            self.core_limits,
            Arc::clone(&self.mailbox_state),
            Arc::clone(&self.mailbox_names),
            self.shutdown.clone(),
        )
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, Error>> {
        let seed = self.seed_states.get(&scope).cloned();
        Box::pin(async move {
            let server_state = seed.ok_or(Error::Unsupported)?;
            Ok(CursorEstablishment::Ready(ChangeCursor {
                scope,
                server_state,
                advanced_through: None,
                envelope_version: state::CHANGE_CURSOR_ENVELOPE_VERSION,
            }))
        })
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<SyncEvent<InventoryEntry>> {
        inventory::stream(self.mail.clone(), self.core_limits, scope)
    }

    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<HydratedObject>> {
        hydrate::stream(self.mail.clone(), self.core_limits, ids, projection)
    }

    fn changes_stream(
        &self,
        cursor: ChangeCursor,
    ) -> AccountStream<SyncEvent<bifrost_types::Change>> {
        changes::stream(
            self.mail.clone(),
            self.core_limits,
            cursor,
            Arc::clone(&self.email_state),
            Arc::clone(&self.mailbox_state),
            Arc::clone(&self.thread_state),
        )
    }

    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<SubscriptionHandle, Error>> {
        push::subscribe(
            self.client.clone(),
            self.caps.push,
            self.next_subscription_handle(),
            scopes.to_vec(),
            Arc::clone(&self.subscriptions),
            Arc::clone(&self.ws.enabled),
        )
    }

    fn push_unsubscribe(&self, handle: SubscriptionHandle) -> AccountFuture<Result<(), Error>> {
        push::unsubscribe(
            self.client.clone(),
            handle,
            Arc::clone(&self.subscriptions),
            Arc::clone(&self.ws.enabled),
        )
    }

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        push::stream(self.ws.tx.subscribe())
    }

    fn open_blob(&self, handle: BlobHandle) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open(self.client.clone(), self.mail.id().clone(), handle)
    }

    fn open_blob_range(
        &self,
        handle: BlobHandle,
        range: ByteRange,
    ) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open_range(handle, range)
    }

    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        op: bifrost_types::FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<MutationResult>> {
        mutation::set_flags(
            self.mail.clone(),
            self.core_limits,
            Arc::clone(&self.email_state),
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
        mutation::move_to(
            self.mail.clone(),
            self.core_limits,
            Arc::clone(&self.email_state),
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
        mutation::destroy(
            self.mail.clone(),
            self.core_limits,
            Arc::clone(&self.email_state),
            targets,
            key,
        )
    }

    fn close(&self) -> AccountFuture<Result<(), Error>> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Box::pin(async { Ok(()) });
        }

        let client = self.client.clone();
        let shutdown = self.shutdown.clone();
        Box::pin(async move {
            shutdown.cancel();
            let result = client.disable_push_ws().await;
            match result {
                Ok(()) | Err(crate::Error::WebSocketNotConnected) => Ok(()),
                Err(err) => Err(super::error::to_account_error(err)),
            }
        })
    }
}
