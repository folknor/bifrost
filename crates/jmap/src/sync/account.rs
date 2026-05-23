use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bifrost_types::{
    Account, AccountCapabilities, AccountFuture, AccountStream, AttachmentHandle, BlobHandle,
    ByteRange, ChangeCursor, Container, ContainerId, ContainerKind, CostClass, CursorDescriptor,
    CursorEstablishment, CursorScope, DraftHandle, DraftPatch, Error, HydratedObject,
    HydrationProjection, IdempotencyKey, Identity, IdentityId, IdentityPatch, InventoryEntry,
    InventoryPartition, InventoryPartitioning, Label, MembershipScope, Message, MutationResult,
    MutationTarget, ObjectId, Page, Priority, Projection, QuotaInfo, ScopeLifecycle, SearchRequest,
    SendRequest, SubscriptionHandle, SyncEvent, SyncStrategy, ThreadHydration, ThreadId,
    VacationConfig, WatchEvent,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::client::Client;
use crate::transport_reqwest::ReqwestTransport;

use super::capabilities::CoreLimits;
use super::{blob, changes, discover, hydrate, inventory, mutation, pim, push, state};

type MailAccount = crate::account::Account<ReqwestTransport>;

pub(crate) struct JmapAccount {
    pub(crate) client: Client,
    pub(crate) mail: MailAccount,
    pub(crate) submission: Option<MailAccount>,
    pub(crate) vacation: Option<MailAccount>,
    pub(crate) quota: Option<MailAccount>,
    pub(crate) caps: AccountCapabilities,
    pub(crate) core_limits: CoreLimits,
    pub(crate) seed_states: HashMap<CursorScope, bifrost_types::OpaqueChangeState>,
    pub(crate) ws: push::WsState,
    pub(crate) subscriptions: Arc<Mutex<HashMap<SubscriptionHandle, push::DataTypeSet>>>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) closed: AtomicBool,
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
        submission: Option<MailAccount>,
        vacation: Option<MailAccount>,
        quota: Option<MailAccount>,
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
            submission,
            vacation,
            quota,
            caps,
            core_limits,
            seed_states,
            ws,
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            shutdown,
            closed: AtomicBool::new(false),
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
        self.client.transport().set_priority(priority);
    }

    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        self.client.transport().set_bandwidth_cap(bps);
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

    fn inventory_partitioning(&self, scope: &CursorScope) -> InventoryPartitioning {
        match scope {
            CursorScope::Type(bifrost_types::ObjectType::Email) => {
                InventoryPartitioning::PageCount {
                    total: None,
                    page_size: u32::try_from(self.core_limits.max_objects_in_get).ok(),
                }
            }
            _ => InventoryPartitioning::Full,
        }
    }

    fn inventory_partition_stream(
        &self,
        scope: CursorScope,
        partition: InventoryPartition,
    ) -> AccountStream<SyncEvent<InventoryEntry>> {
        inventory::stream_partition(self.mail.clone(), self.core_limits, scope, partition)
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
        push::stream(self.ws.tx.subscribe(), self.shutdown.clone())
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

    fn add_to_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), Error>> {
        pim::add_to_container(
            self.mail.clone(),
            Arc::clone(&self.email_state),
            target,
            container,
        )
    }

    fn remove_from_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), Error>> {
        pim::remove_from_container(
            self.mail.clone(),
            Arc::clone(&self.email_state),
            target,
            container,
        )
    }

    fn set_keyword(
        &self,
        target: MutationTarget,
        keyword: String,
        value: bool,
    ) -> AccountFuture<Result<(), Error>> {
        pim::set_keyword(
            self.mail.clone(),
            Arc::clone(&self.email_state),
            target,
            keyword,
            value,
        )
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), Error>> {
        Box::pin(async { Err(Error::Unsupported) })
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), Error>> {
        Box::pin(async { Err(Error::Unsupported) })
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), Error>> {
        Box::pin(async { Err(Error::Unsupported) })
    }

    fn set_is_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), Error>> {
        pim::set_is_read(
            self.mail.clone(),
            Arc::clone(&self.email_state),
            target,
            is_read,
        )
    }

    fn send_message(&self, request: SendRequest) -> AccountFuture<Result<ObjectId, Error>> {
        if self.submission.is_none() {
            return Box::pin(async { Err(Error::Unsupported) });
        }
        pim::send_message(self.mail.clone(), Arc::clone(&self.email_state), request)
    }

    fn attachment_upload(
        &self,
        bytes: AccountStream<Result<bytes::Bytes, Error>>,
        mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, Error>> {
        pim::attachment_upload(self.mail.clone(), bytes, mime)
    }

    fn draft_create(&self, patch: DraftPatch) -> AccountFuture<Result<DraftHandle, Error>> {
        pim::draft_create(self.mail.clone(), Arc::clone(&self.email_state), patch)
    }

    fn draft_update(
        &self,
        draft: DraftHandle,
        patch: DraftPatch,
    ) -> AccountFuture<Result<(), Error>> {
        pim::draft_update(
            self.mail.clone(),
            Arc::clone(&self.email_state),
            draft,
            patch,
        )
    }

    fn draft_discard(&self, draft: DraftHandle) -> AccountFuture<Result<(), Error>> {
        pim::draft_discard(self.mail.clone(), Arc::clone(&self.email_state), draft)
    }

    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<ObjectId, Error>> {
        if self.submission.is_none() {
            return Box::pin(async { Err(Error::Unsupported) });
        }
        pim::draft_send(self.mail.clone(), Arc::clone(&self.email_state), draft)
    }

    fn search(&self, request: SearchRequest) -> AccountFuture<Result<Page<ThreadId>, Error>> {
        pim::search(self.mail.clone(), request)
    }

    fn search_messages(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, Error>> {
        pim::search_messages(self.mail.clone(), request)
    }

    fn containers_list(&self) -> AccountFuture<Result<Vec<Container>, Error>> {
        pim::containers_list(self.mail.clone())
    }

    fn container_create(
        &self,
        kind: ContainerKind,
        name: String,
        parent: Option<ContainerId>,
    ) -> AccountFuture<Result<ContainerId, Error>> {
        pim::container_create(
            self.mail.clone(),
            Arc::clone(&self.mailbox_state),
            kind,
            name,
            parent,
        )
    }

    fn container_rename(
        &self,
        container: ContainerId,
        name: String,
    ) -> AccountFuture<Result<(), Error>> {
        pim::container_rename(
            self.mail.clone(),
            Arc::clone(&self.mailbox_state),
            container,
            name,
        )
    }

    fn container_move(
        &self,
        container: ContainerId,
        new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), Error>> {
        pim::container_move(
            self.mail.clone(),
            Arc::clone(&self.mailbox_state),
            container,
            new_parent,
        )
    }

    fn container_delete(&self, container: ContainerId) -> AccountFuture<Result<(), Error>> {
        pim::container_delete(
            self.mail.clone(),
            Arc::clone(&self.mailbox_state),
            container,
        )
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, Error>> {
        pim::identities_list(self.submission.clone())
    }

    fn identity_update(
        &self,
        identity: IdentityId,
        patch: IdentityPatch,
    ) -> AccountFuture<Result<(), Error>> {
        pim::identity_update(self.submission.clone(), identity, patch)
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, Error>> {
        pim::vacation_get(self.vacation.clone())
    }

    fn vacation_set(&self, config: VacationConfig) -> AccountFuture<Result<(), Error>> {
        pim::vacation_set(self.vacation.clone(), config)
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, Error>> {
        pim::quota_get(self.quota.clone())
    }

    fn thread_hydrate(&self, thread: ThreadId) -> AccountFuture<Result<ThreadHydration, Error>> {
        pim::thread_hydrate(self.mail.clone(), thread)
    }

    fn message_hydrate(
        &self,
        message: ObjectId,
        projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, Error>> {
        pim::message_hydrate(self.mail.clone(), message, projection)
    }

    fn move_thread(
        &self,
        thread: ThreadId,
        target: ContainerId,
        source: Option<ContainerId>,
    ) -> AccountFuture<Result<(), Error>> {
        pim::move_thread(
            self.mail.clone(),
            Arc::clone(&self.email_state),
            thread,
            target,
            source,
        )
    }

    fn apply_label(
        &self,
        target: MutationTarget,
        label: Label,
    ) -> AccountFuture<Result<(), Error>> {
        match label.provenance.kind {
            ContainerKind::Label => self.set_keyword(target, label.provenance.native, true),
            ContainerKind::Folder => self.add_to_container(target, label.id),
            _ => Box::pin(async { Err(Error::Unsupported) }),
        }
    }

    fn remove_label(
        &self,
        target: MutationTarget,
        label: Label,
    ) -> AccountFuture<Result<(), Error>> {
        match label.provenance.kind {
            ContainerKind::Label => self.set_keyword(target, label.provenance.native, false),
            ContainerKind::Folder => self.remove_from_container(target, label.id),
            _ => Box::pin(async { Err(Error::Unsupported) }),
        }
    }

    fn delete_thread(
        &self,
        thread: ThreadId,
        current: Option<ContainerId>,
    ) -> AccountFuture<Result<(), Error>> {
        pim::delete_thread(
            self.mail.clone(),
            Arc::clone(&self.email_state),
            thread,
            current,
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
