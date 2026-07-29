//! Attach-time recovery from an undecodable durable cursor.
//!
//! `handle_schema_incompatible` - the account-wide schema-clear loop -
//! runs on the reopen listener, and the reopen listener only exists
//! once `attach_inner` has finished spawning workers. A cursor that
//! fails to decode during attach therefore has no listener to route to:
//! if `establish_one` propagated the error, attach would fail, nothing
//! would be left running to clear the row, and every subsequent attach
//! would fail identically. The account would be permanently stranded by
//! one unreadable row.
//!
//! These tests pin the attach path healing the row itself: delete the
//! scope's cursor, re-establish from the live account, keep going.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bifrost_sync::{CheckpointStore, Error, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountFactory, AccountFuture, AccountId, AccountStream, AttachmentHandle, BackfillCheckpoint,
    Batch, BatchingPolicy, BlobHandle, BlobRangeSupport, ByteRange, Cause, Change, ChangeCursor,
    CloudUploadMeta, ContactCard, ContactCreate, ContactId, ContactPatch, ContactSearchRequest,
    Container, ContainerId, ContainerKind, ConvenienceShape, CursorDescriptor, CursorEstablishment,
    CursorFreshness, CursorScope, DraftHandle, DraftPatch, EventCreate, EventId, EventPatch,
    EventRange, EventSearchRequest, FilterRuleShape, FilterValidation, FlagOp, HostedAttachment,
    HydratedObject, HydrationProjection, IdempotencyKey, Identity, IdentityId, IdentityPatch,
    Importance, InventoryEntry, ItemOutcome, MembershipScope, Message, MutationCapabilities,
    MutationConcurrency, MutationReplaySafety, MutationSuccess, MutationTarget, ObjectId,
    OpaqueChangeState, Page, PimMethodSupport, Priority, Projection, ProtocolKind, PushCapability,
    QuotaInfo, QuotaSignal, RateLimitClass, RequestCause, RsvpStatus, ScopeLifecycleEvent,
    SearchRequest, SendRequest, ServerFilter, ServerFilterCreate, ServerFilterId,
    ServerFilterPatch, SubscriptionHandle, SyncEvent, ThreadHydration, ThreadId, VacationConfig,
    WatchEvent,
};
use bifrost_types::{AddressBook, AddressBookId};
use bifrost_types::{Calendar, CalendarEvent};
use bytes::Bytes;
use futures::stream;

fn unsupported(op: bifrost_types::AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(op),
        Cause::Request(RequestCause::Unsupported { operation: op }),
    )
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}

fn caps() -> AccountCapabilities {
    AccountCapabilities {
        cursor_freshness: CursorFreshness::ServerIssued,
        blob_range: BlobRangeSupport::No,
        blob_digest_pre_download: false,
        push: PushCapability::None,
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::None,
            replay_safety: MutationReplaySafety::None,
        },
        batching_policy: BatchingPolicy {
            max_items: 100,
            max_wait: std::time::Duration::from_millis(50),
            flush_on_input_close: true,
        },
        rate_limit_class: RateLimitClass::Generous,
        quota_signal: QuotaSignal::None,
        requires_uidvalidity_recheck: false,
        historyid_expires_after: None,
        delta_token_expires_after: None,
        pim_methods: PimMethodSupport::default(),
        filter_rule_shape: FilterRuleShape::None,
        conveniences: ConvenienceShape::default(),
        foreign_namespaces_advertised: false,
    }
}

fn cursor_for(scope: &CursorScope, state: &[u8]) -> ChangeCursor {
    ChangeCursor {
        scope: scope.clone(),
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::Jmap,
            envelope_version: 1,
            bytes: state.to_vec(),
        },
        advanced_through: None,
        envelope_version: 1,
    }
}

/// A store whose `get_change_cursor` reports the row as undecodable -
/// what a consumer store returns when `decode_envelope` hits a schema
/// it cannot read - until the engine deletes it.
#[derive(Default)]
struct PoisonedStore {
    inner: InMemoryCheckpointStore,
    /// Scopes still reporting `SchemaIncompatible`.
    poisoned: Mutex<Vec<CursorScope>>,
    /// Scopes the engine asked to delete, in call order.
    deleted: Mutex<Vec<CursorScope>>,
    fail_reads: bool,
}

impl PoisonedStore {
    fn with_poisoned(scopes: Vec<CursorScope>) -> Self {
        Self {
            poisoned: Mutex::new(scopes),
            ..Default::default()
        }
    }

    fn deleted(&self) -> Vec<CursorScope> {
        self.deleted.lock().expect("deleted lock").clone()
    }

    fn with_read_failure() -> Self {
        Self {
            fail_reads: true,
            ..Default::default()
        }
    }
}

impl CheckpointStore for PoisonedStore {
    fn put_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        cursor: ChangeCursor,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.put_change_cursor(account, cursor)
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>>
    {
        if self.fail_reads {
            return Box::pin(async {
                Err(Error::CheckpointStore(
                    "forced attach-time read failure".into(),
                ))
            });
        }
        let still_poisoned = self
            .poisoned
            .lock()
            .expect("poisoned lock")
            .iter()
            .any(|s| s == scope);
        if still_poisoned {
            return Box::pin(async { Err(Error::SchemaIncompatible) });
        }
        self.inner.get_change_cursor(account, scope)
    }

    fn put_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        checkpoint: BackfillCheckpoint,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.put_backfill(account, checkpoint)
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Option<BackfillCheckpoint>, Error>> + Send + 'a,
        >,
    > {
        self.inner.get_backfill(account, scope)
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.deleted
            .lock()
            .expect("deleted lock")
            .push(scope.clone());
        // Clearing the row clears the poison: a re-read now behaves
        // like any other absent cursor.
        self.poisoned
            .lock()
            .expect("poisoned lock")
            .retain(|s| s != scope);
        self.inner.delete_change_cursor(account, scope)
    }
}

/// Account that discovers a fixed scope list and establishes a cursor
/// for each, recording which scopes it was asked to establish.
type SubscriptionCalls = Arc<Mutex<Vec<(usize, Vec<CursorScope>)>>>;

struct HealAccount {
    caps: AccountCapabilities,
    scopes: Vec<CursorScope>,
    established: Arc<Mutex<Vec<CursorScope>>>,
    closed: Arc<AtomicUsize>,
    generation: usize,
    closed_generations: Arc<Mutex<Vec<usize>>>,
    subscribed: SubscriptionCalls,
    unsubscribed: Arc<Mutex<Vec<(usize, SubscriptionHandle)>>>,
    lifecycle_calls: Arc<Mutex<Vec<usize>>>,
}

struct HealFactory {
    scopes: Vec<CursorScope>,
    established: Arc<Mutex<Vec<CursorScope>>>,
    closed: Arc<AtomicUsize>,
}

impl HealFactory {
    fn new(
        scopes: Vec<CursorScope>,
        established: Arc<Mutex<Vec<CursorScope>>>,
        closed: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            scopes,
            established,
            closed,
        }
    }
}

impl AccountFactory for HealFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<Arc<dyn Account>, AccountError>> {
        let scopes = self.scopes.clone();
        let established = Arc::clone(&self.established);
        let closed = Arc::clone(&self.closed);
        Box::pin(async move {
            let account: Arc<dyn Account> = Arc::new(HealAccount {
                caps: caps(),
                scopes,
                established,
                closed,
                generation: 0,
                closed_generations: Arc::new(Mutex::new(Vec::new())),
                subscribed: Arc::new(Mutex::new(Vec::new())),
                unsubscribed: Arc::new(Mutex::new(Vec::new())),
                lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
            });
            Ok(account)
        })
    }
}

struct RotatingFactory {
    scopes: Mutex<VecDeque<Vec<CursorScope>>>,
    established: Arc<Mutex<Vec<CursorScope>>>,
    closed: Arc<AtomicUsize>,
    closed_generations: Arc<Mutex<Vec<usize>>>,
    subscribed: SubscriptionCalls,
    unsubscribed: Arc<Mutex<Vec<(usize, SubscriptionHandle)>>>,
    lifecycle_calls: Arc<Mutex<Vec<usize>>>,
    opens: AtomicUsize,
}

impl AccountFactory for RotatingFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<Arc<dyn Account>, AccountError>> {
        let scopes = self
            .scopes
            .lock()
            .expect("scopes lock")
            .pop_front()
            .expect("one scope set per expected open");
        let generation = self.opens.fetch_add(1, Ordering::SeqCst);
        let established = Arc::clone(&self.established);
        let closed = Arc::clone(&self.closed);
        let closed_generations = Arc::clone(&self.closed_generations);
        let subscribed = Arc::clone(&self.subscribed);
        let unsubscribed = Arc::clone(&self.unsubscribed);
        let lifecycle_calls = Arc::clone(&self.lifecycle_calls);
        Box::pin(async move {
            let mut account_caps = caps();
            account_caps.push = if generation == 0 {
                PushCapability::None
            } else {
                PushCapability::InProcess
            };
            let account: Arc<dyn Account> = Arc::new(HealAccount {
                caps: account_caps,
                scopes,
                established,
                closed,
                generation,
                closed_generations,
                subscribed,
                unsubscribed,
                lifecycle_calls,
            });
            Ok(account)
        })
    }
}

impl Account for HealAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.caps
    }

    fn set_priority(&self, _priority: Priority) {}

    fn set_bandwidth_cap(&self, _bps: Option<u64>) {}

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        unreachable!("describe_cursor not exercised by attach recovery tests")
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        let items: Vec<SyncEvent<CursorScope>> = vec![
            SyncEvent::Batch(Batch {
                items: self.scopes.clone(),
                page_boundary: bifrost_types::PageBoundary::Final,
                server_latency: std::time::Duration::ZERO,
                bytes_in: 0,
                checkpoint: None,
            }),
            SyncEvent::Done(None),
        ];
        Box::pin(stream::iter(items))
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        Box::pin(stream::empty())
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        self.lifecycle_calls
            .lock()
            .expect("lifecycle calls lock")
            .push(self.generation);
        Box::pin(stream::empty())
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        self.established
            .lock()
            .expect("established lock")
            .push(scope.clone());
        Box::pin(async move {
            Ok(CursorEstablishment::Ready(cursor_for(
                &scope,
                b"freshly-established",
            )))
        })
    }

    fn inventory_stream(&self, _scope: CursorScope) -> AccountStream<SyncEvent<InventoryEntry>> {
        Box::pin(stream::empty())
    }

    fn get_stream(
        &self,
        _ids: AccountStream<ObjectId>,
        _projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        Box::pin(stream::empty())
    }

    fn changes_stream(&self, _cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        Box::pin(stream::empty())
    }

    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<SubscriptionHandle, AccountError>> {
        self.subscribed
            .lock()
            .expect("subscribed lock")
            .push((self.generation, scopes.to_vec()));
        let handle = SubscriptionHandle(format!("generation-{}", self.generation));
        Box::pin(async move { Ok(handle) })
    }

    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.unsubscribed
            .lock()
            .expect("unsubscribed lock")
            .push((self.generation, handle));
        Box::pin(async { Ok(()) })
    }

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        Box::pin(stream::empty())
    }

    fn open_blob(&self, _handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        Box::pin(stream::empty())
    }

    fn open_blob_range(
        &self,
        _handle: BlobHandle,
        _range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        Box::pin(stream::empty())
    }

    fn open_raw_rfc822(&self, _message: ObjectId) -> AccountStream<SyncEvent<Bytes>> {
        Box::pin(stream::empty())
    }

    fn bulk_set_flags(
        &self,
        _targets: AccountStream<ObjectId>,
        _op: FlagOp,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        Box::pin(stream::empty())
    }

    fn bulk_move(
        &self,
        _targets: AccountStream<ObjectId>,
        _destination: MembershipScope,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        Box::pin(stream::empty())
    }

    fn bulk_destroy(
        &self,
        _targets: AccountStream<ObjectId>,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        Box::pin(stream::empty())
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        self.closed.fetch_add(1, Ordering::SeqCst);
        self.closed_generations
            .lock()
            .expect("closed generations lock")
            .push(self.generation);
        Box::pin(async { Ok(()) })
    }

    fn add_to_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::AddToContainer)) })
    }

    fn remove_from_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
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
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SetKeyword)) })
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::SetLabelMembership,
            ))
        })
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SetCategory)) })
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::SetExtendedProperty,
            ))
        })
    }

    fn set_is_read(
        &self,
        _target: MutationTarget,
        _is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SetIsRead)) })
    }

    fn set_importance(
        &self,
        _target: MutationTarget,
        _level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SetImportance)) })
    }

    fn send_message(&self, _request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::Send)) })
    }

    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::AttachmentUpload,
            ))
        })
    }

    fn host_attachment(
        &self,
        _bytes: Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::HostAttachment)) })
    }

    fn draft_create(&self, _patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::DraftCreate)) })
    }

    fn draft_update(
        &self,
        _draft: DraftHandle,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::DraftUpdate)) })
    }

    fn draft_discard(&self, _draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::DraftDiscard)) })
    }

    fn draft_send(&self, _draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::DraftSend)) })
    }

    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::CancelScheduledSend,
            ))
        })
    }

    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::RescheduleSend)) })
    }

    fn search(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::Search)) })
    }

    fn search_messages(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SearchMessages)) })
    }

    fn containers_list(&self) -> AccountFuture<Result<Vec<Container>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContainersList)) })
    }

    fn container_create(
        &self,
        _kind: ContainerKind,
        _name: String,
        _parent: Option<ContainerId>,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::ContainerCreate,
            ))
        })
    }

    fn container_rename(
        &self,
        _container: ContainerId,
        _name: String,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::ContainerRename,
            ))
        })
    }

    fn container_move(
        &self,
        _container: ContainerId,
        _new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContainerMove)) })
    }

    fn container_delete(&self, _container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::ContainerDelete,
            ))
        })
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::IdentitiesList)) })
    }

    fn identity_update(
        &self,
        _identity: IdentityId,
        _patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::IdentityUpdate)) })
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::VacationGet)) })
    }

    fn vacation_set(&self, _config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::VacationSet)) })
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::QuotaGet)) })
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FiltersList)) })
    }

    fn filter_create(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FilterCreate)) })
    }

    fn filter_update(
        &self,
        _filter: ServerFilterId,
        _patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FilterUpdate)) })
    }

    fn filter_delete(&self, _filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FilterDelete)) })
    }

    fn filter_validate(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FilterValidate)) })
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::AddressBooksList,
            ))
        })
    }

    fn contacts_list(
        &self,
        _address_book: Option<AddressBookId>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactsList)) })
    }

    fn contact_get(&self, _contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactGet)) })
    }

    fn contact_create(
        &self,
        _contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactCreate)) })
    }

    fn contact_update(
        &self,
        _contact: ContactId,
        _patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactUpdate)) })
    }

    fn contact_delete(&self, _contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactDelete)) })
    }

    fn contact_search(
        &self,
        _request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactSearch)) })
    }

    fn directory_search(
        &self,
        _query: String,
        _limit: Option<u32>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryCard>, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::DirectorySearch,
            ))
        })
    }

    fn directory_groups_list(
        &self,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroup>, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::DirectoryGroupsList,
            ))
        })
    }

    fn directory_group_expand(
        &self,
        _group: bifrost_types::DirectoryGroupId,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroupMember>, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::DirectoryGroupExpand,
            ))
        })
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::CalendarsList)) })
    }

    fn events_in_range(
        &self,
        _range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventsInRange)) })
    }

    fn event_get(&self, _event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventGet)) })
    }

    fn event_create(&self, _event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventCreate)) })
    }

    fn event_update(
        &self,
        _event: EventId,
        _patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventUpdate)) })
    }

    fn event_delete(&self, _event: EventId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventDelete)) })
    }

    fn event_rsvp(
        &self,
        _event: EventId,
        _status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventRsvp)) })
    }

    fn event_search(
        &self,
        _request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventSearch)) })
    }

    fn thread_hydrate(
        &self,
        _thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::HydrateThread)) })
    }

    fn message_hydrate(
        &self,
        _message: ObjectId,
        _projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::HydrateMessage)) })
    }
}

#[tokio::test]
async fn attach_heals_an_undecodable_cursor_instead_of_failing() {
    let account_id = AccountId("schema-heal".to_owned());
    let scope = CursorScope::Account;
    let store = Arc::new(PoisonedStore::with_poisoned(vec![scope.clone()]));
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));

    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![scope.clone()],
        Arc::clone(&established),
        Arc::clone(&closed),
    ));

    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("an undecodable cursor must not fail the attach");

    assert_eq!(
        store.deleted(),
        vec![scope.clone()],
        "the unreadable row must be cleared, otherwise the next attach hits it again"
    );
    assert_eq!(
        *established.lock().expect("established lock"),
        vec![scope.clone()],
        "the scope must be re-established from the live account"
    );
    let persisted = store
        .get_change_cursor(&account_id, &scope)
        .await
        .expect("the healed row decodes")
        .expect("re-establishment persists a replacement cursor");
    assert_eq!(
        persisted.server_state.bytes,
        b"freshly-established".to_vec()
    );

    engine.detach(&account_id).await.expect("detach succeeds");
    assert_eq!(closed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn one_undecodable_scope_does_not_disturb_its_siblings() {
    let account_id = AccountId("schema-heal-sibling".to_owned());
    let poisoned = CursorScope::Type(bifrost_types::ObjectType::Email);
    let healthy = CursorScope::Type(bifrost_types::ObjectType::Mailbox);
    let store = Arc::new(PoisonedStore::with_poisoned(vec![poisoned.clone()]));
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));

    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![poisoned.clone(), healthy.clone()],
        Arc::clone(&established),
        Arc::clone(&closed),
    ));

    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");

    assert_eq!(
        store.deleted(),
        vec![poisoned.clone()],
        "only the unreadable scope's row is cleared"
    );
    // Both scopes still establish: the healthy one because it had no
    // stored cursor, the poisoned one because its row was dropped.
    assert_eq!(
        *established.lock().expect("established lock"),
        vec![poisoned, healthy]
    );

    engine.detach(&account_id).await.expect("detach succeeds");
    assert_eq!(closed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_attach_closes_the_opened_account() {
    let account_id = AccountId("attach-failure-close".to_owned());
    let store = Arc::new(PoisonedStore::with_read_failure());
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![CursorScope::Account],
        established,
        Arc::clone(&closed),
    ));

    let result = engine.attach(account_id.clone(), factory).await;
    assert!(matches!(result, Err(Error::CheckpointStore(_))));
    assert_eq!(
        closed.load(Ordering::SeqCst),
        1,
        "every post-open attach failure must close the live handle"
    );
    assert!(
        !engine.attached_account_ids().contains(&account_id),
        "failed attach must not leave a slot behind"
    );
}

#[tokio::test]
async fn reopen_refreshes_topology_subscriptions_and_lifecycle_handle() {
    let account_id = AccountId("full-reattach".to_owned());
    let old_scope = CursorScope::Account;
    let new_scope = CursorScope::Type(bifrost_types::ObjectType::Email);
    let established = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(AtomicUsize::new(0));
    let closed_generations = Arc::new(Mutex::new(Vec::new()));
    let subscribed = Arc::new(Mutex::new(Vec::new()));
    let unsubscribed = Arc::new(Mutex::new(Vec::new()));
    let lifecycle_calls = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(RotatingFactory {
        scopes: Mutex::new(VecDeque::from([
            vec![old_scope.clone()],
            vec![new_scope.clone()],
        ])),
        established: Arc::clone(&established),
        closed: Arc::clone(&closed),
        closed_generations: Arc::clone(&closed_generations),
        subscribed: Arc::clone(&subscribed),
        unsubscribed: Arc::clone(&unsubscribed),
        lifecycle_calls: Arc::clone(&lifecycle_calls),
        opens: AtomicUsize::new(0),
    });
    let store = Arc::new(InMemoryCheckpointStore::new());
    let engine = SyncEngine::builder()
        .checkpoints(Arc::clone(&store) as Arc<dyn CheckpointStore>)
        .build()
        .expect("default engine config is valid");
    let factory_trait: Arc<dyn AccountFactory> = Arc::clone(&factory) as Arc<dyn AccountFactory>;

    engine
        .attach(account_id.clone(), factory_trait)
        .await
        .expect("initial attach");
    engine
        .subscribe_push(&account_id, std::slice::from_ref(&old_scope))
        .await
        .expect("initial push subscription");
    engine.reopen(&account_id).await.expect("full reattach");

    assert_eq!(factory.opens.load(Ordering::SeqCst), 2);
    assert_eq!(
        engine
            .account_capabilities(&account_id)
            .expect("capabilities")
            .push,
        PushCapability::InProcess,
        "capability snapshot must come from the replacement handle"
    );
    assert!(
        store
            .get_change_cursor(&account_id, &old_scope)
            .await
            .expect("old cursor lookup")
            .is_none(),
        "vanished scopes must be removed durably"
    );
    assert!(
        store
            .get_change_cursor(&account_id, &new_scope)
            .await
            .expect("new cursor lookup")
            .is_some(),
        "newly discovered scopes must be established"
    );
    assert_eq!(
        *subscribed.lock().expect("subscribed lock"),
        vec![(0, vec![old_scope.clone()]), (1, vec![new_scope.clone()]),],
        "replacement handle must recreate every registered subscription"
    );
    assert_eq!(
        *unsubscribed.lock().expect("unsubscribed lock"),
        vec![(0, SubscriptionHandle("generation-0".into()))],
        "old subscription must be removed through the old handle"
    );
    assert_eq!(
        *closed_generations.lock().expect("closed generations lock"),
        vec![0],
        "successful reopen closes exactly the old handle"
    );

    for _ in 0..1000 {
        if lifecycle_calls
            .lock()
            .expect("lifecycle calls lock")
            .contains(&1)
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        lifecycle_calls
            .lock()
            .expect("lifecycle calls lock")
            .contains(&1),
        "lifecycle task must resubscribe to the replacement handle"
    );

    engine.detach(&account_id).await.expect("detach");
    assert_eq!(
        *closed_generations.lock().expect("closed generations lock"),
        vec![0, 1]
    );
}
