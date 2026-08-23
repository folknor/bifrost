//! Read-back guard tests.
//!
//! Builds a synthetic `Account` impl whose `get_stream` returns
//! predetermined flag sets per id, then asserts the guard reconciles
//! applied/skipped correctly against a `FlagOp::Add` target.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bifrost_sync::{Error, IdempotencyVendor, SyncEngine, run_readback_guard};
use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountFactory, AccountFuture, AccountId, AccountStream, AttachmentHandle, Batch,
    BatchingPolicy, BlobHandle, BlobRangeSupport, ByteRange, Cause, Change, ChangeCursor,
    CloudUploadMeta, ContactCard, ContactCreate, ContactId, ContactPatch, ContactSearchRequest,
    ContainerId, ContainerKind, Control, ConvenienceShape, CursorDescriptor, CursorEstablishment,
    CursorFreshness, CursorScope, DraftHandle, DraftPatch, EventCreate, EventId, EventPatch,
    EventRange, EventSearchRequest, FilterRuleShape, FilterValidation, FlagOp, HostedAttachment,
    HydratedObject, HydratedObjectKind, HydrationProjection, IdempotencyKey, Identity, IdentityId,
    IdentityPatch, Importance, InventoryEntry, ItemOutcome, MembershipScope, Message,
    MutationCapabilities, MutationConcurrency, MutationReplaySafety, MutationSuccess,
    MutationTarget, ObjectId, OpenedAccount, Page, PageBoundary, PimMethodSupport, Priority,
    Projection, ProtocolKind, PushCapability, QuotaInfo, QuotaSignal, RateLimitClass, RequestCause,
    RsvpStatus, ScopeLifecycleEvent, SearchRequest, SendRequest, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch, SubscriptionHandle, SyncEvent, ThreadHydration, ThreadId,
    VacationConfig, WatchEvent,
};
use bifrost_types::{AddressBook, AddressBookId};
use bifrost_types::{Calendar, CalendarEvent};
use bytes::Bytes;
use futures::stream::{self, StreamExt};

/// Convenience for stubs that just need to signal "this operation is
/// not implemented in this test double." Maps to `Unsupported(op)` +
/// `ClientBug` recovery so any unexpected call is obvious in output.
fn unsupported(op: bifrost_types::AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(op),
        Cause::Request(RequestCause::Unsupported { operation: op }),
    )
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}

/// Synthetic Account that returns predetermined flags for each id on
/// `get_stream(Projection::FlagsOnly)`. All other methods are
/// unreachable in this test.
struct FlagsAccount {
    caps: AccountCapabilities,
    flag_table: std::collections::HashMap<ObjectId, HashSet<String>>,
    campaign: Option<Arc<CampaignState>>,
}

#[derive(Default)]
struct CampaignState {
    submissions: AtomicUsize,
    submitted: tokio::sync::Notify,
    keys: std::sync::Mutex<Vec<IdempotencyKey>>,
    /// How many targets each successive submission actually carried, so a
    /// test can observe the width of a resubmission rather than only its
    /// count.
    widths: std::sync::Mutex<Vec<usize>>,
}

struct FlagsFactory(Arc<FlagsAccount>);

impl AccountFactory for FlagsFactory {
    fn open(&self, _account: AccountId) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        let concrete = Arc::clone(&self.0);
        let account: Arc<dyn Account> = concrete;
        Box::pin(async move { Ok(OpenedAccount::complete(account)) })
    }
}

fn retryable_transport() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network),
        Cause::Transport(bifrost_types::TransportCause::new(
            bifrost_types::TransportKind::Network,
            None,
        )),
    )
    .try_build()
    .expect("valid retryable transport error")
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
        reopen_discovers_foreign_namespaces: false,
    }
}

impl Account for FlagsAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.caps
    }

    fn set_priority(&self, _priority: Priority) {}

    fn set_bandwidth_cap(&self, _bps: Option<u64>) {}

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        unreachable!("describe_cursor not exercised by readback guard tests")
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        Box::pin(stream::empty())
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        Box::pin(stream::empty())
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        Box::pin(stream::empty())
    }

    fn establish_initial_cursor(
        &self,
        _scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::EstablishCursor,
            ))
        })
    }

    fn inventory_stream(&self, _scope: CursorScope) -> AccountStream<SyncEvent<InventoryEntry>> {
        Box::pin(stream::empty())
    }

    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        assert_eq!(projection, Projection::FlagsOnly);
        let table = self.flag_table.clone();
        let collected: Pin<Box<dyn futures::Future<Output = Vec<ObjectId>> + Send>> =
            Box::pin(async move { ids.collect::<Vec<_>>().await });
        let s = async move {
            let ids: Vec<ObjectId> = collected.await;
            let items: Vec<ItemOutcome<HydratedObject>> = ids
                .into_iter()
                .map(|id| {
                    let flags = table.get(&id).cloned().unwrap_or_default();
                    let hydrated = HydratedObject {
                        id: id.clone(),
                        kind: HydratedObjectKind::FlagsOnly(flags),
                        blobs: Vec::new(),
                    };
                    ItemOutcome::Succeeded(bifrost_types::BatchSuccess::new(
                        bifrost_types::BatchItemId(id.0),
                        hydrated,
                    ))
                })
                .collect();
            SyncEvent::Batch(Batch {
                items,
                page_boundary: PageBoundary::Final,
                server_latency: std::time::Duration::from_millis(1),
                bytes_in: 0,
                checkpoint: None,
            })
        };
        Box::pin(stream::once(s).chain(stream::once(async { SyncEvent::Done(None) })))
    }

    fn changes_stream(&self, _cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        Box::pin(stream::empty())
    }

    fn push_subscribe(
        &self,
        _scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::PushSubscribe)) })
    }

    fn push_unsubscribe(
        &self,
        _handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::PushUnsubscribe,
            ))
        })
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
        targets: AccountStream<ObjectId>,
        _op: FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        let Some(campaign) = &self.campaign else {
            return Box::pin(stream::empty());
        };
        campaign.keys.lock().expect("keys lock").push(key);
        let attempt = campaign.submissions.fetch_add(1, Ordering::SeqCst);
        campaign.submitted.notify_one();
        if attempt == 0 {
            // Drain first so the submission width is recorded, then terminate
            // retryably without emitting any per-item outcome. Whether the
            // account consumed its own input stream is invisible to the engine,
            // which sweeps unresolved ids out of `remaining`, so this records
            // the width without changing what the campaign observes.
            let campaign = Arc::clone(campaign);
            return Box::pin(stream::once(async move {
                let width = targets.count().await;
                campaign.widths.lock().expect("widths lock").push(width);
                SyncEvent::Terminated(retryable_transport())
            }));
        }
        let widths = Arc::clone(campaign);
        Box::pin(stream::once(async move {
            let targets = {
                let collected: Vec<ObjectId> = targets.collect().await;
                widths
                    .widths
                    .lock()
                    .expect("widths lock")
                    .push(collected.len());
                stream::iter(collected)
            };
            let items = targets
                .map(|id| {
                    ItemOutcome::Succeeded(bifrost_types::BatchSuccess::new(
                        bifrost_types::BatchItemId(id.0),
                        MutationSuccess::Applied,
                    ))
                })
                .collect()
                .await;
            SyncEvent::Batch(Batch {
                items,
                page_boundary: PageBoundary::Final,
                server_latency: std::time::Duration::ZERO,
                bytes_in: 0,
                checkpoint: None,
            })
        }))
    }

    /// Echoes the destination it was handed back as a single
    /// `Succeeded` item id, so a caller can prove which arguments
    /// reached the wire method.
    fn bulk_move(
        &self,
        _targets: AccountStream<ObjectId>,
        destination: MembershipScope,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        let item = ItemOutcome::Succeeded(bifrost_types::BatchSuccess::new(
            bifrost_types::BatchItemId(format!("{destination:?}")),
            MutationSuccess::Applied,
        ));
        Box::pin(stream::iter([SyncEvent::Batch(Batch {
            items: vec![item],
            page_boundary: PageBoundary::Page,
            server_latency: std::time::Duration::ZERO,
            bytes_in: 0,
            checkpoint: None,
        })]))
    }

    fn bulk_destroy(
        &self,
        _targets: AccountStream<ObjectId>,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        Box::pin(stream::empty())
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Ok(()) })
    }

    // PIM primitives stubbed to Err(Unsupported); the read-back guard
    // only exercises `get_stream`.
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

    fn containers_list(&self) -> AccountFuture<Result<bifrost_types::ContainerList, AccountError>> {
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

fn set(items: &[&str]) -> HashSet<String> {
    items.iter().map(|s| (*s).to_string()).collect()
}

#[tokio::test]
async fn readback_guard_marks_applied_as_skipped() {
    // Three ids retried. The server actually applied \Seen to id `a`
    // and id `b` (so they read back with the flag), but not to `c`.
    // The guard must mark a + b as skipped, leave c as still_failed.
    let mut table = std::collections::HashMap::new();
    table.insert(ObjectId("a".into()), set(&["\\Seen"]));
    table.insert(ObjectId("b".into()), set(&["\\Seen", "\\Flagged"]));
    table.insert(ObjectId("c".into()), set(&["\\Flagged"]));
    let acc = FlagsAccount {
        caps: caps(),
        flag_table: table,
        campaign: None,
    };
    let ids = vec![
        ObjectId("a".into()),
        ObjectId("b".into()),
        ObjectId("c".into()),
    ];
    let outcome = run_readback_guard(&acc, ids, &FlagOp::Add(set(&["\\Seen"])))
        .await
        .expect("guard ok");
    assert_eq!(outcome.skipped, 2);
    assert_eq!(outcome.still_failed, 1);
}

#[tokio::test]
async fn readback_guard_handles_remove() {
    let mut table = std::collections::HashMap::new();
    table.insert(ObjectId("a".into()), set(&["\\Seen"]));
    table.insert(ObjectId("b".into()), set(&[]));
    let acc = FlagsAccount {
        caps: caps(),
        flag_table: table,
        campaign: None,
    };
    let ids = vec![ObjectId("a".into()), ObjectId("b".into())];
    let outcome = run_readback_guard(&acc, ids, &FlagOp::Remove(set(&["\\Seen"])))
        .await
        .expect("guard ok");
    assert_eq!(outcome.skipped, 1, "b is already without \\Seen");
    assert_eq!(outcome.still_failed, 1, "a still has \\Seen");
}

#[tokio::test]
async fn readback_guard_handles_set() {
    let mut table = std::collections::HashMap::new();
    table.insert(ObjectId("a".into()), set(&["\\Seen"]));
    table.insert(ObjectId("b".into()), set(&["\\Seen", "\\Flagged"]));
    let acc = FlagsAccount {
        caps: caps(),
        flag_table: table,
        campaign: None,
    };
    let ids = vec![ObjectId("a".into()), ObjectId("b".into())];
    let outcome = run_readback_guard(&acc, ids, &FlagOp::Set(set(&["\\Seen"])))
        .await
        .expect("guard ok");
    assert_eq!(outcome.skipped, 1, "only a matches exact set");
    assert_eq!(outcome.still_failed, 1);
}

#[tokio::test]
async fn readback_guard_with_no_ids_is_no_op() {
    let acc = FlagsAccount {
        caps: caps(),
        flag_table: std::collections::HashMap::new(),
        campaign: None,
    };
    let outcome = run_readback_guard(&acc, vec![], &FlagOp::Add(set(&["\\Seen"])))
        .await
        .expect("guard ok");
    assert_eq!(outcome.skipped, 0);
    assert_eq!(outcome.still_failed, 0);
}

/// The `bulk_move_from` default impl must forward to `bulk_move` with
/// the destination intact. An Account that only implements the
/// relocating form (IMAP / Graph / JMAP) already vacates the source as
/// part of the move, so dropping `source` here is the correct default,
/// but the destination must survive.
#[tokio::test]
async fn bulk_move_from_defaults_to_bulk_move() {
    let acc = FlagsAccount {
        caps: caps(),
        flag_table: std::collections::HashMap::new(),
        campaign: None,
    };
    let destination = MembershipScope::Label(bifrost_types::LabelId("Label_42".into()));
    let source = MembershipScope::Label(bifrost_types::LabelId("Label_7".into()));
    let events: Vec<_> = acc
        .bulk_move_from(
            Box::pin(stream::empty()),
            destination.clone(),
            Some(source),
            key(),
        )
        .collect()
        .await;
    let ids = events
        .into_iter()
        .filter_map(|event| match event {
            SyncEvent::Batch(batch) => Some(batch.items),
            _ => None,
        })
        .flatten()
        .filter_map(|item| match item {
            ItemOutcome::Succeeded(success) => Some(success.item.0),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![format!("{destination:?}")]);
}

fn key() -> IdempotencyKey {
    IdempotencyKey {
        run_id: bifrost_types::RunId("test-run".into()),
        sequence: 0,
        protocol_salt: bifrost_types::ProtocolSalt::Gmail("test-salt".into()),
    }
}

/// Cross-check: a guard run against an arc'd Account behaves the same.
#[tokio::test]
async fn readback_guard_works_through_arc() {
    let mut table = std::collections::HashMap::new();
    table.insert(ObjectId("only".into()), set(&["\\Seen"]));
    let acc: Arc<dyn Account> = Arc::new(FlagsAccount {
        caps: caps(),
        flag_table: table,
        campaign: None,
    });
    let outcome = run_readback_guard(
        acc.as_ref(),
        vec![ObjectId("only".into())],
        &FlagOp::Add(set(&["\\Seen"])),
    )
    .await
    .expect("guard ok");
    assert_eq!(outcome.skipped, 1);
    assert_eq!(outcome.still_failed, 0);
}

fn campaign_account(state: Arc<CampaignState>) -> Arc<FlagsAccount> {
    Arc::new(FlagsAccount {
        caps: caps(),
        flag_table: std::collections::HashMap::new(),
        campaign: Some(state),
    })
}

fn vendor() -> IdempotencyVendor {
    IdempotencyVendor::with_run_id(
        bifrost_types::RunId("boundary-test".into()),
        Box::new(|_| bifrost_types::ProtocolSalt::Imap),
    )
}

#[tokio::test(start_paused = true)]
async fn paused_campaign_resumes_once_without_losing_retry_accounting() {
    let state = Arc::new(CampaignState::default());
    let engine = Arc::new(SyncEngine::builder().build().expect("engine"));
    let account_id = AccountId("mutation-boundary".into());
    let control = engine
        .attach(
            account_id.clone(),
            Arc::new(FlagsFactory(campaign_account(Arc::clone(&state)))),
        )
        .await
        .expect("attach");
    let campaign_engine = Arc::clone(&engine);
    let campaign_id = account_id.clone();
    let campaign = tokio::spawn(async move {
        campaign_engine
            .bulk_set_flags(
                &campaign_id,
                vec![ObjectId("one".into())],
                FlagOp::Add(set(&["\\Seen"])),
                &vendor(),
                ProtocolKind::Imap,
            )
            .await
    });

    state.submitted.notified().await;
    control.pause().await.expect("pause");
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    assert_eq!(state.submissions.load(Ordering::SeqCst), 1);

    control.resume();
    let counters = campaign.await.expect("campaign task").expect("campaign");
    assert_eq!(counters.applied, 1);
    assert_eq!(counters.pending_retry, 0);
    assert_eq!(state.submissions.load(Ordering::SeqCst), 2);
    {
        let keys = state.keys.lock().expect("keys lock");
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].run_id, keys[1].run_id);
        assert_eq!(keys[0].sequence, keys[1].sequence);
        assert_eq!(keys[0].protocol_salt, keys[1].protocol_salt);
    }
    engine.detach(&account_id).await.expect("detach");
}

/// `MutationConfig::retry_queue_cap` bounds how wide ONE resubmission may be,
/// and the targets it defers are reported rather than dropped.
///
/// The first submission terminates retryably with no per-item outcome, so all
/// five targets become retry candidates. With the cap at two, the second
/// submission may carry only two of them - and the other three must still be
/// accounted for, because an id merely truncated out of the resubmission is no
/// longer among the campaign's outstanding targets and no later attempt will
/// resolve it. Before the cap was wired, the field was never read and the
/// second submission carried all five.
#[tokio::test(start_paused = true)]
async fn retry_queue_cap_bounds_a_resubmission_without_losing_the_excess() {
    let state = Arc::new(CampaignState::default());
    let mut config = bifrost_sync::EngineConfig::default();
    config.mutation.retry_queue_cap = 2;
    let engine = Arc::new(
        SyncEngine::builder()
            .config(config)
            .build()
            .expect("engine"),
    );
    let account_id = AccountId("retry-queue-cap".into());
    engine
        .attach(
            account_id.clone(),
            Arc::new(FlagsFactory(campaign_account(Arc::clone(&state)))),
        )
        .await
        .expect("attach");

    let counters = engine
        .bulk_set_flags(
            &account_id,
            (0..5)
                .map(|n| ObjectId(format!("id-{n}")))
                .collect::<Vec<_>>(),
            FlagOp::Add(set(&["\\Seen"])),
            &vendor(),
            ProtocolKind::Imap,
        )
        .await
        .expect("campaign");

    {
        let widths = state.widths.lock().expect("widths lock");
        assert_eq!(
            widths.as_slice(),
            &[5, 2],
            "the resubmission must be capped at retry_queue_cap"
        );
    }
    // Two were retried and applied. The three the cap deferred are marked
    // pending and then run through the read-back guard, which resolves them
    // against real server state - so they land in the skipped or still-failed
    // lane rather than staying nominally outstanding. Which lane is the
    // guard's business; what this pins is that all five are still accounted
    // for. Truncating the resubmission WITHOUT marking the excess drops them
    // from every lane and this total falls to two.
    assert_eq!(counters.applied, 2);
    let accounted = counters.applied
        + counters.skipped
        + counters.failed_terminal
        + counters.blocked_by_engine
        + counters.pending_retry;
    assert_eq!(
        accounted, 5,
        "every submitted target must land in exactly one lane: {counters:?}"
    );
    engine.detach(&account_id).await.expect("detach");
}

#[tokio::test(start_paused = true)]
async fn detach_abandons_a_parked_campaign() {
    let state = Arc::new(CampaignState::default());
    let engine = Arc::new(SyncEngine::builder().build().expect("engine"));
    let account_id = AccountId("mutation-shutdown".into());
    let control = engine
        .attach(
            account_id.clone(),
            Arc::new(FlagsFactory(campaign_account(Arc::clone(&state)))),
        )
        .await
        .expect("attach");
    let campaign_engine = Arc::clone(&engine);
    let campaign_id = account_id.clone();
    let campaign = tokio::spawn(async move {
        campaign_engine
            .bulk_set_flags(
                &campaign_id,
                vec![ObjectId("one".into())],
                FlagOp::Add(set(&["\\Seen"])),
                &vendor(),
                ProtocolKind::Imap,
            )
            .await
    });

    state.submitted.notified().await;
    control.pause().await.expect("pause");
    engine.detach(&account_id).await.expect("detach");
    assert!(matches!(
        campaign.await.expect("campaign task"),
        Err(Error::ShuttingDown)
    ));
    assert_eq!(state.submissions.load(Ordering::SeqCst), 1);
}
