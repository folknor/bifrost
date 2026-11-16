//! Calendar direct-call contracts, with all unrelated Account work delegated.
mod common;
use bifrost_sync::SyncEngine;
use bifrost_types::*;
use bytes::Bytes;
use std::sync::{Arc, Mutex};

struct CalendarAccount {
    inner: common::StubAccount,
    degraded: bool,
    calls: Mutex<Vec<(EventId, RsvpStatus, Option<ServerVersion>)>>,
}
impl CalendarAccount {
    fn page(&self, operation: AccountOperation) -> Page<CalendarEvent> {
        Page::single(
            Vec::new(),
            if self.degraded {
                vec!["private-event-id".into()]
            } else {
                Vec::new()
            },
            if self.degraded {
                vec![SkippedScope {
                    scope: ErrorScope::Cursor(CursorScope::Account),
                    error: common::unsupported(operation),
                }]
            } else {
                Vec::new()
            },
        )
    }
}
impl Account for CalendarAccount {
    fn flag_target(
        &self,
        target: &ObjectId,
        observed: &bifrost_types::FlagSet,
        op: &FlagOp,
    ) -> bifrost_types::FlagTargetState {
        self.inner.flag_target(target, observed, op)
    }
    fn protocol(&self) -> bifrost_types::Protocol {
        self.inner.protocol()
    }
    fn capabilities(&self) -> &AccountCapabilities {
        self.inner.capabilities()
    }
    fn set_priority(&self, _priority: Priority) {
        self.inner.set_priority(_priority);
    }
    fn set_bandwidth_cap(&self, _bps: Option<bifrost_types::BandwidthCap>) {
        self.inner.set_bandwidth_cap(_bps);
    }
    fn throttle_keys(
        &self,
        _account: &AccountId,
        work: bifrost_types::ThrottleWork<'_>,
    ) -> Vec<bifrost_types::ThrottleKey> {
        self.inner.throttle_keys(_account, work)
    }
    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        self.inner.describe_cursor(_cursor)
    }
    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        self.inner.discover_cursor_scopes()
    }
    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        self.inner.discover_memberships()
    }
    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        self.inner.scope_lifecycle_stream()
    }
    fn is_inventory_cursor(&self, cursor: &ChangeCursor) -> bool {
        self.inner.is_inventory_cursor(cursor)
    }
    fn inventory_resume_stream(
        &self,
        cursor: ChangeCursor,
    ) -> Option<AccountStream<InventoryEvent>> {
        self.inner.inventory_resume_stream(cursor)
    }
    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        self.inner.establish_initial_cursor(scope)
    }
    fn inventory_partitioning(&self, _scope: &CursorScope) -> InventoryPartitioning {
        self.inner.inventory_partitioning(_scope)
    }
    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        self.inner.inventory_stream(scope)
    }
    fn inventory_partition_stream(
        &self,
        scope: CursorScope,
        partition: InventoryPartition,
    ) -> AccountStream<InventoryEvent> {
        self.inner.inventory_partition_stream(scope, partition)
    }
    fn get_stream(
        &self,
        _ids: AccountStream<ObjectId>,
        _projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        self.inner.get_stream(_ids, _projection)
    }
    fn repair_inventory(
        &self,
        requests: AccountStream<bifrost_types::InventoryRepairRequest>,
    ) -> AccountStream<bifrost_types::InventoryRepairEvent> {
        self.inner.repair_inventory(requests)
    }
    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        self.inner.changes_stream(cursor)
    }
    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        self.inner.push_subscribe(scopes)
    }
    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.push_unsubscribe(handle)
    }
    fn push_lanes(&self) -> Vec<bifrost_types::PushLaneSpec> {
        self.inner.push_lanes()
    }
    fn push_stream(
        &self,
        lane: bifrost_types::PushLane,
        stop: bifrost_types::SessionStop,
        gate: bifrost_types::PushGate,
    ) -> AccountStream<bifrost_types::PushSessionEvent> {
        self.inner.push_stream(lane, stop, gate)
    }
    fn open_blob(&self, _handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        self.inner.open_blob(_handle)
    }
    fn open_blob_range(
        &self,
        _handle: BlobHandle,
        _range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        self.inner.open_blob_range(_handle, _range)
    }
    fn open_raw_rfc822(&self, _message: ObjectId) -> AccountStream<SyncEvent<Bytes>> {
        self.inner.open_raw_rfc822(_message)
    }
    fn bulk_set_flags(
        &self,
        _targets: AccountStream<ObjectId>,
        _op: FlagOp,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        self.inner.bulk_set_flags(_targets, _op)
    }
    fn bulk_move(
        &self,
        _targets: AccountStream<ObjectId>,
        _destination: MembershipScope,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        self.inner.bulk_move(_targets, _destination)
    }
    fn bulk_destroy(
        &self,
        _targets: AccountStream<ObjectId>,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        self.inner.bulk_destroy(_targets)
    }
    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        self.inner.close()
    }
    fn add_to_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.add_to_container(_target, _container)
    }
    fn remove_from_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.remove_from_container(_target, _container)
    }
    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: bifrost_types::Flag,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_keyword(_target, _keyword, _value)
    }
    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_label_membership(_target, _label, _value)
    }
    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_category(_target, _category, _value)
    }
    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner
            .set_extended_property(_target, _property_id, _value)
    }
    fn set_is_read(
        &self,
        _target: MutationTarget,
        _is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_is_read(_target, _is_read)
    }
    fn set_importance(
        &self,
        _target: MutationTarget,
        _level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_importance(_target, _level)
    }
    fn send_message(
        &self,
        _request: SendRequest,
    ) -> AccountFuture<Result<SendReceipt, AccountError>> {
        self.inner.send_message(_request)
    }
    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        self.inner.attachment_upload(_bytes, _mime)
    }
    fn host_attachment(
        &self,
        _bytes: Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        self.inner.host_attachment(_bytes, _meta)
    }
    fn draft_create(
        &self,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<bifrost_types::Created<DraftHandle>, AccountError>> {
        self.inner.draft_create(_patch)
    }
    fn draft_update(
        &self,
        _draft: DraftHandle,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.draft_update(_draft, _patch)
    }
    fn draft_discard(&self, _draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        self.inner.draft_discard(_draft)
    }
    fn draft_send(&self, _draft: DraftHandle) -> AccountFuture<Result<SendReceipt, AccountError>> {
        self.inner.draft_send(_draft)
    }
    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.cancel_scheduled_send(_handle)
    }
    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        self.inner.reschedule_send(_handle, _scheduled)
    }
    fn search(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        self.inner.search(_request)
    }
    fn search_messages(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        self.inner.search_messages(_request)
    }
    fn containers_list(&self) -> AccountFuture<Result<bifrost_types::ContainerList, AccountError>> {
        self.inner.containers_list()
    }
    fn container_create(
        &self,
        _kind: ContainerKind,
        _name: String,
        _parent: Option<ContainerId>,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<bifrost_types::Created<ContainerId>, AccountError>> {
        self.inner.container_create(_kind, _name, _parent, _style)
    }
    fn container_rename(
        &self,
        _container: ContainerId,
        _name: String,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.container_rename(_container, _name, _style)
    }
    fn container_move(
        &self,
        _container: ContainerId,
        _new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.container_move(_container, _new_parent)
    }
    fn container_delete(&self, _container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.container_delete(_container)
    }
    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        self.inner.identities_list()
    }
    fn identity_update(
        &self,
        _identity: IdentityId,
        _patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.identity_update(_identity, _patch)
    }
    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        self.inner.vacation_get()
    }
    fn vacation_set(&self, _config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        self.inner.vacation_set(_config)
    }
    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        self.inner.quota_get()
    }
    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        self.inner.filters_list()
    }
    fn filter_create(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<bifrost_types::Created<ServerFilterId>, AccountError>> {
        self.inner.filter_create(_filter)
    }
    fn filter_update(
        &self,
        _filter: ServerFilterId,
        _patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.filter_update(_filter, _patch)
    }
    fn filter_delete(&self, _filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.filter_delete(_filter)
    }
    fn filter_validate(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        self.inner.filter_validate(_filter)
    }
    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        self.inner.address_books_list()
    }
    fn contacts_list(
        &self,
        _address_book: Option<AddressBookId>,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        self.inner.contacts_list(_address_book, _page_cursor)
    }
    fn contact_get(&self, _contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        self.inner.contact_get(_contact)
    }
    fn contact_create(
        &self,
        _contact: ContactCreate,
    ) -> AccountFuture<Result<bifrost_types::Created<ContactId>, AccountError>> {
        self.inner.contact_create(_contact)
    }
    fn contact_update(
        &self,
        _contact: ContactId,
        _patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.contact_update(_contact, _patch)
    }
    fn contact_delete(&self, _contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.contact_delete(_contact)
    }
    fn contact_search(
        &self,
        _request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        self.inner.contact_search(_request)
    }
    fn directory_search(
        &self,
        _query: String,
        _limit: Option<u32>,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryCard>, AccountError>> {
        self.inner.directory_search(_query, _limit, _page_cursor)
    }
    fn directory_groups_list(
        &self,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroup>, AccountError>> {
        self.inner.directory_groups_list(_page_cursor)
    }
    fn directory_group_expand(
        &self,
        _group: bifrost_types::DirectoryGroupId,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroupMember>, AccountError>> {
        self.inner.directory_group_expand(_group, _page_cursor)
    }
    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        self.inner.calendars_list()
    }
    fn event_get(&self, _event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        self.inner.event_get(_event)
    }
    fn event_create(
        &self,
        _event: EventCreate,
    ) -> AccountFuture<Result<bifrost_types::Created<EventId>, AccountError>> {
        self.inner.event_create(_event)
    }
    fn event_update(
        &self,
        _event: EventId,
        _patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.event_update(_event, _patch)
    }
    fn event_delete(&self, _event: EventId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.event_delete(_event)
    }
    fn event_rsvp(
        &self,
        _event: EventId,
        _status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.event_rsvp(_event, _status)
    }
    fn thread_hydrate(
        &self,
        _thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        self.inner.thread_hydrate(_thread)
    }
    fn message_hydrate(
        &self,
        _message: ObjectId,
        _projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        self.inner.message_hydrate(_message, _projection)
    }
    fn events_in_range(
        &self,
        range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        assert_eq!(range.calendar_id, CalendarId("calendar".into()));
        assert_eq!(range.limit, Some(7));
        let page = self.page(AccountOperation::EventsInRange);
        Box::pin(async move { Ok(page) })
    }
    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        assert_eq!(request.query, "meeting");
        assert!(request.include_cancelled);
        let page = self.page(AccountOperation::EventSearch);
        Box::pin(async move { Ok(page) })
    }
    fn event_rsvp_if_version(
        &self,
        event: EventId,
        status: RsvpStatus,
        version: Option<ServerVersion>,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.calls.lock().unwrap().push((event, status, version));
        Box::pin(async { Ok(()) })
    }
}
struct Factory(Arc<CalendarAccount>);
impl AccountFactory for Factory {
    fn open(&self, _: AccountId) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        let account = Arc::clone(&self.0);
        let account: Arc<dyn Account> = account;
        Box::pin(async move { Ok(OpenedAccount::complete(account)) })
    }
}

#[tokio::test]
async fn calendar_pages_preserve_loss_and_announce_only_degraded_pages() {
    for degraded in [false, true] {
        let engine = SyncEngine::builder().build().unwrap();
        let id = AccountId("calendar-pages".into());
        let account = Arc::new(CalendarAccount {
            inner: common::StubAccount::new(Vec::new()),
            degraded,
            calls: Mutex::new(Vec::new()),
        });
        engine
            .attach(id.clone(), Arc::new(Factory(account)))
            .await
            .unwrap();
        let mut changes = engine.account_changes_stream(&id).unwrap();
        for method in ["events_in_range", "event_search"] {
            let page = if method == "events_in_range" {
                let mut range = EventRange::new(
                    CalendarId("calendar".into()),
                    EventTime::from_observed("2026-11-16", None),
                    EventTime::from_observed("2026-11-17", None),
                );
                range.limit = Some(7);
                engine.events_in_range(&id, range).await.unwrap()
            } else {
                let mut request = EventSearchRequest::new("meeting");
                request.include_cancelled = true;
                engine.event_search(&id, request).await.unwrap()
            };
            assert_eq!(page.failed_ids.len(), usize::from(degraded));
            assert_eq!(page.skipped_scopes.len(), usize::from(degraded));
            if degraded {
                assert_eq!(page.failed_ids, ["private-event-id"]);
                let delivery = changes.try_recv().expect("page warning is synchronous");
                let SyncEvent::Warning(warning) = delivery.event.as_ref() else {
                    panic!("expected warning")
                };
                assert_eq!(
                    warning.kind,
                    WarningKind::Engine(EngineNotice::IncompletePage {
                        skipped_scopes: 1,
                        failed_ids: 1
                    })
                );
                assert!(warning.message.value.contains(method));
                assert!(!warning.message.value.contains("private-event-id"));
                assert!(warning.next_action.is_some());
            }
            assert!(matches!(
                changes.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ));
        }
        engine.detach(&id).await.unwrap();
    }
}

#[tokio::test]
async fn calendar_rsvp_forwards_present_and_absent_versions() {
    let engine = SyncEngine::builder().build().unwrap();
    let id = AccountId("calendar-rsvp".into());
    let account = Arc::new(CalendarAccount {
        inner: common::StubAccount::new(Vec::new()),
        degraded: false,
        calls: Mutex::new(Vec::new()),
    });
    engine
        .attach(id.clone(), Arc::new(Factory(Arc::clone(&account))))
        .await
        .unwrap();
    for version in [Some(ServerVersion::HistoryAt(731)), None] {
        engine
            .event_rsvp_if_version(
                &id,
                EventId("event".into()),
                RsvpStatus::Declined,
                version.clone(),
            )
            .await
            .unwrap();
        assert_eq!(
            account.calls.lock().unwrap().last(),
            Some(&(EventId("event".into()), RsvpStatus::Declined, version))
        );
    }
    assert_eq!(account.calls.lock().unwrap().len(), 2);
    engine.detach(&id).await.unwrap();
}
