//! Hermetic tests for the closed-account gate, driven through a counting stub
//! provider. The stub counts every provider invocation, so a refusal is
//! proved to reach no provider code at all, not merely to return an error.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::channel::oneshot;
use futures::{FutureExt, StreamExt};

use super::{ClosingAccount, Refusal};
use crate::account::{Account, AccountFuture, AccountStream, CategoryDefinition};
use crate::error::{AccountError, AccountErrorKind, AccountOperation};
use crate::events::{InventoryEvent, SyncEvent};
use crate::{ChangeCursor, CursorScope, Protocol};

#[derive(Default)]
struct Stub {
    /// Provider invocations of every non-exempt method.
    calls: AtomicUsize,
    /// Inventory resume streams the provider constructed.
    resume_built: AtomicUsize,
    /// Provider `close` invocations.
    closes: AtomicUsize,
    /// Held by `vacation_get` so a test can keep an admitted call pending.
    vacation_release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl Stub {
    fn hit(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }

    fn fail<T: Send + 'static>(&self) -> AccountFuture<Result<T, AccountError>> {
        self.hit();
        let error = AccountError::unsupported(AccountOperation::Search, Protocol::Unknown);
        Box::pin(async move { Err(error) })
    }

    fn ended<E: Send + 'static>(&self) -> AccountStream<E> {
        self.hit();
        Box::pin(futures::stream::empty())
    }
}

/// The stub's resume hook accepts exactly the cursors whose payload is `[1]`.
fn cursor(bytes: &[u8]) -> ChangeCursor {
    ChangeCursor {
        scope: CursorScope::Account,
        server_state: crate::OpaqueChangeState {
            protocol: crate::ProtocolKind::Jmap,
            envelope_version: crate::PayloadVersion::new(1),
            bytes: bytes.to_vec(),
        },
        advanced_through: None,
        envelope_version: crate::TraitEnvelopeVersion::new(crate::CHANGE_CURSOR_ENVELOPE_VERSION),
    }
}

macro_rules! failing {
    ($($name:ident($($arg:ident: $ty:ty),*) -> $out:ty;)*) => {
        $(
            fn $name(&self, $($arg: $ty),*) -> AccountFuture<Result<$out, AccountError>> {
                $(let _ = $arg;)*
                self.fail()
            }
        )*
    };
}

macro_rules! ending {
    ($($name:ident($($arg:ident: $ty:ty),*) -> $item:ty;)*) => {
        $(
            fn $name(&self, $($arg: $ty),*) -> AccountStream<$item> {
                $(let _ = $arg;)*
                self.ended()
            }
        )*
    };
}

type Outcome = SyncEvent<crate::ItemOutcome<crate::MutationSuccess>>;

impl Account for Stub {
    fn protocol(&self) -> Protocol {
        Protocol::Unknown
    }

    fn capabilities(&self) -> &crate::AccountCapabilities {
        unreachable!("capabilities are not exercised by these tests")
    }

    fn set_priority(&self, _priority: crate::Priority) {}

    fn set_bandwidth_cap(&self, _bps: Option<crate::BandwidthCap>) {}

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> crate::CursorDescriptor {
        unreachable!("cursor descriptors are not exercised by these tests")
    }

    /// Overrides a default that refuses as `Unsupported`, so a forwarded
    /// call is distinguishable from an inherited default.
    fn category_definitions_list(
        &self,
    ) -> AccountFuture<Result<Vec<CategoryDefinition>, AccountError>> {
        self.hit();
        Box::pin(async { Ok(Vec::new()) })
    }

    fn inventory_resume_stream(
        &self,
        cursor: ChangeCursor,
    ) -> Option<AccountStream<InventoryEvent>> {
        (cursor.server_state.bytes == [1]).then(|| {
            self.resume_built.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::empty()) as AccountStream<InventoryEvent>
        })
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<crate::VacationConfig>, AccountError>> {
        self.hit();
        let release = self
            .vacation_release
            .lock()
            .expect("stub lock")
            .take()
            .expect("the test installs a release channel");
        Box::pin(async move {
            let _ = release.await;
            Ok(None)
        })
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }

    fn push_stream(
        &self,
        _lane: crate::PushLane,
        _stop: crate::SessionStop,
        _gate: crate::PushGate,
    ) -> AccountStream<crate::PushSessionEvent> {
        self.ended()
    }

    fn get_stream(
        &self,
        _ids: AccountStream<crate::ObjectId>,
        _projection: crate::Projection,
    ) -> AccountStream<SyncEvent<crate::ItemOutcome<crate::HydratedObject>>> {
        self.ended()
    }

    fn bulk_set_flags(
        &self,
        _targets: AccountStream<crate::ObjectId>,
        _op: crate::FlagOp,
    ) -> AccountStream<Outcome> {
        self.ended()
    }

    fn bulk_move(
        &self,
        _targets: AccountStream<crate::ObjectId>,
        _destination: crate::MembershipScope,
    ) -> AccountStream<Outcome> {
        self.ended()
    }

    fn bulk_destroy(&self, _targets: AccountStream<crate::ObjectId>) -> AccountStream<Outcome> {
        self.ended()
    }

    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<crate::AttachmentHandle, AccountError>> {
        self.fail()
    }

    ending! {
        discover_cursor_scopes() -> SyncEvent<CursorScope>;
        discover_memberships() -> SyncEvent<crate::MembershipScope>;
        scope_lifecycle_stream() -> crate::ScopeLifecycleEvent;
        inventory_stream(_scope: CursorScope) -> InventoryEvent;
        changes_stream(_cursor: ChangeCursor) -> SyncEvent<crate::Change>;
        open_blob(_handle: crate::BlobHandle) -> SyncEvent<Bytes>;
        open_blob_range(_handle: crate::BlobHandle, _range: crate::ByteRange) -> SyncEvent<Bytes>;
        open_raw_rfc822(_message: crate::ObjectId) -> SyncEvent<Bytes>;
    }

    failing! {
        establish_initial_cursor(_scope: CursorScope) -> crate::CursorEstablishment;
        push_subscribe(_scopes: &[CursorScope]) -> crate::PushSubscription;
        push_unsubscribe(_handle: crate::SubscriptionHandle) -> ();
        add_to_container(_t: crate::MutationTarget, _c: crate::ContainerId) -> ();
        remove_from_container(_t: crate::MutationTarget, _c: crate::ContainerId) -> ();
        set_keyword(_t: crate::MutationTarget, _k: crate::Flag, _v: bool) -> ();
        set_label_membership(_t: crate::MutationTarget, _l: crate::ContainerId, _v: bool) -> ();
        set_category(_t: crate::MutationTarget, _c: String, _v: bool) -> ();
        set_extended_property(_t: crate::MutationTarget, _p: String, _v: Option<String>) -> ();
        set_importance(_t: crate::MutationTarget, _l: crate::Importance) -> ();
        set_is_read(_t: crate::MutationTarget, _r: bool) -> ();
        send_message(_r: crate::SendRequest) -> crate::SendReceipt;
        host_attachment(_b: Bytes, _m: crate::CloudUploadMeta) -> crate::HostedAttachment;
        draft_create(_p: crate::DraftPatch) -> crate::Created<crate::DraftHandle>;
        draft_update(_d: crate::DraftHandle, _p: crate::DraftPatch) -> ();
        draft_discard(_d: crate::DraftHandle) -> ();
        draft_send(_d: crate::DraftHandle) -> crate::SendReceipt;
        cancel_scheduled_send(_h: crate::ObjectId) -> ();
        reschedule_send(_h: crate::ObjectId, _s: std::time::SystemTime) -> crate::ObjectId;
        search(_r: crate::SearchRequest) -> crate::Page<crate::ThreadId>;
        search_messages(_r: crate::SearchRequest) -> crate::Page<crate::ObjectId>;
        containers_list() -> crate::ContainerList;
        container_create(
            _k: crate::ContainerKind,
            _n: String,
            _p: Option<crate::ContainerId>,
            _s: Option<crate::ContainerStyle>
        ) -> crate::Created<crate::ContainerId>;
        container_rename(_c: crate::ContainerId, _n: String, _s: Option<crate::ContainerStyle>) -> ();
        container_move(_c: crate::ContainerId, _p: Option<crate::ContainerId>) -> ();
        container_delete(_c: crate::ContainerId) -> ();
        identities_list() -> Vec<crate::Identity>;
        identity_update(_i: crate::IdentityId, _p: crate::IdentityPatch) -> ();
        vacation_set(_c: crate::VacationConfig) -> ();
        quota_get() -> Option<crate::QuotaInfo>;
        filters_list() -> Vec<crate::ServerFilter>;
        filter_create(_f: crate::ServerFilterCreate) -> crate::Created<crate::ServerFilterId>;
        filter_update(_f: crate::ServerFilterId, _p: crate::ServerFilterPatch) -> ();
        filter_delete(_f: crate::ServerFilterId) -> ();
        filter_validate(_f: crate::ServerFilterCreate) -> crate::FilterValidation;
        address_books_list() -> Vec<crate::AddressBook>;
        contacts_list(
            _b: Option<crate::AddressBookId>,
            _c: Option<crate::PageCursor>
        ) -> crate::Page<crate::ContactCard>;
        contact_get(_c: crate::ContactId) -> crate::ContactCard;
        contact_create(_c: crate::ContactCreate) -> crate::Created<crate::ContactId>;
        contact_update(_c: crate::ContactId, _p: crate::ContactPatch) -> ();
        contact_delete(_c: crate::ContactId) -> ();
        contact_search(_r: crate::ContactSearchRequest) -> crate::Page<crate::ContactCard>;
        directory_search(
            _q: String,
            _l: Option<u32>,
            _c: Option<crate::PageCursor>
        ) -> crate::Page<crate::DirectoryCard>;
        directory_groups_list(_c: Option<crate::PageCursor>) -> crate::Page<crate::DirectoryGroup>;
        directory_group_expand(
            _g: crate::DirectoryGroupId,
            _c: Option<crate::PageCursor>
        ) -> crate::Page<crate::DirectoryGroupMember>;
        calendars_list() -> Vec<crate::Calendar>;
        events_in_range(_r: crate::EventRange) -> crate::Page<crate::CalendarEvent>;
        event_get(_e: crate::EventId) -> crate::CalendarEvent;
        event_create(_e: crate::EventCreate) -> crate::Created<crate::EventId>;
        event_update(_e: crate::EventId, _p: crate::EventPatch) -> ();
        event_delete(_e: crate::EventId) -> ();
        event_rsvp(_e: crate::EventId, _s: crate::RsvpStatus) -> ();
        event_search(_r: crate::EventSearchRequest) -> crate::Page<crate::CalendarEvent>;
        thread_hydrate(_t: crate::ThreadId) -> crate::ThreadHydration;
        message_hydrate(_m: crate::ObjectId, _p: crate::HydrationProjection) -> crate::Message;
    }
}

fn gate() -> (Arc<Stub>, ClosingAccount<Stub>) {
    let stub = Arc::new(Stub::default());
    (Arc::clone(&stub), ClosingAccount::new(stub))
}

fn assert_closed(error: &AccountError, operation: AccountOperation) {
    assert_eq!(error.kind(), &AccountErrorKind::AccountClosed);
    assert_eq!(error.operation(), Some(operation));
}

fn drain<E>(stream: AccountStream<E>) -> Vec<E> {
    stream
        .collect::<Vec<_>>()
        .now_or_never()
        .expect("stub streams are immediately ready")
}

#[test]
fn a_future_built_before_close_and_polled_after_is_refused_without_reaching_the_provider() {
    let (stub, gate) = gate();
    let future = gate.containers_list();
    drop(gate.close());
    let result = future
        .now_or_never()
        .expect("a refusal is immediately ready");
    assert_closed(
        &result.expect_err("refused"),
        AccountOperation::ContainersList,
    );
    assert_eq!(stub.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn a_call_pending_at_close_stays_admitted_and_keeps_the_provider_result() {
    let (stub, gate) = gate();
    let (release, held) = oneshot::channel();
    *stub.vacation_release.lock().expect("stub lock") = Some(held);
    let mut future = gate.vacation_get();
    assert!(
        (&mut future).now_or_never().is_none(),
        "first poll admits and pends"
    );
    drop(gate.close());
    release
        .send(())
        .expect("the admitted call is still waiting");
    let result = future.now_or_never().expect("released");
    assert!(
        matches!(result, Ok(None)),
        "admitted work keeps its own result"
    );
    assert_eq!(stub.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn an_open_gate_forwards_to_the_provider_override_not_the_trait_default() {
    let (stub, gate) = gate();
    let result = gate
        .category_definitions_list()
        .now_or_never()
        .expect("ready");
    assert!(result.expect("the override answers Ok").is_empty());
    assert_eq!(stub.calls.load(Ordering::SeqCst), 1);

    drop(gate.close());
    let refused = gate
        .category_definitions_list()
        .now_or_never()
        .expect("ready");
    assert_closed(
        &refused.expect_err("refused"),
        AccountOperation::CategoryDefinitionsList,
    );
    assert_eq!(stub.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn a_stream_first_polled_after_close_ends_on_one_refusal_and_never_reports_done() {
    let (stub, gate) = gate();
    let stream = gate.changes_stream(cursor(&[9]));
    drop(gate.close());
    let events = drain(stream);
    let [SyncEvent::Terminated(error)] = events.as_slice() else {
        panic!("a refused stream yields exactly one Terminated");
    };
    assert_closed(error, AccountOperation::SyncChanges);

    let events = drain(gate.inventory_stream(CursorScope::Account));
    let [InventoryEvent::Terminated(error)] = events.as_slice() else {
        panic!("a refused inventory claims no completion");
    };
    assert_closed(error, AccountOperation::SyncInventory);
    assert_eq!(stub.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn a_default_bodied_arc_method_is_refused_after_close() {
    let (stub, gate) = gate();
    let gate = Arc::new(gate);
    drop(gate.close());
    let events =
        drain(Arc::clone(&gate).bulk_destroy_if_version(Box::pin(futures::stream::empty())));
    let [SyncEvent::Terminated(error)] = events.as_slice() else {
        panic!("a refused bulk mutation yields exactly one Terminated");
    };
    assert_closed(error, AccountOperation::BulkDestroy);
    assert_eq!(stub.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn borrowed_arguments_are_owned_by_the_deferred_call() {
    let (stub, gate) = gate();
    let future = {
        let scopes = vec![CursorScope::Account];
        gate.push_subscribe(&scopes)
    };
    let result = future.now_or_never().expect("ready");
    assert!(matches!(
        result.expect_err("the stub refuses").kind(),
        AccountErrorKind::Unsupported(_)
    ));
    assert_eq!(stub.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn classification_survives_close_and_agrees_with_the_predicate() {
    let (stub, gate) = gate();
    drop(gate.close());

    let change = cursor(&[9]);
    assert!(gate.inventory_resume_stream(change.clone()).is_none());
    assert!(!gate.is_inventory_cursor(&change));

    let inventory = cursor(&[1]);
    assert!(gate.is_inventory_cursor(&inventory));
    let stream = gate
        .inventory_resume_stream(inventory)
        .expect("an inventory cursor keeps its Some answer after close");
    let events = drain(stream);
    let [InventoryEvent::Terminated(error)] = events.as_slice() else {
        panic!("the resumed stream is refused, never completed");
    };
    assert_closed(error, AccountOperation::SyncInventory);
    assert_eq!(stub.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn a_resume_stream_built_while_open_is_admitted_only_at_first_poll() {
    let (stub, gate) = gate();
    let admitted = gate
        .inventory_resume_stream(cursor(&[1]))
        .expect("inventory cursor");
    let refused = gate
        .inventory_resume_stream(cursor(&[1]))
        .expect("inventory cursor");
    assert_eq!(stub.resume_built.load(Ordering::SeqCst), 2);

    let mut admitted = admitted;
    assert!(
        admitted.next().now_or_never().expect("ready").is_none(),
        "polled while open: the provider stream runs"
    );
    drop(gate.close());
    let events = drain(refused);
    assert!(
        matches!(events.as_slice(), [InventoryEvent::Terminated(_)]),
        "built while open but first polled after close: refused"
    );
}

#[test]
fn close_shuts_admission_when_called_and_always_reaches_the_provider() {
    let (stub, gate) = gate();
    drop(gate.close());
    assert!(
        gate.is_closed(),
        "a close future dropped unpolled still closes"
    );
    gate.close()
        .now_or_never()
        .expect("ready")
        .expect("repeated close returns the provider's answer");
    assert_eq!(stub.closes.load(Ordering::SeqCst), 2);
}

#[test]
fn exempt_methods_are_named_and_stay_live_after_close() {
    let mut exempt = ClosingAccount::<Stub>::EXEMPT_METHODS.to_vec();
    exempt.sort_unstable();
    assert_eq!(
        exempt,
        [
            "capabilities",
            "describe_cursor",
            "flag_target",
            "inventory_partitioning",
            "is_inventory_cursor",
            "protocol",
            "push_lanes",
            "set_bandwidth_cap",
            "set_priority",
            "throttle_keys",
        ]
    );
    let (_, gate) = gate();
    drop(gate.close());
    assert_eq!(gate.protocol(), Protocol::Unknown);
    assert!(gate.push_lanes().is_empty());
}

#[test]
fn refusal_shapes_match_each_stream_contract() {
    let error = || AccountError::account_closed(AccountOperation::PushStream, Protocol::Unknown);
    assert!(matches!(
        crate::PushSessionEvent::refusal(error()),
        crate::PushSessionEvent::Failed(_)
    ));
    assert!(matches!(
        crate::repair::InventoryRepairEvent::refusal(error()),
        crate::repair::InventoryRepairEvent::Terminated(_)
    ));
    assert!(matches!(
        crate::ScopeLifecycleEvent::refusal(error()),
        crate::ScopeLifecycleEvent::Terminated(_)
    ));
}
