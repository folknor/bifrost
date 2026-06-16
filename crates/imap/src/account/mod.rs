//! IMAP implementation of the shared `Account` trait.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bifrost_types::{
    Account, AccountError, AccountFuture, AccountOperation, AccountStream, AttachmentHandle,
    BlobHandle, ByteRange, Calendar, CalendarEvent, Change, ChangeCursor, ContactCard,
    ContactCreate, ContactId, ContactPatch, ContactSearchRequest, Container, ContainerId,
    ContainerKind, CursorDescriptor, CursorEstablishment, CursorScope, DraftHandle, DraftPatch,
    EventCreate, EventId, EventPatch, EventRange, EventSearchRequest, FilterValidation,
    HydratedObject, HydrationProjection, IdempotencyKey, Identity, IdentityId, IdentityPatch,
    InventoryEntry, ItemOutcome, MembershipScope, Message, MutationSuccess, MutationTarget,
    ObjectId, Page, Priority, Projection, QuotaInfo, RsvpStatus, SearchRequest, SendRequest,
    ServerFilter, ServerFilterCreate, ServerFilterId, ServerFilterPatch, SubscriptionHandle,
    SyncEvent, ThreadHydration, ThreadId, VacationConfig, WatchEvent,
};
use bifrost_types::{AddressBook, AddressBookId};
use futures::stream::Stream;
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::types::{MailboxName, SyncSelectOptions, SyncSelectResult, UidSet, UidValidity};

mod blob;
mod capabilities;
mod changes;
mod close;
mod envelope;
pub(crate) mod error;
mod factory;
mod folder_registry;
mod get;
mod inventory;
mod mutate;
mod pim;
mod pool;
mod push;
mod scopes;
mod sieve;
mod submission;

// pub: consumers register this factory with bifrost-sync without naming ImapAccount.
pub use factory::{ImapAccountConfig, ImapAccountFactory};
pub use sieve::ManageSieveConfig;
// pub: consumers configure IMAP submission (SMTP send) through these.
pub use submission::{SmtpSubmissionConfig, SubmissionCredentials, SubmissionTls};

pub(crate) use submission::SubmissionTransport;

pub(crate) use envelope::{
    DecodedObjectId, FolderCursor, decode_blob_id, decode_cursor, decode_object_id,
    decode_thread_id, encode_cursor, encode_object_id, encode_thread_id,
};
pub(crate) use folder_registry::{CompactUidSet, FolderRegistry};
pub(crate) use pool::{Pool, PooledConn};

const STREAM_CAPACITY: usize = 64;
const BATCH_ITEMS: usize = 128;
const UNLIMITED_BANDWIDTH: u64 = u64::MAX;

/// Open IMAP account handle. The handle owns the connection pool.
#[derive(Clone)]
pub(crate) struct ImapAccount {
    inner: Arc<ImapAccountInner>,
}

pub(crate) struct ImapAccountInner {
    pub(crate) config: Arc<ImapAccountConfig>,
    pub(crate) capabilities: bifrost_types::AccountCapabilities,
    pub(crate) pool: Arc<Pool>,
    pub(crate) folders: Arc<FolderRegistry>,
    pub(crate) qresync_enabled: AtomicBool,
    pub(crate) qresync_negotiation_warning: Option<String>,
    pub(crate) qresync_negotiation_warning_sent: AtomicBool,
    pub(crate) shutdown: CancellationToken,
    pub(crate) closed: AtomicBool,
    pub(crate) priority: AtomicU8,
    pub(crate) bandwidth_cap: Arc<AtomicU64>,
    pub(crate) push: push::PushState,
    pub(crate) contacts: Option<Arc<dyn Account>>,
    pub(crate) calendars: Option<Arc<dyn Account>>,
    pub(crate) submission: Option<Arc<SubmissionTransport>>,
}

pub(crate) struct ImapAccountParts {
    pub(crate) config: Arc<ImapAccountConfig>,
    pub(crate) capabilities: bifrost_types::AccountCapabilities,
    pub(crate) pool: Arc<Pool>,
    pub(crate) folders: Arc<FolderRegistry>,
    pub(crate) qresync_enabled: bool,
    pub(crate) qresync_negotiation_warning: Option<String>,
    pub(crate) bandwidth_cap: Arc<AtomicU64>,
    pub(crate) contacts: Option<Arc<dyn Account>>,
    pub(crate) calendars: Option<Arc<dyn Account>>,
    pub(crate) submission: Option<Arc<SubmissionTransport>>,
}

impl ImapAccount {
    pub(crate) fn new(parts: ImapAccountParts) -> Self {
        Self {
            inner: Arc::new(ImapAccountInner {
                config: parts.config,
                capabilities: parts.capabilities,
                pool: parts.pool,
                folders: parts.folders,
                qresync_enabled: AtomicBool::new(parts.qresync_enabled),
                qresync_negotiation_warning: parts.qresync_negotiation_warning,
                qresync_negotiation_warning_sent: AtomicBool::new(false),
                shutdown: CancellationToken::new(),
                closed: AtomicBool::new(false),
                priority: AtomicU8::new(Priority::Normal as u8),
                bandwidth_cap: parts.bandwidth_cap,
                push: push::PushState::new(),
                contacts: parts.contacts,
                calendars: parts.calendars,
                submission: parts.submission,
            }),
        }
    }
}

impl std::ops::Deref for ImapAccount {
    type Target = ImapAccountInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ImapAccount {
    pub(crate) fn command_timeout(&self) -> Duration {
        self.config.imap.command_timeout
    }

    pub(crate) async fn checkout_for_folder(
        &self,
        folder: &MailboxName,
    ) -> Result<PooledConn, Error> {
        self.pool.checkout_for_folder(folder).await
    }

    pub(crate) async fn select_folder(
        &self,
        conn: &mut PooledConn,
        folder: &MailboxName,
        cursor: Option<&FolderCursor>,
        read_only: bool,
    ) -> Result<SyncSelectResult, Error> {
        let options = self.select_options(cursor, read_only);
        let result = conn
            .connection()
            .select_for_sync(folder.as_str(), &options, self.command_timeout())
            .await?;
        conn.set_selected(folder.clone());
        self.folders.mark_seen(folder);
        Ok(result)
    }

    fn select_options(&self, cursor: Option<&FolderCursor>, read_only: bool) -> SyncSelectOptions {
        let mut options = if read_only {
            SyncSelectOptions::read_only()
        } else {
            SyncSelectOptions::read_write()
        };

        if self.qresync_enabled()
            && let Some(FolderCursor::QResync {
                uidvalidity,
                modseq,
                known_uids,
                known_uids_complete,
            }) = cursor
            && let Some(validity) = UidValidity::new(*uidvalidity)
        {
            let known_uids = if *known_uids_complete {
                let uids = known_uids
                    .to_uids()
                    .into_iter()
                    .filter_map(crate::types::Uid::new);
                crate::types::UidSet::from_uids(uids)
            } else {
                None
            };
            options =
                options.with_qresync(validity, crate::types::ModSeq::new(*modseq), known_uids);
        }
        options
    }

    pub(crate) fn cursor_from_select(
        &self,
        selected: &crate::types::SelectedMailbox,
        known_uids: Option<CompactUidSet>,
    ) -> Result<FolderCursor, Error> {
        let uidvalidity = selected
            .uid_validity
            .ok_or_else(|| Error::Protocol("SELECT missing UIDVALIDITY".into()))?;
        let known_uids = known_uids.unwrap_or_default();
        if self.qresync_enabled()
            && let Some(modseq) = selected.highest_mod_seq
            && !selected.no_mod_seq
        {
            return Ok(FolderCursor::QResync {
                uidvalidity,
                modseq,
                known_uids,
                known_uids_complete: true,
            });
        }
        if let Some(modseq) = selected.highest_mod_seq
            && !selected.no_mod_seq
        {
            return Ok(FolderCursor::Condstore {
                uidvalidity,
                modseq,
                known_uids,
            });
        }
        Ok(FolderCursor::Basic {
            uidvalidity,
            uidnext: selected.uid_next.unwrap_or_default(),
            known_uids,
        })
    }

    pub(crate) fn qresync_enabled(&self) -> bool {
        self.qresync_enabled.load(Ordering::Acquire)
    }

    pub(crate) fn disable_qresync_for_session(&self) {
        // Other folder syncs may already have passed their QRESYNC gate
        // on this account. They are allowed to finish or independently
        // downgrade; this one-way flag only prevents new QRESYNC work.
        self.qresync_enabled.store(false, Ordering::Release);
    }

    pub(crate) fn take_qresync_negotiation_warning(&self) -> Option<String> {
        let warning = self.qresync_negotiation_warning.as_ref()?;
        if self
            .qresync_negotiation_warning_sent
            .swap(true, Ordering::Release)
        {
            return None;
        }
        Some(warning.clone())
    }
}

impl Account for ImapAccount {
    // Account: exposes the factory-built capability snapshot; direct users call server_profile().
    fn capabilities(&self) -> &bifrost_types::AccountCapabilities {
        &self.capabilities
    }

    // Account: stores the engine priority hint; direct ImapConnection calls remain caller scheduled.
    fn set_priority(&self, priority: Priority) {
        self.priority.store(priority as u8, Ordering::Release);
    }

    // Account: records the engine bandwidth cap until IMAP byte-metering is wired.
    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        let raw = match bps {
            None => UNLIMITED_BANDWIDTH,
            Some(0) => {
                tracing::warn!(
                    target: "bifrost_imap::bandwidth",
                    "set_bandwidth_cap(Some(0)) clamped to 1 B/s; use None for unlimited",
                );
                1
            }
            Some(n) => n,
        };
        self.bandwidth_cap.store(raw, Ordering::Release);
    }

    // Account: describes opaque engine cursors; direct users inspect SyncSelectResult.
    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor {
        changes::describe_cursor(self, cursor)
    }

    // Account: discovers engine cursor scopes from the folder registry; direct users call LIST.
    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        scopes::discover_cursor_scopes(self.clone())
    }

    // Account: reports engine membership scopes; direct users call LIST/LIST-STATUS.
    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        scopes::discover_memberships(self.clone())
    }

    // Account: streams folder lifecycle events from push state; direct users consume IDLE/NOTIFY.
    fn scope_lifecycle_stream(&self) -> AccountStream<bifrost_types::ScopeLifecycleEvent> {
        scopes::scope_lifecycle_stream(self.clone())
    }

    // Account: mints an engine cursor from folder state; direct users call select_for_sync().
    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        inventory::establish_initial_cursor(self.clone(), scope)
    }

    // Account: emits engine inventory batches; direct users compose UID SEARCH/FETCH.
    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<SyncEvent<InventoryEntry>> {
        inventory::inventory_stream(self.clone(), scope)
    }

    // Account: hydrates bifrost ObjectIds; direct users call uid_fetch* with native UIDs.
    fn get_stream(
        &self,
        ids: AccountStream<bifrost_types::ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        get::get_stream(self.clone(), ids, projection)
    }

    // Account: advances opaque cursors; direct users call sync_fetch() or UID commands.
    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        changes::changes_stream(self.clone(), cursor)
    }

    // Account: IMAP has no durable server-side subscription, so this starts in-process push.
    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<SubscriptionHandle, AccountError>> {
        push::push_subscribe(self.clone(), scopes.to_vec())
    }

    // Account: unsubscribes the synthetic in-process push handle; direct users call notify_none().
    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        push::push_unsubscribe(self.clone(), handle)
    }

    // Account: forwards IDLE/NOTIFY wakeups as WatchEvents; direct users call idle().
    fn push_stream(&self) -> AccountStream<WatchEvent> {
        push::push_stream(self.clone())
    }

    // Account: streams a bifrost BlobHandle; direct users fetch BODY[] sections.
    fn open_blob(&self, handle: BlobHandle) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open_blob(self.clone(), handle)
    }

    // Account: maps shared byte ranges to IMAP partial BODY[]; direct users choose sections.
    fn open_blob_range(
        &self,
        handle: BlobHandle,
        range: ByteRange,
    ) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open_blob_range(self.clone(), handle, range)
    }

    // Account: maps shared FlagOp batches to UID STORE; direct users call uid_store().
    fn bulk_set_flags(
        &self,
        targets: AccountStream<bifrost_types::ObjectId>,
        op: bifrost_types::FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutate::bulk_set_flags(self.clone(), targets, op, key)
    }

    // Account: maps shared membership moves to UID MOVE; direct users call uid_move_messages().
    fn bulk_move(
        &self,
        targets: AccountStream<bifrost_types::ObjectId>,
        destination: MembershipScope,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutate::bulk_move(self.clone(), targets, destination, key)
    }

    // Account: maps shared destroy batches to STORE Deleted plus UID EXPUNGE.
    fn bulk_destroy(
        &self,
        targets: AccountStream<bifrost_types::ObjectId>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutate::bulk_destroy(self.clone(), targets, key)
    }

    fn add_to_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::add_to_container(self.clone(), target, container)
    }

    fn remove_from_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::remove_from_container(self.clone(), target, container)
    }

    fn set_keyword(
        &self,
        target: MutationTarget,
        keyword: String,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::set_keyword(self.clone(), target, keyword, value)
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::unsupported_unit(bifrost_types::AccountOperation::SetLabelMembership)
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::unsupported_unit(bifrost_types::AccountOperation::SetCategory)
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::unsupported_unit(bifrost_types::AccountOperation::SetExtendedProperty)
    }

    fn set_is_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::set_is_read(self.clone(), target, is_read)
    }

    fn send_message(&self, request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::send_message(self.clone(), request)
    }

    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<bytes::Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        pim::unsupported_attachment(bifrost_types::AccountOperation::AttachmentUpload)
    }

    fn draft_create(&self, patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        pim::draft_create(self.clone(), patch)
    }

    fn draft_update(
        &self,
        _draft: DraftHandle,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::unsupported_unit(bifrost_types::AccountOperation::DraftUpdate)
    }

    fn draft_discard(&self, draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        pim::draft_discard(self.clone(), draft)
    }

    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::draft_send(self.clone(), draft)
    }

    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        // SMTP FUTURERELEASE is fire-and-submit: RFC 4865 has no verb to
        // recall an accepted HOLDUNTIL submission. Honest provider limit.
        pim::unsupported_unit(bifrost_types::AccountOperation::CancelScheduledSend)
    }

    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::unsupported_object(bifrost_types::AccountOperation::RescheduleSend)
    }

    fn search(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        pim::search(self.clone(), request)
    }

    fn search_messages(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        pim::search_messages(self.clone(), request)
    }

    fn containers_list(&self) -> AccountFuture<Result<Vec<Container>, AccountError>> {
        pim::containers_list(self.clone())
    }

    fn container_create(
        &self,
        kind: ContainerKind,
        name: String,
        parent: Option<ContainerId>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        pim::container_create(self.clone(), kind, name, parent)
    }

    fn container_rename(
        &self,
        container: ContainerId,
        name: String,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::container_rename(self.clone(), container, name)
    }

    fn container_move(
        &self,
        container: ContainerId,
        new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::container_move(self.clone(), container, new_parent)
    }

    fn container_delete(&self, container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        pim::container_delete(self.clone(), container)
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        pim::identities_list()
    }

    fn identity_update(
        &self,
        identity: IdentityId,
        patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::identity_update(identity, patch)
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        pim::vacation_get()
    }

    fn vacation_set(&self, config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        pim::vacation_set(config)
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        pim::quota_get(self.clone())
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        sieve::filters_list(self.clone())
    }

    fn filter_create(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        sieve::filter_create(self.clone(), filter)
    }

    fn filter_update(
        &self,
        filter: ServerFilterId,
        patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        sieve::filter_update(self.clone(), filter, patch)
    }

    fn filter_delete(&self, filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        sieve::filter_delete(self.clone(), filter)
    }

    fn filter_validate(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        sieve::filter_validate(self.clone(), filter)
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.address_books_list();
        }
        unsupported_future(AccountOperation::AddressBooksList)
    }

    fn contacts_list(
        &self,
        address_book: Option<AddressBookId>,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contacts_list(address_book, page_cursor);
        }
        unsupported_future(AccountOperation::ContactsList)
    }

    fn contact_get(&self, contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contact_get(contact);
        }
        unsupported_future(AccountOperation::ContactGet)
    }

    fn contact_create(
        &self,
        contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contact_create(contact);
        }
        unsupported_future(AccountOperation::ContactCreate)
    }

    fn contact_update(
        &self,
        contact: ContactId,
        patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contact_update(contact, patch);
        }
        unsupported_future(AccountOperation::ContactUpdate)
    }

    fn contact_delete(&self, contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contact_delete(contact);
        }
        unsupported_future(AccountOperation::ContactDelete)
    }

    fn contact_search(
        &self,
        request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        if let Some(contacts) = &self.contacts {
            return contacts.contact_search(request);
        }
        unsupported_future(AccountOperation::ContactSearch)
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.calendars_list();
        }
        unsupported_future(AccountOperation::CalendarsList)
    }

    fn events_in_range(
        &self,
        range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.events_in_range(range);
        }
        unsupported_future(AccountOperation::EventsInRange)
    }

    fn event_get(&self, event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_get(event);
        }
        unsupported_future(AccountOperation::EventGet)
    }

    fn event_create(&self, event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_create(event);
        }
        unsupported_future(AccountOperation::EventCreate)
    }

    fn event_update(
        &self,
        event: EventId,
        patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_update(event, patch);
        }
        unsupported_future(AccountOperation::EventUpdate)
    }

    fn event_delete(&self, event: EventId) -> AccountFuture<Result<(), AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_delete(event);
        }
        unsupported_future(AccountOperation::EventDelete)
    }

    fn event_rsvp(
        &self,
        event: EventId,
        status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_rsvp(event, status);
        }
        unsupported_future(AccountOperation::EventRsvp)
    }

    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        if let Some(calendars) = &self.calendars {
            return calendars.event_search(request);
        }
        unsupported_future(AccountOperation::EventSearch)
    }

    fn thread_hydrate(
        &self,
        thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        pim::thread_hydrate(self.clone(), thread)
    }

    fn message_hydrate(
        &self,
        message: ObjectId,
        projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        pim::message_hydrate(self.clone(), message, projection)
    }

    fn move_thread(
        &self,
        thread: ThreadId,
        target: ContainerId,
        source: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::move_thread(self.clone(), thread, target, source)
    }

    fn delete_thread(
        &self,
        thread: ThreadId,
        current: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::delete_thread(self.clone(), thread, current)
    }

    // Account: closes the pool; direct users close a single connection with logout().
    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        close::close(self.clone())
    }
}

fn unsupported_future<T: Send + 'static>(
    operation: AccountOperation,
) -> AccountFuture<Result<T, AccountError>> {
    Box::pin(async move { Err(error::unsupported(operation)) })
}

/// Convert a crate-private `crate::Error` into a public `AccountError`
/// at the account-trait boundary, stamping the supplied `ImapErrorContext`.
///
/// Every call site must populate the context with the calling operation
/// so the central recovery mapping has the operation and scope it needs
/// to derive `RecoveryClass`.
pub(crate) fn account_error_with(err: Error, ctx: error::ImapErrorContext) -> AccountError {
    error::into_account_error(err, ctx)
}

/// Wrap a fatal stream cause as `SyncEvent::Terminated`. Accepts any
/// `Into<TerminatedCause>`: a structured `AccountError` (already built
/// at the account boundary) or an `(Error, ImapErrorContext)` pair to
/// classify on the way out. Replaces the previous twin helpers
/// `fatal_event` / `terminated_event` so call sites do not have to
/// pick which lane to dispatch through.
pub(crate) fn terminated_event<T, E: Into<TerminatedCause>>(cause: E) -> SyncEvent<T> {
    SyncEvent::Terminated(cause.into().into_account_error())
}

/// Carrier for the two ways a stream task currently produces a fatal
/// `AccountError`: a structured error built at the boundary, or a
/// crate-private `crate::Error` plus the calling context to classify.
pub(crate) enum TerminatedCause {
    Account(AccountError),
    Classify(Error, error::ImapErrorContext),
}

impl TerminatedCause {
    fn into_account_error(self) -> AccountError {
        match self {
            Self::Account(err) => err,
            Self::Classify(err, ctx) => error::into_account_error(err, ctx),
        }
    }
}

impl From<AccountError> for TerminatedCause {
    fn from(err: AccountError) -> Self {
        Self::Account(err)
    }
}

impl From<(Error, error::ImapErrorContext)> for TerminatedCause {
    fn from((err, ctx): (Error, error::ImapErrorContext)) -> Self {
        Self::Classify(err, ctx)
    }
}

/// Legacy alias for the classify-on-build helper. Same body as
/// `terminated_event::<T, _>((err, ctx))` but reads naturally at
/// call sites that still phrase the action as "fatal-event this
/// `(Error, ImapErrorContext)`."
pub(crate) fn fatal_event<T>(err: Error, ctx: error::ImapErrorContext) -> SyncEvent<T> {
    terminated_event((err, ctx))
}

pub(crate) fn boxed_receiver_stream<T: Send + 'static>(
    rx: tokio::sync::mpsc::Receiver<T>,
) -> AccountStream<T> {
    Box::pin(ReceiverStream { rx })
}

pub(crate) fn iter_stream<T: Send + Unpin + 'static>(items: Vec<T>) -> AccountStream<T> {
    Box::pin(IterStream {
        items: items.into(),
    })
}

struct ReceiverStream<T> {
    rx: tokio::sync::mpsc::Receiver<T>,
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

struct IterStream<T> {
    items: VecDeque<T>,
}

impl<T: Unpin> Stream for IterStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.items.pop_front())
    }
}

pub(crate) fn batch<T>(
    items: Vec<T>,
    boundary: bifrost_types::PageBoundary,
    checkpoint: Option<bifrost_types::Checkpoint>,
) -> SyncEvent<T> {
    SyncEvent::Batch(bifrost_types::Batch {
        items,
        page_boundary: boundary,
        server_latency: Duration::ZERO,
        bytes_in: 0,
        checkpoint,
    })
}

pub(crate) fn folder_from_scope(
    scope: &CursorScope,
    op: bifrost_types::AccountOperation,
) -> Result<MailboxName, AccountError> {
    match scope {
        CursorScope::Folder(folder) => MailboxName::new(folder.0.clone()).map_err(|e| {
            error::into_account_error(
                Error::InvalidInput(e.to_string()),
                error::ImapErrorContext::operation(op),
            )
        }),
        _ => Err(error::unsupported(op)),
    }
}

pub(crate) fn folder_scope(folder: &MailboxName) -> CursorScope {
    CursorScope::Folder(bifrost_types::FolderId(folder.as_str().to_owned()))
}

pub(crate) fn membership_scope(folder: &MailboxName) -> MembershipScope {
    MembershipScope::Folder(bifrost_types::FolderId(folder.as_str().to_owned()))
}

pub(crate) fn uid_set_from_u32(uids: &[u32]) -> Option<UidSet> {
    UidSet::from_uids(uids.iter().filter_map(|uid| crate::types::Uid::new(*uid)))
}
