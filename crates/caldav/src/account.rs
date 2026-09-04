use bifrost_dav_core::{append_path, same_dav_url};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::*;
use bytes::Bytes;
use futures::{StreamExt, stream};
use jiff::tz::Offset;
use jiff::{Timestamp, civil};
use reqwest::Url;

use crate::capabilities::{caldav_capabilities, scheduling_available};
use crate::client::{
    CalDavClient, PutCondition, event_scope, local_error, missing_event_error, unsupported_error,
};
use crate::ical::{
    EventProjectionError, create_to_ical, event_from_ical, events_from_ical, new_uid,
    patch_to_ical, rsvp_patch, rsvp_reply_ical,
};
use crate::parse::CalendarCollection;
use crate::{CalDavConfig, CalDavCredentials};

// Version 2 changes snapshot ids from base-URL-relative to request-URI-relative.
const CURSOR_ENVELOPE_VERSION: u32 = 2;
const CURSOR_MAGIC: &[u8] = b"CALDAVET1";
/// How many `sync-collection` REPORTs one poll may spend draining an RFC 6578
/// truncated result. A bound rather than an unbounded loop: a server that keeps
/// reporting truncation must not hold one poll on the wire forever, and what is
/// left over is picked up by the next poll from the checkpointed token.
const SYNC_TRUNCATION_ROUNDS: usize = 16;

#[derive(Debug)]
pub(crate) struct CalDavAccount {
    client: Arc<CalDavClient>,
    capabilities: AccountCapabilities,
    calendar_home: String,
    /// The collection a call that names no calendar routes to, and `None` when
    /// the home enumerated no collections.
    ///
    /// Deliberately not the calendar home in that case. See
    /// `no_default_calendar`.
    pub(crate) default_calendar_url: Option<String>,
    pub(crate) calendar_urls: Vec<String>,
    rsvp_email: Option<String>,
    schedule_outbox_url: Option<String>,
}

impl CalDavAccount {
    /// Build an account directly around a client, skipping discovery.
    ///
    /// Exists so the `Account` methods can be driven against a scripted
    /// transport: discovery is several round trips of its own and would
    /// dominate a test that is about what a single call puts on the wire.
    #[cfg(test)]
    pub(crate) fn for_tests(client: Arc<CalDavClient>, default_calendar_url: &str) -> Self {
        Self::for_tests_with_collections(client, default_calendar_url, Vec::new())
    }

    /// An account whose calendar home enumerated no collections.
    ///
    /// The shape `open` produces against an empty backend: no default, and no
    /// discovered collection to fall back on.
    #[cfg(test)]
    pub(crate) fn for_tests_without_collections(client: Arc<CalDavClient>, home: &str) -> Self {
        Self {
            client,
            capabilities: crate::capabilities::caldav_capabilities(false),
            calendar_home: home.to_string(),
            default_calendar_url: None,
            calendar_urls: Vec::new(),
            rsvp_email: None,
            schedule_outbox_url: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_tests_with_collections(
        client: Arc<CalDavClient>,
        default_calendar_url: &str,
        additional_calendar_urls: Vec<String>,
    ) -> Self {
        Self {
            client,
            capabilities: crate::capabilities::caldav_capabilities(false),
            calendar_home: default_calendar_url.to_string(),
            default_calendar_url: Some(default_calendar_url.to_string()),
            calendar_urls: std::iter::once(default_calendar_url.to_string())
                .chain(additional_calendar_urls.iter().cloned())
                .collect(),
            rsvp_email: None,
            schedule_outbox_url: None,
        }
    }

    pub(crate) async fn open(
        account_id: AccountId,
        config: CalDavConfig,
    ) -> Result<Self, AccountError> {
        let client = CalDavClient::new(account_id, &config);
        Self::open_with_client(client, rsvp_email_from_config(&config)).await
    }

    /// The whole of `open` after the client exists.
    ///
    /// Split out so the discovery-to-account path can be driven against a
    /// scripted transport: `open` itself builds its client from a `CalDavConfig`
    /// and so cannot take one. Everything `open` decides - the default
    /// collection, the cursor-scope set, the RSVP capability - is decided here.
    pub(crate) async fn open_with_client(
        mut client: CalDavClient,
        configured_rsvp_email: Option<String>,
    ) -> Result<Self, AccountError> {
        let discovery = client.discover_account().await?;
        let rsvp_email = configured_rsvp_email.or(discovery.calendar_user_email);
        let schedule_outbox_url = discovery.schedule_outbox_url;
        client.admit_discovered_urls(
            std::iter::once(discovery.calendar_home.clone())
                .chain(schedule_outbox_url.iter().cloned()),
        );
        // `event_rsvp` is discovery-derived, not assumed: the RSVP path
        // below hard-requires both of these and returns `unsupported`
        // without them, so advertising the method on a scheduling-less
        // RFC 4791 store would only move the failure from the
        // capability gate to the wire.
        let event_rsvp =
            scheduling_available(rsvp_email.as_deref(), schedule_outbox_url.as_deref());
        let calendar_home = discovery.calendar_home;
        let collections = client.list_calendars(&calendar_home).await?;
        let default_calendar_url = default_collection_url(&collections);
        let calendar_urls = discovered_collection_urls(&collections);
        Ok(Self {
            client: Arc::new(client),
            capabilities: caldav_capabilities(event_rsvp),
            calendar_home,
            default_calendar_url,
            calendar_urls,
            rsvp_email,
            schedule_outbox_url,
        })
    }

    fn calendar_url(
        client: &CalDavClient,
        default_calendar_url: Option<&str>,
        calendar: Option<CalendarId>,
        operation: AccountOperation,
    ) -> Result<String, AccountError> {
        if let Some(id) = calendar {
            return Ok(client.resolve_url(&id.0));
        }
        default_calendar_url
            .map(str::to_string)
            .ok_or_else(|| no_default_calendar(operation))
    }

    fn map_calendar(collection: CalendarCollection) -> Calendar {
        let native = collection.href;
        let name = collection
            .display_name
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| "Calendar".to_string());
        let can_edit = collection.can_edit.unwrap_or(true);
        Calendar {
            id: CalendarId(native.clone()),
            native_id: native.clone(),
            name,
            color: collection.color,
            provenance: CalendarProvenance {
                provider: ProtocolKind::CalDav,
                native,
                calendar_native: None,
            },
            is_default: false,
            can_create_events: can_edit,
            can_update_events: can_edit,
            can_delete_events: can_edit,
        }
    }

    /// Move one event resource into another calendar collection.
    ///
    /// WebDAV `MOVE` is the atomic form and is tried first. A server that does
    /// not implement it (405/501) falls back to PUT-to-new then
    /// DELETE-from-old, which is NOT atomic: a failure after the PUT leaves the
    /// event in both collections. That second-leg failure is wrapped
    /// `Protocol(PartialResponse)` with an acknowledged `Attempt`, the same
    /// treatment `event_rsvp` gives its own non-atomic sequence, so a consumer
    /// can tell "not moved" from "copied but not cleaned up" and reconciles
    /// rather than blindly replaying a write that already landed.
    ///
    /// The destination keeps the resource's own file name, so a collection that
    /// already holds that name refuses the move (`Overwrite: F`) rather than
    /// overwriting a stranger's resource.
    async fn relocate_event(
        client: &CalDavClient,
        source_url: &str,
        target_calendar: &str,
        body: String,
        content_changed: bool,
    ) -> Result<(), AccountError> {
        let destination = append_path(target_calendar, &resource_file_name(source_url));
        if client
            .move_resource(source_url, &destination, AccountOperation::EventUpdate)
            .await?
        {
            if !content_changed {
                return Ok(());
            }
            // The move is done and the id has changed. A failure here is a
            // half-applied request, not a failed one.
            return client
                .put_event(
                    &destination,
                    body,
                    PutCondition::None,
                    AccountOperation::EventUpdate,
                )
                .await
                .map(|_| ())
                .map_err(|error| {
                    partial_move_error(&error, "event moved but the field patch failed")
                });
        }
        // No MOVE support: copy first, then remove the original. Ordered this
        // way round because a failed copy leaves the event exactly where it
        // was, while a failed delete leaves it readable in two places - the
        // recoverable direction of a non-atomic pair.
        client
            .put_event(
                &destination,
                body,
                PutCondition::IfNoneMatch,
                AccountOperation::EventUpdate,
            )
            .await?;
        client
            .delete_event(source_url, AccountOperation::EventUpdate)
            .await
            .map_err(|error| {
                partial_move_error(
                    &error,
                    "event copied to the destination calendar but the original could not be removed",
                )
            })
    }

    async fn fetch_event_from_url(
        client: Arc<CalDavClient>,
        default_calendar_url: Option<String>,
        calendar: Option<CalendarId>,
        event: EventId,
        operation: AccountOperation,
    ) -> Result<CalendarEvent, AccountError> {
        let url = client.resolve_url(&event.0);
        // The event's own collection is the first answer and almost always
        // available; the default is only reached for a native id whose parent
        // cannot be derived, and an account with no collections has none.
        let calendar_url = match calendar {
            Some(calendar) => client.resolve_url(&calendar.0),
            None => event_calendar_url(&url)
                .or(default_calendar_url)
                .ok_or_else(|| no_default_calendar(operation))?,
        };
        let fetched = client.get_event(&url, operation).await.map_err(|error| {
            error
                .clone()
                .into_builder()
                .scope(event_scope(event.0.clone()))
                .try_build()
                .unwrap_or(error)
        })?;
        event_from_ical(
            fetched.uri.clone(),
            CalendarId(calendar_url),
            fetched.etag,
            &fetched.data,
        )
        .map_err(|error| match error {
            EventProjectionError::NoVevent => missing_event_error(operation, event.0),
            EventProjectionError::Parse(_) => {
                crate::client::local_error(operation, "CalDAV resource is not valid iCalendar")
            }
        })
    }

    async fn event_snapshot(
        client: &CalDavClient,
        home: Option<&str>,
        calendar: &str,
        operation: AccountOperation,
    ) -> Result<EventSnapshot, AccountError> {
        let sync_token = match home {
            Some(home) => client
                .list_calendars_for_operation(home, operation)
                .await?
                .into_iter()
                .find(|collection| same_url(&collection.href, calendar))
                .and_then(|collection| collection.sync_token),
            None => client.collection_sync_token(calendar, operation).await?,
        };
        let listing = client.list_events_listing(calendar, operation).await?;
        let mut entries = listing
            .entries
            .into_iter()
            .map(|entry| EventSnapshotEntry {
                uri: entry.uri,
                etag: entry.etag,
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.uri.cmp(&right.uri));
        let failed_hrefs = listing.failed_hrefs;
        Ok(EventSnapshot {
            calendar_url: calendar.to_string(),
            sync_token,
            entries,
            failed_hrefs,
        })
    }
}

impl Account for CalDavAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.capabilities
    }

    fn set_priority(&self, priority: Priority) {
        self.client.net().set_priority(priority);
    }

    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        self.client.net().set_bandwidth_cap(bps);
    }

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        CursorDescriptor {
            cost_class: CostClass::Medium,
            strategy: SyncStrategy::ServerCursor,
            freshness: None,
        }
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        let scopes = self
            .calendar_urls
            .iter()
            .cloned()
            .map(|url| CursorScope::Folder(FolderId(url)))
            .collect();
        Box::pin(stream::iter([
            SyncEvent::Batch(Batch {
                items: scopes,
                page_boundary: PageBoundary::Final,
                server_latency: Default::default(),
                bytes_in: 0,
                checkpoint: None,
            }),
            SyncEvent::Done(None),
        ]))
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        unsupported_stream(AccountOperation::DiscoverMemberships)
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        Box::pin(stream::empty())
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        let client = Arc::clone(&self.client);
        let home = self.calendar_home.clone();
        let default_calendar = self.default_calendar_url.clone();
        let calendar_urls = self.calendar_urls.clone();
        Box::pin(async move {
            validate_event_scope(&scope, AccountOperation::EstablishCursor)?;
            let calendar = collection_url_for_scope(
                &scope,
                default_calendar.as_deref(),
                &calendar_urls,
                AccountOperation::EstablishCursor,
            )?;
            let snapshot = Self::event_snapshot(
                &client,
                Some(&home),
                &calendar,
                AccountOperation::EstablishCursor,
            )
            .await?;
            Ok(CursorEstablishment::Ready(cursor_from_snapshot(
                scope, &snapshot,
            )))
        })
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        let client = Arc::clone(&self.client);
        let home = self.calendar_home.clone();
        let default_calendar = self.default_calendar_url.clone();
        let calendar_urls = self.calendar_urls.clone();
        let coverage_scope = scope.clone();
        let coverage_domain = collection_coverage_domain(
            coverage_scope,
            scope_collection_url(&scope, default_calendar.as_deref()),
        );
        // COMPLETE coverage is an accurate claim here: this walk terminates
        // wholesale on any failure, so it never advances a checkpoint across a
        // gap. A version that starts absorbing per-item failures must build
        // `InventoryBatch` directly rather than converting.
        Box::pin(
            stream::once(async move {
                let mut events = Vec::new();
                if let Err(error) = validate_event_scope(&scope, AccountOperation::SyncInventory) {
                    events.push(SyncEvent::Terminated(error));
                    return events;
                }
                let calendar = match collection_url_for_scope(
                    &scope,
                    default_calendar.as_deref(),
                    &calendar_urls,
                    AccountOperation::SyncInventory,
                ) {
                    Ok(calendar) => calendar,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(error));
                        return events;
                    }
                };
                let started = Instant::now();
                let snapshot = match Self::event_snapshot(
                    &client,
                    Some(&home),
                    &calendar,
                    AccountOperation::SyncInventory,
                )
                .await
                {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(error));
                        return events;
                    }
                };
                let checkpoint = cursor_from_snapshot(scope, &snapshot);
                let items = snapshot
                    .entries
                    .iter()
                    .map(inventory_entry_from_snapshot)
                    .collect();
                events.push(SyncEvent::Batch(Batch {
                    items,
                    page_boundary: PageBoundary::Final,
                    server_latency: started.elapsed(),
                    bytes_in: 0,
                    checkpoint: Some(Checkpoint::Change(checkpoint.clone())),
                }));
                events.push(SyncEvent::Done(Some(Checkpoint::Change(checkpoint))));
                events
            })
            .flat_map(stream::iter)
            .map(bifrost_types::lift_complete_walk(coverage_domain)),
        )
    }

    fn inventory_partitioning(&self, _scope: &CursorScope) -> InventoryPartitioning {
        InventoryPartitioning::Full
    }

    fn inventory_partition_stream(
        &self,
        scope: CursorScope,
        partition: InventoryPartition,
    ) -> AccountStream<InventoryEvent> {
        match partition {
            InventoryPartition::Full => self.inventory_stream(scope),
            _ => {
                bifrost_types::unsupported_inventory_stream(scope, AccountOperation::SyncInventory)
            }
        }
    }

    fn get_stream(
        &self,
        _ids: AccountStream<ObjectId>,
        _projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        unsupported_stream(AccountOperation::Hydrate)
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        let client = Arc::clone(&self.client);
        Box::pin(
            stream::once(async move {
                let mut events = Vec::new();
                let previous = match decode_cursor_snapshot(&cursor) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(error));
                        return events;
                    }
                };
                let started = Instant::now();
                let (current, changes) = match changes_from_cursor(&client, &previous).await {
                    Ok(result) => result,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(error));
                        return events;
                    }
                };
                let checkpoint = cursor_from_snapshot(cursor.scope, &current);
                events.push(SyncEvent::Batch(Batch {
                    items: changes,
                    page_boundary: PageBoundary::Final,
                    server_latency: started.elapsed(),
                    bytes_in: 0,
                    checkpoint: Some(Checkpoint::Change(checkpoint.clone())),
                }));
                events.push(SyncEvent::Done(Some(Checkpoint::Change(checkpoint))));
                events
            })
            .flat_map(stream::iter),
        )
    }

    fn push_subscribe(
        &self,
        _scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        unsupported_future(AccountOperation::PushSubscribe)
    }

    fn push_unsubscribe(
        &self,
        _handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::PushUnsubscribe)
    }

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        Box::pin(stream::empty())
    }

    fn open_blob(&self, _handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        unsupported_stream(AccountOperation::OpenBlob)
    }

    fn open_blob_range(
        &self,
        _handle: BlobHandle,
        _range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        unsupported_stream(AccountOperation::OpenBlobRange)
    }

    fn open_raw_rfc822(&self, _message: ObjectId) -> AccountStream<SyncEvent<Bytes>> {
        unsupported_stream(AccountOperation::OpenRawRfc822)
    }

    fn bulk_set_flags(
        &self,
        _targets: AccountStream<ObjectId>,
        op: FlagOp,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        match op.validate_for_account(Protocol::CalDav) {
            Ok(()) => unsupported_stream(AccountOperation::UpdateFlags),
            Err(error) => Box::pin(futures::stream::once(async move {
                SyncEvent::Terminated(error)
            })),
        }
    }

    fn bulk_move(
        &self,
        _targets: AccountStream<ObjectId>,
        _destination: MembershipScope,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        unsupported_stream(AccountOperation::BulkMove)
    }

    fn bulk_destroy(
        &self,
        _targets: AccountStream<ObjectId>,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        unsupported_stream(AccountOperation::BulkDestroy)
    }

    fn add_to_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::AddToContainer)
    }

    fn remove_from_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::RemoveFromContainer)
    }

    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::SetKeyword)
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::SetLabelMembership)
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::SetCategory)
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::SetExtendedProperty)
    }

    fn set_is_read(
        &self,
        _target: MutationTarget,
        _is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::SetIsRead)
    }

    fn set_importance(
        &self,
        _target: MutationTarget,
        _level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::SetImportance)
    }

    fn send_message(&self, _request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        unsupported_future(AccountOperation::Send)
    }

    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        unsupported_future(AccountOperation::AttachmentUpload)
    }

    fn host_attachment(
        &self,
        _bytes: Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        unsupported_future(AccountOperation::HostAttachment)
    }

    fn draft_create(&self, _patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        unsupported_future(AccountOperation::DraftCreate)
    }

    fn draft_update(
        &self,
        _draft: DraftHandle,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::DraftUpdate)
    }

    fn draft_discard(&self, _draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::DraftDiscard)
    }

    fn draft_send(&self, _draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        unsupported_future(AccountOperation::DraftSend)
    }

    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::CancelScheduledSend)
    }

    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        unsupported_future(AccountOperation::RescheduleSend)
    }

    fn search(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        unsupported_future(AccountOperation::Search)
    }

    fn search_messages(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        unsupported_future(AccountOperation::SearchMessages)
    }

    fn containers_list(&self) -> AccountFuture<Result<ContainerList, AccountError>> {
        unsupported_future(AccountOperation::ContainersList)
    }

    fn container_create(
        &self,
        _kind: ContainerKind,
        _name: String,
        _parent: Option<ContainerId>,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        unsupported_future(AccountOperation::ContainerCreate)
    }

    fn container_rename(
        &self,
        _container: ContainerId,
        _name: String,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::ContainerRename)
    }

    fn container_move(
        &self,
        _container: ContainerId,
        _new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::ContainerMove)
    }

    fn container_delete(&self, _container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::ContainerDelete)
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        unsupported_future(AccountOperation::IdentitiesList)
    }

    fn identity_update(
        &self,
        _identity: IdentityId,
        _patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::IdentityUpdate)
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        unsupported_future(AccountOperation::VacationGet)
    }

    fn vacation_set(&self, _config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::VacationSet)
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        unsupported_future(AccountOperation::QuotaGet)
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        unsupported_future(AccountOperation::FiltersList)
    }

    fn filter_create(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        unsupported_future(AccountOperation::FilterCreate)
    }

    fn filter_update(
        &self,
        _filter: ServerFilterId,
        _patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::FilterUpdate)
    }

    fn filter_delete(&self, _filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::FilterDelete)
    }

    fn filter_validate(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        unsupported_future(AccountOperation::FilterValidate)
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        unsupported_future(AccountOperation::AddressBooksList)
    }

    fn contacts_list(
        &self,
        _address_book: Option<AddressBookId>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        unsupported_future(AccountOperation::ContactsList)
    }

    fn contact_get(&self, _contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        unsupported_future(AccountOperation::ContactGet)
    }

    fn contact_create(
        &self,
        _contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        unsupported_future(AccountOperation::ContactCreate)
    }

    fn contact_update(
        &self,
        _contact: ContactId,
        _patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::ContactUpdate)
    }

    fn contact_delete(&self, _contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::ContactDelete)
    }

    fn contact_search(
        &self,
        _request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        unsupported_future(AccountOperation::ContactSearch)
    }

    fn directory_search(
        &self,
        _query: String,
        _limit: Option<u32>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        unsupported_future(AccountOperation::DirectorySearch)
    }

    fn directory_groups_list(
        &self,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroup>, AccountError>> {
        unsupported_future(AccountOperation::DirectoryGroupsList)
    }

    fn directory_group_expand(
        &self,
        _group: DirectoryGroupId,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroupMember>, AccountError>> {
        unsupported_future(AccountOperation::DirectoryGroupExpand)
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        let client = Arc::clone(&self.client);
        let home = self.calendar_home.clone();
        Box::pin(async move {
            let collections = client.list_calendars(&home).await?;
            let mut calendars = collections
                .into_iter()
                .map(Self::map_calendar)
                .collect::<Vec<_>>();
            // A calendar-home that enumerates zero calendar collections
            // surfaces as an empty list, not a fabricated placeholder. The
            // depth-1 PROPFIND above returns the home's own response too, so a
            // home that is itself a calendar collection (resourcetype includes
            // <calendar/>) is already mapped by the parse path. An empty result
            // here therefore means a genuinely empty backend; reporting it as
            // empty lets a consumer reap stale calendars rather than chase a
            // phantom home-calendar whose events_in_range REPORT a spec-correct
            // server 404s.
            if let Some(first) = calendars.first_mut() {
                first.is_default = true;
            }
            Ok(calendars)
        })
    }

    fn events_in_range(
        &self,
        range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        let client = Arc::clone(&self.client);
        Box::pin(async move {
            let offset = decode_event_page_cursor(
                range.page_cursor.clone(),
                AccountOperation::EventsInRange,
            )?;
            let calendar_url = client.resolve_url(&range.calendar_id.0);
            let (range_start, range_end) = caldav_query_range(&range.start, &range.end)?;
            let fetched = client
                .query_events_in_range(&calendar_url, Some(&range_start), Some(&range_end))
                .await?;
            let mut events = Vec::new();
            // Per-resource failures are surfaced (not swallowed) so a
            // consumer can tell a transient failure apart from a real remote
            // deletion. Two kinds land here and they are the same thing to
            // the consumer: a resource the server refused inside the 207
            // (non-2xx propstat), and one that came back 200 but would not
            // tokenize. Neither aborts the rest of the pull.
            let mut failed = fetched.failed_hrefs();
            let mut materialized = HashSet::new();
            for event in fetched.events {
                let uri = event.uri;
                match events_from_ical(
                    uri.clone(),
                    CalendarId(calendar_url.clone()),
                    event.etag,
                    &event.data,
                ) {
                    Ok(projected) => {
                        materialized.insert(uri);
                        events.extend(
                            projected
                                .into_iter()
                                .filter(|event| event_in_range(event, &range.start, &range.end)),
                        );
                    }
                    Err(_) => failed.push(uri),
                }
            }
            one_outcome_per_id(&mut failed, &materialized);
            Ok(event_page(events, offset, range.limit, failed, Vec::new()))
        })
    }

    fn event_get(&self, event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_calendar_url = self.default_calendar_url.clone();
        Box::pin(async move {
            reject_recurrence_instance_id(&event, AccountOperation::EventGet)?;
            Self::fetch_event_from_url(
                client,
                default_calendar_url,
                None,
                event,
                AccountOperation::EventGet,
            )
            .await
        })
    }

    fn event_create(&self, event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_calendar_url = self.default_calendar_url.clone();
        Box::pin(async move {
            let calendar_url = Self::calendar_url(
                &client,
                default_calendar_url.as_deref(),
                Some(event.calendar_id.clone()),
                AccountOperation::EventCreate,
            )?;
            let uid = new_uid();
            let path = format!("{uid}.ics");
            let url = append_path(&calendar_url, &path);
            let body = create_to_ical(&event, &uid);
            client
                .put_event(
                    &url,
                    body,
                    PutCondition::IfNoneMatch,
                    AccountOperation::EventCreate,
                )
                .await?;
            Ok(EventId(url))
        })
    }

    fn event_update(
        &self,
        event: EventId,
        patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let client = Arc::clone(&self.client);
        let default_calendar_url = self.default_calendar_url.clone();
        Box::pin(async move {
            reject_recurrence_instance_id(&event, AccountOperation::EventUpdate)?;
            let url = client.resolve_url(&event.0);
            // A `calendar_id` naming a collection other than the event's own is
            // a relocation. It used to return `Ok(())` having moved nothing,
            // then was refused outright; it is now performed.
            let relocation = patch
                .calendar_id
                .as_ref()
                .map(|calendar| client.resolve_url(&calendar.0))
                .filter(|target| {
                    event_calendar_url(&url).is_some_and(|source| !same_url(target, &source))
                });
            let current = Self::fetch_event_from_url(
                Arc::clone(&client),
                default_calendar_url,
                patch.calendar_id.clone(),
                event.clone(),
                AccountOperation::EventUpdate,
            )
            .await?;
            let body = patch_to_ical(&current, &patch)
                .map_err(|_| unsupported_error(AccountOperation::EventUpdate))?;
            let Some(target_calendar) = relocation else {
                client
                    .put_event(
                        &url,
                        body,
                        put_condition(current.etag.as_deref()),
                        AccountOperation::EventUpdate,
                    )
                    .await?;
                return Ok(());
            };
            // A content patch riding along with the move needs a write of its
            // own; a move-only patch does not, and must not be charged a
            // partial-failure verdict for a leg it never needed.
            let content_changed = patch_changes_content(&patch);
            Self::relocate_event(&client, &url, &target_calendar, body, content_changed).await
        })
    }

    fn event_delete(&self, event: EventId) -> AccountFuture<Result<(), AccountError>> {
        let client = Arc::clone(&self.client);
        Box::pin(async move {
            reject_recurrence_instance_id(&event, AccountOperation::EventDelete)?;
            let url = client.resolve_url(&event.0);
            client
                .delete_event(&url, AccountOperation::EventDelete)
                .await
        })
    }

    fn event_rsvp(
        &self,
        event: EventId,
        status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        let client = Arc::clone(&self.client);
        let default_calendar_url = self.default_calendar_url.clone();
        let rsvp_email = self.rsvp_email.clone();
        let schedule_outbox_url = self.schedule_outbox_url.clone();
        Box::pin(async move {
            reject_recurrence_instance_id(&event, AccountOperation::EventRsvp)?;
            let Some(rsvp_email) = rsvp_email else {
                return Err(unsupported_error(AccountOperation::EventRsvp));
            };
            let Some(schedule_outbox_url) = schedule_outbox_url else {
                return Err(unsupported_error(AccountOperation::EventRsvp));
            };
            let current = Self::fetch_event_from_url(
                Arc::clone(&client),
                default_calendar_url,
                None,
                event.clone(),
                AccountOperation::EventRsvp,
            )
            .await?;
            let reply = rsvp_reply_ical(&current, status, &rsvp_email)
                .map_err(|_| unsupported_error(AccountOperation::EventRsvp))?;
            // Defensive on the success path: `rsvp_reply_ical` above
            // already fails when the event names no organizer, so this
            // guard cannot fire after it succeeded. Kept anyway - the
            // alternative is an `expect` that panics if that coupling
            // ever loosens, and an error is the better failure mode.
            let organizer_email = current
                .organizer
                .as_ref()
                .map(|organizer| organizer.email.clone())
                .ok_or_else(|| unsupported_error(AccountOperation::EventRsvp))?;
            client
                .post_schedule_reply(&schedule_outbox_url, &rsvp_email, &organizer_email, reply)
                .await?;
            // Everything from here on runs after the organizer has already
            // been told. Local encoding failures are as much a second-leg
            // failure as a refused PUT, so they take the same wrapping
            // rather than reporting a bare `unsupported`.
            let patch = rsvp_patch(&current, status, &rsvp_email).map_err(|_| {
                rsvp_local_write_error(unsupported_error(AccountOperation::EventRsvp))
            })?;
            let body = patch_to_ical(&current, &patch).map_err(|_| {
                rsvp_local_write_error(unsupported_error(AccountOperation::EventRsvp))
            })?;
            let url = client.resolve_url(&event.0);
            client
                .put_event(
                    &url,
                    body,
                    put_condition(current.etag.as_deref()),
                    AccountOperation::EventRsvp,
                )
                .await
                .map_err(rsvp_local_write_error)?;
            Ok(())
        })
    }

    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_calendar_url = self.default_calendar_url.clone();
        Box::pin(async move {
            let offset = decode_event_page_cursor(
                request.page_cursor.clone(),
                AccountOperation::EventSearch,
            )?;
            let calendar_url = Self::calendar_url(
                &client,
                default_calendar_url.as_deref(),
                request.calendar_id,
                AccountOperation::EventSearch,
            )?;
            let needle = request.query.to_lowercase();
            let fetched = if needle.is_empty() {
                // The match-all path lists first, then multigets. Both
                // legs can lose individual resources inside their 207,
                // so both feed `failed_ids` - dropping the listing's
                // casualties would report a match-all search as
                // complete when it was not.
                let listing = client
                    .list_events_listing(&calendar_url, AccountOperation::EventSearch)
                    .await?;
                let uris = listing
                    .entries
                    .iter()
                    .map(|entry| entry.uri.clone())
                    .collect::<Vec<_>>();
                let mut fetched = client
                    .fetch_events(&calendar_url, &uris, AccountOperation::EventSearch)
                    .await?;
                fetched.report.failed.extend(
                    listing
                        .failed_hrefs
                        .into_iter()
                        .map(|href| crate::parse::CalDavFailedResource { href, status: None }),
                );
                fetched
            } else {
                client
                    .query_events_text(&calendar_url, &request.query)
                    .await?
            };
            let skipped_scopes = skipped_calendar_scope(&calendar_url, fetched.degraded);
            let fetched = fetched.report;
            let mut seen = HashSet::new();
            // Search dedups across the four per-property REPORTs, so the
            // failed hrefs need the same treatment before they become
            // `failed_ids`.
            let mut failed = fetched.failed_hrefs();
            failed.sort_unstable();
            failed.dedup();
            let mut events = Vec::new();
            let mut materialized = HashSet::new();
            for event in fetched
                .events
                .into_iter()
                .filter(|event| seen.insert(event.uri.clone()))
            {
                let uri = event.uri;
                match events_from_ical(
                    uri.clone(),
                    CalendarId(calendar_url.clone()),
                    event.etag,
                    &event.data,
                ) {
                    Ok(projected) => {
                        materialized.insert(uri);
                        events.extend(
                            projected
                                .into_iter()
                                .filter(|event| event_matches(event, &needle)),
                        );
                    }
                    Err(_) => failed.push(uri),
                }
            }
            one_outcome_per_id(&mut failed, &materialized);
            Ok(event_page(
                events,
                offset,
                request.limit,
                failed,
                skipped_scopes,
            ))
        })
    }

    fn thread_hydrate(
        &self,
        _thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        unsupported_future(AccountOperation::HydrateThread)
    }

    fn message_hydrate(
        &self,
        _message: ObjectId,
        _projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        unsupported_future(AccountOperation::HydrateMessage)
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Ok(()) })
    }
}

fn rsvp_email_from_config(config: &CalDavConfig) -> Option<String> {
    match &config.credentials {
        CalDavCredentials::Basic { username, .. } if username.contains('@') => {
            Some(username.to_ascii_lowercase())
        }
        _ => None,
    }
}

fn decode_event_page_cursor(
    cursor: Option<Vec<u8>>,
    operation: AccountOperation,
) -> Result<usize, AccountError> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let value =
        String::from_utf8(cursor).map_err(|error| local_error(operation, error.to_string()))?;
    value
        .parse()
        .map_err(|error| local_error(operation, format!("invalid CalDAV page cursor: {error}")))
}

/// Slice a materialized result set into one offset page.
///
/// Takes `Vec<CalendarEvent>` rather than being generic on purpose: the sort
/// below is the load-bearing half of the offset contract, and a generic
/// signature would let a caller page something with no stable key at all.
///
/// The offset is local, and each continuation re-runs the remote REPORT. DAV
/// guarantees no ordering on a multistatus, so slicing raw response order
/// would let an unchanged result set come back permuted between page one and
/// page two: events the permutation moved behind the offset are SKIPPED and
/// events it moved past the offset are served TWICE, with nothing to tell the
/// consumer either happened. Sorting first is what makes the offset mean the
/// same thing on both requests. The key is the recurrence-qualified `EventId`
/// (resource href, plus `#RECURRENCE-ID` for an override instance), which is
/// unique per emitted item and stable across polls because it is derived from
/// the resource URL rather than from anything the server chose to order by.
/// `bifrost-carddav::contact_search` already sorted by `native_id` for this
/// reason; the two must not drift apart again.
fn event_page(
    mut items: Vec<CalendarEvent>,
    offset: usize,
    limit: Option<u32>,
    failed_ids: Vec<String>,
    skipped_scopes: Vec<SkippedScope>,
) -> Page<CalendarEvent> {
    items.sort_by(|left, right| left.id.0.cmp(&right.id.0));
    let total = items.len();
    let estimated_total = Some(u64::try_from(total).unwrap_or(u64::MAX));
    // A zero limit is an exhausted page, not a page of nothing that still
    // points at itself. Emitting the current offset again whenever results
    // exist gives a consumer that follows `next_cursor` an infinite loop that
    // never advances and never delivers an item.
    if limit == Some(0) {
        return Page {
            items: Vec::new(),
            next_cursor: None,
            estimated_total,
            failed_ids,
            skipped_scopes,
        };
    }
    let page_size = limit.map_or(total, |value| usize::try_from(value).unwrap_or(usize::MAX));
    let end = offset.saturating_add(page_size).min(total);
    Page {
        items: items.into_iter().skip(offset).take(page_size).collect(),
        next_cursor: (end < total).then(|| end.to_string().into_bytes()),
        estimated_total,
        failed_ids,
        skipped_scopes,
    }
}

/// The scheduling POST completed before the local PUT began. Preserve that
/// acknowledged first-leg evidence on any second-leg failure so callers know
/// the organizer may already have acted on the reply.
fn rsvp_local_write_error(error: AccountError) -> AccountError {
    partial_sequence_error(
        &error,
        AccountOperation::EventRsvp,
        "schedule reply was accepted but the local event update failed",
    )
}

/// Whether the patch changes anything other than which collection the event
/// lives in.
///
/// Derived by zeroing the relocation field and comparing against an empty
/// patch, rather than enumerating the content fields: a field added to
/// `EventPatch` is then covered automatically, where a hand-listed check would
/// silently stop noticing it.
///
/// Comparing SERIALIZED bytes is not equivalent - the writers re-emit, so a
/// move-only patch can compare unequal to the fetched body and earn a redundant
/// write plus the partial-failure verdict that rides on it. Twin of
/// `bifrost-carddav`'s `patch_changes_content`.
fn patch_changes_content(patch: &EventPatch) -> bool {
    let content = EventPatch {
        calendar_id: None,
        ..patch.clone()
    };
    content != EventPatch::default()
}

/// Reclassify the failure of a later leg of a non-atomic sequence whose
/// earlier leg already landed on the server.
///
/// `Protocol(PartialResponse)` plus an acknowledged `Attempt` is what tells a
/// consumer the request was half-applied rather than refused, so it reconciles
/// instead of replaying a write that already took effect. The original cause
/// chain rides along as secondary evidence.
fn partial_sequence_error(
    error: &AccountError,
    operation: AccountOperation,
    detail: &'static str,
) -> AccountError {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::CalDav,
            detail: Some(DiagnosticText::support_only(detail)),
        }),
    )
    .protocol(Protocol::CalDav)
    .operation(operation)
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )));
    for cause in error.chain().iter() {
        builder = builder.push_cause(cause.clone());
    }
    builder
        .try_build()
        .expect("valid partial-response classification")
}

fn partial_move_error(error: &AccountError, detail: &'static str) -> AccountError {
    partial_sequence_error(error, AccountOperation::EventUpdate, detail)
}

/// The last path segment of a resource URL - the name the resource keeps when
/// it moves into another collection.
///
/// Falls back to a fresh UID-backed name when the URL has no usable final
/// segment, so a move never targets the destination collection itself.
fn resource_file_name(url: &str) -> String {
    Url::parse(url)
        .ok()
        .and_then(|parsed| {
            parsed
                .path_segments()
                .and_then(|mut segments| segments.next_back().filter(|name| !name.is_empty()))
                .map(str::to_string)
        })
        .unwrap_or_else(|| format!("{}.ics", new_uid()))
}

fn unsupported_future<T: Send + 'static>(
    operation: AccountOperation,
) -> AccountFuture<Result<T, AccountError>> {
    Box::pin(async move { Err(unsupported_error(operation)) })
}

fn unsupported_stream<T: Send + 'static>(
    operation: AccountOperation,
) -> AccountStream<SyncEvent<T>> {
    Box::pin(stream::iter([
        SyncEvent::Terminated(unsupported_error(operation)),
        SyncEvent::Done(None),
    ]))
}

/// Derive the parent collection URL of an event resource URL.
///
/// This must go through a real URL parse. A final-slash search over the whole
/// string picks up a slash living in the query or fragment - for
/// `https://dav.test/cal/one.ics?redirect=/foo` it yields
/// `https://dav.test/cal/one.ics?redirect=/`, which is not a collection URL at
/// all, and which then travels onward as `calendar_id` and `calendar_native`.
/// Query and fragment are dropped before the final path segment is removed.
fn event_calendar_url(event_url: &str) -> Option<String> {
    bifrost_net::url::parent_collection_url(event_url)
}

/// Reduce the failure lane to one outcome per resource id.
///
/// Search runs a REPORT per property, so the resource the ATTENDEE query
/// refused can be the same resource the SUMMARY query returned in full.
/// The data arrived, so success is the true outcome; reporting the id in
/// both lanes would make a consumer count it twice and treat an event it
/// can display as lost. The sort and dedup finish the job for ids that
/// failed in more than one REPORT.
fn one_outcome_per_id(failed: &mut Vec<String>, materialized: &HashSet<String>) {
    failed.retain(|href| !materialized.contains(href));
    failed.sort_unstable();
    failed.dedup();
}

/// Publish a partially-refused walk as a skipped scope.
///
/// A REPORT leg that failed wholly means this calendar was not fully
/// searched. The items already collected stay valid, but "no more matches"
/// is not what happened, and the consumer needs the classified failure to
/// know whether to reauthorize, retry, or stop. `failed_ids` cannot carry
/// that: it is a bare list of resource ids with no recovery class, and the
/// refused leg often does not even name the resources it lost.
fn skipped_calendar_scope(calendar_url: &str, degraded: Option<AccountError>) -> Vec<SkippedScope> {
    degraded
        .map(|error| SkippedScope {
            scope: ErrorScope::Calendar {
                id: calendar_url.to_string().into(),
            },
            error,
        })
        .into_iter()
        .collect()
}

fn put_condition(etag: Option<&str>) -> PutCondition<'_> {
    etag.filter(|etag| {
        !etag
            .get(..2)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("W/"))
    })
    .map_or(PutCondition::None, PutCondition::IfMatch)
}

/// Resource identity for a consumer-restated calendar id against a URL this
/// crate derived. Delegates to the shared normalizing comparison: percent
/// encoding, host case and a redundant default port are spellings, not
/// relocations, and reading one as a move issues a MOVE onto the collection the
/// resource already lives in (refused by `Overwrite: F` as a spurious 412).
fn same_url(left: &str, right: &str) -> bool {
    same_dav_url(left, right)
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct EventSnapshot {
    calendar_url: String,
    sync_token: Option<String>,
    entries: Vec<EventSnapshotEntry>,
    /// Hrefs the server reported *failed* within the 207 of the poll
    /// that built this snapshot. Not persisted in the cursor (a
    /// per-poll observation); the diff preserves these `previous`
    /// entries rather than destroying them (brick 7).
    failed_hrefs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventSnapshotEntry {
    uri: String,
    etag: Option<String>,
}

/// Refuse an `EventId` that names one occurrence of a recurring series.
///
/// `events_from_ical` mints `EventId("{uri}#{recurrence_id}")` for an override
/// VEVENT so a consumer's index can tell the occurrences of a series apart.
/// That id is NOT addressable: `client.resolve_url` returns an absolute href
/// verbatim, and a URL fragment is never sent on the wire, so every one of
/// these ids silently resolves to the master resource. Unguarded,
/// `event_get` returned the master instead of the instance asked for,
/// `event_update` spliced and PUT the master so editing one occurrence
/// rewrote the series, `event_rsvp` answered for the series, and
/// `event_delete` DELETEd the whole `.ics` - deleting one occurrence
/// destroyed every occurrence.
///
/// A fragment id is therefore read-only, and saying so out loud is the whole
/// point: `Request(Malformed)` classifies to `ClientBug`, which no retry or
/// reopen can heal, and the caller learns the id is not a handle rather than
/// discovering later that a series is gone. This mirrors CardDAV refusing a
/// cross-address-book contact move through the same `local_error` helper.
///
/// Real per-occurrence support means resolving the resource, locating the
/// VEVENT by `RECURRENCE-ID`, and splicing or removing that component (an
/// occurrence delete emitting `EXDATE` on the master, or `STATUS:CANCELLED`
/// on the override - they differ in what attendees see). That is deliberately
/// NOT scheduled: this is the only calendar crate with the problem, because
/// it is the only one whose provider has no per-occurrence resource. Graph
/// syncs through `calendarView`, whose occurrences carry genuine Graph ids;
/// JMAP keeps overrides inside the master object and returns `Unsupported`
/// for one it cannot represent; Google's ids are `{calendar}::{event}` over
/// the provider's own instance ids. None of them can inherit this bug, and
/// none of them benefits from fixing it here.
fn reject_recurrence_instance_id(
    event: &EventId,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if event.0.contains('#') {
        return Err(crate::client::local_error(
            operation,
            "recurrence-instance event ids are read-only: they address the whole \
             series resource on the wire, so writing through one would change or \
             destroy every occurrence",
        ));
    }
    Ok(())
}

fn validate_event_scope(
    scope: &CursorScope,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if matches!(
        scope,
        CursorScope::Type(ObjectType::CalendarEvent) | CursorScope::Folder(_)
    ) {
        Ok(())
    } else {
        Err(crate::client::local_error(
            operation,
            "CalDAV only supports calendar-event cursor scopes",
        ))
    }
}

/// The cursor-scope collection set: one entry per DISCOVERED calendar, and
/// NOTHING when the home holds none.
///
/// `list_calendars` already returns the home itself when the home is genuinely
/// a calendar collection, so an empty result means an empty backend rather
/// than a discovery shape this crate has to paper over. Substituting the home
/// URL here would advertise a folder that does not exist, and cursor
/// establishment and inventory would then issue requests against it and
/// commonly terminate with 404. It would also contradict the empty-home
/// contract the listing APIs are pinned to.
fn discovered_collection_urls(collections: &[crate::parse::CalendarCollection]) -> Vec<String> {
    collections
        .iter()
        .map(|collection| collection.href.clone())
        .collect()
}

/// A call that names no calendar cannot be routed, because the calendar home
/// enumerated no collections.
///
/// The alternative - falling back to the calendar home URL - is what this
/// replaces. The home is not itself a collection in that case (`list_calendars`
/// already returns the home when it genuinely is one, so an empty result means
/// an empty backend), so every such request went to a resource a spec-correct
/// server 404s, and reported it as a remote failure rather than as the local
/// routing failure it is. It also contradicted the empty-home contract
/// `calendars_list` is pinned to.
fn no_default_calendar(operation: AccountOperation) -> AccountError {
    local_error(
        operation,
        "CalDAV account has no calendar collection to route a call that names none",
    )
}

/// The collection a call that names no calendar routes to: the first
/// discovered one, and `None` when the home enumerated none.
///
/// Deliberately has no access to the calendar home, so the fallback this
/// replaced cannot be reintroduced here without also changing the signature.
/// See `no_default_calendar` for why the home is the wrong answer.
fn default_collection_url(collections: &[crate::parse::CalendarCollection]) -> Option<String> {
    collections
        .first()
        .map(|collection| collection.href.clone())
}

fn collection_url_for_scope(
    scope: &CursorScope,
    default_url: Option<&str>,
    collection_urls: &[String],
    operation: AccountOperation,
) -> Result<String, AccountError> {
    match scope {
        CursorScope::Type(ObjectType::CalendarEvent) => default_url
            .map(str::to_string)
            .ok_or_else(|| no_default_calendar(operation)),
        CursorScope::Folder(folder) if collection_urls.contains(&folder.0) => Ok(folder.0.clone()),
        _ => Err(local_error(
            operation,
            "CalDAV cursor scope does not name a discovered calendar",
        )),
    }
}

fn scope_collection_url<'a>(
    scope: &'a CursorScope,
    default_url: Option<&'a str>,
) -> Option<&'a str> {
    match scope {
        CursorScope::Folder(folder) => Some(&folder.0),
        _ => default_url,
    }
}

fn collection_coverage_domain(scope: CursorScope, collection_url: Option<&str>) -> CoverageDomain {
    match scope {
        CursorScope::Folder(_) => CoverageDomain::full(scope),
        // An unroutable legacy scope still needs a domain to carry the
        // terminating stream; the walk fails before the region is read.
        _ => CoverageDomain {
            scope,
            coordinate: CoverageCoordinate::ProviderRegion {
                namespace: "caldav".to_string(),
                region: collection_url.unwrap_or_default().as_bytes().to_vec(),
            },
            snapshot: SnapshotIdentity::unstable(),
        },
    }
}

fn cursor_from_snapshot(scope: CursorScope, snapshot: &EventSnapshot) -> ChangeCursor {
    ChangeCursor {
        scope,
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::CalDav,
            envelope_version: CURSOR_ENVELOPE_VERSION,
            bytes: encode_cursor_snapshot(snapshot),
        },
        advanced_through: None,
        envelope_version: bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION,
    }
}

fn encode_cursor_snapshot(snapshot: &EventSnapshot) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CURSOR_MAGIC);
    write_string(&mut bytes, &snapshot.calendar_url);
    write_option_string(&mut bytes, snapshot.sync_token.as_deref());
    write_u32(&mut bytes, snapshot.entries.len());
    for entry in &snapshot.entries {
        write_string(&mut bytes, &entry.uri);
        write_option_string(&mut bytes, entry.etag.as_deref());
    }
    bytes
}

fn decode_cursor_snapshot(cursor: &ChangeCursor) -> Result<EventSnapshot, AccountError> {
    validate_event_scope(&cursor.scope, AccountOperation::SyncChanges)?;
    if cursor.server_state.protocol != ProtocolKind::CalDav
        || cursor.server_state.envelope_version != CURSOR_ENVELOPE_VERSION
    {
        return Err(cursor_error("CalDAV cursor protocol or version mismatch"));
    }
    let mut input = cursor.server_state.bytes.as_slice();
    if !input.starts_with(CURSOR_MAGIC) {
        return Err(cursor_error("CalDAV cursor magic mismatch"));
    }
    input = &input[CURSOR_MAGIC.len()..];
    let calendar_url = read_string(&mut input)?;
    let sync_token = read_option_string(&mut input)?;
    let count = read_u32(&mut input)?;
    if count > input.len() / 5 {
        return Err(cursor_error(
            "CalDAV cursor entry count exceeds remaining payload",
        ));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push(EventSnapshotEntry {
            uri: read_string(&mut input)?,
            etag: read_option_string(&mut input)?,
        });
    }
    if !input.is_empty() {
        return Err(cursor_error("CalDAV cursor has trailing bytes"));
    }
    Ok(EventSnapshot {
        calendar_url,
        sync_token,
        entries,
        failed_hrefs: Vec::new(),
    })
}

async fn changes_from_cursor(
    client: &CalDavClient,
    previous: &EventSnapshot,
) -> Result<(EventSnapshot, Vec<Change>), AccountError> {
    if let Some(sync_token) = previous.sync_token.as_deref() {
        let mut current = previous.clone();
        let mut changes = Vec::new();
        let mut token = sync_token.to_string();
        // RFC 6578 s3.6: a server may answer a sync REPORT with only part of
        // the change set, marking it with a 507 response for the collection
        // itself and a token that represents that partial progress. Stopping
        // there and checkpointing the partial token as if it were complete
        // loses every change beyond the truncation point until etag drift
        // happens to surface it. iCloud and large Cyrus collections truncate in
        // practice, so drain the rest by re-issuing the REPORT with the token
        // the server just handed back.
        for _ in 0..SYNC_TRUNCATION_ROUNDS {
            let report = client.sync_events(&previous.calendar_url, &token).await?;
            let truncated = report.truncated;
            let next_token = report.sync_token;
            changes.extend(apply_sync_report(&mut current, report.entries));
            let Some(next_token) = next_token else {
                tracing::warn!(
                    target: "bifrost_caldav::sync",
                    calendar = %previous.calendar_url,
                    "sync-collection response omitted the required sync-token; retaining the previous token"
                );
                break;
            };
            current.sync_token = Some(next_token.clone());
            if !truncated {
                break;
            }
            // Forward progress or stop. A server that repeats the token it was
            // given while still claiming truncation would otherwise spin this
            // loop against the wire for as many rounds as the cap allows.
            if next_token == token {
                tracing::warn!(
                    target: "bifrost_caldav::sync",
                    calendar = %previous.calendar_url,
                    "truncated sync-collection response returned the same sync-token; stopping the drain"
                );
                break;
            }
            token = next_token;
        }
        return Ok((current, changes));
    }
    let mut current = CalDavAccount::event_snapshot(
        client,
        None,
        &previous.calendar_url,
        AccountOperation::SyncChanges,
    )
    .await?;
    preserve_unobserved_event_entries(previous, &mut current);
    let changes = diff_event_snapshots(previous, &current);
    Ok((current, changes))
}

fn preserve_unobserved_event_entries(previous: &EventSnapshot, current: &mut EventSnapshot) {
    if current.entries.is_empty() && !previous.entries.is_empty() {
        current.entries.clone_from(&previous.entries);
        return;
    }
    let failed: HashSet<&str> = current.failed_hrefs.iter().map(String::as_str).collect();
    current.entries.extend(
        previous
            .entries
            .iter()
            .filter(|entry| failed.contains(entry.uri.as_str()))
            .cloned(),
    );
    current
        .entries
        .sort_by(|left, right| left.uri.cmp(&right.uri));
    current
        .entries
        .dedup_by(|left, right| left.uri == right.uri);
}

fn apply_sync_report(
    current: &mut EventSnapshot,
    entries: Vec<crate::parse::CalDavSyncEntry>,
) -> Vec<Change> {
    let mut changes = Vec::new();
    for entry in entries {
        let uri = entry.uri;
        // A per-member status that is neither success nor a removal is a
        // refusal to report on THAT member (403, 507, 503 ...). It says nothing
        // about the resource's content, so the prior snapshot entry is
        // preserved untouched: upserting it would record an etag-less entry and
        // emit a Created/Updated for a resource nobody observed, and the
        // etag-less entry then makes the next poll report an Updated as well.
        if entry
            .status
            .is_some_and(|code| !(200..300).contains(&code) && !matches!(code, 404 | 410))
        {
            continue;
        }
        if matches!(entry.status, Some(404 | 410)) {
            // Only emit a Destroyed event for an href the prior snapshot
            // actually held. A 404/410 sync-report entry for an unknown
            // href (a resource created and deleted between polls, or one
            // the consumer never saw) would otherwise surface a phantom
            // delete for an id the consumer has no record of.
            if let Some(index) = current.entries.iter().position(|known| known.uri == uri) {
                current.entries.remove(index);
                changes.push(object_change(&uri, ObjectChangeKind::Destroyed));
            }
            continue;
        }
        let resolved = EventSnapshotEntry {
            uri: uri.clone(),
            etag: entry.etag,
        };
        match current
            .entries
            .binary_search_by(|known| known.uri.cmp(&resolved.uri))
        {
            Ok(index) => {
                if current.entries[index].etag != resolved.etag {
                    current.entries[index] = resolved;
                    changes.push(object_change(&uri, ObjectChangeKind::Updated));
                }
            }
            Err(index) => {
                current.entries.insert(index, resolved);
                changes.push(object_change(&uri, ObjectChangeKind::Created));
            }
        }
    }
    changes
}

fn diff_event_snapshots(previous: &EventSnapshot, current: &EventSnapshot) -> Vec<Change> {
    // Suspected transient empty multistatus: a server returning zero
    // hrefs against a populated local snapshot would emit a Destroyed for
    // every object and wipe the consumer's store. Treat
    // empty-vs-nonempty as "no observation," not "everything deleted."
    // A genuine empty-out reconciles on the next non-empty poll or via
    // the sync-token path (apply_sync_report), which is not exposed to a
    // bare empty multistatus.
    //
    // Accepted cost, stated plainly because it is easy to re-file as a bug:
    // a REAL empty-out - a user deleting every event in the collection - is
    // suppressed along with the transient empty-207, on this poll AND on
    // later ones, because on the wire the two are identical. There is no
    // signal that separates them, so the choice is between never wrongly
    // wiping a consumer's store and never missing a genuine mass delete;
    // this crate takes the first. `reference/caldav.md` says so, with the
    // ctag short-circuit caveat, and `bifrost-carddav` makes the same trade
    // in its own snapshot diff - change one and change the other.
    if current.entries.is_empty() && !previous.entries.is_empty() {
        return Vec::new();
    }
    let failed: HashSet<&str> = current.failed_hrefs.iter().map(String::as_str).collect();
    let mut changes = Vec::new();
    let mut left = 0;
    let mut right = 0;
    while left < previous.entries.len() || right < current.entries.len() {
        match (previous.entries.get(left), current.entries.get(right)) {
            (Some(old), Some(new)) if old.uri == new.uri => {
                if old.etag != new.etag {
                    changes.push(object_change(&new.uri, ObjectChangeKind::Updated));
                }
                left += 1;
                right += 1;
            }
            (Some(old), Some(new)) if old.uri < new.uri => {
                push_destroyed_unless_failed(&mut changes, &failed, &old.uri);
                left += 1;
            }
            (Some(_), Some(new)) => {
                changes.push(object_change(&new.uri, ObjectChangeKind::Created));
                right += 1;
            }
            (Some(old), None) => {
                push_destroyed_unless_failed(&mut changes, &failed, &old.uri);
                left += 1;
            }
            (None, Some(new)) => {
                changes.push(object_change(&new.uri, ObjectChangeKind::Created));
                right += 1;
            }
            (None, None) => break,
        }
    }
    changes
}

/// Emit `Destroyed` for `uri` unless the server reported that resource
/// *failed* within the 207. A transiently-failed resource is preserved
/// locally rather than treated as absent (brick 7).
fn push_destroyed_unless_failed(changes: &mut Vec<Change>, failed: &HashSet<&str>, uri: &str) {
    if failed.contains(uri) {
        return;
    }
    changes.push(object_change(uri, ObjectChangeKind::Destroyed));
}

fn object_change(uri: &str, kind: ObjectChangeKind) -> Change {
    Change::ObjectChange(ObjectChange {
        id: ObjectId(uri.to_string()),
        kind,
    })
}

fn inventory_entry_from_snapshot(entry: &EventSnapshotEntry) -> InventoryEntry {
    InventoryEntry {
        id: ObjectId(entry.uri.clone()),
        memberships: Vec::new(),
        size: None,
        blob_id: None,
        fingerprint: Fingerprint {
            server_version: entry
                .etag
                .clone()
                .map(ServerVersion::ETag)
                .unwrap_or(ServerVersion::Unavailable),
            size: None,
            flags_hash: bifrost_types::canonical_flags_hash(std::iter::empty::<&str>()),
        },
        thread_id: None,
        message_id: None,
        references: Vec::new(),
        in_reply_to: None,
    }
}

fn write_string(bytes: &mut Vec<u8>, value: &str) {
    write_u32(bytes, value.len());
    bytes.extend_from_slice(value.as_bytes());
}

fn write_option_string(bytes: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            bytes.push(1);
            write_string(bytes, value);
        }
        None => bytes.push(0),
    }
}

fn write_u32(bytes: &mut Vec<u8>, value: usize) {
    let value = u32::try_from(value).unwrap_or(u32::MAX);
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn read_string(input: &mut &[u8]) -> Result<String, AccountError> {
    let len = read_u32(input)?;
    if input.len() < len {
        return Err(cursor_error("CalDAV cursor string length exceeds payload"));
    }
    let value = String::from_utf8(input[..len].to_vec())
        .map_err(|error| cursor_error(format!("CalDAV cursor string is not UTF-8: {error}")))?;
    *input = &input[len..];
    Ok(value)
}

fn read_option_string(input: &mut &[u8]) -> Result<Option<String>, AccountError> {
    let Some((tag, rest)) = input.split_first() else {
        return Err(cursor_error("CalDAV cursor option tag is missing"));
    };
    *input = rest;
    match tag {
        0 => Ok(None),
        1 => read_string(input).map(Some),
        _ => Err(cursor_error("CalDAV cursor option tag is invalid")),
    }
}

fn read_u32(input: &mut &[u8]) -> Result<usize, AccountError> {
    let bytes = input
        .get(..4)
        .ok_or_else(|| cursor_error("CalDAV cursor integer is truncated"))?;
    let bytes = <[u8; 4]>::try_from(bytes)
        .map_err(|error| cursor_error(format!("CalDAV cursor integer shape: {error}")))?;
    let value = u32::from_be_bytes(bytes);
    *input = &input[4..];
    Ok(value as usize)
}

fn cursor_error(message: impl Into<String>) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
        Cause::State(StateCause::SchemaIncompatible),
    )
    .protocol(Protocol::CalDav)
    .operation(AccountOperation::SyncChanges)
    .text(DiagnosticText::support_only(message))
    .try_build()
    .expect("valid account error classification")
}

fn event_in_range(event: &CalendarEvent, start: &EventTime, end: &EventTime) -> bool {
    if start.value.is_empty() || end.value.is_empty() || event.start.value.is_empty() {
        return true;
    }
    let Some((range_start, range_end)) = time_interval(start, end, false) else {
        return true;
    };
    let Some((event_start, event_end)) = time_interval(&event.start, &event.end, event.is_all_day)
    else {
        return true;
    };
    let rrule = event.recurrence.rrule.as_deref().unwrap_or_default();
    if rrule.is_empty() {
        // Non-recurring: plain interval overlap.
        return event_start < range_end && event_end > range_start;
    }
    // Recurring master: its own interval can sit entirely before the
    // window while a later occurrence lands inside it. The server's
    // time-range REPORT already expanded the RRULE and returned this
    // resource because an occurrence overlaps, so the local guard must
    // not drop it. Occurrences only run forward from the master start, so
    // the series can reach the window unless it begins after the window
    // ends, or provably ends (RRULE UNTIL) before the window starts.
    if event_start >= range_end {
        return false;
    }
    if event_end > range_start {
        return true;
    }
    match rrule_until(rrule) {
        Some(until) => until >= range_start,
        // COUNT-bounded or open-ended series: without full expansion we
        // trust the server's REPORT and retain the master.
        None => true,
    }
}

/// Parse the instant named by an RRULE `UNTIL=` part, if present. Handles
/// both `YYYYMMDD` (date) and `YYYYMMDDTHHMMSS[Z]` (date-time) forms.
fn rrule_until(rrule: &str) -> Option<Timestamp> {
    let value = rrule.split(';').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        key.eq_ignore_ascii_case("UNTIL").then_some(value)
    })?;
    parse_ical_instant(value.trim())
}

fn parse_ical_instant(value: &str) -> Option<Timestamp> {
    if value.len() == 8 {
        let date = civil::Date::strptime("%Y%m%d", value).ok()?;
        return Offset::UTC
            .to_timestamp(date.to_datetime(civil::Time::MIN))
            .ok();
    }
    let core = value.trim_end_matches('Z');
    let naive = civil::DateTime::strptime("%Y%m%dT%H%M%S", core).ok()?;
    Offset::UTC.to_timestamp(naive).ok()
}

fn time_interval(
    start: &EventTime,
    end: &EventTime,
    is_all_day: bool,
) -> Option<(Timestamp, Timestamp)> {
    let start = comparable_time(start, is_all_day)?;
    let end = comparable_time(end, is_all_day).unwrap_or(start);
    Some((start, end))
}

fn comparable_time(time: &EventTime, is_all_day: bool) -> Option<Timestamp> {
    if is_all_day || time.value.len() == 10 {
        let date = civil::Date::strptime("%Y-%m-%d", &time.value).ok()?;
        return Offset::UTC
            .to_timestamp(date.to_datetime(civil::Time::MIN))
            .ok();
    }
    time.value.parse::<Timestamp>().ok()
}

fn caldav_query_time(time: &EventTime) -> Option<String> {
    let instant = if time.value.len() == 10 {
        let date = civil::Date::strptime("%Y-%m-%d", &time.value).ok()?;
        Offset::UTC
            .to_timestamp(date.to_datetime(civil::Time::MIN))
            .ok()?
    } else {
        time.value.parse::<Timestamp>().ok()?
    };
    Some(
        Offset::UTC
            .to_datetime(instant)
            .strftime("%Y%m%dT%H%M%SZ")
            .to_string(),
    )
}

fn caldav_query_range(
    start: &EventTime,
    end: &EventTime,
) -> Result<(String, String), AccountError> {
    let start = caldav_query_time(start).ok_or_else(|| {
        crate::client::local_error(
            AccountOperation::EventsInRange,
            format!("invalid event range start: {}", start.value),
        )
    })?;
    let end = caldav_query_time(end).ok_or_else(|| {
        crate::client::local_error(
            AccountOperation::EventsInRange,
            format!("invalid event range end: {}", end.value),
        )
    })?;
    Ok((start, end))
}

fn event_matches(event: &CalendarEvent, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    let needle = needle.to_lowercase();
    event
        .title
        .as_deref()
        .is_some_and(|value| contains(value, &needle))
        || event
            .description
            .as_deref()
            .is_some_and(|value| contains(value, &needle))
        || event
            .location
            .as_deref()
            .is_some_and(|value| contains(value, &needle))
        || event
            .attendees
            .iter()
            .any(|attendee| contains(&attendee.email, &needle))
}

fn contains(value: &str, needle: &str) -> bool {
    value.to_lowercase().contains(needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_dav_core::test_support::{dav_script_empty, scripted_dav_net};

    /// `set_priority` and `set_bandwidth_cap` reach the account's transport.
    ///
    /// Both were silent no-ops for as long as this crate ran its own reqwest
    /// client: an IMAP-shaped account composed `with_caldav` and given a
    /// bandwidth cap did not cap its DAV legs, and nothing said so. This is the
    /// door dav-B9 exists to open, so it is pinned at the door rather than
    /// inferred from the transport swap.
    #[tokio::test]
    async fn the_priority_and_bandwidth_doors_reach_the_transport() {
        let net = scripted_dav_net(&dav_script_empty());
        let client = CalDavClient::with_account_net("https://dav.example.test", net.clone());
        let account = CalDavAccount::for_tests(Arc::new(client), "https://dav.example.test/cal/");

        account.set_bandwidth_cap(Some(4_096));
        assert_eq!(net.bandwidth_cap(), Some(4_096));
        account.set_priority(Priority::Background);
        assert_eq!(net.priority(), Priority::Background);

        // And back off again, so a test that only ever sets a value cannot
        // pass against a door wired to a constant.
        account.set_bandwidth_cap(None);
        assert_eq!(net.bandwidth_cap(), None);
        account.set_priority(Priority::Foreground);
        assert_eq!(net.priority(), Priority::Foreground);
    }

    fn collection(href: &str) -> crate::parse::CalendarCollection {
        crate::parse::CalendarCollection {
            href: href.to_string(),
            display_name: None,
            color: None,
            can_edit: None,
            sync_token: None,
        }
    }

    /// An empty calendar home yields NO cursor scopes.
    ///
    /// The collection walk returns the home itself when the home is a calendar
    /// collection, so an empty walk is an empty backend. Substituting the home
    /// would advertise a folder that does not exist and drive every cursor and
    /// inventory request for it into a 404.
    #[test]
    fn an_empty_home_produces_no_cursor_scope_collections() {
        assert!(discovered_collection_urls(&[]).is_empty());
    }

    /// An empty home leaves the account with NO default calendar, rather than
    /// the calendar home standing in for one.
    ///
    /// This is the `open`-side half of
    /// `an_empty_backend_refuses_collection_less_calls_before_the_wire`, which
    /// pins what a `None` default does but constructs it directly. Without this
    /// assertion, restoring the home fallback here would leave that test
    /// passing. Twin of the CardDAV assertion; keep them in step.
    #[test]
    fn an_empty_home_leaves_no_default_calendar() {
        assert_eq!(default_collection_url(&[]), None);
        assert_eq!(
            default_collection_url(&[
                collection("https://dav.example.test/cal/work/"),
                collection("https://dav.example.test/cal/personal/"),
            ])
            .as_deref(),
            Some("https://dav.example.test/cal/work/")
        );
    }

    #[test]
    fn every_discovered_calendar_becomes_a_cursor_scope_collection() {
        assert_eq!(
            discovered_collection_urls(&[
                collection("https://dav.example.test/cal/work/"),
                collection("https://dav.example.test/cal/home/"),
            ]),
            vec![
                "https://dav.example.test/cal/work/".to_string(),
                "https://dav.example.test/cal/home/".to_string(),
            ]
        );
    }

    /// A CalDAV `CalendarId` IS the resolved collection href, on the listing
    /// surface as well as on the request-routing and `ErrorScope` surfaces.
    ///
    /// This is the agreement a consumer relies on when it correlates an
    /// `ErrorScope::Calendar` against ids from `list_calendars`. It holds
    /// because the XML decode boundary rebases every href against its request
    /// URI, which makes the account layer's `resolve_url` a no-op on an id
    /// that came out of the listing. Pinned rather than merely written down:
    /// if either side stopped absolutizing, the two ids would still both be
    /// `CalendarId` and the mismatch would be silent.
    #[test]
    fn a_listed_calendar_id_is_the_absolute_href_error_scopes_carry() {
        let client = CalDavClient::with_account_net(
            "https://dav.example.com/dav/",
            scripted_dav_net(&dav_script_empty()),
        );
        let mut collection = CalendarCollection {
            href: "work/".to_string(),
            display_name: Some("Work".to_string()),
            color: None,
            can_edit: Some(true),
            sync_token: None,
        };
        collection.resolve_href("https://dav.example.com/dav/calendars/");

        let calendar = CalDavAccount::map_calendar(collection);

        assert_eq!(calendar.id.0, "https://dav.example.com/dav/calendars/work/");
        assert_eq!(calendar.native_id, calendar.id.0);
        assert_eq!(
            client.resolve_url(&calendar.id.0),
            calendar.id.0,
            "the routing/ErrorScope side must not rewrite a listed calendar id"
        );
    }

    #[test]
    fn a_materialized_resource_leaves_the_failure_lane() {
        let mut failed = vec![
            "/cal/one.ics".to_string(),
            "/cal/two.ics".to_string(),
            "/cal/two.ics".to_string(),
        ];
        let materialized = HashSet::from(["/cal/one.ics".to_string()]);

        one_outcome_per_id(&mut failed, &materialized);

        assert_eq!(failed, vec!["/cal/two.ics".to_string()]);
    }

    #[test]
    fn a_degraded_leg_becomes_a_skipped_calendar_scope() {
        let error = crate::client::status_error(
            AccountOperation::EventSearch,
            reqwest::StatusCode::UNAUTHORIZED,
            "refused".to_string(),
        );
        let recovery = error.recovery().clone();

        let skipped = skipped_calendar_scope("https://dav.example.test/cal/", Some(error));

        assert_eq!(skipped.len(), 1);
        assert_eq!(
            skipped[0].scope,
            ErrorScope::Calendar {
                id: ("https://dav.example.test/cal/".to_string()).into(),
            }
        );
        assert_eq!(skipped[0].error.recovery(), &recovery);
        assert!(skipped_calendar_scope("https://dav.example.test/cal/", None).is_empty());
    }

    fn time(value: &str) -> EventTime {
        EventTime {
            value: value.to_string(),
            timezone: None,
        }
    }

    fn event(start: &str, end: &str, is_all_day: bool) -> CalendarEvent {
        CalendarEvent {
            id: EventId("/cal/one.ics".to_string()),
            calendar_id: CalendarId("/cal/".to_string()),
            native_id: "/cal/one.ics".to_string(),
            uid: None,
            etag: None,
            provenance: CalendarProvenance {
                provider: ProtocolKind::CalDav,
                native: "/cal/one.ics".to_string(),
                calendar_native: None,
            },
            title: None,
            description: None,
            location: None,
            start: time(start),
            end: time(end),
            is_all_day,
            status: EventStatus::Confirmed,
            availability: EventAvailability::Busy,
            visibility: EventVisibility::Default,
            self_response: RsvpStatus::Unknown,
            organizer: None,
            attendees: Vec::new(),
            reminders: Vec::new(),
            recurrence: EventRecurrence::default(),
            html_link: None,
            raw_ical: None,
        }
    }

    fn recurring_event(start: &str, end: &str, rrule: &str) -> CalendarEvent {
        let mut event = event(start, end, false);
        event.recurrence.rrule = Some(rrule.to_string());
        event
    }

    #[tokio::test]
    async fn caldav_host_attachment_unsupported() {
        // CalDAV has no cloud-drive hosting; the flag is false (Default) and
        // the leg returns `Unsupported(HostAttachment)`.
        assert!(!caldav_capabilities(true).pim_methods.host_attachment);

        let err = unsupported_future::<HostedAttachment>(AccountOperation::HostAttachment)
            .await
            .expect_err("caldav host_attachment is unsupported");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::HostAttachment)
        );
    }

    #[tokio::test]
    async fn caldav_open_raw_rfc822_unsupported() {
        // CalDAV advertises no raw RFC822 read; the flag is false and the
        // stream's first item terminates `Unsupported(OpenRawRfc822)`.
        assert!(!caldav_capabilities(true).pim_methods.open_raw_rfc822);

        let mut stream = unsupported_stream::<Bytes>(AccountOperation::OpenRawRfc822);
        let first = stream.next().await.expect("first event");
        match first {
            SyncEvent::Terminated(err) => assert_eq!(
                err.kind(),
                &AccountErrorKind::Unsupported(AccountOperation::OpenRawRfc822)
            ),
            other => panic!("expected Terminated, got {other:?}"),
        }
    }

    #[test]
    fn range_filter_includes_all_day_on_window_start() {
        let event = event("2026-06-02", "2026-06-03", true);

        assert!(event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-03T00:00:00Z")
        ));
    }

    #[test]
    fn range_filter_includes_event_that_started_before_window() {
        let event = event("2026-06-01T23:00:00Z", "2026-06-02T01:00:00Z", false);

        assert!(event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-02T23:59:59Z")
        ));
    }

    #[test]
    fn range_filter_excludes_event_after_window() {
        let event = event("2026-06-04T00:00:00Z", "2026-06-04T01:00:00Z", false);

        assert!(!event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-03T00:00:00Z")
        ));
    }

    #[test]
    fn range_filter_excludes_event_ending_at_window_start() {
        let event = event("2026-06-01", "2026-06-02", true);

        assert!(!event_in_range(
            &event,
            &time("2026-06-02T00:00:00Z"),
            &time("2026-06-03T00:00:00Z")
        ));
    }

    #[test]
    fn rsvp_second_leg_error_records_acknowledged_first_leg() {
        let error = crate::client::transport_error(AccountOperation::EventRsvp, "put failed");
        let decorated = rsvp_local_write_error(error);

        assert!(decorated.chain().iter().any(|cause| matches!(
            cause,
            Cause::Attempt(attempt)
                if attempt.transmission_state == TransmissionState::Acknowledged
        )));
        assert!(matches!(
            decorated.recovery(),
            RecoveryClass::Reconcile(advice)
                if advice.reason == ReconcileReason::PartialCompletionSignal
        ));
    }

    #[test]
    fn range_filter_retains_recurring_master_before_window() {
        // A weekly master whose own interval sits a month before the
        // window still has in-window occurrences; the recurrence-aware
        // guard must keep it (the server's time-range REPORT already
        // returned it).
        let event = recurring_event(
            "2026-05-04T12:00:00Z",
            "2026-05-04T13:00:00Z",
            "FREQ=WEEKLY",
        );

        assert!(event_in_range(
            &event,
            &time("2026-06-01T00:00:00Z"),
            &time("2026-06-08T00:00:00Z")
        ));
    }

    #[test]
    fn range_filter_drops_recurring_master_that_ended_before_window() {
        // A bounded weekly series whose UNTIL is before the window start
        // cannot reach it, so the guard drops the out-of-window master.
        let event = recurring_event(
            "2026-05-04T12:00:00Z",
            "2026-05-04T13:00:00Z",
            "FREQ=WEEKLY;UNTIL=20260525T120000Z",
        );

        assert!(!event_in_range(
            &event,
            &time("2026-06-01T00:00:00Z"),
            &time("2026-06-08T00:00:00Z")
        ));
    }

    #[test]
    fn range_filter_drops_recurring_master_starting_after_window() {
        let event = recurring_event(
            "2026-07-04T12:00:00Z",
            "2026-07-04T13:00:00Z",
            "FREQ=WEEKLY",
        );

        assert!(!event_in_range(
            &event,
            &time("2026-06-01T00:00:00Z"),
            &time("2026-06-08T00:00:00Z")
        ));
    }

    #[test]
    fn rrule_until_parses_date_form_and_case_insensitive_key() {
        // UNTIL comes in both date and date-time forms, and RRULE part
        // names are case-insensitive per RFC 5545.
        let until = rrule_until("FREQ=WEEKLY;until=20260525").expect("date-form UNTIL");
        assert_eq!(until.to_string(), "2026-05-25T00:00:00Z");

        let until = rrule_until("FREQ=DAILY;UNTIL=20260525T120000").expect("datetime UNTIL");
        assert_eq!(until.to_string(), "2026-05-25T12:00:00Z");

        let until = rrule_until("FREQ=DAILY;UNTIL=20260525T120000Z").expect("UTC UNTIL");
        assert_eq!(until.to_string(), "2026-05-25T12:00:00Z");

        assert!(rrule_until("FREQ=DAILY").is_none());
        assert!(rrule_until("FREQ=DAILY;UNTIL=garbage").is_none());
        assert!(rrule_until("").is_none());
    }

    #[test]
    fn caldav_query_time_formats_rfc3339_as_utc_basic() {
        assert_eq!(
            caldav_query_time(&time("2026-06-02T02:30:00+02:00")).as_deref(),
            Some("20260602T003000Z")
        );
    }

    #[test]
    fn caldav_query_time_formats_dates_as_utc_midnight() {
        assert_eq!(
            caldav_query_time(&time("2026-06-02")).as_deref(),
            Some("20260602T000000Z")
        );
    }

    #[test]
    fn event_calendar_url_is_the_resource_parent() {
        assert_eq!(
            event_calendar_url("https://dav.example.test/calendars/work/one.ics").as_deref(),
            Some("https://dav.example.test/calendars/work/")
        );
        // A slash in the query or fragment is not a path separator; a raw
        // final-slash search over the string returns a query fragment here.
        assert_eq!(
            event_calendar_url("https://dav.example.test/calendars/work/one.ics?redirect=/foo")
                .as_deref(),
            Some("https://dav.example.test/calendars/work/")
        );
        assert_eq!(
            event_calendar_url("https://dav.example.test/calendars/work/one.ics#a/b").as_deref(),
            Some("https://dav.example.test/calendars/work/")
        );
        assert_eq!(event_calendar_url("not-a-url"), None);
    }

    #[test]
    fn invalid_query_range_is_rejected_before_report_construction() {
        let error = caldav_query_range(&time("not-a-time"), &time("2026-06-03T00:00:00Z"))
            .expect_err("invalid start is rejected");

        assert_eq!(
            error.kind(),
            &AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
        assert_eq!(error.operation(), Some(AccountOperation::EventsInRange));
    }

    #[test]
    fn event_search_matches_unicode_case_folded_text() {
        let mut event = event("2026-06-02T00:00:00Z", "2026-06-02T01:00:00Z", false);
        event.title = Some("A-ring workshop".to_string());
        event.location = Some("\u{00c5}RSTAD".to_string());

        assert!(event_matches(&event, "\u{00e5}"));
        assert!(event_matches(&event, "\u{00e5}r"));
    }

    #[test]
    fn event_cursor_snapshot_round_trips() {
        let snapshot = EventSnapshot {
            calendar_url: "https://dav.example.test/cal/".to_string(),
            sync_token: Some("token-1".to_string()),
            entries: vec![
                EventSnapshotEntry {
                    uri: "https://dav.example.test/cal/a.ics".to_string(),
                    etag: Some("a".to_string()),
                },
                EventSnapshotEntry {
                    uri: "https://dav.example.test/cal/b.ics".to_string(),
                    etag: None,
                },
            ],
            failed_hrefs: Vec::new(),
        };
        let cursor = cursor_from_snapshot(CursorScope::Type(ObjectType::CalendarEvent), &snapshot);

        let decoded = decode_cursor_snapshot(&cursor).expect("cursor should decode");

        assert_eq!(decoded, snapshot);
    }

    /// A v1 payload is byte-identical in shape to a v2 one, so the version
    /// gate has to reject it BEFORE the reader runs - a misparse would hand
    /// back base-relative ids that then surface as a wave of deletes plus
    /// creates. The recovery has to be `SchemaIncompatible` specifically, not
    /// a scope restart: only that directive deletes the backfill checkpoint,
    /// and without the re-walk the already-backfilled objects keep their old
    /// id spelling forever.
    #[test]
    fn event_cursor_rejects_the_base_relative_id_version() {
        let snapshot = EventSnapshot::default();
        let mut cursor =
            cursor_from_snapshot(CursorScope::Type(ObjectType::CalendarEvent), &snapshot);
        cursor.server_state.envelope_version = 1;

        let error = decode_cursor_snapshot(&cursor).expect_err("v1 cursors are disowned");
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
        assert!(matches!(
            error.recovery(),
            RecoveryClass::Engine(EngineDirective::SchemaIncompatible)
        ));
    }

    #[test]
    fn event_cursor_rejects_impossible_count_before_allocating() {
        let snapshot = EventSnapshot {
            calendar_url: "https://dav.example.test/cal/".to_string(),
            sync_token: None,
            entries: Vec::new(),
            failed_hrefs: Vec::new(),
        };
        let mut cursor =
            cursor_from_snapshot(CursorScope::Type(ObjectType::CalendarEvent), &snapshot);
        let count_offset = cursor.server_state.bytes.len() - 4;
        cursor.server_state.bytes[count_offset..].copy_from_slice(&u32::MAX.to_be_bytes());

        assert!(decode_cursor_snapshot(&cursor).is_err());
    }

    #[test]
    fn event_snapshot_diff_classifies_object_changes() {
        let previous = EventSnapshot {
            calendar_url: "cal".to_string(),
            sync_token: None,
            entries: vec![
                EventSnapshotEntry {
                    uri: "a.ics".to_string(),
                    etag: Some("old".to_string()),
                },
                EventSnapshotEntry {
                    uri: "b.ics".to_string(),
                    etag: Some("same".to_string()),
                },
                EventSnapshotEntry {
                    uri: "d.ics".to_string(),
                    etag: Some("gone".to_string()),
                },
            ],
            failed_hrefs: Vec::new(),
        };
        let current = EventSnapshot {
            calendar_url: "cal".to_string(),
            sync_token: None,
            entries: vec![
                EventSnapshotEntry {
                    uri: "a.ics".to_string(),
                    etag: Some("new".to_string()),
                },
                EventSnapshotEntry {
                    uri: "b.ics".to_string(),
                    etag: Some("same".to_string()),
                },
                EventSnapshotEntry {
                    uri: "c.ics".to_string(),
                    etag: Some("created".to_string()),
                },
            ],
            failed_hrefs: Vec::new(),
        };

        let changes = diff_event_snapshots(&previous, &current);
        let kinds = changes
            .into_iter()
            .map(|change| match change {
                Change::ObjectChange(change) => (change.id.0, change.kind),
                _ => panic!("unexpected scope change"),
            })
            .collect::<Vec<_>>();

        assert_eq!(
            kinds,
            vec![
                ("a.ics".to_string(), ObjectChangeKind::Updated),
                ("c.ics".to_string(), ObjectChangeKind::Created),
                ("d.ics".to_string(), ObjectChangeKind::Destroyed),
            ]
        );
    }

    fn event_snapshot_with(entries: &[&str]) -> EventSnapshot {
        EventSnapshot {
            calendar_url: "cal".to_string(),
            sync_token: None,
            entries: entries
                .iter()
                .map(|uri| EventSnapshotEntry {
                    uri: (*uri).to_string(),
                    etag: Some("e".to_string()),
                })
                .collect(),
            failed_hrefs: Vec::new(),
        }
    }

    #[test]
    fn diff_event_snapshots_suppresses_empty_207_mass_delete() {
        // Brick 6: a populated previous diffed against an empty current
        // yields zero changes (no Destroyed).
        let previous = event_snapshot_with(&["a.ics", "b.ics"]);
        let empty = event_snapshot_with(&[]);
        assert!(diff_event_snapshots(&previous, &empty).is_empty());
        assert!(diff_event_snapshots(&previous, &previous).is_empty());
        assert!(diff_event_snapshots(&empty, &empty).is_empty());
    }

    #[test]
    fn empty_poll_checkpoint_preserves_snapshot_for_the_recovery_poll() {
        let previous = event_snapshot_with(&["a.ics", "b.ics"]);
        let mut empty = event_snapshot_with(&[]);
        empty.sync_token = Some("refreshed".to_string());

        preserve_unobserved_event_entries(&previous, &mut empty);
        assert_eq!(empty.entries.len(), 2);
        assert_eq!(empty.sync_token.as_deref(), Some("refreshed"));

        let recovered = event_snapshot_with(&["a.ics", "b.ics"]);
        assert!(diff_event_snapshots(&empty, &recovered).is_empty());
    }

    #[test]
    fn event_page_emits_a_cursor_for_truncated_results() {
        let first = event_page(
            identified_events(&["a", "b", "c"]),
            0,
            Some(2),
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(event_ids(&first), vec!["a", "b"]);
        assert_eq!(first.next_cursor, Some(b"2".to_vec()));

        let second = event_page(
            identified_events(&["a", "b", "c"]),
            2,
            Some(2),
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(event_ids(&second), vec!["c"]);
        assert_eq!(second.next_cursor, None);
    }

    fn identified_events(ids: &[&str]) -> Vec<CalendarEvent> {
        ids.iter()
            .map(|id| {
                let mut event = event("2026-01-01T09:00:00Z", "2026-01-01T10:00:00Z", false);
                event.id = EventId((*id).to_string());
                event.native_id = (*id).to_string();
                event
            })
            .collect()
    }

    fn event_ids(page: &Page<CalendarEvent>) -> Vec<String> {
        page.items.iter().map(|event| event.id.0.clone()).collect()
    }

    /// The offset is local and every continuation re-runs the REPORT, so an
    /// unchanged result set returned in a DIFFERENT order across the two pages
    /// must still yield each event exactly once. Against unsorted slicing page
    /// two returns `a` again and `c` is never delivered at all.
    #[test]
    fn offset_pages_survive_a_reordered_second_report() {
        let first = event_page(
            identified_events(&["a", "b", "c"]),
            0,
            Some(2),
            Vec::new(),
            Vec::new(),
        );
        let first_ids = event_ids(&first);
        assert_eq!(first_ids, vec!["a", "b"]);
        let offset = String::from_utf8(first.next_cursor.expect("page one truncates"))
            .expect("ascii cursor")
            .parse::<usize>()
            .expect("numeric cursor");

        // Same three events, the order the server happened to answer with.
        let second = event_page(
            identified_events(&["c", "a", "b"]),
            offset,
            Some(2),
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(event_ids(&second), vec!["c"]);

        let mut delivered = first_ids;
        delivered.extend(event_ids(&second));
        delivered.sort();
        assert_eq!(delivered, vec!["a", "b", "c"]);
    }

    /// A zero limit must terminate. Emitting the current offset again gives a
    /// consumer that follows `next_cursor` an infinite non-advancing loop.
    #[test]
    fn a_zero_limit_is_an_exhausted_page_with_no_continuation() {
        let page = event_page(
            identified_events(&["a", "b", "c"]),
            0,
            Some(0),
            Vec::new(),
            Vec::new(),
        );
        assert!(page.items.is_empty());
        assert_eq!(page.next_cursor, None);
    }

    #[test]
    fn failed_uri_preserved_in_event_diff() {
        // Brick 7: a previous entry whose href is in current.failed_hrefs
        // is NOT emitted as Destroyed.
        let previous = event_snapshot_with(&["a.ics", "b.ics"]);
        let mut current = event_snapshot_with(&["a.ics"]);
        current.failed_hrefs = vec!["b.ics".to_string()];
        assert!(
            diff_event_snapshots(&previous, &current).is_empty(),
            "a transiently-failed resource must not be destroyed"
        );

        // Without the failed-href, b.ics's absence IS a destroy.
        let current_no_failed = event_snapshot_with(&["a.ics"]);
        let changes = diff_event_snapshots(&previous, &current_no_failed);
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            changes[0],
            Change::ObjectChange(ObjectChange {
                kind: ObjectChangeKind::Destroyed,
                ..
            })
        ));
    }

    #[test]
    fn sync_report_updates_snapshot_and_classifies_changes() {
        let mut snapshot = EventSnapshot {
            calendar_url: "https://dav.example.test/cal/".to_string(),
            sync_token: Some("token-1".to_string()),
            entries: vec![
                EventSnapshotEntry {
                    uri: "https://dav.example.test/cal/one.ics".to_string(),
                    etag: Some("old".to_string()),
                },
                EventSnapshotEntry {
                    uri: "https://dav.example.test/cal/two.ics".to_string(),
                    etag: Some("same".to_string()),
                },
            ],
            failed_hrefs: Vec::new(),
        };

        let changes = apply_sync_report(
            &mut snapshot,
            vec![
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/one.ics".to_string(),
                    etag: Some("new".to_string()),
                    status: Some(200),
                },
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/two.ics".to_string(),
                    etag: None,
                    status: Some(404),
                },
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/three.ics".to_string(),
                    etag: Some("created".to_string()),
                    status: Some(200),
                },
            ],
        );
        let kinds = changes
            .into_iter()
            .map(|change| match change {
                Change::ObjectChange(change) => (change.id.0, change.kind),
                _ => panic!("unexpected scope change"),
            })
            .collect::<Vec<_>>();

        assert_eq!(
            kinds,
            vec![
                (
                    "https://dav.example.test/cal/one.ics".to_string(),
                    ObjectChangeKind::Updated,
                ),
                (
                    "https://dav.example.test/cal/two.ics".to_string(),
                    ObjectChangeKind::Destroyed,
                ),
                (
                    "https://dav.example.test/cal/three.ics".to_string(),
                    ObjectChangeKind::Created,
                ),
            ]
        );
        assert_eq!(snapshot.entries.len(), 2);
    }

    /// A per-member status that is neither success nor a removal is the
    /// server declining to report on that member. Upserting it recorded an
    /// etag-less entry and emitted a change for a resource nobody observed.
    #[test]
    fn a_refused_sync_member_is_preserved_rather_than_upserted() {
        let mut snapshot = EventSnapshot {
            calendar_url: "https://dav.example.test/cal/".to_string(),
            sync_token: Some("token-1".to_string()),
            entries: vec![EventSnapshotEntry {
                uri: "https://dav.example.test/cal/one.ics".to_string(),
                etag: Some("kept".to_string()),
            }],
            failed_hrefs: Vec::new(),
        };

        let changes = apply_sync_report(
            &mut snapshot,
            vec![
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/one.ics".to_string(),
                    etag: None,
                    status: Some(403),
                },
                crate::parse::CalDavSyncEntry {
                    uri: "https://dav.example.test/cal/two.ics".to_string(),
                    etag: None,
                    status: Some(507),
                },
            ],
        );

        assert!(changes.is_empty(), "refusals are not changes: {changes:?}");
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].etag.as_deref(), Some("kept"));
    }

    /// RFC 6578 s3.6: a truncated result must be drained with the partial
    /// token it came with, or every change past the truncation point is lost
    /// until etag drift happens to surface it.
    #[tokio::test]
    async fn a_truncated_sync_report_is_drained_before_the_cursor_advances() {
        use bifrost_dav_core::DavResponse;
        use bifrost_dav_core::test_support::{dav_script, transcripts};
        use reqwest::StatusCode;
        use reqwest::header::HeaderMap;

        let multistatus = |body: &str| DavResponse {
            status: StatusCode::MULTI_STATUS,
            headers: HeaderMap::new(),
            body: body.to_string(),
            url: String::new(),
        };
        let script = dav_script([
            multistatus(
                "<D:multistatus xmlns:D=\"DAV:\"><D:response><D:href>/cal/</D:href><D:status>HTTP/1.1 507 Insufficient Storage</D:status></D:response><D:response><D:href>/cal/one.ics</D:href><D:propstat><D:prop><D:getetag>\"a\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response><D:sync-token>partial</D:sync-token></D:multistatus>",
            ),
            multistatus(
                "<D:multistatus xmlns:D=\"DAV:\"><D:response><D:href>/cal/two.ics</D:href><D:propstat><D:prop><D:getetag>\"b\"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response><D:sync-token>complete</D:sync-token></D:multistatus>",
            ),
        ]);
        let client =
            CalDavClient::with_account_net("https://dav.example.test", scripted_dav_net(&script));
        let previous = EventSnapshot {
            calendar_url: "https://dav.example.test/cal/".to_string(),
            sync_token: Some("start".to_string()),
            entries: Vec::new(),
            failed_hrefs: Vec::new(),
        };

        let (current, changes) = changes_from_cursor(&client, &previous)
            .await
            .expect("the scripted drain succeeds");

        let ids = changes
            .iter()
            .map(|change| match change {
                Change::ObjectChange(change) => change.id.0.clone(),
                _ => panic!("unexpected scope change"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                "https://dav.example.test/cal/one.ics".to_string(),
                "https://dav.example.test/cal/two.ics".to_string(),
            ],
            "the second REPORT's members must reach the consumer"
        );
        // The collection's own 507 response is a statement about the REPORT,
        // never a member: it must not become an entry or a phantom Created.
        assert_eq!(current.entries.len(), 2);
        assert!(
            current
                .entries
                .iter()
                .all(|entry| entry.uri.ends_with(".ics")),
            "the collection self-response leaked into the snapshot: {:?}",
            current.entries
        );
        assert_eq!(current.sync_token.as_deref(), Some("complete"));
        let requests = transcripts(&script);
        assert_eq!(requests.len(), 2, "the drain must re-issue the REPORT");
        assert!(requests[1].body.contains("partial"));
    }

    /// A consumer restating the calendar it read back, in the encoding it read
    /// it in, is not asking for a relocation. Byte comparison after a slash
    /// trim read it as one and issued a MOVE onto the collection the event
    /// already lives in, which `Overwrite: F` refuses with a 412 the consumer
    /// sees as a conflict.
    #[test]
    fn a_restated_calendar_url_is_not_a_relocation() {
        assert!(same_url(
            "https://dav.example.test/cal/My%20Cal/",
            "https://DAV.example.test:443/cal/My Cal"
        ));
        assert!(!same_url(
            "https://dav.example.test/cal/work/",
            "https://dav.example.test/cal/home/"
        ));
    }

    #[test]
    fn sync_report_404_for_unknown_href_emits_no_destroyed() {
        let mut snapshot = EventSnapshot {
            calendar_url: "https://dav.example.test/cal/".to_string(),
            sync_token: Some("token-1".to_string()),
            entries: vec![EventSnapshotEntry {
                uri: "https://dav.example.test/cal/one.ics".to_string(),
                etag: Some("e".to_string()),
            }],
            failed_hrefs: Vec::new(),
        };

        // A 404/410 entry for an href the snapshot never held (created and
        // deleted between polls) must not surface a phantom Destroyed.
        let changes = apply_sync_report(
            &mut snapshot,
            vec![crate::parse::CalDavSyncEntry {
                uri: "https://dav.example.test/cal/ghost.ics".to_string(),
                etag: None,
                status: Some(404),
            }],
        );

        assert!(changes.is_empty());
        assert_eq!(snapshot.entries.len(), 1);
    }
}
