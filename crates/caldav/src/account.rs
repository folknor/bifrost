use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::*;
use bytes::Bytes;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use futures::{StreamExt, stream};

use crate::capabilities::{caldav_capabilities, scheduling_available};
use crate::client::{CalDavClient, PutCondition, event_scope, unsupported_error};
use crate::ical::{
    create_to_ical, event_from_ical, events_from_ical, new_uid, patch_to_ical, rsvp_patch,
    rsvp_reply_ical,
};
use crate::parse::CalendarCollection;
use crate::{CalDavConfig, CalDavCredentials};

const CURSOR_ENVELOPE_VERSION: u32 = 1;
const CURSOR_MAGIC: &[u8] = b"CALDAVET1";

#[derive(Debug)]
pub(crate) struct CalDavAccount {
    client: Arc<CalDavClient>,
    capabilities: AccountCapabilities,
    calendar_home: String,
    default_calendar_url: String,
    rsvp_email: Option<String>,
    schedule_outbox_url: Option<String>,
}

impl CalDavAccount {
    pub(crate) async fn open(
        _account_id: AccountId,
        config: CalDavConfig,
    ) -> Result<Self, AccountError> {
        let client = CalDavClient::new(&config)?;
        let rsvp_email = match rsvp_email_from_config(&config) {
            Some(email) => Some(email),
            None => client.discover_calendar_user_email().await.ok().flatten(),
        };
        let schedule_outbox_url = client.discover_schedule_outbox_url().await.ok().flatten();
        // `event_rsvp` is discovery-derived, not assumed: the RSVP path
        // below hard-requires both of these and returns `unsupported`
        // without them, so advertising the method on a scheduling-less
        // RFC 4791 store would only move the failure from the
        // capability gate to the wire.
        let event_rsvp =
            scheduling_available(rsvp_email.as_deref(), schedule_outbox_url.as_deref());
        let calendar_home = client.discover_calendar_home().await?;
        let collections = client.list_calendars(&calendar_home).await?;
        let default_calendar_url = collections
            .first()
            .map(|collection| client.resolve_url(&collection.href))
            .unwrap_or_else(|| client.resolve_url(&calendar_home));
        Ok(Self {
            client: Arc::new(client),
            capabilities: caldav_capabilities(event_rsvp),
            calendar_home,
            default_calendar_url,
            rsvp_email,
            schedule_outbox_url,
        })
    }

    fn calendar_url(
        client: &CalDavClient,
        default_calendar_url: &str,
        calendar: Option<CalendarId>,
    ) -> String {
        if let Some(id) = calendar {
            return client.resolve_url(&id.0);
        }
        default_calendar_url.to_string()
    }

    fn map_calendar(client: &CalDavClient, collection: CalendarCollection) -> Calendar {
        let native = client.resolve_url(&collection.href);
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

    async fn fetch_event_from_url(
        client: Arc<CalDavClient>,
        default_calendar_url: String,
        calendar: Option<CalendarId>,
        event: EventId,
        operation: AccountOperation,
    ) -> Result<CalendarEvent, AccountError> {
        let calendar_url = Self::calendar_url(&client, &default_calendar_url, calendar);
        let url = client.resolve_url(&event.0);
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
        .map_err(|_| {
            crate::client::local_error(operation, "CalDAV resource is not valid iCalendar")
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
                .find(|collection| same_url(&client.resolve_url(&collection.href), calendar))
                .and_then(|collection| collection.sync_token),
            None => None,
        };
        let listing = client.list_events_listing(calendar, operation).await?;
        let mut entries = listing
            .entries
            .into_iter()
            .map(|entry| EventSnapshotEntry {
                uri: client.resolve_url(&entry.uri),
                etag: entry.etag,
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.uri.cmp(&right.uri));
        let failed_hrefs = listing
            .failed_hrefs
            .into_iter()
            .map(|href| client.resolve_url(&href))
            .collect();
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

    fn set_priority(&self, _priority: Priority) {}

    fn set_bandwidth_cap(&self, _bps: Option<u64>) {}

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        CursorDescriptor {
            cost_class: CostClass::Medium,
            strategy: SyncStrategy::ServerCursor,
            freshness: None,
        }
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        Box::pin(stream::iter([
            SyncEvent::Batch(Batch {
                items: vec![CursorScope::Type(ObjectType::CalendarEvent)],
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
        let calendar = self.default_calendar_url.clone();
        Box::pin(async move {
            validate_event_scope(&scope, AccountOperation::EstablishCursor)?;
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

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<SyncEvent<InventoryEntry>> {
        let client = Arc::clone(&self.client);
        let home = self.calendar_home.clone();
        let calendar = self.default_calendar_url.clone();
        Box::pin(
            stream::once(async move {
                let mut events = Vec::new();
                if let Err(error) = validate_event_scope(&scope, AccountOperation::SyncInventory) {
                    events.push(SyncEvent::Terminated(error));
                    return events;
                }
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
            .flat_map(stream::iter),
        )
    }

    fn inventory_partitioning(&self, _scope: &CursorScope) -> InventoryPartitioning {
        InventoryPartitioning::Full
    }

    fn inventory_partition_stream(
        &self,
        scope: CursorScope,
        partition: InventoryPartition,
    ) -> AccountStream<SyncEvent<InventoryEntry>> {
        match partition {
            InventoryPartition::Full => self.inventory_stream(scope),
            _ => unsupported_stream(AccountOperation::SyncInventory),
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
    ) -> AccountFuture<Result<SubscriptionHandle, AccountError>> {
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
        _op: FlagOp,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        unsupported_stream(AccountOperation::UpdateFlags)
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
                .map(|collection| Self::map_calendar(&client, collection))
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
            let calendar_url = client.resolve_url(&range.calendar_id.0);
            let range_start = caldav_query_time(&range.start);
            let range_end = caldav_query_time(&range.end);
            let fetched = client
                .query_events_in_range(&calendar_url, range_start.as_deref(), range_end.as_deref())
                .await?;
            let mut events = Vec::new();
            // Per-resource failures are surfaced (not swallowed) so a
            // consumer can tell a transient failure apart from a real remote
            // deletion. Two kinds land here and they are the same thing to
            // the consumer: a resource the server refused inside the 207
            // (non-2xx propstat), and one that came back 200 but would not
            // tokenize. Neither aborts the rest of the pull.
            let mut failed = fetched
                .failed_hrefs()
                .into_iter()
                .map(|href| client.resolve_url(&href))
                .collect::<Vec<_>>();
            let mut materialized = HashSet::new();
            for event in fetched.events {
                let uri = client.resolve_url(&event.uri);
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
            if let Some(limit) = range.limit.and_then(|limit| usize::try_from(limit).ok()) {
                events.truncate(limit);
            }
            Ok(Page {
                items: events,
                next_cursor: None,
                estimated_total: None,
                failed_ids: failed,
                skipped_scopes: Vec::new(),
            })
        })
    }

    fn event_get(&self, event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_calendar_url = self.default_calendar_url.clone();
        Box::pin(async move {
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
                &default_calendar_url,
                Some(event.calendar_id.clone()),
            );
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
            let url = client.resolve_url(&event.0);
            client
                .put_event(
                    &url,
                    body,
                    put_condition(current.etag.as_deref()),
                    AccountOperation::EventUpdate,
                )
                .await?;
            Ok(())
        })
    }

    fn event_delete(&self, event: EventId) -> AccountFuture<Result<(), AccountError>> {
        let client = Arc::clone(&self.client);
        Box::pin(async move {
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
            let current = Self::fetch_event_from_url(
                Arc::clone(&client),
                default_calendar_url,
                None,
                event.clone(),
                AccountOperation::EventRsvp,
            )
            .await?;
            let Some(rsvp_email) = rsvp_email else {
                return Err(unsupported_error(AccountOperation::EventRsvp));
            };
            let Some(schedule_outbox_url) = schedule_outbox_url else {
                return Err(unsupported_error(AccountOperation::EventRsvp));
            };
            let reply = rsvp_reply_ical(&current, status, &rsvp_email)
                .map_err(|_| unsupported_error(AccountOperation::EventRsvp))?;
            let organizer_email = current
                .organizer
                .as_ref()
                .map(|organizer| organizer.email.clone())
                .ok_or_else(|| unsupported_error(AccountOperation::EventRsvp))?;
            client
                .post_schedule_reply(&schedule_outbox_url, &rsvp_email, &organizer_email, reply)
                .await?;
            let patch = rsvp_patch(&current, status, &rsvp_email)
                .map_err(|_| unsupported_error(AccountOperation::EventRsvp))?;
            let body = patch_to_ical(&current, &patch)
                .map_err(|_| unsupported_error(AccountOperation::EventRsvp))?;
            let url = client.resolve_url(&event.0);
            client
                .put_event(
                    &url,
                    body,
                    put_condition(current.etag.as_deref()),
                    AccountOperation::EventRsvp,
                )
                .await?;
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
            let calendar_url =
                Self::calendar_url(&client, &default_calendar_url, request.calendar_id);
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
            let mut failed = fetched
                .failed_hrefs()
                .into_iter()
                .map(|href| client.resolve_url(&href))
                .collect::<Vec<_>>();
            failed.sort_unstable();
            failed.dedup();
            let mut events = Vec::new();
            let mut materialized = HashSet::new();
            for event in fetched
                .events
                .into_iter()
                .filter(|event| seen.insert(event.uri.clone()))
            {
                let uri = client.resolve_url(&event.uri);
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
            if let Some(limit) = request.limit.and_then(|limit| usize::try_from(limit).ok()) {
                events.truncate(limit);
            }
            Ok(Page {
                items: events,
                next_cursor: None,
                estimated_total: None,
                failed_ids: failed,
                skipped_scopes,
            })
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

fn append_path(base: &str, path: &str) -> String {
    if base.ends_with('/') {
        format!("{base}{path}")
    } else {
        format!("{base}/{path}")
    }
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
                id: calendar_url.to_string(),
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

fn same_url(left: &str, right: &str) -> bool {
    left.trim_end_matches('/') == right.trim_end_matches('/')
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

fn validate_event_scope(
    scope: &CursorScope,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if matches!(scope, CursorScope::Type(ObjectType::CalendarEvent)) {
        Ok(())
    } else {
        Err(crate::client::local_error(
            operation,
            "CalDAV only supports calendar-event cursor scopes",
        ))
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
        envelope_version: CURSOR_ENVELOPE_VERSION,
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
        let report = client
            .sync_events(&previous.calendar_url, sync_token)
            .await?;
        let mut current = previous.clone();
        current.sync_token = report.sync_token.or_else(|| previous.sync_token.clone());
        let changes = apply_sync_report(client, &mut current, report.entries);
        return Ok((current, changes));
    }
    let current = CalDavAccount::event_snapshot(
        client,
        None,
        &previous.calendar_url,
        AccountOperation::SyncChanges,
    )
    .await?;
    let changes = diff_event_snapshots(previous, &current);
    Ok((current, changes))
}

fn apply_sync_report(
    client: &CalDavClient,
    current: &mut EventSnapshot,
    entries: Vec<crate::parse::CalDavSyncEntry>,
) -> Vec<Change> {
    let mut changes = Vec::new();
    for entry in entries {
        let uri = client.resolve_url(&entry.uri);
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
            flags_hash: 0,
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
        return event_start <= range_end && event_end >= range_start;
    }
    // Recurring master: its own interval can sit entirely before the
    // window while a later occurrence lands inside it. The server's
    // time-range REPORT already expanded the RRULE and returned this
    // resource because an occurrence overlaps, so the local guard must
    // not drop it. Occurrences only run forward from the master start, so
    // the series can reach the window unless it begins after the window
    // ends, or provably ends (RRULE UNTIL) before the window starts.
    if event_start > range_end {
        return false;
    }
    if event_end >= range_start {
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
fn rrule_until(rrule: &str) -> Option<DateTime<FixedOffset>> {
    let value = rrule.split(';').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        key.eq_ignore_ascii_case("UNTIL").then_some(value)
    })?;
    parse_ical_instant(value.trim())
}

fn parse_ical_instant(value: &str) -> Option<DateTime<FixedOffset>> {
    let zero = FixedOffset::east_opt(0)?;
    if value.len() == 8 {
        let date = NaiveDate::parse_from_str(value, "%Y%m%d").ok()?;
        return zero
            .from_local_datetime(&date.and_time(NaiveTime::MIN))
            .single();
    }
    let core = value.trim_end_matches('Z');
    let naive = NaiveDateTime::parse_from_str(core, "%Y%m%dT%H%M%S").ok()?;
    zero.from_local_datetime(&naive).single()
}

fn time_interval(
    start: &EventTime,
    end: &EventTime,
    is_all_day: bool,
) -> Option<(DateTime<FixedOffset>, DateTime<FixedOffset>)> {
    let start = comparable_time(start, is_all_day)?;
    let end = comparable_time(end, is_all_day).unwrap_or(start);
    Some((start, end))
}

fn comparable_time(time: &EventTime, is_all_day: bool) -> Option<DateTime<FixedOffset>> {
    if is_all_day || time.value.len() == 10 {
        let date = NaiveDate::parse_from_str(&time.value, "%Y-%m-%d").ok()?;
        let naive = date.and_time(NaiveTime::MIN);
        return FixedOffset::east_opt(0)?
            .from_local_datetime(&naive)
            .single();
    }
    DateTime::parse_from_rfc3339(&time.value).ok()
}

fn caldav_query_time(time: &EventTime) -> Option<String> {
    if time.value.len() == 10 {
        let date = NaiveDate::parse_from_str(&time.value, "%Y-%m-%d").ok()?;
        let naive = date.and_time(NaiveTime::MIN);
        let utc = Utc.from_local_datetime(&naive).single()?;
        return Some(utc.format("%Y%m%dT%H%M%SZ").to_string());
    }
    DateTime::parse_from_rfc3339(&time.value).ok().map(|time| {
        time.with_timezone(&Utc)
            .format("%Y%m%dT%H%M%SZ")
            .to_string()
    })
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
                id: "https://dav.example.test/cal/".to_string(),
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
        let event = event("2026-06-02", "2026-06-02", true);

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
        assert_eq!(until.to_rfc3339(), "2026-05-25T00:00:00+00:00");

        let until = rrule_until("FREQ=DAILY;UNTIL=20260525T120000").expect("datetime UNTIL");
        assert_eq!(until.to_rfc3339(), "2026-05-25T12:00:00+00:00");

        let until = rrule_until("FREQ=DAILY;UNTIL=20260525T120000Z").expect("UTC UNTIL");
        assert_eq!(until.to_rfc3339(), "2026-05-25T12:00:00+00:00");

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
        let client = CalDavClient::new(&CalDavConfig {
            base_url: "https://dav.example.test".to_string(),
            credentials: CalDavCredentials::bearer("token"),
        })
        .expect("client");
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
            &client,
            &mut snapshot,
            vec![
                crate::parse::CalDavSyncEntry {
                    uri: "/cal/one.ics".to_string(),
                    etag: Some("new".to_string()),
                    status: Some(200),
                },
                crate::parse::CalDavSyncEntry {
                    uri: "/cal/two.ics".to_string(),
                    etag: None,
                    status: Some(404),
                },
                crate::parse::CalDavSyncEntry {
                    uri: "/cal/three.ics".to_string(),
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

    #[test]
    fn sync_report_404_for_unknown_href_emits_no_destroyed() {
        let client = CalDavClient::new(&CalDavConfig {
            base_url: "https://dav.example.test".to_string(),
            credentials: CalDavCredentials::bearer("token"),
        })
        .expect("client");
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
            &client,
            &mut snapshot,
            vec![crate::parse::CalDavSyncEntry {
                uri: "/cal/ghost.ics".to_string(),
                etag: None,
                status: Some(404),
            }],
        );

        assert!(changes.is_empty());
        assert_eq!(snapshot.entries.len(), 1);
    }
}
