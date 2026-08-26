use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFuture, AccountId, AccountOperation,
    AccountStream, AddressBook, AddressBookId, AttachmentHandle, BlobHandle, ByteRange, Calendar,
    CalendarEvent, Change, ChangeCursor, Checkpoint, CloudUploadMeta, ContactCard, ContactCorpus,
    ContactCreate, ContactId, ContactPatch, ContactProvenance, ContactSearchRequest, ContainerId,
    ContainerKind, ContainerList, CostClass, CursorDescriptor, CursorEstablishment, CursorScope,
    DirectoryCard, DirectoryGroup, DirectoryGroupId, DirectoryGroupMember, DraftHandle, DraftPatch,
    ErrorScope, EventCreate, EventId, EventPatch, EventRange, EventSearchRequest, FilterValidation,
    FlagOp, HostedAttachment, HydratedObject, HydrationProjection, IdempotencyKey, Identity,
    IdentityId, IdentityPatch, Importance, InventoryEntry, InventoryEvent, InventoryPartition,
    InventoryPartitioning, ItemOutcome, MembershipScope, Message, MutationSuccess, MutationTarget,
    ObjectChange, ObjectChangeKind, ObjectId, ObjectType, OpaqueChangeState, Page, PageBoundary,
    Priority, ProtocolKind, QuotaInfo, RsvpStatus, SearchRequest, SendRequest, ServerFilter,
    ServerFilterCreate, ServerFilterId, ServerFilterPatch, ServerVersion, SkippedScope,
    SubscriptionHandle, SyncEvent, SyncStrategy, ThreadHydration, ThreadId, VacationConfig,
    WatchEvent,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use uuid::Uuid;

use crate::CardDavConfig;
use crate::capabilities::carddav_capabilities;
use crate::client::{
    CardDavClient, PutCondition, local_error, not_found_error, parse_error, unsupported_error,
    unsupported_scope_error,
};
use crate::parse::{AddressBookCollection, CardDavFetchedVCard, CardDavMultigetReport};
use crate::vcard::{VCardParseError, contact_from_vcard, vcard_from_create, vcard_from_patch};

const CONTACT_PAGE_SIZE: usize = 250;
// Version 2 changes snapshot ids from base-URL-relative to request-URI-relative.
const CURSOR_ENVELOPE_VERSION: u32 = 2;
const CURSOR_MAGIC: &[u8] = b"CDAVCTAG1";

#[derive(Debug)]
pub(crate) struct CardDavAccount {
    client: Arc<CardDavClient>,
    capabilities: AccountCapabilities,
    addressbook_home: String,
    default_addressbook_url: String,
    /// Address books discovery found and the cursor lanes do NOT cover.
    ///
    /// Same shape as bifrost-caldav's `unsynced_calendar_urls`, and kept in
    /// step with it deliberately: the cursor model is one scope for the whole
    /// account (`CursorScope::Type(Contact)`) and all three sync lanes read
    /// `default_addressbook_url`, which is just `collections.first()`. An
    /// account with three address books syncs one. The contact primitives are
    /// unaffected - they route through `addressbook_url` with the caller's
    /// `address_book_id` - so direct API access reaches every book while
    /// inventory and changes cover one.
    ///
    /// `address_books_list` enumerates all of them, so without
    /// `open_skipped_scopes` a consumer sees a complete account and silently
    /// receives changes for one book.
    unsynced_addressbook_urls: Vec<String>,
}

impl CardDavAccount {
    /// Build an account directly around a client, skipping discovery.
    ///
    /// Twin of `CalDavAccount::for_tests`: discovery is several round trips of
    /// its own and would dominate a test about what one call does.
    #[cfg(test)]
    pub(crate) fn for_tests(client: Arc<CardDavClient>, addressbook_home: &str) -> Self {
        Self {
            client,
            capabilities: carddav_capabilities(),
            addressbook_home: addressbook_home.to_string(),
            default_addressbook_url: addressbook_home.to_string(),
            unsynced_addressbook_urls: Vec::new(),
        }
    }

    pub(crate) async fn open(
        _account_id: AccountId,
        config: CardDavConfig,
    ) -> Result<Self, AccountError> {
        let mut client = CardDavClient::new(&config)?;
        let addressbook_home = client.discover_addressbook_home().await?;
        client.admit_discovered_urls(std::iter::once(addressbook_home.clone()));
        let collections = client.list_addressbooks(&addressbook_home).await?;
        let default_addressbook_url = collections
            .first()
            .map(|collection| collection.href.clone())
            .unwrap_or_else(|| client.resolve_url(&addressbook_home));
        // Every collection past the first is enumerated but NOT synced - see
        // `unsynced_addressbook_urls`. Recorded at open so the gap is
        // reportable rather than invisible.
        let unsynced_addressbook_urls = collections
            .iter()
            .skip(1)
            .map(|collection| collection.href.clone())
            .collect();
        Ok(Self {
            client: Arc::new(client),
            capabilities: carddav_capabilities(),
            addressbook_home,
            default_addressbook_url,
            unsynced_addressbook_urls,
        })
    }

    /// One `SkippedScope` per address book the cursor lanes do not cover.
    ///
    /// Empty for the common single-book account. `Unsupported(...)` because it
    /// is a standing limitation of this crate's cursor model, not a transient
    /// failure: no reopen or retry heals it. Mirrors
    /// `CalDavAccount::open_skipped_scopes`.
    pub(crate) fn open_skipped_scopes(&self) -> Vec<SkippedScope> {
        self.unsynced_addressbook_urls
            .iter()
            .map(|url| SkippedScope {
                scope: ErrorScope::Contact {
                    id: (url.clone()).into(),
                },
                error: unsupported_scope_error(
                    AccountOperation::DiscoverCursorScopes,
                    ErrorScope::Contact {
                        id: (url.clone()).into(),
                    },
                    "bifrost-carddav syncs only the first discovered address \
                     book collection; this address book is reachable through \
                     the contact primitives but produces no inventory or \
                     change events",
                ),
            })
            .collect()
    }

    fn addressbook_url(
        client: &CardDavClient,
        default_addressbook_url: &str,
        address_book: Option<AddressBookId>,
    ) -> String {
        if let Some(id) = address_book {
            return client.resolve_url(&id.0);
        }
        default_addressbook_url.to_string()
    }

    fn map_addressbook(collection: AddressBookCollection) -> AddressBook {
        let native = collection.href;
        let name = collection
            .display_name
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| "Address Book".to_string());
        AddressBook {
            id: AddressBookId(native.clone()),
            native_id: native.clone(),
            name,
            provenance: ContactProvenance {
                provider: ProtocolKind::CardDav,
                native,
                address_book_native: None,
            },
            corpus: ContactCorpus::Main,
            is_default: false,
            can_create_contacts: true,
            can_update_contacts: true,
            can_delete_contacts: true,
        }
    }

    async fn fetch_contact_from_url(
        client: Arc<CardDavClient>,
        default_addressbook_url: String,
        address_book: Option<AddressBookId>,
        contact: ContactId,
        operation: AccountOperation,
    ) -> Result<ContactCard, AccountError> {
        let addressbook = if let Some(address_book) = address_book {
            client.resolve_url(&address_book.0)
        } else {
            contact_addressbook_url(&client, &contact).unwrap_or(default_addressbook_url)
        };
        let card = Self::fetch_contact_resource(&client, &addressbook, &contact, operation).await?;
        contact_from_vcard(
            card.uri,
            Some(AddressBookId(addressbook)),
            card.etag,
            &card.data,
        )
        .map_err(|error| project_error(operation, &error))
    }

    async fn fetch_contact_resource(
        client: &CardDavClient,
        addressbook: &str,
        contact: &ContactId,
        operation: AccountOperation,
    ) -> Result<CardDavFetchedVCard, AccountError> {
        let report = client
            .fetch_vcards(addressbook, std::slice::from_ref(&contact.0), operation)
            .await?
            .report;
        if report
            .missing_data
            .iter()
            .any(|href| href == &client.resolve_url(&contact.0))
        {
            return Err(parse_error(
                operation,
                "CardDAV multiget response omitted address-data",
            ));
        }
        let (cards, _) = resolved_report(report);
        cards
            .into_iter()
            .next()
            .ok_or_else(|| not_found_error(operation, contact.0.clone()))
    }

    async fn hydrated_contacts(
        client: &CardDavClient,
        default_addressbook_url: &str,
        address_book: Option<AddressBookId>,
        operation: AccountOperation,
    ) -> Result<SearchedContacts, AccountError> {
        let addressbook = Self::addressbook_url(client, default_addressbook_url, address_book);
        let listing = client
            .list_contacts_listing(&addressbook, operation)
            .await?;
        let uris = listing
            .entries
            .iter()
            .map(|entry| entry.uri.clone())
            .collect::<Vec<_>>();
        let fetch = client.fetch_vcards(&addressbook, &uris, operation).await?;
        let skipped_scopes = skipped_addressbook_scope(fetch.degraded);
        let (fetched, mut failed_ids) = resolved_report(fetch.report);
        merge_listing_failures(&mut failed_ids, listing.failed_hrefs);
        let (cards, projection_failures) = partition_hydrated_vcards(&addressbook, fetched);
        failed_ids.extend(projection_failures);
        one_outcome_per_id(&mut failed_ids, &cards);
        Ok(SearchedContacts {
            cards,
            failed_ids,
            skipped_scopes,
        })
    }

    async fn searched_contacts(
        client: &CardDavClient,
        default_addressbook_url: &str,
        address_book: Option<AddressBookId>,
        query: &str,
    ) -> Result<SearchedContacts, AccountError> {
        let addressbook = Self::addressbook_url(client, default_addressbook_url, address_book);
        let mut seen = HashSet::new();
        let fetch = client.query_vcards_text(&addressbook, query).await?;
        let skipped_scopes = skipped_addressbook_scope(fetch.degraded);
        let (fetched, mut failed_ids) = resolved_report(fetch.report);
        let fetched = fetched
            .into_iter()
            .filter(|card| seen.insert(card.uri.clone()))
            .collect();
        let (cards, projection_failures) = partition_hydrated_vcards(&addressbook, fetched);
        failed_ids.extend(projection_failures);
        one_outcome_per_id(&mut failed_ids, &cards);
        Ok(SearchedContacts {
            cards,
            failed_ids,
            skipped_scopes,
        })
    }

    async fn hydrated_contacts_page(
        client: &CardDavClient,
        default_addressbook_url: &str,
        address_book: Option<AddressBookId>,
        offset: usize,
        page_size: usize,
        operation: AccountOperation,
    ) -> Result<Page<ContactCard>, AccountError> {
        let addressbook = Self::addressbook_url(client, default_addressbook_url, address_book);
        let listing = client
            .list_contacts_listing(&addressbook, operation)
            .await?;
        let total = listing.entries.len();
        // The offset is local and each page re-runs the depth-1 PROPFIND. DAV
        // guarantees no ordering on a multistatus, so paging raw response
        // order would let an unchanged collection come back permuted between
        // pages, skipping the contacts the permutation moved behind the offset
        // and serving twice the ones it moved past. The resolved href is the
        // stable key that makes the offset mean the same thing on every page.
        let mut entries = listing.entries;
        entries.sort_by(|left, right| left.uri.cmp(&right.uri));
        let uris = entries
            .into_iter()
            .skip(offset)
            .take(page_size)
            .map(|entry| entry.uri)
            .collect::<Vec<_>>();
        let fetch = client.fetch_vcards(&addressbook, &uris, operation).await?;
        let skipped_scopes = skipped_addressbook_scope(fetch.degraded);
        let (fetched, mut failed_ids) = resolved_report(fetch.report);
        merge_listing_failures(&mut failed_ids, listing.failed_hrefs);
        let (cards, projection_failures) = partition_hydrated_vcards(&addressbook, fetched);
        failed_ids.extend(projection_failures);
        one_outcome_per_id(&mut failed_ids, &cards);
        Ok(Page {
            items: cards,
            next_cursor: (offset + page_size < total)
                .then(|| (offset + page_size).to_string().into_bytes()),
            estimated_total: Some(estimated_total(total)),
            failed_ids,
            skipped_scopes,
        })
    }

    /// Snapshot one address book's contact listing plus its ctag.
    ///
    /// `ctag` says where the collection tag comes from, because the callers
    /// differ in what they already know:
    ///
    /// - [`CtagSource::Home`] runs the depth-1 PROPFIND over the address book
    ///   home and picks this collection out of it. Cursor establishment and
    ///   inventory use it: they have no prior cursor to compare against.
    /// - [`CtagSource::Known`] takes a value the caller already has. The poll
    ///   path uses it, because deciding whether to short-circuit at all
    ///   required asking for the ctag first - re-deriving it from a depth-1
    ///   listing of the whole home made a changed-ctag poll cost three round
    ///   trips where two suffice, and that listing grows with the number of
    ///   address books rather than staying one collection wide.
    pub(crate) async fn contact_snapshot(
        client: &CardDavClient,
        ctag: CtagSource<'_>,
        addressbook: &str,
        operation: AccountOperation,
    ) -> Result<ContactSnapshot, AccountError> {
        let ctag = match ctag {
            CtagSource::Home(home) => client
                .list_addressbooks_for_operation(home, operation)
                .await?
                .into_iter()
                .find(|collection| same_collection_url(&collection.href, addressbook))
                .and_then(|collection| collection.ctag),
            CtagSource::Known(ctag) => ctag,
        };
        let listing = client.list_contacts_listing(addressbook, operation).await?;
        let mut entries = listing
            .entries
            .into_iter()
            .map(|entry| ContactSnapshotEntry {
                uri: entry.uri,
                etag: entry.etag,
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.uri.cmp(&right.uri));
        let failed_hrefs = listing.failed_hrefs;
        Ok(ContactSnapshot {
            addressbook_url: addressbook.to_string(),
            ctag,
            entries,
            failed_hrefs,
        })
    }
}

/// Where [`CardDavAccount::contact_snapshot`] gets a collection's ctag.
pub(crate) enum CtagSource<'a> {
    /// Pick it out of a depth-1 PROPFIND over the address book home.
    Home(&'a str),
    /// Use a value the caller already resolved, spending no request.
    Known(Option<String>),
}

impl Account for CardDavAccount {
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
            SyncEvent::Batch(bifrost_types::Batch {
                items: vec![CursorScope::Type(ObjectType::Contact)],
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

    fn scope_lifecycle_stream(&self) -> AccountStream<bifrost_types::ScopeLifecycleEvent> {
        Box::pin(stream::empty())
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        let client = Arc::clone(&self.client);
        let home = self.addressbook_home.clone();
        let addressbook = self.default_addressbook_url.clone();
        Box::pin(async move {
            validate_contact_scope(&scope, AccountOperation::EstablishCursor)?;
            let snapshot = Self::contact_snapshot(
                &client,
                CtagSource::Home(&home),
                &addressbook,
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
        let home = self.addressbook_home.clone();
        let addressbook = self.default_addressbook_url.clone();
        let coverage_scope = scope.clone();
        // COMPLETE coverage is accurate here: the walk terminates wholesale on
        // any failure, so it never advances a checkpoint across a gap.
        Box::pin(
            stream::once(async move {
                let mut events = Vec::new();
                if let Err(error) = validate_contact_scope(&scope, AccountOperation::SyncInventory)
                {
                    events.push(SyncEvent::Terminated(error));
                    return events;
                }
                let started = Instant::now();
                let snapshot = match Self::contact_snapshot(
                    &client,
                    CtagSource::Home(&home),
                    &addressbook,
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
                events.push(SyncEvent::Batch(bifrost_types::Batch {
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
            .map(bifrost_types::lift_complete_walk(
                bifrost_types::CoverageDomain::full(coverage_scope),
            )),
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
        _projection: bifrost_types::Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        unsupported_stream(AccountOperation::Hydrate)
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        let client = Arc::clone(&self.client);
        // No address book home is captured: the poll path resolves its ctag
        // against the collection itself, never by re-listing the home.
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
                // ctag short-circuit (brick 8): when the prior cursor
                // carries a ctag and a cheap depth-0 getctag PROPFIND
                // shows the collection unchanged, skip the full depth-1
                // PROPFIND + diff and carry the cursor forward with no
                // changes. Mirrors CalDAV's sync-token short-circuit.
                //
                // The ctag is resolved AT MOST ONCE per poll, and the value is
                // carried into the snapshot below rather than re-derived. It
                // used to be fetched here and then recovered a second time from
                // a depth-1 listing of the whole address book home - three
                // round trips for a changed collection, one of them scaling
                // with the number of address books.
                // Resolved unconditionally, not only when the prior cursor
                // carried one. A cursor with no ctag previously had to recover
                // one from the depth-1 home listing, which is both dearer and
                // likelier to come back empty (it finds nothing when the
                // collection does not appear in its own home). Asking the
                // collection directly seeds the ctag for the next poll, so a
                // cursor that starts without one is not stuck without one.
                let current_ctag = match client
                    .collection_ctag(&previous.addressbook_url, AccountOperation::SyncChanges)
                    .await
                {
                    Ok(ctag) => ctag,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(error));
                        return events;
                    }
                };
                if let (Some(prev_ctag), Some(current_ctag)) =
                    (previous.ctag.as_deref(), current_ctag.as_deref())
                    && prev_ctag == current_ctag
                {
                    let checkpoint = cursor_from_snapshot(cursor.scope, &previous);
                    events.push(SyncEvent::Batch(bifrost_types::Batch {
                        items: Vec::new(),
                        page_boundary: PageBoundary::Final,
                        server_latency: started.elapsed(),
                        bytes_in: 0,
                        checkpoint: Some(Checkpoint::Change(checkpoint.clone())),
                    }));
                    events.push(SyncEvent::Done(Some(Checkpoint::Change(checkpoint))));
                    return events;
                }
                // ctag changed, server omits getctag, or the prior cursor
                // carried none: fall through to the full snapshot + diff
                // (which still runs brick 6's empty-207 guard). `Known`
                // spends no request - it reuses whatever the check above
                // resolved, including `None`, which is exactly the value the
                // old depth-1 path would have produced for a server that does
                // not publish getctag.
                let mut current = match Self::contact_snapshot(
                    &client,
                    CtagSource::Known(current_ctag),
                    &previous.addressbook_url,
                    AccountOperation::SyncChanges,
                )
                .await
                {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        events.push(SyncEvent::Terminated(error));
                        return events;
                    }
                };
                preserve_unobserved_contact_entries(&previous, &mut current);
                let checkpoint = cursor_from_snapshot(cursor.scope, &current);
                let changes = diff_contact_snapshots(&previous, &current);
                events.push(SyncEvent::Batch(bifrost_types::Batch {
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
        match op.validate_for_account(bifrost_types::Protocol::CardDav) {
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
        _property_id: String,
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
        let client = Arc::clone(&self.client);
        let home = self.addressbook_home.clone();
        Box::pin(async move {
            let collections = client.list_addressbooks(&home).await?;
            let mut books = collections
                .into_iter()
                .map(Self::map_addressbook)
                .collect::<Vec<_>>();
            // An address book home that enumerates zero addressbook
            // collections surfaces as an empty list, not a fabricated
            // placeholder. The depth-1 PROPFIND above returns the home's own
            // response too, so a home that is itself an addressbook collection
            // is already mapped by the parse path. An empty result here
            // therefore means a genuinely empty backend; reporting it as empty
            // lets a consumer reap stale address books rather than chase a
            // phantom home-book whose queries a spec-correct server 404s.
            //
            // The phantom this replaces also advertised `can_create_contacts:
            // true`, so a consumer that trusted it and POSTed a vCard to the
            // home URL got a 404 or 405 it had done nothing to deserve.
            // `bifrost-caldav::calendars_list` removed the identical shape for
            // the identical reasons; the two must not drift apart again.
            if let Some(first) = books.first_mut() {
                first.is_default = true;
            }
            Ok(books)
        })
    }

    fn contacts_list(
        &self,
        address_book: Option<AddressBookId>,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_addressbook_url = self.default_addressbook_url.clone();
        Box::pin(async move {
            let offset = decode_offset_cursor(page_cursor, AccountOperation::ContactsList)?;
            Self::hydrated_contacts_page(
                &client,
                &default_addressbook_url,
                address_book,
                offset,
                CONTACT_PAGE_SIZE,
                AccountOperation::ContactsList,
            )
            .await
        })
    }

    fn contact_get(&self, contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_addressbook_url = self.default_addressbook_url.clone();
        Box::pin(async move {
            Self::fetch_contact_from_url(
                client,
                default_addressbook_url,
                None,
                contact,
                AccountOperation::ContactGet,
            )
            .await
        })
    }

    fn contact_create(
        &self,
        contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_addressbook_url = self.default_addressbook_url.clone();
        Box::pin(async move {
            let addressbook = Self::addressbook_url(
                &client,
                &default_addressbook_url,
                contact.address_book_id.clone(),
            );
            let id = format!("{}.vcf", Uuid::new_v4());
            let url = append_path(&addressbook, &id);
            let data = vcard_from_create(&contact, &id);
            client
                .put_vcard(
                    &url,
                    data,
                    PutCondition::IfNoneMatch,
                    AccountOperation::ContactCreate,
                )
                .await?;
            Ok(ContactId(url))
        })
    }

    fn contact_update(
        &self,
        contact: ContactId,
        patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let client = Arc::clone(&self.client);
        let default_addressbook_url = self.default_addressbook_url.clone();
        Box::pin(async move {
            let addressbook = contact_addressbook_url(&client, &contact).unwrap_or(
                Self::addressbook_url(&client, &default_addressbook_url, None),
            );
            if let Some(target) = patch.address_book_id.as_ref() {
                let target = client.resolve_url(&target.0);
                if !same_collection_url(&target, &addressbook) {
                    return Err(local_error(
                        AccountOperation::ContactUpdate,
                        "CardDAV contact_update cannot move contacts between address books",
                    ));
                }
            }
            let raw = Self::fetch_contact_resource(
                &client,
                &addressbook,
                &contact,
                AccountOperation::ContactUpdate,
            )
            .await?;
            let current = contact_from_vcard(
                raw.uri.clone(),
                Some(AddressBookId(addressbook)),
                raw.etag.clone(),
                &raw.data,
            )
            .map_err(|error| project_error(AccountOperation::ContactUpdate, &error))?;
            let data = vcard_from_patch(&current, &raw.data, &patch);
            let url = client.resolve_url(&contact.0);
            client
                .put_vcard(
                    &url,
                    data,
                    put_condition(current.etag.as_deref()),
                    AccountOperation::ContactUpdate,
                )
                .await
                .map(|_| ())
        })
    }

    fn contact_delete(&self, contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        let client = Arc::clone(&self.client);
        Box::pin(async move {
            let url = client.resolve_url(&contact.0);
            client
                .delete_vcard(&url, AccountOperation::ContactDelete)
                .await
                .map(|_| ())
        })
    }

    fn contact_search(
        &self,
        request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        let client = Arc::clone(&self.client);
        let default_addressbook_url = self.default_addressbook_url.clone();
        Box::pin(async move {
            let offset =
                decode_offset_cursor(request.page_cursor.clone(), AccountOperation::ContactSearch)?;
            let needle = request.query.to_lowercase();
            let searched = if needle.is_empty() {
                Self::hydrated_contacts(
                    &client,
                    &default_addressbook_url,
                    request.address_book_id.clone(),
                    AccountOperation::ContactSearch,
                )
                .await?
            } else {
                Self::searched_contacts(
                    &client,
                    &default_addressbook_url,
                    request.address_book_id.clone(),
                    &request.query,
                )
                .await?
            };
            let mut items = searched
                .cards
                .into_iter()
                .filter(|contact| contact_matches(contact, &needle))
                .collect::<Vec<_>>();
            items.sort_by(|left, right| left.native_id.cmp(&right.native_id));
            // A zero limit is honored as an empty exhausted page, not clamped
            // up to one. Clamping silently served a contact the caller had
            // asked not to receive; `page_from_offset` is what makes the zero
            // case terminate instead of pointing at itself.
            let page_size = request.limit.map_or(CONTACT_PAGE_SIZE, |limit| {
                usize::try_from(limit).unwrap_or(usize::MAX)
            });
            Ok(page_from_offset(
                items,
                offset,
                page_size,
                searched.failed_ids,
                searched.skipped_scopes,
            ))
        })
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
        unsupported_future(AccountOperation::CalendarsList)
    }

    fn events_in_range(
        &self,
        _range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        unsupported_future(AccountOperation::EventsInRange)
    }

    fn event_get(&self, _event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        unsupported_future(AccountOperation::EventGet)
    }

    fn event_create(&self, _event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        unsupported_future(AccountOperation::EventCreate)
    }

    fn event_update(
        &self,
        _event: EventId,
        _patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::EventUpdate)
    }

    fn event_delete(&self, _event: EventId) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::EventDelete)
    }

    fn event_rsvp(
        &self,
        _event: EventId,
        _status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        unsupported_future(AccountOperation::EventRsvp)
    }

    fn event_search(
        &self,
        _request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        unsupported_future(AccountOperation::EventSearch)
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

fn contact_addressbook_url(client: &CardDavClient, contact: &ContactId) -> Option<String> {
    let resolved = client.resolve_url(&contact.0);
    let trimmed = resolved.trim_end_matches('/');
    trimmed
        .rfind('/')
        .map(|index| trimmed[..=index].to_string())
        .filter(|url| !url.is_empty())
}

fn same_collection_url(left: &str, right: &str) -> bool {
    left.trim_end_matches('/') == right.trim_end_matches('/')
}

fn put_condition(etag: Option<&str>) -> PutCondition<'_> {
    etag.filter(|etag| {
        !etag
            .get(..2)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("W/"))
    })
    .map_or(PutCondition::None, PutCondition::IfMatch)
}

/// Map a vCard projection failure to an `AccountError` for the single-resource
/// paths (get/update), where there is no listing to degrade a skip into.
fn project_error(operation: AccountOperation, error: &VCardParseError) -> AccountError {
    local_error(
        operation,
        format!("CardDAV vCard could not be parsed: {}", error.0),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ContactSnapshot {
    addressbook_url: String,
    pub(crate) ctag: Option<String>,
    entries: Vec<ContactSnapshotEntry>,
    /// Hrefs the server reported *failed* within the 207 of the poll
    /// that built this snapshot. Not persisted in the cursor (a
    /// per-poll observation); the diff preserves these `previous`
    /// entries rather than destroying them.
    failed_hrefs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ContactSnapshotEntry {
    uri: String,
    etag: Option<String>,
}

fn validate_contact_scope(
    scope: &CursorScope,
    operation: AccountOperation,
) -> Result<(), AccountError> {
    if matches!(scope, CursorScope::Type(ObjectType::Contact)) {
        Ok(())
    } else {
        Err(local_error(
            operation,
            "CardDAV only supports contact cursor scopes",
        ))
    }
}

fn cursor_from_snapshot(scope: CursorScope, snapshot: &ContactSnapshot) -> ChangeCursor {
    ChangeCursor {
        scope,
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::CardDav,
            envelope_version: CURSOR_ENVELOPE_VERSION,
            bytes: encode_cursor_snapshot(snapshot),
        },
        advanced_through: None,
        envelope_version: bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION,
    }
}

fn encode_cursor_snapshot(snapshot: &ContactSnapshot) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CURSOR_MAGIC);
    write_string(&mut bytes, &snapshot.addressbook_url);
    write_option_string(&mut bytes, snapshot.ctag.as_deref());
    write_u32(&mut bytes, snapshot.entries.len());
    for entry in &snapshot.entries {
        write_string(&mut bytes, &entry.uri);
        write_option_string(&mut bytes, entry.etag.as_deref());
    }
    bytes
}

fn decode_cursor_snapshot(cursor: &ChangeCursor) -> Result<ContactSnapshot, AccountError> {
    validate_contact_scope(&cursor.scope, AccountOperation::SyncChanges)?;
    if cursor.server_state.protocol != ProtocolKind::CardDav
        || cursor.server_state.envelope_version != CURSOR_ENVELOPE_VERSION
    {
        return Err(cursor_error("CardDAV cursor protocol or version mismatch"));
    }
    let mut input = cursor.server_state.bytes.as_slice();
    if !input.starts_with(CURSOR_MAGIC) {
        return Err(cursor_error("CardDAV cursor magic mismatch"));
    }
    input = &input[CURSOR_MAGIC.len()..];
    let addressbook_url = read_string(&mut input)?;
    let ctag = read_option_string(&mut input)?;
    let count = read_u32(&mut input)?;
    if count > input.len() / 5 {
        return Err(cursor_error(
            "CardDAV cursor entry count exceeds remaining payload",
        ));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push(ContactSnapshotEntry {
            uri: read_string(&mut input)?,
            etag: read_option_string(&mut input)?,
        });
    }
    if !input.is_empty() {
        return Err(cursor_error("CardDAV cursor has trailing bytes"));
    }
    Ok(ContactSnapshot {
        addressbook_url,
        ctag,
        entries,
        failed_hrefs: Vec::new(),
    })
}

fn diff_contact_snapshots(previous: &ContactSnapshot, current: &ContactSnapshot) -> Vec<Change> {
    // Suspected transient empty multistatus: a server returning zero
    // hrefs against a populated local snapshot would emit a Destroyed for
    // every contact and wipe the consumer's store. Treat
    // empty-vs-nonempty as "no observation," not "everything deleted."
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

fn preserve_unobserved_contact_entries(previous: &ContactSnapshot, current: &mut ContactSnapshot) {
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

fn inventory_entry_from_snapshot(entry: &ContactSnapshotEntry) -> InventoryEntry {
    InventoryEntry {
        id: ObjectId(entry.uri.clone()),
        memberships: Vec::new(),
        size: None,
        blob_id: None,
        fingerprint: bifrost_types::Fingerprint {
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
        return Err(cursor_error("CardDAV cursor string length exceeds payload"));
    }
    let value = String::from_utf8(input[..len].to_vec())
        .map_err(|error| cursor_error(format!("CardDAV cursor string is not UTF-8: {error}")))?;
    *input = &input[len..];
    Ok(value)
}

fn read_option_string(input: &mut &[u8]) -> Result<Option<String>, AccountError> {
    let Some((tag, rest)) = input.split_first() else {
        return Err(cursor_error("CardDAV cursor option tag is missing"));
    };
    *input = rest;
    match tag {
        0 => Ok(None),
        1 => read_string(input).map(Some),
        _ => Err(cursor_error("CardDAV cursor option tag is invalid")),
    }
}

fn read_u32(input: &mut &[u8]) -> Result<usize, AccountError> {
    let bytes = input
        .get(..4)
        .ok_or_else(|| cursor_error("CardDAV cursor integer is truncated"))?;
    let bytes = <[u8; 4]>::try_from(bytes)
        .map_err(|error| cursor_error(format!("CardDAV cursor integer shape: {error}")))?;
    let value = u32::from_be_bytes(bytes);
    *input = &input[4..];
    Ok(value as usize)
}

fn cursor_error(message: impl Into<String>) -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::SyncState(
            bifrost_types::SyncStateErrorKind::SchemaIncompatible,
        ),
        bifrost_types::Cause::State(bifrost_types::StateCause::SchemaIncompatible),
    )
    .protocol(bifrost_types::Protocol::CardDav)
    .operation(AccountOperation::SyncChanges)
    .text(bifrost_types::DiagnosticText::support_only(message))
    .try_build()
    .expect("valid account error classification")
}

fn decode_offset_cursor(
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
        .map_err(|error| local_error(operation, format!("invalid CardDAV page cursor: {error}")))
}

/// Slice a materialized result set into one offset page.
///
/// `failed_ids` and `skipped_scopes` describe the fetch that produced
/// `items`, not the slice, and every page of a CardDAV search reruns that
/// fetch against the server. So both lanes are reported on every page,
/// which is exactly what `Page::failed_ids` documents ("resources the
/// provider fetched FOR THIS PAGE"): a resource that first starts failing
/// while the consumer is on page three is news on page three, and
/// suppressing it after page one would lose it entirely. The cost is that
/// a resource failing throughout is named once per page, so a consumer
/// accumulating across pages must treat the lane as a set, not a tally.
fn page_from_offset<T>(
    items: Vec<T>,
    offset: usize,
    page_size: usize,
    failed_ids: Vec<String>,
    skipped_scopes: Vec<SkippedScope>,
) -> Page<T> {
    let total = items.len();
    // A zero page size is an exhausted page, not a page of nothing that still
    // points at itself: emitting the current offset again whenever results
    // exist gives a consumer that follows `next_cursor` an infinite loop that
    // never advances and never delivers an item.
    if page_size == 0 {
        return Page {
            items: Vec::new(),
            next_cursor: None,
            estimated_total: Some(estimated_total(total)),
            failed_ids,
            skipped_scopes,
        };
    }
    let end = offset.saturating_add(page_size).min(total);
    let page_items = items.into_iter().skip(offset).take(page_size).collect();
    Page {
        items: page_items,
        next_cursor: (end < total).then(|| end.to_string().into_bytes()),
        estimated_total: Some(estimated_total(total)),
        failed_ids,
        skipped_scopes,
    }
}

/// One CardDAV search or hydration leg: what materialized, the resources
/// that did not, and any scope the walk could not finish.
struct SearchedContacts {
    cards: Vec<ContactCard>,
    failed_ids: Vec<String>,
    skipped_scopes: Vec<SkippedScope>,
}

/// Reduce the failure lane to one outcome per resource id.
///
/// Text search runs a REPORT per property, so the resource the EMAIL query
/// refused can be the same resource the FN query returned in full. The
/// data arrived, so success is the true outcome; reporting the id in both
/// lanes would make a consumer count it twice and treat a contact it can
/// display as lost. The sort and dedup finish the job for ids that failed
/// in more than one REPORT.
fn one_outcome_per_id(failed_ids: &mut Vec<String>, cards: &[ContactCard]) {
    let materialized = cards
        .iter()
        .map(|card| card.native_id.as_str())
        .collect::<HashSet<_>>();
    failed_ids.retain(|id| !materialized.contains(id.as_str()));
    failed_ids.sort_unstable();
    failed_ids.dedup();
}

fn merge_listing_failures(failed_ids: &mut Vec<String>, listing_failed_hrefs: Vec<String>) {
    failed_ids.extend(listing_failed_hrefs);
}

/// Publish a partially-refused walk as a skipped scope.
///
/// A REPORT leg that failed wholly means this address book was not fully
/// searched. The cards already collected stay valid, but "no more matches"
/// is not what happened, and the consumer needs the classified failure to
/// know whether to reauthorize, retry, or stop. `failed_ids` cannot carry
/// that: it is a bare list of resource ids with no recovery class, and the
/// refused leg often does not even name the resources it lost.
/// `ErrorScope::ContactCollection` carries no id of its own; the refused
/// address book is the one named by the request, and the error's own
/// diagnostics carry the URL.
fn skipped_addressbook_scope(degraded: Option<AccountError>) -> Vec<SkippedScope> {
    degraded
        .map(|error| SkippedScope {
            scope: ErrorScope::ContactCollection,
            error,
        })
        .into_iter()
        .collect()
}

fn estimated_total(total: usize) -> u64 {
    u64::try_from(total).unwrap_or(u64::MAX)
}

/// Project a batch of hydrated vCard resources, splitting successful
/// cards from the native ids of resources that could not be parsed.
///
/// A resource the server returned but whose body will not project is
/// recorded in the returned `failed_ids` rather than dropped, so the
/// consumer can tell a transient per-resource hydration failure apart
/// from a real remote deletion and preserve the row instead of
/// destroying it. The captured id is the same `uri` a successful card
/// would carry as its native id.
/// Parsed multiget reports already use the absolute native-id namespace.
/// DAV response hrefs are rebased at the XML decoding boundary, before this
/// account layer can place them in success or failure lanes.
fn resolved_report(report: CardDavMultigetReport) -> (Vec<CardDavFetchedVCard>, Vec<String>) {
    let cards = report.cards;
    let failed = report
        .failed
        .into_iter()
        .map(|failure| failure.href)
        .chain(report.missing_data)
        .collect();
    (cards, failed)
}

fn partition_hydrated_vcards(
    addressbook: &str,
    fetched: Vec<CardDavFetchedVCard>,
) -> (Vec<ContactCard>, Vec<String>) {
    let mut cards = Vec::new();
    let mut failed_ids = Vec::new();
    for card in fetched {
        let native = card.uri.clone();
        match contact_from_vcard(
            card.uri,
            Some(AddressBookId(addressbook.to_string())),
            card.etag,
            &card.data,
        ) {
            Ok(contact) => cards.push(contact),
            Err(_) => failed_ids.push(native),
        }
    }
    (cards, failed_ids)
}

fn contact_matches(contact: &ContactCard, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    contact
        .display_name
        .as_deref()
        .is_some_and(|value| contains(value, needle))
        || contact
            .emails
            .iter()
            .any(|email| contains(&email.value, needle))
        || contact
            .phones
            .iter()
            .any(|phone| contains(&phone.value, needle))
        || contact.organizations.iter().any(|org| {
            contains(&org.name, needle)
                || org
                    .title
                    .as_deref()
                    .is_some_and(|title| contains(title, needle))
        })
        || contact.addresses.iter().any(|address| {
            address
                .formatted
                .as_deref()
                .is_some_and(|value| contains(value, needle))
                || address.street.iter().any(|value| contains(value, needle))
                || address
                    .locality
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
                || address
                    .region
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
                || address
                    .postal_code
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
                || address
                    .country
                    .as_deref()
                    .is_some_and(|value| contains(value, needle))
        })
        || contact
            .notes
            .as_deref()
            .is_some_and(|value| contains(value, needle))
}

fn contains(value: &str, needle: &str) -> bool {
    // Unicode case folding (matching CalDAV's `event_matches`) so a
    // search needle matches differently-cased non-ASCII letters in
    // international names; `to_ascii_lowercase` only folds A-Z.
    value.to_lowercase().contains(needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{AccountErrorKind, EngineDirective, RecoveryClass, SyncStateErrorKind};

    #[tokio::test]
    async fn carddav_host_attachment_unsupported() {
        // CardDAV has no cloud-drive hosting; the flag is false (Default) and
        // the leg returns `Unsupported(HostAttachment)`.
        assert!(!carddav_capabilities().pim_methods.host_attachment);

        let err = unsupported_future::<HostedAttachment>(AccountOperation::HostAttachment)
            .await
            .expect_err("carddav host_attachment is unsupported");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::HostAttachment)
        );
    }

    #[tokio::test]
    async fn carddav_directory_search_unsupported() {
        // CardDAV exposes no organization directory; the flag is false (Default)
        // and the leg returns `Unsupported(DirectorySearch)`.
        assert!(!carddav_capabilities().pim_methods.directory_search);

        let err = unsupported_future::<Page<DirectoryCard>>(AccountOperation::DirectorySearch)
            .await
            .expect_err("carddav directory_search is unsupported");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::DirectorySearch)
        );
    }

    #[tokio::test]
    async fn carddav_open_raw_rfc822_unsupported() {
        // CardDAV advertises no raw RFC822 read; the flag is false and the
        // stream's first item terminates `Unsupported(OpenRawRfc822)`.
        assert!(!carddav_capabilities().pim_methods.open_raw_rfc822);

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
    fn multiget_hrefs_are_rebased_onto_the_snapshot_id_namespace() {
        // XML decoding rebases every response href before the account layer
        // sees it, so hydration cannot split native ids from the snapshot.
        let mut report = CardDavMultigetReport {
            cards: vec![CardDavFetchedVCard {
                uri: "/contacts/one.vcf".to_string(),
                etag: Some("e1".to_string()),
                data: "BEGIN:VCARD\r\nFN:Ada\r\nEND:VCARD\r\n".to_string(),
            }],
            failed: vec![crate::parse::CardDavFailedResource {
                href: "/contacts/two.vcf".to_string(),
                status: Some(403),
            }],
            missing_data: vec!["/contacts/empty.vcf".to_string()],
        };

        report.resolve_hrefs("https://dav.example.test/");
        let (cards, failed) = resolved_report(report);

        assert_eq!(cards[0].uri, "https://dav.example.test/contacts/one.vcf");
        assert_eq!(
            failed,
            vec![
                "https://dav.example.test/contacts/two.vcf",
                "https://dav.example.test/contacts/empty.vcf",
            ]
        );
    }

    #[test]
    fn partition_hydrated_vcards_records_unparseable_ids() {
        // A well-formed vCard projects into `items`; a malformed one is
        // captured in `failed_ids` under its native uri rather than dropped,
        // so the snapshot diff can preserve it instead of destroying it.
        let good = CardDavFetchedVCard {
            uri: "/ab/good.vcf".to_string(),
            etag: Some("e1".to_string()),
            data: "BEGIN:VCARD\r\nFN:Ada Lovelace\r\nEND:VCARD\r\n".to_string(),
        };
        let bad = CardDavFetchedVCard {
            uri: "/ab/bad.vcf".to_string(),
            etag: None,
            // Unterminated quoted parameter: the projector rejects this body.
            data: "BEGIN:VCARD\r\nEMAIL;TYPE=\"work:ada@example.test\r\nEND:VCARD\r\n".to_string(),
        };

        let (cards, failed_ids) = partition_hydrated_vcards("/ab/", vec![good, bad]);

        assert_eq!(cards.len(), 1);
        assert_eq!(cards[0].native_id, "/ab/good.vcf");
        assert_eq!(cards[0].corpus, ContactCorpus::Main);
        assert_eq!(failed_ids, vec!["/ab/bad.vcf".to_string()]);
    }

    #[test]
    fn offset_cursor_decodes_ascii_offsets() {
        assert_eq!(
            decode_offset_cursor(Some(b"250".to_vec()), AccountOperation::ContactsList).unwrap(),
            250
        );
        assert!(
            decode_offset_cursor(Some(b"bad".to_vec()), AccountOperation::ContactsList).is_err()
        );
    }

    #[test]
    fn page_from_offset_returns_next_cursor() {
        let page = page_from_offset(vec![1, 2, 3, 4], 1, 2, Vec::new(), Vec::new());

        assert_eq!(page.items, vec![2, 3]);
        assert_eq!(page.next_cursor, Some(b"3".to_vec()));
        assert_eq!(page.estimated_total, Some(4));

        let tail = page_from_offset(vec![1, 2, 3, 4], 3, 2, Vec::new(), Vec::new());
        assert_eq!(tail.items, vec![4]);
        assert_eq!(tail.next_cursor, None);
    }

    /// A zero page size must terminate. Emitting the current offset again
    /// gives a consumer that follows `next_cursor` an infinite non-advancing
    /// loop. The CalDAV twin pins the same rule for `limit: Some(0)`.
    #[test]
    fn a_zero_page_size_is_an_exhausted_page_with_no_continuation() {
        let page = page_from_offset(vec![1, 2, 3, 4], 0, 0, Vec::new(), Vec::new());

        assert!(page.items.is_empty());
        assert_eq!(page.next_cursor, None);
        assert_eq!(page.estimated_total, Some(4));
    }

    #[test]
    fn a_materialized_resource_leaves_the_failure_lane() {
        let card = ContactCard {
            id: ContactId("/book/one.vcf".to_string()),
            address_book_id: Some(AddressBookId("/book/".to_string())),
            native_id: "/book/one.vcf".to_string(),
            etag: None,
            provenance: ContactProvenance {
                provider: ProtocolKind::CardDav,
                native: "/book/one.vcf".to_string(),
                address_book_native: Some("/book/".to_string()),
            },
            corpus: ContactCorpus::Main,
            display_name: None,
            emails: Vec::new(),
            phones: Vec::new(),
            organizations: Vec::new(),
            addresses: Vec::new(),
            notes: None,
            photo_url: None,
            photo: None,
        };
        let mut failed_ids = vec![
            "/book/one.vcf".to_string(),
            "/book/two.vcf".to_string(),
            "/book/two.vcf".to_string(),
        ];

        one_outcome_per_id(&mut failed_ids, std::slice::from_ref(&card));

        assert_eq!(failed_ids, vec!["/book/two.vcf".to_string()]);
    }

    #[test]
    fn a_degraded_leg_becomes_a_skipped_contact_collection_scope() {
        let error = crate::client::status_error(
            AccountOperation::ContactSearch,
            reqwest::StatusCode::UNAUTHORIZED,
            "refused".to_string(),
        );
        let recovery = error.recovery().clone();

        let skipped = skipped_addressbook_scope(Some(error));

        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].scope, ErrorScope::ContactCollection);
        assert_eq!(skipped[0].error.recovery(), &recovery);
        assert!(skipped_addressbook_scope(None).is_empty());
    }

    #[test]
    fn a_failure_first_seen_on_a_later_page_is_still_reported() {
        // Every page reruns the remote search, so the failure set is
        // re-observed per page. A resource that only starts failing while
        // the consumer is on page two must be reported on page two - the
        // page it was observed on is the only page that can report it.
        let first = page_from_offset(vec![1, 2, 3], 0, 1, Vec::new(), Vec::new());
        let second = page_from_offset(
            vec![1, 2, 3],
            1,
            1,
            vec!["/book/failed.vcf".to_string()],
            Vec::new(),
        );

        assert!(first.failed_ids.is_empty());
        assert_eq!(second.failed_ids, vec!["/book/failed.vcf"]);
    }

    #[test]
    fn contact_cursor_snapshot_round_trips() {
        let snapshot = ContactSnapshot {
            addressbook_url: "https://dav.example.test/contacts/".to_string(),
            ctag: Some("42".to_string()),
            entries: vec![
                ContactSnapshotEntry {
                    uri: "https://dav.example.test/contacts/a.vcf".to_string(),
                    etag: Some("\"a\"".to_string()),
                },
                ContactSnapshotEntry {
                    uri: "https://dav.example.test/contacts/b.vcf".to_string(),
                    etag: None,
                },
            ],
            failed_hrefs: Vec::new(),
        };
        let cursor = cursor_from_snapshot(CursorScope::Type(ObjectType::Contact), &snapshot);

        // failed_hrefs is a per-poll observation, not persisted; the
        // decoded cursor carries an empty failed_hrefs.
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
    fn contact_cursor_rejects_the_base_relative_id_version() {
        let snapshot = ContactSnapshot::default();
        let mut cursor = cursor_from_snapshot(CursorScope::Type(ObjectType::Contact), &snapshot);
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
    fn contact_cursor_rejects_impossible_count_before_allocating() {
        let snapshot = ContactSnapshot {
            addressbook_url: "https://dav.example.test/contacts/".to_string(),
            ctag: None,
            entries: Vec::new(),
            failed_hrefs: Vec::new(),
        };
        let mut cursor = cursor_from_snapshot(CursorScope::Type(ObjectType::Contact), &snapshot);
        let count_offset = cursor.server_state.bytes.len() - 4;
        cursor.server_state.bytes[count_offset..].copy_from_slice(&u32::MAX.to_be_bytes());

        assert!(decode_cursor_snapshot(&cursor).is_err());
    }

    #[test]
    fn contact_snapshot_diff_classifies_object_changes() {
        let previous = ContactSnapshot {
            addressbook_url: "book".to_string(),
            ctag: Some("1".to_string()),
            entries: vec![
                ContactSnapshotEntry {
                    uri: "a.vcf".to_string(),
                    etag: Some("old".to_string()),
                },
                ContactSnapshotEntry {
                    uri: "b.vcf".to_string(),
                    etag: Some("same".to_string()),
                },
                ContactSnapshotEntry {
                    uri: "d.vcf".to_string(),
                    etag: Some("gone".to_string()),
                },
            ],
            failed_hrefs: Vec::new(),
        };
        let current = ContactSnapshot {
            addressbook_url: "book".to_string(),
            ctag: Some("2".to_string()),
            entries: vec![
                ContactSnapshotEntry {
                    uri: "a.vcf".to_string(),
                    etag: Some("new".to_string()),
                },
                ContactSnapshotEntry {
                    uri: "b.vcf".to_string(),
                    etag: Some("same".to_string()),
                },
                ContactSnapshotEntry {
                    uri: "c.vcf".to_string(),
                    etag: Some("created".to_string()),
                },
            ],
            failed_hrefs: Vec::new(),
        };

        let changes = diff_contact_snapshots(&previous, &current);
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
                ("a.vcf".to_string(), ObjectChangeKind::Updated),
                ("c.vcf".to_string(), ObjectChangeKind::Created),
                ("d.vcf".to_string(), ObjectChangeKind::Destroyed),
            ]
        );
    }

    fn snapshot_with(entries: &[&str]) -> ContactSnapshot {
        ContactSnapshot {
            addressbook_url: "book".to_string(),
            ctag: None,
            entries: entries
                .iter()
                .map(|uri| ContactSnapshotEntry {
                    uri: (*uri).to_string(),
                    etag: Some("e".to_string()),
                })
                .collect(),
            failed_hrefs: Vec::new(),
        }
    }

    #[test]
    fn diff_contact_snapshots_suppresses_empty_207_mass_delete() {
        // Brick 6: a populated previous diffed against an empty current
        // yields zero changes (no Destroyed) - the suspected-transient
        // empty multistatus.
        let previous = snapshot_with(&["a.vcf", "b.vcf"]);
        let empty = snapshot_with(&[]);
        assert!(diff_contact_snapshots(&previous, &empty).is_empty());

        // populated-vs-populated unchanged; empty-vs-empty zero.
        let same = diff_contact_snapshots(&previous, &previous);
        assert!(same.is_empty());
        assert!(diff_contact_snapshots(&empty, &empty).is_empty());
    }

    #[test]
    fn empty_poll_checkpoint_preserves_snapshot_for_the_recovery_poll() {
        let previous = snapshot_with(&["a.vcf", "b.vcf"]);
        let mut empty = snapshot_with(&[]);
        empty.ctag = Some("refreshed".to_string());

        preserve_unobserved_contact_entries(&previous, &mut empty);
        assert_eq!(empty.entries.len(), 2);
        assert_eq!(empty.ctag.as_deref(), Some("refreshed"));

        let recovered = snapshot_with(&["a.vcf", "b.vcf"]);
        assert!(diff_contact_snapshots(&empty, &recovered).is_empty());
    }

    #[test]
    fn listing_failures_survive_into_the_page_failure_lane() {
        let mut failed_ids = vec!["multiget.vcf".to_string()];
        merge_listing_failures(&mut failed_ids, vec!["listing.vcf".to_string()]);
        assert_eq!(failed_ids, vec!["multiget.vcf", "listing.vcf"]);
    }

    #[test]
    fn failed_uri_preserved_in_contact_diff() {
        // Brick 7: a previous entry whose href is in current.failed_hrefs
        // is NOT emitted as Destroyed.
        let previous = snapshot_with(&["a.vcf", "b.vcf"]);
        let mut current = snapshot_with(&["a.vcf"]);
        current.failed_hrefs = vec!["b.vcf".to_string()];

        let changes = diff_contact_snapshots(&previous, &current);
        assert!(
            changes.is_empty(),
            "a transiently-failed resource must not be destroyed: {changes:?}"
        );

        // Without the failed-href, b.vcf's absence IS a destroy.
        let current_no_failed = snapshot_with(&["a.vcf"]);
        let changes = diff_contact_snapshots(&previous, &current_no_failed);
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
    fn contact_search_matches_common_projected_fields() {
        let contact = ContactCard {
            id: ContactId("/book/ada.vcf".to_string()),
            address_book_id: Some(AddressBookId("/book/".to_string())),
            native_id: "/book/ada.vcf".to_string(),
            etag: None,
            provenance: ContactProvenance {
                provider: ProtocolKind::CardDav,
                native: "/book/ada.vcf".to_string(),
                address_book_native: Some("/book/".to_string()),
            },
            corpus: ContactCorpus::Main,
            display_name: Some("Ada Lovelace".to_string()),
            emails: vec![bifrost_types::ContactEmail {
                value: "ada@example.test".to_string(),
                kind: None,
                is_primary: true,
            }],
            phones: Vec::new(),
            organizations: vec![bifrost_types::ContactOrganization {
                name: "Analytical Engines".to_string(),
                title: Some("Programmer".to_string()),
            }],
            addresses: vec![bifrost_types::ContactAddress {
                kind: Some("work".to_string()),
                formatted: None,
                street: vec!["1 Analytical Way".to_string()],
                locality: Some("London".to_string()),
                region: None,
                postal_code: Some("N1".to_string()),
                country: Some("UK".to_string()),
                is_primary: false,
            }],
            notes: Some("First algorithm".to_string()),
            photo_url: None,
            photo: None,
        };

        assert!(contact_matches(&contact, "lovelace"));
        assert!(contact_matches(&contact, "example.test"));
        assert!(contact_matches(&contact, "programmer"));
        assert!(contact_matches(&contact, "analytical way"));
        assert!(contact_matches(&contact, "algorithm"));
        assert!(!contact_matches(&contact, "missing"));
    }

    #[test]
    fn contact_search_folds_non_ascii_case() {
        // The needle is lowercased by the search path; matching must use
        // Unicode case folding so a non-ASCII capital (here the Nordic
        // `Å`) in the stored value matches its lowercase form. With
        // `to_ascii_lowercase` the `Å`/`å` pair would not fold and the
        // match would be missed.
        let contact = ContactCard {
            id: ContactId("/book/aase.vcf".to_string()),
            address_book_id: Some(AddressBookId("/book/".to_string())),
            native_id: "/book/aase.vcf".to_string(),
            etag: None,
            provenance: ContactProvenance {
                provider: ProtocolKind::CardDav,
                native: "/book/aase.vcf".to_string(),
                address_book_native: Some("/book/".to_string()),
            },
            corpus: ContactCorpus::Main,
            display_name: Some("Åse Bø".to_string()),
            emails: Vec::new(),
            phones: Vec::new(),
            organizations: Vec::new(),
            addresses: Vec::new(),
            notes: None,
            photo_url: None,
            photo: None,
        };

        let needle = "åse".to_lowercase();
        assert!(contact_matches(&contact, &needle));
    }
}
