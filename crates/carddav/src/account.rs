use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFuture, AccountId, AccountOperation,
    AccountStream, AddressBook, AddressBookId, AttachmentHandle, BlobHandle, ByteRange, Calendar,
    CalendarEvent, Change, ChangeCursor, Checkpoint, ContactCard, ContactCreate, ContactId,
    ContactPatch, ContactProvenance, ContactSearchRequest, Container, ContainerId, ContainerKind,
    CostClass, CursorDescriptor, CursorEstablishment, CursorScope, DraftHandle, DraftPatch,
    EventCreate, EventId, EventPatch, EventRange, EventSearchRequest, FilterValidation, FlagOp,
    HydratedObject, HydrationProjection, IdempotencyKey, Identity, IdentityId, IdentityPatch,
    InventoryEntry, InventoryPartition, InventoryPartitioning, ItemOutcome, MembershipScope,
    Message, MutationSuccess, MutationTarget, ObjectChange, ObjectChangeKind, ObjectId, ObjectType,
    OpaqueChangeState, Page, PageBoundary, Priority, ProtocolKind, QuotaInfo, RsvpStatus,
    SearchRequest, SendRequest, ServerFilter, ServerFilterCreate, ServerFilterId,
    ServerFilterPatch, ServerVersion, SubscriptionHandle, SyncEvent, SyncStrategy, ThreadHydration,
    ThreadId, VacationConfig, WatchEvent,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use uuid::Uuid;

use crate::CardDavConfig;
use crate::capabilities::carddav_capabilities;
use crate::client::{CardDavClient, PutCondition, contact_scope, local_error, unsupported_error};
use crate::parse::{AddressBookCollection, CardDavFetchedVCard};
use crate::vcard::{contact_from_vcard, vcard_from_create, vcard_from_patch};

const CONTACT_PAGE_SIZE: usize = 250;
const CURSOR_ENVELOPE_VERSION: u32 = 1;
const CURSOR_MAGIC: &[u8] = b"CDAVCTAG1";

#[derive(Debug)]
pub(crate) struct CardDavAccount {
    client: Arc<CardDavClient>,
    capabilities: AccountCapabilities,
    addressbook_home: String,
    default_addressbook_url: String,
}

impl CardDavAccount {
    pub(crate) async fn open(
        _account_id: AccountId,
        config: CardDavConfig,
    ) -> Result<Self, AccountError> {
        let client = CardDavClient::new(&config)?;
        let addressbook_home = client.discover_addressbook_home().await?;
        let collections = client.list_addressbooks(&addressbook_home).await?;
        let default_addressbook_url = collections
            .first()
            .map(|collection| client.resolve_url(&collection.href))
            .unwrap_or_else(|| client.resolve_url(&addressbook_home));
        Ok(Self {
            client: Arc::new(client),
            capabilities: carddav_capabilities(),
            addressbook_home,
            default_addressbook_url,
        })
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

    fn map_addressbook(client: &CardDavClient, collection: AddressBookCollection) -> AddressBook {
        let native = client.resolve_url(&collection.href);
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
        Ok(contact_from_vcard(
            card.uri,
            Some(AddressBookId(addressbook)),
            card.etag,
            &card.data,
        ))
    }

    async fn fetch_contact_resource(
        client: &CardDavClient,
        addressbook: &str,
        contact: &ContactId,
        operation: AccountOperation,
    ) -> Result<CardDavFetchedVCard, AccountError> {
        let cards = client
            .fetch_vcards(addressbook, std::slice::from_ref(&contact.0), operation)
            .await?;
        cards.into_iter().next().ok_or_else(|| {
            unsupported_error(operation)
                .into_builder()
                .scope(contact_scope(contact.0.clone()))
                .try_build()
                .expect("valid account error classification")
        })
    }

    async fn hydrated_contacts(
        client: &CardDavClient,
        default_addressbook_url: &str,
        address_book: Option<AddressBookId>,
        operation: AccountOperation,
    ) -> Result<Vec<ContactCard>, AccountError> {
        let addressbook = Self::addressbook_url(client, default_addressbook_url, address_book);
        let entries = client.list_contacts(&addressbook).await?;
        let uris = entries
            .iter()
            .map(|entry| entry.uri.clone())
            .collect::<Vec<_>>();
        let cards = client
            .fetch_vcards(&addressbook, &uris, operation)
            .await?
            .into_iter()
            .map(|card| {
                contact_from_vcard(
                    card.uri,
                    Some(AddressBookId(addressbook.clone())),
                    card.etag,
                    &card.data,
                )
            })
            .collect();
        Ok(cards)
    }

    async fn searched_contacts(
        client: &CardDavClient,
        default_addressbook_url: &str,
        address_book: Option<AddressBookId>,
        query: &str,
    ) -> Result<Vec<ContactCard>, AccountError> {
        let addressbook = Self::addressbook_url(client, default_addressbook_url, address_book);
        let mut seen = HashSet::new();
        let cards = client
            .query_vcards_text(&addressbook, query)
            .await?
            .into_iter()
            .filter(|card| seen.insert(card.uri.clone()))
            .map(|card| {
                contact_from_vcard(
                    card.uri,
                    Some(AddressBookId(addressbook.clone())),
                    card.etag,
                    &card.data,
                )
            })
            .collect();
        Ok(cards)
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
        let entries = client.list_contacts(&addressbook).await?;
        let total = entries.len();
        let uris = entries
            .into_iter()
            .skip(offset)
            .take(page_size)
            .map(|entry| entry.uri)
            .collect::<Vec<_>>();
        let cards = client
            .fetch_vcards(&addressbook, &uris, operation)
            .await?
            .into_iter()
            .map(|card| {
                contact_from_vcard(
                    card.uri,
                    Some(AddressBookId(addressbook.clone())),
                    card.etag,
                    &card.data,
                )
            })
            .collect::<Vec<_>>();
        Ok(Page {
            items: cards,
            next_cursor: (offset + page_size < total)
                .then(|| (offset + page_size).to_string().into_bytes()),
            estimated_total: Some(estimated_total(total)),
        })
    }

    async fn contact_snapshot(
        client: &CardDavClient,
        home: &str,
        addressbook: &str,
        operation: AccountOperation,
    ) -> Result<ContactSnapshot, AccountError> {
        let collections = client
            .list_addressbooks_for_operation(home, operation)
            .await?;
        let ctag = collections
            .into_iter()
            .find(|collection| {
                same_collection_url(&client.resolve_url(&collection.href), addressbook)
            })
            .and_then(|collection| collection.ctag);
        let mut entries = client
            .list_contacts_for_operation(addressbook, operation)
            .await?
            .into_iter()
            .map(|entry| ContactSnapshotEntry {
                uri: client.resolve_url(&entry.uri),
                etag: entry.etag,
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.uri.cmp(&right.uri));
        Ok(ContactSnapshot {
            addressbook_url: addressbook.to_string(),
            ctag,
            entries,
        })
    }
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
                &home,
                &addressbook,
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
        let home = self.addressbook_home.clone();
        let addressbook = self.default_addressbook_url.clone();
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
                    &home,
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
        _projection: bifrost_types::Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        unsupported_stream(AccountOperation::Hydrate)
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        let client = Arc::clone(&self.client);
        let home = self.addressbook_home.clone();
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
                let current = match Self::contact_snapshot(
                    &client,
                    &home,
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

    fn containers_list(&self) -> AccountFuture<Result<Vec<Container>, AccountError>> {
        unsupported_future(AccountOperation::ContainersList)
    }

    fn container_create(
        &self,
        _kind: ContainerKind,
        _name: String,
        _parent: Option<ContainerId>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        unsupported_future(AccountOperation::ContainerCreate)
    }

    fn container_rename(
        &self,
        _container: ContainerId,
        _name: String,
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
                .map(|collection| Self::map_addressbook(&client, collection))
                .collect::<Vec<_>>();
            if books.is_empty() {
                let native = client.resolve_url(&home);
                books.push(AddressBook {
                    id: AddressBookId(native.clone()),
                    native_id: native.clone(),
                    name: "Address Book".to_string(),
                    provenance: ContactProvenance {
                        provider: ProtocolKind::CardDav,
                        native,
                        address_book_native: None,
                    },
                    is_default: true,
                    can_create_contacts: true,
                    can_update_contacts: true,
                    can_delete_contacts: true,
                });
            } else if let Some(first) = books.first_mut() {
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
            );
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
            let needle = request.query.to_ascii_lowercase();
            let cards = if needle.is_empty() {
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
            let items = cards
                .into_iter()
                .filter(|contact| contact_matches(contact, &needle))
                .collect::<Vec<_>>();
            let page_size = request
                .limit
                .and_then(|limit| usize::try_from(limit).ok())
                .unwrap_or(CONTACT_PAGE_SIZE)
                .max(1);
            Ok(page_from_offset(items, offset, page_size))
        })
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
    Box::pin(stream::iter([SyncEvent::Terminated(unsupported_error(
        operation,
    ))]))
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
    etag.map_or(PutCondition::None, PutCondition::IfMatch)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ContactSnapshot {
    addressbook_url: String,
    ctag: Option<String>,
    entries: Vec<ContactSnapshotEntry>,
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
        envelope_version: CURSOR_ENVELOPE_VERSION,
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
    })
}

fn diff_contact_snapshots(previous: &ContactSnapshot, current: &ContactSnapshot) -> Vec<Change> {
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
                changes.push(object_change(&old.uri, ObjectChangeKind::Destroyed));
                left += 1;
            }
            (Some(_), Some(new)) => {
                changes.push(object_change(&new.uri, ObjectChangeKind::Created));
                right += 1;
            }
            (Some(old), None) => {
                changes.push(object_change(&old.uri, ObjectChangeKind::Destroyed));
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

fn page_from_offset<T>(items: Vec<T>, offset: usize, page_size: usize) -> Page<T> {
    let total = items.len();
    let end = offset.saturating_add(page_size).min(total);
    let page_items = items.into_iter().skip(offset).take(page_size).collect();
    Page {
        items: page_items,
        next_cursor: (end < total).then(|| end.to_string().into_bytes()),
        estimated_total: Some(estimated_total(total)),
    }
}

fn estimated_total(total: usize) -> u64 {
    u64::try_from(total).unwrap_or(u64::MAX)
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
    value.to_ascii_lowercase().contains(needle)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let page = page_from_offset(vec![1, 2, 3, 4], 1, 2);

        assert_eq!(page.items, vec![2, 3]);
        assert_eq!(page.next_cursor, Some(b"3".to_vec()));
        assert_eq!(page.estimated_total, Some(4));

        let tail = page_from_offset(vec![1, 2, 3, 4], 3, 2);
        assert_eq!(tail.items, vec![4]);
        assert_eq!(tail.next_cursor, None);
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
        };
        let cursor = cursor_from_snapshot(CursorScope::Type(ObjectType::Contact), &snapshot);

        let decoded = decode_cursor_snapshot(&cursor).expect("cursor should decode");

        assert_eq!(decoded, snapshot);
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
}
