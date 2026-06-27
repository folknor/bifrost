mod blobs;
mod calendar;
mod capabilities;
mod changes;
mod cloud;
mod contacts;
mod cursor;
mod error;
mod filters;
mod flags;
mod inventory;
mod mutation;
mod pim;
mod push;
mod scopes;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFactory, AccountFuture, AccountId,
    AccountOperation, AccountStream, AddressBook, AddressBookId, AttachmentHandle, BlobHandle,
    ByteRange, Calendar, CalendarEvent, Change, ChangeCursor, CloudUploadMeta, ContactCard,
    ContactCreate, ContactId, ContactPatch, ContactSearchRequest, Container, ContainerId,
    ContainerKind, CostClass, CursorDescriptor, CursorEstablishment, CursorScope, DirectoryCard,
    DraftHandle, DraftPatch, EventCreate, EventId, EventPatch, EventRange, EventSearchRequest,
    FilterValidation, FlagOp, HostedAttachment, HydratedObject, HydrationProjection,
    IdempotencyKey, Identity, IdentityId, IdentityPatch, Importance, InventoryEntry, ItemOutcome,
    MembershipScope, Message, MutationSuccess, MutationTarget, ObjectId, OpaqueChangeState, Page,
    Priority, Projection, QuotaInfo, RsvpStatus, ScopeLifecycleEvent, SearchRequest, SendRequest,
    ServerFilter, ServerFilterCreate, ServerFilterId, ServerFilterPatch, SubscriptionHandle,
    SyncEvent, SyncStrategy, ThreadHydration, ThreadId, VacationConfig, WatchEvent,
};
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use bifrost_net::TokenSource;

use crate::client::GmailClient;
use crate::types::GmailProfile;

pub use push::PubSubConfig;

use self::capabilities::gmail_capabilities;
use self::cursor::{
    GmailChangeState, cursor_from_state, decode_gmail_state_for_profile, encode_gmail_state,
};
use self::push::PubSubControl;
use self::scopes::{ScopeCache, ScopeSnapshot};

fn non_empty<T>(iter: impl Iterator<Item = T>) -> Option<Vec<T>> {
    let values = iter.collect::<Vec<_>>();
    (!values.is_empty()).then_some(values)
}

/// Factory for opening Google accounts through the shared `Account` API.
pub struct GoogleAccountFactory {
    client: Arc<GmailClient>,
    pubsub: Option<PubSubConfig>,
}

impl GoogleAccountFactory {
    fn from_client(client: GmailClient) -> Self {
        Self {
            client: Arc::new(client),
            pubsub: None,
        }
    }

    /// Construct a Google factory from a bearer access token.
    #[must_use]
    pub fn from_access_token(access_token: impl Into<String>) -> Self {
        Self::from_client(GmailClient::new(access_token))
    }

    /// Construct a Google factory from a shared token source. ratatoskr
    /// supplies an `Arc<dyn TokenSource>` (typically an `OAuthRefresher`
    /// over its own refresh-token store) so a refreshed-and-persisted
    /// token is read live at every Gmail/People request without reopen.
    #[must_use]
    pub fn from_token_source(source: Arc<dyn TokenSource>) -> Self {
        Self::from_client(GmailClient::with_source(source))
    }

    /// Construct a Google factory from a bearer access token, redirecting
    /// the Gmail API base. Test seam mirroring `bifrost-graph`'s
    /// `GraphClient::with_api_base`: lets a harness point Gmail requests at a
    /// mock endpoint instead of `www.googleapis.com`.
    #[must_use]
    pub fn from_access_token_with_api_base(
        access_token: impl Into<String>,
        api_base: impl Into<String>,
    ) -> Self {
        Self::from_client(GmailClient::with_api_base(api_base, access_token))
    }

    /// Construct a Google factory from a shared token source, redirecting the
    /// Gmail API base. Test seam mirroring `bifrost-graph`'s
    /// `GraphClient::with_source`: combines a live `OAuthRefresher` (or any
    /// `TokenSource`) with a mock Gmail endpoint.
    #[must_use]
    pub fn from_token_source_with_api_base(
        source: Arc<dyn TokenSource>,
        api_base: impl Into<String>,
    ) -> Self {
        Self::from_client(GmailClient::with_api_base_and_source(api_base, source))
    }

    /// Configure Gmail Cloud Pub/Sub watch ownership for opened accounts.
    #[must_use]
    pub fn with_pubsub_config(mut self, config: PubSubConfig) -> Self {
        self.pubsub = Some(config);
        self
    }

    /// Configure an account-wide Gmail Cloud Pub/Sub watch topic.
    #[must_use]
    pub fn with_pubsub_topic(self, topic: impl Into<String>) -> Self {
        self.with_pubsub_config(PubSubConfig::new(topic))
    }
}

impl AccountFactory for GoogleAccountFactory {
    fn open(&self, account_id: AccountId) -> AccountFuture<Result<Arc<dyn Account>, AccountError>> {
        let client = Arc::new(self.client.for_account(account_id));
        let pubsub = self.pubsub.clone();
        Box::pin(async move {
            let account = GoogleAccount::open(client, pubsub).await?;
            Ok(account as Arc<dyn Account>)
        })
    }
}

struct GoogleAccount {
    client: Arc<GmailClient>,
    capabilities: AccountCapabilities,
    profile: GmailProfile,
    seed_state: OpaqueChangeState,
    pubsub: Arc<PubSubControl>,
    scope_cache: ScopeCache,
    shutdown: CancellationToken,
    closed: AtomicBool,
}

impl GoogleAccount {
    async fn open(
        client: Arc<GmailClient>,
        pubsub: Option<PubSubConfig>,
    ) -> Result<Arc<Self>, AccountError> {
        let profile = client
            .get_profile()
            .await
            .map_err(|error| error::into_account_error(error, error::GmailErrorContext::open()))?;
        let history_id = profile.history_id.parse::<u64>().map_err(|error| {
            error::into_account_error(
                crate::error::Error::missing_field(
                    "historyId",
                    format!("gmail profile invalid history id: {error}"),
                ),
                error::GmailErrorContext::open(),
            )
        })?;
        let seed_state = encode_gmail_state(&GmailChangeState::new(
            history_id,
            profile.email_address.clone(),
        ));
        Ok(Arc::new(Self {
            client,
            capabilities: gmail_capabilities(),
            profile,
            seed_state,
            pubsub: Arc::new(PubSubControl::new(pubsub)),
            scope_cache: Arc::new(std::sync::RwLock::new(ScopeSnapshot::empty())),
            shutdown: CancellationToken::new(),
            closed: AtomicBool::new(false),
        }))
    }
}

impl Account for GoogleAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.capabilities
    }

    fn set_priority(&self, priority: Priority) {
        self.client.account_net().set_priority(priority);
    }

    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        self.client.account_net().set_bandwidth_cap(bps);
    }

    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor {
        let valid =
            decode_gmail_state_for_profile(&cursor.server_state, &self.profile.email_address)
                .is_ok();
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
            freshness: valid.then(Instant::now),
        }
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        scopes::discover_cursor_scopes()
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        scopes::discover_memberships(Arc::clone(&self.client), Arc::clone(&self.scope_cache))
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        scopes::scope_lifecycle_stream(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            self.shutdown.clone(),
        )
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        let seed = self.seed_state.clone();
        Box::pin(async move {
            if !matches!(scope, CursorScope::Account) {
                return Err(error::into_account_error(
                    crate::error::Error::unsupported(AccountOperation::EstablishCursor),
                    error::GmailErrorContext::establish_cursor(),
                ));
            }
            Ok(CursorEstablishment::Ready(cursor_from_state(seed)))
        })
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<SyncEvent<InventoryEntry>> {
        inventory::inventory_stream(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            scope,
        )
    }

    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        inventory::get_stream(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            ids,
            projection,
        )
    }

    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        changes::changes_stream(Arc::clone(&self.client), self.profile.clone(), cursor)
    }

    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<SubscriptionHandle, AccountError>> {
        push::push_subscribe(
            Arc::clone(&self.client),
            Arc::clone(&self.pubsub),
            self.shutdown.clone(),
            scopes.to_vec(),
        )
    }

    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        push::push_unsubscribe(Arc::clone(&self.client), Arc::clone(&self.pubsub), handle)
    }

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        push::push_stream(Arc::clone(&self.pubsub), self.shutdown.clone())
    }

    fn open_blob(&self, handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        blobs::open_blob(Arc::clone(&self.client), handle)
    }

    fn open_blob_range(
        &self,
        handle: BlobHandle,
        range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        blobs::open_blob_range(Arc::clone(&self.client), handle, range)
    }

    fn open_raw_rfc822(&self, message: ObjectId) -> AccountStream<SyncEvent<Bytes>> {
        blobs::open_raw_rfc822(Arc::clone(&self.client), message)
    }

    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        op: FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::bulk_set_flags(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
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
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::bulk_move(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            targets,
            destination,
            key,
        )
    }

    fn bulk_destroy(
        &self,
        targets: AccountStream<ObjectId>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::bulk_destroy(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            targets,
            key,
        )
    }

    fn add_to_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::add_to_container(Arc::clone(&self.client), target, container)
    }

    fn remove_from_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::remove_from_container(Arc::clone(&self.client), target, container)
    }

    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(error::into_account_error(
                crate::error::Error::unsupported(AccountOperation::SetKeyword),
                error::GmailErrorContext::mutation(AccountOperation::SetKeyword),
            ))
        })
    }

    fn set_label_membership(
        &self,
        target: MutationTarget,
        label: ContainerId,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::set_label_membership(Arc::clone(&self.client), target, label, value)
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(error::into_account_error(
                crate::error::Error::unsupported(AccountOperation::SetCategory),
                error::GmailErrorContext::mutation(AccountOperation::SetCategory),
            ))
        })
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(error::into_account_error(
                crate::error::Error::unsupported(AccountOperation::SetExtendedProperty),
                error::GmailErrorContext::mutation(AccountOperation::SetExtendedProperty),
            ))
        })
    }

    fn set_is_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::set_is_read(Arc::clone(&self.client), target, is_read)
    }

    fn set_importance(
        &self,
        _target: MutationTarget,
        _level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        // Gmail has no message-importance field.
        Box::pin(async {
            Err(error::into_account_error(
                crate::error::Error::unsupported(AccountOperation::SetImportance),
                error::GmailErrorContext::mutation(AccountOperation::SetImportance),
            ))
        })
    }

    fn send_message(&self, request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::send_message(
            Arc::clone(&self.client),
            self.profile.email_address.clone(),
            request,
        )
    }

    fn send_raw_message(
        &self,
        raw: Bytes,
        save_to_sent: Option<bool>,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::send_raw_message(Arc::clone(&self.client), raw, save_to_sent)
    }

    fn attachment_upload(
        &self,
        bytes: AccountStream<Result<Bytes, AccountError>>,
        mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        pim::attachment_upload(bytes, mime)
    }

    fn host_attachment(
        &self,
        bytes: Bytes,
        meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        cloud::host_attachment(
            Arc::clone(&self.client),
            self.profile.email_address.clone(),
            bytes,
            meta,
        )
    }

    fn draft_create(&self, patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        pim::draft_create(
            Arc::clone(&self.client),
            self.profile.email_address.clone(),
            patch,
        )
    }

    fn draft_update(
        &self,
        draft: DraftHandle,
        patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::draft_update(
            Arc::clone(&self.client),
            self.profile.email_address.clone(),
            draft,
            patch,
        )
    }

    fn draft_discard(&self, draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        pim::draft_discard(Arc::clone(&self.client), draft)
    }

    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::draft_send(Arc::clone(&self.client), draft)
    }

    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        // Gmail's REST API exposes no scheduled-send model.
        pim::cancel_scheduled_send_unsupported()
    }

    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        pim::reschedule_send_unsupported()
    }

    fn search(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        pim::search(Arc::clone(&self.client), request)
    }

    fn search_messages(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        pim::search_messages(Arc::clone(&self.client), request)
    }

    fn containers_list(&self) -> AccountFuture<Result<Vec<Container>, AccountError>> {
        pim::containers_list(Arc::clone(&self.client), Arc::clone(&self.scope_cache))
    }

    fn container_create(
        &self,
        kind: ContainerKind,
        name: String,
        parent: Option<ContainerId>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        pim::container_create(Arc::clone(&self.client), kind, name, parent)
    }

    fn container_rename(
        &self,
        container: ContainerId,
        name: String,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::container_rename(Arc::clone(&self.client), container, name)
    }

    fn container_move(
        &self,
        container: ContainerId,
        new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::container_move(container, new_parent)
    }

    fn container_delete(&self, container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        pim::container_delete(Arc::clone(&self.client), container)
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        pim::identities_list(Arc::clone(&self.client))
    }

    fn identity_update(
        &self,
        identity: IdentityId,
        patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::identity_update(Arc::clone(&self.client), identity, patch)
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        pim::vacation_get(Arc::clone(&self.client))
    }

    fn vacation_set(&self, config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        pim::vacation_set(Arc::clone(&self.client), config)
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        pim::quota_get()
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        filters::list(Arc::clone(&self.client))
    }

    fn filter_create(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        filters::create(Arc::clone(&self.client), filter)
    }

    fn filter_update(
        &self,
        _filter: ServerFilterId,
        _patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(error::into_account_error(
                crate::error::Error::unsupported(AccountOperation::FilterUpdate),
                error::GmailErrorContext::base(AccountOperation::FilterUpdate),
            ))
        })
    }

    fn filter_delete(&self, filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        filters::delete(Arc::clone(&self.client), filter)
    }

    fn filter_validate(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        filters::validate(filter)
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        contacts::address_books_list(Arc::clone(&self.client))
    }

    fn contacts_list(
        &self,
        address_book: Option<AddressBookId>,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        contacts::list(Arc::clone(&self.client), address_book, page_cursor)
    }

    fn contact_get(&self, contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        contacts::get(Arc::clone(&self.client), contact)
    }

    fn contact_create(
        &self,
        contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        contacts::create(Arc::clone(&self.client), contact)
    }

    fn contact_update(
        &self,
        contact: ContactId,
        patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        contacts::update(Arc::clone(&self.client), contact, patch)
    }

    fn contact_delete(&self, contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        contacts::delete(Arc::clone(&self.client), contact)
    }

    fn contact_search(
        &self,
        request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        contacts::search(Arc::clone(&self.client), request)
    }

    fn directory_search(
        &self,
        query: String,
        limit: Option<u32>,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        contacts::directory_search(Arc::clone(&self.client), query, limit, page_cursor)
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        calendar::calendars_list(Arc::clone(&self.client))
    }

    fn events_in_range(
        &self,
        range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        calendar::events_in_range(Arc::clone(&self.client), range)
    }

    fn event_get(&self, event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        calendar::get(Arc::clone(&self.client), event)
    }

    fn event_create(&self, event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        calendar::create(Arc::clone(&self.client), event)
    }

    fn event_update(
        &self,
        event: EventId,
        patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        calendar::update(Arc::clone(&self.client), event, patch)
    }

    fn event_delete(&self, event: EventId) -> AccountFuture<Result<(), AccountError>> {
        calendar::delete(Arc::clone(&self.client), event)
    }

    fn event_rsvp(
        &self,
        event: EventId,
        status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        calendar::rsvp(
            Arc::clone(&self.client),
            self.profile.email_address.clone(),
            event,
            status,
        )
    }

    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        calendar::search(Arc::clone(&self.client), request)
    }

    fn thread_hydrate(
        &self,
        thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        pim::thread_hydrate(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            thread,
        )
    }

    fn message_hydrate(
        &self,
        message: ObjectId,
        projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        pim::message_hydrate(
            Arc::clone(&self.client),
            Arc::clone(&self.scope_cache),
            message,
            projection,
        )
    }

    fn move_thread(
        &self,
        thread: ThreadId,
        target: ContainerId,
        source: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::move_thread(Arc::clone(&self.client), thread, target, source)
    }

    fn delete_thread(
        &self,
        thread: ThreadId,
        current: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::delete_thread(Arc::clone(&self.client), thread, current)
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Box::pin(async { Ok(()) });
        }
        self.shutdown.cancel();
        let pubsub = Arc::clone(&self.pubsub);
        Box::pin(async move {
            pubsub.abort_renewer().await;
            Ok(())
        })
    }
}
