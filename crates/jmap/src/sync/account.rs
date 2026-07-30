use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFuture, AccountOperation, AccountStream,
    AddressBook, AddressBookId, AttachmentHandle, BlobHandle, ByteRange, Calendar, CalendarEvent,
    ChangeCursor, CloudUploadMeta, ContactCard, ContactCreate, ContactId, ContactPatch,
    ContactSearchRequest, ContainerId, ContainerKind, ContainerList, CostClass, CursorDescriptor,
    CursorEstablishment, CursorScope, DirectoryCard, DirectoryGroup, DirectoryGroupId,
    DirectoryGroupMember, DraftHandle, DraftPatch, ErrorScope, EventCreate, EventId, EventPatch,
    EventRange, EventSearchRequest, FilterValidation, HostedAttachment, HydratedObject,
    HydrationProjection, IdempotencyKey, Identity, IdentityId, IdentityPatch, Importance,
    InventoryEntry, InventoryPartition, InventoryPartitioning, ItemOutcome, Label, MembershipScope,
    Message, MutationSuccess, MutationTarget, ObjectId, Page, Priority, Projection, QuotaInfo,
    RsvpStatus, ScopeLifecycleEvent, SearchRequest, SendAs, SendRequest, ServerFilter,
    ServerFilterCreate, ServerFilterId, ServerFilterPatch, SubscriptionHandle, SyncEvent,
    SyncStrategy, ThreadHydration, ThreadId, VacationConfig, WatchEvent,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::client::Client;
use crate::transport_reqwest::ReqwestTransport;

use super::capabilities::CoreLimits;
use super::state_cache::StateMap;
use super::{
    blob, calendar_ops, changes, contacts, discover, filters, foreign, hydrate, inventory,
    mutation, pim, push, state,
};

type MailAccount = crate::account::Account<ReqwestTransport>;

pub(crate) struct JmapAccount {
    pub(crate) client: Client,
    pub(crate) mail: MailAccount,
    /// Foreign (shared/delegate) mail accounts, keyed by JMAP
    /// `accountId`. Enumerated at `open` from the session's non-personal
    /// accounts that advertise the mail capability. A foreign `Folder`
    /// scope routes here via `mail_for_scope`.
    pub(crate) foreign_mail: Arc<HashMap<String, MailAccount>>,
    /// Foreign mail accounts that were successfully seeded and advertise
    /// `urn:ietf:params:jmap:submission`.
    pub(crate) foreign_submission: Arc<HashSet<String>>,
    pub(crate) submission: Option<MailAccount>,
    /// `urn:ietf:params:jmap:submission` `maxDelayedSend` (seconds);
    /// `0` when the server advertises no scheduled-send window. Read by
    /// `send_message` / `reschedule_send` for boundary validation.
    pub(crate) max_delayed_send: usize,
    pub(crate) vacation: Option<MailAccount>,
    pub(crate) quota: Option<MailAccount>,
    pub(crate) sieve: Option<MailAccount>,
    pub(crate) contacts: Option<MailAccount>,
    pub(crate) calendars: Option<MailAccount>,
    pub(crate) self_emails: Vec<String>,
    pub(crate) caps: AccountCapabilities,
    pub(crate) core_limits: CoreLimits,
    pub(crate) seed_states: HashMap<CursorScope, bifrost_types::OpaqueChangeState>,
    pub(crate) ws: push::WsState,
    pub(crate) subscriptions: Arc<Mutex<HashMap<SubscriptionHandle, push::DataTypeSet>>>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) closed: AtomicBool,
    pub(crate) subscription_seq: AtomicU64,
    /// Per-`accountId` `Email/changes` state cache (the mutation
    /// `ifInState` cache and changes side-cache). Keyed by accountId so
    /// the primary and each foreign account hold distinct state - a
    /// single `Option<String>` would clobber one with the other.
    pub(crate) email_states: StateMap,
    pub(crate) mailbox_states: StateMap,
    pub(crate) mailbox_names: Arc<Mutex<HashMap<String, String>>>,
}

impl JmapAccount {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: Client,
        mail: MailAccount,
        foreign_mail: HashMap<String, MailAccount>,
        foreign_submission: HashSet<String>,
        submission: Option<MailAccount>,
        max_delayed_send: usize,
        vacation: Option<MailAccount>,
        quota: Option<MailAccount>,
        sieve: Option<MailAccount>,
        contacts: Option<MailAccount>,
        calendars: Option<MailAccount>,
        self_emails: Vec<String>,
        caps: AccountCapabilities,
        core_limits: CoreLimits,
        seed_states: HashMap<CursorScope, bifrost_types::OpaqueChangeState>,
        ws: push::WsState,
        shutdown: CancellationToken,
        email_states: HashMap<String, Option<String>>,
        mailbox_states: HashMap<String, Option<String>>,
        mailbox_names: HashMap<String, String>,
    ) -> Self {
        Self {
            client,
            mail,
            foreign_mail: Arc::new(foreign_mail),
            foreign_submission: Arc::new(foreign_submission),
            submission,
            max_delayed_send,
            vacation,
            quota,
            sieve,
            contacts,
            calendars,
            self_emails,
            caps,
            core_limits,
            seed_states,
            ws,
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            shutdown,
            closed: AtomicBool::new(false),
            subscription_seq: AtomicU64::new(1),
            email_states: Arc::new(Mutex::new(email_states)),
            mailbox_states: Arc::new(Mutex::new(mailbox_states)),
            mailbox_names: Arc::new(Mutex::new(mailbox_names)),
        }
    }

    /// Resolve the `MailAccount` handle that routes a cursor scope. A
    /// foreign `Folder` scope (whose `FolderId` carries a JMAP
    /// `accountId`) routes to that account's handle; every primary
    /// `Type(_)` / `Query(_)` scope routes to `self.mail`.
    pub(crate) fn mail_for_scope(&self, scope: &CursorScope) -> &MailAccount {
        match resolve_foreign_account_id(scope, |id| self.foreign_mail.contains_key(id)) {
            Some(account_id) => self
                .foreign_mail
                .get(&account_id)
                .expect("resolve_foreign_account_id only returns a known foreign id"),
            None => &self.mail,
        }
    }

    /// The accountId that keys the state cache for a scope: the parsed
    /// foreign accountId for a `Folder` scope, else the primary account's
    /// id. Both forms key the same per-accountId maps.
    pub(crate) fn account_id_for_scope(&self, scope: &CursorScope) -> String {
        self.mail_for_scope(scope).id_str().to_string()
    }

    /// Resolve a single object mutation to its owning account. As with
    /// hydration, a qualified id for an account no longer in this session
    /// stays on the primary route so the server, not a guessed local route,
    /// reports the miss.
    fn mail_for_object_id(&self, id: &ObjectId) -> MailAccount {
        foreign_account_id_for_object(id, |account_id| self.foreign_mail.contains_key(account_id))
            .as_deref()
            .and_then(|account_id| self.foreign_mail.get(account_id))
            .cloned()
            .unwrap_or_else(|| self.mail.clone())
    }

    fn mail_for_mutation_target(&self, target: &MutationTarget) -> MailAccount {
        match target {
            MutationTarget::Message(id) => self.mail_for_object_id(id),
            MutationTarget::Thread(_) => self.mail.clone(),
            _ => self.mail.clone(),
        }
    }

    /// The owning shared-account identity for a foreign `Folder` scope,
    /// or `None` for a primary scope. Drives the revocation-isolation
    /// decision (quarantine the foreign scope vs escalate account-wide).
    pub(crate) fn owner_of_scope(&self, scope: &CursorScope) -> Option<bifrost_types::MailboxId> {
        resolve_foreign_account_id(scope, |id| self.foreign_mail.contains_key(id))
            .map(bifrost_types::MailboxId)
    }

    /// True when `scope` is a `Folder` scope that parses as a foreign
    /// (shared/delegate) mailbox but whose account is no longer
    /// registered in `foreign_mail`. This happens when a foreign scope
    /// was seeded at a prior `open` but the foreign account has since
    /// disappeared from the session (revoked delegation, removed share).
    /// Such a scope must NOT silently fall back to the primary account in
    /// `mail_for_scope`: the foreign mailbox id (e.g. `inbox`) would then
    /// resolve against the *primary* mailbox, conflating two accounts.
    /// Callers route it to a terminal error instead.
    pub(crate) fn is_unregistered_foreign_scope(&self, scope: &CursorScope) -> bool {
        is_unregistered_foreign(scope, |id| self.foreign_mail.contains_key(id))
    }

    pub(crate) fn cursor_scopes(&self) -> Vec<CursorScope> {
        let ordered = [
            CursorScope::Type(bifrost_types::ObjectType::Email),
            CursorScope::Type(bifrost_types::ObjectType::Mailbox),
        ];

        let mut scopes: Vec<CursorScope> = ordered
            .into_iter()
            .filter(|scope| self.seed_states.contains_key(scope))
            .collect();

        // Foreign (shared/delegate) account mailboxes surface as seeded
        // `Folder` scopes (the foreign accountId rides in the FolderId).
        // Preserve a deterministic order so discovery is stable.
        let mut foreign: Vec<CursorScope> = self
            .seed_states
            .keys()
            .filter(|scope| matches!(scope, CursorScope::Folder(_)))
            .cloned()
            .collect();
        foreign.sort_by(|a, b| match (a, b) {
            (CursorScope::Folder(x), CursorScope::Folder(y)) => x.0.cmp(&y.0),
            _ => std::cmp::Ordering::Equal,
        });
        scopes.extend(foreign);
        scopes
    }

    /// The owner-tag memberships every foreign account contributes:
    /// `Mailbox(accountId)` per foreign account. Emitted alongside the
    /// primary account's per-mailbox memberships during discovery.
    pub(crate) fn foreign_owner_memberships(&self) -> Vec<MembershipScope> {
        foreign_owner_memberships_from_scopes(self.seed_states.keys())
    }

    pub(crate) fn next_subscription_handle(&self) -> SubscriptionHandle {
        let next = self.subscription_seq.fetch_add(1, Ordering::AcqRel);
        SubscriptionHandle(format!("jmap-ws-{next}"))
    }
}

fn foreign_account_id_for_object<F>(id: &ObjectId, is_registered: F) -> Option<String>
where
    F: Fn(&str) -> bool,
{
    super::foreign::parse_object(&id.0)
        .filter(|(account_id, _)| is_registered(account_id))
        .map(|(account_id, _)| account_id.to_string())
}

/// Pure descriptor logic behind `Account::describe_cursor`, split out
/// for direct testing.
///
/// Decoding alone is not support: legacy `Thread` and `Query` cursors
/// still decode (their scope tags remain in the V1 envelope so old
/// durable rows stay readable) but `changes::stream` terminates both
/// `Unsupported(SyncChanges)` - thread inventory derives from Email
/// inventory, and the v1 contract registers no query definitions to
/// turn a query id into a filter. Reporting `Cheap`/`ServerCursor` for
/// one would promise a strategy that dies on its first poll, so only
/// the scopes the change stream actually drives report one.
fn describe_cursor_support(cursor: &ChangeCursor) -> CursorDescriptor {
    let supported = matches!(
        state::decode_cursor(cursor),
        Ok((
            state::JmapScopeRepr::Email
                | state::JmapScopeRepr::Mailbox
                | state::JmapScopeRepr::Folder { .. },
            _
        ))
    );
    CursorDescriptor {
        cost_class: if supported {
            CostClass::Cheap
        } else {
            CostClass::Expensive
        },
        strategy: if supported {
            SyncStrategy::ServerCursor
        } else {
            SyncStrategy::None
        },
        freshness: None,
    }
}

impl Account for JmapAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.caps
    }

    fn set_priority(&self, priority: Priority) {
        self.client.transport().set_priority(priority);
    }

    fn set_bandwidth_cap(&self, bps: Option<u64>) {
        self.client.transport().set_bandwidth_cap(bps);
    }

    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor {
        describe_cursor_support(cursor)
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        discover::cursor_scopes(self.cursor_scopes())
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        discover::memberships(self.mail.clone(), self.foreign_owner_memberships())
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        discover::scope_lifecycle(
            self.mail.clone(),
            self.core_limits,
            Arc::clone(&self.mailbox_states),
            self.mail.id_str().to_string(),
            Arc::clone(&self.mailbox_names),
            self.shutdown.clone(),
        )
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        let seed = self.seed_states.get(&scope).cloned();
        let scope_for_err = scope.clone();
        Box::pin(async move {
            let server_state = seed.ok_or_else(|| {
                // Go through the shared `unsupported_error` helper so
                // every `Unsupported` AccountError in this crate flows
                // through a single construction path. The inline
                // `AccountErrorBuilder::new(...)` that used to live
                // here drifted from the helper's invariants (no scope
                // attached, no diagnostic text).
                super::error::unsupported_error(
                    AccountOperation::EstablishCursor,
                    Some(ErrorScope::Cursor(scope_for_err)),
                    "JMAP did not seed a cursor for this scope",
                )
            })?;
            Ok(CursorEstablishment::Ready(ChangeCursor {
                scope,
                server_state,
                advanced_through: None,
                envelope_version: state::CHANGE_CURSOR_ENVELOPE_VERSION,
            }))
        })
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<SyncEvent<InventoryEntry>> {
        if self.is_unregistered_foreign_scope(&scope) {
            let err =
                super::error::unregistered_foreign_scope(scope, AccountOperation::SyncInventory);
            return Box::pin(async_stream::stream! { yield super::error::terminated(err); });
        }
        let mail = self.mail_for_scope(&scope).clone();
        let owner = self.owner_of_scope(&scope);
        inventory::stream(mail, self.core_limits, scope, owner)
    }

    fn inventory_partitioning(&self, scope: &CursorScope) -> InventoryPartitioning {
        match scope {
            CursorScope::Type(bifrost_types::ObjectType::Email) => {
                InventoryPartitioning::PageCount {
                    total: None,
                    page_size: u32::try_from(self.core_limits.max_objects_in_get).ok(),
                }
            }
            _ => InventoryPartitioning::Full,
        }
    }

    fn inventory_partition_stream(
        &self,
        scope: CursorScope,
        partition: InventoryPartition,
    ) -> AccountStream<SyncEvent<InventoryEntry>> {
        if self.is_unregistered_foreign_scope(&scope) {
            let err =
                super::error::unregistered_foreign_scope(scope, AccountOperation::SyncInventory);
            return Box::pin(async_stream::stream! { yield super::error::terminated(err); });
        }
        let mail = self.mail_for_scope(&scope).clone();
        let owner = self.owner_of_scope(&scope);
        inventory::stream_partition(mail, self.core_limits, scope, partition, owner)
    }

    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        // A foreign-qualified id must hydrate against its OWNING account
        // (the same selection `mail_for_scope` makes on the changes path);
        // `Email/get` is accountId-scoped, so routing it through the primary
        // account cannot reach a shared mailbox's mail.
        hydrate::stream(
            self.mail.clone(),
            Arc::clone(&self.foreign_mail),
            self.core_limits,
            ids,
            projection,
        )
    }

    fn changes_stream(
        &self,
        cursor: ChangeCursor,
    ) -> AccountStream<SyncEvent<bifrost_types::Change>> {
        if self.is_unregistered_foreign_scope(&cursor.scope) {
            let err = super::error::unregistered_foreign_scope(
                cursor.scope,
                AccountOperation::SyncChanges,
            );
            return Box::pin(async_stream::stream! { yield super::error::terminated(err); });
        }
        let mail = self.mail_for_scope(&cursor.scope).clone();
        let account_id = self.account_id_for_scope(&cursor.scope);
        let owner = self.owner_of_scope(&cursor.scope);
        changes::stream(
            mail,
            account_id,
            self.core_limits,
            cursor,
            owner,
            Arc::clone(&self.email_states),
            Arc::clone(&self.mailbox_states),
        )
    }

    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<SubscriptionHandle, AccountError>> {
        push::subscribe(
            self.client.clone(),
            self.caps.push,
            self.next_subscription_handle(),
            scopes.to_vec(),
            Arc::clone(&self.subscriptions),
            Arc::clone(&self.ws.enabled),
        )
    }

    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        push::unsubscribe(
            self.client.clone(),
            handle,
            Arc::clone(&self.subscriptions),
            Arc::clone(&self.ws.enabled),
        )
    }

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        push::stream(self.ws.tx.subscribe(), self.shutdown.clone())
    }

    fn open_blob(&self, handle: BlobHandle) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open(
            self.client.clone(),
            self.mail.id().clone(),
            Arc::clone(&self.foreign_mail),
            handle,
        )
    }

    fn open_blob_range(
        &self,
        handle: BlobHandle,
        range: ByteRange,
    ) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open_range(handle, range)
    }

    fn open_raw_rfc822(&self, message: ObjectId) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open_raw_rfc822(
            self.client.clone(),
            self.mail.id().clone(),
            self.mail.clone(),
            Arc::clone(&self.foreign_mail),
            message,
        )
    }

    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        op: bifrost_types::FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::set_flags(
            self.mail.clone(),
            Arc::clone(&self.foreign_mail),
            self.core_limits,
            Arc::clone(&self.email_states),
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
        mutation::move_to(
            self.mail.clone(),
            Arc::clone(&self.foreign_mail),
            self.core_limits,
            Arc::clone(&self.email_states),
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
        mutation::destroy(
            self.mail.clone(),
            Arc::clone(&self.foreign_mail),
            self.core_limits,
            Arc::clone(&self.email_states),
            targets,
            key,
        )
    }

    fn add_to_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::add_to_container(
            mail.clone(),
            Arc::clone(&self.email_states),
            mail.id_str().to_string(),
            target,
            container,
        )
    }

    fn remove_from_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::remove_from_container(
            mail.clone(),
            Arc::clone(&self.email_states),
            mail.id_str().to_string(),
            target,
            container,
        )
    }

    fn set_keyword(
        &self,
        target: MutationTarget,
        keyword: String,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::set_keyword(
            mail.clone(),
            Arc::clone(&self.email_states),
            mail.id_str().to_string(),
            target,
            keyword,
            value,
        )
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::SetLabelMembership,
            None,
            "JMAP does not support label membership",
        );
        Box::pin(async move { Err(err) })
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::SetCategory,
            None,
            "JMAP does not support categories",
        );
        Box::pin(async move { Err(err) })
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::SetExtendedProperty,
            None,
            "JMAP does not support extended properties",
        );
        Box::pin(async move { Err(err) })
    }

    fn set_is_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::set_is_read(
            mail.clone(),
            Arc::clone(&self.email_states),
            mail.id_str().to_string(),
            target,
            is_read,
        )
    }

    fn set_importance(
        &self,
        target: MutationTarget,
        level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::set_importance(
            mail.clone(),
            Arc::clone(&self.email_states),
            mail.id_str().to_string(),
            target,
            level,
        )
    }

    fn send_message(&self, request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        if let Some(send_as) = request.send_as.as_ref() {
            let id = send_as.mailbox().0.clone();
            if let Err(err) = route_send_as(
                send_as,
                request.scheduled.is_some(),
                self.foreign_mail.contains_key(&id),
                self.foreign_submission.contains(&id),
            ) {
                return Box::pin(async move { Err(err) });
            }
            let mail = self
                .foreign_mail
                .get(&id)
                .cloned()
                .expect("route_send_as validated foreign_mail membership");
            let self_address = self
                .self_emails
                .first()
                .cloned()
                .map(bifrost_types::Address::bare);
            return pim::send_message(
                mail,
                Arc::clone(&self.email_states),
                id,
                0,
                Some(pim::ForeignSubmission {
                    mode: send_as.clone(),
                    self_address,
                }),
                request,
            );
        }
        let Some(submission) = self.submission.clone() else {
            let err = super::error::unsupported_error(
                AccountOperation::Send,
                None,
                "JMAP submission capability not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // Route through the resolved `Submission`-capability account
        // handle, not `self.mail`. They are usually the same accountId,
        // but `primary_account::<Submission>()` can resolve a distinct
        // account; the draft `Email/set` and the `EmailSubmission/set`
        // must both target the submission account.
        let account_id = submission.id_str().to_string();
        pim::send_message(
            submission,
            Arc::clone(&self.email_states),
            account_id,
            self.max_delayed_send,
            None,
            request,
        )
    }

    fn send_raw_message(
        &self,
        raw: bytes::Bytes,
        save_to_sent: Option<bool>,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        let Some(submission) = self.submission.clone() else {
            let err = super::error::unsupported_error(
                AccountOperation::Send,
                None,
                "JMAP submission capability not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // Same submission-account routing as `send_message`: the import +
        // EmailSubmission must both target the submission-capable account.
        pim::send_raw_message(submission, raw, save_to_sent)
    }

    fn attachment_upload(
        &self,
        bytes: AccountStream<Result<bytes::Bytes, AccountError>>,
        mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        pim::attachment_upload(self.mail.clone(), bytes, mime)
    }

    fn host_attachment(
        &self,
        _bytes: bytes::Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::HostAttachment,
            None,
            "JMAP has no cloud-drive attachment hosting",
        );
        Box::pin(async move { Err(err) })
    }

    fn draft_create(&self, patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        pim::draft_create(
            self.mail.clone(),
            Arc::clone(&self.email_states),
            self.mail.id_str().to_string(),
            patch,
        )
    }

    fn draft_update(
        &self,
        draft: DraftHandle,
        patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::draft_update(
            self.mail.clone(),
            Arc::clone(&self.email_states),
            self.mail.id_str().to_string(),
            draft,
            patch,
        )
    }

    fn draft_discard(&self, draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        pim::draft_discard(
            self.mail.clone(),
            Arc::clone(&self.email_states),
            self.mail.id_str().to_string(),
            draft,
        )
    }

    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        let Some(submission) = self.submission.clone() else {
            let err = super::error::unsupported_error(
                AccountOperation::DraftSend,
                None,
                "JMAP submission capability not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // The `EmailSubmission/set` and the Sent-folder move both target
        // the resolved submission account, not necessarily `self.mail`.
        let account_id = submission.id_str().to_string();
        pim::draft_send(
            submission,
            Arc::clone(&self.email_states),
            account_id,
            draft,
        )
    }

    fn cancel_scheduled_send(&self, handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        let cancellable = self
            .submission
            .clone()
            .filter(|_| self.max_delayed_send != 0);
        let Some(submission) = cancellable else {
            let err = super::error::unsupported_error(
                AccountOperation::CancelScheduledSend,
                None,
                "JMAP scheduled send not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // The submission id was minted on the `Submission` account; the
        // `EmailSubmission/set` cancel must go to that account.
        pim::cancel_scheduled_send(submission, handle)
    }

    fn reschedule_send(
        &self,
        handle: ObjectId,
        scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        let reschedulable = self
            .submission
            .clone()
            .filter(|_| self.max_delayed_send != 0);
        let Some(submission) = reschedulable else {
            let err = super::error::unsupported_error(
                AccountOperation::RescheduleSend,
                None,
                "JMAP scheduled send not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // The submission id was minted on the `Submission` account; the
        // cancel + recreate must both target that account.
        pim::reschedule_send(submission, self.max_delayed_send, handle, scheduled)
    }

    fn search(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        pim::search(self.mail.clone(), request)
    }

    fn search_messages(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        pim::search_messages(self.mail.clone(), request)
    }

    fn containers_list(&self) -> AccountFuture<Result<ContainerList, AccountError>> {
        pim::containers_list(self.mail.clone(), Arc::clone(&self.foreign_mail))
    }

    fn container_create(
        &self,
        kind: ContainerKind,
        name: String,
        parent: Option<ContainerId>,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        pim::container_create(
            self.mail.clone(),
            Arc::clone(&self.mailbox_states),
            self.mail.id_str().to_string(),
            kind,
            name,
            parent,
            style,
        )
    }

    fn container_rename(
        &self,
        container: ContainerId,
        name: String,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::container_rename(
            self.mail.clone(),
            Arc::clone(&self.mailbox_states),
            self.mail.id_str().to_string(),
            container,
            name,
            style,
        )
    }

    fn container_move(
        &self,
        container: ContainerId,
        new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::container_move(
            self.mail.clone(),
            Arc::clone(&self.mailbox_states),
            self.mail.id_str().to_string(),
            container,
            new_parent,
        )
    }

    fn container_delete(&self, container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        pim::container_delete(
            self.mail.clone(),
            Arc::clone(&self.mailbox_states),
            self.mail.id_str().to_string(),
            container,
        )
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        pim::identities_list(self.submission.clone())
    }

    fn identity_update(
        &self,
        identity: IdentityId,
        patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::identity_update(self.submission.clone(), identity, patch)
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        pim::vacation_get(self.vacation.clone())
    }

    fn vacation_set(&self, config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        pim::vacation_set(self.vacation.clone(), config)
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        pim::quota_get(self.quota.clone())
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        filters::list(self.sieve.clone())
    }

    fn filter_create(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        filters::create(self.sieve.clone(), filter)
    }

    fn filter_update(
        &self,
        filter: ServerFilterId,
        patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        filters::update(self.sieve.clone(), filter, patch)
    }

    fn filter_delete(&self, filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        filters::delete(self.sieve.clone(), filter)
    }

    fn filter_validate(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        filters::validate(self.sieve.clone(), filter)
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        contacts::address_books_list(self.contacts.clone())
    }

    fn contacts_list(
        &self,
        address_book: Option<AddressBookId>,
        page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        contacts::list(self.contacts.clone(), address_book, page_cursor)
    }

    fn contact_get(&self, contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        contacts::get(self.contacts.clone(), contact)
    }

    fn contact_create(
        &self,
        contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        contacts::create(self.contacts.clone(), contact)
    }

    fn contact_update(
        &self,
        contact: ContactId,
        patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        contacts::update(self.contacts.clone(), contact, patch)
    }

    fn contact_delete(&self, contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        contacts::delete(self.contacts.clone(), contact)
    }

    fn contact_search(
        &self,
        request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        contacts::search(self.contacts.clone(), request)
    }

    fn directory_search(
        &self,
        _query: String,
        _limit: Option<u32>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::DirectorySearch,
            None,
            "JMAP has no organization-directory concept",
        );
        Box::pin(async move { Err(err) })
    }

    fn directory_groups_list(
        &self,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroup>, AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::DirectoryGroupsList,
            None,
            "JMAP has no organization-directory concept",
        );
        Box::pin(async move { Err(err) })
    }

    fn directory_group_expand(
        &self,
        _group: DirectoryGroupId,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<DirectoryGroupMember>, AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::DirectoryGroupExpand,
            None,
            "JMAP has no organization-directory concept",
        );
        Box::pin(async move { Err(err) })
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        calendar_ops::calendars_list(self.calendars.clone())
    }

    fn events_in_range(
        &self,
        range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        calendar_ops::events_in_range(self.calendars.clone(), range)
    }

    fn event_get(&self, event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        calendar_ops::get(self.calendars.clone(), event)
    }

    fn event_create(&self, event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        calendar_ops::create(self.calendars.clone(), event)
    }

    fn event_update(
        &self,
        event: EventId,
        patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        calendar_ops::update(self.calendars.clone(), event, patch)
    }

    fn event_delete(&self, event: EventId) -> AccountFuture<Result<(), AccountError>> {
        calendar_ops::delete(self.calendars.clone(), event)
    }

    fn event_rsvp(
        &self,
        event: EventId,
        status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        calendar_ops::rsvp(
            self.calendars.clone(),
            self.self_emails.clone(),
            event,
            status,
        )
    }

    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        calendar_ops::search(self.calendars.clone(), request)
    }

    fn thread_hydrate(
        &self,
        thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        pim::thread_hydrate(self.mail.clone(), thread)
    }

    fn message_hydrate(
        &self,
        message: ObjectId,
        projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        pim::message_hydrate(
            self.mail.clone(),
            Arc::clone(&self.foreign_mail),
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
        pim::move_thread(
            self.mail.clone(),
            Arc::clone(&self.email_states),
            self.mail.id_str().to_string(),
            thread,
            target,
            source,
        )
    }

    fn apply_label(
        &self,
        target: MutationTarget,
        label: Label,
    ) -> AccountFuture<Result<(), AccountError>> {
        match label.provenance.kind {
            ContainerKind::Label => self.set_keyword(target, label.provenance.native, true),
            ContainerKind::Folder => self.add_to_container(target, label.id),
            _ => {
                let err = super::error::unsupported_error(
                    AccountOperation::SetLabelMembership,
                    None,
                    "JMAP does not support this label kind",
                );
                Box::pin(async move { Err(err) })
            }
        }
    }

    fn remove_label(
        &self,
        target: MutationTarget,
        label: Label,
    ) -> AccountFuture<Result<(), AccountError>> {
        match label.provenance.kind {
            ContainerKind::Label => self.set_keyword(target, label.provenance.native, false),
            ContainerKind::Folder => self.remove_from_container(target, label.id),
            _ => {
                let err = super::error::unsupported_error(
                    AccountOperation::SetLabelMembership,
                    None,
                    "JMAP does not support this label kind",
                );
                Box::pin(async move { Err(err) })
            }
        }
    }

    fn delete_thread(
        &self,
        thread: ThreadId,
        current: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::delete_thread(
            self.mail.clone(),
            Arc::clone(&self.email_states),
            self.mail.id_str().to_string(),
            thread,
            current,
        )
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Box::pin(async { Ok(()) });
        }

        let client = self.client.clone();
        let shutdown = self.shutdown.clone();
        Box::pin(async move {
            shutdown.cancel();
            let result = client.disable_push_ws().await;
            match result {
                Ok(()) | Err(crate::Error::WebSocketNotConnected) => Ok(()),
                Err(err) => Err(super::error::into_account_error(
                    err,
                    super::error::JmapErrorContext::new(bifrost_types::AccountOperation::Close),
                )),
            }
        })
    }
}

/// One `Mailbox(accountId)` owner tag per distinct foreign account among
/// the given scopes. Pure, so discovery's owner-tag emission is
/// unit-testable without a live account.
fn foreign_owner_memberships_from_scopes<'a, I>(scopes: I) -> Vec<MembershipScope>
where
    I: IntoIterator<Item = &'a CursorScope>,
{
    let mut owners: Vec<String> = scopes
        .into_iter()
        .filter_map(|scope| match scope {
            CursorScope::Folder(folder) => {
                foreign::parse_foreign(folder).map(|parsed| parsed.account_id)
            }
            _ => None,
        })
        .collect();
    owners.sort();
    owners.dedup();
    owners
        .into_iter()
        .map(|id| foreign::owner_tag(&id))
        .collect()
}

/// The foreign JMAP `accountId` a scope routes to, or `None` for a
/// primary scope. Only a `CursorScope::Folder` whose `FolderId` parses
/// as a foreign mailbox AND whose account is registered (per
/// `is_registered`) routes foreign; every other scope is primary. Pure,
/// so the routing decision is unit-testable without a live `Client`.
fn resolve_foreign_account_id<F>(scope: &CursorScope, is_registered: F) -> Option<String>
where
    F: Fn(&str) -> bool,
{
    if let CursorScope::Folder(folder) = scope
        && let Some(parsed) = foreign::parse_foreign(folder)
        && is_registered(&parsed.account_id)
    {
        Some(parsed.account_id)
    } else {
        None
    }
}

/// True when `scope` is a foreign-mailbox `Folder` scope whose account is
/// NOT registered. Pure counterpart to `resolve_foreign_account_id`:
/// where that returns `None` for both "primary scope" and "foreign but
/// unregistered", this isolates the second case so callers can route it
/// to a terminal error instead of misrouting it to the primary account.
fn is_unregistered_foreign<F>(scope: &CursorScope, is_registered: F) -> bool
where
    F: Fn(&str) -> bool,
{
    match scope {
        CursorScope::Folder(folder) => {
            foreign::parse_foreign(folder).is_some_and(|parsed| !is_registered(&parsed.account_id))
        }
        _ => false,
    }
}

/// Pure send-as routing decision. Returns `Ok(())` when the request may be
/// dispatched to the foreign account named by `send_as`, or the boundary
/// rejection otherwise. Precedence: scheduled foreign sends are refused
/// first (their bare submission handles cannot be safely cancelled or
/// rescheduled through the primary account); an id absent from the seeded
/// routing table is a malformed request (the consumer got it from foreign
/// membership ownership); a known-but-not-submission-capable id is
/// unsupported.
fn route_send_as(
    send_as: &SendAs,
    scheduled: bool,
    known: bool,
    submission_capable: bool,
) -> Result<(), AccountError> {
    if scheduled {
        return Err(super::error::unsupported_error(
            AccountOperation::Send,
            None,
            "scheduled foreign send is not supported",
        ));
    }
    if !known {
        return Err(super::error::send_as_unknown_account(send_as.mailbox()));
    }
    if !submission_capable {
        return Err(super::error::unsupported_error(
            AccountOperation::Send,
            None,
            "foreign account does not advertise submission",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use bifrost_types::{
        AccountErrorKind, AccountOperation, CursorScope, MailboxId, MembershipScope, ObjectId,
        ObjectType, QueryId, SendAs,
    };

    use super::super::{foreign, state};
    use super::{
        describe_cursor_support, foreign_account_id_for_object,
        foreign_owner_memberships_from_scopes, is_unregistered_foreign, resolve_foreign_account_id,
        route_send_as,
    };
    use bifrost_types::{ChangeCursor, CostClass, SyncStrategy};

    fn as_shared() -> SendAs {
        SendAs::As(MailboxId("foreign-id".to_string()))
    }

    #[test]
    fn route_send_as_known_submission_capable_ok() {
        assert!(route_send_as(&as_shared(), false, true, true).is_ok());
    }

    #[test]
    fn route_send_as_scheduled_is_unsupported() {
        let err = route_send_as(&as_shared(), true, true, true).expect_err("scheduled rejected");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::Send)
        ));
    }

    #[test]
    fn route_send_as_unknown_account_is_malformed() {
        let err =
            route_send_as(&as_shared(), false, false, false).expect_err("unknown id rejected");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
    }

    #[test]
    fn route_send_as_known_not_submission_capable_is_unsupported() {
        let err =
            route_send_as(&as_shared(), false, true, false).expect_err("no submission rejected");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::Send)
        ));
    }

    fn cursor(scope: CursorScope) -> ChangeCursor {
        state::cursor_for_scope(scope, "state-1").expect("scope encodes")
    }

    #[test]
    fn supported_scopes_describe_a_server_cursor() {
        for scope in [
            CursorScope::Type(ObjectType::Email),
            CursorScope::Type(ObjectType::Mailbox),
            CursorScope::Folder(foreign::encode_foreign("acct-9", "mbx-1")),
        ] {
            let descriptor = describe_cursor_support(&cursor(scope.clone()));
            assert_eq!(descriptor.cost_class, CostClass::Cheap, "{scope:?}");
            assert_eq!(descriptor.strategy, SyncStrategy::ServerCursor, "{scope:?}");
        }
    }

    #[test]
    fn legacy_thread_and_query_cursors_are_not_advertised_as_usable() {
        // Both decode (their scope tags remain in the V1 envelope) but
        // `changes::stream` terminates them Unsupported(SyncChanges), so
        // the descriptor must not promise a strategy that dies on its
        // first poll. Strategy `None` routes the consumer back through
        // inventory instead.
        for scope in [
            CursorScope::Type(ObjectType::Thread),
            CursorScope::Query(QueryId("unread-in-inbox".to_string())),
        ] {
            let legacy = cursor(scope.clone());
            assert!(
                state::decode_cursor(&legacy).is_ok(),
                "premise: the legacy cursor still decodes ({scope:?})"
            );
            let descriptor = describe_cursor_support(&legacy);
            assert_eq!(descriptor.cost_class, CostClass::Expensive, "{scope:?}");
            assert_eq!(descriptor.strategy, SyncStrategy::None, "{scope:?}");
        }
    }

    #[test]
    fn an_undecodable_cursor_describes_no_strategy() {
        let mut broken = cursor(CursorScope::Type(ObjectType::Email));
        broken.server_state.bytes.pop();
        let descriptor = describe_cursor_support(&broken);
        assert_eq!(descriptor.cost_class, CostClass::Expensive);
        assert_eq!(descriptor.strategy, SyncStrategy::None);
    }

    #[test]
    fn discover_emits_foreign_account_owner_membership() {
        // Two foreign mailboxes in one account plus one in another: the
        // owner tag is per-account (deduped), not per-mailbox.
        let scopes = [
            CursorScope::Type(ObjectType::Email),
            CursorScope::Folder(foreign::encode_foreign("acct-9", "mbx-1")),
            CursorScope::Folder(foreign::encode_foreign("acct-9", "mbx-2")),
            CursorScope::Folder(foreign::encode_foreign("acct-7", "mbx-3")),
        ];
        let owners = foreign_owner_memberships_from_scopes(scopes.iter());
        assert!(owners.contains(&MembershipScope::Mailbox(MailboxId("acct-9".to_string()))));
        assert!(owners.contains(&MembershipScope::Mailbox(MailboxId("acct-7".to_string()))));
        // One owner tag per account, not per mailbox.
        assert_eq!(owners.len(), 2);
    }

    #[test]
    fn mail_for_scope_routes_foreign_account() {
        let registered: HashSet<String> = ["acct-9".to_string()].into_iter().collect();
        let is_registered = |id: &str| registered.contains(id);

        // A foreign Folder scope for a registered account routes foreign.
        let foreign_scope = CursorScope::Folder(foreign::encode_foreign("acct-9", "mbx-3"));
        assert_eq!(
            resolve_foreign_account_id(&foreign_scope, is_registered),
            Some("acct-9".to_string())
        );

        // A foreign Folder scope for an UNregistered account resolves to
        // no handle (None). It is NOT silently routed to the primary
        // account - `is_unregistered_foreign` flags it so callers emit a
        // terminal error (see `unregistered_foreign_scope_is_flagged_not_misrouted`).
        let unknown_scope = CursorScope::Folder(foreign::encode_foreign("acct-other", "mbx-3"));
        assert_eq!(
            resolve_foreign_account_id(&unknown_scope, is_registered),
            None
        );

        // Primary scopes never route foreign.
        assert_eq!(
            resolve_foreign_account_id(&CursorScope::Type(ObjectType::Email), is_registered),
            None
        );
        assert_eq!(
            resolve_foreign_account_id(
                &CursorScope::Query(QueryId("q1".to_string())),
                is_registered
            ),
            None
        );
    }

    #[test]
    fn message_mutations_select_only_registered_foreign_accounts() {
        let registered: HashSet<String> = ["acct-9".to_string()].into_iter().collect();
        let is_registered = |id: &str| registered.contains(id);

        assert_eq!(
            foreign_account_id_for_object(
                &ObjectId(foreign::encode_object("acct-9", "M1")),
                is_registered,
            ),
            Some("acct-9".to_string())
        );
        assert_eq!(
            foreign_account_id_for_object(
                &ObjectId(foreign::encode_object("acct-gone", "M1")),
                is_registered,
            ),
            None,
            "an unreachable account stays on the primary route for a real server miss"
        );
        assert_eq!(
            foreign_account_id_for_object(&ObjectId("M1".to_string()), is_registered),
            None
        );
    }

    #[test]
    fn unregistered_foreign_scope_is_flagged_not_misrouted() {
        let registered: HashSet<String> = ["acct-9".to_string()].into_iter().collect();
        let is_registered = |id: &str| registered.contains(id);

        // A seeded-then-unregistered foreign scope: routing returns None
        // (would fall back to primary), but the misroute guard flags it
        // so the caller terminates instead of running it against the
        // primary mailbox.
        let gone = CursorScope::Folder(foreign::encode_foreign("acct-gone", "inbox"));
        assert_eq!(resolve_foreign_account_id(&gone, is_registered), None);
        assert!(is_unregistered_foreign(&gone, is_registered));

        // A registered foreign scope is NOT flagged (routes foreign).
        let live = CursorScope::Folder(foreign::encode_foreign("acct-9", "inbox"));
        assert!(!is_unregistered_foreign(&live, is_registered));

        // Primary scopes are never flagged.
        assert!(!is_unregistered_foreign(
            &CursorScope::Type(ObjectType::Email),
            is_registered
        ));
    }
}
