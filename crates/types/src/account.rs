//! The `Account` trait and the `AccountFactory` companion trait.
//!
//! The trait is dyn-safe by construction: all stream and future
//! returns are erased through `AccountStream<T>` / `AccountFuture<T>`,
//! the cursor state is a concrete tagged blob rather than an
//! associated type, and `close` takes `&self` so it composes with
//! `Arc<dyn Account>`. A compile-time `_dyn_safe` check in
//! `lib.rs` verifies this at every build.
//!
//! Two-tier surface:
//!
//! - Primitives (no default impl): wire-level operations every
//!   protocol crate implements. The Account-impl-side surface.
//! - Conveniences (default impl in terms of primitives): ratatoskr-
//!   shaped composites. Protocol crates override only when the
//!   default is wrong for that provider; consumers who disagree with
//!   ratatoskr's defaults use the primitives directly.

use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::Future;
use futures::stream::Stream;

use crate::blob::{BlobHandle, ByteRange};
use crate::capabilities::{AccountCapabilities, StarredFlagShape};
use crate::compose::{AttachmentHandle, DraftHandle, DraftPatch, IdentityId, SendRequest};
use crate::container::{Container, ContainerId, ContainerKind, Label, MutationTarget};
use crate::cursor::{
    ChangeCursor, CursorDescriptor, CursorEstablishment, CursorScope, MembershipScope,
    ScopeLifecycleEvent,
};
use crate::error::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, ItemOutcome,
    MutationSuccess, RequestCause,
};
use crate::events::{
    Change, InventoryEntry, InventoryPartition, InventoryPartitioning, Priority, SyncEvent,
    WatchEvent,
};
use crate::hydration::{HydrationProjection, Message, ThreadHydration};
use crate::ids::{AccountId, ObjectId, SubscriptionHandle, ThreadId};
use crate::mutation::{FlagOp, HydratedObject, IdempotencyKey, Projection};
use crate::page::Page;
use crate::search::SearchRequest;
use crate::settings::{Identity, IdentityPatch, QuotaInfo, VacationConfig};

/// Erased streaming return type for `Account` methods.
///
/// Each Account method that streams returns a boxed `Stream`. The
/// erasure is required for `dyn Account` to be reachable.
pub type AccountStream<T> = Pin<Box<dyn Stream<Item = T> + Send + 'static>>;

/// Erased async return type for `Account` methods.
///
/// `'static` because the returned future may outlive the Account
/// handle's local borrow.
pub type AccountFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// The contract every protocol crate implements and the engine drives.
///
/// All implementations are `Send + Sync`. Construction is consumer-
/// side via an `AccountFactory`; the engine owns the open
/// `Arc<dyn Account>` and calls the factory on reopen.
pub trait Account: Send + Sync {
    /// Read-once snapshot of the account's capabilities. Mid-session
    /// transitions are signaled by ending streams with
    /// `RecoveryClass::CapabilityChanged`, never through a live
    /// channel here.
    fn capabilities(&self) -> &AccountCapabilities;

    /// Apply the engine's current priority hint to this account.
    ///
    /// Implementations that own a `bifrost_net::AccountNet` should
    /// forward this to `AccountNet::set_priority`. Non-HTTP or
    /// priority-agnostic implementations may store it for their own
    /// scheduler.
    fn set_priority(&self, priority: Priority);

    /// Apply the engine's current bandwidth cap to this account.
    ///
    /// Implementations that own a `bifrost_net::AccountNet` should
    /// forward this to `AccountNet::set_bandwidth_cap`. `None` means
    /// unlimited.
    fn set_bandwidth_cap(&self, bps: Option<u64>);

    /// Cursor introspection. Account-aware because cost depends on
    /// capability state, not the cursor in isolation.
    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor;

    /// Cursor-scope discovery: what scopes does the engine multiplex
    /// `changes_stream` over? Bounded; terminates with `Done`.
    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>>;

    /// Membership-scope discovery: what containers can the consumer
    /// ask about (folder trees, labels, mailboxes, queries)? Bounded.
    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>>;

    /// Ongoing scope lifecycle events (folder created, renamed, deleted).
    ///
    /// Yields `ScopeLifecycleEvent::Lifecycle(_)` for scope changes
    /// and `ScopeLifecycleEvent::Terminated(AccountError)` when the
    /// long-running poll cannot continue. The structured terminal
    /// signal lets the engine escalate auth-lost or schema breaks
    /// observed by the lifecycle poller; previously the protocol
    /// could only sleep-and-retry silently because the stream element
    /// type was a bare `ScopeLifecycle` with no terminal carrier.
    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent>;

    /// Initial cursor establishment. The engine calls this exactly
    /// once per `(account, scope)` pair before its first
    /// `changes_stream(cursor)` call. See `CursorEstablishment` for
    /// the two outcomes.
    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>>;

    /// Inventory: projection-only cold-start primitive. Also the
    /// cursor-establishment pass for scopes that returned
    /// `CursorEstablishment::EstablishViaInventory`; for those scopes
    /// the terminal `SyncEvent::Done` carries the established cursor
    /// in its checkpoint.
    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<SyncEvent<InventoryEntry>>;

    /// Which inventory partition shape this account can serve for
    /// `scope`. Implementations that do not override this keep the
    /// original full-pass behavior.
    fn inventory_partitioning(&self, _scope: &CursorScope) -> InventoryPartitioning {
        InventoryPartitioning::Full
    }

    /// Inventory for a single partition. The default implementation
    /// supports only `InventoryPartition::Full`; protocol crates that
    /// advertise a stronger `inventory_partitioning` must override
    /// this to honor the matching partition variants.
    fn inventory_partition_stream(
        &self,
        scope: CursorScope,
        partition: InventoryPartition,
    ) -> AccountStream<SyncEvent<InventoryEntry>> {
        match partition {
            InventoryPartition::Full => self.inventory_stream(scope),
            _ => {
                let op = AccountOperation::SyncInventory;
                let error = AccountErrorBuilder::new(
                    AccountErrorKind::Unsupported(op),
                    Cause::Request(RequestCause::Unsupported { operation: op }),
                )
                .operation(op)
                .try_build()
                .expect("valid account error classification");
                Box::pin(futures::stream::iter([
                    SyncEvent::Terminated(error),
                    SyncEvent::Done(None),
                ]))
            }
        }
    }

    /// Hydrate known ids at a chosen projection. Input ids are
    /// streamed so the engine can backpressure long fetch passes.
    /// Per-item outcomes flow through `ItemOutcome<HydratedObject>`:
    /// successful hydrations emit `Succeeded`, locally-invalid or
    /// remotely-rejected ids emit `Failed`, transport drops that
    /// leave the item state ambiguous emit `Uncertain`. This is the
    /// same lane shape every other streaming bulk surface uses.
    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>>;

    /// Post-cursor diff. Yields `Change` (the sum of `ObjectChange`
    /// and `ScopeChange`).
    fn changes_stream(&self, cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>>;

    /// Server-side push subscription CRUD: create.
    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<SubscriptionHandle, AccountError>>;

    /// Server-side push subscription CRUD: destroy.
    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Event stream from the protocol crate to the engine. Contents
    /// depend on `push_in_process`:
    ///
    /// - In-process (IMAP IDLE, JMAP WebSocket, EWS streaming): full
    ///   `WatchEvent` traffic - `Invalidated` + `Disconnected` +
    ///   `Reconnected`.
    /// - Out-of-process (Gmail Pub/Sub, Graph webhooks):
    ///   `Invalidated` events flow through `InvalidationSink`, not
    ///   here. The stream may carry `Disconnected` / `Reconnected` on
    ///   sustained subscription-health failure, or yield nothing.
    fn push_stream(&self) -> AccountStream<WatchEvent>;

    /// Open a blob for streaming download.
    fn open_blob(&self, handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>>;

    /// Open a byte range of a blob. Errors with
    /// `AccountErrorKind::Unsupported(OpenBlobRange)` where the blob's
    /// capability flag is false.
    fn open_blob_range(
        &self,
        handle: BlobHandle,
        range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>>;

    /// Bulk flag mutation. `targets` is a streaming input so
    /// engine-driven mutation pipelines backpressure cleanly.
    /// `op` carries both the operation kind AND the flag set per
    /// variant; a `FlagOp::Set(HashSet)` pins the target flag set
    /// directly.
    ///
    /// Returns per-item `ItemOutcome<MutationSuccess>` envelopes.
    /// Every pulled item produces exactly one outcome. Locally-invalid
    /// items emit `Failed` rather than poisoning the stream. Early
    /// termination via `SyncEvent::Terminated(AccountError)` covers
    /// only already-pulled items.
    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        op: FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>;

    /// Bulk move. Targets are moved into `destination`.
    ///
    /// Same streaming invariants as `bulk_set_flags`.
    fn bulk_move(
        &self,
        targets: AccountStream<ObjectId>,
        destination: MembershipScope,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>;

    /// Bulk destroy.
    ///
    /// Same streaming invariants as `bulk_set_flags`.
    fn bulk_destroy(
        &self,
        targets: AccountStream<ObjectId>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>;

    // ------------------------------------------------------------
    // Mail mutation primitives (S1-W1)
    //
    // Each maps to one wire-level operation. Account impls that do
    // not natively support a given primitive return
    // `Err(Error::Unsupported)` and clear the matching flag in
    // `capabilities().pim_methods`.
    // ------------------------------------------------------------

    /// Add `target` to `container`. JMAP `Email/set` mailboxIds add;
    /// IMAP `COPY`; Gmail `messages.modify` addLabels; Graph
    /// `POST /messages/{id}/move`.
    fn add_to_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Remove `target` from `container`. JMAP `Email/set` mailboxIds
    /// remove; IMAP `+FLAGS \Deleted` + `EXPUNGE`; Gmail
    /// `messages.modify` removeLabels; Graph has no symmetric op
    /// (move replaces source).
    fn remove_from_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Set or clear a single IMAP / JMAP keyword. Gmail and Graph
    /// return `Err(AccountErrorKind::Unsupported)`; the convenience
    /// layer chooses an alternate primitive instead.
    fn set_keyword(
        &self,
        target: MutationTarget,
        keyword: String,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Set or clear membership in a Gmail-style label. JMAP / IMAP /
    /// Graph return `Err(AccountErrorKind::Unsupported)`.
    fn set_label_membership(
        &self,
        target: MutationTarget,
        label: ContainerId,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Set or clear a Graph category. JMAP / IMAP / Gmail return
    /// `Err(AccountErrorKind::Unsupported)`.
    fn set_category(
        &self,
        target: MutationTarget,
        category: String,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Set or clear a Graph singleValueExtendedProperty. JMAP / IMAP
    /// / Gmail return `Err(AccountErrorKind::Unsupported)`.
    fn set_extended_property(
        &self,
        target: MutationTarget,
        property_id: String,
        value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Mark read state. Every protocol implements this; canonical
    /// across the four.
    fn set_is_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>>;

    // ------------------------------------------------------------
    // Mail composition primitives (S1-W1)
    // ------------------------------------------------------------

    /// Send an RFC 5322 message. JMAP `EmailSubmission/set`; Gmail
    /// `messages.send`; Graph `POST /me/sendMail`; IMAP via the
    /// configured `bifrost-smtp` transport.
    fn send_message(&self, request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>>;

    /// Streaming upload of an attachment. Returns a handle the
    /// consumer references in subsequent `SendRequest` /
    /// `DraftPatch` payloads. The `mime` argument is the
    /// Content-Type the server should record on the resulting
    /// attachment.
    fn attachment_upload(
        &self,
        bytes: AccountStream<Result<Bytes, AccountError>>,
        mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>>;

    /// Create a new draft.
    fn draft_create(&self, patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>>;

    /// Update an existing draft. The patch is partial: only `Some`
    /// fields are applied.
    fn draft_update(
        &self,
        draft: DraftHandle,
        patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Discard (delete) a draft without sending.
    fn draft_discard(&self, draft: DraftHandle) -> AccountFuture<Result<(), AccountError>>;

    /// Convert a draft into a sent message. The provider chooses
    /// atomicity; protocols that don't support atomic draft->send
    /// implement this as draft fetch + send + discard.
    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>>;

    // ------------------------------------------------------------
    // Search primitives (S1-W1)
    // ------------------------------------------------------------

    /// Thread-shaped search. Returns a page of `ThreadId`. Each
    /// call's page cursor is opaque; pass the previous page's
    /// `next_cursor` back to fetch the next page.
    fn search(&self, request: SearchRequest)
    -> AccountFuture<Result<Page<ThreadId>, AccountError>>;

    /// Message-shaped search using the same request AST.
    fn search_messages(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>>;

    // ------------------------------------------------------------
    // Container CRUD primitives (S1-W1)
    // ------------------------------------------------------------

    /// Enumerate containers (folders, labels, mailboxes).
    fn containers_list(&self) -> AccountFuture<Result<Vec<Container>, AccountError>>;

    /// Create a new container of the given `kind` with `name` under
    /// `parent`. Returns the engine-facing id.
    ///
    /// Contract: protocols that do not support nesting return
    /// `Err(AccountErrorKind::Unsupported)` when `parent` is `Some`.
    fn container_create(
        &self,
        kind: ContainerKind,
        name: String,
        parent: Option<ContainerId>,
    ) -> AccountFuture<Result<ContainerId, AccountError>>;

    /// Rename a container.
    fn container_rename(
        &self,
        container: ContainerId,
        name: String,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Move a container under a new parent. Folder-kind only;
    /// label-kind containers return `Err(AccountErrorKind::Unsupported)`.
    fn container_move(
        &self,
        container: ContainerId,
        new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Delete a container. Contract: a non-empty container either
    /// fails or moves contents to Trash; protocols MUST NOT silently
    /// drop messages.
    fn container_delete(&self, container: ContainerId) -> AccountFuture<Result<(), AccountError>>;

    // ------------------------------------------------------------
    // Settings primitives (S1-W1)
    // ------------------------------------------------------------

    /// List all sending identities on this account.
    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>>;

    /// Update one identity. `patch` is partial; only `Some` fields
    /// are applied.
    fn identity_update(
        &self,
        identity: IdentityId,
        patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>>;

    /// Read the vacation responder config, when supported.
    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>>;

    /// Replace the vacation responder config.
    fn vacation_set(&self, config: VacationConfig) -> AccountFuture<Result<(), AccountError>>;

    /// Read the storage quota readout, when supported.
    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>>;

    // ------------------------------------------------------------
    // Threading + hydration primitives (S1-W1)
    // ------------------------------------------------------------

    /// Hydrate every message in a thread. JMAP `Thread/get` +
    /// `Email/get`; IMAP `THREAD REFERENCES` + per-message FETCH;
    /// Gmail `threads.get`; Graph conversation API.
    fn thread_hydrate(
        &self,
        thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>>;

    /// Hydrate one message at a specific projection level.
    fn message_hydrate(
        &self,
        message: ObjectId,
        projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>>;

    // ------------------------------------------------------------
    // Conveniences (S1-W1)
    //
    // Default impls encode ratatoskr's product opinions. Each
    // dispatches into primitives and reads
    // `capabilities().conveniences` to pick the right one.
    //
    // Default-impl strategy notes (see S1-W1 report):
    //
    // - Single-call conveniences (`apply_label`, `remove_label`,
    //   `set_read`, `set_starred`, `mark_replied`, `mark_forwarded`)
    //   have a real default impl. `set_starred`, `mark_replied`,
    //   and `mark_forwarded` use capability-based dispatch through
    //   `capabilities().conveniences` rather than a hard-coded
    //   protocol switch, so consumers can override the dispatch
    //   shape per-account without subclassing the trait.
    // - Multi-call conveniences (`move_thread`, `delete_thread`)
    //   default to `Err(Unsupported)`: the boxed `'static` future
    //   returned by a trait method cannot borrow `&self` across the
    //   await boundary between the first and second primitive, and
    //   demanding every Account impl ship around in `Arc<Self>`
    //   shape was the wrong tradeoff. Protocol crates implement
    //   these in-crate where they have access to their own `Arc`-
    //   shaped handle.
    // ------------------------------------------------------------

    /// Move a thread between containers. `source` is the container
    /// being moved out of (`None` when the consumer is not tracking
    /// a source container, e.g. unifying a flat label-rendering UI).
    ///
    /// Default impl returns `Err(AccountErrorKind::Unsupported)`; protocol
    /// crates override with `add_to_container(target)` followed by
    /// `remove_from_container(source)`. Order: add-then-remove so a
    /// failure on the second step leaves the message in both
    /// containers (recoverable) rather than neither (lost).
    fn move_thread(
        &self,
        _thread: ThreadId,
        _target: ContainerId,
        _source: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported_error(AccountOperation::BulkMove)) })
    }

    /// Apply a label to `target`. Dispatches by `label.provenance`
    /// to the right primitive.
    fn apply_label(
        &self,
        target: MutationTarget,
        label: Label,
    ) -> AccountFuture<Result<(), AccountError>> {
        dispatch_label(self, target, label, true)
    }

    /// Remove a label from `target`. Same dispatch shape as
    /// `apply_label`.
    fn remove_label(
        &self,
        target: MutationTarget,
        label: Label,
    ) -> AccountFuture<Result<(), AccountError>> {
        dispatch_label(self, target, label, false)
    }

    /// Alias for `set_is_read`. Exists for naming symmetry with the
    /// other convenience setters.
    fn set_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.set_is_read(target, is_read)
    }

    /// Toggle the starred / flagged bit. Dispatches by
    /// `capabilities().conveniences.starred` to the right primitive.
    ///
    /// Capability-based dispatch (rather than a `match` on
    /// `ProtocolKind`) is the chosen design here: protocol crates
    /// know best which primitive corresponds to "starred" for their
    /// account (Graph has both `flag.flagStatus` and categories;
    /// IMAP has both `\Flagged` and Sieve flag-keyword adjuncts;
    /// custom deployments may differ). The capability shape lets
    /// the protocol crate declare the answer once at open-time
    /// rather than have the convenience embed a hard-coded provider
    /// switch.
    fn set_starred(
        &self,
        target: MutationTarget,
        starred: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        const STARRED_KEYWORD: &str = "$flagged";
        const STARRED_LABEL: &str = "STARRED";
        match self.capabilities().conveniences.starred {
            StarredFlagShape::Keyword => {
                self.set_keyword(target, STARRED_KEYWORD.to_string(), starred)
            }
            StarredFlagShape::LabelMembership => {
                self.set_label_membership(target, ContainerId(STARRED_LABEL.to_string()), starred)
            }
            StarredFlagShape::Category => {
                self.set_category(target, STARRED_KEYWORD.to_string(), starred)
            }
            StarredFlagShape::None => {
                Box::pin(async { Err(unsupported_error(AccountOperation::UpdateFlags)) })
            }
        }
    }

    /// Mark a message as replied. Default impl dispatches based on
    /// `capabilities().conveniences.replied_via_keyword` /
    /// `replied_via_extended_property`; if neither flag is set,
    /// returns `Err(Unsupported)` (Gmail's case: replied state is
    /// derived on sync, not a writeable flag).
    fn mark_replied(&self, message: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        const ANSWERED_KEYWORD: &str = "$answered";
        const PR_LAST_VERB_EXECUTED: &str = "PR_LAST_VERB_EXECUTED";
        const PR_LAST_VERB_REPLIED: &str = "102";
        let target = MutationTarget::Message(message);
        let conv = self.capabilities().conveniences;
        if conv.replied_via_keyword {
            return self.set_keyword(target, ANSWERED_KEYWORD.to_string(), true);
        }
        if conv.replied_via_extended_property {
            return self.set_extended_property(
                target,
                PR_LAST_VERB_EXECUTED.to_string(),
                Some(PR_LAST_VERB_REPLIED.to_string()),
            );
        }
        Box::pin(async { Err(unsupported_error(AccountOperation::UpdateFlags)) })
    }

    /// Mark a message as forwarded. Same dispatch shape as
    /// `mark_replied`.
    fn mark_forwarded(&self, message: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        const FORWARDED_KEYWORD: &str = "$forwarded";
        const PR_LAST_VERB_EXECUTED: &str = "PR_LAST_VERB_EXECUTED";
        const PR_LAST_VERB_FORWARDED: &str = "104";
        let target = MutationTarget::Message(message);
        let conv = self.capabilities().conveniences;
        if conv.forwarded_via_keyword {
            return self.set_keyword(target, FORWARDED_KEYWORD.to_string(), true);
        }
        if conv.forwarded_via_extended_property {
            return self.set_extended_property(
                target,
                PR_LAST_VERB_EXECUTED.to_string(),
                Some(PR_LAST_VERB_FORWARDED.to_string()),
            );
        }
        Box::pin(async { Err(unsupported_error(AccountOperation::UpdateFlags)) })
    }

    /// Move a thread to Trash, or delete-permanently if already in
    /// Trash.
    ///
    /// Default impl returns `Err(Unsupported)`. The wire-level
    /// dispatch ("trash if elsewhere; expunge if in Trash") needs
    /// to read the current container set, which is a chained call;
    /// the default impl cannot perform a chained call without
    /// borrowing `&self` across the await boundary into a `'static`
    /// future. Protocol crates ship this convenience using their
    /// own `Arc<Self>`-shaped handle.
    fn delete_thread(
        &self,
        _thread: ThreadId,
        _current: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported_error(AccountOperation::BulkDestroy)) })
    }

    /// Graceful local-handle teardown. IMAP `LOGOUT` + pool drain,
    /// JMAP WebSocket close, Graph subscription stream end, local
    /// worker shutdown. Idempotent: takes `&self` so it composes
    /// with `Arc<dyn Account>` and is safe to call more than once.
    ///
    /// Does NOT destroy durable server-side push subscriptions; those
    /// go through `push_unsubscribe` explicitly.
    fn close(&self) -> AccountFuture<Result<(), AccountError>>;
}

/// Engine-facing factory.
///
/// Consumers register one per account so the engine can perform
/// reopen cycles (capability change, transport reset) without
/// knowing protocol config. The engine owns the current open
/// `Arc<dyn Account>` and calls `open(account_id)` when it needs a
/// fresh one. The engine-minted `AccountId` is threaded through so
/// the protocol crate can attach to `bifrost-net` / `MeterSink` /
/// trace correlation under the right key on every reopen.
pub trait AccountFactory: Send + Sync + 'static {
    /// Open the account with the engine's identifier for it.
    ///
    /// The `AccountId` is the engine-minted handle the consumer
    /// passed to `SyncEngine::attach`. Protocol crates that wire
    /// `bifrost-net` (JMAP, Gmail, Graph), drive a `MeterSink`
    /// (IMAP, SMTP), or correlate logs to a per-account trace use
    /// it as the registration key. On reopen the engine calls
    /// `open` with the same id so attached resources can be
    /// re-registered against the same key.
    fn open(&self, account_id: AccountId) -> AccountFuture<Result<Arc<dyn Account>, AccountError>>;
}

/// Shared dispatch for `apply_label` / `remove_label`.
///
/// Reads `label.provenance` to decide which primitive to invoke.
/// The mapping is fixed at the trait surface because
/// `Provenance::kind` (Folder | Label) is the load-bearing signal:
/// a label-kind id under Gmail's provider is a label-membership
/// flip; under JMAP / IMAP it is a keyword flip; under Graph it is
/// a category (Graph categories are the closest analog to labels,
/// and Graph cannot express IMAP keywords). Folder-kind ids are
/// container-membership flips for everyone except Graph, which
/// uses categories for its folder-shaped labels too.
fn dispatch_label<T: Account + ?Sized>(
    receiver: &T,
    target: MutationTarget,
    label: Label,
    value: bool,
) -> AccountFuture<Result<(), AccountError>> {
    use crate::cursor::ProtocolKind;
    match (label.provenance.kind, label.provenance.provider) {
        (ContainerKind::Label, ProtocolKind::Gmail) => {
            receiver.set_label_membership(target, label.id, value)
        }
        (ContainerKind::Label, ProtocolKind::Graph) => {
            receiver.set_category(target, label.provenance.native, value)
        }
        (ContainerKind::Label, _) => receiver.set_keyword(target, label.provenance.native, value),
        (ContainerKind::Folder, ProtocolKind::Graph) => {
            receiver.set_category(target, label.provenance.native, value)
        }
        (ContainerKind::Folder, _) => {
            if value {
                receiver.add_to_container(target, label.id)
            } else {
                receiver.remove_from_container(target, label.id)
            }
        }
    }
}

/// Construct a canonical `AccountError` for an unsupported operation.
///
/// Used by default impls that have no provider-specific context. The
/// `AccountOperation` narrows the error so the engine and consumer know
/// which operation was rejected without having to infer it from the call site.
fn unsupported_error(op: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(op),
        Cause::Request(RequestCause::Unsupported { operation: op }),
    )
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}
