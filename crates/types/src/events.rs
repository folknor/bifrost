//! Stream event types shared between protocol crates and the engine.
//!
//! Every Account-trait batched stream yields `SyncEvent<T>`, where
//! `SyncEvent::Batch(Batch<T>)` is the dominant variant. The
//! checkpoint travels inside the batch so the consumer persists data
//! and cursor in one transaction. `push_stream` yields raw
//! `WatchEvent`s - those are wake-up signals, not paged data.

use std::time::Duration;

use crate::cursor::{ChangeCursor, CursorScope, MembershipScope};
use crate::error::{AccountError, Warning};
use crate::ids::{AccountId, ObjectId};
use crate::mutation::Fingerprint;

/// Cursor and backfill progress checkpoint persisted by the consumer.
///
/// Opaque to the consumer: protocol-owned bytes plus an envelope tag.
/// Carried inside `Batch` at advance boundaries; never travels as its
/// own event. Resuming from a checkpoint whose covering batch was not
/// durably written is unsafe.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Checkpoint {
    Change(ChangeCursor),
    Backfill(BackfillCheckpoint),
}

/// Backfill checkpoint. Partition-aware, finite. The engine's
/// backfill scheduler partitions newest-first so foreground mail is
/// hydrated before deep history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillCheckpoint {
    pub scope: CursorScope,
    pub partition: Partition,
    pub progress_marker: Option<crate::cursor::OpaqueProgressBytes>,
    pub progress: BackfillProgress,
    pub envelope_version: u32,
}

/// Backfill partition descriptor. Consumer-store key, opaque outside
/// the engine. `InventoryPartition` is the Account-facing shape; this
/// byte key is the durable checkpoint identity derived from it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Partition(pub Vec<u8>);

/// What an enumeration pass PROVED about the objects it walked past.
///
/// The load-bearing rule for inventory is not that failures are visible, it is
/// that no accepted checkpoint may certify coverage it does not have:
///
/// > Any accepted inventory progress checkpoint must certify that every
/// > provider result before that checkpoint was either materialized as an
/// > `InventoryEntry` or proved irrelevant to the inventory snapshot.
///
/// A checkpoint advances a cursor past the objects behind it, and the changes
/// stream only reports SUBSEQUENT changes - so an object the walk skipped
/// without recording becomes permanently invisible to that account. Surfacing
/// the failure to a consumer does not fix that; the checkpoint has to carry the
/// unresolved obligations with it, atomically, or not advance.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum InventoryCoverage {
    /// Every result before this point was materialized or definitively
    /// discharged. The checkpoint certifies full coverage.
    Complete,
    /// The walk advanced but left obligations open. The scope is live and
    /// DEGRADED: it converges and enters the change stream rather than
    /// re-walking forever, and the obligations are the rediscovery mechanism
    /// for what it could not represent.
    Degraded {
        obligations: Vec<InventoryObligation>,
    },
}

impl InventoryCoverage {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// One thing an enumeration pass could not account for.
///
/// Split by what is KNOWN, because that decides what repair is possible.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum InventoryObligation {
    /// A discovered object with a stable identity that could not be
    /// represented. Repairable by id: a later read either produces the entry
    /// or proves the object absent.
    Object {
        id: ObjectId,
        error: AccountError,
        /// Account-owned opaque repair token. The engine persists and returns
        /// it without interpreting it - only the protocol crate knows what a
        /// provider-native re-read of this object requires.
        repair: Vec<u8>,
    },
    /// A provider result that could not even be assigned an identity, or a page
    /// whose completeness could not be established.
    ///
    /// Deliberately NOT an `Object` with a synthetic id. An absent id may mean
    /// one malformed object, a schema mismatch affecting many, a truncated
    /// page, or a response that cannot be correlated with pagination at all -
    /// so calling it a single-object loss overstates what is known, and there
    /// is nothing to name, retry, or reconcile against. The obligation is
    /// therefore scoped to a replayable REGION instead.
    Region {
        /// Account-defined key naming the failure, for deduplicated operator
        /// reporting.
        failure_key: String,
        error: AccountError,
        /// Account-owned opaque token identifying the region to replay.
        replay: Vec<u8>,
    },
}

/// Account-facing inventory partition.
///
/// `Full` preserves the original one-pass inventory contract. Other
/// variants let the engine request a finite slice without coupling to
/// a specific protocol crate.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum InventoryPartition {
    Full,
    /// Time window as Unix epoch seconds, inclusive-exclusive.
    Time {
        from_unix_seconds: Option<i64>,
        to_unix_seconds: Option<i64>,
    },
    /// Inclusive UID range.
    Uid {
        from: u32,
        to: u32,
    },
    /// Count-based page range, inclusive-exclusive.
    Page {
        from: u32,
        to: u32,
    },
}

/// Partitioning modes an Account can honor for a scope.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum InventoryPartitioning {
    /// Only the original full `inventory_stream(scope)` pass is
    /// supported.
    Full,
    /// Account can honor `InventoryPartition::Time` windows.
    TimeWindowed,
    /// Account can honor `InventoryPartition::Uid` ranges. `max_uid`
    /// is optional because some protocol impls only know it after a
    /// folder open; the engine falls back to `Full` when it is absent.
    UidRange { max_uid: Option<u32> },
    /// Account can honor `InventoryPartition::Page` ranges. `total`
    /// is optional; when absent, the engine walks pages until a short
    /// page is observed. `page_size` lets the account cap engine
    /// requests at a protocol-advertised per-page maximum.
    PageCount {
        total: Option<u32>,
        page_size: Option<u32>,
    },
}

/// Progress within a single backfill partition.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackfillProgress {
    pub items_done: u64,
    pub items_estimated: Option<u64>,
}

/// Page boundary marker on a batch.
///
/// `kind` is metadata for observability. The cursor advance itself
/// lives on `Batch::checkpoint`, not here - keeping two encodings
/// (one in PageBoundary, one in Batch.checkpoint) let them disagree.
/// `Batch::checkpoint = Some(_)` IS the cursor-advance signal; the
/// page boundary just tells the engine what shape of boundary it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PageBoundary {
    /// Mid-page partial flush (e.g., max_wait elapsed before page
    /// filled). Batch::checkpoint is `None`.
    Partial,
    /// Natural page boundary as the protocol crate defines it.
    /// Batch::checkpoint MAY be `Some` if the cursor advanced.
    Page,
    /// Stream terminus. Batch::checkpoint carries the final cursor
    /// if there is one to carry.
    Final,
}

/// Streaming batch envelope.
///
/// The consumer atomically persists `(items, checkpoint.unwrap())` in
/// one transaction when `checkpoint` is `Some`.
#[derive(Debug, Clone)]
pub struct Batch<T> {
    pub items: Vec<T>,
    pub page_boundary: PageBoundary,
    pub server_latency: Duration,
    pub bytes_in: u64,
    pub checkpoint: Option<Checkpoint>,
}

/// Top-level event from every Account-trait stream.
///
/// `Done(Option<Checkpoint>)` carries the final checkpoint at stream
/// completion. `None` for streams that terminate before establishing
/// or advancing any cursor (a discovery stream, a no-op pass).
///
/// The `Batch` variant dominates traffic (one per page, every page);
/// `Progress` / `Warning` / `Terminated` / `Done` are rare. Boxing
/// the rare arms would slow the hot path without saving real memory,
/// so we accept the size asymmetry instead.
///
/// `Terminated(AccountError)` signals that this stream ends here. The
/// engine reads `error.recovery()` to decide what to do next. It is
/// distinct from `bifrost-types::error::Fatal`, which collapses any
/// terminal `RecoveryClass` at the engine boundary; stream termination
/// is not always a terminal error (the engine may retry or restart).
#[derive(Debug)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum SyncEvent<T> {
    Batch(Batch<T>),
    Progress(Progress),
    Warning(Warning),
    Terminated(AccountError),
    Done(Option<Checkpoint>),
}

/// Coarse, consumer-visible progress for long streams.
#[derive(Debug, Clone, Copy, Default)]
pub struct Progress {
    pub items_done: u64,
    pub bytes_in: u64,
    pub estimated_total: Option<u64>,
    pub eta: Option<Duration>,
}

/// Object change emitted by `changes_stream`.
///
/// No `memberships` field: scope changes are a separate variant.
#[derive(Debug, Clone)]
pub struct ObjectChange {
    pub id: ObjectId,
    pub kind: ObjectChangeKind,
}

/// Per-object change kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ObjectChangeKind {
    Created,
    Updated,
    Destroyed,
}

/// Object-to-container membership change.
///
/// `membership` stays a single `MembershipScope`, not a `Vec`
/// (adjudicated, likely WONTFIX): the single `Folder(id)` is the
/// correct routing membership - the engine's covering rule would not
/// cover a `Mailbox(owner)` tag against a folder cursor. Widening
/// would touch types + jmap + graph + engine and is only worth it if
/// change events must carry the owner tag for some future consumer.
#[derive(Debug, Clone)]
pub struct ScopeChange {
    pub id: ObjectId,
    pub membership: MembershipScope,
    pub kind: ScopeChangeKind,
}

/// Per-scope change kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScopeChangeKind {
    Added,
    Removed,
}

/// Sum type yielded by `changes_stream`. Consumers that care only
/// about object state filter to `ObjectChange`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Change {
    ObjectChange(ObjectChange),
    ScopeChange(ScopeChange),
}

/// Inventory entry yielded by `inventory_stream`.
///
/// Projection-only cold-start primitive. `memberships` is a `Vec`
/// because Gmail messages routinely sit in many labels and JMAP
/// messages can sit in many mailboxes.
#[derive(Debug, Clone)]
pub struct InventoryEntry {
    pub id: ObjectId,
    pub memberships: Vec<MembershipScope>,
    /// Message size in bytes. `Option` because Microsoft Graph does
    /// not expose `size` on the message resource; consumers fall
    /// back to `(server_version, flags_hash)` for diff there.
    pub size: Option<u64>,
    pub blob_id: Option<crate::ids::BlobId>,
    pub fingerprint: Fingerprint,
    pub thread_id: Option<crate::ids::ThreadId>,
    pub message_id: Option<String>,
    pub references: Vec<String>,
    pub in_reply_to: Option<String>,
}

/// Push wake-up event. Push surfaces are wake-ups, not change feeds.
///
/// `Terminated(AccountError)` carries a classified error when the push
/// stream ends and cannot be reconnected without engine intervention
/// (auth lost, subscription deleted, schema break). It is the push
/// equivalent of [`SyncEvent::Terminated`]. `Disconnected` /
/// `Reconnected` remain advisory: a transient transport drop emits
/// `Disconnected` followed by `Reconnected` once the renewer succeeds.
///
/// `Warning(Warning)` carries a structured advisory event (renewal
/// hiccups, throttle telemetry, etc.) without ending the stream. Push
/// transports that classify a transient error use this rather than
/// bare-`tracing::warn!` so consumers and the engine see a typed
/// signal.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum WatchEvent {
    Invalidated { hint: InvalidationHint },
    Disconnected,
    Reconnected,
    Terminated(AccountError),
    Warning(Warning),
}

/// Push payload, type-erased to a protocol-agnostic shape.
#[derive(Debug, Clone)]
pub struct InvalidationHint {
    pub source: PushSource,
    pub payload: HintPayload,
}

/// Which push transport originated the wake-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PushSource {
    JmapStateChange,
    ImapNotify,
    GmailPubsub,
    GraphSubscription,
    EwsStreaming,
    /// Engine-synthesized: the sink overflowed and the precise source
    /// could not be preserved; the wake-up is a coalesced full
    /// reconcile.
    Coalesced,
}

/// Hint about which scope was touched. Engine treats `Unknown` and a
/// specific hint identically in v1.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum HintPayload {
    SpecificCursorScope(CursorScope),
    SpecificMembership(MembershipScope),
    Unknown,
}

/// Sink for out-of-process push events (Gmail Pub/Sub listeners,
/// Graph webhook receivers). The consumer wires its receiver to
/// invoke `push` on every event; the engine merges sink-injected
/// events with in-process events into one logical push channel per
/// account.
pub trait InvalidationSink: Send + Sync + 'static {
    fn push(&self, account: AccountId, event: WatchEvent);
}

/// Consumer-to-producer control handle returned alongside every
/// engine-driven stream.
///
/// `pause` and `checkpoint_now` are async because they must wait for
/// "stream is at a safe boundary, here is the latest checkpoint if
/// one exists, you can now drop." An idle account that has never
/// produced a checkpoint returns `None`. `resume`, `priority`,
/// `bandwidth_cap`, and
/// `bandwidth_observed` stay synchronous (fire-and-forget signals or
/// pure reads).
pub trait Control: Send + Sync {
    fn pause(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<Checkpoint>, AccountError>> + Send + '_>,
    >;
    fn checkpoint_now(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<Checkpoint>, AccountError>> + Send + '_>,
    >;
    fn resume(&self);
    fn priority(&self, p: Priority);
    fn bandwidth_cap(&self, bps: Option<u64>);
    fn bandwidth_observed(&self) -> u64;
}

/// Scheduling priority on the four-lane scheduler.
///
/// `#[repr(u8)]` so the discriminant is stable for `AtomicU8`
/// storage in hot per-request paths (see `bifrost-net`'s
/// `AccountNet`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum Priority {
    /// User-visible; preempts background work.
    Foreground = 0,
    /// Default.
    Normal = 1,
    /// Backfill, archive-folder polling.
    Background = 2,
    /// Batch operations the user will not watch.
    Bulk = 3,
}

/// Per-account control signal exchanged between the engine and the
/// consumer. Carried on `AccountControlEvent` channels so the consumer
/// can pause an account (e.g. on operator-override directives) or
/// resume it after intervention.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AccountControl {
    Pause(PauseReason),
    Resume,
}

/// Reason an account is paused. Bounded enum, not a free-form string
/// or `DiagnosticText`: the engine is the producer and the set of
/// pause causes is enumerable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PauseReason {
    /// The protocol or engine surfaced `EngineDirective::
    /// OperatorOverrideRequired`. Consumer must resolve the underlying
    /// issue (typically reachable through the account stream's prior
    /// `Warning::OperatorAttentionNeeded`) before resuming.
    OperatorOverrideRequired,
    /// Consumer-initiated pause via `Control::pause` or equivalent.
    ConsumerRequested,
    /// Engine paused this account because a tenant-level throttle
    /// covers it. Resumes automatically once the throttle clears.
    TenantThrottle,
    /// Engine exhausted its retry budget for a recurring failure;
    /// consumer intervention required before further attempts.
    RetryBudgetExhausted,
}
