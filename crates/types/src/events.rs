//! Stream event types shared between protocol crates and the engine.
//!
//! Every Account-trait batched stream yields `SyncEvent<T>`, where
//! `SyncEvent::Batch(Batch<T>)` is the dominant variant. The
//! checkpoint travels inside the batch so the consumer persists data
//! and cursor in one transaction. `push_stream` yields raw
//! `WatchEvent`s - those are wake-up signals, not paged data.

use std::time::Duration;

use crate::coverage::{CoverageDomain, CoverageOutcome, InventoryCoverageReport};
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

/// One page of an inventory walk, plus what the walk PROVED up to here.
///
/// Distinct from `Batch<InventoryEntry>` because a page of an enumeration has
/// to say more than a page of anything else: an advancing checkpoint must name
/// every unresolved obligation preceding it, in the same event, or the
/// checkpoint certifies coverage it does not have.
///
/// `coverage` describes THIS batch and everything before it, not just this
/// batch. Obligations ride checkpoint-bearing batches rather than only the
/// terminal completion because Graph and `BackfillRunner` both advance
/// per page - waiting for `Done` would let a page checkpoint become durable
/// across a gap it never declared.
#[derive(Debug, Clone)]
pub struct InventoryBatch {
    pub items: Vec<InventoryEntry>,
    pub page_boundary: PageBoundary,
    pub server_latency: Duration,
    pub bytes_in: u64,
    pub checkpoint: Option<Checkpoint>,
    pub coverage: InventoryCoverageReport,
}

/// How an inventory walk ended.
///
/// Deliberately NOT expressed as `Done(None)`. That already means "this stream
/// established no cursor" for discovery and no-op passes, maps to
/// `FusionOutcome::NoCursor`, and loses the reason - which invites a caller to
/// read end-of-stream as success. An incomplete walk is a different fact from
/// an empty one and gets its own carrier.
#[derive(Debug, Clone)]
pub struct InventoryCompletion {
    pub checkpoint: Option<Checkpoint>,
    /// Coverage for the walk as a whole. `Degraded` means the enumeration
    /// space was NOT exhausted cleanly, so a consumer must not read `Done` as
    /// proof of completeness and the engine must not write a backfill
    /// completion sentinel.
    pub coverage: InventoryCoverageReport,
}

impl InventoryCompletion {
    /// A walk that exhausted the whole of `scope` with nothing left
    /// unaccounted for.
    ///
    /// Takes the scope because a completeness claim is only meaningful about a
    /// stated extent: "complete" is a fact about a region of one enumeration,
    /// not about a scope in the abstract, and a claim with no domain cannot be
    /// checked against the debt it would discharge.
    #[must_use]
    pub fn complete(scope: CursorScope, checkpoint: Option<Checkpoint>) -> Self {
        Self {
            checkpoint,
            coverage: InventoryCoverageReport::complete(scope),
        }
    }
}

/// Inventory's own stream envelope.
///
/// Inventory does not use `SyncEvent<T>`. The generic envelope is shared by
/// hydration, changes, discovery, blobs and mutation, and coverage is
/// meaningless to all of them - adding an arm or a field there would force an
/// irrelevant lane onto every consumer of every stream, which is precisely the
/// "stale consumer policy silently applies to a new lane" hazard `ItemOutcome`
/// is documented against.
///
/// It equally cannot live in the element type alone: an element can report an
/// obligation, but only the envelope can say whether a later checkpoint
/// includes that obligation atomically, or whether the stream finished with
/// coverage unproven.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum InventoryEvent {
    Batch(InventoryBatch),
    Progress(Progress),
    Warning(Warning),
    /// The walk cannot continue at all. Distinct from finishing with degraded
    /// coverage: this is a whole-response failure, not an item or region one.
    Terminated(AccountError),
    Done(InventoryCompletion),
}

/// Lift a generic stream event into the inventory envelope, asserting COMPLETE
/// coverage over `domain`.
///
/// The domain is the caller's to state, and a PARTITION walk must not pass a
/// full-scope domain: a partition that finished cleanly proves nothing about
/// the partitions either side of it, and a `Full` claim would discharge their
/// debt on the strength of an unrelated range's success.
///
/// For producers that have no way to express an unresolved obligation: their
/// behaviour is to terminate the whole walk on any failure, which never
/// advances a checkpoint across a gap, so claiming `Complete` is accurate for
/// them. A producer that starts absorbing failures and continuing MUST stop
/// using this and build `InventoryBatch` / `InventoryCompletion` itself -
/// otherwise it reports full coverage over a walk that skipped something, which
/// is the exact failure this envelope exists to make impossible.
///
/// Deliberately a scope-taking adapter rather than a `From` impl. A bare
/// `.into()` manufactured a completeness claim out of nothing, with no extent
/// attached and no syntax at the call site to notice - the same shape as every
/// other implicit-`Complete` path that let a durable record certify coverage it
/// did not have.
pub fn lift_complete_walk(
    domain: CoverageDomain,
) -> impl FnMut(SyncEvent<InventoryEntry>) -> InventoryEvent {
    let report = move || InventoryCoverageReport {
        domain: domain.clone(),
        outcome: CoverageOutcome::Complete,
    };
    move |event| match event {
        SyncEvent::Batch(batch) => InventoryEvent::Batch(InventoryBatch {
            items: batch.items,
            page_boundary: batch.page_boundary,
            server_latency: batch.server_latency,
            bytes_in: batch.bytes_in,
            checkpoint: batch.checkpoint,
            coverage: report(),
        }),
        SyncEvent::Progress(progress) => InventoryEvent::Progress(progress),
        SyncEvent::Warning(warning) => InventoryEvent::Warning(warning),
        SyncEvent::Terminated(error) => InventoryEvent::Terminated(error),
        SyncEvent::Done(checkpoint) => InventoryEvent::Done(InventoryCompletion {
            checkpoint,
            coverage: report(),
        }),
    }
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
