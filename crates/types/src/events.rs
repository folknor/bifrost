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
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Checkpoint {
    Change(ChangeCursor),
    Backfill(BackfillCheckpoint),
}

/// Backfill checkpoint. Partition-aware, finite. The engine's
/// backfill scheduler partitions newest-first so foreground mail is
/// hydrated before deep history.
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone, Copy, Default)]
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
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum WatchEvent {
    Invalidated { hint: InvalidationHint },
    Disconnected,
    Reconnected,
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
/// "stream is at a safe boundary, here is the checkpoint, you can
/// now drop." `resume`, `priority`, `bandwidth_cap`, and
/// `bandwidth_observed` stay synchronous (fire-and-forget signals or
/// pure reads).
pub trait Control: Send + Sync {
    fn pause(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Checkpoint, AccountError>> + Send + '_>,
    >;
    fn checkpoint_now(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Checkpoint, AccountError>> + Send + '_>,
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
