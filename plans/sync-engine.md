# Sync engine

Bifrost is designed for accounts with 150-300 GB of mail, millions of
messages, and hundreds of new arrivals per day. At that scale the
hard part of an email client is not "parse the protocol" - it is
"make a multi-hour sync survivable, resumable, prioritizable, and
debuggable." This document defines the engine that owns that work.

## Three layers

Bifrost is three crate tiers, not one:

- `bifrost-net` - shared transport. HTTP/2 connection pool, OAuth
  refresh under load, retry budget with `Retry-After` honor, rate-
  limit governor, bandwidth meter, TLS, tracing context propagation.
  Used by every protocol crate. Not buried inside the engine, because
  one-shot sends, account discovery, and ad-hoc blob downloads use it
  too.
- `bifrost-{jmap,imap,gmail,graph,smtp}` - wire primitives. Per-
  protocol operations, per-protocol cursor types, per-protocol
  capability flags. No orchestration. Streams here are the data
  plane the engine drives.
- `bifrost-sync` - the engine. Scheduler, checkpoint persistence,
  multi-folder multiplexing, backfill partitioning, push
  reconciliation, retry policy, observability. Exposes the `Account`
  trait the protocol crates implement and the `SyncRun` state
  machine consumers drive.

The orchestration layer lives in bifrost, not in consumers. The
patterns are stable across protocols and reimplementing them per-
consumer drifts subtly out of sync over time.

The `Account` trait the protocol crates implement, the `CursorScope`
and `MembershipScope` types the engine multiplexes, and the load-
bearing types referenced throughout this document (`Fingerprint`,
`PageBoundary`, `InvalidationHint`, `IdempotencyKey`, `Digest`,
`CursorDescriptor`, `AccountCapabilities`, `ByteRange`,
`MutationCapabilities`) are defined in `plans/account-trait.md`.
Read it alongside this document; the two are paired.

## The stream contract

Every stream surface yields `SyncEvent<Batch<T>>`. The checkpoint
travels *inside* the batch, not as a separate event, so the consumer
persists data and cursor in one transaction without staging:

```rust
enum SyncEvent<T> {
    Batch(Batch<T>),
    Progress(Progress),
    Warning(NonFatal),
    Fatal(Error),
    Done(Checkpoint),    // final checkpoint at stream end
}

struct Batch<T> {
    items: Vec<T>,
    page_boundary: PageBoundary,
    server_latency: Duration,
    bytes_in: u64,
    checkpoint: Option<Checkpoint>,  // Some at advance boundaries
}
```

Rationale:

- Batches are the natural unit of cost on every protocol. A page is
  one round-trip, one cursor advance, one bounded buffer. Per-item
  streams pretend the consumer can pause between any two items; in
  reality the producer is holding a page in memory and the cursor
  advances at page granularity. Item streams are convenience
  adapters over batch streams via `.flatten()`, not the primitive.
- `checkpoint: Option<Checkpoint>` is `Some` at boundaries where
  `PageBoundary::cursor_delta == Some(Advanced(..))`, `None` at
  mid-page or non-advance boundaries. The consumer atomically
  persists `(items, checkpoint.unwrap())` in one transaction when
  `checkpoint` is `Some`. This contract is implementable as written
  (no staged in-memory state between events).
- `Progress` carries items-done, bytes-in, estimated-total, ETA.
- `Warning` is recoverable per-item or per-batch failure that did
  not stop the stream. `Fatal` ends the stream.
- `Done` carries the final checkpoint at stream completion.

Alongside each stream, the engine returns a `Control` handle for
consumer-to-producer signaling:

```rust
struct Control {
    async fn pause(&self) -> Result<Checkpoint, Error>;
    async fn checkpoint_now(&self) -> Result<Checkpoint, Error>;
    fn resume(&self);
    fn priority(&self, p: Priority);
    fn bandwidth_cap(&self, bps: Option<u64>);
    fn bandwidth_observed(&self) -> u64;
}

enum Priority {
    Foreground,    // user-visible; preempts background work
    Normal,        // default
    Background,    // backfill, archive-folder polling
    Bulk,          // batch operations the user will not watch
}
```

`pause()` and `checkpoint_now()` are async because the consumer must
await "stream is at a safe boundary, here is the checkpoint, you can
now drop." Void methods could not satisfy the graceful-shutdown
contract. `resume`, `priority`, `bandwidth_cap`, and
`bandwidth_observed` stay synchronous; they are fire-and-forget
signals or pure reads.

`Control::priority` is partly cosmetic on IMAP: an in-flight FETCH
cannot be reprioritized, only the next folder pick can. The handle
honors priority at the next safe boundary, which on IMAP is between-
folder rather than between-batch. Document this asymmetry where the
IMAP `Account` impl lands.

## State model: change vs backfill

Two distinct cursor types, never conflated:

```rust
struct ChangeCursor<T> {
    scope: CursorScope,
    server_state: ServerState<T>,                  // opaque
    advanced_through: Option<OpaqueProgressBytes>, // protocol-owned
    envelope_version: u32,
}

struct BackfillCheckpoint {
    scope: BackfillScope,                          // membership + window
    partition: Partition,                          // newest-first id
    progress_marker: Option<OpaqueProgressBytes>,  // protocol-owned
    progress: BackfillProgress,
    envelope_version: u32,
}
```

Rules:

- A `ChangeCursor` advances only on consumer ack of the next batch
  (via durable persistence of the batch + checkpoint).
- `advanced_through` is protocol-owned opaque bytes. The protocol
  writes it when emitting `Checkpoint` inside a `Batch`; the engine
  treats it as opaque; on resume the protocol interprets it. For
  protocols where mid-page resumption is impossible (cursor
  advances only at page granularity), `advanced_through` is always
  `None`.
- A `BackfillCheckpoint` is consumer-managed, partition-aware,
  finite. The engine's backfill scheduler partitions newest-first
  (most-recent N days, then next N, etc.) so foreground newer mail
  is hydrated before deep history.
- Live change tracking starts before backfill on protocols where
  cursor establishment is cheap (JMAP, Gmail, Graph: server-issued
  cursor in O(1)). On IMAP without QRESYNC, cursor establishment is
  itself an inventory pass (see Multi-folder multiplexing); the
  engine fuses the two operations rather than ordering them.
- **Checkpoint atomicity invariant.** `Checkpoint` is carried
  *inside* `Batch` at advance boundaries. The consumer must persist
  `(items, checkpoint)` in one transaction; resuming from a
  checkpoint whose covering batch was not durably written is unsafe.
  The engine emits `Some(checkpoint)` only when
  `PageBoundary::cursor_delta == Some(Advanced(..))`.
- **Envelope version.** Both cursor types carry `envelope_version`
  so the engine can detect a cursor written by an older schema and
  migrate it (or reject with `Fatal(SchemaIncompatible)`). At the
  volume target, "format changed, throw away the cursor" means a
  20-hour re-sync; the envelope is the migration story.

## Change taxonomy

Email has two orthogonal change axes; one variant cannot represent
both:

```rust
struct ObjectChange {
    id: ObjectId,
    kind: ObjectChangeKind,  // Created | Updated | Destroyed
}

struct ScopeChange {
    id: ObjectId,
    membership: MembershipScope,  // Folder | Label | Mailbox | Query
    kind: ScopeChangeKind,        // Added | Removed
}
```

Gmail label removal is `ScopeChange::Removed`, not `Destroyed`. JMAP
queryChanges removal is `ScopeChange::Removed` from a query
membership. IMAP expunge in one folder of a label-style server is
`ScopeChange` on that folder, not object destruction. A single
change stream yields both variants; consumers that care only about
object state filter to `ObjectChange`.

**Emission rule.** The protocol crate emits what the protocol
observed; the engine derives `Destroyed` from per-object membership
tracking:

- JMAP emits `Destroyed` directly (`Email/changes` returns
  destroyed ids). Per-mailbox membership changes flow as
  `ScopeChange` with `MembershipScope::Mailbox(_)`.
- Gmail emits `ScopeChange::Added` / `Removed` for label changes;
  `Destroyed` when history records the deletion.
- IMAP emits `ScopeChange::Removed` per folder. Engine tracks per-
  object memberships across all syncing membership scopes and
  derives `Destroyed` when the last reports `Removed`. Pure-IMAP
  servers have one folder per message at the protocol level, so
  the derivation usually collapses to one step. Engine derivation
  is fallibly incomplete: messages in unsynced folders or shared
  mailboxes stay `Removed`-not-`Destroyed`, the correct level of
  fallibility.
- Graph emits `ScopeChange::Added` / `Removed` per folder; engine
  derives `Destroyed` likewise.

See `plans/account-trait.md` -> Change emission for the full rule
including reference to membership tracking.

## Inventory as a first-class primitive

Initial sync of a 300 GB account against an item-fetch stream is a
20-hour cold start. The right primitive is projection-only with
memberships:

```rust
trait Inventory {
    fn inventory_stream(&self, scope: CursorScope)
        -> impl Stream<Item = SyncEvent<Batch<InventoryEntry>>>;
}

struct InventoryEntry {
    id: ObjectId,
    memberships: Vec<MembershipScope>,   // labels, mailboxes, folder
    size: u64,
    blob_id: Option<BlobId>,
    fingerprint: Fingerprint,
    thread_id: Option<ThreadId>,         // protocol-native, opaque
    message_id: Option<String>,          // RFC 5322
    references: Vec<String>,             // RFC 5322
    in_reply_to: Option<String>,         // RFC 5322
}
```

Implementations: JMAP `Email/query` + `Email/get` with `properties=
[id, mailboxIds, threadId, blobId, size, keywords, messageId,
references, inReplyTo]` (`mailboxIds` becomes `memberships`); IMAP
`UID FETCH 1:* (FLAGS MODSEQ RFC822.SIZE BODY.PEEK[HEADER.FIELDS
(MESSAGE-ID REFERENCES IN-REPLY-TO)])` (singleton membership);
Gmail `messages.list` + `messages.get?fields=...` (`labelIds`
becomes `memberships`); Graph delta with `$select=...,
parentFolderId,...` (singleton membership). Cost: roughly 2-3
orders of magnitude cheaper than full-object fetch.

`memberships` is a `Vec`, not an `Option`. Gmail messages routinely
sit in many labels; JMAP messages can sit in many mailboxes. Without
this the engine cannot show folder listings, cannot track
membership for `Destroyed` derivation, and cannot present the
account to the user the way the protocol presents it.

Threading headers are included at inventory cost. Native `thread_id`
covers JMAP and Gmail (each opaque within its protocol); `references`
and `in_reply_to` let the consumer thread IMAP and Graph without a
separate fetch round-trip, and let the consumer thread cross-
protocol where native thread ids are not interchangeable.

Inventory is distinct from `Projection::Metadata` even though both
are projection-only. Inventory is the cold-start diff primitive
optimized for "have I seen this, what does it belong to, what does
it thread to"; `Metadata` is the next-step-up hydration that adds
Subject, From / To / Cc, Date, and flags for folder-listing UI. The
engine emits inventory once per scope at cold start and `Metadata`
on demand afterwards. Folding them would force cold start to pay
envelope-header cost it does not need.

## Cursor introspection

Cursors are inspected via `Account::describe_cursor`, not as methods
on the cursor itself - cost depends on capability state (whether
QRESYNC is enabled, whether historyId is fresh, etc.), not the
cursor in isolation:

```rust
trait Account {
    fn describe_cursor(&self, cursor: &ChangeCursor<Self::ChangeState>)
        -> CursorDescriptor;
}

struct CursorDescriptor {
    cost_class: CostClass,         // Cheap | Medium | Expensive
    strategy: SyncStrategy,
    freshness: Option<Instant>,
}

enum CostClass { Cheap, Medium, Expensive }
```

The cost differential between QRESYNC and the no-extension fallback
is four orders of magnitude on a 200K-message folder. The
consumer's scheduling, its UI ("a minute" vs "an hour"), and its
retry policy all need to see this. Same applies cross-protocol: a
stale Gmail `historyId` that will fall back to full resync is
`Expensive`; a fresh JMAP `State` is `Cheap`.

Per-protocol cursor representation (IMAP three-state lifecycle,
JMAP State, Gmail historyId, Graph deltaLink) is internal to each
protocol crate. The descriptor is the engine-facing view.

For IMAP specifically:

```rust
// Inside bifrost-imap, internal:
enum FolderCursor {
    QResync   { uidvalidity: u32, modseq: u64 },
    Condstore { uidvalidity: u32, modseq: u64, known_uids: CompactUidSet },
    Basic     { uidvalidity: u32, uidnext: u32, known_uids: CompactUidSet },
}

// Run-length-encoded inclusive UID ranges. IMAP servers issue UIDs
// roughly monotonically with sparse expunge gaps; RLE handles this
// case in O(gaps) memory rather than O(uids). Must serialize to
// under 64 KB for a 200K-UID folder (typical: 1-2 KB at sparse-gap
// density). v1 representation; revisit if production folders show
// pathological fragmentation.
struct CompactUidSet(Vec<RangeInclusive<u32>>);
```

`describe_cursor` for IMAP returns `cost_class = Cheap` for
`QResync`, `Medium` for `Condstore`, `Expensive` for `Basic`.

## Body fetch: projection + lazy blob

Two-stage body handling:

```rust
enum Projection {
    Metadata,         // headers + flags + size, no body
    Preview(usize),   // metadata + N bytes of decoded text
    TextOnly,         // metadata + full text/plain part
    Full,             // all parts decoded, no attachment blobs
    FullWithBlobs,    // full plus inline attachment bytes
}

struct BlobHandle {
    id: BlobId,
    size: Option<u64>,
    content_type: Option<String>,
    digest: Option<Digest>,
    capabilities: BlobCapabilities,
}

impl BlobHandle {
    fn open(&self) -> impl Stream<Item = SyncEvent<Bytes>>;
    fn open_range(&self, range: ByteRange) 
        -> impl Stream<Item = SyncEvent<Bytes>>;
    fn download_to(&self, path: &Path, opts: DownloadOpts)
        -> impl Future<Output = Result<(), Error>>;
}

struct ByteRange {
    start: u64,
    length: Option<u64>,    // None = open-ended to end of blob
}

struct BlobCapabilities {
    supports_range: bool,
    supports_parallel: bool,
    digest_available_pre_download: bool,
    encoding: BlobEncoding,
}
```

Projection normalizes `EmailBodyProperties` (JMAP) /
`BODYSTRUCTURE` + `BODY.PEEK[]` (IMAP) / `format=METADATA` (Gmail) /
`$select` (Graph) into one consumer-facing enum. The engine maps to
the best protocol-native expression.

Blob handles separate enumeration from hydration. The inventory
stream yields handles; the consumer opens them on demand. For "put
this on disk reliably," `download_to` does parallel range fetches
where `supports_parallel` is true and falls back to single-stream
where it is not.

`BlobHandle::download_to` is an engine-level utility on top of
`Account::open_blob_range` (defined in `plans/account-trait.md`),
not a trait method. The Account trait offers `open_blob` and
`open_blob_range`; the engine composes range fetches into the
download-to-path convenience. Each parallel range fetch validates
`Content-Range` on the response so the engine detects misaligned
bytes before they corrupt a parallel-assembled download.

Capability-tagged honesty matters: Gmail attachments are base64url
in JSON, not raw chunked bytes - range resume is not generally
available there and the capability flag says so. Graph `$value`
works only on file attachments. JMAP HTTP download may or may not
honor `Range` depending on server. The engine does not pretend
uniformity the protocols do not provide.

The same honesty applies to digest-based dedup. `BlobHandle::digest`
is `Some` only when `digest_available_pre_download` is true; the
consumer can dedup against its own digest store before opening the
stream. Where the capability is false, the consumer must post-hash
the bytes after download.

## Push is invalidation, never truth

Push surfaces are wake-ups, not change feeds:

```rust
enum WatchEvent {
    Invalidated { hint: InvalidationHint },
    Disconnected,
    Reconnected,
}

// Engine-side sink for out-of-process push delivery.
trait InvalidationSink {
    fn push(&self, account: AccountId, event: WatchEvent);
}
```

Two push paths per the account-trait `push_in_process` capability:

- **In-process** (IMAP IDLE, JMAP WebSocket, EWS streaming
  subscription). The protocol crate's `Account::push_stream`
  delivers `WatchEvent` directly. The engine reads from the stream.
- **Out-of-process** (Gmail Pub/Sub, Graph webhooks). The protocol
  crate manages subscription CRUD (`Account::push_subscribe` /
  `push_unsubscribe`) but does not see the events. The consumer
  wires a Pub/Sub listener or webhook receiver to the engine's
  `InvalidationSink`; the engine merges sink-injected events with
  in-process events into one logical push channel per account.

The reconciler runs `changes_stream` against the freshly-advanced
`ChangeCursor` on every `Invalidated`. There is no "resolved push
event" surface. This rule is uniform across both paths and across
all five push sources (JMAP `StateChange`, IMAP IDLE/NOTIFY, Gmail
Pub/Sub, Graph webhooks, EWS streaming). Per-protocol push payload
detail is preserved inside `InvalidationHint` for engines that can
act on it (e.g. "only the Inbox state changed") but is never the
source of truth.

On QRESYNC-IDLE this discards authoritative `* VANISHED` UID data
that arrives in-band on the IDLE stream; the engine accepts that
bandwidth cost in v1 for protocol uniformity. A future
`InvalidationHint::Authoritative(payload)` variant could let the
engine apply the in-band data directly and skip the reconcile
round-trip, but that is a v2 optimization, not a v1 requirement.

## Bulk mutation

Write paths use the same shape as read paths, with streaming input
so engine-driven mutation pipelines backpressure cleanly:

```rust
trait Mutate {
    fn bulk_set_flags_stream(
        &self,
        targets: impl Stream<Item = ObjectId> + Send,
        flags: FlagSet,
        op: FlagOp,
        idempotency: IdempotencyKey,
    ) -> impl Stream<Item = SyncEvent<Batch<MutationResult>>>;

    fn bulk_move_stream(...) -> ...;
    fn bulk_destroy_stream(...) -> ...;
}

struct MutationResult {
    id: ObjectId,
    outcome: MutationOutcome,  // Applied | Skipped | Failed(Error)
}
```

At 5M messages, "mark 200K as read" is a streaming problem with the
same concerns as read-side streams: batching, rate-limit honor,
partial success, retry. Consumers with a static list adapt via
`stream::iter(vec)`.

Two orthogonal mutation concerns, distinguished by capability:

- **Optimistic concurrency** (`MutationConcurrency::StateBased`)
  prevents lost updates when state changed under the client: JMAP
  `ifInState`, Graph `If-Match: <etag>`, IMAP `STORE UNCHANGEDSINCE`.
  The mutation fails if the state has advanced; the consumer must
  re-read and retry.
- **Replay safety** (`MutationReplaySafety::ReplayToken`) prevents
  double-apply on transient-failure retry: Gmail
  `X-Goog-Request-Id`, Graph batch request-id. The server dedupes
  exact retries on the token.

`IdempotencyKey` (defined in `plans/account-trait.md`) is engine
bookkeeping that compiles to a `ReplayToken` where the protocol
supports it, otherwise the engine reads back affected items after
retry. Partial-success surfaces let the consumer recover from "12
of 5000 failed."

SMTP bulk send (mailing lists, scheduled sends, retry queues) has
the same shape but is out of scope for v1. Send remains request /
response in `bifrost-smtp`; a future `bulk_send_stream` on the
engine can layer on top once the read-side mutation contract is
proven in production.

## Multi-folder multiplexing

Enterprise mailboxes expose tens to hundreds of scopes (filing
trees, shared mailboxes, public folders, per-type cursors). The
engine queries `Account::discover_cursor_scopes()` for the initial
bounded enumeration of what to track changes for, queries
`Account::discover_memberships()` for what to show the user (folder
trees, labels, mailboxes), subscribes to
`Account::scope_lifecycle_stream()` for ongoing created / renamed /
deleted events, and multiplexes one `changes_stream` per cursor
scope. Pushing this orchestration to consumers means every consumer
reimplements the same multiplexer, badly.

`bifrost-sync` exposes one `account_changes_stream` per account.
Internally:

- Holds IDLE on the most-active cursor scope (IMAP).
- Round-robins NOOP/STATUS across the rest with adaptive cadence
  (active scopes polled more frequently than archives).
- Surfaces scope lifecycle events from `scope_lifecycle_stream` onto
  the unified output stream.
- Yields `Change` (`ObjectChange` or `ScopeChange`) tagged with the
  cursor scope of origin, regardless of source protocol.

On IMAP without QRESYNC, change-cursor baseline establishment is
itself an inventory pass: `UID FETCH 1:* (FLAGS MODSEQ RFC822.SIZE
...)` produces both the cursor anchor and the per-message
fingerprints for the diff. The capability flag
`inventory_is_change_cursor_establish` signals this fusion. The
engine schedules differently when it is true: the "start cheap,
backfill underneath" pattern does not apply because the cheap-start
step does not exist.

The same multiplexer handles cross-protocol cases: JMAP push +
per-type cursor, Gmail watch + history list, Graph subscriptions +
per-folder delta. Per-protocol the strategy differs; the consumer-
facing shape is uniform.

## Backpressure

Stated bounded-buffering must be actually bounded. The work is not
a type swap; it is a driver-design change for IMAP specifically:

- IMAP streaming FETCH currently uses `UnboundedSender` in
  `crates/imap/src/connection/dispatch/fetch.rs` because the
  current consumer hook is synchronous in
  `crates/imap/src/connection/dispatch.rs`. Converting to a bounded
  sender at that boundary risks blocking the driver or dropping
  data. The driver must learn to slow its read pump when downstream
  is full, yield the socket without dropping data, and respect
  backpressure cooperatively with IMAP's continuation-request
  protocol. This is design work that lands in `bifrost-imap`
  alongside the sync-engine plumbing; the current code is rewrite
  fodder for this task, not a constraint.
- JMAP blob download in `crates/jmap/src/core/transport.rs`
  materializes `Bytes` rather than chunking. Reqwest `bytes_stream`
  exists; the swap is simpler than the IMAP case.
- Every internal channel carries a documented capacity and a spill-
  to-disk option for slow consumers. Default: 64 batches in flight,
  no spill. Consumer can configure.

Backpressure flows: consumer poll on the outer `Stream` slows the
batch producer, which slows the wire fetcher, which yields the
HTTP/2 stream window or IMAP read pump. No layer accumulates
unboundedly.

## Cancellation taxonomy

Drop-is-cancel is the floor, not the ceiling. Four distinct exits:

- **Drop.** Abort. Underlying transport torn down without protocol-
  level close. Cursor advance since last `Batch` with checkpoint is
  discarded.
- **`Control::pause().await`.** Graceful pause at next batch
  boundary. Returns the latest durable `Checkpoint`; stream resumes
  on `Control::resume`.
- **`Control::checkpoint_now().await + Drop`.** Graceful shutdown.
  Engine drives to the next safe boundary, returns the final
  `Checkpoint`, ends with `Done`.
- **Fatal error.** Stream ends with `SyncEvent::Fatal(Error)`. The
  error type carries a recovery class derived from the protocol
  crate's `RecoveryClass` emission (defined in
  `plans/account-trait.md` -> Recovery vocabulary). The engine-
  facing Fatal taxonomy is the coarse projection:
  - `Network` (retry next run, same cursor).
  - `AuthLost` (consumer must re-authenticate; cursor preserved).
  - `ScopeInvalidated` (IMAP UIDVALIDITY change, Graph deltaLink
    expired): engine discards the cursor for that scope and re-
    establishes from inventory. Other scopes unaffected.
  - `CursorExpired` (Gmail stale `historyId` >7 days, Graph stale
    delta token): full resync for the affected scope.
  - `SchemaIncompatible` (cursor envelope_version older than the
    engine can migrate): full resync, but the consumer can preserve
    inventory and only re-establish the cursor.
  - `CapabilityChanged` (mid-session capability transition): engine
    re-opens the account, reads capabilities fresh, restarts streams.

  For protocols with `CursorScope::Account` (Gmail),
  `ScopeInvalidated` / `CursorExpired` / `RestartAccount` collapse
  into one recovery action: full resync. The Fatal variants stay
  distinct for observability; the engine's response is identical.

  Layering: the protocol crate handles fine-grained recovery
  internally (e.g., QRESYNC-to-CONDSTORE downgrade on iCloud
  `ENABLE` failure) without surfacing Fatal; it emits a
  `Warning::StrategyDowngraded` for observability. The engine
  surfaces only the coarse classes above.

`Drop` cannot await; `pause` and `checkpoint_now` can. The handle
exists precisely so the consumer can choose graceful exit when
possible.

## Observability

Designed in, not bolted on. Pinned specifics:

- **Tracing crate.** `tracing` (https://docs.rs/tracing). One span
  per stream; cursor identity, scope, and account-id as span
  attributes. Span name `bifrost.sync.{operation}` where operation
  is `inventory | changes | hydrate | blob | mutate | push`.
- **Metric naming.** Prefix `bifrost_sync_`. Counters:
  `bifrost_sync_items_total`, `bifrost_sync_bytes_total`,
  `bifrost_sync_warnings_total`, `bifrost_sync_retries_total`.
  Histograms: `bifrost_sync_batch_latency_seconds`,
  `bifrost_sync_page_size_items`,
  `bifrost_sync_page_size_bytes`. Labels: `account_id`, `protocol`,
  `scope`, `operation`.
- **Trace propagation.** W3C `traceparent` header for HTTP-based
  protocols (JMAP, Gmail, Graph). `bifrost-net` injects and
  propagates. IMAP has no trace header; the span lives entirely
  inside the process.
- **Warning schema.** Structured fields: `kind`, `recovery_class`,
  `retry_count`, `next_action`, `protocol_detail`. Named variants
  include `StrategyDowngraded { from, to, reason }` (e.g., QRESYNC
  to CONDSTORE on iCloud `ENABLE` failure - the protocol crate
  handles this internally and surfaces only the warning),
  `OperatorAttentionNeeded { reason }` (e.g., "QRESYNC consistently
  failing on this account; consider runtime flip to CONDSTORE-only"
  - consumer-config concern, not Fatal), and `Throttled { wait,
  source }`. Log aggregation spots patterns on `kind`.
- `Progress` events are consumer-visible observability; spans and
  metrics are operator-visible. Neither replaces the other.

These are settled, not phase-2 TODO. They are part of the engine's
public surface; downstream consumers and operators design dashboards
and alerts against them.

## Open questions

- **Backfill partitioning policy.** Newest-first by day, by week,
  by month? Adaptive to message density? Probably configurable with
  a sensible default; pin defaults per protocol.
- **Resource budgets.** Bandwidth, concurrency, and request-rate
  budgets are per-account or per-process? Most likely per-account
  with a global ceiling; needs spec.
- **Cross-account scheduling.** When a process is syncing five
  accounts, do they share the global ceiling? Round-robin or
  weighted by user attention?
- **Checkpoint storage.** The engine emits checkpoints; the
  consumer persists them. Should the engine ship a default
  `CheckpointStore` (sled? sqlite? plain files?) or stay storage-
  agnostic? Likely agnostic with a reference impl for tests.
- **Mutation conflict policy.** Two consumers of one account both
  setting flags: last-write-wins per protocol semantics, but the
  engine should surface conflicts where the protocol detects them
  (JMAP `ifInState` is the model).
- **Clock skew for time-window backfill.** IMAP `SINCE` is server-
  local; the client clock may differ. Newest-first partitioning by
  date risks gaps or duplication at the partition boundary when
  skew is non-trivial. Probably: ask the server for its time once
  per session (`CAPABILITY ID`, `STATUS`, or first response
  timestamp) and adjust partition edges by the observed delta.
- **`InvalidationSink` API shape.** Channel-based, queue-based, or
  callback-based? The sink connects out-of-process push receivers
  (Pub/Sub listeners, webhook endpoints) to the engine. Lean
  channel; needs spec before consumers wire receivers.
- **Trait dispatch, scope cardinality, capability negotiation
  timing, multi-account scope identity, and the EWS path** are
  tracked in `plans/account-trait.md` since they are properties of
  the trait, not the engine.

## Decision

Write the engine. Per-crate streams are designed against this
contract, not in isolation.
