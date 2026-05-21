# bifrost-sync internal layout

How the sync engine is wired internally. The public contract lives
in `plans/sync-engine.md` (what consumers see) and
`plans/account-trait.md` plus `plans/account-trait-shape.md` (what
the engine drives). This document specifies the engine's modules,
the primitives each module owns, and how they compose.

Phase-2 implementers should be able to read this end to end and
know which file each piece of work lands in, what types it owns,
and which other modules it talks to.

## Crate layout

```
crates/sync/
- Cargo.toml
- src/
  - lib.rs                  // re-exports; SyncEngine entry type
  - engine.rs               // SyncEngine, attach/detach, Account map
  - control.rs              // Control handle, Priority signalling
  - scheduler/
    - mod.rs                // four-lane scheduler; Worker trait
    - lanes.rs              // priority queues, lane-pull primitive
    - budget.rs             // per-account and global concurrency budget
  - multiplexer/
    - mod.rs                // per-account multiplexer task
    - idle.rs               // IDLE / WebSocket holder for active scope
    - poll.rs               // NOOP/STATUS round-robin for rest
    - fusion.rs             // inventory-as-cursor-establish fusion (IMAP)
    - changes.rs            // account_changes_stream output funnel
  - backfill/
    - mod.rs                // backfill orchestrator per account
    - partitioner.rs        // newest-first partition planner
    - runner.rs             // per-partition runner; honors live overlay
    - checkpoint.rs         // BackfillCheckpoint envelope I/O hooks
  - push/
    - mod.rs                // InvalidationSink type, push fan-in
    - reconciler.rs         // changes_stream replay against advanced cursor
    - subscription.rs       // push_subscribe/unsubscribe lifecycle
  - mutation/
    - mod.rs                // mutation pipeline entry
    - idempotency.rs        // IdempotencyKey vending, run_id persistence
    - readback.rs           // post-retry read-back guard;
                          // applies to every protocol today
                          // (all four declare
                          // MutationReplaySafety::None)
    - fanout.rs             // cross-account bulk_set_flags composition
  - cursor/
    - mod.rs                // cursor envelope + persistence shim
    - envelope.rs           // serde envelope (version + opaque payload)
    - store.rs              // CheckpointStore trait + in-memory impl
  - observability/
    - mod.rs                // tracing span helpers + metric handles
    - metrics.rs            // bifrost_sync_* counter and histogram names
    - warnings.rs           // Warning surfacing + log mapping
  - cancel/
    - mod.rs                // cancel taxonomy: Drop/pause/checkpoint/Fatal
    - boundary.rs           // safe-boundary signal plumbing
  - error.rs                // engine Error and Fatal mapping
  - types.rs                // engine-only types not exported to consumers
- tests/
  - envelope_roundtrip.rs   // cursor envelope version up/down
  - scheduler_priority.rs   // lane-pull ordering, budget eviction
  - partition_planner.rs    // newest-first partition selection
  - readback_guard.rs       // mutation retry skip-already-applied
```

Public surface (re-exported from `lib.rs`):

- `SyncEngine`, `SyncEngineBuilder`
- `Control`, `Priority`
- `InvalidationSink`, `WatchEvent` (re-exported from the trait crate
  for ergonomic consumer wiring)
- `CheckpointStore` trait + `InMemoryCheckpointStore` reference impl
- `BackfillPolicy`, `BackfillStrategy` (consumer-facing tunables)
- `EngineConfig`, `ConcurrencyBudget`
- `Fatal`, `Warning`, `Error` (engine-side variants only; trait-side
  variants come through re-export)

Internal-only modules (`pub(crate)` at most): everything under
`scheduler/`, `multiplexer/`, `backfill/runner.rs`,
`backfill/partitioner.rs`, `push/reconciler.rs`,
`mutation/{readback,fanout}.rs`, `cursor/envelope.rs`, `cancel/*`,
`observability/*`. The cursor envelope format is not public because
its version field is engine-managed; consumers see only the typed
`ChangeCursor` / `BackfillCheckpoint` from the trait crate.

## Engine entry points

`SyncEngine` is the top-level handle. Construction is builder-driven
so the consumer can wire a `CheckpointStore` and an
`EngineConfig` without a giant positional constructor.

```rust
pub struct SyncEngine {
    accounts: DashMap<AccountId, AccountSlot>,
    config: Arc<EngineConfig>,
    store: Arc<dyn CheckpointStore>,
    sink: Arc<InvalidationSinkInner>,
    runtime: tokio::runtime::Handle,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
}

pub struct SyncEngineBuilder { /* config knobs + store */ }

impl SyncEngine {
    pub fn builder() -> SyncEngineBuilder;

    pub async fn attach(
        &self,
        account_id: AccountId,
        factory: Arc<dyn AccountFactory>,
    ) -> Result<Control, Error>;

    pub async fn detach(&self, account_id: AccountId)
        -> Result<(), Error>;

    pub fn account_changes_stream(&self, account_id: AccountId)
        -> EngineStream<SyncEvent<Batch<Change>>>;

    pub fn invalidation_sink(&self) -> Arc<dyn InvalidationSink>;

    pub async fn shutdown(self) -> Result<(), Error>;
}
```

`AccountSlot` is the engine's per-account state:

```rust
struct AccountSlot {
    factory: Arc<dyn AccountFactory>,
    current: ArcSwap<Arc<dyn Account>>,   // hot-swapped on reopen
    multiplexer: MultiplexerHandle,
    backfill: BackfillHandle,
    push: PushHandle,
    mutation: MutationHandle,
    cursors: Arc<CursorRegistry>,         // ChangeCursor by CursorScope
    backfill_state: Arc<BackfillRegistry>,// BackfillCheckpoint by scope
    control_tx: watch::Sender<Priority>,
    shutdown: CancellationToken,
    metrics: AccountMetrics,
}
```

`attach` flow:

1. Call `factory.open()` to get the first `Arc<dyn Account>`.
2. Read `capabilities()` once. Stash on the slot.
3. Spawn the multiplexer task (`multiplexer::mod.rs`).
4. Spawn the backfill orchestrator (`backfill::mod.rs`).
5. Spawn the push reconciler (`push::reconciler.rs`).
6. Hand back a `Control` whose drop signals graceful pause.

`detach` flow:

1. Trip `slot.shutdown` (a `CancellationToken`).
2. Wait for in-flight `Batch`-with-checkpoint to flush and persist
   via the safe-boundary mechanism (`cancel/boundary.rs`).
3. Call `account.close().await` on the current handle.
4. Remove the slot.

Server-side push subscriptions are NOT torn down by `detach`. The
consumer destroys them explicitly through a separate
`engine.unsubscribe_push(account_id)` call (which forwards to
`Account::push_unsubscribe`). This matches `account-trait-shape.md`
Q3: `close(&self)` is local-handle teardown only.

`account_changes_stream` is the unified output funnel. Each
subscription gets a `broadcast::Receiver` adapted into the
`EngineStream<SyncEvent<Batch<Change>>>` shape; the multiplexer is
the sole producer. Slow consumers see backpressure as `Warning::
LaggedBroadcast` rather than a silent drop; the broadcast capacity
is configurable in `EngineConfig`.

The `Control` handle owns a `watch::Sender<Priority>` clone and
the slot's `shutdown` token (clone). Drop on `Control` does not
detach - it triggers `checkpoint_now` semantics so the consumer
can release the handle without losing the account.

## Scheduler

Four lanes, one per `Priority`: `Foreground`, `Normal`,
`Background`, `Bulk`. The scheduler owns nothing protocol-specific;
it just gates which tasks pull work from which queues and how often.

### Primitive choice

A custom multi-lane scheduler over `tokio::select!` on per-lane
`Notify` plus a strict-priority lane-pull. Justification:

- Separate tokio runtimes per priority are heavy (each owns
  threads). We need millions of small work items, not isolated
  CPU pools.
- `tokio::select!` over weighted lanes alone leaks the round-robin
  fairness Tokio gives to `select!` arms - Foreground must
  dominate, not share.
- A custom executor is overkill; we are not scheduling future
  polling, we are scheduling *which Account operations get to
  acquire concurrency tokens*.

The scheduler is a *gate*, not an executor. Tokio's runtime still
executes futures; the scheduler decides which futures are allowed
to run concurrently.

### Lanes

```rust
pub enum Priority {
    Foreground,   // user clicked something; sub-second SLA
    Normal,       // default; live change tracking + push reconcile
    Background,   // backfill of recent partitions (last week)
    Bulk,         // deep history backfill; battery-aware
}

struct LaneSet {
    foreground: LaneQueue,
    normal: LaneQueue,
    background: LaneQueue,
    bulk: LaneQueue,
}

struct LaneQueue {
    pending: SegQueue<WorkItem>,
    notify: Notify,
    metric: IntCounter,
}
```

### Lane-pull primitive

Every worker (multiplexer, backfill runner, mutation pipeline)
loops:

```text
loop {
    let item = lane_set.pull(self.required_priority_floor).await;
    let permit = budget.acquire(item.account_id).await;
    item.run(permit).await;
}
```

`pull(floor)` is strict-priority above the floor and FIFO within
a lane. Foreground always preempts Normal until Foreground is
empty; same for Normal over Background; Background over Bulk.
Inter-lane preemption is at `WorkItem` boundary, not mid-await;
the unit of preemption is the protocol-level batch, which is the
natural cancellation safe point in the streaming contract.

### Priority flow

`Control::priority(p)` writes to the slot's
`watch::Sender<Priority>`. The multiplexer task reads
`watch::Receiver<Priority>` before submitting each `WorkItem` and
tags the item with the current priority. The backfill orchestrator
reads the same watch and adjusts the lane it submits partitions
to: Foreground caps backfill at Background; Bulk caps backfill at
Bulk regardless of the consumer's request. Push reconcile work
always uses Normal because Invalidated wake-ups need bounded
latency.

### Starvation guards

Bulk and Background lanes have a starvation floor: after N
consecutive Foreground/Normal items, the pull primitive pulls one
lower-lane item regardless. `N` is configurable; default 64. The
floor is necessary because backfill on a 5M-message account is a
multi-hour campaign and must not stall indefinitely behind a busy
Foreground.

## Multiplexer

One multiplexer task per account. It owns:

- The set of `ChangeCursor`s discovered from
  `Account::discover_cursor_scopes()`.
- The IDLE / WebSocket holder for the *most-active* scope.
- A round-robin NOOP/STATUS poller for the rest.
- The fan-out to `account_changes_stream` consumers.

### Most-active scope selection

A scope's activity score is exponentially-weighted change rate
over the trailing 30 minutes, decayed every 30 seconds. The
multiplexer recomputes scores on a 60-second tick and on every
`Invalidated` from push:

```text
score(scope) = alpha * recent_changes_per_minute(scope)
             + (1 - alpha) * decayed_score(scope)
alpha = 0.3
```

The highest-scoring scope holds IDLE. On ties or empty history
(fresh attach), Inbox (or its protocol-specific equivalent
identified via `SpecialUse` / `\Inbox` / Gmail INBOX label) wins.

The active scope changes only when the second-place scope's score
exceeds the active scope's score by 25% for two consecutive
ticks. The hysteresis prevents thrashing on bursty folders.

For non-IDLE-capable protocols (Gmail Pub/Sub, Graph webhooks),
"most-active" still selects a scope but the holder runs a longer
WebSocket / SSE pull, not IDLE.

### Round-robin cadence

The non-active scopes share a single rotating cursor: on each
tick, the next scope in a Vec is polled via NOOP (IMAP) or STATUS
(IMAP) or `changes_stream` with a short timeout (JMAP/Gmail/Graph
when not holding the active push).

Default cadence: 5-minute full round-trip. Adaptive: if a scope
returns "Advanced" two ticks in a row, its individual interval
halves (min 30 seconds). If a scope returns "no change" five ticks
in a row, its interval doubles (max 30 minutes). This per-scope
backoff is held in a `HashMap<CursorScope, AdaptiveCadence>` on
the multiplexer.

### Inventory-fusion (establish-via-inventory)

The multiplexer's cursor-establishment path is governed by
`Account::establish_initial_cursor(scope)` (per
`plans/account-trait.md` -> Cursor establishment), called
per-scope on first attach. Two cases:

1. **`CursorEstablishment::EstablishViaInventory`** (Graph all
   scopes, IMAP-Basic / IMAP-CONDSTORE-only folders). The
   multiplexer fuses inventory and cursor establishment:
   - `inventory_stream(scope)` is the cursor-establishment
     pass. Its terminal `Batch` carries the `ChangeCursor` for
     that scope inside the page boundary.
   - The multiplexer registers the cursor in `CursorRegistry`
     only on inventory completion (the `Done` event).
   - Subsequent `changes_stream(cursor)` calls drive incremental
     deltas cheaply.
2. **`CursorEstablishment::Ready(cursor)`** (JMAP all scopes,
   Gmail Account scope, IMAP-QRESYNC folders). The cursor is
   already minted (factory-cached or one cheap probe). The
   multiplexer registers it immediately and starts both
   `changes_stream(cursor)` and `inventory_stream(scope)` in
   parallel; inventory runs underneath as backfill.

Per-scope decision, not per-account. An IMAP account with one
QRESYNC folder + one Basic-downgraded folder runs both paths
concurrently, each on its own folder.

### Output funnel

Every `Change` event reaching the multiplexer (from the active
push, from any round-robin poll, from `scope_lifecycle_stream`)
is tagged with `cursor_scope` and pushed onto the account's
broadcast channel. The broadcast channel is the source for
`account_changes_stream`. Items emitted include:

- `SyncEvent::Batch(Batch<Change>)` with `checkpoint: Some(..)`
  at scope advance.
- `SyncEvent::Warning(..)` for per-scope warnings that did not
  end the stream.
- `SyncEvent::Progress(..)` aggregated across active scopes.

Fatals from any underlying stream are translated by
`error.rs::map_recovery_to_fatal` and emitted to the broadcast,
then the multiplexer either re-opens the account
(`RecoveryClass::RestartAccount`,
`RecoveryClass::CapabilityChanged`) or marks the scope offline
(`RestartScope`, `DowngradeStrategy`) and continues.

## Backfill partitioner

Backfill is *separate* from change tracking. The partitioner
plans newest-first partitions and the runner drives them.

### Default partition policy

Time-windowed, exponentially-widening:

```text
partition[0] = (now - 7 days, now)
partition[1] = (now - 30 days, now - 7 days)
partition[2] = (now - 90 days, now - 30 days)
partition[3] = (now - 180 days, now - 90 days)
partition[4] = (now - 365 days, now - 180 days)
partition[k>=5] = ((k-3) years ago, (k-4) years ago)
```

Default rationale: most active accounts have 80%+ of touch
activity in the trailing 7 days. The first partition completes
fast enough to give the user near-complete recent state inside
the first sync run; later partitions hydrate deep history while
the user already has a working app.

The strategy is configurable per account through `BackfillPolicy`:

```rust
pub enum BackfillStrategy {
    TimeWindowed { boundaries: Vec<chrono::Duration> },
    UidRange { chunk_size: u32 },           // IMAP-only
    PageCount { items_per_partition: u32 }, // JMAP queryChanges
}
```

`UidRange` is the IMAP-on-Basic fallback when time-bounded SEARCH
is expensive or unavailable; it walks `UID 1:N` in chunks of
configurable size (default 5000).

`PageCount` is the JMAP optimization that leans on the `position`
count exposed in `plans/jmap/streaming.md` - resume from a count
across partitions.

Clock skew handling per `sync-engine.md` open question: at
account-open the engine takes one `STATUS` or first-response
timestamp from the server, computes `client_skew = server_now -
client_now`, and offsets time-windowed partition edges by
`client_skew`. Surfaced as a `Warning::ClockSkew { delta }` when
`|delta| > 5 minutes`.

### Interleaving with live changes

Two regimes per `CursorEstablishment` (returned from
`Account::establish_initial_cursor(scope)`):

- **`Ready(cursor)` scopes (JMAP all scopes, Gmail Account,
  IMAP-QRESYNC folders).** Cursor is minted cheaply.
  `changes_stream(cursor)` starts immediately on account attach.
  The backfill orchestrator submits the first inventory partition
  to the lane selected by `Control::priority(p)`. Live changes
  stream through the multiplexer in parallel. When the backfill
  runner observes an `ObjectChange::Updated` for an item it has
  not yet hydrated, it drops the corresponding inventory entry on
  the floor; the live stream supersedes it. When it observes
  `ObjectChange::Created` for an item it has not yet seen, the
  runner notes the id in a `LiveSupersedes` set; on hitting that
  id during backfill, it skips.
- **`EstablishViaInventory` scopes (Graph all scopes,
  IMAP-Basic and IMAP-CONDSTORE-only folders).** Inventory's
  walk IS the cursor establishment for that scope. Live
  tracking starts only when inventory completes and the cursor
  from its terminal `Done` is registered. Other scopes on the
  same account may already have live tracking running because
  establishment is per-scope. This is the asymmetry called out
  in `sync-engine.md` and `account-trait.md` -> Cursor
  establishment.

The runner persists `BackfillCheckpoint` at every partition
boundary (a `Batch` with `checkpoint: Some(..)`) via
`CheckpointStore::put_backfill`. On resume, the partitioner reads
the latest checkpoint per scope and resumes inside the partition
that contains the checkpoint's `progress_marker`.

## Push reconciler and InvalidationSink

### InvalidationSink shape

Channel-based, per the lean in `sync-engine.md` open questions:

```rust
pub trait InvalidationSink: Send + Sync + 'static {
    fn push(&self, account: AccountId, event: WatchEvent);
}

pub(crate) struct InvalidationSinkInner {
    senders: DashMap<AccountId, mpsc::Sender<WatchEvent>>,
    capacity: usize,                  // bounded, default 256
    drop_counter: IntCounter,
}

impl InvalidationSink for InvalidationSinkInner {
    fn push(&self, account: AccountId, event: WatchEvent) {
        if let Some(tx) = self.senders.get(&account) {
            match tx.try_send(event) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    // coalesce: full means a reconcile is queued;
                    // dropping is safe because reconcile re-reads
                    // changes_stream regardless of hint specificity
                    self.drop_counter.inc();
                }
                Err(TrySendError::Closed(_)) => {
                    // account detached during in-flight push
                }
            }
        }
    }
}
```

The `mpsc::Sender` is created in `attach` and registered in
`senders`. Both in-process push (the engine reading
`Account::push_stream`) and out-of-process push (Pub/Sub listener
and webhook server processes calling `InvalidationSink::push`
from the consumer side) feed the same `mpsc::Sender`.

The capacity is bounded because the reconciler treats `Unknown`
and a specific `InvalidationHint` identically in v1 - one
queued wake is sufficient to cover N coalesced wakes. The
`drop_counter` is exposed as `bifrost_sync_push_dropped_total`.

### Reconciler

Per account, one task:

```text
loop {
    let event = rx.recv().await?;
    match event {
        WatchEvent::Invalidated { hint } => {
            for scope in scopes_for_hint(hint) {
                let cursor = cursor_registry.snapshot(scope);
                let stream = account.changes_stream(cursor);
                drive_to_completion(stream).await;
            }
        }
        WatchEvent::Disconnected => surface_warning(),
        WatchEvent::Reconnected => kick_full_reconcile(),
    }
}
```

`scopes_for_hint(hint)`:

- `HintPayload::SpecificCursorScope(s)` -> `vec![s]`
- `HintPayload::SpecificMembership(m)` -> enumerate cursor scopes
  containing `m` (per the cursor registry's index)
- `HintPayload::Unknown` -> all registered cursor scopes for the
  account

`drive_to_completion` is the same primitive the multiplexer uses
when its round-robin poll detects a change. Output batches flow
to the same broadcast as the multiplexer's, so consumers do not
see "push-derived" vs "poll-derived" changes - the engine's
single output is a unified `Change` stream.

### In-process / out-of-process fusion

The two paths merge cleanly because both end up writing to the
same per-account `mpsc::Sender<WatchEvent>`:

- **In-process.** The multiplexer task spawns a forwarder that
  reads `Account::push_stream()` and calls
  `sink.push(account_id, event)` on every item. This is the
  IMAP IDLE / JMAP WebSocket / EWS streaming path.
- **Out-of-process.** The consumer holds an
  `Arc<dyn InvalidationSink>` (from
  `engine.invalidation_sink()`) and its Pub/Sub or webhook
  receiver calls `sink.push(account_id, event)` directly. This
  is the Gmail Pub/Sub and Graph webhook path.

The reconciler does not know or care which path produced the
event. Both result in a `changes_stream` run against the
freshly-advanced cursor.

## Mutation pipeline

Mutations land through `engine.bulk_set_flags(account_id, ...)`
and friends. The engine layer adds:

- IdempotencyKey vending + run_id persistence.
- Partial-success accumulation across protocol-level retries.
- Read-back guard for `MutationReplaySafety::None` protocols.
- Cross-account fanout where the consumer addresses N accounts
  at once.

### IdempotencyKey vending

```rust
pub struct MutationCampaignId(pub Uuid);

pub(crate) struct IdempotencyVendor {
    run_id: Uuid,                          // persisted per campaign
    sequence: AtomicU64,                   // monotonic within run
    protocol_salt_factory: Box<dyn Fn(ProtocolKind) -> ProtocolSalt + Send + Sync>,
}

impl IdempotencyVendor {
    fn next(&self, protocol: ProtocolKind) -> IdempotencyKey {
        IdempotencyKey {
            run_id: self.run_id,
            sequence: self.sequence.fetch_add(1, Ordering::AcqRel),
            salt: (self.protocol_salt_factory)(protocol),
        }
    }
}
```

`run_id` is loaded from `CheckpointStore` (under a campaign-scoped
key) at vendor construction; if absent, a fresh UUID is minted and
persisted *before* the first mutation is submitted. This satisfies
the rule in `account-trait.md`: `run_id` is consumer-minted in
spirit (the engine acts as consumer-side bookkeeping) and persists
across process restarts.

The campaign id is the consumer's handle to the mutation
operation - a single "mark 200K read" is one campaign with one
run_id and a growing sequence.

### Partial-success accumulation

Every protocol-level batch returns
`SyncEvent<Batch<MutationResult>>`. The engine forwards each
batch upstream and tracks aggregate counters on the campaign:

- `applied: u64`
- `skipped: u64`
- `failed_terminal: u64`
- `pending_retry: u64`

`pending_retry` items live on a per-campaign retry queue. On
transient failure (`RecoveryClass::Retry { after }`), the
mutation runner sleeps for `after`, then re-submits the batch
with the same `IdempotencyKey` (sequence reused) for engine-side
correlation. No protocol crate today emits the key on the wire as
a dedup token; all four declare `MutationReplaySafety::None`. The
engine's read-back guard (next section) is therefore the
universal disambiguation step after retry.

### Read-back guard

For `MutationReplaySafety::None`, the engine post-retry runs:

```text
for batch in retried_batches {
    let ids = batch.items.iter().map(|r| r.id).collect();
    let inv = account.get_stream(stream::iter(ids).boxed(),
                                  Projection::FlagsOnly);
    for hydrated in inv {
        if hydrated.matches_target_state(&mutation) {
            // server already applied this on the first try
            mark_skipped(hydrated.id);
        } else {
            // retry was the actual apply (or it still failed)
            // leave outcome as protocol-reported
        }
    }
}
```

**Applies to all four protocols today.** None of JMAP, Gmail,
Graph, or IMAP has a documented client-mintable replay token on
the wire:

- **JMAP**: `ifInState` is optimistic concurrency, not replay
  protection. A `StateMismatch` on retry can fire for any
  unrelated state change.
- **Gmail**: Google's REST docs do not document
  `X-Goog-Request-Id` (or any other header) as a Gmail-side
  dedup primitive for `messages.modify` /
  `messages.batchModify` / `messages.batchDelete`. Cloud-API
  conventions from other Google products do not transfer.
- **Graph**: Microsoft documents JSON-batch `id` as per-batch
  request/response correlation only, and `client-request-id` as
  debugging/support correlation (per the dev-proxy
  troubleshooting docs). Neither is a dedup primitive.
- **IMAP**: No native replay token. `STORE UNCHANGEDSINCE` is
  optimistic concurrency, separate from idempotency.

The read-back guard is the universal safety net. The prior
"JMAP / Gmail / Graph skip read-back" claim was based on
incorrect attribution of replay-token semantics to those
protocols' debugging or correlation primitives and has been
removed.

If a future protocol surface lands a real client-mintable dedup
token (`MutationReplaySafety::ReplayToken` is reserved in the
trait enum for this), the engine can skip read-back for it. No
protocol qualifies today.

### Cross-account fanout

`engine.bulk_set_flags(targets, flags, op)` where `targets` is
`AccountStream<(AccountId, ObjectId)>`:

1. Partition the input stream by `AccountId` into N per-account
   `AccountStream<ObjectId>` channels.
2. For each account, submit a `WorkItem` to the scheduler at the
   campaign's priority lane.
3. Merge per-account output streams into one
   `AccountStream<SyncEvent<Batch<MutationResult>>>` (using
   `futures::stream::select_all`) and return it.

Partitioning is single-pass and bounded - each per-account
sub-channel has a configurable buffer (default 256
`(account, id)` pairs). Backpressure on any sub-channel slows the
input stream uniformly.

## Observability glue

### Tracing

One root span per stream subscription, named per the doc:

- `bifrost.sync.changes` (per multiplexer subscription, per
  account)
- `bifrost.sync.inventory` (per inventory pass)
- `bifrost.sync.backfill` (per backfill partition)
- `bifrost.sync.reconcile` (per push-driven reconcile run)
- `bifrost.sync.mutation` (per campaign)

Spans carry `account_id`, `cursor_scope` (where applicable), and
`priority` as fields. Child spans cover per-batch work; the
batch span ends with a `tracing::Event` at level info on success
(`items=...`, `bytes=...`), level warn on `Warning`, level error
on `Fatal`.

`traceparent` is injected by `bifrost-net` on every outbound
HTTP-based request; the engine sets the current span context via
`tracing::Span::current().in_scope(...)` before calling into the
`Account` trait. IMAP carries no native trace context - tracing
is local-only on that path.

### Metrics

Counters and histograms with labels `account_id`, `protocol`,
`scope`, `priority`. Exposed through `metrics` crate handles
behind a feature gate so non-metric consumers do not pull in
`metrics-exporter-*` transitively.

Counter set:

- `bifrost_sync_items_total`
- `bifrost_sync_bytes_total`
- `bifrost_sync_warnings_total`
- `bifrost_sync_retries_total`
- `bifrost_sync_fatals_total`
- `bifrost_sync_push_received_total`
- `bifrost_sync_push_dropped_total`
- `bifrost_sync_reopens_total`
- `bifrost_sync_mutations_applied_total`
- `bifrost_sync_mutations_skipped_total`

Histogram set:

- `bifrost_sync_batch_latency_seconds`
- `bifrost_sync_page_size_items`
- `bifrost_sync_page_size_bytes`
- `bifrost_sync_reconcile_latency_seconds`
- `bifrost_sync_backfill_partition_duration_seconds`

The `Metrics` struct is constructed once per `SyncEngine` and
cloned (Arc-internally) into every task. Per-account
`AccountMetrics` are `Arc<Metrics> + label cache` so the hot
path does not stringify `account_id` on every observation.

### Warning surfacing

`Warning` events flow on two channels:

- The data stream itself (`SyncEvent::Warning`), so consumers
  that drive the streams see warnings inline with their batches.
- The tracing layer at `level = warn` with a structured payload,
  so operators with no consumer code see them in logs.

Warnings are *not* metricsified one-by-one (the counter total is
enough); the per-warning detail lives in the trace event.

Warning vs Fatal: a warning is a per-item or per-batch failure
that did not stop the stream. A Fatal ends the stream and is
also emitted as a tracing event at `level = error`. The mapping
from `RecoveryClass` to `Warning` vs `Fatal` lives in
`observability/warnings.rs` and `error.rs::map_recovery_to_fatal`
respectively.

## Checkpoint envelope

`ChangeCursor` and `BackfillCheckpoint` both carry
`envelope_version: u32`. The envelope is the migration story for
"engine got smarter and the on-disk cursor is from an older
engine."

### Envelope format

```rust
pub(crate) struct CursorEnvelope {
    pub version: u32,
    pub kind: EnvelopeKind,           // Change | Backfill
    pub scope_repr: ScopeRepr,        // serialized CursorScope
    pub payload: Vec<u8>,             // bincode of the inner struct
}

pub(crate) enum EnvelopeKind {
    Change,
    Backfill,
}

// Wire format:
//   1 byte    magic = 0xB5
//   3 bytes   reserved (zero)
//   4 bytes   version (little-endian u32)
//   1 byte    kind
//   varint    scope_repr length
//   N bytes   scope_repr
//   varint    payload length
//   N bytes   payload
```

`bincode` over the inner struct because it is compact and the
engine version-tags the outer layer. `serde_json` would round-trip
but doubles disk and parse cost on a per-batch checkpoint. The
protocol-owned `OpaqueChangeState::bytes` is itself opaque to the
envelope - it nests cleanly.

### Migration path

Three cases on read:

- `version == ENGINE_VERSION`: pass through.
- `version < ENGINE_VERSION` and `version >= MIN_MIGRATABLE`:
  invoke the chain of `migrate_v{N}_to_v{N+1}` functions in
  `cursor/envelope.rs` until at current. Persist the migrated
  envelope back to the store atomically on the next checkpoint
  boundary (not on read - reads are non-mutating).
- `version < MIN_MIGRATABLE` *or* `version > ENGINE_VERSION`:
  emit `RecoveryClass::SchemaIncompatible` from the protocol
  crate via the description path, surfaced as
  `Fatal::SchemaIncompatible` to the consumer. The consumer
  must clear the cursor for that scope; the engine restarts
  with a fresh inventory.

`MIN_MIGRATABLE` lives in `cursor/envelope.rs` as a const,
starting at `1`. Each engine version that requires a migration
bumps `ENGINE_VERSION` and adds one `migrate_vN_to_vN1` function.

Cursors *and* backfill checkpoints share the same envelope
because they share the same migration concerns; the
`EnvelopeKind` byte lets the migration dispatcher pick the right
fixup. The migration code path is dead until a real schema
change; `MIN_MIGRATABLE == ENGINE_VERSION` from day one and the
test in `tests/envelope_roundtrip.rs` covers same-version round
trip plus a synthetic forward/backward case.

## Cancellation plumbing

Four exits per `sync-engine.md`: Drop, pause, checkpoint_now,
Fatal.

### Internal primitives

Every task owns one `CancellationToken` from
`tokio_util::sync::CancellationToken`. Tokens are arranged in a
tree:

```text
engine.cancel  (root)
+- slot.shutdown
   +- multiplexer.cancel
   +- backfill.cancel
   +- push.cancel
   +- mutation.cancel
```

Canceling a parent cancels children. The tree gives us
account-level shutdown via `slot.shutdown.cancel()` without
touching the engine root.

A second primitive, the *safe-boundary signal*, is a
`watch::Receiver<BoundaryRequest>`:

```rust
enum BoundaryRequest {
    Run,                  // default
    Pause,                // checkpoint and idle
    CheckpointNow,        // checkpoint at next batch boundary
    Stop,                 // hard stop after next safe point
}
```

Every streaming worker calls `boundary.peek()` at the top of each
batch iteration. The worker's reaction:

- `Run` -> proceed.
- `Pause` -> if the next batch carries a `checkpoint: Some(..)`,
  persist it, then park (`watch::Receiver::changed().await`)
  until the request changes to `Run` or `Stop`.
- `CheckpointNow` -> on the next `Batch` with checkpoint, persist
  it and emit `SyncEvent::Done`. Transitions itself back to
  `Run` once the checkpoint is flushed.
- `Stop` -> emit `SyncEvent::Done` after persisting the most
  recent checkpoint, then exit the worker.

### Exit mapping

- **Drop on the public stream future.** Tokio drops the future.
  The driver task sees its forward sender close; on the next
  `boundary.peek()` (or send-error on the broadcast), the worker
  cancels its in-flight protocol-level request via the protocol
  crate's cancel-safe driver, *without* persisting the in-flight
  batch's checkpoint. Cursor remains at the last persisted
  position.
- **`Control::pause()`.** Writes `BoundaryRequest::Pause` to the
  slot's watch. Tasks reach the next batch boundary, persist,
  and park.
- **`Control::checkpoint_now()`.** Writes `CheckpointNow`. Tasks
  emit `Done` after the next persisted checkpoint and exit
  cleanly.
- **`Fatal`.** The protocol crate emits `SyncEvent::Fatal(..)`.
  `error.rs::map_recovery_to_fatal` decides whether to restart
  (cancel + re-attach the account), downgrade strategy, or
  surface to the consumer.

The safe-boundary mechanism is the only place "checkpoint is
durable" is established. Every `Batch` with
`checkpoint: Some(c)` triggers a synchronous
`CheckpointStore::put_change` or `put_backfill` before the worker
moves on to the next batch. This is the consumer-side guarantee
the trait doc cites: persistence is atomic at the protocol
boundary.

## Concurrency budget

Default: per-account budget with a global ceiling. Primitive:
`tokio::sync::Semaphore` with leased permits, two layers.

```rust
pub struct ConcurrencyBudget {
    pub per_account: usize,         // default 8
    pub global: usize,              // default 64
    pub mutation_share: f32,        // default 0.25 of per-account
}

pub(crate) struct BudgetGate {
    global: Arc<Semaphore>,
    per_account: DashMap<AccountId, Arc<Semaphore>>,
    mutation: DashMap<AccountId, Arc<Semaphore>>,
}

impl BudgetGate {
    pub async fn acquire(&self, account: AccountId, kind: WorkKind)
        -> BudgetPermit
    {
        let outer = self.global.clone().acquire_owned().await?;
        let inner = match kind {
            WorkKind::Mutation => self.mutation_sem(account),
            _ => self.account_sem(account),
        };
        let inner = inner.acquire_owned().await?;
        BudgetPermit { _outer: outer, _inner: inner }
    }
}
```

Justification for the picks:

- **Semaphore, not token bucket.** Token-bucket primitives time-
  shape requests; semaphores cap concurrency. The constraint
  bifrost actually has is "how many open HTTP/2 streams or IMAP
  driver tasks per account" - that is a count, not a rate. Rate-
  shaping (Gmail's quota-units-per-second budget) is enforced in
  `bifrost-net`, not here.
- **Per-account first, global second.** Two big accounts under
  one engine should not starve each other; a runaway mutation on
  one must not exhaust the engine's concurrency.
- **Reserved mutation share.** Mutations sharing the same
  semaphore as inventory/changes/reconcile means a 200K-flag
  mutation pegs all 8 permits and freezes the multiplexer. Carve
  out a fraction (default 1/4 of per-account, rounded up to 1)
  for mutations.

Per-account defaults can be lowered on Gmail's 250-quota-units-
per-second tier (per `plans/gmail/streaming.md`); the protocol
crate's capabilities can declare a `concurrency_hint: usize` that
the engine uses to clamp `per_account`.

## Crate dependency policy

`bifrost-sync` depends on:

- **`bifrost-types`** (new) - the carve-out crate hosting the
  shared sync surface: `Account` trait, `AccountFactory`,
  `SyncEvent`, `Batch`, `Change`, `BlobHandle`, capability and
  cursor types, `WatchEvent`, `InvalidationHint`, `RecoveryClass`,
  `MembershipScope`, `CursorScope`, `IdempotencyKey`,
  `ProtocolSalt`, `MutationResult`, `Projection`,
  `HydratedObject`, `InventoryEntry`. Created as a sibling crate
  during phase 1 because `bifrost-sync` cannot depend on
  `bifrost-{jmap,imap,gmail,graph}` and the trait cannot live in
  any single one of them. The name is deliberately broad so future
  cross-protocol shared types (a unified `Message` shape, say) can
  land here without renaming.
- **`tokio`** (`sync`, `time`, `rt-multi-thread`, `macros`) -
  channels, semaphores, watch, runtime handles.
- **`tokio-util`** (`sync` for `CancellationToken`).
- **`futures`** - stream combinators (`select_all`, `flatten`,
  `boxed`, `iter`).
- **`tracing`** - spans and events. No `tracing-subscriber`
  dependency in the engine itself.
- **`metrics`** - feature-gated; counters and histograms.
- **`serde`**, **`bincode`** - cursor envelope serialization.
- **`bytes`** - reused from trait crate where it surfaces.
- **`chrono`** - time-windowed backfill partitions.
- **`uuid`** - `IdempotencyKey::run_id` and campaign ids.
- **`dashmap`** - per-account state maps. The maps are written
  on attach/detach and on cursor advances; read on every push,
  every poll, every mutation. `RwLock<HashMap>` is acceptable
  too; dashmap is one less await point on the hot reconcile path.
- **`arc-swap`** - hot-swapping the active `Arc<dyn Account>` on
  reopen without taking out the multiplexer.

Explicitly **not** depended on:

- `bifrost-jmap`, `bifrost-imap`, `bifrost-gmail`, `bifrost-graph`,
  `bifrost-smtp`. The dependency direction is the protocol crates
  depending on `bifrost-types`; the engine depends only on the
  types crate.
- `bifrost-net`. The engine never opens a socket - it drives an
  `Account`. `bifrost-net` is a dependency of the protocol crates.
- Any persistence backend (sled, sqlite, rocksdb). The
  `CheckpointStore` trait is the engine's only persistence
  contract; consumers wire their own. An in-memory reference
  implementation lives in `cursor/store.rs` for tests.

The `bifrost-types` carve-out is the dependency-direction
inversion that makes the whole engine compile-time clean: any
protocol crate can be added without recompiling the engine, and
the engine can be tested without any protocol crate by
implementing a fake `Account`.
