# bifrost-sync reference

Current architecture of the sync engine.

Scope: scheduler, multiplexer, backfill orchestrator, push
reconciler, mutation pipeline, checkpoint envelope versioning,
observability. Depends only on `bifrost-types` (plus
`bifrost-net` for the bandwidth meter); opaque to every protocol
crate. Consumers wire one `Arc<dyn AccountFactory>` per account.

## Architecture

```
SyncEngine
  AccountSlot per attached account:
    current: ArcSwap<Arc<dyn Account>>
    control: SyncControl
    workers: Vec<WorkerTask {join, abort}>
    Multiplexer task        -> per-scope poll + lifecycle + reopen
    Backfill orchestrator   -> partition runner per scope
    Push reconciler task    -> reads per-account mpsc<WatchEvent>
    Push forwarder task     -> Account::push_stream -> per-account mpsc
    Ack writer task         -> AckRequest -> CheckpointStore
    Control applier task    -> priority / bandwidth-cap watchers
    Bandwidth feed (optional) -> BandwidthMeter -> control atomic
    Reopen listener         -> ReopenRequest -> handle_recovery
    Mutation campaigns      -> bulk_set_flags retry loop
  Shared:
    Scheduler (four-lane: Foreground / Normal / Background / Bulk)
    BudgetGate (global + per-account semaphores)
    CursorRegistry (scope -> cursor; membership index)
    CheckpointStore (consumer-provided; InMemoryCheckpointStore for tests)
    InvalidationSinkInner (DashMap<AccountId, mpsc::Sender<WatchEvent>>)
    SubscriptionRegistry (per-engine push subscription handles)
```

`slot.current` is `ArcSwap<Arc<dyn Account>>` so reopens are
visible to spawned tasks immediately: every iteration calls
`current.load_full()`. No `RwLock` on the hot path.

## Construction

`SyncEngineBuilder` is the entry point. Methods:

- `budget(ConcurrencyBudget)` - sets `EngineConfig::budget`.
- `config(EngineConfig)` - replaces the whole config.
- `checkpoints(Arc<DynCheckpointStore>)` - consumer-provided
  store; defaults to `InMemoryCheckpointStore` when omitted.
- `with_bandwidth_meter(Arc<bifrost_net::BandwidthMeter>)` -
  wires the meter; when unset, `Control::bandwidth_observed`
  returns 0 for every account.
- `build()` - validates the budget, constructs the scheduler
  with the configured lane capacity, returns `SyncEngine`.

`SyncEngine::builder()` is the canonical constructor; no public
`SyncEngine::new()`.

## Lifecycle

```rust
let control = engine.attach(account_id, factory).await?;
// ... engine drives the account ...
engine.detach(account_id).await?;
engine.shutdown().await?; // explicit cleanup; preferred over Drop
```

`attach`:
1. Take the per-account in-flight guard
   (`AsyncMutex<HashSet<AccountId>>` on `engine.attaching`) and
   reject duplicate / racing attaches with
   `Error::AccountAlreadyAttached`. The guard is released on both
   success and failure paths so a failed `attach_inner` does not
   strand the slot.
2. `factory.open(account_id).await` -> `Arc<dyn Account>`. The
   engine threads its own `AccountId` through so the protocol crate
   can register against `bifrost-net` / `MeterSink` / trace
   correlation under the same key the engine knows the account by.
3. Read `capabilities()` (snapshotted on the slot).
4. `discover_cursor_scopes()` -> for each, `establish_initial_cursor(scope)`:
   - `Ready(cursor)`: persist, start `changes_stream` immediately.
   - `EstablishViaInventory`: defer to backfill; the inventory
     pass's terminal `Done` carries the cursor.
5. `discover_memberships()` -> populate `CursorRegistry`
   membership index for push-hint routing. `scope_covers_membership`
   in `engine.rs` is the engine-policy mapping (account-wide
   cursors cover every membership; folder-typed cursors cover the
   matching folder; query cursors cover the matching query).
6. Spawn workers: ack writer, control applier, push reconciler,
   push forwarder (if `PushCapability != None`), multiplexer,
   backfill orchestrator, deferred-inventory worker (if any),
   reopen listener, bandwidth feed (if a meter is wired). Store
   all `WorkerTask`s.
7. Return `SyncControl`.

`detach` sends `Stop` on the boundary channel, cancels the
shutdown token, calls `Account::close()`, awaits all workers
with `EngineConfig::detach_timeout` (default 5s) and aborts
stragglers, removes the slot, unregisters the sink and ack
sender.

`shutdown(self)` enumerates attached accounts, calls `detach`
for each (logging but not failing on per-account errors), then
cancels the engine-root token. Strongly preferred over relying
on `Drop`, which can only fire a best-effort sync cancel.

`reopen` (driven by `RecoveryClass::RestartAccount` or
`CapabilityChanged`) calls `factory.open(account_id)` again and
`slot.current.store(Arc::new(next))` after reapplying the
priority and bandwidth-cap snapshots. Spawned tasks pick up the
new handle on their next `load_full()`.

## Stream contract: broadcast + consumer ack

`drive_changes_stream` broadcasts each `Batch` (item plus
optional `Checkpoint`) onto the per-account broadcast channel and
advances the in-memory `CursorRegistry` immediately so the next
poll iteration starts from the freshly-yielded cursor. It does
NOT write to `CheckpointStore`. Durable persistence is consumer-
ack-driven:

1. Consumer subscribes via `account_changes_stream`, receives a
   `MultiplexerEvent { scope, event, checkpoint }`.
2. Consumer atomically persists `(items, checkpoint)` in their
   own store.
3. Consumer calls `SyncEngine::ack_checkpoint(account, scope,
   checkpoint)`.
4. The per-account `ack_writer` task (one per slot, fed by an
   mpsc kept in `engine.ack_senders`) receives an `AckRequest`,
   writes to `CheckpointStore`, then fires
   `SyncControl::record_checkpoint` to wake `pause` /
   `checkpoint_now` waiters. The waiter contract is "the
   returned checkpoint has been persisted."

On restart the engine reads the last-acked cursor from the store
and re-runs `changes_stream` from there; items the consumer
never durably persisted come across again.

`InventoryFusion::run_with_broadcast` follows the same shape for
`EstablishViaInventory` scopes: inventory batches stream as the
cursor-establishment pass progresses and the cursor persists on a
successful `Done`.

`ChangesEvent` is the per-batch outcome returned by the driver:
`Advanced` / `Done` / `Stopped` / `Paused` / `Fatal(RecoveryClass)`.

## Multiplexer

`Multiplexer::run` drives:

- **In-process push** (IMAP IDLE, JMAP WebSocket, EWS streaming)
  on the most-active cursor scope.
- **Adaptive polling** for everything else: per-scope
  `AdaptiveCadence` starts at `MultiplexerConfig::poll_initial`
  (default 60s), halves on observed change down to `poll_min`
  (default 30s), doubles after **five** consecutive no-change
  ticks up to `poll_max` (default 30 minutes). The pure helper is
  `Multiplexer::updated_cadence(cur, seen_change, min, max)`.
- **Scope lifecycle** events from `Account::scope_lifecycle_stream`.
  `Created` and `Renamed` translate to a
  `ReopenRequest::Recovery { recovery: RestartScope }` via
  `lifecycle_reopen` so the engine drives the fresh cursor
  establishment through the same recovery path; `Deleted`
  cancels the per-scope token and drops the cursor.
- **Reopen requests** on a `mpsc::Sender<ReopenRequest>` channel:
  `RestartScope` deletes the in-memory and durable cursor before
  re-establishing; `RestartAccount` and `CapabilityChanged` route
  up to the engine's reopen path.

Per-scope cancellation tokens live in
`Multiplexer::scope_tokens` so a `ScopeLifecycle::Deleted` stops
the matching poll task without disturbing siblings.

Output is a `broadcast::Sender<MultiplexerEvent>`. The event
carries `{ scope, event: Arc<SyncEvent<Change>>, checkpoint }`.

## Backfill

`BackfillRunner::run_partition` walks
`inventory_partition_stream(scope, partition)` and emits
`BackfillCheckpoint`s at partition boundaries via
`CheckpointStore::put_backfill`. The orchestrator spawns runners
from `discover_cursor_scopes()` and plans partitions from
`Account::inventory_partitioning(scope)`. The partitioner
(`backfill/partitioner.rs::plan`) handles three strategies:
`TimeWindowed` (boundaries plus a final open-ended partition),
`UidRange` (newest-first chunking), and `PageCount` (page-size
chunking). `Full` falls through as a single partition; JMAP
Email currently advertises open-ended `PageCount`.

Backfill persistence calls `SyncControl::record_checkpoint` only
after `CheckpointStore::put_backfill` succeeds, so pause and
checkpoint waiters observe durable boundaries.

`LiveSupersedes` is the ring-evicting `(VecDeque + HashSet)` set
the multiplexer feeds with live `Created` ids so cold-start
hydration does not double-emit objects the live stream already
showed. Default cap `LIVE_SUPERSEDES_DEFAULT_CAP = 100_000`;
overflow drops the oldest insertion.

`BackfillCheckpointWriter` (in `backfill/checkpoint.rs`) is a
thin wrapper over `CheckpointStore::put_backfill` so the runner
does not need to know the persistence shape.

## Push

`InvalidationSinkInner` is a `DashMap<AccountId,
mpsc::Sender<WatchEvent>>` plus a drop counter. `attach` calls
`register(account, watch_tx)` with the per-account bounded mpsc
(capacity `MultiplexerConfig::watch_capacity`, default 256) that
the reconciler reads; `detach` calls `unregister`. The drop
counter is exposed as `bifrost_sync_push_dropped_total`.

Implements `bifrost_types::InvalidationSink::push`:
`try_send` first; on `Full`, increment the drop counter and
spawn a short task that does `tokio::time::timeout(100ms,
send().await)` of a synthesized `coalesced_event` carrying
`HintPayload::Unknown` so the reconciler still performs a full
reconcile rather than swallow the wakeup. `Closed` (account
detached mid-push) is silently ignored.

The in-process push forwarder spawned in `attach` runs the same
coalesce-on-full path against its per-account `tx` so an
`Account::push_stream` burst that outpaces the reconciler is
collapsed to an `Unknown` reconcile request rather than dropped.

`push::reconciler` receives `WatchEvent::Invalidated { hint }`,
calls `scopes_for_hint(&cursors, &hint.payload)` (which consults
the membership index populated at attach), then drives
`changes_stream` to completion for each affected scope. On
`Fatal`, the reconciler routes the recovery class to the slot's
reopen channel; `Retry { after }` sleeps in place. On
`Disconnected` / `Reconnected`, `Reconciler::warning_event`
synthesizes a `MultiplexerEvent` carrying
`SyncEvent::Warning { kind: WarningKind::Other("push:disconnected"
| "push:reconnected"), ... }` and `Reconnected` additionally
triggers a full `HintPayload::Unknown` reconcile.

`SubscriptionRegistry` (per-engine `DashMap<AccountId,
Vec<SubscriptionHandle>>`) stashes handles returned by
`Account::push_subscribe` so the consumer can later call
`SyncEngine::unsubscribe_push(account)` to walk the handles back
through `Account::push_unsubscribe`. `SyncEngine::subscribe_push`
is the engine-side entry that records the handle on success.

## Mutation pipeline

`bulk_set_flags` flow:

1. Submit the batch via `Account::bulk_set_flags(targets, op, key)`.
2. Collect `MutationOutcome::Failed` ids whose error is a
   `RecoveryClass::Retry { after }`.
3. Sleep `after`, resubmit with the **same** `IdempotencyKey`
   (engine bookkeeping; no protocol today emits it on the wire).
   Repeat up to `EngineConfig::mutation_max_retries`.
4. Run `run_readback_guard` once at the end against unresolved
   failures: `get_stream(Projection::FlagsOnly)` re-fetches and
   reconciles applied / skipped / failed_terminal.

`MutationCounters` buckets `Failed` outcomes by `Error` variant:
`is_terminal_mutation_error` returns true for `Auth`,
`Unsupported`, `MissingCoreCapability`, `CursorProtocolMismatch`,
`CursorEnvelopeUnknown`, `SchemaIncompatible`; those land in
`failed_terminal`. Everything else goes to `pending_retry`.

`IdempotencyKey` is `{ run_id, sequence, protocol_salt }`. `run_id`
is consumer-minted and consumer-persisted across process restarts
so retries from a previous process correlate.

`mutation::fanout::partition_by_account` is a cross-account
fanout helper: given an input stream of `(AccountId, T)` and a
prebuilt `HashMap<AccountId, mpsc::Sender<T>>`, it spawns a task
that routes each item to the matching sender, dropping items
addressed to unattached accounts.

`MutationConfig` (`fanout_buffer = 256`, `retry_queue_cap =
4096`) lives on `EngineConfig::mutation`. `PushConfig` is
currently empty (reserved for future push-only knobs; the
per-account `WatchEvent` mpsc capacity lives on
`MultiplexerConfig::watch_capacity`).

## Scheduler + budget

`Scheduler` is a strict-priority gate (not an executor) with four
lanes: `Foreground` / `Normal` / `Background` / `Bulk`. Starvation
guard forces a lower-lane pull after a configurable threshold of
consecutive higher-lane pulls (`SchedulerConfig::starvation_floor`,
default 64).

`LaneQueue` is bounded (default 1024 items, configurable via
`EngineConfig::lane_capacity`). On overflow, sheds according to
`LaneShedPolicy`: `DropOldest` (default, evicts the head) or
`DropNewest` (rejects the incoming submission). Both increment a
shed counter and emit a `warn!`.

`BudgetGate` exposes two semaphores per account (sync + mutation)
plus global caps. Lazy-creation uses
`DashMap::entry().or_insert_with(...)` to close the original
data race.

**Status (v1):** the scheduler and budget gate are intentionally
NOT WIRED into the engine's production work paths. Multiplexer,
backfill, and mutation tasks acquire from `Account::*_stream`
directly. The scheduler exists as infrastructure for a follow-up
pass that threads every protocol call through
`Scheduler::submit` and `BudgetGate::acquire`; `Scheduler` and
`BudgetGate` are deliberately absent from `lib.rs` re-exports
until then. See `scheduler/mod.rs` module docs.

## Control

`SyncControl` carries the priority hint, bandwidth-observed
counter, pause/resume token, and the boundary channel.
`record_checkpoint` wakes `pause().await` and
`checkpoint_now().await` waiters parked on
`boundary_recipient.notified()`. It is fired by:

- backfill, after `CheckpointStore::put_backfill` succeeds
  (`backfill/runner.rs`).
- the ack writer, after `put_change_cursor` /
  `put_backfill` succeeds for an `AckRequest` (`engine.rs`).

The mutation pipeline records counters, not checkpoints; it does
not call `record_checkpoint`.

`bandwidth_observed` is fed by the optional bandwidth-feed task
spawned in `attach` when `SyncEngineBuilder::with_bandwidth_meter`
was set; the task polls `BandwidthMeter::account(id).observed_bps()`
once per second and calls `control.observe_bandwidth(bps)`.

`Control` itself (the trait re-exported from `bifrost-types`) is
dyn-safe; `SyncControl` is the engine's concrete implementation.

## Cursor envelope

`OpaqueChangeState` carries `protocol`, `envelope_version`, and
`bytes`. `decode_envelope` returns:

- `Error::SchemaIncompatible` if `envelope_version <
  MIN_MIGRATABLE`.
- `Error::Other("cursor envelope: version N exceeds engine
  version M")` if `envelope_version > ENGINE_VERSION`.
- Otherwise decoded `Checkpoint::Change` / `Checkpoint::Backfill`.

(These are engine `Error` enum variants, not `RecoveryClass`
variants.)

`ChangeCursor` carries its own `envelope_version` separately from
`OpaqueChangeState.envelope_version` (outer vs inner versioning).

## Checkpoint store

```rust
pub trait CheckpointStore: Send + Sync {
    fn put_change_cursor<'a>(&'a self, account: &'a AccountId, cursor: ChangeCursor)
        -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;
    fn get_change_cursor<'a>(&'a self, account: &'a AccountId, scope: &'a CursorScope)
        -> Pin<Box<dyn Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>>;
    fn put_backfill<'a>(&'a self, account: &'a AccountId, checkpoint: BackfillCheckpoint)
        -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;
    fn get_backfill<'a>(&'a self, account: &'a AccountId, scope: &'a CursorScope)
        -> Pin<Box<dyn Future<Output = Result<Option<BackfillCheckpoint>, Error>> + Send + 'a>>;
    fn delete_change_cursor<'a>(&'a self, account: &'a AccountId, scope: &'a CursorScope)
        -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;
}
```

`put_*` take owned `ChangeCursor` / `BackfillCheckpoint`; `get_*`
take a borrowed `&CursorScope`. `delete_change_cursor` is required
because `RecoveryClass::RestartScope` must drop the durable cursor
so the next establish re-runs via inventory; a no-op delete would
silently preserve the stale cursor.

`InMemoryCheckpointStore` is the test backend (HashMap-backed). No
sled / sqlite default; storage is consumer-owned.

`Partition` is `Hash + Eq` (additive change to `bifrost-types`)
because the in-memory store keys on it.

## Read-back guard

Applies to all four protocols today (every protocol declares
`MutationReplaySafety::None`; the engine reads back after retry to
disambiguate ambiguous transport failures). The guard re-fetches
affected ids via `Account::get_stream(ids, Projection::FlagsOnly)`
and reconciles against the intended mutation.

## Error / Fatal / Warning

Engine-side `Error` (distinct from `bifrost-types::Error`):

- `AccountNotAttached(AccountId)`
- `AccountAlreadyAttached(AccountId)`
- `OpenFailed(#[source] bifrost_types::Error)`
- `EstablishCursorFailed(String)`
- `EstablishCursorFatal { message, recovery }`
- `CheckpointStore(String)`
- `SchemaIncompatible`
- `ShuttingDown`
- `Paused`
- `Account(#[from] bifrost_types::Error)`
- `Other(String)`

`Fatal` is a stream terminator carrying a `RecoveryClass`;
`FatalAction` is the engine's response (drop / restart-scope /
restart-account / reopen / surface to consumer). `Warning` is a
re-export of `bifrost_types::Warning`.

## File map

```
crates/sync/src/
  lib.rs                  // public re-exports; AccountId/Priority/etc.
                          // from bifrost-types
  engine.rs               // SyncEngine, SyncEngineBuilder,
                          // attach/detach/reopen/shutdown,
                          // ack_checkpoint, ack_writer,
                          // handle_recovery, scope_covers_membership
  control.rs              // SyncControl + record_checkpoint hook
  error.rs                // engine Error / Fatal / Warning
  types.rs                // EngineConfig, MultiplexerConfig,
                          // BackfillConfig, MutationConfig, PushConfig,
                          // SchedulerConfig, AccountSlot, WorkerTask
  multiplexer/
    mod.rs                // Multiplexer::run; ReopenRequest;
                          // MultiplexerEvent { scope, event, checkpoint };
                          // lifecycle_reopen wiring
    changes.rs            // drive_changes_stream + ChangesEvent +
                          // AckRequest (broadcast-then-consumer-ack)
    fusion.rs             // InventoryFusion::run_with_broadcast
    idle.rs poll.rs       // helper drivers (AdaptiveCadence)
  backfill/
    mod.rs                // BackfillHandle, BackfillRegistry,
                          // BackfillPolicy / Strategy
    runner.rs             // BackfillRunner::run_partition,
                          // LiveSupersedes (ring-evicting, default
                          // cap LIVE_SUPERSEDES_DEFAULT_CAP)
    partitioner.rs        // plan() for TimeWindowed / UidRange / PageCount
    checkpoint.rs         // BackfillCheckpointWriter wrapper
  push/
    mod.rs                // InvalidationSinkInner
                          // (DashMap<AccountId, mpsc>) + coalesced_event
    reconciler.rs         // hint -> scope -> changes_stream;
                          // warning_event on Disconnected/Reconnected
    subscription.rs       // SubscriptionRegistry (per-engine DashMap)
  mutation/
    mod.rs                // MutationHandle, MutationCounters,
                          // fanout::partition_by_account
    idempotency.rs        // IdempotencyKey vending
    readback.rs           // Projection::FlagsOnly read-back guard
  cursor/
    mod.rs                // CursorRegistry + membership index
    envelope.rs           // MIN_MIGRATABLE / ENGINE_VERSION + migrations
    store.rs              // CheckpointStore trait (5 methods) +
                          // InMemoryCheckpointStore
  scheduler/
    mod.rs                // four-lane Scheduler (NOT WIRED into work paths)
    lanes.rs              // bounded LaneQueue + LaneShedPolicy
                          // (DropOldest default / DropNewest)
    budget.rs             // BudgetGate (DashMap entry/or_insert_with)
  cancel/
    mod.rs boundary.rs    // safe-boundary mechanism

crates/sync/tests/
  cross_crate_conformance.rs  // cross-crate trait/contract checks
  envelope_roundtrip.rs       // cursor envelope encode/decode tests
  partition_planner.rs        // backfill partitioner unit tests
  readback_guard.rs           // mutation readback reconciliation
  scheduler_priority.rs       // lane ordering and starvation guard
```

## Open follow-ups (post-Phase-2 hardening)

- Scheduler / `BudgetGate` not yet wired into multiplexer,
  backfill, or mutation acquisition paths; tracked in
  `scheduler/mod.rs` module docs.
