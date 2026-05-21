# bifrost-sync reference

Current architecture of the sync engine. In-flight design lives in
`plans/bifrost-sync.md` and `plans/sync-engine.md`.

Scope: scheduler, multiplexer, backfill orchestrator, push
reconciler, mutation pipeline, checkpoint envelope versioning,
observability. Depends only on `bifrost-types`; opaque to every
protocol crate. Consumers wire one `Arc<dyn AccountFactory>` per
account.

## Architecture

```
SyncEngine
  AccountSlot per attached account:
    current: ArcSwap<Arc<dyn Account>>
    control: SyncControl
    workers: Vec<JoinHandle<()>>
    Multiplexer task        -> per-scope poll + lifecycle + reopen
    Backfill orchestrator   -> partition runner per scope
    Push reconciler task    -> InvalidationSink -> changes_stream
    Push forwarder task     -> in-process push_stream relay
    Mutation campaigns      -> bulk_set_flags retry loop
  Shared:
    Scheduler (four-lane: Foreground / Normal / Background / Bulk)
    BudgetGate (global + per-account semaphores)
    CursorRegistry (scope -> cursor; membership index)
    CheckpointStore (consumer-provided; InMemoryCheckpointStore for tests)
    InvalidationSink (bounded mpsc with coalesce-on-overflow)
```

`slot.current` is `ArcSwap<Arc<dyn Account>>` so reopens are
visible to spawned tasks immediately: every iteration calls
`current.load_full()`. No `RwLock` on the hot path.

## Lifecycle

```rust
let control = engine.attach(account_id, factory).await?;
// ... engine drives the account ...
engine.detach(account_id).await?;
```

`attach`:
1. `factory.open().await` -> `Arc<dyn Account>`.
2. Read `capabilities()` (snapshotted on the slot).
3. `discover_cursor_scopes()` -> for each, `establish_initial_cursor(scope)`:
   - `Ready(cursor)`: persist, start `changes_stream` immediately.
   - `EstablishViaInventory`: defer to backfill; the inventory
     pass's terminal `Done` carries the cursor.
4. `discover_memberships()` -> populate `CursorRegistry`
   membership index for push-hint routing.
5. Spawn workers: multiplexer, backfill orchestrator, push
   reconciler, push forwarder. Store all `JoinHandle`s.
6. Return `Control`.

`detach` sends `Stop` on the boundary channel, cancels the
shutdown token, calls `Account::close()`, awaits all worker
`JoinHandle`s with `EngineConfig::detach_timeout` (default 5s),
removes the slot.

`reopen` (driven by `RecoveryClass::RestartAccount` or
`CapabilityChanged`) calls `factory.open()` again and
`slot.current.store(Arc::new(next))`. Spawned tasks pick up the
new handle on their next `load_full()`.

## Stream contract: broadcast before cursor

`drive_changes_stream` broadcasts the batch onto the per-account
channel **before** writing the cursor to `CheckpointStore`. The
cursor write is the engine's commitment that the data has been
delivered downstream; reversing this order produces silent data
loss across restarts.

`InventoryFusion::run_with_broadcast` follows the same rule for
`EstablishViaInventory` scopes: each inventory `Batch` is forwarded
as the cursor-establishment pass progresses, and the cursor
persists only on a successful `Done`.

## Multiplexer

`Multiplexer::run` drives:

- **In-process push** (IMAP IDLE, JMAP WebSocket, EWS streaming)
  on the most-active cursor scope.
- **Adaptive polling** for everything else: per-scope
  `AdaptiveCadence` starts at 30s, doubles on no-change, caps at
  30 minutes. `updated_cadence(prev, had_changes)` is the pure
  helper.
- **Scope lifecycle** events from `Account::scope_lifecycle_stream`.
- **Reopen requests** on a `mpsc::Sender<ReopenRequest>` channel:
  `RestartScope` resets the scope's cursor to an empty-bytes stub
  and re-establishes via inventory; `RestartAccount` and
  `CapabilityChanged` route up to the engine's reopen path.

Output goes to the per-account `broadcast::Sender<MultiplexerEvent>`
that the consumer subscribes to.

## Backfill

`BackfillRunner::run_partition` walks `inventory_stream(scope)` and
emits `BackfillCheckpoint`s at partition boundaries via
`CheckpointStore::put_backfill`. The orchestrator spawns one
runner per scope from `discover_cursor_scopes()`.

Current shape: one inventory pass per scope; the
`BackfillPolicy::Strategy` (TimeWindow / UidRange / PageCount)
informs page-size hints but partition planning is a follow-up.

## Push

`InvalidationSinkInner` is a bounded `mpsc::Sender<WatchEvent>`
(default capacity per `MultiplexerConfig::watch_capacity`). On
overflow, **coalesces** into `HintPayload::Unknown` with the
original `PushSource` preserved, forcing a full reconcile rather
than dropping non-coalesced work. Drop counter incremented for
observability.

`push::reconciler` receives `WatchEvent::Invalidated { hint }`,
calls `CursorRegistry::scopes_for_hint(hint)` (which consults the
membership index populated at attach), then drives
`changes_stream` to completion for each affected scope.
`Disconnected` / `Reconnected` surface as `Warning`s.

For `push_in_process = true` accounts, the push forwarder task
relays `Account::push_stream` directly into the sink.

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
`Auth`, `Unsupported`, `MissingCoreCapability`, cursor-version
errors -> `failed_terminal`; everything else -> `pending_retry`.

`IdempotencyKey` is `{ run_id, sequence, protocol_salt }`. `run_id`
is consumer-minted and consumer-persisted across process restarts
so retries from a previous process correlate.

## Scheduler + budget

`Scheduler` is a strict-priority gate (not an executor) with four
lanes: `Foreground` / `Normal` / `Background` / `Bulk`. Starvation
guard forces a lower-lane pull after a configurable threshold of
consecutive higher-lane pulls.

`LaneQueue` is bounded (default 1024 items, configurable via
`EngineConfig::lane_capacity`). On overflow, sheds the oldest item
with a `warn!` log and a shed counter.

`BudgetGate` exposes two semaphores per account (sync + mutation)
plus global caps. Lazy-creation uses
`DashMap::entry().or_insert_with(...)` to close the original
data race.

## Control

`SyncControl` carries the priority hint, bandwidth observed
counter, pause/resume token, and the boundary channel. Every
checkpoint persistence path (changes, backfill, mutation) calls
`SyncControl::record_checkpoint(checkpoint)`, which wakes
`pause().await` and `checkpoint_now().await` waiters parked on
`boundary_recipient.notified()`.

`Control` itself (the trait re-exported from `bifrost-types`) is
dyn-safe; `SyncControl` is the engine's concrete implementation.

## Cursor envelope

`OpaqueChangeState` carries `protocol`, `envelope_version`, and
`bytes`. Read path:

- `envelope_version < MIN_MIGRATABLE` or `> ENGINE_VERSION` ->
  surface `RecoveryClass::SchemaIncompatible`.
- Otherwise decode via `decode_envelope` / re-encode forward via
  `encode_envelope` on the next checkpoint boundary.

`ChangeCursor` carries its own `envelope_version` separately from
`OpaqueChangeState.envelope_version` (outer vs inner versioning).

## Checkpoint store

```rust
pub trait CheckpointStore: Send + Sync {
    async fn put_change_cursor(&self, account: &AccountId, scope: &CursorScope, cursor: &ChangeCursor) -> Result<(), Error>;
    async fn get_change_cursor(&self, account: &AccountId, scope: &CursorScope) -> Result<Option<ChangeCursor>, Error>;
    async fn put_backfill(&self, account: &AccountId, scope: &CursorScope, checkpoint: &BackfillCheckpoint) -> Result<(), Error>;
    async fn get_backfill(&self, account: &AccountId, scope: &CursorScope) -> Result<Option<BackfillCheckpoint>, Error>;
}
```

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

- `Account(#[from] bifrost_types::Error)` - preserves the typed
  protocol-side error.
- `OpenFailed(#[source] bifrost_types::Error)` - factory.open()
  failed.
- `Cursor*` - envelope / schema / scope errors.

`Fatal` is a stream terminator carrying a `RecoveryClass`;
`FatalAction` is the engine's response (drop / restart-scope /
restart-account / reopen / surface to consumer).

`Warning::StrategyDowngraded { from, to }` uses `SyncStrategy`
states (not the transition enum).

## File map

```
crates/sync/src/
  lib.rs                  // public re-exports; AccountId/Priority/etc.
                          // from bifrost-types
  engine.rs               // SyncEngine, AccountSlot, attach/detach/reopen
  control.rs              // SyncControl + record_checkpoint hook
  error.rs                // engine Error / Fatal / Warning
  types.rs                // AccountSlot, EngineConfig, MultiplexerConfig
  multiplexer/
    mod.rs                // Multiplexer::run; reopen channel
    changes.rs            // drive_changes_stream (broadcast-before-cursor)
    fusion.rs             // InventoryFusion::run_with_broadcast
    idle.rs poll.rs       // helper drivers
  backfill/
    mod.rs runner.rs      // BackfillRunner::run_partition
    partitioner.rs        // partition planning (one-pass v1)
    checkpoint.rs         // BackfillCheckpoint persistence
  push/
    mod.rs                // InvalidationSink + coalesce-on-overflow
    reconciler.rs         // hint -> scope -> changes_stream
    subscription.rs       // push_subscribe/unsubscribe orchestration
  mutation/
    mod.rs                // bulk_set_flags pipeline + partition_by_account
    idempotency.rs        // IdempotencyKey vending
    readback.rs           // Projection::FlagsOnly read-back guard
  cursor/
    mod.rs                // CursorRegistry + membership index
    envelope.rs           // MIN_MIGRATABLE / ENGINE_VERSION + migrations
    store.rs              // CheckpointStore trait + InMemoryCheckpointStore
  scheduler/
    mod.rs                // four-lane Scheduler
    lanes.rs              // bounded LaneQueue + shed counter
    budget.rs             // BudgetGate (DashMap entry/or_insert_with)
  cancel/
    mod.rs boundary.rs    // safe-boundary mechanism
```

## Open follow-ups (post-Phase-2 hardening)

- `BackfillPolicy::Strategy` informs page sizing but partition
  planning is a single inventory pass per scope today.
- `RestartScope` resets the cursor to an empty-bytes stub with a
  placeholder `ProtocolKind`; replaced on the next
  `establish_initial_cursor`. Engine-internal bookkeeping; should
  never escape.
- The multiplexer routes `ChangesEvent::Fatal` conservatively to
  `ReopenRequest::Scope` regardless of inner `RecoveryClass`;
  more nuanced mapping (Retry vs RestartScope vs
  CapabilityChanged) needs a side-channel from
  `drive_changes_stream`.
- Subscription-health surfacing for out-of-process push (Gmail
  Pub/Sub, Graph webhooks) is silent today; tracked in
  `plans/account-trait.md` -> Open questions.
