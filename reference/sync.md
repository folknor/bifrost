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
   - Error whose derived `RecoveryClass` is
     `Engine(DisableScope(_))` (`scope_local_establish_failure`,
     pure): SKIP the scope, log, keep attaching. The protocol crate
     has already classified the failure as independently
     quarantinable (a revoked shared mailbox, an unreadable public
     folder), and the running path honors that via `disable_scope`;
     this is the same rule at attach. Previously every establish
     error propagated, so one dead share failed the whole account
     and the consumer got no sync at all, primary mail included.
     Any other error still fails the attach.
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

`reopen` (driven by `EngineDirective::RestartAccount`) calls
`factory.open(account_id)` again and
`slot.current.store(Arc::new(next))` after reapplying the priority
and bandwidth-cap snapshots. Spawned tasks pick up the new handle
on their next `load_full()`. Capability shifts no longer have a
dedicated directive variant; the convergence rewrite collapsed
them onto `RestartAccount` because account reopen already re-runs
`discover_cursor_scopes` / `discover_memberships` / `push_subscribe`.

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
`Advanced` / `Done` / `Stopped` / `Paused` / `Terminated(AccountError)`.

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
  Not every protocol feeds this stream: IMAP deliberately emits
  no lifecycle events (the stream stays open and yields nothing
  until shutdown). IMAP discovers folders only at open/reopen, and
  mid-session folder mutations surface as
  `WatchEvent::Invalidated` via push IDLE rather than as
  `ScopeLifecycle` events, so a folder that appears after attach is
  invisible to the engine until the next account reopen. True
  NOTIFY-MAILBOXES / LIST-diff lifecycle detection for IMAP is a
  deferred feature, not a bug.
- **Reopen requests** on a `mpsc::Sender<ReopenRequest>` channel:
  `RestartScope` deletes the in-memory and durable cursor before
  re-establishing; `RestartAccount` routes up to the engine's reopen
  path. Capability shifts arrive as `RestartAccount` since the
  `EngineDirective::CapabilityChanged` variant was removed in Phase
  5A.

Per-scope cancellation tokens live in
`Multiplexer::scope_tokens` so a `ScopeLifecycle::Deleted` stops
the matching poll task without disturbing siblings.

Output is a `broadcast::Sender<MultiplexerEvent>`. The event
carries `{ scope, event: Arc<SyncEvent<Change>>, checkpoint }`.

## Backfill

`BackfillRunner::run_partition` walks
`inventory_partition_stream(scope, partition)` and broadcasts each
page as a synthetic `Batch` carrying a `BackfillCheckpoint`. It does
NOT write to `CheckpointStore`: durable persistence is
consumer-ack-deferred, exactly like `drive_changes_stream`. The
consumer persists the page items, then calls
`SyncEngine::ack_checkpoint` with the `Checkpoint::Backfill`; the
ack writer routes that to `CheckpointStore::put_backfill`. This keeps
at-least-once intact for cold-start hydration - a crash between
broadcast and the consumer's write re-walks the partition on restart
(the orchestrator always restarts a scope from its first partition),
so no inventory page is lost. An eager engine-side write would let the
store record progress the consumer never durably persisted, losing
that page's objects permanently. The orchestrator spawns runners
from `discover_cursor_scopes()` and plans partitions from
`Account::inventory_partitioning(scope)`. Before walking any scope the
orchestrator parks on `wait_for_real_subscriber` (the same guard the
deferred-inventory fusion path uses): cold-start pages broadcast onto
the per-account channel during `attach`, but the consumer can only
`account_changes_stream` after `attach` inserts the slot, and a
`tokio::broadcast` receiver that joins late starts at the ring tail and
never sees values sent before it subscribed (the slot's sentinel
receiver keeps `receiver_count()` at 1, so the send "succeeds" in front
of no real reader). For a `Ready`-cursor account whose entire cold start
rides backfill (Gmail `CursorScope::Account`, JMAP
`CursorScope::Type(Email)`) skipping the wait silently drops the initial
inventory page and the consumer ingests zero objects. The partitioner
(`backfill/partitioner.rs::plan`) handles three strategies:
`TimeWindowed` (boundaries plus a final open-ended partition),
`UidRange` (newest-first chunking), and `PageCount` (page-size
chunking). `Full` falls through as a single partition; JMAP
Email currently advertises open-ended `PageCount`.

Open-ended `PageCount` (`total: None`) drives `BackfillPlan::OpenPages`:
the orchestrator walks `Page { from, to }` windows of width `chunk`,
advancing `from = to` each pass, and terminates only when a pass returns
a genuinely empty page (`outcome.seen == 0`), never on a merely short
one. The contract this rests on: an `OpenPages` partition stream MUST
fill its window (paging internally past any server-side page cap below
the window width) so that a short window is unambiguously the final
partial page and the next pass comes back empty. Terminating on a short
page instead would let a server whose query cap sits below `chunk`
truncate cold-start hydration after one page.

Resume: the `BackfillRegistry` (`BackfillState::Pending/Running/Completed`)
is in-memory only and is wiped when the slot is torn down, so it cannot
carry completion across an attach -> detach -> re-attach cycle. The sole
durable record is the consumer-acked `BackfillCheckpoint` in the
`CheckpointStore`. On completion the orchestrator broadcasts a durable
completion marker - a synthetic empty `Batch` whose `BackfillCheckpoint`
sits on the `completion_partition` sentinel key (`partitioner::
completion_partition`, distinct from every `page:F:T` / `uid:F:T` /
`time:F:T` key). It rides the same consumer-ack path as the page batches
(`emit_backfill_complete` -> broadcast -> consumer persist+ack -> ack
writer `put_backfill`), and because it is ordered behind every page it
only lands durably after the consumer has persisted all of them - a crash
before completion re-walks rather than recording a false "done". Its
`items_done` is stamped at `total_walked + 1` (the honest total rides in
`items_estimated`) so it strictly wins `get_backfill`'s "latest by
`items_done`" query regardless of how a store breaks ties, and on
re-attach the marker is the checkpoint returned.

This skip-on-complete signal is uniform across plan kinds; the difference
is whether positional resume is also possible:

- **`OpenPages`** (JMAP Email): the orchestrator runs the persisted
  checkpoint through the pure `open_pages_resume` decision - completion
  sentinel -> **skip entirely**; a short final `page:F:T`
  (`items_done < T - F`) -> also skip (inventory ran out inside that window
  even if no marker landed, e.g. the consumer never acked it); a full
  `page:F:T` -> resume the walk at `T` rather than page 0; no checkpoint or
  an unrecognised partition kind -> start fresh at 0.
- **`Fixed`** (`Full` / `TimeWindowed` / `UidRange`): the partition list is
  a known finite set with no positional "resume from here", so the signal
  is binary - the completion marker is present (`backfill_complete_recorded`
  -> **skip the whole plan**, no inventory call) or it is not (walk every
  partition). A crash mid-plan leaves no marker and re-walks all partitions
  on re-attach; re-emitting consumer-acked pages is idempotent, so nothing
  is lost. Positional mid-plan resume is a possible future refinement.

Because `get_backfill` returns a consumer-acked checkpoint, resume never
skips a window the consumer has not durably persisted; a read error falls
back to a full walk.

Pause and checkpoint waiters observe backfill boundaries through the
ack writer: it calls `SyncControl::record_checkpoint` after the
consumer-acked `put_backfill` lands, so waiters wake on a durable,
consumer-acknowledged boundary - identical to the change-cursor path.

`LiveSupersedes` is the ring-evicting `(VecDeque + HashSet)` set
the multiplexer feeds with live `Created` ids so cold-start
hydration does not double-emit objects the live stream already
showed. Default cap `LIVE_SUPERSEDES_DEFAULT_CAP = 100_000`;
overflow drops the oldest insertion.

`BackfillCheckpointWriter` (in `backfill/checkpoint.rs`) is a
thin wrapper over `CheckpointStore::put_backfill`. The runner no
longer persists (the ack writer does), so the wrapper is unused on
the hot path today; it remains as a typed helper for consumer-side
store wiring.

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
`Terminated(err)`, the reconciler routes through
`crate::recovery::plan_recovery` and forwards
`RecoveryPlan::Engine(directive)` to the slot's reopen channel
carrying the original account error; `Retry(advice)` sleeps for the
duration derived from `advice.retry_hint`. On
`Disconnected` / `Reconnected`, `Reconciler::warning_event`
synthesizes a `MultiplexerEvent` carrying
`SyncEvent::Warning { kind: WarningKind::Other("push:disconnected"
| "push:reconnected"), ... }` and `Reconnected` additionally
triggers a full `HintPayload::Unknown` reconcile.

`WatchEvent::Terminated(AccountError)` (push streams that classify
their own exit) flows through the same `plan_recovery` dispatch:
engine directives route through the reopen channel, terminal
classes broadcast `SyncEvent::Terminated(err)` so consumers observe
the structured error, and retryable / reconcilable verdicts emit a
warning while the in-process forwarder reconnects on its next
iteration.

`SubscriptionRegistry` (per-engine `DashMap<AccountId,
Vec<SubscriptionHandle>>`) stashes handles returned by
`Account::push_subscribe` so the consumer can later call
`SyncEngine::unsubscribe_push(account)` to walk the handles back
through `Account::push_unsubscribe`. `SyncEngine::subscribe_push`
is the engine-side entry that records the handle on success.

## Mutation pipeline

`bulk_set_flags` flow:

1. Submit the batch via `Account::bulk_set_flags(targets, op, key)`.
2. Collect `ItemOutcome::Failed` ids whose `AccountError::recovery()`
   is `RecoveryClass::Retry(advice)` or
   `RecoveryClass::Reconcile(_)`.
3. Sleep for the effective delay from `advice.retry_hint`
   (`RetryHint::min_delay(now)` or `RetryHint::not_before(now)` per
   the caller's scheduler shape) and resubmit with the **same**
   `IdempotencyKey`
   (engine bookkeeping; no protocol today emits it on the wire).
   Repeat up to `EngineConfig::mutation_max_retries`.
4. Run `run_readback_guard` once at the end against unresolved
   failures: `get_stream(Projection::FlagsOnly)` re-fetches and
   reconciles applied / skipped / failed_terminal.

`MutationCounters` buckets `ItemOutcome::Failed` outcomes via
`crate::recovery::plan_recovery`:
- `Retry { SameRequest | AfterAuthRefresh }` -> `pending_retry`.
- `Retry { AfterStateRefresh }` and
  `Reconcile { CheckTarget }` -> `pending_retry` then the
  read-back guard reclassifies into `skipped` /
  `failed_terminal`.
- `Reconcile { DedupeByClientId }` increments
  `dedupe_by_client_id` and broadcasts a
  `Warning::OperatorAttentionNeeded` (the dedupe lives at the
  consumer because the client-id space is theirs).
- `Engine(_)` -> `blocked_by_engine` AND the directive is
  forwarded through `ReopenRequest::Recovery` so the engine's
  recovery dispatch performs the restart / downgrade / schema
  clear. `blocked_by_engine` is distinct from `failed_terminal`:
  the campaign was halted by the engine, not by per-item
  terminal classification.
- terminal recovery -> `failed_terminal`.

`ItemOutcome::Uncertain` always queues for read-back so a
non-idempotent transport drop never replays blindly.

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

## Hydration passthrough

The change and inventory streams the engine broadcasts are
projection-only. A `Change` carries `{ id, kind }`; an `InventoryEntry`
carries a fingerprint and threading headers, never message content. A
consumer that turns those signals into real rows (message bodies,
attachment bytes, parsed threads) must fetch full content out-of-band.
The `Account` handle that can do so lives behind `slot.current`
(`ArcSwap<Arc<dyn Account>>`) and is otherwise private, so `SyncEngine`
exposes a read-only passthrough cluster as the consumer's single door
to hydration:

- `get_stream(account, ids, projection)` -> `Account::get_stream`. The
  primary entry: streams `ItemOutcome<HydratedObject>` for a stream of
  ids at a chosen `Projection`.
- `message_hydrate(account, id, projection)` -> `Account::message_hydrate`.
  One parsed `Message` at a `HydrationProjection`.
- `thread_hydrate(account, thread)` -> `Account::thread_hydrate`.
- `open_raw_rfc822(account, id)` -> `Account::open_raw_rfc822`. Verbatim
  server-assembled MIME octets.
- `open_blob(account, handle)` / `open_blob_range(account, handle, range)`
  -> the matching `Account` blob openers.

Two deliberate properties:

- **Always-live handle.** Every method resolves through the private
  `live_account` helper, i.e. `ArcSwap::load_full`, so a hydrate issued
  after a `RestartAccount` reopen runs against the freshly-installed
  connection, never a stale snapshot the consumer cached. This is the
  same discipline the spawned workers follow on their hot paths. The
  forwarded methods return `'static` streams / futures that capture
  their own internal `Arc` clones, so they outlive the short-lived
  handle resolved per call; an unattached account yields
  `Error::AccountNotAttached` up front.
- **Read surface only.** Mutations stay funnelled through
  `bulk_set_flags` so the idempotency / read-back / recovery pipeline
  remains the one chokepoint for writes; cursor and push driving stay
  engine-owned. Handing out a raw `Arc<dyn Account>` would leak both the
  write surface and the reopen-snapshot discipline, so the engine does
  not - it forwards the read methods explicitly instead.

These calls do not pass through the `Scheduler` / `BudgetGate` (neither
does any production path today; see below). Consumer-driven hydration
and engine-driven backfill share the same underlying client, where
`bifrost-net` is the rate-limit chokepoint.

Alongside the read-only hydration cluster, the engine forwards sibling
**direct passthrough** clusters that resolve through the same
`live_account` discipline and forward 1:1 to the matching `Account`
method, inventing no new semantics: object-level mutation conveniences,
compose / draft, container read + CRUD, the **contact** cluster
(`address_books_list`, `contacts_list`, `contact_get`, `contact_create`,
`contact_update`, `contact_delete`, `directory_search`), the
**server-filter** cluster (`filters_list`, `filter_create`,
`filter_update`, `filter_delete`, `filter_validate`) - whose supported
model a consumer reads off `capabilities().filter_rule_shape` and
`capabilities().pim_methods` before dispatching - and the **settings**
cluster (`identities_list`, `identity_update`, `vacation_get`,
`vacation_set`, `quota_get`), whose per-method support a consumer reads
off `capabilities().pim_methods`. Each mirrors the
`Account` trait's argument shapes but takes `account_id: &AccountId` and
returns the engine `Error` (the trait's `AccountError` folds in through
`?`); an unattached account yields `Error::AccountNotAttached` up front.
These are single-op conveniences and reads, so - like the container /
compose clusters - they deliberately bypass the idempotency / read-back /
recovery pipeline that guards the volume mutations.

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
`boundary_recipient.notified()`. It is fired by the ack writer after
`put_change_cursor` / `put_backfill` succeeds for an `AckRequest`
(`engine.rs`). Both the change-cursor and backfill paths are
consumer-ack-deferred, so every recorded boundary is one the consumer
has durably acknowledged; the backfill runner no longer writes or
records checkpoints itself.

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
because `RecoveryClass::Engine(EngineDirective::RestartScope)` must
drop the durable cursor so the next establish re-runs via inventory;
a no-op delete would silently preserve the stale cursor.

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

## Error / Terminated / Warning

Engine-side `Error` (distinct from `bifrost-types::AccountError`):

- `AccountNotAttached(AccountId)`
- `AccountAlreadyAttached(AccountId)`
- `OpenFailed(#[source] bifrost_types::AccountError)`
- `EstablishCursorFailed(String)`
- `EstablishCursorTerminated(#[source] bifrost_types::AccountError)`
- `CheckpointStore(String)`
- `SchemaIncompatible`
- `ShuttingDown`
- `Paused`
- `Account(#[from] bifrost_types::AccountError)`
- `Other(String)`

Stream termination is the `SyncEvent::Terminated(AccountError)`
variant; the engine dispatches every termination through
`crate::recovery::plan_recovery(error)`, which collapses
`AccountError::recovery()` into the closed
`RecoveryPlan { Retry, Reconcile, Engine, Terminal(Fatal) }` enum.
Adding a future `RecoveryClass` variant fails to compile in
`plan_recovery` (the four mutually-exclusive helpers cover the
case, but the closed `RecoveryPlan` enum enforces the dispatch
surface). `Engine(EngineDirective::*)` directives drive
`restart-scope` / `restart-account` / `downgrade-strategy` /
`downgrade-capability` / `schema-clear` / `operator-override` /
`disable-scope` flows. `DisableScope` quarantines a single cursor scope
(an admin revoked one shared/other-user IMAP folder mid-sync): the engine
deletes the scope's in-memory and durable cursor, drops it from the
membership index, and broadcasts a scoped
`Warning::OperatorAttentionNeeded`; the poll loop self-drains on the next
iteration, siblings keep syncing, and the account is neither paused nor
treated as auth-lost. `Terminal(Fatal)` is the type-system collapse point
that enforces "the engine has nothing left to try"; both the terminal arms
(multiplexer and engine) emit a structured `TelemetryView` `warn!` rather
than writing to any built-in queue or dashboard. Any operator-notification
queue is the consumer's to build off the broadcast
`SyncEvent::Terminated`.

Reopens use exponential backoff with ±20% jitter (1s initial, 5min
cap) and a three-attempt budget. After three failures the engine
broadcasts `SyncEvent::Terminated(last_error)` for the affected
scope (per-scope re-establishment) or for the account
(`RestartAccount`), then publishes
`AccountControl::Pause(PauseReason::RetryBudgetExhausted)` and
flips the boundary to `Pause` so workers park. Consumers subscribe
to the per-account `AccountControl` broadcast via
`SyncEngine::account_control_stream` and flip back via
`SyncEngine::resume_account`.

`EngineDirective::OperatorOverrideRequired { reason }` auto-pauses
the account with `PauseReason::OperatorOverrideRequired` and emits
a `Warning::OperatorAttentionNeeded` carrying the protocol-supplied
reason. The reason rides on the warning's free-form fields; the
`PauseReason` enum is bounded.

`crate::recovery::ThrottleBucket` is a `HashMap<ThrottleKey,
SystemTime>` keyed by `ThrottleKey::{Mailbox, Account, Tenant,
Provider}`. `Tenant` and `Provider` keys cross account boundaries
- a `Tenant` throttle pauses every account on that tenant.
`ThrottleScope::CurrentOperation` is a per-call hint and never
enters the bucket. The engine records `RetryAdvice::throttle_scope
+ retry_hint` on `Retry` dispatch via `apply_throttle`.

Cursor envelope schema mismatches at `get_change_cursor` are
translated through `crate::recovery::cursor_decode_failure` into an
`AccountError` whose derived recovery is
`Engine(SchemaIncompatible)`; the reopen listener then clears every
in-memory and durable cursor and re-establishes each scope under
the same backoff/budget contract above. Per-scope failures
escalate per-scope (account continues for sibling scopes);
`RestartAccount` failure escalates per-account (every scope pauses).

`Warning` is a re-export of `bifrost_types::Warning` and is
outside the error model (advisory only, never aborts streams).

## File map

```
crates/sync/src/
  lib.rs                  // public re-exports; AccountId/Priority/etc.
                          // from bifrost-types
  engine.rs               // SyncEngine, SyncEngineBuilder,
                          // attach/detach/reopen/shutdown,
                          // ack_checkpoint, ack_writer,
                          // handle_recovery, scope_covers_membership,
                          // live_account + hydration passthrough
  control.rs              // SyncControl + record_checkpoint hook
  error.rs                // engine Error wrapping AccountError + Warning
  types.rs                // EngineConfig, MultiplexerConfig,
                          // BackfillConfig, MutationConfig, PushConfig,
                          // SchedulerConfig, AccountSlot, WorkerTask
  recovery.rs             // plan_recovery + RecoveryPlan dispatch;
                          // ThrottleBucket + throttle_key_for;
                          // retry_delay; restart_scope_error;
                          // cursor_decode_failure translator
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
