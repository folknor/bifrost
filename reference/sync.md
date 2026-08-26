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
1. Take the per-account lifecycle guard
   (`AsyncMutex<HashSet<AccountId>>` on `engine.lifecycle_inflight`)
   and reject duplicate / racing attaches with
   `Error::AccountAlreadyAttached`. The guard is released on both
   success and failure paths so a failed `attach_inner` does not
   strand the slot. `detach` holds the same guard for its whole
   teardown (see below), so an attach racing a detach of the same id is
   refused rather than admitted into that window.
2. `factory.open(account_id).await` -> `OpenedAccount { account,
   skipped_scopes }`. The engine threads its own `AccountId` through
   so the protocol crate can register against `bifrost-net` /
   `MeterSink` / trace correlation under the same key the engine knows
   the account by. The skip lane - parts of the surface the protocol
   crate discovered but could not bring up (an unreachable foreign
   JMAP account, a failed composed-DAV open), each with its classified
   error - is warn-logged and stored on the slot;
   `SyncEngine::open_skipped_scopes(account)` exposes the current
   lane. `Err(_)` still fails the attach as `OpenFailed`: the contract
   reserves it for the primary surface being unavailable, precisely
   because this path has no retry budget (only the reopen path does).

   This open-time lane is the only skip lane the engine STORES. For the
   page-level lanes see "Page loss lanes" below.
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
   push forwarder, multiplexer,
   backfill orchestrator, deferred-inventory worker (if any),
   reopen listener, bandwidth feed (if a meter is wired). Store
   all `WorkerTask`s. The push forwarder parks while
   `PushCapability::None` and wakes on the reopen-generation signal,
   so a capability change does not require spawning a new task.
7. Return `SyncControl`.

Any error after `factory.open` and before slot installation closes the
opened account best-effort before returning, so failed discovery or
establishment cannot strand protocol workers or connections.

`detach` sends `Stop` on the boundary channel, cancels the
shutdown token, calls `Account::close()`, awaits all workers
with `EngineConfig::detach_timeout` (default 5s) and aborts
stragglers, removes the slot, unregisters the sink and ack
sender.

The whole teardown runs under the same `lifecycle_inflight` guard
`attach` takes, claimed together with the slot removal under one lock
acquisition. The slot leaves `engine.accounts` at the top while the
registry cleanup (invalidation sink, budget gate, backfill registry,
throttle memberships, bandwidth meter) happens at the very bottom,
after up to `detach_timeout` of worker awaits plus `Account::close()`.
Without the guard an attach landing in that window saw no slot, no
in-flight entry, succeeded, and then had its brand-new registrations
unregistered by the detach's tail - leaving an account that reported
itself attached while silently dropping every out-of-process push
(`push` returns early on a missing sender), with nothing logged. A
racing attach now gets `AccountAlreadyAttached`, which is literally
true: the incarnation is attached and draining. A racing detach, and an
attach still in flight, both yield `AccountNotAttached`. Pinned by
`tests/attach_schema_recovery.rs::attach_cannot_land_inside_an_in_flight_detach`.

`shutdown(self)` enumerates attached accounts, calls `detach`
for each (logging but not failing on per-account errors), then
cancels the engine-root token. Strongly preferred over relying
on `Drop`, which can only fire a best-effort sync cancel.

`reopen` (driven by `EngineDirective::RestartAccount`, and also public
as `SyncEngine::reopen`) is a staged reattach. It opens a replacement,
reapplies priority and bandwidth, rediscovers cursor scopes and
memberships into a temporary registry, establishes newly-appeared
scopes, removes vanished cursors, recreates registered push
subscriptions whose requested scopes still exist, refreshes the
capability snapshot, and then swaps the
handle and registry topology. On a successful swap the replacement
open's `skipped_scopes` replace the slot's stored lane, so a healed
namespace disappears from `open_skipped_scopes` and a still-degraded
one reappears with a fresh classification. The public entry is what a consumer pairs
with `capabilities().reopen_discovers_foreign_namespaces`: when that
flag is true, a share granted after the last open surfaces only through
this rediscovery, and the scheduling cadence (how often the reattach's
wire cost is worth paying) is consumer policy - the engine does not
schedule speculative reopens on its own. A generation watch wakes
the push and lifecycle readers even when their old streams never end.

Cursor and membership topology live under one registry state lock, so
reattach replaces both atomically. The same replacement increments a
registry generation while holding that lock. Every live change drive
captures the generation with its scope lease and conditionally publishes
the batch and installs its cursor together under the state lock. The swap
also overlays the final live cursor snapshot onto every retained scope.
A batch from an old handle that finishes after cutover is therefore fenced
out before broadcast or durable ack, instead of rolling either cursor copy
backward. This fencing avoids waiting for an unbounded old stream during
reopen.

## Page loss lanes

`Page<T>` carries two lanes beside `items`: `failed_ids` (resources the
provider fetched but could not materialize - an unparseable vCard, a
resource refused inside a 207) and `skipped_scopes` (scopes a multi-scope
walk quarantined rather than visited, each with its classified error).
Absence of results from a skipped scope is missing data, not evidence of
absence.

`Page` appears only on on-demand QUERY surfaces (`search`,
`search_messages`, `contacts_list`, `contacts_search`, `directory_*`, the
calendar list/range walks), never in the background sync pipeline, which
runs on streams and `SyncEvent`. The engine forwards four of them
(`contacts_list`, `directory_search`, `directory_groups_list`,
`directory_group_expand`) and does not expose the rest at all, so in every
case the consumer physically receives both lanes in the returned `Page`.
That copy is the actionable one: it names the scopes and carries their
errors.

What the engine adds is an announcement, not a record.
`announce_page_loss` emits a `SyncEvent::Warning`
(`OperatorAttentionNeeded`, with a `next_action` pointing at the lanes) on
the account's normal change stream when a forwarded page comes back with
either lane non-empty. The message carries COUNTS only - `failed_ids`
holds native provider identifiers, which are not user-safe text. A page
with both lanes empty emits nothing.

This is deliberately an event rather than engine state, and the reasoning
is worth keeping: a page lane is true of one walk at one moment. Unlike
the open-time lane there is no later point at which it can be said to have
healed, so accumulating it would need an invented expiry, an invented
dedupe key, and a cap. Worse, an accessor would be systematically
incomplete - the query surfaces the engine does not expose would never
contribute - so it would report "no skips" while a direct `Account` call
had just quarantined three scopes. A warning stream makes no completeness
claim; a queryable lane named after the data would.
If every requested scope of a registered push subscription vanished,
the record is dropped rather than widened to all discovered scopes; its
old handle is explicitly unsubscribed before the swap. All old-handle
push teardowns must succeed before the replacement is installed, so an
account that retains failed server-side DELETE state (Graph) remains
live and retryable instead of being closed with an orphaned subscription.
`Account::close()` is called best-effort after the swap. Any failure
before the swap closes the replacement and leaves the running handle
installed - and any subscription already created on that replacement is
torn down first, with every handle whose delete did not succeed kept in
the registry as `teardown_unconfirmed`. `close()` does not delete
server-side subscriptions, so a handle the engine forgets is an orphan
that keeps delivering until the provider expires it; an unconfirmed
record is therefore never recreated against a replacement, is carried
across swaps, and is retried by the next reopen or `unsubscribe_push`.
A repeat failure on an already-unconfirmed record is logged rather than
aborting the swap, because a handle belonging to a long-dead connection
must not wedge every future reopen.

Cursor establishment for newly discovered scopes is staged in memory. After
replacement subscription creation, reattach durably persists only the cursors
it freshly created - a scope whose row already existed in the store (persisted
by a prior session and rediscovered by the replacement) is resumed from,
never rewritten. An aborted swap (store failure or old-handle teardown
failure) compensates by deleting exactly those freshly created rows, so a
replacement that is later closed cannot leave durable cursor state for a
topology the engine never installed, and the abort path can never destroy or
overwrite a preexisting row. Vanished-scope rows are deleted only after the
cutover is committed: the old account's ack writer persists checkpoints
concurrently with a reopen (nothing serializes it against one), so a
pre-cutover delete would need a snapshot-and-restore that races that writer
and could overwrite a newer acknowledged checkpoint. A failed post-cutover
delete, or a retired stream's late ack re-persisting one, leaks a stale row
that a later rediscovery validly resumes from - a leak, never data loss.
Compensation failure is error-logged because the checkpoint-store trait has
no transaction primitive.

The rule that makes the above safe: **never write what an abort would have to
restore**, and derive the "preexisting versus freshly created" classification
from the SAME store read the action uses. A separate pre-check `get` whose
errors were swallowed (`.is_ok_and`) demoted a preexisting prior-session row to
"created" on a transient read failure, and the abort path then deleted it - the
exact data loss the staged design existed to eliminate, resurfacing through the
error path of a duplicated read, plus a TOCTOU between the two reads.
`run_establish` now reports the origin off its own single read
(`EstablishOrigin`), so a store error aborts the reattach before any durable
write. Pinned by
`transient_get_failure_cannot_demote_preexisting_cursor_to_created`. Generally:
a classification that feeds a destructive compensation must come from the read
the action uses, and must fail closed on error.

**One writer per account owns every durable mutation.** Reattach does NOT
touch `CheckpointStore` directly: it persists freshly created cursors and rolls
them back through the same `WriterRequest` channel the ack writer serves.
Direct writes raced acknowledged ones, and not hypothetically - `run_establish`
hands `changes_tx` to `InventoryFusion`, so a replacement's inventory pass
broadcasts checkpoint-bearing batches BEFORE the cutover, a consumer can
acknowledge one, and the abort path then deleted the row that acknowledgement
had just committed.

Serializing is necessary but not sufficient on its own: a FIFO queue would
order the acknowledged write ahead of an unconditional delete and faithfully
destroy it. The writer therefore tracks which scopes are still PROVISIONAL -
inserted by an uncommitted reattach and not since claimed by an
acknowledgement - and an abort deletes exactly those. That makes the rollback a
conditional delete implemented in the engine, where there is one
implementation, rather than a compare-and-swap every `CheckpointStore` backend
would have to get right. `commit_reattach_inserts` clears the marks past the
last abort point, so a later reattach's abort cannot delete rows belonging to
one that succeeded; it runs after the generation bump because the lifecycle
reader is waiting on that watch and no await belongs between the topology swap
and the bump that publishes it.

A compare-and-swap primitive on `CheckpointStore` would remove the class
outright. It was considered and deliberately not taken - it is a published trait
change, so it is an owner-level proposal rather than hardening work.

A public or engine-initiated reopen queues behind `Pause` and runs after
resume: pause is a quiescence boundary, not permission to open a
replacement connection in the background. The activity registration that
makes the account non-quiescent is taken BEFORE `factory.open()`, not
after it, so `pause` cannot report quiescence while a replacement
connection is being established; a pause that wins the race against the
registration means nothing was opened at all, and the caller loops back
to the boundary wait without spending a retry attempt.

## Stream contract: broadcast + consumer ack

### Batch boundary validation

Before anything else, `drive_changes_stream` and `InventoryFusion::run_stream`
call `validate_boundary` on every batch the account yields. `Batch`'s fields
are public, so `PageBoundary::Partial` carrying a checkpoint is constructible
and no type can stop a protocol crate from emitting it.

A violation is NOT a bare `Error::Other`. `handle_drive_outcome` only logs a
generic engine error before re-entering the poll loop from the same unchanged
cursor, so against a provider that keeps producing the shape that is an
unbounded run of requests with no recovery dispatch, no scope stop, and
nothing on the stream to tell a consumer the scope has quietly stopped making
progress. `recovery::batch_boundary_violation` classifies it as
`Protocol(ContractViolation)`, which derives to the terminal
`ProviderContractViolation`; the driver publishes `SyncEvent::Terminated` to
subscribers and returns `ChangesEvent::Terminated` /
`FusionOutcome::Terminated`, so `plan_recovery` stops the scope. A provider
emitting a nonsense boundary does not heal by being asked again.

More generally, `handle_drive_outcome` normalizes every `Error::Account` onto
the same `plan_recovery` path as `ChangesEvent::Terminated`; the generic
log-and-repoll arm is reserved for engine-internal errors with no account
recovery classification. Cursor-envelope rejection publishes `Terminated`
before returning its classified account error, so an engine directive is
dispatched and subscribers are not left with a silently stalled scope.

The protocol tag on that error is read off the offending checkpoint's change
cursor. There is no protocol accessor on `dyn Account`, so a checkpoint that
carries no tag yields `Protocol::Unknown` rather than a guess.

### Broadcast

`drive_changes_stream` broadcasts each `Batch` (item plus
optional `Checkpoint`) onto the per-account broadcast channel and
advances the in-memory `CursorRegistry` immediately so the next
poll iteration starts from the freshly-yielded cursor. It does
NOT write to `CheckpointStore`. Durable persistence is consumer-
ack-driven:

Polling and push reconciliation claim the same async drive lease for
each `CursorScope` before snapshotting its cursor and hold it through
the stream drive. This makes lane + scope a single-producer channel:
the next producer starts from the cursor installed by the previous one,
and checkpoint supersession cannot retire another producer's unacked
batch.

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
   `checkpoint_now` waiters. A returned `Some(checkpoint)` is
   durable; `None` means the account is safely idle without any
   durable checkpoint yet.

On restart the engine reads the last-acked cursor from the store
and re-runs `changes_stream` from there; items the consumer
never durably persisted come across again.

`InventoryFusion::run_with_broadcast` follows the same shape for
`EstablishViaInventory` scopes: inventory batches stream as the
cursor-establishment pass progresses. A provider may attach a page checkpoint
before its terminal cursor exists; that checkpoint persists only after the
consumer ack, and BOTH cursor-establishment paths - `establish_initial_cursor`
at attach and `run_establish` on reopen - ask whether the saved state is a
resumable inventory position before putting it into the registry. The default
`is_inventory_cursor` implementation delegates directly to
`inventory_resume_stream(cursor.clone()).is_some()`, and the resume seam guards
both directions of that equivalence with classified schema recovery so an
override cannot silently diverge. A
stored page position must never reach `changes_stream`, which has no delta link
to walk. The terminal
`Done` still installs the ordinary live
change cursor in the registry.

`ChangesEvent` is the per-batch outcome returned by the driver:
`Advanced` / `Done` / `Stopped` / `Paused` / `Terminated(AccountError)`.

`account_changes_stream` returns `ChangesReceiver`, which preserves the
ordinary broadcast `recv` / `try_recv` shape but converts ring overflow
into a synthetic account-scoped `OperatorAttentionNeeded` warning. The
warning carries the overwritten batch count and directs the consumer to
reconcile from its last acknowledged checkpoint; retained ring entries
remain readable afterward. The engine cannot replay the overwritten
batches in-session, but it never presents that loss as success.

Observing a lag also abandons the account's outstanding checkpoint
registrations (`SyncControl::abandon_pending_checkpoints`), and the
warning's `next_action` reports how many. This is load-bearing, not
tidy-up: `expect_checkpoint` runs before the send and an entry leaves
only through a matching consumer ack, so a registration whose batch the
ring destroyed can never be retired and would gate every later `pause` /
`checkpoint_now` on the account forever. Surfacing lag without this
would turn silent in-session loss into a permanent hang. The cost is
bounded and disclosed: a registration whose batch is still in the ring
is also abandoned, so a boundary wait taken between the lag and that
batch's ack reports the previous durable checkpoint rather than the
newer one. Nothing durable is invented - the snapshot is not advanced -
and the accompanying warning is what tells the consumer to reconcile.

## Multiplexer

`Multiplexer::run` drives:

- **Adaptive polling** per scope:
  `AdaptiveCadence` starts at `MultiplexerConfig::poll_initial`
  (default 60s), halves on observed change down to `poll_min`
  (default 30s), doubles after **five** consecutive no-change
  ticks up to `poll_max` (default 30 minutes). The pure helper is
  `Multiplexer::updated_cadence(cur, seen_change, min, max)`.
- **Scope lifecycle** events from `Account::scope_lifecycle_stream`.
  `Created` and `Renamed` consult the registered cursor shapes.
  Account-wide and type-wide models already cover the new membership
  and create no cursor. Per-folder and query models translate the
  membership into matching `RestartScope` requests so the engine
  establishes only shapes the protocol already advertises. `Deleted`
  cancels the per-scope token and drops the cursor.
  Not every protocol feeds this stream: IMAP deliberately emits
  no lifecycle events (the stream stays open and yields nothing
  until shutdown). IMAP discovers folders only at open/reopen, and
  mid-session folder mutations surface as
  `WatchEvent::Invalidated` via push IDLE rather than as
  `ScopeLifecycle` events, so a folder that appears after attach is
  invisible to the engine until the next account reopen. True
  NOTIFY-MAILBOXES / LIST-diff lifecycle detection for IMAP is a
  deferred feature, not a bug. The lifecycle reader reloads the
  replacement account whenever the reopen-generation watch changes;
  a naturally-ended or retryable stream reconnects with bounded
  exponential backoff. A terminal lifecycle error is broadcast once and
  stops that reader. Every non-terminal classified error is handed to
  the reopen channel exactly once - the reopen path owns the
  three-strike budget, so one dead lifecycle stream cannot enqueue
  unbounded account reopens - and only `RestartAccount`, the one
  directive that actually replaces this reader's connection, then waits
  for the generation change. That wait is bounded (30s): a reopen that
  exhausts its budget or queues behind a pause never bumps the
  generation. Every other directive (scope restarts, scope disable,
  schema reset, capability downgrade, operator override) is handled
  without swapping the account, so the reader reconnects on its own
  backoff instead of parking for a signal that is never coming.
- **Reopen requests** on a `mpsc::Sender<ReopenRequest>` channel:
  `RestartScope` deletes the in-memory and durable cursor before
  re-establishing; `RestartAccount` routes up to the engine's reopen
  path. Capability shifts arrive as `RestartAccount` since the
  `EngineDirective::CapabilityChanged` variant was removed in Phase
  5A.

Per-scope cancellation tokens live in
`Multiplexer::scope_tokens` so a `ScopeLifecycle::Deleted` stops
the matching poll task without disturbing siblings. Each entry is a
`ScopeToken { generation, token }` and an exiting task retires its
registration by generation match (`retire_scope_token`), never by key.
Removing by key lets a departing task evict a successor's entry: task A
exits while its scope is momentarily absent from the registry
(mid-`restart_scope`), the 1s scan spawns B under the same key, A's
cleanup deletes B's entry, and the next scan - seeing no token - spawns
C. B and C then poll one scope forever, which is both doubled wire
traffic and two concurrent producers on a lane+scope that the
checkpoint supersession rule in `control.rs` assumes has exactly one.

Output is a `broadcast::Sender<MultiplexerEvent>`. The event
carries `{ scope, event: Arc<SyncEvent<Change>>, checkpoint }`.

After publishing any complete stream item, `Pause` returns `Paused` and
drops the activity guard even when that item carried no checkpoint.
`CheckpointNow` still waits for a checkpoint-bearing item because its
request is specifically for a durable cursor boundary.

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
that page's objects permanently. The orchestrator continuously rescans
the cursor registry by scope incarnation and plans partitions from
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
chunking). `Full` falls through as a single partition. No protocol crate
in this workspace currently advertises `PageCount`: JMAP Email used to,
and now reports `Full`, because its inventory pages `Email/query` by
`anchor` rather than by integer position and an engine partition
boundary would discard that anchor (see `reference/jmap.md`). The
`PageCount` strategy and the `OpenPages` plan it drives stay published
for a protocol whose paging really is positionally stable.

Every range partition in this workspace is HALF-OPEN, `[from, to)`, with no
exceptions. `PartitionBounds::Time`, `PartitionBounds::Page`,
`InventoryPartition::Time`, `InventoryPartition::Page` and
`CoverageCoordinate::PageRange` always were; `PartitionBounds::Uid`,
`InventoryPartition::Uid` and `CoverageCoordinate::UidRange` were inclusive
until this was unified, and are now half-open too. Their endpoints are `u64`
rather than `u32` for exactly that reason: a walk covering the whole UID space
has an exclusive end of `u32::MAX + 1`, which a `u32` cannot hold. The UID
values themselves are still `u32` - `InventoryPartitioning::UidRange`'s
`max_uid` and the partitioner's `total` stay `u32`, and the widening is only
in the range endpoints. No protocol crate currently serves
`InventoryPartition::Uid`, so the only producers and consumers are the
partitioner and `CoverageDomain::for_partition`; `CoverageDomain::covers`
tests containment (`self_from <= other_from && self_to >= other_to`) and reads
identically under either convention.

One upgrade consequence: `partition_key` renders a UID partition as
`uid:{from}:{to}`, so durable backfill checkpoints written under the old
inclusive convention no longer match the newly planned keys. Those partitions
re-walk once. That is re-work, not loss - a missing partition checkpoint means
"start this chunk over", and the chunk boundaries themselves are unchanged.

The deferred-inventory set captured at attach is a structural exclusion:
the first registry incarnation of each such scope belongs to
`InventoryFusion` and is never walked again by backfill, regardless of
which worker wins the subscriber wake. Newly-created scopes and later
delete + re-establish incarnations are discovered by subsequent scans
and receive their own pass.

The rescan retires an incarnation only on a durable conclusion: the plan
ran to completion, or the checkpoint store already carries its
completion marker. A partition failure or a checkpoint-store read
failure deliberately leaves the scope `Pending`, so those incarnations
stay eligible and return on an exponential delay (5s doubling to a 5min
ceiling) rather than being filtered out for the rest of the attachment -
otherwise one transient request would cost that scope its whole backfill
until the next attach. `CursorRegistry::all_scope_incarnations`
enumerates live cursors and annotates each with its incarnation, so the
rescan can never plan a walk for a scope the registry has dropped.

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
is an engine-wide observability index keyed by `(AccountId, CursorScope)`.
Account rows are removed on detach, so the registry cannot merge
identically-shaped scopes from different accounts or carry completion
across an attach -> detach -> re-attach cycle. The sole durable record is
the consumer-acked `BackfillCheckpoint` in the `CheckpointStore`. On
completion the orchestrator broadcasts a durable completion marker - a
synthetic empty `Batch` whose `BackfillCheckpoint` sits on the
`completion_partition` sentinel key (`partitioner::completion_partition`,
distinct from every `page:F:T` / `uid:F:T` / `time:F:T` key). It rides
the same consumer-ack path as the page batches (`emit_backfill_complete`
-> broadcast -> consumer persist+ack -> ack writer `put_backfill`), and
because it is ordered behind every page it only lands durably after the
consumer has persisted all of them - a crash before completion re-walks
rather than recording a false "done". Its `items_done` is stamped at
`total_walked + 1` (the honest total rides in `items_estimated`) so it
strictly wins `get_backfill`'s "latest by items_done" query regardless of
how a store breaks ties, and on re-attach the marker is the checkpoint
returned.

This skip-on-complete signal is uniform across plan kinds; the difference
is whether positional resume is also possible:

- **`OpenPages`** (no current producer; see `PageCount` above): the
  orchestrator runs the persisted
  checkpoint through the pure `open_pages_resume` decision - completion
  sentinel -> **skip entirely**; any other `page:F:T` -> resume the walk
  at `T` rather than page 0; no checkpoint or an unrecognised partition
  kind -> start fresh at 0. A SHORT page (`items_done < T - F`) is
  deliberately not read as exhaustion: a partition may emit fewer entries
  than its window width while the scope still has results (ids deleted
  between the listing call and the hydration call, id-less objects
  dropped), so
  only the completion marker proves exhaustion. Resuming at `T` after a
  genuinely final short page costs one empty probe query, which then
  lands the marker.
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
`BackfillRunner::run_partition` filters each inventory page through,
via `filter_supersedes`. Default cap
`LIVE_SUPERSEDES_DEFAULT_CAP = 100_000`; overflow drops the oldest
insertion.

**Nothing populates it, and that is deliberate.** No production
path calls `add`, so the filter is a no-op and every inventory
entry is forwarded. Cold-start can therefore re-emit a `Created`
for an object the live stream already announced. That duplicate is
accepted: consumers must already tolerate a repeated `Created`,
because a crash mid-plan re-walks every partition and re-emits
consumer-acked pages.

Populating it from `drive_changes_stream` looks obvious and is
wrong, for two independent reasons:

- **Broadcasting is not receiving.** The per-account channel is a
  `tokio::broadcast`. The slot's sentinel receiver makes `send`
  report success with no consumer attached, a subscriber that
  attaches later starts at the ring's tail, and a lagging
  subscriber has values overwritten out from under it. `ChangesReceiver`
  surfaces the skipped count but the crate still cannot replay those
  entries. Recording on `send`
  records ids the consumer will never see, and suppressing the
  inventory copy of one of those loses that object for the session,
  since the in-memory cursor has already advanced.
- **Selection and publication are separate operations.** The runner
  decides to keep an entry and broadcasts it later; the live feed
  records and broadcasts separately. These are distinct spawned
  tasks on a multi-threaded runtime, so they run genuinely in
  parallel - the absence of an await between the runner's `take` and
  its `send` does not close the window. A mutex makes each `add` and
  `take` atomic but does nothing for the compound take-then-publish,
  which is what the guarantee needs.

The only sound record trigger the engine has is the consumer ack:
`ack_checkpoint` is what this crate already treats as proof of
receipt, precisely because broadcast delivery is not. A correct
design buffers ids as pending keyed by their batch's checkpoint and
promotes them only on ack, with a policy for batches carrying no
checkpoint and a bound on the pending buffer. It would also need
take-and-publish to be one indivisible decision on both sides, and
a test-only seam to prove it - the window has no scheduling point,
so no mock server can stage the interleaving black-box.

`take` is O(1): it removes from the membership set only and leaves
a tombstone slot in the eviction ring, so a future sound producer
does not make cold-start quadratic. A tombstone cannot cause a
wrong suppression - suppression reads the membership set, which
only `add` writes - and eviction reclaims tombstone slots as they
reach the front, so membership reconverges on `cap` with no scan.

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
`try_send` first; on `Full`, increment the drop counter only for
coalescible invalidations and connection-health transitions. They are
coalesced into one `HintPayload::Unknown` send with a 100ms bound.
`Terminated` and `Warning` are lossless control information. Each
registration made on a Tokio runtime creates one ordered forwarder on that
runtime and stores its unbounded ingress sender, so calls from receiver
threads outside Tokio do not depend on whichever account registered first
and later events cannot bypass an event waiting for bounded queue space. A
registration made outside Tokio is explicitly a direct, non-waiting fallback;
a full queue drops and counts every event. `Closed` (account detached
mid-push) is silently ignored.

The in-process push forwarder spawned in `attach` runs the same
overflow policy against its per-account `tx`: redundant invalidations
collapse to an `Unknown` reconcile request, while warnings and
terminations retain their original classification.

`push::reconciler` receives `WatchEvent::Invalidated { hint }`,
calls `scopes_for_hint(&cursors, &hint.payload)` (which consults
the membership index populated at attach), then drives
`changes_stream` to completion for each affected scope. On
`Terminated(err)`, the reconciler routes through
`crate::recovery::plan_recovery` and forwards
`RecoveryPlan::Engine(directive)` to the slot's reopen channel
carrying the original account error; `Retry(advice)` sleeps for the
duration derived from `advice.retry_hint`.

`Disconnected` emits an account-scoped
`WarningKind::Other` with message "push transport disconnected".
`Reconnected` emits no warning and triggers a full
`HintPayload::Unknown` reconcile whose synthetic source is
`PushSource::Coalesced`.

`WatchEvent::Terminated(AccountError)` (push streams that classify
their own exit) flows through the same `plan_recovery` dispatch:
engine directives route through the reopen channel, terminal
classes broadcast `SyncEvent::Terminated(err)` so consumers observe
the structured error, and retryable / reconcilable verdicts emit a
warning while the in-process forwarder reconnects on its next
iteration.

`SubscriptionRegistry` (per-engine `DashMap<AccountId,
Vec<RegisteredSubscription>>`) stashes each returned handle together
with only the scopes in `PushSubscription.outcomes`' succeeded lane. Failed
scopes remain polling-only and are never recorded as covered. The validated
three-lane outcome accounts for every requested scope, and an all-rejected
request returns no handle. That accepted-scope snapshot lets account
reopen recreate subscriptions against the replacement topology.
The consumer can later call
`SyncEngine::unsubscribe_push(account)` to walk the handles back
through `Account::push_unsubscribe`. A failed teardown is returned to
the caller and its registry record is retained, while successful records
are retired; a later call retries only the failed handles. This composes
with Graph's retained server ids after a failed DELETE.
`SyncEngine::subscribe_push` is the engine-side entry that returns the
per-scope result and records its handle only when at least one scope succeeded.
Reopen likewise recreates and records only accepted scopes. Both entries
serialize on the slot's reopen lock:
reattach snapshots the registry, tears old handles down, and installs
the replacement set wholesale, so a registration or teardown landing
inside that window would either be silently erased - orphaning a
server-side subscription whose handle is connection-local state on
Graph and IMAP - or resurrect records the consumer just tore down.
The lock means both calls queue behind an active open/swap attempt rather
than racing it. A reopen queued behind `Pause` does not hold the lock
while it waits for `Run`, so `unsubscribe_push` remains available for
shutdown cleanup during an indefinitely paused account.

That property is structural rather than remembered. `open_replacement`
is the single acquisition site for an open/swap, taking the guard after
its activity registration and returning it in its `Ok` tuple so it lives
exactly as long as the open plus the `reattach_account` that consumes
it. Callers wait for the boundary to read `Run` before calling in and
hold nothing while they wait, so "hold the reopen lock across a pause"
is not a shape a caller can write. `handle_engine_directive`'s other
directives are bounded scope-local repairs that must not interleave with
a swap, so `handle_account_error` still takes the lock around them - and
skips it for `RestartAccount` precisely because that path reaches
`open_replacement`; reintroducing it there deadlocks loudly instead of
regressing quietly.

Two entry points sit one layer apart and their names read as near
anagrams of each other, so it is worth naming the difference:
`Account::push_unsubscribe(handle)` is the protocol-crate trait method
and destroys ONE subscription; `SyncEngine::unsubscribe_push(account)`
is the engine method that takes the account's registry records and
calls the trait method once per record. An application holds an engine,
not an `Account`, so an application calls the second.

`detach` drops the detaching incarnation's registry records. The handles
in the registry are connection-local on Graph and IMAP, so carrying them
across a detach would let a later attach of the same `AccountId` inherit
handles minted by a dead connection and present them to the provider as
live. Dropping loses nothing retryable: after detach, `unsubscribe_push`
rejects with `AccountNotAttached` and reopen only runs on an attached
slot, so nothing could have reached those records anyway.

Detach is therefore the last point at which server-side teardown is
possible, and it deliberately does NOT perform it - the contract leaves
that with the consumer, because push delivers to a consumer-owned
endpoint and an application that shuts down wanting events to queue for
its next start is a legitimate pattern that unconditional teardown would
break silently. A detach with records still registered means
`unsubscribe_push` was never called, so the provider holds live
subscriptions until it expires them itself (24h for Graph); that case is
logged on `bifrost.sync.push` rather than absorbed.

## Mutation pipeline

All three bulk operations share `run_bulk_pipeline`. `BulkPipelineOp`
selects `SetFlags`, `Move`, or `Destroy`, which determines the wire
method and final read-back guard. Campaign flow:

1. Submit the batch via the arm's wire method, all sharing `key`:
   `Account::bulk_set_flags(targets, op, key)`,
   `Account::bulk_move_from(targets, destination, source, key)`, or
   `Account::bulk_destroy(targets, key)`.
2. Collect `ItemOutcome::Failed` ids whose `AccountError::recovery()`
   is `RecoveryClass::Retry(advice)` or
   `RecoveryClass::Reconcile(_)`.
   If the stream itself terminates with `Retry`, ids the stream never
   resolved join the retry set too - a stream can end before it emits
   an outcome for every submitted target, and without that sweep those
   ids are neither resubmitted, read back, nor counted. The sweep is
   deliberately narrower than the `Reconcile` / `Engine` termination
   arms: it claims only unseen ids and ids already queued for retry.
   An id sitting in the read-back lane belongs to the guard (that lane
   exists so a write whose first attempt may have landed is verified
   rather than replayed), and an engine-blocked id is waiting on a
   directive.
3. Sleep for the effective delay from `advice.retry_hint`
   (`RetryHint::min_delay(now)` or `RetryHint::not_before(now)` per
   the caller's scheduler shape) and resubmit with the **same**
   `IdempotencyKey`
   (engine bookkeeping; no protocol today emits it on the wire).
   Repeat up to `EngineConfig::mutation_max_retries`.

   One resubmission is at most `MutationConfig::retry_queue_cap` targets
   wide (default 4096). The retry set only shrinks - it is filtered out of
   the ids still outstanding - so this is not a guard against unbounded
   growth; it caps the per-attempt work one oversized campaign can demand of
   an account, which would otherwise resubmit its whole still-failing set on
   every attempt. Targets past the cap are marked `PendingRetry` AT THE
   TRUNCATION, not left to the sweep that runs when the loop exits: an id
   merely truncated out of the resubmission is no longer among the campaign's
   outstanding targets, so no later attempt resolves it and it would leave
   the campaign counted in no lane at all - the campaign reporting success
   for work that never happened. Marked, it is counted as pending and then
   passes through the read-back guard like any other unresolved id.
   `retry_queue_cap_bounds_a_resubmission_without_losing_the_excess` pins
   both halves, and was checked by ablation against each.
4. Run `run_readback_guard` once at the end against unresolved
   failures: `get_stream(Projection::FlagsOnly)` re-fetches and
   reconciles applied / skipped / failed_terminal. The read-back set
   is derived from the per-id outcome map when the retry loop exits
   (every id still `PendingRetry` or `PendingReadback`), not
   accumulated during it, so an id parked in the read-back lane cannot
   be dropped by a later attempt that resubmits its siblings.

`ItemOutcome::Succeeded` buckets by its `MutationSuccess` payload:
`Applied` -> `applied`, `Skipped` -> `skipped`, and **`Downgraded { .. }` ->
`PendingReadback`**. A downgrade means the provider did something weaker than
asked, so its own success report is precisely the claim that must not be
trusted; routing it into the read-back set makes the final accounting come
from observed state. It is deliberately NOT pushed onto `retry_ids`: a
downgrade is not transient, and resubmitting earns the same downgrade while
consuming the campaign's attempts. Gmail's trash-instead-of-destroy fallback
is the motivating case, and filing it `Applied` produced a permanent
destroy/reappear reconcile loop. The catch-all arm for this match sends any
FUTURE `MutationSuccess` variant to `PendingReadback` too, rather than
assuming success - folding an unknown variant into `Applied` is how the
downgrade went unnoticed originally.

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
  terminal classification. Forwarding is deduped per campaign by
  directive identity - the variant plus its target scope - not by the
  target alone: a 500-item batch whose items all name one directive
  costs one reopen request, while a mixed batch still delivers each
  distinct directive. Keying on the target alone would collapse every
  account-wide directive onto `None` (the first `RestartAccount`
  swallowing a later `OperatorOverrideRequired` or schema reset) and
  every same-folder directive onto that folder (a `RestartScope`
  swallowing a later `DisableScope`).
- terminal recovery -> `failed_terminal`.

`ItemOutcome::Uncertain` always queues for read-back so a
non-idempotent transport drop never replays blindly.

`SyncEngine::bulk_move` and `SyncEngine::bulk_move_from` use the move
arm, which submits through
`Account::bulk_move_from(targets, destination, source, key)` and
`bulk_move` is exactly `source: None`. `source` exists for request
count, not semantics: the folder-model providers vacate the source as
part of the move itself, so the trait's default impl forwards to
`bulk_move` and drops it, while Gmail folds it into the same
`batchModify` and thereby removes the O(n) `remove_from_container`
workaround. The read-back guard is unchanged either way - it
reconciles membership of `destination`, the property that says the
move landed, and does not separately re-verify absence from `source`.

`IdempotencyKey` is `{ run_id, sequence, protocol_salt }`. `run_id`
is consumer-minted and consumer-persisted across process restarts
so retries from a previous process correlate.

Before every wire submission, including read-back, the campaign waits
through `SyncControl::wait_until_running` and registers an activity
guard. A pause that wins the registration race parks the campaign
without changing its retry set, attempt count, outcome map, or
idempotency key. An in-flight attempt finishes under the activity guard,
so `pause().await` cannot report quiescence until its accounting is
settled; any retry then parks before resubmission. Detach cancellation
interrupts retry delays, throttle waits, mutation streams, and read-back
and returns `Error::ShuttingDown` instead of parking.

The retry candidate list is an unbounded `Vec<ObjectId>`. No engine
configuration field caps it - `MutationConfig::retry_queue_cap` is inert
and does not bound this vector - so consumers must bound campaign input
if retaining every unresolved id is too costly.

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

The engine broadcast is id-only. A live `Change` already carries
`{ id, kind }`; both `InventoryFusion` and `BackfillRunner` deliberately
down-convert each `InventoryEntry` to
`Change::ObjectChange { id, Created }`. Fingerprints, threading headers,
and inventory memberships do not reach consumers through
`MultiplexerEvent`. A consumer that turns those signals into real rows
must call `get_stream`, normally with `Projection::Metadata` first and
then a fuller projection as needed. This repeats metadata the protocol
may already have fetched during cold start, but keeps one event
vocabulary and is the explicit v1 design.
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
`contact_update`, `contact_delete`, `directory_search`,
`directory_groups_list`, `directory_group_expand`), the
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

**`Scheduler`, `ConcurrencyBudget`, `SchedulerConfig`, `MutationConfig`,
`LiveSupersedes`, `BackfillCheckpointWriter` and `mutation::fanout` stay. Do not
delete them.** All seven were removed in one commit on the reasoning that nothing
in this workspace wired them, and restored at the repository owner's instruction.
This is a library crate: its consumers are outside the workspace by definition,
so a workspace-wide grep establishes nothing about who uses a published item, and
"documented as deliberately unwired, with no dated plan" describes somebody's
plan rather than evidence of abandonment. Where one of these reads as unfinished,
the fix is to finish it - see `MutationConfig::retry_queue_cap` in
`notes/todo.md`, which bounds nothing today and should be made to bound the
queue rather than dropped. Removing or renaming any published item here is the
owner's call; the standing lessons in `AGENTS.md` carry the full account.

`Scheduler` is a strict-priority gate (not an executor) with four
lanes: `Foreground` / `Normal` / `Background` / `Bulk`. Starvation
guard forces a lower-lane pull after a configurable threshold of
consecutive higher-lane pulls (`SchedulerConfig::starvation_floor`,
default 64).

`LaneQueue` is bounded (default 1024 items, configurable via
`EngineConfig::lane_capacity`). Production construction always uses
`LaneShedPolicy::DropOldest`, which evicts the head, increments a shed
counter, and emits a `warn!`. `DropNewest` remains an internal policy
variant for direct queue construction and tests; `EngineConfig` does
not expose a selector.

`BudgetGate` exposes two semaphores per account (sync + mutation)
plus global caps. Acquisition takes the per-account permit first and
the global permit second, so a waiter parked on a busy account cannot
consume global capacity needed by another account. Lazy-creation uses
`DashMap::entry().or_insert_with(...)` to close the original
data race. `ConcurrencyBudget::validate` requires `per_account >= 2`
and a mutation share that leaves at least one real sync permit; the
gate no longer masks a zero split by granting an extra permit. Its public
constructor still floors the global semaphore at one permit, because direct
construction bypasses builder validation and must not create a permanently
blocked gate from `global: 0`.

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
`pause().await` and `checkpoint_now().await` return
`Result<Option<Checkpoint>, AccountError>`. The value is the latest
durable checkpoint, or `None` when an idle account has never produced
one. `SyncControl` tracks active stream operations plus every broadcast
checkpoint awaiting a consumer ack. A boundary waiter resolves only
when activity reaches zero and that pending set is empty. This gives an
idle account a completion source without claiming safety while a batch
is still in flight.

Every producer registers its checkpoint with `expect_checkpoint`
BEFORE broadcasting the batch and retracts it with
`retire_checkpoint` when the send reached only the slot's sentinel
receiver. Registering after the send would let a consumer that acks
in that window strand an entry no ack can match.

An entry leaves the pending set three ways, and every broadcast hits
one of them:

- `record_checkpoint`, fired by the ack writer after
  `put_change_cursor` / `put_backfill` succeeds for an `AckRequest`.
  Removes the entry by exact identity and refreshes the durable
  snapshot. Both change-cursor and backfill paths are
  consumer-ack-deferred.
- `retire_checkpoint`, fired when the ack was processed but produced
  nothing durable (store write failed) or when no real subscriber
  received the batch. The entry stops gating waiters - it is no
  longer in flight - but the durable snapshot is NOT advanced, and
  the consumer learns of a failed write from `ack_checkpoint`'s own
  `Result`. Leaving it pending instead would wedge every later
  `pause` / `checkpoint_now` on the account for the process lifetime.
- Supersession: a newer broadcast on the same lane + scope replaces
  the older one. The shared per-scope drive lease makes polling and
  push reconciliation one sequential producer, while `ScopeToken`
  generation matching prevents duplicate poll tasks. Acking the newest
  checkpoint therefore proves the earlier ones from that producer are
  durable too. This bounds the set by the account's scope count
  instead of by how many batches a consumer left unacked, and lets a
  consumer that acks coarsely (persist N batches, ack the last
  checkpoint) still reach a boundary. `record_checkpoint` removes by
  exact identity rather than by key, so acking an OLD checkpoint
  never retires a newer outstanding one.

The mutation pipeline records counters, not checkpoints; it does
not call `record_checkpoint`.

`bandwidth_observed` is fed by the optional bandwidth-feed task
spawned in `attach` when `SyncEngineBuilder::with_bandwidth_meter`
was set; the task polls `BandwidthMeter::account(id).observed_bps()`
once per second and calls `control.observe_bandwidth(bps)`.

`Control` itself (the trait re-exported from `bifrost-types`) is
dyn-safe; `SyncControl` is the engine's concrete implementation.
Boundary, priority, bandwidth-cap, and checkpoint watch senders use
`send_replace`, so their canonical snapshots update even when no
receiver is alive.

Boundary writes from `SyncControl` are compare-and-set, not blind
writes:

- `checkpoint_now` installs `CheckpointNow` and reads the request it
  displaced in one atomic step (`Boundary::request_checkpoint`), then
  restores that value only if the boundary still reads `CheckpointNow`
  (`Boundary::restore_if_current`). An interleaved pause, stop, or
  resume wins in both halves - neither the install nor the restore can
  clobber it. The restore runs from a drop guard, so it fires on every
  exit from the wait: the checkpoint landing, the watch channel closing
  under the waiter, and the caller dropping the future mid-await. A
  latch left installed is not cosmetic - `wait_until_running` reads
  `CheckpointNow` as not-running, so backfill, deferred inventory, and
  `restart_account` would park until some unrelated write moved the
  boundary.
- `Stop` is terminal for consumer-driven writes
  (`Boundary::set_unless_stopped`, used by `pause` / `resume`, and
  refused outright by `request_checkpoint`). A pause written over a
  detach's `Stop` parks the workers instead of letting them drain,
  and `detach` then waits out its whole timeout. `Boundary::set` is
  still the unconditional primitive the engine's own teardown uses.

## Inventory coverage

Inventory answers a question no other stream does: what did the enumeration
PROVE about the objects it walked past? The rule is

> Any accepted inventory progress checkpoint must certify that every provider
> result before that checkpoint was either materialized as an `InventoryEntry`
> or proved irrelevant to the inventory snapshot.

A checkpoint advances a cursor past everything behind it, and the changes
stream only reports SUBSEQUENT changes, so an object a walk skipped without
recording becomes permanently invisible to that account. Making the failure
visible to a consumer does not fix that; the checkpoint has to carry the
unresolved obligations with it, atomically, or not advance.

Inventory therefore has its own envelope (`InventoryEvent`, not `SyncEvent<T>`)
in which every checkpoint-bearing `InventoryBatch` and the terminal
`InventoryCompletion` carry an `InventoryCoverageReport`. Coverage rides
checkpoint-bearing BATCHES, not only the terminal completion, because Graph and
`BackfillRunner` both advance per page - waiting for `Done` would let a page
checkpoint become durable across a gap it never declared.

### Report and ledger are different things

`InventoryCoverageReport` (in `bifrost-types`) is one account's observation about
one enumeration of one region. `DebtLedger` (in `bifrost-sync`) is what the
engine has concluded after folding every ACCEPTED report, every operator
decision, and eventually every repair result together. Calling both "coverage"
hid the seam, and the seam is where the mistakes live: the account reports
evidence, the engine owns retry and acceptable-loss policy, and no protocol crate
constructs ledger state. `DebtLedger::ingest` is the single exhaustive path in.

### A report names its extent

A completeness claim is only meaningful about a stated region, so every report
carries a `CoverageDomain`: scope, a semantic `CoverageCoordinate`, and a
`SnapshotIdentity`. The coordinate is deliberately NOT the durable `Partition`
key - that key names an execution unit and indexes resume state, and says nothing
about what range of objects the unit covered. Repartitioning makes the difference
concrete: debt raised under a `30d..90d` window is covered by the union of
`7d..60d` and `60d..180d` and by neither alone, and an opaque key comparison sees
three unrelated strings.

`CoverageDomain::covers` answers conservatively, because a wrong `true`
discharges debt nothing re-read (silent loss with a proof record attached) while
a wrong `false` only leaves a scope degraded until something wider covers it. Two
rules with teeth: a UID range does not cross a UIDVALIDITY change, and PAGE
RANGES ARE INCOMPARABLE ACROSS SNAPSHOTS - page 500..1000 of a later walk may
hold entirely different objects, so repeating the coordinate proves nothing
unless both reports observed the same snapshot. Two walks that cannot name their
snapshot are never the same snapshot, which is why `SnapshotIdentity` comparison
is `same_snapshot_as` rather than `==`.

Discharge needs BOTH a generation not older than the debt's AND a covering
domain. Generation alone stops a stale report overwriting newer state; it proves
nothing on its own, because a newer partial walk is still partial.

### Proof and policy are separate axes

`ProofStatus` is what the system knows (`Unresolved` / `Discharged`).
`PolicyStatus` is what it will do (`Retrying` / `OperatorBlocked` / `Waived`).

Collapsing them is the mistake the split exists to prevent. A waiver is accepted
loss, NOT proof that anything was materialized or proved irrelevant, so a waived
obligation stays `Unresolved` forever - what changes is that it stops blocking
automatic completion. Otherwise no audit, and no later walk, can tell proved
coverage from loss somebody agreed to live with.

**No local counter may produce `Waived` or `Discharged`.** A retry budget
expiring is evidence that automatic work is not helping, which is
`OperatorBlocked`: still visible, still blocking, still manually retryable. Only
an operator waives, through `SyncEngine::waive_obligation`, because only an
operator can decide what loss is acceptable. Waiver targets ONE occurrence by
`ObligationKey`; waiving a failure CLASS would authorize every future instance
sight unseen.

### `Object`, `Region`, and the barrier

`InventoryObligation` splits `Object` from `Region`. An id-less provider result
must not become an `Object` with a synthetic id: an absent id may mean one
malformed object, a schema mismatch affecting many, a truncated page, or a
response that cannot be correlated with pagination at all, so naming it a
single-object loss claims knowledge the walk does not have.

A `Region` declares its `RegionRecovery`: `DurableReplay { token }` or
`CheckpointBarrier`. Two variants, not three. A finite-horizon token - "this page
link works for another thirty minutes" - is not durable coverage, because the
engine cannot guarantee repair before a deadline across crashes, offline periods
or operator blocking; at the moment the checkpoint is considered it has exactly
the same safe disposition as no token at all. Converting a perishable
continuation into a durable artifact is the ACCOUNT's job, done before it hands
the obligation over. `CheckpointBarrier::transient_replay` is diagnostics only,
deliberately a field rather than a variant so no scheduler can read "not expired
yet" as permission to advance.

**A barrier taints the walk.** Refusing one checkpoint is not enough - the next
page's checkpoint or the terminal delta link would leap the same region. So the
batch's items are still delivered, its checkpoint is stripped, and the walk
stops. Checkpoints accepted EARLIER in that walk stand: each certifies a prefix
ending before the barrier region begins. Barriers do not become ledger debt (no
cursor advanced past them, so there is nothing durable to hang debt off); they
are recorded as `BarrierIncident`, which is what gives a restart its memory and
an operator something to waive.

`bifrost-graph` reports its id-less-value case as `CheckpointBarrier`. The only
token at page granularity is a continuation of that delta session, dead as soon
as the walk takes its `deltaLink`, so recording it as repairable would create
debt nothing could ever discharge - silent permanent loss wearing a declared-debt
costume.

**Elsewhere, the scope converges DEGRADED rather than never converging.** A walk
that hits an unreadable but re-readable object records it and keeps going, so the
cursor establishes and the account enters its change stream. The alternative
considered and rejected was refusing every checkpoint after the first failure,
which is safe but never converges: one permanently-broken object would cost the
mailbox its entire backfill, forever. Declared loss beats permanent
non-convergence; SILENT loss beats neither. For a barrier there is no third
option that preserves both progress and coverage, which is exactly why the
operator waiver is load-bearing rather than a nicety.

### Publication identity

Coverage reaches the durable record through `PendingCoverage`, keyed by an
engine-issued `PublicationId`. It cannot ride `Checkpoint`, which is a published
type crossing the broadcast channel and back through `ack_checkpoint`.

Keying by scope is unsound: two backfill partitions of one scope are in flight
together, and the second's report overwrites the first's. Keying by `Checkpoint`
VALUE is also unsound, because `Checkpoint: Eq` is value equality and never
promised to identify a publication:

- `BackfillRunner` counts only entries a page MATERIALIZED, so a page whose
  content was entirely unrepresentable increments nothing, and with no progress
  marker the next `BackfillCheckpoint` is byte-identical to its predecessor while
  describing a different boundary and a different report;
- inventory fusion publishes its final checkpoint twice, on the final batch and
  again on `Done`;
- a later walk can produce the same cursor bytes as an earlier one while proving
  different coverage.

`MultiplexerEvent::publication` therefore carries the identity to the consumer,
and `ack_checkpoint` takes it back. A repeated acknowledgement of the same
publication is idempotent; an UNKNOWN one is refused. It is never defaulted to
complete coverage - that is the original lying-record bug in another costume.

Supersession FOLDS (`PendingCoverage::supersede`). When a newer publication
supersedes an older outstanding one for acknowledgement purposes, the survivor
absorbs the superseded claim; dropping it would stop the control path waiting for
the older checkpoint while quietly discarding its obligations. Cumulative reports
within one walk make this harmless duplication; across partitions it is the only
thing keeping partition A's debt alive when B supersedes it.

### One atomic transition

`CheckpointStore::apply_transition` is the only mutating operation, taking the
checkpoint and the resulting ledger together. They cannot be two calls: checkpoint
first loses the debt, ledger first records debt for progress that never
committed. `put_change_cursor` / `put_backfill` remain as conveniences for writes
that carry NO coverage report, and they read the ledger and write it back
unchanged - "this write says nothing about coverage" means the debt survives, and
emphatically not that coverage is complete. An earlier revision had a convenience
of exactly that shape stamping `Complete` on every write, which is how a durable
record ends up certifying coverage nothing proved.

Two mutations have no cursor to ride and use `put_ledger`: a barrier incident
(nothing advanced, by definition) and an operator decision.

Claims are applied when the CONSUMER acknowledges, never when the account emits.
An unacknowledged report may describe entries the consumer never persisted, so it
cannot prove anything. The one exception is `ingest_debt_only`, used for a
backfill partition's terminal summary, which has no checkpoint of its own: it
records obligations but never proof, because over-reporting debt costs a degraded
scope while under-reporting it costs objects nobody sees again.

### Sentinel eligibility is decided by the writer

`BackfillRunner` reports `complete` per partition, but that flag is NOT
authoritative for the durable sentinel. Debt from a sibling partition can be
acknowledged after the runner decides and before the sentinel reaches the writer,
and the sentinel makes the next attach skip the walk entirely - so writing it
over an open obligation converts a declared gap into a permanent one. The writer
therefore evaluates `DebtLedger::completion_permitted` against the ledger AS IT
STANDS at the moment it processes the sentinel acknowledgement, and withholds the
marker if the scope owes anything unwaived. The consumer's acknowledgement still
succeeds; the marker simply does not become durable, so the next attach re-walks.

Note `seen` counts only entries a page materialized, so an object that never
became an entry is not in that total - the count cannot be used to detect any of
this.

Degraded coverage is surfaced to the consumer as a
`Warning::OperatorAttentionNeeded`, and open debt is enumerable through
`SyncEngine::debt`. Durable debt that nothing reports, or that nothing can list,
is only half a fix.

### Repair

`SyncEngine::repair_debt` runs one pass: it reads the ledger back, asks the
account to re-read what it could not represent, publishes what came back, and
discharges only what the consumer acknowledged. Caller-driven rather than
scheduled, because repair is remote work against an account that may be
throttled or degraded and the consumer knows better than the engine when to
spend that budget.

`Account::repair_inventory` takes a stream of `InventoryRepairRequest` and
yields correlated outcomes. Not `get_stream`, which hydrates known ids at a
projection and cannot express region replay, completeness proof, or
authoritative absence. The default implementation refuses PER REQUEST rather
than terminating the stream, so every attempt gets its answer and unsupported
repair classifies straight to `OperatorBlocked` instead of burning a transient
budget on a capability that will never exist.

Correlation is by engine-issued `RepairAttemptId`, not by `ObligationKey`. The
key identifies durable debt, not one execution: an attempt may be retried after
a stream terminates, a stale event may arrive from an abandoned stream, and the
same key may be reopened at a newer generation - so key-only correlation risks
applying an old result to a newly reopened instance. Exactly one terminal
outcome per accepted request; a duplicate, an unknown attempt, or an outcome
whose kind does not match its request is dropped. `Terminated` explains why a
stream stopped and does NOT answer for outstanding requests: those become LOCAL
deferrals, because recording a conclusion nobody reached is the same invented
certainty the coverage model exists to prevent.

Ledger entries retain the account-minted `InventoryRepairTarget`. Without it the
executor cannot reconstruct a request at all - key, domain and error are engine
or diagnostic facts, and the error in particular must never serve as a repair
descriptor because classifications change between revisions.

**Repair publishes ids, never object state, and that is what removes the race it
looks like it should have.** No inventory path delivers an `InventoryEntry` to a
consumer: `InventoryFusion` and `BackfillRunner` both keep the id, discard the
entry, and publish `ObjectChange::Created`. Repair mirrors that exactly. A
repair publication therefore carries no payload that could overwrite a newer
representation and nothing to insert after a tombstone; a stale repair `Created`
is resolved the way a stale backfill `Created` already is, by hydration
returning not-found. So there is no conditional application, no tombstone
requirement, no version-relation hook, and no lease held against polling. This
matters because `ServerVersion` could not have arbitrated it anyway - `ETag` and
`StateAt` are equality tokens, `ModSeq` orders only within a UIDVALIDITY epoch,
and the type derives `Eq` and not `Ord` for exactly that reason.

The entry still crosses the ACCOUNT boundary on `ObjectRecovered`, because
building it is the proof that the representation failure which raised the
obligation has healed; an id alone would only prove the object still exists. The
engine validates it (one entry, matching id, right request kind), keeps the id,
and drops the rest.

Discharge happens on the consumer's acknowledgement of the repair publication,
and both halves are recorded as `DischargeEvidence::RepairedAndPublished`.
Neither alone suffices: an account recovery nobody was told about leaves the
consumer unaware, and a published id with no successful account result merely
repeats an id. An unacknowledged recovery costs an attempt and stays owed. The
bar is deliberately no higher than the successful path's: a durably acknowledged
`Created` is the most any walk ever achieves for any object, so demanding proof
of successful hydration would fuse two failure domains - coverage asks whether
enumeration announced the object, hydration asks whether a projection can be
fetched now, and a later hydration failure belongs to the hydration lane and its
`ItemOutcome`.

`DefinitivelyIrrelevant` discharges and publishes NOTHING: absence from an old
inventory snapshot is not a deletion to apply against current state. It is a
typed account conclusion, never a status code - a 404 may mean deleted, moved
out of scope, permission lost, wrongly routed, or not yet replicated. Google's
variant is `AbsentUnderCursorBridge`, and it rests on the Gmail cursor being
anchored at the historyId sampled BEFORE the walk that raised the obligation, so
any deletion since is carried by the change stream. Under a different cursor
model the same `NotFound` would not be dischargeable.

Region discharge needs proof, never just entries. `RegionRepairProof` is
`ExactReplay` (identity-checked: the account consumed the exact region its own
token named), `CoveredBy` (checked with `CoverageDomain::covers`), or
`Partitioned` (the account asserts the split where the extent is opaque and the
lattice cannot do set algebra over it).

Retry budgets accrue at the LINEAGE ROOT, counting completed outcomes only. A
crash after provider work but before acknowledgement must cost a repeated
attempt, not a consumed budget. Per-key budgets would be evaded by re-minting an
equivalent obligation each pass, so `replace_obligation` is an atomic
parent-to-children swap - appending would double-count the extent and replay the
parent forever - and children inherit the root. A child already open under a
different root is refused outright rather than silently given two lineages.
Progress is judged across extent, granularity, proof gained and repair
authority, not extent equality: turning one opaque region into three addressable
objects is progress at identical extent, while repartitioning into equally
opaque children is not, and only the multi-dimensional test gets both right.

**Still not built:** region repair has no provider implementing it (Graph's
region is a barrier by construction), so `ExactReplay`, `Partitioned` and
lineage splitting are exercised by tests rather than by a live provider. Ledger
compaction is also outstanding - see `notes/todo.md`.

## Cursor envelope

`OpaqueChangeState` carries `protocol`, `envelope_version`, and
`bytes`. `decode_envelope` returns:

- `Error::SchemaIncompatible` for a header version outside
  `[MIN_MIGRATABLE, ENGINE_VERSION]`, in either direction.
- Otherwise decoded `Checkpoint::Change` / `Checkpoint::Backfill`.

Both out-of-range directions are the same classification on purpose:
both describe a durable row this revision cannot read, and
`SchemaIncompatible` is the only signal the two healing paths key on.
An over-version row classified as `Error::Other` instead would
propagate as an ordinary establish failure - three burnt reopen
attempts, a broadcast `Terminated`, and the undecodable row still on
disk for the next attach to trip over identically.

Encoding an `ObjectType` or `ProtocolKind` variant the codec does not
know panics, the same rule `encode_scope` already applied to unknown
`CursorScope` variants. Writing a placeholder tag instead would
persist a durable row nothing can decode, and the account would then
re-establish, re-encode the same unreadable value, and re-poison the
row on every attach. Decoding the reserved tag `0xFF` (written by an
engine revision that did exactly that) yields
`Error::SchemaIncompatible` so those rows still heal.

Every encoded length is checked against `u32::MAX`. An impossible
larger in-memory field panics at the encoding boundary instead of
writing a false truncated prefix and silently corrupting the envelope.

An unreadable row is healed where it is found. `run_establish` - the
running-account path - returns a typed `AccountError` whose derived
`RecoveryClass` is `Engine(SchemaIncompatible)`, and the reopen
listener runs the account-wide schema-clear loop. `establish_one` -
the attach path - deletes the row and re-establishes that one scope
inline, because the reopen listener is spawned later in `attach_inner`
and does not exist yet: propagating the error there would fail the
attach with nothing left running to clear the row, so every later
attach would fail identically.

(These are engine `Error` enum variants, not `RecoveryClass`
variants.)

`ChangeCursor` carries its own `envelope_version` separately from
`OpaqueChangeState.envelope_version` (outer vs inner versioning). The outer
version is owned by `bifrost-types` through
`CHANGE_CURSOR_ENVELOPE_VERSION`; every producer stamps that shared constant,
the persisted change envelope carries it, and the engine calls
`ChangeCursor::validate_envelope` before protocol dispatch. The inner version
remains protocol-owned and rides inside the payload, where a bump invalidates
only that protocol's bytes.

`validate_envelope` is a STRICT EQUALITY gate, and the decoder is the single
migration boundary that makes that safe. `decode_envelope` accepts the whole
`[MIN_MIGRATABLE, ENGINE_VERSION]` window, but a `Checkpoint::Change` it
returns has already been run through `migrate_change_cursor` and stamped
current - so every cursor in the engine's hands is at the current layout, and
the codec is the only code in the workspace that knows a historical one.

The alternative - widening `validate_envelope` to accept the migration window -
was considered and rejected. It spreads knowledge of every past layout across
every consumer, and it hands a protocol crate a cursor in a shape that crate
has no code to interpret. Leaving the window and the gate disagreeing was the
live defect: a version-1 row would decode successfully and then fail
validation, so the engine would classify a migratable durable checkpoint as
`SchemaIncompatible` and discard it - a full resync per account, arriving the
moment `ENGINE_VERSION` is first bumped.

Pinned by
`tests/envelope_roundtrip.rs::every_accepted_envelope_version_decodes_into_a_cursor_that_validates`,
which loops the window constants rather than a literal so it widens on its own
at the next bump, and by
`an_unknown_change_cursor_outer_version_is_detectable`. Bumping
`ENGINE_VERSION` means adding the per-version fixup in `migrate_change_cursor`
AND a real byte fixture for the outgoing layout; the window loop patches the
header over current-layout bytes and so cannot prove a fixup reads an old
payload correctly.

Validation is applied at every seam where an account-authored cursor enters the
engine - the attach path, `run_establish`, `persist_ack_request`,
`drive_changes_stream`, the inventory fusion checkpoints, and
`fuse_inventory_done` - and a mismatch maps to classified schema recovery
(`Error::SchemaIncompatible` or `recovery::cursor_decode_failure`), never to a
silent drop. `CursorRegistry::put` carries only a `debug_assert!`: by then the
condition is unreachable, and aborting on a value a third-party `Account` impl
authored would trade a recoverable resync for an outage. `encode_envelope` does
panic on an unsupported outer version, consistent with the rest of that codec,
which refuses to write any row its own decoder would reject.

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
    fn delete_backfill<'a>(&'a self, account: &'a AccountId, scope: &'a CursorScope)
        -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;
}
```

`put_*` take owned `ChangeCursor` / `BackfillCheckpoint`; `get_*`
take a borrowed `&CursorScope`. `delete_change_cursor` is required
because `RecoveryClass::Engine(EngineDirective::RestartScope)` must
drop the durable cursor so the next establish re-runs via inventory;
a no-op delete would silently preserve the stale cursor. It is also
what attach calls to clear an undecodable envelope, so a store that
no-ops here strands the account on a row it cannot read.
`delete_backfill` drops every backfill row for the scope, completion
marker included; only `SchemaIncompatible` recovery calls it (the
marker is what would make the next attach skip the inventory re-walk
that re-mints ids under the new encoding), and routine `RestartScope`
deliberately does not - re-walking a completed backfill after every
cursor invalidation would re-hydrate the scope for no schema reason.

A store that decodes durable bytes must surface a schema it cannot
read as `Error::SchemaIncompatible` from `get_change_cursor`; that is
the signal both establish paths key on. Any other error is treated as
a store failure and propagates.

`InMemoryCheckpointStore` is the test backend (HashMap-backed). No
sled / sqlite default; storage is consumer-owned.

`get_backfill` selects the greatest `items_done`. Equal-progress rows
use the greatest parsed page upper bound, then lexicographically
greatest opaque partition bytes, so equal-sized page windows resume
deterministically from the furthest one. Page checkpoint
`items_done` counts entries observed before `LiveSupersedes` filtering;
filtering therefore cannot make a full inventory window look short and
falsely signal exhaustion.

`Partition` is `Hash + Eq` (additive change to `bifrost-types`)
because the in-memory store keys on it.

## Read-back guard

Applies to all four protocols today (every protocol declares
`MutationReplaySafety::None`; the engine reads back after retry to
disambiguate ambiguous transport failures). The guard re-fetches
affected ids via `Account::get_stream(ids, Projection::FlagsOnly)`
and reconciles against the intended mutation. Destroy read-back treats
only an explicit `AccountErrorKind::NotFound` as proof that the object
is absent; any other per-item fetch failure remains uncertain.

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

The reopen listener never sleeps for a bare `RecoveryPlan::Retry`.
The originating poll, push, or mutation path owns an actionable retry;
the listener only records shared throttle deadlines and stays
available for engine directives. Sleeping there would serialize
restart and schema recovery behind a delay after which no operation was
actually retried.

Reopens use exponential backoff with ±20% jitter (1s initial, 5min
cap) and a three-attempt budget. Per-scope re-establishment
dispatches each failure through `plan_recovery` rather than
blind-retrying every class: a `DisableScope` establish failure
quarantines the scope immediately (same rule attach applies), and a
terminal class skips the remaining budget straight into the
exhaustion tail - another attempt cannot repair it. After the budget
is spent (or short-circuited terminal) the engine
broadcasts `SyncEvent::Terminated(last_error)` for the affected
scope (per-scope re-establishment) or for the account
(`RestartAccount`), then publishes
`AccountControl::Pause(PauseReason::RetryBudgetExhausted)` and
flips the boundary to `Pause` so workers park. Consumers subscribe
to the per-account `AccountControl` broadcast via
`SyncEngine::account_control_stream` and flip back via
`SyncEngine::resume_account`. The account-wide shape is pinned
end-to-end by
`tests/attach_schema_recovery.rs::three_failed_reopens_terminate_and_pause_the_account`,
which runs under paused time so the recorded per-attempt open instants
expose the 1s / 2s backoff schedule exactly.

`EngineDirective::OperatorOverrideRequired { reason }` auto-pauses
the account with `PauseReason::OperatorOverrideRequired` and emits
a `Warning::OperatorAttentionNeeded` carrying the protocol-supplied
reason. The reason rides on the warning's free-form fields; the
`PauseReason` enum is bounded.

`crate::recovery::ThrottleBucket` is engine-wide (one per
`SyncEngine`, shared by every slot): deadlines keyed by
`ThrottleKey::{Mailbox, Account, Tenant, Provider}` plus a
membership index recording which cross-account (`Tenant` /
`Provider`) keys each account belongs to. An account joins a shared
key the first time its own error stream names that identity;
membership survives `cleanup_expired` because it is an identity
fact, not a deadline, and is bounded by the identity space.
`ThrottleScope::CurrentOperation` is a per-call hint and never
enters the bucket.

Both sides are wired. Recording: the reopen listener, the poll loop's
and push reconciler's Retry and Reconcile arms (both the per-drive
terminations and the reconciler's top-level `WatchEvent::Terminated`),
and the mutation campaigns (per-item outcomes and stream-level
terminations) funnel through `recovery::record_throttle` or
`recovery::record_reconcile_throttle`, and the local sleep those arms
observe comes from `recovery::retry_delay` /
`recovery::reconcile_delay`, which honor the carried `RetryHint` and
otherwise fall back to one second. Both recorders resolve the key from the
identities the classified error actually carries
(`ErrorScope::Mailbox`, `AccountError::provider()`) and degrades
toward the `Account` key rather than dropping the deadline - the
throttle applies to at least this account, so recording the subset
beats an unrecorded truth. `Tenant` ALWAYS degrades today: the
error contract carries no tenant identity string, so cross-account
tenant pausing is blocked on that types-level channel. Reading: the poll loop (before each drive), the
reconciler (before each hinted scope), and the mutation campaigns
(before each attempt) call `recovery::account_throttle_wait` - the
longest pending wait across the account's own key and its shared
memberships, re-checked after waking since a longer deadline can
land mid-sleep. The backfill partition runner and the
deferred-inventory worker consult it too, at the same boundary as
their pause checks: cold-start hydration is the heaviest request
lane the engine drives, so it must not barrel through a Retry-After
that paused the polls. `detach` forgets the account's
memberships so a reattached id cannot inherit a previous life's
provider enrollment.

Two documented limits on the cross-account reach. Enrollment is
lazy - an account joins a shared key only when its own error stream
names the identity - so the very FIRST provider-wide deadline is
invisible to a sibling that has never failed; attach-time
enrollment needs a provider identity channel the contract does not
carry. `Mailbox` keys are recorded but deliberately excluded from
the account-wide wait: a per-mailbox throttle must not pause the
whole account, and the engine has no scope-to-mailbox mapping to
pause anything narrower with. Both remain open.

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
                          // open_skipped_scopes (open-time skip lane),
                          // live_account + hydration passthrough
  control.rs              // SyncControl + record_checkpoint hook
  error.rs                // engine Error wrapping AccountError + Warning
  types.rs                // EngineConfig, MultiplexerConfig,
                          // BackfillConfig, MutationConfig, PushConfig,
                          // SchedulerConfig, AccountSlot, WorkerTask
  recovery.rs             // plan_recovery + RecoveryPlan dispatch;
                          // ThrottleBucket (engine-wide) +
                          // resolve_throttle_key / record_throttle /
                          // account_throttle_wait;
                          // retry_delay; restart_scope_error;
                          // cursor_decode_failure translator
  multiplexer/
    mod.rs                // Multiplexer::run; ReopenRequest;
                          // MultiplexerEvent { scope, event, checkpoint };
                          // lifecycle_reopen wiring
    changes.rs            // drive_changes_stream + ChangesEvent +
                          // AckRequest (broadcast-then-consumer-ack)
    fusion.rs             // InventoryFusion::run_with_broadcast
    poll.rs               // adaptive cadence helper
  backfill/
    mod.rs                // account-keyed BackfillRegistry,
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
    mod.rs                // MutationCounters,
                          // fanout::partition_by_account
    idempotency.rs        // IdempotencyKey vending
    readback.rs           // Projection::FlagsOnly read-back guard
  cursor/
    mod.rs                // CursorRegistry + membership index
    envelope.rs           // MIN_MIGRATABLE / ENGINE_VERSION + migrations
    store.rs              // CheckpointStore trait (6 methods) +
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
