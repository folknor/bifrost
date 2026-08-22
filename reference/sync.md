# bifrost-sync reference

Current architecture of the sync engine.

Scope: multiplexer, backfill orchestrator, push
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

- `config(EngineConfig)` - replaces the whole config.
- `checkpoints(Arc<DynCheckpointStore>)` - consumer-provided
  store; defaults to `InMemoryCheckpointStore` when omitted.
- `with_bandwidth_meter(Arc<bifrost_net::BandwidthMeter>)` -
  wires the meter; when unset, `Control::bandwidth_observed`
  returns 0 for every account.
- `build()` - constructs and returns `SyncEngine`.

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

A public or engine-initiated reopen queues behind `Pause` and runs after
resume: pause is a quiescence boundary, not permission to open a
replacement connection in the background. The activity registration that
makes the account non-quiescent is taken BEFORE `factory.open()`, not
after it, so `pause` cannot report quiescence while a replacement
connection is being established; a pause that wins the race against the
registration means nothing was opened at all, and the caller loops back
to the boundary wait without spending a retry attempt.

## Stream contract: broadcast + consumer ack

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
at attach and `run_establish` on reopen - ask the account's
`is_inventory_cursor` predicate whether the saved state is a resumable
inventory position before putting it into the registry. A stored page position
must never reach `changes_stream`, which has no delta link to walk. Only after
classification does `inventory_resume_stream` build the stream. The predicate
and the hook must accept exactly the same cursors, and an account implementing
one implements both from a single condition: a cursor the predicate accepts but
the hook refuses reaches the deferred inventory worker, which gets `None`,
reports `NoCursor`, and leaves the scope with neither a live cursor nor a
recovery path - whereas that same cursor reaching `changes_stream` would be
classified schema-incompatible and restarted. The terminal
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
chunking). `Full` falls through as a single partition; JMAP
Email currently advertises open-ended `PageCount`.

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

- **`OpenPages`** (JMAP Email): the orchestrator runs the persisted
  checkpoint through the pure `open_pages_resume` decision - completion
  sentinel -> **skip entirely**; any other `page:F:T` -> resume the walk
  at `T` rather than page 0; no checkpoint or an unrecognised partition
  kind -> start fresh at 0. A SHORT page (`items_done < T - F`) is
  deliberately not read as exhaustion: a partition may emit fewer entries
  than its window width while the scope still has results (ids deleted
  between `Email/query` and `Email/get`, id-less objects dropped), so
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

Backfill forwards every inventory entry. It does not suppress an entry
merely because a live `Created` was broadcast: broadcast is not proof
that a consumer received the event, and consumers already have to
tolerate repeated `Created` events when a crash re-walks a partition.

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
coalesced into one
`HintPayload::Unknown` send with a 100ms bound. `Terminated` and
`Warning` are lossless control information: the captured engine
runtime waits for queue space and sends the original event without
demoting its classification or incrementing the drop counter. Capturing
the runtime during `register` also lets receiver threads outside Tokio
use the sink. If no runtime handle was ever captured (`register` itself
ran outside Tokio), nothing can wait for queue space, so a full queue
discards the event regardless of classification - and that discard is
counted, because an uncounted lossless discard would hide the loss of
control information from the one signal built to expose it. `Closed`
(account detached mid-push) is silently ignored.

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
   (`RetryHint::min_delay(now)` or `RetryHint::not_before(now)`) and
   resubmit with the **same**
   `IdempotencyKey`
   (engine bookkeeping; no protocol today emits it on the wire).
   Repeat up to `EngineConfig::mutation_max_retries`.
4. Run `run_readback_guard` once at the end against unresolved
   failures: `get_stream(Projection::FlagsOnly)` re-fetches and
   reconciles applied / skipped / failed_terminal. The read-back set
   is derived from the per-id outcome map when the retry loop exits
   (every id still `PendingRetry` or `PendingReadback`), not
   accumulated during it, so an id parked in the read-back lane cannot
   be dropped by a later attempt that resubmits its siblings.

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

The retry candidate list is an unbounded `Vec<ObjectId>`. There is no
engine configuration field that caps it; consumers must bound campaign
input if retaining every unresolved id is too costly.

`PushConfig` is currently empty (reserved for future push-only knobs; the
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

Consumer-driven hydration and engine-driven backfill share the same
underlying client, where
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
`OpaqueChangeState.envelope_version` (outer vs inner versioning). The
outer one is engine-owned and `pack_envelope` stamps `ENGINE_VERSION`
into the header rather than reading the field off the checkpoint. The
field is authored by whichever protocol crate minted the cursor, and
IMAP, CalDAV, CardDAV, and Gmail all fill it from the same constant
that versions their own opaque payload; trusting it means the first
protocol-side bump - which is exactly what that constant is for -
stamps a header the engine then refuses to read, turning a payload
format change into permanently unreadable rows for every account on
that protocol. JMAP and Graph keep the two apart explicitly
(`OUTER_CURSOR_ENVELOPE_VERSION` vs `PAYLOAD_ENVELOPE_VERSION`). The
inner version rides inside the payload, where a bump invalidates only
that protocol's own bytes. Pinned by
`tests/envelope_roundtrip.rs::a_protocol_authored_outer_version_does_not_reach_the_header`.

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
deterministically from the furthest one. Page checkpoint `items_done`
counts inventory entries observed.

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

Both sides are wired. Recording: the reopen listener
(`apply_throttle`), the poll loop's and push reconciler's Retry
arms (both the per-drive terminations and the reconciler's
top-level retryable `WatchEvent::Terminated`), and the mutation
campaigns (per-item Retry outcomes in `classify_item_outcome` and
stream-level Retry terminations) all funnel through
`recovery::record_throttle`, which resolves the key from the
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
                          // BackfillConfig, PushConfig, AccountSlot,
                          // WorkerTask
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
    runner.rs             // BackfillRunner::run_partition
    partitioner.rs        // plan() for TimeWindowed / UidRange / PageCount
  push/
    mod.rs                // InvalidationSinkInner
                          // (DashMap<AccountId, mpsc>) + coalesced_event
    reconciler.rs         // hint -> scope -> changes_stream;
                          // warning_event on Disconnected/Reconnected
    subscription.rs       // SubscriptionRegistry (per-engine DashMap)
  mutation/
    mod.rs                // MutationCounters
    idempotency.rs        // IdempotencyKey vending
    readback.rs           // Projection::FlagsOnly read-back guard
  cursor/
    mod.rs                // CursorRegistry + membership index
    envelope.rs           // MIN_MIGRATABLE / ENGINE_VERSION + migrations
    store.rs              // CheckpointStore trait (6 methods) +
                          // InMemoryCheckpointStore
  cancel/
    mod.rs boundary.rs    // safe-boundary mechanism

crates/sync/tests/
  cross_crate_conformance.rs  // cross-crate trait/contract checks
  envelope_roundtrip.rs       // cursor envelope encode/decode tests
  partition_planner.rs        // backfill partitioner unit tests
  readback_guard.rs           // mutation readback reconciliation
```
