# bifrost-sync bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/sync/` (engine, control, cancel,
multiplexer, backfill, push, mutation, cursor, recovery, scheduler, types). Findings are
unverified work material.

2026-08-07: four findings verified against the code and fixed, each with a regression test -
the cursor envelope outer version, the detach/re-attach teardown race, the scope-token cleanup
identity bug, and the latched `CheckpointNow`. Their sections are removed below; the behaviour
now lives in `reference/sync.md`. Everything still listed is unverified.

## Two producers drive the same scope concurrently, falsifying the checkpoint supersession invariant

`crates/sync/src/multiplexer/mod.rs::spawn_scope_poll_inner` and
`crates/sync/src/push/reconciler.rs::reconcile` both call
`drive_changes_stream(account, scope, cursor, ...)` for the same `CursorScope`, on separate
tasks, with no lock, token, or per-scope lease between them. A push invalidation arriving while
that scope's poll task is mid-drive gives you two `changes_stream(cursor)` runs against the same
cursor at once.

Consequences, in increasing order of seriousness:

- Duplicate broadcast of the same batch (tolerable, consumers must absorb repeats).
- Racing `cursors.put()`; the slower drive installs the older `server_state`, so the in-memory
  cursor regresses.
- The real one: `control.rs::expect_checkpoint` supersedes any pending entry with the same
  `pending_key` (lane + scope), and both `reference/sync.md` ("Broadcasts within a lane+scope
  come from a single sequential task, so acking the newest proves the earlier ones are durable
  too") and the doc comment on `expect_checkpoint` justify that with an invariant the code does
  not enforce. With two producers, producer B's `expect_checkpoint` evicts producer A's still
  outstanding entry, and `pause()` / `checkpoint_now()` then return a "safe boundary" while A's
  batch is genuinely unacked. That is exactly the failure the pending set exists to prevent.

Either serialize per scope (a per-scope async mutex in `scope_tokens`, so push reconcile and poll
take turns), or make the pending set producer-keyed. Serializing is also what makes the
cursor-clobber go away.

## reopen_lock held across an unbounded pause wait, so unsubscribe_push can hang forever

`handle_account_error`'s `RecoveryPlan::Engine` arm takes `ctx.reopen_lock`, then
`restart_account` loops on `ctx.control.wait_until_running(ctx.shutdown)`, which blocks until
resume or shutdown. `SyncEngine::subscribe_push`, `unsubscribe_push`, and the public `reopen` all
take the same lock with no timeout.

Scenario: an `OperatorOverrideRequired` directive pauses the account (`engine_pause`), a later
`RestartAccount` directive arrives, `restart_account` parks holding the lock. A consumer calling
`unsubscribe_push` to clean up server-side subscriptions before shutting down now hangs
indefinitely with no error and no timeout, and `unsubscribe_push`-before-`detach` is exactly what
the push contract tells consumers to do. The lock should be released while parked on the
boundary, or the wait should be inside a bounded/cancelable region.

## Backfill orchestrator snapshots scopes once, and races the fusion worker it claims not to

`engine.rs::run_backfill_orchestrator` calls `cursors.all_scopes()` exactly once, after
`wait_for_real_subscriber`, and never rescans. Two problems:

- Any scope established later (deferred inventory, `ScopeLifecycle::Created`, `RestartScope`,
  `SchemaIncompatible` re-establishment) never gets a backfill pass at all for the life of the
  attach. Its cold-start hydration simply does not happen until the next attach.
- The comment says "Snapshot only Ready scopes. `EstablishViaInventory` scopes are owned by the
  concurrent fusion worker... Adding them to this already-running plan after fusion would
  double-walk and double-publish." Nothing enforces that. Both workers park on the same
  `subscriber_notify`; if the fusion worker wins the wake and completes a scope's inventory before
  the orchestrator's `all_scopes()` executes, the fused scope is in the snapshot and gets walked
  and published a second time. The claimed exclusion is a scheduling accident, not an invariant.
  Track the deferred set explicitly and subtract it.

## Pause is only observed at a checkpoint boundary; a checkpoint-free stream never parks

In `multiplexer/changes.rs::drive_changes_stream`, the `boundary.peek()` at the top of the loop
has an empty arm for `Pause` (and `CheckpointNow`); the only Pause handling is inside
`if checkpoint.is_some()`. A protocol whose `changes_stream` emits `Batch`es with
`checkpoint: None` (permitted by the trait) is never parkable: `pause()` waits on activity
reaching zero, the activity guard is held for the whole drive, and `detach` burns its entire 5s
timeout before aborting. `reference/sync.md`'s "worker reacts at the next safe boundary" is only
true for checkpoint-bearing streams; that qualifier is not stated anywhere and is not enforced on
the `Account` contract.

## CursorRegistry::replace_from is not atomic, and reattach can roll cursors back

`cursor/mod.rs::replace_from` takes two write locks in sequence while the doc comment claims
"workers never observe a half-refreshed membership index." A reader between the two writes sees
new cursors against the old membership index, the exact state the comment promises cannot happen.

Separately, `reattach_account` refreshes `staged` from the live registry "at the last possible
moment", but that refresh happens before the push-subscription teardown loop, which does one
network `push_unsubscribe` per record. Any cursor a still-running old-handle poll advances during
that window is discarded by `replace_from`. It is at-least-once safe (durable cursor only moves on
ack), but it silently re-delivers a window of changes and can hand a protocol a `server_state`
older than the one it just issued.

## Broadcast lag is unhandled, silent, in-session data loss

`changes_capacity` defaults to 256 and every producer uses `tokio::broadcast::send` with the
return value used only as a delivery-count heuristic. A consumer that falls 256 batches behind
gets `RecvError::Lagged`; this crate never detects it, never replays, and the in-memory cursor has
already advanced past those batches, so within the session those changes are gone. Recovery only
happens on process restart from the last acked cursor. The `LiveSupersedes` docs describe this
behaviour accurately as a reason not to do something else, but nowhere is it treated as the
standing data-loss hole it is. At minimum the engine should surface lag (it owns the sentinel
receiver and could watch for it) rather than leaving each consumer to discover it.

Related and smaller: the `delivered <= 1` sentinel heuristic in `drive_changes_stream`,
`BackfillRunner::run_partition`, `emit_backfill_complete`, and `InventoryFusion` hardcodes
"exactly one sentinel receiver". It is duplicated four times and silently wrong if that ever
changes; it wants to be one helper on the slot.

## Roughly a thousand lines of machinery that nothing calls

- `scheduler/` (mod + lanes + budget, ~600 lines plus a test file), documented as deliberately
  unwired, with no dated plan.
- `LiveSupersedes` in `backfill/runner.rs`: ~160 lines of implementation and doc plus 8 unit tests
  for a set that is, by an extensively argued decision, never populated. The argument for not
  populating it is convincing; the conclusion should therefore be to delete the type, not to
  maintain a no-op filter, a ring-with-tombstones eviction policy, and tests pinning the tombstone
  semantics of dead code.
- `BackfillCheckpointWriter`: the file itself says it is unused on the hot path.
- `mutation::fanout::partition_by_account`: no caller.
- `MutationConfig::{fanout_buffer, retry_queue_cap}` and `BackfillConfig::clock_skew_warn`: never
  read anywhere, yet documented in `reference/sync.md` as if they tune something.
  `retry_queue_cap` in particular reads as a bound on the mutation retry queue, which is an
  unbounded `Vec`.

Every one of these has doc comments and reference-doc paragraphs that a reader must process before
discovering they describe nothing running.

## bulk_set_flags is a verbatim copy of run_bulk_pipeline

`engine.rs` carries the same ~230-line campaign loop twice: same retry structure, same throttle
wait, same `classify_item_outcome` handling, same four `plan_recovery` arms, same counter
rebalance, differing only in which `Account::bulk_*` method submits and which read-back guard
runs. `BulkPipelineOp` already exists; `SetFlags(FlagOp)` should be a third variant and the
duplicate deleted. As it stands, a fix to one arm (and there have clearly been several, judging by
the sync-D5/retry-sweep comments) has to be mirrored by hand.

## Mutation campaigns ignore pause, boundary, and shutdown

Neither campaign consults `BoundaryView` or the slot's `shutdown` token. An account paused by
`RetryBudgetExhausted` or `OperatorOverrideRequired`, i.e. one the engine has decided it should
not be talking to, will still have a running `bulk_set_flags` campaign resubmitting batches to the
wire on its retry schedule. Polls, push reconcile, backfill, and deferred inventory all park;
mutations do not. `reference/sync.md` claims pause "halts all engine-driven work for the account",
which is not true of the mutation pipeline.

## Structural: engine.rs is 5278 lines and mixes five unrelated concerns

Lifecycle (attach/detach/reopen), recovery dispatch (~900 lines of free functions), the backfill
orchestrator (~500), the mutation pipeline (~500), and roughly 1200 lines of 1:1 passthrough
forwarders that invent no semantics. The passthrough cluster in particular is mechanical: every
method is `live_account(id)?` then forward, with the identical doc comment shape, and is a macro
or a blanket forwarding trait, not 60 hand-written methods. Splitting recovery into `recovery/`
(it already has a `recovery.rs` that holds only the helpers, while the dispatch lives in
`engine.rs`, so the split is in the wrong place), the orchestrator into
`backfill/orchestrator.rs`, and the campaigns into `mutation/campaign.rs` would leave a
`SyncEngine` that fits in a head. `reference/sync.md`'s file map already describes this layout
aspirationally; the code does not match it.

## Smaller observations

- `push/mod.rs::push`: on a full queue the lossless lane spawns a task that `send().await`s, so a
  later `try_send` that succeeds can be delivered before the earlier spawned event.
  `Disconnected`/`Reconnected` ordering can invert, and the reconciler treats `Reconnected` as
  "full reconcile", so an inverted pair leaves the account marked disconnected with no reconcile.
- `drive_changes_stream` still takes `_account_id` and `_ack_tx` and threads them from four call
  sites through `spawn_scope_poll_inner`; dead parameters that obscure the actual data flow.
- `InvalidationSinkInner::runtime` is a `OnceLock` captured from whichever account registered
  first. Correct today (all `register` calls happen on the engine's runtime) but fragile and
  unstated.
- `open_pages_resume`'s "short final page, skip" arm is largely unreachable: `get_backfill` picks
  max `items_done`, so a full earlier window (500) outranks a short final one (300) and the walk
  resumes at the earlier window's end. Self-healing, but the documented decision table describes a
  path the store's selection rule mostly precludes.
- `reattach_account` calls `run_establish` with the real `store`, so newly discovered scopes get
  durable cursors written even when the swap later fails and the replacement is closed. Harmless
  today, but it leaves durable state for a topology that was never installed.
- `handle_schema_incompatible` and `restart_scope` hold `reopen_lock` across
  `re_establish_scope_with_backoff`, which sleeps 1s/2s between attempts. Every other recovery for
  that account queues behind it. Given the `unsubscribe_push` hang above, the lock's scope is
  worth a deliberate pass.

## Cross-cutting, outside this scope

`reference/sync.md` reads as a design essay rather than a reference. Several of its strongest
claims are invariants the code does not enforce: single sequential producer per lane+scope (half
fixed - poll-vs-poll is now enforced by generation-matched scope tokens, poll-vs-push-reconcile
is not, and the doc now says so); orchestrator excludes fused scopes; pause halts all
engine-driven work; `replace_from` atomicity. A doc that must be true is more useful when its
invariants are also test-pinned, and the remaining three have no test.
