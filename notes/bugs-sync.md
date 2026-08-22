# bifrost-sync bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/sync/` (engine, control, cancel,
multiplexer, backfill, push, mutation, cursor, recovery, scheduler, types). Findings are
unverified work material.

2026-08-07: four findings verified against the code and fixed, each with a regression test -
the cursor envelope outer version, the detach/re-attach teardown race, the scope-token cleanup
identity bug, and the latched `CheckpointNow`. Their sections are removed below; the behaviour
now lives in `reference/sync.md`. Everything still listed is unverified.

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
