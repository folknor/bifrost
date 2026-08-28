# bifrost-sync: hunt findings

Scope: `crates/sync/` - scheduler, concurrency budget, multiplexer, partitioned
backfill and checkpoint writer, push reconciler, mutation pipeline and fanout,
checkpoint envelope versioning, scope lifecycle, inventory coverage ledger.

Hunter note: read all of `crates/sync/src` (engine.rs selectively past the 1:1
passthrough clusters), cross-checked every claim against `reference/sync.md`, and
verified the load-bearing ones by grep (unused symbols, lease scopes, direct
store writes).

## Round 1 closure note (A1, A3, A4, D1, F2)

Landed as the unified `Publications` ledger. Three things the cold review
surfaced turned out to be consequences of the unification itself and were fixed
in the same pass, and are recorded here because later rounds touch the same
code:

- Supersession is keyed by LANE, and the backfill lane now includes the
  PARTITION. Keying it by scope alone (which is what `SyncControl::pending_key`
  did) made acknowledging partition A resolve to an unknown publication once
  sibling partition B published - a batch the consumer really received.
- A superseded publication is still acknowledgeable. Its claim moved to the
  survivor, so the acknowledgement persists the checkpoint and ingests nothing;
  a per-lane `folded` watermark carries that, bounded.
- Lag carry-forward is DEBT ONLY. Folding a `Complete` report into a later
  publication would discharge obligations on the strength of a batch the ring
  destroyed - the same silent loss A3 is about, arriving through A3's fix.

Not defects, recorded so nobody re-opens them: `PendingCoverage::claim` and
`SyncControl::record_checkpoint` remain published and remain kind- and
value-identified respectively. No engine path uses either; every engine path
goes through the lane-checked and publication-identified forms.

## Round 3 closure note (A2, A6, C2, E3, E7, F3, F1-residual)

What landed, and the two holes the round's own fix opened one layer up - both
found by cold review, not by the fix pass's tests. Same signature as rounds 1
and 2.

- **Both inventory front ends now share `InventoryWalk`** (`inventory_walk.rs`).
  It owns the barrier decision and the last accepted resume checkpoint for
  `BackfillRunner` and `InventoryFusion` alike, including a barrier carried by
  the terminal `Done` (E3). The DIVERGENCE of the two walks was A2's cause, so
  the rules can now only change for both at once.
- **A barrier stops the SCOPE, not just the partition.** The first fix returned
  `complete: false` from the partition and both orchestrator loops read that as
  "degraded, carry on", requested the next fixed partition or page window, and
  handed out checkpoints beyond the barrier - A2 recreated exactly, one layer up.
  Partition sequencing now belongs to `backfill/scope_walk.rs`: the loop asks the
  driver for the next partition and a stopped walk has none to give, so there is
  no flag left for a loop to forget.
- **A barrier the store refused is not a recorded barrier.** The first fix
  checked only whether the oneshot sender was dropped, so `Ok(Err(store_error))`
  counted as success: the walk announced a barrier that a restart forgets,
  leaving nothing durable for E7's `block` to act on. `record_barriers` returns
  `Result` and the runner refuses to announce over an `Err`. A DEPARTED writer
  (detach, shutdown) stays non-fatal - there is nothing to persist to. The
  mid-arc close pass found the SAME hole still open on the fusion front end
  (log-and-`NoCursor` over a refused write) and closed it: fusion now fails the
  walk instead, pinned by
  `a_store_refused_barrier_fails_the_fusion_walk_instead_of_announcing_it`.
- **A6** discharges by proof UNION and retains only proofs still load-bearing
  for an open obligation or barrier.
- **F1 residual** is closed: `BackfillCheckpointWriter` routes through the
  account writer. Its `store` field changed shape, so a public
  `BackfillCheckpointTarget::direct` and `BackfillCheckpointWriter::new` keep the
  type constructible by external code with what it could already supply; the
  direct route carries the read-modify-write race by construction and says so.

**F3 landed only in part.** The shared piece is the barrier and resume state.
Checkpoint MINTING (fusion takes the account's, backfill mints positional ones)
and terminal-summary debt recording are still two implementations. The
divergence that caused A2 is closed; the structural item is not, and is left
open under F below.

### Refuted, not fixed - do not re-open without new evidence

- **C2. `drive_changes_stream` always emits `publication: None`.** Does not
  reproduce. `drive_changes_stream`'s `publish` closure sets
  `me.publication = Some(..)` whenever `control` is `Some` and `checkpoint` is
  `Some`, and both production callers (`multiplexer/mod.rs` and
  `push/reconciler.rs`) pass `Some(control)`. `emit_backfill_complete` publishes
  unconditionally. The invariant was unpinned, which is probably how the finding
  arose; it is pinned now by
  `a_published_change_checkpoint_always_carries_its_publication_id` in
  `tests/attach_schema_recovery.rs`.

## A. Correctness - lost data / lost debt

### A8. Waiving a barrier never releases the scope: the crossing path is unbuilt

**Confidence: high.** Found by the mid-arc close pass over rounds 1-3.
`DebtLedger::barrier_waived` has ZERO production callers - neither
`InventoryWalk::inspect` nor either front end consults it - so a walk stops at
a waived barrier exactly as at an unwaived one. The only place the waiver has
effect is `completion_permitted`, evaluated when the completion sentinel's
acknowledgement reaches the writer (`persist_ack_request` in `engine.rs`); but
a barrier-stopped scope never EMITS the sentinel, because `ScopeWalkDriver`
reports `completed() == false` and both orchestrator arms skip
`emit_backfill_complete`. Net effect: `SyncEngine::waive_obligation` on a
barrier key is accepted, persisted, and inert. The operator escape hatch that
`reference/sync.md` calls load-bearing ("declared loss beats permanent
non-convergence") does not release anything.

What the fix needs, and why it was not done in a close pass: the walk must
learn which barrier keys are waived (a writer read, or a snapshot passed into
`run_partition` / `run_stream`), skip the barrier decision for a report whose
barrier obligations are ALL waived, and the crossing checkpoint must atomically
record an unresolved-but-waived ledger entry for the crossed ground (the
`BarrierIncident` doc in `cursor/ledger.rs` spells out the intended shape).
Skipping only SOME of a report's barriers is not sound - the checkpoint still
crosses the others. Watch the staleness race: a waiver landing mid-walk versus
a walk that snapshotted before it; re-checking at the barrier hit (a writer
round-trip per barrier, not per page) is the cheap sound point.

Related liveness observation for the round-4 brief: a barrier-stopped scope
stays `Pending` and re-walks on `BackfillScan`'s 5s-doubling-to-5min backoff
FOREVER, re-broadcasting every pre-barrier page and re-recording the incident
each pass (idempotent, but full wire cost). Nothing consults the durable
ledger's `OperatorBlocked` policy to park the rescan. Bounded (one walk per
5min per blocked scope) but permanent until an operator acts - and per this
finding, `waive` does not currently stop it either.

## B. Liveness / latency

### B1. The per-scope drive lease is held across the poll cadence sleep

**Confidence: high.** In `spawn_scope_poll_inner`, `let drive =
cursors.claim_drive(&scope).await` is bound in the loop body and never dropped
early, so it lives through `handle_drive_outcome` (retry sleeps,
`reopen_tx.send().await`) **and** `tokio::time::sleep(cadence.interval)`.
`Reconciler::reconcile` awaits the same lease per hinted scope. Result: a push
invalidation for a scope blocks for up to `poll_max` (30 minutes) waiting for the
poll task's idle sleep. Push exists to beat the cadence; this makes push strictly
no better than polling. Same for the reconciler's own lease, held across its retry
sleeps. Fix is a one-line `drop(drive)` after the drive - but the structural fix is
to make the lease cover the drive only, by construction (an RAII scope or a
`with_drive(async {...})` combinator), since two call sites already got the extent
wrong.

### B2. A full `reopen_tx` channel wedges every scope

**Confidence: medium-high.** The reopen listener is one serial task;
`restart_account` runs a full open + reattach inline. The channel is depth 16, and
poll tasks `.send().await` on it **while holding their drive lease** (B1). A slow
reopen therefore blocks recovery reporting, which blocks the poll task, which
blocks push reconciliation for that scope.

### B3. Ack-writer identification by position

**Confidence: high (fragility), low (live bug).** `detach_inner` recovers the ack
writer as `drained.remove(0)`, relying on it being spawned first. Nothing enforces
that. Reordering a spawn silently makes detach await the writer before the stream
workers, which the comment says always burns the whole `detach_timeout`.

## C. Contracts a consumer cannot honour

### C3. Reference contradicts itself on `retry_queue_cap`

**Confidence: high.** `reference/sync.md` says both "One resubmission is at most
`MutationConfig::retry_queue_cap` targets wide" and "`MutationConfig::retry_queue_cap`
is inert and does not bound this vector". The code implements the first. The second
paragraph is stale and should go.

### C4. `IdempotencyVendor` doc claims a `CheckpointStore` campaign key that does not exist

**Confidence: high.** The module doc says `run_id` "is persisted via the
`CheckpointStore` under a campaign-scoped key"; the trait has no such method and
nothing does it. The vendor is consumer-supplied, which is the real (and fine)
contract.

## D. Leaks

- **D2.** `CursorRegistry::drive_leases` is never pruned; `delete(&scope)` leaves
  the lease behind. Bounded by the scope identity space, so minor. **Confidence: high.**
- **D3.** `ThrottleBucket::waits` under `ThrottleKey::Account` is deliberately never
  forgotten on detach; combined with D2 it is bounded, noted only for completeness.

## E. Smaller correctness items

- **E1. Mailbox throttle degrades *wider*, not narrower.** `resolve_throttle_key`
  maps `ThrottleScope::Mailbox` to `ThrottleKey::Account` when the error names no
  mailbox, and `wait_for_account` includes the `Account` key. The comment claims the
  fallback "is a subset - never wider"; for `Mailbox` the fallback is strictly wider,
  so a per-mailbox 429 stalls the entire account. The doc elsewhere says the exact
  opposite is intended. **Confidence: high.**
- **E2. `drive_changes_stream` returns `Paused` on a `Stop`.** The early
  `begin_activity()`-failed return happens before any boundary read, so a detaching
  account's drive reports `ChangesEvent::Paused`; `handle_drive_outcome` then sets
  `exit: false` and the poll loop takes another lap before noticing the token.
  Cosmetic in practice, wrong in kind. **Confidence: high.**
- **E4. Per-item engine-blocked campaigns leave unseen ids in no bucket.** When
  `classify_item_outcome` returns an engine directive, `blocked_by_engine` breaks out
  of the attempt loop with no sweep of `remaining` ids the stream never resolved -
  unlike the stream-level `Engine`/`Reconcile` arms, which do sweep. Those ids are
  counted in no lane, i.e. the campaign reports success for work that never happened.
  Same defect the `retry_queue_cap` truncation comment describes at length, in a
  sibling path. **Confidence: medium.**
- **E5. `ScopeLifecycle::Deleted`/`Renamed` remove scope tokens by key, not by
  generation** - the exact eviction hazard `retire_scope_token` exists to prevent. It
  cancels first, so it is much less dangerous, but the asymmetry is an accident
  waiting for a delete/recreate race. **Confidence: medium.**
- **E6. `Reconciler::reconcile` uses `?` on `drive_changes_stream`,** abandoning
  every remaining scope of a multi-scope hint on the first engine-level error, with
  only a `warn!`. **Confidence: high.**
- **E8. `MultiplexerHandle::cancel` is a child token nobody ever cancels** (the
  multiplexer is driven by `slot.shutdown`). Dead field that reads as a live control.
  **Confidence: high.**

## F. Structural - what shape this should have had

3. **`BackfillRunner` and `InventoryFusion` are two implementations of one walk.**
   PARTIALLY RESOLVED in round 3. The safety-critical half - barrier detection,
   the resume checkpoint, and barrier-incident recording - is now the shared
   `InventoryWalk` in `crates/sync/src/inventory_walk.rs`, which is what closed
   A2 and E3. **Confidence: high (residual is real, and unfixed).** Still two
   implementations: checkpoint MINTING (fusion forwards the account's checkpoint,
   backfill mints a positional `page:F:T` from its own `seen_total`) and terminal
   `Done` handling (backfill sends `RecordDebt` for a degraded summary and
   `break`s, fusion `finalize`s the cursor). One driver parameterised by "who
   mints the checkpoint" remains the shape this should have. Nothing here is
   currently a lost-data path; it is the RE-DIVERGENCE risk that A2 already
   charged the project for once.

4. **The scope drive lease should be a scoped combinator.** Both call sites
   over-extend it (B1). `cursors.with_drive(&scope, |cursor, gen| async { ... }).await`
   makes the extent unwritable-wrong.

5. **The scheduler and `BudgetGate` are an entire dead subsystem.** Nothing in any
   work path calls `submit`/`pull`/`acquire`; `Scheduler::pull` is not even
   async-wakeup-capable (callers must supply their own notification). Meanwhile the
   engine has *no* admission control: N per-scope poll tasks, a reconciler, a backfill
   orchestrator, and unbounded mutation campaigns all hit one connection concurrently,
   throttled only by whatever `bifrost-net` does. Either wire it (which means the lane
   queues need a real wakeup source and `pull` needs to be async) or delete it and say
   plainly that `bifrost-net` is the only chokepoint. Per the standing rule the delete
   option is filed as a finding, not a mandate - and the keep-it fix is real work, not
   a rename.

## Out-of-scope observations

- `bifrost-types`: `InventoryBatch::checkpoint` is `Option<Checkpoint>` with no way
  to distinguish "this page has no checkpoint" from "I stripped this checkpoint
  because of a barrier". The barrier signal rides only in `coverage`, which is why
  A2 was easy to miss. A dedicated `PageCheckpoint::{Advance(..), Withheld}` would
  make the omission a compile error. STILL OPEN after round 3: the shared
  `InventoryWalk` makes both front ends read `coverage` the same way, but nothing
  in the type system stops a third front end from ignoring it. `bifrost-types` was
  not touched in this round.
- `bifrost-types`: `AccountError` carries no tenant identity, so
  `ThrottleScope::Tenant` is unimplementable and always degrades - the engine
  documents this as blocked on types. Worth deciding, since `Tenant` degrading to
  `Account` has the same widening problem as E1 in reverse (it silently under
  throttles siblings).
