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

- **B2. A full `reopen_tx` channel wedges every scope.** Refused in round 4,
  after B1 landed. The finding's force was compounding: poll tasks
  `.send().await` on the depth-16 reopen channel **while holding their drive
  lease**, so a slow serial reopen blocked recovery reporting, which blocked
  the poll task, which blocked push reconciliation for that scope. With
  `CursorRegistry::with_drive` the lease is released before `handle_drive_outcome`
  runs, and every `reopen_tx.send().await` in both the poll loop and the
  reconciler is now outside it. What survives is the ordinary and intended
  behaviour of a bounded channel: a slow reopen listener applies backpressure
  to the ORIGINATING poll task and no longer to push reconciliation of that
  scope or to any other scope. Do not re-open on the bare observation that the
  channel is bounded and the listener is serial - both are true and neither is
  the defect. New evidence would have to be a case where the backpressure
  crosses back into a scope the sender does not own.

## Round 4 closure note (A8, B1, B3, E2, E6, F4; B2 refused)

What landed, plus the hole the round's own fix opened one layer up - found, as
in every round of this arc, by cold review rather than by the fix pass's tests.

- **`CursorRegistry::with_drive` (B1, F4)** is the scoped combinator refactor 4
  asked for. It snapshots the cursor and registry generation under the lease,
  runs exactly one drive, and releases before recovery handoff, retry delay,
  reopen backpressure or cadence sleep. `claim_drive` stays published and
  working. Pinned end to end by
  `push_invalidation_drives_mid_poll_cadence_without_waiting_for_the_sleep`
  (ablated by re-taking the lease across the cadence sleep: fails).
- **The narrowed lease moved one invariant.** `handle_drive_outcome` used to
  compute `advanced` by re-reading the registry after the drive, which was only
  correct because the lease still excluded the reconciler. It now takes an
  `advanced: bool` measured inside the combinator. Left as-is, a push reconcile
  landing between the drive and the read would have been credited to the poll
  and pinned the cadence at `poll_min` on a scope the poll never moved. This is
  the only such site the round-4 audit found; the durable side was never
  lease-protected in the first place (the generation fence and `with_drive`
  returning `None` for a deleted scope do that work).
- **A8 is built**: `DebtLedger::cross_waived_barriers`, reached from both
  inventory front ends through `inventory_walk::cross_waived_barriers`. The
  writer re-checks the waiver AT THE BARRIER HIT against its live ledger,
  converts an all-waived report into unresolved waived debt, persists, and only
  then authorizes the crossing. All-or-nothing per report. An `OperatorBlocked`
  barrier parks the backfill rescan through
  `WriterRequest::ScopeBarrierBlocked` - and the park is recorded via
  `BackfillScan::record_attempt`, so the check rides the 5s-to-5min ramp instead
  of re-querying the single account writer every second for the life of the
  block.
- **E6 plus its cold-review correction.** The reconciler no longer `?`s out of a
  multi-scope hint, BUT the first implementation logged every error including
  `Error::Account`, which silently converted a fatal-but-RECOVERED condition
  (an incompatible cursor envelope derives `Engine(SchemaIncompatible)`) into a
  swallowed one: the invalid cursor stayed installed and the scope stalled until
  an unrelated poll reproduced the failure. `Error::Account` is now normalized
  onto the `Terminated` path, mirroring `handle_drive_outcome`, and the Engine
  arm ends the sweep only for an account-wide directive (`directive_target_scope
  == None`). Both properties are pinned separately by
  `a_push_drive_account_error_reaches_engine_recovery` and
  `a_failed_hinted_scope_reaches_recovery_without_abandoning_its_siblings`; the
  round's original sibling test counted drive ATTEMPTS only and passed against
  the swallowing code.
- **E2**: the refused-admission outcome is now `refused_activity_outcome`, a
  named boundary mapping - `Stop` reports `Stopped` (which is what sets
  `DriveRecovery::exit`), everything else parks.
- **B3**: `take_ack_writer` selects by `WorkerRole::AckWriter`, not `drained[0]`.

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
