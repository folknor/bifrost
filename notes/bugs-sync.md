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

## Round 5 closure note (C3, C4, D2, D3, E1, E4, E5, E8, F5)

The round's first pass landed all nine findings; its cold review then found four
defects in that pass's own code, three of them in F5 and one in D2. Same
signature as every earlier round of this arc, and the D2 case is the cleanest
instance yet of a fix opening a hole one layer up: the finding asked for the
lease to be pruned on delete, and pruning it is exactly what broke exclusive
drive. All four are fixed below, in the same commit.

- **F5 admission control is real, not relocated.** The first pass routed
  admission through `submit`/`pull` and then had every admit caller run the
  scheduler itself, so a request could dequeue and immediately park on the
  `BudgetGate` semaphore. That defeated both scheduler properties: a dequeued
  request no longer counted against `lane_capacity` (so the bound was vacuous),
  and once parked on the semaphore its order was arrival order, so a later
  `Foreground` request could not preempt an earlier `Background` one. Admission
  is now a separate path with its own four lanes and ONE dispatcher task that
  grants a request only when its budget can actually be granted. The dispatcher
  races one acquisition per distinct `(account, kind)` class rather than only
  the head class - a single-head dispatcher reintroduces the cross-account
  starvation `BudgetGate`'s per-account layer exists to prevent, which is
  pinned by `one_saturated_account_does_not_stall_another` (it failed against
  the first dispatcher written here).
- **Every wire path is admitted, including the two the first pass missed.** The
  mutation READ-BACK guard hydrates after the attempt loop released the
  campaign's permit, and deferred inventory fusion - one of the heaviest
  cold-start walks - had no admission at all, while `reference/sync.md` had
  already been edited to claim read-back was admitted. Both now admit; the
  reference says what the code does.
- **Change-drive admission is outside `with_drive`**, and the permit is
  released before recovery handoff and cadence sleeps. Round 4's narrowed lease
  extent is untouched: keeping a lease ENTRY alive past a delete is not the same
  as widening a lease EXTENT.
- **A refused admission is transient, not terminal.** The first pass returned
  from the poll loop on an admission error, which retires a scope's polling
  permanently for a condition that clears by itself. The poll loop now backs off
  one cadence step; the push sweep skips only that scope, per E6.
- Per-item engine recovery now sweeps every unresolved campaign id into
  `blocked_by_engine`. The real campaign test was ablated and fell from three
  accounted ids to one.
- **D2 as landed, after its own correction.** A deleted scope KEEPS its drive
  lease entry, so a re-established incarnation waits for the previous
  incarnation's drive rather than minting a second mutex and running a
  concurrent protocol stream against the same scope; the generation fence stops
  the stale publication but never stopped the concurrent wire work. Unbounded
  growth is answered by pruning entries no drive holds, which is safe because a
  held lease keeps a second `Arc` alive and `claim_drive` clones it under the
  same write lock. The fence itself became a PAIR (`DriveGeneration`): the first
  pass bumped the account-wide registry generation on every delete, which fenced
  unrelated scopes' in-flight drives and made them discard valid results and
  re-walk from their old cursors. Lifecycle token cancellation retires by
  generation, and `MultiplexerHandle::cancel` is the actual token driving the
  multiplexer task.
- Mailbox throttles without mailbox identity remain operation-local instead of
  widening to the account. Tenant-wide enforcement remains blocked on a tenant
  identity in `bifrost-types` and is filed as a standalone cross-crate item in
  `notes/todo.md`.
- Account-key throttle deadlines deliberately survive detach until their deadline:
  they describe the stable account, prevent reattach from bypassing a provider
  wait, and are time-bounded by expiry cleanup. The durable reference now states
  that decision.
- The stale `retry_queue_cap` and `IdempotencyVendor` claims were removed. The
  durable scheduler section now describes the wired system.
- F3 remains an accepted structural residual, not an open defect: both inventory
  front ends share the safety-critical `InventoryWalk`; checkpoint minting differs
  by protocol shape and no current lost-data path was found.

### The `pull` API question - kept, not reshaped

The first pass changed `Scheduler::pull` from `fn pull(&self) -> Option<WorkItem>`
to `async fn pull(&self) -> WorkItem`. Five earlier cold-review objections in
this arc were pure SHAPE changes and were overruled on standing precedent; this
one was not the same, and the reviewer was right. Losing the non-blocking
"is there work right now?" answer removes a CAPABILITY, not a shape - and it
removes it from the exact subsystem whose deletion-as-dead-code was once
reverted at the owner's instruction.

So `pull` keeps its original signature and semantics, `try_pull` is a
name-symmetric alias, and the waiting form is the new `pull_next().await`.
`DriveGeneration` is `#[non_exhaustive]` but carries a `new` constructor for the
same reason: `drive_changes_stream` takes one, and a published type an external
caller cannot construct is removed in substance.

## Out-of-scope observations (carried, not fixed)

Restored here rather than dropped with round 5's edits - neither is resolved.

- `bifrost-types`: `InventoryBatch::checkpoint` is `Option<Checkpoint>` with no
  way to distinguish "this page has no checkpoint" from "I stripped this
  checkpoint because of a barrier". The barrier signal rides only in `coverage`,
  which is why A2 was easy to miss. A dedicated
  `PageCheckpoint::{Advance(..), Withheld}` would make the omission a compile
  error. The shared `InventoryWalk` makes both current front ends read
  `coverage` the same way, but nothing in the type system stops a third front
  end from ignoring it. `bifrost-types` was not touched in this arc.
- `bifrost-types`: `AccountError` carries no tenant identity, so
  `ThrottleScope::Tenant` is unimplementable and always degrades to the account
  key, silently under-throttling siblings under a tenant-wide 429. Filed as a
  standalone cross-crate item in `notes/todo.md`.

Round 5 leaves no open findings in this document.
