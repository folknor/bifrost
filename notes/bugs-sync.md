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

## A. Correctness - lost data / lost debt

### A2. `BackfillRunner` ignores the barrier protocol entirely

**Confidence: high.** `InventoryFusion` honours a barrier: strip the checkpoint,
deliver items, stop the walk. `BackfillRunner::run_partition` never calls
`coverage.has_barrier()`, never reads `batch.checkpoint` (the field exists on
`InventoryBatch` and is discarded), and mints its own `BackfillCheckpoint` from
its own `seen_total` for every page unconditionally. So on the backfill path a
provider cannot refuse a checkpoint: the `page:F:T` key lands, the consumer acks
it, and `open_pages_resume` resumes at `T` - leaping the barrier region
permanently. Only the *completion sentinel* is withheld (`complete=false`), which
does not protect positional resume. This is the JMAP-Email / Gmail cold-start
lane, i.e. the one where all hydration rides backfill.

### A5. Three recovery paths write durably outside the single writer

**Confidence: high.** `restart_scope`, `disable_scope`, and
`handle_schema_incompatible` call `ctx.store.delete_change_cursor` /
`delete_backfill` directly (engine.rs 4580, 4589, 4621, 4630, 4654). The reference
states "One writer per account owns every durable mutation" and explains at length
why direct writes race acknowledged ones. An in-flight consumer ack for that scope,
ordered behind the delete in the writer, re-persists the stale cursor that
`restart_scope` deleted precisely to force re-establishment - the account then
resumes from an invalid cursor and re-enters the same recovery loop. Same shape as
the reattach race that motivated `WriterRequest`. They also bypass the ledger,
leaving debt hanging off a cursor that no longer exists.

### A6. `DebtLedger::proved` is write-only

**Confidence: high.** The field is pushed to in `record_proof` and never read. So
(a) the documented union discharge - "debt raised under `30d..90d` is covered by
the union of `7d..60d` and `60d..180d` and by neither alone" - is not implemented;
discharge only ever tests one domain at a time at ingest. And (b) it is an
unbounded `Vec` inside the per-account ledger that `apply_transition` rewrites
wholesale on *every* acknowledged checkpoint. That is durable, unbounded, and on
the hot path.

### A7. `record_checkpoint` moves the durable snapshot backwards, and the snapshot is account-global

**Confidence: high.** The snapshot is a single `Option<Checkpoint>` overwritten by
whichever ack arrives last, with no ordering check and no per-scope keying.
`pause()` on a multi-scope account returns the checkpoint of an arbitrary scope; an
out-of-order ack of an older checkpoint publishes it as "the latest durable
checkpoint". `Control::pause() -> Option<Checkpoint>` is structurally unable to
describe a boundary for an account with more than one cursor scope - the right
shape is a per-scope map (or a monotonic boundary token), not one slot.

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

### C1. `PublicationId` is not durable, but an unknown publication is a hard error

**Confidence: high.** `persist_ack_request` returns `Err(CheckpointStore("unknown
publication"))` for `ClaimLookup::Unknown`. The documented consumer flow is
"persist (items, checkpoint) atomically, then ack". A consumer that persists a
checkpoint, crashes, and acks after restart passes a publication id that no longer
exists - its ack is permanently refused, so the durable cursor never advances.
Passing `None` "works" but silently means *no coverage claim*, which is the lying
record failure mode in the other direction. There is no correct choice available to
the consumer.

### C2. `drive_changes_stream` always emits `publication: None`

**Confidence: high.** `MultiplexerEvent::publication` is documented as "`Some`
exactly when `checkpoint` is `Some`", and the live change path - the primary path -
violates that on every batch. `emit_backfill_complete` does too. Consumers written
to the documented invariant will assert or mis-branch.

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
- **E3. Fusion loses the resume position for a `Done`-carried barrier.** The `Batch`
  barrier arm passes `last_accepted` into `record_barriers`; the `Done` arm passes
  `None`, discarding the prefix checkpoint a later walk would resume from.
  **Confidence: medium-high.**
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
- **E7. `DebtLedger::block` does not reach barriers** while `waive` does - an
  operator can waive a barrier but not block one. **Confidence: high.**
- **E8. `MultiplexerHandle::cancel` is a child token nobody ever cancels** (the
  multiplexer is driven by `slot.shutdown`). Dead field that reads as a live control.
  **Confidence: high.**

## F. Structural - what shape this should have had

1. **The durable-write path should be a type, not a convention.** "One writer per
   account owns every durable mutation" is enforced by nothing:
   `Arc<DynCheckpointStore>` is handed to `RecoveryContext`, and three paths use it
   (A5). The store handle should not be reachable outside `ack_writer` at all - hand
   recovery paths a `WriterHandle` whose only methods are `WriterRequest` sends, and
   make `CheckpointStore` private to the writer module. That deletes an entire defect
   class rather than re-auditing for it.

3. **`BackfillRunner` and `InventoryFusion` are two implementations of one walk**
   and have already diverged on the safety-critical rule (A2): barrier handling,
   checkpoint minting, and terminal-summary debt are all handled differently. They
   should be one driver parameterised by "who mints the checkpoint" - the fusion path
   takes the account's, the backfill path mints positional ones - with
   barrier/coverage handling in the shared body.

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

6. **`Control::pause()`'s return type is wrong for the domain** (A7). One checkpoint
   cannot describe a multi-scope account.

## Out-of-scope observations

- `bifrost-types`: `InventoryBatch::checkpoint` is `Option<Checkpoint>` with no way
  to distinguish "this page has no checkpoint" from "I stripped this checkpoint
  because of a barrier". The barrier signal currently rides only in `coverage`, which
  is why A2 was easy to miss. A dedicated `PageCheckpoint::{Advance(..), Withheld}`
  would make the backfill runner's omission a compile error.
- `bifrost-types`: `AccountError` carries no tenant identity, so
  `ThrottleScope::Tenant` is unimplementable and always degrades - the engine
  documents this as blocked on types. Worth deciding, since `Tenant` degrading to
  `Account` has the same widening problem as E1 in reverse (it silently under
  throttles siblings).
