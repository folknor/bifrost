# Bug hunt: bifrost-sync (engine)

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/sync/` - scheduler, multiplexer, partitioned backfill, push
reconciler, mutation pipeline, checkpoint envelope versioning, scope
lifecycle. Every source file read (tail of `cursor/ledger_envelope.rs` and the
engine's test module skimmed rather than exhaustively read).

## Confident defects

(Finding 1 - a terminal changes-stream error respawning/re-driving the scope
once per second forever - is fixed: poll exits now carry a `PollExit`
disposition, and the Terminal arm parks the scope by leaving its uncancelled
`ScopeToken` in the map as a tombstone the 1s scan honors; pinned by
`terminal_drive_outcome_parks_the_scope_instead_of_retiring_it` and documented
in `reference/sync.md`. The push reconciler's milder sibling - a later hint
can still re-drive a terminally failed scope, since the reconciler does not
consult scope tokens - remains open and is now stated in the reference.)

### 2. `ScopeLifecycle::Deleted` never touches durable state, and the surviving backfill completion marker silently skips a recreated folder's backfill

`multiplexer/mod.rs` lifecycle arm + `engine.rs` orchestrator. The `Deleted`
(and the delete half of `Renamed`) handler calls only
`lifecycle_cursors.delete(&scope)` and cancels the token - no
`writer.reset_scope_for_disable`, unlike every other scope-retirement path.
The durable change cursor AND all backfill rows, completion marker included,
survive. Consequences: (a) a stale durable row leaks until the next full
reopen's vanished-scope cleanup; (b) worse, if a folder is deleted and later
recreated under the same `FolderId` (a folder name, for most protocols), the
new incarnation's backfill scan finds the old completion marker via
`backfill_complete_recorded` and **skips the entire cold-start walk**. The
consumer, which plausibly purged the folder's data on `Deleted`, never
receives the recreated folder's contents except via live changes. That is a
data-invisibility shape, not just a leak, and it violates the crate's own "a
leak, never data loss" standard for stale rows. (For IMAP, UIDVALIDITY may
save the change cursor, but nothing saves the backfill marker.)

### 3. A withheld backfill completion sentinel is reported as a durable checkpoint

`engine.rs::persist_ack_request` + `control.rs::announce_durable`. When the
ledger has open debt, `persist_ack_request` deliberately withholds the
completion sentinel and persists only the ledger, returning `Ok`. The ack
writer's success arm then runs `coverage.settle_checkpoint(...)` (moving the
persisted watermark for a checkpoint the store never accepted - a later retry
answers `AlreadyPersisted`) and `control.record_publication(...)`, whose
`announce_durable` inserts the sentinel into the `DurableCheckpointSet` under
`DurableLane::Backfill(scope, "complete")`. Every subsequent `pause()` /
`checkpoint_now()` therefore reports the backfill-complete boundary as durable
while the store holds no such row and the next attach will re-walk.
Re-delivery, not loss - but the module's own rule is "nothing durable is
invented; the snapshot is not advanced," and here it is. The withheld-sentinel
path needs to skip both `settle_checkpoint` and `announce_durable` (retiring
the publication instead, the way a failed write does), or return a
distinguishable outcome.

### 4. Repair publications misattribute every recovered id to the first id's scope

`repair.rs::publish_recovered`. `plan_requests` plans across *all* repairable
obligations, which can span multiple scopes, yet the single batched
`MultiplexerEvent` uses `recovered[0].0` as the scope for the whole batch. The
recovered ids are collected as `(CursorScope, ObjectId)` pairs - the per-id
scope is right there and then discarded. A consumer that routes broadcast
events by `MultiplexerEvent::scope` (the type's stated purpose) files scope-B
`Created` ids under scope A, or drops them if it filters. Either publish one
event per scope or per (scope-grouped) batch.

## Suspected defects / contract tensions

### 5. An engine directive raised by one item erases sibling items' read-back protection

`engine.rs::run_bulk_pipeline`. When any item (or the stream) yields
`RecoveryPlan::Engine`, the `blocked_by_engine` sweep overwrites every outcome
not in {Applied, Skipped, FailedTerminal, BlockedByEngine} - including
`PendingReadback`. Those are exactly the ids whose write "may have landed and
must be verified rather than replayed" (`Uncertain`, downgrades,
`AfterStateRefresh`). After the sweep, `unresolved_readback_ids` finds
nothing, the read-back guard is skipped entirely, and a mutation that actually
applied is reported `blocked_by_engine`. The retry-termination sweep was
deliberately narrowed to *not* claim read-back-lane ids ("An id sitting in the
read-back lane belongs to the guard"); the engine-directive sweep tramples the
same invariant. If the intent is "campaign halted, accounting stops," the
reference should say the read-back guarantee is void on engine directives; if
not, the sweep should leave `PendingReadback` alone and still run the guard.

### 6. Public `SyncEngine::reopen` is not serialized against `detach`, allowing a leaked replacement connection

`engine.rs`. `reopen` takes neither the `lifecycle_inflight` guard nor
anything that observes slot removal; it holds a pre-detach
`Arc<AccountSlot>`. Race: `reopen`'s `begin_activity()` succeeds just before
detach flips the boundary to `Stop`; `factory.open()` proceeds while detach
removes the slot, awaits workers, and closes the *old* handle. If the reattach
then completes with no newly-established cursors and no push subscriptions
(the two paths that would touch the now-closed writer channel and abort),
`reattach_account` swaps the replacement into the orphaned slot's `ArcSwap`
and closes the *previous* (already-closed) handle. The replacement connection
has no owner and is never closed. Narrow window, but detach explicitly does
not wait for consumer-driven activity, so nothing excludes it. A cheap fix:
have `reopen` re-check `accounts.contains_key` under the lifecycle guard (or
make `open_replacement` fail when the slot's shutdown token is cancelled - it
currently never consults it between `begin_activity` and the swap).

### 7. Inline recovery sleeps stall the single push reconciler account-wide and ignore scope cancellation

`push/reconciler.rs`, `multiplexer/mod.rs`. The reconciler's `Terminated ->
Retry/Reconcile` arms `tokio::time::sleep(delay).await` inline, un-`select!`ed,
inside the one reconciler task. A provider `Retry-After` of minutes on one
scope freezes all push reconciliation for the account for that duration (the
watch channel meanwhile fills and coalesces). The throttle bucket already
records the same deadline and every drive path consults it, so the inline
sleep is redundant with a much cheaper "skip this scope, continue the sweep."
The poll loop's Retry sleep has the milder version: it ignores
`scope_cancel`/`shutdown`, so a deleted scope or detach waits out the full
provider hint (detach then burns toward `detach_timeout` and aborts). Same
pattern in `re_establish_scope_with_backoff` / `restart_account` backoff
sleeps (no shutdown select), which guarantees detach-during-recovery always
hits the abort path rather than draining cleanly.

### 8. Per-item retryable failures resubmit with no delay

`engine.rs::run_bulk_pipeline`. `retry_advice` is set only from a
*stream-level* Retry termination. A stream that ends `Done` after emitting
per-item `Retry(SameRequest)` failures loops straight into the next attempt
with zero sleep unless the advice happened to carry a throttle scope + hint (a
bare `RetryHint` with `throttle_scope: None` records nothing). Bounded by
`mutation_max_retries` (5), but it's 5 back-to-back resubmissions against a
failing provider where every other retry path honors the hint.

## Latent / design-level observations

### 9. Broadcast ring pressure during cold start is structural

`changes_capacity` defaults to 256 with no producer-side backpressure; a
backfill of a large mailbox can outrun a consumer, triggering the (well-built)
lag-abandonment machinery routinely rather than exceptionally - every
abandonment costs re-reads from the last durable checkpoint. Since the engine
already gates backfill on `wait_for_real_subscriber`, a bounded per-account
mpsc (or a permit from the consumer per N batches) for the backfill lane
specifically would turn routine loss-plus-reconcile into flow control. This is
the one place the hunter would consider a real structural rewrite: the
broadcast channel is the right shape for live changes and the wrong shape for
cold-start bulk delivery, and the crate has accumulated an impressive amount
of machinery (publications ledger abandonment, debt carry-forward, lag
warnings) compensating for that mismatch.

### 10. `engine.rs` (8k lines) concentrates too much in one file

Attach wiring, the backfill orchestrator, the ack writer, reattach, and the
bulk pipeline in one file, with 15-20-argument free functions
(`run_backfill_orchestrator`, `run_deferred_inventory_establishment`) that are
`RecoveryContext`-shaped but hand-rolled. The two orchestrator arms (`Fixed` /
`OpenPages`) are ~150 duplicated lines differing only in resume logic; both
already drive `ScopeWalkDriver`, so the duplication is one `enum`-resume seam
away from collapsing. Pre-1.0, splitting engine.rs into
attach/orchestrator/writer/reattach modules and bundling worker wiring into a
context struct would pay for itself.

### 11. Minor - RESOLVED, finding was wrong

The `forward_events` drop-counter claim ("a successfully delivered coalesced
invalidation is still counted as dropped - the metric over-reports") misread
the metric's intent: `dropped` counts payload loss (the specific event
replaced by a coarser coalesced form), not delivery loss, and
`full_sink_counts_a_coalesced_invalidation_as_dropped` pins exactly that -
the coalesced event is received AND `dropped == 1`. A comment now documents
the semantics at the increment. The `announce_durable` `(None, _)` half of
the finding was real-but-unreachable and is now commented at the arm.

### 12. Doc drift, small

`reference/sync.md` says lifecycle `Deleted` "drops the cursor" without noting
the durable rows survive (finding 2 makes that gap load-bearing), and the
mutation section's read-back derivation claim ("every id still PendingRetry or
PendingReadback") is falsified by the engine-directive sweep (finding 5) -
whichever way 5 is resolved, one of the two texts needs updating.

## Not found / verified sound

The heavily documented invariants (publication identity, supersession folding,
provisional reattach rows, barrier taint, drive leases, generation fencing,
throttle key resolution, envelope migration) all check out against their code
and are pinned by unusually sharp tests; no soundness hole found in
`PendingCoverage`, the ack writer's transition ordering, or the scheduler's
admission dispatcher.
