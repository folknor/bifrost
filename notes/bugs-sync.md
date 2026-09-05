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
in `reference/sync.md`. The push reconciler's milder sibling is now fixed too:
the tombstone is marked `parked`, the reconciler shares the same token map and
skips a parked scope rather than re-driving it. Pinned by
`a_terminally_parked_scope_is_not_re_driven_by_a_later_hint`.)

(Finding 2 - lifecycle `Deleted` leaving the durable change cursor and
backfill rows (completion marker included) behind, so a folder recreated
under the same id skipped its cold-start walk - is fixed: the extracted
`apply_lifecycle_transition` raises `ReopenRequest::ScopeDeleted` for the
delete half of `Deleted`/`Renamed`, and the engine's reopen listener purges
via the new `reset_scope_for_deletion` (delete_backfill = true). Pinned by
`deleted_lifecycle_purges_cursor_and_requests_durable_deletion`;
`reference/sync.md` updated.)

(Finding 3 - a withheld backfill completion sentinel reported as a durable
checkpoint - is fixed: `persist_ack_request` now answers `Durable` or
`SentinelWithheld`, and the withheld arm skips `settle_checkpoint` and
`announce_durable`, retiring the publication instead exactly as a failed store
write does. The consumer's ack still returns `Ok`. Pinned by
`a_withheld_completion_sentinel_is_not_announced_durable`; `reference/sync.md`
updated.)

(Finding 4 - repair publications misattributing every recovered id to the first
id's scope - is fixed: `run_repair_pass` groups both the recovered ids and the
resolutions by scope and emits one publication plus one `ApplyRepair` per scope,
so an obligation discharges only on the acknowledgement of the batch that
carried its own id. Pinned by
`recovered_ids_are_published_under_their_own_scope`; `reference/sync.md`
updated.)

## Suspected defects / contract tensions

(Finding 5 - an engine directive erasing sibling items' read-back protection -
is fixed: the `blocked_by_engine` sweep now skips `PendingReadback` the way the
retry-termination sweep already did, so the read-back guard still runs and a
mutation that actually landed is reconciled instead of being reported
`blocked_by_engine`. Pinned by
`an_engine_directive_leaves_a_sibling_read_back_alone`. The mutation section of
`reference/sync.md` named in finding 12 is updated to state the invariant on
every exit path, which closes that half of 12 too.)

(Finding 6 - `reopen` not serialized against `detach`, leaking a replacement
connection - is fixed: `open_replacement` consults the slot's shutdown token
before and after `factory.open()` and answers a new `ReplacementOpen::Detached`,
closing the replacement it opened rather than swapping it into an orphaned slot.
The public entry reports `AccountNotAttached`; `restart_account` stops. Pinned by
`a_reopen_racing_detach_closes_its_replacement_instead_of_leaking_it`;
`reference/sync.md` updated.)

(Finding 7 - inline recovery sleeps stalling the single push reconciler and
ignoring cancellation - is fixed: the reconciler's Retry/Reconcile arms record
their deadline in the shared throttle bucket and skip that scope, continuing the
sweep; the poll loop's Retry and Reconcile delays select on scope cancellation
and shutdown; and both engine backoffs go through the new
`sleep_unless_shutdown`. Pinned by
`a_throttled_scope_does_not_freeze_the_account_wide_sweep`,
`a_cancelled_scope_does_not_wait_out_its_retry_hint` and
`a_recovery_backoff_is_cut_short_by_shutdown`; `reference/sync.md` updated.)

(Finding 8 - per-item retryable failures resubmitting with no delay - is fixed:
`classify_item_outcome` now reports the retry advice of the items it queued,
folded to the longest delay, and the campaign uses it when the stream carried no
termination-level advice. Pinned by `a_per_item_retry_delays_the_resubmission`;
`reference/sync.md` updated.)

## Latent / design-level observations

### 9. Broadcast ring pressure during cold start is structural

(Ruled as item 8 in `notes/todo.md`: WILL HAPPEN, sequenced after the other
structural items. The consumer surface stays one stream; backfill pages get a
bounded per-account lane with the existing acks as the permit signal. The
engine split it was sequenced behind has landed; it runs once the DAV
watermark cursor lands, alone, with a cold review.)

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

(DONE 2026-09-04 as ruling 1 in `notes/todo.md`: `engine/{mod,context,attach,
backfill,ack,reattach,bulk,passthrough,tests}.rs`, `SlotContext` bundling the
worker wiring, the two orchestrator arms collapsed behind
`BackfillPlan::resume -> ScopeResume`.)

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

(Resolved. The `Deleted` half went with finding 2, and the mutation section's
read-back derivation claim is now true on every exit path: finding 5 was fixed
in the direction that preserves it, and the reference says so explicitly.)

## Not found / verified sound

The heavily documented invariants (publication identity, supersession folding,
provisional reattach rows, barrier taint, drive leases, generation fencing,
throttle key resolution, envelope migration) all check out against their code
and are pinned by unusually sharp tests; no soundness hole found in
`PendingCoverage`, the ack writer's transition ordering, or the scheduler's
admission dispatcher.
