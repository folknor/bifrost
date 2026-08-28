# Orchestration carry-forward

State the bug-hunt loop needs but no agent in it can see. Every brief carries the
relevant slice forward, because each agent arrives with only its own round.

This is not a history. When a piece of machinery is superseded, replace the entry
rather than appending to it.

## Standing loop conventions

- A change may alter or replace a published API. It may never remove one - no
  public type, variant, field, method or function. Three cold reviewers have
  objected to shape changes on source-break grounds; the owner overruled all
  three. Shape changes are fine, removals are not.
- Closure by verification is not closure. "Not a current defect" is weaker than
  "fixed": the invariant must be enforced by a type, a constructor or a guard.
- An assert on account-authored data is the wrong tool. A recoverable resync
  beats a process abort.
- Refusing a finding with a reason is a good outcome. Two have been rejected on
  the merits so far, both recorded below.

## From the `bugs-graph.md` arc (closed, dac58d3..8e607fd)

Scope was `crates/graph/`. One round plus a close pass. Every finding in the
document was worked in a single round; the cold review then found three defects
in that round's own new code, all fixed structurally rather than by a corrected
conditional.

Machinery later work may build on and must not break:

- **`PageWalk` now bounds the two delta walks**, which are the crate's primary
  sync loops and previously had no `nextLink` bound at all while
  `reference/graph.md` claimed the guard was universal. The reference now carries
  an ENUMERATION, verified one-for-one against the eleven production
  `PageWalk::new` sites; every remaining `next_link` use is a single-page-per-call
  cursor API with no traversal loop. A refusal projects as
  `InventoryEvent::Terminated` / `SyncEvent::Terminated` with
  `Protocol(ParseFailed)`. If a new traversal loop appears, it must enter the walk
  and the enumeration must be updated, or the reference goes false again.
- **The public-folder cap is real by construction.** `capped_baseline` returns
  ids, version map and degraded flag as ONE `Baseline`, so every per-item vector
  empties together. The predecessor `apply_cap` computed the new `degraded` while
  the caller cleared `live_versions` off the old one, so the first over-cap scan
  persisted an unbounded `(id, change_key)` map behind a cursor advertised as
  capped. The under-cap arm filters `live_versions` to the surviving id set, so no
  orphan versions persist.
- **Public-folder change detection exists at all now.** Fingerprints share the
  canonical read/flag/category hashing with REST inventory through one shared
  function, and full scans compare persisted change keys to emit `Updated` for
  in-place edits. Before this, flag, category and read changes in a public folder
  were permanently invisible - the incremental poll is a `DateTimeReceived`
  restriction and the throttled full scan diffed only deletions.
- **`resolve_batch_responses` is the single validated, first-response-only
  projection**, consumed by BOTH the etag cache update and the outcome
  reconciliation. Two paths reading the same wire data under different rules was
  the defect: a `412` followed by a duplicate `200` installed an etag the accepted
  outcome had rejected. The uncertain-lane accounting for unanswered ids survives
  the reshape.
- **Bulk mutations maintain the etag cache they consume.** A 2xx non-destroy
  subresponse overwrites the entry from the subresponse etag and a 412 evicts, so
  the retry actually refreshes. Previously the cache kept the pre-mutation
  `changeKey` after every successful bulk write, and the `AfterStateRefresh` retry
  re-read that same stale value - a 412 livelock. `bulk_move` was worse: Graph
  mints a new message id, so the old key named a message that no longer existed.
- **Webhook renewal stores the expiry Graph actually GRANTED**, deserialized from
  the PATCH response like `create_subscription` already did. A locally computed
  expiry silently outlives a server-clamped grant, and the subscription then dies
  with the renewal worker seeing nothing due.
- **`close()` is best-effort per subscription.** One failed DELETE no longer
  abandons the remaining `server_id`s; nothing retries after close, so a `?` there
  left live webhooks POSTing to the consumer's receiver for up to 24h.
- **The EWS reconnect backoff resets ONLY on a completed long-poll read**
  (`ReconnectBackoff::record_healthy_read`), never on a successful Subscribe. This
  is load-bearing: a broken proxy or idle-timeout appliance produces an endless
  subscribe-then-die loop in which every Subscribe succeeds, so a Subscribe-keyed
  reset restores exactly the hot spin the backoff exists to stop.
- Contact, directory and event searches use bounded walks and resume INSIDE an
  over-delivered page. The three hand-copied loops previously truncated to `limit`
  and set the cursor to the NEXT page, dropping every match past the limit on the
  final page read.

Accepted residuals:

- A server that repeatedly answers EWS long-polls with an explicit resubscribe
  directive exits via `StreamLoopExit::Resubscribe` with no backoff. Judged
  defensible: each cycle is a full server-directed round trip rather than a
  client-side hot spin, and honouring an explicit server instruction promptly is
  reasonable. Flagged rather than changed.
- `pim.rs` (4,795 lines) and `push.rs` (2,600) are a maintenance smell, low
  confidence as defects. No round has judged the churn worth it. Still recorded in
  `notes/bugs-graph.md`.

Carried into the `bugs-google.md` arc, from this document's out-of-scope section:
`bifrost-google`'s `calendars_list` is cited in `paging.rs` as having learned the
paging lesson independently. **Check whether Google's delta/history walks got the
guard or only its list walks** - the miss in graph was exactly that split, and it
is the highest-value thing this arc can hand the next one.

Testing traps this arc recorded, both caught only because the round after the
fix pass went back and ablated:

- **An assertion true of both the correct and the buggy value does not bite.**
  The webhook renewal test asserted only "not the stale expiry", which the
  locally computed expiry it was written to exclude also satisfied. Pin the
  exact expected value when the defect is "stored the wrong one of two
  plausible values".
- **A codec round-trip is not a behaviour test.** The search-resume test
  encoded and decoded the new page cursor without ever driving a search
  against a page that over-delivers, so it passed with the resume logic
  absent. The replacement drives `contacts::search` against a scripted page
  and asserts the second call re-reads the SAME page URL.

## From the `bugs-sync.md` arc (closed, 776471d..b29ba6d plus the final close pass)

Scope was `crates/sync/`. Five rounds, a mid-arc close pass over rounds 1-3,
and a final close pass over rounds 4-5 and the mid-arc pass's own commits.
`notes/bugs-sync.md` has no open findings, only closure notes, refusals and two
carried out-of-scope `bifrost-types` observations. The final pass re-ablated
the round-5 admission tests mechanically (a queueless direct-semaphore admit
and a priority-blind single lane both fail them), audited the dispatcher for
deadlock, lost wakeups and shutdown, verified admission-before-lease ordering
at every call site, and found no code defect; its only fixes were doc drift
(`reference/sync.md`'s hydration section still claimed no production path
passes through the scheduler, contradicting the round-5 scheduler section).
Rounds 1-3 share one subject: who owns
durable state, and what a durable boundary can honestly claim. Round 4 is
teardown, ordering and lifetimes - leases, sleeps, channel depth, detach
ordering.

Machinery later work may build on and must not break:

- **One `Publications` ledger per account** (`cursor/coverage.rs`,
  `PendingCoverage`, alias `Publications`) owns checkpoint registration,
  coverage claims, boundary gating, supersession, lag abandonment, retirement
  and claim consumption, under ONE lock. The LANE is both the supersession key
  and the acknowledgement key: one changes stream per scope, one backfill
  PARTITION per scope, one repair lane. Registration is a single atomic
  transition; acknowledgement idempotency is a per-lane watermark of the
  highest PERSISTED publication, moved only AFTER the store write lands.
  Supersession FOLDS the superseded claim into the survivor; lag abandonment
  carries DEBT ONLY forward, never what a lost batch proved clean. Boundary
  release and retirement identify the PUBLICATION, never the checkpoint value -
  equal values are routinely different publications.
- **`PublicationId` is `(u64, Arc<PublicationReceipt>)`** - it lost `Copy`, by
  design, and the receipt carries the exact checkpoint lane and coverage claim
  so an acknowledgement can replay after the issuing writer restarts. An id's
  high 32 bits are a per-`PendingCoverage`-instance segment, so a stale
  pre-reattach id can never outrank a current one in the durable-lane ordering.
- **One writer task per account owns every durable mutation**
  (`WriterRequest` in `multiplexer/changes.rs`, served by `ack_writer` in
  `engine.rs`). Recovery code only ever holds a `WriterHandle`; reattach
  inserts are tracked PROVISIONAL until commit/abort, and an acknowledgement
  discharges the provisional mark. The writer holds the authoritative in-memory
  `DebtLedger`, loaded once at attach; `CheckpointStore::apply_transition`
  writes checkpoint plus ledger as one operation. The `WriterHandle` names
  reset intent per caller (`reset_scope_for_restart` / `_for_disable` /
  `_for_schema_recovery`); only schema recovery deletes backfill state -
  routine `RestartScope` deleting the completion marker was a round-2
  regression against a documented contract, do not reintroduce it.
- **Scope reset atomicity**: `invalidate_scope` retires the scope's
  publications, extracts their degraded debt, and raises a per-scope
  acknowledgement FENCE (exclusive bound against the mint counter) as one
  operation under the ledger lock, BEFORE the first store await - and runs
  AGAIN after the deletes land. There is no unfencing step; post-reset ids sit
  above the fence. The fence is checked before the live-claim lookup in
  `claim_in`, so a publication registered during the deletes is refused too.
- **`DurableCheckpointSet`** (bifrost-types) replaced the single
  `Option<Checkpoint>` for `pause` / `checkpoint_now`: one entry per change
  scope and per (scope, partition) backfill lane, normalized in `new` so a
  duplicate lane is unrepresentable, equality symmetric and order-insensitive,
  and an older ack cannot move a lane backwards (`announce_durable` compares
  publication ids).
- **`InventoryWalk`** (`inventory_walk.rs`) is the shared barrier/resume state
  both inventory front ends hold - the DIVERGENCE of the two walks was A2.
  `record_barriers` returns the writer's inner result, and BOTH front ends
  refuse to announce a barrier the store refused: the backfill partition fails
  (scope stays Pending, re-walks), the fusion walk returns the error. A
  DEPARTED writer (detach/shutdown) is deliberately non-fatal. The fusion half
  of this rule was the close pass's fix - round 3 had it on the runner only,
  the same divergence shape one call further up.
- **`ScopeWalkDriver`** (`backfill/scope_walk.rs`) owns partition sequencing
  for both plan shapes; a barrier stops the SCOPE - a stopped walk hands out no
  further partition, so there is no flag for an orchestrator loop to forget.
  `BackfillPartitionOutcome` carries a `#[must_use]` `ScopeWalkStep` and its
  `Default` is the SAFE answer (stopped, not complete). The stop is enforced
  twice (fold sets `exhausted`; `next_partition` also checks `walk.stopped()`) -
  deliberate redundancy, both ablated.
- **Sentinel eligibility is decided by the writer**, not the runner: the
  completion sentinel's ack runs `DebtLedger::completion_permitted` against the
  ledger as it stands and silently withholds the marker (persisting the ledger
  only) when the scope owes anything unwaived. The runner's `complete` flag is
  advisory.
- **The `DebtLedger`** discharges by proof UNION (interval union over
  time/uid/page coordinates, generation-gated, `domains_may_join` guards
  UIDVALIDITY and snapshot identity) and retains only proofs still load-bearing
  for an open obligation or barrier. Barriers are incidents, never ledger debt;
  re-recording one is idempotent and preserves operator policy. No local
  counter may produce `Waived` or `Discharged`; an expired budget is
  `OperatorBlocked`. Repair budgets accrue at the lineage ROOT.

Machinery round 4 added - the lease contract above all, which round 5 must not
undo:

- **`CursorRegistry::with_drive` is the ONLY way a change drive is entered.**
  It claims the per-scope lease, snapshots cursor and registry generation
  under it, runs exactly one drive, and RELEASES before recovery handoff,
  retry/reconcile sleeps, `reopen_tx` backpressure and the poll cadence sleep.
  Both call sites had previously over-extended the lease across a whole poll
  iteration, so a push invalidation waited out up to `poll_max` (30 min) of
  idle sleep - push was strictly no better than polling. **Do not re-extend
  it**, and do not reintroduce a bare `claim_drive` in a drive path
  (`claim_drive` stays published and working, per the no-removal rule).
  The contract the narrowing buys is narrow: **only the drive is serialized.**
  Anything that reads a drive's EFFECT must read it inside the callback,
  because the other producer can start on that scope the instant it returns.
  The poll loop measures its own `advanced` inside the callback for exactly
  this reason; measuring it after would credit a push reconcile's progress to
  the poll and pin the cadence at `poll_min`. Durable state was never
  lease-protected: `publish_if_generation` (the generation fence) and
  `with_drive` returning `None` for a deleted scope do that, unchanged.
- **A8 is built.** `DebtLedger::cross_waived_barriers` plus
  `inventory_walk::cross_waived_barriers` (used by BOTH front ends, keeping
  the A2 anti-divergence rule). The WRITER re-checks the waiver at the barrier
  hit against its live ledger - never a walk-start snapshot - converts an
  all-waived report from barrier incidents into unresolved WAIVED ledger
  entries, persists, and only then answers `true`. All-or-nothing per report:
  a checkpoint crosses the whole domain, so one unwaived or unknown key keeps
  the walk stopped. A cumulative terminal report repeating an already-crossed
  barrier stays authorized rather than rebuilding the wall one event later.
  Crossing is accepted loss, never proof.
- **An `OperatorBlocked` barrier parks the backfill rescan** via
  `WriterRequest::ScopeBarrierBlocked`, checked before any wire work - and the
  park is recorded through `BackfillScan::record_attempt`. That second half is
  load-bearing: the rescan tick is one second and the query lands on the single
  account writer that owns every durable mutation, so without recording it the
  blocked scope's retry deadline stays in the past and it re-queries the writer
  every second for as long as the block stands.
- **Push reconciliation routes `Err(Error::Account)` through recovery**, the
  same normalization `handle_drive_outcome` does. E6 (one failed hinted scope
  must not abandon its siblings) and recovery routing must BOTH hold: the
  round's first implementation logged every drive error and thereby turned an
  incompatible cursor envelope - `Engine(SchemaIncompatible)`, which must reset
  state - into a swallowed one. The sweep now ends only on an ACCOUNT-WIDE
  directive; `directive_target_scope` is the blast radius (`Some(scope)` =
  keep sweeping, `None` = stop).
- **`take_ack_writer` selects the ack writer by `WorkerRole::AckWriter`.**
  Detach waits on stream workers first (they hold the writer's sender clones)
  and the writer last; the predecessor took `drained[0]`, coupling teardown
  order to spawn order with nothing announcing it.
- **`refused_activity_outcome`** names what a drive refused by
  `begin_activity` reports: `Stop` -> `Stopped` (which is what sets
  `DriveRecovery::exit`), everything else -> `Paused`.

Machinery round 5 added - the admission contract above all:

- **Admission is a SEPARATE path from `submit`/`pull`.** `Scheduler::admit`
  queues into its own four priority lanes; ONE dispatcher task per `Scheduler`
  (spawned lazily on the first `admit`, stopped when the last handle drops)
  grants a `BudgetPermit` only when the budget can actually be granted. The
  first implementation ran admission THROUGH `submit`/`pull` and had every
  admit caller drive the scheduler, so a request dequeued and then parked on
  the semaphore - which relocated the unboundedness rather than removing it
  (a parked request no longer counts against `lane_capacity`) and destroyed
  preemption (the semaphore's order is arrival order). **Nothing may park on
  the `BudgetGate` semaphore except the dispatcher.** The dispatcher races one
  acquisition per distinct `(account, kind)` class, never only the head:
  head-only blocks the entire engine behind one account whose per-account
  sub-pool is exhausted. It re-plans on a strictly better lane or a new class,
  and deliberately NOT on an arrival that is neither, because restarting the
  in-flight acquisitions on every submission starves them.
- **`Scheduler::pull` kept its synchronous non-blocking signature**;
  `pull_next().await` is the waiting form and `try_pull` an alias. Round 5's
  first pass made `pull` async, and the cold review's objection was upheld:
  losing the non-blocking empty check REMOVES a capability rather than
  reshaping one. Five earlier shape objections in this arc were overruled; this
  is the line between the two. `DriveGeneration` carries a `new` constructor
  for the same reason - `#[non_exhaustive]` on a type a published function
  takes would make it unconstructible, which is removal in substance.
- **Every wire path is admitted**: poll and push change drives, backfill
  partitions, the separately-admitted operator-barrier writer query, mutation
  attempts, the mutation READ-BACK guard, and deferred inventory fusion. The
  last two were the paths round 5's first pass missed while the reference had
  already been edited to claim read-back was covered. Admission is acquired
  OUTSIDE `with_drive` and released before recovery handoff and cadence sleeps;
  round 4's narrow lease extent is intact.
- **A refused admission is transient.** `admit` errors when its lane is full or
  the scheduler is stopping. The poll loop backs off one cadence step (the
  first pass RETURNED, retiring the scope's polling forever), and the push
  sweep skips only that scope, per E6's blast-radius rule.
- **The publication fence is a PAIR, `DriveGeneration { registry, scope }`.**
  `registry` moves only on wholesale topology replacement; `scope` is bumped by
  `delete`. An account-wide bump on delete fenced every SIBLING scope's
  in-flight drive, which then discarded valid results and re-walked from its
  old cursor. `publish_if_drive_generation` is the form every drive path uses;
  `publish_if_generation` stays published for account-wide-only callers.
- **`delete` KEEPS the scope's drive lease entry.** D2 asked for the lease to be
  pruned on delete, and pruning it is what broke exclusive drive: a
  re-established scope minted a DIFFERENT mutex and ran its protocol stream
  concurrently with the still-running old drive. The generation fence stops the
  stale publication, never the concurrent wire work. Growth is bounded by
  pruning entries no drive holds, which is safe because a held lease keeps a
  second `Arc` alive and `claim_drive` clones it under the same write lock.

Reasoned rejections and refutations - do not silently relitigate:

- **B2 was REFUSED in round 4, after B1 landed.** Its force was compounding:
  poll tasks sent on the depth-16 `reopen_tx` *while holding the drive lease*.
  With `with_drive`, every `reopen_tx.send().await` in both the poll loop and
  the reconciler is outside the lease, and what remains is a bounded channel
  doing its job - backpressure on the ORIGINATING poll task only. Bounded
  channel + serial listener is not itself the defect.

- **C2 was REFUTED with evidence, not fixed**: `drive_changes_stream` attaches
  `publication` whenever control and checkpoint are both present, both
  production callers pass `Some(control)`, and `emit_backfill_complete`
  publishes unconditionally. Pinned by
  `a_published_change_checkpoint_always_carries_its_publication_id` (ablated by
  the close pass; it bites). Do not re-open without new evidence.
- `PendingCoverage::claim` (kind-agnostic) and `SyncControl::record_checkpoint`
  (value-identified) stay published and stay unreachable from every engine
  path; kept under the no-removal rule, documented as foot-guns. Every engine
  path uses the lane-checked, publication-identified forms.
- F1 is half-landed BY DESIGN: `CheckpointStore` stays published
  (consumer-implemented; ownership is enforced by which internal type can reach
  the `Arc`), and `BackfillCheckpointTarget::direct` keeps the old
  read-modify-write semantics for external constructors, documenting the race.
  A compare-and-swap on `CheckpointStore` is an owner-level trait-change
  proposal, deliberately not taken.
- Lag carry-forward is DEBT ONLY - folding a `Complete` report forward would
  discharge obligations against a batch the ring destroyed.
- A departed writer channel is non-fatal while a store error is fatal, so
  detach/shutdown does not fail partitions.

Accepted residuals:

- `PublicationId` lost `Copy` (receipt behind an `Arc`); real ergonomic cost,
  judged unavoidable.
- F3 is only partially resolved: checkpoint MINTING (fusion forwards the
  account's, backfill mints positional `page:F:T`) and terminal-`Done`
  handling are still two implementations; the shared `InventoryWalk` covers
  only the safety-critical barrier/resume half. Re-divergence risk, filed
  under F3 in `notes/bugs-sync.md`.
- Receipt-based ack replay is scoped to one `PendingCoverage` instance per
  attachment; across a detach/re-attach a late ack of a prior incarnation's
  publication replays from its receipt and can re-persist a stale row over a
  freshly re-established one. Same class as the documented vanished-scope
  late-ack leak: re-delivery, never loss. Noted, not fixed.

Accepted residuals round 5 added:

- `ThrottleKey::Account` deadlines deliberately survive detach until expiry:
  they describe the stable account identity, not the connection incarnation, so
  detach-plus-reattach cannot bypass a provider wait. Bounded by expiry pruning.
- A mailbox throttle whose error names no mailbox stays operation-local rather
  than degrading to the account key: degrading it WIDENS a per-mailbox 429 into
  an account-wide stall. Broader (provider, tenant) scopes still degrade toward
  the account key, which is a subset of what they describe.
- `ThrottleScope::Tenant` remains unenforceable across siblings; filed in
  `notes/todo.md` as a `bifrost-types` identity-channel item.

Test seam rounds 4 and 5 should build on:

- **`crates/sync/tests/common/mod.rs` now holds a reusable `Account` double**:
  `StubAccount` (closure hooks for discovery, establishment, partitioning,
  partitioned and whole-scope inventory, changes; `walked` / `established` /
  `closed` recorders; everything else Unsupported or empty) plus
  `StubFactory::queue`. Built by the close pass because round 3 could not
  write an end-to-end orchestrator test without it.
  `tests/backfill_barrier_scope.rs` is the model consumer: it drives attach ->
  orchestrator -> runner -> broadcast against the stub and pins that the
  partition past a barrier is never REQUESTED, the barrier page's checkpoint is
  stripped, no sentinel is broadcast, and nothing becomes durable unacked
  (ablated: reverting the driver stop fails it). Keep the double honest - hooks
  must return only shapes production protocol crates can produce
  (boundary-valid batches, envelope-valid cursors).
  `attach_schema_recovery.rs` still carries its own older `HealAccount` double;
  migrating it onto the seam is optional cleanup, not owed.
- **`tests/push_poll_latency.rs` (round 4) is the second model consumer** and
  the pattern round 5 should copy for anything push- or recovery-shaped: a
  one-hour `poll_initial`/`poll_min`/`poll_max` config, so any drive observed
  after attach's first pass provably came from the push path and not the poll
  timer. Its recovery assertions key on `StubAccount::established`: schema
  recovery re-establishes every scope and `RestartScope` re-establishes one, so
  a fresh `establish_initial_cursor` call is the observable fingerprint of a
  failure having REACHED recovery. That is what distinguishes "the scope was
  driven" from "the scope was recovered"; the round's own first sibling test
  counted drive attempts only and passed against the bug.

- **`tests/scheduler_admission.rs` (round 5) is the third model consumer** and
  the pattern for anything budget-shaped. `ConcurrencyBudget { per_account: 2,
  mutation_share 1/2 }` gives an account exactly ONE sync and ONE mutation
  permit, which is what makes contention observable at all. The overtake test
  queues the Background requests FIRST and asserts the resulting ORDER, because
  a symmetric priority test cannot distinguish lane selection from the
  semaphore's arrival order - the uniform-inputs trap in its concurrency form.
  `SyncEngine::scheduler()` hands a test the live handle, but a permit must be
  taken AFTER `attach`: `BudgetGate::register` installs fresh per-account
  semaphores, so one taken earlier is against an orphan and gates nothing.

Testing traps this arc recorded:

- `brokkr test -p bifrost-sync <NAME>` - the package is `bifrost-sync`, not
  `sync` (`-p sync` fails as outside the workspace). The filter is a substring
  match and a filter matching nothing reports PASS - check the run count.
- The orchestrator parks on `wait_for_real_subscriber`, so an end-to-end
  cold-start test must subscribe via `account_changes_stream` after attach or
  it hangs; conversely nothing is raced away before the subscribe.
- A barrier-stopped or failed scope re-walks after `BACKFILL_RETRY_INITIAL`
  (5s): assertions about "never walked again" are safe immediately after the
  triggering event but not after multi-second waits, and paused-time
  auto-advance can fast-forward through the backoff and make walk counts
  flaky.
- The engine-side pin for uniform-input traps: the FIFO/no-queue lesson from
  earlier arcs held here too - the concurrency-shaped ledger tests
  (`concurrent_registration_leaves_one_entry_per_lane`) use genuinely
  concurrent registrars, keep that shape.

## From the `bugs-dav.md` arc (closed, a65acb6..dac58d3)

Scope was `crates/caldav/` and `crates/carddav/`, plus the IMAP composition seam.
Two rounds and a close pass. The arc's own signature: the recurring
fix-opens-a-hole-one-layer-up shape fired in all three stages, and each time the
new hole was in a CONSUMER of the changed thing rather than in the change.

Machinery later work may build on and must not break:

- **Well-known discovery is origin-rooted, through one helper.**
  `bifrost_net::url::well_known_url` parses the base, clears query and fragment,
  and replaces the path; `parent_collection_url` is the shared parent extraction
  that replaced CardDAV's `rfind`. Both crates use both. The old
  `format!("{}/.well-known/...", base)` was live in CardDAV and broke opens
  against a path-bearing base - a server answering 401/403 for the bogus path
  failed the open without the configured base ever being tried.
- **Per-collection cursor scopes.** Both crates mint one `CursorScope::Folder`
  per collection, so `CoverageDomain::full(Folder)` is a TRUE claim for a
  whole-collection walk. The legacy type-scope lane survives with a
  `ProviderRegion` coordinate that only self-covers, so it can no longer
  discharge debt it did not earn. All three lanes are per-scope; the changes lane
  resolves its collection from the cursor's own snapshot. The `unsynced_*_urls` /
  `open_skipped_scopes` apparatus is gone because the skip lane is now empty by
  construction - `calendars_beyond_the_first_are_reported_as_skipped_scopes` went
  with it, correctly.
- **`DavScopeIndex` is ownership by construction, not a membership check.**
  Built once at open in `crates/imap/src/account/factory.rs` AFTER the folder
  registry, so it is disjoint from the real mailbox set. `FolderId` carries no
  protocol namespace and IMAP mailbox names are arbitrary strings, so raw string
  membership let a DAV href steal a real IMAP mailbox and resolved CalDAV/CardDAV
  collisions silently in favour of contacts. An href equal to a known mailbox, or
  claimed by both sub-accounts, is never indexed; both exclusions warn on the
  degraded-DAV lane. Namespacing `FolderId` was considered and rejected: it would
  require rewriting the scope on delegation and back out of the minted
  `ChangeCursor`, and every rewrite point is the hole-opening shape again.
- **Every `Folder`-scope consumer must consult the index, not just the sync
  lanes.** This is the arc's sharpest lesson. Round 2 routed the four sync lanes
  correctly and missed `push_subscribe`, a fifth consumer: an href is a
  syntactically valid mailbox name, so a DAV collection was admitted as an IDLE
  mailbox watch - reported in the succeeded lane that bifrost-sync trusts, burning
  one of four IDLE budget slots, and handing a worker a SELECT that can never
  succeed. No data loss (polling is never suppressed on push coverage), but a
  misreport, a burned slot and a doomed SELECT. A new consumer of `Folder` scopes
  must be checked against the index.
- **Empty means empty.** When collection discovery returns empty, the home URL is
  NOT substituted as a scope. The parsers already return the home when it
  genuinely is a collection, so an empty result means an empty backend, and
  fabricating a scope there advertises a folder that 404s at cursor
  establishment. Pinned by the `an_empty_home_lists_no_*_rather_than_a_phantom`
  tests, which predate the arc and which round 2 contradicted in the sync path
  while leaving intact.
- Multiget fan-out is bounded at `MULTIGET_LEG_CONCURRENCY = 4` via ORDERED
  `buffered`, not `buffer_unordered`: two existing chunked-multiget tests depend
  on the merged report and surviving degraded error being deterministic.

Accepted residuals:

- A genuine empty-out (a user deleting every event/contact) is suppressed along
  with the transient empty-207, because the wire looks identical. Both reference
  docs now say plainly it is not reported on that poll OR a later one, with the
  CardDAV ctag short-circuit caveat.
- A scope excluded from `DavScopeIndex` is still ADVERTISED by discovery, since
  sub-account scopes pass through `merge_scope_streams` verbatim. Mailbox
  collisions resolve to the same scope value as the real mailbox; contested ones
  fail loudly at establishment. Judged honest and sufficient, warned at open.
- `default_calendar_url` / `default_addressbook_url` home fallback for PIM calls
  is pre-existing published behaviour, untouched.

Testing traps this arc recorded:

- **The pre-existing well-known tests used a path-less base URL**, where the
  buggy and correct constructions produce byte-identical output. A textbook
  uniform-inputs failure: the tests existed, ran, and could not fail. Any test of
  URL construction needs a path-bearing base, and a query or fragment is worth
  pinning too.
- A `pub(crate)` item held alive by `allow(dead_code)` is ordinary cleanup, NOT a
  published-API removal. An earlier round refused to delete two such items on the
  no-removal rule; that was an error of scope, and applying the rule to
  crate-private items launders ordinary cleanup into a prohibition.

## From the `bugs-imap-sasl.md` arc (closed, 8c365f0..HEAD)

Scope was `crates/imap/` plus `crates/sasl/`: the auth-dispatch and mutation
cluster, then the change-strategy paging and baseline consolidation.

Machinery later work may build on and must not break:

- **STORE tagged-NO strictness is compiler-enforced.** `StoreConsumer::new`
  takes the command's own `unchanged_since` and has no argument-less
  constructor, so how a tagged `NO` is treated is a property of the command
  sent, not a call-site choice. Conditional STOREs preserve `NO [MODIFIED ...]`
  in `StoreResult::status`; unconditional STOREs keep erroring, which is what
  stops `delete_messages` from EXPUNGEing after a refused `+FLAGS \Deleted`.
  `StoreWireOutcome::from_store_result` takes the whole result so no caller can
  keep the code and drop the status.
- **Mutation error lanes follow transmission evidence**, not the loop that
  caught the error: `InFlight` evidence is `Uncertain`, `Unsent`/`Acknowledged`
  is `Failed`. Applies to grouped STORE, MOVE, and EXPUNGE paths, and to the
  second half of a two-sided `FlagOp::Patch`.
- **SCRAM finalize checks the tagged status before the state machine**, so a
  bare tagged `NO` (legal under RFC 5802, no server-final) is an auth error
  reaching `ReauthorizationRequired`; a tagged `OK` still requires server-final
  verification.
- **Baselines are exact everywhere.** A QRESYNC/CONDSTORE cursor with
  `known_uids_complete == false` terminates with `CursorInvalid` deriving
  `RestartScope` before any wire I/O - live `UID SEARCH ALL` seeding was
  removed because it cannot reconstruct historical consumer state and produced
  `Updated` for objects the consumer never had.
- **Three-way Basic classification.** Every server-authored `changed_messages`
  UID on a Basic run goes through `basic_updated_change`: baseline UID is
  `Updated`, live-only UID is an arrival left to the diff's `Added` lane, a UID
  in neither set invalidates the cursor. The live-only lane is the ordinary
  case, not an edge - the `[NOMODSEQ]` downgrade hands Basic a QRESYNC SELECT's
  FETCH data, which includes arrivals (RFC 7162 3.2.5).
- **The UIDNEXT/EXISTS fast path.** Basic skips `UID SEARCH ALL` when SELECT
  returns the cursor's unchanged nonzero UIDNEXT and EXISTS equals the baseline
  count (an arrival strictly increases UIDNEXT; equal EXISTS then excludes
  expunges). Every way either signal is absent, zero, or stale falls back to
  the SEARCH, and the neither-set invalidation above is what makes the skip
  safe, since there the live set is the baseline.
- **The paging guarantee is by convention, not construction.** All three
  strategies emit at most `BATCH_ITEMS` per page, but through separate loops:
  `flush_page` owns the boundary for CONDSTORE and both Basic entries, QRESYNC
  flushes inline (it must also track `flushed_qresync_changes`, because its
  mid-stream CONDSTORE downgrade is legal only while no page has escaped -
  SELECT-side flushes count). A new emission loop must page and must respect
  that downgrade rule; nothing forces it to.
- The CONDSTORE change loop drives its bounded FETCH receiver and command
  future together like inventory and QRESYNC: the command future is
  authoritative on `recv() -> None`, so a truncated FETCH cannot checkpoint.
- `CompactUidSet::diff` is range-native subtraction in u64 arithmetic (the
  `u32::MAX + 1` overflow is real), pinned by a differential test against
  expand-and-compare over 400 dense generated pairs.
- bifrost-sasl: CRAM-MD5 raw digest is zeroized; `xor_bytes` asserts equal
  widths - acceptable because both operands are same-hash HMAC outputs, never
  server-influenced lengths.

Reasoned rejections - do not silently relitigate:

- **`StoreResult` visibility**: the raw connection surface is crate-private, so
  no external consumer can reach `uid_store` or need to name `StoreResult`.
- **The `Strategy`-trait rewrite of `changes.rs`**: QRESYNC owns a mid-stream
  downgrade legal only before any page escapes (with connection discard),
  CONDSTORE has a fallible bounded stream but no VANISHED lane, Basic has no
  change-source stream at all; a shared runner would own strategy-specific wire
  policy. Consolidation happened at the narrower `flush_page` seam instead.
- No consumer-side TODO for the unknown-`Updated` guarantee: bifrost-sync only
  ever produces `ObjectChange` (`Created`) and never reconciles membership on
  it, so the producer-side guard closes the hazard.

Accepted residuals:

- `flush_page`/`finish_changes` can end a run on `[Page, Page]` with no `Final`
  batch when the residual is empty; `SyncEvent::Done(checkpoint)` is the
  terminator, so nothing downstream may key on `Final`.
- On a non-conformant server reporting the same UID in both VANISHED and FETCH,
  a `Removed` that already escaped in a flushed page is followed by an `Added`
  rather than rewritten into one `Updated` - a coherent remove/re-add for the
  consumer, deliberately not suppressed.

Testing traps this crate recorded:

- The scripted driver-pair tests answer only the commands their script expects;
  a code path that issues an extra command (e.g. an un-skipped `UID SEARCH`)
  hangs until the command timeout rather than failing crisply - keep scripts
  and wire expectations exactly in step.
- The paging tests need 129 changes (`BATCH_ITEMS` is 128) to observe a page
  boundary at all; a smaller fixture passes against a build with no paging.

## From the `bugs-smtp.md` arc (closed, 60d834c..8c365f0)

This crate has both an async and a blocking transport half, both published and
both load-bearing, and its central lesson is about them. `reference/smtp.md`
carries a sentence that the halves are held in step deliberately, and the arc
kept it true by recording exactly one asymmetry rather than letting a second
appear. **Whatever changes in one half changes in the other**; the arc's own
defects were repeatedly a fix landing in one half only.

Machinery later work may build on and must not break:

- **STARTTLS is defended in two layers in both halves**: the generic
  surplus-bytes check after every state-managed reply parse, and an explicit
  buffer gate before `upgrade_tls`. Layer 2 is defence in depth and is currently
  shadowed by layer 1 on every reachable path - do not collapse them, and note
  the first attempt's dedicated guard was unreachable while its test appeared to
  pass, pinning a different fix entirely.
- A surplus or unsolicited reply breaks the connection immediately, and a
  connection carrying reply drift cannot be recycled. Both properties are pinned:
  the stale reply surfaces loudly, and `has_broken()` blocks the recycle.
- **The pool.** `PoolConfig::max_size` bounds live connections in both halves.
  Async shutdown closes the admission semaphore and notifies waiters; async
  checkout re-reads pool state after winning admission and before dialing. Async
  recycle performs no I/O and has no await point, so there is no recycling future
  to drop before its first poll and no blocking close in a `Drop`. The blocking
  pool notifies `available` **while holding the connections mutex** - a condvar
  notification is not sticky, so notifying outside the lock strands a waiter that
  has just failed its `try_reserve`.
- **The one deliberate asymmetry:** async `abort()` honours the operation
  timeout, blocking `abort()` does not, because it is `Shutdown::Both`, a syscall
  returning immediately, while only async `poll_shutdown` waits for the peer's
  `close_notify`.
- **Pipelined phase decoration is compiler-enforced.** `send_pipelined` is a
  one-line funnel over `send_pipelined_inner`, whose error type `PhasedError` has
  no `From<Error>` impl and no phase-less constructor, so `?` on an undecorated
  SMTP result does not compile inside the driver. This replaced a call-site sweep
  that had missed six negative-reply paths - the ones that actually run when a
  relay rejects a recipient - and it immediately caught a seventh nobody had
  listed. `SmtpError::phase()` is the sole phase authority;
  `SmtpErrorContext` stores neither a phase nor a scope, so no call site can
  attach a disagreeing one.
- **Error scope.** Recipient-command transport failures carry NO `ErrorScope` in
  either half. `ErrorScope`'s id-bearing variants take typed account-surface ids
  and none can name an SMTP envelope address, so `ErrorScope::Account` falsely
  located a single-transaction failure at the account. Correlation runs through
  the batch item id.
- All ten all-recipients-rejected early returns across both halves route through
  one `reset_transaction` per half, keeping the connection on a positively
  acknowledged RSET and aborting otherwise. A rejected `MAIL FROM` on the
  pipelined path deliberately does not reset - it opened no transaction.

Testing traps this crate recorded, worth carrying anywhere:

- A `5.1.1` enhanced status classifies identically for every phase, so tests
  written on it cannot distinguish what they appear to test. Reaching the
  deciding branch took a bare `550` with no enhanced code.
- Two `transcript.invalid` pool tests perform a DNS lookup **only on their
  failure path**, which is the ablation path - so an ablation failure there can
  look like a network flake when it is not.

## From the `bugs-jmap.md` arc (closed, 5407925..60d834c)

Machinery later work may build on and must not break:

- **The inventory walk is anchor-based** over a stable `queryState`; positional
  partitions are refused and not advertised. Every error path is
  yield-Terminated-then-return, so `Done(None)` is reachable from exactly one
  place: an empty anchored page under an unchanged `queryState`. A partial walk
  therefore cannot report complete coverage.
- **A mid-walk `queryState` move, and a cursor-scoped `anchorNotFound`, both
  classify `SyncState(CursorInvalid)` mapping to `RestartScope`** - deliberately
  not a terminal `ContractViolation`, because one delivered message advances
  `queryState`, and ordinary mail arriving during a backfill must not permanently
  kill a scope. The non-cursor `anchorNotFound` arm stays `ContractViolation`.
- **Push.** RFC 8887 `pushState` is captured off the wire, replayed after
  reconnect, and carried across a reconfigure: subscribe and unsubscribe both
  send the live value, and `commit_push_set` retains the position on reconfigure
  while clearing it when the union goes empty. Subscription state commits only
  after successful push configuration: the registry, `enabled` and `push_state`
  guards are held across the single await in one uniform lock order
  (`subscriptions -> enabled -> push_state`) with every commit after it, so a
  cancelled subscribe future mutates nothing. `push_subscribe` reports
  per-position outcomes, so repeated scopes stay distinct.
- **Capability limits** go through a three-state `CallLimit`: `Unadvertised`,
  `Invalid` (a server advertising `maxCallsInRequest: 0`, which RFC 8620
  forbids), and `Advertised(NonZeroUsize)`. Only the third enforces. Absent and
  zero are different states - conflating them bricked account opening once.
  Enforcement lives in the single `Request::call` door.
- Session divergence wakes lifecycle handling immediately via a watch channel
  rather than waiting on the 300s poll.

Disclosed residuals and rulings, deliberately left:

- `reenable_current_push_set` reads `enabled` and `push_state` under separate
  critical sections. The worst case is extra wire frames and a spurious
  invalidation hint - never a missed change, since hints are over-approximate by
  contract - and it self-heals. Closing it means holding a guard across
  `set_push_data_types`, the lock-across-await teardown shape that has opened a
  new hole every time this loop has tried it.
- `refresh_session` stays published but unwired, disclosed in
  `reference/jmap.md`. `terminated_contract_violation` is kept under a reasoned
  `allow(dead_code)`.
- EventSource push remains a documented deferral; it resumes via `Last-Event-ID`
  rather than `pushState`, which is correct for that transport.

## From the `bugs-net.md` arc (closed, a2b5fb4..5407925)

The arc's own signature defect, worth stating first because it fired in six
consecutive rounds and once more in the close pass: **a fix gets wired on the
success path, or the path the finding named, and the error path or the adjacent
path keeps the old behaviour.** A deadline that bounded every wait except the
metering wrapper's; accounting that counted error-path bytes but never charged
them against the cap; a governor that gained a key dimension at registration but
not at selection; an EWS tally written only on success. Every one was caught by
the cold reviewer, never by the fix pass that wrote it.

Machinery later work may build on and must not break:

- **RequestDeadline** is a genuine overall request deadline surviving retries and
  redirects. Its instant is unreachable outside the impl; every wait routes
  through `bound` or `bound_body`. Do not reintroduce raw instant arithmetic.
  `AttemptBudget`, `AuthBudget` and `RedirectBudget` own their own arithmetic,
  and the redirect arm resets by construction.
- **The body-path ordering** is `record_bytes_in`, then `deadline.check_body`,
  then `deadline.bound_body` around `ByteBucket::consume`. Metering happens
  BEFORE the deadline check, deliberately: those bytes came off the wire whether
  or not we may hand them up. Both `wrap_metered` and the error-path drains in
  `read_capped_response_body` follow it, so every byte counted is also charged
  against the per-account cap. A mid-body deadline expiry reports
  `Timeout { Acknowledged }` mapping to `Protocol(PartialResponse)`, always as an
  `Err`, so no prefix is returned as a complete body.
- **Classification.** The header timeout returns `InFlight`, correctly, because
  it can fire after the body was written; `connect_timeout` reaches the client
  builder so `Unsent` comes from real evidence rather than a guess.
- **Auth.** The 401 refresh is single-flight and cancellation-safe: there is no
  await point between the `Refreshing` transition and the driver spawn, so a
  dropped request future strands no waiter and poisons no state.
- **The governor** is keyed on `(host, quota_scope)`.
  `RequestBuilder::quota_scope` names the bucket per request and wins for every
  hop; the account declaration is the default, first-wins with a warning naming
  the override. Every ticket path checks bucket generation against ticket
  generation, and a `RateDebit` captures its own host, scope and generation, so a
  redirect cannot refund the wrong bucket. Registration rejects `burst = 0`.
- **Per-request accounting.** A response carries the bytes actually read for that
  request. `RequestBuilder::count_bytes_into` hands the caller its own
  `RequestByteCounter`, which is the only way to read the number back after an
  `Err` - use it rather than reading a count off a response that error paths
  never produce. Consumers aggregate with a batch-scoped `ByteTally` per crate,
  recorded at that crate's wire funnel and cleared per batch. JMAP reaches the
  count through a defaulted `HttpTransport::api_request_measured`, which is why
  its scripted doubles needed no change.
- **Redirect passthrough** bodies reach the caller through `into_byte_stream` and
  are counted. `ScriptedDispatch` defers its build, snapshot and step pop into
  the async block, so an unpolled dispatch future no longer consumes a step.

Disclosed exclusions, deliberately left:

- OAuth issuer traffic through the caller's own `TokenSource` is neither metered
  nor capped; closing it changes a published contract. `reference/net.md`
  discloses it.
- Long-lived EWS streaming and `StreamingResponse`'s counter stay out of batch
  totals, because a stream's count is only as complete as the caller's draining
  and a partial number must never be published as a total.
- Three sites report `bytes_in: 0` correctly because they perform no request at
  all: Gmail's constant `discover_cursor_scopes`, JMAP's session-derived
  `cursor_scopes`, and the two locally-rejected mutation lanes. Each says so at
  the call site.
- `NetErrorContext` and `FinalResponse` are deliberately not `#[non_exhaustive]`:
  the former is consumer-constructed with no constructor, the latter is
  crate-produced evidence rather than configuration.

## From the `bugs-types.md` arc (closed, 2004c2c..a2b5fb4)

Machinery later work may build on and must not break:

- **ErrorScope** serializes through a single `scope_fields` projection whose
  declared field count comes from the projection itself. A second parallel match
  computing a count WAS the original bug. Its resource fields are typed ids, not
  Strings. There is no `Deserialize`; serialization is one-way into support
  exports.
- **AccountError construction.** `CauseChain` construction is fallible and wired
  to `EmptyChain`, which is unreachable because `try_build` always pushes the
  primary cause first. `into_builder` preserves idempotency and throttle
  overrides. `set_telemetry_token` assigns unconditionally, so a rejected
  replacement clears rather than preserving a stale token; telemetry ids are the
  bounded `TelemetryToken`. `validate_batch_input` takes the operation as a
  required parameter, because centralizing the construction is what made losing
  that context possible.
- **Recovery.** Throttle reconciliation keeps `ThrottleScope` and `retry_hint`
  via `ReconcileReason::ThrottledMidFlight`; reconcile sleep comes from the
  shared `recovery::reconcile_delay`. `RecoveryClass::is_terminal` is an
  exhaustive match, not a negation. `handle_drive_outcome` routes
  `Err(Error::Account)` through `plan_recovery` rather than logging and
  re-polling an unchanged cursor forever.
- **Cursor envelope versioning.** Decode is the SINGLE migration boundary:
  `decode_change_payload` routes through `migrate_change_cursor`, applies the
  fixup chain and stamps `CHANGE_CURSOR_ENVELOPE_VERSION`. The codec is therefore
  the only code in the workspace that ever holds a non-current cursor, and
  `validate_envelope` stays strict equality everywhere else. Bumping
  `ENGINE_VERSION` means adding both a fixup and a byte fixture. Do not widen the
  gate to the migration window - that was considered and rejected, because it
  spreads knowledge of historical layouts across every consumer.
- **Coverage and inventory.** `InventoryCompletion::complete` demands an exact
  `CoverageDomain`; unsupported inventory emits only `Terminated`, never a `Done`
  carrying a full-scope complete claim. Degraded coverage is structurally
  non-empty. UID partitions are half-open with `u64` endpoints. Batch-boundary
  violations classify as `Protocol(ContractViolation)`, which is terminal, and
  publish `Terminated` to subscribers.
- **Fingerprints.** Flag hashing goes through the crate-owned
  `canonical_flags_hash`, comparable within one provider only (`\Seen` vs
  `$seen` vs `UNREAD`). `InventoryEntry::differs_from` is the written contract
  for what constitutes "changed".

Disclosed residuals, deliberately left:

- `Batch` and `InventoryBatch` have guarded `try_new` constructors, but their
  fields stay public because making them private would delete published fields.
  Enforcement is the consumer-side boundary check in sync, and
  `reference/types.md` says so honestly rather than claiming the constructor is a
  gate.

Decisions already ruled on - argue against them if you have a reason, but do not
silently reopen them:

- The idempotency table deliberately does not mark `ContainerRename`. IMAP
  `RENAME` keys on the old mailbox NAME, not a stable id, so replaying a
  dropped-but-landed rename addresses a mailbox that no longer exists and reports
  a spurious permanent failure for an operation that succeeded.
- Nor the Google Calendar composite move-then-patch path, which carries an
  explicit `idempotency_override(false)` because replaying it is unsafe.
- `ReconcileAdvice` has carried `#[non_exhaustive]` since the original
  error-model commit, and `ReconcileGuidance` is the plain constructible one. A
  finding claiming the reverse rested on an inverted premise.
