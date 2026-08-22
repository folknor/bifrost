# Orchestration carry-forward

Machinery established by bug-hunt arcs that later rounds may build on and must
not break. This is loop state: no agent in the loop can see any round but its
own, so every brief carries the relevant slice of this forward.

Keep this current as of the last close pass. It records invariants and couplings,
not history - when something here stops being true, change it rather than
appending to it.

## From the bifrost-graph arc (closed 2026-08-22, `f0d7d99` and `b460cdb`)

These are cross-crate. They bind every protocol crate, not just `bifrost-graph`.

- **`push_subscribe` answers per scope, not per request.** It returns
  `PushSubscription` (in `crates/types/src/account.rs`), which carries an
  optional handle plus a validated per-scope `BatchOutcome` over the shared
  three-lane model in `reference/error-model.md`. `Err` is reserved for genuine
  whole-request faults: an empty scope list, no webhook endpoint configured,
  nothing subscribable at all, or a create failure that was rolled back. A single
  refused scope must never fail its siblings, and that applies to every bail path,
  including ones that run before any translation or resolution step. The ids in
  the outcome are submission positions.

  Non-Graph implementors were audited, not assumed: caldav, carddav and the IMAP
  `StubAccount` refuse push outright; google (`users.watch` is per mailbox), jmap
  (per-account PushSubscription) and imap (IDLE stores the whole scope set
  unconditionally) genuinely cover the entire requested list when they return
  `Ok`, so `all_succeeded` is correct for each.

- **`Account::is_inventory_cursor` must share ONE condition with
  `Account::inventory_resume_stream`.** A predicate that merely agrees with the
  hook today can drift from it, and that drift strands a scope with neither a live
  cursor nor a recovery path - it was a live defect in round 2, caught in review.
  Graph derives both from `resumable_inventory_payload`. Overriding either means
  overriding both from a single condition; the trait doc says so.

- **Constructing an `Account` change/inventory stream must be I/O-free and
  side-effect-free.** `crates/sync/src/engine.rs` no longer builds and drops
  streams to classify a cursor (that is what `is_inventory_cursor` replaced), but
  the contract stands for implementors.

- **`CursorError::InventoryInProgress` maps to `CursorInvalid`, not
  `SchemaIncompatible`.** A mid-inventory cursor arriving where a changes cursor
  belongs is well-formed and misrouted, so the recovery restarts that scope; it
  must not clear the account's whole stored schema.

- **One worker-slot discipline, shared by both push modes.**
  `crates/graph/src/account/worker_slot.rs` owns `ensure_worker` /
  `retire_worker_slot` / `take_worker` and pins the lock ordering once:
  registration writes state under the registration lock, drops it, then ensures;
  an exiting worker retires its slot while STILL holding the registration guard
  that decided to exit. Two divergent worker lifecycles guarding the same shape
  produced two separate bugs before this existed. Do not fork it back apart.

- **The EWS frame decoder matches the LOCAL XML name**
  (`GetStreamingEventsResponseMessage`), never a literal namespace prefix.
  Prefixes are aliases; matching `<m:` silently discarded valid responses and
  disabled push while still advertising `push_in_process()`.

- **Typed protocol errors survive the streaming path.** `EwsError` is preserved
  through the frame consumer so a terminal recovery yields
  `StreamLoopExit::Terminated`. Stringifying it turned access-denied into an
  endless reconnect loop.

- Cursor envelope is at **v4**.

## Standing lessons this project has paid for

- **Audit new tests for bite, mechanically.** Revert the production change,
  confirm the test fails, restore. Three tests in this project have been caught
  passing against the bug they were written for - most recently an entire
  "exhaustive" alias-pair suite that passed against the pre-fix code, which also
  proved the finding it came from was never a defect.

- **The second half of a fix-and-commit stage is never cold-reviewed.** That
  stage fixes review findings and commits in one step, so its own work ships
  unreviewed. The close pass looks hardest there.

- **The recurring defect shape is a fix that opens a new hole one layer up.**
  Check what a fix does to its consumer, not only to the unit test in front of it.

- **`git add -N` every untracked source file before the cold review.** The fix
  pass leaves new modules untracked, and the cold review reads the unstaged diff,
  so a round whose central deliverable is a NEW FILE gets reviewed with that file
  invisible. This bit the bifrost-imap round 1: the shared hydration module that
  was the entire point of the unification did not appear in `git diff` at all.
  Intent-to-add makes it visible without staging its content.

- **The fix pass sometimes verifies with a scoped `-p <crate>` run.** That is
  exactly why the orchestrator gate at stage 2 is unconditional: scoped runs miss
  cross-crate breakage, and feature unification makes them non-equivalent to the
  real thing.
