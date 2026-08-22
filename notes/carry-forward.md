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
  `StubAccount` refuse push outright; google (`users.watch` is per mailbox) and
  jmap (per-account PushSubscription) genuinely cover the entire requested list
  when they return `Ok`, so `all_succeeded` is correct for each. **imap no
  longer does** - see the IDLE budget below.

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

## From the bifrost-imap arc (round 2, 2026-08-22)

- **IMAP push coverage is bounded and partial.** Without RFC 5465 NOTIFY the
  account runs at most `ImapAccountConfig::idle_connection_budget` (default 4)
  dedicated IDLE sessions, one per distinct pushed folder. Folders past the
  budget come back in `PushSubscription.outcomes.failed` as
  `Unsupported(PushSubscribe)`, distinguished from a non-folder scope only by
  the diagnostic text. This is the first `Account` impl whose `push_subscribe`
  legitimately partially fails, so it is the live test of the per-scope
  contract above: `Err` stays reserved for whole-request faults, a refusal
  never fails a sibling, and a subscribe that accepts nothing returns no
  handle at all. Admission accepts exactly what worker assignment can watch:
  a folder name `MailboxName::new` rejects goes to the failed lane, since the
  assignment side (`subscribed_idle_folders`) silently drops such names.

  The coupling that makes this safe is that **bifrost-sync never suppresses
  polling on push coverage.** `Engine::subscribe_push` records only
  `outcomes.succeeded()` in the subscription registry, and that registry is
  teardown bookkeeping only - no scheduler path consults it to skip a poll.
  A refused folder therefore stays polled rather than becoming invisible. Any
  future change that lets push coverage relax polling must first make the
  uncovered-scope lane explicit, or it silently reintroduces that hole.

- **`Pool::close()` means no session of that account is still connected when
  it returns.** Every session the pool mints is registered weakly; `close`
  gates the pool, closes the permit semaphore, drains the registry, requests
  LOGOUT on all of them concurrently under one `command_timeout`, then aborts
  any surviving driver and drops its transport. This holds for a checkout
  whose command is still in flight - it is terminated, not waited for - and
  the promise is about the transport, not about a graceful LOGOUT, which is
  best effort. The `sessions` mutex is the linearization point: `close` stores
  the flag before draining, registration re-reads it while holding the lock,
  so a dial or a permit acquisition that completes after `close` refuses
  rather than landing a live session past the drain. Registration also prunes
  dead weak entries, because a reconnecting IDLE worker would otherwise grow
  the registry by one entry per dial for the life of the account.

- **Multi-worker wakeups must latch.** The IDLE workers are woken by a
  `watch` generation counter, not a `Notify`: `notify_one` wakes one of N
  workers and leaves the rest on a stale assignment, and `notify_waiters`
  stores nothing, so a worker between deciding it has no folder and awaiting
  the wake parks forever. Anything that fans out to several workers here needs
  the same latching property. The seen-mark lives at exactly ONE site, before
  `choose_idle_folder` reads the scope set; a second mark later in the round
  discards bumps latched during the dial/SELECT/`NOTIFY SET` awaits and leaves
  the worker on a stale assignment for a full `idle_timeout` - the close pass
  removed exactly such a mark.

- **Get and mutation streams flush at `TARGET_BUFFER_ITEMS` (256) decoded
  targets.** Output ordering is therefore input-window order, then lexical
  folder order within a window; it used to be lexical folder order over the
  whole input. bifrost-sync tolerates this because it keys results by
  `ObjectId` and `BatchOutcome` carries its own submission-order index. A
  consumer that starts depending on cross-window ordering breaks this.

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
