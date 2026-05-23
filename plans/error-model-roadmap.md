# Error model: implementation roadmap

This is the execution sequence for landing the error model
convergence described in `plans/error-model-convergence.md`. Per-crate
plans live in `plans/error-model-<crate>.md`. This document specifies
*when* each piece lands; the per-crate plans specify *what* lands in
that piece.

## Goals

- Replace the current `Error` / `RecoveryClass` / `Fatal` surface with
  the opaque `AccountError` + builder model described in the
  convergence plan, across every protocol crate, the net crate, and
  the sync engine.
- Land the change without intermediate compromise types or
  transitional shims. The convergence plan rejects transitional
  shapes; this roadmap honors that.
- Allow multi-agent execution where it makes sense (Phase 2) and
  single-agent execution where the work is too tightly coupled to
  parallelize (Phases 1, 3, 4).

## Out of scope

- Account trait API additions beyond what the error model requires
  (no new methods, no new capabilities).
- Live-server tests. The test surface stays per the project rule:
  small technical tests.
- Downstream consumer updates (`ratatoskr`). This roadmap covers
  bifrost only; consumer migration happens against the released
  surface.

## Branch strategy

All work lands on a long-running feature branch
`error-model/main` off `main`. Each phase commits to this branch.
After Phase 4, the branch merges to `main` in a single squash. The
workspace does not compile mid-branch (Phase 1 lands the new types
before consumer crates are migrated); that is the expected state.
`main` itself stays green throughout.

## Phase structure

Four phases, each gating the next. Validation gates between phases
are explicit.

### Phase 1: types foundation

**Goal:** land the new `bifrost-types::error` module from the
convergence plan, plus the matching `lib.rs` re-export update and
the deletion of the old `error.rs`. Nothing else.

Single agent (the main conversation), single commit on the feature
branch. After this commit, `bifrost-types` does **not** compile —
`account.rs`, `events.rs`, and `mutation.rs` still reference the
removed `Error`, `Fatal`, `RecoveryClass`, `Warning`, `Warning`,
`MutationResult`, and `MutationOutcome` types. That is intentional;
those surface migrations belong to Phase 3 (workspace integration),
where the entire workspace migrates in one coordinated commit and
compilation comes back.

**Plan:** `plans/error-model-types.md`.

**Validation:** patch audit, not compilation. Per the types plan's
exit criteria:

- The new `error/` module exists with the documented file layout.
- Every public type from the convergence plan is present.
- `lib.rs` re-exports updated.
- `account.rs`, `events.rs`, `mutation.rs` not touched.
- No transitional shims or compatibility aliases.

`brokkr check` is not run at this phase boundary — the workspace
will not compile until Phase 3.

### Phase 2: per-crate patch authorship

**Goal:** rewrite each consumer crate's internal error types and
`into_account_error`-equivalent translation boundary against the
new builder. Old helpers (`recovery_for_*`, `fatal_for_*`, Graph
substring matching, Gmail `account_error_from_template`) deleted
in the same commits.

**Orchestration:** multi-agent per the project pattern
(AGENTS.md). Strict file ownership by crate. No agent touches
files outside its crate. The orchestrator validates between agents
and merges results.

**Compilation is not a gate at this phase.** Each protocol crate's
patches reference `bifrost-types` types whose own crate does not
compile (per Phase 1). Phase 2 commits land in the broken-branch
state alongside Phase 1's. Validation is by patch audit against
each per-crate plan's exit criteria.

**Sub-phase ordering within Phase 2:**

1. `bifrost-net` lands first.
   - Plan: `plans/error-model-net.md`.
   - Reason: it sets the `AttemptCause` emission convention that
     `bifrost-jmap`, `bifrost-gmail`, and `bifrost-graph` rely
     on for transmission-state evidence. Agents writing those
     crates need to know what `bifrost-net`'s translation surface
     looks like even though they cannot compile against it yet.
   - Single agent, single commit.

2. The five consumer crates land in parallel agents:
   - `bifrost-jmap` per `plans/error-model-jmap.md`.
   - `bifrost-imap` per `plans/error-model-imap.md`.
   - `bifrost-smtp` per `plans/error-model-smtp.md`.
   - `bifrost-gmail` per `plans/error-model-gmail.md`.
   - `bifrost-graph` per `plans/error-model-graph.md`.

   Each agent owns only its crate's files. `bifrost-imap` and
   `bifrost-smtp` do not depend on `bifrost-net` and could in
   principle land before it, but for orchestration simplicity all
   five run after net.

3. `bifrost-sync` lands last in Phase 2.
   - Plan: `plans/error-model-sync.md`.
   - Reason: sync consumes `RecoveryClass` and `SyncEvent`; its
     own rewrite depends on the convergence plan's final shape
     being clear. Sync does **not** own any rename inside
     `bifrost-types`; the `SyncEvent::Fatal` →
     `SyncEvent::Terminated(AccountError)` rename happens in
     Phase 3 inside `bifrost-types/events.rs`. Sync's Phase 2
     work is updating its own engine code to consume the new
     `RecoveryClass` shape and dispatching `EngineDirective`.
   - Single agent, single commit.

**Validation:** patch audit per crate against the relevant
`plans/error-model-<crate>.md`. No `brokkr check`, no `brokkr
test` at this phase. The workspace remains broken throughout
Phase 2.

### Phase 3: workspace integration

**Goal:** restore compilation across the workspace. This is the
phase that performs every surface migration Phase 1 and Phase 2
left dangling, in one coordinated commit:

In `bifrost-types`:
- `account.rs`: trait imports updated, every `Result<_, Error>`
  return-type site updated to `Result<_, AccountError>` (~45
  sites), `bulk_set_flags` / `bulk_move` / `bulk_destroy`
  signatures updated to
  `AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>`, the
  default impl of `inventory_partition_stream` and the
  `Unsupported`-short-circuit conveniences updated to construct
  `AccountError` via the builder.
- `events.rs`: `SyncEvent::Fatal(Fatal)` renamed to
  `SyncEvent::Terminated(AccountError)`. `Control` trait's
  `Result<Checkpoint, Error>` returns updated.
- `mutation.rs`: `MutationResult` and `MutationOutcome` deleted.
  `use crate::error::Error` import removed.

In every protocol crate (`bifrost-net`, `bifrost-jmap`,
`bifrost-imap`, `bifrost-smtp`, `bifrost-gmail`, `bifrost-graph`):
- `Account` trait impl signatures updated to match the new
  trait declarations.
- Any `SyncEvent::Fatal` emission sites updated to
  `SyncEvent::Terminated(AccountError)`.
- `MutationResult` references migrated to
  `ItemOutcome<MutationSuccess>`.

In `bifrost-sync`:
- Engine consumers updated to `RecoveryClass` four-helper API.
- `SyncEvent::Fatal` match arms updated to
  `SyncEvent::Terminated(AccountError)`.
- `EngineDirective` dispatch wired.

**Orchestration:** single agent. The integration touches every
crate; the project rule "agents do not work on diverged snapshots"
applies hard. One agent owns the whole integration commit.

**Validation:** this is the phase where compilation comes back.

- `brokkr check` clean workspace-wide.
- The ~95-100 tests written in Phase 1 now execute. They may need
  minor module-path or import-path repair if Phase 3's
  integration touches `error/` module organization (it should
  not, but in practice integration sometimes surfaces module-path
  adjustments). Phase 3's job includes fixing those tests; they
  must be all-green by the phase exit gate.
- All `Account` methods return `Result<_, AccountError>`.
- No `Result<(), AccountError>` or `Result<Vec<T>, AccountError>`
  on any multi-target method.
- Streaming bulk methods emit `ItemOutcome<T>` per-item; no items
  silently drop.
- `Warning` fields use `DiagnosticText`.
- `SyncEvent::Terminated(AccountError)` is the only stream
  termination event; no `SyncEvent::Fatal` remaining.

### Phase 4: merge to main

**Goal:** land the feature branch on `main`.

- Squash merge `error-model/main` → `main`.
- Single commit message summarizing the convergence.
- `Cargo.lock` updates committed alongside (project rule: always
  commit lockfile changes).

## Multi-agent orchestration rules

These apply specifically to Phase 2 (and to any future phase where
parallel agents make sense). Per AGENTS.md:

- Each agent gets exclusive ownership of specific files. Phase 2
  ownership is by crate boundary; an agent assigned `bifrost-jmap`
  touches files in `crates/jmap/` only.
- Agents read their target files first. They do not replace existing
  code with placeholders or stub it out — they read, then rewrite
  in-place.
- Agents must NOT run `cargo` or `brokkr`. The orchestrator (the
  main conversation) validates between agents. This prevents three
  agents from triggering simultaneous test runs.
- **Wire-enum escape hatch:** Phase 2 agents may not edit
  `crates/types/src/error/cause.rs` even if their crate needs a
  new `GraphSignal` / `JmapMethod` / `ImapResponseCode` /
  `EnhancedStatusCode` / `GmailSignal` variant. The orchestrator
  patches `bifrost-types` centrally on the agent's behalf and
  re-runs the affected agent. Crate-ownership rules are
  non-negotiable; centralized patches to wire enums are the only
  exception.
- Required reading for every agent:
  - `plans/error-model-convergence.md`
  - `plans/error-model-<crate>.md` (the agent's specific plan)
  - `reference/<crate>.md` (the crate's reference doc — kept in
    sync with code per project convention)
- Agents must launch in the foreground (never `run_in_background`)
  so the user can approve tool requests.

## Audit protocol

Per AGENTS.md, every phase ends with a 3-pass audit:

1. **Domain-specific verification.** Does the new code do what the
   relevant plan says? Per-crate: every old helper removed, new
   builder used, mapping table rows produce the documented
   `RecoveryClass`, no transitional shims.

2. **Cross-cutting reconciliation.** Do the parts wire together
   correctly? Specifically: does every consumer crate's
   `into_account_error` end up calling `AccountErrorBuilder::build`?
   Are there orphan `Cause` variants the central recovery mapping
   doesn't handle? Are `AttemptCause` emissions consistent with
   recovery derivation expectations?

3. **Editorial normalization.** Docs match code. No stale references
   to old types in reference/*.md or in code comments. The
   convergence plan's exit criteria checklist runs green.

Discrepancies discovered during audit go into a per-phase
`plans/error-model-phase<N>-audit.md` (only current gaps, no
historical records — resolved items are deleted from the doc, not
struck through).

## Validation gates summary

| Phase | Gate | Method |
|---|---|---|
| 1 | new error module landed, audit clean | patch audit against `plans/error-model-types.md` exit criteria |
| 2.1 | bifrost-net translation surface authored | patch audit against `plans/error-model-net.md` |
| 2.2 | each consumer crate translation authored | patch audit against `plans/error-model-<crate>.md` |
| 2.3 | bifrost-sync engine adaption authored | patch audit against `plans/error-model-sync.md` |
| 3 | workspace integration complete, compiles, tests pass | `brokkr check` workspace-wide; the Phase 1 tests now execute |
| 4 | feature branch merged | `git merge --squash` to main |

Phases 1 and 2 do not run `brokkr`. The workspace is broken
throughout. Phase 3 is the first phase that runs `brokkr check`,
and it is also the first phase where it passes.

## Rollback

If Phase 2 hits an unforeseen blocker on one crate, the feature
branch holds. Phase 1 alone is not useful on `main` — workspace
broken — so we never merge a partial state. The feature branch can
be abandoned without affecting `main`.

If Phase 3 reveals that the convergence plan's trait surface choices
are wrong, abandoning the branch costs only the work already
committed; `main` is untouched.

## Test discipline

Per the project rule ("Bifrost tests are small and technical").
Each phase adds tests in proportion to the surface it changes:

- Phase 1: recovery mapping table (~30-40 tests), message_key
  derivation (~30 tests), builder invariants (~10 tests),
  diagnostic accessors (~5 tests), `BatchOutcome` ordering and
  uniqueness (~5 tests).
- Phase 2: per-crate translation tests — given a wire-level error,
  the protocol crate produces the expected `AccountErrorKind` and
  `RecoveryClass`. Roughly 10-20 per crate.
- Phase 3: trait signature changes are caught by `brokkr check`;
  ItemOutcome streaming semantics get ~5 tests per bulk method.

No live-server tests. No end-to-end tests. No mock servers.

## Sequencing summary

```
Phase 1: bifrost-types       [single agent, one commit]
   ↓
Phase 2.1: bifrost-net       [single agent, one commit]
   ↓
Phase 2.2: bifrost-jmap   ─┐
           bifrost-imap   ─┤
           bifrost-smtp   ─┤ [five agents in parallel,
           bifrost-gmail  ─┤  five commits, no overlap]
           bifrost-graph  ─┘
   ↓
Phase 2.3: bifrost-sync      [single agent, one commit]
   ↓
Phase 3: trait surface       [single agent, one commit]
   ↓
Phase 4: merge to main       [squash merge, one commit on main]
```

Eight commits on the feature branch, one squash commit on `main`.
