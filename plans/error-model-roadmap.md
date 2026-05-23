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

**Goal:** land all `bifrost-types` changes from the convergence plan.
Single agent (the main conversation), single commit on the feature
branch. After this commit, `bifrost-types` compiles in isolation and
its tests pass; consumer crates fail to compile because they still
reference removed types. That is intentional.

**Plan:** `plans/error-model-types.md`.

**Validation gate:**

- `cargo check -p bifrost-types` clean.
- `cargo test -p bifrost-types` passes — recovery mapping table fully
  covered by unit tests, message_key derivation covered, builder
  invariants covered, diagnostic accessors covered.
- `brokkr check --all` will fail at consumer crates. The failures
  must be exactly the kinds Phase 2 addresses (references to removed
  types). No other regressions.

### Phase 2: wire / protocol crate rewrite

**Goal:** per-crate consumer migration. Each consumer crate's
internal error types and `into_account_error`-equivalent translation
boundary are rewritten against the new builder. Old helpers
(`recovery_for_*`, `fatal_for_*`, Graph substring matching, Gmail
`account_error_from_template`) deleted in the same commits.

**Orchestration:** multi-agent per the project pattern
(AGENTS.md). Strict file ownership by crate. No agent touches files
outside its crate. The orchestrator (the main conversation)
validates between agents and merges results.

**Ordering within Phase 2:**

1. `bifrost-net` lands first.
   - Plan: `plans/error-model-net.md`.
   - Reason: it sets the `AttemptCause` emission convention that
     `bifrost-jmap`, `bifrost-gmail`, and `bifrost-graph` rely on
     for transmission-state evidence. Consumer crates that route
     HTTP through `bifrost-net` cannot complete until net's
     `NetErrorContext` / `into_account_error` is rewritten.
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

3. `bifrost-sync` lands last.
   - Plan: `plans/error-model-sync.md`.
   - Reason: sync consumes `RecoveryClass` and `SyncEvent`; its
     migration depends on consumer crates emitting stable
     `AccountError` shapes. Sync also owns the `SyncEvent::Fatal` →
     `SyncEvent::Terminated(AccountError)` rename, which touches
     every consumer crate's emission path.
   - Single agent, single commit.

**Validation gate:**

- After `bifrost-net` lands: `cargo check -p bifrost-net` clean.
- After each consumer crate lands: `cargo check -p <crate>` clean.
- After `bifrost-sync` lands: `cargo check -p bifrost-sync` clean,
  workspace `brokkr check` clean.
- 3-pass audit per the project convention (see Audit protocol below).

### Phase 3: trait surface migration

**Goal:** apply the contract changes that touch the `Account` trait
signatures themselves:

- Every method return type `Result<_, Error>` → `Result<_, AccountError>`.
  This is mechanical but ripples through every consumer impl and
  every sync engine call site.
- Multi-target Vec-batch methods (if any) adopt
  `Result<BatchOutcome<T>, AccountError>` with `Vec<BatchItem<I>>`
  inputs. SMTP multi-recipient send is the canonical site;
  identification of any others is a Phase 3 sub-task.
- Streaming bulk methods (`bulk_set_flags`, `bulk_move`,
  `bulk_destroy`) migrate from
  `AccountStream<SyncEvent<MutationResult>>` to
  `AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>` (or the
  wrapped `SyncEvent::Batch<ItemOutcome<MutationSuccess>>` shape).
  Per-item ID becomes `BatchItemId`; `MutationOutcome::Skipped`
  folds into `MutationSuccess::Skipped`.
- `Warning` adoption of `DiagnosticText` for free-form fields.

**Orchestration:** single agent. Trait signature changes are tightly
coupled across every consumer impl; the project rule that "agents do
not work on diverged snapshots" applies hard here. One agent owns
the whole surface for one commit.

**Validation gate:**

- `brokkr check` clean workspace-wide.
- All `Account` methods return `Result<_, AccountError>`.
- No `Result<(), AccountError>` or `Result<Vec<T>, AccountError>` on
  any multi-target method.
- Streaming bulk methods emit `ItemOutcome<T>` per-item; no items
  silently drop.
- `Warning` fields use `DiagnosticText`.
- `SyncEvent::Terminated(AccountError)` is the rename target; no
  `SyncEvent::Fatal` remaining.

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
- Required reading for every agent:
  - `CLAUDE.md`
  - `AGENTS.md`
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
| 1 | bifrost-types compiles + tests pass | `cargo check -p bifrost-types && cargo test -p bifrost-types` |
| 2.1 | bifrost-net compiles | `cargo check -p bifrost-net` |
| 2.2 | each consumer crate compiles | `cargo check -p <crate>` per crate |
| 2.3 | bifrost-sync compiles, workspace clean | `brokkr check` |
| 3 | trait surface migrated, workspace clean | `brokkr check --all` |
| 4 | feature branch merged | `git merge --squash` to main |

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
- Phase 3: trait signature changes are caught by `cargo check`;
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
