# Error model: implementation roadmap

This is the execution sequence for landing the error model
convergence described in `plans/error-model-convergence.md`. This
document specifies *when* each piece lands; the per-crate plans
specify *what* lands in that piece.

## Document index

- `plans/error-model-convergence.md` - target contract. Public
  shape, recovery model, cause taxonomy, streaming invariants,
  batch boundary, partial-success semantics.
- `plans/error-model-roadmap.md` - this file. Phase sequencing and
  exit gates for the original four phases plus the post-merge
  phase 5.
- `plans/error-model-phase4-audit.md` - frozen evidence. Eight
  per-crate audit findings from after Phase 4 squash-merged.
  Triage labels (`[bug]`, `[gap]`, `[smell]`, `[nit]`) with
  file:line citations. Per `plans/error-model-phase4-decisions.md`'s
  doc-D2 override, this doc is *not* trimmed as items get fixed;
  it stays as the snapshot of "what was wrong post-merge" until
  phase 5 completes.
- `plans/error-model-phase4-decisions.md` - locked decisions for
  every design ambiguity surfaced in the audit. Live tracker:
  resolved decisions get marked `[done]` inline. Drives phase 5
  execution.

The per-crate phase plans (`plans/error-model-{types,net,jmap,imap,
smtp,gmail,graph,sync}.md`) were deleted after Phase 4 landed. The
work they specified is captured in code and in `reference/*.md`;
their substantive design choices that needed revision in phase 5
are restated in the decisions doc. Git history preserves the
originals if needed.

## Goals

- Replace the current `Error` / `RecoveryClass` / `Fatal` surface with
  the opaque `AccountError` + builder model described in the
  convergence plan, across every protocol crate, the net crate, and
  the sync engine.
- Land the change without intermediate compromise types or
  transitional shims. The convergence plan rejects transitional
  shapes; this roadmap honors that.
- Allow multi-agent execution where it makes sense (Phase 2 and
  phase 5C) and single-agent execution where the work is too
  tightly coupled to parallelize (Phases 1, 3, 4, 5A, 5B).
- Close every audit finding from phase 5 to the standard of the
  decisions doc (no compromise adapters, no compatibility shims,
  no stringly typed escape hatches).

## Out of scope

- Account trait API additions beyond what the error model requires
  (no new methods, no new capabilities).
- Live-server tests. The test surface stays per the project rule:
  small technical tests.
- Downstream consumer updates (`ratatoskr`). This roadmap covers
  bifrost only; consumer migration happens against the released
  surface.

## Branch strategy

Phases 1-4 landed on a long-running feature branch
`error-model/main` off `main`, squash-merged at the end of Phase 4.
The workspace did not compile mid-branch (Phase 1 landed new types
before consumer crates were migrated); that was the expected state.
`main` stayed green throughout.

Phase 5 lands directly on `main`. The phase 5A commit intentionally
leaves the workspace uncompilable (same shared-surface pattern as
Phase 1); phase 5B and 5C bring it back. Because phase 5 has no
working consumers gating the API surface, the broken-branch period
sits on `main` rather than on a feature branch. Other branches and
working trees should rebase against the phase-5C exit commit, not
the intermediate phase-5A or phase-5B commits.

## Phase structure

Four original phases plus phase 5 (post-merge audit + fix cycle).
Each gates the next. Validation gates are explicit.

### Phase 1: types foundation

**Status:** landed in commit `ac47289`. The workspace is now in the
intentional broken-branch state described below. Subsequent phases
build on top; do not re-implement the types module.

**Goal:** land the new `bifrost-types::error` module from the
convergence plan, plus the matching `lib.rs` re-export update and
the deletion of the old `error.rs`. Nothing else.

Single agent (the main conversation), single commit on the feature
branch. After this commit, `bifrost-types` does **not** compile -
`account.rs`, `events.rs`, and `mutation.rs` still reference the
removed `Error`, `Fatal`, `RecoveryClass`, `Warning`, `Warning`,
`MutationResult`, and `MutationOutcome` types. That is intentional;
those surface migrations belong to Phase 3 (workspace integration),
where the entire workspace migrates in one coordinated commit and
compilation comes back.

**Plan:** captured at the time in `plans/error-model-types.md`
(deleted; see git history for the original exit criteria).

**Validation:** patch audit, not compilation. Per the types plan's
exit criteria:

- The new `error/` module exists with the documented file layout.
- Every public type from the convergence plan is present.
- `lib.rs` re-exports updated.
- `account.rs`, `events.rs`, `mutation.rs` not touched.
- No transitional shims or compatibility aliases.

`brokkr check` is not run at this phase boundary - the workspace
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

1. `bifrost-net` landed first. It set the `AttemptCause` emission
   convention that `bifrost-jmap`, `bifrost-gmail`, and
   `bifrost-graph` rely on for transmission-state evidence. Single
   agent, single commit.

2. The five consumer crates landed in parallel agents:
   `bifrost-jmap`, `bifrost-imap`, `bifrost-smtp`, `bifrost-gmail`,
   `bifrost-graph`. Each agent owned only its crate's files.

3. `bifrost-sync` landed last. Sync consumes `RecoveryClass` and
   `SyncEvent`; its rewrite depended on the convergence plan's
   final shape being clear. Sync did not own any rename inside
   `bifrost-types`; the `SyncEvent::Fatal` ->
   `SyncEvent::Terminated(AccountError)` rename happened in Phase 3
   inside `bifrost-types/events.rs`. Single agent, single commit.

Per-crate plans (deleted; see git history) carried the per-crate
exit criteria.

**Validation:** patch audit per crate. No `brokkr check`, no
`brokkr test` at this phase. The workspace remained broken
throughout Phase 2.

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

**Phase 3 correctness blockers.** The following gates fail Phase 3
exit if they remain unresolved. Each is written as a grep-checkable
assertion against the post-Phase-3 tree; staging during Phase 2 is
fine, but these items must be green at the Phase 3 gate. A gap in
any one can cause recovery to choose `Retry::SameRequest` where the
correct action is `Reconcile`, which is a correctness bug, not
cleanup. The grep recipes below are the cheap proof that the
canonicalization actually took.

1. **`TransmissionState` is read only from `AttemptCause`.** Both
   recipes are zero-hit assertions; no audit-by-inspection step.
   - Assert: `rg 'TransportCause::transmission_state' crates/` returns
     zero hits. This is the only old-shape access pattern; legitimate
     uses of `transmission_state` (the `AttemptCause` field, the
     `TelemetryView` field, the type definition itself) are not
     matched.
   - Assert: `rg 'TransportCause \{[^}]*transmission_state' crates/`
     returns zero hits. Catches struct-literal constructions of the
     old shape that the colon-form grep misses.
2. **`ServerCause::Error` uses `Option<u16>`; no sentinel values.**
   Both recipes are zero-hit assertions.
   - Assert: `rg 'ServerCause::Error \{ status: 0[^0-9]' crates/`
     returns zero hits. Catches the specific sentinel the IMAP Phase
     2.2 commit invented. `0[^0-9]` avoids matching `0_u16`/`02`/etc
     when the IMAP boundary moves to `None`.
   - Assert: `rg 'ServerCause::Error \{ status: [0-9]' crates/`
     returns zero hits. After the amendment every `ServerCause::Error`
     must construct with `Some(_)` or `None`; a bare integer literal
     is the old shape.
3. **No placeholder `AccountOperation` remains in PIM/account code.**
   - Assert: every PIM module's translation shim threads the correct
     operation per call site. The per-crate plans below name the
     temporary placeholders currently in use (`Discover`,
     `HydrateMessage`) and the semantic exceptions where they are
     correct.
4. **SMTP send/LMTP command paths carry transmission state through
   recovery.**
   - Assert: each SMTP command phase (HELO/EHLO, MAIL FROM, RCPT TO,
     DATA initiation, DATA body, DATA final, RSET, QUIT) constructs
     errors with an `AttemptCause` whose `transmission_state` reflects
     the wire-level evidence at that phase.
   - Assert: `bifrost-smtp` `into_account_error` test cases cover
     `Send` (non-idempotent) with `InFlight` producing `Reconcile`.
5. **Known provider codes are typed, not matched as strings through
   `Unknown { code }`.**
   - Assert: `rg 'GraphSignal::Unknown' crates/graph/` matches only
     genuine forward-compatibility paths; no string comparison
     (`==`, `contains`, `starts_with`) against well-known Microsoft
     vocabulary remains.
   - Assert: JMAP `SetErrorType` family routes through typed
     `JmapMethod` variants, not through `JmapMethod::Unknown { code }`.
6. **No stream discards a computed `AccountError`.**
   - Assert: `rg 'let _account_error|let _ = .* AccountError' crates/`
     returns zero hits in stream-termination paths.
   - Assert: every stream-termination site emits
     `SyncEvent::Terminated(AccountError)` carrying the structured
     error.
7. **No per-item accounted failure is duplicated as a trailing global
   termination.**
   - Assert: per-item `ItemOutcome::Failed`/`Uncertain` lanes in
     bulk-mutation streams are not followed by a stream-level
     `SyncEvent::Terminated(_)` covering the same items. Trailing
     terminations are reserved for stream-level errors that prevent
     further attempts.

The audit pass at Phase 3 gate must run these greps verbatim and
report results. A pass-by-inspection claim is not acceptable.

### Phase 4: merge to main

**Status:** landed. Squash merge `error-model/main` -> `main` shipped
as commit `eeaa386` plus follow-ups. `Cargo.lock` updates committed
alongside per the project rule.

### Phase 5: post-merge audit and fix cycle

**Goal:** close the gap between "the contract landed" and "the
contract is actually wired end-to-end." The post-merge audit at
`plans/error-model-phase4-audit.md` found that the shape landed but
the model was not wired: every crate except `bifrost-net` had P0
correctness bugs (double-send hazards in SMTP/Graph/Gmail/IMAP,
structured errors flattened to strings in Graph EWS / webhooks,
engine drops directives in sync, type-system gaps in
`bifrost-types`). Phase 5 lands the decisions documented in
`plans/error-model-phase4-decisions.md`.

Phase 5 has five sub-phases (A-E). The decisions doc is the source
of truth for *what* lands in each; this roadmap is the source of
truth for *when* and *how it gates*.

#### Phase 5A: shared surface

Single agent, single commit. Lands every shared type and trait
change from the decisions doc:

- `types-D*` and `types-N*` items.
- `Account::get_stream` trait signature change to
  `SyncEvent<ItemOutcome<HydratedObject>>` (types-D17).
- `WatchEvent::Terminated(AccountError)` variant (types-D15).
- `AccountControl` + `PauseReason` enums (types-D16).
- `ResourceKind` widening with `PushSubscription` (types-D13).
- `ThrottleScope` rename + `ThrottleKey` enum type definition
  (types-D14; the actual `ThrottleBucket` lives in Phase 5B).
- `RetryHint` shape on `ServerCause` and `RetryAdvice` (types-D7).
- `AccountErrorBuildError` enum + `build()` -> `try_build()` rename
  (types-D10), with `CursorInvalidWithoutScope` as a build error
  (types-D10b).
- `EngineDirective::CapabilityChanged` removal + cause-payload
  becoming `Option<CapabilityDelta>` (types-D2, types-D2b).
- `RemediationAction` engine-action variant removal (types-D8).
- `Fatal` field privatization (types-D1).
- `BatchOutcomeBuilder` separating mutable construction from
  immutable return (types-D3).
- Convergence doc edits cascading from these decisions (doc-D3).
- New tests: helper exclusivity (types-D11), `Fatal::try_from`
  round-trip (types-D12).

**Validation:** patch audit against the decisions doc. Phase 5A
intentionally leaves protocol implementations uncompilable; Phase
5B and 5C bring the workspace back to a buildable state. `brokkr
check` does not run at this gate.

#### Phase 5B: sync alone

Single agent, single commit. Sync gets a dedicated phase because it
owns the largest semantic-surface change: `RecoveryPlan` dispatch,
`ThrottleBucket` implementation keyed by `ThrottleKey`,
`AccountControl::Pause/Resume` plumbing, three-retry-then-
`Terminated(last_error)` backoff with `Pause(RetryBudgetExhausted)`,
`ReconcileAction::CheckTarget` routing to read-back, cursor-decode
-> `SchemaIncompatible` translator, removal of the
`EngineDirective::CapabilityChanged` arm.

All `sync-D*` and `sync-N*` items. Agent prompt explicitly forbids:

- compromise adapters between the old variant-direct
  `RecoveryClass` dispatch and the new `plan_recovery` helper,
- compatibility shims for the removed
  `EngineDirective::CapabilityChanged`,
- stringly pause reasons (use the `PauseReason` enum only),
- ad-hoc `Fatal` construction (only
  `RecoveryPlan::Terminal(Fatal)` via `Fatal::try_from`),
- silent error swallows in any long-running loop (e.g.
  `scope_lifecycle`, `discover_memberships`).

**Validation:** patch audit against `sync-D*` / `sync-N*` items.
Sync compiles cleanly against Phase 5A; protocol crates still do
not. `brokkr check -p bifrost-sync` may pass; workspace-wide
`brokkr check` does not.

#### Phase 5C: five protocol crates in parallel

Five agents in parallel: `bifrost-jmap`, `bifrost-imap`,
`bifrost-smtp`, `bifrost-gmail`, `bifrost-graph`. Strict file
ownership per AGENTS.md; no agent reads or writes outside its
crate. Orchestrator runs `brokkr check` between agents.

Each agent's prompt cites the relevant `*-D*` and `*-N*` items
from the decisions doc. Same compromise-free discipline as Phase
5B.

**Wire-enum escape hatch:** as in Phase 2, Phase 5C agents may not
edit `crates/types/src/error/cause.rs` for new wire variants. The
orchestrator patches `bifrost-types` centrally on the agent's
behalf and re-runs the affected agent.

**Validation:** workspace-wide `brokkr check` clean. Every
decision in `plans/error-model-phase4-decisions.md` is `[done]`.

#### Phase 5D: re-audit

Six agents (sync + five protocols) in parallel. Same per-crate
prompt shape as the original phase 4 audit but asking "did the fix
land cleanly; are there residual gaps; report findings labeled
bug / gap / smell / nit." Surfaces P2 items that were deferred plus
anything missed.

**Validation:** triage the re-audit output. New `[bug]` or `[gap]`
findings escalate back to Phase 5C for the affected crate.

#### Phase 5E: P2 cleanup and doc sweep

Smells and nits from the original audit plus Phase 5D residuals.
One commit per crate, sequential.

`reference/{graph,jmap,sync,gmail,imap}.md` final pass to make
sure documentation matches the post-phase-5 code. Convergence and
roadmap docs final edit.

`plans/error-model-phase4-audit.md` deleted at the end of this
phase. With every decision `[done]` and every audit finding either
fixed or reclassified as out-of-scope, the audit doc's evidence
value is spent.

## Multi-agent orchestration rules

These apply specifically to Phase 2 (and to any future phase where
parallel agents make sense). Per AGENTS.md:

- Each agent gets exclusive ownership of specific files. Phase 2
  ownership is by crate boundary; an agent assigned `bifrost-jmap`
  touches files in `crates/jmap/` only.
- Agents read their target files first. They do not replace existing
  code with placeholders or stub it out - they read, then rewrite
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
  - `reference/<crate>.md` (the crate's reference doc - kept in
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
historical records - resolved items are deleted from the doc, not
struck through).

## Validation gates summary

| Phase | Gate | Method |
|---|---|---|
| 1 | new error module landed, audit clean | patch audit against the (deleted) types plan's exit criteria |
| 2.1 | bifrost-net translation surface authored | patch audit against the (deleted) net plan |
| 2.2 | each consumer crate translation authored | patch audit against each (deleted) per-crate plan |
| 2.3 | bifrost-sync engine adaption authored | patch audit against the (deleted) sync plan |
| 3 | workspace integration complete, compiles, tests pass | `brokkr check` workspace-wide; the Phase 1 tests now execute |
| 4 | feature branch merged | `git merge --squash` to main |
| 5A | shared-surface decisions landed | patch audit against `plans/error-model-phase4-decisions.md` types/shared section |
| 5B | sync engine adapted | patch audit against `plans/error-model-phase4-decisions.md` sync section; `brokkr check -p bifrost-sync` |
| 5C | five protocol crates updated | workspace-wide `brokkr check` clean; every decision `[done]` |
| 5D | re-audit clean | per-crate audit reports show no new `[bug]` or `[gap]` |
| 5E | P2 cleanup + doc sweep | `reference/*.md` matches code; `plans/error-model-phase4-audit.md` deleted |

Phases 1, 2, 5A, and 5B do not run workspace-wide `brokkr`. Phase 3
and Phase 5C are the gates where the workspace must compile cleanly.

## Rollback

For phases 1-4 (feature-branch era): if Phase 2 hit an unforeseen
blocker on one crate, the feature branch held. Phase 1 alone was
not useful on `main` (workspace broken), so partial state was never
merged. The branch could be abandoned without affecting `main`.

For phase 5 (on-main era): rollback is harder because phase 5A
lands directly on `main`. If phase 5B or 5C reveals that a phase-5A
shared-surface choice was wrong, the fix is forward: amend the
decisions doc, re-do the affected sub-phase. Reverting phase 5A is
a last resort because the audit findings it addresses are
correctness bugs in the merged state, and reverting would re-
introduce them.

## Test discipline

Per the project rule ("Bifrost tests are small and technical").
Each phase adds tests in proportion to the surface it changes:

- Phase 1: recovery mapping table (~30-40 tests), message_key
  derivation (~30 tests), builder invariants (~10 tests),
  diagnostic accessors (~5 tests), `BatchOutcome` ordering and
  uniqueness (~5 tests).
- Phase 2: per-crate translation tests - given a wire-level error,
  the protocol crate produces the expected `AccountErrorKind` and
  `RecoveryClass`. Roughly 10-20 per crate.
- Phase 3: trait signature changes are caught by `brokkr check`;
  ItemOutcome streaming semantics get ~5 tests per bulk method.
- Phase 5A: helper exclusivity (one test, one assertion per
  `RecoveryClass` variant - types-D11), `Fatal::try_from` round-
  trip (one test per terminal and non-terminal variant -
  types-D12), `AccountErrorBuildError` coverage (one test per of
  the four variants - types-D10), `RetryHint` accessor parity
  (`not_before` / `min_delay` on the same hint produce consistent
  values).
- Phase 5B: `RecoveryPlan` dispatch exhaustiveness (compile-time),
  `ThrottleBucket` cross-account semantics for `Tenant` /
  `Provider` keys, `AccountControl::Pause(RetryBudgetExhausted)`
  emission after three failed reopens, cursor-decode ->
  `SchemaIncompatible` translator path.
- Phase 5C: per-crate updates to existing translation tests for
  the new shapes; new tests where the audit found classification
  gaps (Graph webhook auth/policy failures, Gmail Pub/Sub renewer
  classification, IMAP tagged NO/BAD `Acknowledged` attempt
  state, SMTP LMTP DATA negative-reply per-recipient lanes).

No live-server tests. No end-to-end tests. No mock servers.

## Sequencing summary

Phases 1-4 (feature-branch era, landed):

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

Phase 5 (on-main, pending):

```
Phase 5A: shared surface     [single agent, one commit on main]
   ↓
Phase 5B: bifrost-sync       [single agent, one commit]
   ↓
Phase 5C: bifrost-jmap    ─┐
          bifrost-imap    ─┤
          bifrost-smtp    ─┤ [five agents in parallel,
          bifrost-gmail   ─┤  five commits, no overlap]
          bifrost-graph   ─┘
   ↓
Phase 5D: re-audit           [six agents in parallel, no commits]
   ↓
Phase 5E: P2 cleanup +       [one commit per crate, sequential]
          doc sweep
```

Up to twelve commits on `main` for phase 5 (1 + 1 + 5 + 0 + 5 if
every crate produces a P2 commit; fewer if some crates have no P2
items left).
