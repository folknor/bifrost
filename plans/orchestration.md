# Orchestration: bifrost sync engine and protocol Account impls

Operational notes for the multi-phase bring-up of `bifrost-types`,
`bifrost-net`, `bifrost-sync`, and the four protocol `Account`
impls (JMAP, IMAP, Gmail, Graph). Phases 0-2 are done at the
surface level; CONDSTORE/QRESYNC and Phase 3 reconciliation
remain. Current code state lives in
`reference/{net,sync,jmap,imap,gmail,graph}.md`; this doc is the
operational backstop, not an architecture spec.

## Key decisions

Resolved during Phase 0 / Phase 1. Recorded here because the
rationale is not obvious from the code alone.

- **Workspace shape.** Carve-out crate `bifrost-types` hosts the
  `Account` trait and the entire shared sync surface (events,
  batches, capabilities, cursor types, blob handles). Protocol
  crates and `bifrost-sync` depend on it; it depends on nothing
  in the workspace.
- **Dispatch.** `dyn Account` reachable via type-erased streams
  and a concrete `OpaqueChangeState` tagged with protocol and
  envelope version.
- **Send + Sync.** IMAP is pool-shaped from day one. The pool is
  the public `Account` type; `ImapConnection` is the checkout
  unit.
- **Lifecycle.** Consumer registers an `AccountFactory`; engine
  owns the open `Arc<dyn Account>` and calls the factory on
  reopen.
- **InvalidationSink.** Channel-based.
- **TLS.** `native-tls` workspace-wide. No rustls dependency.
- **Cursor establishment.** Per-scope and dynamic, returned from
  `Account::establish_initial_cursor(scope)`. Two variants:
  `Ready(cursor)` (cheap mint - JMAP all scopes, Gmail Account,
  IMAP-QRESYNC folders) and `EstablishViaInventory` (Graph all
  scopes, IMAP-Basic / IMAP-CONDSTORE-only folders). Replaces an
  earlier account-level binary that could not represent IMAP
  mixed-tier accounts or Graph's full-sync-on-first-delta
  semantics.
- **Replay safety.** No current protocol declares
  `MutationReplaySafety::ReplayToken`. Microsoft Graph's
  `client-request-id` is debugging correlation; Google's
  `X-Goog-Request-Id` is not documented as a Gmail-side dedup
  primitive; JMAP and IMAP have no wire replay tokens. All four
  protocols declare `MutationReplaySafety::None` and rely on the
  engine's read-back guard. The enum variant stays in the trait
  for forward-compatibility.

## Progress

- **Phase 0** complete. Four per-protocol Account-impl plans
  shipped; reviewer pass folded in. Those plans have since been
  deleted - the work they specified is in the crates.
- **Phase 1** complete. `bifrost-types` and `bifrost-net`
  skeletons landed; reviewer pass folded in. Trait surface
  frozen.
- **Phase 2** mostly complete:
  - **P2-A5** (`bifrost-net` full impl): shipped. Real retry
    loop with `Retry-After` honor, OAuth single-flight refresh,
    per-host token-bucket rate limiter, sliding-window
    bandwidth meter, `traceparent` injection. Reviewer pass
    folded in. Code state in `reference/net.md`.
  - **P2-A6** (`bifrost-sync` engine): shipped. Multiplexer,
    backfill orchestrator, push reconciler, mutation pipeline,
    cursor envelope versioning, `ArcSwap`-based reopen, bounded
    lane queues, coalesce-on-overflow `InvalidationSink`,
    `record_checkpoint` wired through every persistence path.
    Reviewer pass folded in. Code state in `reference/sync.md`.
  - **P2-A1** (`crates/jmap/` Account impl): shipped behind a
    `sync` feature. `JmapAccount` + `JmapAccountFactory`,
    cursor envelope, discovery, inventory, changes, hydration,
    WebSocket push, blob fetch, mutations, and error mapping
    onto the recovery taxonomy. Unit tests cover cursor
    round-trip, capability building, and error classification.
  - **P2-A2** (`crates/imap/` Account impl): shipped.
    `ImapAccount` + `ImapAccountFactory` wired with pool,
    registry, cursor envelope, capabilities, inventory, changes,
    get, blob, mutation, push, and close submodules. QRESYNC
    negotiation (with iCloud server-ID downgrade, parse-failure
    session disable, and known-UID baseline seeding via
    `UID SEARCH ALL`), CONDSTORE-only fallback, per-folder
    UIDVALIDITY-scoped modseq cache, and opportunistic
    `STORE UNCHANGEDSINCE` in flag and destroy mutations all
    landed. Mutation concurrency is still advertised as
    `MutationConcurrency::None`: the modseq cache is
    opportunistic and may be cold, so promoting to `StateBased`
    would let the engine assume UNCHANGEDSINCE is always wired
    up when it is not. The engine's read-back-after-retry path
    remains the lost-update safety net.
  - **P2-A3** (`crates/gmail/` Account impl): shipped.
    `GmailAccount` + `GmailAccountFactory` with cached
    `getProfile` historyId seed, history-based changes,
    inventory, hydration, watch/stop push, mutations, blobs,
    recovery mapping, and Gmail flag canonicalization.
  - **P2-A4** (`crates/graph/` Account impl): shipped.
    `GraphAccount` + `GraphAccountFactory` with cursor
    envelopes, recursive mail-folder discovery, inventory,
    changes, hydration, blob fetch, mutation, push, and an EWS
    streaming notification parser plus request builders. Unit
    tests cover cursor round-trip, EWS XML parse, capability
    shape, error mappings, and membership derivation.

`brokkr check` passes workspace-wide. The four protocol crates
now host their Phase 2 `Account` impls under
`crates/{jmap,imap,gmail,graph}/src/`; pre-Phase-2 surfaces
remain alongside them.

## Outstanding work

- **Phase 3: Reconcile and validate.** Round out per-protocol
  conformance tests, add a minimal cross-crate conformance
  assertion in `bifrost-sync`, and write `reference/{jmap,imap,
  gmail,graph}.md` to describe the account-layer code. Full
  spec under **Phase 3** below.
- **Phase 3.1: Dependency and shared-crate-wiring audit.** The
  structural shoehorn left `bifrost-jmap`, `bifrost-gmail`, and
  `bifrost-graph` each maintaining their own HTTP client,
  bearer-token storage, and URL-encoding path instead of
  routing through `bifrost-net`. Workspace dependency pins are
  also inconsistent (per-crate `async-stream` / `thiserror` /
  `futures-util`, `log` in gmail / graph while everyone else
  uses `tracing`). Full spec in
  `plans/dependency-audit.md`. Runs before Phase 3.5; the
  structural fix dissolves most of Phase 3.5's
  `Shared -> protocol duplication` bucket.
- **Phase 3.5: Post-shoehorn surface audit.** Trait shoehorns
  leave dead and duplicated code in their wake. Audit each
  crate that was modified to host the `Account` / `AccountFactory`
  traits, `bifrost-net`, or `bifrost-types` for items that can
  be made private, deleted outright, or collapsed into a single
  way of doing things. Full spec under **Phase 3.5** below.
- **Phase 4: Error model convergence.** Reconcile the
  per-crate error stories (rich `Response`-carrying variants in
  `bifrost-smtp`, separate models in `bifrost-jmap` and
  `bifrost-imap`) into a shared shape that gives ratatoskr a
  unified error-handling pattern. The open question framing
  lives in `plans/error-model-convergence.md`; that doc needs
  to be expanded into an agent brief (decision + file
  ownership + exit criteria) before launch.

## Reference material

`research/{gmail,graph,imap,jmap,provider-sync}/` contains the
existing ratatoskr implementations being replaced. Not part of
the workspace build. Useful as precedent when writing engine
tests, reconciling protocol semantics, or describing the
account-layer code in the reference docs.

## Phase 3

### Sequencing

CONDSTORE/QRESYNC has merged (P3-A0). `reference/imap.md` has
been refreshed with the account-layer architecture, the
QRESYNC / CONDSTORE / Basic cursor strategy, the modseq cache,
and the rationale for keeping `MutationConcurrency::None`. The
remaining Phase 3 work (jmap / gmail / graph reference docs,
conformance test fill-in, cross-crate assertion in
`bifrost-sync`) proceeded in parallel and is now merged
modulo the deferred jmap tests.

### Test scope

Project rules cap bifrost tests at small deterministic unit
tests. No integration tests, no mock protocol servers, no
external harnesses. Phase 3 stays inside that envelope.

Three buckets:

1. **Per-protocol conformance.** Each crate gets unit tests for
   cursor envelope round-trip, capability shape, error
   classification to the recovery taxonomy, and scope-to-method
   wiring. Most of these already exist from P2; Phase 3 fills
   gaps and pins behavior the reviewer pass flagged.
2. **Cross-crate conformance.** A small assertion in
   `bifrost-sync` (or `bifrost-types`) that constructs each
   `AccountFactory`, verifies `dyn Account` object-safety, and
   confirms the four impls compose with `SyncEngine::attach` at
   the type level. Catches accidental trait-surface breakage
   that per-crate tests miss.
3. **No engine integration tests.** `bifrost-sync` keeps its
   existing fake-`Account` engine tests from P2-A6. Phase 3 does
   not drive the real four impls through the engine - that
   surface is exercised downstream in ratatoskr.

### Reference doc structure

`reference/net.md` and `reference/sync.md` are the exemplars.
Each new `reference/{jmap,imap,gmail,graph}.md` covers:

- Crate purpose and scope (1-2 paragraphs)
- Module layout under `src/account/` (or `src/sync/` for jmap)
- `Account` / `AccountFactory` shape and lifecycle
- Capabilities advertised and the rationale for each
- Cursor envelope: tag, version, payload, validation
- Per-scope inventory / changes / hydration approach
- Push mechanism and reconnect policy
- Mutation pipeline and replay-safety classification
- Error mapping to the recovery taxonomy
- Known limitations (e.g., Gmail blob ranges unsupported,
  Graph discovery limited to mail, IMAP mutation concurrency
  intentionally still `None` because the modseq cache is
  opportunistic)

### File ownership

P3-A0, P3-A2, P3-A3, P3-A4, and the orchestrator cross-crate
conformance are merged. P3-A1's reference doc is merged; its
conformance tests are deferred behind in-flight sync-engine work
in `crates/jmap/src/sync/`. `brokkr check` was held green across
the phase. Phase 3 is functionally complete; the deferred jmap
tests will be picked up after the coworker's sync-engine work in
that tree settles.

- **P3-A0 (CONDSTORE/QRESYNC)**: merged. Code lives across
  `crates/imap/src/account/{factory,capabilities,changes,
  folder_registry,inventory,mutate,push,get,blob,mod,pool}.rs`.
  Mutation concurrency stays `MutationConcurrency::None` by
  design; see the P2-A2 note above.
- **P3-A1 (jmap)**: reference doc merged. `reference/jmap.md`
  now appends an "Account layer" section describing
  `JmapAccount` / `JmapAccountFactory`, the hand-rolled cursor
  envelope, per-scope inventory / changes / hydration, WebSocket
  push + `ReconnectPolicy`, the mutation pipeline, and the
  recovery-taxonomy error mapping. Conformance test additions
  inside `crates/jmap/src/sync/` are deferred until the sync
  engine's `InventoryPartition` rework settles in that tree.
- **P3-A2 (imap)**: merged. `reference/imap.md` describes the
  account-layer architecture, the QRESYNC / CONDSTORE / Basic
  cursor strategy, the modseq cache lifecycle, and the
  mutation-concurrency rationale. Conformance test additions
  inside `crates/imap/src/account/` modules are still
  outstanding.
- **P3-A3 (gmail)**: merged. `reference/gmail.md` covers the
  history-id seeded sync, Cloud Pub/Sub push with renewer task,
  and the recovery-taxonomy error mapping. Conformance tests
  added across `crates/gmail/src/account/{cursor,capabilities,
  recovery,inventory}.rs` cover cursor round-trip, capability
  shape, error classification, and scope-to-method wiring.
- **P3-A4 (graph)**: merged. `reference/graph.md` covers the
  delta-token sync, webhook + EWS streaming push (with the
  renewal health worker that just landed), the cursor envelope
  validation, and the recovery-taxonomy error mapping.
  Conformance tests added across
  `crates/graph/src/account/{cursor,capabilities,error,
  inventory}.rs` cover cursor round-trip, capability shape,
  error classification, and scope-to-method wiring.
- **Orchestrator**: cross-crate conformance assertion merged.
  `crates/sync/tests/cross_crate_conformance.rs` constructs each
  of the four `AccountFactory` impls (`ImapAccountFactory`,
  `JmapAccountFactory`, `GmailAccountFactory`,
  `GraphAccountFactory`), witnesses `dyn Account` and
  `dyn AccountFactory` object-safety, and verifies that each
  factory composes with `SyncEngine::attach` at the type level
  by constructing the returned future without polling it (so no
  network work runs). The four protocol crates enter as
  `[dev-dependencies]` of `crates/sync`; JMAP is enabled with
  the `sync` feature.

### Sync-engine follow-ups (resolved)

The follow-up list at the bottom of `reference/sync.md` has been
worked off. Notable items landed alongside Phase 3:

- Protocol-neutral `InventoryPartition` and `InventoryPartitioning`
  added to `bifrost-types`, with default full-pass behavior on
  `Account`.
- Sync backfill plans and runs real partitions, calls
  `inventory_partition_stream`, and records durable backfill
  checkpoints through `SyncControl`.
- JMAP Email inventory supports open-ended page partitions.
- Push forwarding runs for every push-capable account, not only
  in-process push.
- Gmail Pub/Sub publishes health events and applies retry
  backoff.
- Graph webhook subscriptions stream health events and run a
  renewal health worker.
- `reference/sync.md` describes the resulting behavior; the old
  follow-up section has been removed.

### Exit criteria

Phase 3 is done when all of these hold:

- CONDSTORE/QRESYNC merged (done). `STORE UNCHANGEDSINCE` is
  wired into the IMAP mutation pipeline opportunistically;
  mutation concurrency stays `MutationConcurrency::None` by
  design because the modseq cache is cold-startable. Any future
  flip to `StateBased` would need a separate plan.
- All four `reference/{jmap,imap,gmail,graph}.md` files exist
  and match the section list above (done).
- Per-protocol conformance tests cover cursor envelope round-
  trip, capability shape, error classification, and scope-to-
  method wiring for imap, gmail, and graph (done). jmap
  conformance tests are deferred behind in-flight engine work
  in `crates/jmap/src/sync/`.
- Cross-crate conformance assertion in `bifrost-sync` compiles
  and passes for all four factories (done).
- `brokkr check` clean workspace-wide (done; held green
  throughout Phase 3).

## Phase 3.1

Dependency and shared-crate-wiring audit. The big-ticket finding
is that `bifrost-net` was shoehorned into the workspace but never
adopted by `bifrost-jmap`, `bifrost-gmail`, or `bifrost-graph`:
each of those still maintains its own `reqwest::Client`, its own
bearer-token storage (plain `String`, not `Zeroizing<String>`),
and its own URL-encoding path. Five smaller findings sit around
that core: per-crate `async-stream` / `thiserror` / `futures-util`
pins that should be workspace-pinned, `log` in gmail / graph
while everyone else is on `tracing` (events silently dropped if
the downstream installs only `tracing-subscriber`), the
abandoned `urlencoding` crate that should be replaced with
`percent-encoding`, and a handful of cosmetic odds and ends.

Full spec, per-finding methodology, file ownership, suggested
order of operations, and exit criteria live in
`plans/dependency-audit.md`.

Sequencing: Phase 3.1 runs before Phase 3.5. The structural fix
(routing the three HTTP-based crates through `bifrost-net`)
dissolves most of Phase 3.5's `Shared -> protocol duplication`
bucket and the secret-handling half of Phase 3.5's two-ways
audit, so doing the smaller surface-level work first would just
create rebase churn.

## Phase 3.5

Post-shoehorn surface audit. `Account` / `AccountFactory`,
`bifrost-net`, and the whole of `bifrost-types` were grafted
onto pre-existing crates. Shoehorns leave behind three kinds of
debris:

1. **Newly-internal public items.** Code that was `pub` before
   the shoehorn but now only has internal callers.
2. **Duplicated surfaces, protocol → shared.** Types or helpers
   the protocol crate carried that now have a canonical home in
   `bifrost-types` or `bifrost-net`. Two-`AccountId`-types,
   parallel `ProblemDetails`, per-crate retry-classifier shapes,
   etc.
3. **Duplicated surfaces, shared → protocol.** The reverse
   direction. `bifrost-net` likely now has a hardened HTTP
   transport with retries, rate-limit handling, and OAuth refresh
   that supersedes whatever each HTTP-based protocol crate built
   for itself pre-shoehorn. `bifrost-sync` likely has canonical
   shapes (idempotency keys, mutation outcome enums, cursor
   envelope versioning, checkpoint persistence) that some
   protocol crates still re-implement locally.
4. **Two ways to accomplish the same thing.** Every Account
   method has a pre-Account equivalent (a direct method on the
   underlying `Client` or driver). Some of those equivalents are
   still useful for non-engine consumers. Some are not. Decide
   per case; eliminate the dead ones.

### Methodology

For each crate, in this order:

1. `pub` audit. Every `pub` / `pub use` in `lib.rs` and module
   roots gets justified or downgraded. Use `cargo doc --no-deps`
   output and grep for `use <crate>::Name` patterns in the rest
   of the workspace to find external consumers. Anything with no
   external consumer becomes `pub(crate)` or `pub(super)`.
2. Type-duplication audit, both directions. For each type
   definition in the crate, ask whether `bifrost-types` already
   has it, or should. For each `bifrost-types` type, ask whether
   any protocol crate has a sibling. Resolve toward the shared
   home unless there is a documented protocol-specific reason
   for the divergence.
3. Shared-supersedes-local audit. Walk each protocol crate's
   transport / retry / rate-limit / OAuth-refresh code and ask
   whether `bifrost-net` now does the same job better. Walk each
   protocol crate's mutation-result / idempotency-key /
   cursor-envelope code and ask whether `bifrost-sync` (via
   `bifrost-types`) now does the same job better.
4. Two-ways audit. For each Account method, identify the
   pre-Account equivalent. Decide: keep both with documented
   non-overlapping use cases, deprecate the legacy path, or
   delete it.
5. Dead-code audit. Items with no callers inside or outside the
   crate.

### Sequencing

One agent per crate, six in total, run-in-parallel-safe because
each agent owns one crate's `src/` tree and `Cargo.toml`.
`bifrost-net` and `bifrost-types` audits should run *last*: the
protocol-crate audits will surface requests of the form "I want
to use `bifrost-types::X` but `bifrost-types` doesn't expose it"
or "this would be cleaner as a `bifrost-net::Y`". Collecting
those signals first lets the shared-crate audits delete dead
exports and add new ones in one pass.

### File ownership

- **P3.5-A0 (imap)**: `crates/imap/src/` and `crates/imap/Cargo.toml`.
- **P3.5-A1 (jmap)**: `crates/jmap/src/` and `crates/jmap/Cargo.toml`.
- **P3.5-A2 (gmail)**: `crates/gmail/src/` and `crates/gmail/Cargo.toml`.
- **P3.5-A3 (graph)**: `crates/graph/src/` and `crates/graph/Cargo.toml`.
- **P3.5-A4 (smtp)**: `crates/smtp/src/` and `crates/smtp/Cargo.toml`.
  Smaller scope: bifrost-smtp was not modified by the Account
  shoehorn, but bifrost-types and bifrost-net likely still
  apply. Quick sweep.
- **P3.5-A5 (net + types)**: `crates/net/src/`, `crates/types/src/`,
  and both `Cargo.toml`s. Runs after the protocol-crate agents
  report. Folds in the cross-crate signals they surface.

Agents must NOT edit any crate other than their assigned one.
Cross-crate findings get reported to the orchestrator for
P3.5-A5 to fold in.

### Exit criteria

Phase 3.5 is done when all of these hold:

- Every `pub` / `pub use` in every crate has an external
  consumer, or a one-line `pub` justification in the surrounding
  code.
- No type defined in a protocol crate has a sibling in
  `bifrost-types` that means the same thing. Either the
  protocol-crate type goes (replaced by the `bifrost-types`
  one) or the divergence is documented.
- No protocol crate re-implements an HTTP transport / retry
  loop / OAuth refresh that `bifrost-net` already provides.
- No protocol crate re-implements an idempotency-key shape /
  mutation-outcome enum / checkpoint envelope that
  `bifrost-types` already provides.
- For every `Account` method on each impl, there is a
  documented reason the pre-Account equivalent still exists, or
  the equivalent has been removed.
- `brokkr check` remains clean under `--all-features`.

## Phase 4

Error model convergence across `bifrost-smtp`, `bifrost-jmap`,
`bifrost-imap`, and (now) the four account-layer error
taxonomies in the Phase 2 work.

Not yet planned at agent-launch depth. The open question framing
in `plans/error-model-convergence.md` (whether to converge, and
what convergence buys ratatoskr) needs to be resolved into a
concrete shape before file ownership and exit criteria can be
written. Expected predecessors of that fleshing-out:

- Survey the current error types in each protocol crate
  (`bifrost-{smtp,jmap,imap,gmail,graph}`) and the recovery
  taxonomy in `bifrost-types`. Decide what "converged" means in
  practice - a shared trait, a shared enum, or a documented
  pattern each crate implements independently.
- Decide whether `bifrost-smtp`'s rich `Response`-carrying shape
  is the target, a starting point, or out of scope (sync-layer
  errors and SMTP submission errors may not benefit from the
  same model).
- Decide whether `bifrost-types::AccountError` (the account-
  layer error already in use) is the convergence target or sits
  alongside protocol-native errors.

Once those decisions are taken, this section gets the same
treatment as Phase 3: sequencing, file ownership, exit criteria.

## Coordination rules

Apply to any agents launched against this codebase.

- All agents work in the same git tree. No worktrees.
- Agents read `AGENTS.md` and `CLAUDE.md` first.
- Agents do not run `brokkr`, `cargo`, or any build/test
  commands. Validation is the orchestrator's job between phases.
- Each agent's brief lists exact files it owns. No two agents
  ever own the same file.
- Subagents always run in the foreground.

## Phase gates and audit

Between phases, the orchestrator runs a 3-pass audit.

1. **Domain-specific verification.** Per agent, does the
   delivered work match the brief? Files exist, types exist,
   trait methods exist, behavior matches the plan.
2. **Cross-cutting reconciliation.** Does the new wiring
   actually dispatch? Does each protocol's `Account::open`
   compose with the engine? Are new types in `bifrost-types`
   used by at least one protocol, or are they dead weight?
3. **Editorial.** Clippy clean (workspace-wide, not -p),
   `brokkr fmt`, no orphan files, reference docs aligned with
   code.

Do not trust agent claims of completion. Verify existence +
wiring + behavior.
