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
  - **P2-A2** (`crates/imap/` Account impl): partial. Bounded
    streaming FETCH backpressure rework landed; `ImapAccount` +
    `ImapAccountFactory` wired with pool, registry, cursor
    envelope, capabilities, inventory, changes, get, blob,
    mutation, push, and close submodules. CONDSTORE/QRESYNC
    integration intentionally deferred: mutation concurrency is
    advertised as `None` and no STORE UNCHANGEDSINCE capability
    is claimed.
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

Two tracks remain before the engine drives the four Account
impls end-to-end.

- **Close P2-A2: CONDSTORE/QRESYNC.** Implement
  `STORE UNCHANGEDSINCE` and the QRESYNC resync path in
  `crates/imap/`, then flip IMAP's advertised mutation
  concurrency from `None` to the appropriate non-`None` value.
  Spec lives in `plans/imap/condstore-qresync.md`.
- **Phase 3: Reconcile and validate.** Wire `bifrost-sync`
  against the four `Account` impls under unit tests (no live
  servers per `AGENTS.md`). Workspace-wide `brokkr check`
  cleanup. Update `reference/{jmap,imap,gmail,graph}.md` to
  describe the new account-layer code.

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
