# Orchestration: bifrost sync engine and protocol Account impls

The path from "plans landed" to "engine driving four Account
impls under tests." This document is operational, not
architectural. For design rationale see the per-area plans.

## State

Resolved decisions:

- **Workspace shape.** Carve-out crate `bifrost-types` hosts the
  `Account` trait and the entire shared sync surface (events,
  batches, capabilities, cursor types, blob handles). Protocol
  crates and `bifrost-sync` depend on it; it depends on nothing
  in the workspace. See `bifrost-sync.md` -> Workspace policy.
- **Dispatch.** `dyn Account` reachable via type-erased streams
  and a concrete `OpaqueChangeState` tagged with protocol +
  envelope version. See `account-trait-shape.md` -> Q1.
- **Send + Sync.** IMAP is pool-shaped from day one. The pool is
  the public `Account` type; existing `ImapConnection` becomes
  the checkout unit. See `account-trait-shape.md` -> Q2.
- **Lifecycle.** Consumer registers an `AccountFactory`; engine
  owns the open `Arc<dyn Account>` and calls the factory on
  reopen. See `account-trait-shape.md` -> Q3.
- **InvalidationSink.** Channel-based. See `bifrost-sync.md`.
- **TLS.** `native-tls` workspace-wide. No rustls dependency.
  See `bifrost-net.md` -> TLS.

Plans landed: `account-trait.md`, `account-trait-shape.md`,
`sync-engine.md`, `bifrost-net.md`, `bifrost-sync.md`,
`imap/backpressure.md`.

Plans outstanding: four per-protocol Account-impl plans (one
each for JMAP, IMAP, Gmail, Graph).

## Phase 0 - Plan closure (4 agents, parallel)

Write a per-protocol Account-impl plan for each crate. Each plan
extends its existing `streaming.md` (or for IMAP, sits alongside
`condstore-qresync.md` and `backpressure.md`) and pins how the
protocol's primitives map to `Account` trait methods, what its
`CursorScope` / `MembershipScope` shapes look like, which
capability flags it advertises, and how push wires through to
the engine's `InvalidationSink`.

Agents and outputs:

- **P0-A1**: `plans/jmap/account-impl.md`
- **P0-A2**: `plans/imap/account-impl.md` (must reference
  `backpressure.md` and `condstore-qresync.md` for the streaming
  contract; does not re-spec them)
- **P0-A3**: `plans/gmail/account-impl.md`
- **P0-A4**: `plans/graph/account-impl.md`

File-disjoint. May run concurrent with Phase 1 (which touches
only `crates/`).

## Phase 1 - Foundation skeletons (2 agents, parallel)

Two new crates, types and traits and error vocabulary only, no
behavior. End-of-phase gate: workspace compiles, public surface
frozen for downstream agents.

- **P1-A1**: `crates/types/` -> `bifrost-types`. The trait crate.
  Owns: `Account`, `AccountFactory`, `SyncEvent`, `Batch`,
  `Change` (sum of `ObjectChange` + `ScopeChange`), `Projection`,
  `BlobHandle`, all capability types, `CursorScope`,
  `MembershipScope`, `OpaqueChangeState`, `ChangeCursor`,
  `RecoveryClass`, `WatchEvent`, `InvalidationHint`,
  `InvalidationSink`, `MutationResult`, `IdempotencyKey`,
  `Priority`, `Control`, `Checkpoint`, `Progress`,
  `HydratedObject`, `InventoryEntry`, error taxonomy.
  Zero workspace deps. No protocol-specific code. No engine
  code. Type aliases for `AccountStream<T>` and
  `AccountFuture<T>` here.
- **P1-A2**: `crates/net/` -> `bifrost-net`. Skeleton only.
  Owns: `Net`, `NetConfig`, `AccountNet`, `RequestBuilder`,
  `TokenSource`, `OAuthRefresher` (trait + single-flight
  scaffold), `RetryPolicy`, `RateLimitGovernor`,
  `BandwidthMeter`, traceparent injection. Wired up with reqwest
  on native-tls, but no real refresh logic, no real retry loop;
  trait surface only with `todo!()`-style stubs where
  appropriate. Unit tests for the retry decision function and
  token-bucket math allowed but not required.

Gate: `brokkr check` passes workspace-wide. `bifrost-types` and
`bifrost-net` public APIs are read-only from Phase 2 onward.

## Phase 2 - Parallel implementation (6 agents)

File-disjoint, one agent per crate. Each agent owns its crate's
`src/` exclusively; no two agents touch the same file.

- **P2-A1**: `crates/jmap/` -> JMAP `Account` impl. Implements
  the trait against `plans/jmap/account-impl.md`. Consumes
  `bifrost-net` for HTTP. Soft-stub `bifrost-net` calls if
  P2-A5 hasn't landed it yet (the public surface is frozen, so
  the call sites are stable).
- **P2-A2**: `crates/imap/` -> IMAP backpressure rework +
  CONDSTORE/QRESYNC + `Account` impl. Sequenced internally:
  backpressure rework first (per `imap/backpressure.md`), then
  CONDSTORE/QRESYNC implementation (per
  `imap/condstore-qresync.md`), then `Account` wiring (per
  `imap/account-impl.md`). Single agent because `dispatch.rs` is
  shared across all three sub-tracks.
- **P2-A3**: `crates/gmail/` -> Gmail `Account` impl.
- **P2-A4**: `crates/graph/` -> Graph `Account` impl + EWS
  streaming notification parser.
- **P2-A5**: `crates/net/` -> `bifrost-net` full impl. Fills in
  retry loop, single-flight refresher, token-bucket governor,
  bandwidth meter, traceparent. Soft dep: A1/A3/A4 want this
  usable but can stub minimal HTTP clients until it lands.
- **P2-A6**: `crates/sync/` -> `bifrost-sync` engine internals.
  Owns scheduler, multiplexer, backfill partitioner, push
  reconciler, mutation pipeline, observability, checkpoint
  envelope. Depends only on `bifrost-types`. Could sub-split
  into multiplexer / backfill / push / mutate / telemetry as
  separate agents on separate module files; do not split unless
  A6 is the long pole, because intra-crate coordination across
  five agents costs more than it buys.

Hard rule: no agent touches another's `src/`. Cross-crate edits
(e.g., A6 needs a method on `bifrost-types` it didn't think of
in Phase 1) escalate to the orchestrator, who arbitrates the
change in a brief Phase 1.5 commit before Phase 2 resumes on
the affected agent.

## Phase 3 - Reconcile and validate (1-2 agents)

Wire `bifrost-sync` against the four Account impls under unit
tests (no live servers per `AGENTS.md`). Workspace-wide
`brokkr check` cleanup. Update `reference/*.md` to match shipped
code. Delete the outdated `streaming.md` files that the
`account-impl.md` plans replaced.

## Concurrency ceiling

|  Window         | Concurrent agents | Bottleneck                                |
|---  |---  |---  |
| Phase 0 + Phase 1 (overlap)  | 6 (4 plan + 2 skeleton)  | Plan agents and crate agents are file-disjoint  |
| Phase 1 alone  | 2  | Two new crates, fully independent  |
| Phase 2  | 6  | One per crate, file-disjoint  |
| Phase 3  | 1-2  | Mostly integration  |

Peak useful concurrency is six. More agents than that means
sub-splitting `bifrost-sync` engine modules in Phase 2, which
should be avoided unless A6 is clearly the long pole.

## Coordination rules

- All agents work in the same git tree. No worktrees.
- Agents read `AGENTS.md` and `CLAUDE.md` first.
- Agents do not run `brokkr`, `cargo`, or any build/test
  commands. Validation is the orchestrator's job between phases.
- Each agent's brief lists exact files it owns. No two agents
  ever own the same file.
- Subagents always run in the foreground.

## Phase gates and audit

Between phases, orchestrator runs a 3-pass audit:

1. **Domain-specific verification.** Per agent, does the
   delivered work match the brief? Files exist, types exist,
   trait methods exist, behavior matches the plan.
2. **Cross-cutting reconciliation.** Does Phase 2's wiring
   actually dispatch? Does each protocol's `Account::open`
   compose with the engine? Are new types in `bifrost-types`
   used by at least one protocol, or are they dead weight?
3. **Editorial.** Clippy clean (workspace-wide, not -p),
   `brokkr fmt`, no orphan files, reference docs aligned with
   code.

Do not trust agent claims of completion. Verify existence +
wiring + behavior.

---

# Agent briefs

Drafts for the immediately-actionable agents: Phase 0 (4 plan
agents) and Phase 1 (2 skeleton agents). Phase 2 and Phase 3
briefs are written when their phase gate opens.

## P0-A1: JMAP Account-impl plan

**Subagent type**: `general-purpose`
**Output file**: `plans/jmap/account-impl.md` (write only this
file; do not touch any other file)
**Required reading**: `AGENTS.md`, `CLAUDE.md`,
`plans/account-trait.md`, `plans/account-trait-shape.md`,
`plans/sync-engine.md`, `plans/bifrost-sync.md`,
`plans/jmap/streaming.md`, `reference/jmap.md`, the existing
`crates/jmap/src/` tree (read for module shape and capability
handling).

**Brief**: Write a detailed plan for implementing the `Account`
trait against `bifrost-jmap`. Pin: how `CursorScope` maps to
JMAP accounts and types; how `MembershipScope` maps to mailbox
membership; what `OpaqueChangeState` carries on the wire
(StateString from `Email/changes`); how WebSocket push wires to
`push_stream`; how `push_subscribe` / `push_unsubscribe` use
`PushSubscription/*`; concrete `AccountCapabilities` field
values for JMAP; what `inventory_stream` does on first open
versus on `RecoveryClass::CapabilityChanged`; how
`bulk_set_flags` maps to `Email/set` with `ifInState`; cursor
versioning policy (when `envelope_version` ticks); error mapping
from JMAP problem documents to `RecoveryClass`. Reference, do
not re-spec, the trait shape in `account-trait.md`. End with a
short risks/opens list, not new design.

Subagents do not run shell commands. Output is one markdown
file.

## P0-A2: IMAP Account-impl plan

**Subagent type**: `general-purpose`
**Output file**: `plans/imap/account-impl.md` (write only this
file)
**Required reading**: `AGENTS.md`, `CLAUDE.md`,
`plans/account-trait.md`, `plans/account-trait-shape.md`,
`plans/sync-engine.md`, `plans/bifrost-sync.md`,
`plans/imap/backpressure.md`,
`plans/imap/condstore-qresync.md`, `reference/imap.md`, the
existing `crates/imap/src/` tree (especially `driver/`,
`connection/`, and any existing pool sketch).

**Brief**: Write the IMAP `Account` impl plan. This is the
heaviest of the four because the surface composes
`backpressure.md` and `condstore-qresync.md` (do not re-spec
either; cite them). Pin: pool shape (the `Account` is the pool,
each driver is a checkout unit, IDLE owns one checkout while
backfill / reconcile run on other checkouts); per-folder
cursor state (UIDVALIDITY + HIGHESTMODSEQ vs UID-list digest on
Basic); three-state cursor lifecycle for QRESYNC -> CONDSTORE ->
Basic downgrade; `MembershipScope::Folder` derivation from
LIST; how IDLE feeds `push_stream`; how `bulk_set_flags` uses
`UID STORE` with optional `UNCHANGEDSINCE`; how
`open_blob_range` uses `BODY[<section>]<offset.length>`;
`AccountCapabilities` field values across the three capability
tiers; sequencing within the crate (backpressure first,
CONDSTORE second, Account third). Risks/opens at the end.

Subagents do not run shell commands.

## P0-A3: Gmail Account-impl plan

**Subagent type**: `general-purpose`
**Output file**: `plans/gmail/account-impl.md` (write only this
file)
**Required reading**: `AGENTS.md`, `CLAUDE.md`,
`plans/account-trait.md`, `plans/account-trait-shape.md`,
`plans/sync-engine.md`, `plans/bifrost-sync.md`,
`plans/gmail/streaming.md`, `reference/` (Gmail reference if
present; otherwise read `crates/gmail/src/` for current shape).

**Brief**: Write the Gmail `Account` impl plan. Pin: single
`CursorScope::Account`, with `MembershipScope::Label` for label
membership; how `history.list` maps to `changes_stream`; how
`messages.list` pagination maps to `inventory_stream`; how
Pub/Sub `users.watch` maps to `push_subscribe` (out-of-process
delivery, `push_in_process = false`, consumer feeds engine's
`InvalidationSink`); how Gmail historyId expiration drives a
`RecoveryClass::CapabilityChanged` event; idempotency key
strategy (Gmail's per-request retry tokens); flag canonicalization
for Gmail label state; AccountCapabilities field values. Risks
and opens at the end.

Subagents do not run shell commands.

## P0-A4: Graph Account-impl plan

**Subagent type**: `general-purpose`
**Output file**: `plans/graph/account-impl.md` (write only this
file)
**Required reading**: `AGENTS.md`, `CLAUDE.md`,
`plans/account-trait.md`, `plans/account-trait-shape.md`,
`plans/sync-engine.md`, `plans/bifrost-sync.md`,
`plans/graph/streaming.md` (if present), `crates/graph/src/`
for current shape.

**Brief**: Write the Graph `Account` impl plan. Pin: per-folder
`CursorScope::FolderType` (Graph delta is per-folder, not
account-wide); `MembershipScope::Folder` derived from
`parentFolderId`; how Graph `/subscriptions` maps to
`push_subscribe` and `/subscriptions/{id}` DELETE to
`push_unsubscribe` (`push_in_process = false`, webhook
delivery); EWS streaming notification parser as the
alternative push path on tenants where Graph subscriptions are
restricted (scope, format, lifecycle); delta token versioning;
ETag-based concurrency (`If-Match`) and replay safety;
`AccountCapabilities` field values. Risks and opens at the end.

Subagents do not run shell commands.

## P1-A1: bifrost-types skeleton

**Subagent type**: `general-purpose`
**Output**: New crate `crates/types/` (Cargo.toml + src/).
Update workspace `Cargo.toml` to include the new member and
hoist any new deps into `[workspace.dependencies]`.
**Files this agent owns** (and only these):
- `crates/types/Cargo.toml`
- `crates/types/src/lib.rs`
- `crates/types/src/account.rs` (the trait)
- `crates/types/src/cursor.rs` (cursor and scope types)
- `crates/types/src/capabilities.rs`
- `crates/types/src/events.rs` (SyncEvent, Batch, Change,
  Progress, Checkpoint, WatchEvent, InvalidationHint, etc.)
- `crates/types/src/blob.rs` (BlobHandle, BlobCapabilities,
  DownloadOpts)
- `crates/types/src/mutation.rs` (MutationResult,
  IdempotencyKey, FlagSet, FlagOp, projection, hydrated, etc.)
- `crates/types/src/error.rs` (top-level Error and
  RecoveryClass)
- `Cargo.toml` (workspace root - members list and dep hoisting)
- `Cargo.lock` (regenerated; do not hand-edit)
**Required reading**: `AGENTS.md`, `CLAUDE.md`,
`plans/account-trait.md`, `plans/account-trait-shape.md`,
`plans/sync-engine.md`, `plans/bifrost-sync.md`,
workspace root `Cargo.toml`.

**Brief**: Land `bifrost-types` as a skeleton crate. Define
every type and trait the plans reference. No protocol-specific
code. No engine code. Zero workspace deps; only `tokio`,
`futures`, `bytes`, `serde` (if needed for opaque-bytes
boundary), and similar foundational crates. Type aliases
`AccountStream<T>` and `AccountFuture<T>` live here. Trait
methods compile as `todo!()` stubs - the trait surface must be
dyn-safe per `account-trait-shape.md` Q1. Public re-exports go
through `lib.rs`. Workspace-wide clippy lints inherit from
workspace root.

Do not implement any logic. Do not write tests beyond compile
checks (a `#[allow(dead_code)] fn _dyn_safe(_: &dyn Account)`
helper proves dyn-safety at compile time).

Subagents do not run `cargo`, `brokkr`, or any build commands.
The orchestrator runs `brokkr check` after this agent reports
done.

## P1-A2: bifrost-net skeleton

**Subagent type**: `general-purpose`
**Output**: New crate `crates/net/` (Cargo.toml + src/).
**Files this agent owns** (and only these):
- `crates/net/Cargo.toml`
- `crates/net/src/lib.rs`
- `crates/net/src/config.rs` (NetConfig)
- `crates/net/src/net.rs` (Net, AccountNet)
- `crates/net/src/request.rs` (RequestBuilder)
- `crates/net/src/auth.rs` (TokenSource, OAuthRefresher trait)
- `crates/net/src/retry.rs` (RetryPolicy)
- `crates/net/src/rate.rs` (RateLimitGovernor)
- `crates/net/src/bandwidth.rs` (BandwidthMeter)
- `crates/net/src/error.rs`
- `Cargo.toml` (workspace root - members list and dep hoisting,
  coordinate with P1-A1 by NOT touching the same lines; if you
  collide, escalate)
**Required reading**: `AGENTS.md`, `CLAUDE.md`,
`plans/bifrost-net.md` (every section), workspace root
`Cargo.toml`.

**Brief**: Land `bifrost-net` as a skeleton. Every type and
trait from `bifrost-net.md` exists with the right signature.
`reqwest` is wired with `default-features = false, features =
["native-tls", "stream", "json"]`. `native-tls` and `httpdate`
are hoisted to `[workspace.dependencies]`. `webpki-roots` and
`rustls` are NOT added.

Implementation depth: just enough to compile. Single-flight
refresher is a struct with a `Mutex<RefreshState>` field but
`refresh()` returns `todo!()`. Retry loop exists as a function
signature returning the right type but the body is `todo!()`.
Token bucket has the right fields (`tokens`, `last_refill`,
`refill_rate`) but `acquire()` is `todo!()`. The point is to
freeze the public surface; behavior comes in Phase 2.

No tests required beyond compile checks. No HTTP requests, no
network code that actually runs.

Subagents do not run `cargo`, `brokkr`, or any build commands.

---

## Coordination note on the workspace `Cargo.toml`

Both P1 agents need to add their crate to `[workspace.members]`
and hoist their deps. To avoid a merge conflict on this one
file:

- P1-A1 (bifrost-types) edits `Cargo.toml` first.
- P1-A2 (bifrost-net) edits second, after the orchestrator
  confirms P1-A1 is done.

OR: orchestrator pre-stages `Cargo.toml` with both members and
all hoisted deps before either agent runs, and both agents are
told "do not touch the workspace Cargo.toml; it's already
prepared." The pre-stage approach is cheaper and recommended.

If pre-staging: workspace root `Cargo.toml` gets a one-shot
edit by the orchestrator before launching P1 agents, adding
`crates/types` and `crates/net` to `[workspace.members]` and
hoisting `native-tls` and `httpdate` to
`[workspace.dependencies]`. Then both agents run truly in
parallel with zero shared files.
