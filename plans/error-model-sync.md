# Error model: bifrost-sync implementation plan

**Scaffold.** Fill in the details by reading `reference/sync.md` and
the `crates/sync/` source. Sections marked `<TODO>` are placeholders.

This is **Phase 2.3** of `plans/error-model-roadmap.md`, landing
last in Phase 2 because it consumes `RecoveryClass` from all five
protocol crates and owns the `SyncEvent::Fatal` →
`SyncEvent::Terminated(AccountError)` rename that ripples through
their emission paths.

## Scope

`bifrost-sync` is the sync engine: scheduler, multiplexer,
partitioned backfill, push reconciler, mutation pipeline, checkpoint
envelope versioning, scope lifecycle.

What changes:

- Engine call sites consume the new `RecoveryClass` via four
  mutually-exclusive helpers (`is_retryable`,
  `requires_reconciliation`, `requires_engine_action`,
  `is_terminal`).
- `RecoveryClass::Engine(EngineDirective)` becomes the dispatch
  surface for engine-control directives that previously lived as
  top-level `RecoveryClass` variants (`RestartScope`,
  `RestartAccount`, `DowngradeStrategy`, etc.).
- Stream termination event renames: `SyncEvent::Fatal(Fatal)` →
  `SyncEvent::Terminated(AccountError)`. The carried payload is
  now the full `AccountError`, not the old `Fatal` struct.
- `Fatal::try_from(&account_error)` is the boundary check for
  "engine has nothing more to try" code paths.
- Mutation pipeline consumes `Retry(AfterStateRefresh)` for
  concurrency conflicts, `Reconcile(_)` for uncertain outcomes,
  `Engine(_)` for restart directives.

## Dependencies

- Phase 1 (`plans/error-model-types.md`).
- Phase 2.1 (`plans/error-model-net.md`).
- Phase 2.2 (all five protocol crates) — every crate that emits
  `AccountError` must have stable shapes before sync can be
  rewritten to consume them.

## Files to modify

`<TODO>` — discover. Likely candidates based on `reference/sync.md`:

- the scheduler (per-account / per-scope retry budget logic)
- the multiplexer (where streams from `changes_stream`,
  `inventory_stream`, etc. are demuxed and recovery decisions
  taken)
- the partitioned backfill driver (where `Engine(RestartScope)`
  and `Engine(DowngradeStrategy)` produce flow changes)
- the push reconciler (where push-derived invalidations interact
  with cursor state)
- the mutation pipeline (where idempotency and retry are decided)
- the checkpoint envelope code (where
  `EngineDirective::SchemaIncompatible` is consumed)
- the scope lifecycle handler (where
  `EngineDirective::CapabilityChanged` triggers stream reopen)

## Files to delete

`<TODO>` — discover. Anything that hand-maps the old
`RecoveryClass` to engine actions; the new four-helper API replaces
those.

## Engine consumption surface

The engine's primary access pattern is reading `RecoveryClass` off
an `AccountError` and dispatching:

```rust
match err.recovery() {
    RecoveryClass::Retry(advice) => schedule_retry(advice),
    RecoveryClass::Reconcile(advice) => begin_reconciliation(advice),
    RecoveryClass::Engine(directive) => apply_directive(directive),
    _terminal => {
        if let Ok(fatal) = Fatal::try_from(err.clone()) {
            surface_terminal(fatal);
        }
    }
}
```

Or, using the four-helper API:

```rust
if err.recovery().is_retryable() {
    let RecoveryClass::Retry(advice) = err.recovery() else { unreachable!() };
    schedule_retry(advice);
} else if err.recovery().requires_reconciliation() {
    // ...
} else if err.recovery().requires_engine_action() {
    // ...
} else {
    // terminal
}
```

The pattern is the engine's choice; the public API supports both.

## `apply_directive` shape

```rust
fn apply_directive(directive: &EngineDirective, ctx: &EngineContext) {
    match directive {
        EngineDirective::RestartScope(scope) => restart_scope(scope, ctx),
        EngineDirective::RestartAccount => restart_account(ctx),
        EngineDirective::DowngradeStrategy(downgrade) => apply_downgrade(downgrade, ctx),
        EngineDirective::DowngradeCapabilityForScope(scope) => drop_capability(scope, ctx),
        EngineDirective::SchemaIncompatible => clear_cursor_state(ctx),
        EngineDirective::CapabilityChanged { delta } => reopen_account(delta, ctx),
        EngineDirective::OperatorOverrideRequired { reason } => surface_operator_alert(reason, ctx),
    }
}
```

Each engine action is a function the sync engine already has, just
keyed off the directive enum.

## `SyncEvent::Terminated` consumption

Every stream the engine consumes can emit
`SyncEvent::Terminated(AccountError)`. The engine reads
`error.recovery()` to decide:

- `Retry` / `Reconcile` / `Engine(_)`: schedule the appropriate
  action; the stream is closed but the engine may reopen.
- Terminal: surface via `Fatal::try_from(error)` to the operator
  notification path, mark the scope as failed in the scheduler.

## Notable concerns

- **Pre-1.0 trait stability.** Per `CLAUDE.md`, all crates are
  pre-1.0. `bifrost-sync` consumes the `Account` trait directly;
  Phase 3 will change trait signatures. Phase 2.3 (this plan)
  updates the engine to work with the *current* trait signatures
  still returning the old `Error` type — wait, no, the convergence
  plan rejects transitional types. Resolution:
  Phase 2.3 lands together with Phase 3's trait-signature change
  if it can't be done independently. **Re-evaluate during
  implementation whether Phase 2.3 and Phase 3 should merge.**
- **Stream termination paths.** Per `reference/sync.md`, multiple
  streams can terminate independently per account / per scope. The
  rename affects every `match` arm against `SyncEvent::Fatal` —
  discoverable by grep.
- **Mutation pipeline idempotency.** The pipeline currently has its
  own retry-after / fatal logic; per the convergence plan, this
  collapses to "consume `RecoveryClass` from the returned
  `AccountError`." The pipeline becomes thinner — it dispatches
  per the recovery class rather than re-classifying.
- **Read-back guard.** Per the existing model, `MutationOutcome::Skipped`
  signals the read-back guard caught a no-op. In the new model
  this becomes `BatchSuccess<MutationSuccess::Skipped>`. The
  engine's accounting (the `ReadbackSkipped` warning) still fires
  for observability.
- **Cursor lifecycle interactions.** `EngineDirective::RestartScope`
  reaches the cursor lifecycle code; verify the scope-id is
  preserved through the directive.

## Tests

Per project rules. ~10-15 tests:

- `RecoveryClass::Retry` dispatch: scheduler queues a retry.
- `RecoveryClass::Reconcile` dispatch: engine begins reconciliation
  (mock the reconciliation step; assert the engine called it).
- `RecoveryClass::Engine(RestartScope)`: scheduler tears down the
  scope and restarts.
- `RecoveryClass::Engine(DowngradeStrategy)`: strategy table updates.
- `RecoveryClass::Engine(CapabilityChanged)`: account reopens.
- `Fatal::try_from` produces `Ok` for terminal variants, `Err` for
  non-terminal.
- `SyncEvent::Terminated(AccountError)` is recognized by every
  stream consumer in the engine.
- Mutation pipeline: `ConcurrencyConflict` triggers an
  `AfterStateRefresh` retry path.

No live accounts. Construct synthetic `AccountError` values and
assert engine state transitions.

## Exit criteria

- Engine call sites consume `RecoveryClass` via the new four-helper
  API.
- `EngineDirective` dispatch covers all variants.
- `SyncEvent::Fatal` references replaced with
  `SyncEvent::Terminated(AccountError)` throughout sync code.
- `Fatal::try_from` used at every "operator notification" boundary.
- Mutation pipeline rebuilt around the new `RecoveryClass`.
- Old hand-rolled `RecoveryClass` → engine-action mapping deleted.
- `cargo check -p bifrost-sync` clean.
- Workspace `brokkr check` clean (this is Phase 2's final gate).
- Per-crate tests pass.

## Discovery items for the agent

`<TODO>`:

1. Where the engine currently consumes `RecoveryClass` (the set of
   call sites).
2. How `SyncEvent::Fatal` flows through the multiplexer.
3. Whether the scheduler's retry budget logic depends on the old
   `RecoveryClass::Retry { after: Duration }` shape; update to
   `RetryAdvice` with `min_delay` and `not_before`.
4. The mutation pipeline's current idempotency / retry surface
   (the convergence plan implies it has hand-rolled recovery
   classification).
5. Whether `bifrost-sync` references any provider-specific
   `WireCause` variants (it should not; this is a smell).
6. Whether Phase 2.3 can land independently of Phase 3's trait
   surface changes, or whether they must merge.
