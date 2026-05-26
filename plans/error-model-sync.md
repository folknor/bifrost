# Error model: bifrost-sync implementation plan

This is Phase 2.3 of `plans/error-model-roadmap.md`, landing last in
Phase 2 because sync consumes the final `RecoveryClass`,
`EngineDirective`, `RetryAdvice`, and `ReconcileAdvice` shapes emitted
by every protocol crate.

Phase 2 is still on the intentionally broken feature branch. Author
the sync patch and deterministic tests, but do not run `brokkr`,
`cargo`, or `./diff_test.sh` from the crate agent. Phase 3 performs
the workspace-wide trait and stream-event reconciliation.

## Required reading

- `CLAUDE.md`
- `plans/error-model-roadmap.md`
- `plans/error-model-convergence.md`
- `reference/sync.md`
- `crates/types/src/error/` (the landed Phase 1 API)

## Kind/cause shape conventions

Landed-API quirks the agent must keep straight:

- `Protocol` (`crates/types/src/error/scope.rs`) enumerates only
  `Jmap, Imap, Smtp, Lmtp, Gmail, Graph, Ews`. There is no
  `Protocol::Sync`. Sync-internal errors omit Protocol entirely (the
  field on `AccountError` is `Option<Protocol>`; don't call
  `.protocol(...)` on the builder for these).
- `AccountOperation` has `SyncInventory` and `SyncChanges`; there is no
  plain `Inventory`.
- `ServerErrorKind::Error { status: Option<u16> }` and
  `ServerCause::Error { status: Option<u16> }` - both carry
  `Option<u16>` after the Phase 1 amendment. Sync rarely constructs
  `Server(_)` itself; this applies to errors it forwards.
- Every kind/cause pair the engine constructs must satisfy
  `recovery::kind_matches_cause` (asserted at runtime by
  `AccountErrorBuilder::build`).

## Scope

Own only `crates/sync/` files. Do not edit `crates/types/` from this
phase. The Phase 3 integration pass owns these workspace-wide shape
changes:

- `SyncEvent::Fatal(Fatal)` to `SyncEvent::Terminated(AccountError)`.
- Account trait results from old `bifrost_types::Error` to
  `AccountError`.
- Bulk mutation stream payloads from `MutationResult` /
  `MutationOutcome` to `ItemOutcome<MutationSuccess>`.
- Final public re-export cleanup across crates.

The sync patch should be written against those Phase 1 type names and
make every sync-side call site mechanically obvious for Phase 3. Do
not preserve compatibility shims that reintroduce the old top-level
`RecoveryClass::{RestartScope, Retry { after }, Fatal, ...}` shape.

## Current state

Important files:

- `crates/sync/src/error.rs`
  - Engine `Error` wraps old `bifrost_types::Error` in `OpenFailed`
    and `Account`.
  - `EstablishCursorFatal` carries `message` plus old
    `RecoveryClass`.
  - `FatalAction`, `map_recovery_to_fatal`, `Fatal::from_types`,
    `recovery_targets_scope`, `recovery_targets_downgrade`, and
    `recovery_targets_capability` all pattern match old top-level
    `RecoveryClass` variants.
- `crates/sync/src/lib.rs`
  - Re-exports engine-side `Fatal` and `FatalAction`; these overlap
    with the new `bifrost_types::Fatal(pub AccountError)` vocabulary.
- `crates/sync/src/multiplexer/changes.rs`
  - `drive_changes_stream` detects `SyncEvent::Fatal(f)` and returns
    `ChangesEvent::Fatal(f.recovery.clone())`, discarding the rest of
    the error.
- `crates/sync/src/multiplexer/mod.rs`
  - `ReopenRequest::Recovery` carries only `RecoveryClass`.
  - Scope lifecycle creates old
    `RecoveryClass::RestartScope(scope.clone())`.
  - `handle_drive_outcome` sleeps directly on old
    `RecoveryClass::Retry { after }` or forwards the recovery class
    to the reopen listener.
- `crates/sync/src/push/reconciler.rs`
  - Push reconcile repeats the same old retry-vs-reopen split.
  - Warning construction still uses old `WarningKind::Other(String)`
    and a raw `String` message.
- `crates/sync/src/multiplexer/fusion.rs`
  - `FusionOutcome::Fatal(RecoveryClass)` preserves only the old
    recovery class from inventory fusion.
- `crates/sync/src/backfill/runner.rs`
  - Backfill forwards `SyncEvent::Fatal(f)` but returns
    `Error::Other(format!(... f.message ...))`, losing the structured
    account error.
- `crates/sync/src/engine.rs`
  - `attach` and `reopen` call `factory.open(...).map_err(Error::OpenFailed)`.
  - `discover_scopes`, membership discovery, deferred inventory,
    `run_establish`, and `run_deferred_inventory_establishment` treat
    fatal stream endings as formatted strings or old recovery classes.
  - `handle_recovery` is the current engine directive dispatch point,
    but it matches old top-level `RecoveryClass` variants.
  - `bulk_set_flags` consumes old `MutationResult` and old
    `MutationOutcome::Failed(bifrost_types::Error)`.
  - `is_terminal_mutation_error` hand-classifies old error variants.
- `crates/sync/src/mutation/readback.rs`
  - `run_readback_guard` turns stream fatal endings into a stringy
    `Error::Other`.
- `crates/sync/src/mutation/mod.rs` and `crates/sync/src/types.rs`
  - Comments and configuration describe old
    `RecoveryClass::Retry { after }`.
- `crates/sync/src/cursor/mod.rs`, `cursor/store.rs`, and
  `cursor/envelope.rs`
  - Comments and decode errors still describe old
    `RecoveryClass::RestartScope` or local `Error::SchemaIncompatible`
    rather than a builder-derived
    `EngineDirective::SchemaIncompatible`.
- `crates/sync/tests/readback_guard.rs`
  - The synthetic `Account` implementation still uses old
    `bifrost_types::Error`, old mutation stream payloads, and old
    unsupported variants.

The scheduler itself does not currently consume `RecoveryClass`.
`crates/sync/src/scheduler/mod.rs` says the scheduler and budget gate
are not wired into production paths yet. Do not invent scheduler
integration in this error-model patch. The retry work should stay in
the existing multiplexer, push reconciler, reopen listener, and
mutation campaign paths.

## Files to modify

Primary files:

- `crates/sync/src/error.rs`
- `crates/sync/src/lib.rs`
- `crates/sync/src/engine.rs`
- `crates/sync/src/multiplexer/changes.rs`
- `crates/sync/src/multiplexer/mod.rs`
- `crates/sync/src/multiplexer/fusion.rs`
- `crates/sync/src/push/reconciler.rs`
- `crates/sync/src/backfill/runner.rs`
- `crates/sync/src/mutation/mod.rs`
- `crates/sync/src/mutation/readback.rs`
- `crates/sync/src/types.rs`
- `crates/sync/src/cursor/mod.rs`
- `crates/sync/src/cursor/store.rs`
- `crates/sync/src/cursor/envelope.rs`
- `crates/sync/tests/readback_guard.rs`

Recommended new file:

- `crates/sync/src/recovery.rs`

Use `recovery.rs` for sync-specific dispatch helpers:
`RecoveryPlan`, retry-delay calculation, directive target helpers, and
small constructors for engine-created account errors. Keeping these
out of `error.rs` prevents the engine `Error` type from growing into a
second copy of the account error taxonomy.

## Files to delete

No whole file must be deleted.

Delete or replace these items:

- `FatalAction`
- `Fatal`
- `Fatal::from_types`
- `map_recovery_to_fatal`
- `recovery_targets_scope` in its old top-level-variant form
- `recovery_targets_downgrade` in its old top-level-variant form
- `recovery_targets_capability` in its old top-level-variant form
- `is_terminal_mutation_error`

After this phase, sync should not contain a hand-written table that
maps old `bifrost_types::Error` variants or old top-level
`RecoveryClass` variants into engine actions.

## Dependencies

Required:

- Phase 1 types from `crates/types/src/error/`.
- Phase 2.1 `bifrost-net` conversion.
- Phase 2.2 protocol crate conversions to `AccountError`.

The concrete Phase 1 shapes already present in code are:

- `AccountError`
- `AccountErrorBuilder`
- `RecoveryClass::{Retry, Reconcile, Engine, AuthLost,
  NeedsAdminConsent, NeedsPolicyChange, NoPermission, Unsupported,
  ClientBug, ProviderContractViolation, ProviderRefused,
  UnknownPermanent}`
- `RetryAdvice`
- `RetryDisposition::{SameRequest, AfterStateRefresh,
  AfterAuthRefresh}`
- `ReconcileAdvice`
- `ReconcileAction::{CheckTarget, DedupeByClientId}`
- `EngineDirective::{RestartScope, RestartAccount, DowngradeStrategy,
  DowngradeCapabilityForScope, SchemaIncompatible,
  CapabilityChanged, OperatorOverrideRequired}`
- `Fatal(pub AccountError)` with `TryFrom<AccountError>`
- `Warning { kind, message: DiagnosticText, next_action,
  protocol_detail, retry_count }`
- `ItemOutcome<T>` and `MutationSuccess::{Applied, Skipped}`

Use these names exactly. Do not resurrect removed names such as
`RecoveryClass::Fatal` or `RecoveryClass::Retry { after }`.

## Target model

Sync consumes provider failures as `AccountError`. The engine is not a
classifier. It dispatches the recovery already derived by
`AccountErrorBuilder` in the protocol crate.

The central helper should look conceptually like this:

```rust
#[derive(Clone, Debug)]
pub(crate) enum RecoveryPlan {
    Retry(bifrost_types::RetryAdvice),
    Reconcile(bifrost_types::ReconcileAdvice),
    Engine(bifrost_types::EngineDirective),
    SurfaceTerminal(bifrost_types::Fatal),
}

pub(crate) fn plan_recovery(err: bifrost_types::AccountError) -> RecoveryPlan {
    match err.recovery() {
        bifrost_types::RecoveryClass::Retry(advice) => {
            RecoveryPlan::Retry(advice.clone())
        }
        bifrost_types::RecoveryClass::Reconcile(advice) => {
            RecoveryPlan::Reconcile(advice.clone())
        }
        bifrost_types::RecoveryClass::Engine(directive) => {
            RecoveryPlan::Engine(directive.clone())
        }
        _ => RecoveryPlan::SurfaceTerminal(
            bifrost_types::Fatal::try_from(err)
                .expect("terminal recovery must convert to Fatal"),
        ),
    }
}
```

The `.expect()` here is sound because the wildcard arm matches
precisely the terminal `RecoveryClass` variants, and
`Fatal::TryFrom<AccountError>` is implemented to succeed on exactly
those. If the landed `Fatal` impl is later changed to reject a
variant the wildcard matches, the dispatch path will panic at
runtime. To prevent silent drift, the test suite MUST include
`plan_recovery_terminal_round_trip` that exercises EVERY current
terminal variant: `AuthLost`, `NeedsAdminConsent`, `NeedsPolicyChange`,
`NoPermission`, `Unsupported`, `ClientBug`, `ProviderContractViolation`,
`ProviderRefused`, `UnknownPermanent`. The test fails if any variant
panics. This replaces the looser "client-bug or auth-lost" suggestion
below.

If the code uses the helper methods instead, keep the same four-way
split:

- `recovery.is_retryable()`
- `recovery.requires_reconciliation()`
- `recovery.requires_engine_action()`
- `recovery.is_terminal()`

The payload carried between workers should be the full `AccountError`,
not only `RecoveryClass`. This preserves diagnostics, scope,
operation, provider, protocol, telemetry, and cause-chain evidence for
terminal surfacing and reconciliation decisions.

## Engine error type

Rewrite `crates/sync/src/error.rs` so it contains only local engine
failures plus wrapped account errors.

Recommended shape:

```rust
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("account already attached: {0:?}")]
    AccountAlreadyAttached(bifrost_types::AccountId),

    #[error("account not attached: {0:?}")]
    AccountNotAttached(bifrost_types::AccountId),

    #[error("failed to open account")]
    OpenFailed(#[source] bifrost_types::AccountError),

    #[error("cursor establishment failed: {0}")]
    EstablishCursorFailed(String),

    #[error("cursor establishment terminated")]
    EstablishCursorTerminated(#[source] bifrost_types::AccountError),

    #[error("checkpoint store failed: {0}")]
    CheckpointStore(String),

    #[error("invalid engine configuration: {0}")]
    InvalidConfig(String),

    #[error("schema incompatible")]
    SchemaIncompatible,

    #[error("account operation failed")]
    Account(#[from] bifrost_types::AccountError),

    #[error("{0}")]
    Other(String),
}
```

Notes:

- Keep `SchemaIncompatible` if cursor envelope decode uses it
  internally, but do not expose it as a protocol-account failure. When
  it crosses into recovery dispatch, build an `AccountError` with
  `SyncStateErrorKind::SchemaIncompatible` and
  `StateCause::SchemaIncompatible` so the derived recovery is
  `RecoveryClass::Engine(EngineDirective::SchemaIncompatible)`.
- `OpenFailed` vs `Account` routing policy:
  - `attach()` returns `Err(Error::OpenFailed(account_error))` when
    the initial open during attach fails. The consumer's caller sees
    this and decides whether to retry the attach.
  - `reopen()` and the engine-internal `handle_account_error`
    `RestartAccount` path call `factory.open(...)` and route the
    resulting error through `handle_account_error` itself by wrapping
    in `Error::Account(account_error)`. The engine never returns
    `OpenFailed` from a recovery dispatch path.
  - Every other engine path that crosses a stream boundary or
    surfaces an account-side failure uses `Error::Account(...)`.
  In short: `OpenFailed` is the attach-time variant; `Account` is the
  everywhere-else variant. A caller seeing `OpenFailed` knows the
  account never reached the running state and can safely retry the
  attach; a caller seeing `Account` knows the account WAS running and
  the engine is reporting an in-flight failure.
- Delete the engine-side `Fatal` wrapper and `FatalAction`. Public
  users should see `bifrost_types::Fatal` when a terminal account
  error is surfaced.

## Stream termination data flow

Phase 3 renames the event, but sync should plan around the new
payload now:

```rust
// Phase 3 final shape.
SyncEvent::Terminated(account_error)
```

Until that rename is applied, every old `SyncEvent::Fatal(f)` match
site should be treated as a future `Terminated(AccountError)` match
site. The desired sync-side flow is:

- `drive_changes_stream` broadcasts the event unchanged.
- It returns `ChangesEvent::Terminated(AccountError)`.
- The multiplexer and push reconciler pass the full error into
  `handle_account_error`.
- Inventory fusion returns `FusionOutcome::Terminated(AccountError)`.
- Backfill returns an `Error::Account(account_error)` or forwards the
  terminated event and leaves retry/reopen to the same recovery path.
- Read-back guard returns `Err(Error::Account(account_error))`.

Do not extract only `err.recovery().clone()` as the worker payload.

Recommended local enums:

```rust
pub enum ChangesEvent {
    Advanced,
    Done,
    Stopped,
    Paused,
    Terminated(bifrost_types::AccountError),
}

pub enum FusionOutcome {
    Established,
    NoCursor,
    Terminated(bifrost_types::AccountError),
}

pub enum ReopenRequest {
    Recovery {
        /// Cursor scope for scope-bound directives
        /// (`RestartScope`, `DowngradeCapabilityForScope`,
        /// `DowngradeStrategy` with `ErrorScope::Cursor`).
        /// `None` for account-wide directives (`RestartAccount`,
        /// `SchemaIncompatible`, `CapabilityChanged`,
        /// `OperatorOverrideRequired`) where no specific scope applies.
        scope: Option<bifrost_types::CursorScope>,
        error: bifrost_types::AccountError,
    },
}
```

`scope` is `Option<CursorScope>`: scope-bound directives carry
`Some(scope)`, account-wide directives carry `None`. The
`handle_account_error` dispatcher inspects `error.recovery()` to pick
the right branch - `scope` is the convenience field for the common
case where the directive's scope is the same as the worker's current
scope. Workers that send `ReopenRequest::Recovery` for an account-wide
directive MUST pass `None`; passing `Some(arbitrary_scope)` would mask
the directive's account-wide intent.

Use `Terminated` for stream endings, not `Fatal`, because non-terminal
errors can also terminate a stream and be recovered by the engine.

### `Terminated` policy on the multiplexer side

`ChangesEvent::Terminated(AccountError)` is the carrier; it does NOT
imply that the stream is finished from the engine's perspective. The
multiplexer decides whether to re-poll by dispatching the carried
error through `plan_recovery`:

- `RecoveryPlan::Retry(_)` and `RecoveryPlan::Reconcile(_)`: sleep
  per `RetryAdvice` (or schedule a near-term reconcile poll), then
  continue the poll loop. The terminated event was a transient
  failure; the stream resumes.
- `RecoveryPlan::Engine(_)`: send `ReopenRequest::Recovery` and stop
  the poll loop until the reopen completes. The reopen path restarts
  the loop with a fresh handle/cursor.
- `RecoveryPlan::SurfaceTerminal(_)`: stop the poll loop permanently.
  The terminal event was already broadcast; do not re-poll, do not
  spawn another driver. The scope remains in a terminal state until
  a new attach or an explicit consumer reopen.

The "do not re-poll a terminal stream forever" rule is enforced by
the SurfaceTerminal branch exiting the poll loop; the other branches
continue exactly because their carried recovery says so.

## Retry advice

Replace every direct sleep on `RecoveryClass::Retry { after }` with a
helper that understands `RetryAdvice`.

Recommended helper:

```rust
pub(crate) fn retry_delay(
    advice: &bifrost_types::RetryAdvice,
    now: std::time::SystemTime,
    fallback: std::time::Duration,
) -> std::time::Duration {
    let not_before = advice
        .not_before
        .and_then(|when| when.duration_since(now).ok());
    not_before
        .into_iter()
        .chain(advice.min_delay)
        .max()
        .unwrap_or(fallback)
}
```

Rules:

- `not_before` is an absolute floor.
- `min_delay` is a relative floor.
- When both exist, sleep until both constraints are satisfied.
- When neither exists, use the existing poll cadence or a small local
  fallback such as 1 second. Do not hot-spin.
- Honor `throttle_scope` only for observability in this phase. There
  is no production throttle bucket in `bifrost-sync` yet.

Disposition handling:

- `RetryDisposition::SameRequest`
  - Multiplexer and push reconciler sleep, then allow the same scope
    poll/reconcile to run again.
  - Mutation resubmits only unresolved items with the same
    `IdempotencyKey`.
- `RetryDisposition::AfterStateRefresh`
  - For changes or inventory, perform the same behavior as
    `SameRequest` because the stream itself refreshes server state.
  - For mutations, run a read-back refresh before retrying unresolved
    items.
- `RetryDisposition::AfterAuthRefresh`
  - Do not classify as terminal. Sleep according to the advice and
    re-enter through the current account handle. If a consumer-owned
    auth layer swaps credentials under the same factory, a later
    `RestartAccount` or explicit `reopen` will pick up the refreshed
    handle.

## Reconciliation advice

`RecoveryClass::Reconcile` means "the server may already have applied
the requested operation, but sync cannot prove it from the failed
response."

For this phase:

- Changes and inventory streams should surface a warning and schedule
  a same-scope reconcile by re-running from the last known cursor. A
  reconcile advice on a read stream is rare, but treating it as a
  scoped reconcile is safer than terminal surfacing.
- Mutation streams must use the read-back guard for
  `ReconcileAction::CheckTarget`.
- Mutation streams must use idempotency-key bookkeeping or provider
  client ids for `ReconcileAction::DedupeByClientId` when the target
  operation has a client-visible dedup key. The existing flag-mutation
  pipeline has only the engine `IdempotencyKey`, so it should record
  this action for telemetry and rely on read-back for the actual
  decision.
- Reconciliation failures become ordinary `Error::Account` results,
  preserving the account error that blocked reconciliation.

Do not invent provider-specific reconciliation inside sync. Sync can
only ask `Account` for current state through trait methods.

## Engine directives

Replace old `handle_recovery(..., RecoveryClass)` with
`handle_account_error(..., AccountError)`. It should call
`plan_recovery` and dispatch `RecoveryPlan::Engine(directive)` here.

Recommended signature:

```rust
async fn handle_account_error(
    factory: &std::sync::Arc<dyn bifrost_types::AccountFactory>,
    current: &std::sync::Arc<arc_swap::ArcSwap<std::sync::Arc<dyn bifrost_types::Account>>>,
    cursors: &std::sync::Arc<crate::cursor::CursorRegistry>,
    store: &std::sync::Arc<crate::cursor::store::DynCheckpointStore>,
    changes_tx: &tokio::sync::broadcast::Sender<crate::multiplexer::MultiplexerEvent>,
    account_id: &bifrost_types::AccountId,
    control: &crate::control::SyncControl,
    scope: bifrost_types::CursorScope,
    error: bifrost_types::AccountError,
) {
    // dispatch RecoveryPlan
}
```

Directive rules:

- `EngineDirective::RestartScope(scope_from_error)`
  - Use the directive's scope when present; fall back to the worker's
    current scope only if the directive came from a scope-less error
    that already targeted this worker.
  - Delete the in-memory cursor.
  - Delete the durable change cursor.
  - Re-establish via `run_establish`.
  - Refresh membership links after successful establishment.
  - Let the multiplexer scan loop spawn a fresh poll task.
- `EngineDirective::DowngradeCapabilityForScope(scope_from_error)`
  - Use the same cursor deletion and re-establishment path as
    `RestartScope`.
  - Do not silently mutate protocol capabilities in sync. Protocol
    crates own their capability snapshot. Add a warning that names the
    downgraded scope.
- `EngineDirective::RestartAccount`
  - Call `factory.open(account_id.clone())`.
  - Reapply `priority_snapshot` and `bandwidth_cap_snapshot`.
  - Store the new handle in `ArcSwap`.
  - Do not drop every cursor by default. Existing cursors remain valid
    unless the provider also emitted a cursor/state directive.
- `EngineDirective::CapabilityChanged { delta }`
  - Reopen the account as above.
  - Emit a `WarningKind::Other` warning carrying the `delta` payload
    in `protocol_detail`:
    ```rust
    Warning {
        kind: WarningKind::Other,
        message: DiagnosticText::user_safe("account capabilities changed"),
        next_action: None,
        protocol_detail: Some(DiagnosticText::support_only(
            format!("{delta:?}")
        )),
        retry_count: 0,
    }
    ```
    The delta is captured so telemetry can pivot on capability shifts
    even though no live `AccountSlot.capabilities` field consumes it.
  - Do not mutate `AccountSlot.capabilities`; the field is currently
    an attach-time snapshot and not used as the source of truth by
    workers. If a later code patch wants live capability snapshots,
    make that a separate change.
- `EngineDirective::DowngradeStrategy(downgrade)`
  - There is no sync-owned strategy table today. Emit a
    `WarningKind::StrategyDowngraded` warning that carries the
    `downgrade` payload:
    ```rust
    Warning {
        kind: WarningKind::StrategyDowngraded,
        message: DiagnosticText::user_safe(format!(
            "downgraded sync strategy: {downgrade:?}"
        )),
        next_action: None,
        protocol_detail: Some(DiagnosticText::support_only(
            format!("{downgrade:?}")
        )),
        retry_count: 0,
    }
    ```
    The `downgrade` value must reach both `message` (for the human
    summary) and `protocol_detail` (for support exports); not piping
    it through is the "engine swallowed the payload" bug the audit
    catches.
  - Reopen the account so the protocol crate can choose the lower
    strategy on the next `establish_initial_cursor`.
  - If the directive's originating error has `ErrorScope::Cursor`,
    delete and re-establish that scope after reopen. Otherwise keep
    existing cursors and let the reopened account decide.
- `EngineDirective::SchemaIncompatible`
  - Stop trusting durable cursor envelopes.
  - Clear all in-memory cursors for the account.
  - Delete every durable change cursor known to the registry before
    clearing it. If the store has no "delete all" API, iterate
    `cursors.all_scopes()` first.
  - Re-establish every discovered scope from the current account.
  - If rediscovery itself fails, surface the account error as
    terminal or retryable according to its own recovery.
- `EngineDirective::OperatorOverrideRequired { reason }`
  - Do not reopen automatically.
  - Emit a `WarningKind::OperatorAttentionNeeded` with
    `DiagnosticText::user_safe(reason.clone())` only if the reason is
    suitable for display. Otherwise use `support_only`.
  - Surface the original account error through the account stream so
    the consumer can pause or alert.

Terminal recoveries:

- `AuthLost`
- `NeedsAdminConsent`
- `NeedsPolicyChange`
- `NoPermission`
- `Unsupported`
- `ClientBug`
- `ProviderContractViolation`
- `ProviderRefused`
- `UnknownPermanent`

These must convert through `Fatal::try_from(error.clone())`. If the
conversion returns `Err`, that is a bug in the dispatch branch and the
test suite should catch it.

## Engine-created account errors

Sync sometimes creates failures itself: unsupported inventory
partition in the default `Account` impl, cursor envelope mismatch,
wrong checkpoint type, read-back projection mismatch, and local
checkpoint decode failures.

For errors that are internal to sync and do not cross the `Account`
trait, keep `crate::error::Error`.

For errors that are sent through `SyncEvent::Terminated` or
`ReopenRequest::Recovery`, build an `AccountError`:

- Cursor envelope version below `MIN_MIGRATABLE`
  - `AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)`
  - `Cause::State(StateCause::SchemaIncompatible)`
  - No `Protocol` on the builder (there is no `Protocol::Sync`; the
    landed `AccountError.protocol` is `Option<Protocol>` and sync-
    internal errors omit it).
  - Operation: the operation being performed if known, otherwise
    `AccountOperation::SyncChanges`.
- Cursor identity or state invalid for a scope
  - `AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)`
  - `Cause::State(StateCause::CursorInvalid)`
  - `ErrorScope::Cursor(scope)`
  - No `Protocol`.
- Inventory partition unsupported by the default trait path
  - This is owned by `crates/types` in Phase 3 because the default
    implementation lives there. Sync should only consume the resulting
    `Unsupported(AccountOperation::SyncInventory)` recovery (the
    landed enum has `SyncInventory`, not plain `Inventory`).
- Malformed checkpoint from an account stream
  - Use `ProtocolErrorKind::ContractViolation` if the account emitted
    an invalid checkpoint shape. The `Protocol` stamped on the
    resulting `AccountError` is the upstream protocol that produced
    the malformed checkpoint (whichever the offending account
    advertised) - sync attaches it via `.protocol(upstream_protocol)`
    rather than omitting it, because the diagnostic value of "this
    checkpoint came from a JMAP/IMAP/Graph/Gmail account" is high.

Do not use `Error::Other(format!(...))` to hide an account error at a
stream boundary.

## Multiplexer changes

In `multiplexer/changes.rs`:

- Rename `ChangesEvent::Fatal` to `ChangesEvent::Terminated`.
- Carry `AccountError`, not `RecoveryClass`.
- Preserve the broadcast-before-return behavior. Consumers must still
  see the terminating event.
- `checkpoint_for` remains unchanged.

In `multiplexer/mod.rs`:

- Change `ReopenRequest::Recovery` to carry `error: AccountError`.
- In scope lifecycle handling, construct a sync-state cursor-invalid
  `AccountError` whose recovery derives to
  `Engine(RestartScope(scope))`, or add a private helper such as
  `restart_scope_error(scope, AccountOperation::SyncChanges)`.
- Replace `handle_drive_outcome`'s retry branch with a call to
  `handle_account_error` or a smaller local helper that returns a
  `DriveRecovery`.
- For retryable changes-stream terminations, sleep using
  `retry_delay` and keep the poll loop alive.
- For reconcile terminations, schedule a near-term same-scope poll and
  keep the poll loop alive.
- For engine directives, send `ReopenRequest::Recovery { scope,
  error }`. `scope` is `Some(directive_scope)` for
  `RestartScope(scope)`, `DowngradeCapabilityForScope(scope)`, and
  `DowngradeStrategy(_)` when the originating error has
  `ErrorScope::Cursor`; `scope` is `None` for `RestartAccount`,
  `SchemaIncompatible`, `CapabilityChanged`, and
  `OperatorOverrideRequired`.
- For terminal errors, broadcast has already happened; log telemetry
  and stop the scope poll if continuing would hot-loop on a terminal
  stream.

Do not let a terminal stream ending be re-polled forever.

## Push reconciler changes

In `push/reconciler.rs`:

- Remove direct imports of `RecoveryClass`.
- On `ChangesEvent::Terminated(error)`, call the same recovery helper
  as the multiplexer.
- For `Retry`, sleep according to `RetryAdvice`, then continue the
  reconcile loop.
- For `Reconcile`, rerun the same hinted scope set once after the
  read/state refresh delay. Bound this so a bad provider cannot make
  the reconciler recurse forever; one immediate reconcile plus normal
  future push/poll is enough.
- For `Engine`, send `ReopenRequest::Recovery { scope, error }` and
  return from the current reconcile pass.
- For terminal, rely on the already-broadcast terminating event and
  return.
- Update warnings to the Phase 1 `Warning` shape:
  - `WarningKind::Other`
  - `message: DiagnosticText::user_safe("push transport disconnected")`
  - `next_action: None`
  - `protocol_detail: None`
  - `retry_count: 0`

## Inventory fusion and backfill

In `multiplexer/fusion.rs`:

- Rename `FusionOutcome::Fatal` to `FusionOutcome::Terminated`.
- Carry `AccountError`.
- Broadcast the terminating event unchanged.
- `finalize` should still reject wrong-scope or wrong-checkpoint
  terminal `Done` checkpoints, but if that rejection crosses a stream
  boundary later, convert it through `AccountErrorBuilder` rather
  than formatting it into `Error::Other`.

In `backfill/runner.rs`:

- When inventory partition stream terminates with an account error,
  forward the terminating event unchanged on `changes_tx`.
- Return `Err(Error::Account(error))`, not a formatted string.
- The backfill orchestrator currently marks the scope `Pending` on
  any error. Keep that behavior for retryable, reconcile, and engine
  errors. Terminal errors should be logged with terminal telemetry;
  they still leave the backfill state `Pending` because retry policy
  is not integrated with the scheduler yet.

In `engine.rs`:

- `run_deferred_inventory_establishment` should pass
  `FusionOutcome::Terminated(error)` into `handle_account_error`
  instead of only logging a recovery class.
- `run_establish` should return `Error::EstablishCursorTerminated`
  for terminated inventory fusion.

## Cursor envelope recovery

`cursor/envelope.rs` can keep returning local `Error` while decoding.
The recovery conversion happens at the boundary that decides what to
do with a decoded checkpoint failure.

Rules:

- `Error::SchemaIncompatible` from `decode_envelope` becomes an
  `AccountError` with `SyncStateErrorKind::SchemaIncompatible`.
- Malformed bytes, bad magic, unknown kind, truncated payload, or
  future engine version become `ProtocolErrorKind::ContractViolation`
  only when they came from an account-provided cursor. If they came
  from the consumer's checkpoint store, keep them as local engine
  errors and return from `attach` or `ack_checkpoint`.
- `EngineDirective::SchemaIncompatible` handling clears cursor state
  and re-establishes. It must not be converted to terminal.

Update comments in `cursor/mod.rs` and `cursor/store.rs` to reference
`EngineDirective::RestartScope`, not old
`RecoveryClass::RestartScope`.

## Mutation pipeline

Phase 3 changes the trait stream item to
`SyncEvent<ItemOutcome<MutationSuccess>>`. Sync's mutation accounting
should be rewritten to that model.

Replace this old logic:

- `MutationOutcome::Applied` to `applied`.
- `MutationOutcome::Skipped` to `skipped`.
- `MutationOutcome::Failed(old Error)` to either terminal or retry
  by `is_terminal_mutation_error`.
- Stream `Fatal` retry by `RecoveryClass::Retry { after }`.

With this logic:

- `ItemOutcome::Succeeded(BatchSuccess { output: Applied, .. })`
  records `applied`.
- `ItemOutcome::Succeeded(BatchSuccess { output: Skipped, .. })`
  records `skipped`.
- `ItemOutcome::Failed(BatchFailure { error, .. })` dispatches by
  `error.recovery()`:
  - `Retry(SameRequest)` queues the item for retry with the same
    `IdempotencyKey`.
  - `Retry(AfterStateRefresh)` queues the item for read-back first,
    then retries only if the target state is not already present.
  - `Retry(AfterAuthRefresh)` queues the item after the retry delay.
  - `Reconcile(_)` queues the item for read-back.
  - `Engine(_)` marks the campaign blocked and sends a recovery
    request for the account or scope if one is available.
  - terminal marks `failed_terminal`.
- `ItemOutcome::Uncertain(BatchUncertain { error, .. })` dispatches
  like `Reconcile(_)` even if the error also says retryable. The
  uncertainty lane exists specifically to avoid blindly replaying
  writes whose first attempt may have landed.
- Stream `Terminated(error)` applies to every unresolved item in the
  current attempt:
  - retryable: retry unresolved items.
  - reconcile: read-back unresolved items.
  - engine directive: stop the campaign and route the directive.
  - terminal: mark unresolved items as terminal failed.

Keep these existing invariants:

- One `IdempotencyKey` per campaign, reused for every retry attempt
  in that campaign.
- Counters are computed from final per-id state, not by summing every
  attempt.
- Applied and skipped final states are not retried.
- Read-back guard runs before terminally failing retry candidates
  that may have been applied before a transport drop.
- No live server tests.

Recommended replacement buckets:

```rust
enum MutationBucket {
    Applied,
    Skipped,
    FailedTerminal,
    PendingRetry(bifrost_types::RetryAdvice),
    PendingReconcile(bifrost_types::ReconcileAdvice),
    BlockedByEngine(bifrost_types::EngineDirective),
}
```

If storing full advice in the bucket makes the map noisy, store a
separate side table by `ObjectId`.

Read-back guard updates:

- Return `Err(Error::Account(error))` on stream termination.
- On `ItemOutcome` migration, the guard still consumes hydration
  streams, not mutation streams. It only needs the event rename.
- If a hydration batch returns the wrong projection, keep treating it
  as not matched. That is a local reconciliation failure, not a
  provider terminal.

## Warnings

Update sync-created warnings to the Phase 1 `Warning` type:

```rust
bifrost_types::Warning {
    kind: bifrost_types::WarningKind::Other,
    message: bifrost_types::DiagnosticText::user_safe("..."),
    next_action: None,
    protocol_detail: None,
    retry_count,
}
```

Use specific kinds where available:

- `StrategyDowngraded` for `DowngradeStrategy`.
- `OperatorAttentionNeeded` for `OperatorOverrideRequired`.
- `Throttled` when surfacing `RetryReason::RateLimited` or
  `RetryReason::QuotaExhausted`.
- `ReadbackSkipped` when the read-back guard converts apparent
  failures into skipped successes.

Do not put provider raw bodies, stack traces, or support-only data in
`DiagnosticText::user_safe`.

## Public re-exports

In `lib.rs`:

- Stop re-exporting sync-local `Fatal` and `FatalAction`.
- Re-export `bifrost_types::Fatal` only if consumers previously
  imported terminal errors from `bifrost_sync`; otherwise leave fatal
  under `bifrost_types`.
- Continue re-exporting sync-local `Error`.
- Consider re-exporting `RecoveryPlan` only if tests or consumers need
  to pattern match it. Prefer keeping it `pub(crate)`.

Do not add a public API that exposes the old recovery-action table.

## Implementation order

1. Rewrite `error.rs`.
   - Swap `bifrost_types::Error` for `AccountError`.
   - Delete `FatalAction`, `Fatal`, and old target helpers.
   - Add local `Error::EstablishCursorTerminated(AccountError)`.
2. Add `recovery.rs`.
   - Define `RecoveryPlan`.
   - Add `plan_recovery`.
   - Add `retry_delay`.
   - Add target helpers that inspect `RecoveryClass::Engine`.
   - Add sync-created account-error constructors needed by scope
     lifecycle and cursor recovery.
3. Update stream consumers.
   - `multiplexer/changes.rs`
   - `multiplexer/mod.rs`
   - `push/reconciler.rs`
   - `multiplexer/fusion.rs`
   - `backfill/runner.rs`
   - `mutation/readback.rs`
4. Rewrite engine recovery dispatch.
   - Replace `handle_recovery` with `handle_account_error`.
   - Implement every `EngineDirective` branch.
   - Route deferred inventory, establish, discovery, subscribe, and
     reopen paths through `AccountError`.
5. Rewrite mutation accounting.
   - Consume `ItemOutcome<MutationSuccess>`.
   - Delete `is_terminal_mutation_error`.
   - Route retries, reconciliation, engine directives, and terminal
     failures from `AccountError::recovery`.
6. Update warnings and comments.
   - New warning shape.
   - No stale references to old top-level `RecoveryClass` variants.
7. Update tests.
   - Use `AccountErrorBuilder` helpers.
   - Use new stream event and mutation item shapes.

## Tests

Keep tests small and deterministic. Add unit tests near the helpers
they exercise.

Suggested tests:

- `plan_recovery_retry_preserves_advice`
  - Build a transport or rate-limit `AccountError`.
  - Assert `RecoveryPlan::Retry` carries the expected disposition,
    reason, and throttle scope.
- `plan_recovery_reconcile_preserves_actions`
  - Build a non-idempotent in-flight transport error.
  - Assert `ReconcileAction::CheckTarget` is preserved.
- `plan_recovery_engine_restart_scope`
  - Build a cursor-invalid error scoped to a cursor.
  - Assert `EngineDirective::RestartScope(scope)`.
- `plan_recovery_terminal_round_trip`
  - Build an `AccountError` for EACH terminal `RecoveryClass`
    variant: `AuthLost`, `NeedsAdminConsent`, `NeedsPolicyChange`,
    `NoPermission`, `Unsupported`, `ClientBug`,
    `ProviderContractViolation`, `ProviderRefused`,
    `UnknownPermanent`.
  - Assert `plan_recovery` returns `SurfaceTerminal` for each - none
    panic, none route to Retry/Reconcile/Engine.
  - Pins the `.expect("terminal recovery must convert to Fatal")` in
    `plan_recovery` against silent drift if the `Fatal::TryFrom`
    impl in `bifrost-types` ever changes.
- `retry_delay_uses_later_of_not_before_and_min_delay`
  - Pure unit test with a fixed `SystemTime`.
- `changes_driver_returns_terminated_account_error`
  - Synthetic account stream yields a terminating event.
  - Assert `ChangesEvent::Terminated` carries the original
    `AccountError`.
- `fusion_returns_terminated_account_error`
  - Inventory stream terminates.
  - Assert `FusionOutcome::Terminated`.
- `reopen_request_carries_account_error`
  - Drive `handle_drive_outcome` with an engine directive.
  - Assert the mpsc request carries the full error.
- `handle_account_error_restart_scope_deletes_and_reestablishes`
  - Use in-memory checkpoint store and synthetic account.
  - Assert old cursor removed and new cursor registered.
- `handle_account_error_restart_account_swaps_arc`
  - Synthetic factory returns a distinct account handle.
  - Assert `ArcSwap` changes and priority/bandwidth snapshots are
    reapplied.
- `handle_account_error_schema_incompatible_clears_scopes`
  - Seed multiple cursors.
  - Assert registry is cleared and establishment is attempted.
- `mutation_failed_retry_queues_same_item`
  - Feed `ItemOutcome::Failed` with retryable `AccountError`.
  - Assert only that item remains pending.
- `mutation_uncertain_runs_readback`
  - Feed `ItemOutcome::Uncertain`.
  - Assert read-back path, not blind retry.
- `mutation_terminal_marks_failed_terminal`
  - Feed terminal `AccountError`.
  - Assert no retry.
- `readback_guard_terminated_returns_account_error`
  - Hydration stream terminates.
  - Assert `Error::Account`.
- `warning_event_uses_diagnostic_text`
  - Construct push disconnect warning and assert Phase 1 fields.

Do not add integration, live-account, fixed-port, or mock-server tests.
Synthetic `Account` implementations and pure helper tests are enough.

## Exit criteria

- `crates/sync` no longer imports or matches old
  `bifrost_types::Error`.
- `crates/sync` no longer references old
  `RecoveryClass::{Retry { after }, RestartScope, RestartAccount,
  DowngradeStrategy, DowngradeCapabilityForScope, CapabilityChanged,
  SchemaIncompatible, OperatorOverrideRequired, Fatal}`.
- Every stream termination path carries `AccountError`, not only
  `RecoveryClass`.
- `EngineDirective` dispatch covers every current variant.
- `RetryAdvice` delay and disposition are honored in multiplexer,
  push reconcile, and mutation campaign paths.
- `ReconcileAdvice` routes mutation uncertainty through read-back.
- Terminal account errors convert through `Fatal::try_from`.
- Old `FatalAction` and sync-local `Fatal` are gone.
- Mutation accounting consumes `ItemOutcome<MutationSuccess>` and no
  longer hand-classifies old error variants.
- Sync-created warnings use `DiagnosticText`.
- Comments no longer name old top-level recovery variants.
- The patch includes deterministic unit tests for the new helper and
  worker dispatch behavior.
- The crate agent does not run `brokkr`, `cargo`, or
  `./diff_test.sh`; Phase 3 validates the workspace.
- `ReopenRequest::Recovery::scope` is `Option<CursorScope>`; account-
  wide directives carry `None`, scope-bound directives carry `Some`.
- `EngineDirective::DowngradeStrategy(downgrade)` and
  `CapabilityChanged { delta }` dispatch routes the payload into a
  `Warning::protocol_detail` (and `message` for downgrades) - the
  audit greps that no payload is silently dropped on the directive
  match arm.
- Every kind/cause pair the engine constructs satisfies
  `recovery::kind_matches_cause`.
- `plan_recovery_terminal_round_trip` covers every current terminal
  `RecoveryClass` variant.

## Audit checklist

After implementation, audit with these searches:

```text
rg -n "bifrost_types::Error|TypesError|RecoveryClass::Retry \\{|RecoveryClass::RestartScope|RecoveryClass::RestartAccount|RecoveryClass::Downgrade|RecoveryClass::CapabilityChanged|RecoveryClass::SchemaIncompatible|RecoveryClass::OperatorOverrideRequired|RecoveryClass::Fatal|FatalAction|map_recovery_to_fatal|is_terminal_mutation_error" crates/sync
rg -n "SyncEvent::Fatal|ChangesEvent::Fatal|FusionOutcome::Fatal" crates/sync
rg -n "MutationOutcome|MutationResult" crates/sync
rg -n "WarningKind::Other\\(|message: .*\\.to_string\\(\\)" crates/sync
```

Expected result: no matches except historical text in this plan, if
the search is run over `plans/`.

Then perform the three-pass review:

1. Domain pass
   - Multiplexer, push reconciler, backfill, inventory fusion, cursor
     envelope, and mutation paths all preserve `AccountError`.
2. Cross-cutting pass
   - Every engine directive reaches real existing engine machinery.
   - Retry and reconcile do not hot-loop.
   - Terminal errors are not silently retried.
3. Editorial pass
   - Comments, public exports, and tests use the new vocabulary.
   - No stale old-recovery examples remain in sync source.
