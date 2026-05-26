# Error model: bifrost-types implementation plan

This is Phase 1 of `plans/error-model-roadmap.md`. It covers the
**additive** part of the `bifrost-types` change: a new `error/`
module containing the entire new error model, plus deletion of the
old `error.rs` and the matching `lib.rs` re-export update. It does
not touch the trait surface (`account.rs`, the trait imports and
trait method declarations), the event surface (`events.rs`), or the
mutation surface (`mutation.rs`). Those changes belong to Phase 3
(workspace integration), where every crate's surface migrates in
the same commit and compilation comes back.

Phase 1 leaves `bifrost-types` in a deliberately broken state: the
new types are present, the old `Error` type is gone, and
`account.rs`, `events.rs`, and `mutation.rs` still reference the
removed types. This is intentional per the roadmap's "broken
intermediate states are acceptable" framing. Phase 1 validates by
patch audit, not by compilation.

The convergence plan
(`plans/error-model-convergence.md`) is the authoritative source for
type definitions, invariants, and the recovery mapping table. This
plan does not repeat those; it describes *how* to land them in
`bifrost-types`.

## Current state of the crate

`crates/types/src/` (15 files, ~80KB) is structured as a flat module
collection. `lib.rs` does the re-export work. The error / recovery
surface lives entirely in `error.rs` (188 lines). Relevant existing
files:

- `crates/types/src/error.rs` - `Error`, `Fatal`, `RecoveryClass`
  (with `Retry { after: Duration }`, `DowngradeStrategy`,
  `DowngradeCapabilityForScope`, `RestartScope`, `RestartAccount`,
  `AuthLost`, `SchemaIncompatible`, `CapabilityChanged`,
  `OperatorOverrideRequired`, `Fatal`), `StrategyDowngrade`,
  `Warning`, `WarningKind`.
- `crates/types/src/events.rs:150-156` - `SyncEvent<T>` with
  `Batch / Progress / Warning / Fatal / Done` variants.
  `SyncEvent::Fatal(Fatal)` at line 154 is the rename target.
- `crates/types/src/events.rs:289-300` - `Control` trait. Two
  method returns use `Error` and must update to `AccountError`.
- `crates/types/src/mutation.rs:155-171` - `MutationResult` and
  `MutationOutcome` with `Failed(Error)` arm.
- `crates/types/src/account.rs:34` - `use crate::error::{Error, Fatal, RecoveryClass}`.
  The `Account` trait at line 63 uses `Result<_, Error>` in
  every method signature.
- `crates/types/src/account.rs:127-143` - default impl of
  `inventory_partition_stream` constructs
  `SyncEvent::Fatal(Fatal { recovery: RecoveryClass::Fatal, ... })`.
  This emission site is left as-is by Phase 1 and rewritten in
  Phase 3 alongside the rest of `account.rs`.
- `crates/types/src/capabilities.rs:296-313` -
  `CapabilityChange`, `CapabilityDelta`, `CapabilityKey`,
  `CapabilityValue`. Already public; the new
  `EngineDirective::CapabilityChanged { delta }` reuses
  `CapabilityDelta` verbatim.
- `crates/types/src/cursor.rs` - `CursorScope`, `SyncStrategy`,
  `ChangeCursor`, `CursorEstablishment`. No `Error` references.
  `EngineDirective::DowngradeStrategy(StrategyDowngrade)` reuses
  `StrategyDowngrade` (currently in `error.rs:127-132`); the type
  itself moves to the new recovery module.
- `crates/types/src/lib.rs:63` -
  `pub use error::{Error, Fatal, RecoveryClass, StrategyDowngrade, Warning, WarningKind};`.
  Re-exports change wholesale.

## Scope

What this phase changes in `bifrost-types`:

- Deletes `crates/types/src/error.rs` (the old `Error`,
  `RecoveryClass`, `Fatal`, `Warning`, `WarningKind`,
  `StrategyDowngrade`).
- Creates the new `crates/types/src/error/` module with the entire
  new error model: `AccountError` (opaque), `AccountErrorBuilder`,
  `RecoveryClass` with nested `EngineDirective`, `RetryAdvice`,
  `ReconcileAdvice`, all subkind enums, all `*Cause` types
  including `AttemptCause` and `TransmissionState`, the
  diagnostic accessor surface (`TelemetryView`, support exports,
  `DiagnosticInfo`, `DiagnosticText`), the Vec-batch surface
  (`BatchItem`, `BatchOutcome`, `BatchItemOutcome`,
  `BatchSuccess`, `BatchFailure`, `BatchUncertain`,
  `BatchItemId`), the streaming surface (`ItemOutcome`,
  `MutationSuccess`), `Warning` rebuilt around `DiagnosticText`,
  the `Fatal` newtype with `TryFrom<AccountError>`, the central
  recovery mapping (`recovery::derive`, `recovery::suggest`),
  `message_key::derive`, and all wire enums.
- Updates `crates/types/src/lib.rs` to remove the old re-exports
  and add the new ones.

What this phase does **not** change:

- `crates/types/src/account.rs`. The trait's
  `use crate::error::{Error, Fatal, RecoveryClass}` import and
  every `Result<_, Error>` return type are left as-is. They are
  broken (referencing removed types) after Phase 1. Repaired in
  Phase 3.
- `crates/types/src/events.rs`. `SyncEvent::Fatal(Fatal)`,
  the `Control` trait's `Error` returns, and the
  `use crate::error::{Error, Fatal, Warning}` import are left
  as-is. The `SyncEvent::Fatal` → `SyncEvent::Terminated(AccountError)`
  rename is a surface change owned by Phase 3. Sync consumes the
  rename but does not own it.
- `crates/types/src/mutation.rs`. `MutationResult`,
  `MutationOutcome::Failed(Error)`, and the
  `use crate::error::Error` import are left as-is. The
  reshape into `ItemOutcome<MutationSuccess>` is a Phase 3
  surface change.
- Any other crate. Phase 1 is bifrost-types only.

This phase deliberately leaves the crate in a non-compiling state.
Validation is by patch audit, not by `brokkr check`.

## Module layout

The current single-file `error.rs` (188 lines) becomes a
multi-file `error/` module. The new model is ~3x the surface area
of the old; one file would push past the project's source-bloat
threshold.

```
crates/types/src/
├── account.rs            // UNCHANGED by Phase 1 (broken; Phase 3 fixes)
├── blob.rs               // unchanged
├── capabilities.rs       // unchanged
├── compose.rs            // unchanged
├── container.rs          // unchanged
├── cursor.rs             // unchanged
├── error.rs              // DELETED (becomes error/ module)
├── error/
│   ├── mod.rs            // re-exports the public surface
│   ├── kind.rs           // AccountErrorKind, RequestErrorKind,
│   │                     //   AuthErrorKind, AccessErrorKind,
│   │                     //   ServerErrorKind, SyncStateErrorKind,
│   │                     //   ProtocolErrorKind, ResourceKind,
│   │                     //   MailboxUnavailableKind
│   ├── scope.rs          // ErrorScope, AccountOperation
│   │                     //   (+ is_idempotent), Provider, Protocol
│   ├── cause.rs          // Cause, CauseChain, TransportCause,
│   │                     //   TransportKind, AttemptCause,
│   │                     //   TransmissionState, AuthCause,
│   │                     //   AccessCause, ServerCause, StateCause,
│   │                     //   RequestCause, WireCause,
│   │                     //   BatchInputInvalidItem,
│   │                     //   BatchInputInvalidReason
│   ├── diagnostic.rs     // DiagnosticInfo, DiagnosticText,
│   │                     //   DetailVisibility, TelemetryView,
│   │                     //   SupportExportMinimal (type alias),
│   │                     //   SupportExportConsented,
│   │                     //   SupportExportInternal, CauseSummary
│   ├── recovery.rs       // RecoveryClass, RetryAdvice,
│   │                     //   RetryDisposition, RetryReason,
│   │                     //   ReconcileAdvice, ReconcileReason,
│   │                     //   ReconcileGuidance, ReconcileAction,
│   │                     //   EngineDirective, ThrottleScope,
│   │                     //   RemediationAction, Fatal newtype,
│   │                     //   StrategyDowngrade (moved from
│   │                     //   error.rs:127-132), derive(),
│   │                     //   suggest(), kind_matches_cause()
│   ├── account_error.rs  // AccountError opaque struct,
│   │                     //   accessors, StdError impl,
│   │                     //   Display impl
│   ├── builder.rs        // AccountErrorBuilder + build()
│   ├── batch.rs          // BatchItem, BatchOutcome,
│   │                     //   BatchItemOutcome, BatchSuccess,
│   │                     //   BatchFailure, BatchUncertain,
│   │                     //   BatchItemId, validate_batch_input()
│   ├── stream.rs         // ItemOutcome, MutationSuccess
│   ├── warning.rs        // Warning, WarningKind
│   └── message_key.rs    // derive_message_key(&AccountErrorKind)
│                         //   -> &'static str
├── events.rs             // UNCHANGED by Phase 1 (broken; Phase 3 fixes)
├── hydration.rs          // unchanged
├── ids.rs                // unchanged
├── lib.rs                // modified: re-exports (only file edit
│                         //   outside error/ in this phase)
├── mutation.rs           // UNCHANGED by Phase 1 (broken; Phase 3 fixes)
├── page.rs               // unchanged
├── search.rs             // unchanged
└── settings.rs           // unchanged
```

12 new files (under `error/`), 1 modified (`lib.rs`), 1 deleted
(`error.rs`). All other files untouched.

## Files to create

In dependency order (each file depends on types defined above it):

1. `crates/types/src/error/scope.rs` - `ErrorScope`,
   `AccountOperation`, `Provider`, `Protocol`. No dependencies
   beyond existing types (`CursorScope` from `cursor.rs`).
2. `crates/types/src/error/kind.rs` - `AccountErrorKind` and all
   subkind enums. Depends on `scope.rs` (for `AccountOperation` in
   `Unsupported(AccountOperation)`). `RequestErrorKind` carries no
   payload (the `BatchInputInvalidItem` list lives in
   `RequestCause::BatchInputInvalid`), so there is no
   `BatchItemId` dependency.
3. `crates/types/src/error/cause.rs` - all `*Cause` types,
   `Cause`, `CauseChain`. Depends on `kind.rs` and `scope.rs`. The
   `WireCause` enum's per-protocol variants (`GraphSignal`,
   `JmapMethod`, `ImapResponseCode`, `EnhancedStatusCode`,
   `GmailSignal`) are defined here as `#[non_exhaustive]` enums
   that the protocol crates fill in via their builder calls.
4. `crates/types/src/error/diagnostic.rs` - `DiagnosticInfo`,
   `DiagnosticText`, `DetailVisibility`, export view types.
   Depends on `kind.rs`, `scope.rs`, `cause.rs`. The
   `TelemetryView::from_account_error` constructor lives elsewhere
   (in `account_error.rs`) because it needs access to
   `AccountError`'s private fields.
5. `crates/types/src/error/batch.rs` - `BatchItem<I>`,
   `BatchOutcome<T>`, `BatchItemOutcome`, `BatchSuccess<T>`,
   `BatchFailure`, `BatchUncertain`, `BatchItemId`. Depends on
   nothing else in error/ except `account_error.rs` (via
   `BatchFailure.error: AccountError`) - declare with a forward
   reference, define after `account_error.rs` lands. Also
   exposes `pub(crate) fn validate_batch_input` for the
   preflight uniqueness / emptiness check.
6. `crates/types/src/error/stream.rs` - `ItemOutcome<T>`,
   `MutationSuccess`. Depends on `batch.rs`.
7. `crates/types/src/error/warning.rs` - `Warning`, `WarningKind`.
   Depends on `diagnostic.rs`.
8. `crates/types/src/error/recovery.rs` - `RecoveryClass`,
   `RetryAdvice`, `RetryDisposition`, `RetryReason`,
   `ReconcileAdvice`, `ReconcileReason`, `ReconcileGuidance`,
   `ReconcileAction`, `EngineDirective`, `ThrottleScope`,
   `RemediationAction`, `Fatal` newtype, the moved
   `StrategyDowngrade`, plus the `derive`, `suggest`, and
   `kind_matches_cause` functions. Depends on `kind.rs`,
   `scope.rs`, `cause.rs`, and `crate::capabilities::CapabilityDelta`.
9. `crates/types/src/error/account_error.rs` - `AccountError`
   struct, all accessors, `StdError` impl, `Display` impl,
   `TelemetryView::from_account_error` constructor, the
   support-export constructors. Depends on every module above.
10. `crates/types/src/error/builder.rs` - `AccountErrorBuilder`
    and `build()`. Depends on `account_error.rs` and
    `recovery.rs::derive` / `suggest`.
11. `crates/types/src/error/message_key.rs` -
    `derive_message_key(&AccountErrorKind) -> &'static str`.
    Depends on `kind.rs`.
12. `crates/types/src/error/mod.rs` - `pub use` re-exports.
    Last, since it surfaces everything above.

Order 5/6 has a circular reference at the type level
(`BatchFailure::error: AccountError` vs
`AccountError` doesn't depend on `BatchFailure`). Resolution:
declare `BatchFailure` with a forward `AccountError` reference;
Rust handles this fine at the module level.

## Files to modify

### `crates/types/src/lib.rs`

Line 63 (`pub use error::{Error, Fatal, RecoveryClass, StrategyDowngrade, Warning, WarningKind};`)
replaces wholesale with re-exports from the new `error/` module:

```rust
pub use error::account_error::AccountError;
pub use error::builder::AccountErrorBuilder;
pub use error::kind::{
    AccountErrorKind, AccessErrorKind, AuthErrorKind, MailboxUnavailableKind,
    ProtocolErrorKind, RequestErrorKind, ResourceKind, ServerErrorKind,
    SyncStateErrorKind,
};
pub use error::scope::{AccountOperation, ErrorScope, Protocol, Provider};
pub use error::cause::{
    AccessCause, AttemptCause, BatchInputInvalidItem, BatchInputInvalidReason,
    Cause, CauseChain, RequestCause, ServerCause, StateCause,
    TransmissionState, TransportCause, TransportKind, WireCause,
    /* per-protocol WireCause variants */
};
pub use error::diagnostic::{
    DetailVisibility, DiagnosticInfo, DiagnosticText, SupportExportConsented,
    SupportExportInternal, SupportExportMinimal, TelemetryView,
};
pub use error::recovery::{
    EngineDirective, Fatal, ReconcileAction, ReconcileAdvice, ReconcileGuidance,
    ReconcileReason, RecoveryClass, RemediationAction, RetryAdvice,
    RetryDisposition, RetryReason, StrategyDowngrade, ThrottleScope,
};
pub use error::batch::{
    BatchFailure, BatchItem, BatchItemId, BatchItemOutcome, BatchOutcome,
    BatchSuccess, BatchUncertain,
};
pub use error::stream::{ItemOutcome, MutationSuccess};
pub use error::warning::{Warning, WarningKind};
```

Line 84 (`pub use mutation::{... MutationOutcome, MutationResult ...}`)
is **not modified** by Phase 1. `mutation.rs` still contains
`MutationResult` and `MutationOutcome` (those are Phase 3
deletions); leaving the re-export keeps `lib.rs` consistent with
`mutation.rs` as it stands. Phase 3 removes both the types and
the re-export in the same commit.

`pub mod error;` at line 23 stays (the module is still called
`error`; the file becomes a directory).

## Files to delete

- `crates/types/src/error.rs` (replaced by `error/` module).

## Out of scope for Phase 1

Phase 3 owns the following surface migrations. They appear here
only so the next agent knows what is *not* being touched in this
phase:

- `crates/types/src/account.rs`: trait imports, the ~45
  `Result<_, Error>` return-type sites, `bulk_set_flags` /
  `bulk_move` / `bulk_destroy` signatures
  (`AccountStream<SyncEvent<MutationResult>>` →
  `AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>`), the
  default impl of `inventory_partition_stream` constructing
  `SyncEvent::Fatal(Fatal { ... })` at lines 127-143, and the
  `Err(Error::Unsupported)` short-circuits in the convenience
  default impls.
- `crates/types/src/events.rs`: the
  `use crate::error::{Error, Fatal, Warning}` import, the
  `Fatal(Fatal)` variant on `SyncEvent` at line 154 (renamed to
  `Terminated(AccountError)` in Phase 3), the `Control` trait's
  `Result<Checkpoint, Error>` returns.
- `crates/types/src/mutation.rs`: the `use crate::error::Error`
  import, `MutationResult` and `MutationOutcome` removal.

## Type definitions

See `plans/error-model-convergence.md` for the canonical
definitions. Implementation notes follow for the non-trivial cases.

### `AccountError`

```rust
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct AccountError {
    kind: AccountErrorKind,
    recovery: RecoveryClass,
    remediation: Option<RemediationAction>,
    scope: Option<ErrorScope>,
    operation: Option<AccountOperation>,
    provider: Option<Provider>,
    protocol: Option<Protocol>,
    diagnostics: Arc<DiagnosticInfo>,
    chain: Arc<CauseChain>,
}
```

All fields private. Accessors per the convergence plan §Public shape.

`StdError` impl: `source()` returns the outermost `Cause` cast to
`&dyn StdError`. Each `Cause` variant's own `source()` returns
`None`. The current cause-type definitions do not store an
`Arc<dyn StdError + Send + Sync>` for an embedded system error;
threading one through would require Clone-via-Arc on every cause
variant and serialization carve-outs for `serde::Serialize` on
the support exports - not worth the complexity for the marginal
benefit of one extra level of standard-walker visibility.

The wire-level text (the formatted message that the underlying
system error would have produced) is captured as `DiagnosticText`
on `TransportCause::message` (and equivalents); support exports
surface it via `support_internal()`. Standard ecosystem walkers
(`anyhow`, `tracing`) see one typed cause level via
`AccountError::source()`; the typed chain is reached only through
`AccountError::chain().iter()`.

`Display`: short form using `message_key` + kind discriminant
(e.g. `"server.rate-limited (Server(RateLimited))"`). Intended for
log lines, not user display.

### `AccountErrorBuilder::build()`

1. Construct `CauseChain` from `[primary_cause, ...chain_extras]`.
   Non-empty by construction.
2. Validate `kind_matches_cause(&kind, &primary_cause)`. On
   mismatch: `debug_assert!`, then continue with a defensive
   classification (the kind is the source of truth in release;
   producers fix mismatches via unit tests).
3. Call `recovery::derive(...)` to produce `RecoveryClass`.
4. Call `recovery::suggest(...)` to produce
   `Option<RemediationAction>`.
5. Call `message_key::derive(&kind)` to produce `&'static str`
   (stored on `AccountError` for stable `message_key()` access -
   though since it's a function of `kind`, it's cheap to recompute
   on every call; store on the struct only if profiling shows
   benefit).
6. Wrap `DiagnosticInfo` and `CauseChain` in `Arc`.

### Central recovery mapping

`recovery::derive` implements the table in
`plans/error-model-convergence.md` §Recovery:

```rust
pub(crate) fn derive(
    kind: &AccountErrorKind,
    scope: Option<&ErrorScope>,
    operation: Option<AccountOperation>,
    chain: &CauseChain,
    retry_not_before: Option<SystemTime>,
    throttle_scope: Option<ThrottleScope>,
    idempotency_override: Option<bool>,
) -> RecoveryClass {
    let tx_state = chain.iter().find_map(|c| {
        if let Cause::Attempt(a) = c { Some(a.transmission_state) } else { None }
    });
    let idempotent = idempotency_override
        .unwrap_or_else(|| operation.map_or(true, AccountOperation::is_idempotent));

    match kind {
        AccountErrorKind::Transport(_) => derive_transport(tx_state, idempotent),
        AccountErrorKind::Authentication(a) => derive_auth(a),
        AccountErrorKind::Authorization(a) => derive_authz(a),
        AccountErrorKind::Server(s) => derive_server(s, tx_state, idempotent, retry_not_before, throttle_scope),
        AccountErrorKind::SyncState(s) => derive_sync_state(s, scope),
        AccountErrorKind::ConcurrencyConflict => RecoveryClass::Retry(/* AfterStateRefresh */),
        AccountErrorKind::Request(_) => RecoveryClass::ClientBug,
        AccountErrorKind::NotFound(_) => RecoveryClass::ProviderRefused,
        AccountErrorKind::Unsupported(op) => RecoveryClass::Unsupported(*op),
        AccountErrorKind::Protocol(p) => derive_protocol(p, idempotent),
    }
}
```

`recovery::suggest` mirrors this shape and produces
`Option<RemediationAction>`.

### `BatchOutcome::iter()` ordering

`iter()` yields items in submission order. The lane `Vec`
accessors yield items in submission order within each lane.

Representation is the implementing agent's call. The plan
specifies the invariant; how `BatchOutcome` threads submission
order through the public `succeeded` / `failed` / `uncertain`
fields plus the `iter()` method is implementation detail to be
worked out against the type system.

### `WireCause`

Closed `#[non_exhaustive]` enum:

```rust
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum WireCause {
    Graph(GraphSignal),
    Jmap(JmapMethod),
    Imap(ImapResponseCode),
    Smtp(EnhancedStatusCode),
    Gmail(GmailSignal),
}
```

The protocol-native enum types live in `bifrost-types`
(`error/cause.rs`) because the cause chain is part of the public
API. Each per-protocol enum is `#[non_exhaustive]`.

**Ownership: Phase 1 ships every wire enum as complete as we can
make it.** The convergence plan's recovery table implies a
specific vocabulary per protocol; Phase 1 ships those variants
plus any additional variants documented in the per-crate
scaffolds. Protocol crates in Phase 2 cannot edit
`bifrost-types/src/error/cause.rs` - that violates crate
ownership boundaries. If a Phase 2 agent needs a new wire-enum
variant, the orchestrator patches `bifrost-types` centrally and
re-runs the affected agent. Crate-ownership rules are
non-negotiable; centralized patches to wire enums are the escape
hatch.

### `Fatal` newtype

```rust
pub struct Fatal(pub AccountError);

impl TryFrom<AccountError> for Fatal {
    type Error = AccountError;
    fn try_from(err: AccountError) -> Result<Self, Self::Error> {
        if err.recovery().is_terminal() {
            Ok(Fatal(err))
        } else {
            Err(err)
        }
    }
}
```

### `Warning`

```rust
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Warning {
    pub kind: WarningKind,
    pub message: DiagnosticText,
    pub next_action: Option<DiagnosticText>,
    pub protocol_detail: Option<DiagnosticText>,
    pub retry_count: u32,
}
```

`WarningKind` retains the existing categories (`StrategyDowngraded`,
`OperatorAttentionNeeded`, `Throttled`, `ClockSkew`,
`BlobNotByteStream`, `ReadbackSkipped`, `Other`). `Other` carries
no payload - putting free text inside the kind is the same
anti-pattern that `RequestErrorKind::BatchInputInvalid` already
avoids. The detail belongs in `Warning.protocol_detail`, which is
a `DiagnosticText` and inherits the visibility discipline.

Existing `Warning` fields `message: String`, `next_action: Option<String>`,
`protocol_detail: Option<String>` all upgrade to `DiagnosticText`.

### `EngineDirective::CapabilityChanged`

Reuses the existing `capabilities::CapabilityDelta`:

```rust
use crate::capabilities::CapabilityDelta;

EngineDirective::CapabilityChanged { delta: CapabilityDelta }
```

**Phase 1 modifies `capabilities.rs`** to add `PartialEq, Eq`
derives to `CapabilityKey`, `CapabilityValue`, `CapabilityChange`,
and `CapabilityDelta`. The new `RecoveryClass` and `EngineDirective`
derive `PartialEq, Eq` (the recovery mapping tests need it for
assertions), and that propagates: every type they transitively
contain must also derive both. `CapabilityKey(String)` and
`CapabilityValue(String)` derive trivially; `CapabilityChange` and
`CapabilityDelta` follow. This is the only non-`error/` Phase 1
edit besides `lib.rs`.

### `StrategyDowngrade`

Moves from `error.rs:127-132` to `error/recovery.rs`. The type
itself (the enum with `QResyncToCondstore` and `CondstoreToBasic`)
stays the same shape. Existing re-export at `lib.rs:63` updates to
re-export from the new location.

## Test plan

Tests cannot run in Phase 1: the crate does not compile (the
trait, event, and mutation surfaces still reference removed
types). Phase 3 is the earliest point the workspace compiles and
the test suite runs.

The tests themselves are written in Phase 1 alongside the types -
they live in `#[cfg(test)]` modules inside each `error/*.rs`
file. They simply don't execute until integration. Per
`CLAUDE.md`: small technical tests, no live servers, no mock
servers. Targets:

- Recovery mapping (`error/recovery.rs`): one test per
  convergence-plan mapping-table row. ~40-50 tests.
- Message-key (`error/message_key.rs`): one test per documented
  key. ~30 tests.
- Builder invariants (`error/builder.rs`): chain non-empty,
  `kind_matches_cause` panic in debug, `throttle_scope`
  restricted to rate-limit / quota, `idempotency_override`
  precedence, `retry_not_before` propagation. ~10 tests.
- Diagnostic accessors (`error/account_error.rs`):
  `user_safe_text` yields only `UserSafe`, `telemetry_fields`
  carries no free-form text, `support_internal` includes
  serialized chain, `serde::Serialize` round-trips. ~6 tests.
- Batch shape (`error/batch.rs`): `iter()` yields submission
  order, lane `Vec` accessors preserve submission order within
  lane, `validate_batch_input` rejects empty and duplicate
  `BatchItemId`. ~5 tests.
- Cause chain (`error/cause.rs`): `StdError::source()` returns
  outermost cause, `CauseChain::iter()` yields outer-to-inner,
  `Cause::Attempt` carries `TransmissionState`, embedded
  `StdError::source()` works. ~4 tests.

Total: ~95-100 deterministic tests. All execute after Phase 3.

## Exit criteria

Phase 1 validates by patch audit, not by `brokkr check` or
`brokkr test`. The workspace does not compile after this phase,
by design.

Patch-audit criteria:

- `crates/types/src/error/` directory exists with the file
  structure above (or a defensible variation).
- `crates/types/src/error.rs` deleted.
- Every public type from `plans/error-model-convergence.md` is
  present in the new module.
- `crates/types/src/lib.rs` re-exports point to the new module;
  old re-exports of `Error`, `Fatal`, `RecoveryClass`,
  `StrategyDowngrade`, `Warning`, `WarningKind`,
  `MutationResult`, `MutationOutcome` removed.
- `AccountError` is opaque (no `pub` fields).
- `AccountErrorBuilder::build` is the only public construction
  path for `AccountError`.
- `recovery::derive` implemented with every mapping-table row
  covered (test in `#[cfg(test)]`; executes in Phase 3).
- `message_key::derive` implemented with every documented key
  covered (test in `#[cfg(test)]`; executes in Phase 3).
- `Fatal` newtype defined with `TryFrom<AccountError>`.
- `Warning` uses `DiagnosticText` for all free-form fields;
  `WarningKind::Other` carries no payload.
- All wire enums (`GraphSignal`, `JmapMethod`, `ImapResponseCode`,
  `EnhancedStatusCode`, `GmailSignal`) shipped in `error/cause.rs`
  with their initial vocabularies.
- `crates/types/src/account.rs`, `events.rs`, `mutation.rs`
  UNCHANGED by this phase.
- `crates/types/src/capabilities.rs` modified: `CapabilityKey`,
  `CapabilityValue`, `CapabilityChange`, `CapabilityDelta` now
  derive `PartialEq, Eq` (required for `RecoveryClass` /
  `EngineDirective` derives to propagate).
- No transitional shims, no compatibility aliases, no
  `#[deprecated]` markers on removed types.

Phase 3 picks up validation: at the end of Phase 3, `brokkr check`
runs clean and the ~95-100 tests written in Phase 1 execute green.

## Phase 1 amendment (post-2.2 correctness pass)

Phase 1 landed as commit `ac47289`. The Phase 2.2 audit surfaced
shape-level corrections that must land **before** Phase 2.3 (sync)
and Phase 3 (integration) so downstream agents build against the
correct surface. These ship as a single amendment commit on the
feature branch.

### Shape corrections (correctness-load-bearing)

1. **`ServerCause::Error::status` becomes `Option<u16>`.**
   - Schema: `Error { status: Option<u16> }`.
   - Rationale: HTTP-like providers carry numeric status as `Some(_)`;
     protocol server failures without numeric status (IMAP `NO`/`BAD`
     outside any response code) carry `None`. The Phase 2.2 IMAP
     agent was forced to invent `status: 0` as a sentinel because
     the field was `u16`, not `Option<u16>` - exactly the bug this
     amendment closes.
   - Migration: the central recovery table gains explicit rows for
     `Server(Error { status: None })` × each `TransmissionState`.
     See `plans/error-model-convergence.md` recovery table.
   - Builder method: `pub fn status(self, status: Option<u16>) -> Self;`.
   - Tests: add `Server(Error { status: None })` derivation test for
     each transmission state.

2. **`AccountError::into_builder()` added to the `impl AccountError`
   block.**
   - Signature: `pub fn into_builder(self) -> AccountErrorBuilder;`.
   - Rationale: the Phase 2.2 Gmail agent reported a manual
     five-step clone-walk-rebuild dance for the TRASH-fallback merge
     pattern (preserve a primary `AccountError`, decorate with
     secondary evidence). `into_builder` returns a builder
     pre-populated with the error's kind, scope, operation, provider,
     protocol, diagnostics, and chain so callers can `push_cause(_)
     .build()` to add evidence while keeping `build()` as the single
     invariant funnel.
   - Explicitly **not** added: a `with_secondary_cause` mutator on
     `AccountError`. Such a mutator would either bypass `build()`
     (losing the invariant funnel) or duplicate it (drifting).
   - Tests: round-trip an `AccountError` through `into_builder()
     .build()` and assert all observable fields are unchanged; then
     push an additional `Cause` and assert the chain grows correctly
     without other field drift.

### Known-vocabulary additions to wire enums

Phase 1 originally shipped wire enums "as complete as we can make
them" with the escape hatch that the orchestrator centrally patches
`bifrost-types` if Phase 2 agents need new variants. Phase 2.2
exercised the escape hatch:

3. **`JmapMethod` gains typed variants for the known `SetErrorType`
   family.** Routing known JMAP set-error codes through
   `JmapMethod::Unknown { code }` plus string matching is forbidden
   per the convergence plan; the following must be named variants:
   - `Forbidden`
   - `OverQuota`
   - `TooLarge`
   - `RateLimit`
   - `NotFound`
   - `InvalidPatch`
   - `WillDestroy`
   - `Singleton`
   - `MailboxHasChild`
   - `MailboxHasEmail`
   - `BlobNotFound`
   - `TooManyKeywords`
   - `TooManyMailboxes`
   - `ForbiddenFrom`
   - `InvalidEmail`
   - `TooManyRecipients`
   - `NoRecipients`
   - `InvalidRecipients`
   - `ForbiddenMailFrom`
   - `ForbiddenToSend`
   - `CannotUnsend`
   - `AlreadyExists`
   - `InvalidScript`
   - `ScriptIsActive`
   - `InvalidProperties`

   The semantic requirement is the named coverage; if Phase 1's
   provider-error-code enum is named something other than
   `JmapMethod`, the variants go there. `Unknown { code }` remains
   for genuine forward compatibility (codes the spec adds after this
   amendment).

4. **`GraphSignal` gains `InvalidDeltaToken` and `SyncStateNotFound`.**
   - `InvalidDeltaToken` (HTTP 400, Graph delta link expired before
     410 Gone).
   - `SyncStateNotFound` / `syncStateNotFound` (HTTP 400 from
     mail/calendar delta endpoints).
   - Routing these through `GraphSignal::Unknown { code }` plus
     exact-string match in `is_cursor_invalid_unknown` is forbidden.
     Graph substring or exact-string recovery must be gone by
     Phase 3 exit.

### Shared helpers (ergonomics, but unblock multiple crates)

5. **Promote `bifrost_types::error::batch::validate_batch_input` to
   `pub`.**
   - Phase 2.2 SMTP duplicated the validator inside the crate because
     it was `pub(crate)`. Promoting saves the duplication and ensures
     a single source of truth for empty / duplicate `BatchItemId`
     rejection.

6. **Promote `bifrost-net::request::parse_retry_after` to `pub`** (or
   provide a thin `pub` wrapper at the crate root).
   - Phase 2.2 Gmail inlined a copy of the parser because the net
     helper was `pub(crate)`. Every protocol crate that reads
     `Retry-After` will hit the same problem.

### Out of scope for the amendment

- `BatchItemId::new` ergonomic constructor. The field is already
  `pub String`; the constructor is convenience-only and not
  correctness-load-bearing.
- Any new derivation rules in `recovery::derive` beyond the new
  `Server(Error { status: None })` rows.

### Amendment exit criteria

- `cargo expand` (or equivalent inspection) shows `ServerCause::Error`
  with `status: Option<u16>`.
- `cargo expand` shows the new `JmapMethod` and `GraphSignal`
  variants.
- `AccountError::into_builder()` exists, returns the builder type,
  and a round-trip test passes.
- `validate_batch_input` and `parse_retry_after` are reachable from
  outside their defining crates.
- The ~95-100 Phase 1 tests still pass (they execute under
  `#[cfg(test)]` in `bifrost-types`); existing assertions remain
  valid against the corrected shapes.
- No transitional shims; the amendment is a clean shape change.

The amendment lands as one commit on `error-model/main` between
Phase 2.2 and Phase 2.3 so sync builds against the corrected surface.
