# Error model: bifrost-types implementation plan

This is Phase 1 of `plans/error-model-roadmap.md`. It covers every
change in the `bifrost-types` crate that the convergence plan
demands. After this phase lands on the feature branch, the workspace
will not compile until Phase 2 migrates the consumer crates — that is
the intentional state of the branch.

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

- `crates/types/src/error.rs` — `Error`, `Fatal`, `RecoveryClass`
  (with `Retry { after: Duration }`, `DowngradeStrategy`,
  `DowngradeCapabilityForScope`, `RestartScope`, `RestartAccount`,
  `AuthLost`, `SchemaIncompatible`, `CapabilityChanged`,
  `OperatorOverrideRequired`, `Fatal`), `StrategyDowngrade`,
  `Warning`, `WarningKind`.
- `crates/types/src/events.rs:150-156` — `SyncEvent<T>` with
  `Batch / Progress / Warning / Fatal / Done` variants.
  `SyncEvent::Fatal(Fatal)` at line 154 is the rename target.
- `crates/types/src/events.rs:289-300` — `Control` trait. Two
  method returns use `Error` and must update to `AccountError`.
- `crates/types/src/mutation.rs:155-171` — `MutationResult` and
  `MutationOutcome` with `Failed(Error)` arm.
- `crates/types/src/account.rs:34` — `use crate::error::{Error, Fatal, RecoveryClass}`.
  The `Account` trait at line 63 uses `Result<_, Error>` in
  every method signature.
- `crates/types/src/account.rs:127-143` — default impl of
  `inventory_partition_stream` constructs
  `SyncEvent::Fatal(Fatal { recovery: RecoveryClass::Fatal, ... })`.
  This is the one in-crate emission site that Phase 1 owns.
- `crates/types/src/capabilities.rs:296-313` —
  `CapabilityChange`, `CapabilityDelta`, `CapabilityKey`,
  `CapabilityValue`. Already public; the new
  `EngineDirective::CapabilityChanged { delta }` reuses
  `CapabilityDelta` verbatim.
- `crates/types/src/cursor.rs` — `CursorScope`, `SyncStrategy`,
  `ChangeCursor`, `CursorEstablishment`. No `Error` references.
  `EngineDirective::DowngradeStrategy(StrategyDowngrade)` reuses
  `StrategyDowngrade` (currently in `error.rs:127-132`); the type
  itself moves to the new recovery module.
- `crates/types/src/lib.rs:63` —
  `pub use error::{Error, Fatal, RecoveryClass, StrategyDowngrade, Warning, WarningKind};`.
  Re-exports change wholesale.

## Scope

What this phase changes in `bifrost-types`:

- Replaces `Error` (per-operation enum at `error.rs:27-79`) with the
  opaque `AccountError`.
- Replaces `RecoveryClass` (at `error.rs:89-121`) with the new
  enum whose terminal variants are decomposed and whose engine-
  control variants live nested under `EngineDirective`.
- Replaces `Fatal` (the struct at `error.rs:136-140`) with the
  `Fatal(AccountError)` newtype in the new recovery module.
- Updates `Warning` (at `error.rs:144-150`) to use `DiagnosticText`
  for free-form fields.
- Reshapes `MutationOutcome` (at `mutation.rs:163-171`) into
  `MutationSuccess { Applied, Skipped }`. The streaming bulk surface
  uses `ItemOutcome<MutationSuccess>` instead of `MutationResult`.
- Renames `SyncEvent::Fatal(Fatal)` (at `events.rs:154`) to
  `SyncEvent::Terminated(AccountError)`.
- Introduces `AccountErrorBuilder` as the only construction path.
- Introduces the central recovery mapping in
  `bifrost-types::recovery::derive`.
- Introduces `message_key` derivation.
- Introduces the diagnostic accessor surface (`TelemetryView`,
  `SupportExportMinimal`, `SupportExportConsented`,
  `SupportExportInternal`).
- Introduces the batch surface (`BatchItem`, `BatchOutcome`,
  `BatchItemOutcome`, `BatchSuccess`, `BatchFailure`,
  `BatchUncertain`, `BatchItemId`).
- Introduces the cause chain (`Cause`, `CauseChain`, `AttemptCause`,
  `TransmissionState`, all `*Cause` types, all subkind enums).

What this phase does **not** change:

- `Account` trait method signatures (`crates/types/src/account.rs:63-595`).
  Phase 3 owns trait surface migration. After Phase 1, the trait's
  `use crate::error::{Error, Fatal, RecoveryClass}` import at line
  34 is broken; `cargo check -p bifrost-types` will refuse to build
  unless the trait is updated. **Resolution:** Phase 1 updates the
  trait imports in place (changing `Result<_, Error>` to
  `Result<_, AccountError>` is the smallest change that lets the
  crate compile), even though the convergence plan classifies trait
  signature changes as Phase 3. The alternative — leaving the trait
  broken until Phase 3 — means `bifrost-types` itself fails to
  compile after Phase 1, which violates the Phase 1 exit criterion
  ("`cargo check -p bifrost-types` clean"). The Phase 3 invasiveness
  (`Result<(), Error>` → `Result<(), AccountError>` ripple across
  every consumer impl) still happens in Phase 3; Phase 1 only
  changes the trait's *declarations*. Same-named, different-type:
  consumer impls break, types crate compiles.
- `Control` trait at `events.rs:289-300` — same treatment as
  `Account`: imports updated, declarations switch to `AccountError`,
  consumer impls in `bifrost-sync` break until Phase 2.3.
- Inventory streaming, change streaming, push subscription types
  beyond the `SyncEvent::Fatal` → `SyncEvent::Terminated` rename.

## Module layout

The current single-file `error.rs` (188 lines) becomes a
multi-file `error/` module. The new model is ~3x the surface area
of the old; one file would push past the project's source-bloat
threshold.

```
crates/types/src/
├── account.rs            // modified: imports + trait signature
│                         //   declarations (Result<_, AccountError>)
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
├── events.rs             // modified: SyncEvent::Fatal renamed,
│                         //   Control trait signatures updated
├── hydration.rs          // unchanged
├── ids.rs                // unchanged
├── lib.rs                // modified: re-exports
├── mutation.rs           // modified: MutationOutcome reshape,
│                         //   MutationResult removed, Error import
│                         //   removed
├── page.rs               // unchanged
├── search.rs             // unchanged
└── settings.rs           // unchanged
```

15 files total: 9 new (under `error/`), 4 modified (`account.rs`,
`events.rs`, `lib.rs`, `mutation.rs`), 1 deleted (`error.rs`).
Eleven files untouched.

## Files to create

In dependency order (each file depends on types defined above it):

1. `crates/types/src/error/scope.rs` — `ErrorScope`,
   `AccountOperation`, `Provider`, `Protocol`. No dependencies
   beyond existing types (`CursorScope` from `cursor.rs`).
2. `crates/types/src/error/kind.rs` — `AccountErrorKind` and all
   subkind enums. Depends on `scope.rs` (for `AccountOperation` in
   `Unsupported(AccountOperation)`) and on `BatchItemId` from
   `batch.rs` — but only via `cause.rs`, not directly.
3. `crates/types/src/error/cause.rs` — all `*Cause` types,
   `Cause`, `CauseChain`. Depends on `kind.rs` and `scope.rs`. The
   `WireCause` enum's per-protocol variants (`GraphSignal`,
   `JmapMethod`, `ImapResponseCode`, `EnhancedStatusCode`,
   `GmailSignal`) are defined here as `#[non_exhaustive]` enums
   that the protocol crates fill in via their builder calls.
4. `crates/types/src/error/diagnostic.rs` — `DiagnosticInfo`,
   `DiagnosticText`, `DetailVisibility`, export view types.
   Depends on `kind.rs`, `scope.rs`, `cause.rs`. The
   `TelemetryView::from_account_error` constructor lives elsewhere
   (in `account_error.rs`) because it needs access to
   `AccountError`'s private fields.
5. `crates/types/src/error/batch.rs` — `BatchItem<I>`,
   `BatchOutcome<T>`, `BatchItemOutcome`, `BatchSuccess<T>`,
   `BatchFailure`, `BatchUncertain`, `BatchItemId`. Depends on
   nothing else in error/ except `account_error.rs` (via
   `BatchFailure.error: AccountError`) — declare with a forward
   reference, define after `account_error.rs` lands. Also
   exposes `pub(crate) fn validate_batch_input` for the
   preflight uniqueness / emptiness check.
6. `crates/types/src/error/stream.rs` — `ItemOutcome<T>`,
   `MutationSuccess`. Depends on `batch.rs`.
7. `crates/types/src/error/warning.rs` — `Warning`, `WarningKind`.
   Depends on `diagnostic.rs`.
8. `crates/types/src/error/recovery.rs` — `RecoveryClass`,
   `RetryAdvice`, `RetryDisposition`, `RetryReason`,
   `ReconcileAdvice`, `ReconcileReason`, `ReconcileGuidance`,
   `ReconcileAction`, `EngineDirective`, `ThrottleScope`,
   `RemediationAction`, `Fatal` newtype, the moved
   `StrategyDowngrade`, plus the `derive`, `suggest`, and
   `kind_matches_cause` functions. Depends on `kind.rs`,
   `scope.rs`, `cause.rs`, and `crate::capabilities::CapabilityDelta`.
9. `crates/types/src/error/account_error.rs` — `AccountError`
   struct, all accessors, `StdError` impl, `Display` impl,
   `TelemetryView::from_account_error` constructor, the
   support-export constructors. Depends on every module above.
10. `crates/types/src/error/builder.rs` — `AccountErrorBuilder`
    and `build()`. Depends on `account_error.rs` and
    `recovery.rs::derive` / `suggest`.
11. `crates/types/src/error/message_key.rs` —
    `derive_message_key(&AccountErrorKind) -> &'static str`.
    Depends on `kind.rs`.
12. `crates/types/src/error/mod.rs` — `pub use` re-exports.
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
updates to remove `MutationOutcome` and `MutationResult` from the
re-export list.

`pub mod error;` at line 23 stays (the module is still called
`error`; the file becomes a directory).

### `crates/types/src/account.rs`

Line 34 (`use crate::error::{Error, Fatal, RecoveryClass};`) replaces with:

```rust
use crate::error::{AccountError, Fatal, RecoveryClass};
```

Every `Result<_, Error>` in trait method declarations (lines 107,
161, 164, 232, 242, 252, 261, 270, 279, 287, 296, 307, 310, 318,
321, 326, 335, 341, 348, 360, 367, 375, 380, 387, 395, 398, 401,
404, 413, 420, 462, 472, 482, 488, 508, 521, 530, 546, 551, 567,
584, 595, 617) replaces with `Result<_, AccountError>`. ~45 sites.

`crates/types/src/account.rs:41` import of `MutationResult` from
`mutation.rs` updates to import `ItemOutcome` and `MutationSuccess`
from `error::stream`.

Lines 194-214 (`bulk_set_flags`, `bulk_move`, `bulk_destroy`
signatures returning `AccountStream<SyncEvent<MutationResult>>`)
update to `AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>`.

Lines 127-143 (default impl of `inventory_partition_stream`)
updates to construct an `AccountError` via the new builder and
emit `SyncEvent::Terminated(account_error)`:

```rust
SyncEvent::Terminated(
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(AccountOperation::SyncInventory),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::user_safe(
                "inventory partition is not supported by this account",
            ),
        }),
    )
    .operation(AccountOperation::SyncInventory)
    .scope(ErrorScope::Cursor(scope.clone()))
    .build(),
),
```

Default impls returning `Err(Error::Unsupported)` (lines 463, 521,
546, 567, 585 — the `Unsupported` short-circuits in conveniences)
update to:

```rust
Box::pin(async {
    Err(AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(/* the appropriate op */),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::user_safe("convenience not supported"),
        }),
    )
    .build())
})
```

These default-impl conversion sites are the test surface for the
builder during Phase 1.

### `crates/types/src/events.rs`

Line 12 (`use crate::error::{Error, Fatal, Warning};`) replaces with:

```rust
use crate::error::{AccountError, Warning};
```

Line 154 (`Fatal(Fatal)`) renames to
`Terminated(AccountError)`.

Lines 292 and 295 (`Control::pause` and `Control::checkpoint_now`
returning `Result<Checkpoint, Error>`) replace with
`Result<Checkpoint, AccountError>`.

### `crates/types/src/mutation.rs`

Line 12 (`use crate::error::Error;`) removed.

Lines 155-171 (`MutationResult`, `MutationOutcome`) deleted or
reshaped: `MutationResult` removed entirely; `MutationOutcome`
removed. The new shape (`ItemOutcome<T>`, `MutationSuccess`) lives
in `error/stream.rs`.

If anything in `mutation.rs` still needs to express "per-item
outcome" inside the `IdempotencyKey` / `FlagOp` infrastructure
(it does not, based on the current shape), it imports the new
types from `error::stream`.

## Files to delete

- `crates/types/src/error.rs` (replaced by `error/` module).

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
`&dyn StdError`. Each `Cause` variant's own `source()` returns its
embedded system error (`io::Error`, `rustls::Error`, etc.) where
one exists; it does NOT traverse the typed chain. The convergence
plan documents this limit honestly.

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
   (stored on `AccountError` for stable `message_key()` access —
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

`iter()` yields items in submission order. Implementation: each
`BatchSuccess` / `BatchFailure` / `BatchUncertain` carries a
private `submission_index: u32` field set by the protocol crate at
emission time. `BatchOutcome` stores a `Vec<BatchItemOutcomeOwned>`
in submission order internally; the lane `Vec` fields are derived
views (or built once at construction with index-mapped clones).

For Phase 1, define `BatchItemOutcomeOwned` as a `pub(crate)`
internal enum and let `BatchItemOutcome<'a, T>` (the public
borrowed view) implement `From<&'a BatchItemOutcomeOwned>`. Lane
field iteration uses the owned form; `iter()` walks the submission-
order vec.

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
API. Each per-protocol enum is `#[non_exhaustive]`. Phase 1 ships
them as minimal vocabularies (the variants implied by the
convergence plan's recovery table); protocol crates extend the
vocabularies during Phase 2 by adding variants (non-breaking).

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
`BlobNotByteStream`, `ReadbackSkipped`, `Other`). The `Other(String)`
variant becomes `Other(DiagnosticText)`.

Existing `Warning` fields `message: String`, `next_action: Option<String>`,
`protocol_detail: Option<String>` all upgrade to `DiagnosticText`.

### `EngineDirective::CapabilityChanged`

Reuses the existing `capabilities::CapabilityDelta` verbatim:

```rust
use crate::capabilities::CapabilityDelta;

EngineDirective::CapabilityChanged { delta: CapabilityDelta }
```

No changes to `capabilities.rs`.

### `StrategyDowngrade`

Moves from `error.rs:127-132` to `error/recovery.rs`. The type
itself (the enum with `QResyncToCondstore` and `CondstoreToBasic`)
stays the same shape. Existing re-export at `lib.rs:63` updates to
re-export from the new location.

## Test plan

Per `CLAUDE.md` testing rules: small technical tests, no live
servers, no mock servers. Target ~95-100 tests, all sub-second.

**Recovery mapping tests** (`error/recovery.rs` test module):
- Every row in the convergence plan's mapping table becomes at
  least one test. Construct an input `(kind, scope, operation,
  chain, ...)` and assert the produced `RecoveryClass`.
- ~40-50 tests.

**Message-key tests** (`error/message_key.rs` test module):
- One test per documented key.
- ~30 tests.

**Builder invariant tests** (`error/builder.rs` test module):
- Chain non-empty (by construction).
- `kind_matches_cause` panic in debug builds on mismatch.
- `throttle_scope` set only for `RateLimited` / `QuotaExhausted`.
- `idempotency_override` applied before operation default.
- `retry_not_before` populated for relevant reasons.
- ~10 tests.

**Diagnostic accessor tests** (`error/account_error.rs` test
module):
- `user_safe_text()` yields only `UserSafe` items.
- `telemetry_fields()` carries no free-form text.
- `support_minimal()` is structurally identical to
  `telemetry_fields()`.
- `support_internal()` includes serialized chain.
- `serde::Serialize` round-trips for each export tier.
- ~6 tests.

**Batch shape tests** (`error/batch.rs` test module):
- `BatchOutcome::iter()` yields submission order.
- Lane `Vec` accessors preserve submission order within lane.
- `BatchItemId` empty rejected by `validate_batch_input`.
- `BatchItemId` duplicate rejected by `validate_batch_input`.
- `BatchItemOutcome` exhaustiveness (closed enum check).
- ~5 tests.

**Cause chain tests** (`error/cause.rs` test module):
- `StdError::source()` returns outermost cause.
- `CauseChain::iter()` yields outer-to-inner.
- `Cause::Attempt` carries `TransmissionState`.
- Each variant's embedded `StdError::source()` works.
- ~4 tests.

## Exit criteria

- `crates/types/src/error/` directory exists with the file
  structure above (or a defensible variation).
- `crates/types/src/error.rs` deleted.
- Every public type from `plans/error-model-convergence.md` is
  present and exported from `lib.rs`.
- `AccountError` is opaque (no `pub` fields).
- `AccountErrorBuilder::build` is the only public construction
  path for `AccountError`.
- Central recovery mapping (`recovery::derive`) implemented;
  every mapping-table row unit-tested.
- `message_key::derive` implemented; every documented key
  unit-tested.
- Old `Error` enum, old `RecoveryClass`, old `Fatal` struct
  REMOVED.
- `MutationResult` and `MutationOutcome` removed from
  `mutation.rs`; `ItemOutcome<MutationSuccess>` defined in
  `error/stream.rs`.
- `Warning` uses `DiagnosticText` for all free-form fields.
- `WarningKind::Other(String)` → `Other(DiagnosticText)`.
- `SyncEvent::Fatal(Fatal)` renamed to
  `SyncEvent::Terminated(AccountError)`.
- `Fatal` newtype defined with `TryFrom<AccountError>`.
- `Control` trait signatures updated to `AccountError`.
- `Account` trait signatures updated to `AccountError`
  (declarations only; consumer impls in protocol crates break
  until Phase 2).
- Default impl of `inventory_partition_stream` and the
  `Unsupported`-short-circuit conveniences updated to construct
  `AccountError` via the builder.
- `cargo check -p bifrost-types` clean.
- `cargo test -p bifrost-types` passes (~95-100 tests).
- Workspace `brokkr check` fails only at consumer crate
  references — no other regressions.
- No transitional shims, no compatibility aliases, no
  `#[deprecated]` markers on removed types.
