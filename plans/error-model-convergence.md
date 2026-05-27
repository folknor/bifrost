# Error model convergence

This document specifies the bifrost error contract. It is the target
design, not a phased rollout. There is no "this wave" and no "later
wave" - the shape described here is what consumers can rely on.

## Phase 5 amendments (post-merge)

The post-Phase-4 audit
(`plans/error-model-phase4-audit.md`) and the locked decisions in
`plans/error-model-phase4-decisions.md` revised several shapes
described in this doc. Where the body below disagrees with these
amendments, the amendments win. The decisions doc is the live tracker;
the convergence doc retains historical context that the amendments
modify in place.

- **`RetryAdvice` carries `retry_hint: Option<RetryHint>`** instead of
  separate `not_before` and `min_delay` fields. `RetryHint::After(Duration)`
  and `RetryHint::At(SystemTime)` are the two carrier shapes; consumers
  call `RetryHint::not_before(now)` and `RetryHint::min_delay(now)` to
  derive either form. `ServerCause::{Unavailable,RateLimited,QuotaExhausted}`
  carries `retry_hint: Option<RetryHint>` instead of `retry_after:
  Option<Duration>`. The builder no longer has a `retry_not_before`
  setter; the hint flows through the cause.
- **`AccountErrorBuilder::build` is renamed to `try_build`** and
  returns `Result<AccountError, AccountErrorBuildError>`. Variants:
  `EmptyChain`, `KindCauseMismatch { kind, primary_cause }`,
  `TransportAcknowledged`, `CursorInvalidWithoutScope`. The error
  never escapes a producer; translation boundaries call
  `.expect("valid account error classification")`.
- **`SyncState(CursorInvalid)` without scope is a build error**
  (`CursorInvalidWithoutScope`). The previous "falls back to
  `RestartAccount`" rule is gone: producers must thread the cursor
  scope from the call site.
- **`EngineDirective::CapabilityChanged { delta }` is removed.**
  Capability shifts map to `EngineDirective::RestartAccount` so
  discovery re-runs. `StateCause::CapabilityChanged { delta:
  Option<CapabilityDelta> }` retains the optional payload for forensic
  exports only.
- **`RemediationAction::RestartScope` and `RestartAccount` variants
  are removed.** Engine actions are not consumer remediations.
  `recovery::suggest` returns `None` for `SyncState(CursorInvalid)`
  and `SchemaIncompatible`.
- **`RemediationAction::RetryLater { not_before }` becomes
  `RetryLater { retry_hint: Option<RetryHint> }`.**
- **`BatchOutcome` is immutable on return; mutable construction goes
  through `BatchOutcomeBuilder` with `push_*` setters and a
  `finalize(expected: &[BatchItemId]) -> Result<BatchOutcome,
  BatchInvariantError>` terminator.** Lane access is via
  `succeeded()` / `failed()` / `uncertain()` slice accessors.
- **`ItemOutcome` is closed** (not `#[non_exhaustive]`). Three lanes
  are the model; adding a fourth is a deliberate breaking change.
- **`Fatal(pub AccountError)` becomes `Fatal(AccountError)` with the
  field private.** `Fatal::into_inner(self) -> AccountError` and
  `impl AsRef<AccountError> for Fatal` give access. `TryFrom` is the
  only constructor.
- **`Protocol(Unknown)` maps to `UnknownPermanent` unconditionally**;
  the "unless paired with higher-level retryable cause" clause is
  gone. Producers with a retryable signal use that as the kind.
- **Recovery table row for `Server(Error { status: None })` is
  reinstated for both `InFlight + idempotent` (-> `Retry::SameRequest`)
  and `InFlight + !idempotent` (-> `Reconcile {
  TransportDropAfterSend }`).**
- **`recovery::derive` uses `debug_assert!`** for the `Transport +
  Acknowledged` and `kind_matches_cause` invariants. In release
  builds, `Transport + Acknowledged` falls back to treating the
  attempt as `InFlight` (conservative). The same invariants are
  rejected at construction by `try_build`.
- **`ResourceKind` widens with `Draft`, `Identity`, `Vacation`,
  `PushSubscription`.** `PushSubscription` is provider-neutral and
  covers Gmail Pub/Sub watch, Graph webhook subscriptions, JMAP push,
  and any future IMAP IDLE abstraction.
- **`ThrottleScope` gains `CurrentOperation`** as a per-call hint
  that does not enter any shared bucket. The other four variants
  (`Mailbox`, `Account`, `Tenant`, `Provider`) map onto sharable
  `ThrottleKey` entries the engine consults across work items.
- **`ThrottleKey` enum exists in `bifrost-types::error`** with
  variants `Mailbox { account, mailbox }`, `Account(AccountId)`,
  `Tenant(String)`, `Provider(Provider)`. `Tenant` and `Provider`
  cross account boundaries; a tenant throttle pauses every account
  on that tenant.
- **`AccountControl { Pause(PauseReason), Resume }` and `PauseReason`
  enum** are the engine pause primitive. `PauseReason` variants:
  `OperatorOverrideRequired`, `ConsumerRequested`, `TenantThrottle`,
  `RetryBudgetExhausted`. No free-form strings.
- **`WatchEvent::Terminated(AccountError)` variant** added so push
  streams terminate with a classified error when the stream cannot
  be reconnected without engine intervention.
- **`Account::get_stream` returns
  `AccountStream<SyncEvent<ItemOutcome<HydratedObject>>>`** so per-item
  hydration failures surface as `ItemOutcome::Failed` /
  `ItemOutcome::Uncertain` rather than dropped warnings.
- **`EnhancedStatusCode::code` field is renamed to `reply`** to
  disambiguate from the dotted enhanced status (X.Y.Z) carried in
  the `enhanced` field.
- **`AccountErrorBuilder::idempotency_override`** is reserved for
  exceptional cases (e.g. a normally non-idempotent op made safe by
  an `If-Match` etag). `AccountOperation::is_idempotent()` is the
  authoritative source for the common case; producers reaching for
  the override should verify their operation classification first.

Revised after a three-round critique pass focused on the enterprise
consumer surface. The contract:

- `AccountError` is opaque and builder-constructed (drift between
  derived fields is structurally impossible).
- `RecoveryClass::Fatal` is decomposed into product-meaningful
  terminal dispositions (`AuthLost`, `NeedsAdminConsent`,
  `NeedsPolicyChange`, `NoPermission`, `Unsupported`, `ClientBug`,
  `ProviderContractViolation`, `ProviderRefused`, `UnknownPermanent`).
- `Retry` carries only safe-retry dispositions (`SameRequest`,
  `AfterStateRefresh`, `AfterAuthRefresh`); cases requiring
  reconciliation get a separate top-level `Reconcile(ReconcileAdvice)`
  variant. There is no `DoNotRetry` disposition - the previous
  contradictory shape (`Retry { DoNotRetry }`) is gone.
- Engine-control directives (`RestartScope`, `RestartAccount`,
  `DowngradeStrategy`, `DowngradeCapabilityForScope`,
  `SchemaIncompatible`, `CapabilityChanged`, `OperatorOverrideRequired`)
  live in a nested `EngineDirective` enum under
  `RecoveryClass::Engine(...)`. They share the carrier with errors
  but are categorized as "engine takes a directed action," not as
  retry, reconcile, or terminal. Four mutually exclusive helpers:
  `is_retryable`, `requires_reconciliation`, `requires_engine_action`,
  `is_terminal`.
- Streaming bulk operations return per-item
  `ItemOutcome<T>` envelopes with the same three lanes as Vec-batch.
  Streaming invariants are explicit: every pulled item produces
  exactly one outcome, locally-invalid items emit `Failed` rather
  than poisoning the stream, early termination via
  `SyncEvent::Terminated(AccountError)` covers only already-pulled
  items.
- `MutationSuccess::{Applied, Skipped}` folds the read-back guard's
  "already-in-state" outcome into success; no fourth batch lane.
- `Warning` is explicitly outside the error model but shares the
  `DiagnosticText` visibility discipline.
- `SyncEvent::Fatal(Fatal)` renamed to
  `SyncEvent::Terminated(AccountError)`. The new-model `Fatal` is a
  separate newtype from `bifrost-types::recovery` that collapses
  terminal `RecoveryClass` values at the engine boundary.
- `TransmissionState` (`Unsent` / `InFlight` / `Acknowledged`) lives
  on `AttemptCause`, which is the single evidence carrier for whether
  a network attempt happened and how far it got. It drives `Reconcile`
  classification for non-idempotent operations whose request was in
  flight when the transport dropped. Absence of `AttemptCause` from
  the chain means no network attempt was made; recovery classifies
  absence the same as `Unsent`, but support exports preserve the
  distinction. `TransportCause` does not carry transmission state.
- `RetryAdvice` carries `throttle_scope` for rate-limit and quota
  failures so engines can pause at the right granularity (request,
  mailbox, account, tenant, provider).
- `AccountOperation` is split finely enough that `is_idempotent()`
  does not hedge; the generic `Mutate` variant is gone.
- Every error carries a stable `message_key`. The dotted namespace
  is the documented fallback convention.
- Raw diagnostic text is reached only through filtered accessors;
  `TelemetryView` is structured fields only, with no free-form text.
- Support exports are structured and `serde::Serialize` at three
  consent tiers: minimal, consented, internal.
- `StdError::source()` bridges to the typed chain for ecosystem
  interop.
- Multi-target operations return `Result<BatchOutcome<T>, AccountError>`
  with three lanes (succeeded, failed, uncertain) and caller-correlated
  `BatchItemId`. `Err` means "didn't transmit"; `Ok(BatchOutcome)`
  means "transmitted, every item is accounted for exactly once."
  Collapsing per-item outcomes into a single `AccountError` after
  the side-effect boundary is forbidden.

## Audiences

1. Consumer UI, ratatoskr included, which needs stable typed outcomes
   that can be localized and converted into actions.
2. Sync engine, which needs deterministic recovery behavior.
3. Support and debugging, which need enough provider-native evidence to
   reconstruct the wire failure without string parsing.

The library serves typed data. User-facing strings remain the
consumer's job. The public error must still provide stable message keys,
remediation hints, recovery behavior, scope, operation context, and
diagnostics.

## Design goal

The public surface must not make consumers inspect a cause chain to
answer basic questions. A serious email client should be able to ask:

- What kind of failure is this?
- What operation failed?
- What account, folder, message, or cursor scope is affected?
- What stable message key should i18n and analytics bind to?
- Is this operation safe to retry, and on what schedule may the engine
  attempt it?
- Can the engine retry, restart a scope, restart an account, or stop?
- Does the user need to refresh, reauthorize, request admin consent, or
  contact support?
- What diagnostic identifiers should be logged?
- Which details are safe for UI, telemetry, or support only?

The cause chain is still valuable, but it is the forensic layer, not the
primary consumer contract.

## Public shape

Every `Account` method returns `Result<T, AccountError>`.

`AccountError` is opaque. All fields are private; construction is
funneled through `AccountErrorBuilder` in `bifrost-types` (see
[Chain composition](#chain-composition)). The builder computes the
derived fields (`recovery`, `remediation`, `message_key`) and validates
invariants. Consumers read state through accessors.

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

impl AccountError {
    pub fn kind(&self) -> &AccountErrorKind;
    pub fn recovery(&self) -> &RecoveryClass;
    pub fn suggested_remediation(&self) -> Option<&RemediationAction>;
    pub fn scope(&self) -> Option<&ErrorScope>;
    pub fn operation(&self) -> Option<AccountOperation>;
    pub fn provider(&self) -> Option<Provider>;
    pub fn protocol(&self) -> Option<Protocol>;

    pub fn message_key(&self) -> &'static str;

    pub fn user_safe_text(&self) -> impl Iterator<Item = &str>;
    pub fn telemetry_fields(&self) -> TelemetryView<'_>;
    pub fn support_minimal(&self) -> SupportExportMinimal<'_>;
    pub fn support_consented(&self) -> SupportExportConsented<'_>;
    pub fn support_internal(&self) -> SupportExportInternal<'_>;

    pub fn chain(&self) -> &CauseChain;

    /// Consume this error and return a builder pre-populated with its
    /// fields and chain. Used by call sites that need to decorate an
    /// existing error with additional evidence (e.g. fallback paths
    /// where the primary failure carries the operative diagnostics
    /// and the fallback attempt adds a secondary `Cause`).
    ///
    /// The returned builder is identical to one constructed via
    /// `AccountErrorBuilder::new(self.kind, primary_cause).` plus the
    /// accumulated chain, scope, operation, provider, protocol, and
    /// diagnostic fields. Callers can `push_cause`, adjust diagnostics,
    /// then `build()` to obtain a new `AccountError` with derived
    /// fields recomputed. `build()` remains the single invariant funnel;
    /// there is no mutator that bypasses re-derivation.
    ///
    /// `into_builder` is for *decoration*, not reclassification: the
    /// returned builder carries the original `kind` and primary
    /// `Cause`, and has no kind-changing path. `push_cause` only
    /// appends secondary evidence to the chain. If a caller needs a
    /// different primary `kind` or a different outermost `Cause`, it
    /// must construct a fresh builder via `AccountErrorBuilder::new`;
    /// going through `into_builder` first would either silently retain
    /// the wrong kind/cause pair or require an escape hatch that
    /// defeats the invariant funnel.
    pub fn into_builder(self) -> AccountErrorBuilder;
}
```

The canonical pattern for "preserve a primary error and decorate it
with secondary evidence" is `err.into_builder().push_cause(cause)
.build()`. Phase 2.2 reports cited a manual five-step
clone-walk-rebuild dance in the Gmail TRASH-fallback merge; that
dance is what `into_builder` exists to replace. There is no
`with_secondary_cause` mutator on `AccountError` itself, because such
a mutator would either bypass `build()` (losing the invariant funnel)
or duplicate it (drifting). `into_builder` keeps `build()` as the
single point where derived fields are computed.

`provider` and `protocol` are top-level because telemetry routing and
dashboard filters partition on them first. They are still reflected in
the support dump but are not buried inside `DiagnosticInfo`.

`Arc`-wrapping `DiagnosticInfo` and `CauseChain` keeps `Clone` cheap
when an error fans out to logs, UI state, retry queues, and analytics
sinks simultaneously.

`AccountError` implements `StdError`. `source()` returns a reference to
the outermost `Cause` (which itself implements `StdError`), so standard
ecosystem walkers (`anyhow`, `tracing`, `eyre`, custom log enrichers)
see the typed chain through the normal interface. `CauseChain` remains
the primary structured surface; `source()` is a courtesy for interop.

Free-form provider text is allowed only inside typed diagnostic
payloads with an explicit visibility policy, and is only reachable
through the filtered accessors above.

## Stable outcome

`AccountErrorKind` is the normalized consumer-facing classification.
It is the first field consumers should match on for localization,
analytics, and UX.

```rust
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccountErrorKind {
    Transport(TransportErrorKind),
    Authentication(AuthErrorKind),
    Authorization(AccessErrorKind),
    Server(ServerErrorKind),
    SyncState(SyncStateErrorKind),
    ConcurrencyConflict,
    Request(RequestErrorKind),
    NotFound(ResourceKind),
    Unsupported(AccountOperation),
    Protocol(ProtocolErrorKind),
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum RequestErrorKind {
    Malformed,
    BatchInputInvalid,
}
```

`RequestErrorKind` carries no payload: kinds are stable classification
for matching, message keys, and analytics. The specific items that
were invalid live in the cause chain, not in the kind. `BatchItemId`
lists and other unbounded data belong in `RequestCause` (see
[Cause variants](#cause-variants)):

```rust
#[non_exhaustive]
pub enum RequestCause {
    Malformed { detail: DiagnosticText },
    BatchInputInvalid { items: Vec<BatchInputInvalidItem> },
}

pub struct BatchInputInvalidItem {
    pub id: BatchItemId,
    pub reason: BatchInputInvalidReason,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchInputInvalidReason {
    Malformed,
    Duplicate,
    Empty,
}
```

Kind comparison stays cheap and stable; the chain carries the
forensic detail. Consumers route on `kind` and render details from
the cause.

Inner subkind enums (`TransportErrorKind`, `AuthErrorKind`,
`AccessErrorKind`, `ServerErrorKind`, `SyncStateErrorKind`,
`ProtocolErrorKind`, `ResourceKind`) are similarly `#[non_exhaustive]`
with the variants implied by the recovery table and the message-key
list below. Adding a variant is a non-breaking change; renaming or
removing one is breaking.

The kind is always set by the protocol translation boundary. Consumers
must not need to scan `chain` to discover the outcome.

`AccountError::message_key()` returns a stable `&'static str` derived
from `kind` (plus, where relevant, the inner subkind). The keys form a
flat namespace consumers can bind to translation files, analytics
events, and dashboards:

```
transport.network
transport.timeout
transport.tls
auth.expired
auth.refresh-transient
auth.revoked
auth.reauthorization-required
authz.admin-consent-required
authz.conditional-access-blocked
authz.policy-blocked
authz.insufficient-scope
authz.permission-denied
authz.account-disabled
authz.mailbox-unavailable
authz.mailbox-not-licensed
server.unavailable
server.rate-limited
server.quota-exhausted
server.error
syncstate.cursor-invalid
syncstate.strategy-failure
syncstate.scope-capability-lost
syncstate.schema-incompatible
syncstate.capability-changed
syncstate.operator-override-needed
concurrency.conflict
request.malformed
request.batch-input-invalid
notfound.message
notfound.mailbox
notfound.thread
notfound.calendar
notfound.contact
unsupported
protocol.parse-failed
protocol.missing-field
protocol.contract-violation
protocol.partial-response
protocol.unknown
```

Keys are append-only across releases. Renames count as a breaking
change in the public API.

The dotted namespace is the documented fallback convention. Consumers
ship translation catalogs out of cadence with library releases, so
new keys can land before a catalog has copy for them. Consumers walk
the dotted segments - `authz.mailbox-not-licensed` falls back to
`authz`, which falls back to a root key the consumer owns. The
library does not ship a fallback chain because the chain depends on
which keys the consumer has translated; the namespace shape is the
contract, the walk is the consumer's.

## Recovery

`RecoveryClass` is engine-facing advice, not a schedule and not a
verdict on what the consumer's UX must do. It is derived centrally
from `(kind, scope, operation, primary_cause)` in
`bifrost-types::recovery`, and that mapping is the only thing that
constructs a `RecoveryClass` value. Protocol-specific `to_recovery`,
`recovery_for_*`, and `fatal_for_*` helpers go away.

```rust
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryClass {
    Retry(RetryAdvice),
    Reconcile(ReconcileAdvice),
    Engine(EngineDirective),
    AuthLost,
    NeedsAdminConsent { needed: &'static str },
    NeedsPolicyChange,
    NoPermission { resource: Option<ResourceKind> },
    Unsupported(AccountOperation),
    ClientBug,
    ProviderContractViolation,
    ProviderRefused,
    UnknownPermanent,
}

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EngineDirective {
    /// Restart this one scope from scratch (UIDVALIDITY change,
    /// modseq reset, delta-token rejected).
    RestartScope(CursorScope),

    /// Restart the entire account (Gmail stale historyId beyond
    /// recovery, opaque global state loss).
    RestartAccount,

    /// Downgrade the cursor strategy and restart the scope
    /// (QRESYNC -> CONDSTORE, CONDSTORE -> Basic).
    DowngradeStrategy(StrategyDowngrade),

    /// Drop a capability for a specific scope and replay (Gmail
    /// folder reports modseq=0 on subsequent open).
    DowngradeCapabilityForScope(CursorScope),

    /// Cursor envelope older than the engine can migrate; clear
    /// cursor state and re-establish from inventory.
    SchemaIncompatible,

    /// Account capabilities changed mid-session. Reopen the account
    /// and restart streams.
    CapabilityChanged { delta: CapabilityDelta },

    /// Persistent strategy failure pattern; consumer should consider
    /// a runtime configuration override.
    OperatorOverrideRequired { reason: String },
}

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryAdvice {
    pub disposition: RetryDisposition,
    pub not_before: Option<SystemTime>,
    pub min_delay: Option<Duration>,
    pub reason: RetryReason,
    pub throttle_scope: Option<ThrottleScope>,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryDisposition {
    /// The same request is safe to send again as-is. Either the
    /// operation is genuinely idempotent (delete, update_flags, get)
    /// or the provider explicitly rejected before commit, so no
    /// side effect occurred.
    SameRequest,

    /// Rebuild the request from refreshed state before retrying
    /// (etag mismatch, server-side state moved).
    AfterStateRefresh,

    /// Retry only after the token source has refreshed credentials.
    AfterAuthRefresh,
}

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconcileAdvice {
    pub reason: ReconcileReason,
    pub guidance: ReconcileGuidance,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileReason {
    /// Transport dropped mid-stream after the request was transmitted
    /// but before any acknowledgement. The server may or may not have
    /// committed.
    TransportDropAfterSend,

    /// Provider returned a partial-completion signal (e.g. SMTP after
    /// some recipients accepted, truncated batch response).
    PartialCompletionSignal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconcileGuidance {
    pub actions: Vec<ReconcileAction>,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileAction {
    /// Probe the target resource (Sent folder, inventory, message
    /// list) to determine whether the operation took effect.
    CheckTarget,

    /// Dedupe by client-side identifier before retrying.
    DedupeByClientId,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryReason {
    Transport,
    ServerUnavailable,
    RateLimited,
    QuotaExhausted,
    ConcurrencyConflict,
    RefreshTransient,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThrottleScope {
    Request,
    Mailbox,
    Account,
    Tenant,
    Provider,
}

impl RecoveryClass {
    /// True for `Retry` only. Engine resends the same operation per
    /// `RetryAdvice`.
    pub fn is_retryable(&self) -> bool;

    /// True for `Reconcile` only. Caller probes state before
    /// deciding what to do next.
    pub fn requires_reconciliation(&self) -> bool;

    /// True for `Engine` only. Engine takes the directed action
    /// (restart, downgrade, schema clear, etc.).
    pub fn requires_engine_action(&self) -> bool;

    /// True for every other variant. No further automatic action.
    /// The four helpers are mutually exclusive and exhaustive.
    pub fn is_terminal(&self) -> bool;
}
```

`Retry` carries only dispositions that are genuinely safe to retry.
`RetryDisposition` does not include a "do not retry" variant; cases
that previously got `DoNotRetry` either reclassify to `Retry::SameRequest`
(provider explicitly rejected before commit; no side effect occurred)
or escalate to a terminal `RecoveryClass` variant. A `Retry`
`RecoveryClass` is always a green light for the engine's retry
machinery, modulo the rest of `RetryAdvice`.

`Reconcile` is the separate verdict for "we don't know what happened -
go check before deciding what to do next." It is not retry. A consumer
seeing `Reconcile(advice)` must perform the indicated reconciliation
(probe inventory, check Sent, dedupe by `BatchItemId` /
client-message-id) before either retrying or surfacing failure.

`Engine(EngineDirective)` carries instructions to the sync engine
that are neither retry nor reconcile: restart this scope, restart
the account, downgrade the strategy, drop a per-scope capability,
clear cursor state, react to a capability delta, escalate to
operator override. These are engine-control flow, not error recovery
per se, but they share a carrier because they arrive through the
same `Result` and `SyncEvent::Terminated` channels and the engine
needs a single dispatch surface for "what happens next." Future-
returning methods (`establish_initial_cursor`, `push_subscribe`)
emit them through the same `RecoveryClass` field they emit auth
losses through; nothing about engine directives requires a streaming
context.

The four helpers (`is_retryable`, `requires_reconciliation`,
`requires_engine_action`, `is_terminal`) are mutually exclusive
and exhaustive. Consumer code can branch on the high-level outcome
without enumerating every variant.

`Retry` is a hint, not a schedule. The library surfaces `min_delay`
as a floor and `not_before` as a provider-supplied wall-clock hint
(e.g. parsed `Retry-After`). Consumers and the engine own everything
else: jitter, caps, network-condition awareness, per-account
throttling, outbox priority, tenant-wide backoff during auth storms.

The central mapping derives `RecoveryClass` from `(operation, reason,
transmission_state, provider_signal)`. The dispositions in `Retry`
cover only safe-retry cases:

- Idempotent operation + transient failure (any transmission state) →
  `Retry::SameRequest`.
- Non-idempotent operation + acknowledged failure before commit
  (`TransmissionState::Acknowledged`, structured 4xx, structured
  method error) → `Retry::SameRequest`.
- Non-idempotent operation + `TransmissionState::InFlight` (transport
  dropped without acknowledgement) → `Reconcile { TransportDropAfterSend, actions: [CheckTarget] }`.
- Provider returned partial-completion signal on non-idempotent op →
  `Reconcile { PartialCompletionSignal, actions: [CheckTarget, DedupeByClientId] }`.
- Etag/state mismatch → `Retry::AfterStateRefresh`.
- `Authentication(RefreshTransient)` → `Retry::AfterAuthRefresh`.
- Client-side request error → `ClientBug` (terminal; no retry
  envelope is constructed).

`throttle_scope` is populated only for `RateLimited` and
`QuotaExhausted` reasons. The library encodes documented provider
behavior (Graph tenant-wide throttles, Gmail per-user quotas) rather
than guessing. Consumers and engines use it to decide whether to pause
the single request, the mailbox, the account, the tenant, or the
entire provider fleet.

The terminal variants - `AuthLost`, `NeedsAdminConsent`,
`NeedsPolicyChange`, `NoPermission`, `Unsupported`, `ClientBug`,
`ProviderContractViolation`, `ProviderRefused`, `UnknownPermanent` -
are all "stop trying" from the engine's perspective but carry
distinct product meaning. Consumers route them to different surfaces:

- `AuthLost` and `NeedsAdminConsent` → reauthorization or IT-contact
  flows.
- `NeedsPolicyChange` → tenant administrator surface, not the user.
- `NoPermission` → per-user feature toggle or capability disclosure.
- `Unsupported` → roadmap or fallback path; never a "try again" UX.
- `ClientBug` → internal telemetry, not user-visible.
- `ProviderContractViolation` → provider broke its own protocol
  (parse failed, missing required field). Internal telemetry plus
  support escalation; consumers do not surface as user-fixable.
- `ProviderRefused` → deliberate permanent provider response (account
  disabled, region-locked, 451 unavailable-for-legal-reasons). Support
  escalation with provider-facing copy.
- `UnknownPermanent` → classification gap. Treated as a library bug;
  surfaces in telemetry so the gap can be closed in a later release.

`RecoveryClass::is_terminal()` returns true for the terminal
variants only - `AuthLost`, `NeedsAdminConsent`, `NeedsPolicyChange`,
`NoPermission`, `Unsupported`, `ClientBug`, `ProviderContractViolation`,
`ProviderRefused`, `UnknownPermanent`. `Retry`, `Reconcile`, and
`Engine` are not terminal because each implies a defined next step
(retry, reconcile, engine action). `Fatal` collapses any terminal
disposition at the engine boundary:

```rust
pub struct Fatal(pub AccountError);

impl TryFrom<AccountError> for Fatal {
    type Error = AccountError;
    fn try_from(err: AccountError) -> Result<Self, Self::Error>;
}
```

Base mapping. `op.idem` abbreviates `AccountOperation::is_idempotent`.
`tx_state` is `AttemptCause::transmission_state` (`Unsent`, `InFlight`,
`Acknowledged`); the row applies when an `Attempt` cause with that
state is present. Absence of `Attempt` is treated as `Unsent` for
classification purposes.

| Kind or cause | RecoveryClass |
|---|---|
| `Transport(_)`, `tx_state: Unsent` or absent | `Retry { disposition: SameRequest, reason: Transport, .. }` |
| `Transport(_)`, `tx_state: InFlight`, `op.idem` | `Retry { disposition: SameRequest, reason: Transport, .. }` |
| `Transport(_)`, `tx_state: InFlight`, `!op.idem` | `Reconcile { TransportDropAfterSend, actions: [CheckTarget] }` |
| `Transport(_)`, `tx_state: Acknowledged` | impossible by construction; mapping rejects |
| `Authentication(Expired)` | `AuthLost` |
| `Authentication(RefreshTransient)` | `Retry { disposition: AfterAuthRefresh, reason: RefreshTransient, .. }` |
| `Authentication(Revoked \| ReauthorizationRequired)` | `AuthLost` |
| `Authorization(AdminConsentRequired)` | `NeedsAdminConsent { needed }` |
| `Authorization(ConditionalAccessBlocked \| PolicyBlocked \| InsufficientScope \| MailboxNotLicensed)` | `NeedsPolicyChange` |
| `Authorization(PermissionDenied)` | `NoPermission { resource }` |
| `Server(Unavailable)`, `tx_state: Acknowledged` | `Retry { disposition: SameRequest, reason: ServerUnavailable, not_before: retry_after, .. }` |
| `Server(Unavailable)`, `tx_state: InFlight`, `op.idem` | `Retry { disposition: SameRequest, reason: ServerUnavailable, .. }` |
| `Server(Unavailable)`, `tx_state: InFlight`, `!op.idem` | `Reconcile { TransportDropAfterSend, actions: [CheckTarget] }` |
| `Server(RateLimited)`, `tx_state: Acknowledged` | `Retry { disposition: SameRequest, reason: RateLimited, not_before: retry_after, throttle_scope, .. }` |
| `Server(RateLimited)`, `tx_state: InFlight`, `op.idem` | `Retry { disposition: SameRequest, reason: RateLimited, throttle_scope, .. }` |
| `Server(RateLimited)`, `tx_state: InFlight`, `!op.idem` | `Reconcile { TransportDropAfterSend, actions: [CheckTarget] }` |
| `Server(QuotaExhausted)`, `tx_state: Acknowledged` | `Retry { disposition: SameRequest, reason: QuotaExhausted, not_before: retry_after, throttle_scope, .. }` |
| `Server(QuotaExhausted)`, `tx_state: InFlight`, `op.idem` | `Retry { disposition: SameRequest, reason: QuotaExhausted, throttle_scope, .. }` |
| `Server(QuotaExhausted)`, `tx_state: InFlight`, `!op.idem` | `Reconcile { TransportDropAfterSend, actions: [CheckTarget] }` |
| `Server(Error { status: Some(5xx) })`, `tx_state: Acknowledged` | `Retry { disposition: SameRequest, reason: ServerUnavailable, .. }` |
| `Server(Error { status: Some(5xx) })`, `tx_state: InFlight`, `op.idem` | `Retry { disposition: SameRequest, reason: ServerUnavailable, .. }` |
| `Server(Error { status: Some(5xx) })`, `tx_state: InFlight`, `!op.idem` | `Reconcile { TransportDropAfterSend, actions: [CheckTarget] }` |
| `Server(Error { status: Some(4xx) })` (unclassified) | `ProviderRefused` |
| `Server(Error { status: Some(other permanent) })` | `ProviderRefused` |
| `Server(Error { status: None })`, `tx_state: Acknowledged` | `ProviderRefused` (server actively responded with an error the protocol carries without a numeric status; e.g. IMAP `NO`/`BAD` without a response code). Protocol crates that have a more specific interpretation must push a more specific `Cause` so the classifier picks it up before this row. |
| `Server(Error { status: None })`, `tx_state: InFlight`, `op.idem` | `Retry { disposition: SameRequest, reason: ServerUnavailable, .. }` (terminal response never arrived; idempotent retry is safe). |
| `Server(Error { status: None })`, `tx_state: InFlight`, `!op.idem` | `Reconcile { TransportDropAfterSend, actions: [CheckTarget] }` (non-idempotent op with no terminal response; reconcile before retry). |

`Server(Error { status: None })` represents an **active server failure
without a numeric status** - the server responded with an error the
protocol does not carry a number for (IMAP `NO`/`BAD` outside any
tagged response code). It is **not** the shape for "no terminal
reply arrived" cases like a TCP drop after SMTP DATA. Drops before
any terminal reply pair `Transport(_)` with `Attempt(InFlight)` and
omit `Server(_)` entirely; the `InFlight` rows above exist for cases
where a server actively responded with an error but the protocol
crate has reason to model the wire state as in-flight (a malformed
or partial structured response, etc.) - they are not an invitation
to fabricate a `Server(Error)` cause for missing replies.
| `SyncState(CursorInvalid)` with `scope` | `Engine(RestartScope(scope))` |
| `SyncState(CursorInvalid)` without `scope` | `Engine(RestartAccount)` |
| `SyncState(StrategyFailure)` | `Engine(DowngradeStrategy(downgrade))` |
| `SyncState(ScopeCapabilityLost)` | `Engine(DowngradeCapabilityForScope(scope))` |
| `SyncState(SchemaIncompatible)` | `Engine(SchemaIncompatible)` |
| `SyncState(CapabilityDelta { delta })` | `Engine(CapabilityChanged { delta })` |
| `SyncState(OperatorOverrideNeeded { reason })` | `Engine(OperatorOverrideRequired { reason })` |
| `ConcurrencyConflict` | `Retry { disposition: AfterStateRefresh, min_delay: None, reason: ConcurrencyConflict, .. }` |
| `Request(Malformed)` | `ClientBug` |
| `Request(BatchInputInvalid)` | `ClientBug` |
| `NotFound(_)` | `ProviderRefused` unless absorbed by the operation (see [NotFound semantics](#notfound-semantics)) |
| `Unsupported(op)` | `Unsupported(op)` |
| `Access(AccountDisabled)` | `ProviderRefused` |
| `Access(MailboxUnavailable { kind: Transient })` | `Retry { disposition: SameRequest, reason: ServerUnavailable, .. }` |
| `Access(MailboxUnavailable { kind: Permanent })` | `ProviderRefused` |
| `Protocol(ParseFailed \| MissingField \| ContractViolation)` | `ProviderContractViolation` |
| `Protocol(PartialResponse)`, `!op.idem` | `Reconcile { PartialCompletionSignal, actions: [CheckTarget, DedupeByClientId] }` |
| `Protocol(PartialResponse)`, `op.idem` | `Retry { disposition: SameRequest, reason: Transport, .. }` |
| `Protocol(_)` (other) | `UnknownPermanent` unless paired with a higher-level retryable cause |

Specific 4xx codes route to their proper kinds before reaching the
"unclassified 4xx" row: 401/403 → `Authentication`/`Authorization`,
404 → `NotFound`, 409 → `ConcurrencyConflict`, 410 → `CursorInvalid`
when applicable, 429 → `RateLimited`. The `4xx → ProviderRefused`
fallback applies only when the protocol crate has not classified the
specific code; it is not a default for "any 4xx." `ClientBug` is
reserved for cases where the library or caller demonstrably built an
invalid request (`Request(Malformed)`, `Request(BatchInputInvalid)`,
schema violations the library should have caught) - wire 4xx alone
is not evidence of that.

The central mapping consults the outermost matching cause for
`AttemptCause::transmission_state`, provider-supplied `retry_after`,
throttle scope, and resource identity. The builder stores the
resulting `RecoveryClass` on `AccountError` before it leaves the
protocol crate.

`AttemptCause` is pushed by the transport layer (typically
`bifrost-net`) whenever a network attempt is made, with
`transmission_state` set according to the wire-level evidence:
`Unsent` if no bytes left the socket, `InFlight` if bytes were
transmitted but no terminal acknowledgement arrived, `Acknowledged`
if the server returned a terminal response (even an error response).
For purely local errors there is no `Attempt` cause at all; absence
is the explicit signal that no network attempt was made and the
classification reduces to "as if `Unsent`." The mapping reads
`AttemptCause` off the chain; there is no separate builder setter.

## Remediation

Recovery is engine-facing advice. Remediation is consumer-facing
product guidance - the action a user, admin, or support team must
take. Both are derived from the same `(kind, scope, operation, cause)`
inputs and by the same builder; the split is conceptual, not
structural. Consumers read it through
`AccountError::suggested_remediation()`.

```rust
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemediationAction {
    RefreshToken,
    Reauthorize,
    RequestAdminConsent { needed: &'static str },
    UpdateTenantPolicy,
    CheckMailboxLicense,
    RetryLater { not_before: Option<SystemTime> },
    RestartAccount,
    RestartScope(CursorScope),
    FixClientRequest,
    ContactProviderSupport,
}
```

`suggested_remediation()` returns `None` only when the engine fully
handles the error and no product action is required (most retryable
transport failures, most cursor restarts). When present, the variant
is stable enough for consumers to bind to localized copy and product
actions; `message_key()` provides the matching i18n key.

## Scope and operation

The translation boundary must attach operation and scope whenever the
context is known.

```rust
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ErrorScope {
    Account,
    Cursor(CursorScope),
    Mailbox { id: String },
    Message { id: String },
    Thread { id: String },
    Calendar { id: String },
    CalendarCollection,
    Contact { id: String },
    ContactCollection,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountOperation {
    // Discovery and cursor establishment
    Discover,
    EstablishCursor,
    DiscoverCursorScopes,
    DiscoverMemberships,
    ScopeLifecycle,

    // Sync streams
    SyncInventory,
    SyncChanges,

    // Hydration
    Hydrate,
    HydrateThread,
    HydrateMessage,
    OpenBlob,
    OpenBlobRange,

    // Push
    PushSubscribe,
    PushUnsubscribe,
    PushStream,

    // Mutation primitives
    UpdateFlags,
    BulkMove,
    BulkDestroy,
    AddToContainer,
    RemoveFromContainer,
    SetKeyword,
    SetLabelMembership,
    SetCategory,
    SetExtendedProperty,
    SetIsRead,

    // Composition
    Send,
    AttachmentUpload,
    DraftCreate,
    DraftUpdate,
    DraftDiscard,
    DraftSend,

    // Search
    Search,
    SearchMessages,

    // Containers
    ContainersList,
    ContainerCreate,
    ContainerRename,
    ContainerMove,
    ContainerDelete,

    // Settings
    IdentitiesList,
    IdentityUpdate,
    VacationGet,
    VacationSet,
    QuotaGet,

    // Lifecycle
    Close,
    Expunge,
}

impl AccountOperation {
    pub fn is_idempotent(self) -> bool;
}
```

Scope variants no longer carry `Option<String>` identifiers. If a
specific mailbox, calendar, or contact identifier is not known, the
scope is broader (`Account`, `CalendarCollection`, `ContactCollection`).
A `Mailbox` scope without an id is not expressible; consumers never
have to handle `Some/None` in match arms.

Operations are split finely enough that idempotency is a property
of the variant, not a hedged hint. Idempotent: `UpdateFlags`,
`BulkDestroy`, `SetKeyword`, `SetLabelMembership`, `SetCategory`,
`SetExtendedProperty`, `SetIsRead`, `Expunge`, every read-only
operation (`Hydrate`, `Search`, `ContainersList`, etc.), and the
discard/getter family (`DraftDiscard`, `VacationGet`, `QuotaGet`).
Not idempotent: `Send`, `BulkMove`, `AddToContainer`,
`RemoveFromContainer`, `AttachmentUpload`, `DraftCreate`,
`DraftUpdate`, `DraftSend`, `ContainerCreate`, `ContainerRename`,
`ContainerMove`, `ContainerDelete`, `IdentityUpdate`, `VacationSet`.
`is_idempotent()` does not lie; the central recovery mapping
relies on this distinction to pick the right `RetryDisposition`.

Support logs and UI copy should not have to infer the operation from
stack location. The exact list can be extended as the `Account` trait
gains methods, but the granularity floor is per-mutation-shape - never
a generic `Mutate` again.

## Diagnostics

Diagnostics are provider and protocol evidence. They are for support,
logging, and correlation, not localization. Consumers reach them only
through the filtered accessors on `AccountError`; `DiagnosticInfo` is
not part of the consumer-visible struct shape and the visibility tag
is not the consumer's enforcement point.

```rust
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct DiagnosticInfo {
    pub request_id: Option<String>,
    pub trace_id: Option<String>,
    pub status: Option<u16>,
    pub native_code: Option<String>,
    pub text: Vec<DiagnosticText>,
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct DiagnosticText {
    pub value: String,
    pub visibility: DetailVisibility,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DetailVisibility {
    UserSafe,
    SupportOnly,
}
```

`Provider` and `Protocol` are top-level fields on `AccountError`, not
on `DiagnosticInfo`. The support dump bundles them with the rest of
the evidence; telemetry and routing filters read them directly off
the error.

`DetailVisibility` is a producer-side label that drives the filtered
accessors. The consumer-facing surface is:

```rust
#[derive(Clone, Debug, serde::Serialize)]
pub struct TelemetryView<'a> {
    pub kind_discriminant: &'static str,
    pub message_key: &'static str,
    pub recovery_discriminant: &'static str,
    pub provider: Option<Provider>,
    pub protocol: Option<Protocol>,
    pub status: Option<u16>,
    pub native_code: Option<&'a str>,
    pub request_id: Option<&'a str>,
    pub trace_id: Option<&'a str>,
    pub retry_disposition: Option<RetryDisposition>,
    pub retry_reason: Option<RetryReason>,
    pub throttle_scope: Option<ThrottleScope>,
    pub reconcile_reason: Option<ReconcileReason>,
    pub reconcile_actions: &'a [ReconcileAction],
    pub transmission_state: Option<TransmissionState>,
    pub operation: Option<AccountOperation>,
}

/// Minimal support export is structurally identical to telemetry.
/// The alias exists so support-tooling call sites read with intent.
pub type SupportExportMinimal<'a> = TelemetryView<'a>;

#[derive(Clone, Debug, serde::Serialize)]
pub struct SupportExportConsented<'a> {
    pub telemetry: TelemetryView<'a>,
    pub user_safe_text: Vec<&'a str>,
    pub support_text: Vec<&'a str>,
    pub scope: Option<&'a ErrorScope>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct SupportExportInternal<'a> {
    pub consented: SupportExportConsented<'a>,
    pub chain: Vec<CauseSummary<'a>>,
}
```

- `AccountError::user_safe_text()` yields only items tagged
  `UserSafe`. This is the only public surface that yields strings
  intended for human display.
- `AccountError::telemetry_fields()` yields a `TelemetryView` of
  structured fields only - no free-form text. This is the analytics
  payload. Dashboards group on stable enum discriminants and message
  keys; high-cardinality free-form text never appears here.
- `AccountError::support_minimal()`, `support_consented()`, and
  `support_internal()` are three serializable exports for support
  tooling. Minimal mirrors telemetry. Consented adds user-safe and
  support-only text (used in support bundles with explicit user or
  admin consent). Internal adds the full cause chain (debugging by
  authorized staff). Each is `serde::Serialize`, so support pipelines
  ingest JSON, not formatted strings.

Raw `DiagnosticInfo` is not part of the public surface. Free-form
text reaches a consumer only through `user_safe_text()` (for UI),
`support_consented()`, or `support_internal()`. Telemetry is
structured by construction; free-form text cannot cardinality-bomb
a dashboard because there is no path from `DiagnosticText` into
`TelemetryView`. The two-tier `DetailVisibility` (`UserSafe`,
`SupportOnly`) reflects this: every piece of free-form text is
either safe to show a user or routed only to support exports.

The producer-side contract: protocol crates must tag every
`DiagnosticText` accurately. `SupportOnly` is the default when in
doubt. There is no `Sensitive` tier - text that is genuinely
sensitive (credentials, full request bodies with PII) must be
redacted at the producer; the redacted placeholder is then tagged
`SupportOnly`.

Provider text, IMAP response text, TLS messages, and parse snippets
must use `DiagnosticText`; they must not appear as naked `String`
fields on consumer-facing classifications.

Provider request IDs and trace IDs should be captured whenever the
wire protocol exposes them. In enterprise support flows, these IDs
are often more useful than the provider message.

## Cause chain

The chain preserves the layered interpretation. It is ordered from
outermost interpretation to root evidence.

```rust
#[derive(Clone, Debug)]
pub struct CauseChain {
    causes: Vec<Cause>,
}

impl CauseChain {
    pub fn outermost(&self) -> &Cause;
    pub fn root(&self) -> &Cause;
    pub fn iter(&self) -> impl Iterator<Item = &Cause>;
}
```

The chain must be non-empty. Consumers should use `outermost()` and
`root()` rather than indexing.

```rust
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum Cause {
    Transport(TransportCause),
    Attempt(AttemptCause),
    Auth(AuthCause),
    Access(AccessCause),
    Server(ServerCause),
    State(StateCause),
    Request(RequestCause),
    Wire(WireCause),
}
```

`Cause::Attempt` carries the transmission-state evidence (see
[Cause variants](#cause-variants)). It is pushed onto the chain
whenever the protocol crate has wire-level evidence of a network
attempt - alongside `Transport(_)` for connection failures, alongside
`Server(_)` for terminal responses (success or error), and on its own
when the consumer-visible failure came from elsewhere but the network
attempt is part of the forensic record. Purely local errors
(`Request(BatchInputInvalid)`, schema validation failures, builder
preflight checks) have no `Attempt` cause at all - absence means "no
network attempt was made," distinct from `Attempt { Unsent }` which
means "an attempt was initiated but nothing crossed the boundary."
The recovery mapping treats absence and `Unsent` identically for
classification, but support exports keep the distinction visible.

`Cause` and every variant implement `StdError`. `AccountError::source()`
returns the outermost `Cause`. Each variant's own `source()` returns
`None`: cause types do not store boxed `dyn StdError` embedded
sources, because doing so would require Clone-via-Arc on every
variant and serialization carve-outs on the support exports. The
text content the underlying system error would have produced is
captured as `DiagnosticText` on `TransportCause::message` (and
equivalents); support exports surface it through
`support_internal()`. Standard walkers (`anyhow`, `tracing`'s
error layers, `eyre`) see exactly one level of typed cause via
`AccountError::source()`; the full typed chain is reached only
through `AccountError::chain().iter()`. This is a deliberate
limit: the Vec-backed `CauseChain` is not a linked list and we
do not pretend otherwise.

`WireCause` carries provider-native provenance so support code can
diagnose without string parsing. It should normally be paired with a
higher-level `Cause` and a normalized `AccountErrorKind`.

`WireCause` is diagnostic-only. Classification logic in the library
must produce a sufficiently specific `AccountErrorKind` for every
wire signal the library knows how to interpret. If a consumer ever
needs to match on `WireCause` variants to make a product decision -
routing, retry policy, UX copy - that is a library bug, not a
consumer responsibility. The fix is a tighter `AccountErrorKind`
classification in the next release, not a stable contract on
`WireCause`. Provider-native matching in consumer code is legitimate
only for diagnostic surfaces: telemetry filters, support dashboards,
escalation triage. Code that branches on `WireCause` for product
behavior is on borrowed time.

## Cause variants

```rust
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct TransportCause {
    pub kind: TransportKind,
    pub message: Option<DiagnosticText>,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportKind {
    Network,
    Timeout,
    Tls,
}

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptCause {
    pub transmission_state: TransmissionState,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransmissionState {
    /// An attempt was initiated but no bytes crossed the side-effect
    /// boundary (DNS failed, TLS handshake aborted locally, etc.).
    Unsent,
    /// Bytes crossed the boundary but no terminal response arrived.
    InFlight,
    /// Server returned a terminal response (success or error).
    Acknowledged,
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum AuthCause {
    Expired,
    RefreshTransient,
    Revoked,
    ReauthorizationRequired,
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum AccessCause {
    InsufficientScope { needed: &'static str },
    AdminConsentRequired { needed: &'static str },
    ConditionalAccessBlocked,
    PolicyBlocked,
    AccountDisabled,
    MailboxUnavailable { kind: MailboxUnavailableKind },
    MailboxNotLicensed,
    PermissionDenied { resource: Option<ResourceKind> },
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MailboxUnavailableKind {
    Transient,
    Permanent,
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum ServerCause {
    Unavailable { retry_after: Option<Duration> },
    RateLimited { retry_after: Option<Duration> },
    QuotaExhausted { retry_after: Option<Duration> },
    Error { status: Option<u16> },
}

/// `Error::status` is `Some(_)` for HTTP-like providers (JMAP, Graph,
/// Gmail) where every server error carries a numeric status. It is
/// `None` for protocol server failures that have no numeric status
/// (e.g. IMAP `NO`/`BAD` outside any tagged response, SMTP transport
/// shutdown without a final reply). The classifier handles both via
/// the explicit `status: None` recovery row above; protocol crates
/// must not synthesize sentinel values like `0` to satisfy the type.

#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum StateCause {
    CursorInvalid,
    StrategyFailure { downgrade: StrategyDowngrade },
    ScopeCapabilityLost,
    SchemaIncompatible,
    CapabilityChanged { delta: CapabilityDelta },
    OperatorOverrideNeeded { reason: String },
    ConcurrencyConflict,
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum RequestCause {
    Malformed { detail: DiagnosticText },
    BatchInputInvalid { items: Vec<BatchInputInvalidItem> },
    Unsupported { operation: AccountOperation },
    InvalidArgument { field: Option<&'static str>, message: Option<DiagnosticText> },
    NotFound { what: ResourceKind, id: Option<String> },
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum WireCause {
    Graph(GraphSignal),
    Jmap(JmapMethod),
    Imap(ImapResponseCode),
    Smtp(EnhancedStatusCode),
    Gmail(GmailSignal),
    MalformedResponse { protocol: Protocol, detail: Option<DiagnosticText> },
}
```

`StateCause` covers every `SyncStateErrorKind` so the central recovery
mapping can read the engine directive payload (strategy downgrade,
operator reason, capability delta) directly off the chain. `RequestCause`
folds the local-validation variants (`Malformed`, `BatchInputInvalid`)
into the same enum as the request-shape variants (`Unsupported`,
`InvalidArgument`, `NotFound`).

`WireCause` uses tuple variants over the provider-native enums rather
than named struct fields; SMTP wire text lives inside `EnhancedStatusCode`
itself, and IMAP wire text lives inside the `ImapResponseCode::Unknown`
variant. Both as `DiagnosticText`, so the visibility discipline survives.

Provider-native enums must include typed unknown variants instead of a
catch-all public `Other`:

```rust
pub enum GraphSignal {
    Gone,
    InvalidAuthenticationToken,
    AccessDenied,
    Forbidden,
    AccessRestricted,
    ConditionalAccessBlocked,
    AdminConsentRequired,
    MailboxNotEnabledForRestApi,
    MailboxStoreUnavailable,
    ResyncRequired,
    TooManyRequests,
    GenericFileError,
    PreconditionFailed,
    NotFound,
    Unknown { code: String },
}
```

This preserves forward compatibility without making consumers parse
strings.

## Chain composition

Each protocol crate exposes one `pub(crate) fn into_account_error`.
That function is the translation boundary from crate-internal errors
to the public account surface. It must funnel construction through
`bifrost_types::AccountErrorBuilder` - there is no other public path
to an `AccountError` value.

```rust
pub struct AccountErrorBuilder { /* opaque */ }

impl AccountErrorBuilder {
    pub fn new(kind: AccountErrorKind, primary_cause: Cause) -> Self;

    pub fn operation(self, op: AccountOperation) -> Self;
    pub fn scope(self, scope: ErrorScope) -> Self;
    pub fn provider(self, provider: Provider) -> Self;
    pub fn protocol(self, protocol: Protocol) -> Self;

    pub fn push_cause(self, cause: Cause) -> Self;

    pub fn request_id(self, id: impl Into<String>) -> Self;
    pub fn trace_id(self, id: impl Into<String>) -> Self;
    /// Server-returned numeric status. `None` is permitted and
    /// represents "server responded with an error this protocol does
    /// not carry a numeric status for" (IMAP `NO`/`BAD` without a
    /// response code, etc.). Builders must not synthesize sentinel
    /// values like `0`; pass `None` instead.
    pub fn status(self, status: Option<u16>) -> Self;
    pub fn native_code(self, code: impl Into<String>) -> Self;
    pub fn text(self, text: DiagnosticText) -> Self;

    /// Override the operation's default idempotency. Used by callers
    /// that pass an `If-Match` etag or otherwise know the call is
    /// safe to retry despite the operation's default. With finer
    /// `AccountOperation` granularity this override is rarely needed.
    pub fn idempotency_override(self, idempotent: bool) -> Self;

    /// Provider-supplied retry deadline (e.g. parsed `Retry-After`).
    pub fn retry_not_before(self, when: SystemTime) -> Self;

    /// Provider-documented throttle domain for rate-limit/quota failures.
    pub fn throttle_scope(self, scope: ThrottleScope) -> Self;

    pub fn build(self) -> AccountError;
}
```

`build()`:

1. Validates the chain is non-empty and the outermost `Cause` agrees
   with `kind`.
2. Derives `recovery` from `bifrost-types::recovery` using
   `(kind, scope, operation, primary_cause, retry_not_before,
   throttle_scope, idempotency_override)`. `transmission_state` is
   read off the outermost `AttemptCause` in the chain when present;
   absence of `AttemptCause` is treated as `Unsent` for classification
   but is preserved verbatim in support exports. `TransportCause`
   itself never carries transmission state; the two causes are pushed
   side-by-side when a transport failure occurs (`Transport(_)` for
   the failure type, `Attempt(_)` for the transmission-state evidence).
3. Derives `suggested_remediation` from the same inputs.
4. Computes `message_key`.
5. Wraps `DiagnosticInfo` and `CauseChain` in `Arc`.

`build()` is the only constructor. `AccountError` has no `pub` fields
and no other public constructor, so drift between `kind`, `recovery`,
`remediation`, `message_key`, and `chain` is structurally impossible.

Examples:

Graph 410 on a delta token:

- `kind`: `SyncState(CursorInvalid)`
- `scope`: `ErrorScope::Cursor(scope)`
- derived `recovery`: `Engine(RestartScope(scope))`
- derived `message_key`: `"syncstate.cursor-invalid"`
- `chain[0]`: `State(CursorInvalid)`
- `chain[1]`: `Wire(Graph(GraphSignal::Gone))`
- `chain[2]`: `Server(Error { status: Some(410) })`

JMAP `stateMismatch` during a flag update:

- `kind`: `ConcurrencyConflict`
- `operation`: `AccountOperation::UpdateFlags`
- derived `recovery`: `Retry { disposition: AfterStateRefresh,
  min_delay: None, reason: ConcurrencyConflict, .. }`
- derived `message_key`: `"concurrency.conflict"`
- `chain[0]`: `State(ConcurrencyConflict)`
- `chain[1]`: `Wire(Jmap(JmapMethod::StateMismatch))`

Network drop mid-stream during `Send`:

- `kind`: `Transport(Network)`
- `operation`: `AccountOperation::Send`
- `chain[0]`: `Transport { kind: Network, .. }`,
  `chain[1]`: `Attempt { transmission_state: InFlight }`
- derived `recovery`: `Reconcile(ReconcileAdvice { reason:
  TransportDropAfterSend, guidance: ReconcileGuidance { actions:
  vec![CheckTarget] } })` (send is non-idempotent
  and bytes left the socket without acknowledgement; the consumer
  must probe the target before deciding whether to retry)
- derived `message_key`: `"transport.network"`
- derived `suggested_remediation`: `None` (consumer reconciles; no
  canonical product action)

Graph 429 on a `MoveMessage` after the server acknowledged the request:

- `kind`: `Server(RateLimited)`
- `operation`: `AccountOperation::MoveMessage`
- `chain[0]`: `Server(RateLimited)`,
  `chain[1]`: `Attempt { transmission_state: Acknowledged }`
- builder sets `retry_not_before` from `Retry-After`,
  `throttle_scope: ThrottleScope::Tenant`
- derived `recovery`: `Retry { disposition: SameRequest, reason: RateLimited,
  not_before: <header>, throttle_scope: Some(Tenant), .. }` (the
  server rejected before commit, so the request is safe to resend
  after the deadline; the engine pauses tenant-wide based on
  `throttle_scope`)
- derived `message_key`: `"server.rate-limited"`

Admin consent failure:

- `kind`: `Authorization(AdminConsentRequired)`
- derived `recovery`: `NeedsAdminConsent { needed }`
- derived `suggested_remediation`: `RequestAdminConsent { needed }`
- derived `message_key`: `"authz.admin-consent-required"`
- `chain[0]`: `Access(AdminConsentRequired { needed })`
- `chain[1]`: provider-native wire cause when available

## HTTP transport boundary

`bifrost-net` should expose a context-aware conversion, not a
context-free one.

```rust
pub struct NetErrorContext {
    pub provider: Option<Provider>,
    pub protocol: Protocol,
    pub operation: Option<AccountOperation>,
    pub scope: Option<ErrorScope>,
}

pub fn into_account_error(error: net::Error, ctx: NetErrorContext) -> AccountError;
```

JMAP, Gmail, and Graph call this when only transport-level signal is
available. When a protocol crate has a more precise interpretation
(e.g. a parsed JMAP method error, a Graph `error.code` token), it
constructs its own `AccountError` via the builder rather than
post-mutating the net result. IMAP and SMTP do not use this boundary.

## Construction invariants

Enforced by `AccountErrorBuilder::build`, not by code review:

- `chain` is non-empty.
- `kind` must agree with the outermost semantic cause.
- `recovery` is derived centrally; protocol crates cannot pass one in.
- `suggested_remediation` is derived centrally.
- `message_key` is derived from `kind`.
- `WireCause` alone is valid only for `Protocol(_)` failures or for
  genuinely unclassified provider responses. Otherwise it must be
  paired with a higher-level cause.
- `CursorInvalid` should carry `ErrorScope::Cursor`; missing cursor
  scope falls back to `Engine(RestartAccount)`.
- Free-form text must use `DiagnosticText`. Sensitive material is
  redacted at the producer; the redacted placeholder is tagged
  `SupportOnly`.
- Raw provider text must never be assumed user-safe.

## Token rotation

Token rotation is out of the error model. Factories accept
`Arc<dyn TokenSource>` at construction; rotation happens through the
source the consumer owns. Per-factory `set_access_token` methods go
away.

Token source failures still map into `AccountError`:

- transient refresh failure maps to `AuthCause::RefreshTransient`,
  `Authentication(RefreshTransient)`, and retry recovery.
- expired token maps to `AuthCause::Expired` and `AuthLost`.
- revoked token or permanent refresh rejection maps to
  `AuthCause::Revoked` or `AuthCause::ReauthorizationRequired` and
  `AuthLost`.
- admin or tenant policy failures map to `AccessCause`, not generic auth
  loss.

## Streaming results

Bulk operations (`bulk_set_flags`, `bulk_move`, `bulk_destroy`,
inventory streams, change streams) take streaming input and emit
streaming output. The streaming primitive is load-bearing: it is how
the engine backpressures arbitrarily-large mutation passes without
buffering the entire result in memory. Vec-based `BatchOutcome` and
its Vec-batch invariants apply to genuinely-blocking batch
operations (multi-recipient SMTP send, JMAP `Email/set` batch that
resolves in one wire roundtrip); they do not apply to streaming
operations.

Streaming bulk operations return per-item outcomes wrapped in
`SyncEvent` envelopes:

```rust
pub enum ItemOutcome<T> {
    Succeeded(BatchSuccess<T>),
    Failed(BatchFailure),
    Uncertain(BatchUncertain),
}
```

Bulk methods return `AccountStream<SyncEvent<ItemOutcome<T>>>`, or
the existing `SyncEvent::Batch<ItemOutcome<T>>` envelope where the
protocol crate groups outcomes by wire-level chunk. `BatchSuccess`,
`BatchFailure`, and `BatchUncertain` are the same types Vec-batch
uses; each carries `BatchItemId`. There is no fourth lane.

`MutationSuccess` is the success payload for streaming bulk mutation:

```rust
#[non_exhaustive]
pub enum MutationSuccess {
    /// Server applied the mutation.
    Applied,
    /// Read-back guard determined the target was already in the
    /// requested state; no write was attempted.
    Skipped,
}
```

`Skipped` is a successful final state from the caller's perspective.
Folding it into `Succeeded` rather than a fourth lane keeps the
lane model uniform - the lanes describe how the item left the
side-effect boundary, not whether the server wrote bytes.

### Streaming invariants

Streaming bulk operations uphold four invariants:

1. **Every item pulled from the input stream produces exactly one
   `ItemOutcome<T>`.** No item that the library accepted is silently
   dropped. Classification by where it failed:
   - locally invalid before any wire attempt →
     `Failed(BatchFailure { item, error: Request(BatchInputInvalid) })`,
   - transmitted and rejected → `Failed(...)` with the wire error,
   - transmitted and committed (or no-op via the read-back guard) →
     `Succeeded(BatchSuccess { item, output: MutationSuccess::Applied | Skipped })`,
   - transmitted with unknown final state → `Uncertain(...)`.
2. **Locally invalid items do not poison the stream.** The output
   stream continues past locally-invalid items with subsequent items
   from the input. Vec-batch can preflight the whole input before
   transmission and abort with outer `Err(BatchInputInvalid)`;
   streaming cannot know the full input upfront without destroying
   the streaming contract, so per-item local validation failures
   belong in the item outcome lane.
3. **Early termination covers only already-pulled items.** If the
   output stream ends with `SyncEvent::Terminated(AccountError)`
   before draining the input, only items already pulled from the
   input stream are covered by the exact-once outcome guarantee.
   Items not yet pulled remain unattempted. The consumer can detect
   "what was attempted" by comparing pulled count to outcome count.
4. **Stream-level errors that prevent any further attempts surface
   as `SyncEvent::Terminated(AccountError)`.** They do not appear in
   any per-item outcome lane. Per-item lanes are reserved for items
   the library actually pulled from the input.

### How streaming and Vec-batch differ

| Concern | Vec-batch | Streaming-batch |
|---|---|---|
| Input shape | `Vec<BatchItem<I>>` | `AccountStream<I>` |
| Output shape | `Result<BatchOutcome<T>, AccountError>` | `AccountStream<SyncEvent<ItemOutcome<T>>>` |
| Preflight invalidity | Outer `Err(BatchInputInvalid)` aborts whole batch | Per-item `Failed(BatchInputInvalid)`; stream continues |
| Backpressure | None; whole input buffered | Engine backpressures the input stream |
| Use case | Multi-recipient send, single-roundtrip batch | Bulk flag/move/destroy over arbitrary target sets |

The shape difference is deliberate. Streaming preserves the engine's
ability to drive multi-million-item mutation passes; Vec-batch
preserves the all-or-nothing preflight discipline for operations that
the consumer expects to validate atomically.

## Warnings

Warnings are observability events that flow alongside the data
stream. They are **not** part of the error model:

- Warnings do not flow through `AccountError`.
- Warnings do not carry `RecoveryClass`.
- Warnings do not abort streams. They are advisory; the stream
  continues normally.

Examples: protocol strategy downgrades (QRESYNC → CONDSTORE),
throttling notifications, read-back skip counts, clock-skew
detection, blob-type fallback notices.

```rust
#[non_exhaustive]
pub struct Warning {
    pub kind: WarningKind,
    pub message: DiagnosticText,
    pub next_action: Option<DiagnosticText>,
    pub protocol_detail: Option<DiagnosticText>,
    pub retry_count: u32,
}
```

Warnings share the `DiagnosticText` visibility discipline used by
`AccountError`. The free-text fields are `DiagnosticText`, not raw
`String`, so the same `UserSafe` / `SupportOnly` tagging applies.
Without this discipline, warnings would become the leak path for
raw provider strings into UI, logs, and telemetry - defeating the
visibility model `AccountError` enforces. Producers tag warning
text the same way they tag diagnostic text.

`WarningKind` enumerates the named warning categories
(`StrategyDowngraded`, `OperatorAttentionNeeded`, `Throttled`,
`ClockSkew`, `BlobNotByteStream`, `ReadbackSkipped`, etc.) and is
the stable surface for analytics; consumers do not match on the
diagnostic text.

## Stream termination

`SyncEvent::Fatal(Fatal { recovery, message, source })` is renamed
to `SyncEvent::Terminated(AccountError)`. The old name conflicted
with `Fatal(AccountError)` from `bifrost-types::recovery`, which has
a different meaning (engine-boundary collapse of any terminal
`RecoveryClass`). The two concepts:

- `SyncEvent::Terminated(AccountError)` - this stream ends here.
  The carried `AccountError` describes why. Engine reads
  `error.recovery()` to decide what to do next: retry the stream
  (`Retry`), reconcile (`Reconcile`), follow an `EngineDirective`,
  or stop (terminal variants).
- `Fatal(AccountError)` - newtype from
  `bifrost-types::recovery::Fatal` that collapses any
  `AccountError` whose `RecoveryClass` is terminal. The engine
  uses `Fatal::try_from(&error)` at boundaries where it specifically
  needs the terminal-only subset (operator notification queues,
  permanent-failure dashboards). Construction fails for non-terminal
  errors; this is the type-system guarantee that
  "the engine has nothing more to try."

Stream termination is the event; `Fatal` is the boundary check.
Conflating them under one name in the previous design was a bug.

## NotFound semantics

`AccountErrorKind::NotFound` is either consumer-visible or absorbed
by the operation. The protocol crate makes the decision so consumers
do not have to:

| Operation | NotFound behavior |
|---|---|
| `delete_*`, `expunge_*` | absorbed as `Ok(())` |
| `move_to` when the source message is already gone | absorbed as `Ok(())` |
| `update_flags` against a missing message | absorbed as `Ok(())` |
| `get_*`, `hydrate`, `fetch_body` | surfaced as `Err(NotFound)` |
| `send` with a recipient mailbox the provider rejects as unknown | surfaced as `Err(NotFound)` |
| Mutation guarded by an `If-Match` etag mismatch | surfaced as `ConcurrencyConflict`, not `NotFound` |

Absorbed NotFound returns `Ok` with no signal that the target was
already gone. Consumers needing post-hoc audit must use the inventory
or history surfaces, not the mutation return value.

## Partial-success operations

Bulk and multi-target operations cannot be represented by a single
`AccountError`:

- Multi-recipient SMTP send with per-recipient enhanced status codes.
- JMAP/Graph batch mutations with per-item outcomes.
- Multi-message moves where some messages succeed and others fail.
- Any operation where a transport drop after partial server
  acknowledgement leaves the caller unable to prove final state.

Multi-target `Account` methods take `Vec<BatchItem<I>>` and return
`Result<BatchOutcome<T>, AccountError>`:

```rust
pub struct BatchItem<I> {
    pub id: BatchItemId,
    pub input: I,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BatchItemId(pub String);

/// `#[non_exhaustive]` permits future *non-lane* metadata fields
/// (timing, batch id echo, totals). The three outcome lanes are final;
/// adding a fourth lane is a major-version breaking change.
#[non_exhaustive]
pub struct BatchOutcome<T> {
    pub succeeded: Vec<BatchSuccess<T>>,
    pub failed: Vec<BatchFailure>,
    pub uncertain: Vec<BatchUncertain>,
}

pub struct BatchSuccess<T> {
    pub item: BatchItemId,
    pub output: T,
}

pub struct BatchFailure {
    pub item: BatchItemId,
    pub error: AccountError,
}

pub struct BatchUncertain {
    pub item: BatchItemId,
    pub error: AccountError, // explains the source of uncertainty
}

/// Canonical access pattern. NOT `#[non_exhaustive]` - the three
/// lanes are the model. Adding a fourth lane is a deliberate
/// breaking change.
pub enum BatchItemOutcome<'a, T> {
    Succeeded(&'a BatchSuccess<T>),
    Failed(&'a BatchFailure),
    Uncertain(&'a BatchUncertain),
}

impl<T> BatchOutcome<T> {
    pub fn iter(&self) -> impl Iterator<Item = BatchItemOutcome<'_, T>>;
}
```

`BatchItem<I>` is the **required input shape.** Multi-target methods
do not accept a bare `Vec<I>`; the caller must supply a
`BatchItemId` with each input. This makes correlation enforceable
rather than conventional - implementers cannot invent provider-side
IDs or use sequence indices that diverge under retries.

`BatchItemId` is required to be **non-empty and unique within a
batch.** Empty IDs and duplicates are detected at preflight before
any byte leaves the socket and produce
`Err(AccountError { kind: Request(BatchInputInvalid), .. })` with a
`RequestCause::BatchInputInvalid` carrying every offending ID and
its reason (`Duplicate`, `Empty`). The library does not pick a
disambiguation policy on the caller's behalf; ambiguous input is
caller-fixable input.

`BatchItemId` is **caller-correlated, not provider-correlated.** The
caller chooses what it means (recipient address, draft id, hash of
the input payload, sequence index serialized to string - whatever
maps back to caller-side state). The library echoes it through all
three outcome lanes verbatim. This works even when the provider
returns no stable per-item identifier for failures, which is the
common case for mid-batch rejections.

All three outcome lanes carry `BatchItemId`, so consumer correlation
is identical regardless of outcome. `T` is free to carry per-item
success payload (assigned message id, acceptance code, server-side
metadata) or `()` when there is no useful payload - the lane wrapper
owns identity in all cases.

`BatchItemOutcome<'a, T>` is the canonical access pattern. It is
deliberately **not** `#[non_exhaustive]`: the three lanes are the
model, not an extension point. Wildcard arms over `BatchItemOutcome`
would let stale consumer policy silently apply to a new lane (a fifth
"pending" or "deferred" status added in v2.0), which on partial-
success operations is exactly the failure mode the model exists to
prevent - silently treating uncertain or pending items as successes
can double-send mail or lose audit signal. If the three-lane model
ever needs to grow, that is a major-version breaking change and
every consumer's match arm gets a compile error to update. Smooth
evolution is the wrong goal here. The three `Vec` fields remain for
ergonomic "give me all successes" access, but the documented
canonical iteration is `for outcome in batch.iter()` with exhaustive
matching.

`BatchOutcome::iter()` yields items in **submission order** - the
order in which `BatchItem`s appeared in the input `Vec`, regardless
of lane. The protocol crate tracks submission index alongside lane
assignment. Consumers rendering "recipients in the order I typed
them, with outcomes" get the natural ordering. Consumers processing
lane-major (handle all failures first, then all uncertains) access
the `succeeded`, `failed`, and `uncertain` `Vec` fields directly,
which preserve submission order within each lane. Two access
patterns, two orderings, both documented.

`BatchOutcome` itself is `#[non_exhaustive]` only to admit future
batch-level metadata fields (timing breakdown, server-side batch id,
totals). It is *not* a license to add outcome lanes - those are the
domain of `BatchItemOutcome`, which is closed.

### Boundary invariants

These hold for every multi-target `Account` method. The protocol
crate is responsible for upholding them; consumers may rely on them
without defensive checks.

1. **`Err(AccountError)`: no item crossed the side-effect boundary;
   no per-item reconciliation is required.** The batch was not
   transmitted. DNS failed, TLS failed, auth was rejected before the
   request body went out, the request was malformed and never sent,
   or per-item local validation rejected some inputs (see invariant
   6). Whether retrying is safe is a separate decision the caller
   makes from the `RecoveryClass` on the error; the guarantee here
   is strictly about side effects, not retryability.
2. **`Ok(BatchOutcome)`: every submitted `BatchItem` appears exactly
   once** in `succeeded`, `failed`, or `uncertain`. No item is
   missing; no item is duplicated across lanes; no item appears in
   `Ok(BatchOutcome)` that was not in the original input.
3. **Unknown transmission state is treated as transmitted.** If the
   protocol crate cannot prove the batch was not transmitted, it
   must return `Ok(BatchOutcome)` and place unresolved items in
   `uncertain`. The conservative side of the boundary is "we tried";
   only proven non-transmission produces `Err`.
4. **No partial-success API may return a single summary
   `AccountError` after crossing the side-effect boundary.** Once
   any byte of the request has been transmitted, the return must be
   `Ok(BatchOutcome)` with per-item lanes populated; collapsing
   per-item outcomes into a global error is a data-loss bug.
5. **Global post-transmission failures are expanded to item-level
   entries.** When the provider returns a single global error after
   accepting the batch (transaction rollback, post-write quota
   rejection, opaque batch failure with no per-item structure), the
   protocol crate fans the global `AccountError` out to every
   submitted item - `failed` when the failure is known, `uncertain`
   when outcome ambiguity is known. Each item carries a clone of the
   global error so the consumer can render and route it per-item
   without losing the underlying signal.
6. **Locally-detectable invalid items abort the batch.** Inputs that
   fail validation without a server roundtrip (malformed recipient
   address, oversized payload, missing required field, empty or
   duplicate `BatchItemId`) produce `Err(AccountError)` with
   `kind: Request(BatchInputInvalid)` and a
   `RequestCause::BatchInputInvalid { items }` carrying the offending
   IDs and their reasons. No items cross the side-effect boundary.
   The caller fixes or removes the listed items and resubmits; the
   library does not silently split the caller's intent by
   transmitting the valid subset. The kind itself is a stable
   no-payload discriminator; the unbounded item list is forensic
   detail in the cause.

### Why the boundary contract matters

Consumers reading the return shape know exactly what happened without
inspecting `RecoveryClass` to infer transmission state. `Err` means
"the request did not cross the side-effect boundary; no per-item
reconciliation is required." `Ok(BatchOutcome)` means "the request
crossed the boundary; here is exactly what happened to each item."
Crossing the boundary does not imply that any item produced a
successful side effect - a transmitted batch where every recipient
is rejected is `Ok(BatchOutcome { succeeded: vec![], failed: vec![...] })`,
not `Err`. The hard classification (transmitted vs not, per-item
outcome vs ambiguous) lives in the protocol crate, which has the
wire-level evidence (`AttemptCause::transmission_state`) to make it.

The `uncertain` lane exists for the same reason `RetryDisposition`
exists at the single-target level: some operations do not cleanly
fail. SMTP after partial recipient acceptance, Graph batch responses
truncated by a timeout, a connection drop after some items in a batch
have been committed by the server - these are not failures, and they
are not successes. A two-lane shape forces the caller to
miscategorize them. The `uncertain` lane requires the caller to
reconcile (probe inventory, check Sent, dedupe by `BatchItemId`)
before acting on those items.

Reviewers reject any multi-target signature whose return is
`Result<(), AccountError>` or `Result<Vec<T>, AccountError>` - these
flatten exactly the information the consumer needs. The only
acceptable signature for a multi-target `Account` method is
`Result<BatchOutcome<T>, AccountError>`.

`BatchOutcome` is `#[non_exhaustive]` to admit future *batch-level
metadata* (timing, batch id echo, totals); the three outcome lanes
are final. Adding a fourth lane is a major-version breaking change,
not a smooth extension.

## What this deliberately does not do

- No localized strings. Consumers own copy and localization.
- No generic public `Other` outcome. Provider-native enums use typed
  `Unknown { code }` variants where forward compatibility is needed.
- No naked diagnostic strings in classifications. Use `DiagnosticText`.
- No public field access on `AccountError`. Construction is via
  `AccountErrorBuilder`; reads are via accessors.
- No single-error summary of multi-target results. Partial-success
  operations return `Result<BatchOutcome<T>, AccountError>` (see
  above).
- No exception hierarchy. Nested enums provide coarse-to-detailed
  matching.
- No live-server or end-to-end behavior in this repo's tests. Pin the
  classification, conversion, and recovery behavior with small unit
  tests.

## Open decisions

`EngineDirective::CapabilityChanged { delta: CapabilityDelta }`
currently has no known useful producers if every site passes
`CapabilityDelta::default()`. Recommendation: remove it from the
directive enum and keep `StateCause::CapabilityChanged { delta }`
only if producers compute a real delta. Otherwise, map capability
shifts to `EngineDirective::RestartAccount`.

The exact `AccountOperation`, `ErrorScope`, `ResourceKind`, `Provider`,
and `Protocol` variant lists should be aligned with the final shared
type surface before implementation.

## File ownership and exit criteria

Single agent per `plans/unification.md` S1-W4. Owns:

- `crates/types/src/error.rs` for the public error contract.
- `crates/types/src/recovery.rs` for the central mapping.
- `crates/net/src/recovery.rs` or equivalent for context-aware
  `net::Error` conversion.
- Per-protocol `error*.rs` and `recovery*.rs` files, collapsed to one
  `into_account_error` each.
- Every protocol crate's `impl Account for ...` returns.

Exit:

- All `Account` methods return `Result<_, AccountError>`.
- `AccountError` is opaque (no `pub` fields). Construction is via
  `AccountErrorBuilder::build` only; every protocol crate's
  `into_account_error` routes through it.
- `AccountError` exposes accessors for `kind`, `recovery`,
  `suggested_remediation`, `scope`, `operation`, `provider`,
  `protocol`, and `chain`.
- `AccountError::message_key()` returns a stable `&'static str` for
  every kind; the key namespace is documented and append-only.
- `AccountError::user_safe_text()`, `telemetry_fields()`,
  `support_minimal()`, `support_consented()`, and `support_internal()`
  are the only public diagnostic accessors; `DiagnosticInfo` is not
  part of the consumer surface.
- `TelemetryView` is structured fields only - no free-form text.
  Support exports are `serde::Serialize` and gated by consent tier.
- Consumers do not need to inspect `chain` to classify the error.
  `WireCause` is documented as diagnostic-only; classification gaps
  are library bugs.
- `RecoveryClass` distinguishes `AuthLost`, `NeedsAdminConsent`,
  `NeedsPolicyChange`, `NoPermission`, `Unsupported`, `ClientBug`,
  `ProviderContractViolation`, `ProviderRefused`, and
  `UnknownPermanent` terminal cases. `Fatal` collapses these at the
  engine boundary via `TryFrom`.
- Engine-control directives (`RestartScope`, `RestartAccount`,
  `DowngradeStrategy`, `DowngradeCapabilityForScope`,
  `SchemaIncompatible`, `CapabilityChanged`, `OperatorOverrideRequired`)
  live in `EngineDirective` under `RecoveryClass::Engine(...)`.
- `RecoveryClass` exposes four mutually exclusive helpers:
  `is_retryable`, `requires_reconciliation`, `requires_engine_action`,
  `is_terminal`.
- `RetryAdvice` carries only safe-retry dispositions: `SameRequest`,
  `AfterStateRefresh`, `AfterAuthRefresh`. Cases that previously
  produced `Retry { DoNotRetry }` reclassify to terminal variants;
  cases that previously produced `Retry { OutcomeUncertain }` are now
  `Reconcile(ReconcileAdvice)`. `RetryAdvice` also carries `reason`,
  `not_before`, `min_delay`, and `throttle_scope`. Mid-stream
  transport drops on non-idempotent operations produce `Reconcile`;
  rate-limit and quota failures carry a `throttle_scope`.
- `TransmissionState` (`Unsent` / `InFlight` / `Acknowledged`) is the
  sole field on `AttemptCause`, which the transport layer pushes onto
  the chain whenever a network attempt is made. Recovery derivation
  reads it from `AttemptCause` only; `TransportCause` never carries
  transmission state and the codebase has zero references to
  `TransportCause::transmission_state`. Absence of `AttemptCause`
  classifies as `Unsent` but is preserved verbatim in support exports.
  No separate builder setter.
- `ServerCause::Error { status: Option<u16> }`. `Some(_)` for
  HTTP-like providers; `None` for protocol server failures without
  numeric status (IMAP `NO`/`BAD` outside any response code). No
  sentinel values (no `status: 0`).
- `AccountOperation` granularity matches per-mutation idempotency
  (`UpdateFlags`, `MoveMessage`, `DeleteMessage`, `ChangeLabel`,
  `EditFolder`, etc.); no generic `Mutate` variant.
- `AccountError` implements `StdError`; `source()` returns the
  outermost `Cause`. Every `Cause` variant implements `StdError`.
- `DiagnosticInfo` and `CauseChain` are stored as `Arc` on
  `AccountError`. Clone is cheap.
- `Provider` and `Protocol` are top-level fields on `AccountError`,
  not buried in `DiagnosticInfo`.
- `ErrorScope` carries no `Option<String>` identifiers. Unknown ids
  promote to a broader scope variant.
- `AccountError`, causes, diagnostics, and recovery payloads derive
  `Clone`.
- No generic public `Other` or naked `String` escape-hatch variants.
- Unknown provider codes are typed as `Unknown { code }` **only for
  forward compatibility**. Known provider vocabulary must be typed
  with named variants; routing known codes through `Unknown { code }`
  plus string matching is forbidden. The Phase 2.2 audit identified
  the JMAP `SetErrorType` family, `GraphSignal::InvalidDeltaToken`,
  and `GraphSignal::SyncStateNotFound` as known-vocabulary that must
  ship as typed variants in the Phase 1 amendment.
- `AccountError::into_builder()` is the only supported path for
  decorating an existing error with secondary evidence. `build()`
  remains the single invariant funnel; no mutator bypasses it.
- No stream may discard a computed `AccountError`. Stream termination
  always carries the structured error via
  `SyncEvent::Terminated(AccountError)`.
- No per-item accounted failure (item appears in `BatchOutcome` or
  emits `ItemOutcome::{Failed,Uncertain}`) may also be emitted as a
  trailing global stream termination. Global termination is reserved
  for stream-level errors that prevent further attempts.
- No `AccountOperation` placeholder remains in PIM or `Account` call
  sites. Each call site passes the operation that matches its
  side-effect and idempotency; misclassifying a non-idempotent op as
  idempotent can cause recovery to choose `Retry::SameRequest` where
  the correct action is `Reconcile`.
- SMTP and LMTP send pipelines carry `TransmissionState` from each
  command phase into the `AccountError` via `AttemptCause`. The crate
  is not considered complete until non-idempotent `Send` can be
  classified with the correct transmission state.
- One central recovery mapping exists in `bifrost-types::recovery`,
  consuming `(kind, scope, operation, primary_cause)` plus builder
  overrides; it is the only producer of `RecoveryClass` and
  `RemediationAction` values.
- `bifrost-net` has context-aware account-error conversion; JMAP,
  Gmail, and Graph use it.
- Graph substring-matching recovery is gone.
- Gmail `account_error_from_template` is gone.
- Protocol-specific recovery and fatal constructors are gone.
- All factories take `Arc<dyn TokenSource>` at construction;
  per-factory `set_access_token` is gone.
- Multi-target Vec-batch `Account` methods return
  `Result<BatchOutcome<T>, AccountError>`. `Err` is reserved for
  non-transmitted batches; `Ok(BatchOutcome)` accounts for every
  submitted item exactly once across `succeeded`, `failed`, and
  `uncertain`. `BatchItemId` is caller-correlated. Collapsing
  per-item outcomes into a single `AccountError` is forbidden.
- Streaming bulk `Account` methods return
  `AccountStream<SyncEvent<ItemOutcome<T>>>` (optionally wrapped in
  `SyncEvent::Batch`). Every pulled item produces exactly one
  `ItemOutcome`; locally-invalid items emit `Failed` rather than
  poisoning the stream; early termination via
  `SyncEvent::Terminated(AccountError)` covers only already-pulled
  items.
- `MutationSuccess::{Applied, Skipped}` carries the read-back
  guard's "already-in-state" outcome inside the success lane.
- `SyncEvent::Fatal` is renamed to `SyncEvent::Terminated(AccountError)`.
  The `Fatal` newtype from `bifrost-types::recovery` remains for the
  engine-boundary terminal-only collapse.
- `Warning` uses `DiagnosticText` for all free-form text fields and
  is documented as outside the error model.
- NotFound absorption policy is documented per operation and the
  protocol crates implement it.
- `brokkr check` is clean workspace-wide.
