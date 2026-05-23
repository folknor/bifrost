# Error model: bifrost-net implementation plan

This is Phase 2.1 of `plans/error-model-roadmap.md`.

`bifrost-net` lands first because JMAP, Gmail, and Graph need a
single HTTP transport boundary that can emit `AttemptCause` with the
right `TransmissionState`. Phase 2 still runs on the intentionally
broken feature branch. Author the code and tests, but do not run
`brokkr`, `cargo`, or `./diff_test.sh` from the crate agent.

## Required reading

- `CLAUDE.md`
- `plans/error-model-roadmap.md`
- `plans/error-model-convergence.md`
- `reference/net.md`
- `crates/types/src/error/` (the landed Phase 1 API)

## Scope

Own only `crates/net/` files. Do not edit protocol crates in this
phase. JMAP, Gmail, and Graph consume the new boundary in their own
Phase 2 patches.

The work is not a wholesale rewrite of the transport. Keep the request
pipeline, retry loop, redirect loop, OAuth refresher, rate governor,
and bandwidth meter architecture intact. Add the missing evidence and
conversion boundary.

## Current state

Current files:

- `crates/net/src/error.rs`: public `Error` enum.
- `crates/net/src/request.rs`: request builder, retry loop, redirect
  loop, `Retry-After` parsing, buffered and streaming send paths.
- `crates/net/src/net.rs`: `Net`, `AccountNet`, ranged download
  validation, `into_byte_stream`.
- `crates/net/src/auth.rs`: `TokenSource`, `OAuthRefresher`,
  `RefreshFailed` projection.
- `crates/net/src/retry.rs`: `RetryPolicy`.
- `crates/net/src/rate.rs`: `RateLimitGovernor`.
- `crates/net/src/lib.rs`: public re-exports.

There is currently no `NetErrorContext` and no net-level
`into_account_error`. Protocol crates pattern-match
`bifrost_net::Error` directly.

The current `Error` shape is not precise enough for the convergence
contract:

- `Error::Network` does not say whether no bytes left the socket,
  bytes were sent without a terminal response, or response headers
  arrived and the body then failed.
- `Error::Timeout` is a unit variant, so connect timeout and mid-body
  timeout are indistinguishable.
- `Error::AuthLost` is used both for local token-source loss and for
  a target request that received a second 401 after forced refresh.
- `Error::RangeNotHonored` mixes local invalid range requests with
  provider responses that acknowledged the request but violated the
  range contract.

Fix those evidence gaps before writing the conversion.

## Files to modify

- `crates/net/src/error.rs`
  - Add the evidence fields and helper enums listed below.
  - Keep `Error` as the transport crate's own error type.
- `crates/net/src/request.rs`
  - Populate transmission state in the retry loop.
  - Move or expose `Retry-After` parsing for reuse by the conversion.
- `crates/net/src/net.rs`
  - Populate range-failure cause detail.
  - Mark response-body stream failures as acknowledged partial
    response evidence.
- `crates/net/src/auth.rs`
  - Update `AuthLost` and `RefreshFailed` construction after the
    error-shape change.
- `crates/net/src/account_error.rs`
  - New module containing `NetErrorContext`,
    `into_account_error`, and private classification helpers.
- `crates/net/src/lib.rs`
  - Add `pub mod account_error;`
  - Re-export `NetErrorContext` and `into_account_error`.
- `crates/net/Cargo.toml`
  - Confirm `bifrost-types` dependency is already present (it is, as
    of Phase 1). No new dependency edges expected; if the agent finds
    one missing, document it before adding.
- `crates/net/tests/account_error.rs`
  - New synthetic conversion tests. No live HTTP.

## Files to delete

None expected. `bifrost-net` does not currently have old recovery
helpers to remove.

## Public boundary

Add:

```rust
#[derive(Clone, Debug)]
pub struct NetErrorContext {
    pub provider: Option<Provider>,
    pub protocol: Protocol,
    pub operation: Option<AccountOperation>,
    pub scope: Option<ErrorScope>,
}

pub fn into_account_error(error: Error, ctx: NetErrorContext) -> AccountError;
```

`protocol` is required because `WireCause::MalformedResponse` needs a
protocol. `provider`, `operation`, and `scope` are optional because
not every transport caller has that context at the HTTP layer.

Do not add a context-free `From<Error> for AccountError`. A conversion
without operation and scope would silently misclassify non-idempotent
in-flight failures.

## Error evidence changes

Use the landed `bifrost_types::TransmissionState`.

Update transport-like errors so the conversion can distinguish the
side-effect boundary:

```rust
Network {
    message: String,
    transmission_state: TransmissionState,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

Timeout {
    transmission_state: TransmissionState,
}

Tls {
    message: String,
    transmission_state: TransmissionState,
}

AuthLost {
    transmission_state: Option<TransmissionState>,
}
```

Rules:

- `Unsent`: no request bytes crossed the target operation boundary.
  Examples: token source failed before building the target request,
  connect refused, DNS failure, connect timeout, TLS handshake failure
  (whether reqwest reports it via `NetSetup`, `Tls`, or a `Network`
  send error — handshake completes before any HTTP bytes leave the
  socket, so it is always `Unsent`).
- `InFlight`: request bytes may have crossed the boundary, but no
  terminal response headers arrived. Includes a TLS error raised
  mid-body after the handshake completed.
- `Acknowledged`: response headers arrived. Do not build
  `AccountErrorKind::Transport(_)` with this state. Convert
  acknowledged body failures to `Protocol(PartialResponse)`.

These rules are normative. The mapping table below MUST follow them;
any row that writes `Attempt(state) when known` for a transport-like
error is shorthand for "compute `state` per the rule above" — never an
invitation to omit the attempt cause for handshake failures.

Add range evidence instead of string matching:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RangeFailureKind {
    LocalInvalid,
    ResponseNotPartial,
    MissingContentRange,
    ContentRangeMismatch,
}

RangeNotHonored {
    kind: RangeFailureKind,
    message: String,
}
```

Local invalid range requests map to `Request(Malformed)` with no
attempt cause. Response range failures map to
`Protocol(ContractViolation)` with `Attempt(Acknowledged)`.

Existing public matches in protocol crates will break. That is
acceptable on the feature branch; the protocol crate plans update
their matches in Phase 2.2.

## Request pipeline changes

In `send_streaming_inner`:

- On `request.send().await` error, check in this exact order so that a
  connect-timeout (`is_timeout() && is_connect()` both true) lands in
  the `Timeout { Unsent }` arm and not the `Network { Unsent }` arm:
  1. `e.is_timeout() && e.is_connect()` => `Timeout { Unsent }`.
  2. `e.is_timeout()` (and not `is_connect()`) => `Timeout { InFlight }`.
  3. `e.is_connect()` (and not `is_timeout()`) => `Network { Unsent }`.
  4. any other request-send error => `Network { InFlight }`.
- On repeated target 401 after forced refresh:
  - return `AuthLost { transmission_state: Some(Acknowledged) }`.
- On token-source `current()` or `refresh()` failure before target
  dispatch:
  - preserve the token-source error. It has no target attempt cause.
- On terminal HTTP response converted to `Status`, `RateLimited`, or
  `RetryBudgetExhausted`:
  - the conversion will add `Attempt(Acknowledged)`.
- Keep refund and retry behavior unchanged.

In `into_byte_stream`:

- A body chunk error happens after response headers arrived.
- Return `Timeout { transmission_state: Acknowledged }` for timeout
  body errors.
- Return `Network { transmission_state: Acknowledged, .. }` for other
  body errors.
- The conversion maps these to `Protocol(PartialResponse)`, not
  `Transport`.

Move `parse_retry_after` out of the private tail of `request.rs` so
both the retry loop and `account_error.rs` can use the same parser.
Keep the parser returning `Duration`; the conversion can set both:

- `ServerCause::* { retry_after: Some(duration) }`
- `AccountErrorBuilder::retry_not_before(SystemTime::now() + duration)`

Use `checked_add` and skip `retry_not_before` if the addition
overflows.

## Conversion algorithm

`into_account_error` should:

1. Classify the `Error` into `(AccountErrorKind, primary Cause)`.
2. Build with `AccountErrorBuilder::new(kind, primary_cause)`.
3. Attach context:
   - `.protocol(ctx.protocol)`
   - `.provider(provider)` when `ctx.provider` is `Some`
   - `.operation(op)` when `ctx.operation` is `Some`
   - `.scope(scope.clone())` when `ctx.scope` is `Some`
4. Push `Cause::Attempt(AttemptCause { transmission_state })` when
   the error has target-attempt evidence.
5. Attach diagnostics:
   - `.status(status.as_u16())` for HTTP status errors.
   - `.request_id(...)` from request-id style headers.
   - `.trace_id(...)` from trace headers.
   - `.native_code(...)` only for stable header-native codes. Do not
     parse JSON bodies in `bifrost-net`.
   - `.text(DiagnosticText::support_only(...))` for body excerpts,
     provider text, local validation messages, or source messages.
6. Attach retry and throttle hints:
   - `ServerCause` gets the relative retry delay.
   - builder `.retry_not_before(...)` gets now plus that delay.
   - builder `.throttle_scope(...)` only for rate-limit and quota
     kinds.
7. Call `.build()`.

Do not construct `RecoveryClass` directly in `bifrost-net`. The
builder derives it centrally.

## Error mapping table

### Kind/cause shape asymmetry

The landed Phase 1 types use different shapes for kind vs cause on
the same conceptual signal:

- `ServerErrorKind::Error { status: Option<u16> }` (kind side; the
  status is optional because the kind classifies any server-side
  failure, even one without a numeric code).
- `ServerCause::Error { status: u16 }` (cause side; if there is no
  status, do not push a `ServerCause::Error` — push a different
  cause variant instead).

The table rows reflect this on purpose: the kind column always wraps
the code in `Some(_)`, the cause column does not. The builder's
`recovery::kind_matches_cause` invariant accepts both shapes for the
`Server(_)` family. Do not "fix" one column to match the other.

### Transport and auth

| net error | AccountErrorKind | primary cause | attempt cause |
|---|---|---|---|
| `Network { state: Unsent }` | `Transport(Network)` | `Transport(Network)` | `Attempt(Unsent)` |
| `Network { state: InFlight }` | `Transport(Network)` | `Transport(Network)` | `Attempt(InFlight)` |
| `Network { state: Acknowledged }` | `Protocol(PartialResponse)` | `Wire(MalformedResponse)` | `Attempt(Acknowledged)` |
| `Timeout { state: Unsent }` | `Transport(Timeout)` | `Transport(Timeout)` | `Attempt(Unsent)` |
| `Timeout { state: InFlight }` | `Transport(Timeout)` | `Transport(Timeout)` | `Attempt(InFlight)` |
| `Timeout { state: Acknowledged }` | `Protocol(PartialResponse)` | `Wire(MalformedResponse)` | `Attempt(Acknowledged)` |
| `Tls { state }` | `Transport(Tls)` | `Transport(Tls)` | `Attempt(state)` — always present; per the rules above, handshake failure is `Unsent`, mid-body is `InFlight` |
| `AuthLost { None }` | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` | none |
| `AuthLost { Some(state) }` | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` | `Attempt(state)` |
| `RefreshFailed { source }` | `Authentication(RefreshTransient)` | `Auth(RefreshTransient)` | none for the target request |

For `RefreshFailed`, push one extra support cause when possible:
transport source errors become `Cause::Transport`, server source
errors become `Cause::Server`, and auth source errors become
`Cause::Auth`. Do not recursively call `into_account_error`; that
would attach the target operation context to the token refresh
request.

### Local request and setup errors

| net error | AccountErrorKind | primary cause | attempt cause |
|---|---|---|---|
| `EncodeBody` | `Request(Malformed)` | `Request(InvalidArgument { field: Some("body") })` | none |
| `InvalidHeader` | `Request(Malformed)` | `Request(InvalidArgument { field: Some("header") })` | none |
| `CostExceedsBurst` | `Request(Malformed)` | `Request(InvalidArgument { field: Some("request_cost") })` | none |
| `RangeNotHonored { LocalInvalid }` | `Request(Malformed)` | `Request(InvalidArgument { field: Some("range") })` | none |
| `NetSetup` | `Transport(Tls)` or `Transport(Network)` | matching `Transport` cause | none |
| `Cancelled` | `Transport(Network)` | `Transport(Network)` | none |

For `NetSetup`, classify as TLS when the failure came from
certificate or native TLS setup. Otherwise classify as network setup.
Keep the original message as support-only diagnostic text. `NetSetup`
is pre-attempt by definition, so no `Attempt` cause is pushed — this
is the one transport-like case where "absent `Attempt`" is the correct
encoding (versus "`Attempt { Unsent }`", which means an attempt was
initiated). See convergence §"Cause variants" on the absent-vs-Unsent
distinction.

`Cancelled` is not currently constructed by the request pipeline. If
it reaches the conversion, no target-attempt evidence is available, so
do not invent one.

### Redirect and range response errors

| net error | AccountErrorKind | primary cause | attempt cause |
|---|---|---|---|
| `RedirectRejected` | `Request(Malformed)` | `Request(InvalidArgument { field: Some("redirect_policy") })` | `Attempt(Acknowledged)` |
| `RedirectLoop` | `Protocol(ContractViolation)` | `Wire(MalformedResponse)` | `Attempt(Acknowledged)` |
| `RangeNotHonored { ResponseNotPartial }` | `Protocol(ContractViolation)` | `Wire(MalformedResponse)` | `Attempt(Acknowledged)` |
| `RangeNotHonored { MissingContentRange }` | `Protocol(ContractViolation)` | `Wire(MalformedResponse)` | `Attempt(Acknowledged)` |
| `RangeNotHonored { ContentRangeMismatch }` | `Protocol(ContractViolation)` | `Wire(MalformedResponse)` | `Attempt(Acknowledged)` |

These errors all arise after an HTTP response was received. They are
not transport failures.

### HTTP status and retry-budget errors

`Status { code, body, headers }` always carries
`Attempt(Acknowledged)`.

`RetryBudgetExhausted { last_status: Some(status), ... }` also
carries `Attempt(Acknowledged)` and uses the same status
classification, but without a body.

`RateLimited { retry_after }` is equivalent to HTTP 429 after the
retry budget is exhausted.

| status | AccountErrorKind | primary cause |
|---|---|---|
| 400, 422 | `Request(Malformed)` | `Request(Malformed { detail })` |
| 401 | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` |
| 403 | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` |
| 404 with resource scope | `NotFound(resource)` | `Request(NotFound { what: resource, id })` |
| 404 without resource scope | `Server(Error { status: Some(404) })` | `Server(Error { status: 404 })` |
| 409 | `ConcurrencyConflict` | `State(ConcurrencyConflict)` |
| 410 with `ErrorScope::Cursor` | `SyncState(CursorInvalid)` | `State(CursorInvalid)` |
| 410 without cursor scope | `Server(Error { status: Some(410) })` | `Server(Error { status: 410 })` |
| 408, 502, 503, 504 | `Server(Unavailable)` | `Server(Unavailable { retry_after })` |
| 429 | `Server(RateLimited)` | `Server(RateLimited { retry_after })` |
| 507 | `Server(QuotaExhausted)` | `Server(QuotaExhausted { retry_after })` |
| other 500..=599 | `Server(Error { status: Some(code) })` | `Server(Error { status: code })` |
| other status | `Server(Error { status: Some(code) })` | `Server(Error { status: code })` |

Do not parse provider JSON bodies in this crate. Graph `error.code`,
Gmail reason strings, and JMAP problem details belong to the protocol
crate translation boundaries. `bifrost-net` only supplies the fallback
HTTP classification and diagnostics.

`RetryBudgetExhausted::last_status` is `Option<StatusCode>`, but the
request pipeline only constructs the variant after a retryable HTTP
status with `Some(status)`. The `None` arm is unreachable from current
code. The conversion still has to handle it because the type permits
it: classify as `Transport(Network)` with `Attempt(InFlight)`. Pin
this with a test (`retry_budget_none_status_defensive_arm`) so the
arm cannot drift; if a future net change makes `None` reachable, the
test documents the intended classification.

## Scope helpers

Add private helpers in `account_error.rs`.

`resource_from_scope(scope: Option<&ErrorScope>) -> Option<ResourceKind>`:

- `Message` => `Message`
- `Mailbox` => `Mailbox`
- `Thread` => `Thread`
- `Calendar` => `Calendar`
- `Contact` => `Contact`
- all broader scopes => `None`

`id_from_scope(scope: Option<&ErrorScope>) -> Option<String>`:

- return the contained id for resource scopes.
- return `None` for broader scopes.

Use these for 403 `PermissionDenied { resource }` and 404
`RequestCause::NotFound`.

## Header diagnostics

Add private helpers for common HTTP diagnostics.

Request id candidates, first present wins:

- `x-request-id`
- `request-id`
- `x-ms-request-id`
- `x-goog-request-id`
- `x-guploader-uploadid`

Trace id candidates, first present wins:

- `traceparent`
- `x-cloud-trace-context`
- `x-ms-ags-diagnostic`

Native code candidates:

- only stable header codes. Do not parse response bodies.
- It is fine for this helper to return `None` initially if no current
  net caller exposes a stable header-native code.

All response bodies copied into diagnostics are support-only text and
must use the already-capped `Status.body`.

## Throttle scope

Set `ThrottleScope` only for `Server(RateLimited)` and
`Server(QuotaExhausted)`.

Initial mapping:

| context | throttle scope |
|---|---|
| `Protocol::Graph` or `Provider::Microsoft` | `Tenant` |
| `Protocol::Gmail` or `Provider::Gmail` | `Account` |
| `Protocol::Jmap` with `Provider::Fastmail` | `Account` |
| anything else | none |

Rationale: Microsoft Graph documents tenant-wide throttling on the
`Mailbox concurrency` and `Application` keys (see Graph throttling
limits docs); a 429 from net without protocol-body context is most
safely treated as tenant-scope so the engine pauses tenant-wide rather
than only the current request. Gmail and JMAP/Fastmail document
per-user quotas; absent more specific evidence, `Account` is the
correct floor.

This is fallback behavior for generic HTTP throttles. Protocol crates
SHOULD override by building their own `AccountError` directly when a
structured provider body gives a more precise quota domain (e.g. Graph
`Application` vs `Mailbox` keys; Gmail `userRateLimitExceeded` vs
`rateLimitExceeded` vs `dailyLimitExceeded`).

## Tests

Create `crates/net/tests/account_error.rs`. Tests construct synthetic
`bifrost_net::Error` values and call `into_account_error`; no live
HTTP, no mock servers.

Cover at least:

- `network_unsent_retries_same_request`
  - `Network { transmission_state: Unsent }`
  - operation `Send`
  - kind `Transport(Network)`
  - telemetry transmission state `Unsent`
  - recovery is retryable.
- `network_inflight_non_idempotent_reconciles`
  - `Network { transmission_state: InFlight }`
  - operation `Send`
  - recovery requires reconciliation.
- `network_inflight_idempotent_retries`
  - operation `UpdateFlags`
  - recovery is retryable.
- `acknowledged_body_failure_is_partial_response`
  - `Network { transmission_state: Acknowledged }`
  - operation `Send`
  - kind `Protocol(PartialResponse)`
  - recovery requires reconciliation.
- `timeout_unsent_uses_transport_timeout`
  - kind `Transport(Timeout)`
  - message key `transport.timeout`.
- `rate_limited_sets_retry_and_throttle_scope`
  - `RateLimited { retry_after: Some(...) }`
  - context `Protocol::Graph`
  - kind `Server(RateLimited)`
  - recovery retry advice has `ThrottleScope::Tenant`.
- `retry_budget_503_maps_to_unavailable`
  - kind `Server(Unavailable)`
  - attempt state `Acknowledged`.
- `status_404_with_message_scope_maps_not_found`
  - context `ErrorScope::Message { id }`
  - kind `NotFound(Message)`
  - chain outermost is `Request(NotFound { what: Message, id })`.
- `status_410_cursor_scope_restarts_scope`
  - context `ErrorScope::Cursor(scope)`
  - kind `SyncState(CursorInvalid)`
  - recovery requires engine action.
- `malformed_header_is_client_bug_without_attempt`
  - `InvalidHeader`
  - kind `Request(Malformed)`
  - no transmission state in telemetry.
- `refresh_failed_is_auth_refresh_transient`
  - `RefreshFailed { source: Network { ... } }`
  - kind `Authentication(RefreshTransient)`
  - recovery retry disposition is `AfterAuthRefresh`.
- `range_response_mismatch_is_contract_violation`
  - `RangeNotHonored { kind: ContentRangeMismatch, ... }`
  - kind `Protocol(ContractViolation)`
  - attempt state `Acknowledged`.

Do not run these tests in Phase 2. They are authored now and execute
after Phase 3 restores workspace compilation.

## Exit criteria

- `NetErrorContext` and `into_account_error` exist and are re-exported
  from `bifrost_net`.
- The conversion uses `AccountErrorBuilder::build`; no net code
  constructs `RecoveryClass` or `RemediationAction` directly.
- Every current `Error` variant has an explicit mapping in this plan
  and in code.
- Target-attempt evidence is represented as `Cause::Attempt`.
- Transport failures never build `AccountErrorKind::Transport(_)`
  with `TransmissionState::Acknowledged`.
- Response-body failures map to `Protocol(PartialResponse)`.
- `Status`, `RateLimited`, and status-backed `RetryBudgetExhausted`
  add `Attempt(Acknowledged)`.
- Local validation and setup failures do not invent attempt causes.
- `Retry-After` and throttle scope are populated for rate-limit,
  quota, and unavailable server classifications where data exists.
- Generic HTTP conversion does not parse Graph, Gmail, or JMAP JSON
  bodies. Structured provider body interpretation remains in the
  protocol crates.
- Synthetic conversion tests are added under `crates/net/tests/`.
- Every kind/cause pair the conversion produces satisfies
  `recovery::kind_matches_cause` (the builder asserts this at runtime;
  the agent must verify by inspection that no row in the mapping
  tables violates it, since tests cannot run during Phase 2).
- No `brokkr`, `cargo`, or `./diff_test.sh` command is run by the
  crate agent for this phase.

## Audit checklist

Domain-specific verification:

- Inspect every `return Err(Error::...)` in `crates/net/src/` and
  confirm it carries the planned evidence.
- Inspect `into_byte_stream` and confirm body read failures become
  acknowledged partial-response evidence.
- Inspect ranged download validation and confirm local range errors
  differ from provider range-contract errors.
- Inspect `account_error.rs` and confirm every `Error` variant is
  classified.

Cross-cutting reconciliation:

- Confirm every builder path calls `.protocol(ctx.protocol)`.
- Confirm `.operation(...)` is attached before `.build()` when
  present, because central recovery depends on idempotency.
- Confirm `.scope(...)` is attached before `.build()` when present,
  because cursor restart and not-found classification depend on it.
- Confirm throttle scope is set only for `Server(RateLimited)` and
  `Server(QuotaExhausted)`.
- Confirm no protocol crate is edited in this phase.

Editorial normalization:

- Update `reference/net.md` only after the implementation is real.
  Do not pre-document future behavior there.
- Keep this plan focused on current gaps. Remove resolved audit notes
  rather than preserving history.
