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
  - Add `Error::InvalidRequest`, `Error::MalformedRedirect`, and the
    capped body/headers fields on `Error::RateLimited` and
    `Error::RetryBudgetExhausted`.
  - Reclassify `Error::NetSetup` construction sites (see "Error
    evidence changes" below); the variant itself stays for backward
    compatibility but is no longer reached by the `Net::new()` paths.
  - Keep `Error` as the transport crate's own error type.
- `crates/net/src/request.rs`
  - Populate transmission state in the retry loop.
  - Preserve a capped final response body and headers on the
    retry-budget exhaustion path (currently dropped at line 642-653).
  - Switch the URL reparse error at line 536 from `Error::Network`
    to `Error::InvalidRequest` (it is a local invariant: the URL
    parsed once already for the original request).
  - Move or expose `Retry-After` parsing for reuse by the conversion.
- `crates/net/src/redirect.rs`
  - Switch the three malformed-redirect sites (missing `Location`,
    non-UTF-8 `Location`, unresolvable `Location`) from
    `Error::Network` to `Error::MalformedRedirect` with the matching
    `MalformedRedirectKind`.
- `crates/net/src/net.rs`
  - Populate range-failure cause detail.
  - Mark response-body stream failures as acknowledged partial
    response evidence.
  - Reclassify the three `Net::new()` `Error::NetSetup` construction
    sites (corrupt native_tls cert DER, reqwest DER rejection,
    `ClientBuilder::build()` failure) to `Error::InvalidRequest`
    with `RequestCause::InvalidArgument { field: Some("client_config" |
    "root_certs") }`. These are config-data failures, not transient
    network conditions; retrying against the same `NetConfig` would
    fail the same way.
- `crates/net/src/auth.rs`
  - Update `AuthLost` and `RefreshFailed` construction after the
    error-shape change. Acknowledged `AuthLost` (repeated target 401
    after forced refresh) populates `final_response` with the
    captured 401 status, headers, and capped body.
  - When the OAuth endpoint returns 429/503 with `Retry-After`, parse
    the deadline and store it on a new
    `Error::RefreshFailed { retry_after: Option<SystemTime>, source }`
    field so `account_error.rs` can pass it to
    `AccountErrorBuilder::retry_not_before(...)` without re-walking
    the wrapped source variants. The data ownership is explicit: the
    auth layer sees the OAuth response and writes the deadline onto
    its own error variant; the conversion layer reads it.
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
    pub operation: AccountOperation,
    pub scope: Option<ErrorScope>,
}

pub fn into_account_error(error: Error, ctx: NetErrorContext) -> AccountError;
```

`protocol` and `operation` are both required.

`operation` is required because the central recovery mapper defaults
missing operation to idempotent-true
(`crates/types/src/error/recovery.rs:181-183`). Without operation
context, `Network { InFlight }` routes to `Retry { SameRequest }` -
which on a `Send` is a double-send. Making `operation` non-optional
forces every net caller to supply the target operation and closes
the trap at this boundary. Callers that genuinely cannot supply one
are a producer bug.

`provider` and `scope` remain optional because not every transport
caller has that context at the HTTP layer.

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
  send error - handshake completes before any HTTP bytes leave the
  socket, so it is always `Unsent`).
- `InFlight`: request bytes may have crossed the boundary, but no
  terminal response headers arrived. Includes a TLS error raised
  mid-body after the handshake completed.
- `Acknowledged`: response headers arrived. Do not build
  `AccountErrorKind::Transport(_)` with this state. Convert
  acknowledged body failures to `Protocol(PartialResponse)`.

These rules are normative. The mapping table below MUST follow them;
any row that writes `Attempt(state) when known` for a transport-like
error is shorthand for "compute `state` per the rule above" - never an
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

Add a new variant for malformed redirect responses so the conversion
can distinguish them from generic network failures:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MalformedRedirectKind {
    MissingLocation,
    InvalidLocationEncoding,
    UnresolvableLocation,
}

MalformedRedirect {
    kind: MalformedRedirectKind,
    message: String,
}
```

The three call sites in `crates/net/src/redirect.rs` that currently
construct `Error::Network` for missing / non-UTF-8 / unresolvable
`Location` headers MUST switch to `Error::MalformedRedirect`. These
are protocol contract violations on an acknowledged 3xx response, not
network failures and not partial-completion signals. The conversion
maps them to `Protocol(ContractViolation)` with `Attempt(Acknowledged)`
(see the redirect/range table below). Keeping them as `Error::Network`
would cause the conversion to silently misclassify them as
`Protocol(PartialResponse)` and route to `Reconcile`, which is wrong:
the server's response was structurally invalid, not partially
delivered.

Add a new variant for pre-wire local request construction errors:

```rust
InvalidRequest {
    field: &'static str,
    detail: String,
}
```

`InvalidRequest` covers errors that surface from `request.send().await`
via reqwest's `is_builder()` path (URL parse failure, invalid header
value built into the request, body builder failure), and also covers
the URL reparse failure at `request.rs:536` in the redirect path
(which is a local invariant - the URL parsed once already for the
original request, so reparse failing is a producer bug, not a network
failure). The conversion maps these to `Request(Malformed)` with
`RequestCause::InvalidArgument { field, message }` and no `Attempt`
cause. `field` carries a stable identifier (`"url"`,
`"client_config"`, `"redirect_url"`, etc.) so support filters can
distinguish.

Replace `NetSetup` construction sites with `InvalidRequest`. The
three `Net::new()` sites are config-data failures, not transient
network conditions:

- `cert.to_der()` failure → caller's `native_tls::Certificate` is
  corrupt → `InvalidRequest { field: "root_certs", detail }`.
- `reqwest::Certificate::from_der()` failure → reqwest rejected the
  caller's DER → `InvalidRequest { field: "root_certs", detail }`.
- `ClientBuilder::build()` failure → TLS backend init / config
  failure → `InvalidRequest { field: "client_config", detail }`.

Retrying any of these against the same `NetConfig` would fail the
same way. They route through the conversion as `Request(Malformed)`
(terminal `ClientBug`), not as retryable Transport setup.

Producer rule (durable): only producer paths that are genuinely
transient may construct retryable `Transport` errors. Config/data
validation failures, including corrupt or rejected configured root
certs and any future `ClientBuilder` build-time misconfiguration,
must route to `Request(Malformed)` or another non-`Transport` kind.
If a future `ClientBuilder::build()` source inspection reveals a
truly transient case (e.g. system-cert store temporarily
unavailable), that subset may be split off into a `Transport(_)`
variant - but the default for setup failures is terminal.

Introduce a shared `FinalResponse` struct and extend `RateLimited`,
`RetryBudgetExhausted`, and `AuthLost` to carry it. Protocol crates
that depend on body parsing (Graph `error.code`, Gmail reason
strings, OAuth-error sub-codes on 401) can still classify after the
retry budget is exhausted or after a repeated-401 final attempt:

```rust
#[derive(Clone, Debug)]
pub struct FinalResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,             // capped at STATUS_BODY_CAP
}

RateLimited {
    retry_after: Option<Duration>,
    final_response: FinalResponse,
}

RetryBudgetExhausted {
    final_response: Option<FinalResponse>,
    retry_after_history: Vec<Duration>,
}

AuthLost {
    transmission_state: Option<TransmissionState>,
    final_response: Option<FinalResponse>,
}
```

The cap reuses the existing `STATUS_BODY_CAP` constant from
`crates/net/src/error.rs` (currently `usize = 4096`) so all four
variants share one body-size policy. If the cap needs to grow for
larger provider error JSON, change the constant once.

Invariants made structural by this shape:

- `RateLimited` always carries a `FinalResponse`. The 429 IS the
  acknowledged response that triggered the variant; there is no
  defensive arm.
- `RetryBudgetExhausted` carries `Option<FinalResponse>`. The
  defensive arm (no terminal response was ever received) is the
  `None` case. There is no `last_status: Some(_)` shape with
  missing headers or body - the status now lives inside
  `final_response`.
- `AuthLost` carries `Option<FinalResponse>`. The `Unsent` arm
  (pre-target token refresh failed permanently) has `None`. The
  `Acknowledged` arm (repeated target 401 after forced refresh) has
  `Some` so support staff get the response's WWW-Authenticate
  challenge, sub-error codes, and request IDs.

`request.rs:642-653` currently drops the response body before
constructing `RateLimited` / `RetryBudgetExhausted`; the repeated-
401 path similarly drops the response. The new shapes preserve all
three.

Existing public matches in protocol crates will break. That is
acceptable on the feature branch; the protocol crate plans update
their matches in Phase 2.2.

## Request pipeline changes

In `send_streaming_inner`:

- On `request.send().await` error, check in this exact order. Each
  earlier arm short-circuits later ones:
  1. `e.is_builder()` => the error is pre-wire local request
     construction (URL parse, header construction, body builder).
     Return `Error::InvalidRequest { field, detail }` - never a
     `Network` or `Timeout` variant. The conversion classifies these
     as `Request(Malformed)` with no `Attempt` cause.
  2. `native_tls_error_in_source_chain(&e)` => return
     `Error::Tls { transmission_state: Unsent, message }`. The
     handshake never completed; no HTTP bytes crossed.
  3. `e.is_timeout() && e.is_connect()` => `Timeout { Unsent }`.
  4. `e.is_timeout()` (and not `is_connect()`) => `Timeout { InFlight }`.
  5. `e.is_connect()` (and not `is_timeout()`) => `Network { Unsent }`.
  6. any other request-send error => `Network { InFlight }`.

  Arm 1 must run first because reqwest's `is_builder()` errors (URL
  parse, invalid header value, request body construction) can surface
  from `send().await` if `build_reqwest` constructed a deferred-
  validation builder. Without an explicit builder arm, these would
  fall through to arm 6 and be misclassified as
  `Network { InFlight }` - wrong on both axes (not a network error,
  and no bytes crossed the boundary).

  Arm 2 walks `e.source()` and downcasts to `native_tls::Error` (or
  the platform-specific TLS error type the workspace uses). A small
  helper:

  ```rust
  fn native_tls_error_in_source_chain(e: &reqwest::Error) -> bool {
      let mut current: Option<&(dyn std::error::Error + 'static)> = Some(e);
      while let Some(err) = current {
          if err.downcast_ref::<native_tls::Error>().is_some() {
              return true;
          }
          current = err.source();
      }
      false
  }
  ```

  Reqwest currently also reports TLS handshake failures as
  `is_connect() == true` (so arm 5 catches them as
  `Network { Unsent }`, which keeps state correct but loses the
  TLS-vs-network kind distinction). Routing TLS via source-chain
  inspection in arm 2 preserves the kind and survives reqwest
  updates that change the `is_connect()` reporting. The source-chain
  classifier is the contract; the comment near arm 5 is supporting
  rationale, not the primary mechanism.
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
   - `.protocol(ctx.protocol)` - always
   - `.operation(ctx.operation)` - always (the field is
     non-optional on `NetErrorContext`)
   - `.provider(provider)` when `ctx.provider` is `Some`
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

### Table shorthand expansion

Several rows in the mapping tables below use shorthand for verbosity.
Implementers must expand to the exact Phase 1 cause shapes:

- `Wire(MalformedResponse)` expands to:
  ```rust
  WireCause::MalformedResponse {
      protocol: ctx.protocol,
      detail: Some(DiagnosticText::support_only(format!("…"))),
  }
  ```
  Both fields are required by the landed type
  (`crates/types/src/error/cause.rs:388`); `protocol` is never
  omitted, `detail` is `Some` whenever any diagnostic text is
  available (typically the originating `Error` variant's `message`).
- `Request(InvalidArgument { field: Some("…") })` expands to:
  ```rust
  RequestCause::InvalidArgument {
      field: Some("…"),
      message: Some(DiagnosticText::support_only(format!("…"))),
  }
  ```
  `field` and `message` are both `Option` on the landed type
  (`crates/types/src/error/cause.rs:343`); set `message` whenever
  the underlying `Error::*` variant carries a `detail`/`message`
  string. Empty-string messages are treated as `None`.
- `Wire(Imap(...))`, `Wire(Smtp(...))`, etc. expand to
  `WireCause::Imap(...)`, `WireCause::Smtp(...)`, etc. - the
  `WireCause::` prefix is implicit.

These expansions are not optional. The builder's
`recovery::kind_matches_cause` accepts the kinds, but readers of
support exports and telemetry depend on the `protocol` and
`detail`/`message` slots being populated.

### Kind/cause shape asymmetry

The landed Phase 1 types use different shapes for kind vs cause on
the same conceptual signal:

- `ServerErrorKind::Error { status: Option<u16> }` (kind side; the
  status is optional because the kind classifies any server-side
  failure, even one without a numeric code).
- `ServerCause::Error { status: Option<u16> }` (cause side; after
  the Phase 1 amendment, the cause is also `Option<u16>` so the
  cause-side schema can represent IMAP `NO`/`BAD` without a numeric
  response code). `bifrost-net` operates over HTTP, so every
  net-side construction passes `Some(status)`; the `None` variant
  exists for protocols that lack numeric status (IMAP) and must not
  be invented in `bifrost-net`.

The table rows always write `Some(_)` on both kind and cause columns
for `bifrost-net` because the transport layer always has a numeric
HTTP status when it constructs `Server(Error)`. Other protocol
crates may emit `None` per their wire reality.

### Transport and auth

| net error | AccountErrorKind | primary cause | attempt cause |
|---|---|---|---|
| `Network { state: Unsent }` | `Transport(Network)` | `Transport(Network)` | `Attempt(Unsent)` |
| `Network { state: InFlight }` | `Transport(Network)` | `Transport(Network)` | `Attempt(InFlight)` |
| `Network { state: Acknowledged }` | `Protocol(PartialResponse)` | `Wire(MalformedResponse)` | `Attempt(Acknowledged)` |
| `Timeout { state: Unsent }` | `Transport(Timeout)` | `Transport(Timeout)` | `Attempt(Unsent)` |
| `Timeout { state: InFlight }` | `Transport(Timeout)` | `Transport(Timeout)` | `Attempt(InFlight)` |
| `Timeout { state: Acknowledged }` | `Protocol(PartialResponse)` | `Wire(MalformedResponse)` | `Attempt(Acknowledged)` |
| `Tls { Unsent }` | `Transport(Tls)` | `Transport(Tls)` | `Attempt(Unsent)` |
| `Tls { InFlight }` | `Transport(Tls)` | `Transport(Tls)` | `Attempt(InFlight)` |
| `Tls { Acknowledged }` | `Protocol(PartialResponse)` | `Wire(MalformedResponse { protocol, detail })` | `Attempt(Acknowledged)` |
| `AuthLost { transmission_state: None, final_response: None }` | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` | none |
| `AuthLost { transmission_state: Some(state), final_response: None }` | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` | `Attempt(state)` |
| `AuthLost { transmission_state: Some(Acknowledged), final_response: Some(r) }` | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` | `Attempt(Acknowledged)` - `r.headers` populate `request_id` / `trace_id`; `r.body` populates support-only diagnostic text |
| `RefreshFailed { retry_after, source }` | `Authentication(RefreshTransient)` | `Auth(RefreshTransient)` | none for the target request - if `retry_after` is `Some`, builder calls `.retry_not_before(deadline)` |

`Tls { Acknowledged }` deserves explicit treatment because
`into_account_error` is a public total conversion over `Error` and
must not panic on representable input. The producer-side rules above
say TLS = Unsent (handshake) or InFlight (mid-body) only; if the
producer somehow constructs `Tls { Acknowledged }` anyway, mapping it
to `Transport(Tls)` would trigger the `derive()` runtime assertion on
`Transport(_) + Acknowledged` in `bifrost-types::error::recovery`. The
defensive route - same as `Network { Acknowledged }` - is to classify
as `Protocol(PartialResponse)` with `Wire(MalformedResponse)` and
attach support-only diagnostic text noting the unexpected combination.
This keeps the conversion total and the bifrost-types invariant
intact.

For `RefreshFailed { retry_after, source }`, push one extra support
cause when possible: transport source errors become
`Cause::Transport`, server source errors become `Cause::Server`, and
auth source errors become `Cause::Auth`. Do not recursively call
`into_account_error`; that would attach the target operation context
to the token refresh request.

If `retry_after` is `Some(deadline)`, call
`AccountErrorBuilder::retry_not_before(deadline)` on the
`Authentication(RefreshTransient)` error. The auth layer owns the
extraction (`auth.rs` parses the OAuth `Retry-After` header and sets
the field when constructing `RefreshFailed`); the conversion layer
just reads it. Without the deadline, the central recovery mapper
produces `Retry { AfterAuthRefresh }` with no `not_before`
(`crates/types/src/error/recovery.rs:425`), and the engine
immediately retries the refresh against an endpoint that just told
us to wait. Pushing a `ServerCause` support cause is not sufficient
- `derive_auth` reads only the builder's `retry_not_before`, not
the chain.

### Local request and setup errors

| net error | AccountErrorKind | primary cause | attempt cause |
|---|---|---|---|
| `EncodeBody` | `Request(Malformed)` | `Request(InvalidArgument { field: Some("body") })` | none |
| `InvalidHeader` | `Request(Malformed)` | `Request(InvalidArgument { field: Some("header") })` | none |
| `InvalidRequest { field, detail }` | `Request(Malformed)` | `Request(InvalidArgument { field: Some(field), message })` | none |
| `CostExceedsBurst` | `Request(Malformed)` | `Request(InvalidArgument { field: Some("request_cost") })` | none |
| `RangeNotHonored { LocalInvalid }` | `Request(Malformed)` | `Request(InvalidArgument { field: Some("range") })` | none |
| `NetSetup` (legacy, retained for compat) | `Request(Malformed)` | `Request(InvalidArgument { field: Some("client_config"), message })` | none |
| `Cancelled` | `Transport(Network)` | `Transport(Network)` | `Attempt(InFlight)` |

`InvalidRequest` covers reqwest `is_builder()` errors from arm 1 of
the request-send ordering, the URL reparse failure at
`request.rs:536` (a local invariant - the URL parsed once for the
original request, so reparse failing is a producer bug), and any
future builder-style construction error. The `field` discriminator
maps to the `InvalidArgument.field` slot directly.

`NetSetup` is no longer constructed by the `Net::new()` paths
(those now use `InvalidRequest` per "Error evidence changes" above).
The variant is retained for backward compatibility in case any
external consumer matches on it; the conversion maps it as a
terminal config error, not as transport setup. If a future
`ClientBuilder::build()` source inspection identifies a transient
case worth retrying (e.g. system-cert store temporarily
unavailable), that subset can be split off into a real
`Transport(_)` variant, but the default for setup is terminal.

`Cancelled` is not currently constructed by the request pipeline.
When future code wires it up (futures dropped mid-send, structured
shutdown), the variant MUST carry conservative attempt evidence: an
attempt was initiated, its outcome is unknown, and bytes may have
crossed the boundary. Classify as `Attempt(InFlight)`, NOT as a
missing attempt cause. Reason: the central recovery mapper defaults
absent transmission state to `Unsent`
(`crates/types/src/error/recovery.rs:181`), which routes
`Transport(_) + Unsent` to `Retry { SameRequest }`. For non-idempotent
operations (`Send`, `BulkMove`, `AttachmentUpload`, …) that would
silently double-send. `Attempt(InFlight)` keeps the matrix safe by
routing non-idempotent cancellations to `Reconcile`. Add a Phase 2
exit-criterion note that any future producer constructing `Cancelled`
must attach `Attempt(InFlight)`.

### Redirect and range response errors

| net error | AccountErrorKind | primary cause | attempt cause |
|---|---|---|---|
| `RedirectRejected` | `Request(Malformed)` | `Request(InvalidArgument { field: Some("redirect_policy") })` | `Attempt(Acknowledged)` |
| `RedirectLoop` | `Protocol(ContractViolation)` | `Wire(MalformedResponse)` | `Attempt(Acknowledged)` |
| `MalformedRedirect { MissingLocation }` | `Protocol(ContractViolation)` | `Wire(MalformedResponse { protocol, detail })` | `Attempt(Acknowledged)` |
| `MalformedRedirect { InvalidLocationEncoding }` | `Protocol(ContractViolation)` | `Wire(MalformedResponse { protocol, detail })` | `Attempt(Acknowledged)` |
| `MalformedRedirect { UnresolvableLocation }` | `Protocol(ContractViolation)` | `Wire(MalformedResponse { protocol, detail })` | `Attempt(Acknowledged)` |
| `RangeNotHonored { ResponseNotPartial }` | `Protocol(ContractViolation)` | `Wire(MalformedResponse)` | `Attempt(Acknowledged)` |
| `RangeNotHonored { MissingContentRange }` | `Protocol(ContractViolation)` | `Wire(MalformedResponse)` | `Attempt(Acknowledged)` |
| `RangeNotHonored { ContentRangeMismatch }` | `Protocol(ContractViolation)` | `Wire(MalformedResponse)` | `Attempt(Acknowledged)` |

These errors all arise after an HTTP response was received. They are
not transport failures. The `MalformedRedirect` rows replace the
pre-Phase-2 behavior where `redirect.rs` constructed `Error::Network`
for missing / non-UTF-8 / unresolvable `Location` headers (see "Error
evidence changes" above for the new variant); the conversion would
otherwise misclassify these as `Protocol(PartialResponse)`.

### HTTP status and retry-budget errors

`Status { code, body, headers }` always carries
`Attempt(Acknowledged)`.

`RetryBudgetExhausted { final_response: Some(r), .. }` carries
`Attempt(Acknowledged)` and runs `r.status`, `r.headers`, and
`r.body` through the same `Status`-style classification path. The
preserved evidence feeds the same diagnostic accessors
(`request_id`, `trace_id`, support-only body text); without it,
protocol crates downstream of the retry budget lose the body
evidence they need for `error.code` / reason-string classification.

`RetryBudgetExhausted { final_response: None, .. }` is the defensive
arm (no terminal response ever received). Classify as
`Transport(Network)` with `Attempt(InFlight)` - same as the
`last_status: None` arm previously, but the missing-status shape is
now expressed structurally rather than as an
`Option<StatusCode>` field. See
`retry_budget_none_status_defensive_arm` for the pinned test.

`RateLimited { retry_after, final_response }` is equivalent to HTTP
429 with the response preserved. The 429 itself IS the
`final_response`, so there is no `None` shape for this variant.

| status | AccountErrorKind | primary cause |
|---|---|---|
| 400, 422 | `Request(Malformed)` | `Request(Malformed { detail })` |
| 401 | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` |
| 403 | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` |
| 404 with resource scope | `NotFound(resource)` | `Request(NotFound { what: resource, id })` |
| 404 without resource scope | `Server(Error { status: Some(404) })` | `Server(Error { status: Some(404) })` |
| 409 | `ConcurrencyConflict` | `State(ConcurrencyConflict)` |
| 410 with `ErrorScope::Cursor` | `SyncState(CursorInvalid)` | `State(CursorInvalid)` |
| 410 without cursor scope | `Server(Error { status: Some(410) })` | `Server(Error { status: Some(410) })` |
| 408, 502, 503, 504 | `Server(Unavailable)` | `Server(Unavailable { retry_after })` |
| 429 | `Server(RateLimited)` | `Server(RateLimited { retry_after })` |
| 507 | `Server(QuotaExhausted)` | `Server(QuotaExhausted { retry_after })` |
| other 500..=599 | `Server(Error { status: Some(code) })` | `Server(Error { status: Some(code) })` |
| other status | `Server(Error { status: Some(code) })` | `Server(Error { status: Some(code) })` |

Do not parse provider JSON bodies in this crate. Graph `error.code`,
Gmail reason strings, and JMAP problem details belong to the protocol
crate translation boundaries. `bifrost-net` only supplies the fallback
HTTP classification and diagnostics.

`RetryBudgetExhausted::final_response` is `Option<FinalResponse>`,
but the request pipeline only constructs the variant after a
retryable HTTP status with `Some(final_response)`. The `None` arm is
unreachable from current code. The conversion still has to handle it
because the type permits it: classify as `Transport(Network)` with
`Attempt(InFlight)`. Pin this with a test
(`retry_budget_none_status_defensive_arm`) so the arm cannot drift;
if a future net change makes `None` reachable, the test documents
the intended classification. Folding `last_status` into
`final_response` made the missing-status shape structurally
unrepresentable: there is no `Some(status) + None body` combination
to worry about.

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

These helpers MUST run on `Status`, `RateLimited`, and
`RetryBudgetExhausted` alike; the latter two now carry headers per
"Error evidence changes." Without that, repeated-401 / rate-limited /
budget-exhausted errors lose request IDs that support staff rely on
for cross-system correlation.

Request id candidates, first present wins:

- `x-request-id`
- `request-id`
- `x-ms-request-id`
- `x-goog-request-id`
- `x-guploader-uploadid`

Trace id candidates, first present wins:

- `traceparent`: parse per W3C Trace Context. The header format is
  `version-trace_id-parent_id-trace_flags` (e.g.
  `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`). Only
  the 32-hex-char `trace_id` segment goes into the `trace_id`
  diagnostic slot. The full header value belongs in support-only
  diagnostic text, not the trace_id slot, because telemetry
  dashboards key on the canonical trace_id.
- `x-cloud-trace-context`: parse `TRACE_ID/SPAN_ID;o=OPTIONS`; take
  the `TRACE_ID` portion before the `/`.

`x-ms-ags-diagnostic` is NOT a trace identifier. It is structured
diagnostic text from Graph (often a JSON blob with subsystem
identifiers); route to `DiagnosticText::support_only(...)`, not to
the trace_id slot.

Native code candidates:

- only stable header codes. Do not parse response bodies.
- It is fine for this helper to return `None` initially if no current
  net caller exposes a stable header-native code.

All response bodies copied into diagnostics are support-only text and
must use the already-capped `Status.body` / `final_body`.

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
- `tls_acknowledged_defensive_routes_to_protocol_partial_response`
  - `Tls { transmission_state: Acknowledged }` (synthetic)
  - asserts no panic; kind is `Protocol(PartialResponse)`; attempt
    state `Acknowledged`.
- `rate_limited_preserves_body_and_headers`
  - `RateLimited { retry_after: Some(...), final_status: 429,
    final_headers: { x-request-id, retry-after, x-ms-ags-diagnostic },
    final_body: <provider JSON snippet> }`
  - context `Protocol::Graph`
  - kind `Server(RateLimited)`
  - telemetry view exposes `request_id` and support-only body text;
    `trace_id` slot is empty (x-ms-ags-diagnostic does not populate it).
  - recovery retry advice has `ThrottleScope::Tenant`.
- `retry_budget_exhausted_preserves_body_and_headers`
  - `RetryBudgetExhausted { last_status: Some(503), retry_after_history,
    final_headers: Some(...), final_body: Some(...) }`
  - kind `Server(Unavailable)`
  - attempt state `Acknowledged`
  - support export includes the preserved body text.
- `retry_budget_none_status_defensive_arm`
  - `RetryBudgetExhausted { last_status: None, ... }`
  - kind `Transport(Network)`, attempt state `InFlight`.
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
- `invalid_request_is_client_bug_without_attempt`
  - `InvalidRequest { field: "url", detail }`
  - kind `Request(Malformed)`
  - chain carries `RequestCause::InvalidArgument { field: Some("url") }`
  - no transmission state in telemetry.
- `net_setup_legacy_routes_to_request_malformed`
  - `NetSetup { message }` (synthetic legacy value)
  - kind `Request(Malformed)`; not `Transport(_)`.
- `malformed_redirect_missing_location_is_contract_violation`
  - `MalformedRedirect { kind: MissingLocation, ... }`
  - kind `Protocol(ContractViolation)`
  - attempt state `Acknowledged`.
- `malformed_redirect_invalid_encoding_is_contract_violation`
  - same as above for `InvalidLocationEncoding`.
- `malformed_redirect_unresolvable_is_contract_violation`
  - same as above for `UnresolvableLocation`.
- `cancelled_inflight_non_idempotent_reconciles`
  - `Cancelled` (synthetic future-use value)
  - operation `Send`
  - chain carries `Attempt(InFlight)`
  - recovery requires reconciliation.
- `refresh_failed_is_auth_refresh_transient`
  - `RefreshFailed { source: Network { ... } }`
  - kind `Authentication(RefreshTransient)`
  - recovery retry disposition is `AfterAuthRefresh`.
- `refresh_failed_with_retry_after_propagates_deadline`
  - `RefreshFailed` whose source is a 429 with `Retry-After: 30`
  - resulting `AccountError.recovery()` is `Retry { not_before:
    Some(...), AfterAuthRefresh }`; the deadline matches the header.
- `range_response_mismatch_is_contract_violation`
  - `RangeNotHonored { kind: ContentRangeMismatch, ... }`
  - kind `Protocol(ContractViolation)`
  - attempt state `Acknowledged`.
- `traceparent_extracts_w3c_trace_id`
  - `Status` with header `traceparent: 00-<32hex>-<16hex>-01`
  - `AccountError.telemetry_fields().trace_id` is exactly the 32-hex
    `trace-id` segment; the `parent-id` and flags are not included.
- `x_ms_ags_diagnostic_is_support_only`
  - `Status` with header `x-ms-ags-diagnostic: { ... }`
  - `trace_id` is `None`; the diagnostic JSON is surfaced as
    support-only text via `support_consented()` / `support_internal()`.

Do not run these tests in Phase 2. They are authored now and execute
after Phase 3 restores workspace compilation.

## Exit criteria

- `NetErrorContext` and `into_account_error` exist and are re-exported
  from `bifrost_net`.
- `NetErrorContext.operation` is `AccountOperation` (non-optional).
  No call site passes `None`; the type system enforces this.
- The conversion uses `AccountErrorBuilder::build`; no net code
  constructs `RecoveryClass` or `RemediationAction` directly.
- Every current `Error` variant has an explicit mapping in this plan
  and in code, including the new `MalformedRedirect` and
  `InvalidRequest` variants.
- `crates/net/src/error.rs` defines `Error::InvalidRequest`,
  `Error::MalformedRedirect`, and the preserved-body/headers fields
  on `Error::RateLimited` and `Error::RetryBudgetExhausted`.
- `crates/net/src/redirect.rs` no longer constructs `Error::Network`
  for missing / non-UTF-8 / unresolvable `Location` headers; all three
  sites use `Error::MalformedRedirect` with the matching
  `MalformedRedirectKind`.
- `crates/net/src/request.rs` line 536 (URL reparse in the redirect
  path) no longer constructs `Error::Network`; it uses
  `Error::InvalidRequest { field: "url", detail }`.
- `crates/net/src/net.rs` `Net::new()` no longer constructs
  `Error::NetSetup` for cert DER encoding, reqwest DER rejection, or
  `ClientBuilder::build()` failure. All three sites use
  `Error::InvalidRequest` with `field` set to `"root_certs"` or
  `"client_config"`.
- `crates/net/src/request.rs` line 642-653 (retry-budget exhaustion)
  no longer drops the response body. `Error::RateLimited` and
  `Error::RetryBudgetExhausted` are constructed with `final_headers`
  and a capped `final_body`.
- `crates/net/src/auth.rs` parses `Retry-After` on OAuth 429/503
  responses and propagates the deadline so the
  `Authentication(RefreshTransient)` AccountError carries it via
  the builder's `retry_not_before(...)`.
- Target-attempt evidence is represented as `Cause::Attempt`.
- `into_account_error` is total: no `panic!`, `unwrap`, `expect`, or
  `unreachable!` on any representable `Error` value. `Tls { Acknowledged }`
  routes defensively to `Protocol(PartialResponse)` rather than
  triggering the bifrost-types `Transport(_) + Acknowledged` assertion.
- Transport failures never build `AccountErrorKind::Transport(_)`
  with `TransmissionState::Acknowledged`.
- Response-body failures map to `Protocol(PartialResponse)`.
- `Status`, `RateLimited`, and status-backed `RetryBudgetExhausted`
  add `Attempt(Acknowledged)`; the rate-limit and budget-exhausted
  paths populate `request_id` / `trace_id` from preserved headers
  and surface preserved body text via support-only diagnostics.
- Local validation and setup failures do not invent attempt causes.
  `is_builder()` errors from `request.send().await` route through arm
  1 of the pipeline ordering as `Request(Malformed)` with no
  `Attempt` cause. `Net::new()` config failures route to
  `Request(Malformed)` (no retry).
- `Cancelled` carries `Attempt(InFlight)` if/when constructed; the
  conversion never relies on absent-Attempt defaulting to `Unsent`
  for that variant.
- TLS handshake classification uses an `e.source()` chain probe for
  `native_tls::Error` (arm 2 of the request-send ordering) rather
  than relying solely on reqwest's `is_connect()` reporting. A
  helper `native_tls_error_in_source_chain` exists.
- `Retry-After` and throttle scope are populated for rate-limit,
  quota, and unavailable server classifications where data exists.
- `traceparent` is parsed to the 32-hex `trace-id` segment; the
  parent-id and flags are not stored in the `trace_id` slot.
  `x-ms-ags-diagnostic` is routed to support-only diagnostic text,
  not to the trace_id slot.
- Generic HTTP conversion does not parse Graph, Gmail, or JMAP JSON
  bodies. Structured provider body interpretation remains in the
  protocol crates; net's job is to PRESERVE the body so they can.
- Synthetic conversion tests are added under `crates/net/tests/`,
  including the new tests for `InvalidRequest`, `MalformedRedirect`
  (each kind), `Tls { Acknowledged }` defensive routing, `Cancelled`
  with `Attempt(InFlight)`, body/headers preservation on rate-limit
  and budget-exhausted, traceparent parsing, x-ms-ags-diagnostic
  placement, `RefreshFailed` `Retry-After` propagation, and the
  named `retry_budget_none_status_defensive_arm`.
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

- Confirm every builder path calls `.protocol(ctx.protocol)` and
  `.operation(ctx.operation)`.
- Confirm `NetErrorContext.operation` is `AccountOperation`, not
  `Option<AccountOperation>`, in both the type definition and every
  call-site construction.
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
