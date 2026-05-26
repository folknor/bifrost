# Error model: bifrost-jmap implementation plan

This is Phase 2.2 of `plans/error-model-roadmap.md`, one of the five
consumer-crate patches authored after `bifrost-net`.

Phase 2 is still on the intentionally broken feature branch. Author
the JMAP patch and tests, but do not run `brokkr`, `cargo`, or
`./diff_test.sh` from the crate agent.

## Required reading

- `CLAUDE.md`
- `plans/error-model-roadmap.md`
- `plans/error-model-convergence.md`
- `plans/error-model-net.md`
- `reference/jmap.md`
- `crates/types/src/error/` (the landed Phase 1 API)

## Scope

Own only `crates/jmap/` files. Do not edit `crates/types/` even if a
new `JmapMethod` wire variant would be useful. If the existing
`JmapMethod` enum is insufficient, report the required variants to the
orchestrator under the roadmap's wire-enum escape hatch.

This phase rewrites JMAP error construction and removes the crate-local
recovery mapping. Phase 3 may still perform final trait-surface
reconciliation across all crates (`SyncEvent::Fatal` rename,
`MutationResult` to `ItemOutcome`, and `Account` signature updates).

## Current state

Important files:

- `crates/jmap/src/lib.rs`
  - crate-private `Error` enum.
  - `From<TransportError>` auto-parses RFC 7807 problem details.
  - `From<serde_json::Error>` currently flattens all JSON failures to
    `Error::Parse`, losing whether the failure was outbound request
    encoding or inbound response decoding.
- `crates/jmap/src/core/error.rs`
  - `ProblemDetails`, `JMAPError`, `ProblemType`,
    `MethodError`, `MethodErrorType`.
- `crates/jmap/src/core/set.rs`
  - `SetError` and `SetErrorType` for per-object set failures.
- `crates/jmap/src/core/transport.rs`
  - `TransportError`, currently only message/body/source.
- `crates/jmap/src/transport_reqwest.rs`
  - converts `bifrost_net::Error` into `TransportError`.
- `crates/jmap/src/client.rs`
  - request send, session fetch, session refresh.
- `crates/jmap/src/client_ws.rs`
  - WebSocket setup, message parsing, request-error problem details.
- `crates/jmap/src/sync/error.rs`
  - old `to_recovery`, `to_account_error`,
    `fatal_from_account_error`, and `fatal_from_jmap`.
- `crates/jmap/src/sync/*`
  - many `map_err(super::error::to_account_error)` and
    `fatal_from_jmap` call sites that must pass operation and scope
    context.

## Files to modify

- `crates/jmap/src/lib.rs`
  - Adjust `Error` variants as described below.
- `crates/jmap/src/core/error.rs`
  - Add accessors needed by the mapper if missing.
- `crates/jmap/src/core/transport.rs`
  - Preserve the original `bifrost_net::Error` when the default
    transport produced one.
- `crates/jmap/src/core/request.rs`
  - Map request serialization failures to an outbound-encode error.
- `crates/jmap/src/core/response.rs`
  - Preserve method-error metadata already parsed there.
- `crates/jmap/src/core/set.rs`
  - Add cheap accessors / conversion helpers for `SetErrorType`.
- `crates/jmap/src/transport_reqwest.rs`
  - Preserve net error evidence and response body at the same time.
- `crates/jmap/src/client.rs`
  - Split request encode, session decode, and response decode errors.
- `crates/jmap/src/client_ws.rs`
  - Map WebSocket setup and message decode errors into the new
    conversion boundary.
- `crates/jmap/src/sync/error.rs`
  - Replace the old recovery helpers with the builder-based
    conversion boundary.
- `crates/jmap/src/sync/*.rs`
  - Pass `JmapErrorContext` at all crate-error conversion sites.
- `crates/jmap/src/sync/tests` or inline `sync/error.rs` tests
  - Add small deterministic classification tests.

## Functions to delete

No whole-file deletion is expected. The following functions in
`crates/jmap/src/sync/error.rs` must be removed in the same commit
that lands `into_account_error`:

- `to_recovery`
- `to_account_error`
- `fatal_from_account_error`
- `fatal_from_jmap`

Keep `is_state_mismatch` only if the mutation retry pipeline still
needs the predicate. It must inspect `MethodErrorType::StateMismatch`
directly and must not construct recovery advice.

The exit-criteria audit must grep `crates/jmap/` for these four
identifiers and confirm zero hits.

## Internal error-shape changes

### Preserve net evidence

`TransportError` must retain the original net error:

```rust
pub(crate) struct TransportError {
    pub(crate) message: String,
    pub(crate) body: Option<Bytes>,
    pub(crate) net: Option<bifrost_net::Error>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}
```

`transport_reqwest::transport_error_from_net` should set `net:
Some(error)`. For `bifrost_net::Error::Status`, clone the `Bytes`
body before moving the error into the field so `ProblemDetails`
parsing still works.

Custom transports can keep using `TransportError::new`,
`with_source`, and `with_body`; those constructors set `net: None`.

### Preserve problem transport metadata

`Error::Problem(Box<ProblemDetails>)` loses the HTTP transport
evidence. Replace it with:

```rust
Problem {
    details: Box<ProblemDetails>,
    transport: Option<TransportError>,
}
```

`From<TransportError> for Error`:

- if `body` parses as `ProblemDetails`, return
  `Error::Problem { details, transport: Some(error) }`.
- otherwise return `Error::Transport(error)`.

`From<ProblemDetails> for Error` for WebSocket request errors returns
`Error::Problem { details, transport: None }`.

### Split JSON encode and decode

Current `Error::Parse(serde_json::Error)` is ambiguous. Add:

```rust
RequestEncode(serde_json::Error),
ResponseDecode(serde_json::Error),
```

Then update:

- `Request::call` and `Client::send_request` request serialization:
  `RequestEncode`.
- `ClientBuilder::connect`, `Client::send_request`,
  `Client::refresh_session`, and WebSocket response decoding:
  `ResponseDecode`.

Remove the broad `From<serde_json::Error> for Error` if possible. If
keeping it is necessary for localized call-site churn, it must map to
`ResponseDecode` and the plan's tests must cover the explicit
outbound-encode sites.

## Kind/cause shape conventions

Two landed-API quirks the agent must keep straight when reading the
tables below:

- `ServerErrorKind::Error { status: Option<u16> }` (kind side) vs
  `ServerCause::Error { status: Option<u16> }` (cause side). Both
  carry `Option<u16>` after the Phase 1 amendment. JMAP runs over
  HTTP and always carries a numeric status, so every JMAP-side
  construction passes `Some(status)` on both sides. The `None`
  variant exists for protocols that lack numeric status (IMAP) and
  must not be invented here as a sentinel.
- `AccessErrorKind::PermissionDenied` (kind side; no payload) vs
  `AccessCause::PermissionDenied { resource: Option<ResourceKind> }`
  (cause side; carries optional resource). The `resource` value lives
  only on the cause, not the kind.

Every kind/cause pair below must satisfy
`recovery::kind_matches_cause` (asserted at runtime in
`AccountErrorBuilder::build`). Adding rows requires checking the
matrix in `crates/types/src/error/recovery.rs`.

## JMAP conversion boundary

Add in `sync/error.rs`:

```rust
#[derive(Clone, Debug)]
pub(crate) struct JmapErrorContext {
    pub(crate) provider: Option<Provider>,
    pub(crate) operation: AccountOperation,
    pub(crate) scope: Option<ErrorScope>,
}

pub(crate) fn into_account_error(
    error: crate::Error,
    ctx: JmapErrorContext,
) -> AccountError;
```

`operation` is non-optional. The central recovery mapper defaults
missing operation to idempotent-true
(`crates/types/src/error/recovery.rs:181-183`), which on a
`Network { InFlight }` for a `Send` would silently retry and
double-send. Forcing every JMAP call site to supply its target
operation closes the trap at this boundary; this mirrors
`bifrost_net::NetErrorContext` (Phase 2.1 net plan).

Helper constructors are encouraged:

```rust
impl JmapErrorContext {
    pub(crate) fn new(operation: AccountOperation) -> Self;
    pub(crate) fn with_scope(self, scope: ErrorScope) -> Self;
    pub(crate) fn cursor(operation: AccountOperation, scope: CursorScope) -> Self;
    pub(crate) fn message(operation: AccountOperation, id: impl Into<String>) -> Self;
    pub(crate) fn mailbox(operation: AccountOperation, id: impl Into<String>) -> Self;
}
```

Every builder path must attach:

- `.protocol(Protocol::Jmap)` - always
- `.operation(ctx.operation)` - always
- `.provider(provider)` only when `ctx.provider` is `Some`
- `.scope(scope)` when `ctx.scope` is `Some`

Do not infer `Provider` from the URL. Generic JMAP hosts should leave
`provider` as `None` unless the factory later gains an explicit
provider setting.

## Transport conversion

For `Error::Transport(transport)`:

- If `transport.net` is `Some(net_error)`, delegate to:

```rust
bifrost_net::into_account_error(
    net_error,
    bifrost_net::NetErrorContext {
        provider: ctx.provider,
        protocol: Protocol::Jmap,
        operation: ctx.operation, // non-optional on both sides
        scope: ctx.scope,
    },
)
```

- If `transport.net` is `None`, build:
  - `AccountErrorKind::Transport(TransportErrorKind::Network)`
  - `Cause::Transport(TransportCause { kind: Network, message })`
  - no `AttemptCause`, because custom transports did not provide
    side-effect evidence.

## Method-error mapping

JMAP method errors are parsed successful JMAP responses, so they have
crossed the HTTP side-effect boundary. Push
`Cause::Attempt(AttemptCause { transmission_state:
TransmissionState::Acknowledged })` for every `Error::Method` path.

The outermost cause must match the kind. Push the JMAP wire cause after
the semantic cause:

```rust
builder.push_cause(Cause::Wire(WireCause::Jmap(...)))
```

| MethodErrorType | AccountErrorKind | outer cause |
|---|---|---|
| `ServerUnavailable` | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` |
| `ServerFail` | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` |
| `ServerPartialFail` | `Protocol(PartialResponse)` | `Wire(Jmap(ServerPartialFail))` |
| `UnknownMethod` | `Unsupported(operation)` | `Request(Unsupported { operation })` |
| `InvalidArguments` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `InvalidResultReference` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `Forbidden` | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` |
| `AccountNotFound` | `SyncState(CapabilityChanged)` | `State(CapabilityChanged { delta: default })` |
| `FromAccountNotFound` | `SyncState(CapabilityChanged)` | `State(CapabilityChanged { delta: default })` |
| `AccountNotSupportedByMethod` | `SyncState(CapabilityChanged)` | `State(CapabilityChanged { delta: default })` |
| `FromAccountNotSupportedByMethod` | `SyncState(CapabilityChanged)` | `State(CapabilityChanged { delta: default })` |
| `AccountReadOnly` | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` |
| `RequestTooLarge` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `CannotCalculateChanges` | `SyncState(CursorInvalid)` | `State(CursorInvalid)` |
| `StateMismatch` | `ConcurrencyConflict` | `State(ConcurrencyConflict)` |
| `AlreadyExists` | `ConcurrencyConflict` | `State(ConcurrencyConflict)` |
| `AnchorNotFound` | `SyncState(CursorInvalid)` | `State(CursorInvalid)` |
| `UnsupportedSort` | `Unsupported(operation)` | `Request(Unsupported { operation })` |
| `UnsupportedFilter` | `Unsupported(operation)` | `Request(Unsupported { operation })` |
| `TooManyChanges` | `SyncState(CursorInvalid)` | `State(CursorInvalid)` |
| `Other` | `Protocol(Unknown)` | `Wire(Jmap(Unknown { code }))` - see note |

For `Unsupported(operation)`, if `ctx.operation` is `None`, use
`AccountOperation::Discover` only as a defensive fallback and add
support-only diagnostic text explaining that operation context was
missing. The audit should find no normal path that needs the fallback.

`MethodErrorType::Other` today is a unit variant constructed via
`#[serde(other)]`, which DISCARDS the wire-supplied type string. Do
not synthesize a placeholder code (e.g. `"other"`) - that pollutes
`AccountError::native_code()` and the telemetry view with a value the
server never sent. Pick one of:

1. Update `crates/jmap/src/core/error.rs` so `MethodErrorType::Other`
   carries the captured string (use a custom `Deserialize` impl with
   a typed-variants-first match and a final string fallback; serde's
   `#[serde(other)]` does not capture the unknown value). Then map to
   `Wire(Jmap(Unknown { code }))`.
2. If (1) is rejected as out-of-scope, OMIT the `Wire(Jmap(_))` cause
   for the `Other` row and let `Protocol(Unknown)` stand on its own
   with `Wire(MalformedResponse)` if any provider text is available.

Recommendation: (1). The change is local to `core/error.rs` and
preserves diagnostic value; the alternative leaves a permanent gap in
JMAP wire forensics.

`Forbidden` and `AccountReadOnly` should use
`AccessCause::PermissionDenied { resource }`, where `resource` is
derived from `ctx.scope` when possible.

`CannotCalculateChanges`, `AnchorNotFound`, and `TooManyChanges` need
cursor scope for precise recovery. If `ctx.scope` is not an
`ErrorScope::Cursor`, the central mapper will restart the account.
That is acceptable only as a fallback; call sites in changes/query
streams must pass cursor scope.

`State(CapabilityChanged { delta })` rationale: the four
`AccountNotFound` / `AccountNotSupportedByMethod` / `FromAccountNotFound`
/ `FromAccountNotSupportedByMethod` rows and the `UnknownCapability`
problem-type row all map to `SyncState(CapabilityChanged)` with
`delta: CapabilityDelta::default()`. The JMAP crate does not compute
a real delta - only the engine, comparing two `Capabilities` values
across an `Account` reopen, can. Convergence §"Open decisions"
flags `CapabilityChanged { delta }` as a candidate for removal if no
producer ever computes a real delta. Until that decision lands in
`bifrost-types`, JMAP populates `delta: default()` and the engine's
`EngineDirective::CapabilityChanged { delta }` handler treats a
default delta as "reopen and resync capabilities" (per sync.md). If
Phase 1 later replaces `CapabilityChanged` with `RestartAccount`, the
JMAP rows fall back to that variant by the same central mapping; no
JMAP code change is required.

## Problem-details mapping

`Error::Problem { details, transport }` can come from an HTTP
problem-details response or from a WebSocket `RequestError`.

If `transport.net` is present, inspect the preserved net error
directly:

- `Status { code, headers, .. }`: copy status, request-id and trace
  headers, and parse `Retry-After` if present.
- `RateLimited { retry_after, final_response }`: copy the retry delay
  and throttle scope from JMAP context; pull `request_id` / `trace_id`
  from `final_response.headers` and support-only body text from
  `final_response.body` (capped at `STATUS_BODY_CAP`). Do not match
  the legacy `{ retry_after }`-only shape.
- `RetryBudgetExhausted { final_response, retry_after_history }`:
  `final_response` is `Option<FinalResponse>`; on `Some`, run the
  same headers/body extraction as `Status` (the status moved from
  the top-level `last_status` into `final_response.status`). On
  `None` (defensive arm) classify as transport with
  `Attempt(InFlight)`.
- Other net errors: keep the problem body as the semantic source and
  add the net error text as support-only diagnostic text.

Do not delegate the entire problem to net, because JMAP problem type
is more precise than generic HTTP status.

Always push `Cause::Attempt(Acknowledged)` for problem details that
came from HTTP or WebSocket server response evidence.

| ProblemType | status | AccountErrorKind | outer cause |
|---|---|---|---|
| `JMAP(Limit)` | any | `Server(RateLimited)` | `Server(RateLimited { retry_after: None })` |
| `JMAP(UnknownCapability)` | any | `SyncState(CapabilityChanged)` | `State(CapabilityChanged { delta: default })` |
| `JMAP(NotJSON)` | any | `Request(Malformed)` | `Request(Malformed { detail })` |
| `JMAP(NotRequest)` | any | `Request(Malformed)` | `Request(Malformed { detail })` |
| `Other(_)` | 400 or 422 | `Request(Malformed)` | `Request(Malformed { detail })` |
| `Other(_)` | 401 | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` |
| `Other(_)` | 403 | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` |
| `Other(_)` | 404 with resource scope | `NotFound(resource)` | `Request(NotFound { what, id })` |
| `Other(_)` | 409 | `ConcurrencyConflict` | `State(ConcurrencyConflict)` |
| `Other(_)` | 410 with cursor scope | `SyncState(CursorInvalid)` | `State(CursorInvalid)` |
| `Other(_)` | 429 | `Server(RateLimited)` | `Server(RateLimited { retry_after })` |
| `Other(_)` | 500..=599 | `Server(Unavailable)` | `Server(Unavailable { retry_after })` |
| `Other(_)` | other | `Server(Error { status: Some(status) })` | `Server(Error { status })` |
| `Other(_)` | none | `Protocol(Unknown)` | `Wire(Jmap(Unknown { code }))` |

Attach `ProblemDetails::request_id()` with `.request_id(...)`.
Attach `title`, `detail`, and `limit` as support-only
`DiagnosticText`. The problem type itself is the JMAP wire cause:

- `UnknownCapability` -> `JmapMethod::UnknownCapability`
- `NotJSON` -> `JmapMethod::NotJson`
- `NotRequest` -> `JmapMethod::NotRequest`
- `Limit` -> `JmapMethod::Limit`
- `Other(code)` -> `JmapMethod::Unknown { code }`

For JMAP limit errors, set `ThrottleScope::Account`. Generic JMAP
does not provide a more precise fleet-wide throttle domain.

## Set-error mapping

`SetError` is per-object evidence. Do not flatten it through
`SetError::to_string_error()` and then through a generic
`crate::Error::Set`.

Add a helper:

```rust
pub(crate) fn set_error_to_account_error(
    error: SetError<String>,
    ctx: JmapErrorContext,
    item_scope: Option<ErrorScope>,
) -> AccountError;
```

If `item_scope` is present, prefer it over `ctx.scope` for not-found
and permission resource classification.

Push `Attempt(Acknowledged)` and `Wire(Jmap(Unknown { code }))` for
the set-error string unless the orchestrator adds named `JmapMethod`
variants for set errors.

| SetErrorType | AccountErrorKind |
|---|---|
| `Forbidden`, `ForbiddenFrom`, `ForbiddenMailFrom`, `ForbiddenToSend` | `Authorization(PermissionDenied)` |
| `OverQuota` | `Server(QuotaExhausted)` |
| `RateLimit` | `Server(RateLimited)` |
| `NotFound`, `BlobNotFound` | `NotFound(resource)` when item scope has a resource, otherwise `Server(Error { status: None })` |
| `AlreadyExists` | `ConcurrencyConflict` |
| `TooLarge`, `TooManyKeywords`, `TooManyMailboxes`, `TooManyRecipients` | `Request(Malformed)` |
| `InvalidPatch`, `InvalidProperties`, `InvalidEmail`, `InvalidRecipients`, `NoRecipients`, `InvalidScript` | `Request(Malformed)` |
| `WillDestroy`, `Singleton`, `ScriptIsActive`, `CannotUnsend`, `MailboxHasChild`, `MailboxHasEmail` | `Server(Error { status: None })` |
| `Other` | `Protocol(Unknown)` |

NotFound absorption policy:

- Bulk flag, bulk move source missing, and bulk destroy per-item
  `notFound` must become a success-lane `MutationSuccess::Skipped`
  in the final streaming model.
- Single-object hydrate/get paths surface `NotFound`.
- Container delete failures such as `mailboxHasEmail` are not
  absorbed.

Phase 2 vs Phase 3 split for the bulk-streaming signature: JMAP's
existing `MutationOutcome::Failed(Error::ConcurrencyConflict)` pattern
must be replaced by `ItemOutcome<MutationSuccess>`. Phase 2 authors
the per-item classification helper:

```rust
pub(crate) fn classify_set_item(
    set_error: SetError<String>,
    ctx: JmapErrorContext,
    item_scope: Option<ErrorScope>,
) -> ItemOutcome<MutationSuccess>;
```

which returns `Succeeded(BatchSuccess { item, output: Skipped })` for
absorbed notFound and `Failed(BatchFailure { item, error })` for real
failures. The Phase 2 commit wires this helper into existing
mutation pipeline call sites that today produce
`MutationOutcome::Failed`. The Phase 3 commit changes the `Account`
trait signature itself; JMAP's helper does not move.

This is in scope for Phase 2.2 (JMAP), not deferred to Phase 3.

## Local JMAP errors

Map non-wire crate errors without old `bifrost_types::Error`:

| crate error | AccountErrorKind | outer cause |
|---|---|---|
| `RequestEncode` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `ResponseDecode` | `Protocol(ParseFailed)` | `Wire(MalformedResponse { protocol: Jmap, detail })` |
| `CallNotFound` | `Protocol(MissingField)` | `Wire(MalformedResponse { protocol: Jmap, detail })` |
| `IdNotFound` with item scope | `NotFound(resource)` | `Request(NotFound { what, id })` |
| `IdNotFound` without item scope | `Protocol(MissingField)` | `Wire(MalformedResponse { protocol: Jmap, detail })` |
| `NotParsable` | `Protocol(ParseFailed)` | `Wire(MalformedResponse { protocol: Jmap, detail })` |
| `InvalidUrl` before request dispatch | `Request(Malformed)` | `Request(InvalidArgument { field: Some("url") })` |
| `InvalidUrl` for URL templates from session | `Protocol(ContractViolation)` | `Wire(MalformedResponse { protocol: Jmap, detail })` |
| `NoPrimaryAccount` | `SyncState(CapabilityChanged)` | `State(CapabilityChanged { delta: default })` |
| `WebSocketClosed` after established session | `Protocol(PartialResponse)` | `Wire(MalformedResponse { protocol: Jmap, detail })` + `Attempt(Acknowledged)` |
| `WebSocketClosed` during handshake | `Transport(Network)` | `Transport(Network)` + `Attempt(Unsent)` |
| `WebSocketNotConnected` | `Unsupported(PushSubscribe)` or `Unsupported(PushUnsubscribe)` | `Request(Unsupported { operation })` |
| `WebSocketSetup(Tls)` | `Transport(Tls)` | `Transport(Tls)` + `Attempt(Unsent)` |
| `WebSocketSetup(InvalidHeader)` | `Request(Malformed)` | `Request(InvalidArgument { field: Some("authorization") })` |
| `WebSocketSetup(Subprotocol)` | `SyncState(CapabilityChanged)` | `State(CapabilityChanged { delta: default })` |
| `WebSocket(_)` mid-message after handshake completed | `Protocol(PartialResponse)` | `Wire(MalformedResponse { protocol: Jmap, detail })` + `Attempt(Acknowledged)` |
| `WebSocket(_)` before handshake completed | `Transport(Network)` | `Transport(Network)` + `Attempt(Unsent)` |

WebSocket lifecycle rule. The HTTP upgrade completes when the server's
`101 Switching Protocols` response is received. The two phases require
different kinds, not just different attempt states:

- **Before handshake** (TCP refused, TLS handshake failure, HTTP
  upgrade rejected): no acknowledged response, no session. Classify
  as `Transport(Network)` (or `Transport(Tls)` for handshake-time TLS)
  with `Attempt(Unsent)`. Matches the rule "handshake failure is
  always Unsent."
- **After handshake** (post-`101` session interrupted, mid-message
  disconnect, ping timeout, server-initiated close): the upgrade was
  acknowledged, but the long-lived stream was interrupted. Classify
  as `Protocol(PartialResponse)` with
  `Wire(MalformedResponse { protocol: Jmap, detail })` and
  `Attempt(Acknowledged)`. Do NOT classify as `Transport(_)` with
  `Acknowledged` - that combination triggers the
  `bifrost-types::error::recovery::derive` runtime assertion
  (`recovery.rs:187-190`: `Transport(_)` errors cannot have
  `Acknowledged` transmission state). The defensive route mirrors
  net's `Network { Acknowledged }` and `Tls { Acknowledged }`
  treatment: once response headers / upgrade have been acknowledged,
  subsequent stream failures are protocol-class, not transport-class.

Recovery derivation for the `Protocol(PartialResponse)` rows: the
central mapper uses operation idempotency. `PushSubscribe` is
non-idempotent (per `scope.rs:163-182`), so a mid-session close on a
subscribe call returns `Reconcile { PartialCompletionSignal,
[CheckTarget, DedupeByClientId] }` - the engine probes inventory or
re-subscribes after checking that the subscription is gone.
`PushStream` is idempotent, so a stream interruption returns
`Retry { SameRequest, Transport }` - the engine reconnects the
stream. Both behaviors are correct for "we don't have a transport
problem, the long-lived stream just ended."

For `InvalidUrl`, prefer adding distinct variants or constructors so
the conversion can tell local caller-built URL failures from provider
session URL-template failures. If the implementation keeps one
variant, map to `Protocol(ContractViolation)` only when the failing
path parsed a URL template received from the session object.

## Account-operation context

Every `map_err(super::error::to_account_error)` site must become
`map_err(|err| super::error::into_account_error(err, ctx))` with a
real operation. Use this table:

| module / function | AccountOperation | scope |
|---|---|---|
| factory connect/session/primary accounts | `Discover` | `Account` |
| factory initial state probes | `EstablishCursor` | cursor scope being seeded |
| `discover::memberships` | `DiscoverMemberships` | `Account` |
| `discover::scope_lifecycle` | `ScopeLifecycle` | `Cursor(Mailbox)` where possible |
| inventory email/mailbox/thread/query | `SyncInventory` | `Cursor(scope)` |
| inventory partition page | `SyncInventory` | `Cursor(scope)` |
| changes email/mailbox/thread/query | `SyncChanges` | `Cursor(scope)` |
| hydrate stream | `Hydrate` | message scope when an id is known |
| blob open | `OpenBlob` | `Message` only if handle has message context, otherwise `Account` |
| blob range unsupported/local range | `OpenBlobRange` | `Account` |
| push subscribe | `PushSubscribe` | `Cursor(scope)` when one scope, otherwise `Account` |
| push unsubscribe | `PushUnsubscribe` | `Account` |
| close/disabling push | `Close` | `Account` |
| bulk flags | `UpdateFlags` | per item `Message` for item errors, otherwise `Account` |
| bulk move | `BulkMove` | per item `Message` for item errors, otherwise destination mailbox |
| bulk destroy | `BulkDestroy` | per item `Message` |
| add/remove container | `AddToContainer` / `RemoveFromContainer` | target message or thread |
| set keyword | `SetKeyword` | target message or thread |
| set read state | `SetIsRead` | target message or thread |
| send message | `Send` | `Account` |
| attachment upload | `AttachmentUpload` | `Account` |
| draft create/update/discard/send | matching draft operation | `Account` |
| search/search_messages | `Search` / `SearchMessages` | `Account` |
| containers list/create/rename/move/delete | matching container operation | mailbox scope when id known |
| identities list/update | `IdentitiesList` / `IdentityUpdate` | `Account` |
| vacation get/set | `VacationGet` / `VacationSet` | `Account` |
| quota get | `QuotaGet` | `Account` |
| thread/message hydrate | `HydrateThread` / `HydrateMessage` | thread or message scope |
| move_thread/delete_thread | `BulkMove` / `BulkDestroy` | thread scope |

Unsupported convenience methods in `account.rs` must construct
`AccountErrorKind::Unsupported(operation)` through the builder rather
than returning the removed old `Error::Unsupported`.

## Stream termination helpers

Replace fatal helpers with AccountError helpers:

- `unsupported_error(operation, scope, message) -> AccountError`
- `local_cursor_error(err, scope, operation) -> AccountError`
- `terminated_from_jmap(err, ctx) -> SyncEvent<T>` or the temporary
  Phase 2 equivalent agreed with the orchestrator.

Do not construct `RecoveryClass` in JMAP. The only recovery-related
code that may remain is predicate logic such as
`is_state_mismatch(&crate::Error)`.

When Phase 3 renames stream termination, each helper should collapse
to:

```rust
SyncEvent::Terminated(account_error)
```

### WebSocket push reader task

`crates/jmap/src/client_ws.rs` runs a reader task that emits events
on a broadcast channel rather than at a method call site. Errors
inside that loop (decode failure, server-initiated close after
session establishment, ping timeout) terminate the push stream, not
a method call. Phase 2 wires them as follows:

- The reader's error path constructs an `AccountError` via the
  `JmapErrorContext` with `operation = PushStream`, `scope = Account`,
  using the tables above.
- The reader emits the error onto the push stream as the Phase 2
  temporary equivalent of `SyncEvent::Terminated(AccountError)` (the
  exact wrapper type is the same one Phase 2.3 sync renames in
  Phase 3; JMAP's reader uses the local stand-in that Phase 3 will
  rewrite to `SyncEvent::Terminated`).
- After the error event, the reader task exits. Engine receives the
  terminated event and decides whether to call `push_subscribe`
  again - JMAP does not retry inside the reader.

Disconnect-during-handshake errors (before the `101 Switching
Protocols` response) emit on the `push_subscribe()` call return path,
not on the broadcast channel - the subscribe future has not yet
yielded a stream when handshake fails.

## Tests

Add small unit tests under `sync/error.rs` or a focused sync test
module. Construct synthetic `crate::Error` values directly. No live
JMAP server and no mock server.

Cover at least:

- `state_mismatch_maps_to_concurrency_conflict`
  - kind `ConcurrencyConflict`
  - recovery retry disposition `AfterStateRefresh`
  - wire cause `Jmap(StateMismatch)`.
- `cannot_calculate_changes_restarts_scope`
  - context cursor scope
  - kind `SyncState(CursorInvalid)`
  - recovery requires engine action.
- `cannot_calculate_changes_without_scope_restarts_account`
  - same error without cursor scope
  - recovery requires engine action.
- `server_partial_fail_reconciles_for_send`
  - operation `Send`
  - kind `Protocol(PartialResponse)`
  - recovery requires reconciliation.
- `server_partial_fail_retries_for_idempotent_update`
  - operation `UpdateFlags`
  - recovery is retryable.
- `forbidden_maps_to_no_permission`
  - kind `Authorization(PermissionDenied)`
  - recovery terminal `NoPermission`.
- `unknown_method_maps_to_unsupported_operation`
  - kind `Unsupported(operation)`.
- `jmap_limit_problem_maps_rate_limited`
  - kind `Server(RateLimited)`
  - throttle scope `Account`.
- `problem_status_401_maps_auth_lost`
  - status 401
  - kind `Authentication(ReauthorizationRequired)`.
- `problem_not_json_maps_request_malformed`
  - JMAP notJSON
  - kind `Request(Malformed)`.
- `net_transport_delegates_with_context`
  - synthetic preserved net `Network { InFlight }`
  - operation `Send`
  - recovery requires reconciliation.
- `response_decode_maps_protocol_parse_failed`
  - kind `Protocol(ParseFailed)`.
- `set_not_found_with_message_scope_maps_not_found`
  - kind `NotFound(Message)`.
- `set_rate_limit_maps_rate_limited`
  - kind `Server(RateLimited)`.
- `websocket_subprotocol_maps_capability_changed`
  - kind `SyncState(CapabilityChanged)`.

Do not run these tests in Phase 2. They execute after Phase 3 restores
workspace compilation.

## Exit criteria

- `JmapErrorContext` and `into_account_error` exist.
- Every `crate::Error` variant is mapped to `AccountError`.
- Every `MethodErrorType` variant is mapped.
- Every `JMAPError` problem type is mapped.
- Every `SetErrorType` variant is mapped or explicitly routed through
  `Protocol(Unknown)`.
- The default reqwest transport preserves `bifrost_net::Error` so
  pure transport failures delegate to `bifrost_net::into_account_error`.
- JMAP method errors and problem details push `Attempt(Acknowledged)`.
- No JMAP code constructs `RecoveryClass`, `RetryAdvice`, or old
  `Fatal` structs directly.
- `to_recovery`, `to_account_error`, and old fatal helper logic are
  gone.
- Unsupported operations use `AccountErrorBuilder` with
  `AccountErrorKind::Unsupported(operation)`.
- Operation and scope context is attached at conversion call sites.
- Response decode, request encode, and URL-template failures are not
  collapsed into one generic parse bucket.
- Every kind/cause pair produced by `into_account_error` satisfies
  `recovery::kind_matches_cause`.
- WebSocket reader-task errors emit through the same per-context
  `into_account_error` path as call-site errors, with the correct
  handshake-vs-post-handshake `AttemptCause` distinction.
- `classify_set_item` returns `ItemOutcome<MutationSuccess>` and is
  wired into every existing `MutationOutcome::Failed(...)` call site.
- Synthetic classification tests are authored.
- No `brokkr`, `cargo`, or `./diff_test.sh` command is run by the
  crate agent for this phase.

## Audit checklist

Domain-specific verification:

- Inspect `sync/error.rs` and confirm all classification tables above
  are represented.
- Inspect `transport_reqwest.rs` and confirm net error evidence is not
  discarded.
- Inspect `core/request.rs` and `client.rs` and confirm encode and
  decode failures are distinct.
- Inspect set-error handling and confirm per-item set failures do not
  pass through a string-only error path.

Cross-cutting reconciliation:

- Confirm every builder path calls `.protocol(Protocol::Jmap)`.
- Confirm operation context is attached before `.build()`.
- Confirm cursor error paths pass `ErrorScope::Cursor`.
- Confirm no `RecoveryClass` construction remains in JMAP.
- Confirm no edits outside `crates/jmap/`.

Editorial normalization:

- Do not update `reference/jmap.md` until the implementation is real.
- Keep this plan focused on current gaps. Remove resolved audit notes
  instead of preserving history.

## Phase 3 correctness blockers (post-2.2 audit)

The Phase 2.2 JMAP commit landed the translation boundary and tests
but reported divergences that are now reclassified from "follow-up"
to "correctness blocker." Each must be resolved before Phase 3
exit. The grep recipes in `plans/error-model-roadmap.md`'s Phase 3
correctness gates apply.

### Operation placeholders in PIM (`sync/pim.rs`)

The 2.2 commit bulk-migrated ~50 PIM conversion sites through a
local `to_acct_err_pim` shim that defaults to
`AccountOperation::Discover`. Phase 3 must thread the correct
operation per call site. The mapping is:

- Send entry points → `AccountOperation::Send`.
- Draft create/update/discard/send → `DraftCreate` / `DraftUpdate` /
  `DraftDiscard` / `DraftSend` respectively.
- Search and message search entry points → `Search` / `SearchMessages`.
- Identity list/update → `IdentitiesList` / `IdentityUpdate`.
- Vacation get/set → `VacationGet` / `VacationSet`.
- Quota get → `QuotaGet`.
- Container list/create/rename/move/delete → `ContainersList` /
  `ContainerCreate` / `ContainerRename` / `ContainerMove` /
  `ContainerDelete`.
- Attachment upload → `AttachmentUpload`.

**Semantic exceptions where `Discover` is correct**: only the
initial capability/scope discovery call sites in `discover.rs` and
`scopes.rs`. Every other site must pass the precise operation.

**Why this is a correctness blocker, not hygiene**: misclassifying
`Send` (non-idempotent) as `Discover` (idempotent) causes the
central recovery mapping to choose `Retry::SameRequest` for an
`InFlight` transport drop where the correct answer is `Reconcile`.
A retry of an in-flight `Send` is a duplicate-message bug.

### Known JMAP set-error vocabulary must use typed variants

After the Phase 1 amendment adds typed `JmapMethod` variants for the
known `SetErrorType` family, JMAP's `set_error_to_account_error` must
route each known code through the corresponding typed variant.
`JmapMethod::Unknown { code }` is reserved for codes the spec adds
after the amendment. The audit grep
`rg 'JmapMethod::Unknown' crates/jmap/` must show only the genuine
forward-compatibility branch.

### `MethodErrorType::Other(String)` confirmation

The 2.2 commit added `MethodErrorType::Other(String)` to capture
unknown method-error codes (plan option 1). This is correct and
remains. Phase 3 does not collapse it back.

### Search page-cursor decode confirmation

The 2.2 commit mapped search page-cursor decode failures to
`SchemaIncompatible` rather than `Request(Malformed)`. This is
correct (the page cursor is an engine-persisted opaque value) and
remains.

### Transitional `type Error = AccountError` bridges

Five files (`factory.rs`, `push.rs`, `pim.rs`, `capabilities.rs`,
one other) carry `type Error = AccountError;` aliases so existing
`Result<_, Error>` signatures compile against the broken branch.
Phase 3 deletes all such aliases as part of the `Account` trait
return-type rewrite. The audit grep
`rg 'type Error = AccountError' crates/jmap/` must return zero hits
at Phase 3 exit.
