# Error model: bifrost-graph implementation plan

This is Phase 2.2 of `plans/error-model-roadmap.md`, one of the five
consumer-crate patches authored after `bifrost-net`.

Phase 2 is still on the intentionally broken feature branch. Author
the Graph patch and deterministic tests, but do not run `brokkr`,
`cargo`, or `./diff_test.sh` from the crate agent.

## Required reading

- `CLAUDE.md`
- `plans/error-model-roadmap.md`
- `plans/error-model-convergence.md`
- `plans/error-model-net.md`
- `reference/graph.md`
- `crates/types/src/error/` (the landed Phase 1 API)

## Kind/cause shape conventions

Landed-API quirks the agent must keep straight:

- `ServerErrorKind::Error { status: Option<u16> }` (kind side) vs
  `ServerCause::Error { status: u16 }` (cause side). Table rows write
  `Some(_)` on the kind column and bare `u16` on the cause column.
  Do not "fix" one to match the other.
- `AccessErrorKind::PermissionDenied` (no payload) vs
  `AccessCause::PermissionDenied { resource: Option<ResourceKind> }`.
- The landed `AccountOperation` enum (`crates/types/src/error/scope.rs`)
  does NOT include `MoveMessage` or `DeleteMessage`. Single-target
  moves and destroys use `BulkMove` / `BulkDestroy` (the landed bulk
  variants apply to any cardinality). `UpdateFlags`, `SetIsRead`,
  `SetKeyword`, `SetLabelMembership`, `SetCategory`,
  `SetExtendedProperty` cover the flag/label/category surface.
- Every kind/cause pair below must satisfy
  `recovery::kind_matches_cause` (asserted at runtime by
  `AccountErrorBuilder::build`).

## Scope

Own only `crates/graph/` files. Do not edit `crates/types/` even if a
new `GraphSignal` wire variant would be useful. If the existing
`GraphSignal` enum is insufficient, report the required variants to
the orchestrator under the roadmap's wire-enum escape hatch.

This phase rewrites Graph's internal error boundary and deletes the
substring-matching recovery helper. Phase 3 may still perform final
workspace surface reconciliation across all crates
(`SyncEvent::Fatal` rename, `MutationResult` to `ItemOutcome`, and
`Account` signature updates).

## Current state

`bifrost-graph` is implemented. The relevant current shape is:

- `crates/graph/src/client.rs`
  - `GraphClient::{get_json,get_absolute,post,post_empty,patch,delete,post_batch}`
    return `Result<T, String>` or `Result<(), String>`.
  - `execute` calls `bifrost-net`, then `net_error` flattens
    `bifrost_net::Error` into formatted strings such as
    `"Graph API error 429 Too Many Requests: rate limited"`.
  - `parse_json_response` and `check_response_status` also flatten
    HTTP status, headers, body, and JSON parse errors into strings.
- `crates/graph/src/account/error.rs`
  - `graph_error_to_fatal(message, scope)` and
    `recovery_for_graph_error(message, scope)` classify by lowercased
    substrings.
  - `mutation_outcome_for_status(status, destroy, id)` maps raw
    `$batch` statuses to old `MutationOutcome`.
  - `fatal_from_recovery` constructs old `Fatal` values directly.
- `crates/graph/src/account/inventory.rs` and `changes.rs`
  - Graph delta page failures become `graph_error_to_fatal`.
  - Cursor envelope mismatch is currently converted through old
    `RecoveryClass::SchemaIncompatible` / `Fatal`.
- `crates/graph/src/account/mutate.rs`
  - `$batch` uses `If-Match` for `SetFlags` and `Move`.
  - Per-item 412 is currently `Skipped`.
  - Per-item 429 becomes `Failed(Error::Transport(...))`, plus a
    trailing old retry `Fatal`.
  - Per-item status bodies are not parsed, because
    `BatchResponseItem` carries only status, headers, and optional
    JSON body.
- `crates/graph/src/account/pim.rs`
  - Single-operation PIM methods map many Graph strings into old
    `Error::Transport`.
  - `submit_write_batch` turns per-item 412 into old
    `Error::ConcurrencyConflict` and every other non-success into a
    formatted transport string.
  - Local unsupported Graph gaps return old `Error::Unsupported` or
    `Error::Other`.
- `crates/graph/src/account/push.rs`
  - Webhook create/delete surface old `Error::Transport`.
  - Renewal worker logs renewal errors as strings and emits only
    `WatchEvent::Disconnected` / `Reconnected`.
- `crates/graph/src/ews/client.rs` and
  `crates/graph/src/account/ews_stream.rs`
  - EWS request, status, SOAP fault, and XML parse failures are
    strings.
  - The streaming worker logs them and reconnects.
- `crates/graph/src/account/blob.rs`
  - Blob status errors and range failures are flattened before
    `graph_error_to_fatal`.
  - 405 is intentionally downgraded to a non-byte-stream warning.

The key defect is not the specific mapping table. It is the loss of
structured evidence before the account boundary: HTTP status, retry
headers, Graph `error.code`, Graph `innerError`, request ids,
transmission state, EWS SOAP faults, and per-item `$batch` bodies.

## Files to modify

Primary files:

- `crates/graph/src/client.rs`
- `crates/graph/src/types.rs`
- `crates/graph/src/account/error.rs`
- `crates/graph/src/account/mod.rs`
- `crates/graph/src/account/inventory.rs`
- `crates/graph/src/account/changes.rs`
- `crates/graph/src/account/get.rs`
- `crates/graph/src/account/mutate.rs`
- `crates/graph/src/account/pim.rs`
- `crates/graph/src/account/blob.rs`
- `crates/graph/src/account/push.rs`
- `crates/graph/src/account/push_stream.rs`
- `crates/graph/src/account/ews_stream.rs`
- `crates/graph/src/ews/client.rs`
- `crates/graph/src/ews/xml_helpers.rs`
- `crates/graph/src/webhooks.rs`
- `crates/graph/src/lib.rs`

Recommended new files:

- `crates/graph/src/error.rs`
- `crates/graph/src/account/graph_error.rs`
- `crates/graph/src/ews/error.rs`

Use `crates/graph/src/error.rs` for the crate-wide structured error
shape and Graph HTTP body parser. Use `account/graph_error.rs` for the
builder-based account conversion and context helpers. Use
`ews/error.rs` only if the EWS-specific error shape is large enough to
keep out of `ews/client.rs`.

## Files to delete

No whole file must be deleted.

Delete or replace these functions from `crates/graph/src/account/error.rs`:

- `graph_error_to_fatal`
- `recovery_for_graph_error`
- `fatal_from_recovery` in its old direct-recovery form
- `mutation_outcome_for_status` in its old-error form

`warning_blob_not_byte_stream` can stay, but its `Warning` fields must
match the Phase 1 `Warning` shape when Phase 3 updates warning
construction workspace-wide.

## Dependencies

Required:

- Phase 1 types landed at commit `ac47289` (see
  `crates/types/src/error/` for the API surface).
- Phase 2.1 `bifrost-net` conversion from
  `plans/error-model-net.md`.

Graph already depends on `bifrost-net` and `bifrost-types`, so no new
workspace dependency is expected.

## Structured error shape

Replace the `Result<T, String>` boundary with a structured crate error.
The exact names can vary, but the shape must preserve the evidence
below.

```rust
#[derive(Debug)]
pub(crate) enum GraphError {
    Net(bifrost_net::Error),
    GraphResponse(Box<GraphResponseError>),
    GraphJsonDecode {
        service: GraphService,
        source: serde_json::Error,
    },
    GraphJsonEncode {
        service: GraphService,
        source: serde_json::Error,
    },
    Local(GraphLocalError),
    Ews(EwsError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GraphService {
    Graph,
    Ews,
}

#[derive(Debug)]
pub(crate) struct GraphResponseError {
    pub(crate) service: GraphService,
    pub(crate) status: u16,
    pub(crate) headers: GraphResponseHeaders,
    pub(crate) body: bytes::Bytes,
    pub(crate) envelope: Option<GraphErrorEnvelope>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct GraphResponseHeaders {
    pub(crate) retry_after: Option<std::time::Duration>,
    pub(crate) request_id: Option<String>,
    pub(crate) client_request_id: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct GraphErrorEnvelope {
    pub(crate) code: String,
    pub(crate) message: Option<String>,
    pub(crate) inner: Option<GraphInnerError>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct GraphInnerError {
    pub(crate) code: Option<String>,
    pub(crate) request_id: Option<String>,
    pub(crate) client_request_id: Option<String>,
    pub(crate) date: Option<String>,
    pub(crate) inner: Option<Box<GraphInnerError>>,
}
```

Deserializer notes:

- Microsoft Graph wraps errors under a top-level `error` key. Use a
  private wrapper type and store only the inner envelope.
- Preserve unknown fields by ignoring them, not by failing the parse.
- `innerError` can be nested. Provide an iterator over outer code plus
  all inner codes.
- Graph error text is support-only diagnostic text. Do not mark it
  user-safe.
- Request ids can appear in headers or `innerError`. Prefer headers
  for top-level diagnostics, then fall back to the deepest available
  inner id.
- If the body is not valid Graph JSON, keep the capped raw body bytes
  inside `GraphResponseError` and classify by HTTP status fallback.

`GraphClient` public helpers should become:

```rust
pub(crate) async fn get_json<T: DeserializeOwned>(
    &self,
    path: &str,
) -> Result<T, GraphError>;
```

and equivalent signatures for `get_absolute`, `post`, `post_empty`,
`patch`, `delete`, and `post_batch`.

`execute` should return `Result<Response, GraphError>` and preserve
`GraphError::Net(error)` when `bifrost-net` returns a non-status
transport error. For `bifrost_net::Error::Status`, parse the Graph
body and wrap it as `GraphResponse`.

Do not keep `net_error(service, err) -> String`.

## EWS error shape

EWS shares the account, provider, token, and transport, but it is not
Graph JSON. Keep it typed separately and convert through the same
account boundary with `Protocol::Ews`.

```rust
#[derive(Debug)]
pub(crate) enum EwsError {
    Net(bifrost_net::Error),
    Status {
        status: u16,
        headers: GraphResponseHeaders,
        body: bytes::Bytes,
    },
    SoapFault {
        code: Option<String>,
        message: Option<String>,
    },
    XmlParse {
        operation: EwsOperation,
        message: String,
    },
    EmptySubscribeResponse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EwsOperation {
    Subscribe,
    GetStreamingEvents,
}
```

`check_soap_fault` should parse stable SOAP evidence, not only a
formatted `faultstring`. Capture `faultcode`, `faultstring`, and, when
present in EWS response messages, `ResponseCode`.

EWS XML parser failures are `Protocol(ParseFailed)`. SOAP faults are
classified by stable fault or response code when available and by HTTP
status fallback otherwise. Do not substring-match the SOAP message.

## Local error shape

Current old `Error::Unsupported`, `MissingCoreCapability`,
`RangeNotSupported`, `Other`, and cursor decode failures need a local
typed shape before conversion.

Recommended:

```rust
#[derive(Debug)]
pub(crate) enum GraphLocalError {
    Unsupported {
        operation: AccountOperation,
        detail: Option<&'static str>,
    },
    MissingCoreCapability {
        operation: AccountOperation,
        detail: &'static str,
    },
    InvalidRequest {
        operation: AccountOperation,
        detail: String,
    },
    InvalidCursor {
        kind: GraphCursorFailure,
        scope: Option<CursorScope>,
        detail: String,
    },
    MissingField {
        field: &'static str,
        detail: String,
    },
    BlobNotRangeCapable {
        id: String,
    },
    Internal {
        detail: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GraphCursorFailure {
    ProtocolMismatch,
    EnvelopeUnknown,
    SchemaIncompatible,
    MalformedPayload,
    ScopeMismatch,
}
```

Mapping:

- Unsupported capability or method:
  `AccountErrorKind::Unsupported(operation)` with
  `Cause::Request(RequestCause::Unsupported { operation })`.
- Caller-provided invalid input:
  `Request(RequestErrorKind::Malformed)`.
- Provider success response missing required field:
  `Protocol(ProtocolErrorKind::MissingField)`.
- Cursor protocol/version/schema mismatch:
  `SyncState(SyncStateErrorKind::SchemaIncompatible)` with
  `StateCause::SchemaIncompatible`.
- Cursor payload parse failure:
  `SyncState(SyncStateErrorKind::CursorInvalid)` if the engine can
  reseed the scope, otherwise `Protocol(ParseFailed)` for programmer
  supplied corrupt state outside a cursor scope.

## Account conversion boundary

Add in `account/graph_error.rs`:

```rust
use bifrost_types::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuilder,
    AccountErrorKind, AccountOperation, AuthCause, AuthErrorKind,
    Cause, CursorScope, DiagnosticText, ErrorScope, GraphSignal,
    Protocol, ProtocolErrorKind, Provider, RequestCause,
    RequestErrorKind, ResourceKind, ServerCause, ServerErrorKind,
    StateCause, SyncStateErrorKind, ThrottleScope, TransportCause,
    TransportErrorKind, TransportKind, WireCause,
};

#[derive(Clone, Debug)]
pub(crate) struct GraphErrorContext {
    pub(crate) operation: AccountOperation,
    pub(crate) scope: Option<ErrorScope>,
    pub(crate) cursor_scope: Option<CursorScope>,
    pub(crate) protocol: Protocol,
    pub(crate) throttle_scope: Option<ThrottleScope>,
    pub(crate) resource: Option<GraphResource>,
    pub(crate) idempotency_override: Option<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GraphResource {
    Account,
    Message,
    Mailbox,
    Thread,
    Calendar,
    Contact,
    Blob,
    Subscription,
}

pub(crate) fn into_account_error(
    error: crate::error::GraphError,
    ctx: GraphErrorContext,
) -> AccountError;
```

Helper constructors are encouraged:

```rust
impl GraphErrorContext {
    pub(crate) fn discover() -> Self;
    pub(crate) fn inventory(scope: CursorScope) -> Self;
    pub(crate) fn changes(scope: CursorScope) -> Self;
    pub(crate) fn hydrate_message(id: impl Into<String>) -> Self;
    pub(crate) fn mutation(operation: AccountOperation) -> Self;
    pub(crate) fn push_subscribe() -> Self;
    pub(crate) fn push_unsubscribe() -> Self;
    pub(crate) fn push_stream(protocol: Protocol) -> Self;
    pub(crate) fn open_blob(id: impl Into<String>) -> Self;
    pub(crate) fn send() -> Self;
}
```

Every conversion must attach:

- `.provider(Provider::Microsoft)`
- `.protocol(Protocol::Graph)` for Graph REST
- `.protocol(Protocol::Ews)` for EWS fallback
- `.operation(ctx.operation)`
- `.scope(scope)` when known
- `.idempotency_override(false)` for send/draft-send operations and
  any in-flight non-idempotent write where the default operation
  idempotency would be too weak or unavailable

For `GraphError::Net(error)`, delegate to:

```rust
bifrost_net::into_account_error(
    error,
    bifrost_net::NetErrorContext {
        provider: Some(Provider::Microsoft),
        protocol: ctx.protocol,
        operation: Some(ctx.operation),
        scope: ctx.scope.clone(),
    },
)
```

Do this only for transport-like net errors. For
`bifrost_net::Error::Status`, parse the Graph or EWS body first and
run the provider-specific mapping below. Graph-specific codes are more
precise than generic HTTP status mapping.

## Builder rules

Every Graph-specific conversion must use:

```rust
AccountErrorBuilder::new(kind, primary_cause)
```

When a Graph response exists:

- Push `Cause::Wire(WireCause::Graph(signal))`.
- Set `.status(status)`.
- Set `.native_code(code)` when `error.code` or an inner code exists.
- Set `.request_id(...)` and `.trace_id(...)` from headers or
  `innerError`.
- Add `error.message` and raw unparsed bodies as support-only
  `DiagnosticText`.
- Add `.retry_not_before(...)` and `.throttle_scope(...)` when
  `Retry-After` exists for rate-limit or quota errors.

When a `$batch` item body contains a Graph error envelope, preserve
the item envelope the same way as a top-level response. The batch item
`id` is a local correlation id; put the target object id in
`ErrorScope::Message { id }`, not in `request_id`.

## Graph wire signals

The current Phase 1 `GraphSignal` variants are:

- `Gone`
- `InvalidAuthenticationToken`
- `AccessDenied`
- `Forbidden`
- `AccessRestricted`
- `ConditionalAccessBlocked`
- `AdminConsentRequired`
- `MailboxNotEnabledForRestApi`
- `MailboxStoreUnavailable`
- `ResyncRequired`
- `TooManyRequests`
- `GenericFileError`
- `PreconditionFailed`
- `NotFound`
- `Unknown { code }`

Use exact Graph `error.code` matching. Do not inspect
`error.message` for recovery. If Microsoft returns a stable code that
is not represented by `GraphSignal`, map the `AccountErrorKind`
correctly and use `GraphSignal::Unknown { code }` until the
orchestrator patches `bifrost-types`.

Potential wire-enum escape-hatch requests:

- `InvalidDeltaToken`
- `SyncStateNotFound`
- `ErrorItemNotFound`
- `BadRequest`
- `ErrorInvalidIdMalformed`
- `ServiceUnavailable`

These are not blockers. The implementation can classify them with
`Unknown { code }` while reporting the requested additions in the
audit.

## Graph error-code mapping

Prefer `error.code` and inner codes over HTTP status. Code comparisons
are case-sensitive first, then normalized with ASCII case folding only
for known Microsoft casing drift. Do not parse localized message text.

| Graph code | Account kind | Primary cause | Wire signal | Notes |
| --- | --- | --- | --- | --- |
| `InvalidAuthenticationToken` | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` | `InvalidAuthenticationToken` | Use `Expired` only when a stable inner code explicitly says token expired. |
| `AccessDenied` | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` | `AccessDenied` | Resource from context. |
| `Forbidden` | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` | `Forbidden` | Generic 403 without conditional access. |
| `AccessRestricted` | `Authorization(ConditionalAccessBlocked)` | `Access(ConditionalAccessBlocked)` | `AccessRestricted` | Tenant policy or CA-style block. |
| `ConditionalAccessBlocked` | `Authorization(ConditionalAccessBlocked)` | `Access(ConditionalAccessBlocked)` | `ConditionalAccessBlocked` | Stable CA code. |
| `AdminConsentRequired` | `Authorization(AdminConsentRequired)` | `Access(AdminConsentRequired { needed })` | `AdminConsentRequired` | Use a stable needed string such as `Microsoft Graph mail permissions`. |
| `MailboxNotEnabledForRESTAPI` | `Authorization(MailboxNotLicensed)` | `Access(MailboxNotLicensed)` | `MailboxNotEnabledForRestApi` | User mailbox cannot use Graph mail API. |
| `MailboxStoreUnavailable` | `Authorization(MailboxUnavailable { Transient })` | `Access(MailboxUnavailable { Transient })` | `MailboxStoreUnavailable` | Retryable by builder. |
| `ResyncRequired` | `SyncState(CursorInvalid)` | `State(CursorInvalid)` | `ResyncRequired` | Requires cursor scope. |
| `InvalidDeltaToken` | `SyncState(CursorInvalid)` | `State(CursorInvalid)` | `Unknown { code }` | Request `GraphSignal` addition. |
| `syncStateNotFound` | `SyncState(CursorInvalid)` | `State(CursorInvalid)` | `Unknown { code }` | Request `GraphSignal` addition. |
| `TooManyRequests` | `Server(RateLimited)` | `Server(RateLimited { retry_after })` | `TooManyRequests` | Add throttle scope and retry hint. |
| `GenericFileError` | `Server(Unavailable)` | `Server(Unavailable { retry_after })` | `GenericFileError` | Graph often uses this for transient file/blob failures. |
| `PreconditionFailed` | `ConcurrencyConflict` | `State(ConcurrencyConflict)` | `PreconditionFailed` | Same for HTTP 412. |
| `ErrorItemNotFound` | `NotFound(resource)` | `Request(NotFound { what, id })` | `Unknown { code }` | Request `GraphSignal` addition if common. |
| `Request_ResourceNotFound` | `NotFound(resource)` | `Request(NotFound { what, id })` | `Unknown { code }` | Use context resource. |
| `ErrorInvalidIdMalformed` | `Request(Malformed)` | `Request(Malformed { detail })` | `Unknown { code }` | Bad caller id. |
| `BadRequest` | `Request(Malformed)` | `Request(Malformed { detail })` | `Unknown { code }` | Fallback for structured 400. |
| `InvalidRequest` | `Request(Malformed)` | `Request(Malformed { detail })` | `Unknown { code }` | Fallback for malformed Graph request. |
| `ErrorAccessDenied` | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` | `Unknown { code }` | EWS-ish code can appear in Graph-backed paths. |
| `ErrorQuotaExceeded` | `Server(QuotaExhausted)` | `Server(QuotaExhausted { retry_after })` | `Unknown { code }` | Add throttle scope. |
| Unknown code with 401 | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` | `Unknown { code }` | Preserve code as native. |
| Unknown code with 403 | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` | `Unknown { code }` | Preserve code as native. |
| Unknown code with 404 | `NotFound(resource)` | `Request(NotFound { what, id })` | `Unknown { code }` | Context chooses resource. |
| Unknown code with 429 | `Server(RateLimited)` | `Server(RateLimited { retry_after })` | `Unknown { code }` | Add throttle scope. |
| Unknown code with 5xx | `Server(Unavailable)` | `Server(Unavailable { retry_after })` | `Unknown { code }` | Retryable. |
| Unknown code otherwise | `Server(Error { status })` for HTTP status, else `Protocol(Unknown)` | Server or wire cause | `Unknown { code }` | Do not inspect message text. |

`ResourceKind` has only `Message`, `Mailbox`, `Thread`, `Calendar`,
and `Contact`. Map Graph subscriptions and blobs to the nearest
operation scope instead of inventing a resource kind in this phase.

## HTTP status fallback

Use this when no useful Graph code exists.

| Status | Account kind | Primary cause | Notes |
| --- | --- | --- | --- |
| `400` on delta with known delta context | `SyncState(CursorInvalid)` | `State(CursorInvalid)` | Only for `changes_stream` / delta pagination. |
| `400` otherwise | `Request(Malformed)` | `Request(Malformed { detail })` | Bad query, filter, body, or id shape. |
| `401` | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` | `bifrost-net::AuthLost` may already classify this. |
| `403` | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` | Unless Graph code refines to CA/admin consent. |
| `404` | `NotFound(resource)` | `Request(NotFound { what, id })` | Destroy can still treat already-gone as success. |
| `409` | `ConcurrencyConflict` | `State(ConcurrencyConflict)` | Use for write conflicts; otherwise `Server(Error)`. |
| `410` on delta | `SyncState(CursorInvalid)` | `State(CursorInvalid)` | Restart the cursor scope. |
| `410` elsewhere | `Server(Error { status: Some(410) })` | `Server(Error { status: 410 })` | Provider refused / stale resource. |
| `412` | `ConcurrencyConflict` | `State(ConcurrencyConflict)` | `If-Match` mismatch. |
| `413` | `Request(Malformed)` | `Request(Malformed { detail })` | Request too large. |
| `415` | `Unsupported(operation)` | `Request(Unsupported { operation })` | Unsupported media/content type. |
| `423` | `Authorization(MailboxUnavailable { Transient })` | `Access(MailboxUnavailable { Transient })` | Locked mailbox/resource. |
| `429` | `Server(RateLimited)` | `Server(RateLimited { retry_after })` | Add throttle scope. |
| `500` | `Server(Unavailable)` | `Server(Unavailable { retry_after })` | Retryable. |
| `502` | `Server(Unavailable)` | `Server(Unavailable { retry_after })` | Retryable. |
| `503` | `Server(Unavailable)` | `Server(Unavailable { retry_after })` | Retryable. |
| `504` | `Server(Unavailable)` | `Server(Unavailable { retry_after })` | Retryable. |
| `507` | `Server(QuotaExhausted)` | `Server(QuotaExhausted { retry_after })` | Add throttle scope. |
| Other 4xx | `Server(Error { status: Some(status) })` | `Server(Error { status })` | Provider refused by builder. |
| Other 5xx | `Server(Unavailable)` | `Server(Unavailable { retry_after })` | Retryable. |

Positive or 3xx responses on error paths are
`Protocol(ContractViolation)` unless the operation explicitly expects
that status.

## Throttle scope and retry hints

Microsoft Graph documents tenant-wide throttling on the `Application`
and `Mailbox concurrency` resource keys (see Microsoft Learn:
"Microsoft Graph throttling guidance"). A 429 from Graph cannot be
distinguished as tenant- vs mailbox-scope from headers alone — the
response identifies the limit category in body or in
`Rate-Limit-Reason`, but consumers should still pause at the safest
scope to avoid cascading throttles across the tenant. Convergence
master §"Examples" (Graph 429 on `MoveMessage`) commits to
`ThrottleScope::Tenant` as the canonical Graph default for this
reason.

Initial mapping:

- `ThrottleScope::Tenant` for Graph REST 429 (`TooManyRequests`,
  `ErrorQuotaExceeded`, unknown 429) — safest scope; tenant-wide
  pause is the documented Microsoft recommendation. Refines to
  `Account` or `Mailbox` only when the response body explicitly
  identifies a per-mailbox or per-account limit.
- `ThrottleScope::Mailbox` for stable 429 codes that name a mailbox
  resource (e.g. `ApplicationThrottled` against a folder/message
  endpoint with `Mailbox concurrency` evidence).
- `ThrottleScope::Account` only when the consumer's auth mode is
  delegated and the limit category is explicitly user-scoped.
- `ThrottleScope::Provider` for Graph service-wide 503/504 only if
  the status is not tied to an account request.

The `bifrost-net` Phase 2.1 fallback maps Graph 429 to
`ThrottleScope::Tenant`; this graph.md mapping aligns and refines
where the body provides evidence. Do not invert the default to
`Account`/`Mailbox` without body evidence.

Use the Phase 2.1 `bifrost-net` retry-after parser when available.
For Graph `$batch` item headers, keep a local helper if the net helper
only accepts `HeaderMap`; it must parse integer seconds. HTTP-date
support is optional for `$batch` item headers, but top-level HTTP
responses should get whatever `bifrost-net` already supports.

For rate-limited or quota errors:

- set `ServerCause::{RateLimited,QuotaExhausted} { retry_after }`
- call `.retry_not_before(SystemTime::now() + retry_after)` when
  addition succeeds
- call `.throttle_scope(scope)`

## Cursor and sync mapping

`decode_cursor` and `scope_matches_payload` are local state
validation, not provider failures.

Map:

| Condition | Account kind | Recovery derived |
| --- | --- | --- |
| Wrong `ProtocolKind` | `SyncState(SchemaIncompatible)` | `Engine(SchemaIncompatible)` |
| Envelope version newer than supported | `SyncState(SchemaIncompatible)` | `Engine(SchemaIncompatible)` |
| Envelope version older than supported | `SyncState(SchemaIncompatible)` | `Engine(SchemaIncompatible)` |
| Cursor JSON payload malformed | `SyncState(CursorInvalid)` with cursor scope when available | `Engine(RestartScope(scope))` or restart account fallback |
| Progress marker JSON malformed | `SyncState(CursorInvalid)` with cursor scope | Restart scope. |
| Payload kind does not match cursor scope | `SyncState(SchemaIncompatible)` | Schema incompatible. |
| Unsupported initial scope shape | `Unsupported(EstablishCursor)` | Terminal unsupported. |

Delta page provider failures:

- 410 on `changes_stream` or inventory delta page:
  `SyncState(CursorInvalid)` with
  `ErrorScope::Cursor(cursor.scope.clone())` or equivalent scope.
- 400 with `InvalidDeltaToken`, `syncStateNotFound`, or
  `ResyncRequired`: same as above.
- 429/503/504: server retry with scope.
- Parse failure for a Graph page body that returned success:
  `Protocol(ParseFailed)` with `WireCause::MalformedResponse`.
- Success page missing both `@odata.nextLink` and `@odata.deltaLink`
  on a delta walk should be `Protocol(ContractViolation)` unless the
  current code intentionally treats it as terminal `Done(None)`.

`inventory_stream`, `changes_stream`, and `get_stream` should stop
building old `Fatal` structs directly. Add helpers that return
`AccountError`; Phase 3 wraps them in `SyncEvent::Terminated`.

## Hydration and get mapping

`get_stream` uses `/$batch` for hydrated object fetches. Each item is
independent.

Rules:

- Top-level `/$batch` request failure before a response:
  stream termination `AccountError`.
- `$batch` item 2xx with valid body: emit hydrated object.
- `$batch` item 404: per-item failure. Phase 2 emits the failure as
  `ItemOutcome::Failed(BatchFailure { item, error: AccountError { kind:
  NotFound(ResourceKind::Message), .. } })` through the streaming
  helper added in this phase. If the existing `get_stream` cannot yet
  carry per-item ItemOutcome values, Phase 2's helper still returns
  the typed `ItemOutcome` value, and the stream call site holds it
  until Phase 3 changes the trait signature. Do NOT terminate the
  stream on a single missing item — that would coerce per-item
  evidence into a stream-level error.
- `$batch` item non-2xx with Graph error body: parse item body and
  convert with item id scope.
- `$batch` item success body missing required fields:
  `Protocol(MissingField)`.

If the current `get_stream` cannot represent per-item hydration
failure until Phase 3, keep the helper ready and note the limitation in
the implementation audit. Do not collapse the error through a string.

## Mutation mapping

Bulk mutation streams will move to `ItemOutcome<MutationSuccess>` in
Phase 3. This phase should replace the classification helpers so the
integration can wire them mechanically.

Recommended replacement for `mutation_outcome_for_status`:

```rust
pub(crate) fn mutation_item_outcome_for_response(
    item: BatchResponseItem,
    target: &ObjectId,
    kind: MutationKind,
    ctx: GraphErrorContext,
) -> ItemOutcome<MutationSuccess>;
```

Status rules:

| `$batch` item status | Outcome |
| --- | --- |
| 2xx | `Succeeded(MutationSuccess::Applied)` |
| 404 on destroy | `Succeeded(MutationSuccess::Skipped)` |
| 404 otherwise | `Failed(NotFound(Message))` |
| 409 | `Failed(ConcurrencyConflict)` |
| 412 | `Failed(ConcurrencyConflict)` |
| 429 | `Failed(Server(RateLimited))` for the item; see "Trailing throttle termination" below |
| 5xx | `Failed(Server(Unavailable))` |
| Other 4xx | `Failed` from Graph code or HTTP fallback |

Raw 412 is not `MutationSuccess::Skipped`. `Skipped` is for the
engine read-back guard or idempotent destroy-already-gone. A raw
`If-Match` mismatch is a failed item with
`RecoveryClass::Retry(AfterStateRefresh)`, derived from
`AccountErrorKind::ConcurrencyConflict`.

### Trailing throttle termination

When a `$batch` response contains one or more 429 items, the mutation
helper:

1. Emits each 429 item as `ItemOutcome::Failed(BatchFailure { item,
   error })` with `Server(RateLimited)`, `retry_after`, and
   `throttle_scope` populated. Per-item failure is the only signal the
   caller needs to retry the specific items.
2. After all items in the batch are accounted for, the helper does NOT
   additionally terminate the stream with a trailing `Reconcile`. The
   engine's retry policy reads `not_before` and `throttle_scope` off
   the per-item `AccountError` and pauses at the indicated scope.
   Adding a stream-level termination on top would double-count: the
   engine would see both a per-item `Failed` and a stream-level halt,
   producing two conflicting recovery signals.

The original "mark the stream for a trailing rate-limit termination"
phrasing was an open decision. The commitment is: per-item Failed
only, no trailing termination. If Graph returns a top-level 429 (the
entire batch was throttled before any item was processed), that IS a
stream-level termination — it goes through `into_account_error` at
the `post_batch` call site as `Reconcile` or `Retry` per the
convergence mapping table, not through per-item lanes.

Top-level `post_batch` failure:

- If no batch response arrived, terminate the stream with
  `into_account_error(error, mutation ctx)`.
- If a response arrived and some item statuses were processed before a
  local parse issue, emit outcomes for processed items and terminate
  with `Protocol(PartialResponse)`.

Missing etag refresh:

- `GET /messages/{id}?$select=id` failure for one target is a failed
  item for that target, not a batch-level terminal error.
- Response missing `changeKey` for a mutation requiring etag is
  `Protocol(MissingField)` for that item.

PIM single writes in `pim.rs` use the same Graph status mapper:

- `submit_write_batch` 412 returns `AccountErrorKind::ConcurrencyConflict`.
- 404 on delete/clear paths that are intentionally idempotent returns
  success.
- other non-success batch item bodies must be parsed and converted,
  not formatted into `Error::Transport`.

`send_message` / `draft_send` are non-idempotent. Use
`idempotency_override(false)` and preserve in-flight transport
evidence from `bifrost-net`. If draft creation succeeds and draft send
then fails, add support-only diagnostic text with the draft id so the
consumer can reconcile by sync/search without showing raw provider ids
as user-facing text.

## PIM and local request mapping

Map current local old-error sites explicitly:

| Current condition | Account kind | Notes |
| --- | --- | --- |
| unsupported primitive or target shape | `Unsupported(operation)` | Use the concrete landed `AccountOperation` (`UpdateFlags`, `BulkMove`, `BulkDestroy`, `SetIsRead`, `Send`, `DraftCreate`, `ContainersList`, etc.). The landed enum has no `MoveMessage` / `DeleteMessage` — single-target moves/destroys still use `BulkMove` / `BulkDestroy`. |
| missing push endpoint for webhook mode | `Unsupported(PushSubscribe)` | Missing configured capability, not transport. |
| pre-uploaded attachment handle in Graph send/draft | `Unsupported(Send)` or `Unsupported(DraftCreate)` | Keep existing guidance as support-only diagnostic text. |
| draft update tries to replace attachments | `Unsupported(DraftUpdate)` | Graph upload-session primitive is absent. |
| invalid search cursor UTF-8 | `Request(Malformed)` | Caller supplied opaque cursor. |
| unsupported search filter | `Unsupported(Search)` | Search AST valid but Graph cannot express it. |
| non-folder container create | `Unsupported(ContainerCreate)` | Capability surface says mail folders only. |
| Graph success response missing `id` | `Protocol(MissingField)` | Provider contract violation. |
| Graph message missing `changeKey` where required | `Protocol(MissingField)` | Provider contract violation. |
| local JSON serialization of request body fails | `Request(Malformed)` or `Protocol(ContractViolation)` | Request data should normally be serializable; use malformed for caller data, contract violation for internal impossible states. |

## Blob mapping

`open_blob` / `open_blob_range` must keep the current semantic
downgrade for reference attachments and 405:

- Blob locator JSON decode failure:
  `Request(Malformed)` if the handle came from the caller, or
  `Protocol(ContractViolation)` if it was minted by this crate in the
  same stream.
- Reference attachment:
  warning, not `AccountError`.
- Range requested on a handle whose capabilities say no range:
  `Unsupported(OpenBlobRange)` or `Request(Malformed)` depending on
  whether the caller ignored the capability flag. Prefer
  `Unsupported(OpenBlobRange)`.
- `bifrost_net::Error::RangeNotHonored`:
  delegate to net conversion. Local invalid range maps to
  `Request(Malformed)`; provider range mismatch maps to
  `Protocol(ContractViolation)`.
- 405 Method Not Allowed on attachment `$value`:
  warning, not fatal, matching current non-byte-stream behavior.
- Body stream error after response headers:
  `Protocol(PartialResponse)` through the Phase 2.1 net conversion.

## Webhook push mapping

`push_subscribe` and `push_unsubscribe` are normal account operations
and should return `AccountError` after Phase 3 trait migration.

Rules:

- Missing webhook endpoint:
  `Unsupported(AccountOperation::PushSubscribe)`.
- Unsupported scope shape in `resource_for_scope`:
  per-call `Unsupported(PushSubscribe)` if all requested scopes are
  unsupported; otherwise subscribe only supported scopes and document
  the skipped scope behavior. Prefer failing if any requested scope is
  unsupported, so callers do not think push covers a scope it dropped.
- Create subscription HTTP/Graph failures:
  `into_account_error` with operation `PushSubscribe` and
  `GraphResource::Subscription`.
- Delete subscription 404:
  success, preserving current idempotent delete behavior.
- Delete subscription other failures:
  `into_account_error` with operation `PushUnsubscribe`.
- `getrandom` failure for client-state or handle generation:
  `Protocol(ContractViolation)` or local `Internal`; do not mark it as
  provider transport.

Renewal worker:

- Continue emitting `WatchEvent::Disconnected` on first failed
  renewal pass and `Reconnected` after recovery. `WatchEvent` has no
  error payload today, so do not change the trait surface in this
  phase.
- Convert renewal failures to `AccountError` for structured tracing
  and stored health state if a small local `last_error` field is
  useful.
- Classify subscription 404 / not found during renewal as a
  subscription health failure. Recreate is a push-subscription concern;
  if the implementation adds recreation in this phase, force a sync
  invalidation after successful recreation.
- 401/403 during renewal is auth/authz, not generic disconnect.
- 429/503/504 during renewal is retryable server health.

Do not add a new public push error channel in Phase 2. If the current
types cannot surface the structured health error to consumers, mention
that in the audit and preserve the `WatchEvent` behavior.

## EWS streaming mapping

EWS is the in-process fallback path and should use `Protocol::Ews` in
the account error context.

### EWS Subscribe vs streaming operation tagging

EWS has two distinct lifecycle phases that produce different errors:

- **Subscribe**: the initial `Subscribe` SOAP request that registers
  the streaming subscription and returns a subscription id. Failures
  during this phase use `operation: AccountOperation::PushSubscribe`.
  This includes SOAP faults on the Subscribe request, HTTP errors,
  XML parse failure on the Subscribe response, and the
  `EmptySubscribeResponse` case.
- **Stream**: the long-running `GetStreamingEvents` request that
  pulls events for an established subscription. Failures during this
  phase use `operation: AccountOperation::PushStream`. This includes
  watermark rejection, mid-stream disconnect, server-initiated stream
  end with a fault code, and XML parse failure on a streaming event
  message.

The `EwsOperation` field on `EwsError::XmlParse` (`Subscribe` vs
`GetStreamingEvents`) maps one-to-one onto these two
`AccountOperation` values; do not collapse the distinction.

Rules:

- EWS net errors:
  delegate to `bifrost_net::into_account_error` with
  `Protocol::Ews`, `Provider::Microsoft`, and operation
  `PushSubscribe` or `PushStream` per the phase rule above. Do NOT
  default to `PushStream` for Subscribe-phase failures.
- HTTP 401/403 from EWS:
  auth/authz mapping, not generic server failure.
- HTTP 429/503/504:
  server retry/rate-limit mapping with retry hints.
- SOAP fault stable codes:
  - invalid or missing subscription: `Server(Unavailable)` for
    reconnect/resubscribe, or `SyncState(CursorInvalid)` only if the
    watermark itself is rejected and the scope must be restarted.
  - invalid watermark: `SyncState(CursorInvalid)` with
    `ErrorScope::Cursor(scope)` when the failing watermark's scope is
    known. When scope is unknown, attach
    `ErrorScope::Cursor(CursorScope::Account)` so the central mapper
    derives `Engine(RestartScope(CursorScope::Account))` — do NOT
    omit cursor scope, which would fall through to
    `Engine(RestartAccount)` and restart the entire account session
    instead of just the EWS streaming scope.
  - server busy: `Server(RateLimited)` or `Server(Unavailable)`.
  - access denied: `Authorization(PermissionDenied)`.
- XML parse failure:
  `Protocol(ParseFailed)` with `WireCause::MalformedResponse {
  protocol: Protocol::Ews, ... }`.
- Empty Subscribe response:
  `Protocol(MissingField)`.

The worker already reconnects after EWS failures. Keep that behavior.
The structured error should feed tracing and any future health-state
field; it should not turn every temporary EWS disconnect into a
terminal account failure.

## Graph client request ids

Graph commonly returns request ids in these headers:

- `request-id`
- `client-request-id`
- `x-ms-ags-diagnostic`

Store:

- `request-id` in `AccountErrorBuilder::request_id`.
- `client-request-id` in `trace_id` unless the request id is absent,
  in which case either field is acceptable but must be stable.
- `x-ms-ags-diagnostic` as support-only diagnostic text. Do not parse
  it unless the code already has a structured parser.

Inner error ids should be used only when headers do not carry an id.

## Existing tests to replace

The current tests in `account/error.rs` assert substring mapping:

- 410 / gone
- invalid delta token strings
- 429 / too many requests
- 401 / unauthorized
- 503 / 504
- syncStateNotFound strings
- default retry/auth fallback from message strings

Delete those tests with the old helper. Replace them with structured
error tests that construct `GraphResponseError` and `GraphErrorEnvelope`
directly.

## Test plan

Keep tests deterministic. Do not add live Graph, live EWS, Docker,
fixed ports, external accounts, or webhook servers. Use synthetic
Graph JSON bodies, `bifrost_net::Error` values, EWS SOAP strings, and
batch response items.

Minimum test list:

1. `InvalidAuthenticationToken` maps to
   `Authentication(ReauthorizationRequired)` with
   `Provider::Microsoft` and `Protocol::Graph`.
2. `AccessDenied` maps to `Authorization(PermissionDenied)`.
3. `Forbidden` maps to `Authorization(PermissionDenied)`.
4. `AccessRestricted` maps to
   `Authorization(ConditionalAccessBlocked)`.
5. `AdminConsentRequired` maps to
   `Authorization(AdminConsentRequired)`.
6. `MailboxNotEnabledForRESTAPI` maps to
   `Authorization(MailboxNotLicensed)`.
7. `MailboxStoreUnavailable` maps to transient mailbox unavailable.
8. `ResyncRequired` maps to `SyncState(CursorInvalid)` and derives
   engine restart for the cursor scope.
9. `InvalidDeltaToken` maps to cursor invalid using
   `GraphSignal::Unknown`.
10. `syncStateNotFound` maps to cursor invalid using
    `GraphSignal::Unknown`.
11. `TooManyRequests` maps to `Server(RateLimited)` with retry hint
    and throttle scope.
12. `GenericFileError` maps to retryable server unavailable.
13. `PreconditionFailed` and HTTP 412 map to concurrency conflict.
14. 410 on delta maps to cursor invalid.
15. 410 outside delta maps to provider-refused server error.
16. Unknown 404 maps to the context resource's `NotFound`.
17. Graph error body request id and client request id are preserved.
18. Unparseable Graph error body falls back by HTTP status and keeps
    support-only body text.
19. `bifrost_net::Error::Network` delegates to net conversion with
    Graph provider/protocol context.
20. Local unsupported push subscribe maps to `Unsupported(PushSubscribe)`.
21. Cursor protocol mismatch maps to schema incompatible.
22. Cursor payload JSON failure maps to cursor invalid.
23. Mutation 2xx item becomes applied.
24. Mutation destroy 404 becomes skipped.
25. Mutation non-destroy 404 becomes failed `NotFound(Message)`.
26. Mutation 412 becomes failed concurrency conflict, not skipped.
27. Mutation 429 captures retry-after and throttle scope.
28. Missing etag for required mutation maps to `Protocol(MissingField)`.
29. Send draft-create success plus send failure preserves draft id in
    support diagnostics and non-idempotent context.
30. Blob 405 remains a non-byte-stream warning.
31. Blob range-not-honored delegates to net conversion.
32. EWS HTTP 401 maps to Graph account auth with `Protocol::Ews`.
33. EWS SOAP access denied maps to permission denied.
34. EWS SOAP invalid watermark maps to cursor invalid when scope is
    known.
35. EWS XML parse failure maps to protocol parse failure.
36. Webhook delete 404 is treated as success.
37. Webhook renewal failure conversion produces a structured
    `AccountError` for telemetry without changing `WatchEvent`
    disconnect/reconnect behavior.

Existing tests that cover cursor encoding, inventory projection,
subscription resource construction, EWS XML parsing, and blob handle
construction should remain. Update only the error assertions they need
after the old types are removed.

## Exit criteria

- `GraphClient` no longer returns `Result<T, String>` for internal
  HTTP helpers.
- `net_error(service, err) -> String` is gone.
- Graph HTTP status, headers, body, error code, inner error, request
  id, and retry-after evidence survive to the account mapper.
- EWS transport/status/SOAP/XML errors survive as structured errors.
- `account/graph_error.rs` or equivalent exists and maps every
  structured Graph error through `AccountErrorBuilder`.
- All Graph REST errors stamp `Provider::Microsoft` and
  `Protocol::Graph`; all EWS fallback errors stamp
  `Provider::Microsoft` and `Protocol::Ews`. Verified by grep on
  every conversion call site.
- All 429, 507, and `ErrorQuotaExceeded` mappings populate
  `throttle_scope` (defaulting to `Tenant` for Graph REST per the
  Throttle Scope section); audit by inspecting each rate/quota row.
- `idempotency_override(false)` is set on every non-idempotent write
  call site (`Send`, `DraftCreate`, `DraftUpdate`, `DraftSend`,
  `BulkMove`, `AddToContainer`, `RemoveFromContainer`,
  `AttachmentUpload`, `ContainerCreate`, `ContainerRename`,
  `ContainerMove`, `ContainerDelete`, `IdentityUpdate`,
  `VacationSet`). Audit by grep against the landed `is_idempotent`
  non-idempotent list.
- `WireCause::MalformedResponse { protocol }` carries
  `Protocol::Graph` for Graph JSON parse failures and
  `Protocol::Ews` for EWS XML parse failures. No conversion path
  emits `MalformedResponse` without setting `protocol`.
- No recovery decision depends on substring matching
  `error.message`.
- Delta-token rejection maps to `SyncState(CursorInvalid)` with cursor
  scope. Without cursor scope, the mapper does not silently fall back
  to `RestartAccount`; missing scope is a producer bug surfaced in
  the audit.
- Cursor envelope/schema failures map to the documented sync-state
  categories.
- `If-Match` 412 maps to `ConcurrencyConflict`.
- `$batch` item bodies are parsed when present.
- Per-item mutation helpers return `ItemOutcome<MutationSuccess>` and
  are ready for Phase 3 trait migration.
- Per-item 429 emits per-item Failed with `not_before` and
  `throttle_scope`; no trailing stream-level rate-limit termination
  is emitted on the same batch.
- Webhook renewal failures are converted for telemetry/health without
  changing `WatchEvent`'s public shape.
- Blob 405/reference attachment warning behavior is preserved.
- EWS Subscribe failures use `AccountOperation::PushSubscribe`;
  GetStreamingEvents failures use `AccountOperation::PushStream`.
  Do not collapse the two.
- Every kind/cause pair produced by `into_account_error` satisfies
  `recovery::kind_matches_cause`.
- No source comments point at files under `plans/`.

Compilation and workspace-wide checks are Phase 3 work. Do not run
`brokkr check` merely for this planning phase.

## Audit checklist for the implementation agent

1. Search for `Result<.*, String>` in `crates/graph/src`. Only parser
   helpers that are intentionally local and not crossing account or
   transport boundaries may remain.
2. Search for `contains("` and `to_ascii_lowercase()` in Graph error
   paths. They must not drive recovery.
3. Search for `Graph API error` formatted strings. They should be
   gone from control flow.
4. Verify `GraphError::Net` preserves the original `bifrost_net::Error`.
5. Verify `bifrost_net::Error::Status` is parsed as Graph or EWS
   response before generic net fallback.
6. Verify `WireCause::Graph` is attached for every Graph error-code
   conversion.
7. Verify unknown Graph codes preserve `native_code`.
8. Verify request ids from headers and `innerError` are preserved.
9. Verify delta 410/invalid token requires cursor context and derives
   engine restart.
10. Verify mutation 412 is concurrency conflict, not skipped.
11. Verify destroy 404 remains successful skipped semantics.
12. Verify per-item `$batch` 429 carries retry-after and throttle
    scope.
13. Verify local unsupported operations use
    `AccountErrorKind::Unsupported(AccountOperation::...)`.
14. Verify EWS errors use `Protocol::Ews`.
15. Verify webhook renewal still emits the same `WatchEvent`
    disconnect/reconnect sequence.
16. Verify GraphSignal additions requested under the wire-enum escape
    hatch are listed in the audit if `Unknown { code }` was used for
    stable Microsoft codes.
17. Verify no live-server tests were added.
