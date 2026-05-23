# Error model: bifrost-gmail implementation plan

This is Phase 2.2 of `plans/error-model-roadmap.md`, one of the five
consumer-crate patches authored after `bifrost-net`.

Phase 2 is still on the intentionally broken feature branch. Author
the Gmail patch and deterministic tests, but do not run `brokkr`,
`cargo`, or `./diff_test.sh` from the crate agent.

## Required reading

- `CLAUDE.md`
- `plans/error-model-roadmap.md`
- `plans/error-model-convergence.md`
- `plans/error-model-net.md`
- `reference/gmail.md`
- `crates/types/src/error/` (the landed Phase 1 API)

## Kind/cause shape conventions

Landed-API quirks the agent must keep straight:

- `ServerErrorKind::Error { status: Option<u16> }` (kind side) vs
  `ServerCause::Error { status: u16 }` (cause side). The kind always
  wraps the code in `Some(_)` in this plan's tables; the cause uses
  bare `u16`. This is by design; do not "fix" one column.
- `AccessErrorKind::PermissionDenied` (no payload) vs
  `AccessCause::PermissionDenied { resource: Option<ResourceKind> }`
  (carries optional resource).

Every kind/cause pair below must satisfy
`recovery::kind_matches_cause` (asserted at runtime by
`AccountErrorBuilder::build`).

## Scope

Own only `crates/gmail/` files. Do not edit `crates/types/` even if a
new `GmailSignal` wire variant would be useful. If the existing
`GmailSignal` enum is insufficient, report the required variants to
the orchestrator under the roadmap's wire-enum escape hatch.

`bifrost-gmail` is implemented. This phase rewrites its internal error
boundary and removes the old recovery/template helpers. Phase 3 may
still perform final workspace surface reconciliation across all crates
(`SyncEvent::Fatal` rename, `MutationResult` to `ItemOutcome`, and
`Account` signature updates).

## Current state

Important files:

- `crates/gmail/src/error.rs`
  - `Error` is crate-private and currently has `Transport`,
    `HttpStatus`, `Auth`, `QuotaExhausted`, `Json`, `Base64`,
    `MalformedPayload`, and `InvalidInput`.
  - `From<bifrost_net::Error>` flattens the original net error into
    these variants and loses transmission-state evidence.
  - `Error::status` classifies quota by body substring through
    `looks_like_quota_error`.
- `crates/gmail/src/client.rs`
  - REST helpers return `crate::Result<T>` with the old `Error`.
  - `execute_builder` receives `bifrost-net` errors and immediately
    calls `Error::from_net`.
  - `parse_json_response` decodes successful JSON or calls
    `Error::status` on non-success.
- `crates/gmail/src/account/recovery.rs`
  - `classify_general_error` and `classify_history_error` map old
    `Error` to old `RecoveryClass`.
  - `account_error_from_gmail` projects old Gmail errors to old
    `bifrost_types::Error`.
  - `fatal_for_error` and `fatal_for_account_error` build old
    `Fatal`.
- `crates/gmail/src/account/mutation.rs`
  - `account_error_from_template` clones old account errors so the
    same error can be attached to every failed id. This helper must go.
  - `batchDelete` falls back to a TRASH label patch on 403.
- `crates/gmail/src/account/changes.rs`
  - History-id compaction is recognized by 404 or 410 in
    `classify_history_error`.
  - Cursor identity drift and malformed history ids currently become
    old fatal errors.
- `crates/gmail/src/account/inventory.rs`
  - Non-account scopes are old fatal unsupported errors.
  - Inventory page, hydration, and final profile-check failures call
    `classify_general_error`.
- `crates/gmail/src/account/push.rs`
  - Pub/Sub watch/stop errors become old account errors.
  - Renewer logs errors and emits `WatchEvent::Disconnected` /
    `Reconnected`.
- `crates/gmail/src/account/blobs.rs`
  - Range unsupported and invalid blob handles become old account
    errors.
  - Gmail attachment/base64 failures call `classify_general_error`.
- `crates/gmail/src/account/pim.rs`
  - PIM primitives return old `AccountError::Unsupported` and
    `AccountError::Other` in local paths.

The main evidence gaps are: original `bifrost_net::Error` is lost,
Gmail JSON error bodies are not parsed into stable reasons, quota
classification uses body substring matching, history-id failures are
status-only, and batch mutation error cloning relies on an obsolete
template helper.

## Files to modify

Primary files:

- `crates/gmail/src/error.rs`
- `crates/gmail/src/client.rs`
- `crates/gmail/src/types.rs`
- `crates/gmail/src/api.rs`
- `crates/gmail/src/lib.rs`
- `crates/gmail/src/account/recovery.rs`
- `crates/gmail/src/account/mod.rs`
- `crates/gmail/src/account/cursor.rs`
- `crates/gmail/src/account/changes.rs`
- `crates/gmail/src/account/inventory.rs`
- `crates/gmail/src/account/mutation.rs`
- `crates/gmail/src/account/push.rs`
- `crates/gmail/src/account/blobs.rs`
- `crates/gmail/src/account/scopes.rs`
- `crates/gmail/src/account/pim.rs`
- `crates/gmail/src/account/flags.rs`

Recommended new file:

- `crates/gmail/src/account/error.rs`

Use `crates/gmail/src/error.rs` for the crate-wide Gmail error shape
and wire JSON parser. Use `account/error.rs` for builder-based
conversion, context helpers, and item-outcome helpers. If keeping the
file name `recovery.rs` creates less churn, rewrite its contents
completely and ensure it no longer exports recovery classifiers.

## Files to delete

No whole file must be deleted.

Delete or replace these functions:

- `account_error_from_gmail`
- `fatal_for_error`
- `fatal_for_account_error`
- `classify_history_error`
- `classify_general_error`
- `account_error_from_template`
- `looks_like_quota_error`

The new mapper derives recovery through `AccountErrorBuilder`; it must
not return old `RecoveryClass` values directly.

## Dependencies

Required:

- Phase 1 types landed at commit `ac47289` (see
  `crates/types/src/error/` for the API surface and
  `plans/error-model-convergence.md` for the contract).
- Phase 2.1 `bifrost-net` conversion from
  `plans/error-model-net.md`.

Gmail already depends on `bifrost-net` and `bifrost-types`, so no new
workspace dependency is expected.

## Structured Gmail error shape

Preserve the existing crate-private `Error` name if convenient, but
change its content so it keeps structured wire evidence.

Recommended shape:

```rust
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum Error {
    Net(bifrost_net::Error),
    Response(Box<GmailResponseError>),
    JsonDecode {
        service: GmailService,
        source: serde_json::Error,
    },
    JsonEncode {
        service: GmailService,
        source: serde_json::Error,
    },
    Base64 {
        encoding: Base64Encoding,
        source: base64::DecodeError,
    },
    Local(GmailLocalError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GmailService {
    GmailApi,
}

#[derive(Debug)]
pub(crate) struct GmailResponseError {
    pub(crate) service: GmailService,
    pub(crate) status: u16,
    pub(crate) headers: GmailResponseHeaders,
    pub(crate) body: bytes::Bytes,
    pub(crate) envelope: Option<GmailErrorEnvelope>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct GmailResponseHeaders {
    pub(crate) retry_after: Option<std::time::Duration>,
    pub(crate) request_id: Option<String>,
    pub(crate) trace_id: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct GmailErrorEnvelope {
    pub(crate) code: Option<u16>,
    pub(crate) message: Option<String>,
    pub(crate) status: Option<String>,
    #[serde(default)]
    pub(crate) errors: Vec<GmailErrorDetail>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct GmailErrorDetail {
    pub(crate) domain: Option<String>,
    pub(crate) reason: Option<String>,
    pub(crate) message: Option<String>,
    pub(crate) location_type: Option<String>,
    pub(crate) location: Option<String>,
}
```

Parser rules:

- Gmail wraps errors under top-level `error`. Use a private wrapper
  type and store only the inner envelope.
- The stable classifier is the first non-empty `errors[].reason`,
  then top-level `status`, then HTTP status fallback.
- Preserve every `errors[].reason` in diagnostics or a helper
  iterator; Gmail sometimes carries multiple details.
- Raw Gmail message/body text is support-only diagnostic text.
- If the body is not valid Gmail JSON, keep the body bytes and
  classify by HTTP status fallback.
- Keep body capping from the old implementation, but store bytes so
  converters can attach support-only text later.

`client.rs` should keep the existing high-level helper names, but they
should return the new `crate::Error` variants without flattening net
errors. `Error::from_net` can remain only if it preserves the original
`bifrost_net::Error` as `Error::Net` or converts status bodies into
`Error::Response`.

Do not keep body substring classifiers such as
`looks_like_quota_error`.

## Local error shape

Replace old direct uses of `AccountError::Unsupported`,
`AccountError::Other`, range errors, and cursor errors with a local
typed variant before conversion.

Recommended:

```rust
#[derive(Debug)]
pub(crate) enum GmailLocalError {
    Unsupported {
        operation: AccountOperation,
        detail: Option<&'static str>,
    },
    InvalidRequest {
        operation: AccountOperation,
        detail: String,
    },
    InvalidCursor {
        kind: GmailCursorFailure,
        detail: String,
    },
    AccountIdentityMismatch {
        cursor_email: String,
        profile_email: String,
    },
    MissingField {
        field: &'static str,
        detail: String,
    },
    BlobRangeUnsupported {
        blob_id: String,
    },
    BlobRangeOutOfBounds {
        start: u64,
        total: u64,
    },
    Internal {
        detail: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GmailCursorFailure {
    ProtocolMismatch,
    EnvelopeMismatch,
    SchemaMismatch,
    MalformedPayload,
}
```

Mapping:

- Unsupported methods and capability gaps:
  `AccountErrorKind::Unsupported(operation)` with
  `Cause::Request(RequestCause::Unsupported { operation })`.
- Caller-supplied invalid values:
  `Request(RequestErrorKind::Malformed)`.
- Provider success response missing required fields:
  `Protocol(ProtocolErrorKind::MissingField)`.
- Wrong cursor protocol/envelope/schema or wrong Gmail account:
  `SyncState(SyncStateErrorKind::SchemaIncompatible)`.
- Cursor JSON payload malformed:
  `SyncState(SyncStateErrorKind::CursorInvalid)`.
- Blob range unsupported:
  `Unsupported(AccountOperation::OpenBlobRange)`.
- Blob range out of bounds:
  `Request(RequestErrorKind::Malformed)`.

## Account conversion boundary

Add in `account/error.rs` or rewrite `account/recovery.rs` to contain:

```rust
use bifrost_types::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuilder,
    AccountErrorKind, AccountOperation, AuthCause, AuthErrorKind,
    Cause, CursorScope, DiagnosticText, ErrorScope, GmailSignal,
    Protocol, ProtocolErrorKind, Provider, RequestCause,
    RequestErrorKind, ResourceKind, ServerCause, ServerErrorKind,
    StateCause, SyncStateErrorKind, ThrottleScope, TransportCause,
    TransportErrorKind, TransportKind, WireCause,
};

#[derive(Clone, Debug)]
pub(crate) struct GmailErrorContext {
    pub(crate) operation: AccountOperation,
    pub(crate) scope: Option<ErrorScope>,
    pub(crate) cursor_scope: Option<CursorScope>,
    pub(crate) resource: Option<GmailResource>,
    pub(crate) throttle_scope: Option<ThrottleScope>,
    pub(crate) idempotency_override: Option<bool>,
    pub(crate) history_endpoint: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GmailResource {
    Account,
    Message,
    Thread,
    Label,
    Draft,
    Identity,
    Vacation,
    Blob,
    PubSubWatch,
}

pub(crate) fn into_account_error(
    error: crate::Error,
    ctx: GmailErrorContext,
) -> AccountError;
```

Helper constructors are encouraged:

```rust
impl GmailErrorContext {
    pub(crate) fn open() -> Self;
    pub(crate) fn inventory() -> Self;
    pub(crate) fn changes() -> Self;
    pub(crate) fn hydrate_message(id: impl Into<String>) -> Self;
    pub(crate) fn mutation(operation: AccountOperation) -> Self;
    pub(crate) fn push_subscribe() -> Self;
    pub(crate) fn push_unsubscribe() -> Self;
    pub(crate) fn open_blob(id: impl Into<String>) -> Self;
    pub(crate) fn send() -> Self;
    pub(crate) fn draft(operation: AccountOperation) -> Self;
}
```

Every conversion must attach:

- `.provider(Provider::Gmail)`
- `.protocol(Protocol::Gmail)`
- `.operation(ctx.operation)`
- `.scope(scope)` when known
- `.idempotency_override(false)` for send and draft-send operations
  and for in-flight non-idempotent write operations when needed

For `Error::Net(error)`, delegate to:

```rust
bifrost_net::into_account_error(
    error,
    bifrost_net::NetErrorContext {
        provider: Some(Provider::Gmail),
        protocol: Protocol::Gmail,
        operation: ctx.operation, // non-optional on both sides
        scope: ctx.scope.clone(),
    },
)
```

Gmail-specific classification on preserved net bodies. When the net
error is `Error::RateLimited { final_response, .. }` or
`Error::RetryBudgetExhausted { final_response: Some(r), .. }`,
delegate first, then ALSO parse `final_response.body` for the
Gmail JSON error reason (`rateLimitExceeded`, `userRateLimitExceeded`,
`dailyLimitExceeded`, `quotaExceeded`, etc.) and re-build the
`AccountError` with the structured `GmailSignal` if found. Without
this step, the protocol crate loses the reason-string discrimination
that drives the per-quota throttle-scope decisions and the
quota-vs-rate distinction. The body is JSON, capped at
`STATUS_BODY_CAP`. If parsing fails, keep the net-derived
classification.

Do this only for transport-like net errors. For
`bifrost_net::Error::Status`, parse the Gmail JSON body first and run
the Gmail-specific mapping below. Gmail reason codes are more precise
than generic HTTP status mapping.

## Builder rules

Every Gmail-specific conversion must use:

```rust
AccountErrorBuilder::new(kind, primary_cause)
```

When a Gmail response exists:

- Push `Cause::Wire(WireCause::Gmail(signal))`.
- Set `.status(status)`.
- Set `.native_code(reason_or_status)` to the primary Gmail reason or
  top-level status string.
- Add request id / trace id headers when present.
- Add Gmail `message`, detail messages, and unparsed body text as
  support-only `DiagnosticText`.
- Add `.retry_not_before(...)` and `.throttle_scope(...)` for
  rate-limit or quota failures when `Retry-After` exists.

Do not build `AccountErrorKind::Transport` for a completed Gmail HTTP
status response. HTTP 4xx/5xx with a body is provider/server/request
classification, not transport.

## Gmail wire signals

The current Phase 1 `GmailSignal` variants are:

- `InvalidQuery`
- `FailedPrecondition`
- `InvalidCredentials`
- `AuthError`
- `QuotaExceeded`
- `RateLimitExceeded`
- `UserRateLimitExceeded`
- `Forbidden`
- `NotFound`
- `PreconditionFailed`
- `BackendError`
- `HistoryNotFound`
- `PubSubSubscriptionDeleted`
- `PubSubSubscriptionExpired`
- `Unknown { code }`

Map exact Gmail `errors[].reason` values to these where possible.
Use `GmailSignal::Unknown { code }` for stable Google reasons that do
not yet have variants, and report requested additions in the audit.

The Phase 2 mapper uses `GmailSignal::Unknown { code }` for these
stable Google reasons, which are NOT escape-hatch escalation requests:

- `dailyLimitExceeded`
- `domainPolicy`
- `insufficientPermissions`
- `invalidArgument`
- `conditionNotMet`
- `notAuthorizedToAccessThisResource`

Convergence (§"Stable outcome") says classification gaps via
`Unknown { code }` "surface in telemetry so the gap can be closed in
a later release." That is the durable policy. The Gmail crate does
NOT request typed variants for these reasons in Phase 2; it relies on
`native_code` and telemetry filters to drive prioritization. If a
specific reason later proves load-bearing for product behavior (i.e.
a consumer needs to branch on it), Phase 3.6 or a follow-up may
promote it to a typed variant; promotion criterion is "a consumer
match arm exists," not "the reason is common."

## Gmail reason mapping

Prefer Gmail `errors[].reason` over top-level `status`, then HTTP
status fallback.

| Gmail reason/status | Account kind | Primary cause | Wire signal | Notes |
| --- | --- | --- | --- | --- |
| `invalidQuery` | `Request(Malformed)` | `Request(Malformed { detail })` | `InvalidQuery` | Search query or filter shape. |
| `invalidArgument` | `Request(Malformed)` | `Request(Malformed { detail })` | `Unknown { code }` | Bad caller parameter. |
| `failedPrecondition` on history endpoint | `SyncState(CursorInvalid)` | `State(CursorInvalid)` | `FailedPrecondition` | History id cannot be used. |
| `failedPrecondition` elsewhere | `Server(Error { status })` or `Request(Malformed)` | Context dependent | `FailedPrecondition` | Prefer malformed for caller precondition errors. |
| `invalidCredentials` | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` | `InvalidCredentials` | Token is not usable. |
| `authError` | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` | `AuthError` | Refresh/reauthorize. |
| `forbidden` | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` | `Forbidden` | Generic permission denial. |
| `insufficientPermissions` | `Authorization(InsufficientScope)` | `Access(InsufficientScope { needed })` | `Unknown { code }` | Use a stable needed string for Gmail scopes. |
| `domainPolicy` | `Authorization(PolicyBlocked)` | `Access(PolicyBlocked)` | `Unknown { code }` | Workspace/admin policy. |
| `quotaExceeded` | `Server(QuotaExhausted)` | `Server(QuotaExhausted { retry_after })` | `QuotaExceeded` | Add throttle scope. |
| `rateLimitExceeded` | `Server(RateLimited)` | `Server(RateLimited { retry_after })` | `RateLimitExceeded` | Add throttle scope. |
| `userRateLimitExceeded` | `Server(RateLimited)` | `Server(RateLimited { retry_after })` | `UserRateLimitExceeded` | Usually account throttle. |
| `dailyLimitExceeded` | `Server(QuotaExhausted)` | `Server(QuotaExhausted { retry_after })` | `Unknown { code }` | Request signal addition. |
| `notFound` on history endpoint | `SyncState(CursorInvalid)` | `State(CursorInvalid)` | `HistoryNotFound` | Gmail compacted history id. |
| `notFound` elsewhere | `NotFound(resource)` | `Request(NotFound { what, id })` | `NotFound` | Resource from context. |
| `conditionNotMet` | `ConcurrencyConflict` | `State(ConcurrencyConflict)` | `Unknown { code }` | Use for conditional writes if Gmail ever surfaces them. |
| `preconditionFailed` | `ConcurrencyConflict` | `State(ConcurrencyConflict)` | `PreconditionFailed` | Same for HTTP 412. |
| `backendError` | `Server(Unavailable)` | `Server(Unavailable { retry_after })` | `BackendError` | Retryable. |
| `pubsubSubscriptionDeleted` during renewer health loop | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` | `PubSubSubscriptionDeleted` | Renewer will recreate; transient to the engine. |
| `pubsubSubscriptionDeleted` returned to `push_subscribe` caller | `SyncState(CursorInvalid)` | `State(CursorInvalid)` | `PubSubSubscriptionDeleted` | Caller's watch reference is dead; engine restarts the push scope. |
| `pubsubSubscriptionExpired` during renewer health loop | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` | `PubSubSubscriptionExpired` | Same as deleted-during-renewer. |
| `pubsubSubscriptionExpired` returned to `push_subscribe` caller | `SyncState(CursorInvalid)` | `State(CursorInvalid)` | `PubSubSubscriptionExpired` | Same as deleted-to-caller. |
| Unknown reason with 401 | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` | `Unknown { code }` | Preserve code as native. |
| Unknown reason with 403 | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` | `Unknown { code }` | Preserve code as native. |
| Unknown reason with 404 on history | `SyncState(CursorInvalid)` | `State(CursorInvalid)` | `Unknown { code }` | History context wins. |
| Unknown reason with 404 | `NotFound(resource)` | `Request(NotFound { what, id })` | `Unknown { code }` | Resource from context. |
| Unknown reason with 429 | `Server(RateLimited)` | `Server(RateLimited { retry_after })` | `Unknown { code }` | Add throttle scope. |
| Unknown reason with 5xx | `Server(Unavailable)` | `Server(Unavailable { retry_after })` | `Unknown { code }` | Retryable. |
| Unknown otherwise | `Server(Error { status: Some(status) })` for HTTP status, else `Protocol(Unknown)` | Server cause with bare `u16` or `Wire(MalformedResponse)` | `Unknown { code }` | Do not inspect message text. |

## HTTP status fallback

Use when no useful Gmail reason exists.

| Status | Account kind | Primary cause | Notes |
| --- | --- | --- | --- |
| `400` on history endpoint | `SyncState(CursorInvalid)` if response indicates stale history, otherwise `Request(Malformed)` | Context plus reason decides. |
| `400` otherwise | `Request(Malformed)` | Bad request/query/body. |
| `401` | `Authentication(ReauthorizationRequired)` | Auth refresh or reauthorize. |
| `403` with quota/rate reason | `Server(QuotaExhausted)` or `Server(RateLimited)` | Reason decides. |
| `403` otherwise | `Authorization(PermissionDenied)` | Access denied. |
| `404` on history endpoint | `SyncState(CursorInvalid)` | Restart account cursor scope. |
| `404` otherwise | `NotFound(resource)` | Resource from context. |
| `410` on history endpoint | `SyncState(CursorInvalid)` | Restart account cursor scope. |
| `410` otherwise | `Server(Error { status: Some(410) })` | Provider refused stale resource. |
| `412` | `ConcurrencyConflict` | Conditional state conflict. |
| `413` | `Request(Malformed)` | Request too large. |
| `429` | `Server(RateLimited)` | Add retry hint and throttle scope. |
| `500` | `Server(Unavailable)` | Retryable. |
| `502` | `Server(Unavailable)` | Retryable. |
| `503` | `Server(Unavailable)` | Retryable. |
| `504` | `Server(Unavailable)` | Retryable. |
| Other 4xx | `Server(Error { status: Some(status) })` | Provider refused by builder. |
| Other 5xx | `Server(Unavailable)` | Retryable. |

## Throttle scope and retry hints

Gmail quotas are mostly per-user and quota-unit based. The current
client has no explicit auth-flow or quota-bucket enum, so use:

- `ThrottleScope::Account` for Gmail API account operations.
- `ThrottleScope::Mailbox` only when a failure is clearly tied to one
  message/label resource lane.
- `ThrottleScope::Provider` for provider-wide 5xx only when no account
  scope is meaningful.

For `quotaExceeded`, `dailyLimitExceeded`, `rateLimitExceeded`,
`userRateLimitExceeded`, HTTP 429, or `bifrost_net::Error::RateLimited`:

- set `ServerCause::{QuotaExhausted,RateLimited} { retry_after }`
- set `.retry_not_before(SystemTime::now() + retry_after)` when
  addition succeeds
- set `.throttle_scope(scope)`

Use the Phase 2.1 `bifrost-net` retry-after parser when available.
If a Gmail response lacks `Retry-After`, leave the hint as `None`.
Do not hard-code the old one-second retry in the mapper.

## Cursor and history mapping

Gmail has only `CursorScope::Account`.

Map:

| Condition | Account kind | Recovery derived |
| --- | --- | --- |
| Wrong cursor protocol | `SyncState(SchemaIncompatible)` | `Engine(SchemaIncompatible)` |
| Envelope version mismatch | `SyncState(SchemaIncompatible)` | `Engine(SchemaIncompatible)` |
| Schema version mismatch | `SyncState(SchemaIncompatible)` | `Engine(SchemaIncompatible)` |
| Cursor JSON malformed | `SyncState(CursorInvalid)` with `ErrorScope::Cursor(CursorScope::Account)` | `Engine(RestartScope(CursorScope::Account))` |
| Cursor profile email does not match open account | `SyncState(SchemaIncompatible)` | `Engine(SchemaIncompatible)` |
| `changes_stream` missing cursor | `Protocol(ContractViolation)` | Provider contract/client bug depending context. |
| Profile check during changes returns different email | `SyncState(SchemaIncompatible)` | `Engine(SchemaIncompatible)` |
| `profile.historyId` is not `u64` | `Protocol(MissingField)` or `Protocol(ContractViolation)` | Provider contract violation. |
| History response `historyId` is not `u64` | `Protocol(MissingField)` | Provider contract violation. |
| `users.history.list` returns 404/410/history-not-found | `SyncState(CursorInvalid)` | Restart account cursor. |

Because Gmail history is account-wide, stale history ALWAYS attaches
`ErrorScope::Cursor(CursorScope::Account)`, which makes the central
recovery mapper derive `Engine(RestartScope(CursorScope::Account))`
(per convergence's recovery table row `SyncState(CursorInvalid) with
scope`). The mapper falls back to `Engine(RestartAccount)` only when
the cursor scope is absent — Gmail's mapper MUST pass the cursor
scope so the engine restarts just the changes stream, not the open
session, the push subscription, and unrelated streams.

The exception is when the failure indicates the entire Gmail account
state is unrecoverable (account email no longer matches the cursor
profile, schema incompatibility) — those map to `SyncState(SchemaIncompatible)`
or to a missing-scope `CursorInvalid`, which the mapper routes to
`Engine(RestartAccount)`. The two routes are distinct on purpose;
Gmail's mapper never punts to the broader route when the narrower
one is available.

Non-account scopes in inventory, push subscribe, and cursor
establishment are unsupported, not fatal provider errors.

## Inventory and hydration mapping

`inventory_stream`:

- Non-account scope:
  `Unsupported(AccountOperation::SyncInventory)`.
- `users.messages.list` failure:
  `into_account_error(error, GmailErrorContext::inventory())`.
- Per-message `users.messages.get` failure during inventory:
  stream termination unless Phase 3 converts inventory hydration into
  per-item lanes. Preserve the message id in support diagnostics.
- Final `users.getProfile` failure for checkpoint:
  structured account error with operation `EstablishCursor` or
  `SyncInventory`.
- Missing required fields in successful message/profile responses:
  `Protocol(MissingField)`.

`get_stream`:

- Failed hydration of a requested id should become a per-item failure
  when Phase 3 moves this stream to item outcomes. Until then, keep a
  helper that can produce the per-id `AccountError` and terminate the
  stream at the current call site.
- Base64 raw-message decode failure:
  `Protocol(ParseFailed)` or `Request(Malformed)` depending whether
  Gmail returned malformed data or local caller data was malformed.
  For Gmail response data, use protocol parse failure.
- Missing `raw` for a raw projection:
  `Protocol(MissingField)`.

## Mutation mapping

Bulk mutation streams will move to `ItemOutcome<MutationSuccess>` in
Phase 3. This phase should delete `account_error_from_template` and
make the classification helpers ready for that migration.

Recommended replacement surface:

```rust
pub(crate) fn mutation_error(
    ids: &[ObjectId],
    error: crate::Error,
    ctx: GmailErrorContext,
) -> Vec<ItemOutcome<MutationSuccess>>;
```

The return type is `Vec<ItemOutcome<MutationSuccess>>` directly, not
a Gmail-specific `MutationApply` wrapper. Each `ItemOutcome` variant
follows the landed Phase 1 enum: `Succeeded(BatchSuccess { item,
output: MutationSuccess::{Applied, Skipped} })`,
`Failed(BatchFailure { item, error })`, or
`Uncertain(BatchUncertain { item, error })`. The Phase 3 trait
migration moves these from a helper return value into the streaming
`AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>` signature
on `Account`.

Phase 3 shape:

- Success from `batchModify` / `batchDelete`:
  `Succeeded(MutationSuccess::Applied)` for every id.
- Empty label patch or unsupported flag set:
  `Succeeded(MutationSuccess::Skipped)` for every id. Unsupported
  flags are a no-op chosen by the driver, not a provider error.
- HTTP/auth/server error before a batch response:
  terminate the stream with `AccountError`; no item result is known.
- Permanent request/provider error for a transmitted batch:
  failed item for every id, each with its own cloned `AccountError`.
  `AccountError` is cloneable, so no template helper is needed.
- 404 for all ids in a batch:
  failed `NotFound(Message)` lanes.
- 429/quota/rate:
  terminate the stream with retryable server error if no per-id
  outcome is known.

TRASH fallback:

- Keep the current behavior: `batchDelete` can fall back to adding
  `TRASH` and removing `INBOX`.
- Do not treat every 403 as a fallback trigger. Prefer stable reasons:
  `insufficientPermissions`, `forbidden` for delete scope, or a
  Gmail status that indicates the token cannot permanently delete.
- If primary delete fails and TRASH fallback succeeds, emit success
  lanes and no error.
- If both primary delete and fallback fail, return one
  `AccountError` for the fallback failure and use
  `AccountErrorBuilder::push_cause(primary_delete_failure_outermost_cause)`
  to attach the primary delete's outermost typed cause as a secondary
  cause. The builder supports arbitrary additional causes (the chain
  is a `Vec<Cause>`, not a linked list), so the double-cause is safe.
  Also push the primary delete's diagnostic text via
  `.text(DiagnosticText { value, visibility: SupportOnly })` so the
  support export carries both forensic signals.

Gmail has no `If-Match` or documented replay token on these endpoints.
Keep `IdempotencyKey` engine-side and use
`idempotency_override(false)` only for non-idempotent send/draft-send
paths where transport evidence is in-flight.

## PIM mapping

Map current local and REST failures explicitly:

| Current condition | Account kind | Notes |
| --- | --- | --- |
| unsupported primitive (`set_keyword`, `set_category`, etc.) | `Unsupported(operation)` | Use concrete `AccountOperation`. |
| unsupported target shape | `Unsupported(operation)` | For non-message/thread targets. |
| pre-uploaded attachment handle in send/draft | `Unsupported(Send)` or draft operation | Gmail has no standalone message attachment upload primitive. |
| invalid Gmail page cursor UTF-8 | `Request(Malformed)` | Caller supplied opaque cursor. |
| unsupported search filter | `Unsupported(Search)` | Search AST valid, Gmail cannot express it. |
| unsupported `container_move` | `Unsupported(ContainerMove)` | Gmail labels are flat. |
| non-label container create | `Unsupported(ContainerCreate)` | Gmail containers are labels here. |
| missing id in successful create/send/draft response | `Protocol(MissingField)` | Provider contract violation. |
| failed `users.messages.send` after local MIME build | provider or transport mapping with non-idempotent send context | Preserve in-flight evidence. |
| local MIME construction failure | `Request(Malformed)` | Caller supplied invalid compose data or unsupported attachment handle. |

`send_message`, `draft_create`, `draft_update`, and `draft_send` must
not flatten Gmail errors into `Other`. They should pass
`GmailErrorContext::send()` or the matching draft operation so the
builder knows the operation and idempotency.

## Pub/Sub push mapping

`push_subscribe` and `push_unsubscribe` are account operations and
should return `AccountError` after Phase 3 trait migration.

Rules:

- Missing Pub/Sub config:
  `Unsupported(AccountOperation::PushSubscribe)`.
- Empty scope list or any non-account scope:
  `Unsupported(AccountOperation::PushSubscribe)`.
- `users.watch` failure:
  `into_account_error(error, GmailErrorContext::push_subscribe())`.
- Handle JSON encode/decode failure:
  `Request(Malformed)` for caller-provided unsubscribe handles, or
  `Protocol(ContractViolation)` for local encode impossibility.
- `users.stop` failure:
  `into_account_error(error, GmailErrorContext::push_unsubscribe())`.
- Multiple active handles still share one Gmail watch; do not change
  that behavior.

Renewer:

- Keep current health stream behavior:
  `WatchEvent::Disconnected` once after failure and
  `WatchEvent::Reconnected` after a successful renew.
- Convert renewal failures to structured `AccountError` for tracing
  and optional stored health state. `WatchEvent` has no error payload,
  so do not change its public shape in Phase 2.
- Auth/authz failures during renewal should be logged with
  authentication/authorization classification, not generic transport.
- Rate/quota/server failures should keep the retry loop and use
  retryable server classification.
- Subscription deleted/expired signals should trigger a fresh
  `users.watch`. If the implementation can force a sync invalidation
  after recreation, do so; otherwise record the limitation in the
  audit.

## Blob mapping

Gmail blobs are JSON attachment bodies with base64url data. HTTP range
is not supported.

Rules:

- Invalid blob handle JSON from caller:
  `Request(Malformed)`.
- `open_blob_range` when `supports_range` is false:
  `Unsupported(AccountOperation::OpenBlobRange)`.
- Local range out of bounds in the unreachable slicing branch:
  `Request(Malformed)`.
- `users.messages.attachments.get` failure:
  `into_account_error(error, GmailErrorContext::open_blob(id))`.
- Attachment body missing `data`:
  `Protocol(MissingField)`.
- Base64url decode failure on Gmail attachment data:
  `Protocol(ParseFailed)`.

## Header and diagnostic preservation

Capture from Gmail or Google API responses when present:

- `Retry-After` for retry/quota errors.
- `x-google-request-id`, `x-goog-request-id`, or equivalent stable
  request id header in `.request_id(...)`.
- trace-style headers in `.trace_id(...)` when available.

If the exact header set changes while implementing, prefer preserving
all unknown `x-google-*` / `x-goog-*` diagnostic headers as
support-only text instead of adding ad hoc fields.

## Existing tests to replace

The current tests in `account/recovery.rs` assert old recovery values
and old `bifrost_types::Error` variants. Delete those assertions with
the old helpers and replace them with builder-based `AccountError`
tests.

The current mutation test surface must stop depending on
`account_error_from_template`. `AccountError` is cloneable in the new
model, so failed per-id outcomes can carry direct clones.

## Test plan

Keep tests deterministic. Do not add live Gmail, live Pub/Sub, Docker,
fixed ports, external accounts, or webhook servers. Use synthetic
Gmail JSON bodies, `bifrost_net::Error` values, local cursor payloads,
and local mutation helpers.

Minimum test list:

1. `invalidQuery` maps to `Request(Malformed)`.
2. `invalidArgument` maps to `Request(Malformed)` using
   `GmailSignal::Unknown`.
3. `invalidCredentials` maps to
   `Authentication(ReauthorizationRequired)`.
4. `authError` maps to `Authentication(ReauthorizationRequired)`.
5. `forbidden` maps to `Authorization(PermissionDenied)`.
6. `insufficientPermissions` maps to insufficient scope using
   `GmailSignal::Unknown`.
7. `domainPolicy` maps to `Authorization(PolicyBlocked)`.
8. `quotaExceeded` maps to `Server(QuotaExhausted)` with throttle
   scope.
9. `rateLimitExceeded` maps to `Server(RateLimited)` with throttle
   scope.
10. `userRateLimitExceeded` maps to `Server(RateLimited)` with
    account throttle scope.
11. `failedPrecondition` on history maps to
    `SyncState(CursorInvalid)`.
12. history 404 maps to cursor invalid.
13. history 410 maps to cursor invalid.
14. non-history 404 maps to `NotFound(resource)`.
15. HTTP 500/503 maps to retryable server unavailable.
16. Unparseable Gmail error body falls back by HTTP status and keeps
    support-only text.
17. `bifrost_net::Error::Network` delegates to net conversion with
    Gmail provider/protocol context.
18. Wrong cursor protocol maps to schema incompatible.
19. Truncated cursor bytes map to cursor invalid.
20. Cursor profile-email mismatch maps to schema incompatible.
21. Non-account inventory scope maps to unsupported sync inventory.
22. Mutation unsupported flags become skipped success lanes.
23. Mutation permanent Gmail 404 becomes failed `NotFound(Message)`
    lanes.
24. Mutation retryable Gmail 429 terminates the stream with retryable
    account error.
25. `batchDelete` fallback success emits no error.
26. `batchDelete` primary and fallback failure yields one structured
    error with primary failure retained diagnostically.
27. Missing Pub/Sub config maps to unsupported push subscribe.
28. Pub/Sub handle JSON decode failure maps to malformed request.
29. Pub/Sub renewal failure conversion preserves health events.
30. `open_blob_range` unsupported maps to unsupported open-blob-range.
31. Invalid blob id maps to malformed request.
32. Gmail attachment base64 failure maps to protocol parse failure.
33. Send failure carries non-idempotent send context.
34. Request id and retry-after headers are preserved.

Existing projection, flag, cursor, Pub/Sub renewal-delay, and blob
handle tests should remain; update only their old-error assertions.

## Exit criteria

- Original `bifrost_net::Error` evidence survives to Gmail account
  conversion.
- Gmail JSON error bodies are parsed into stable reason/status fields.
- `looks_like_quota_error` and body substring recovery are gone.
- `account/recovery.rs` no longer exports any of: `to_recovery`,
  `to_account_error`, `account_error_from_gmail`, `fatal_for_error`,
  `fatal_for_account_error`, `classify_history_error`,
  `classify_general_error`, `account_error_from_template`,
  `looks_like_quota_error`. Grep `crates/gmail/` for each identifier;
  zero hits required.
- `account_error_from_template` is deleted (called out separately
  because the audit checklist greps for it).
- Every kind/cause pair produced by `into_account_error` and
  `mutation_error` satisfies `recovery::kind_matches_cause`.
- Every Gmail REST error stamps `Provider::Gmail` and
  `Protocol::Gmail`.
- Every Gmail wire reason adds `WireCause::Gmail`.
- Unknown Gmail reasons preserve `native_code`.
- History-id rejection maps to `SyncState(CursorInvalid)` with
  account cursor scope.
- Pub/Sub watch and renewer failures are classified structurally while
  preserving current health-stream behavior.
- TRASH fallback reports no error when the fallback succeeds and one
  structured error when both paths fail.
- Blob range unsupported and base64 decode failures map to the
  documented new categories.
- Per-item mutation helpers are ready for Phase 3
  `ItemOutcome<MutationSuccess>` migration.
- No source comments point at files under `plans/`.

Compilation and workspace-wide checks are Phase 3 work. Do not run
`brokkr check` merely for this planning phase.

## Audit checklist for the implementation agent

1. Search for `account_error_from_template`. It must be gone.
2. Search for `looks_like_quota_error`, `contains("quota")`,
   `rate limit`, and `to_ascii_lowercase()` in error paths. They must
   not drive recovery.
3. Search for `classify_general_error` and `classify_history_error`.
   They should be gone or rewritten as builder-based conversion
   helpers that return `AccountError`.
4. Verify `Error::Net` preserves the original `bifrost_net::Error`.
5. Verify `bifrost_net::Error::Status` is parsed as Gmail JSON before
   generic net fallback.
6. Verify `WireCause::Gmail` is attached for every Gmail reason
   conversion.
7. Verify unknown Gmail reasons preserve `native_code`.
8. Verify history 404/410 cannot become generic not-found outside
   history context.
9. Verify non-account scopes are unsupported, not fatal provider
   errors.
10. Verify mutation retry/auth failures still terminate the stream
    when no per-id result is known.
11. Verify TRASH fallback distinguishes fallback-triggering 403 from
    generic permission denial.
12. Verify Pub/Sub renewer keeps the same disconnect/reconnect
    behavior.
13. Verify send and draft-send use non-idempotent context.
14. Verify no live-server tests were added.
