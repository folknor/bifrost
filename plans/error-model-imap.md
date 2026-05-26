# Error model: bifrost-imap implementation plan

This is Phase 2.2 of `plans/error-model-roadmap.md`.

`bifrost-imap` does not use `bifrost-net` for protocol traffic. It
owns TCP, TLS, IMAP command dispatch, continuation handling, response
classification, IDLE, and pipeline execution. That makes this crate
responsible for preserving command attempt state before it crosses the
`bifrost_types::Account` boundary.

The current code is the source of truth. This plan is written against:

- `reference/imap.md`
- `crates/imap/src/error.rs`
- `crates/imap/src/connection/`
- `crates/imap/src/account/`
- `crates/types/src/error/` (the landed Phase 1 API)

Required reading: `plans/error-model-roadmap.md`,
`plans/error-model-convergence.md`, and the landed types module above.

## Kind/cause shape conventions

Landed-API quirks the agent must keep straight:

- `ServerErrorKind::Error { status: Option<u16> }` (kind side) vs
  `ServerCause::Error { status: Option<u16> }` (cause side). Both
  carry `Option<u16>` after the Phase 1 amendment. IMAP is the
  primary motivating case for the `None` variant: tagged `NO`/`BAD`
  responses without a numeric `[RESPONSE-CODE]` map to
  `ServerCause::Error { status: None }`. Pre-amendment plan revisions
  forced a `status: 0` sentinel; that workaround is no longer
  permitted. Use `None`.
- `AccountErrorKind::Protocol(ProtocolErrorKind)` - table rows use the
  shorthand `Protocol(Unknown)`, `Protocol(ContractViolation)`,
  `Protocol(ParseFailed)`, `Protocol(PartialResponse)`. These all
  refer to variants of `ProtocolErrorKind`; the shorthand is for
  readability only.
- `AccessErrorKind::PermissionDenied` (no payload) vs
  `AccessCause::PermissionDenied { resource: Option<ResourceKind> }`
  (carries optional resource).

Every kind/cause pair below must satisfy
`recovery::kind_matches_cause` (asserted at runtime by
`AccountErrorBuilder::build`).

## Scope

Only `crates/imap/` should change in this phase, unless implementation
proves that `crates/types/src/error/cause.rs` is missing an IMAP
response-code variant that already exists in
`crates/imap/src/types/response.rs`.

Do not redesign the account trait, stream event enum, or mutation
result shape here. If Phase 3 still needs to adapt `SyncEvent::Fatal`,
`MutationResult`, or public `Account` signatures to the landed Phase 1
types, leave those mechanical cross-crate edits for Phase 3 and make the
IMAP-side helpers ready for them.

Goals:

- Convert crate-private `crate::Error` into `bifrost_types::AccountError`
  through `AccountErrorBuilder`.
- Preserve IMAP response codes as
  `Cause::Wire(WireCause::Imap(ImapResponseCode::*))`.
- Preserve driver-owned command attempt state as
  `Cause::Attempt(AttemptCause { transmission_state })`.
- Replace old `ErrorCategory` and `Recovery` policy helpers.
- Replace account-local flattening into old `bifrost_types::Error`
  variants.
- Map QRESYNC and CONDSTORE downgrade failures through
  `SyncState(StrategyFailure)` so the builder derives
  `RecoveryClass::Engine(EngineDirective::DowngradeStrategy(_))`.
- Keep transparent QRESYNC downgrades as warnings when the stream
  continues successfully.

Non-goals:

- No live IMAP tests.
- No mock server harness.
- No `bifrost-net` dependency for IMAP protocol traffic.
- No public exposure of the raw IMAP protocol error taxonomy.

## Files to modify

Primary files:

- `crates/imap/src/error.rs`
- `crates/imap/src/lib.rs`
- `crates/imap/src/account/mod.rs`
- `crates/imap/src/account/blob.rs`
- `crates/imap/src/account/changes.rs`
- `crates/imap/src/account/factory.rs`
- `crates/imap/src/account/get.rs`
- `crates/imap/src/account/inventory.rs`
- `crates/imap/src/account/mutate.rs`
- `crates/imap/src/account/pim.rs`
- `crates/imap/src/account/pool.rs`
- `crates/imap/src/account/push.rs`
- `crates/imap/src/connection/auth.rs`
- `crates/imap/src/connection/lifecycle.rs`
- `crates/imap/src/connection/driver/mod.rs`
- `crates/imap/src/connection/driver/pipeline.rs`
- `crates/imap/src/connection/driver/wire_send.rs`
- `crates/imap/src/connection/driver/upgrade.rs`
- `crates/imap/src/connection/dispatch/*.rs`
- `crates/imap/src/connection/uid_ops.rs`
- `crates/imap/src/connection/seq_ops.rs`
- `crates/imap/src/connection/mailbox.rs`
- `crates/imap/src/connection/extensions.rs`
- `crates/imap/src/connection/helpers.rs`
- `crates/imap/src/connection/append.rs`
- `crates/imap/src/connection/sort_thread.rs`
- `crates/imap/src/connection/ergonomics.rs`
- `crates/imap/src/error_tests.rs`

Recommended new file:

- `crates/imap/src/account/error.rs`

Use the new file for the account-boundary mapper, context helpers, and
response-code mapping helpers. Keep low-level protocol error definitions
in `crates/imap/src/error.rs`.

## Files to delete

No whole file must be deleted.

Remove these obsolete items from `crates/imap/src/error.rs` after their
tests have been replaced:

- `ErrorCategory`
- `Recovery`
- `Error::category`
- `Error::recovery`

Remove or replace these old account helpers from
`crates/imap/src/account/mod.rs`:

- `account_error(err: Error) -> bifrost_types::Error`
- `fatal_event(err: Error) -> SyncEvent<T>` in its current old
  `RecoveryClass` form

`Error::response_code()` can remain if direct IMAP internals still use
it, but the account-boundary mapper should not rely on `ErrorCategory`.

## Dependencies

Required:

- Phase 1 types from `plans/error-model-types.md`.

Not required:

- Phase 2.1 `bifrost-net` migration. IMAP does not delegate retry,
  rate-limit, or HTTP status classification to `bifrost-net`.

## Current error shape

`crate::Error` is crate-private and cloneable. Current variants:

- `Io(Arc<std::io::Error>)`
- `Auth { text, code }`
- `No { text, code }`
- `Bad { text, code }`
- `Bye { text, code }`
- `Protocol(String)`
- `Parse(String)`
- `Timeout`
- `Closed`
- `StartTlsUnavailable`
- `AuthPolicy(AuthPolicyFailure)`
- `MissingCapability(String)`
- `AppendLimit { size, limit }`
- `FetchLimit { estimated, limit, seq, uid }`
- `InvalidAppendDate(String)`
- `Internal(String)`
- `DriverPanicked(String)`
- `DriverGone`

Current account flattening:

- `Error::Auth` and `Error::AuthPolicy` become old `AccountError::Auth`.
- `Error::Io`, `Closed`, `DriverGone`, and `DriverPanicked` become old
  `AccountError::Transport`.
- Everything else becomes old `AccountError::Other`.
- Stream failures call `fatal_event`, which maps old `Recovery` to old
  `RecoveryClass` variants.

This loses response codes, scope, operation, command attempt state,
and the distinction between local invalid input and provider protocol
breakage.

## Required internal error changes

Keep the raw protocol error type crate-private, but enrich it enough
that the account mapper can build faithful `AccountError`s.

Introduce a small attempt helper:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ImapAttempt {
    pub(crate) transmission_state: TransmissionState,
}
```

Then change transport-like variants to preserve attempt state:

```rust
Io {
    source: Arc<std::io::Error>,
    attempt: Option<ImapAttempt>,
}
Timeout {
    attempt: Option<ImapAttempt>,
}
Closed {
    attempt: Option<ImapAttempt>,
}
Bye {
    text: String,
    code: Option<ResponseCode>,
    attempt: Option<ImapAttempt>,
}
DriverPanicked {
    message: String,
    attempt: Option<ImapAttempt>,
}
DriverGone {
    attempt: Option<ImapAttempt>,
}
```

Keep constructors so most call sites do not construct these directly:

```rust
impl Error {
    pub(crate) fn io(source: std::io::Error) -> Self;
    pub(crate) fn timeout() -> Self;
    pub(crate) fn closed() -> Self;
    pub(crate) fn driver_gone() -> Self;
    pub(crate) fn with_attempt(self, state: TransmissionState) -> Self;
    pub(crate) fn attempt(&self) -> Option<TransmissionState>;
}
```

`From<std::io::Error>` should produce `Error::Io { attempt: None, ... }`.

Add a local invalid-input variant:

```rust
InvalidInput(String)
```

Use it for validation errors and encode validation failures that are
caused by caller or account-layer request construction. Do not keep
mapping those through `Error::Protocol`, because `Protocol` should mean
provider or wire contract failure.

Update `From<crate::types::ValidationError>`:

- Before: `Error::Protocol(e.to_string())`
- After: `Error::InvalidInput(e.to_string())`

Update `From<crate::codec::encode::EncodeError>`:

- `MissingCapability { cmd, cap }` stays `MissingCapability`.
- `Validation(msg)` becomes `InvalidInput(msg)`.

## Attempt-state rules

The driver is the only code that can know whether a command may have
reached the server. Preserve that signal before returning `crate::Error`
to account code.

Use these rules:

| Location | Error source | Transmission state |
| --- | --- | --- |
| Local validation before enqueue | validation, bad mailbox name, bad TLS server name | no attempt cause, or `Unsent` if wrapped as transport |
| TCP connect before greeting | connect I/O, connect timeout | `Unsent` |
| Implicit TLS handshake before greeting | TLS I/O, TLS timeout | `Unsent` |
| Greeting read before any client command | parse, BYE, closed, timeout | `Unsent` |
| Encoding a command before any bytes are written | encode failure | no attempt cause |
| `cmd_tx.send` fails before the driver accepts a command | driver gone | `Unsent` |
| `result_rx` closes after a command was submitted | driver gone or panicked | `InFlight` |
| Timeout around `submit_*` after the command future is polled | timeout | `InFlight` conservatively |
| `send_command_on_wire` write or flush fails | I/O | `InFlight` conservatively |
| `send_with_literal_sync` fails while waiting for continuation after command bytes | I/O, closed, BYE, parse | `InFlight` |
| `run_one_command` response loop after send succeeds | I/O, closed, timeout, parse, unexpected tag | `InFlight` unless a tagged response finalized |
| Tagged `NO` or `BAD` response | server response | `Acknowledged` |
| `AUTH` rejection with tagged response | server response | `Acknowledged` |
| Server `BYE` during a command before tagged completion | server response | `InFlight` |
| Buffered `FETCH` exceeds budget after driver drains tagged OK | local limit | `Acknowledged` |
| Account stream output receiver is dropped | consumer dropped output stream | no `AccountError`; terminate task |

The conservative choice for write failure is `InFlight`, because
`AsyncWriteExt::write_all` may have written a prefix before failing.

### Timeout centralization

The current crate wraps many `self.submit_*` calls in
`tokio::time::timeout(...).map_err(|_| Error::Timeout)`.

Use option 2: keep the wrappers and change every command timeout
mapping to `Error::timeout().with_attempt(TransmissionState::InFlight)`.
The wrappers are scattered across pipeline.rs, dispatch/*, mailbox.rs,
and helpers.rs; a single audited Find-and-Edit pass touching the
existing `map_err` sites is smaller than a wrapper refactor that
must thread `attempt` through every call signature. The wrapper
refactor (option 1) was rejected because the noise it would create
in commit churn outweighs the per-site clarity gain.

Pre-driver connect, greeting, initial capability, and STARTTLS setup
timeouts must use `Unsent`, not `InFlight`.

## Account-boundary surface

Create `crates/imap/src/account/error.rs`:

```rust
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation,
    ErrorScope, Protocol, Provider,
};

#[derive(Clone, Debug, Default)]
pub(crate) struct ImapErrorContext {
    pub(crate) operation: Option<AccountOperation>,
    pub(crate) scope: Option<ErrorScope>,
    pub(crate) provider: Option<Provider>,
    pub(crate) idempotency_override: Option<bool>,
    pub(crate) transmission_state: Option<TransmissionState>,
}

pub(crate) fn into_account_error(
    error: crate::Error,
    ctx: ImapErrorContext,
) -> AccountError;
```

Context rules:

- Always attach `Protocol::Imap`.
- `operation` should be set at every account boundary call site.
- `scope` should be set when a cursor, mailbox, message, thread, or blob
  is known.
- `provider` stays `None` unless a future public configuration field
  explicitly identifies a provider. Do not infer provider from hostname.
- `ctx.transmission_state` is a fallback only. Prefer
  `error.attempt()` when present.
- `idempotency_override` is only for cases where
  `AccountOperation::is_idempotent()` is wrong for the actual IMAP
  command. Most call sites should leave it unset.

Add small constructors:

```rust
impl ImapErrorContext {
    pub(crate) fn operation(operation: AccountOperation) -> Self;
    pub(crate) fn with_scope(self, scope: ErrorScope) -> Self;
    pub(crate) fn with_cursor_scope(self, scope: CursorScope) -> Self;
    pub(crate) fn with_mailbox(self, mailbox: &MailboxName) -> Self;
    pub(crate) fn with_message_id(self, id: impl Into<String>) -> Self;
}
```

Use these helpers at call sites. Avoid inline builder repetition.

## Builder rules

Every conversion must use `AccountErrorBuilder::new(kind, cause)`.

General builder shape:

```rust
let mut builder = AccountErrorBuilder::new(kind, primary_cause)
    .protocol(Protocol::Imap);

if let Some(operation) = ctx.operation {
    builder = builder.operation(operation);
}
if let Some(scope) = ctx.scope {
    builder = builder.scope(scope);
}
if let Some(provider) = ctx.provider {
    builder = builder.provider(provider);
}
if let Some(idempotent) = ctx.idempotency_override {
    builder = builder.idempotency_override(idempotent);
}
```

When an IMAP response code exists:

- Add `Cause::Wire(WireCause::Imap(...))`.
- Set `.native_code(...)` to the IMAP response-code name.
- Add the server text as support-only diagnostic text.

When an attempt state exists:

- Add `Cause::Attempt(AttemptCause { transmission_state })`.
- Never add `Attempt(Acknowledged)` to `Transport` errors. The builder
  asserts that transport failures cannot be acknowledged.

## Response-code conversion

Add:

```rust
fn imap_response_code(code: &ResponseCode) -> ImapResponseCode;
fn response_code_name(code: &ResponseCode) -> &'static str;
fn response_code_payload(code: &ResponseCode) -> Option<DiagnosticText>;
```

Map `crate::types::ResponseCode` to
`bifrost_types::ImapResponseCode` one for one:

| `ResponseCode` | `ImapResponseCode` |
| --- | --- |
| `Alert` | `Alert` |
| `BadCharset(_)` | `BadCharset` |
| `Capability(_)` | `Capability` |
| `Parse` | `Parse` |
| `PermanentFlags(_)` | `PermanentFlags` |
| `ReadOnly` | `ReadOnly` |
| `ReadWrite` | `ReadWrite` |
| `TryCreate` | `TryCreate` |
| `UidNext(_)` | `UidNext` |
| `UidValidity(_)` | `UidValidity` |
| `Unseen(_)` | `Unseen` |
| `AppendUid { .. }` | `AppendUid` |
| `CopyUid { .. }` | `CopyUid` |
| `HighestModSeq(_)` | `HighestModSeq` |
| `Modified(_)` | `Modified` |
| `NoModSeq` | `NoModSeq` |
| `Closed` | `Closed` |
| `MailboxId(_)` | `MailboxId` |
| `Unavailable` | `Unavailable` |
| `AuthenticationFailed` | `AuthenticationFailed` |
| `AuthorizationFailed` | `AuthorizationFailed` |
| `Expired` | `Expired` |
| `PrivacyRequired` | `PrivacyRequired` |
| `ContactAdmin` | `ContactAdmin` |
| `NoPerm` | `NoPerm` |
| `InUse` | `InUse` |
| `ExpungeIssued` | `ExpungeIssued` |
| `Corruption` | `Corruption` |
| `ServerBug` | `ServerBug` |
| `ClientBug` | `ClientBug` |
| `Cannot` | `Cannot` |
| `Limit` | `Limit` |
| `OverQuota` | `OverQuota` |
| `AlreadyExists` | `AlreadyExists` |
| `NonExistent` | `NonExistent` |
| `NewName(_)` | `NewName` |
| `Referral(_)` | `Referral` |
| `UrlMech(_)` | `UrlMech` |
| `BadUrl(_)` | `BadUrl` |
| `BadComparator(_)` | `BadComparator` |
| `Annotate(_)` | `Annotate` |
| `Annotations(_)` | `Annotations` |
| `TempFail(_)` | `TempFail` |
| `MaxConvertMessages(_)` | `MaxConvertMessages` |
| `MaxConvertParts(_)` | `MaxConvertParts` |
| `NoUpdate(_)` | `NoUpdate` |
| `NotificationOverflow(_)` | `NotificationOverflow` |
| `BadEvent(_)` | `BadEvent` |
| `UndefinedFilter(_)` | `UndefinedFilter` |
| `UidNotSticky` | `UidNotSticky` |
| `NotSaved` | `NotSaved` |
| `HasChildren` | `HasChildren` |
| `UnknownCte` | `UnknownCte` |
| `TooBig` | `TooBig` |
| `CompressionActive` | `CompressionActive` |
| `UseAttr` | `UseAttr` |
| `MetadataLongEntries(_)` | `MetadataLongEntries` |
| `MetadataMaxSize(_)` | `MetadataMaxSize` |
| `MetadataTooMany` | `MetadataTooMany` |
| `MetadataNoPrivate` | `MetadataNoPrivate` |
| `Other { name, value }` | `Unknown { code: name, value }` |

For parameterized codes, store the payload as support-only diagnostic
text. `ImapResponseCode` only needs the standardized code identity.

## Top-level `crate::Error` mapping

### Transport and task failures

| Error variant | Account kind | Primary cause | Extra causes and notes |
| --- | --- | --- | --- |
| `Io { .. }` with TLS-looking source only when known | `Transport(Tls)` | `Transport(Tls)` | Prefer explicit TLS variant if implementation adds one. Otherwise keep `Network`. |
| `Io { .. }` | `Transport(Network)` | `Transport(Network)` | Add `Attempt` from error/context unless absent. |
| `Timeout { .. }` | `Transport(Timeout)` | `Transport(Timeout)` | Add `Attempt`. |
| `Closed { .. }` | `Transport(Network)` | `Transport(Network)` | Add `Attempt`. |
| `DriverGone { attempt: Some(InFlight) }` | `Transport(Network)` | `Transport(Network)` | Driver disappeared after submission. |
| `DriverGone { attempt: None }` | `Transport(Network)` | `Transport(Network)` | Use `Unsent` at account boundary. |
| `DriverPanicked { .. }` | `Protocol(ContractViolation)` | `Wire(MalformedResponse { protocol: Imap, detail })` | This is a library/provider-contract failure at the account boundary, not remote TCP. |

`DriverPanicked` should include support-only diagnostic text with the
panic message. Do not expose the panic message as user-facing text.

### Authentication and authorization

| Error variant/code | Account kind | Primary cause | Notes |
| --- | --- | --- | --- |
| `Auth` with `Expired` | `Authentication(Expired)` | `Auth(Expired)` | Add IMAP wire code. |
| `Auth` with `AuthenticationFailed` | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` | Invalid password/token requires user action. |
| `Auth` with `AuthorizationFailed` | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource: None })` | Identity accepted but not authorized. |
| `Auth` with `PrivacyRequired` | `Authorization(PolicyBlocked)` | `Access(PolicyBlocked)` | Server requires stronger security policy. |
| `Auth` with no code | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` | Add server text. |
| `AuthPolicy(_)` | `Authorization(PolicyBlocked)` | `Access(PolicyBlocked)` | Local policy blocked every offered mechanism. |
| `StartTlsUnavailable` | `Authorization(PolicyBlocked)` | `Access(PolicyBlocked)` | Local TLS policy cannot be satisfied by server capabilities. |
| `No`/`Bad` with `NoPerm` | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` | Resource from context when possible. |
| `No`/`Bad` with `ContactAdmin` | `Authorization(PolicyBlocked)` | `Access(PolicyBlocked)` | Add server text. |
| `No`/`Bad` with `AuthorizationFailed` | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` | Add wire code. |

`PermissionDenied` resource mapping:

- `ErrorScope::Mailbox` or folder cursor scope -> `ResourceKind::Mailbox`
- `ErrorScope::Message` -> `ResourceKind::Message`
- `ErrorScope::Thread` -> `ResourceKind::Thread`
- otherwise `None`

### Local request and capability failures

| Error variant | Account kind | Primary cause | Notes |
| --- | --- | --- | --- |
| `InvalidInput(msg)` | `Request(Malformed)` | `Request(Malformed { detail })` | Local request construction failed. |
| `InvalidAppendDate(msg)` | `Request(Malformed)` | `Request(Malformed { detail })` | Draft append input. |
| `AppendLimit { size, limit }` | `Request(Malformed)` | `Request(Malformed { detail })` | Message exceeds known APPENDLIMIT. |
| `FetchLimit { .. }` | `Request(Malformed)` | `Request(Malformed { detail })` | Client-side safety budget exceeded after drain. Add `Attempt(Acknowledged)`. |
| `MissingCapability(cap)` | `Unsupported(ctx.operation)` | `Request(Unsupported { operation })` | If operation is missing, use `Discover` and add text. |
| `Protocol(msg)` | `Protocol(ContractViolation)` | `Wire(MalformedResponse { protocol: Imap, detail })` | Server response or internal protocol invariant violation. |
| `Parse(msg)` | `Protocol(ParseFailed)` | `Wire(MalformedResponse { protocol: Imap, detail })` | Add attempt when parser was in a command response loop. |
| `Internal(msg)` | `Protocol(ContractViolation)` | `Wire(MalformedResponse { protocol: Imap, detail })` | Treat as library/provider contract bug, support-only text. |

### Server status errors

`Error::No`, `Error::Bad`, and `Error::Bye` all preserve
`ResponseCode`. The mapper should first inspect the code. If a
recognized code maps to a specific account kind, use that mapping.
If no code exists, fall back by status:

- `No` without code: `Server(Error { status: None })`
- `Bad` without code: `Request(Malformed)`
- `Bye` without code: `Server(Unavailable)`

Add attempt state:

- Tagged `NO` and `BAD`: `Acknowledged`
- `BYE` during greeting: `Unsent`
- `BYE` during command response loop: `InFlight`

## Response-code semantic mapping

Every semantic mapping below must also add the wire cause from
`imap_response_code(code)`.

### Informational or state-carrying codes

These codes are normally successful metadata. If they appear on an
error response, prefer the enclosing status fallback unless the current
operation has a more specific interpretation:

- `Alert`
- `Capability`
- `PermanentFlags`
- `ReadOnly`
- `ReadWrite`
- `UidNext`
- `UidValidity`
- `Unseen`
- `AppendUid`
- `CopyUid`
- `HighestModSeq`
- `MailboxId`
- `MetadataLongEntries`

Special cases:

- `Capability` on `BYE` or auth failure may mean server capability set
  changed. Map account-open failures to
  `SyncState(CapabilityChanged)` only if there is explicit evidence that
  a previously available capability vanished. Otherwise keep the status
  fallback.
- `ReadOnly` during a write operation maps to
  `Authorization(PermissionDenied)`.
- `MetadataLongEntries` during hydration maps to
  `Protocol(PartialResponse)` because the server explicitly truncated
  data. In single-target hydration paths this is the call-site error
  shape. In bulk hydration/streaming paths (`get_stream`, mutation
  read-back), the per-item lane is `ItemOutcome::Uncertain` carrying
  this `AccountError` - not the call-site `Reconcile` recovery. The
  convergence streaming invariants (§"Streaming invariants",
  rules 1 and 4) require per-item Uncertain rather than a global
  reconcile when the protocol crate observed an item-level partial
  signal.

### Authentication and policy codes

| Code | Account kind | Primary cause |
| --- | --- | --- |
| `AuthenticationFailed` | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` |
| `Expired` | `Authentication(Expired)` | `Auth(Expired)` |
| `AuthorizationFailed` | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` |
| `PrivacyRequired` | `Authorization(PolicyBlocked)` | `Access(PolicyBlocked)` |
| `ContactAdmin` | `Authorization(PolicyBlocked)` | `Access(PolicyBlocked)` |
| `NoPerm` | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource })` |

### Transient server codes

| Code | Account kind | Primary cause | Notes |
| --- | --- | --- | --- |
| `Unavailable` | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` | Retry or reconcile derived from attempt and idempotency. |
| `InUse` | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` | Resource temporarily locked. |
| `Corruption` | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` | Server-side transient per old recovery table. |
| `TempFail(_)` | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` | Add payload text if present. |
| `ServerBug` | `Protocol(ContractViolation)` | `Wire(MalformedResponse { protocol: Imap, detail })` | The IMAP `ServerBug` response code asserts the server itself violated protocol. Map as protocol contract violation, not generic `Server(_)`. |

Note: `ServerErrorKind::Error { status: Option<u16> }` permits `None`,
so the alternative shape `Server(Error { status: None })` is
type-system-legal. The mapping above is a policy choice: a server
admitting `ServerBug` is structurally a contract violation, which is
the more accurate classification and routes to `ProviderContractViolation`
in the central recovery mapper. Reserve `Server(Error { status: None })`
for cases where IMAP returned a server-side failure with no specific
contract-violation evidence.

### Mailbox and cursor state codes

| Code | Account kind | Primary cause | Scope |
| --- | --- | --- | --- |
| `ExpungeIssued` | `SyncState(CursorInvalid)` | `State(CursorInvalid)` | Cursor or mailbox scope. |
| `UidNotSticky` | `SyncState(ScopeCapabilityLost)` | `State(ScopeCapabilityLost)` | Cursor or mailbox scope. |
| `Closed` | `SyncState(CursorInvalid)` | `State(CursorInvalid)` | Cursor or mailbox scope. |
| `NoModSeq` | `SyncState(StrategyFailure)` | `State(StrategyFailure { downgrade: CondstoreToBasic })` | Cursor scope. |
| `Modified(_)` | `ConcurrencyConflict` | `State(ConcurrencyConflict)` | Message or mailbox scope. |
| `AlreadyExists` | `ConcurrencyConflict` | `State(ConcurrencyConflict)` | Mailbox scope for create/rename. |
| `NonExistent` | `NotFound(resource)` | `Request(NotFound { what, id })` | Resource from operation/scope. |
| `TryCreate` | `NotFound(Mailbox)` | `Request(NotFound { what: Mailbox, id })` | Usually copy/move target. |
| `HasChildren` | `ConcurrencyConflict` | `State(ConcurrencyConflict)` | Mailbox delete raced with hierarchy state. |
| `NoUpdate(_)` | `ConcurrencyConflict` | `State(ConcurrencyConflict)` | Metadata/search context update refused. |

`NoModSeq` downgrade:

- If current strategy is QRESYNC, first downgrade is
  `QResyncToCondstore` only when CONDSTORE is still usable.
- If selected mailbox reports no persistent modseqs, use
  `CondstoreToBasic`.
- The existing successful stream downgrade warning remains, but a fatal
  strategy failure must be represented through the builder so the engine
  receives `EngineDirective::DowngradeStrategy`.

### Quota, limits, and size codes

| Code | Account kind | Primary cause | Notes |
| --- | --- | --- | --- |
| `OverQuota` | `Server(QuotaExhausted)` | `Server(QuotaExhausted { retry_after: None })` | Set `ThrottleScope::Account`. |
| `Limit` | `Server(RateLimited)` | `Server(RateLimited { retry_after: None })` | Set `ThrottleScope::Mailbox` when mailbox scoped, else `Account`. |
| `TooBig` | `Request(Malformed)` | `Request(Malformed { detail })` | Message/blob request too large for server. |
| `MaxConvertMessages(_)` | `Request(Malformed)` | `Request(Malformed { detail })` | CONVERT request too broad. |
| `MaxConvertParts(_)` | `Request(Malformed)` | `Request(Malformed { detail })` | CONVERT request too broad. |
| `MetadataMaxSize(_)` | `Request(Malformed)` | `Request(Malformed { detail })` | Annotation too large. |
| `MetadataTooMany` | `Server(QuotaExhausted)` | `Server(QuotaExhausted { retry_after: None })` | Mailbox/account annotation quota. |

### Unsupported or unavailable feature codes

| Code | Account kind | Primary cause |
| --- | --- | --- |
| `BadCharset(_)` | `Unsupported(Search)` or current operation | `Request(Unsupported { operation })` |
| `Cannot` | `Unsupported(ctx.operation)` | `Request(Unsupported { operation })` |
| `BadComparator(_)` | `Unsupported(Search)` | `Request(Unsupported { operation: Search })` |
| `Annotate(_)` | `Unsupported(ctx.operation)` | `Request(Unsupported { operation })` |
| `Annotations(_)` | `Unsupported(ctx.operation)` | `Request(Unsupported { operation })` |
| `BadEvent(_)` | `Unsupported(PushSubscribe)` | `Request(Unsupported { operation: PushSubscribe })` |
| `UndefinedFilter(_)` | `Unsupported(PushSubscribe)` | `Request(Unsupported { operation: PushSubscribe })` |
| `UnknownCte` | `Unsupported(OpenBlob)` or `Unsupported(Hydrate)` | `Request(Unsupported { operation })` |
| `CompressionActive` | `Protocol(ContractViolation)` | `Wire(MalformedResponse { protocol: Imap, detail })` |
| `UseAttr` | `Unsupported(ContainerCreate)` or current operation | `Request(Unsupported { operation })` |
| `MetadataNoPrivate` | `Authorization(PermissionDenied)` | `Access(PermissionDenied { resource: Some(Mailbox) })` |

### Referral and URL codes

The current account layer does not implement referral following.

| Code | Account kind | Primary cause | Notes |
| --- | --- | --- | --- |
| `Referral(_)` | `Unsupported(ctx.operation)` | `Request(Unsupported { operation })` | Add referral payload as support-only diagnostic text. |
| `NewName(_)` | `Protocol(Unknown)` | `Wire(Imap(NewName))` | Obsolete code. |
| `UrlMech(_)` | `Unsupported(ctx.operation)` | `Request(Unsupported { operation })` | IMAP URL auth not exposed. |
| `BadUrl(_)` | `Request(Malformed)` | `Request(Malformed { detail })` | If IMAP URL support is later added, map by operation. |

### Parse and bug codes

| Code | Account kind | Primary cause |
| --- | --- | --- |
| `Parse` | `Protocol(ParseFailed)` | `Wire(Imap(Parse))` |
| `ClientBug` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `ServerBug` | `Protocol(ContractViolation)` | `Wire(Imap(ServerBug))` |
| `Other { .. }` | `Protocol(Unknown)` | `Wire(Imap(Unknown { .. }))` |

`ClientBug` is an IMAP server telling the client that the command was
nonsensical. Treat this as a client bug at the shared boundary.

### Search, saved-result, and notification codes

| Code | Account kind | Primary cause | Notes |
| --- | --- | --- | --- |
| `NotSaved` | `Request(Malformed)` | `Request(Malformed { detail })` | Saved search variable was not available. |
| `NotificationOverflow(_)` | `SyncState(CursorInvalid)` | `State(CursorInvalid)` | Account scope until types grow a push-registration directive. |

`NotificationOverflow` currently maps to watch invalidation in
`account/push.rs`. If it becomes an account-boundary fatal, use
`ErrorScope::Account` and report the type gap to the Phase 3
orchestrator if an explicit push-registration directive is needed. Do
not add a source comment that points at this plan file.

## Account operation context table

Update every account-boundary `map_err(account_error)` and
`fatal_event(err)` call to pass `ImapErrorContext`.

| File/function | Operation | Scope |
| --- | --- | --- |
| `factory.rs::open` connect/auth/profile/list | `Discover` | `Account` |
| `factory.rs::read_server_id` | `Discover` | `Account` |
| `factory.rs::negotiate_qresync` | `Discover` | `Account` |
| `factory.rs::list_folders` | `Discover` | `Account` |
| `inventory.rs::establish_initial_cursor` | `EstablishCursor` | `Cursor(scope)` |
| `inventory.rs::inventory_stream` | `SyncInventory` | `Cursor(scope)` |
| `changes.rs::changes_stream` | `SyncChanges` | `Cursor(cursor.scope)` |
| `changes.rs::search_all` baseline seeding | `SyncChanges` | `Mailbox(folder)` |
| `get.rs::get_stream` | `Hydrate` | `Message(id)` when per item, else `Mailbox(folder)` |
| `blob.rs::open_blob` | `OpenBlob` | `Message` or `Mailbox` from decoded blob id |
| `blob.rs::open_blob_range` | `OpenBlobRange` | `Message` or `Mailbox` from decoded blob id |
| `mutate.rs::bulk_set_flags` | `UpdateFlags` | `Message(id)` for item failures, `Mailbox(folder)` for fatal folder failures |
| `mutate.rs::bulk_move` | `BulkMove` | `Message(id)` or target `Mailbox` when destination fails |
| `mutate.rs::bulk_destroy` | `BulkDestroy` | `Message(id)` for item failures, `Mailbox(folder)` for fatal folder failures |
| `push.rs::push_subscribe` | `PushSubscribe` | `Account` or cursor scopes being watched |
| `push.rs::push_unsubscribe` | `PushUnsubscribe` | `Account` |
| `push.rs::push_stream` internal IDLE loop | `PushStream` | selected mailbox when known |
| `pim.rs::message_hydrate` | `HydrateMessage` | `Message(id)` |
| `pim.rs::thread_hydrate` | `HydrateThread` | `Thread(id)` |
| `pim.rs::search_messages` | `SearchMessages` | `Mailbox` when `SearchFilter::In` is present, else `Account` |
| `pim.rs::containers_list` | `ContainersList` | `Account` |
| `pim.rs::container_create` | `ContainerCreate` | target `Mailbox` |
| `pim.rs::container_rename` | `ContainerRename` | source and target mailbox in diagnostics, source as scope |
| `pim.rs::container_move` | `ContainerMove` | source and target mailbox in diagnostics, source as scope |
| `pim.rs::container_delete` | `ContainerDelete` | `Mailbox(id)` |
| `pim.rs::set_keyword` | `SetKeyword` | `Message(id)` |
| `pim.rs::set_is_read` | `SetIsRead` | `Message(id)` |
| `pim.rs::add_to_container` | `AddToContainer` | `Message(id)` |
| `pim.rs::remove_from_container` | `RemoveFromContainer` | `Message(id)` |
| `pim.rs::draft_create` | `DraftCreate` | Drafts mailbox when known |
| `pim.rs::draft_discard` | `DraftDiscard` | `Message(id)` |
| `pim.rs::quota_get` | `QuotaGet` | mailbox when requested, else `Account` |
| `close.rs::close` | `Close` | `Account` |

Unsupported PIM methods should return an `AccountError` built as:

- kind: `Unsupported(operation)`
- cause: `Request(Unsupported { operation })`
- protocol: `Imap`

Do not keep returning the old unit-like `AccountError::Unsupported`.

## Stream handling

Account stream tasks currently convert failed `tx.send(...)` into
`crate::Error::Closed`, which then becomes a fatal account error. That
confuses "consumer stopped reading this Rust stream" with "IMAP
connection closed".

Change stream send handling:

- If the output receiver is dropped, stop the task and return `Ok(())`.
- Do not emit a fatal event for output-channel closure.
- Reserve `Error::Closed` for IMAP wire EOF or driver connection close.

Affected files:

- `account/inventory.rs`
- `account/changes.rs`
- `account/get.rs`
- `account/blob.rs`
- `account/mutate.rs`

This can be done with a local helper:

```rust
async fn send_or_stop<T>(
    tx: &tokio::sync::mpsc::Sender<SyncEvent<T>>,
    event: SyncEvent<T>,
) -> Result<bool, crate::Error>;
```

Return `Ok(false)` when the receiver is gone. Callers then return
`Ok(())` from the stream task.

## Fatal event conversion

Replace `fatal_event(err)` with:

```rust
pub(crate) fn fatal_event<T>(
    err: crate::Error,
    ctx: ImapErrorContext,
) -> SyncEvent<T>;
```

The implementation should convert with `into_account_error(err, ctx)`
and wrap the resulting `AccountError` in the current `bifrost_types`
fatal type. If Phase 3 changes the fatal wrapper shape, this helper is
the only IMAP account file that should need mechanical adjustment.

Do not manually choose `RecoveryClass` in IMAP. Recovery comes from the
builder and Phase 1 recovery derivation.

## Mutations and per-item failures

`account/mutate.rs` needs special care because it emits per-item
`MutationResult`s as well as fatal stream events.

Current issues:

- `StoreWireOutcome::Modified` maps to old
  `AccountError::ConcurrencyConflict`.
- `failed_all` wraps every failure as `AccountError::Other(error.to_string())`,
  losing structure.
- `PendingRetry` emits "pending retry" as `Other`.

Required behavior:

- `ResponseCode::Modified` maps to structured
  `AccountErrorKind::ConcurrencyConflict` with
  `StateCause::ConcurrencyConflict` and wire code `Modified`.
- Stale UIDVALIDITY item failures map to `SyncState(CursorInvalid)` or
  `NotFound(Message)` depending on whether the object id's epoch is stale
  before any command is sent. Prefer `SyncState(CursorInvalid)` for a
  folder-wide selected UIDVALIDITY mismatch.
- `failed_all` must preserve the structured `AccountError` it receives.
  Do not stringify it into `Other`.
- Protected `STORE UNCHANGEDSINCE` conflicts remain per-item failures,
  not fatal stream failures.
- Fatal mutation failures should include operation and mailbox scope.
- `BulkMove` and `BulkDestroy` are non-idempotent according to
  `AccountOperation::is_idempotent()`. In streaming bulk paths, an
  in-flight transport drop on a non-idempotent operation produces a
  per-item `ItemOutcome::Uncertain` for items that may have been
  partially committed - not a call-site `Reconcile`. The convergence
  streaming invariants forbid collapsing per-item ambiguity into a
  single stream-level `Reconcile`.
- `UpdateFlags` is idempotent enough for retry, matching the current
  operation table.

If the existing `MutationResult` type cannot carry the new
`AccountError` directly until Phase 3, keep the conversion helper local
and document the required Phase 3 mechanical replacement in the Phase 3
audit notes, not in source comments.

### `run_pipeline` exit handling

Pipeline-level transport failure attribution is in scope for Phase 2.
The per-command attribution rules above apply. If the implementation
cannot attribute exactly which sub-batch wrote which commands due to
existing code structure, the minimum bar is:

- Every command in a sub-batch whose write returned an error gets
  `Attempt(InFlight)`.
- Every command whose tagged response was received gets the response
  code mapping plus `Attempt(Acknowledged)`.
- Encoding failures before any batch bytes are written get no attempt
  cause (or `Unsent` if wrapped as transport).

Document any narrower attribution gaps in the Phase 2 PR description.
Do not defer the entire path to Phase 3 - that delays the streaming
ItemOutcome wire-up needed by the engine.

## QRESYNC and CONDSTORE strategy handling

Current successful downgrade behavior:

- QRESYNC negotiation failure sends
  `WarningKind::StrategyDowngraded { QResync, Condstore }`.
- QRESYNC parse/capability failure disables QRESYNC for the session,
  discards the suspect connection when appropriate, warns, and retries
  via CONDSTORE.
- Missing persistent modseqs warn and continue with Basic.

Keep that behavior when the stream can continue.

When the stream cannot continue and must surface a fatal account error:

- QRESYNC to CONDSTORE failure:
  - kind: `SyncState(StrategyFailure)`
  - cause: `State(StrategyFailure { downgrade: QResyncToCondstore })`
  - scope: `ErrorScope::Cursor(folder_scope(folder))`
- CONDSTORE to Basic failure:
  - kind: `SyncState(StrategyFailure)`
  - cause: `State(StrategyFailure { downgrade: CondstoreToBasic })`
  - scope: `ErrorScope::Cursor(folder_scope(folder))`

The builder derives:

```rust
RecoveryClass::Engine(EngineDirective::DowngradeStrategy(...))
```

`uidvalidity_changed_fatal` and `modseq_reset_fatal` should stop
constructing old `Fatal { recovery, message, source }` values manually.
Build structured errors:

- UIDVALIDITY change:
  - kind: `SyncState(CursorInvalid)`
  - cause: `State(CursorInvalid)`
  - scope: `ErrorScope::Cursor(folder_scope(folder))` - MUST be
    `Cursor(_)`, not `Account`. The central recovery mapper derives
    `Engine(RestartScope(scope))` only when the scope is `Cursor(_)`;
    without it, the row falls back to `Engine(RestartAccount)` (per
    convergence's recovery table row `SyncState(CursorInvalid) without
    scope`).
  - diagnostic text: expected and actual UIDVALIDITY
- HIGHESTMODSEQ reset:
  - kind: `SyncState(CursorInvalid)`
  - cause: `State(CursorInvalid)`
  - scope: `ErrorScope::Cursor(folder_scope(folder))` - same scope
    requirement as above. RestartScope vs RestartAccount derivation
    hinges entirely on the presence of `Cursor(_)`.
  - diagnostic text: previous and current modseq

## IDLE and NOTIFY push

Current push behavior is intentionally non-fatal:

- IDLE task connection failure emits `WatchEvent::Disconnected`.
- Successful reconnect emits `WatchEvent::Reconnected`.
- Broadcast lag emits `WatchEvent::Invalidated`.
- `NotificationOverflow` maps to unknown invalidation.

Keep that behavior.

Only use `into_account_error` in push code where the Account trait
method itself returns `Result<_, AccountError>`, such as
`push_subscribe`.

IDLE errors inside the background task should continue as watch events
unless the public Account API later gains a structured push health
stream. Do not add fatal `AccountError` emissions to `push_stream`.

## Connection and driver implementation details

### `WireReader`

`WireReader::read_one`, `read_greeting`, and `write_all` should continue
returning `crate::Error`, but call sites in the driver should attach
attempt state before returning to the handle/account boundary.

Do not make `WireReader` know account operations or scopes.

### `run_one_command`

Required changes:

- If `send_command_on_wire` returns `Io`, `Closed`, `Timeout`, `Bye`,
  `DriverGone`, or `DriverPanicked`, attach `InFlight`.
- If `send_command_on_wire` returns encode/local validation errors, do
  not attach attempt.
- If response-loop read returns `Io`, `Closed`, `Timeout`, `Parse`,
  or `Bye`, attach `InFlight`.
- Tagged `NO` and `BAD` errors produced by consumers should carry
  `Acknowledged`.
- Unexpected tag or continuation after the command was sent should carry
  `InFlight` as protocol/partial-response evidence if the mapper needs
  it.

### `run_prebuilt_command`

APPEND and MULTIAPPEND pre-build all bytes on the handle side, then the
driver sends them with literal synchronization.

Required changes:

- Pre-built validation failures before driver submission are `Unsent`
  or no attempt cause.
- Write/literal-sync failures are `InFlight`.
- Tagged response failures are `Acknowledged`.
- This path is non-idempotent for `DraftCreate` and any future append
  operation. In-flight transport drop should derive reconcile.

### `run_pipeline`

Pipeline results are per command. Required behavior:

- If a sub-batch write fails, every command in that sub-batch that may
  have been written must receive an in-flight transport error.
- If encoding fails before any batch bytes are written, the whole batch
  is local request failure with no attempt cause.
- If a tagged response fails one command with `NO`/`BAD`, that result is
  acknowledged and must preserve its response code.
- If the shared response loop fails after some commands have completed,
  completed commands keep their results and remaining commands receive
  in-flight transport/protocol errors.

If implementing this exactly is too large for Phase 2, document the gap
in the Phase 3 audit notes and at minimum ensure any pipeline-level
transport failure is `InFlight`, never `Unsent`.

### `send_with_literal_sync`

When the server rejects a synchronizing literal before the literal body
is sent:

- Tagged `NO`/`BAD` is `Acknowledged` because the server responded.
- Unexpected tagged `OK` before continuation is
  `Protocol(ContractViolation)` with `Attempt(InFlight)`.
- BYE during literal sync is `Server(Unavailable)` or code-specific
  mapping with `Attempt(InFlight)`.

## Local unsupported operations

The PIM layer currently returns `AccountError::Unsupported` in many
places. Replace with structured unsupported errors:

```rust
fn unsupported(operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(operation),
        Cause::Request(RequestCause::Unsupported { operation }),
    )
    .operation(operation)
    .protocol(Protocol::Imap)
    .build()
}
```

Use the exact operation from the Account method:

- SMTP send: `Send`
- Attachment upload: `AttachmentUpload`
- Draft update: `DraftUpdate`
- Draft send: `DraftSend`
- Gmail label membership: `SetLabelMembership`
- Graph categories: `SetCategory`
- Extended properties: `SetExtendedProperty`
- Identities: `IdentitiesList` or `IdentityUpdate`
- Vacation responder: `VacationGet` or `VacationSet`

## Tests

Keep tests small and deterministic. No live server, no Docker, no fixed
ports, no mock-server harness.

Add or update unit tests in `crates/imap/src/error_tests.rs` and small
module-local tests where helpers are private.

Minimum test list:

1. `Io` with `Unsent` builds `Transport(Network)` and retry recovery.
2. `Io` with `InFlight` on non-idempotent `BulkMove` builds reconcile
   recovery.
3. `Timeout` with `InFlight` on idempotent `SyncChanges` builds retry
   recovery.
4. `Closed` with `InFlight` builds `Transport(Network)` with attempt
   cause.
5. `Auth` plus `AuthenticationFailed` maps to
   `Authentication(ReauthorizationRequired)` and carries
   `Wire(Imap(AuthenticationFailed))`.
6. `Auth` plus `Expired` maps to `Authentication(Expired)`.
7. `AuthPolicy` maps to `Authorization(PolicyBlocked)`.
8. `No` plus `NoPerm` maps to `Authorization(PermissionDenied)` with
   resource from scope.
9. `Bad` with no code maps to `Request(Malformed)`.
10. `Bad` plus `ServerBug` maps to provider contract failure.
11. `Bye` plus `Unavailable` with `InFlight` maps to server unavailable
    and non-idempotent reconcile.
12. `Parse` maps to `Protocol(ParseFailed)` with malformed-response
    wire cause.
13. `InvalidInput` maps to `Request(Malformed)`.
14. `MissingCapability("IDLE")` in `PushSubscribe` maps to
    `Unsupported(PushSubscribe)`.
15. `ResponseCode::Modified` maps to `ConcurrencyConflict`.
16. `ResponseCode::NoModSeq` maps to strategy failure with
    `CondstoreToBasic`.
17. `ResponseCode::OverQuota` maps to quota exhausted with account
    throttle scope.
18. `ResponseCode::Limit` maps to rate limited with mailbox or account
    throttle scope.
19. `ResponseCode::NonExistent` maps to `NotFound(Mailbox)` for mailbox
    scope.
20. `ResponseCode::NotificationOverflow` maps to cursor invalid/account
    scope until a push-specific engine directive exists.
21. Every `ResponseCode` variant maps to an `ImapResponseCode` variant.
22. `Other { name, value }` maps to `ImapResponseCode::Unknown` and
    preserves payload text.
23. Dropped output stream receiver does not produce a fatal IMAP
    transport error.
24. `uidvalidity_changed` structured helper derives
    `EngineDirective::RestartScope`.
25. `modseq_reset` structured helper derives
    `EngineDirective::RestartScope`.
26. QRESYNC strategy failure helper derives
    `EngineDirective::DowngradeStrategy(QResyncToCondstore)`.
27. CONDSTORE strategy failure helper derives
    `EngineDirective::DowngradeStrategy(CondstoreToBasic)`.
28. `StoreWireOutcome::Modified` produces structured per-item
    concurrency conflict without stringifying the error.

Existing tests that assert old `ErrorCategory` or old `Recovery` should
be removed or rewritten to assert `AccountError` kind, cause chain,
scope, operation, protocol, and derived recovery.

## Exit criteria

- `crates/imap/src/error.rs` no longer defines `ErrorCategory`,
  `Recovery`, `Error::category`, or `Error::recovery` - these are
  removed entirely, not "no longer driving the account-boundary
  mechanism." Grep `crates/imap/` for each of the four identifiers
  and confirm zero hits.
- `crate::Recovery` is no longer re-exported from `crates/imap/src/lib.rs`.
- Every kind/cause pair produced by `into_account_error` satisfies
  `recovery::kind_matches_cause`.
- `account/error.rs` contains `ImapErrorContext` and
  `into_account_error`.
- Every account boundary passes operation and scope when available.
- Every IMAP `ResponseCode` variant has a mapping to
  `ImapResponseCode`.
- Every response-code semantic class above has a deterministic
  `AccountErrorKind`.
- Driver and timeout paths preserve `TransmissionState`.
- Stream output-channel closure no longer masquerades as IMAP wire
  `Closed`.
- QRESYNC/CONDSTORE terminal failures derive
  `EngineDirective::DowngradeStrategy`.
- UIDVALIDITY and HIGHESTMODSEQ reset derive
  `EngineDirective::RestartScope`.
- Protected `STORE UNCHANGEDSINCE` conflicts are structured
  `ConcurrencyConflict` item failures.
- Unsupported PIM methods return structured `Unsupported(operation)`
  errors.
- Tests cover synthetic errors and response codes without live servers.
- No source comments point at files under `plans/`.

Compilation and workspace-wide checks are Phase 3 work. Do not run
`brokkr check` merely for this planning phase.

## Audit checklist for the implementation agent

Use this checklist after implementing the IMAP phase:

1. Search for `ErrorCategory`, `Recovery::`, `.recovery()`, and
   `.category()` under `crates/imap/src`. None should drive account
   boundary behavior.
2. Search for `AccountError::Other(` under `crates/imap/src/account`.
   Each remaining use must be local legacy glue that Phase 3 cannot yet
   remove, not a freshly stringified structured error.
3. Search for `AccountError::Unsupported`. Replace with structured
   unsupported builders.
4. Search for `map_err(account_error)`. Every occurrence should pass an
   `ImapErrorContext` or call a context-specific wrapper.
5. Search for `fatal_event(`. Every call should pass context.
6. Search for `map_err(|_| crate::Error::Closed)` in account stream
   send paths. Output-channel closure should stop the task, not emit
   fatal transport errors.
7. Search for `Error::Timeout` construction. Command timeouts should
   attach `InFlight`; connect/greeting/setup timeouts should attach
   `Unsent`.
8. Search for `Error::Io(` construction. Prefer constructors and attach
   attempt state at driver boundaries.
9. Verify tagged `NO`/`BAD` retains the original `ResponseCode`.
10. Verify `BYE` during command execution carries `InFlight`, while
    greeting BYE carries `Unsent`.
11. Verify `ResponseCode::Modified` reaches mutation item failures as
    `ConcurrencyConflict`.
12. Verify `NotificationOverflow` still reaches push invalidation.
13. Verify `crate::Recovery` is not re-exported from the crate root.
14. Verify tests do not spawn servers or depend on live IMAP accounts.

## Phase 3 correctness blockers (post-2.2 audit)

The Phase 2.2 IMAP commit landed the boundary, structured `Error`
reshape, and tests but deliberately deferred the wide
`connection/**` mechanical sweep. The deferred work contains
correctness items that must be resolved before Phase 3 exit.

### `ServerCause::Error { status: None }` - no `status: 0` sentinel

The 2.2 commit used `ServerCause::Error { status: 0 }` for IMAP `NO`
without a numeric status because `status` was `u16`. The Phase 1
amendment widens `status` to `Option<u16>`. Phase 3 must:

- Replace every `status: 0` sentinel under `crates/imap/` with
  `status: None`.
- Audit grep `rg 'ServerCause::Error \{ status: 0' crates/imap/`
  returns zero hits at Phase 3 exit.

### `connection/**` driver: per-site transmission state wiring

The 2.2 commit added `Error::timeout()`, `Error::with_attempt()`,
and the `attempt: Option<ImapAttempt>` field but did not thread
them through the driver's command sites. Phase 3 must wire each
command construction site to attach the correct transmission state:

- **Connect / TLS handshake / pre-greeting timeouts**: `Unsent`.
- **Greeting `BYE`** (server closes during the untagged greeting):
  `Unsent` (no command was in flight).
- **Command timeout during expected continuation/data**: `InFlight`.
- **`BYE` mid-command** (server closes between command transmission
  and tagged response): `InFlight`.
- **Tagged response received** (`OK`/`NO`/`BAD`): `Acknowledged`.
- **`Closed` due to local stream-send failure on the output channel**:
  no attempt cause (stop the task, do not synthesize a transport
  error per audit checklist item 6).

This wiring is what allows recovery to choose `Reconcile` over
`Retry::SameRequest` for non-idempotent operations like `BulkMove`
when the connection drops mid-command. Without it, IMAP defaults to
the safer side only when the central mapping reads absence as
`Unsent`, which is the wrong classification for in-flight drops.

### Structured replacements for legacy fatal sites

The 2.2 commit provided `error::uidvalidity_changed`,
`error::modseq_reset`, `error::unsupported` builders but did not
rewire the call sites. Phase 3 must:

- Switch `account/changes.rs::uidvalidity_changed_fatal` and
  `modseq_reset_fatal` to the new structured builders. Audit grep
  `rg 'RecoveryClass::RestartScope' crates/imap/` returns zero hits.
- Replace `account/blob.rs:40` `SyncEvent::Fatal(Fatal { recovery:
  RecoveryClass::Fatal, ... })` with a structured `Request(Malformed)`
  error event via `SyncEvent::Terminated`.
- Replace `account/envelope.rs` `AccountError::Other(...)` parse
  failures with structured `Request(Malformed)` errors.

### Misclassification of caller input as `Unsupported`

`account/pim.rs::merge_folder:820` returns
`AccountError::Unsupported` when a search filter constrains two
different folders. This is caller input shape, not a missing
capability. Phase 3 must reclassify it as
`Request(InvalidArgument { field: Some("folder"), .. })` (the
caller-provided search filter is structurally invalid because IMAP
search cannot span two folders in a single SEARCH command).
`Unsupported(Search)` is a defensible alternative if the project
prefers to surface it as a missing capability rather than caller-
side malformation; pick one and pin the test. The current
`Unsupported` (no operation argument) is the variant that must go.
