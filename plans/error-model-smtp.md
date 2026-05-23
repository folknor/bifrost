# Error model: bifrost-smtp implementation plan

This is Phase 2.2 of `plans/error-model-roadmap.md`.

`bifrost-smtp` is a public SMTP and LMTP transport crate, not a full
`bifrost_types::Account` implementation today. It still needs to join
the shared error model because IMAP and future composition code use it
as the wire-level send transport.

The current code is the source of truth. This plan is written against:

- `reference/smtp.md`
- `crates/smtp/src/error.rs`
- `crates/smtp/src/transport/smtp/error.rs`
- `crates/smtp/src/transport/smtp/response.rs`
- `crates/smtp/src/transport/smtp/client/connection.rs`
- `crates/smtp/src/transport/smtp/client/async_connection.rs`
- `crates/smtp/src/transport/smtp/transport.rs`
- `crates/smtp/src/transport/smtp/async_transport.rs`
- `crates/types/src/error/` (the landed Phase 1 API)

Required reading: `plans/error-model-roadmap.md`,
`plans/error-model-convergence.md`, and the landed types module above.

## Kind/cause shape conventions

Landed-API quirks the agent must keep straight:

- `ServerErrorKind::Error { status: Option<u16> }` (kind side) vs
  `ServerCause::Error { status: u16 }` (cause side). The kind always
  wraps the code in `Some(_)` in this plan's tables; the cause uses
  bare `u16`. This is by design; do not "fix" one column to match the
  other.
- `AccessErrorKind::PermissionDenied` (no payload) vs
  `AccessCause::PermissionDenied { resource: Option<ResourceKind> }`
  (carries optional resource).
- Table shorthand `Protocol(ContractViolation)` etc. refers to
  `ProtocolErrorKind` variants; the full path is
  `AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)`.

Every kind/cause pair below must satisfy
`recovery::kind_matches_cause` (asserted at runtime by
`AccountErrorBuilder::build`).

## Scope

Only `crates/smtp/` should change in this phase unless implementation
proves that `crates/types/` lacks an already-planned primitive needed
to express SMTP. Do not add a new resource kind just for recipients in
this phase; use `ResourceKind::Mailbox` for remote recipient mailbox
not-found until a shared `Recipient` or `Address` resource exists.

Goals:

- Preserve existing public `Transport` and `AsyncTransport` behavior:
  `SmtpTransport::Ok = Response` and `LmtpTransport::Ok =
  Vec<Response>` remain intact.
- Add shared-error conversion for SMTP transport errors.
- Add an Account-oriented multi-recipient send helper that returns
  `Result<BatchOutcome<()>, AccountError>` without replacing the
  existing transport trait.
- Preserve enhanced status codes as
  `Cause::Wire(WireCause::Smtp(EnhancedStatusCode { ... }))`.
- Preserve SMTP/LMTP command attempt state as
  `Cause::Attempt(AttemptCause { transmission_state })`.
- Make LMTP per-recipient final replies feed `BatchOutcome` lanes.
- Make SMTP partial send uncertainty explicit when the message may have
  crossed the DATA/BDAT delivery boundary.

Non-goals:

- No live SMTP or LMTP tests.
- No mock server harness additions.
- No DSN bounce ingestion. Asynchronous DSN reports are out of scope.
- No public `bifrost_types::Account` implementation in this crate.
- No `bifrost-net` dependency for SMTP protocol traffic.

## Dependencies and features

`bifrost-smtp` currently has no `bifrost-types` dependency. Add it as
an optional dependency so the low-level mailer can stay lightweight:

```toml
bifrost-types = { path = "../types", optional = true }
```

Add a feature:

```toml
account-error = ["dep:bifrost-types"]
```

All `AccountError`, `BatchOutcome`, and `BatchItem` conversion helpers
are gated by `account-error`. If Phase 3 decides the SMTP crate should
always depend on `bifrost-types`, it can remove the feature gate then.

Do not put the conversion behind the existing `tokio` feature. Sync and
async transports need the same mapping.

## Files to modify

Primary files:

- `crates/smtp/Cargo.toml`
- `crates/smtp/src/lib.rs`
- `crates/smtp/src/error.rs`
- `crates/smtp/src/address/envelope.rs`
- `crates/smtp/src/transport/smtp/mod.rs`
- `crates/smtp/src/transport/smtp/error.rs`
- `crates/smtp/src/transport/smtp/response.rs`
- `crates/smtp/src/transport/smtp/transport.rs`
- `crates/smtp/src/transport/smtp/async_transport.rs`
- `crates/smtp/src/transport/smtp/client/connection.rs`
- `crates/smtp/src/transport/smtp/client/async_connection.rs`
- `crates/smtp/src/transport/smtp/client/net.rs`
- `crates/smtp/src/transport/smtp/client/async_net.rs`
- `crates/smtp/src/transport/smtp/client/tls.rs`
- `crates/smtp/src/transport/smtp/extension.rs`
- `crates/smtp/src/transport/smtp/commands.rs`

Recommended new files:

- `crates/smtp/src/transport/smtp/account_error.rs`
- `crates/smtp/src/transport/smtp/batch.rs`

`account_error.rs` should contain the shared-error mapper and enhanced
status mapping. `batch.rs` should contain the Account-oriented
multi-recipient send helper, progress tracker, and lane conversion.

## Files to delete

No file must be deleted.

Do not remove the public SMTP `Error` or `ErrorKind` types. They remain
the low-level transport API. The shared model is an additional
conversion surface.

## Current error shape

Message-builder errors live in `crates/smtp/src/error.rs`:

- `MissingFrom`
- `MissingTo`
- `TooManyFrom`
- `EmailMissingAt`
- `EmailMissingLocalPart`
- `EmailMissingDomain`
- `CannotParseFilename`
- `Io(std::io::Error)`
- `NonAsciiChars`
- `InvalidInput(String)`

SMTP transport errors live in
`crates/smtp/src/transport/smtp/error.rs`:

- `Transient(Response)`
- `Permanent(Response)`
- `Parse`
- `InvalidInput`
- `Internal`
- `Policy`
- `Connection`
- `Network`
- `Timeout`
- `Tls`
- `TransportShutdown`

The transport error already exposes:

- `kind()`
- `smtp_response()`
- `status()`
- `enhanced_status_code()`
- `is_transient()`
- `is_permanent()`
- class predicates for parse, invalid input, policy, connection,
  network, timeout, TLS, and shutdown.

It does not preserve:

- command attempt state
- command phase
- recipient index or batch item id
- whether DATA/BDAT body bytes were sent
- how many LMTP per-recipient final replies were read before failure

Those are the missing pieces for shared recovery and batch outcomes.

## Required internal additions

Add a crate-private attempt state in SMTP, not a public dependency on
`bifrost_types`:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SmtpTransmissionState {
    Unsent,
    InFlight,
    Acknowledged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SmtpAttempt {
    pub(crate) transmission_state: SmtpTransmissionState,
}
```

Store it inside `transport::smtp::Error`:

```rust
struct Inner {
    kind: ErrorKind,
    source: Option<BoxError>,
    attempt: Option<SmtpAttempt>,
    phase: Option<SmtpCommandPhase>,
}
```

Add:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SmtpCommandPhase {
    Connect,
    Greeting,
    Hello,
    StartTls,
    Auth,
    MailFrom,
    RcptTo,
    DataCommand,
    DataBody,
    BdatBody,
    LmtpFinalStatus,
    Noop,
    Vrfy,
    Expn,
    Rset,
}
```

Add crate-private constructors/helpers:

```rust
impl Error {
    pub(crate) fn with_attempt(self, state: SmtpTransmissionState) -> Self;
    pub(crate) fn with_phase(self, phase: SmtpCommandPhase) -> Self;
    pub(crate) fn attempt(&self) -> Option<SmtpTransmissionState>;
    pub(crate) fn phase(&self) -> Option<SmtpCommandPhase>;
    pub(crate) fn diagnostic_text(&self) -> Option<String>;
}
```

`diagnostic_text()` should return support-safe text from the source or
display string. Do not expose secrets from credentials. AUTH commands
already avoid placing credentials in response text; keep that property.

## Attempt-state rules

Attempt state is about the mail-send side effect, not whether any TCP
packet ever crossed the wire.

| Location | Transmission state |
| --- | --- |
| Message builder validation | no attempt cause |
| Envelope validation before transport call | no attempt cause |
| Batch item id validation | no attempt cause |
| DNS, TCP connect, Unix socket connect | `Unsent` |
| Implicit TLS handshake during setup | `Unsent` |
| Greeting read during setup | `Unsent` |
| EHLO/LHLO during setup | `Unsent` |
| STARTTLS during setup | `Unsent` |
| AUTH during setup | `Unsent` for send side effect, but map auth failures semantically |
| Local MAIL/RCPT parameter validation | no attempt cause |
| `MAIL FROM` write or read failure | `InFlight` |
| `MAIL FROM` negative reply | `Acknowledged` |
| `RCPT TO` write or read failure | `InFlight` |
| `RCPT TO` negative reply | `Acknowledged` |
| `DATA` command write or read failure before body | `InFlight` |
| `DATA` negative reply before body | `Acknowledged` |
| DATA body write failure after 354 | `InFlight` |
| DATA final reply failure after body was written | `InFlight` |
| DATA final negative reply | `Acknowledged` |
| BDAT body write failure | `InFlight` |
| BDAT final reply failure after body write | `InFlight` |
| BDAT final negative reply | `Acknowledged` |
| LMTP final status read failure after body write | `InFlight` |
| LMTP per-recipient final negative reply | `Acknowledged` |
| Transport shutdown before send starts | `Unsent` |
| Transport shutdown during send | `InFlight` |

`write_all` and `flush` failures are `InFlight`, because a prefix may
have been sent before the error. This applies to sync and async paths.

For Account-oriented batch outcomes, `Send` is non-idempotent. An
in-flight transport drop after body transmission should derive
`RecoveryClass::Reconcile`, not blind retry.

## Account-error conversion surface

In `transport/smtp/account_error.rs`, behind `account-error`, add:

```rust
use bifrost_types::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuilder,
    AccountErrorKind, AccountOperation, AttemptCause, AuthCause,
    AuthErrorKind, BatchInputInvalidItem, BatchInputInvalidReason,
    BatchItem, BatchItemId, BatchOutcome, Cause, DiagnosticText,
    ErrorScope, MailboxUnavailableKind, Protocol, ProtocolErrorKind,
    Provider, RequestCause, RequestErrorKind, ResourceKind, ServerCause,
    ServerErrorKind, ThrottleScope, TransmissionState, TransportCause,
    TransportErrorKind, TransportKind, WireCause,
};

#[derive(Clone, Debug)]
pub(crate) struct SmtpErrorContext {
    pub(crate) operation: Option<AccountOperation>,
    pub(crate) scope: Option<ErrorScope>,
    pub(crate) provider: Option<Provider>,
    pub(crate) protocol: Protocol, // bifrost_types::Protocol::Smtp or Lmtp
    pub(crate) idempotency_override: Option<bool>,
    pub(crate) transmission_state: Option<SmtpTransmissionState>,
    pub(crate) phase: Option<SmtpCommandPhase>,
}

// Use bifrost_types::Protocol::{Smtp, Lmtp} directly. Do not introduce
// a parallel SmtpProtocol enum — Phase 1's Protocol enum already has
// both variants (crates/types/src/error/scope.rs:198).

pub(crate) fn into_account_error(
    error: super::Error,
    ctx: SmtpErrorContext,
) -> AccountError;

pub(crate) fn message_error_to_account_error(
    error: crate::error::Error,
    ctx: SmtpErrorContext,
) -> AccountError;
```

Context rules:

- `protocol` is `Protocol::Smtp` for SMTP or `Protocol::Lmtp` for LMTP.
- `operation` should be `AccountOperation::Send` for mail submission,
  `Discover` for connection setup/capability probing, and `Close` for
  shutdown paths if they ever become fallible.
- `provider` stays `None`; SMTP relay provider is configuration, not
  inferred from hostname.
- `idempotency_override` should be `Some(false)` for DATA/BDAT send and
  all Account-oriented batch sends.
- `transmission_state` is a fallback. Prefer the state stored on
  `Error`.
- `phase` is diagnostic and can refine mapping, especially AUTH vs send.

Add helper constructors:

```rust
impl SmtpErrorContext {
    pub(crate) fn send(protocol: Protocol) -> Self;
    pub(crate) fn discover(protocol: Protocol) -> Self;
    pub(crate) fn with_scope(self, scope: ErrorScope) -> Self;
    pub(crate) fn with_attempt(self, state: SmtpTransmissionState) -> Self;
    pub(crate) fn with_phase(self, phase: SmtpCommandPhase) -> Self;
}
```

Constructors assert that `protocol` is `Protocol::Smtp` or
`Protocol::Lmtp`; other variants are a caller bug.

## Builder rules

Every conversion must use `AccountErrorBuilder::new(kind, cause)`.

Always attach:

- `Protocol::Smtp` for SMTP
- `Protocol::Lmtp` for LMTP
- `operation` when known
- `scope` when known
- `idempotency_override(false)` for mail send operations

When a response exists:

- Add `Cause::Wire(WireCause::Smtp(...))`.
- Set `.status(u16::from(response.code()))`.
- Set `.native_code(...)` to the enhanced status code string if
  present, otherwise the normal 3-digit status code.
- Add response text as support-only diagnostic text.

When attempt state exists:

- Add `Cause::Attempt(AttemptCause { transmission_state })`. SMTP
  emits transmission state as a separate `Cause::Attempt`, not as a
  field on `TransportCause`. Both shapes are supported by Phase 1
  (the builder reads `transmission_state` off the chain via the first
  matching cause). SMTP uses the separate-cause shape because most
  SMTP errors are `Server(_)` / `Authorization(_)` rather than
  `Transport(_)`, so the cause carries the state independently of the
  primary cause variant.
- Convert `SmtpTransmissionState` to
  `bifrost_types::TransmissionState`.
- Attach `Attempt` whenever wire-level evidence exists at one of the
  three known states.
- For `Transport(_)` errors specifically: emit at most
  `Attempt(Unsent)` or `Attempt(InFlight)`. A `Transport(_)` kind with
  `Attempt(Acknowledged)` is a contradiction in terms — if a terminal
  response arrived, the kind should be `Server(_)`, `Authorization(_)`,
  `Authentication(_)`, etc. (whatever the response classified to), not
  `Transport(_)`. The builder's `kind_matches_cause` invariant does
  not reject this directly, so producers must enforce it at the call
  site.

## Wire-cause construction

The shared `bifrost_types::EnhancedStatusCode` is not the same type as
`transport::smtp::response::EnhancedStatusCode`. The shared type
contains the normal status code plus optional diagnostic text.

Add:

```rust
fn smtp_wire_cause(response: &Response) -> bifrost_types::EnhancedStatusCode {
    bifrost_types::EnhancedStatusCode {
        code: u16::from(response.code()),
        enhanced: response
            .enhanced_status_code()
            .map(|code| DiagnosticText::support_only(code.to_string())),
        text: response
            .message()
            .next()
            .map(|line| DiagnosticText::support_only(line.to_owned())),
    }
}
```

If the response has multiple enhanced status codes, use the first valid
one for native code and add all response lines as support-only text.

## Top-level transport error mapping

The mapping tables below use compact kind names. Implementation code
must use the concrete Phase 1 enums, for example
`AccountErrorKind::Request(RequestErrorKind::Malformed)` with
`Cause::Request(RequestCause::Malformed { ... })`.

| `ErrorKind` | Account kind | Primary cause | Notes |
| --- | --- | --- | --- |
| `Parse` | `Protocol(ParseFailed)` | `Wire(MalformedResponse { protocol, detail })` | Response parser failure. |
| `InvalidInput` | `Request(Malformed)` | `Request(Malformed { detail })` | Local request construction or unsupported parameter for current server. |
| `Internal` | `Protocol(ContractViolation)` | `Wire(MalformedResponse { protocol, detail })` | Client invariant failed. |
| `Policy` | `Authorization(PolicyBlocked)` | `Access(PolicyBlocked)` | Plaintext AUTH refusal, REQUIRETLS over plaintext, etc. |
| `Connection` | `Transport(Network)` | `Transport(Network)` | Add attempt when present. |
| `Network` | `Transport(Network)` | `Transport(Network)` | Add attempt when present. |
| `Timeout` | `Transport(Timeout)` | `Transport(Timeout)` | Add attempt when present. |
| `Tls` | `Transport(Tls)` | `Transport(Tls)` | Setup TLS is `Unsent`; STARTTLS during send remains pre-body. |
| `TransportShutdown` | `Transport(Network)` | `Transport(Network)` | Usually `Unsent` unless send had started. |
| `Transient(response)` | See response mapping | Add SMTP wire cause | Add `Attempt(Acknowledged)`. |
| `Permanent(response)` | See response mapping | Add SMTP wire cause | Add `Attempt(Acknowledged)`. |

Message-builder errors map as local request errors:

| `crate::error::Error` | Account kind | Primary cause |
| --- | --- | --- |
| `MissingFrom` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `MissingTo` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `TooManyFrom` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `EmailMissingAt` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `EmailMissingLocalPart` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `EmailMissingDomain` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `CannotParseFilename` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `NonAsciiChars` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `InvalidInput(msg)` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `Io(err)` while building MIME | `Request(Malformed)` | `Request(Malformed { detail })` |

Builder `Io` is local content generation failure, not SMTP network
transport.

## Enhanced status code mapping

Prefer enhanced status codes over normal 3-digit status codes. The
enhanced code has shape `class.subject.detail`.

Class rules:

- Class `4` is transient.
- Class `5` is permanent.
- Class `2` should not appear in an error conversion. If it does, treat
  it as `Protocol(ContractViolation)`.

### X.1 address status

| Enhanced code | Account kind | Primary cause |
| --- | --- | --- |
| `X.1.0` other address status | Class fallback | Wire cause plus text. |
| `X.1.1` bad destination mailbox | `NotFound(Mailbox)` | `Request(NotFound { what: Mailbox, id })` |
| `X.1.2` bad destination system | `NotFound(Mailbox)` | `Request(NotFound { what: Mailbox, id })` |
| `X.1.3` bad destination mailbox syntax | `Request(Malformed)` | `Request(Malformed { detail })` |
| `X.1.4` ambiguous destination mailbox | `Request(Malformed)` | `Request(Malformed { detail })` |
| `X.1.5` destination address valid | Class fallback | Usually delivery status, not failure. |
| `X.1.6` mailbox moved | `NotFound(Mailbox)` | `Request(NotFound { what: Mailbox, id })` |
| `X.1.7` bad sender mailbox syntax | `Request(Malformed)` | `Request(Malformed { detail })` |
| `X.1.8` bad sender system | `Request(Malformed)` | `Request(Malformed { detail })` |

For per-recipient failures, put the recipient address string in the
`NotFound` id when available. For batch-level `MAIL FROM` failures, put
the sender address in support-only diagnostics instead of `NotFound`.

### X.2 mailbox status

| Enhanced code | Account kind | Primary cause |
| --- | --- | --- |
| `X.2.0` other mailbox status | Class fallback | Wire cause plus text. |
| `X.2.1` mailbox disabled | `Authorization(MailboxUnavailable { kind: MailboxUnavailableKind::Permanent })` for class 5, transient for class 4 | `AccessCause::MailboxUnavailable { kind }` |
| `X.2.2` mailbox full | `Server(QuotaExhausted)` | `Server(QuotaExhausted { retry_after: None })` |
| `X.2.3` message length exceeds admin limit | `Request(Malformed)` | `Request(Malformed { detail })` |
| `X.2.4` mailing list expansion problem | Class fallback | Usually `Server(Unavailable)` for class 4, `Server(Error { status: Some(code) })` for class 5. |

Use `ThrottleScope::Mailbox` for `X.2.2` when the failure is tied to a
recipient lane. Use `ThrottleScope::Account` for sender/account quota
failures.

### X.3 mail system status

| Enhanced code | Account kind | Primary cause |
| --- | --- | --- |
| `X.3.0` other mail system status | Class fallback | Wire cause plus text. |
| `X.3.1` mail system full | `Server(QuotaExhausted)` | `Server(QuotaExhausted { retry_after: None })` |
| `X.3.2` system not accepting network messages | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` |
| `X.3.3` system not capable of selected features | `Unsupported(Send)` | `Request(Unsupported { operation: Send })` |
| `X.3.4` message too big for system | `Request(Malformed)` | `Request(Malformed { detail })` |
| `X.3.5` system incorrectly configured | `Server(Error { status: Some(code) })` | `ServerCause::Error { status: code }` |

### X.4 network and routing status

| Enhanced code | Account kind | Primary cause |
| --- | --- | --- |
| `X.4.0` other network/routing status | `Server(Unavailable)` for class 4, `Server(Error)` for class 5 | Server cause. |
| `X.4.1` no answer from host | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` |
| `X.4.2` bad connection | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` |
| `X.4.3` routing server failure | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` |
| `X.4.4` unable to route | `Server(Unavailable)` for class 4, `NotFound(Mailbox)` for class 5 recipient lane | Server or not-found cause. |
| `X.4.5` network congestion | `Server(RateLimited)` | `Server(RateLimited { retry_after: None })` |
| `X.4.6` routing loop detected | `Server(Error { status: Some(code) })` | Server cause. |
| `X.4.7` delivery time expired | `Server(Unavailable)` for class 4, `Server(Error)` for class 5 | Server cause. |

### X.5 mail delivery protocol status

| Enhanced code | Account kind | Primary cause |
| --- | --- | --- |
| `X.5.0` other protocol status | `Protocol(ContractViolation)` | SMTP wire cause. |
| `X.5.1` invalid command | `Protocol(ContractViolation)` | SMTP wire cause. |
| `X.5.2` syntax error | `Request(Malformed)` | Request cause. |
| `X.5.3` too many recipients | `Server(RateLimited)` | Server rate-limited cause. |
| `X.5.4` invalid command arguments | `Request(Malformed)` | Request cause. |
| `X.5.5` wrong protocol version | `Unsupported(Send)` | Request unsupported cause. |

### X.6 message content or media status

| Enhanced code | Account kind | Primary cause |
| --- | --- | --- |
| `X.6.0` other media error | `Request(Malformed)` | Request cause. |
| `X.6.1` media not supported | `Request(Malformed)` | Request cause. |
| `X.6.2` conversion required and prohibited | `Request(Malformed)` | Request cause. |
| `X.6.3` conversion required but unsupported | `Request(Malformed)` | Request cause. |
| `X.6.4` conversion with loss performed | `Protocol(PartialResponse)` if class 4/5 failure | SMTP wire cause. |
| `X.6.5` conversion failed | `Request(Malformed)` | Request cause. |

### X.7 security or policy status

| Enhanced code | Account kind | Primary cause |
| --- | --- | --- |
| `X.7.0` other security/policy status | `Authorization(PolicyBlocked)` | Access cause. |
| `X.7.1` delivery not authorized or message refused | `Authorization(PermissionDenied)` | Access cause. |
| `X.7.2` mailing list expansion prohibited | `Authorization(PermissionDenied)` | Access cause. |
| `X.7.3` security conversion required but impossible | `Authorization(PolicyBlocked)` | Access cause. |
| `X.7.4` security features not supported | `Authorization(PolicyBlocked)` | Access cause. |
| `X.7.5` cryptographic failure | `Authorization(PolicyBlocked)` | Access cause. |
| `X.7.6` cryptographic algorithm unsupported | `Authorization(PolicyBlocked)` | Access cause. |
| `X.7.7` message integrity failure | `Request(Malformed)` | Request cause. |
| `X.7.8` authentication credentials invalid | `Authentication(ReauthorizationRequired)` | Auth cause. |
| `X.7.9` authentication mechanism too weak | `Authorization(PolicyBlocked)` | Access cause. |
| `X.7.10` encryption needed | `Authorization(PolicyBlocked)` | Access cause. |
| `X.7.11` encryption required | `Authorization(PolicyBlocked)` | Access cause. |
| `X.7.12` password transition needed | `Authentication(ReauthorizationRequired)` | Auth cause. |
| `X.7.13` user account disabled | `Authorization(AccountDisabled)` | Access cause. |
| `X.7.14` trust relationship required | `Authorization(PolicyBlocked)` | Access cause. |
| `X.7.15` priority level too low | `Authorization(PolicyBlocked)` | Access cause. |
| `X.7.16` message too big for specified priority | `Request(Malformed)` | Request cause. |
| `X.7.17` mailbox owner changed | `Authorization(PermissionDenied)` | Access cause. |
| `X.7.18` domain owner changed | `Authorization(PermissionDenied)` | Access cause. |
| `X.7.19` RRVS test cannot complete | `Server(Unavailable)` | Server cause. |
| `X.7.20` no passing DKIM signature | `Request(Malformed)` | Request cause. |
| `X.7.21` no acceptable DKIM signature | `Request(Malformed)` | Request cause. |
| `X.7.22` no valid author-matched DKIM signature | `Request(Malformed)` | Request cause. |
| `X.7.23` SPF validation failed | `Authorization(PolicyBlocked)` | Access cause. |
| `X.7.24` SPF validation error | `Authorization(PolicyBlocked)` | Access cause. |
| `X.7.25` reverse DNS validation failed | `Authorization(PolicyBlocked)` | Access cause. |
| `X.7.26` multiple authentication checks failed | `Authorization(PolicyBlocked)` | Access cause. |
| `X.7.27` sender address has null MX | `Request(Malformed)` | Request cause. |
| `X.7.28` mail flood detected | `Server(RateLimited)` | Server rate-limited cause. |
| `X.7.29` ARC validation failure | `Request(Malformed)` | Request cause. |
| `X.7.30` REQUIRETLS support required | `Authorization(PolicyBlocked)` | Access cause. |

Codes beyond this list in subject `7` should fall back by class:

- class 4: `Server(RateLimited)` when text contains "rate", "limit",
  or "throttle"; otherwise `Server(Unavailable)`
- class 5: `Authorization(PolicyBlocked)`

## Normal SMTP status fallback

Use this table when no enhanced status code is present.

| Status | Account kind | Primary cause |
| --- | --- | --- |
| `421` | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` |
| `432` | `Authentication(RefreshTransient)` | `Auth(RefreshTransient)` |
| `450` | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` |
| `451` | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` |
| `452` | `Server(QuotaExhausted)` | `Server(QuotaExhausted { retry_after: None })` |
| `454` | `Authentication(RefreshTransient)` during AUTH, else `Server(Unavailable)` | Auth or server cause. |
| `455` | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` |
| Other `4xx` | `Server(Unavailable)` | `Server(Unavailable { retry_after: None })` |
| `500` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `501` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `502` | `Unsupported(operation)` | `Request(Unsupported { operation })` |
| `503` | `Protocol(ContractViolation)` | SMTP wire cause. |
| `504` | `Unsupported(operation)` | `Request(Unsupported { operation })` |
| `530` | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` |
| `534` | `Authorization(PolicyBlocked)` | `Access(PolicyBlocked)` |
| `535` | `Authentication(ReauthorizationRequired)` | `Auth(ReauthorizationRequired)` |
| `538` | `Authorization(PolicyBlocked)` | `Access(PolicyBlocked)` |
| `550` | `NotFound(Mailbox)` for recipient lane, else `Authorization(PermissionDenied)` | Request or access cause. |
| `551` | `NotFound(Mailbox)` | `Request(NotFound { what: Mailbox, id })` |
| `552` | `Server(QuotaExhausted)` if mailbox/storage text, else `Request(Malformed)` | Server or request cause. |
| `553` | `Request(Malformed)` | `Request(Malformed { detail })` |
| `554` | `Authorization(PolicyBlocked)` if policy/security text, else `Server(Error { status: Some(554) })` | Access or server cause. |
| `555` | `Unsupported(operation)` | `Request(Unsupported { operation })` |
| Other `5xx` | `Server(Error { status: Some(code) })` | Server cause. |

If a status code is positive but an error path is trying to convert it,
emit `Protocol(ContractViolation)`.

## Batch send API

Do not change `Transport::send_raw`. Add Account-oriented helpers behind
`account-error`.

### Success-lane payload: `BatchOutcome<()>`

The new helpers return `BatchOutcome<()>` — the success lane carries
no per-recipient payload. Rationale: LMTP per-recipient 2xx replies
carry an enhanced status code and acceptance text (e.g. `2.1.5
destination valid`), and SMTP DATA-final 2xx similarly carries a
queue-id-style message. These are forensic for delivery audit but not
part of the typed success contract: caller-side state correlation
goes through `BatchItemId`, not server-supplied per-recipient
payload. If a caller needs the acceptance text, it is reachable
through the support-only diagnostic surface of any subsequent error
on that recipient (`AccountError::support_consented()`), and through
the LMTP transport's existing public `Vec<Response>` return for
callers using `send_raw_lmtp` directly. `BatchOutcome<()>` is the
correct shape for "every recipient is accounted for in a lane; the
lane is what mattered."

Recommended sync surface:

```rust
pub struct SmtpBatchRecipient {
    pub id: BatchItemId,
    pub address: Address,
}

impl SmtpTransport {
    pub fn send_raw_batch_with_options(
        &self,
        from: Option<Address>,
        recipients: Vec<BatchItem<Address>>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<BatchOutcome<()>, AccountError>;
}

impl LmtpTransport {
    pub fn send_raw_batch_with_options(
        &self,
        from: Option<Address>,
        recipients: Vec<BatchItem<Address>>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<BatchOutcome<()>, AccountError>;
}
```

Recommended async surface:

```rust
impl AsyncSmtpTransport<TokioExecutor> {
    pub async fn send_raw_batch_with_options(...) -> Result<BatchOutcome<()>, AccountError>;
}

impl AsyncLmtpTransport<TokioExecutor> {
    pub async fn send_raw_batch_with_options(...) -> Result<BatchOutcome<()>, AccountError>;
}
```

If generic executor support is straightforward, implement it for all
`E: Executor + SmtpExecutor`; otherwise start with `TokioExecutor` and
record the limitation in the implementation audit.

Input validation:

- Empty recipient list:
  `Err(AccountErrorKind::Request(RequestErrorKind::BatchInputInvalid))`.
- Empty `BatchItemId`: same batch-invalid error.
- Duplicate `BatchItemId`: same batch-invalid error.
- Invalid recipient address cannot occur if input type is `Address`,
  but any conversion from strings must fail before transport with
  `RequestErrorKind::BatchInputInvalid`.
- Recipient-specific `SendOptions` that do not match an input recipient
  fail as `Err(Request(Malformed))` before transport.

`bifrost_types::error::batch::validate_batch_input` is currently
`pub(crate)` to `bifrost-types`. SMTP duplicates the small validation
in `crates/smtp/src/transport/smtp/batch.rs` for Phase 2. The
duplication is a single ~15-line function over a `Vec<BatchItem<I>>`
that produces `Vec<BatchInputInvalidItem>` — small enough that
duplicating it is cheaper than blocking SMTP on a Phase 1 visibility
change.

Phase 3.5 follow-up: promote `validate_batch_input` to `pub` in
`bifrost-types` and converge SMTP, IMAP, JMAP, Gmail, and Graph onto
the single helper. Add a NOTE referencing this in the duplicated SMTP
function so the follow-up has an anchor.

## Batch outcome rules

`Err(AccountError)` means no recipient-specific outcome can be
reported. Use it for:

- local message/envelope/batch validation failure
- pool checkout failure before a connection is available
- DNS/connect/TLS/greeting/EHLO/AUTH/setup failure
- `MAIL FROM` rejection (transmitted and acknowledged-negative —
  see rationale below)
- `MAIL FROM` write/read transport drop before any recipient is
  accepted

`MAIL FROM` rejection rationale vs convergence boundary invariant 5
("Global post-transmission failures are expanded to item-level
entries"): `MAIL FROM` is the sender envelope, not a per-recipient
operation. A server reject of `MAIL FROM` is global to the
transaction and pre-recipient — no recipient ever had bytes
transmitted for its lane. The convergence invariant 5 covers cases
where the batch reached recipients and was rejected en bloc (e.g.
a post-DATA transaction rollback); `MAIL FROM` reject is logically
"the side-effect boundary was never crossed for any recipient"
because the recipient stage was never reached. Returning `Err` is
correct here; fanning out the global error to all recipients would
fabricate per-recipient evidence the wire never produced.

`MAIL FROM` transport drop (write or read failure) is similar: no
recipient command was issued, so no recipient lane has anything to
fan out to. Return `Err` with the underlying transport error.

`Ok(BatchOutcome)` means every submitted recipient appears exactly once
in `succeeded`, `failed`, or `uncertain`.

Recipient lane rules:

| Condition | Lane |
| --- | --- |
| RCPT rejected with 4xx or 5xx | `failed` with response-mapped `AccountError` |
| RCPT accepted and SMTP final DATA/BDAT 2xx received | `succeeded` |
| RCPT accepted and DATA command rejected before body | `failed` with DATA response error |
| RCPT accepted and DATA/BDAT final 4xx/5xx received | `failed` with final response error |
| RCPT accepted and connection drops after body write starts | `uncertain` with in-flight transport error |
| RCPT accepted and LMTP final 2xx received | `succeeded` |
| RCPT accepted and LMTP final 4xx/5xx received | `failed` with final response error |
| RCPT accepted and LMTP final status is not read due to drop | `uncertain` |
| PIPELINING response drain fails after batch write | recipients without definitive RCPT or final status become `uncertain` |

All per-recipient failure errors should use:

- `operation: Send`
- `protocol: Smtp` or `Lmtp`
- `idempotency_override(false)` for send-lane errors. This is redundant
  with `AccountOperation::Send`, but explicit. It only changes recovery
  when the attempt state is `InFlight`.
- support-only diagnostic text containing the recipient address.

## Batch progress tracker

Current `send_with_options`, `send_pipelined`,
`send_lmtp_with_options`, and BDAT variants return only final
`Response`, `Vec<Response>`, or `Error`. That is not enough to build
precise `BatchOutcome` after partial progress.

Add a local tracker in `batch.rs`:

```rust
struct RecipientProgress {
    id: BatchItemId,
    address: Address,
    rcpt: RcptProgress,
}

enum RcptProgress {
    Pending,
    Accepted,
    Rejected(Response),
    Final(Response),
    Uncertain(AccountError),
}

struct SendProgress {
    recipients: Vec<RecipientProgress>,
    body_started: bool,
    body_finished: bool,
    data_response: Option<Response>,
}
```

The tracker should be driven by the same command sequence as the
existing send paths. Do not try to infer recipient lanes from a final
transport `Error` alone.

Existing public `send_raw` methods may continue to use the current
paths. The new batch helpers can use shared lower-level helpers if the
implementation can keep the diff manageable.

## SMTP sequential send behavior

For non-PIPELINING SMTP:

1. Validate MAIL and RCPT parameters before sending.
2. Send `MAIL FROM`.
3. If `MAIL FROM` rejects, return `Err(AccountError)`.
4. Send each `RCPT TO` with status accepted.
5. Record rejected recipients as failed lanes; continue trying later
   recipients.
6. If no recipients are accepted, return `Ok(BatchOutcome)` with every
   recipient failed.
7. Send `DATA`.
8. If `DATA` rejects before body, mark accepted recipients failed with
   that response.
9. Write DATA body.
10. If body write or final response read drops, mark accepted
    unresolved recipients uncertain.
11. If final response is negative, mark accepted recipients failed.
12. If final response is positive, mark accepted recipients succeeded.

This differs from current `send_with_options`, which aborts on the
first RCPT rejection. Keep current behavior for existing `send_raw`,
but the batch helper must continue through RCPTs to build per-recipient
lanes.

## SMTP PIPELINING behavior

Current `send_pipelined` writes `MAIL FROM`, every `RCPT TO`, and
`DATA` in one batch, drains replies in order, and writes the body only
after all replies are positive.

For the batch helper:

- Keep body out of the pipelined command batch.
- After the command batch write succeeds, the attempt is `InFlight`
  for the queued commands.
- Drain `MAIL FROM`; if it rejects, return `Err(AccountError)`.
- Drain every `RCPT TO`; negative RCPT replies become failed lanes.
- Drain `DATA`.
- If no recipients were accepted and `DATA` is positive, send the
  terminating dot as current code does or reset/abort according to the
  existing transaction cleanup logic. Return `Ok(BatchOutcome)` with
  failed lanes.
- If `DATA` is negative, mark accepted recipients failed with the DATA
  response.
- If the drain fails before a recipient's RCPT reply is known, that
  recipient is uncertain. This is conservative; no body was sent, but
  the server may have accepted the command batch and the connection is
  now ambiguous.
- If body write or final response read fails after `DATA` positive,
  accepted recipients without final status are uncertain.

Do not collapse a single RCPT failure into a batch-level `Err` for the
new helper.

## LMTP behavior

Current LMTP returns a response vector with one entry per envelope
recipient:

- RCPT-time rejections are preserved at the original index.
- Accepted recipients receive post-DATA delivery responses in original
  order.

Keep that public behavior.

For `BatchOutcome`:

1. Send `MAIL FROM`; batch-level `Err` on rejection.
2. Send every `RCPT TO` accepting negative statuses.
3. Mark RCPT rejects as failed lanes.
4. If no recipients were accepted, return `Ok(BatchOutcome)` with all
   failed lanes.
5. Send `DATA` or `BDAT`.
6. Once body write starts, unresolved accepted recipients become
   uncertain if the connection drops.
7. Read one final LMTP status per accepted recipient.
8. Each final status maps directly to the matching accepted recipient's
   lane.
9. If the final status loop drops after N final statuses, recipients
   with read statuses are succeeded/failed, remaining accepted
   recipients are uncertain.

The existing async LMTP final loop already holds the connection state
`Broken` until all accepted recipient statuses are read. Mirror that
discipline in the sync path if needed for batch tracking.

## BDAT behavior

BDAT is explicit. Do not silently switch DATA sends to BDAT.

For batch helpers:

- Expose separate `send_raw_bdat_batch_with_options` if implementing
  BDAT batch support now.
- If not implemented in Phase 2, leave existing BDAT APIs unchanged and
  map BDAT errors through `into_account_error` only.
- `BDAT LAST` body write starts the side-effect boundary. Transport
  failure after this point produces uncertain lanes for accepted
  recipients.
- A final negative BDAT reply is acknowledged and produces failed lanes,
  not uncertain lanes.

## DSN and SendOptions

DSN support is already encoded by `SendOptions`:

- MAIL FROM: `RET`, `ENVID`
- RCPT TO: `NOTIFY`, `ORCPT`
- recipient-specific RCPT parameters override global parameters

Mapping rules:

- Local DSN parameter validation failure is `Request(Malformed)`.
- DSN parameter used when server does not advertise DSN is
  `Request(Malformed)` because the client constructed a request this
  server cannot accept.
- Server `555` or enhanced `X.5.5` for DSN parameters is
  `Unsupported(Send)`.
- DSN bounce reports received later are out of scope and must not
  affect the send call's return.

Add diagnostic text for ENVID/ORCPT only if it is already xtext-safe
and does not contain private content. Otherwise omit it.

## Authentication mapping

AUTH happens during connection setup, before message delivery.

Map:

- `5.7.8`, `535`: `Authentication(ReauthorizationRequired)`
- `4.7.8`, `454`: `Authentication(RefreshTransient)`
- `5.7.9`, `534`, `538`: `Authorization(PolicyBlocked)`
- plaintext AUTH refusal (`ErrorKind::Policy`): `Authorization(PolicyBlocked)`
- no compatible authentication mechanism (`InvalidInput` during AUTH):
  `Unsupported(Send)` if credentials were required for send, otherwise
  `Authorization(PolicyBlocked)`

AUTH challenge parse failures are `Protocol(ParseFailed)` or
`Protocol(ContractViolation)` depending on whether the parser failed
or the sequence exceeded the challenge cap.

## TLS and security policy mapping

Map:

- Native TLS connector/certificate/hostname failure:
  `Transport(Tls)` with `Attempt(Unsent)`.
- STARTTLS required but server lacks STARTTLS:
  `Authorization(PolicyBlocked)`.
- STARTTLS command rejected:
  response mapping, usually `Authorization(PolicyBlocked)` for policy
  codes or `Server(Unavailable)` for transient codes.
- Unix-domain LMTP STARTTLS refusal:
  `Request(Malformed)` or `Authorization(PolicyBlocked)` depending on
  whether the caller explicitly requested TLS.
- REQUIRETLS requested on plaintext connection:
  `Authorization(PolicyBlocked)`.
- REQUIRETLS unsupported by server:
  `Request(Malformed)` before send, or `Unsupported(Send)` if server
  rejects it on wire.

## Message builder mapping

Account-oriented send code that builds a `Message` must map
`crate::error::Error` with `message_error_to_account_error`.

Rules:

- Missing recipients are `Request(Malformed)`.
- Missing sender is `Request(Malformed)` unless the Account
  configuration can supply a default identity before SMTP is called.
- Non-ASCII address/content failures before SMTPUTF8/8BITMIME
  negotiation are `Request(Malformed)`.
- Attachment filename parse failures are `Request(Malformed)`.
- MIME formatting I/O is local request construction failure, not
  transport.

## Public API compatibility

Do not break these existing APIs in this phase:

- `Transport::send`
- `Transport::send_raw`
- `AsyncTransport::send`
- `AsyncTransport::send_raw`
- `SmtpTransport::send_raw_with_options`
- `LmtpTransport::send_raw_with_options`
- `send_raw_bdat` and `send_raw_bdat_with_options`
- `Response`, `Code`, and transport
  `response::EnhancedStatusCode`
- transport `Error` and `ErrorKind`

The new shared-error APIs are additive and feature-gated.

## Tests

Keep tests deterministic. Do not add live SMTP, live LMTP, Docker, fixed
ports, external accounts, or new mock server harnesses for this phase.
Use synthetic `Response`, `Error`, and progress-tracker objects.

Minimum test list:

1. Message-builder `MissingTo` maps to `Request(Malformed)`.
2. Transport `Network` with `Unsent` maps to retryable
   `Transport(Network)`.
3. Transport `Network` with `InFlight` and `Send` maps to reconcile.
4. Transport `Timeout` with `InFlight` maps to transport timeout with
   attempt cause.
5. Transport `Tls` maps to `Transport(Tls)`.
6. `ErrorKind::Policy` maps to `Authorization(PolicyBlocked)`.
7. `5.1.1` maps to `NotFound(Mailbox)`.
8. `5.1.3` maps to `Request(Malformed)`.
9. `4.2.2` maps to quota exhausted with retry recovery.
10. `5.2.2` maps to quota exhausted with provider-refused or retry
    semantics derived by the builder.
11. `4.4.5` maps to rate limited.
12. `5.5.2` maps to request malformed.
13. `5.7.1` maps to permission denied or policy blocked according to
    table.
14. `5.7.8` maps to auth reauthorization required.
15. `4.7.8` maps to auth refresh transient.
16. `421` fallback maps to server unavailable.
17. `452` fallback maps to quota exhausted.
18. `535` fallback maps to auth reauthorization required.
19. `555` fallback maps to unsupported send.
20. Wire cause includes normal status, enhanced status text, and first
    response line.
21. Batch validation rejects empty input as
    `Request(RequestErrorKind::BatchInputInvalid)`.
22. Batch validation rejects duplicate item ids as
    `Request(RequestErrorKind::BatchInputInvalid)`.
23. SMTP batch all-success puts all recipients in `succeeded`.
24. SMTP batch mixed RCPT rejects puts rejected recipients in `failed`
    and accepted recipients in `succeeded`.
25. SMTP batch all RCPT rejects returns `Ok(BatchOutcome)` with all
    recipients failed, not `Err`.
26. SMTP DATA negative reply marks accepted recipients failed.
27. SMTP drop after body start marks accepted unresolved recipients
    uncertain.
28. PIPELINING RCPT failure is per-recipient failed lane in the new
    batch helper.
29. PIPELINING response drain drop marks unresolved recipients
    uncertain.
30. LMTP mixed final statuses preserve original recipient order.
31. LMTP final-status read drop preserves read statuses and marks the
    rest uncertain.
32. BDAT unsupported maps to request malformed or unsupported according
    to whether failure is local validation or wire response.

Existing tests that assert low-level `ErrorKind` behavior should remain
where they cover the public transport API. Add shared-error tests
separately under the `account-error` feature.

## Exit criteria

- `bifrost-types` is available to SMTP through the chosen feature.
- `transport/smtp/account_error.rs` exists and maps transport and
  message-builder errors to `AccountError`.
- All SMTP wire reply errors produce `WireCause::Smtp`.
- Enhanced status codes drive classification before 3-digit fallback.
- Attempt state is preserved on transport errors that cross command
  boundaries.
- Existing public transport send APIs still return their current
  shapes.
- New Account-oriented batch helper returns
  `Result<BatchOutcome<()>, AccountError>`.
- Batch helper returns `Err` only when no recipient-specific outcome can
  be reported.
- Batch helper accounts for every submitted `BatchItem` exactly once in
  `Ok(BatchOutcome)`.
- LMTP per-recipient finalization feeds the correct lanes.
- SMTP/LMTP drops after DATA/BDAT body start mark unresolved recipients
  uncertain.
- Local batch input validation returns
  `Request(RequestErrorKind::BatchInputInvalid)`.
- No `SmtpProtocol` enum is introduced in `crates/smtp/`;
  `bifrost_types::Protocol::{Smtp, Lmtp}` is used directly.
- No protocol-specific recovery or fatal constructors are added in
  `crates/smtp/`. The existing transport `Error::kind()`,
  `is_transient()`, `is_permanent()`, and class predicates remain as
  the low-level transport API but are not consumed by the new
  account-error mapper for recovery construction — recovery is
  derived centrally by `AccountErrorBuilder`.
- Every kind/cause pair produced by `into_account_error` and
  `message_error_to_account_error` satisfies
  `recovery::kind_matches_cause`.
- No source comments point at files under `plans/`.

Compilation and workspace-wide checks are Phase 3 work. Do not run
`brokkr check` merely for this planning phase.

## Audit checklist for the implementation agent

1. Search for `account-error` feature coverage. Shared-error tests must
   compile under that feature.
2. Search for `Error::new(` and `Error::without_source(` in
   `transport/smtp/error.rs`. Every transport boundary should be able
   to attach attempt state before account conversion.
3. Search for `error::network`, `error::timeout`, and `error::tls`.
   Setup paths should attach `Unsent`; send paths should attach
   `InFlight` when appropriate.
4. Verify AUTH failures map semantically rather than becoming generic
   server errors.
5. Verify `WireCause::Smtp` is attached for every `Transient` and
   `Permanent` reply conversion.
6. Verify positive SMTP replies cannot become server errors silently.
7. Verify existing `Transport::send_raw` and `AsyncTransport::send_raw`
   signatures are unchanged.
8. Verify SMTP batch all-RCPT-rejected is `Ok(BatchOutcome)`, not
   batch-level `Err`.
9. Verify SMTP batch `MAIL FROM` rejection is batch-level `Err`.
10. Verify DATA/BDAT body-start state is tracked explicitly.
11. Verify LMTP final status count mismatch cannot panic in the
    Account-oriented helper; unresolved recipients become uncertain.
12. Verify recipient lane order follows input `BatchItem` order.
13. Verify batch helper does not stringify structured `AccountError`s.
14. Verify no live-server tests were added.
