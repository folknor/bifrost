# bifrost-smtp reference

Current architecture of the SMTP client crate. Descended from lettre 0.11.22; the bifrost fork has substantially diverged. Native-tls only.

## Transport types

Sync and async, SMTP and LMTP:

- `SmtpTransport` / `AsyncSmtpTransport` - submission to MX/relay.
- `LmtpTransport` / `AsyncLmtpTransport` - local delivery via LMTP.

`Transport::Ok` is `Response` for SMTP, `Vec<Response>` for LMTP (one per envelope recipient, original order preserved).

Async transports support Tokio. TLS and plaintext URL parsing via `from_url` use the Tokio backend.

`BoxedTransport<Ok, Error>` and `BoxedAsyncTransport<Ok, Error>` erase the concrete type. `Box<T>` and `Arc<T>` get blanket `Transport` / `AsyncTransport` forwarding.

## Connection lifecycle

```
greeting -> EHLO/LHLO -> [STARTTLS -> EHLO/LHLO] -> [AUTH -> EHLO/LHLO] -> ready
```

EHLO is re-sent after STARTTLS and AUTH because capabilities can change.

- `abort()` closes the connection. There is no live QUIT path; the Quit command builder is gone. If a graceful shutdown is wanted later it will need to be re-added with a real call site.

## Connection state and cancel-safety

Sync and async streams carry an explicit state: `Ok` / `Broken` / `Closed`. Writes, flushes, reads, and TLS upgrades set `Broken` before the await; only success restores `Ok`. Dropped futures leave state `Broken`; the pool drops the connection.

LMTP final delivery-status loop holds `Broken` until every accepted recipient's status has been read. A surplus final status that already reached the read buffer is a protocol violation: the connection is marked `Broken` and the drain fails (the batch driver keeps the per-recipient outcomes it already has, since the surplus changes only the stream's reusability).

Surplus bytes that have not yet crossed into the `BufReader` - still in the socket or a TLS record - cannot be observed without a read that would block against a well-behaved peer, and native-tls exposes no nonblocking peek that would make such a probe honest. So cleanliness is never positively established: every LMTP final-status drain sets a retirement flag (`should_retire()`), and the pool discards those connections at recycle instead of parking them. The cost is one reconnect per LMTP transaction; LMTP is local delivery, so that is cheaper than recycling a possibly desynchronized stream. SMTP connections are unaffected and still pool normally.

`test_connected()` (pooled NOOP probe) aborts on failure so a stale connection cannot be recycled.

## Async setup deadline

`AsyncDeadline` is a single shared deadline across DNS, connect, TLS handshake, banner read, and initial EHLO. The deadline lives only until `connect_impl` returns; established connections use the per-operation timeout. `starttls(...)` on an established connection uses the per-operation timeout because it is an explicit command, not setup.

## PIPELINING

When the server advertises PIPELINING, `MAIL FROM` and `RCPT TO` commands are written in bounded recipient windows, with every reply in one window drained before the next is written. `DATA` is issued only after all recipient windows complete; the body is never in the pipelined batch. On RCPT failure mid-pipeline the transaction is reset before the body. The shared window bound respects the peer TCP window while preserving the original recipient indexes in `SendProgress`.

## DSN and SendOptions

`SendOptions` is the per-message API for advanced ESMTP parameters:

- MAIL FROM params: `REQUIRETLS`, `FUTURERELEASE` (HOLDFOR / HOLDUNTIL), `DELIVERBY`, `MT-PRIORITY` (RFC 6710), DSN `RET`, DSN `ENVID`.
- Uniform RCPT TO params: DSN `NOTIFY`.
- Recipient-specific RCPT TO params: DSN `NOTIFY` overrides per recipient, `ORCPT` (default addr-type `rfc822`).

Server-advertised `DSN` is verified before DSN parameters are emitted. Recipient-specific params override matching global keywords (RFC 3461 §4.1 no-duplicate-keyword). Automatic DATA `SIZE` declarations account for the DATA terminator's leading CRLF but exclude the terminator line itself and transparency dots, as RFC 1870 requires; BDAT uses its raw payload length.

Batch sends validate every recipient's RCPT parameters before `MAIL FROM`, in both sync and async paths. A local parameter error is an unsent `RcptTo` failure, never a silent downgrade to an unparameterized RCPT command.

Public entry points: `send_raw_with_options(...)` on SMTP and LMTP, sync and async.

## xtext encoding

RFC 3461 §4.1: any byte outside `%x21-%x7E`, plus `+`, plus `=`, is encoded as `+HH` with uppercase two-digit hex. UTF-8 multi-byte sequences are encoded byte-by-byte. Used for ENVID, ORCPT address part, and unknown ESMTP parameter values. Centralized in `util.rs`.

## VRFY / EXPN

`verify(addr, ...)` and `expand(list, ...)` on SMTP transports. Negative replies return as `Response`, not `Err`, because they are normal outcomes for these privacy-sensitive commands. Inputs are sanitized for CRLF and control characters before any wire write.

## BDAT

Opt-in via explicit `send_raw_bdat` / `send_raw_bdat_with_options`. Defaults still use DATA so dot-stuffing and BINARYMIME behavior are not changed silently. BDAT requires `CHUNKING` to be advertised.

## SIZE enforcement

If the server advertises `SIZE=<bytes>`, message size is checked client-side before MAIL FROM, and the advertised value is included as `SIZE=<bytes>` on the wire.

`SIZE 0` means "no declared maximum" per RFC 1870 section 3 and is stored as no limit. An unparseable limit is a hard `Parse` error on the EHLO reply rather than a silently dropped ceiling.

For DATA the declared size is `message.len() + 2`. Every DATA writer terminates with `\r\n.\r\n` unconditionally, so the CRLF before the terminating dot is always an extra octet pair on the wire - a message that already ends in CRLF gains a trailing empty line rather than reusing its own CRLF as the terminator's line break. Declaring the bare buffer length would under-report by two octets for every well-formed message. BDAT declares the raw chunk length because it has no terminator or transparency layer.

## Auth

`Credentials` is enum: `Password { username, password: Zeroizing<String> }` and `OAuth2 { identity, token_source: Arc<dyn TokenSource> }` (bifrost-net's trait). The OAuth token is read live from the shared source at each connect, so a token rotated on the source is presented on reconnect without rebuilding the transport. `Credentials` is `Clone` only - no `PartialEq`/`Eq`/serde derives (a live token source is neither comparable nor serializable; rotation material is the consumer's to persist). The `AUTH` command struct (`Auth`) likewise dropped those derives. The async transport reads the token via `oauth2_token().await`; the blocking transport via `oauth2_token_blocking()`, a single-poll of `current()` (a `StaticTokenSource` or already-fresh `OAuthRefresher` resolves immediately; a source needing a network refresh is rejected - live refresh requires the async transport).

Mechanisms (`Mechanism`, `#[non_exhaustive]`): PLAIN, LOGIN, XOAUTH2, OAUTHBEARER, SCRAM-SHA-1, SCRAM-SHA-256, SCRAM-SHA-1-PLUS, SCRAM-SHA-256-PLUS. SCRAM is consumed from `bifrost-sasl` (the `-PLUS` suffix and token spelling have a single authority there); SMTP owns only the wire sequencing.

Password selection (`password_mechanism_order` + `first_attemptable` in `authentication.rs`): the advertised set is intersected with the allowed set in the fixed order `SCRAM-SHA-256-PLUS > SCRAM-SHA-1-PLUS > SCRAM-SHA-256 > SCRAM-SHA-1 > PLAIN > LOGIN`. RFC 5802 Section 6 downgrade protection: the unbound `SCRAM-SHA-N` rung is dropped when `SCRAM-SHA-N-PLUS` is advertised. A PLUS rung whose channel binding cannot resolve (plaintext, EdDSA leaf cert) is skipped and the walk falls through to the next safe rung; only binding-unavailability falls through, a wire-level rejection propagates. SCRAM runs as a no-IR `334` challenge exchange driven by `ScramExchange`; `Mechanism::response` is never called for SCRAM. PLUS channel binding (`tls-server-end-point`) comes from the cached peer-cert DER, no extra round trip.

OAuth credentials never use SCRAM: they pick the first advertised OAUTHBEARER/XOAUTH2 rung in caller order (`oauth_mechanism`) and run the stateless encoder. The connection's auth driver resolves the access token from the source once, up front, and threads it into `Auth::new` / `Mechanism::response_with_token`. The XOAUTH2 / OAUTHBEARER payload bytes are built by `bifrost-sasl` (`xoauth2_payload` / `oauthbearer_payload`, including the OAUTHBEARER GS2 identity escape); `response_with_token` only base64-frames them and owns the RFC 7628 `\x01` error-continuation. OAUTHBEARER before XOAUTH2 by default.

**Default behavior change (migration note).** `PASSWORD_MECHANISMS` now defaults to SCRAM (strongest first) then PLAIN, and LOGIN is no longer in it (opt-in legacy, mirroring IMAP's `allow_login = false`). A server advertising only `AUTH LOGIN` therefore yields an empty attempt order under the default and fails with "no compatible authentication mechanism" instead of silently downgrading to LOGIN. Callers that need LOGIN must pass it explicitly via `authentication(vec![Mechanism::Login, ..])`.

Plaintext AUTH is refused by default for both passwords and OAuth bearer tokens. Trusted local relays opt in with `dangerous_allow_insecure_auth(true)`.

AUTH continuation formatting treats challenge responses as continuation lines even for mechanisms supporting initial response. Required for OAUTHBEARER failed-auth dummy-cancel exchange (`AQ==` on the wire).

`SaslError` maps at the `From<SaslError> for Error` boundary: `Protocol` (malformed SASL, signature mismatch) to `ErrorKind::Parse`; `AuthFailed` (SCRAM `e=` server error) to `ErrorKind::InvalidInput` + `SmtpCommandPhase::Auth` so `account_error.rs` routes it to `Authorization(PolicyBlocked)`.

Builder helpers: `.password(user, password)` for password auth,
`.oauth2(identity, access_token)` for a raw OAuth bearer string, and
`.oauth2_source(identity, Arc<dyn TokenSource>)` for a shared rotation
source.

## LMTP

`LHLO` instead of EHLO. Default TCP port 24 (Postfix/Dovecot convention, not RFC-assigned).

Per-recipient response model: vector length equals envelope recipient count. RCPT-time rejection responses are preserved at the original recipient index; accepted recipients receive the post-DATA delivery response in original order.

Direct LMTP sends expose only that ordered response vector. Use the account-oriented batch send API when the caller needs each recipient's RCPT-versus-final-status phase and recovery classification.

Unix-domain LMTP constructors are `#[cfg(unix)]` on sync and Tokio. Unix sockets refuse STARTTLS explicitly.

## Native-tls only

The TLS backend matrix (rustls, boring-tls, rustls provider/verifier) has been removed. `CertificateStore::Default` always means the native-tls platform verifier.

`TlsParameters` carries the connector, SNI hostname, and a `dangerous_*` set for accepting invalid certs / hostnames (test only).

## Message builder

`Message::builder()` produces a builder for typed headers (From, To, Cc, Bcc, Reply-To, Subject, Date, Message-Id, In-Reply-To, References, MIME-Version, Content-Type, ...). `reply_to_many(...)` for RFC 5322 address-list `Reply-To`.

Display-name encoding emits RFC 5322 phrase text when the name is atom-shaped, RFC 2047 encoded-word otherwise. Avoids DKIM-breaking quoted-string rewrites at relays.

`MultiPart` kinds: `Mixed`, `Alternative`, `Related`, `Signed`, `Encrypted`, `Report { report_type }`. `Mixed` is the default for `MultiPart::builder().build()`.

Multipart builders ensure a `boundary` is present even when a caller supplies a boundary-less multipart `Content-Type`. `try_boundary`, `try_encrypted`, and `try_signed` are fallible validation entry points; default boundaries use OS randomness, and a caller-supplied boundary is regenerated if it appears at a MIME delimiter position in an added part.

`SinglePartBuilder::body(String)` and `MessageBuilder::body(String)` infer `Content-Type: text/plain; charset=utf-8` when no content type is set.

`MessageBuilder::body` CRLF-normalizes bare LF at the builder boundary, so a byte body never reaches the DATA writer's defensive bare-LF dot-stuffing and gains a literal extra dot on a CRLF-strict relay. Normalization is scoped by the encoding that will actually be emitted: `String` input always normalizes, and `Vec<u8>` input normalizes only when it goes on the wire as literal `7bit`/`8bit` text. Byte input that is re-encoded (base64, `binary`) is opaque payload and is passed through untouched, so `0x0A` inside a binary attachment survives the round trip. `body_raw()` (DKIM) and the DATA writer both see the normalized buffer, so signing and delivery still agree. A pre-encoded `Body` is never rewritten.

Typed headers for list management: `List-ID`, `List-Help`, `List-Unsubscribe`, `List-Unsubscribe-Post` (fixed value `List-Unsubscribe=One-Click` per RFC 8058), `List-Subscribe`, `List-Post`, `List-Owner`, `List-Archive`.

Content-type helpers: `text_plain_flowed()`, `text_plain_flowed_delsp()` (RFC 3676).

Batch builders: `MultiPart::multiparts(...)` and `.singleparts(...)`.

## DKIM

Default canonicalization is `relaxed/relaxed`. Signing keys: `From<rsa::RsaPrivateKey>` and `From<ed25519_dalek::SigningKey>` on `DkimSigningKey`. SHA-2 via `sha2` 0.10 to match `rsa` 0.9 digest traits.

Canonicalization follows RFC 6376 for empty bodies and missing final CRLFs. Relaxed headers are normalized iteratively, and `DKIM-Signature` is folded before `b=` before hashing so simple header canonicalization signs the exact emitted bytes. The signer intentionally does not support the `l=` body-length tag because it enables content-append attacks.

## Pool

`PoolConfig` configures min idle, max size, idle timeout, and the checkout NOOP probe. Recycling refuses connections that are `Broken` and connections flagged for retirement by an LMTP final-status drain (see "Connection state and cancel-safety"). `min_idle` defaults to 0: no background pool worker is started, and expired connections are discarded at checkout. A positive `min_idle` enables the worker that expires and replenishes warm connections. `test_on_checkout(false)` opts out of the default NOOP probe for callers willing to retry one stale idle connection; it does not permit a connection already marked `Broken` to be reused. It buys nothing for LMTP transports, whose connections are retired at recycle and therefore never checked out idle.

## Error model

`Error::kind()` returns `&ErrorKind`. SMTP reply failures are `Transient(Response)` or `Permanent(Response)`, so callers can inspect the full server reply through `smtp_response()`, `status()`, and `enhanced_status_code()`. Local buckets are `Parse`, `InvalidInput`, `FeatureUnsupported`, `ParameterOverLimit`, `Internal`, `Policy`, `Connection`, `Network`, `Timeout`, `Tls`, and `TransportShutdown`. `Policy` covers local refusals such as plaintext-AUTH refusal. `FeatureUnsupported` / `ParameterOverLimit` are the FUTURERELEASE discriminators (a relay that did not advertise the extension vs a HOLDFOR over the advertised max interval), kept distinct from generic `InvalidInput` so the account-error mapping can tell scheduled-send-unsupported apart from a malformed parameter. SMTP replies are line-bounded before allocation and decoded from bytes, so oversized or non-UTF-8 replies are `Parse`, not retryable network errors.

Internal pipeline errors carry two value-side decorations the classifier reads:

- `SmtpTransmissionState` (`Unsent` / `InFlight` / `Acknowledged`), attached via `with_attempt`.
- `SmtpCommandPhase`, attached via `with_phase`. The 16 variants cover every wire boundary the driver crosses: `Connect`, `Greeting`, `Hello`, `StartTls`, `Auth`, `MailFrom`, `RcptTo`, `DataCommand`, `DataBody`, `DataFinal`, `BdatBody`, `LmtpFinalStatus`, `Noop`, `Vrfy`, `Expn`, `Rset`. `DataCommand` covers the `DATA` command write/read; `DataBody` covers body upload; `DataFinal` covers the final reply after the dot terminator. Phase lives on `Error::Inner` so a missed `with_phase` decoration cannot silently degrade the classifier.

`crates/smtp/src/transport/smtp/account_error.rs` is the single translation boundary into the shared `AccountError`. It funnels through `AccountErrorBuilder::try_build` (never the removed `.build()`), reads `error.phase()` in preference to the context phase, and routes `InvalidInput` + `SmtpCommandPhase::Auth` (e.g. "no compatible authentication mechanism") to `Authorization(PolicyBlocked)` so consumers see a reauth/policy-change UX instead of `Request(Malformed) -> ClientBug` (internal telemetry). It also maps `FeatureUnsupported` to `Unsupported(AccountOperation::Send)` (so the IMAP layer surfaces a stable `Unsupported(Send)` kind when a relay lacks FUTURERELEASE) and `ParameterOverLimit` to `Request(Malformed)` (the hold time is outside the allowed window). `message_error_to_account_error(MessageError, Protocol) -> AccountError` is the boundary for builder-side validation failures (`MissingFrom`, `MissingTo`, `EmailMissingAt`, ...); every variant maps to `Request(Malformed)`.

Evidence must match what actually crossed the wire. A transport failure in the envelope phase (`MAIL FROM` / `RCPT TO`), including a failed write of a later PIPELINING recipient window, is `Unsent` and resolves through `SendProgress::mark_unresolved_unsent`: `DATA` has not been issued, so no message content can have reached the peer and an `Uncertain` lane would falsely claim a possible delivery. RCPT rejections the server already gave are preserved; accepted and unanswered recipients are both rewritten (an accepted RCPT with no `DATA` would otherwise resolve as a delivery that never happened), into `failed` lanes that `RecoveryClass` derives as `Retry(SameRequest)`. `InFlight` is reserved for failures from the `DATA` command onward.

`batch.rs` `SendProgress::resolve` is the per-recipient lane resolver. LMTP `DATA`-command negative replies route through `mark_accepted_rejected_with_response` so accepted recipients become per-recipient `Failed` lanes - never a batch-level `Err` (the previous shape let the engine resend the entire non-idempotent `Send` after the server rejected it). DATA-final-negative replies tag `SmtpCommandPhase::DataFinal`. LMTP `Accepted` at resolve time without a per-recipient `Final` is a programming bug caught by `debug_assert!` in debug builds and falls back to an `Uncertain` lane in release. Every failed and uncertain lane carries the envelope recipient as `DiagnosticText::support_only` so support exports preserve per-recipient correlation when N lanes share the same wire response text.

## Connection test harness

`test_support::Transcript` is the in-process scripted peer both connection
drivers test against; it replaced the socket-listener tests, which were neither
hermetic nor deterministic. A transcript is a greeting plus an ordered list of
`expect(client_bytes, server_bytes)` steps. Three properties make it bite:

- Each client write must match one scripted step byte-for-byte, so command
  batching shape (which commands share a write) is pinned, not just command
  order.
- Server bytes become readable only after their step's write.
- Reads hand out at most one reply line. Without this, a `BufReader` prefetches
  a whole pipelined reply group, and a driver that skipped replies would still
  pass. Writing while replies are pending is an error, which is how the
  PIPELINING window drain and the LMTP surplus-final-reply desync are detected.
- `expect_coalesced(...)` opts one step out of the line split so its replies
  arrive as a single segment, the way a real peer's TCP stack coalesces
  adjacent replies. That is what a `BufReader` prefetches, so it drives the
  same surplus-detection path production has. The harness has no visibility
  below the buffer that a real transport lacks.

`expect_then_stall` and `Transcript::silent()` model a peer that accepts and
then never answers; reads park with no waker, so only the caller's own timeout
or cancellation resumes the task. That is what the async timeout, setup-deadline
and cancellation tests observe, under `start_paused` tokio time.

## Example validation

Examples live under `crates/smtp/examples`. Sync examples require no crate
features. Tokio examples are gated by `tokio`. Validate examples with the SMTP
crate checks under both the default feature set and `tokio`.

## Module layout

```
crates/smtp/src/
├── message/             - builder, headers, MIME parts, address parsers
├── transport/
│   └── smtp/
│       ├── client/      - connection, async_connection, net, async_net
│       ├── transport.rs / async_transport.rs - public sync + async transports
│       ├── commands.rs  - EHLO, MAIL, RCPT, DATA, BDAT, AUTH, NOOP, RSET, QUIT, VRFY, EXPN, STARTTLS, LHLO
│       ├── extension.rs - ServerInfo, SendOptions, MAIL/RCPT parameters
│       ├── response.rs  - parser, enhanced status codes
│       └── test_support.rs - in-process `Transcript` harness + mock servers
└── error.rs             - Error, ErrorKind
```

## SMTP-specific code style

- `client::` is private; low-level connection/network stream types are crate-internal.
- TLS uses native-tls unconditionally; rustls/boring features have been removed.
- `#[non_exhaustive]` on `Error`, `ErrorKind`, DKIM enums, `CertificateStore`.
- `AsyncTransport` requires `Sync` so borrowed methods return `Send` futures.
- Async traits use native `impl Future + Send` return types; no `async-trait`.
