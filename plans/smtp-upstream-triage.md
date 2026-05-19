# bifrost-smtp upstream triage

Working notes from the first pass over `lettre/lettre` open issues and PRs.
This is intentionally a live scratch document, not polished project docs.

## Context

`bifrost-smtp` currently descends from `lettre` 0.11.22. The original clone was
copied into `crates/smtp`, renamed, and then the root `lettre` checkout was
deleted.

Primary bifrost motivation: SMTP auth needs to support modern OAuth/OIDC
workflows cleanly. That changes the priority from generic upstream parity to
bearer-token authentication and related SMTP auth API cleanup.

## Issue 1028: Wishlist for v0.12

Upstream issue: https://github.com/lettre/lettre/issues/1028

The upstream maintainer lists breaking-change candidates:

- Drop `async-trait`.
- Fix timeouts: #917, #1027.
- Clean up `lettre::transport::smtp::client`, which was not intended as public
  API.
- Consider API suggestions: #927, #695.
- Support boxing transports: #938.
- Builder and message fixes: #856, #970.
- Consider rustls as default TLS backend. Bifrost rejects this for now because
  ratatoskr uses native-tls exclusively.
- Review TODO/FIXME.
- Remove deprecated features: #1052.
- Mark `CertificateStore` non-exhaustive.
- Decrease default connection timeout.

Comment on #1028 also points out that `Reply-To` should be an RFC 5322
`address-list`, not just a `Mailbox`.

## Open upstream PRs scanned

- #813: switch SMTP SASL handling to `rsasl`.
  - Useful direction for bifrost because OAuth/OIDC auth should not stay
    hand-rolled forever.
  - The PR itself says it was discussion-grade, changed public API, and was not
    close to mergeable when opened.
  - Current rsasl docs say protocol crates should depend on rsasl with minimal
    features and avoid re-exporting rsasl types.
  - rsasl supports `oauthbearer` and `xoauth2` features.
- #1073: docs only, clarifies `Credentials::new` takes a bearer token for
  XOAUTH2.
  - This confirms upstream has the same API smell: password-shaped API for
    bearer tokens.
- #994: SMTP error handling, draft. Probably worth revisiting after auth.
- #948: SMTP implementation refactor, draft. Large overlap with future bifrost
  rewrite.
- #1087: `TlsParametersV2`, draft. Relevant later, not first OIDC blocker.
- #831: LMTP support. Interesting, but lower priority than OIDC.
- #877: do not send commands when aborting. Small behavior fix candidate later.
- #1123/#1124: DKIM key object API. Lower priority for SMTP auth.
- Dependabot PRs are dependency churn, not core design.

## Open issues scanned

Auth/OIDC related:

- #822, closed: users asked how to use XOAUTH2.
- #1072, closed PR: fixed base64 encoding of XOAUTH2 response. Our copied code
  already base64 encodes `Auth` responses at command formatting time.
- #1073, open PR: docs clarify bearer token usage for XOAUTH2.
- No open upstream issue found for OAUTHBEARER specifically.

Transport correctness:

- #917: async send/test_connection can wait indefinitely when network is down.
- #1027: async STARTTLS send does not honor timeout.
- #978: test_connection blocks and ignores timeout.
- #1010: async transport makes frequent DNS queries, likely from connection
  pool liveness or idle management.
- #970: resend EHLO after AUTH succeeds.
- #743: client breaks after SMTP server closes connection.

Message builder and signing:

- #1125: From and Reply-To quoting can break DKIM validation at Gmail.
- #1118: Date header timezone support and raw_header ignored.
- #1104/#927: DKIM default canonicalization should likely be relaxed/relaxed.
- #856: generated messages without Content-Type can bother clients.
- #796: multipart can panic when kind is not set.

Security/API:

- #1132: store passwords in `Zeroizing<String>`.
- #965/#940: expose and document SMTP error kinds better.
- #938/#458/#770: transport trait object and Send/Sync ergonomics.
- #1138: async-std dependency leakage report. Needs direct manifest check later.
- #1137: system default SSL request. Bifrost now uses native TLS defaults only.

## Current decision

First bifrost-specific patch should make OAuth/OIDC auth explicit:

- Stop treating OAuth access tokens as passwords in the public API.
- Add `OAUTHBEARER` support, not just XOAUTH2.
- Configure `OAUTHBEARER` before XOAUTH2 so the transport picks the standard
  mechanism when the server advertises it.
- Keep the door open to replacing this with rsasl, but do not block the fork on
  a large dependency and API port before we have a concrete bifrost auth shape.

## Implemented in this pass

- `Credentials` is now an enum with distinct `Password` and `OAuth2` variants.
- Added `Credentials::password(username, password)`.
- Added `Credentials::oauth2(identity, access_token)`.
- Added `Mechanism::OAuthBearer` with RFC 7628 initial response formatting.
- `OAUTHBEARER` server support is parsed from EHLO `AUTH` lines.
- Added `OAUTH2_MECHANISMS = [OAuthBearer, Xoauth2]`.
- `SmtpTransportBuilder::oauth2(identity, access_token)` and
  `AsyncSmtpTransportBuilder::oauth2(identity, access_token)` configure OAuth2
  credentials and configure `OAUTHBEARER` before `XOAUTH2`.
- `SmtpTransportBuilder::password(username, password)` and async equivalent are
  the preferred classic password auth API in examples and README.
- `credentials(...)` now preserves explicitly configured mechanisms. It only
  infers mechanism defaults from the credential kind when `authentication(...)`
  has not been called.
- The public enum variant is named `Mechanism::OAuthBearer` to match Rust
  acronym convention.
- README wording now describes mechanism preference rather than implying a retry
  fallback after authentication failure.
- Tests pin that OAUTHBEARER sends the RFC-required cancel byte for any server
  challenge payload.
- AUTH continuation formatting now treats challenge responses as continuation
  lines even for mechanisms that support initial responses. This matters for
  OAUTHBEARER failed-auth exchanges, where the client sends the required dummy
  response after a `334` challenge.

Verification:

- `brokkr fmt` passed with only the known brokkr history warning.
- `brokkr check` passed across default and minimal sweeps.

## Follow-up candidates

- Consider replacing the whole auth exchange with rsasl once bifrost's public
  auth API is clearer. If we do this, do it around bifrost-owned wrapper types
  rather than blindly exposing rsasl everywhere.

## Upstream PR 877

Upstream PR: https://github.com/lettre/lettre/pull/877

Decision: apply the behavior. `abort()` should not attempt to send `QUIT`,
because abort is used after protocol or stream failures and the stream may no
longer be writable. The graceful path remains `quit()`.

Implemented:

- Sync and async SMTP connection `abort()` now only marks the connection broken
  and closes the stream.
- Added sync coverage proving abort does not write `QUIT` after a successful
  connection.

## Small #1028 cleanup pass

Implemented:

- `CertificateStore` is now `#[non_exhaustive]`.
- Default SMTP command/connect timeout reduced from 60 seconds to 10 seconds.
- Removed stale FIXME comments from the time helper around clippy allowances.

## Native TLS simplification

Decision: remove the SMTP TLS backend matrix from the fork. Ratatoskr uses
native-tls exclusively, so maintaining rustls, rustls provider/verifier
features, and boring-tls would add review and API weight without product value.

Implemented:

- Removed rustls, rustls provider/verifier, webpki, boring-tls, futures-rustls,
  tokio-rustls, and tokio-boring dependencies/features from `bifrost-smtp`.
- Deleted the rustls crypto provider helper.
- Simplified sync and async network streams to plaintext plus native-tls only.
- Removed Boring/rustls-only public inspection APIs:
  `tls_verify_result()` and `certificate_chain()`.
- Kept `peer_certificate()` for native-tls at this point. This was later
  removed with the private-client cleanup because the low-level connection API
  is no longer public.
- Removed async-std TLS examples. Async-std remains available where it does not
  need SMTP TLS.
- Applied timeout handling around async DNS lookup and tokio native-tls
  handshake during connection setup. This does not yet use a shared deadline
  across phases.
- Added a tokio regression test matching the sync abort test: `abort()` closes
  without sending `QUIT`.

Deferred:

- Shared-deadline budgeting (#917/#1027/#978) needs a dedicated pass if we
  decide configured timeout should cap the whole connection lifecycle instead
  of each phase or operation.
- Focused tests for async DNS and TLS-handshake timeout paths are still needed.
- Dropping `async-trait` is a larger trait/API rewrite and belongs with the
  async transport redesign.

## Upstream issue batch: AUTH, message builder, and credential storage

Issues covered:

- #970: resend EHLO after successful AUTH.
- #856: generated text messages should include `Content-Type`.
- #796: `MultiPart::builder()` should not create a value that later panics
  because no multipart kind was configured.
- #1118: raw `Date` headers with non-UTC offsets should not be ignored and
  replaced during message build.
- #1132: store secret credential material in zeroizing memory.
- #1028 comment: `Reply-To` is an address-list, not just a single mailbox.

Implemented:

- `SmtpConnection` and `AsyncSmtpConnection` now retain their EHLO identity and
  automatically send EHLO again after successful AUTH. The refreshed
  `ServerInfo` replaces pre-auth capabilities.
- Added sync and tokio protocol tests proving AUTH sends a second EHLO and
  updates advertised capabilities.
- `MessageBuilder::body(String)` and `SinglePartBuilder::body(String)` now
  infer `Content-Type: text/plain; charset=utf-8` when the caller did not set a
  content type. Binary `Vec<u8>` bodies still do not guess a content type.
- `MultiPart::builder().build()` defaults to `multipart/mixed`; `.boundary(...)`
  before `.kind(...)` also defaults to mixed instead of unwrapping a missing
  header.
- `MessageBuilder` now checks for a raw `Date` header by name before inserting
  `Date::now()`, so a pre-encoded non-UTC Date survives.
- Added `MessageBuilder::reply_to_many(...)` for RFC 5322 address-list
  `Reply-To` values without breaking the existing `.reply_to("...".parse()?)`
  inference path.
- `Credentials` now stores passwords and OAuth access tokens as
  `Zeroizing<String>`.
- Added `IntoSecretString` so callers can pass either normal strings or an
  existing `Zeroizing<String>` into `Credentials`, `SmtpTransportBuilder`, and
  `AsyncSmtpTransportBuilder`.

Verification:

- `brokkr fmt` passed with only the known brokkr history warning.
- `brokkr check` passed across default and minimal sweeps.

## Async SMTP command timeout pass

Issues covered:

- #917: async send and connection tests can wait indefinitely when a connected
  peer stops responding.
- #1027: async STARTTLS does not carry the configured timeout through the
  STARTTLS exchange and TLS upgrade.
- #978: async `test_connection` can block waiting for the `NOOP` response.

Implemented:

- `AsyncSmtpConnection` now records the configured timeout and the runtime used
  to create it.
- Async SMTP writes, flushes, and response reads are wrapped in the configured
  per-operation timeout for both tokio and async-std.
- Initial banner reads and EHLO during async connection setup now use the same
  timeout path.
- STARTTLS now passes the configured timeout into the native-tls upgrade rather
  than using an unbounded handshake.
- Added tokio regression tests for timing out while waiting for the initial
  banner and while waiting for a command response.

Verification:

- `brokkr fmt` passed with only the known brokkr history warning.
- `brokkr check --package bifrost-smtp --features tokio1-native-tls -- -- times_out`
  passed.

## Upstream issue 1125: display-name quoting and DKIM

Upstream issue: https://github.com/lettre/lettre/issues/1125

Problem:

- The header encoder used quoted-string formatting for every display name with
  whitespace, producing `"John Smith" <john@example.com>`.
- Gmail may later render or reserialize that as `John Smith <john@example.com>`.
  When the original message was DKIM-signed over `From` or `Reply-To`, that
  harmless-looking syntactic rewrite can invalidate the signature.

Decision:

- Encode plain RFC 5322 phrase names as phrase text instead of quoted strings.
- Keep the existing encoded-word/quoted-string path for names containing
  punctuation that is not valid `atext`, names needing escaping, and non-ASCII
  names.

Implemented:

- `Mailbox::encode` now trims display names like `Display` already did and
  skips empty display names. This intentionally drops the old header-only
  behavior where a whitespace-only name could still force angle brackets.
- Display names made of atom characters separated by spaces or tabs are written
  directly, so `John Smith` remains unquoted in `From` and `Reply-To`.
- Fixed the inherited atom-range typo that treated `9` as invalid in display
  names, which made names such as `User9` quote unnecessarily.
- Names such as `Pony P.`, names containing commas, and non-ASCII names still
  use the previous encoder path.
- Added regression coverage for `From` and `Reply-To` phrase display names,
  including a display name ending in `9`.

Verification:

- `brokkr fmt` passed with only the known brokkr history warning.
- `brokkr check --package bifrost-smtp --features builder` passed.
- `brokkr check --package bifrost-smtp --features builder -- -- format_single_with_phrase_name`
  passed.
- `brokkr check --package bifrost-smtp --features builder -- -- format_reply_to_with_phrase_name`
  passed.

## Upstream issues 927 and 1104: DKIM default canonicalization

Upstream issues:

- https://github.com/lettre/lettre/issues/927
- https://github.com/lettre/lettre/issues/1104

Problem:

- `DkimConfig::default_config` used `simple/relaxed`, which is brittle when
  MTAs rewrap, refold, or normalize signed headers.
- `relaxed/relaxed` is the practical default users expect, and upstream agreed
  there was no strong reason to keep the original `simple` header default.

Implemented:

- `DkimCanonicalization::default()` is now `relaxed/relaxed`.
- `DkimConfig::default_config(...)` now uses that default instead of
  duplicating a hard-coded canonicalization pair.
- `bifrost-smtp` now uses `sha2` 0.10 for DKIM so RSA signing builds against
  the digest traits expected by `rsa` 0.9.
- Removed stale debug macros from DKIM tests that were only caught once the
  `dkim` feature was checked.
- Added regression coverage for the default config canonicalization.
- Marked the public DKIM enum types as non-exhaustive while this fork is still
  free to break API.

Verification:

- `brokkr fmt` passed with only the known brokkr history warning.
- `brokkr check --package bifrost-smtp --features dkim` passed.

## Upstream issues 940 and 965: SMTP error classification

Upstream issues:

- https://github.com/lettre/lettre/issues/940
- https://github.com/lettre/lettre/issues/965

Problem:

- Users could not match on SMTP error kind without relying on debug strings or
  incomplete boolean helpers.
- `is_transient` and `is_permanent` read like opposites, but both are false for
  parse, client, connection, network, TLS, and shutdown errors.

Implemented:

- Added public `transport::smtp::ErrorKind` and `Error::kind()` for structured
  classification.
- Added `Error::is_connection()` and `Error::is_network()` helpers.
- Clarified docs for `is_response`, `is_transient`, and `is_permanent`.
- Re-exported `ErrorKind` next to `Error` from `transport::smtp`.
- Added unit coverage for connection, network, and SMTP reply classifications.

Verification:

- `brokkr fmt` passed with only the known brokkr history warning.
- `brokkr check --package bifrost-smtp --features builder -- -- exposes_connection_kind`
  passed.
- `brokkr check --package bifrost-smtp --features builder` passed.

Final SMTP-only verification after concurrent IMAP edits:

- From `crates/smtp`: `brokkr check --features builder` passed.
- From `crates/smtp`: `brokkr check --features dkim` passed.
- From `crates/smtp`: `brokkr check --features tokio1-native-tls -- -- times_out`
  passed.

Root-level `brokkr check` is currently blocked by gremlin findings in concurrent
IMAP proto files, so these verification runs were scoped to the SMTP crate
without rewriting IMAP files.

## Upstream issue 743: stale pooled connections

Upstream issue: https://github.com/lettre/lettre/issues/743

Problem:

- The pool checks a parked connection with `NOOP` before reuse, but
  `test_connected()` returned `false` without marking the connection broken.
- Public `test_connection()` with pooling could therefore test a dead
  connection, return `false`, drop the pooled wrapper, and recycle that same
  dead connection back into the pool.
- The async-std transport path also sent `QUIT` unconditionally after
  `send_raw`, which meant an async-std pooled connection could be gracefully
  closed and then recycled.

Implemented:

- Sync and async `test_connected()` now abort the connection when `NOOP` fails,
  so pooled wrappers drop it instead of recycling it.
- `test_connected()` docs now explicitly say failed checks close and mark the
  connection broken.
- Async-std `send_raw` now matches the sync and tokio paths: it only aborts a
  successfully used connection when the pool feature is disabled.
- Cleaned up async-std feature compilation after the native-TLS-only fork:
  async URL parsing is exposed only for tokio native-tls, and async-std no
  longer trips over tokio-only imports or private SMTP error helpers.
- Added comments on the async URL parsing cfg gate because the tokio TLS /
  plaintext-only async split is easy to misread.
- Added sync and tokio regression tests proving failed `test_connected()` marks
  the connection broken.

Verification:

- From `crates/smtp`: `brokkr fmt` passed.
- From `crates/smtp`: `brokkr check --features async-std1` passed.
- From `crates/smtp`: `brokkr check --features builder` passed.
- From `crates/smtp`: `brokkr check --features tokio1-native-tls -- -- failed_test_connected_marks_connection_broken`
  passed.

## Upstream PRs 1123 and 1124: DKIM key objects

Upstream PRs:

- https://github.com/lettre/lettre/pull/1123
- https://github.com/lettre/lettre/pull/1124

Decision:

- Prefer PR 1123's narrower API over PR 1124's public inner enum. Callers can
  pass an already parsed key object without making bifrost expose its internal
  signing-key representation.

Implemented:

- Added `From<rsa::RsaPrivateKey> for DkimSigningKey`.
- Added `From<ed25519_dalek::SigningKey> for DkimSigningKey`.
- Added regression coverage for both conversions.

Verification:

- From `crates/smtp`: `brokkr fmt` passed.
- From `crates/smtp`: `brokkr check --features dkim -- -- signing_key_can_wrap`
  passed.
- From `crates/smtp`: `brokkr check --features dkim` passed.

## Small v0.12 deprecated API cleanup

Context:

- #1028 calls out removing deprecated features and APIs.

Implemented:

- Removed hidden deprecated `ClientId::new(domain)`. Use
  `ClientId::Domain(domain)` directly.
- Removed hidden deprecated `PoolConfig::connection_timeout(...)`. Connection
  timeout is configured on the SMTP transport builder, not the pool.

Verification:

- From `crates/smtp`: `brokkr fmt` passed.
- From `crates/smtp`: `brokkr check --features builder` passed.

Final SMTP-only verification for this batch:

- From `crates/smtp`: `brokkr fmt` passed.
- From `crates/smtp`: `brokkr check --features builder` passed.
- From `crates/smtp`: `brokkr check --features async-std1` passed.
- From `crates/smtp`: `brokkr check --features dkim` passed.
- From `crates/smtp`: `brokkr check --features tokio1-native-tls -- -- failed_test_connected_marks_connection_broken`
  passed.

Review follow-up verification:

- From `crates/smtp`: `brokkr fmt` passed.
- From `crates/smtp`: `brokkr check --features builder` passed.
- From `crates/smtp`: `brokkr check --features async-std1` passed.
- From `crates/smtp`: `brokkr check --features tokio1-native-tls -- -- failed_test_connected_marks_connection_broken`
  passed.

## v0.12 async trait cleanup

Context:

- #1028 calls out dropping `async-trait`.
- The old macro boxed the async transport and executor futures and forced the
  async `send` helper to take ownership of `Message`, unlike the blocking
  transport API.

Implemented:

- Removed the optional `async-trait` dependency from `bifrost-smtp`.
- Converted `Executor`, `SpawnHandle`, and `AsyncTransport` to native trait
  futures.
- Kept public trait declarations on explicit `impl Future + Send` return types
  so public async trait methods do not hide auto-trait bounds.
- `AsyncTransport` now requires `Sync` so borrowed async methods can return
  `Send` futures. This is a public API break for custom async transports.
- This intentionally gives up the object-safety that `async-trait`'s boxed
  futures could provide. Transport boxing is still a separate #938 design pass.
- Converted impl bodies to `async fn` where clippy can verify the desugaring.
- Changed `AsyncTransport::send` from `send(Message)` to `send(&Message)`,
  matching `Transport::send` and avoiding unnecessary clones in tests. This is
  a public API break for callers of async transports.
- Updated async examples, docs, and tests for borrowed send.
- Reopened `SmtpTransport::from_url` for plaintext `smtp://` URLs when
  `native-tls` is disabled; TLS URL forms still require `native-tls`.
- Left async `from_url` exposed only for the tokio native-tls path for now.
  Plain async transports can still use `builder_dangerous`; broader async URL
  parsing belongs with the later transport API pass.
- Cleaned up native-tls cfg fallout in sync network streams, TLS placeholder
  types, imports, and URL parsing tests so plaintext tokio builds compile.
- Fixed a real deserialization bug: malformed serialized `Address` values now
  return a serde error instead of panicking through `unwrap`.
- Fixed a small all-features clippy finding in `ContentType` serde formatting.

Verification:

- From `crates/smtp`: `brokkr fmt` passed.
- From `crates/smtp`: `brokkr check --features tokio1-native-tls` passed.
- From `crates/smtp`: `brokkr check --features async-std1` passed.
- From `crates/smtp`: `brokkr check --no-default-features --features tokio1,smtp-transport,builder,pool`
  passed.

## Deterministic sendmail transport tests

Problem:

- The sendmail transport integration tests used `SendmailTransport::new()` and
  `AsyncSendmailTransport::new()`, so they depended on a working `sendmail`
  command in the test host's `PATH`.
- That made `brokkr check` fail after a clean all-features clippy pass on
  machines without local sendmail configured.

Implemented:

- Added a test-local fake sendmail command generated under
  `crates/smtp/target/sendmail-tests`.
- Sync, tokio, and async-std sendmail tests now use `new_with_command(...)`
  and assert both command-line arguments and stdin message bytes.
- The fake command directory is recreated per label and process id so repeated
  runs do not accumulate stale captured files.
- Added sync, tokio, and async-std coverage for non-zero sendmail exit with
  stderr propagated as a client error.
- Added sync coverage for non-zero sendmail exit without stderr.
- Full SMTP `brokkr check` now passes in this workspace.

Verification:

- From `crates/smtp`: `brokkr fmt` passed.
- From `crates/smtp`: `brokkr check --no-default-features --features sendmail-transport,builder,tokio1,async-std1 -- -- sendmail_transport`
  passed.
- From `crates/smtp`: `brokkr check` passed.

## Async connection setup deadline

Upstream issues:

- https://github.com/lettre/lettre/issues/917
- https://github.com/lettre/lettre/issues/1027
- https://github.com/lettre/lettre/issues/978

Problem:

- The async transport timeout had become a per-operation guard, but connection
  setup still applied the full configured timeout separately to DNS lookup,
  each address connect attempt, TLS handshake, banner read, and initial EHLO.
- A 10-second timeout could therefore still take much longer during setup when
  several phases were slow in sequence.

Implemented:

- Added an internal `AsyncDeadline` helper for async connection setup.
- Public deprecated `AsyncNetworkStream::connect_tokio1` and
  `connect_asyncstd1` keep their signatures, but delegate to internal
  deadline-aware variants.
- Async setup now shares one deadline across DNS, connect attempts, implicit
  TLS handshake, banner read, EHLO write/flush, and EHLO response parsing.
- The setup deadline lives only until `connect_impl` returns. Normal SMTP
  commands, pooled `test_connected()` NOOP probes, and other operations on an
  established connection keep the existing per-operation timeout behavior.
- `AsyncSmtpConnection::starttls(...)` still uses the configured timeout as a
  per-operation budget because it is an explicit command on an already-open
  connection, not part of initial setup.
- Added a tokio regression test where banner and EHLO each respond within the
  configured timeout individually, but exceed the single setup deadline
  together.
- Added matching async-std coverage for the same banner/EHLO deadline boundary.

Verification:

- From `crates/smtp`: `brokkr fmt` passed.
- From `crates/smtp`: `brokkr check --features tokio1-native-tls -- -- connect_setup_uses_single_deadline`
  passed.
- From `crates/smtp`: `brokkr check --features async-std1 -- -- asyncstd_connect_setup_uses_single_deadline`
  passed.
- From `crates/smtp`: `brokkr check --features tokio1-native-tls` passed.
- From `crates/smtp`: `brokkr check --features async-std1` passed.
- From `crates/smtp`: `brokkr check --no-default-features --features tokio1,smtp-transport,builder,pool`
  passed.

## Upstream issue 938: boxed transport ergonomics

Upstream issue: https://github.com/lettre/lettre/issues/938

Problem:

- Sync transports can be used as trait objects, but `Box<dyn Transport<...>>`
  did not itself implement `Transport`, so callers had to unwrap or hand-roll
  forwarding.
- After dropping `async-trait`, `AsyncTransport` intentionally uses native
  `impl Future` return types and is not object-safe. Callers still need a
  concrete way to store an async transport behind one stable type.

Implemented:

- Added `BoxedTransport<Ok, Error>` as a public boxed sync transport trait
  object alias.
- Added forwarding `Transport` impls for `Box<T>` and `Arc<T>`.
- Added forwarding `AsyncTransport` impls for `Box<T>` and `Arc<T>`.
- Added `BoxedAsyncTransport<Ok, Error>`, which erases any async transport into
  boxed futures internally without reintroducing `async-trait`.
- `BoxedAsyncTransport` is intentionally `Send + Sync` only. There is no
  local/non-Send boxed async transport because `AsyncTransport` itself requires
  `Sync` for borrowed methods that return `Send` futures.
- Added sync, tokio, and async-std stub transport coverage for boxed and `Arc`
  transport forwarding, including compile-time Send/Sync assertions for
  `BoxedAsyncTransport`.

Verification:

- From `crates/smtp`: `brokkr fmt` passed.
- From `crates/smtp`: `brokkr check --no-default-features --features builder,tokio1,async-std1 -- -- boxed_stub_transport`
  passed.

## Plaintext auth hardening and client API cleanup

Context:

- Bifrost's SMTP fork is prioritizing bearer-token/OIDC auth, so accidentally
  sending credentials over plaintext SMTP is worse than a generic convenience
  footgun.
- #1028 calls out that `transport::smtp::client` was never meant to be public.
  Once the module is private, the old low-level connection and network stream
  APIs should stop shaping the public surface.
- Upstream PR #994 is still a draft and mainly targets connection shutdown and
  cancel-safety around errors. We already picked up the important abort/no-QUIT
  behavior; the remaining design is better handled after the public client
  namespace is gone.
- Upstream PR #831 adds LMTP with a protocol generic and multi-response send
  semantics. That still looks valuable, but it deserves a first-class bifrost
  API instead of being squeezed into this hardening pass.

Implemented:

- SMTP transports now refuse to run AUTH over an unencrypted connection by
  default. This covers password credentials and OAuth bearer tokens.
- Added public `ErrorKind::Policy` and `Error::is_policy()` so callers can
  distinguish local policy refusals from malformed credentials or other client
  errors.
- Added `SmtpTransportBuilder::dangerous_allow_insecure_auth(bool)` and
  `AsyncSmtpTransportBuilder::dangerous_allow_insecure_auth(bool)` as explicit
  opt-ins for trusted local relays and tests.
- Added sync and tokio protocol tests proving plaintext auth is rejected after
  EHLO and before any AUTH line is sent.
- Added sync coverage proving the dangerous opt-in still sends AUTH for local
  plaintext test relays.
- Reopened async `from_url` for plaintext `smtp://` URLs when `native-tls` is
  not compiled in. TLS URL forms still require the tokio native-tls path.
- Made `transport::smtp::client` private.
- Re-exported TLS configuration types directly from `transport::smtp`.
- Moved low-level `SmtpConnection`, `AsyncSmtpConnection`, network stream, and
  async stream hook APIs to crate/internal visibility.
- Split the SMTP-specific async executor `connect` hook out of the public
  `Executor` trait into a crate-private `SmtpExecutor` trait, so the public
  executor API no longer leaks the internal SMTP connection type.
- Documented why public async transport methods carry `#[allow(private_bounds)]`
  around the crate-private `SmtpExecutor` bound.
- Removed now-unused low-level stream hooks such as public peer-address access,
  `set_stream`, `connect_with_transport`, and native-tls `peer_certificate`.
- Added a `CHANGELOG.md` entry for the bifrost-smtp breaking API surface
  changes.

Verification:

- From `crates/smtp`: `brokkr fmt` passed.
- From `crates/smtp`: `brokkr check --no-default-features --features tokio1,smtp-transport,builder,pool`
  passed.
- From `crates/smtp`: `brokkr check` passed.
- From `crates/smtp`: `brokkr check --features async-std1` passed.
- From `crates/smtp`: `brokkr check --features tokio1-native-tls -- -- plaintext_auth_is_refused`
  passed.

## Upstream PR 831: LMTP transport support

Upstream PR: https://github.com/lettre/lettre/pull/831

Decision:

- Take the protocol behavior, but not the upstream public const-generic
  `SmtpTransport<true>` shape.
- Bifrost exposes first-class `LmtpTransport` and `AsyncLmtpTransport` types.
  This keeps SMTP and LMTP return types obvious at the call site:
  SMTP returns one `Response`, while LMTP returns `Vec<Response>` with one
  status per recipient.
- Reuse the existing connection, auth, STARTTLS, timeout, and pool machinery
  internally through a crate-private `Protocol` enum. Public callers do not see
  that implementation detail.

Implemented:

- Added `LMTP_PORT = 24` and a crate-private `Protocol::{Smtp, Lmtp}`. RFC
  2033 does not assign a TCP port, but 24 is the deployed convention used by
  Postfix and Dovecot for TCP LMTP listeners.
- Added the `LHLO` command.
- Sync and async connections now send `EHLO` for SMTP and `LHLO` for LMTP.
  AUTH and STARTTLS capability refreshes repeat the correct greeting.
- Added LMTP message sending that preserves one status per envelope recipient.
  Recipients rejected during `RCPT` keep that `RCPT` response; accepted
  recipients are filled with the post-DATA delivery response in original
  envelope order.
- LMTP recipient 4xx/5xx statuses are returned as `Response` values instead of
  being converted into transport errors. Protocol, parse, and network failures
  still abort the connection and return `Error`.
- Added `LmtpTransport`, `LmtpTransportBuilder`,
  `AsyncLmtpTransport`, and `AsyncLmtpTransportBuilder`.
- Added an LMTP example.
- Added sync, tokio, and async-std protocol tests proving `LHLO` is used and
  mixed per-recipient statuses are returned. The tests cover both an RCPT-time
  rejection and post-DATA delivery statuses for accepted recipients.

Verification:

- From `crates/smtp`: `brokkr fmt` passed.
- From `crates/smtp`: `brokkr check -- -- lmtp` passed.
- From `crates/smtp`: `brokkr check --no-default-features --features smtp-transport,builder,pool -- -- lmtp`
  passed.
- From `crates/smtp`: `brokkr check --no-default-features --features tokio1,smtp-transport,builder,pool -- -- lmtp`
  passed.
- From `crates/smtp`: `brokkr check --no-default-features --features async-std1,smtp-transport,builder,pool -- -- lmtp`
  passed.
- From `crates/smtp`: `brokkr check` passed.
- Review follow-up: changed the LMTP TCP default from 11200 to 24 and preserved
  RCPT-time failures in the returned per-recipient status vector.
- Review follow-up verification from `crates/smtp`: `brokkr check -- -- lmtp`
  passed.

## Post-LMTP cleanup batch

Scope:

- User asked to continue everything except the rsasl migration and the async
  DNS/TLS edge-test pass.

Upstream items revisited:

- #994/#948: SMTP connection refactors and cancel-safety.
- #1010: frequent async DNS lookups.
- #1087: `TlsParametersV2` and certificate-store ambiguity.
- #1138: async-std dependency leakage.
- #1028: TODO/FIXME sweep and breaking API cleanup.

Implemented:

- Removed `Credentials::from((user, pass))`. The explicit
  `Credentials::password(...)` constructor is now the only password credential
  path, which avoids reintroducing password-shaped ambiguity next to
  `Credentials::oauth2(...)`.
- Added sync and tokio in-process protocol coverage for successful
  `AUTH OAUTHBEARER`, including GS2 identity escaping and the post-auth EHLO
  refresh.
- Added sync and tokio protocol coverage for RFC 7628 failure handling: a
  server challenge is answered with the required dummy cancel response
  (`AQ==` on the wire), and the failed exchange marks the connection broken.
- Immediate AUTH rejection now follows the same abort path as challenge-loop
  rejection. Sync and tokio tests cover a direct `535` response to the initial
  `AUTH OAUTHBEARER` command.
- Documented and tested the current OAUTHBEARER host/port decision. Bifrost
  does not include `host` or `port` in the initial bearer response because
  those fields are only required for keyed message-digest schemes, and adding
  them would couple mechanism encoding to SMTP connection state.
- Added stream-level connection state for sync and async SMTP network streams.
  The state is set to broken before writes, flushes, reads, and TLS upgrades,
  restored to ok only after success, and set to closed by abort/shutdown. This
  picks up the useful cancel-safety part of upstream #994/#948 while preserving
  bifrost's no-QUIT abort behavior.
- Added tokio and async-std coverage proving an externally cancelled command
  future leaves the connection broken and prevents later reuse.
- LMTP final delivery-status collection now keeps the async connection marked
  broken until every accepted recipient status has been read. Dropping the
  future in the middle of that loop no longer leaves a stream with unread LMTP
  responses eligible for pool reuse.
- Removed stale SMTP transport TODO/FIXME comments that no longer reflected the
  code.
- Moved the duplicated LMTP mock server used by sync, tokio, and async-std
  tests into shared test support.
- Documented that `PoolConfig::min_idle` defaults to zero and that setting it
  above zero intentionally allows background connection creation and DNS
  lookups. This resolves the practical #1010 concern for defaults: bifrost does
  not keep reconnecting in the background unless the caller asks for warm idle
  connections.
- Clarified that `CertificateStore::Default` always means the native-tls
  platform verifier in bifrost-smtp. The upstream #1087 ambiguity was caused by
  a multi-TLS-backend matrix that this fork has already removed.

Audit notes:

- #1138 does not require a manifest change in bifrost-smtp at this point:
  `sendmail-transport` uses `async-std?/unstable`, so the sendmail feature only
  enables the unstable feature on async-std when some other selected feature
  already pulled async-std in.
- The remaining `TODO`/`FIXME` items under `crates/smtp/src` are message
  builder/parser/sendmail notes rather than SMTP transport issues.
- `test_connected()` intentionally treats any failed NOOP as fatal for that
  parked connection. A transient blip may close a connection that could have
  recovered, but the pool default is "discard until proven reusable" rather
  than trying to resynchronize a long-lived SMTP stream.

Verification:

- From `crates/smtp`: `brokkr fmt` passed.
- From `crates/smtp`: `brokkr check --no-default-features --features smtp-transport,builder,pool,tokio1,async-std1`
  passed.
- From `crates/smtp`: `brokkr check` passed.
- From workspace root: `git diff --check` passed.
