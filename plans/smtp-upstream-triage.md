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

- Decide whether to remove `Credentials::from((user, pass))` or keep it as a
  low-friction password-auth helper.
- Consider replacing the whole auth exchange with rsasl once bifrost's public
  auth API is clearer. If we do this, do it around bifrost-owned wrapper types
  rather than blindly exposing rsasl everywhere.
- Add focused protocol tests for SMTP `AUTH OAUTHBEARER` success and failure
  exchanges using an in-process test server.
- Decide whether OAUTHBEARER should include `host` and `port` key/value pairs.
  RFC 7628 examples include them, while the bearer-token requirements only make
  them mandatory for keyed message digest schemes.

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
- Kept `peer_certificate()` for native-tls.
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
