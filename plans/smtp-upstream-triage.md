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

- Full timeout semantics for async SMTP command reads/writes, STARTTLS command
  exchange, `test_connection`, and shared-deadline budgeting
  (#917/#1027/#978) need a dedicated pass.
- Focused tests for async DNS and TLS-handshake timeout paths are still needed.
- Dropping `async-trait` is a larger trait/API rewrite and belongs with the
  async transport redesign.
