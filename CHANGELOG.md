# Changelog

## bifrost-smtp 0.1.0 - unreleased

Initial release under the `bifrost-smtp` name.

This crate descends from [`lettre`](https://github.com/lettre/lettre) 0.11.22
and is being adapted for ratatoskr. Notable fork-time API breaks so far:

- OAuth credentials are explicit through `Credentials::oauth2(...)` and
  builder `.oauth2(...)` helpers. `OAUTHBEARER` is preferred before `XOAUTH2`.
- SMTP AUTH refuses plaintext connections by default. Trusted local relays can
  opt in with `.dangerous_allow_insecure_auth(true)`.
- `transport::smtp::client` is no longer public. TLS configuration types moved
  to `transport::smtp::{Tls, TlsParameters, Certificate, Identity, ...}`.
- Low-level SMTP connection and stream APIs, including `connect_with_transport`,
  `set_stream`, `peer_addr`, `peer_certificate`, and direct
  `AsyncSmtpConnection::connect_tokio1` access, are now internal.
- Async transports no longer use `async-trait`; custom async transports must be
  `Sync`, and async `send` now takes `&Message`.

## bifrost-jmap 0.1.0 - unreleased

Initial release under the `bifrost-jmap` name.

This crate descends from [`jmap-client`](https://github.com/stalwartlabs/jmap-client), which is unmaintained upstream. Substantial portions have been rewritten since the fork point - most notably:

- **Trait-based method dispatch** replaces the central `Method` / `Arguments` / `MethodResponse` enums.
- **Transport abstraction** - `Client<T: HttpTransport>` allows custom transports (testing, WASM, etc.).
- **Calendars** (draft-ietf-jmap-calendars-26), **Contacts** (RFC 9610), **Blob Management** (RFC 9404), **Quotas** (RFC 9425), and **Sharing** (RFC 9670) added.
- **Structured errors** - `Error::Internal(String)` removed; every variant is matchable.
- **`Field<T>`** three-state nullable replaces `Option<Option<T>>`.

For pre-fork history, see <https://github.com/stalwartlabs/jmap-client/blob/main/CHANGELOG.md>.
