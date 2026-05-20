# bifrost-smtp reference

Current architecture of the SMTP client crate. Descended from lettre 0.11.22; the bifrost fork has substantially diverged. Native-tls only.

## Transport types

Sync and async, SMTP and LMTP:

- `SmtpTransport` / `AsyncSmtpTransport` - submission to MX/relay.
- `LmtpTransport` / `AsyncLmtpTransport` - local delivery via LMTP.

`Transport::Ok` is `Response` for SMTP, `Vec<Response>` for LMTP (one per envelope recipient, original order preserved).

Async transports support tokio and async-std. TLS URL parsing via `from_url` is tokio-only; plaintext URLs work on both.

`BoxedTransport<Ok, Error>` and `BoxedAsyncTransport<Ok, Error>` erase the concrete type. `Box<T>` and `Arc<T>` get blanket `Transport` / `AsyncTransport` forwarding.

## Connection lifecycle

```
greeting -> EHLO/LHLO -> [STARTTLS -> EHLO/LHLO] -> [AUTH -> EHLO/LHLO] -> ready
```

EHLO is re-sent after STARTTLS and AUTH because capabilities can change.

- `quit()` sends QUIT and reads the final response.
- `abort()` closes without sending QUIT. Used after protocol failure when the stream may no longer be writable.

## Connection state and cancel-safety

Sync and async streams carry an explicit state: `Ok` / `Broken` / `Closed`. Writes, flushes, reads, and TLS upgrades set `Broken` before the await; only success restores `Ok`. Dropped futures leave state `Broken`; the pool drops the connection.

LMTP final delivery-status loop holds `Broken` until every accepted recipient's status has been read.

`test_connected()` (pooled NOOP probe) aborts on failure so a stale connection cannot be recycled.

## Async setup deadline

`AsyncDeadline` is a single shared deadline across DNS, connect, TLS handshake, banner read, and initial EHLO. The deadline lives only until `connect_impl` returns; established connections use the per-operation timeout. `starttls(...)` on an established connection uses the per-operation timeout because it is an explicit command, not setup.

## PIPELINING

When the server advertises PIPELINING, `MAIL FROM`, every `RCPT TO`, and `DATA` are written in one batch. Replies are drained in order before the body is sent. The body is never in the pipelined batch. On RCPT failure mid-pipeline the transaction is aborted before the body.

## DSN and SendOptions

`SendOptions` is the per-message API for advanced ESMTP parameters:

- MAIL FROM params: `REQUIRETLS`, `FUTURERELEASE` (HOLDFOR / HOLDUNTIL), `DELIVERBY`, `MT-PRIORITY` (RFC 6710), DSN `RET`, DSN `ENVID`.
- Uniform RCPT TO params: DSN `NOTIFY`.
- Recipient-specific RCPT TO params: DSN `NOTIFY` overrides per recipient, `ORCPT` (default addr-type `rfc822`).

Server-advertised `DSN` is verified before DSN parameters are emitted. Recipient-specific params override matching global keywords (RFC 3461 §4.1 no-duplicate-keyword).

Public entry points: `send_raw_with_options(...)` on SMTP and LMTP, sync and async.

## xtext encoding

RFC 3461 §4.1: any byte outside `%x21-%x7E`, plus `+`, plus `=`, is encoded as `+HH` with uppercase two-digit hex. UTF-8 multi-byte sequences are encoded byte-by-byte. Used for ENVID, ORCPT address part, and unknown ESMTP parameter values. Centralized in `util.rs`.

## VRFY / EXPN

`verify(addr, ...)` and `expand(list, ...)` on SMTP transports. Negative replies return as `Response`, not `Err`, because they are normal outcomes for these privacy-sensitive commands. Inputs are sanitized for CRLF and control characters before any wire write.

## BDAT

Opt-in via explicit `send_raw_bdat` / `send_raw_bdat_with_options`. Defaults still use DATA so dot-stuffing and BINARYMIME behavior are not changed silently. BDAT requires `CHUNKING` to be advertised.

## SIZE enforcement

If the server advertises `SIZE=<bytes>`, message size is checked client-side before MAIL FROM, and the advertised value is included as `SIZE=<bytes>` on the wire.

## Auth

`Credentials` is enum: `Password { user, password }` and `OAuth2 { identity, access_token }`. Both store `Zeroizing<String>`.

Mechanisms: PLAIN, LOGIN, XOAUTH2, OAUTHBEARER. Server-advertised mechanisms are honored in this order: OAUTHBEARER before XOAUTH2 (for OAuth2 credentials); explicit `authentication(...)` overrides defaults.

Plaintext AUTH is refused by default for both passwords and OAuth bearer tokens. Trusted local relays opt in with `dangerous_allow_insecure_auth(true)`.

AUTH continuation formatting treats challenge responses as continuation lines even for mechanisms supporting initial response. Required for OAUTHBEARER failed-auth dummy-cancel exchange (`AQ==` on the wire).

## LMTP

`LHLO` instead of EHLO. Default TCP port 24 (Postfix/Dovecot convention, not RFC-assigned).

Per-recipient response model: vector length equals envelope recipient count. RCPT-time rejection responses are preserved at the original recipient index; accepted recipients receive the post-DATA delivery response in original order.

Unix-domain LMTP constructors are `#[cfg(unix)]` on sync, tokio, and async-std. Unix sockets refuse STARTTLS explicitly.

## Native-tls only

The TLS backend matrix (rustls, boring-tls, rustls provider/verifier) has been removed. `CertificateStore::Default` always means the native-tls platform verifier.

`TlsParameters` carries the connector, SNI hostname, and a `dangerous_*` set for accepting invalid certs / hostnames (test only).

## Message builder

`Message::builder()` produces a builder for typed headers (From, To, Cc, Bcc, Reply-To, Subject, Date, Message-Id, In-Reply-To, References, MIME-Version, Content-Type, ...). `reply_to_many(...)` for RFC 5322 address-list `Reply-To`.

Display-name encoding emits RFC 5322 phrase text when the name is atom-shaped, RFC 2047 encoded-word otherwise. Avoids DKIM-breaking quoted-string rewrites at relays.

`MultiPart` kinds: `Mixed`, `Alternative`, `Related`, `Signed`, `Encrypted`, `Report { report_type }`. `Mixed` is the default for `MultiPart::builder().build()`.

`SinglePartBuilder::body(String)` and `MessageBuilder::body(String)` infer `Content-Type: text/plain; charset=utf-8` when no content type is set.

Typed headers for list management: `List-ID`, `List-Help`, `List-Unsubscribe`, `List-Unsubscribe-Post` (fixed value `List-Unsubscribe=One-Click` per RFC 8058), `List-Subscribe`, `List-Post`, `List-Owner`, `List-Archive`.

Content-type helpers: `text_plain_flowed()`, `text_plain_flowed_delsp()` (RFC 3676).

Batch builders: `MultiPart::multiparts(...)` and `.singleparts(...)`.

## DKIM

Default canonicalization is `relaxed/relaxed`. Signing keys: `From<rsa::RsaPrivateKey>` and `From<ed25519_dalek::SigningKey>` on `DkimSigningKey`. SHA-2 via `sha2` 0.10 to match `rsa` 0.9 digest traits.

## Sendmail transport

`SendmailTransport::new_with_command(...)` takes the command path. Tests use a fake-command directory under `target/sendmail-tests` keyed by label + pid; no real sendmail required.

## Pool

`PoolConfig` configures min idle, max size, and idle timeout. `min_idle` defaults to 0 (no background DNS / reconnect). `test_connected()` runs before reuse and aborts on failure.

## Error model

`Error` is `#[non_exhaustive]`. `Error::kind()` returns `ErrorKind`: `Response`, `Network`, `Connection`, `Client`, `Tls`, `Policy`, `Parse`, `Permanent`, `Transient`. Helpers: `is_response`, `is_network`, `is_connection`, `is_permanent`, `is_transient`, `is_policy`. `Policy` covers local refusals such as plaintext-AUTH refusal.

## Module layout

```
crates/smtp/src/
├── message/             - builder, headers, MIME parts, address parsers
├── transport/
│   ├── sendmail/        - sendmail invocation
│   └── smtp/
│       ├── client/      - connection, async_connection, net, async_net
│       ├── transport.rs / async_transport.rs - public sync + async transports
│       ├── commands.rs  - EHLO, MAIL, RCPT, DATA, BDAT, AUTH, NOOP, RSET, QUIT, VRFY, EXPN, STARTTLS, LHLO
│       ├── extension.rs - ServerInfo, SendOptions, MAIL/RCPT parameters
│       ├── response.rs  - parser, enhanced status codes
│       └── test_support.rs - shared mock servers
└── error.rs             - Error, ErrorKind
```

## SMTP-specific code style

- `client::` is private; low-level connection/network stream types are crate-internal.
- TLS variants are gated on `native-tls`; rustls/boring features have been removed.
- `#[non_exhaustive]` on `Error`, `ErrorKind`, DKIM enums, `CertificateStore`.
- `AsyncTransport` requires `Sync` so borrowed methods return `Send` futures.
- Async traits use native `impl Future + Send` return types; no `async-trait`.
