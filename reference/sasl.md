# bifrost-sasl

Private shared SASL/SCRAM computation layer for the bifrost protocol crates.
Not in the public API of any protocol crate. Depended on by `bifrost-imap`
and `bifrost-smtp`. No `bifrost-types`, no
`bifrost-net`, no tokio: the crate is sync and computation-only.

## Scope

Pure computation: no I/O, no async, no protocol command flow. The protocol
crates own the wire sequencing - driving `+` continuations, framing commands -
and call into the transition functions here. The SASL crate is invoked from
inside the protocol consumers, never the other way around.

## Surface

- `Secret` - zeroizing string wrapper (`Zeroizing<String>` newtype),
  constant-time `PartialEq`, redacted `Debug`, `as_str` / `as_bytes` /
  `into_zeroizing`, plus `From<String>` / `From<&str>` /
  `From<Zeroizing<String>>` / `Deref<Target=str>` / `AsRef<str>`. Each protocol
  crate keeps its own secret type and converts at the boundary (IMAP has
  `From<bifrost_sasl::Secret> for SecretString`, which moves the inner
  allocation so no plaintext copy is left un-zeroized).
- `SaslError` - `#[non_exhaustive]`. `Protocol(String)` for malformed or
  unexpected SASL/SCRAM messages; `AuthFailed(String)` for a SCRAM `e=` server
  error or a failed exchange.
- `ScramHash` - `#[non_exhaustive]` `{ Sha1, Sha256 }`. Names the hash, not a
  protocol-layer mechanism. `mechanism_name(ChannelBinding)` returns the wire
  name, owning the `-PLUS` suffix in one place:
  `"SCRAM-SHA-1"` / `"SCRAM-SHA-256"` for `ChannelBinding::None`,
  `"SCRAM-SHA-1-PLUS"` / `"SCRAM-SHA-256-PLUS"` for
  `ChannelBinding::TlsServerEndPoint`.
- `ChannelBinding` - `#[non_exhaustive]` `{ None, TlsServerEndPoint }`. The
  channel-binding dimension, orthogonal to `ScramHash`, so the proof math stays
  hash-parameterized.
- `ScramChannelBinding` - `#[non_exhaustive]` data-carrying enum
  `{ None, TlsServerEndPoint(Vec<u8>) }`: the GS2 channel-binding input to
  `scram_client_final`. `gs2_header()` returns the GS2 header string
  (`"n,,"` / `"p=tls-server-end-point,,"`); `binding()` returns the matching
  `ChannelBinding`. A data-carrying enum, not a discriminant-plus-data struct,
  so the GS2 header and the `c=` value never desync. A PLUS consumer MUST build
  its client-first header from `gs2_header()`, not a literal.
- `tls_server_end_point(cert_der) -> Result<Vec<u8>, SaslError>` - RFC 5929
  Section 4 `tls-server-end-point` binding data: the hash of the whole
  DER-encoded certificate, using the hash named by the cert's own
  `signatureAlgorithm` with MD5/SHA-1 upgraded to SHA-256 (Section 4.1). A
  small in-crate TLV walk reads the signatureAlgorithm OID (no X.509 stack, no
  ASN.1 dependency); SHA-384/512 hash families are supported via `sha2`.
  Unrecognized OIDs and EdDSA (Ed25519/Ed448) are a hard `Protocol` error
  rather than a guessed binding hash - a wrong binding value is a silent auth
  failure. Returns raw hash bytes; the caller base64-frames them.
- `scram_client_final(hash, password, client_nonce, client_first_bare,
  server_first, binding: &ScramChannelBinding) -> Result<(Secret, Vec<u8>),
  SaslError>` - parses the server-first message, runs PBKDF2/HMAC, returns the
  base64 client-final and the expected server signature. The `c=` attribute is
  `base64(gs2_header || cbind_data)` derived from `binding`; `None` is
  byte-identical to the old `c=biws`.
- `verify_server_final(server_final, expected_signature) -> Result<(),
  SaslError>` - checks for a server `e=` error, then verifies the `v=`
  signature.
- `escape_username(user) -> String` - RFC 5802 saslname escaping of `=` / `,`.
- `decode_continuation(data) -> Result<String, SaslError>` - base64-decode a
  SASL continuation to UTF-8.
- `cram_md5_response(user, pass, challenge) -> Result<Secret, SaslError>` -
  RFC 2195 CRAM-MD5 response.
- `xoauth2_payload(user, access_token) -> Secret` /
  `oauthbearer_payload(identity, access_token) -> Secret` (`oauth.rs`) - OAuth
  SASL client payloads as a zeroizing `Secret` of the *raw* (un-base64'd)
  bytes; the protocol crate frames them (IMAP base64-encodes for AUTHENTICATE,
  SMTP's AUTH command base64-encodes downstream). XOAUTH2 has no GS2 framing so
  nothing is escaped; OAUTHBEARER's `a=` identity is GS2-escaped via
  `escape_username` (same RFC-derived escape table as SCRAM saslname) and the
  optional RFC 7628 `host=` / `port=` attributes are deliberately omitted to
  stay transport-agnostic.

The per-hash proof helpers, `scram_field`, and the HMAC/XOR primitives stay
private to the crate.

## Error mapping contract

`SaslError` is mapped back into each protocol crate's error enum at the single
call boundary. For IMAP: `Protocol(m)` becomes `Error::Protocol(m)` (identical
message); `AuthFailed(m)` becomes `Error::auth_with_code(m, None)` (the
auth-failure lane). This preserves the pre-extraction classification exactly -
protocol-class messages stay protocol-class, the SCRAM `e=` server error stays
on the auth-failure lane. A SCRAM signature-verification mismatch is a
protocol-class failure (`Protocol`), not `AuthFailed`.

## Correctness pins

Unit tests in the crate are the load-bearing proof that the computation is
correct:

- RFC 5802 SHA-1 client-final + server signature (`scram.rs`), now passing
  `ScramChannelBinding::None` - proves the `None` path stays byte-identical to
  the old `c=biws`.
- RFC 2195 CRAM-MD5 vector (`cram.rs`).
- SCRAM-PLUS construction (`scram.rs`): a constructed deterministic pin (no
  published RFC 5802 PLUS vector exists) that the `c=` field equals
  `base64("p=tls-server-end-point,," || cbind)` and that PLUS and non-PLUS
  diverge in proof + server signature for the same inputs.
- RFC 5929 cert binding (`channel_binding.rs`): `tls_server_end_point` over a
  hand-built DER hashes the whole DER under the family named by the
  signatureAlgorithm OID; an SHA-1-signed cert upgrades to SHA-256; truncated,
  unrecognized-OID, and EdDSA certs are `Protocol` errors. The OID-to-family
  table is exercised directly so a mistyped OID byte fails a type-level test.

## Forward note

IMAP and SMTP both drive the PLUS path: their consumers send the
`p=tls-server-end-point,,` GS2 header, pull the peer cert DER, select PLUS when
advertised, and enforce RFC 5802 Section 6 downgrade protection (see
`reference/imap.md` / `reference/smtp.md` and git history). SMTP consumes
`ScramHash` / `ScramChannelBinding` / `scram_client_final` /
`verify_server_final` via `ScramExchange`. OAuth (XOAUTH2 / OAUTHBEARER)
payload construction now lives here (`xoauth2_payload` / `oauthbearer_payload`),
collapsing the formerly duplicated IMAP and SMTP builders. The crate still
exposes no selection function - selection policy lives in each protocol crate.
EdDSA leaf-cert channel binding is deliberately a hard error until an
evidence-driven vector pins the binding hash a real server uses.

## Mechanism preference

When password credentials are configured, each protocol crate picks the
strongest mechanism the server advertises, in this canonical order:

1. SCRAM-SHA-256-PLUS
2. SCRAM-SHA-1-PLUS
3. SCRAM-SHA-256
4. SCRAM-SHA-1
5. PLAIN (only over TLS)

A channel-bound SHA-1 exchange outranks an unbound SHA-256 one, so
SCRAM-SHA-1-PLUS sits above SCRAM-SHA-256. LOGIN and any other
cleartext-credential mechanism are never auto-selected when something
stronger is on offer; they are opt-in only (IMAP `AuthPolicy::allow_login`,
SMTP an explicit `authentication(...)` list - LOGIN is not in SMTP's default
`PASSWORD_MECHANISMS`). OAuth (XOAUTH2 / OAUTHBEARER) is an orthogonal path:
when OAuth credentials are configured the SCRAM ladder does not apply. RFC 5802
Section 6 downgrade protection drops the unbound `SCRAM-SHA-N` rung whenever
`SCRAM-SHA-N-PLUS` is advertised, and a PLUS rung whose binding cannot be
produced (plaintext, or an EdDSA cert) is skipped rather than downgraded - so a
binding-incapable connection against a PLUS-advertising server falls through to
PLAIN-over-TLS. The selection function lives per-protocol (IMAP
`password_mechanism_ladder`, SMTP `password_mechanism_order`); see
`reference/imap.md` / `reference/smtp.md`.

## Non-goals

- `tls-unique` channel binding. TLS 1.3 removed the extractor and bifrost only
  supports modern TLS, so `tls-server-end-point` is the only binding offered.
- SCRAM-SHA-512 family. Not implemented; revisit if a real deployment requires
  it.
- GSSAPI / Kerberos. Different stack, no demand.
- A typed public auth-outcome surface (which mechanism + binding were used, or a
  typed failure reason) is not built; the protocol crates map into their
  existing error types. Not scheduled.
