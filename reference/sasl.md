# bifrost-sasl

Private shared SASL/SCRAM computation layer for the bifrost protocol crates.
Not in the public API of any protocol crate. Depended on by `bifrost-imap`
today; `bifrost-smtp` joins in a later step. No `bifrost-types`, no
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
- `ScramHash` - `#[non_exhaustive]` `{ Sha1, Sha256 }`, with
  `mechanism_name()` returning `"SCRAM-SHA-1"` / `"SCRAM-SHA-256"`. Names the
  hash, not a protocol-layer mechanism.
- `scram_client_final(hash, password, client_nonce, client_first_bare,
  server_first) -> Result<(Secret, Vec<u8>), SaslError>` - parses the
  server-first message, runs PBKDF2/HMAC, returns the base64 client-final and
  the expected server signature.
- `verify_server_final(server_final, expected_signature) -> Result<(),
  SaslError>` - checks for a server `e=` error, then verifies the `v=`
  signature.
- `escape_username(user) -> String` - RFC 5802 saslname escaping of `=` / `,`.
- `decode_continuation(data) -> Result<String, SaslError>` - base64-decode a
  SASL continuation to UTF-8.
- `cram_md5_response(user, pass, challenge) -> Result<Secret, SaslError>` -
  RFC 2195 CRAM-MD5 response.

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

Two RFC vectors live as unit tests in the crate and are the load-bearing proof
that the computation is correct: the RFC 5802 SHA-1 client-final + server
signature (`scram.rs`) and the RFC 2195 CRAM-MD5 vector (`cram.rs`).

## Forward note

Later steps land here: `tls-server-end-point` channel binding (RFC 5929) and
SCRAM-PLUS variants, mechanism-selection policy with downgrade protection, and
OAuth (XOAUTH2 / OAUTHBEARER) payload construction currently duplicated in the
protocol crates. None of that is present yet; `ScramHash` stays two-variant and
the crate exposes no selection function.
