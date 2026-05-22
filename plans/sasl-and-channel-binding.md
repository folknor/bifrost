# SASL and channel binding

Pre-ratatoskr stabilization item. Auth policy enters ratatoskr's
account model very quickly; landing this after ratatoskr depends on
bifrost's public behavior means migration churn, duplicated edge
cases, and harder enterprise debugging. Do it before, not after.

Enterprise deployments treat SCRAM-SHA-*-PLUS as a hardening
requirement. Both IMAP and SMTP need it. Enterprise SMTP submission
servers frequently carry stricter auth policy than the matching IMAP
servers; SMTP must not stay weaker just because sending is the
"secondary" path.

## Current state

- IMAP hand-rolls SCRAM-SHA-1 and SCRAM-SHA-256 in
  `crates/imap/src/connection/dispatch/auth.rs:322`. State machine,
  `scram_client_final`, `scram_proof_sha1` / `scram_proof_sha256`,
  and server-final verification are all already there. No
  channel-binding variant yet.
- SMTP advertises only PLAIN, LOGIN, XOAUTH2, OAUTHBEARER in
  `crates/smtp/src/transport/smtp/authentication.rs:132`. No SCRAM
  family at all.
- IMAP and SMTP both wrap `native-tls` streams but neither surfaces
  the peer certificate upward. That is the real blocker for SCRAM-PLUS.

## Plan

### 1. Private shared SASL layer

A `bifrost-sasl` workspace crate (or a private module re-exported
through both protocol crates). Both `bifrost-imap` and `bifrost-smtp`
depend on it. The crate is private - it does not appear in the
public APIs of either protocol crate.

The shared layer owns:

- Mechanism selection (priority list, downgrade-protection logic).
- SCRAM computation (state machine, `scram_client_final`,
  per-hash proof functions, server-final verification, PLUS variants).
- OAuth payload construction (XOAUTH2 / OAUTHBEARER framing, which is
  identical between IMAP and SMTP today and currently duplicated).
- Secret handling (`SecretString` lifetime, zeroization,
  base64 framing).
- Channel-binding input handling (consuming a `Vec<u8>` of cert DER,
  computing `tls-server-end-point` per RFC 5929).

Protocol command flow stays in each protocol crate. IMAP keeps its
`Consumer` / `AuthenticateScramConsumer` plumbing; SMTP keeps its
command pipeline. The SASL crate is invoked from inside those, not
the other way around.

If a focused evaluation later shows `rsasl` is meaningfully better,
it hides behind the shared crate. The protocol-facing API is shaped
by policy and outcomes (mechanism selection, channel-binding
availability, typed success / failure), not by whichever SASL stack
is underneath.

### 2. Transport plumbing before policy

Channel binding is the real blocker; landing it first unblocks
SCRAM-PLUS for both crates.

`NetworkStream` / `AsyncNetworkStream` in both crates grow a
`peer_certificate()` accessor returning the server certificate DER.
`native-tls`'s `TlsStream::peer_certificate()` already exposes the
DER bytes; the work is just plumbing them upward through the stream
wrappers.

The SASL crate then implements `tls-server-end-point` once.

**RFC 5929 §4, done properly.** The channel-binding value is the
hash of the **DER-encoded server certificate** (not the
SubjectPublicKeyInfo). The hash function is the one used in the
certificate's `signatureAlgorithm` field, with two exceptions: if
that signature hash is MD5 or SHA-1, use SHA-256 instead. So the
input is "DER of the cert" and the hash choice depends on the cert's
own signature algorithm. Implementations that hardcode
SHA-256-of-SPKI interoperate with most modern certs but break on
edge cases - do it correctly the first time.

The signature-algorithm OID is parseable directly from the cert DER
without pulling in a full X.509 stack; a small targeted parser in
the SASL crate is sufficient.

### 3. SCRAM-PLUS on both IMAP and SMTP

Once channel binding exists below the protocols:

- IMAP grows SCRAM-SHA-256-PLUS (and optionally SCRAM-SHA-1-PLUS)
  alongside the existing non-PLUS variants.
- SMTP grows the full SCRAM family at once - non-PLUS and PLUS
  together, since it currently has none.

Downgrade protection (RFC 5802 §6): when the server's advertised
mechanism list includes a PLUS variant, the client must not fall
back to the matching non-PLUS variant, even if the server also lists
it. This logic lives in the shared SASL crate so both protocols get
it for free.

### 4. Mechanism preference

OAuth and password auth are orthogonal paths in the account config.

- If the account configures OAuth credentials (XOAUTH2 or
  OAUTHBEARER), OAuth is used. The SCRAM ladder does not apply.
- If the account configures password credentials, the client picks
  the strongest mechanism the server advertises, in this order:
  1. SCRAM-SHA-256-PLUS
  2. SCRAM-SHA-256
  3. SCRAM-SHA-1
  4. PLAIN, only over TLS
- LOGIN and any cleartext-credential mechanism stay opt-in legacy
  fallbacks. They are never picked automatically when something
  stronger is on offer; the account config has to opt in explicitly.

The mechanism-selection function lives in the shared SASL crate so
both protocols apply the same policy.

### 5. Public API shape

Public auth surface (in `bifrost-imap` and `bifrost-smtp`) exposes
policy and outcomes, not the SASL library underneath. Consumers
configure credentials and an optional policy override (e.g. "allow
LOGIN fallback"); the client returns either success with a typed
record of which mechanism and binding were used (useful for audit
logs and enterprise debugging) or a typed failure (mechanism
rejected, channel binding required but unavailable, credential
rejected, server protocol violation).

## Out of scope

- `tls-unique` channel binding. TLS 1.3 removed the extractor and
  bifrost only supports modern TLS. `tls-server-end-point` is the
  only binding we offer.
- SCRAM-SHA-512 family. Revisit if a real deployment requires it.
- GSSAPI / Kerberos. Different stack, no demand.

## Implementation order

1. `peer_certificate()` accessor on `NetworkStream` and
   `AsyncNetworkStream` in both crates.
2. New private `bifrost-sasl` crate. Move IMAP's SCRAM state
   machine and helpers into it; rewire IMAP to consume them. IMAP
   stays green.
3. Implement `tls-server-end-point` (with correct signature-hash
   selection) and SCRAM-PLUS variants in the SASL crate.
4. Wire IMAP's mechanism selection to prefer PLUS when the server
   advertises it; add downgrade-protection check.
5. Add the SCRAM family (non-PLUS and PLUS) to SMTP via the shared
   crate; wire the same mechanism selection and downgrade
   protection.
6. Move OAuth payload construction into the shared crate; collapse
   the duplicated XOAUTH2 / OAUTHBEARER builders in IMAP and SMTP.
