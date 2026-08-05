# bifrost-smtp / bifrost-sasl bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/smtp/` and `crates/sasl/`, hunted
together because sasl exists to serve the auth paths. Read-only review; no build or test was run.
Findings are unverified work material. Line numbers are as of the hunt and will drift.

## bifrost-sasl

### No SASLprep (RFC 4013) on the SCRAM password or username

`crates/sasl/src/scram.rs`. RFC 5802 section 5.1 requires the password (and the `n=` saslname) to
be normalized with SASLprep before PBKDF2. `scram_client_final` feeds `password.as_bytes()` straight
in, and `escape_username` only does the `=`/`,` escape. Any non-ASCII password (accented characters,
non-NFKC input, embedded non-ASCII space) produces a different `SaltedPassword` than a conforming
server computes, so SCRAM silently fails auth against e.g. Cyrus/Dovecot while PLAIN would have
worked. Neither `reference/sasl.md` nor its "Correctness pins" section mentions this; the docs read
as if the computation is complete. This is the most likely real-world interop bug in the crate. At
minimum it needs to be a documented non-goal with a stringprep-shaped error for non-ASCII input;
properly it needs `stringprep::saslprep`.

### No minimum on the SCRAM iteration count

`crates/sasl/src/scram.rs`. There is a ceiling (`MAX_SCRAM_ITERATIONS`) and an `i == 0` rejection,
but nothing between. RFC 7677 section 4 sets a hard floor of 4096. A hostile or MITM'd server sends
`i=1` and the client happily runs a single PBKDF2 round, making the salted password trivially
brute-forceable offline from the captured `p=` proof. The ceiling is defended in a long comment as a
DoS guard; the floor, which is the actually-specified requirement and the one with a
credential-disclosure consequence, is absent. Same class of downgrade the crate carefully guards
against elsewhere (`-PLUS` stripping, nonce non-extension).

### Key material is never zeroized

`crates/sasl/src/scram.rs`. `salted` (`[u8; 20]`/`[u8; 32]`), `client_key`, `stored_key`,
`server_key`, and the `proof` `Vec` from `xor_bytes` are ordinary stack arrays and `Vec<u8>` that
drop without wiping. `SaltedPassword` and `ClientKey` are password-equivalent (they authenticate
forever without the password); `StoredKey` lets you impersonate the server. The crate goes to real
trouble to wrap the base64 client-final in `Zeroizing` two lines later, so the intent is clearly
there and the intermediates were missed. `hmac_digest` returning `Vec<u8>` is the shape that makes it
easy to miss; it should return `Zeroizing<Vec<u8>>`, and `salted` should be a `Zeroizing<[u8; N]>`.

### constant_time_eq is hand-rolled and duplicated

`crates/sasl/src/scram.rs` and `crates/sasl/src/secret.rs`. Two byte-identical implementations.
Neither uses `subtle` or `core::hint::black_box`, so nothing prevents LLVM from recognizing the
accumulator pattern and short-circuiting; on the `verify_server_final` path that is a
server-impersonation timing oracle, which is exactly the threat the comment names. This should be
one `subtle::ConstantTimeEq` call site. Low likelihood of the optimizer actually breaking it today,
but the code is carrying a correctness claim it cannot enforce.

### RSASSA-PSS certificates get no channel binding

`crates/sasl/src/channel_binding.rs`. The OID table covers PKCS#1 v1.5 RSA, ECDSA, bare digests, and
EdDSA-as-error, but not `1.2.840.113549.1.1.10` (`id-RSASSA-PSS`). PSS leaf certs are issued in
production today. The result is not a loud failure: `resolve_scram_binding` in the SMTP driver does
`.ok()?`, so an unrecognized OID silently degrades to `None`, `first_attemptable` skips the PLUS
rung, and the connection quietly falls through to PLAIN-over-TLS. So the "fail loud rather than
guess" property the module docs claim is defeated one layer up: the typed error is discarded. Two
things wanted: add PSS (its binding hash comes from the `parameters` field, not the OID, so it needs
a small extra parse), and make the driver distinguish "no TLS" from "TLS but binding computation
failed" so the latter is at least observable.

### CRAM-MD5 response string is not zeroized

`crates/sasl/src/cram.rs`. `response` is a plain `String` containing the HMAC digest; the base64 of
it becomes a `Secret`, but the pre-encoded buffer leaks to the allocator. Minor relative to the
SCRAM key material, same fix shape.

### scram_field takes the first matching field

`message.split(',').find_map(...)`. A server sending `v=<forged>,v=<real>` gets the first one
verified. Not exploitable (the attacker controls both), but the parser is lenient where RFC 5802 is
not: duplicate attributes are malformed and should be rejected, and the same leniency means an `e=`
buried after other fields is found while a malformed message with a `,`-containing value could be
mis-split. Cheap to tighten.

## bifrost-smtp

### HeaderName::new_from_ascii permits CR and LF, so header injection

`crates/smtp/src/message/header/mod.rs`. The check is
`!empty && len <= 76 && is_ascii() && !contains([':', ' '])`. Control characters are not excluded.
`HeaderName::new_from_ascii("X-Foo\r\nBcc".to_owned())` succeeds, and `Display for Headers` writes
`name`, `": "`, value, `"\r\n"` verbatim with no re-validation. Any caller building a raw header from
an untrusted name string injects arbitrary headers. This matters because the value side is carefully
defended (`allowed_char` excludes 10 and 13, so CR/LF in a value get RFC 2047 encoded, and there is a
test for it) while the name side, going through the same `Display`, is not. The const
`new_from_ascii_str` has the same gap but is compile-time so it is only a footgun. Fix: reject every
byte outside `!`..`~` (RFC 5322 `ftext` is `%d33-57 / %d59-126`, printable minus colon), which also
subsumes the existing space/colon check.

### Cleartext credentials leave Secret at the SMTP boundary

`crates/smtp/src/transport/smtp/authentication.rs` and `commands.rs`.
`Mechanism::response_with_token` returns `String`. For PLAIN it is
`format!("\0{username}\0{password}")`; for LOGIN it is `password.to_owned()`; for
XOAUTH2/OAUTHBEARER it copies the `Secret` out with `.as_str().to_owned()` immediately after
`bifrost-sasl` went to the trouble of building it zeroizing. That `String` is then stored in the
`Auth` command struct (`response: Option<String>`), cloned around, base64-encoded into another plain
`String` in `Display`, and dropped un-wiped at every layer. The whole `Secret`/`Zeroizing` apparatus
in sasl and in `Credentials` is defeated at this one boundary. `response_with_token` should return
`bifrost_sasl::Secret`, `Auth.response` should hold one, and `crate::base64::encode` needs a
zeroizing variant. Also `Credentials::oauth2` does `token.to_string()` on the `Zeroizing<String>`,
creating an un-zeroized copy to hand to `StaticTokenSource`.

### PLAIN does not reject NUL in the username

`authentication.rs`: `format!("\u{0}{username}\u{0}{password}")` with no validation. RFC 4616 forbids
NUL in authcid/authzid/passwd. A username containing `\0` splits into an extra field, so a
caller-supplied identity can inject an authzid (`authzid\0authcid\0passwd`): an
authorization-identity injection, not just a malformed message. Same for the password. Should be a
hard error, not silently formatted.

### LOGIN challenge matching is text equality against a fixed list, and the helper is misnamed

`authentication.rs`. `contains_ignore_ascii_case` does `needle.eq_ignore_ascii_case(haystack)`; it is
`any_eq`, not `contains`, so the name is actively misleading and the list entries that look like
substrings (`"Username:"` vs `"Username"`, `"User Name\0"`) are exact-match alternatives. Any server
whose prompt is not exactly one of six strings (`"Enter username:"`, a localized prompt, a prompt
with a trailing space) fails with "Unrecognized challenge". LOGIN is positional (first challenge is
the username, second is the password) and driving it off a challenge counter would be both correct
and shorter. Since LOGIN is now opt-in-only this is low-impact, but it is a latent breakage for the
exact legacy servers LOGIN exists to serve.

### Auth's Display can panic

`commands.rs`: two `.unwrap()`s on `encoded_response`. `Auth::new` maintains the invariant today, but
`Display` is an infallible trait impl on a type whose field is `Option`; the invariant is implicit
and unenforced. Making `response` non-optional for the challenge/IR paths (separate constructors, or
an enum) removes the possibility.

### Channel binding is resolved per-mechanism although it does not vary by mechanism

`client/connection.rs` and the mirror in `async_connection.rs`. The loop calls
`resolve_scram_binding()` once for `ScramSha256Plus` and again for `ScramSha1Plus`, each time cloning
the peer certificate DER out of the TLS stream and re-running a SHA-2 over the whole certificate, and
stores two identical values in a `HashMap` keyed by mechanism. `tls-server-end-point` is a property
of the certificate, not of the SCRAM hash. This should be one `Option<ScramChannelBinding>` computed
once; the `HashMap` also allocates on every authenticated connect. It reads like the type was chosen
to fit `first_attemptable`'s `Fn(Mechanism) -> bool` closure rather than the other way round.

### first_attemptable consumes only the head of a fully-computed ladder

`password_mechanism_order` builds an ordered `Vec` of every acceptable mechanism, and the only
consumer takes the first non-skipped element and discards the rest; there is no retry down the ladder
on wire rejection (deliberate, and documented). The ordered-list abstraction therefore earns nothing
over a function that returns the single chosen mechanism. Either collapse it, or make it load-bearing
by actually retrying the next rung on a 535 (which is what most clients do, and what makes an ordered
ladder worth having).

### The DATA writer appends an unconditional extra CRLF, altering every delivered message

`client/mod.rs` and the DATA writers. `smtp_data_size` is `message.len() + 2` because the terminator
is always written as `\r\n.\r\n` regardless of whether the buffer already ends in CRLF. RFC 5321
section 4.1.1.4 specifies the terminator as `<CRLF>.<CRLF>` where the leading CRLF is the final CRLF
of the message body, so every well-formed message bifrost sends gains a trailing empty line that the
sender did not write, and `Message::formatted()` is not what the recipient receives.
`reference/smtp.md` documents this at length and calls changing it "wire-compatible only after a
dedicated API decision", but the change is a two-line conditional
(`if !buf.ends_with(b"\r\n") { write CRLF }`) plus the matching `smtp_data_size` adjustment, and it
makes `formatted()` honest. The hunter's read is that the doc is rationalizing an inherited lettre
bug and it is worth fixing rather than documenting.

### The entire blocking transport half is dead weight in this workspace

`connection.rs` (2867 lines) / `async_connection.rs` (2701), `transport.rs` (1429) /
`async_transport.rs` (1433), `pool/sync_impl.rs` (358) / `pool/async_impl.rs` (377), `net.rs` (377) /
`async_net.rs` (488): roughly 5,000 lines of hand-mirrored state machine, with two copies of the
PIPELINING window drain, the LMTP final-status drain and retirement rule, the SCRAM driver, the
metering funnel, and the connection-state (`Ok`/`Broken`/`Closed`) discipline. The only in-workspace
consumer is `crates/imap/src/account/submission.rs`, which uses `AsyncSmtpTransport` exclusively.
Every invariant in the "Connection state and cancel-safety" and "PIPELINING" sections of the
reference has to be proven twice and can drift silently. Given the pre-1.0 posture, the right move is
to delete the blocking half outright; if a blocking API must survive for external consumers, it
should be a thin `block_on` shim over the async driver rather than a second implementation. This is
the largest structural finding in the crate and it would remove more code than everything else here
combined.

### oauth2_token_blocking polls a future once with a noop waker and drops it

`authentication.rs`. This exists solely to serve the blocking transport. Polling and dropping a
`TokenSource::current()` future is not free in general: a source that acquires a lock, starts an HTTP
refresh, or registers with a shared in-flight map will have that work started and then cancelled at
an arbitrary await point, once per connect attempt. It also means a perfectly fresh token behind an
async mutex returns `Pending` and is rejected with a misleading "requires a network refresh" message.
It works today because the only sources are `StaticTokenSource` and `OAuthRefresher`; it is a
correctness landmine for any third `TokenSource` impl. Deleting the sync transport deletes this.

## Doc divergence

`reference/sasl.md` presents the SCRAM computation as pinned-correct without noting the absent
SASLprep or the absent iteration floor; its "Correctness pins" list should name both as gaps or
non-goals. It also says binding failures are "a hard `Protocol` error rather than a guessed binding
hash", which is true inside the crate but not observable to the caller because the SMTP driver
swallows it with `.ok()?`. Either the reference should say the consumer treats it as a skip signal,
or the consumer should stop discarding it.

## Uncertain, needs a pin

- The hunter did not verify whether `email_address::is_valid_local_part` rejects CR/LF inside a
  quoted local part (`"a\r\nb"@x.com`). If it does not, `Address::from_str` into
  `MAIL FROM:<{}>` in `commands.rs` is a command-injection path from a parsed address string, since
  neither `Mail` nor `Rcpt` re-validate at `Display` time (`Vrfy`/`Expn` do, via
  `validate_single_line_argument`). Either way, `Mail::new`/`Rcpt::new` should apply the same
  control-character check the VRFY/EXPN builders apply: the defense belongs at the wire boundary, not
  in a dependency's parser, and `Address::new_dangerous` is a public constructor that bypasses
  validation entirely.
- `parse_response`'s last-line `alt` (`response.rs`) requires either `" "` or an immediate CRLF after
  the code, but there is a test named `parse_response_accepts_reply_without_a_space_separator`. The
  hunter could not reconcile those by reading; either the test asserts a failure or the combinator was
  misread. Worth a look.
