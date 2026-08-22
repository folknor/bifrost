# bifrost-smtp / bifrost-sasl bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/smtp/` and `crates/sasl/`, hunted
together because sasl exists to serve the auth paths. Read-only review; no build or test was run.
Findings are unverified work material. Line numbers are as of the hunt and will drift.

## bifrost-smtp

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
