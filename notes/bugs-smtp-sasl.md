# bifrost-smtp / bifrost-sasl bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/smtp/` and `crates/sasl/`, hunted
together because sasl exists to serve the auth paths. Read-only review; no build or test was run.
Findings are unverified work material. Line numbers are as of the hunt and will drift.

## bifrost-smtp

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
