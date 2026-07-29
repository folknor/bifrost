# bifrost-smtp bug hunt

Scope: `crates/smtp/src/**` and `crates/smtp/tests/**`. Read-through of the
transport (sync + async connection drivers, pipelining, batch resolver, DSN
parameter encoding, LMTP per-recipient semantics, pool), the message builder
(MIME, headers, bodies), the address/envelope types, and DKIM.

Findings are ordered by severity within each section. Nothing here has been
fixed - tests landed alongside this document pin behavior *as it is today*, and
every test that documents behavior I believe is wrong says so in a comment that
points back here.

---

## Bugs

### B1 - DKIM relaxed body canonicalization does not remove a body that is only empty lines

`crates/smtp/src/message/dkim.rs:243-260`

```rust
DkimCanonicalizationType::Relaxed => {
    ...
    // Remove empty lines at end
    while out.ends_with(b"\r\n\r\n") {
        out.truncate(out.len() - 2);
    }
```

RFC 6376 3.4.4 step b: "Ignore all empty lines at the end of the message body.
... Note that a completely empty or missing body is canonicalized as a null
input; therefore, there will be no trailing CRLF." The loop stops as soon as one
CRLF is left, so a body consisting entirely of empty lines canonicalizes to
`"\r\n"` where every verifier computes `""`.

Path to failure:

1. `Message::builder()...body(String::new())` -> `MessageBody::Raw(vec![])`.
2. `Message::body_raw()` (`message/mod.rs:547`) appends `b"\r\n"` unconditionally,
   so the DKIM input is `b"\r\n"`. That is deliberate and correct - the DATA
   writer (`connection.rs:1631`, `write_body` at `:920`) always emits
   `\r\n.\r\n`, so the body the receiving MTA stores really is one empty line.
3. `dkim_canonicalize_body(b"\r\n", Relaxed)` returns `b"\r\n"`; bifrost signs
   `bh=` over `sha256("\r\n")`.
4. The verifier canonicalizes the received body `"\r\n"` per RFC 6376 3.4.4 to
   `""` and computes `sha256("")`.
5. `bh=` mismatch -> `permerror` / DKIM fail, for every message with an empty
   body under the *default* `relaxed/relaxed` canonicalization.

The same applies to a body that is nothing but blank lines
(`b"\r\n\r\n\r\n"` -> should be `""`, is `"\r\n"`).

Proposed fix: strip *all* trailing CRLFs in the relaxed branch, then append a
single CRLF only if the result is non-empty:

```rust
while out.ends_with(b"\r\n") {
    out.truncate(out.len() - 2);
}
if !out.is_empty() {
    out.extend_from_slice(b"\r\n");
}
```

Pinned by `body_of_only_empty_lines_canonicalization` in `dkim.rs`.

### B2 - DKIM simple body canonicalization of an empty body returns 0 octets, not CRLF

`crates/smtp/src/message/dkim.rs:236-242`

RFC 6376 3.4.3: "Note that a completely empty or missing body is canonicalized
as a single `CRLF`; that is, the canonicalized length will be 2 octets."
`dkim_canonicalize_body(b"", Simple)` returns `b""`.

Latent rather than live: `body_raw()` never hands the canonicalizer an empty
slice, so `dkim_sign` cannot reach it today. It is one refactor of `body_raw`
away from becoming a live signature failure, and the function is wrong on its
own terms.

Proposed fix: in the simple branch, strip trailing `\r\n\r\n` as today, then
`if !body.ends_with(b"\r\n") { append b"\r\n" }` (returning `Cow::Owned`).

Pinned by `empty_body_canonicalization` in `dkim.rs`.

### B3 - Neither canonicalization appends the RFC-mandated final CRLF

`crates/smtp/src/message/dkim.rs:231-262`

Both RFC 6376 3.4.3 and 3.4.4 step b end with "If the body is non-empty but does
not end with a CRLF, a CRLF is added." Neither branch does. Same containment as
B2 (`body_raw` always appends), same recommended fix, folded into B1/B2.

Pinned by `body_canonicalization_does_not_append_a_missing_final_crlf`.

### B4 - DKIM `simple` header canonicalization very likely signs different bytes than it transmits (needs one experiment to confirm)

`crates/smtp/src/message/dkim.rs:388-423`

`dkim_sign_fixed_time` builds the DKIM-Signature header twice:

```rust
let dkim_header = dkim_header_format(dkim_config, timestamp, &signed_headers_list, &bh, "");
// ... hash canonicalize(dkim_header) ...
let dkim_header = dkim_header_format(dkim_config, timestamp, &signed_headers_list, &bh, &signature);
```

`dkim_header_format` builds the value through `HeaderValue::new`, which RFC-2047
encodes *and line-folds* it. RFC 6376 3.7 requires the header covered by the
hash to be byte-identical to the transmitted header except for the value of
`b=`. Under `simple` header canonicalization folds are preserved verbatim, so
any difference in *where* the folder breaks the line changes the hashed bytes.

The two calls differ only in the `b=` value: `""` versus ~344 base64 characters.
The folder is greedy left-to-right over space-separated words, so the tail of the
header changes: with an empty `b=`, `bh=<44 chars>; b=` is ~51 characters and
plausibly fits on the same continuation line, while with a real signature the
folder must break before `b=`. The existing test
(`test_signature_rsa_simple` -> `assert_dkim_signature`) asserts that the
*emitted* message has `bh=...;\r\n` and a separate line starting with ` b=`,
which is consistent with exactly that split.

If confirmed, every `simple` header canonicalization signature bifrost produces
fails at a real verifier while passing this crate's own structural tests -
precisely the failure mode that structural DKIM tests cannot catch. `relaxed`
(the default) is unaffected because
`dkim_canonicalize_headers_relaxed` unfolds continuation lines.

I did not write a test for this: asserting it either way without being able to
run the folder is a coin flip that breaks the build if I guess wrong. The
experiment is two lines in `dkim.rs`'s test module:

```rust
let empty  = dkim_header_format(&config, 0, "date:from", &"B".repeat(44), "").to_string();
let signed = dkim_header_format(&config, 0, "date:from", &"B".repeat(44), &"A".repeat(344)).to_string();
// Everything up to and including "b=" must be byte-identical.
```

Proposed fix if confirmed: emit the DKIM-Signature header pre-folded via
`HeaderValue::dangerous_new_pre_encoded`, with a fold inserted immediately
before `b=` in *both* calls, so the prefix cannot depend on the signature
length. (This is what the mainstream signers do.)

### B5 - FUTURERELEASE error kinds diverge between the sync and async connections

`crates/smtp/src/transport/smtp/client/connection.rs:1191-1211` versus
`crates/smtp/src/transport/smtp/client/async_connection.rs:1275-1295`

Async:

```rust
return Err(error::feature_unsupported("FUTURERELEASE requires server FUTURERELEASE support"));
...
return Err(error::parameter_over_limit("HOLDFOR exceeds the server-advertised FUTURERELEASE limit"));
```

Sync: the same two branches use `error::invalid_input`.

`reference/smtp.md` states the contract explicitly: "`FeatureUnsupported` /
`ParameterOverLimit` are the FUTURERELEASE discriminators ... kept distinct from
generic `InvalidInput` so the account-error mapping can tell scheduled-send-
unsupported apart from a malformed parameter", and `account_error.rs:131,145`
maps them to `Unsupported(AccountOperation::Send)` and `Request(Malformed)`.

`grep` confirms `feature_unsupported` and `parameter_over_limit` have exactly two
call sites in the whole crate, both in `async_connection.rs`. So
`SmtpTransport::send_raw_with_options` (sync) with a `hold_for`/`hold_until`
against a relay that does not advertise FUTURERELEASE produces
`InvalidInput` -> `Request(Malformed)` -> `ClientBug` telemetry instead of the
contracted `Unsupported(Send)`.

Proposed fix: mirror the two async arms into the sync `validate_mail_parameter`.

### B6 - `tracing` at debug level logs credentials and full message bodies verbatim

`crates/smtp/src/transport/smtp/client/connection.rs:1690-1692` and
`async_connection.rs:1843-1845`

```rust
#[cfg(feature = "tracing")]
tracing::debug!("Wrote: {}", escape_crlf(&String::from_utf8_lossy(string)));
```

`write()` is the single funnel for every byte the client sends, so with the
`tracing` feature enabled and a debug-level subscriber this emits:

- `AUTH PLAIN AHVzZXIAcGFzc3dvcmQ=` - base64, trivially reversible, so the
  account password lands in the log.
- `AUTH XOAUTH2 <base64 bearer token>` / `AUTH OAUTHBEARER ...` - same for OAuth
  access tokens.
- the SCRAM client-first/client-final lines.
- the entire DATA body of every message (`message_iter` calls `self.write` with
  the whole encoded message), i.e. the full plaintext of every email sent.

`commands.rs:303-309` additionally logs the decoded AUTH challenge. There is no
redaction anywhere on this path; `reference/smtp.md`'s claim that "AUTH paths
already redact credentials before they reach here" is about
`Error::diagnostic_text`, not about the wire logger.

Proposed fix: in `write()`, suppress or elide the payload when the connection is
inside an AUTH exchange and when writing the DATA body - e.g. keep a
`log_wire: bool` on the connection that is cleared for the duration of
`auth_*`/`message_*`, and log `"Wrote: <N bytes of message body>"` instead.
Downgrading to `trace!` is not sufficient; a plaintext-password log line should
not exist at any level.

### B7 - `MultiPart::boundary()` panics on a caller-supplied multipart Content-Type without a `boundary` parameter

`crates/smtp/src/message/mimebody.rs:303-314` and `:455-464`

```rust
pub fn build(mut self) -> MultiPart {
    if self.headers.get::<ContentType>().is_none() {
        self.headers.set(ContentType::from_mime(MultiPartKind::Mixed.to_mime::<String>(None)));
    }
    ...
pub fn boundary(&self) -> String {
    let content_type = self.headers.get::<ContentType>().unwrap();
    content_type.as_ref().get_param("boundary").unwrap()   // <- None
```

`build()` injects the default Content-Type (and with it a generated boundary)
only when *no* Content-Type is set. Setting one by hand skips that, and nothing
re-checks for the `boundary` parameter.

Reproduction, entirely through public API:

```rust
MultiPart::builder()
    .header(ContentType::parse("multipart/mixed").unwrap())
    .singlepart(SinglePart::plain("hello".to_owned()))
    .formatted();          // panics: Option::unwrap on a None value
```

Proposed fix: in `build()`, if a Content-Type is present but has no `boundary`
parameter, re-derive the mime with a generated boundary
(`MultiPartKind::from_mime(ct).unwrap_or(Mixed).to_mime::<String>(None)`),
preserving the parsed kind.

Pinned (as `#[should_panic]`) by
`multipart_with_a_boundary_less_content_type_panics_when_formatted`.

### B8 - `MultiPartBuilder::boundary` accepts an unvalidated boundary and panics inside `to_mime`

`crates/smtp/src/message/mimebody.rs:202-232, 291-300`

```rust
format!("multipart/{}; boundary=\"{}\"{}", ..., boundary, ...).parse().unwrap()
```

The boundary is interpolated into a quoted-string and the result is `parse()`d
with an `unwrap()`. A boundary containing `"` (`a"b`) or CRLF makes the `mime`
parser reject the string, so a caller-supplied value turns into a panic instead
of an error. `report_type`, `protocol`, and `micalg` go through the same
`format!` + `unwrap`; only `report_type` is token-validated (and only via an
`assert!`, which is itself a panic - see `to_mime`'s `assert!(is_mime_token(..))`).

I have not executed this, so I am labelling the exact failure mode as
"panic, almost certainly" rather than confirmed - the certain part is that
nothing validates the input before it reaches `.unwrap()`.

Proposed fix: validate `boundary` (RFC 2046 `bcharsnospace` set, 1-70 chars) and
`protocol`/`micalg` in a fallible constructor, mirroring `try_report`; keep the
panicking shorthand only for `&'static str` call sites.

### B9 - The MIME boundary is chosen without looking at the content it delimits

`crates/smtp/src/message/mimebody.rs:197-199, 484-497`

`make_boundary()` draws 40 random alphanumerics and `format_body` writes the
delimiter without ever checking whether the part bodies already contain it.
With a random 40-character boundary an accidental collision is not a real risk,
but:

- `MultiPartBuilder::boundary(s)` lets a caller pin a short, guessable value,
  and a body containing `\r\n--<that value>\r\n` then forges a part split at the
  receiving parser (attachment injection / content spoofing).
- `fastrand` is explicitly not cryptographic (the doc comment says so), so the
  boundary is predictable given other `fastrand` outputs from the same thread -
  `make_message_id` uses the same generator and its output *is* transmitted in
  `Message-ID`. An attacker who sees the Message-ID of one message can in
  principle recover the `fastrand` state and predict the boundary of the next.

This is the "boundary scanner fooled by attacker-controlled content" pattern in
its SMTP form. Severity is moderate: exploiting it requires the attacker to
control body content *and* predict or pin the boundary.

Proposed fix: after building the parts, scan the concatenated bodies for
`\r\n--<boundary>` and re-draw until absent (bounded retry), or draw the
boundary from `getrandom` (already a dependency) instead of `fastrand`.

Pinned by `a_body_containing_the_boundary_forges_a_part_delimiter`.

### B10 - Dot-stuffing is CRLF-only, so a bare-LF body can smuggle SMTP commands

`crates/smtp/src/transport/smtp/client/mod.rs:84-107`

`ClientCodec` transitions to `StartOfNewLine` only on the `\r\n` pair. A bare
`\n` while in `MiddleOfLine` falls into the `_ => {}` arm and the state is
unchanged, so a `.` following a bare LF is *not* doubled.

Path to failure:

1. `MessageBuilder::body` normalizes line endings to CRLF only for `String`
   bodies (`body.rs:196-201`: `MaybeString::Binary(_) => {}`). A `Vec<u8>` body,
   or `Body::dangerous_pre_encoded`, keeps bare LFs.
2. A short all-ASCII `Vec<u8>` body is classified `7bit` by
   `Encoding::choose` and passes through the encoder unchanged.
3. `codec.encode(b"body\n.\nMAIL FROM:<attacker@example.com>\r\n", ..)` emits
   those bytes verbatim.
4. A relay that accepts a bare LF as a line terminator (the permissive mode
   behind the 2023 "SMTP smuggling" class of bugs) sees a lone `.` line, ends
   DATA there, and parses the rest of the body as SMTP commands - injecting a
   second, attacker-controlled message into the session.

Proposed fix: reject or normalize bare LF in bodies before they reach the codec
(the cheapest correct place is `Body::new` for the `Binary` arm and
`dangerous_pre_encoded`'s documented contract), or - more defensively - treat a
bare `\n` as a line terminator in `ClientCodec` so the `.` is stuffed. Note the
second option changes transmitted bytes for existing bare-LF bodies, so it needs
a deliberate decision.

Pinned by `codec_does_not_treat_a_bare_lf_as_a_line_break`.

### B11 - `ClientId::Domain` is written into EHLO/LHLO without any validation

`crates/smtp/src/transport/smtp/commands.rs:32-36, 52-56`;
`extension.rs:52-60`

`Display for ClientId` writes `Domain(String)` straight into
`EHLO {}\r\n`. `Vrfy`/`Expn` route their argument through
`validate_single_line_argument` (`commands.rs:15-23`); the greeting commands do
not. A `hello_name` containing CRLF - e.g. from `SmtpTransport::from_url`, whose
EHLO name comes from the URL *path* (`connection_url.rs`), or from a
config-supplied hostname - emits a second command line before any reply is read,
desynchronizing the reply stream and letting the caller drive arbitrary SMTP
commands.

Proposed fix: validate in `ClientId::Domain` construction (make the variant
non-constructible and add a fallible `ClientId::domain(&str)`), or at minimum
reuse `validate_single_line_argument` in `Ehlo::new`/`Lhlo::new` and return
`Result`.

Pinned by `ehlo_and_lhlo_do_not_validate_the_client_id`.

### B12 - `HOLDUNTIL` datetime is never validated and is interpolated verbatim into MAIL FROM

`crates/smtp/src/transport/smtp/extension.rs:521-539, 795-804`

```rust
pub(crate) fn validate_syntax(&self) -> Result<(), Error> {
    match self {
        MailParameter::EnvelopeId(value) => validate_envelope_id(value),
        MailParameter::Other { keyword, .. } => validate_esmtp_keyword(keyword),
        MailParameter::OtherRaw { .. } => ...,
        _ => Ok(()),          // <- FutureRelease lands here
    }
}
```

`FutureReleaseParameter::HoldUntil(String)` is emitted as
`HOLDUNTIL={datetime}` with no escaping, so
`SendOptions::hold_until("20260519T120000Z\r\nRSET")` writes two command lines.
Every other free-form parameter value on this path is either xtext-encoded
(`ENVID`, `ORCPT`, `Other`) or printable-ASCII-validated (`OtherRaw`);
`FutureRelease` is the one hole.

The only in-tree caller is `crates/imap/src/account/submission.rs:306`, which
formats the datetime from a `SystemTime`, so this is not currently exploitable
through bifrost's own stack - but `SendOptions` is public API and this is the
kind of gap that only stays closed by accident.

Proposed fix: add a `FutureRelease` arm to `validate_syntax` requiring the RFC
3339 / RFC 4865 `date-time` shape, or at minimum printable ASCII with no
whitespace (reuse `validate_esmtp_raw_value`).

Pinned by `hold_until_datetime_is_not_syntax_validated`.

### B13 - The response reader buffers an unbounded line before enforcing the line cap

`crates/smtp/src/transport/smtp/client/connection.rs:1704-1743`
(and the identical async code at `async_connection.rs:1867-1923`)

```rust
while self.stream.read_line(&mut buffer).map_err(error::network)? > 0 {
    if buffer.len() - pre > MAX_RESPONSE_LINE_BYTES {
        return Err(error::parse("SMTP response line too long"));
    }
```

`BufRead::read_line` grows the `String` until it sees `\n` or EOF. A hostile or
broken server that sends bytes without a newline makes the client allocate
without limit; the 1000-byte cap is only consulted *after* the whole line is in
memory. `MAX_RESPONSE_BYTES` has the same shape (it is a post-hoc check on the
accumulated buffer, which is fine since each line is already bounded - once the
line bound is actually enforced).

Proposed fix: read through a bounded adaptor -
`(&mut self.stream).take(MAX_RESPONSE_LINE_BYTES as u64 + 1)` with
`read_until(b'\n', ..)` plus an explicit UTF-8 conversion - and treat "cap hit
without a newline" as the parse error.

### B14 - A non-UTF-8 byte in an SMTP reply is classified as a network error, so it looks retryable

`crates/smtp/src/transport/smtp/client/connection.rs:1711`

`read_line` on a `String` returns `io::ErrorKind::InvalidData` for invalid
UTF-8. That goes through `error::network` (`error.rs:443`), which maps only
`TimedOut`/`WouldBlock` to `Timeout` and everything else to `ErrorKind::Network`.
`account_error.rs` treats `Network` as a transport-class failure, i.e.
retryable.

A relay that emits Latin-1 text in a reply string (a real thing: non-ASCII
mailbox names in a 550 message from a server that has not adopted RFC 6531)
therefore produces an infinitely retryable error against a server that will send
the same bytes on every attempt. It should be `ErrorKind::Parse` - a terminal,
protocol-class failure.

Proposed fix: read bytes (`read_until`) and convert with
`String::from_utf8(..).map_err(error::parse)`, or special-case
`io::ErrorKind::InvalidData` in `error::network`.

### B15 - A bare `250\r\n` reply (legal per RFC 5321) is rejected as a parse error

`crates/smtp/src/transport/smtp/response.rs:350-380`

```rust
let (i, (last_code, last_line)) = (parse_code, preceded(tag(" "), take_until("\r\n"))).parse(i)?;
```

RFC 5321 4.2 gives the final reply line as `Reply-code [ SP textstring ] CRLF` -
the text, and with it the space, is optional. The parser requires `tag(" ")`, so
a server that answers `250\r\n` (or `220\r\n` as a greeting) makes bifrost fail
with `ErrorKind::Parse` and abort the connection before it can send anything.

Low field impact (mainstream MTAs always send text) but it is a spec deviation
in a parser that has to tolerate whatever the peer sends.

Proposed fix: `preceded(tag(" "), take_until("\r\n"))` ->
`alt((preceded(tag(" "), take_until("\r\n")), success("")))` guarded so that the
CRLF still has to follow.

Pinned by `parse_response_rejects_reply_without_a_space_separator`.

### B16 - Batch RCPT parameter validation errors are silently swallowed

`crates/smtp/src/transport/smtp/client/connection.rs:506, 615-617, 810`

```rust
let rcpt_options = self.rcpt_options_single(&addr, options).unwrap_or_default();
```

All three batch paths (`send_smtp_batch`, `send_smtp_batch_pipelined`,
`send_lmtp_batch`) discard the `Err` from `rcpt_options_single` and send the
RCPT with *no* parameters. The non-batch paths (`send_with_options` ->
`rcpt_options`) return the error to the caller.

Path to failure: a caller passes `SendOptions::new().never_notify()` to
`send_raw_batch_with_options` against a relay that does not advertise `DSN`.
`validate_rcpt_parameter` returns `InvalidInput`, `unwrap_or_default()` throws
it away, and the message is delivered *with* delivery-status notifications the
caller explicitly asked to suppress - reported back as a fully successful batch.
The same call through `send_raw_with_options` errors out. Silent policy
downgrade is worse than either outcome.

Proposed fix: propagate the error as a batch-level
`Err((error.with_attempt(Unsent).with_phase(RcptTo), progress))` before any wire
write, matching how `mail_options_for_batch` failures are handled two statements
earlier.

### B17 - Pipelined batch writes the whole command group before reading any reply

`crates/smtp/src/transport/smtp/client/connection.rs:609-631` (and
`send_pipelined` at `:228-274`)

The entire `MAIL FROM` + N x `RCPT TO` + `DATA` group is built into one `String`
and pushed through a single `write_all` before the first read. RFC 2920 3.1 is
explicit that a client "SHOULD ... check the TCP window" and must not send more
than the receiver can buffer without reading replies.

With a large recipient list (a mailing-list fan-out through
`send_raw_batch_with_options`) the client blocks in `write_all` while the server
blocks writing replies into a full socket buffer: a classic pipelining deadlock.
It is bounded by the write timeout (default 10 s) rather than hanging forever, so
the symptom is a slow, confusing timeout on exactly the sends that matter most.

No test: proving it needs a real socket with a controlled buffer size, and the
"never write a test that proves a hang by hanging" rule applies.

Proposed fix: chunk the pipelined group (e.g. 50 RCPTs) and drain the replies for
each chunk before writing the next, keeping the same `SendProgress` indexing.

---

## Deviations and gaps (correct-enough, but worth a decision)

### D1 - Relaxed header canonicalization does not delete WSP *before* the colon

`crates/smtp/src/message/dkim.rs:274-284`

RFC 6376 3.4.2 step 3: "Delete any WSP characters remaining before and after the
colon separating the header field name from the header field value."
`dkim_canonicalize_headers_relaxed`'s `name()` splits at the first `:` and only
`skip_whitespace`es the value side, so an input `B : Y` canonicalizes to `b :Y`
instead of `b:Y`.

Not reachable through this crate's own types: `HeaderName::new_from_ascii`
rejects names containing a space (`header/mod.rs:184`). It makes the function
wrong for any externally-supplied header block, which matters if it is ever
reused for *verification* rather than signing.

Pinned by `relaxed_header_canonicalization_matches_the_rfc_6376_example`, which
also carries the RFC 6376 3.4.5 known-answer vector for the reachable case.

### D2 - `dkim_canonicalize_headers_relaxed` is character-recursive and can overflow the stack

`crates/smtp/src/message/dkim.rs:264-317`

`value()` recurses once per input character and `name()`/`value()` are mutually
recursive. Rust does not guarantee tail-call elimination; in a debug build (which
is how `brokkr check` runs the suite) each character is a stack frame.

Signing a message whose covered headers total a few hundred kilobytes - a long
`References` chain accumulated from a reply thread is the realistic vector -
overflows the stack and aborts the process. I did not write a test for this: a
stack overflow aborts rather than fails, which is exactly the "do not prove a
hang by hanging" trap in a different costume.

Proposed fix: rewrite as a byte loop with an explicit two-state machine
(in-name / in-value). It is a ~30-line change and removes the whole class.

### D3 - DKIM does not support the `l=` body-length tag

`crates/smtp/src/message/dkim.rs:215-228`

`dkim_header_format` emits `v= a= d= s= c= q= t= h= bh= b=` and never `l=`.
This is the *correct* choice - `l=` enables the classic DKIM content-append
attack - but it is worth stating as deliberate in `reference/smtp.md` so nobody
"fixes" it later. No `l=` parsing exists either, which is fine for a
signing-only implementation.

### D4 - `SIZE 0` is treated as a zero-byte maximum

`crates/smtp/src/transport/smtp/extension.rs:221-225`,
`connection.rs:979-995`

RFC 1870 3: a server advertising `SIZE 0` declares that it has *no* fixed
maximum. Bifrost stores `Size(Some(0))` and
`size_limit().is_some_and(|limit| email.len() > limit)` then rejects every
non-empty message client-side, before MAIL FROM, with
"Message is larger than the server-advertised SIZE limit".

Proposed fix: treat `SIZE 0` as `Size(None)` at parse time.

Pinned (current behavior) by `size_zero_means_no_declared_maximum`.

### D5 - An unparsable `SIZE` value silently disables the ceiling check

`crates/smtp/src/transport/smtp/extension.rs:221-225`

`split.next().and_then(|limit| limit.parse().ok())` turns `SIZE not-a-number`
and `SIZE 99999999999999999999` (overflows `usize`) into `Size(None)`, so the
client-side pre-check is skipped and the server rejects the message after the
body is uploaded. Contrast `FUTURERELEASE`, whose malformed value is a hard
parse error. Inconsistent, and the silent direction is the wasteful one.

Pinned by `size_with_an_unparsable_limit_disables_the_client_side_check`.

### D6 - `ServerInfo::from_response` fails outright if the greeting's first line has no word

`crates/smtp/src/transport/smtp/extension.rs:200-204`

The server name is `response.first_word()`, and a `250-\r\n250 HELP\r\n` reply
(legal: the continuation text is optional) yields `None` -> "Could not read
server name" -> connection aborted. The name is only used for the `Display`
impl.

Proposed fix: default to an empty name instead of erroring.

Pinned by `parse_response_accepts_empty_continuation_lines` and
`server_name_comes_from_the_first_word_of_the_first_line`.

### D7 - The legacy `AUTH=MECH` advertisement form is ignored

`crates/smtp/src/transport/smtp/extension.rs:279-311`

Servers of a certain vintage advertise both `250-AUTH LOGIN PLAIN` and
`250-AUTH=LOGIN PLAIN`. Only the space form is parsed. Since the `=` form is
always accompanied by the space form in practice, this is fine - documenting it
so a future reader does not mistake it for an oversight.

Pinned by `legacy_auth_equals_form_is_not_recognized`.

### D8 - `Message::formatted()` output is not byte-identical to what is delivered

`crates/smtp/src/message/mod.rs:621-634`, `connection.rs:1618-1634`

`message_iter`/`write_body` always terminate with `\r\n.\r\n`, even when the
message already ends with CRLF, so every delivered message gains one trailing
empty line relative to `Message::formatted()`. `Message::body_raw()` compensates
for DKIM by appending the same CRLF - the two are consistent, and that
consistency is load-bearing for B1 - but a caller comparing `formatted()`
against what an IMAP APPEND stored will find a one-line difference.

Worth a sentence in `reference/smtp.md`; changing the terminator to conditional
`.\r\n` would be more correct but changes bytes on the wire for every message.

### D9 - `SIZE=` under-declares the transmitted octet count

`crates/smtp/src/transport/smtp/client/connection.rs:994`

`MailParameter::Size(email.len())` uses the pre-dot-stuffing length. The actual
DATA payload is larger by one byte per stuffed dot plus the terminator. RFC 1870
calls SIZE an estimate, so this is legal, but a message sitting exactly on the
server's limit with many dot-stuffed lines will be accepted at MAIL FROM and
rejected after the upload.

---

## Efficiency and API notes

### E1 - `Response::has_code` allocates two `String`s per call

`crates/smtp/src/transport/smtp/response.rs:212-214`

```rust
pub fn has_code(&self, code: u16) -> bool {
    self.code.to_string() == code.to_string()
}
```

`From<Code> for u16` already exists two screens up. `u16::from(self.code) == code`
is allocation-free and identical in behavior for the 3-digit domain. `has_code`
runs on every AUTH continuation (`auth_legacy`'s `response.has_code(334)` loop)
and on every SCRAM step.

### E2 - Every SMTP command allocates a `String`

`connection.rs:1664-1672`: `self.write(command.to_string().as_bytes())`. The
`Display` impls could write into a reusable `String` held on the connection.
Small, but it is one allocation per command on the hot send path, and the
pipelined builder already demonstrates the pattern.

### E3 - The pool spawns a background thread even when `min_idle == 0`

`crates/smtp/src/transport/smtp/pool/sync_impl.rs:33-136`

`Pool::new` unconditionally spawns `bifrost-smtp-connection-pool`, which then
wakes every `idle_timeout` to do nothing when there are no parked connections and
`min_idle` is 0 (the documented default). `reference/smtp.md` says "min_idle
defaults to 0 (no background DNS / reconnect)", which reads as "no background
thread". One idle thread per transport per account adds up.

Proposed fix: spawn lazily on the first `recycle`, or skip the spawn entirely
when `min_idle == 0` and drive expiry from `connection()`.

### E4 - Pooled checkout costs a full NOOP round trip

`pool/sync_impl.rs:166-182` calls `conn.test_connected()` on every reuse. That is
one extra RTT before every single send. It is a deliberate documented choice
(`reference/smtp.md`: "`test_connected()` runs before reuse and aborts on
failure"), and it is the only thing standing between a server-closed connection
and a mis-attributed send failure - but on a low-latency local relay it doubles
the round trips for a one-recipient message. Worth an opt-out on `PoolConfig` for
callers who would rather retry once on a broken pipe.

### E5 - `send_lmtp_with_options` returns `Vec<Response>` with no way to tell which index was rejected at RCPT

`connection.rs:298-369` preserves per-recipient order, which is the documented
contract, but the caller cannot distinguish "rejected at RCPT" from "rejected at
delivery" without re-deriving it from the status class. The batch API
(`send_raw_batch_with_options`) carries that distinction properly through
`SmtpCommandPhase`. Worth pointing the docs at the batch API for anything that
needs the phase.

### E6 - `Address::new_dangerous` and `Address::new_unchecked` are two names for one function

`crates/smtp/src/address/types.rs:105-146`

`new_dangerous` is `new_unchecked` verbatim, both public, both with the same
doc-comment. One of them should go.

---

## Untested behavior that matters (beyond what I added)

These are gaps I could not close without a test seam that does not exist yet.

- **No in-process transcript harness for the SMTP driver.** `NetworkStream` /
  `AsyncNetworkStream` are concrete enums over TCP/TLS/Unix with no generic
  parameter, so a `tokio::io::duplex` cannot be handed to `AsyncSmtpConnection`
  the way `crates/imap/src/connection/test_support.rs` does for IMAP. Every
  transport-level test in this crate consequently spawns a real
  `TcpListener` on `127.0.0.1:0` and a thread
  (`test_support.rs`, `connection.rs`'s test module, `transport.rs`'s test
  module). Adding a `Duplex(tokio::io::DuplexStream)` variant to
  `AsyncNetworkStream` behind `#[cfg(test)]` would let the whole
  pipelining/LMTP/batch matrix be exercised as byte transcripts with no
  listener, no port, and no thread. That is a source change I deliberately did
  not make unilaterally; it is the single highest-leverage follow-up in this
  crate.
- **Batch resolver drive-through.** `batch.rs`'s unit tests exercise
  `SendProgress` directly. The wiring from `send_smtp_batch` /
  `send_lmtp_batch` into it - which is where the lane assignments actually get
  made - is only covered indirectly. B16 is exactly the kind of bug that hides
  there.
- **LMTP final-status drain against a server that sends the wrong number of
  statuses.** `send_lmtp_batch` reads exactly one reply per `Accepted`
  recipient. A server sending one extra leaves it buffered, and the pooled
  connection's next `test_connected()` NOOP will consume that stale reply and
  see a positive code - so a desynchronized connection is silently returned to
  the pool. Needs the duplex harness above.
- **STARTTLS downgrade.** `Tls::Required` -> `conn.starttls(..)` -> `Err` is the
  security-critical path and has no test; `can_starttls()` gating for
  `Tls::Opportunistic` likewise.
- **`connection_url.rs`** has no tests of its own beyond the four assertions in
  `transport.rs`; the EHLO name comes from the URL path there (see B11).
- **Async transport** (`async_transport.rs`, 1138 lines) has no direct tests in
  the files I own beyond what `async_connection.rs`'s own module carries.

---

## What I did not get to, and why

- **`authentication.rs` (1291 lines)** - read only the parts `connection.rs`
  calls into (`password_mechanism_order`, `first_attemptable`, `oauth_mechanism`,
  `ScramExchange`). The SCRAM downgrade-protection ladder and the
  `-PLUS`-binding fallthrough are the highest-risk untested-by-me logic left in
  the crate. `reference/smtp.md` describes the intended rules precisely enough
  to audit against; I ran out of budget before doing it.
- **`account_error.rs` (1225 lines)** - read the FUTURERELEASE arms only
  (enough to confirm B5). The enhanced-status-code -> `AccountErrorKind` table
  is the other place a wrong mapping would be invisible.
- **`async_connection.rs` (2690 lines)** - read the response reader, the write
  path, and `validate_mail_parameter`. The `AsyncDeadline` / `TimeoutBudget`
  logic and the async batch paths were not compared line-by-line against their
  sync twins; B5 shows the two files *do* drift, so a full diff of the two is
  worth someone's afternoon.
- **`client/tls.rs`, `net.rs`, `async_net.rs`** - not reviewed.
- **`message/mailbox/parsers/`** (RFC 2822 / 5336 address parsing) - not
  reviewed; the task framing said `message/` is densely covered already and the
  transport is where the interesting failures live.
- **B4 (DKIM simple-canonicalization fold mismatch)** is stated as unconfirmed
  on purpose: confirming it needs one compile-and-run, and guessing the
  direction in an assertion would have landed a broken test. It is the finding I
  would verify first.
