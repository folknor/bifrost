# Bug hunt: bifrost-smtp

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/smtp/` - both connection drivers, transports, pool, batch, auth, error
model, metering, message builder, DKIM, parsers, plus `reference/smtp.md` and
the test harness.

Hunter's verification note: the high-severity finding below was verified by
re-reading both drivers side by side - the sync batch pipelined driver really
does call the `manage_state = true` reader (`read_response_accepting_status`,
connection.rs lines 709 and 732) while the async twin calls
`read_response_with_budget_inner(_, true, false)`, and the surplus-bytes check
inside the `manage_state` reader errors whenever the `BufReader` still holds
bytes after one reply.

## Confident defects

### 1. (HIGH) Sync `send_smtp_batch_pipelined` misfires the "unsolicited reply" check on normally-coalesced pipelined replies - the whole batch fails against a real PIPELINING relay

`crates/smtp/src/transport/smtp/client/connection.rs` lines ~709 and ~732: the
sync batch pipelined driver drains window replies with
`read_response_accepting_status()`, which is the `manage_state = true` reader.
That reader (line ~1976) returns `Err(parse("SMTP server sent an unsolicited
reply"))` whenever the `BufReader` buffer is non-empty after parsing one
reply. In a pipelined window the peer's replies for MAIL + all RCPTs almost
always arrive in one TCP segment, so after parsing the MAIL reply the buffer
still holds the RCPT replies - the first drain errors, the connection is
aborted, and `SmtpTransport::send_raw_batch_with_options` returns a
batch-level error (or all-Unsent lanes) for a perfectly healthy exchange. The
async twin does it correctly: `async_connection.rs` lines ~874/900 use
`read_response_with_budget_inner(budget, true, false)` (surplus check
deferred), plus `finish_reply_group()` at the end of each window; the sync
batch path has neither the `manage_state=false` reads, nor the
hold-Broken-across-the-window invariant, nor a `finish_reply_group`. This
contradicts two explicit reference claims ("Both sync and async drivers hold
the stream Broken from a successful window write until the complete reply
group has drained", "This shape is identical in the blocking and async
drivers"). The tests never catch it because the `Transcript` harness's default
one-line-per-read behavior empties the buffer between replies; the sync batch
pipelined tests never use `expect_coalesced`, which exists precisely to model
this. Fix: mirror the async shape (verify + set Broken after the window
write, `read_response_inner(true, false)` for the drain, `finish_reply_group()`
after) and add a sync batch test with `expect_coalesced` window replies. This
is exactly the crate's own documented recurring defect shape: a fix landed in
one half only.

### 2. Non-pipelined direct sends abort the connection on every routine negative reply

`send_with_options` / `send_bdat_with_options` (both drivers; e.g.
`connection.rs` 236-271, `async_connection.rs` 377-429) use `self.command(...)`,
whose reader converts any 4xx/5xx into `Err(status)`, and `try_smtp!` then
**aborts** the connection. So on a server that does not advertise PIPELINING,
a plain 550 recipient rejection - or even a rejected MAIL FROM, which per the
reference "opened no transaction so the connection stays reusable" - tears
down the pooled connection (`abort()` -> `Shutdown::Both`, marked Broken,
dropped at recycle). The pipelined path for the identical wire exchange sends
RSET and keeps the connection. This is a behavioral/pooling inconsistency,
wasteful (one reconnect per rejection), and diverges from the documented
reset_transaction contract. The batch paths handle this correctly with
`command_accepting_status`; the direct paths should too.

### 3. Direct (non-batch) LMTP body-upload failures are phase-tagged `LmtpFinalStatus`, not `DataBody`

`send_lmtp_with_options` / `send_lmtp_bdat_with_options` wrap the entire
`message_lmtp` / `message_lmtp_bdat` call (body write + terminator +
final-status drain) in one `try_smtp!(..., SmtpCommandPhase::LmtpFinalStatus)`.
A transport failure during the body upload is therefore misattributed to the
final-status phase. The batch path distinguishes `DataBody` from
`LmtpFinalStatus` correctly. Consequence: any future per-phase classifier
refinement (which the code explicitly anticipates) gets wrong evidence on the
direct LMTP path.

## Contract / documentation mismatches

### 4. The reference claims the socket-listener tests were retired; they were not

`reference/smtp.md` "Connection test harness": "it replaced the
socket-listener tests, which were neither hermetic nor deterministic." But
`test_support.rs` still contains `spawn_lmtp_delivery_server` /
`spawn_unix_lmtp_delivery_server` (real `TcpListener`/`UnixListener` + threads
+ 2-second read timeouts), used by transport tests in `transport.rs` and
`async_transport.rs`, and `async_net.rs` has a `TcpListener`-based
TLS-deadline test with a `thread::sleep(250ms)`. These also sit on the wrong
side of AGENTS.md's testing rule ("Out of scope, still: real sockets or
listeners... wall-clock sleeps"). Either the tests should move to the
transcript harness or the doc claim should be corrected.

### 5. `client/mod.rs` module doc example is rotted and cannot compile

It calls `SmtpConnection::connect` (which is `#[cfg(test)]`), uses `client::`
paths (the module is private, so the doctest never runs - which is the only
reason it doesn't fail CI), and ends with `client.command(Quit)` - there is no
`Quit` command any more ("the Quit command builder is gone" per the reference;
`commands.rs` confirms). The reference's module-layout section also still
lists "QUIT" among `commands.rs` contents. Dead documentation that will
mislead the next reader.

### 6. Async bandwidth throttling: outbound debt delays the next read, contradicting the stated invariant

`async_net.rs`: `throttle` is a single `Option<Pin<Box<Sleep>>>` slot polled
at the top of **both** `poll_read` and `poll_write`. The reference insists
"Inbound and outbound are separate buckets: a large send must not throttle the
reply to it" - the buckets are separate, but the parked outbound debt from the
final DATA-body write gates the subsequent read of the server's reply. Under a
tight cap, reply reads (and thereby the per-operation read timeout budget) are
consumed by outbound debt. If the invariant is meant literally, the stream
needs two debt slots (or read-side polling should only wait inbound debt). The
blocking half has the same wall-clock effect (sleeps inside `write`), so at
minimum the reference's claim overstates what the design delivers.

## Suspected / minor defects

### 7. Per-line timeout re-arming on multi-line replies

`read_response_with_budget_inner` applies the full per-operation timeout to
*each* line read. A malicious or pathological peer can trickle one line per
timeout period; the total is bounded only by `MAX_RESPONSE_BYTES / line`
iterations (~100 lines of 1000 bytes -> 100x the configured timeout). Low
severity (caps bound it), but a per-reply deadline would match user
expectations of "timeout".

### 8. `into_message_body` computes the normalization decision on pre-normalized bytes

`body.rs` `into_message_body`: `effective = body.encoding(false)` is evaluated
before CRLF normalization, then `Body::new` re-chooses the encoding after
normalization. A byte body sitting exactly at an encoding boundary could be
CRLF-rewritten (`effective` = 8bit) and then end up base64-encoded (final
choice), mutating what should have been opaque payload. Extremely narrow
window; flagged as a suspicion, not a confirmed repro.

### 9. `in_place_crlf_line_endings` is O(n*m)

One `String::insert` (which shifts the tail) per bare LF; a multi-megabyte
LF-only body pays quadratic cost. The byte-path sibling `in_place_crlf_bytes`
already does the single-pass rebuild; the String path should too.

### 10. `ServerInfo::from_response` inconsistent strictness

An unparseable `SIZE` or `FUTURERELEASE` limit is a hard `Parse` error
(documented), but an unparseable `DELIVERBY` minimum is silently swallowed
(`minimum.parse().ok()`), storing `DeliverBy(None)` - a silently dropped
floor, the exact shape the SIZE rule was written to prevent.

### 11. Boundary re-roll can drop foreign Content-Type parameters

`MultiPart::ensure_boundary_absent` rebuilds the Content-Type from
`MultiPartKind` alone; any non-standard parameter a caller put on the
multipart Content-Type (e.g. `charset`, vendor params) is lost when a body
forces a boundary re-roll.

### 12. `connection_url` ignores a username without a password

`smtp://user@host` yields no credentials at all (the block is keyed on
`password()`), and the EHLO-name path segment is not percent-decoded. Minor
URL-parsing asymmetries.

### 13. `Headers` cannot represent repeated header fields

`insert_raw` overwrites by name, so a second `Received`, `Comments`, or
repeated trace header is silently dropped (the DKIM test even pins "last write
wins"). Fine for submission of freshly-built mail, but a structural limitation
worth stating in the reference for anyone routing pre-existing messages
through `Message`.

## Lateral findings

- **Stale litter inside the crate:**
  `crates/smtp/target/sendmail-tests/asyncstd-failure-*` - leftovers from
  lettre's deleted sendmail/async-std machinery - and `target/t` created by
  `spawn_unix_lmtp_delivery_server` when `CARGO_TARGET_TMPDIR` is unset.
  Untracked junk; delete, and point the unix-socket helper at the workspace
  target dir.
- **`ClientCodec` treats a bare CR as a line break in progress:** after `\r` +
  non-LF the state goes to `MiddleOfLine`, so `...\r.` is never stuffed.
  Bare-LF is defended (good); bare-CR smuggling would require a relay that
  treats lone CR as EOL - theoretical, but the asymmetry is undocumented.
- **`dkim_sign_fixed_time` panics on a pre-epoch clock**
  (`duration_since(UNIX_EPOCH).unwrap()`).
- **`Pool::idle_count_for_test` (async) is needlessly `async`** - cosmetic.

## Posture / structural note (owner proposal, not a defect)

The most valuable structural move suggested by finding 1 is the one the crate
already half-believes in: the sync and async drivers are ~3300/3900-line
near-clones "held in step deliberately," and the batch pipelined path proves
the mirroring discipline fails silently. Given pre-1.0 freedom, factoring the
*protocol state machine* (command sequencing, reply-group accounting, phase
decoration, `SendProgress` transitions) into one sans-I/O core driven by both
a blocking and an async I/O adapter would eliminate the entire class of
one-half-only defects - three of which (1, 2's asymmetry with the batch path,
3) this hunt found. That is a rewrite proposal for the owner, not a defect;
the published blocking surface itself stays untouched.

Key files: `crates/smtp/src/transport/smtp/client/connection.rs`,
`client/async_connection.rs`, `client/async_net.rs`, `client/mod.rs`,
`src/message/body.rs`, `reference/smtp.md`.
