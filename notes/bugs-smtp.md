# Bug hunt: bifrost-smtp

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/smtp/` - both connection drivers, transports, pool, batch, auth, error
model, metering, message builder, DKIM, parsers, plus `reference/smtp.md` and
the test harness.

## Confident defects

(Finding 1, HIGH - the sync batch pipelined driver misfiring the unsolicited-
reply check on coalesced window replies - is fixed: the sync path now mirrors
the async shape (verify + Broken after the window write, deferred surplus
reads, `finish_reply_group` per window), pinned by
`pipelined_batch_drains_coalesced_window_replies` with revert-and-confirm.)

(Finding 2, MEDIUM - non-pipelined direct sends aborting the connection on
every routine negative reply - is fixed: both drivers now issue MAIL FROM /
RCPT TO / DATA through `command_accepting_status` via a shared
`run_unpipelined_envelope`, so a rejected MAIL FROM leaves the connection
untouched and a rejected RCPT or DATA sends RSET and keeps it, matching the
pipelined and batch paths; pinned in both halves by
`unpipelined_server_rejections_keep_the_connection_reusable` with
revert-and-confirm.)

## Contract / documentation mismatches

(Findings 4a and 4b - the reference claiming the socket-listener tests were
retired when they were not - are fixed together: `spawn_lmtp_delivery_server`,
`spawn_unix_lmtp_delivery_server` and `assert_lmtp_delivery_commands` are gone,
so `test_support` binds no listener, spawns no thread and sleeps nowhere. The
transport-level LMTP delivery tests now run a parked transcript connection
through `send_raw` in both halves - the transcript asserts the exact command
sequence the old command-capture asserted - and the Unix-socket case pins what
is provable in-process: builder routing plus the TLS-over-Unix refusal raised
in `connection()` before any dial. `tokio_tls_handshake_uses_deadline` drives
`upgrade_tls_stream` against a new `test_support::StalledPeer` instead of a
listener plus a 250ms `thread::sleep`. Both ports verified by
revert-and-confirm. The reference was corrected to match, including the note
that a few loopback-listener tests for the pre-AUTH refusal ladder remain
outside `test_support` - see the residual item below.)

(Finding 4c - the residual loopback-listener tests for the pre-AUTH refusal
ladder - is fixed. No test in `crates/smtp/` binds a socket, spawns a listener
thread or sleeps on the wall clock any more.

The post-greeting authentication stage was factored out of `connection()` into
`SmtpClient::authenticate_if_configured` / `AsyncSmtpClient::authenticate_if_configured`
(the sync one also de-duplicates the block that the TCP and Unix funnels each
carried a copy of). That stage is what the listener tests were really
exercising - the refusal is decided after the greeting and EHLO and has nothing
to do with how the socket was dialled - so it now runs against a transcript
connection in both halves.

`plaintext_auth_is_refused_before_auth_command` and its tokio twin script the
greeting and the `AUTH PLAIN`-advertising EHLO reply and NOTHING else: an AUTH
command reaching the wire fails the write outright, which is the same
observable the old listener's zero-length read gave. Ablating
`ensure_can_authenticate` makes both fail with exactly
"SMTP transcript exhausted by client write". The escape-hatch mirror is now
`dangerous_allow_insecure_auth_sends_auth_plain_on_a_plaintext_connection` (plus
tokio twin), which is strictly stronger than the listener version: the
transcript asserts the exact `AUTH PLAIN AHVzZXIAcGFzcw==` line and the
post-AUTH EHLO, where the old test only checked an `AUTH PLAIN ` prefix.

`tests/transport_smtp.rs` no longer has a `read_response_caps` module. Its
oversized-banner cap moved in-crate as
`an_oversized_greeting_line_is_a_parse_error_not_a_hang` in both connection
transcript suites, and the wall-clock 5s "did it return" timeout is gone -
a transcript read cannot hang there in the first place.

Lateral find while porting it: the ORIGINAL integration test did not bite. It
wrote 4096 bytes of `x` with no reply code, which fails to parse whether or not
`MAX_RESPONSE_LINE_BYTES` is enforced - ablating the cap left it green. The
ported test uses a well-formed but oversize greeting (`220 ` + 4096 `x`), which
parses cleanly once the cap is removed, so it fails under ablation. The cap had
been effectively unpinned since it was written.)

(Finding 6 - outbound throttle debt delaying the next read - is fixed on the
async side: `AsyncNetworkStream` now carries separate `throttle_in` and
`throttle_out` sleep slots, `poll_read` consults only inbound debt and
`poll_write` only outbound, so the debt parked by the final DATA-body write no
longer eats the read timeout budget for the reply. Pinned by
`outbound_throttle_debt_does_not_gate_the_next_read`, which polls the read once
while the write is still in debt, with revert-and-confirm. The blocking half
genuinely cannot deliver the invariant - `charge` sleeps the very thread that
will issue the next read, and overlapping the directions would need a second
thread or a non-blocking socket - so the reference now scopes the guarantee to
the async funnel and says so explicitly rather than overstating it.)

## Suspected / minor defects

(Finding 7 - per-line timeout re-arming on multi-line replies - is fixed in
both halves. The async reader collapses a `PerOperation` budget into an
`AsyncDeadline` on entry so every line draws from one reply-wide slack; a
`SetupDeadline` budget passes through unchanged. The blocking reader has only
`SO_RCVTIMEO`, which is per syscall and has no getter, so `SmtpConnection` now
keeps the configured value in `read_timeout`, re-arms the socket with the
remaining slack before each line, and restores the full value when the reply
ends - treating an exactly-spent deadline as an error rather than a zero
re-arm, since a zero `SO_RCVTIMEO` means "block forever". Pinned by
`a_trickled_multi_line_reply_cannot_outrun_the_operation_timeout` (a
clock-trickling peer under paused tokio time) and
`multi_line_reply_reads_share_one_deadline` (the transcript now records armed
read timeouts), both with revert-and-confirm.

Lateral find while fixing it: `AsyncDeadline` measured on `std::time::Instant`
while every timeout it arms is a `tokio::time::timeout`. Harmless in
production, where both track the system clock, but it means the deadline and
its own timers read different clocks - and under `start_paused` test time the
deadline never expired at all, which is why the setup deadline was effectively
untestable. Switched to `tokio::time::Instant`.)

(Finding 8 - `into_message_body` computing the normalization decision on
pre-normalized bytes - is closed as hardened, NOT as a live defect. The window
turns out to be unreachable: CRLF normalization only ever lengthens a line
TERMINATOR or splits a line in two, never lengthens a line, so the
post-normalization choice can never be stronger than the pre-normalization
one - and only a stronger choice would mean an already-rewritten buffer being
treated as opaque. That is now pinned
directly by `crlf_normalization_never_changes_the_chosen_encoding` over a
spread of byte bodies (bare CR, bare LF, mixed terminators, over-long lines,
high bytes, NUL, empty). The suspicion is also structurally removed:
`into_message_body` carries its ONE decision through to `Body::new_with_encoding`
instead of letting `Body::new` choose a second time, so a future change to
`MaybeString::encoding` cannot reintroduce the gap silently - it fails the
property test instead.)

(Finding 11 - boundary re-roll dropping foreign Content-Type parameters - is
fixed: `ensure_boundary_absent` now replaces the `boundary` parameter on the
existing Content-Type and keeps every other parameter verbatim, falling back to
the kind-rebuilt header only for a parameter value containing a `"` or a `\`,
which cannot be re-emitted losslessly as a quoted string. Pinned by
`a_boundary_re_roll_keeps_foreign_content_type_parameters`, which checks both
the no-collision and the re-roll path, with revert-and-confirm.)

(Finding 13 - `Headers` cannot represent repeated header fields - is closed as
a documentation item, which is what it asked for: `reference/smtp.md` now
states the limitation under "Message builder", says who it bites (a caller
routing pre-existing mail through `Message`, not a caller building fresh mail
for submission) and points at `send_raw` for that case. No code change; the
last-write-wins behaviour is intended for the crate's purpose.)

## Posture / structural note (owner proposal, not a defect)

(CLOSED. Ruled on and executed as ruling 7 in `notes/todo.md`: the send paths
of both drivers now run on one sans-I/O core, `client/core.rs`, and the two
halves are adapters that move bytes and apply deadlines. Command sequencing,
reply-group accounting, phase decoration, `SendProgress` transitions and the
RSET-and-keep versus abort decision exist once. The published surface is
unchanged and no test was lost. See "Protocol core and I/O adapters" in
`reference/smtp.md`.)

The original note: the most valuable structural move suggested by finding 1 is
the one the crate already half-believes in: the sync and async drivers are
~3300/3900-line near-clones "held in step deliberately," and the batch
pipelined path proves the mirroring discipline fails silently. Given pre-1.0
freedom, factoring the *protocol state machine* (command sequencing,
reply-group accounting, phase decoration, `SendProgress` transitions) into one
sans-I/O core driven by both a blocking and an async I/O adapter would
eliminate the entire class of one-half-only defects - three of which (1, 2's
asymmetry with the batch path, 3) this hunt found. That is a rewrite proposal
for the owner, not a defect; the published blocking surface itself stays
untouched.

Key files: `crates/smtp/src/transport/smtp/client/connection.rs`,
`client/async_connection.rs`, `client/async_net.rs`, `client/mod.rs`,
`src/message/body.rs`, `reference/smtp.md`.
