# bifrost-smtp bug hunt

Current gaps after the first repair pass. Resolved findings are intentionally
removed rather than retained as history.

## Open bugs

### B17 - Large PIPELINING groups can deadlock against a peer that stops reading commands

`crates/smtp/src/transport/smtp/client/connection.rs` and
`async_connection.rs` serialize `MAIL FROM`, every `RCPT TO`, and `DATA` into
one write before draining a reply. RFC 2920 section 3.1 says the client SHOULD
respect the TCP window. With a very large recipient list, the client can block
writing commands while the server blocks writing replies.

Assessment: this is a real robustness risk, but not a confirmed protocol
correctness bug. The RFC wording is SHOULD, and the configured write timeout
eventually breaks the stalemate. Keep it open as a throughput and reliability
improvement, not a security issue.

Proposed fix: pipeline bounded recipient chunks and drain the corresponding
replies before writing the next chunk, preserving `SendProgress` indexes.

### B18 - Bare-LF bodies gain a literal extra dot on CRLF-strict relays

`ClientCodec::encode` now treats a bare `LF` as a line break, so a body carrying
`\n.` is stuffed to `\n..`. That closes the command-injection hole against
relays that accept bare LF as a terminator, but a relay that de-stuffs only
after CRLF delivers the extra dot as content.

Assessment: the trade is correct as it stands - corrupting one line beats
letting an attacker-supplied body end DATA early. The real fix is upstream:
`MessageBuilder::body` normalizes line endings for `String` input but not for
`Vec<u8>`, so byte bodies reach the codec un-normalized in the first place.

Proposed fix: CRLF-normalize byte bodies at the builder boundary, or reject them
when they carry bare LF, and leave the codec's defensive stuffing in place.

## Deviations and deliberate choices

### D8 - `Message::formatted()` is not byte-identical to DATA delivery

The DATA writer always terminates with `\r\n.\r\n`, so delivered content gains
one trailing empty line relative to `Message::formatted()`. `body_raw()` does
the same before DKIM canonicalization, so signing and delivery agree, and
`smtp_data_size` counts the CRLF so the declared `SIZE` matches the wire.
Changing this is wire-compatible only after a dedicated API decision.

### D9 - A malformed `SIZE` limit fails the whole EHLO

`ServerInfo::from_response` returns `Parse` when the advertised SIZE value does
not parse as a `usize`, which makes the connection unusable rather than
degrading to no client-side ceiling. Deliberate: a silently dropped ceiling is
the failure mode that shipped a message the relay was always going to refuse.
The cost is that one sloppy MTA bricks the account instead of falling back on
the server's own 552.

### D10 - `MultiPartBuilder::boundary` and friends panic on invalid input

`boundary`, `MultiPart::encrypted`, and `MultiPart::signed` keep their infallible
signatures and now panic on values that would break out of the MIME parameter.
`try_boundary` / `try_encrypted` / `try_signed` are the fallible entry points.
This is a runtime behavior change for existing callers that passed unvalidated
strings.

### D3 - DKIM intentionally omits `l=`

DKIM signing does not emit the body-length tag because `l=` permits
content-append attacks. This is deliberate and should stay documented in the
SMTP reference.

## Efficiency and API follow-ups

### E2 - Command serialization allocates one `String` per command

`connection.rs` uses `command.to_string()` for individual commands. A reusable
command buffer would remove small allocations from the send path.

### E4 - Pooled checkout performs a NOOP round trip

The defensive NOOP probe avoids reusing a server-closed connection but costs an
extra RTT. Consider an explicit `PoolConfig` opt-out for callers willing to
retry one broken send.

### E5 - LMTP direct sends do not expose RCPT versus final-status phase

`send_lmtp_with_options` preserves recipient order but returns only
`Vec<Response>`. The batch API carries the phase through `SmtpCommandPhase` and
should be documented as the richer API.

### E6 - `Address::new_dangerous` and `Address::new_unchecked` duplicate one API

The public constructors have identical behavior and documentation. Deprecate
one name in the next API cleanup.

## Test seam and audit gaps

- The connection drivers still have no in-process transcript harness. Their
  existing socket-listener tests do not meet this repository's hermetic test
  policy. A test-only duplex stream variant should replace those tests and
  cover PIPELINING, LMTP final-status drain, and STARTTLS downgrade behavior.
- Batch resolver unit tests do not drive the complete sync and async send
  paths. The repaired RCPT-option validation needs this harness for broader
  sequencing coverage.
- LMTP final-status handling has not been exercised against a peer that sends
  too many or too few final responses.
- TLS/network modules, mailbox parsers, and direct async transport tests remain
  outside this pass.
