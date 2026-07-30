# bifrost-smtp bug hunt

Current gaps after the first repair pass. Resolved findings are intentionally
removed rather than retained as history.

## Open bugs

### B19 - A surplus LMTP final reply silently desynchronizes the connection

A peer that emits more post-DATA final replies than it accepted recipients
leaves the extra replies queued. `send_lmtp` returns the expected number of
statuses, does not mark the connection broken, and the pooled connection is
handed back for reuse - so the next command reads a stale reply as its own
response, off by one forever.

`lmtp_surplus_final_status_desynchronizes_the_next_command` pins the current
behavior: the transcript harness refuses the following write while replies are
still pending, which is exactly the desync a real socket would hide.

Proposed fix: after draining the expected finals, treat any residual buffered
input as a protocol violation - mark the connection `Broken` so the pool
discards it rather than recycling a desynchronized stream. Symmetric with the
too-few case, which already breaks the connection.

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

- Batch resolver unit tests do not drive the complete sync and async send
  paths. The repaired RCPT-option validation needs this harness for broader
  sequencing coverage.
- Envelope-phase transport failures now split into two evidence classes, and
  only one of them is settled. A failed *write* of a later PIPELINING recipient
  window is `Unsent` (B-round fix). A failed *read* while draining RCPT replies
  is still stamped `InFlight`, in both the pipelined and non-pipelined paths,
  even though `DATA` has not been issued there either and no content can have
  reached the peer. The existing comment in `connection.rs` treats "transport
  drop is `InFlight`" as a blanket rule; that rule is too coarse for the
  envelope phase. Deliberately out of scope for this round because it changes
  pre-existing non-pipelined behavior. Decide the phase-aware rule, then apply
  it to both drivers at once.
- The `Transcript` harness models a peer that answers or a peer that goes
  silent, but not a peer that half-answers a reply line, closes mid-response,
  or interleaves writes with pending replies (which a real full-duplex socket
  permits and the harness deliberately rejects). The write-while-pending
  refusal is a sequencing assertion, not a fidelity claim.
- TLS/network modules, mailbox parsers, and direct async transport tests remain
  outside this pass. `starttls` upgrade past the capability check is not
  covered: the transcript has no TLS handshake.
