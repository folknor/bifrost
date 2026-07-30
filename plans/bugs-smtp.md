# bifrost-smtp bug hunt

Current gaps after the first repair pass. Resolved findings are intentionally
removed rather than retained as history.

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

### D12 - LMTP connections are retired after every final-status drain (B19)

B19 was a surplus post-DATA final reply desynchronizing a pooled LMTP
connection: the extra reply stayed queued and the next command read it as its
own response.

Detection is only partial by construction. A surplus reply that already reached
the driver's `BufReader` is provable and is now a hard failure: the stream is
marked `Broken` and `send_lmtp` / `message_lmtp_*` return
"more final statuses than accepted recipients". The batch drivers apply the same
check but keep the per-recipient outcomes they already resolved - the surplus
changes the stream's reusability, not the delivery result.

Bytes still below the buffer (socket receive queue, TLS record layer) cannot be
observed without a read that would block against a well-behaved peer, and
native-tls exposes no nonblocking peek that would make such a probe honest. The
first fix attempt papered over this with a test-only accessor into the
`Transcript` peer, which gave the harness visibility production does not have.
The decision instead is conservative retirement: every LMTP final-status drain
sets a retirement flag and the pool discards the connection at recycle rather
than parking it.

The trade is one reconnect per LMTP transaction. LMTP is local delivery, so the
reconnect is cheap relative to recycling a stream that may be off by one reply
forever. SMTP pooling is untouched.

### D11 - Envelope transport failures before DATA are `Unsent`

For both sync and async SMTP and LMTP batch drivers, a failed write or reply
drain from `MAIL FROM` through `RCPT TO` carries `Unsent` evidence: `DATA` has
not been issued, so no message content can have reached the peer. Existing
recipient rejections remain authoritative, and all still-open recipients move
through `SendProgress::mark_unresolved_unsent`. `DATA` and later retain their
`InFlight` / `Acknowledged` evidence semantics.

## Test seam and audit gaps

- Batch resolver unit tests do not drive the complete sync and async send
  paths. The repaired RCPT-option validation needs this harness for broader
  sequencing coverage.
- The `Transcript` harness models a peer that answers or a peer that goes
  silent, but not a peer that half-answers a reply line, closes mid-response,
  or interleaves writes with pending replies (which a real full-duplex socket
  permits and the harness deliberately rejects). The write-while-pending
  refusal is a sequencing assertion, not a fidelity claim. `expect_coalesced`
  now models one real segmentation shape (adjacent replies in one segment), but
  partial lines and mid-response close remain unmodelled.
- No test covers the pool's retirement of a drained LMTP connection end to end;
  retirement is pinned at the connection level (`should_retire()`) and the pool
  branch is a one-line predicate. A pooled-transport harness would close that
  gap.
- TLS/network modules, mailbox parsers, and direct async transport tests remain
  outside this pass. `starttls` upgrade past the capability check is not
  covered: the transcript has no TLS handshake.
