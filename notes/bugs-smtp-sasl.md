# bifrost-smtp / bifrost-sasl bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/smtp/` and `crates/sasl/`, hunted
together because sasl exists to serve the auth paths. Read-only review; no build or test was run.
Findings are unverified work material. Line numbers are as of the hunt and will drift.

All hunt findings are resolved and landed. What follows is the one gap the arc could not close,
recorded rather than dropped so the document is not falsely empty.

## Remaining: the connection-level channel-binding gate is unpinnable in a hermetic suite

`AsyncSmtpConnection::auth_password` decides whether to resolve a channel binding at all by asking
whether any *allowed* PLUS mechanism is also *advertised* (`plus_candidate`), then passes
`binding.is_some()` to `password_mechanism`. If that gate were ever wrong in the `false` direction,
a PLUS-advertising server would be answered with PLAIN - the exact silent downgrade this arc spent
four rounds refusing - and nothing would notice.

Nothing tests it. Neutering `plus_candidate` to a constant `false` leaves the whole crate suite
green. This is not a regression from the round-4 collapse: the previous per-mechanism `HashMap` loop
was gated the same way and was equally untested. The cause is structural. The transcript harness is
an in-memory duplex with no TLS, so no test in this crate can produce a peer certificate, and
`peer_certificate_der()` is unconditionally `None` under test. Per the project testing rules a real
socket or a live certificate is out of scope here.

What *is* pinned: the pure seam `resolve_scram_binding` (absent certificate -> `Ok(None)` skip;
present-but-unusable -> hard error, never a fall-through) by
`scram_binding_only_treats_absent_certificate_as_unavailable`, and the whole selection function
including RFC 5802 Section 6 by `password_mechanism_selection`,
`scram_binding_skip_falls_through` and
`unbound_scram_is_never_selected_when_its_plus_variant_is_advertised`. The untested residue is
exactly the few lines of connection plumbing between the two.

Closing it properly needs a seam that lets a test inject the peer-certificate DER into an
`AsyncSmtpConnection` built from a transcript - i.e. making `peer_certificate_der` overridable in
test builds. That is a real, small, hermetic change; it was left out of round 4 because the round
was scoped to three named findings and inventing a new test seam was not one of them.
