# bifrost-smtp / bifrost-sasl bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/smtp/` and `crates/sasl/`, hunted
together because sasl exists to serve the auth paths.

All hunt findings are resolved and landed, and the arc has had its close pass
(2026-08-22). No gaps remain.

The one residual the rounds could not close - the connection-level
`plus_candidate` channel-binding gate being unpinnable in a hermetic suite -
was closed by the close pass: `AsyncNetworkStream` gained a test-only
peer-certificate DER injection seam, and three transcript tests in
`async_connection.rs` now pin the gate in both directions (PLUS answered with
SCRAM-PLUS when a usable certificate exists; a present-but-unusable
certificate is a hard error with nothing written; an absent certificate falls
through to PLAIN). Neutering the gate to a constant `false` now fails the
suite with the exact downgrade the gate exists to prevent (`AUTH PLAIN`
answered to a PLUS-advertising server).

Cross-crate and release-note items from this arc live in `TODO.md`
(`account-error` feature gates nothing; blocking transport API removal; DATA
byte change), not here.
