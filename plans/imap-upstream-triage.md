# IMAP upstream triage

Current tracker for unresolved or deferred IMAP work after the upstream triage
pass.

Last verified: 2026-05-20.

## Remaining work

### SCRAM channel binding

Sources:

- `chatmail/async-imap` #99
- `chatmail/async-imap` #100

`SCRAM-SHA-*-PLUS` mechanisms should stay unavailable until the transport layer
exposes TLS channel-binding material. Treat this as a shared SASL/TLS design
item rather than a narrow IMAP-only authentication change.

Open design points:

- Which TLS backends can expose the channel-binding data needed by SCRAM-PLUS.
- Whether channel binding belongs in a reusable SASL layer shared with SMTP.
- How callers should inspect why a `*-PLUS` mechanism was advertised but not
  selected.

### Docs, examples, CI, and maintenance

Sources:

- `chatmail/async-imap` #119, #84, #113, #40, #43, #10, #8
- `djc/tokio-imap` #2, #24, #31

These are not current implementation blockers. Revisit them once the public IMAP
API shape is stable enough that examples and CI policy will not immediately
churn.

## Not currently tracked

No concrete IMAP4rev2 or UIDPLUS implementation gap is tracked here. Add a new
entry only when a fresh audit finds a specific missing behavior.
