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

### Large-file cleanup

Current Rust files above 800 lines:

```text
13930  crates/imap/src/codec/decode/tests.rs
 7471  crates/imap/src/codec/encode/tests.rs
 1547  crates/imap/src/types/response_tests.rs
 1093  crates/imap/src/error_tests.rs
  855  crates/imap/src/types/command_tests.rs
  851  crates/imap/src/types/search_tests.rs
  803  crates/imap/src/codec/encode/dispatch.rs
```

Line count is a smell, not a quota. Split only when the new module has a clear
protocol or responsibility boundary.

Remaining treatment:

- `codec/encode/dispatch.rs` is a cohesive `Command` dispatcher and is only
  barely over the threshold. Leave it together unless new command families make
  a real dispatch substructure visible.
- Test files should be split only along obvious protocol-family or fixture-suite
  boundaries. The large parser/encoder fixture files may remain large if a split
  would hide fixture flow or make individual cases harder to audit.
- `types/response_tests.rs`, `error_tests.rs`, `types/command_tests.rs`, and
  `types/search_tests.rs` are test-only follow-ups. Revisit when modifying those
  areas, not as a standalone churn task.

## Not currently tracked

No concrete IMAP4rev2 or UIDPLUS implementation gap is tracked here. Add a new
entry only when a fresh audit finds a specific missing behavior.
