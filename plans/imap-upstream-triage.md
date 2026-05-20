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
 3590  crates/imap/src/connection/dispatch.rs
 1868  crates/imap/src/connection/driver/mod.rs
 1834  crates/imap/src/codec/encode/commands.rs
 1833  crates/imap/src/codec/encode/mod.rs
 1547  crates/imap/src/types/response_tests.rs
 1336  crates/imap/src/connection/mod.rs
 1255  crates/imap/src/types/response.rs
 1095  crates/imap/src/codec/decode/response.rs
 1093  crates/imap/src/error_tests.rs
 1049  crates/imap/src/connection/helpers.rs
  855  crates/imap/src/types/command_tests.rs
  851  crates/imap/src/types/search_tests.rs
```

Natural split candidates:

- `connection/dispatch.rs`: split consumers by response family or command
  family.
- `connection/driver/mod.rs`: split pipeline execution, command execution, and
  stream upgrade helpers.
- `codec/encode/commands.rs` and `codec/encode/mod.rs`: split command-specific
  encoders from shared encoding context and literal handling.
- `connection/mod.rs`: split stream/compression internals from public connection
  handle types.
- `types/response.rs`: split capabilities and extension result payloads from
  core response enums.
- `codec/decode/response.rs`: split untagged response parsers by response
  family.
- Test files: split when there are clear protocol-family boundaries, but giant
  parser fixture files may be acceptable if splitting would hide fixture flow.

## Not currently tracked

No concrete IMAP4rev2 or UIDPLUS implementation gap is tracked here. Add a new
entry only when a fresh audit finds a specific missing behavior.
