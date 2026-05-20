# SMTP review current gaps

No blockers are currently tracked for the SMTP changes.

## Wire-level

- Pipelined RCPT-failure path drops the session instead of sending `RSET` (`connection.rs:201-243`, `async_connection.rs:391-413`). On a RCPT 5xx after DATA is already in the batch, the server has accepted DATA and awaits the body; the transaction abort wanted here is `RSET`, not a connection drop.
- Pipelined test reads command-by-command via `read_line` (`connection.rs:990-1081`). It does not prove single-syscall batching; use `read_exact` on the full batch length to witness pipelining.
- No async-std cancel-safety test for the non-LMTP pipelined path.

## Extensions / params

- Only first enhanced status code is returned in multi-line responses (`response.rs:228-238`). Per-line codes can vary in multi-RCPT/DSN replies; document that behavior or expose per-line values.
- Recipient-specific parameters still match by exact `Address` equality (`extension.rs:677,724-732`). Case differences or alternate parsing paths can silently miss a configured recipient.
- `MailParameter::Other` always xtext-encodes the value; some extensions emit raw `key=value`. Consider an opt-out.

## Transport / API

- Unix LMTP connect has no timeout (`client/net.rs:141-144`). `UnixStream::connect` can block indefinitely on stale or abstract paths.
- `test_support::spawn_unix_lmtp_delivery_server` uses `CARGO_MANIFEST_DIR` for socket paths and only cleans up via `commands()`. Parallel test collisions and crash leftovers remain possible.

## Messages / headers

- `MultiPart::report(report_type: String)` (`mimebody.rs:368-371,217`) is still an open builder. It does not enforce RFC 6522's 2-or-3-part structure, and `report_type` needs token validation.
- List headers other than `List-Unsubscribe-Post` still have minimal validation (`textual.rs:42-78`): missing angle brackets, embedded commas in URLs, and illegal comments are accepted.

## Nits

- `Bdat::new(size, last)` uses two positional arguments that are easy to swap; consider `Bdat::chunk(size)` and `Bdat::last(size)`.
- LMTP does not pipeline RCPTs with DATA even though RFC 2033 section 4.2 allows it.
- `connect_unix_with_protocol` duplicates setup from `connect_with_protocol`.
- BDAT writes raw message data without verifying trailing CRLF; the next command can glue to the body if absent.
- `content_type.rs` flowed helpers use `Self::parse(...).expect(...)` rather than a const path.
- `executor.rs` Unix-socket TLS rejection leaves cfg-driven dead-code shape that could use a short comment.
