# bifrost-jmap deferred work

Items consciously left in place. Each has a rationale and a revisit
trigger. This file is not a TODO list - it exists so the next person
who looks at these spots doesn't redo the analysis from scratch.

Unimplemented spec work lives in the per-RFC plans (`MDN.md`,
`SMIME.md`, `SHARING.md`). API redesign history lives in `API.md`.

## Accepted micro-optimizations

### `Session::capability_config()` serde round-trip

`core/session.rs:286` serializes the `Capabilities` enum variant to
`serde_json::Value` then deserializes into `C::Config`. Two serde
passes per access.

**Why accepted:** Session parsing happens once per connection.
Capability configs are accessed infrequently. The hand-written
accessors (`core_capabilities()`, `mail_capabilities()`, etc.) return
`&T` with zero overhead and are the primary path; `capability_config`
is a convenience bridge for generic code.

**Revisit when:** A consumer calls `capability_config` in a hot loop,
or the hand-written accessors get removed in favor of the generic
path (which would require storing raw JSON alongside the enum).

### `CallHandle.call_id` is a heap `String`

`core/request.rs:19` stores `call_id: String` always equal to a short
token like `"s0"` / `"s1"`. The same string is cloned into
`RawMethodCall`.

**Why accepted:** Two tiny allocations per method call, on the order
of ~100 bytes of heap per request. Dwarfed by JSON serialization and
HTTP transfer. Fixing means switching to `usize` internally and
formatting to string at serialization time, which adds complexity to
`CallHandle`, `ResultReference`, and the response lookup path.

**Revisit when:** Profiling shows per-request overhead dominates at
the sub-microsecond level.

### SSE `Bytes` -> `Vec<u8>` copy at the parser boundary

`transport_reqwest.rs:177` wraps reqwest's `bytes::Bytes` stream and
calls `.to_vec()` per chunk (`transport_reqwest.rs:192`). The SSE
parser in `event_source/parser.rs` consumes owned `Vec<u8>`.
`HttpTransport` returns `Bytes` directly elsewhere; only the SSE path
still copies.

**Why accepted:** True zero-copy SSE would require rewriting the
parser to use `Bytes` with an offset cursor. SSE chunks are short
state-change notifications, not bulk data.

**Revisit when:** SSE is used for high-volume event delivery, or the
parser is rewritten for other reasons.

## API cleanup still pending

### Sentinel getters on typed structs

`Mailbox::role()` returns `Option<&Role>` and `total_emails()` returns
`Option<usize>` (`mailbox/get.rs:22,30`). The redesign called for the
same shape across all typed-struct getters, but three sentinel cases
remain on `Email` / `EmailBodyPart`:

- `Email::size()` -> `usize`, `unwrap_or(0)` (`email/get.rs:53`).
- `Email::has_attachment()` -> `bool`, `unwrap_or(&false)`
  (`email/get.rs:153`).
- `EmailBodyPart::size()` -> `usize`, `unwrap_or(&0)`
  (`email/get.rs:188`).

Fixing this is one decision plus a mechanical sweep. The open
question is whether ratatoskr ever branches on omitted-vs-zero for
`size` / `has_attachment`; if it doesn't, the sentinels are
defensible and the API.md "Remaining gaps" entry can be closed.

**Revisit when:** A consumer hits a real bug from the
omitted-vs-zero collapse, or the next pre-1.0 ergonomics pass.

## See also

- `API.md` - archived ADR for the pre-1.0 surface redesign.
- `MDN.md`, `SMIME.md`, `SHARING.md` - per-RFC implementation plans.
