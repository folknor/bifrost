# TODO - jmap-client

Items below have been evaluated and consciously deferred. Each includes the rationale for deferral and the conditions under which it should be revisited.

---

## Optimization (accepted trade-offs)

### 1. `capability_config()` serde round-trip

`Session::capability_config::<C>()` at `core/session.rs` serializes the `Capabilities` enum variant to `serde_json::Value`, then deserializes it into `C::Config`. This is two serde passes to extract a typed config that's already stored in the enum.

**Why accepted:** Session parsing happens once per connection (at connect) and capability configs are accessed infrequently. The hand-written accessors (`core_capabilities()`, `mail_capabilities()`, etc.) return `&T` with zero overhead and are the primary path. `capability_config()` is a convenience bridge for generic code. The round-trip cost is ~microseconds on a structure with a handful of fields.

**Revisit when:** If `capability_config()` is called in a hot loop, or if the hand-written accessors are removed in favor of the generic path (which would require storing raw JSON alongside the enum for direct deserialization).

---

### 2. `default_account_id().to_string()` copied in ~90 helper methods

Every convenience helper method (e.g., `email_get`, `mailbox_create`, `calendar_event_query`) calls `request.default_account_id().to_string()` to pass the account ID to method struct constructors. This allocates one `String` per helper call.

**Why accepted:** The allocation is small (account IDs are typically 10-30 bytes) and happens once per JMAP operation - dwarfed by the HTTP request cost. Fixing would require changing all method constructors to accept `&str` and store `Cow<'_, str>`, or passing the account ID by reference through the entire builder chain. That's ~90 call sites of mechanical churn for a sub-microsecond improvement.

**Revisit when:** If profiling shows helper method overhead is significant relative to network I/O, or if the `AccountScope` type grows helper methods that can pass the account ID by reference internally.

---

### 3. `CallHandle.call_id` is heap-allocated `String`

`CallHandle` stores `call_id: String` which is always a short string like `"s0"`, `"s1"`. This allocates 2-3 bytes on the heap per method call (plus the `String` header overhead). The call_id is also cloned into `RawMethodCall`.

**Why accepted:** Two tiny allocations per method call. A typical JMAP request has 1-5 method calls. The total overhead is ~100 bytes of heap allocation per request - noise compared to the JSON serialization and HTTP transfer. Fixing would require using `usize` internally and formatting to string only during serialization, which adds complexity to `CallHandle`, `ResultReference`, and the response lookup path.

**Revisit when:** If the crate is used in an extremely high-throughput scenario where per-request overhead matters at the sub-microsecond level.

---

### 4. SSE stream copies `Bytes` to `Vec<u8>` at parser boundary

`SseTransport::ByteStream` yields `Vec<u8>` chunks. `ReqwestByteStream` converts reqwest's `bytes::Bytes` to `Vec<u8>` via `.to_vec()`, and the SSE parser (`event_source/parser.rs`) stores owned `Vec<u8>` internally. While `HttpTransport` now returns `Bytes` directly, the SSE path still copies because the parser iterates byte-by-byte over an owned buffer.

**Why accepted:** True zero-copy SSE would require rewriting the parser to use `Bytes` with an offset cursor instead of consuming `Vec<u8>`. The copy cost per SSE chunk is small (chunks are typically short JSON state-change notifications, not large payloads), and SSE is a low-throughput notification channel, not a bulk data path.

**Revisit when:** If SSE is used for high-volume event delivery, or if the parser is rewritten for other reasons.

---

## API ergonomics (pre-1.0 breaking changes)

Surfaced by downstream consumer feedback. All are breaking; bundle into a single pre-1.0 release with a migration note rather than trickling out.

### 1. `Option`-less getters use sentinel values

`Mailbox::role()` returns `Role` directly, requiring `== Role::None` to check for unset. `Mailbox::total_emails()` returns `usize` directly with no way to distinguish "zero" from "server didn't return it". Audit all typed-struct getters for the same pattern.

**Change:** Return `Option<Role>` / `Option<usize>` etc. where the JMAP spec allows the property to be absent.

**Why:** Sentinel values are un-Rusty and surprise users who reach for `if let Some(role)`. The `Field<T>` machinery already distinguishes omitted/null/value internally - the getter layer is flattening that away incorrectly.

---

### 2. `mailbox_get(id, props)` name implies plural

`mailbox_get(id, props)` fetches exactly one mailbox by ID. Consumers expect it to fetch many (the JMAP `Mailbox/get` method accepts a list, and `ids: null` means "all"). The builder path (`MailboxGet::new(...)` without `.ids()`) is the only way to get all.

**Change:** Either rename the single-fetch helper (`mailbox_get_one` / keep builder-only for bulk) or change the helper to accept `Option<&[Id]>` and fetch many, with `None` = all. Apply consistently across all `<type>_get` helpers (email, calendar, contact, etc.).

**Why:** The current name lies about behavior. Whichever direction we pick, the helper and the builder should agree.

---

### 3. Common arguments hidden on `.arguments()`

`fetch_text_body_values(true)` and similar common flags are only reachable via `get_req.arguments().fetch_text_body_values(true)` on the method struct. Consumers don't discover them.

**Change:** Lift frequently-used arguments to first-class builder methods on the method struct. Keep `.arguments()` as an escape hatch for rarely-used ones.

**Why:** Discoverability. Users shouldn't need to read the source to find `fetch_text_body_values`.

---

### 4. `max_changes: 0` silently invalid

`mailbox_changes(since_state, 0)` compiles and sends, but `0` violates the JMAP spec (must be > 0). Server rejects at runtime.

**Change:** Validate at the call site (return `Error::InvalidArgument` for `0`), or redefine `0` to mean "server default" and document it. Likely the former - silent semantic overloading is worse than an error.

**Why:** Catch the bug before the network round-trip.

---

### 5. `AccountId` / `IdentityId` confusable in `email_submission_create`

`email_submission_create(email_id, identity_id)` takes two string-ish IDs. Consumers pass an account ID by mistake because there's no type-level distinction.

**Change:** Use the phantom-typed `Id<T>` wrapper (`Id<IdentityId>`, `Id<EmailId>`) on helper signatures so mix-ups fail to compile. `Id<T>` already exists in the crate (per CLAUDE.md "Available for incremental adoption") - this is the incremental adoption.

**Why:** The type system should enforce what the parameter name only suggests.

---

### Explicitly not changing

These came up in the same feedback but are Rust-isms, not API bugs:

- `take_id()` / `take_list()` needing `let mut response` - ownership semantics, correct as-is.
- `changes.created()` returning `&[String]` not `&[&str]` - matches storage, `.map(String::as_str)` is idiomatic.
- Filter type inference requiring an explicit binding - a generics limitation; fixing it would need a less-generic API.
- `download(blob_id)` signature - consumer expected a wrong signature; current shape is correct.

---

## Remaining specs

Implementation plans for additional JMAP specifications live alongside this file:

- **MDN (RFC 9007)** - `MDN.md` - Read receipts (MDN/send, MDN/parse)
- **S/MIME (RFC 9219)** - `SMIME.md` - Email signature verification properties and filters
