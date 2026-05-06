# CLAUDE.md

## Agent rules

- Always launch subagents in the **foreground** (never use `run_in_background`). Background agents cannot get tool approvals.

## Project

`bifrost` — Cargo workspace of Rust clients for email/calendar/contact protocols, built for [ratatoskr](https://github.com/folknor/ratatoskr).

Crates:
- `crates/jmap/` → `bifrost-jmap` — JMAP client (RFC 8620 / 8621 / 8887 / 9404 / 9425 / 9610 / 9670, calendars draft-26, sieve draft-14). Pre-1.0, API stabilization phase.

Planned (not yet present in this workspace): `bifrost-imap`, `bifrost-graph`, `bifrost-gmail`, `bifrost-smtp`.

## Build & test

All commands run from the workspace root unless noted.

```bash
cargo build                                                    # default features (tls-rustls)
cargo test -p bifrost-jmap --lib                               # 74 tests with all features
cargo test -p bifrost-jmap --lib --no-default-features -F tls-rustls   # 22 core-only tests
cargo build -p bifrost-jmap --no-default-features -F tls-rustls        # minimal build
cargo clippy --workspace --all-targets                         # zero warnings expected
```

Lints are in `[workspace.lints.clippy]` in the root `Cargo.toml`. Per-crate manifests inherit via `[lints] workspace = true`.

## Architecture (`bifrost-jmap`)

### Trait-based method dispatch (no central enums)

Every JMAP method is a self-describing struct implementing `JmapMethod`:
```rust
pub trait JmapMethod: Serialize + Send {
    const NAME: &'static str;       // "Email/get"
    type Cap: Capability;           // capability::Mail
    type Response: DeserializeOwned; // GetResponse<Email<Get>>
}
```

Adding a new method: define a struct, use `define_get_method!` / `define_set_method!` etc., done. **Zero central files touched.**

### Request/Response flow

```rust
let mut request = client.build();
let handle = request.call(EmailGet::new(&account_id))?;  // typed CallHandle<EmailGet>
let mut response = request.send().await?;
let result = response.get(&handle)?;  // compile-time safe extraction
```

`CallHandle<M>` validates call_id and method name. `Response::get()` handles method errors (returns `Error::Method` for JMAP error responses).

### Transport abstraction

`Client<T: HttpTransport = ReqwestTransport>` — generic over transport.
- `HttpTransport` — api_request, upload, download, get_session (returns `Bytes`)
- `SseTransport` — open_sse (EventSource, with `last_event_id` support)
- `ReqwestTransport` — default implementation with pooled reqwest::Client
- `Client::with_transport(transport, session)` — custom transport injection
- WebSocket remains reqwest-specific (documented)

All convenience helpers are `impl<Tr: HttpTransport> Client<Tr>` — custom transports get the full API.

### Module pattern

Every JMAP object type under `crates/jmap/src/<type>/`:
- `mod.rs` — struct with `<State = Get>` phantom, Property enum, method struct definitions via `define_*_method!` macros
- `get.rs` — getters on `T<Get>`, GetObject impl
- `set.rs` — builder methods on `T<Set>`, SetObject + SetObjectCreatable impls
- `query.rs` — Filter/Comparator enums, QueryObject impl
- `helpers.rs` — `impl<Tr: HttpTransport> Client<Tr>` convenience methods

### Two data models

**Typed structs** (Mailbox, Calendar, AddressBook, etc.): serde derive, `Field<T>` for nullable properties.

**JSON map backing** (CalendarEvent, ContactCard): `serde_json::Map` via `json_object_struct!` macro. Property enum has `Other(String)`. Extension properties preserved on round-trip.

### Key types

- `Field<T>` — three-state nullable: `Omitted` / `Null` / `Value(T)`. Use instead of `Option<Option<T>>`.
- `Id<T>` — phantom-typed string ID: `AccountId`, `BlobId`, `State`. Available for incremental adoption.
- `Account<'a, Tr>` — account-scoped view of Client. Use `account.build()` for scoped requests.
- `Capability` trait — typed URIs with associated `Config` type.
- `TransportError` — crate-owned, `#[non_exhaustive]`, carries response body (`Bytes`) for ProblemDetails parsing.

### Capabilities

`Capabilities` enum in `session.rs` uses `deserialize_capabilities_map` to dispatch on URI key string. When adding a new capability:
1. Add struct in session.rs
2. Add variant to `Capabilities` enum (with `#[cfg]` if feature-gated)
3. Add match arm in deserializer
4. Add `Capability` impl in capability.rs with `type Config`
5. Add session accessor method

`Session::typed_capability::<C>()` is a convenience bridge (serde round-trip). Hand-written accessors are zero-cost and primary.

### Feature gates

Per-RFC features: `mail`, `calendars`, `contacts`, `blob`, `quota`. Each gates:
- Module declarations in lib.rs
- DataType enum variants (with `#[serde(other)]` catch-all)
- Capabilities enum variants + session accessors + deserializer arms
- PushObject/PushNotification variants
- Test modules

### Error model

Structured variants — no `Error::Internal(String)`:
- `CallNotFound`, `IdNotFound`, `EmptyResponse`, `NotParsable`, `InvalidUrl`, `WebSocketNotConnected`
- `Transport(TransportError)` — wraps transport errors, auto-parses ProblemDetails from body
- `Method(MethodError)` — JMAP method-level errors
- No `From<reqwest::Error>` — reqwest errors converted to TransportError at point of use

### PatchObject null semantics

RFC 8620: `null` removes map keys, not `false`. Email `patch` field uses `HashMap<String, serde_json::Value>` with `Value::Null` for removals.

## Code style

- No per-file copyright headers; attribution lives in the workspace-root `NOTICE`.
- Async-only (no maybe_async, no blocking)
- `#[non_exhaustive]` on all public enums and TransportError
- Clippy lints in workspace root `Cargo.toml` `[workspace.lints.clippy]`
- `#[serde(skip_serializing_if = "...")]` on optional fields
- `Field::is_omitted` for skip_serializing_if on Field<T> fields (with `#[serde(default)]`)
- SetObjectCreatable::new() initializes optional fields to None/Omitted, not empty collections
- Don't commit .md reference docs (CALENDARS.md, etc.) outside `plans/`.
- Helper impl blocks use `impl<Tr: HttpTransport> Client<Tr>` (not bare `impl Client`)
