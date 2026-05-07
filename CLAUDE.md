# CLAUDE.md

## Agent rules

- Always launch subagents in the **foreground** (never use `run_in_background`). Background agents cannot get tool approvals.

### Multi-Agent Orchestration

**Do NOT use worktree isolation for parallel agents.** Worktrees create merge conflicts that silently drop agent work. Instead, launch agents in the same tree with strict file ownership - zero overlap.

**Why no worktrees:** Worktrees let agents work on diverged snapshots. When merging back, `git checkout --ours/--theirs` drops code, conflict markers get missed, and features end up "existing but not wired" - types/functions created but never connected to bytecode dispatch, the standard library, or call sites. This has happened in long sessions and was only caught by a rigorous 3-pass audit.

**Agent coordination rules:**

- Each agent gets exclusive ownership of specific files. No two agents touch the same file.
- Agents must read their target file FIRST. Do not replace existing code with placeholders or stub it out.
- Agents must NOT run `brokkr check`, `brokkr test`, `cargo`, or `./diff_test.sh`. The orchestrator validates between agents.
- Include `CLAUDE.md` (and any other top-level docs they'll need, e.g. `LLM.md`) in every agent's required reading.

**Audit protocol:**

- Do not trust agent claims of completion. Verify existence + wiring + behavior.
- Use the 3-pass audit structure: domain-specific verification, then cross-cutting reconciliation (does the new instruction actually dispatch? is the new builtin actually installed by `open_libs`?), then editorial normalization.
- Any discrepancies doc should contain only current gaps, not historical records. Remove resolved items entirely.

## Project

`bifrost` - Cargo workspace of Rust clients for email/calendar/contact protocols, built for [ratatoskr](https://github.com/folknor/ratatoskr).

Crates:
- `crates/jmap/` → `bifrost-jmap` - JMAP client (RFC 8620 / 8621 / 8887 / 9404 / 9425 / 9610 / 9670, calendars draft-26, sieve draft-14). Pre-1.0, API stabilization phase.

Planned (not yet present in this workspace): `bifrost-imap`, `bifrost-graph`, `bifrost-gmail`, `bifrost-smtp`.

## Rules

### General rules

- Don't use gremlins! Em-dash, en-dash, strange quotes, whatever - they're all verboten.
- Don't remind the user of CLAUDE.md rules. They wrote them, so they know them.

### Memory rules

Do not use your Memory functionality. Do not read, write, or update memories. Do not suggest saving things to memory. Durable context belongs in CLAUDE.md or the relevant docs, not in per-session memory files - this project is developed across several hosts and users, and memory does not transfer between them; CLAUDE.md does.

### Bash rules

- Never use `sed`, `find`, `awk`, `head`, `tail`, or complex bash commands.
- Never chain commands with `&&`.
- Never chain commands with `;`.
- Never chain/pipe commands with `|`. Exception: piping into `review` is allowed (writing scratch prompt files is wasteful).
- Never capture stdout into env vars (`UUID=$(...)`).
- Never read or write from `/tmp`. All data lives in the project.
- Never redirect command output to scratch files (`> .check.log`, `> out.txt`, etc.) just to read it back. The terminal output is the channel - read it inline, or use the tool's own filtering / `--json` mode if the volume is too high.
- Never run raw `cargo`, `curl`, `pkill`. Use `brokkr`.
- Never run `git` with `-C <path>`. Run `git` from the current working directory.

### git commit rules

- Never commit markdown changes alone. Bundle them with upcoming code commits.
- When committing other changes: always tag along markdown files if dirty.
- Write substantive engineering-focused commit messages.
- Has `Cargo.lock` changed? Commit it.
- Never `git push` unless the user explicitly asks. Stop after the commit.
- Always run `cargo fmt` before a commit.

## Commands

Use `brokkr` (not `cargo`) for check/test. It runs a gremlins scan (banned Unicode), then clippy, then tests - clippy denies warnings project-wide, so a clippy failure short-circuits before tests run. By default output is filtered to changed files and capped at 20 diagnostics per phase.

Always run `brokkr check` in the foreground with a 4-minute (240000ms) timeout. A healthy `brokkr check` finishes well under that. If it does not, something is wrong - kill it and investigate (most often: a test hangs because a background task wasn't drained on shutdown). Do not raise the timeout to "wait it out", and do not run `brokkr check` in the background.

- `brokkr check` - gremlins + clippy + all tests (changed-files scope)
- `brokkr check --all` - show every diagnostic, no cap, no scope filter
- `brokkr check --fix-gremlins` - rewrite banned Unicode in tracked files (em/en dash -> `-`, smart quotes -> straight, NBSP -> space, zero-width/bidi deleted) before checking
- `brokkr check -p <crate>` - scope to one package (e.g. `-p rtsk`, `-p app`, `-p squeeze`)
- `brokkr check -- --test <file>` - forward args to `cargo test` (args after the second `--` go to the test binary)
- `brokkr test -p <crate> <NAME>` - release-mode focused single-test runner. Always passes `--release --include-ignored --nocapture --test-threads=1`. `<NAME>` is a case-sensitive substring filter (matches both unit and integration tests). Streams the test's own stdout/stderr live and prints a `[test] PASS/FAIL` footer with wall time. Defaults to `--all-features`; runs a second sweep if `[check].consumer_features` is set in `brokkr.toml`. Gated off for litehtml/sluggrs (use `brokkr visual` there).
  - `-p, --package <PKG>` - cargo package. Required in this workspace - no default package, and overrides `[test] default_package` in `brokkr.toml` if set.
  - `-N, --repeat <N>` - run the test N times per sweep (flaky-test hunting).
  - `-j, --jobs <N>` - parallel cargo compile jobs.
  - `--raw` - bypass output filtering, print everything cargo emits.
  - `--debug` - build and run the test in dev profile instead of release. Use this for subprocess-lifecycle / IPC / boot-path tests where release-LTO compile time (3-4 min for the full workspace) dominates wall time and the optimization level doesn't change the behavior under test. `BROKKR_TEST_BIN_DIR` points at `<target>/debug` accordingly.
  - Example: `brokkr test -p common truncates_without_splitting` or `brokkr test -p calendar extract_tag_value_flattens_nested_text -N 5` or `brokkr test -p app terminal_failure_at_initial_boot_does_not_respawn --debug`.
- `cargo run -p app` - run the iced app (requires a seeded DB, see `crates/app/seed-db.py`)

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

`Client<T: HttpTransport = ReqwestTransport>` - generic over transport.
- `HttpTransport` - api_request, upload, download, get_session (returns `Bytes`)
- `SseTransport` - open_sse (EventSource, with `last_event_id` support)
- `ReqwestTransport` - default implementation with pooled reqwest::Client
- `Client::with_transport(transport, session)` - custom transport injection
- WebSocket remains reqwest-specific (documented)

All convenience helpers are `impl<Tr: HttpTransport> Client<Tr>` - custom transports get the full API.

### Module pattern

Every JMAP object type under `crates/jmap/src/<type>/`:
- `mod.rs` - struct with `<State = Get>` phantom, Property enum, method struct definitions via `define_*_method!` macros
- `get.rs` - getters on `T<Get>`, GetObject impl
- `set.rs` - builder methods on `T<Set>`, SetObject + SetObjectCreatable impls
- `query.rs` - Filter/Comparator enums, QueryObject impl
- `helpers.rs` - `impl<Tr: HttpTransport> Client<Tr>` convenience methods

### Two data models

**Typed structs** (Mailbox, Calendar, AddressBook, etc.): serde derive, `Field<T>` for nullable properties.

**JSON map backing** (CalendarEvent, ContactCard): `serde_json::Map` via `json_object_struct!` macro. Property enum has `Other(String)`. Extension properties preserved on round-trip.

### Key types

- `Field<T>` - three-state nullable: `Omitted` / `Null` / `Value(T)`. Use instead of `Option<Option<T>>`.
- `Id<T>` - phantom-typed string ID: `AccountId`, `BlobId`, `State`. Available for incremental adoption.
- `Account<'a, Tr>` - account-scoped view of Client. Use `account.build()` for scoped requests.
- `Capability` trait - typed URIs with associated `Config` type.
- `TransportError` - crate-owned, `#[non_exhaustive]`, carries response body (`Bytes`) for ProblemDetails parsing.

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

Structured variants - no `Error::Internal(String)`:
- `CallNotFound`, `IdNotFound`, `EmptyResponse`, `NotParsable`, `InvalidUrl`, `WebSocketNotConnected`
- `Transport(TransportError)` - wraps transport errors, auto-parses ProblemDetails from body
- `Method(MethodError)` - JMAP method-level errors
- No `From<reqwest::Error>` - reqwest errors converted to TransportError at point of use

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
