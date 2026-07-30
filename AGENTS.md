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

Crates present:

- `crates/jmap/` → `bifrost-jmap` - JMAP client. RFC 8620 / 8621 / 8887 / 9404 / 9425 / 9610 / 9670, calendars draft-26, sieve draft-14.
- `crates/imap/` → `bifrost-imap` - IMAP client. Daaki-derived driver-task model. Tokio + native-tls only.
- `crates/google/` → `bifrost-google` - Google account client. Gmail mail, People contacts, and Google Calendar are wired.
- `crates/graph/` → `bifrost-graph` - Microsoft Graph account client.
- `crates/carddav/` → `bifrost-carddav` - CardDAV account crate for contacts.
- `crates/caldav/` → `bifrost-caldav` - CalDAV account crate for calendars.
- `crates/smtp/` → `bifrost-smtp` - SMTP and LMTP client. Lettre-derived. Native-tls only.
- `crates/sasl/` → `bifrost-sasl` - private shared SASL/SCRAM computation layer (SCRAM, CRAM-MD5). Consumed by `bifrost-imap`; not public.

All crates are pre-1.0, API stabilization phase.

CalDAV composes into IMAP-shaped accounts when configured.

## Rules

### General rules

- Don't use gremlins! Em-dash, en-dash, strange quotes, whatever - they're all verboten.
- Don't remind the user of the rules. They wrote them, so they know them.
- The user can exempt you from any rule at any time.

### Bash rules

- Never chain commands with `&&`.
- Never chain commands with `;`.
- Never chain/pipe commands with `|`. Exception: piping into `review` is allowed (writing scratch prompt files is wasteful).
- Never capture stdout into env vars (`UUID=$(...)`).
- Never read or write from `/tmp`. All data lives in the project.
- Never run raw `cargo`, `curl`, `pkill`. Use `brokkr`.

### git commit rules

- Always run `brokkr fmt` before a commit.
- Never commit markdown changes alone. Bundle them with upcoming code commits.
- When committing other changes: always tag along markdown files if dirty.
- Write substantive engineering-focused commit messages.
- Has `Cargo.lock` changed? Commit it.
- Never `git push` unless the user explicitly asks. Stop after the commit.

### Testing rules

The line is hermeticity, not size. A test belongs here if it is deterministic and runs entirely in-process.

- In scope: parser and encoder tests, type-level and object-safety checks, validation rules, error classification and recovery mapping, serde round-trips, state-machine sequencing driven through in-crate test doubles, and byte-level protocol transcripts fed through an in-memory duplex.
- Test doubles are fine and encouraged: `StubAccount`-style fakes implementing the crate's own traits (see `crates/imap/src/account/test_support.rs`), stub transports, canned server transcripts. These are not "integration tests" - no socket, no port, no daemon.
- Out of scope, still: real sockets or listeners, fixed ports, Docker, live credentials, external accounts, wall-clock sleeps, anything that can fail because a network did.
- Public APIs are exercised against real servers downstream, not here. Bifrost does not prove its protocol round-trips against live endpoints.
- Prefer the smallest test that pins the behavior. Breadth is welcome where it is hermetic; ceremony is not.
- Coverage is not a target in itself. Do not propose growing the suite to "match coverage of similar crates" - but do not treat the current size as a ceiling either.

## Commands

Use `brokkr` (not `cargo`) for check/test. By default output is filtered to changed files and capped at 20 diagnostics per phase.

- `brokkr check` - gremlins + clippy + all tests (changed-files scope)
- `brokkr check --all` - show every diagnostic, no cap, no scope filter
- `brokkr check -p <crate>` - scope to one package (e.g. `-p app`). You generally do not want to run this; a single `brokkr check` is faster than 2-3 `-p` runs, and brokkr intelligently filters which warnings and errors to show you
- `brokkr check -- --test <file>` - forward args to `cargo test` (args after the second `--` go to the test binary)
- `brokkr test -p <crate> <NAME>` - focused single-test runner. Always passes `--include-ignored --nocapture --test-threads=1`. Profile comes from `[test] debug` in `brokkr.toml`, which is currently `true`, so it builds dev by default. `<NAME>` is a case-sensitive substring filter (matches both unit and integration tests). Streams the test's own stdout/stderr live and prints a `[test] PASS/FAIL` footer with wall time. Defaults to `--all-features`; runs a second sweep if `[check].consumer_features` is set in `brokkr.toml`. Gated off for litehtml/sluggrs (use `brokkr visual` there).
  - `-p, --package <PKG>` - cargo package. Required in this workspace - no default package, and overrides `[test] default_package` in `brokkr.toml` if set.
  - `-N, --repeat <N>` - run the test N times per sweep (flaky-test hunting).
  - `-j, --jobs <N>` - parallel cargo compile jobs.
  - `--raw` - bypass output filtering, print everything cargo emits.
  - `--debug` - force the dev profile. Redundant while `[test] debug = true` is set in `brokkr.toml`, which is the current default; it matters only if that is flipped back. `BROKKR_TEST_BIN_DIR` points at `<target>/debug` accordingly.
  - Example: `brokkr test -p common truncates_without_splitting` or `brokkr test -p calendar extract_tag_value_flattens_nested_text -N 5` or `brokkr test -p app terminal_failure_at_initial_boot_does_not_respawn --debug`.
- `cargo run -p app` - run the iced app

## Code style

Workspace-wide conventions. Per-crate conventions live in `reference/<crate>.md`.

- No per-file copyright headers; attribution lives in `README.md`.
- Async-only (no maybe_async, no blocking).
- `#[non_exhaustive]` on all public enums.
- Clippy lints in workspace root `Cargo.toml` `[workspace.lints.clippy]`.
- Don't commit ad-hoc reference docs outside `plans/` or `reference/`.
- Code comments must never point to documents in `plans/`. Plans move, get renamed, and get deleted; source comments that name a plan path rot silently and require cross-tree edits when the doc moves. Explain the "why" in the comment itself, or point at a stable doc under `reference/`.

## Reference

Per-crate architecture and conventions. Single source of truth for current code state; in-flight design lives in `plans/`.

**Before working in a crate, read its `reference/<crate>.md` first.** These docs are kept in sync with the code and exist precisely so agents do not have to rediscover module layout, trait surfaces, or invariants from scratch. Skipping the read is how mistakes that the reference would have flagged get made.

- `reference/error-model.md` - cross-cutting `AccountError` contract every protocol crate produces and `bifrost-sync` reads: opaque builder funnel (`try_build` invariants), `AccountErrorKind` + message-key namespace, `RecoveryClass` and the central `derive` mapping, cause chain + transmission evidence, three-lane `BatchOutcome`, diagnostics consent tiers. The shared target the per-crate error mappings below map onto.
- `reference/jmap.md` - bifrost-jmap dispatch, transport, module pattern, capabilities, error model, and the `Account` impl under `crates/jmap/src/sync/` (cursor envelope, inventory / changes / hydration, WebSocket push, mutation pipeline, recovery taxonomy).
- `reference/imap.md` - bifrost-imap driver model, cancellation safety, streaming FETCH, typed IDs, auth, and the account layer under `crates/imap/src/account/` (QRESYNC / CONDSTORE / Basic cursor strategy, per-folder modseq cache, opportunistic `STORE UNCHANGEDSINCE`).
- `reference/google.md` - bifrost-google `Account` impl: Gmail history-id seeded sync, Cloud Pub/Sub push with renewer and health stream, mutation pipeline with flag canonicalization and TRASH fallback, and Google People contacts.
- `reference/graph.md` - bifrost-graph `Account` impl: Microsoft Graph delta-token sync, webhook push with renewal health worker plus EWS streaming fallback, cursor envelope and validation, `If-Match` etag mutations, error mapping.
- `reference/carddav.md` - bifrost-carddav standalone contact Account implementation.
- `reference/caldav.md` - bifrost-caldav standalone calendar Account implementation.
- `reference/smtp.md` - bifrost-smtp transport types, PIPELINING, DSN, message builder, LMTP.
- `reference/net.md` - bifrost-net shared HTTP transport: retry, rate-limiting, observability.
- `reference/sasl.md` - bifrost-sasl private SASL/SCRAM computation crate: `Secret`, `SaslError`, `ScramHash`, the pure SCRAM/CRAM functions, and the error-mapping contract with the protocol crates.
- `reference/sync.md` - bifrost-sync engine: scheduler, multiplexer, partitioned backfill, push reconciler, mutation pipeline, checkpoint envelope versioning, scope lifecycle.
