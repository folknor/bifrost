# IMAP daaki migration plan

Private working plan for replacing the async-imap derived `bifrost-imap`
implementation with a daaki-derived implementation.

## Decision

We are moving the IMAP crate onto daaki's architecture instead of continuing to
grow the async-imap fork.

The main reason is architectural, not feature-count vanity. Typed events,
cancellation safety, streaming FETCH, command pipelining, COMPRESS, UTF8, and
safe stream upgrades all want one owner for the socket and parser state. Daaki
already has that driver-task model. Rebuilding those semantics inside the
current `Session<T>` API would be a rewrite with more compatibility drag.

## Source Snapshot

- Local source: `/home/folk/Programs/bifrost/research/daaki`
- IMAP crate: `/home/folk/Programs/bifrost/research/daaki/crates/imap`
- Remote: `https://github.com/steelbrain/daaki`
- Commit: `e81a169c52afee65eb8a2fdfd252f8ebaacdb997`
- Inspection date: 2026-05-19
- Local checkout status at inspection: clean

## Current Status

The daaki cutover is complete for the first bifrost-native IMAP crate shape.

Completed:

- `crates/imap/src` is daaki-derived source, excluding daaki's top-level
  external integration tests and fuzz harness.
- `crates/imap` is tokio plus native-tls only. The async-std, rustls, and old
  async-imap feature shims are gone.
- `crates/imap-proto` is removed from the workspace. Parser, encoder,
  connection, and protocol types now live in the IMAP crate.
- Daaki attribution is in `NOTICE`.
- The imported parser is migrated to nom 8 without the temporary deprecated
  tuple allowance.
- The connection lifecycle uses native-tls for implicit TLS and STARTTLS while
  preserving driver-owned stream upgrades.
- The old `daaki-message` dependency is not imported. IMAP owns a small local
  `Address`, `ValidationError`, and `TlsMode`.
- PLAIN, XOAUTH2, CRAM-MD5, SCRAM-SHA-1, and SCRAM-SHA-256 are exposed as
  connection methods. SASL-IR is used when advertised or active through
  IMAP4rev2.
- Internal credential and SASL initial-response strings use zeroizing storage
  and redact under `Debug`.
- Gmail `X-GM-MSGID`, `X-GM-THRID`, and `X-GM-EXT-1` support is restored on
  top of daaki's fetch model.
- The first API ergonomics stretch is complete: typed IDs and sequence sets,
  secret-aware credentials, auth policy selection, connection config, server
  profiles, sync helpers, limited and callback-based fetch helpers, event
  impact mapping, and structured error classification.
- `brokkr fmt -p bifrost-imap` passes.
- `brokkr check` passes.

Deliberate choices:

- `expunge_deleted_uids()` was not reintroduced. The daaki architecture keeps
  native `uid_expunge()` and the safer MOVE fallback that avoids plain EXPUNGE
  deleting unrelated messages.
- The old `Flags<'_>` wrapper was not reintroduced. Daaki's
  `FetchResponse.flags: Option<Vec<Flag>>` already distinguishes absent
  `FLAGS` from `FLAGS ()`.
- The old boxed transport API was dropped. Driver-owned tokio/native-tls
  transport is the supported model.
- Daaki's imported style warnings are not treated as this slice's blocker.
  They are normal cleanup work after the architectural migration.

## Constraints

- Do not copy daaki's docker-backed integration tests.
- Do not copy hardcoded-server tests such as `integration.rs`,
  `dovecot_extended.rs`, or `stalwart.rs`.
- Ratatoskr owns the end-to-end integration harness. Bifrost should carry unit,
  parser, encoder, state, and API tests that are cheap and deterministic.
- Bifrost IMAP is moving to tokio plus native-tls only.
- Do not preserve async-std support.
- Do not preserve rustls support unless a later concrete consumer needs it.
- Keep the crate name `bifrost-imap`. A separate `imap-proto` crate was useful
  during the async-imap phase but is no longer part of the workspace.
- This is source adoption, not a blind vendor drop. The result should look like
  a bifrost crate.

## Import Scope

Copy from `research/daaki/crates/imap`:

- `src/codec/`
- `src/connection/`
- `src/types/`
- `src/error.rs`
- source-local unit tests that live under `src/`
- selected property or fuzz logic only if it can run without external services

Do not copy:

- `tests/integration.rs`
- `tests/dovecot_extended.rs`
- `tests/stalwart.rs`
- any other test that depends on Docker, real servers, fixed ports, external
  accounts, or external network state
- daaki README examples as doctests
- daaki workspace metadata as-is
- rustls-only connection entry points as final API

Handle `daaki-message` deliberately:

- First pass: copy only the minimal message/address/TLS pieces required to
  compile the IMAP crate.
- Second pass: decide whether these belong in a shared `bifrost-message` crate
  used by IMAP and SMTP, or whether IMAP should own smaller local types.
- `TlsMode` should become a bifrost type using native-tls semantics.

## Feature Target

The migrated crate should preserve or add these user-visible capabilities:

- tokio driver task owns all wire I/O
- cancellation-safe public methods
- typed event queue
- `drain_events()` and `next_event()`
- IDLE returning typed events
- NOTIFY events routed into typed events
- streaming `FETCH` and `UID FETCH`
- buffered FETCH convenience wrappers over streaming FETCH
- typed CAPABILITY, ENABLE, and protocol revision state
- IMAP4rev1 and IMAP4rev2 groundwork
- UTF8=ACCEPT and UTF8=ONLY behavior
- COMPRESS=DEFLATE
- UIDPLUS APPENDUID and COPYUID
- UID EXPUNGE
- MOVE and safe MOVE fallback
- CONDSTORE and QRESYNC
- OBJECTID, SAVEDATE, PREVIEW, BINARY
- ESEARCH, SEARCHRES, SORT, THREAD, WITHIN
- LIST-EXTENDED, LIST-STATUS, SPECIAL-USE, NAMESPACE
- ACL, QUOTA, METADATA
- SASL-IR and UNAUTHENTICATE
- typed validated inputs such as mailbox names, sequence sets, atoms, and
  object IDs

Authentication target:

- Keep typed PLAIN and XOAUTH2 as first-class.
- The local daaki source exposed PLAIN and XOAUTH2 but not CRAM or SCRAM in the
  IMAP auth module. CRAM-MD5, SCRAM-SHA-1, and SCRAM-SHA-256 are implemented in
  the driver/continuation architecture.
- Preserve the rule that malformed SASL mechanism names are rejected before
  anything is written to the wire.
- Prefer SASL-IR when advertised or implied by IMAP4rev2.
- Secret strings and SASL initial responses are zeroized on drop and redacted
  under `Debug`.

UID expunge target:

- Daaki has native `uid_expunge()` gated on UIDPLUS and returns an
  `ExpungeResult`.
- Daaki's MOVE fallback uses COPY, `+FLAGS.SILENT \Deleted`, and UID EXPUNGE,
  and refuses plain EXPUNGE for MOVE because it can delete unrelated messages.
- Our current `expunge_deleted_uids()` helper with `UidExpungeStrategy` and a
  STORE plus EXPUNGE fallback is not present in daaki. Re-add it only after
  re-evaluating whether we still want that best-effort fallback in a
  cancellation-safe driver world.

## Cutover Strategy

### Phase 1: Prepare the workspace - done

- Add daaki attribution to `NOTICE`.
- Record the daaki source snapshot path and date in this plan.
- Convert `crates/imap/Cargo.toml` to tokio plus native-tls only.
- Remove `async-std1`, `runtime-async-std`, `runtime-tokio`, and rustls-oriented
  feature paths from IMAP.
- Keep `serde` optional if the imported public types already support it cleanly.
- Add any required dependencies:
  - `tokio`
  - `tokio-native-tls`
  - `native-tls`
  - `tokio-util`
  - `bytes`
  - `flate2`
  - `encoding_rs`
  - `socket2`
  - `tracing`
  - `base64`
  - `thiserror`
  - `nom = 8`

### Phase 2: Import source without external tests - done

- Replace the current `crates/imap/src` implementation with the daaki-derived
  source modules.
- Do not copy the daaki integration test directory.
- Keep cheap source-local tests, but expect to edit them for bifrost naming,
  native-tls, nom 8, and lint policy.
- Either temporarily keep `crates/imap-proto` unused or remove it in the same
  slice once no workspace member depends on it.
- Do not keep async-imap compatibility shims unless ratatoskr actually needs
  them.

### Phase 3: Port from rustls to native-tls - done

- Replace daaki `TlsMode` and rustls connector usage with native-tls and
  tokio-native-tls.
- Preserve the same behavior:
  - plaintext
  - implicit TLS
  - STARTTLS
  - custom TLS connector or connector configuration for tests and ratatoskr
- Preserve atomic upgrade semantics:
  - driver sends STARTTLS
  - driver waits for tagged OK
  - driver verifies buffered plaintext state is safe
  - driver swaps stream ownership without caller access
- Keep COMPRESS upgrade independent from TLS and still driver-owned.

### Phase 4: Migrate nom 7 code to nom 8 - done

- Update parser imports and combinator calls.
- Replace function-call parser style with `.parse(...)` where needed.
- Keep daaki parser tests close while doing this, because the parser surface is
  large.
- Carry over our current parser regression cases:
  - streamed command final status behavior
  - FLAGS attribute presence ambiguity
  - BODYSTRUCTURE recursion limits
  - malformed ENVELOPE display-name lenience
  - numbered `BODY[1]` sections
  - IMAP4rev2 capability and STATUS `DELETED`/`SIZE`

### Phase 5: Make the API bifrost-native - done for the cutover

- Public connection type should be a cheap handle around the driver task.
- Public methods should take `&self`.
- Every public operation should take an explicit `Duration` or use a clearly
  documented timeout policy chosen by the caller.
- Public response types should be owned.
- Public enums and structs should be `#[non_exhaustive]` unless there is a
  strong reason not to.
- Remove broad `Error::Internal(String)` style where practical. Prefer
  structured variants. This is ongoing cleanup; the cutover only removed the
  old async-imap error model.
- Align naming with bifrost conventions, not daaki names by default.
- Preserve high-value daaki names when they are already better than the old
  async-imap names.

### Phase 6: Reconcile current bifrost work - done for the cutover

Explicitly re-check every feature we added during the async-imap phase:

- pre-auth CAPABILITY
- typed SASL mechanism checks
- PLAIN and XOAUTH2 typed flows
- streamed terminal `NO` or `BAD` propagation
- `FLAGS ()` versus absent FLAGS
- Gmail `X-GM-MSGID` and `X-GM-THRID`
- RFC 8474 `EMAILID` and `THREADID`
- UIDPLUS APPENDUID and COPYUID
- UID EXPUNGE return semantics
- `expunge_deleted_uids()` strategy helper decision: deliberately not kept
- ENABLE
- IDLE ergonomics
- NOTIFY validation
- boxed transport need: obsolete after tokio-native-tls driver adoption
- IMAP4rev2 capability helpers
- STATUS `DELETED` and `SIZE`

For each item, either:

- keep daaki's implementation,
- port our behavior into the daaki architecture, or
- deliberately drop it with a note in this plan.

### Phase 7: Validation - done

Run local deterministic checks only:

- `brokkr fmt`
- `brokkr check`
- focused `brokkr test` for parser and command modules when useful

Do not add Docker or fixed-port server tests to bifrost. Ratatoskr validates
real server behavior.

Before a commit, ensure:

- no daaki integration tests were copied
- no rustls dependency remains in IMAP unless explicitly re-approved
- no async-std feature remains in IMAP
- `NOTICE` includes daaki attribution
- `Cargo.lock` is updated if dependencies changed
- the plan contains only current gaps and decisions

### Phase 8: API ergonomics stretch - done

The first post-cutover API pass is intentionally breaking and consumer-first.
The goal is to make common correct behavior obvious, not to preserve daaki or
async-imap surface area.

Completed:

- Added typed numeric identities:
  - `Uid`
  - `Seq`
  - `UidValidity`
  - `ModSeq`
  - `GmailMessageId`
  - `GmailThreadId`
- Added typed sequence-set wrappers:
  - `UidSet`
  - `SeqSet`
  These prevent accidental UID/sequence-number mixing at API boundaries while
  still reusing the validated IMAP sequence-set encoder.
- Added public secret handling:
  - `SecretString`
  - `IntoSecretString`
  Secrets redact under `Debug` and zeroize owned storage on drop.
- Added credential and auth policy types:
  - `Credentials`
  - `AuthMechanism`
  - `AuthPolicy`
  - `AuthOutcome`
  `authenticate_best()` chooses a permitted mechanism from advertised server
  capabilities and refuses cleartext password mechanisms without TLS unless the
  caller explicitly opts in.
- Added `ImapConfig` as the low-ceremony connection entry point for implicit
  TLS, STARTTLS, and plaintext. It also centralizes connect timeout, command
  timeout, keepalive, and custom native-tls connector configuration.
- Added `ServerProfile` so callers can ask capability questions without
  re-learning CAPABILITY, ENABLE, IMAP4rev2 implications, AUTH tokens,
  APPENDLIMIT, or THREAD algorithm parsing.
- Added sync-oriented request and result types:
  - `SyncSelectOptions`
  - `SyncSelectResult`
  - `SyncFetchRequest`
  - `SyncFetchResult`
  These wrap SELECT/EXAMINE, CONDSTORE, QRESYNC, CHANGEDSINCE, VANISHED, and
  common full-message fetches behind explicit typed choices.
- Added fetch helpers that hide channel ceremony:
  - `uid_fetch_each()`
  - `uid_fetch_limited()`
  - `uid_fetch_full_messages()`
  The limited variant enforces a hard client-side budget in the driver
  consumer and returns structured `Error::FetchLimit` when crossed.
- Removed a fetch-loss footgun from the daaki-derived streaming path:
  `fetch_streaming()` and `uid_fetch_streaming()` now use an unbounded sender
  instead of a bounded channel plus `try_send`, and buffered `uid_fetch()` no
  longer depends on that streaming path. A slow consumer can still create
  memory pressure, but responses are no longer silently dropped.
- Added event impact mapping:
  - `TypedEvent::impact()`
  - `EventImpact`
  Consumers can classify unsolicited events by mailbox-cache impact without a
  large ad hoc match in every application.
- Added error classification:
  - `Error::category()`
  - `Error::recovery()`
  - `Error::response_code()`
  - `ErrorCategory`
  - `Recovery`
  This gives consumers stable policy hooks without string matching server text.

Design constraints for follow-up API work:

- Prefer typed wrappers over raw strings and raw integers at public boundaries.
- Prefer a single high-level method for the common safe flow, while keeping the
  protocol-shaped primitive available underneath.
- Refuse footguns by default. Cleartext auth, LOGIN, and lossy UID/sequence
  conversions should be explicit.
- Keep connection I/O driver-owned. Do not reintroduce public stream ownership
  or boxed transport escape hatches unless a concrete consumer requires them.

## Risk Register

- Size: even without integration tests, this is a large source import. Keep the
  first code slice focused on compileability and deterministic tests.
- TLS rewrite: rustls to native-tls touches connection lifecycle and STARTTLS
  upgrade paths. Preserve driver ownership while changing connector types.
- Parser migration: nom 7 to nom 8 is broad. Do it mechanically with tests
  close at hand.
- Error model: daaki still has some stringly internal errors. The cutover is
  green, but structured error cleanup remains valid follow-up work.
- Message dependency: daaki-message was intentionally not imported. Revisit a
  shared `bifrost-message` crate only if IMAP and SMTP converge on enough
  reusable message/address behavior.
- API breakage: this is intentionally not async-imap compatible. Ratatoskr
  integration will drive the final ergonomics.
- Auth gap: resolved for PLAIN, XOAUTH2, CRAM-MD5, SCRAM-SHA-1, and
  SCRAM-SHA-256. Future mechanisms should be added as explicit connection
  methods or typed auth helpers, not by reviving the old async-imap
  authenticator trait.

## Next Work

- Keep replacing raw-string and raw-integer protocol boundaries with typed
  request/response objects where the safety win is obvious.
- Consider typed mailbox names, mailbox paths, and flag sets next. These are
  still common footgun points.
- Clean imported clippy style warnings when touching nearby code.
- Continue structured error cleanup.
- Add more deterministic unit coverage for new IMAP auth edge cases if an
  integration finding exposes gaps.
