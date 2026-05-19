# IMAP upstream triage scratch

Private working notes for `bifrost-imap` and vendored `imap-proto`.

Last scan: 2026-05-19 with `gh api`.

## Working stance

- This is no longer just a vendor import. We should turn the IMAP crates into
  crates that feel good to use.
- Upstream compatibility is useful context, not a design ceiling. If an API is
  awkward, misleading, or preserves old ceremony for no good reason, redesign it.
- The immediate reason for the fork is SASL capability discovery. Pre-auth
  capability inspection and clear SASL mechanism helpers are the first-class API
  priority.

## Sources checked

- `chatmail/async-imap` open issues and PRs.
- `djc/tokio-imap` open issues and PRs.

## Current branch actions

- Implemented the Gmail thread ID accessor requested by `chatmail/async-imap` PRs
  #129 and #133. Local API name: `Fetch::gmail_thr_id()`, matching existing
  `Fetch::gmail_msg_id()`.
- Added pre-auth `Client::capabilities()` and explicit SASL helpers on
  `Capabilities`: `supports_sasl` and `sasl_mechanisms`.
- Added typed SASL authenticators for PLAIN and XOAUTH2, plus
  `Client::authenticate_with` so common auth flows do not duplicate mechanism
  strings at the call site. Raw `Client::authenticate` now validates SASL
  mechanism names and normalizes them to uppercase before writing an
  `AUTHENTICATE` command.
- Fixed the streamed command status path for `FETCH`, `STORE`, `EXPUNGE`,
  `LIST`, `SEARCH`, `ID`, `GETQUOTA`, `GETQUOTAROOT`, `GETMETADATA`, and
  `NOOP` style parsers. Tagged `NO` or `BAD` now reaches callers instead of
  being hidden by the old `take_while` stop condition.
- Added `Fetch::flags_attribute()` with a typed `Flags` view so callers can
  distinguish `FLAGS ()` from a `FETCH` response with no `FLAGS` attribute.
- Added explicit recursion limits to vendored `imap-proto` BODYSTRUCTURE and
  body-extension parsing so hostile nested input fails as a parser error instead
  of exhausting the Rust stack.
- Exposed UIDPLUS response data from APPEND, COPY, UID COPY, MOVE, and UID MOVE
  through typed response values instead of dropping parsed `APPENDUID` and
  `COPYUID` response codes.
- Added `Session::enable` for RFC 5161 so clients can opt into extensions such
  as CONDSTORE or QRESYNC and inspect the returned `ENABLED` capability set.
- Added `Session::idle_once` as a one-shot ergonomic wrapper around the lower
  level IDLE handle for the common wait-then-resume workflow.
- Added RFC 5465 `Session::notify` and `Session::notify_none` with typed
  `NotifySettings`, mailbox filters, events, and validation for the RFC event
  constraints before writing to the stream.
- Added bounded parser lenience for malformed ENVELOPE address display names
  where real servers emit extra unescaped quote fragments, while keeping the
  remaining address fields on the normal nstring parser.
- Added parser coverage for numbered `BODY[1]` fetch sections and public
  `Fetch::email_id`, `Fetch::thread_id`, and `Fetch::thread_id_attribute`
  accessors for RFC 8474 IDs already parsed by vendored `imap-proto`.
- Added `Session::expunge_deleted_uids` as a high-level UIDPLUS-aware helper
  that uses `UID EXPUNGE` when available and a best-effort
  search/protect/expunge/restore fallback otherwise. The fallback restores
  protected flags even when `EXPUNGE` returns `NO` or `BAD`, and preserves the
  original `EXPUNGE` error if the restore command also fails.
- Added `ImapTransport`, `BoxedTransport`, `BoxedClient`, and `BoxedSession`
  for callers that need one client type across runtime-selected plain/TLS
  transports.

## Already covered by the vendored import

- `chatmail/async-imap` #131 and `djc/tokio-imap` #190: targeted parser lenience
  for extra whitespace before the closing `)` in FETCH attributes. Local test:
  `test_fetch_response_whitespace_tolerance`.
- `djc/tokio-imap` #175: nom 8 migration. Local `imap-proto` is on nom 8 and
  no longer uses deprecated `nom::sequence::tuple`.

## Priority queue

P0: streamed command final status

- Source: `chatmail/async-imap` #95.
- Problem: `FETCH`, `UID FETCH`, `STORE`, `UID STORE`, `EXPUNGE`, and similar
  streaming parsers stop at the tagged `Done` response via `take_while`, so the
  terminal `NO` or `BAD` status can be dropped instead of returned to callers.
- Why it matters: this is a correctness issue, not docs or polish. A failed
  command can look like an empty successful result stream.
- Status: initial fix landed locally through a shared command-response adapter.
  Keep an eye out for any remaining command parser still hand-rolling final
  tagged status handling.

P1: FETCH flags presence ambiguity

- Source: `chatmail/async-imap` #98.
- Problem: `Fetch::flags()` returns an empty iterator for both `FLAGS ()` and a
  FETCH response with no `FLAGS` attribute.
- Status: fixed locally with `Fetch::flags_attribute() -> Option<Flags<'_>>`.
  Existing `Fetch::flags()` remains as a convenience iterator.

P0: SASL capability API

- Source: `chatmail/async-imap` #45.
- Problem: CAPABILITY is valid in any state, and authentication selection needs
  direct access to advertised `AUTH=<mechanism>` SASL mechanisms.
- Status: capability discovery, typed mechanism checks, and common PLAIN/XOAUTH2
  authenticators are in place. Continue to review whether SCRAM and TLS channel
  binding belong here or in a shared SASL crate. Future hardening: decide how
  owned IMAP authenticator secrets should zeroize on drop without pretending
  borrowed strings can be cleared.

P2: parser hardening for recursion

- Source: `djc/tokio-imap` #90.
- Problem: recursive body parsing can stack overflow on hostile or fuzzed input.
- Status: fixed locally with explicit limits around recursive BODYSTRUCTURE body
  nesting and body-extension list nesting.

P2: mailbox/parser edge cases

- Source: `djc/tokio-imap` #171 and `chatmail/async-imap` #71.
- Status: #171 has a local lenient fallback for malformed ENVELOPE address
  display names. #71 has local parser coverage for numbered `BODY[1]` sections;
  the available upstream repro was malformed by copy/paste, so no stream-layer
  change without a real failing byte sequence.

P2: IDLE ergonomics and NOTIFY

- Sources: `chatmail/async-imap` #55, #89, `djc/tokio-imap` #18.
- Status: local crate has IDLE, `idle_once`, and typed RFC 5465 NOTIFY command
  helpers. Later follow-up: decide whether unsolicited STATUS/LIST/FETCH
  responses from NOTIFY should get higher-level event wrappers or remain on the
  existing unsolicited response channel.

P2: generic boxed transport type

- Source: `chatmail/async-imap` #18.
- Problem: applications that choose between plain TCP and TLS streams at runtime
  need a single erased client/session type without building their own wrapper.
- Status: fixed locally with `ImapTransport`, `BoxedTransport`, `BoxedClient`,
  and `BoxedSession`.

P3 or skip for now

- `chatmail/async-imap` #119 and #84: Gmail OAuth2 example waits for greeting.
  Example/documentation issue; not relevant unless we keep examples.
- `chatmail/async-imap` #113, #40, #43, #10, #8 and `djc/tokio-imap` #2, #24,
  #31: docs, examples, CI, or broad maintenance items. Defer.
- `chatmail/async-imap` #99 and #100: SCRAM and TLS channel binding. Important
  protocol work, but broader than IMAP consolidation and probably belongs in a
  SASL/TLS authentication design pass.
- `chatmail/async-imap` #126 and `djc/tokio-imap` #186: IMAP4rev2. Track as a
  larger protocol feature after rev1 behavior is stable.
- `chatmail/async-imap` #11: UIDPLUS. Parser and several client pieces already
  exist locally; `APPENDUID` and `COPYUID` are now surfaced by the relevant
  commands, and `expunge_deleted_uids` covers the common UID EXPUNGE fallback
  workflow.
