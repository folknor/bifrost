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
- Fixed the streamed command status path for `FETCH`, `STORE`, `EXPUNGE`,
  `LIST`, `SEARCH`, `ID`, `GETQUOTA`, `GETQUOTAROOT`, `GETMETADATA`, and
  `NOOP` style parsers. Tagged `NO` or `BAD` now reaches callers instead of
  being hidden by the old `take_while` stop condition.

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
- Likely approach: keep `flags()` for compatibility and add a presence-aware
  accessor, probably `flags_opt()` or `flags_present()`, returning `Option<impl
  Iterator<Item = Flag<'_>>>` or a small wrapper. Need to avoid awkward lifetimes.

P0: SASL capability API

- Source: `chatmail/async-imap` #45.
- Problem: CAPABILITY is valid in any state, and authentication selection needs
  direct access to advertised `AUTH=<mechanism>` SASL mechanisms.
- Status: initial API added locally. Continue to review naming and whether auth
  selection deserves a higher-level helper once the SASL implementation shape is
  clearer.

P2: parser hardening for recursion

- Source: `djc/tokio-imap` #90.
- Problem: recursive body parsing can stack overflow on hostile or fuzzed input.
- Likely approach: introduce explicit recursion depth limits around recursive
  body/body-extension parsing. This should be a bounded parser behavior change
  with tests.

P2: mailbox/parser edge cases

- Source: `djc/tokio-imap` #171 and `chatmail/async-imap` #71.
- Status: reports need reduced repros against current vendored parser. Do not
  chase without failing local tests. #71 may overlap with literal/body section
  parsing and current parser improvements.

P2: IDLE ergonomics and NOTIFY

- Sources: `chatmail/async-imap` #55, #89, `djc/tokio-imap` #18.
- Status: local crate already has IDLE. NOTIFY is not implemented. Main need is
  ergonomic examples or API review, not first priority while consolidating core
  command correctness.

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
  exist locally; audit later for API completeness around APPENDUID/COPYUID/MOVE.
