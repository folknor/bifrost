# bifrost-imap: `account/**` + `types/**` remaining bug hunt

Scope: `crates/imap/src/account/**`, `crates/imap/src/types/**`,
`crates/imap/src/error.rs`, `error_tests.rs`, and `lib.rs`.

This document contains only current gaps. Finding IDs stay stable so code
comments and review discussion can refer to the original audit without
renumbering.

## Bugs

### B9 - full hydration puts raw RFC 5322 source into `Message::body_text`

`account/pim.rs:attrs_for_hydration` and `fetch_to_message`.

Full hydration fetches `BODY[]`, then converts the first section to a
lossy UTF-8 string and stores the entire raw message in `body_text`.
`body_html` is always `None` and attachments are always empty. Multipart,
base64, and quoted-printable messages therefore surface wire source
instead of decoded content.

OPEN, blocked on a cross-crate prerequisite. The correct fix is to parse
the fetched octets with shared `bifrost-types` MIME machinery and populate
text, HTML, and attachments; `bifrost-types::mime` only serializes outgoing
RFC 5322 messages today, so the shared inbound parser/decoder must land
first. That prerequisite is filed as **types-G2** in `TODO.md` (symptom,
what bifrost-types must ship, what was done here, what remains wrong).
A narrower `BODY[TEXT]` change would remove headers but would still be
wrong for multipart and transfer encodings. The existing
`full_hydration_puts_the_whole_raw_message_in_body_text` test documents
the current bug.

## Remaining coverage work

The byte-level harness now exists: `account/scripted_tests.rs` builds a
real `ImapAccount` over a `connection::test_support::driver_pair` and
drives account entry points against canned transcripts (pool-permit
discipline and the selected-mailbox deselect fallback are covered there).
What is still uncovered through it is the sync half: QRESYNC, CONDSTORE,
and Basic strategy dispatch, VANISHED/FETCH deduplication, the downgrade
paths, and checkpoint emission.

The full `account/error.rs`, crate `error.rs`, and their recovery mapping
tests still merit a dedicated audit against `reference/error-model.md`.
ManageSieve and submission were checked only for unsafe text ingress, not
for their full logic.
