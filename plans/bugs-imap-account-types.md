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

## Gaps and smells

### G8 - push subscription lifecycle has small races

`account/push.rs` has three issues:

- unsubscribing an unknown handle while the scope map is empty stops the
  IDLE task;
- a new subscription can start a second IDLE loop while the cancelled
  loop is still unwinding;
- folder choice comes from HashMap iteration and is nondeterministic.

### G11 - `discover_memberships` emits duplicate mailbox memberships

Every shared folder emits `MembershipScope::Mailbox(owner)`. Many
folders owned by one principal therefore produce many identical batch
items. Dedupe before emission.

### G12 - failed `UID EXPUNGE` leaves `\Deleted` set with misleading diagnostics

`bulk_destroy` reports failure after STORE succeeded and EXPUNGE failed,
but the message remains flagged deleted. The current error detail says
STORE failed. Add an EXPUNGE-specific error that reports the partial
side effect honestly.

### G13 - mailbox events do not refresh attributes on known folders

`FolderRegistry::apply_mailbox_event` refreshes create, rename, and
delete-then-create only. A known folder that gains `\NoAccess`,
`\Noselect`, or a SPECIAL-USE role keeps stale attributes until reopen.
Decide whether live attribute refresh belongs in the folder lifecycle
contract.

### G14 - `CompactUidSet::len` means UID cardinality

The method expands range cardinality rather than returning the range
count and has no `is_empty` companion. `uid_count()` would communicate
the contract more clearly.

## Remaining coverage work

The highest-value missing coverage is byte-level account streaming over
the existing in-memory driver pair. `Pool::from_connection` or a
test-only `ImapAccountParts` constructor would allow canned transcripts
to cover QRESYNC, CONDSTORE, Basic strategy dispatch, VANISHED/FETCH
deduplication, downgrade paths, and checkpoint emission.

The full `account/error.rs`, crate `error.rs`, and their recovery mapping
tests still merit a dedicated audit against `reference/error-model.md`.
ManageSieve and submission were checked only for unsafe text ingress, not
for their full logic.
