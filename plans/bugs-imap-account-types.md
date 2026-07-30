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

### G1 - blob support is advertised but production code mints no IMAP blob IDs

`blob_range` is advertised as supported and the openers decode
`imapblob1:` handles, but inventory and hydration emit no `BlobHandle`.
Either derive part handles from BODYSTRUCTURE and expose them, or
advertise `BlobRangeSupport::No` until the surface is reachable.

### G2 - `close()` does not close composed DAV sub-accounts

`account/close.rs` stops IMAP push and the pool without calling
`close()` on the optional CardDAV and CalDAV accounts. Both delegates are
no-ops today, but this becomes a resource leak when either delegate gains
real shutdown work.

### G3 - CONDSTORE baseline seeding issues `UID SEARCH ALL` twice

`account/changes.rs` searches once to seed an incomplete known-UID
baseline, then immediately searches again to calculate a necessarily
empty diff. Reuse the first result or skip the first diff when seeding.

### G4 - `dial_idle()` bypasses pooling and the pool cap

Folder CRUD, quota, draft, sent-copy, refresh, and push paths open fresh
authenticated connections. Non-push callers should use a pooled
`checkout_any()`; the IDLE loop is the legitimate dedicated-connection
case.

### G6 - `SearchCriteria::header` does not validate the field name

`types/search.rs::header` quotes the value but appends the header name
verbatim. Validate the name as an atom or encode it as an astring before
building SEARCH syntax.

### G7 - `mutation_results` carries a dead requested-UID parameter

`account/mutate.rs::mutation_results` accepts `_requested_uids` and never
reads it. Remove the parameter and the temporary vectors that exist only
to populate it.

### G8 - push subscription lifecycle has small races

`account/push.rs` has three issues:

- unsubscribing an unknown handle while the scope map is empty stops the
  IDLE task;
- a new subscription can start a second IDLE loop while the cancelled
  loop is still unwinding;
- folder choice comes from HashMap iteration and is nondeterministic.

### G10 - `encode_blob_id` is test-only while `decode_blob_id` is production

The visibility mismatch is part of G1. Wiring blob handles requires
promoting the encoder; dropping unreachable blob support should remove
the decoder and openers instead.

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
