# bifrost-imap: `account/**` + `types/**` remaining bug hunt

Scope: `crates/imap/src/account/**`, `crates/imap/src/types/**`,
`crates/imap/src/error.rs`, `error_tests.rs`, and `lib.rs`.

This document contains only current gaps. Finding IDs stay stable so code
comments and review discussion can refer to the original audit without
renumbering.

## Bugs

### B5 - a partially conflicting `FlagOp::Patch` reports full success for half-applied messages

`account/mutate.rs:apply_patch`.

`FlagOp::Patch { add, remove }` expands into two STORE commands. The
guarded add command can return tagged OK with `MODIFIED` UIDs, meaning the
add landed for every non-conflicting UID. `apply_patch` returns early on
that outcome, so the remove command never runs for those UIDs.
`mutation_results` then reports the non-conflicting UIDs as
`Succeeded(Applied)` even though only half of their requested patch
landed.

The clean fix is to expand patch handling where the requested UID vector
is available:

1. Run the guarded add against the whole group.
2. Subtract conflicts with `applied_uids_after_store`.
3. Run the unguarded remove against the applied subset.
4. Merge both command outcomes without reporting partial application as
   success.

The conservative fallback is to report the non-conflicting UIDs as
`Uncertain`, which at least routes them through engine read-back.

### B8 - `Projection::Preview` returns headers and drops preview text

`account/get.rs:attrs_for_projection` and `fetch_to_hydrated`.

Preview requests `RFC822.HEADER` followed by a partial `BODY[TEXT]`.
Both decode into `body_sections`, but hydration takes only the first
section containing data. Servers normally answer in request order, so
the returned `RawMime` contains headers and no snippet.

The result should combine the returned header and text sections into a
parseable MIME payload. The existing
`preview_hydration_keeps_only_the_first_returned_section` test documents
the current bug.

### B9 - full hydration puts raw RFC 5322 source into `Message::body_text`

`account/pim.rs:attrs_for_hydration` and `fetch_to_message`.

Full hydration fetches `BODY[]`, then converts the first section to a
lossy UTF-8 string and stores the entire raw message in `body_text`.
`body_html` is always `None` and attachments are always empty. Multipart,
base64, and quoted-printable messages therefore surface wire source
instead of decoded content.

The correct fix is to parse the fetched octets with the shared
`bifrost-types::mime` machinery and populate text, HTML, and attachments.
A narrower `BODY[TEXT]` change would remove headers but would still be
wrong for multipart and transfer encodings. The existing
`full_hydration_puts_the_whole_raw_message_in_body_text` test documents
the current bug.

### B11 - `get_stream` silently drops stale or missing targets

`account/get.rs:run_folder_get`.

Targets whose UIDVALIDITY differs from the freshly selected mailbox are
filtered out without any `ItemOutcome`. Requested UIDs that the server
does not return are also absent from every lane. This violates the
streaming form of the three-lane accounting contract.

Mirror the mutation path:

- partition stale targets and emit per-item `Failed` outcomes carrying a
  UIDVALIDITY-changed error;
- reconcile requested UIDs against returned FETCH data and emit an
  explicit outcome for missing messages, normally `Failed(NotFound)`.

### B13 - an invalid bulk-move destination is reported `Uncertain`

`account/mutate.rs:run_folder_mutation`.

`MailboxName::new` validates the shared destination inside each
per-folder operation. An invalid destination escapes through `?`, and
the outer stream converts the folder error to `Uncertain` even though no
mutation command was transmitted.

Validate the destination once before the folder loop and emit per-item
`Failed(Request(Malformed))` for the whole batch on rejection.

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

### G9 - the LIST-STATUS double-strip bug is not reachable from `account/`

`connection/helpers.rs` corrupts the first STATUS item while parsing a
LIST-STATUS return option. The account layer currently requests only
`SPECIAL-USE`, so the defect is dormant here. It becomes live if folder
listing is optimized to use LIST-STATUS.

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
