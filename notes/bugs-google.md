# Bug hunt: bifrost-google

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/google/` - Gmail history-id sync, Pub/Sub push, mutation pipeline,
People contacts, Google Calendar. All 24 source files read against
`reference/google.md` and the error-model contract.

## Overall verdict

The crate is in unusually good shape. The reference doc is accurate against the
code in every place checked - cancellation discipline, the push actor's
commit-after-response guard, the mutation driver's uncertain-lane shutdown,
bisection, checkpoint anchoring, coverage obligations, the Drive session
cancel, and the quota table are all implemented as documented and pinned by
tests that bite. No unsound concurrency, no resource leaks, no contract
mismatch with bifrost-sync found. What remains is a short list of genuine
latent defects and smaller observations.

## Confident defects

(Finding 1 - the comma-split address-list parser corrupting quoted display
names on `draft_update`'s write path - is fixed: `split_address_list` now
splits on top-level commas only (quote-, escape- and angle-aware, mirroring
the imap crate's parser) and `parse_address` unescapes `\"` / `\\` in quoted
names. Pinned by `address_list_respects_quoted_commas_and_escapes` with
revert-and-confirm; `reference/google.md` updated.)

## Suspected defects (verify before filing as fixes)

### 3. `events_in_range` combines `orderBy=startTime` with `showDeleted=true`

`crates/google/src/account/calendar.rs` line ~91. Google's docs bless
`showDeleted=true` with `singleEvents=true`, and `orderBy=startTime` requires
`singleEvents=true` - but there are long-standing reports of the live API
answering 400 "The requested ordering is not available for the particular
query" for certain `showDeleted`+`orderBy` combinations, and cancelled
instances have no `start` to order by (only `originalStartTime`, which the
projection itself has to substitute). The scripted tests cannot catch a
live-API refusal. Worth one live-API probe; if Google rejects it, every
production range read fails, which would be a top-severity defect hiding behind
a hermetic test suite.

## Minor observations / smells

(The `get_stream` label-refresh failure terminator scoping its error to
`ids[0]` - an arbitrary message the failing `labels.list` never mentioned,
while the other 31 ids of the drained batch vanish into the `Terminated` - is
fixed: the terminator now uses a new `GmailErrorContext::hydrate_labels()`
carrying `ErrorScope::Account` and `GmailResource::Label`, keeping
`AccountOperation::HydrateMessage`. No per-id lanes are manufactured - nothing
was transmitted for those ids, so "Terminated means unreported" is unchanged,
and there is no message-collection `ErrorScope` to name the batch. Pinned by
`a_failed_label_refresh_terminates_without_borrowing_one_id_of_the_batch` with
revert-and-confirm; `reference/google.md` updated. The same mis-scoping in
`repair_inventory`'s label-refresh terminator - it reused
`GmailErrorContext::inventory()`, whose `resource: Message` classified a
`labels.list` 404 as a missing MESSAGE and whose `Cursor(CursorScope::Account)`
scope is the coordinate bifrost-sync routes cursor directives by - is fixed the
same way with a `repair_labels()` sibling. Pinned by
`a_failed_repair_label_refresh_names_the_label_resource_not_the_cursor`, whose
scope and resource halves were each revert-and-confirmed separately. The
`unsupported(SyncInventory)` region refusal in the same function still uses
`inventory()` and was left alone.)
