# bifrost-google: hunt findings

Scope: `crates/google/` - Gmail history-id seeded sync, Cloud Pub/Sub push with
renewer and health stream, mutation pipeline with flag canonicalization and TRASH
fallback, Google People contacts, Google Calendar.

Hunter note: read the full account layer (mod/changes/push/mutation/flags/scopes/
inventory/cursor, plus the mutation-error helpers and the calendar collection
paths). Read-only, nothing edited.

## Round 4 closure

G9, G10, G11, G13, G15 and the remainder of G16 were resolved in the final
round. Hydration now overlaps a full 32-item batch while retaining each id on
its result. Inventory builds its label-name index once per vocabulary snapshot
and emits one coverage report per provider page, rather than rebuilding the
index and cloning a growing obligation ledger for every 32-item sub-batch.
Every long-running sync and mutation stream observes the account shutdown token
around provider and input waits. Shared stale label refresh is single-flight.
Both remaining internally driven paging walks refuse repeated tokens and a
10,000-page budget breach; inventory emits only `Terminated`, while People
returns `Err`, so neither can publish a truncated prefix as complete.

The cold review of that work found G17 below, which the same round fixed: a
mutation batch already on the wire when shutdown fires now reports every
consumed id `Uncertain` instead of vanishing.

**Status: no open findings.** Every entry in this document is fixed, refuted
with its evidence, or recorded as an accepted residual with the reason it is not
a defect. The durable content lives in `reference/google.md`; the machinery,
refutations and testing traps a later arc must not relitigate are summarized in
`notes/carry-forward.md`.

## G17: the round-4 cancellation fix lost in-flight mutation evidence

Found by the round-4 cold review; the round's own fix created it, which is the
arc's recurring shape (a fix opening a hole one layer up, in a consumer of what
it changed). G13 asked the mutation driver to observe shutdown, and the fix
wrapped the DISPATCHED `batchModify` / `batchDelete` in a `select!` whose
cancellation arm returned `None`. Gmail may have accepted some or all of those
writes; the stream ended reporting the consumed ids in no lane at all, so
`close()` could silently lose writes.

Fixed by following the transmission evidence rather than the loop that caught
the error. Cancellation still preempts freely BEFORE dispatch (nothing was
transmitted, nothing is owed). Once dispatch has begun, the driver emits every
id of the batch as `ItemOutcome::Uncertain` on a `PageBoundary::Page` batch and
then `Terminated` with `Transport(Network)` plus a secondary
`AttemptCause(InFlight)` - the same rule bifrost-imap already applies, and the
lane that queues for read-back rather than asserting the write did not land.

Two consumers of that fix had to move with it:

- The stream head tested the shutdown token BEFORE draining `pending_event`, so
  the terminator parked behind the uncertain lanes would have been swallowed by
  the very token that produced it. The drain now comes first.
- The `select!` is `biased` toward the operation, so a request that has already
  answered is classified normally when the token fires in the same poll.

The other three stream families wired this round were checked for the same
shape and do NOT have it: a dropped in-flight READ loses no writes, and every
cancellation arm in `inventory_stream`, `get_stream` and `changes_stream`
returns before yielding anything, so none can emit `Done`, a final page
boundary, a checkpoint or a coverage report on the way out.

## G12 refuted: byte accounting was already repaired by the net arc

The finding no longer reproduces. `changes_stream`, `inventory_stream`,
`get_stream`, membership discovery, and the mutation driver each use
`GmailClient::metered` and publish `ByteTally::take()` per emitted batch.
`GmailClient::send_recorded` attaches `RequestBuilder::count_bytes_into`, so
failed requests and caller-built batch requests are included too. The remaining
literal zero in `discover_cursor_scopes` is correct because that stream is a
constant and performs no request. Locally rejected mutation lanes likewise make
no request and emit no fabricated traffic.

Re-verified independently in round 4 by enumerating every `bytes_in` site in
`crates/google/src`. Three shapes exist and all three are honest: `tally.take()`
fed from `send_recorded`'s request-local `count_bytes_into` counter (changes,
inventory, hydration, membership discovery, mutation), the exact transferred
size in `blobs.rs`, and two documented no-request constants
(`discover_cursor_scopes`, and a `discover_memberships` cache hit). The only
requests that bypass `send_recorded` are the two Drive resumable-upload calls in
`cloud.rs`, built directly off `account_net()` - they feed no `bytes_in` field
at all, so they under-report nothing; recorded in `reference/google.md` so it is
not rediscovered as a hole. G12 stays refuted.

## G16 closure evidence

Round 3 bounded history paging. Round 4 bounded Gmail inventory and People
address-book paging with unit-testable refusal helpers, repeated-token tests,
and budget tests. The inventory guard runs before hydration or emission of the
page in hand and therefore cannot emit `Done`, `PageBoundary::Final`, a
checkpoint, or a coverage claim for refused work.

### Where this came from

`notes/carry-forward.md` handed this document an open question out of the
bugs-graph arc. `crates/graph/src/.../paging.rs` cites bifrost-google's
`calendars_list` as having independently learned the paging lesson, and the
question was whether Google's DELTA and HISTORY walks got the same guard or
only its LIST walks. In graph the answer was a split: `PageWalk` bounded six
incidental traversals and NEITHER of the two delta sync loops, which were the
crate's PRIMARY sync loops - while `reference/graph.md` claimed the guard was
universal. The same split exists here.

### Final enumeration

Internally traversed and bounded, with repeated-token detection and a
10,000-page budget:

- Calendar `calendars_list` (`calendar.rs`, `MAX_CALENDAR_LIST_PAGES`).
- Gmail `changes_stream` (`changes.rs`, `MAX_HISTORY_PAGES`).
- Gmail `inventory_stream` (`inventory.rs`, `MAX_INVENTORY_PAGES`).
- People `address_books_list` (`contacts.rs`, `MAX_ADDRESS_BOOK_PAGES`).

Page-resumable - one provider request per call, returning the provider token,
so the caller bounds the walk:

- Gmail `search` / `search_messages`.
- People personal-contact list, other-contact list, contact search,
  other-contact search, autocomplete, directory search. None clips an
  over-delivered page, so none needs an intra-page offset.
- Calendar `events_in_range`, and a search constrained to one calendar.

Partly resumable:

- Cross-calendar event search resumes by calendar id AND provider page token,
  but NOT within an over-delivered page. It relies on Google honouring
  `maxResults` and defensively clips a loose page, so an over-delivery is
  discarded rather than resumed. Low severity; recorded so it is not
  rediscovered as a hole.

### Documentation

`reference/google.md` carries an explicit per-site "Paging inventory" section,
kept in step with the final enumeration above.

### Lateral, not fixed

`events_search_url` (`calendar.rs`) sends `singleEvents=true` with no
`showDeleted`, exactly as `events_in_range` did before G8. Not filed as the same
defect: a range reread is a coverage question, where a missing tombstone is
indistinguishable from a page boundary, while a SEARCH returning cancelled
instances is a product decision about what a query surface should answer. Worth
a deliberate answer, not a reflex copy of the G8 fix.

## Carried forward from round 2

Round 2 landed G4, G5 and G6. Two residual items came out of it and are NOT
defects the round left unfixed - they are known, bounded, and documented in
`reference/google.md`. Recorded here so a later round does not rediscover them
as bugs.

- **A `FlagOp::Set` that OMITS `\Draft` against a message that really is a draft
  reports success it did not achieve.** `\Draft` asserted in a Set now lands in
  `unsupported_flags` and is reported through the partial-application path; its
  omission is deliberately silent, because draft-ness is structural in Gmail and
  every ordinary exact set omits it, so reporting each one would make
  `MutationSuccess::Applied` unreachable for `Set` forever. Closing the residual
  gap needs a read-back the translation layer in `flags.rs` does not have; it is
  a `bifrost-sync` read-back question, not a translation question.
- **A mid-bisection terminal error discards the sub-batches not yet attempted.**
  `apply_label_patch_bisected` emits the ids it already resolved as one non-final
  page and then `Terminated`. Ids never transmitted go unreported, which is what
  `Terminated` has always meant on this driver, and the engine re-issues the
  whole operation. Worth revisiting only if a checkpoint ever lets a mutation
  stream resume mid-batch.
