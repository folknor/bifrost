# bifrost-google: hunt findings

Scope: `crates/google/` - Gmail history-id seeded sync, Cloud Pub/Sub push with
renewer and health stream, mutation pipeline with flag canonicalization and TRASH
fallback, Google People contacts, Google Calendar.

Hunter note: read the full account layer (mod/changes/push/mutation/flags/scopes/
inventory/cursor, plus the mutation-error helpers and the calendar collection
paths). Read-only, nothing edited.

## Structural / performance

- **G9. `get_stream` hydrates its batch serially.** `inventory.rs`, `for id in ids {
  hydrate_one(...).await }` - 32 sequential round trips per batch, on the engine's
  primary hydration path, while `inventory_stream` right above it uses
  `buffer_unordered(32)` for the identical work. The single largest throughput defect
  in the crate; the fix is mechanical.
- **G10. `canonical_flags` rebuilds the whole label `HashMap` per message.**
  `flags.rs` line 96. Called once per message in inventory (500/page), once per
  hydration, once per repair. The vocabulary is already an `Arc<Vec<GmailLabel>>` at
  every call site; the index should be built beside it, once.
- **G11. Inventory obligations are O(n^2).** `coverage_of(&scope, &obligations)` is
  called for every 32-item sub-batch and clones the entire growing obligation vector.
  A mailbox with a few thousand unreadable messages makes the walk quadratic in
  obligations and holds every `AccountError` for the whole pass.
- **G12. `bytes_in: 0` everywhere.** changes, mutation, inventory, get_stream, scopes
  all hard-code zero. The engine's byte accounting and any bandwidth cap derived from
  it are blind for this provider. `blobs.rs` is the only place that populates it.
- **G13. Streams are not wired to `shutdown`.** `changes_stream`, `inventory_stream`,
  `get_stream`, and the mutation driver take no `CancellationToken`. After `close()`
  the transport is detached by `DetachOnDrop` but any in-flight sync stream keeps
  calling Gmail through a deregistered `AccountNet` - unmetered by the governor, and
  failing in a way that classifies as a transport error rather than "account closed".
  Only `push_stream` and `scope_lifecycle_stream` observe the token.
- **G15. `labels_for_flags` has no single-flight.** A stale cache plus a wide
  `buffer_unordered` fan-out means N concurrent `labels.list` calls, each 1 quota
  unit, each racing to overwrite the cache.

## G16. The paging guard covers a LIST walk and neither primary sync walk

Confidence: high (whole-crate enumeration, done in round 3). Round 3 fixed one
half of this and deliberately left the rest; the residue is a live finding, not
a closed one.

### Where this came from

`notes/carry-forward.md` handed this document an open question out of the
bugs-graph arc. `crates/graph/src/.../paging.rs` cites bifrost-google's
`calendars_list` as having independently learned the paging lesson, and the
question was whether Google's DELTA and HISTORY walks got the same guard or
only its LIST walks. In graph the answer was a split: `PageWalk` bounded six
incidental traversals and NEITHER of the two delta sync loops, which were the
crate's PRIMARY sync loops - while `reference/graph.md` claimed the guard was
universal. The same split exists here.

### The enumeration

Bounded, with repeated-token detection and a 10,000-page budget:

- Calendar `calendars_list` (`calendar.rs`, `MAX_CALENDAR_LIST_PAGES`).
- Gmail `changes_stream` (`changes.rs`, `MAX_HISTORY_PAGES`) - added by round 3,
  see below.

Unbounded - follows `nextPageToken` until the field is absent, with no budget
and no repeated-token detection:

- **Gmail `inventory_stream`** (`inventory.rs`). The crate's primary object
  enumeration. Also has no durable mid-walk resume, so an interrupted pass
  restarts from the beginning.
- **People `address_books_list`** (`contacts.rs`, `contactGroups.list`).
  Returns no partial result and no resume cursor, so a cycling token is an
  unbounded loop that can never produce an answer.

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

### What round 3 fixed, and what it did not

Round 3 bounded `changes_stream` only, because `changes.rs` was already its own
file (G7 rewrote the checkpointing there) and bounding a loop whose checkpoint
semantics you just changed belongs in the same change. Refusal TERMINATES with
`Protocol(ContractViolation)`; it does not truncate. This matters more than the
bound: `changes_stream` checkpoints only on its final page, so stopping early
and reporting normal completion would emit a checkpoint claiming coverage the
walk never read. A `Terminated` costs a redone walk and claims nothing.

**Gmail `inventory_stream` and People `address_books_list` are left to round 4,
which owns `inventory.rs`.** Do not treat them as accepted. What a fix needs:

- A page budget and a `HashSet` of seen tokens, as in `walk_refusal`
  (`changes.rs`) and `calendars_list` (`calendar.rs`). The budget is a refusal
  boundary, not a paging policy - set it far above any real account.
- A refusal that classifies and terminates. For `inventory_stream` that is
  `SyncEvent::Terminated`; for `address_books_list`, which returns a `Result`,
  an `Err`. Neither may return a short list that reads as complete.
- Care with `inventory_stream` specifically: it is the coverage-obligation
  producer, so a truncated inventory that reports normally would understate
  coverage debt - the same lie in a different currency.

### Documentation

`reference/google.md` carried no universal-guard claim (the false claim graph
had); round 3 added an explicit per-site "Paging inventory" section stating that
Google has no universal guard, and it is kept in step with the enumeration
above. If a later round bounds inventory or contact groups, update that section
in the same commit.

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
