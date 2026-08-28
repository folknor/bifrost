# bifrost-google: hunt findings

Scope: `crates/google/` - Gmail history-id seeded sync, Cloud Pub/Sub push with
renewer and health stream, mutation pipeline with flag canonicalization and TRASH
fallback, Google People contacts, Google Calendar.

Hunter note: read the full account layer (mod/changes/push/mutation/flags/scopes/
inventory/cursor, plus the mutation-error helpers and the calendar collection
paths). Read-only, nothing edited.

## Correctness defects

### G1. `scope_lifecycle_stream` silently loses label Created/Deleted/Renamed events

**High confidence.** `crates/google/src/account/scopes.rs`, the unfold body: each
poll does `let old = snapshot(&state.cache)` and then `refresh_scope_snapshot(...)`,
diffing against whatever is *currently* in the shared `ScopeCache`. But that cache
is also written by `labels_for_flags` and `discover_memberships` - every flag
mutation, every hydration batch, every inventory prelude refreshes it whenever it
is older than five minutes. When any of those refresh between two 30-second
lifecycle polls, the lifecycle stream's `old` already contains the new label set,
`diff_snapshots` returns empty, and the create/delete/rename is never announced.
The consumer's container list then silently diverges until something else forces a
re-read. The fix is that the lifecycle stream must own its own last-emitted
snapshot in `LifecycleState` rather than reading the shared cache; sharing a cache
for "current vocabulary" and for "previous state to diff against" is the structural
mistake.

### G7. Multi-page history walk can skip changes at the checkpoint

**Medium confidence.** `changes.rs` checkpoints the *last* page's
`response.history_id`, which is the mailbox's current history record at the time of
that request, not a marker for how far the walk actually read. Records created while
pages 2..n were being fetched are past the snapshot `history.list` is serving but
under the id being checkpointed, so they are in neither the emitted batches nor any
future walk. On a busy mailbox with a multi-page backlog this is a silent skip. The
safe value is the `historyId` observed on the *first* page of the walk (replay is
idempotent; skip is not), or the schema bump to a page-token cursor the module doc
already sketches.

### G8. `events_in_range` cannot report cancelled instances

**Medium confidence.** `calendar.rs`, line ~91:
`singleEvents=true&orderBy=startTime` with no `showDeleted`. Google omits cancelled
instances under that combination, so a consumer re-reading a range after an
organizer cancels a single occurrence of a recurring event sees the instance simply
absent from the page - indistinguishable from a page boundary - and has no signal to
delete its local copy.

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

## Shape it should have had

Two recurring root causes, worth naming above the individual bugs.

First, **`ScopeCache` is doing two incompatible jobs** - "current label vocabulary
for canonicalization" and "previous state for lifecycle diffing" - and G1 falls
straight out of that. Split them: the vocabulary stays a shared refresh-on-stale
cache with single-flight; the lifecycle stream keeps a private `last_emitted:
Option<ScopeSnapshot>` and never reads the shared one.

Nothing here requires removing published API.

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
