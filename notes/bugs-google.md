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

### G2. Renewer never restarts after a terminal renewal failure; the Gmail watch then lapses silently

**High confidence.** `push.rs::start_renewer` bails on `if guard.is_some() {
return; }`, but the stored `JoinHandle` is only cleared by `clear_watch_state`
(successful `push_unsubscribe` or `close_watch`). The renewer's *terminal* exit
path (`account_error.recovery().is_terminal()` -> `report_health(Terminated)` ->
`return`) leaves a finished handle in `renewer`. A later `push_subscribe` - exactly
what the engine does after recovering from `AuthLost` - issues `users.watch`,
stores the new expiration, and then no-ops on the renewer. Seven days later the
watch expires, push dies with no event, and the account degrades to poll-only with
nothing reporting it. `guard.as_ref().is_some_and(|h| !h.is_finished())` is the
minimal fix; the real fix is a renewer that owns its own restart rather than an
ambient `Option<JoinHandle>` guarding a spawn.

### G3. `push_subscribe` after `close()` creates an orphan watch

**Medium-high confidence.** `push_subscribe` never checks `closed`/`shutdown`.
Post-close it takes the lifecycle mutex, issues `users.watch`, and spawns a renewer
whose very first `select!` arm is an already-cancelled token - so the renewer
returns immediately. The result is a Gmail-side watch nobody renews, nobody stops,
and no `close()` will ever retire (the handle set was cleared, and the account is
done). It should reject with the same `Unsupported`/closed error path, under the
lifecycle mutex.

### G4. A `batchModify`/`batchDelete` 404 marks all 1000 ids as failed

**High confidence.** `error.rs::mutation_error` fans one `AccountError` out to every
id in the batch, and `terminates_mutation_stream` deliberately lets `NotFound`
through to that fan-out. But Gmail fails the whole batch when *one* id is missing.
So one deleted message causes 999 live messages to report
`Failed(NotFound(Message))` - their mutation was never applied and the engine is
told the object is gone. This is the "reports a state it did not achieve" shape,
inverted. It needs a fallback: on 404 for a `Move`/`SetFlags` batch, re-drive the
batch as per-id `users.messages.modify` (or bisect) so only the genuinely absent
ids take the `NotFound` lane.

### G5. `FlagOp::Set` and `\Draft` always produce a Gmail-rejected patch

**Medium-high confidence.** `flags.rs::patch_for_set` unconditionally puts `DRAFT`
in either `add_label_ids` or `remove_label_ids` for every `Set` (see the `for flag
in [FLAG_FLAGGED, FLAG_DRAFT, FLAG_IMPORTANT]` loop), and `flag_to_add_label` maps
`\Draft` -> add `DRAFT`. Gmail's `messages.modify`/`batchModify` refuse `DRAFT` and
`SENT` in both label lists and answer 400. So every `bulk_set_flags(FlagOp::Set(..))`
against real Gmail fails the entire batch as a malformed request. The test suite
never catches it because `translate_flag_op` is tested in isolation with no wire.
`DRAFT` should be excluded from the reverse translation entirely (it is a read-only
projection), the same way `CATEGORY_*` is already exempted from `Set` re-derivation.

### G6. One unsupported flag poisons a 1000-message batch, permanently

**High confidence, partly by design.** `apply_label_patch` fails every id when
`patch.unsupported_flags` is non-empty, and the classification is
`Request(Malformed)` -> `ClientBug` -> terminal. An IMAP-origin keyword the Gmail
vocabulary cannot express (`$Junk`, `$Phishing`, any cross-provider keyword)
therefore means the `\Seen` and `\Flagged` halves of that same op never reach Gmail
either, forever, with no retry class that would ever fix it. The reference documents
this as a decision, but the decision costs the *representable* part of the mutation.
The better shape is to apply what translates and surface the untranslatable flags as
a `Warning` plus a `Downgraded` lane - the machinery for exactly that already exists
for the TRASH fallback.

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
- **G14. `store_watch_response` is two independent mutex acquisitions**, so
  `renewal_delay` can read a new `history_id` against a stale `expiration`.
  `PubSubControl` has five separate mutexes over what is one piece of state; a single
  `Mutex<WatchState>` would remove that class of interleaving outright.
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

Second, **`PubSubControl` is an ambient bag of mutexes that the renewer, subscribe,
unsubscribe, and close all poke at from outside**, and G2/G3/G14 are all
consequences. The watch lifecycle is a state machine with four states (Unwatched /
Watched{expiry} / Renewing / Retired) and it should be one owned actor task holding
that state exclusively, with `subscribe`/`unsubscribe`/`close` as messages to it.
That makes "did the renewer exit" unrepresentable rather than a `JoinHandle` someone
has to remember to clear, and makes "subscribe after close" a rejected message
rather than an unchecked path.

Nothing here requires removing published API. G5 and G6 change wire behavior of
`bulk_set_flags` but not its signature; G4 adds a fallback path rather than deleting
one.

## Out-of-scope observation

`bifrost_net::test_support::scripted_account` gives this crate a perfectly good
scripted-transport seam, yet `reference/google.md` asserts "the crate has no
scripted-transport seam" as the reason `apply_destroy` has no end-to-end test. That
claim is stale - `push.rs`, `changes.rs`, and `inventory.rs` all use exactly that
seam - and it is currently being used to justify a gap in coverage of the TRASH
fallback path.
