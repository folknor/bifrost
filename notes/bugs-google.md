# bifrost-google bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/google/` (client/api/error plus
every `account/` module). No builds or tests were run; everything is from reading. Findings are
unverified work material.

## How to read this document (triage pass 2026-08-23)

This is an unverified hunt ledger, not a work queue, and its findings are mixed in kind. Each one
now carries a category on its own line under its heading (or inline, for bullets):

- **C1 live defect** - wrong answer, data loss, hang, or security hole today. Work these.
- **C2 latent defect** - correct today, misbehaves silently when an unhandled case arrives. Work
  these.
- **C3 refactor opinion** - duplication, cost, awkward abstraction. Nothing misbehaves. Backlog.
- **C4 product decision** - what the published surface should be. The repository owner decides;
  the loop must never act on one unilaterally.
- **STALE** - no longer reproduces. Kept in full, with the reason.

**PUBLISHED SURFACE** is an orthogonal marker: the finding's remedy would remove, rename, or
reshape a published item or behavior, and needs the owner's sign-off whatever its category.

Categories were checked against the tree on 2026-08-23; where the check changed the picture the
marker says so. No finding text was altered, compressed, or removed.

Published-surface findings in this document, collected: the `bulk_destroy` downgrade (its remedy
adds a `MutationSuccess` variant), and the calendar endpoint override (its remedy adds a published
`with_calendar_api_base` constructor). Both are additive rather than removals, which makes them the
mild end of the category, but they are still the owner's to approve.

Highest severity, ahead of its position: **a single message 404 terminating the whole inventory
walk is a live data-loss-of-progress defect on the crate's most expensive path, and the Drive chunk
loop can spin forever with no attempt counter.** Both verified against the current tree.

## A single message 404 kills the entire inventory walk

**C1 live defect.** Verified 2026-08-23: the hydration loop's `Err` arm still does
`yield SyncEvent::Terminated(...); return;` with no classification step, while `get_stream` in the
same file fans every hydrate failure into `ItemOutcome::Failed`. Two lanes, opposite policies, one
routine server behavior. No published shape changes to fix it.

`crates/google/src/account/inventory.rs`, `inventory_stream`: the `buffer_unordered(32)`
hydration loop does `Err(error) => { yield SyncEvent::Terminated(...); return; }`. Gmail returns
404 for a message id that was listed and then deleted/moved between `users.messages.list` and
`users.messages.get`, which is routine on a mailbox with hundreds of thousands of messages where
a full walk takes many minutes. One such 404 terminates the whole backfill partition and discards
every page already emitted for it.

`get_stream` in the same file already has the right shape (per-item `ItemOutcome::Failed(BatchFailure)`
so "a single bad id no longer poisons the stream", per the doc). Inventory has the opposite policy
and the reference doc does not mention the divergence. Inventory should fan out per-id failures the
same way; the only failures that should terminate are the ones `terminates_mutation_stream`-style
classification calls terminal.

## The rate limiter is calibrated in requests but Gmail bills in quota units

**C1 live defect.** Verified 2026-08-23: `GOOGLE_API_QUOTA_PER_SECOND = 250.0` with `cost_default:
1` at both registration sites. The consequence is observable today on any real mailbox (sustained
429 backoff on the inventory hot loop), and the capability surface separately advertises a
`QuotaUnits` model the transport does not implement, which is a documentation-versus-code
divergence of the kind this project has been bitten by. The remedy is additive plumbing, not a
surface change.

`crates/google/src/client.rs`: `GOOGLE_API_QUOTA_PER_SECOND = 250.0`, `cost_default: 1`. 250 is
Gmail's per-user quota units per second, not requests per second. `messages.get` costs 5 units,
`messages.list` 5, `messages.modify` 5, `threads.modify` 10, `messages.send` 100, `history.list` 2.
At `cost_default: 1` the limiter will happily sustain 250 rps, i.e. ~1250 quota units/s: 5x over
quota, and 25x on a send-heavy path. Meanwhile `capabilities.rs` advertises
`quota_signal: QuotaUnits` and `rate_limit_class: Tiered`, so the capability surface claims a model
the transport does not implement. The inventory hot loop is the worst case: `buffer_unordered(32)`
of `messages.get` at 5 units each will spend the whole session in 429/`rateLimitExceeded` backoff.

The fix is structural: pass a per-call cost into the bifrost-net rate limiter (Gmail publishes the
table), rather than pretending every request costs 1.

## bulk_destroy reports Applied for messages that were only trashed

**C1 live defect. PUBLISHED SURFACE (additive).** Verified 2026-08-23: the fallback path calls
`apply_label_patch`, whose success arm is `applied_outcomes(ids)`, and
`is_batch_delete_scope_failure` still returns `true` on an unparseable 403 body on both branches.
A permanent delete silently becomes a trash reported as `Applied`, which is a permanent engine
reconcile loop. Note the two halves are separable: narrowing the unparseable-body trigger needs no
surface change at all and can land immediately; only the "distinct outcome" half touches the
published `MutationSuccess` enum and needs the owner.

`crates/google/src/account/mutation.rs::apply_destroy`: when `batchDelete` fails the scope check,
the fallback is a TRASH label patch, and `apply_label_patch` returns `applied_outcomes(ids)`, i.e.
`MutationSuccess::Applied`. The engine is told the destroy succeeded; the messages are still in
Trash and will reappear in the next inventory/history pass, generating a permanent reconcile loop
between "engine thinks destroyed" and "server says exists". Worse,
`is_batch_delete_scope_failure` treats any 403 whose body does not parse as a Gmail envelope as a
fallback trigger (`None => return true` on both branches), so a 403 from a proxy or a policy layer
also silently downgrades a permanent delete into a trash.

At minimum the downgrade needs a distinct outcome (`MutationSuccess::Skipped` plus a Warning, or a
new variant); ideally it should not fire on an unparseable body at all.

## push_unsubscribe leaks the Gmail watch across a process restart

**C1 live defect (both halves).** Verified 2026-08-23: `HandleRemoval::NotPresent` is still folded
into the early `return Ok(())`, and `active_handles` is still in-memory only, so unsubscribe after
a restart is a silent no-op. The second half is confirmed by the code order too: `stop_watch(...)?`
sits above the three state clears, so a failing stop leaves an empty handle set beside a live
renewer.

`crates/google/src/account/push.rs::push_unsubscribe`: `HandleRemoval::NotPresent` returns `Ok(())`
without calling `users.stop`. `active_handles` lives only in the in-memory `PubSubControl`, so
after any reopen/restart the set is empty and a persisted handle is always `NotPresent`. The
consumer's "unsubscribe" is a silent no-op and Gmail keeps pushing to the topic for the remainder
of the 7-day watch. Same class of leak: `GoogleAccount::close()` cancels the renewer but never
calls `users.stop` either, and `Drop` cannot.

Also in the same function: on the `Last` path, if `stop_watch` fails, the handle has already been
removed from the set and neither `expiration`, `last_history_id`, nor the renewer are cleared. The
account is left with an empty handle set and a live renewer that keeps re-issuing `users.watch`
forever. The state teardown should happen regardless of the network result, or the handle should be
re-inserted on failure.

## open_blob_range returns an empty stream on its "unreachable" branch

**C2 latent defect, plus a live doc defect.** Verified 2026-08-23: the branch is still
`let _ = client; Box::pin(stream::empty())` and the comment above it calls itself defensive while
claiming "we slice locally", which it does not. Unreachable today (`supports_range` is hardcoded
false at construction and a test asserts it), so the stream half is latent; the reference doc being
wrong is true right now. Note the finding offers "delete the branch" as one remedy - that is a
private function, not published surface, so it is a legitimate option here rather than the pattern
this triage warns about.

`crates/google/src/account/blobs.rs`: if `handle.capabilities.supports_range` is ever true, the
function returns `stream::empty()`: no `Batch`, no `Done`, no `Terminated`. A consumer awaiting
bytes gets a silently-ended stream. `reference/google.md` claims "The range-supporting branch
(slicing the decoded buffer) exists for trait symmetry"; there is no slicing branch, only
`let _ = client;` and an empty stream. Either delete the branch and always return `Unsupported`, or
implement the slice. The doc is wrong today.

## calendarList is not paginated, so accounts with many calendars silently lose data

**C1 live defect.** Verified 2026-08-23: `calendars_list` still issues one GET and returns
`response.items` with no `nextPageToken` loop. Silent truncation at 100 calendars, and search
inherits it.

`crates/google/src/account/calendar.rs::calendars_list` issues one GET to `/users/me/calendarList`
and returns `response.items`. Google paginates that endpoint (default 100, max 250, with
`nextPageToken`). An account with more than 100 calendars gets a truncated `calendars_list`, and
worse, `search` with no `calendar_id` builds its cross-calendar cursor index from that truncated
list, so events in calendars 101+ are invisible to search and a cursor minted before a list change
can resolve to the wrong index (`decode_cross_calendar_cursor` matches by id, so it errors rather
than mis-indexes; but the truncation itself is silent).

## Calendar's endpoint override is an env var, unlike every other Google surface

**C3 refactor opinion. PUBLISHED SURFACE (additive).** Verified 2026-08-23: the
`std::env::var("RATATOSKR_TEST_GCAL_ENDPOINT")` read is still there and still per-call. Nothing
misbehaves in production, so this is not a defect; the objections are hygiene ones (process-global
state in a library, a bifrost crate naming its downstream consumer, no two-endpoint testing). The
proposed remedy adds a published `with_calendar_api_base` constructor, which is the owner's to
approve even though it is additive. The trailing note about hardcoded rate-limit hosts is **C3**.

`calendar.rs::calendar_api_base()` reads `std::env::var("RATATOSKR_TEST_GCAL_ENDPOINT")` on every
single call. It is the only env-var read in the workspace. Consequences: process-global state a
library should not read, a per-request `getenv` on a hot path, no way to run two accounts against
different calendar endpoints in one process, and a bifrost crate naming its downstream consumer.
This should be a `calendar_base` field on `ClientInner` with a `with_calendar_api_base` factory
method, matching Gmail's `with_api_base` and People's `with_people_api_base`.

Related: `default_account_net` registers rate limits for `www.googleapis.com` and
`people.googleapis.com` by literal string. If the calendar or Gmail base is redirected, requests to
the redirected host are unlimited; that is test-only, but the People host being hardcoded while its
base is configurable is the same asymmetry.

## event_move_url encodes a query value with the path encoder

**C2 latent defect.** Verified 2026-08-23: `event_move_url` still calls
`bifrost_net::url::encode_path_component` on `destination`. Latent because ordinary Google calendar
ids survive the wrong encoder; it bites on ids carrying `+`, `&`, or `=`. Note the finding also
says the test pins the wrong encoder, which is the "test with no bite" shape this project has been
caught by three times - fix both.

`calendar.rs`:
`format!("{}/move?destination={}", event_url(...), bifrost_net::url::encode_path_component(target_calendar_id))`.
`destination` is a query parameter and must go through `encode_query_value`. Path and query
encoders differ on exactly the characters that matter in a Google calendar id (`+` decoding to
space, `&`/`=` truncating). The whole rest of the file uses `encode_query_value` for query
positions; this is the one slip, and its test pins the wrong encoder.

## move_placement_patch sends the caller's casing to addLabelIds

**C2 latent defect.** Verified 2026-08-23: removal matching is still `eq_ignore_ascii_case` while
`add_label_ids` gets `destination.to_string()` verbatim. The two halves of one function disagree
about whether label ids are case-sensitive, and Gmail says they are. The finding's own note that a
test asserts the current behavior as correct is the important part: that test has to change with
the code, so do not read a green suite as evidence against this.

`crates/google/src/account/flags.rs`: exclusive-container removal matching is
`eq_ignore_ascii_case`, but the destination is pushed into `add_label_ids` verbatim.
`move_placement_patch("inbox")` produces `addLabelIds: ["inbox"]`, which Gmail rejects (label ids
are case-sensitive on the wire). The test `exclusive_container_match_is_case_insensitive` asserts
this behavior as if it were correct. Either normalize a case-insensitive system-container match to
its canonical spelling before adding, or drop the case-insensitive comparison entirely and require
exact ids.

## get_stream never emits PageBoundary::Final

**C2 latent defect (contract), downgraded by verification.** The symptom is confirmed:
`get_stream` hardcodes `page_boundary: PageBoundary::Page` on every batch and ends with
`Done(None)`. The finding asks for the consumer to be checked, so it was: as of 2026-08-23
`bifrost-sync` reads `PageBoundary::Final` in no hydration path - `fusion.rs` and
`backfill/runner.rs` only propagate `batch.page_boundary` through, and the sole `Final` literal in
the crate is in a test. So nothing breaks today and this is a contract inconsistency with a fuse on
it, not the live break the finding's "medium confidence" allowed for. The sub-point about
`inventory_stream` emitting no `Final` batch when the last page hydrates to zero items is also
confirmed, same category.

`inventory.rs::get_stream` hardcodes `page_boundary: PageBoundary::Page` on every batch and then
emits `Done(None)`. Both the mutation driver and `inventory_stream` do one-item lookahead
specifically so the last batch is marked `Final`; `get_stream` does not, and the reference doc does
not call this out as intentional. Any consumer keyed on `Final` (the mutation driver's own tests
assert the invariant matters) will never see it for hydration. Medium confidence this is a real
contract break rather than a documented exemption; worth checking against `bifrost-sync`.

Related smaller edge case in `inventory_stream`: the `Final` batch is only emitted
`if !items.is_empty()`. A last list page whose hydration produced zero items, or an entirely empty
mailbox, yields no `Final` batch at all, only `Done`.

## changes_stream pays a users.getProfile round trip on every poll

**C3 refactor opinion (cost), containing a C2.** The round-trip half is a cost argument: nothing
misbehaves, the call is redundant with a local check, and removing it is a judgment about how much
identity re-validation belongs on the hot path. The second half is a real **C2 latent defect** -
the exact-case `!=` comparison wedges the account into a permanent
`SchemaIncompatible`-clear-re-establish cycle if `getProfile` ever returns different casing, and
`eq_ignore_ascii_case` costs nothing. Fix the comparison independently of the round-trip question.

`crates/google/src/account/changes.rs`: before the first `history.list` page it decodes the cursor,
compares `decoded.profile_email` against the already-held `state.profile.email_address` (a pure
local check), and then issues `client.get_profile()` to compare a third time. The open-time profile
is already the account identity; the extra call only catches "the token now points at a different
Google account", which is an auth-layer concern, not a per-sync-tick one. On a 30-second poll
cadence that is a doubled request count on the highest-frequency path, plus a doubled failure
surface (a transient 5xx on `getProfile` terminates the change stream that would otherwise have
succeeded).

Also: the email comparison is exact-case (`!=`) in both `changes.rs` and
`cursor::decode_gmail_state_for_profile`. Google normalizes addresses, but if `getProfile` ever
returns a differently-cased local part the account wedges permanently into `SchemaIncompatible`,
the engine clears the cursor, re-establishes, and hits the same mismatch. `eq_ignore_ascii_case`
costs nothing here.

## The Drive chunk loop can spin forever

**C1 live defect (hang).** Verified 2026-08-23: `while offset < total` with
`offset = parse_resume_offset(&response.headers)?` on 308 and no comparison against the previous
offset, no attempt counter, and no cancellation token. A server repeating a Range answer holds the
task forever. `parse_resume_offset` itself is correctly strict about gaps, which makes the missing
progress check easy to miss. The second half (abandoned resumable session on a mid-upload `Net`
error) is **C2**.

`crates/google/src/account/cloud.rs::upload_file_chunked`: on 308 it sets
`offset = parse_resume_offset(...)` with no check that the new offset is greater than the old one.
A server that repeatedly answers `Range: bytes=0-N` for an unchanged N (or a smaller one) puts the
loop into an unbounded re-upload of the same chunk, with no attempt counter and no cancellation
token. Add a strict-progress assertion (`new_offset > offset`, else fail) and a bounded retry count.

Second issue in the same file: a `Net` error mid-upload aborts the function and abandons the
resumable session; Drive keeps a partial upload for a week. There is no cleanup or
resume-on-reopen. The module doc acknowledges "a stray uploaded-but-unlinked file is the worst
failure mode" for the link step but not for the upload step.

## FullWithBlobs fetches the whole message twice

**C3 refactor opinion (cost).** Verified 2026-08-23: `hydrate_one` does issue both `raw` and `full`
for `Projection::FullWithBlobs`. Nothing is wrong with the result - the answer is correct, just
expensive - so this is a backlog item however large the number is. Worth doing; not a bug.

`inventory.rs::hydrate_one`: `Projection::FullWithBlobs` issues `get_message(id, "raw")` and
`get_message(id, "full")`. For a message with a 20 MB attachment that is ~40 MB of transfer and 10
quota units to obtain data the `raw` fetch already contains: the attachment ids in `full` are the
only thing the second call adds, and they are derivable from the MIME structure the `raw` bytes
already carry, or more cheaply from a `format=metadata` call. This is the single most expensive
line in the crate's read path.

## Smaller / lower-confidence observations

Each bullet carries its category inline. None of these touches a published surface.

- **[C3]** `push.rs`: the transient-failure `Warning` is always built `.with_retry_count(1)` regardless of
  how many consecutive failures have occurred: misleading telemetry, and the renewer already tracks
  `disconnected` state it could count from. Also, `WatchEvent::Warning` is emitted by the renewer
  but `reference/google.md`'s renewer section documents only `Terminated`/`Disconnected`/`Reconnected`.
- **[C3]** `push.rs`: `push_subscribe` emits `WatchEvent::Reconnected` on the broadcast channel before any
  consumer has had a chance to call `push_stream()`. `broadcast` drops messages with no receivers,
  so the initial `Reconnected` is normally lost. Not harmful today, but it means the event stream's
  first observable state is undefined.
- **[C2]** `cursor.rs`: `encode_gmail_state` uses `serde_json::to_vec(state).unwrap_or_default()`, so a
  serialization failure silently mints an empty cursor envelope that decodes as `MalformedPayload`,
  then `CursorInvalid`, then the engine restarts the scope. Practically unreachable for this struct,
  but "silently produce a poisoned cursor" is the wrong failure mode; the function should return
  `Result` or `expect`.
- **[C3]** `client.rs::execute` sets `Content-Type: application/json` on GET and DELETE requests that have no
  body.
- **[C2]** `blobs.rs::open_blob` reports `bytes_in: handle.size.unwrap_or(0)`, the decoded size, whereas the
  wire carried base64url inside a JSON envelope (~4/3 larger). Bandwidth accounting under-reports by
  a third on every attachment.
- **[C2]** `calendar.rs::update`: a cross-calendar move followed by a failing field PATCH leaves the event
  moved but unpatched, with no compensation and no mention in the reference doc.
- **[C3]** `calendar.rs::search`: the clipped-tail comment ("Any clipped tail is recoverable") is
  load-bearing but unverified. If Google ever returns more items than `maxResults` without a
  `nextPageToken`, the tail is dropped silently and the cursor advances to the next calendar. A
  defensive `debug_assert` or an explicit `Warning` would make the assumption visible.
- **[C3, half STALE]** The ignored `IdempotencyKey` is no longer unexplained: the parameter is now `_key` with a comment stating Gmail accepts no client-mintable replay token, so that half is answered and only the duplicated URL assembly stands. `mutation.rs::post_empty_json` re-implements URL assembly (`api_base()` plus leading-slash
  handling) that `GmailClient::api_url` already owns, and takes an `IdempotencyKey` it explicitly
  ignores. Both are small but they mean the raw-builder path and the typed path can drift.
- **[C3]** `flags.rs::patch_for_set`: the `Set` re-derivation names every user label in the account in
  `removeLabelIds`. The code documents this honestly, but on an account with a few hundred labels
  every `Set` mutation ships a several-KB body per batch. This is the design that would benefit most
  from a read-back-then-diff, since the engine's read-back guard already fetches current state.
- **[C3, no action]** The finding concludes the mapping is probably correct; it is a note to a future reader, not a defect. `error.rs::gmail_scope_for` maps `HostAttachment` to `drive.file`, but `create_sharing_permission`
  writes a `permissions` resource, which `drive.file` grants only for files the app created. It does
  here, so this is probably correct; noted because it is the one scope claim in the table that is
  not self-evident.

## Design-level view

**Both entries in this section are C3 refactor opinions.** They are well-argued and the causal
claims are true (the missing `Final` and the inventory terminate really do follow from the policy
living in four places), but nothing here misbehaves that is not already filed above as its own
finding, and "collapse ~400 lines into one generic driver" is a backlog item, not a defect. Work
the individual defects first; if the unification then falls out naturally, good. Do not let the
phrase "worth doing rather than patching each site" convert a backlog item into a blocker on the
defect fixes - the defects are each small and local, and the unification is neither.

**The stream drivers are four near-identical hand-rolled `stream::unfold` state machines.**
`changes.rs`, `mutation.rs`, `inventory.rs::get_stream`, and `scopes.rs::scope_lifecycle_stream`
each carry their own `finished`/`emitted_done` pair, their own "drain N then look ahead one"
batching, their own terminate-and-emit-Done dance. The missing `Final` in `get_stream` and
inventory terminating where `get_stream` fans out are both direct consequences: the policy lives in
four places so it diverges in four places. A single generic `BatchedStream<Item, Err>` driver
parameterized by (drain size, per-item vs per-stream failure policy, checkpoint function) would
collapse ~400 lines and make the boundary/terminate contract enforceable in one spot. Worth doing
rather than patching each site.

**Error classification is centralized but the call sites decide the fan-out policy ad hoc.**
`terminates_mutation_stream` encodes a real policy (which `RecoveryClass`es fan out per-id versus
terminate the stream) and it lives in `error.rs` where it belongs, but only the mutation driver
consults it. Inventory, hydration, and the lifecycle stream each make their own decision inline
(inventory: always terminate; `get_stream`: always per-item; lifecycle:
`is_terminal() || requires_engine_action()`). That is three different answers to one question.
Lifting it to a single `FailurePolicy::for(error, lane)` used by all four drivers would fix the
inventory-404 finding as a side effect.

The hunter noted the rest of the crate is in good shape: the relocation rule shared between
`flags::move_placement_patch` and both entry points, the label-cache `Option<Instant>` staleness
model, the cursor envelope's identity check, and the 308-gap-refusal in `cloud.rs` are all correct
and well-reasoned.
