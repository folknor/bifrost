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

### 1. `parse_address_list` corrupts RFC 5322 addresses with quoted commas - and it sits on a write path

`crates/google/src/account/pim.rs` (`parse_address_list` / `parse_address`):
the list is split on every `,` with no awareness of quoted strings, so
`"Doe, John" <jd@example.com>` becomes two garbage addresses (`"Doe` as a bare
address, plus `John" <jd@example.com>`). On the read path (thread/message
hydrate) this is a display defect. The dangerous consumer is `draft_update`:
`document_from_message` parses the existing draft's headers through this
function, and the re-render (`render_rfc5322`) then **writes the mangled
recipients back into the stored draft**. A draft addressed to anyone with a
comma in their display name is silently corrupted by any field-level patch. The
fix belongs in a real address-list parser (or reusing whatever
bifrost-types/imap already has); comma-splitting is not salvageable. Also note
`.trim_matches('"')` only strips quotes, never unescapes `\"`.

### 2. `actor_subscribe`'s encode-failure path can stop a live shared watch while leaving `Watched` intact

`crates/google/src/account/push.rs` lines 447-459: when handle JSON encoding
fails after `watch_once` succeeded, the code calls `stop_watch` - correct for a
*first* subscribe, but a second subscriber joining an existing watch (non-empty
`handles`, lifecycle `Watched`) would tear down the watch the existing
subscribers depend on, while lifecycle stays `Watched` and the renewer keeps
renewing a watch it just stopped (the next renewal would actually recreate it,
masking the outage window). Severity is theoretical - `serde_json::to_string`
of three `String` fields cannot fail - but the branch as written is wrong for
the multi-handle case, and since it's unreachable it will never be caught by a
test. Either scope the `stop_watch` to `handles.is_empty()`, or replace the
branch with an `expect` and a comment, since the failure is structurally
impossible.

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

### 4. `decode_base64url_nopad` is strict no-pad

`crates/google/src/encoding.rs`. Gmail is documented (and observed) to emit
unpadded base64url, but a padded value - from a proxy, or a future API change -
fails the decode and classifies as `Protocol(ParseFailed)` for the whole
message/attachment. A forgiving decoder (`general_purpose::URL_SAFE` with
`DecodePaddingMode::Indifferent`) removes the fragility at zero cost. Low
probability, cheap insurance.

## Minor observations / smells

- **`pim::search` / `search_messages` with `limit: Some(0)`** sends
  `maxResults=0`, which Gmail treats as "use default" rather than "return
  nothing" - a zero-limit caller gets a full default page. (`pim.rs`
  `request.limit.unwrap_or(...).min(...)`.)
- **`get_stream`'s label-refresh failure terminator** (`inventory.rs` line
  ~500) scopes the error to `ids[0]` only; the other 31 ids of the drained
  batch vanish into the `Terminated` with no per-id lane. Consistent with
  "Terminated means unreported", but the arbitrary first-id scope on an error
  that has nothing to do with that message is mildly misleading in support
  exports.
- **`actor_unsubscribe` runs before the shutdown arm** (the actor `select!` is
  biased toward commands), so an unsubscribe already queued when `close()`
  cancels the token can still issue a wire `users.stop` post-close-intent.
  Harmless - the transport is still attached at that point and stopping the
  watch is what close wants anyway - but it is a small hole in the "no wire
  traffic after shutdown" story the subscribe path enforces.
- **`snapshot()` on a poisoned `RwLock`** (`scopes.rs`) silently returns an
  empty snapshot, which reads as "never fetched" and triggers a refetch - a
  reasonable degradation, but a poisoned lock means a panic happened mid-write
  and nothing logs it.
- **`contacts::update` re-serializes the fetched `Person` DTO as the PATCH
  body**, relying on `updatePersonFields` to fence off the unmodeled People
  fields the DTO dropped. Correct today, but a trap for whoever next adds a
  field to `update_fields_for_patch` without adding it to the `Person` DTO: the
  mask would then name a field the body no longer carries, clearing it
  server-side. A comment at `update_fields_for_patch` naming this coupling
  would be cheap.
- **`filters.rs` read/write asymmetry**: `criteria.to` projects to
  `FilterCondition::Recipient` on read, while `To`/`Cc`/`Recipient` all
  collapse into `criteria.to` on write - so a created `To` filter reads back as
  `Recipient`. Defensible (Gmail's `to` matches all recipient fields) but worth
  a doc line, since a consumer doing create-then-list equality checks will trip
  on it.
- **`updateContactPhoto` response shape**: the code parses the response as
  `Person`, but the People API wraps it as `{ "person": {...} }`. It only
  "works" because every `Person` field is optional and the value is discarded -
  a latent decode landmine if anyone ever reads that response.
