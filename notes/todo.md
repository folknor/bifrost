# TODO

## What belongs here, and what does not

This file holds OPEN work: defects to fix, decisions the repository owner
has not made, and ruled work that has not landed. Nothing else.

- **A completed item is DELETED**, never annotated DONE or CLOSED. Its
  "what" is in the code and `reference/`; its "why" is in git history.
- **A known limit, an accepted non-defect, a deliberate trade, a coverage
  boundary, or a "do not re-file this" marker does NOT go here.** It goes
  to an inline comment at the code it describes - the function, constant,
  match arm or test whose behaviour it explains - written so the next
  reader of that code understands the limit, why it is accepted, and what
  would change it. Where a consumer relies on it, it is also stated in the
  crate's `reference/` doc. A bullet in this file is invisible from the
  code, and every past close-out has had to move such bullets there by
  hand; put them there directly.
- **A rejected proposal gets the same treatment**: a comment at the code
  the proposal would have changed, stating the ruling and its condition
  for re-raising. Code comments never cite `notes/` paths.
- Verify every item against the code before working it. This file has no
  truth guarantee; `reference/` and passing tests are authoritative and a
  finding that contradicts them loses. Fix only what is named and
  confirmed; a change that removes or renames a published item stops and
  asks the owner, whatever the argument. A shared-crate change (`types`,
  `net`, `sasl`, `sync`) is verified with the full-workspace `brokkr
  check`, never `-p`.

## Blocked on an unvalidated consumer contract

Recorded 2026-09-07, while working the open rulings serially. Several items in
this file are not engineering questions at all: they ask what a CONSUMER should
get from a surface no consumer uses. `ratatoskr` is the intended consumer of
every one of them, it has not wired them, and what it needs is open. Deciding
them now means guessing on the consumer's behalf and then defending the guess.

Known members of the class: the backfill `subsumed` history (see below),
**types-B1** (the rewrite half), **types-B2**, **sync-B6**. The tell is that the
remedy is a default, a shape, or a policy that only the caller can evaluate.

**google-B13 LEFT this class on 2026-09-15** by taking the first way out below.
Its blocking prerequisite was verified and cleared: all three calendar backends
can honour an `include_cancelled` field truthfully, so the surface can stop
deciding instead of guessing a default. See the item itself for the per-backend
work, and note that Google's half is gated on the google-C4 defect.

Two ways out, both better than ruling blind. Where the surface can simply STOP
deciding - google-B13 is the clean case, an additive `include_cancelled` request
field defaulting to today's behaviour - take that, subject to every backend on
the shared trait being able to honour it, since a field two of three
implementations ignore is worse than no field. Otherwise leave the item parked
and say so, as the `subsumed` entry now does.

## Sync residuals

The three items ruled on 2026-09-06 (the receipt bound, the explicit
observer subscription, the teardown window) have all landed. Two earlier
structural landings of the same week, the dav-core `ResponseParts` collapse
and the smtp sans-I/O core, never received the cold review their rulings
asked for; that review debt is listed under their crates below.

- **`InventoryFusion::run_stream` still has no shutdown arm.** The other two
  loops were armed on 2026-09-15 (`drive_changes_stream` selects the account
  BOUNDARY - `Stop` and `Pause`, never `CheckpointNow`, plus an entry peek for a
  request the view has already consumed; `BackfillRunner::run_partition` selects
  the `LaneGate` shutdown token and returns `Error::ShuttingDown`). Fusion was
  left alone deliberately and still wants its own ruling, for the reasons it
  always did: its fix moves published surface (a token field on a struct
  consumers construct by literal), and it forces restaging all three tests that
  park an inventory stream through fusion to hold a teardown window open. One of
  them, `a_straggler_worker_does_not_cost_the_ack_writer_its_drain`, names this
  exact change in its own doc as the ablation that breaks it, and it measures the
  stalled stream's destruction instant from inside `Drop` precisely because
  elapsed time cannot discriminate - a restaging that falls back to elapsed time
  passes against the bug the test exists to pin. The cost being bought out is one
  `detach_timeout` in the worker phase, already fenced off from the writer and
  close phases by the per-phase budgets. If it is wanted, the restaged test must
  keep the `Drop`-instant probe.

- **imap: the driver's two prebuilt-command refusals lose their `Unsent`
  evidence.** Found 2026-09-29 by the test that finally reached the
  `WireAssumptions` guard. `run_prebuilt_command` returns
  `Error::Protocol(..).with_attempt(TransmissionState::Unsent)` for both the
  stale-encoding refusal and the session-legality refusal, but `Protocol(String)`
  has no attempt field and `with_attempt` is a documented no-op on it, so
  `attempt()` reads `None`. The caller of a non-idempotent APPEND that provably
  wrote nothing then falls back to `ImapErrorContext::transmission_state`, and
  an unknown attempt takes the conservative reconcile path rather than a clean
  retry. `Protocol` is also the wrong kind: its doc says "protocol violation by
  the server", and these are local refusals. That has a second, worse
  consequence: `Error::is_connection_fatal` lists `Protocol(_)`, so the driver
  closes its command channel after refusing, and a connection whose framing is
  provably intact - nothing was written - is retired. The guard's stated intent
  is "make the caller re-issue against the new state"; today the re-issue needs
  a fresh connection. The fix is a variant that carries an attempt and is not
  connection-fatal, either a reshaped `Protocol` or a new local-refusal variant,
  and `Error` is PUBLISHED SURFACE, hence the owner's call. Once it lands, the
  test `a_queued_append_is_refused_when_the_state_it_was_built_for_moved`
  should assert `Unsent` and a working follow-up command on the same
  connection.

- **imap: the structured-command APPEND refactor is unbuilt.** The prebuilt
  path is guarded (one snapshot borrow per APPEND, plus the driver's
  `WireAssumptions` comparison against live state, both pinned by transcript
  tests), but the guard patches a class the architecture still has.
  The architectural fix is still the right one and is not done: give
  `Command` an `Append { mailbox, messages }` variant and let the DRIVER encode
  it from live state, which deletes the whole prebuilt path and with it this
  entire class. It also collapses a real duplication - single-APPEND
  re-implements flag filtering, date quoting, UTF8-wrapper selection and RFC
  7888 marker policy that the encoder already owns, and
  `encode_multi_append_header` is `#[cfg(test)]` while the encoder suite
  exercises THAT rather than any production path. Sparred and confirmed sound:
  no obstacle in `Command`, the driver channel, response dispatch or the
  continuation machinery, and APPEND is not special to the sender - N+1 segments
  for N synchronizing literals is exactly what `send_encoded_segments` already
  does. Two conditions on doing it. The driver must also VALIDATE at execution
  (session legality, MULTIAPPEND capability, BINARY for NUL bodies), since
  moving serialization alone does not give the state gate. And a naive version
  through `EncodedCommand::from_flat_buffer` copies the body twice or three
  times, a real transient 2-3x footprint on large messages; avoiding that means
  ownership-preserving segmentation, and the cleanest form of it wants
  `AppendMessage::data` to become `Bytes` - PUBLISHED SURFACE, hence the owner's
  call. A `Vec<u8>`-preserving version is possible by encoding directly into
  owned segments rather than flatten-then-split.

- **imap: the rev2-IMPLIED-capability list is the next instance of the same
  drift.** The dual-mode ACTIVE rule was unified on 2026-09-15 (it had five
  copies, not the two filed; `types/profile.rs::imap4rev2_active` is now the
  single authority and the four views delegate). This is the adjacent question -
  "given active rev2, which extension capabilities are part of the rev2
  baseline?" - and it is a strictly larger surface: the list is enumerated
  centrally TWICE, in `ServerProfile::rev2_implies` and
  `EncodeOptions::rev2_implies`, which are byte-identical today, and then
  re-derived ad hoc at roughly 15 connection-handle sites as
  `snap.capabilities.contains(&X) || is_rev2_from_snapshot(&snap)`. An audit
  found NO live mismatch - every gate that should carry the `|| is_rev2` clause
  does, and every capability correctly absent from the rev2 base set lacks it -
  but the invariant is maintained by hand at 15 sites with nothing checking it,
  so adding a capability to the two central lists silently fails to reach any
  handle gate.
  Deliberately NOT folded into the active-rule unification, because it has a
  different correctness argument: it moves a policy boundary spanning encoder
  command admission, handle-side validation, account-facing
  `ServerProfile::supports`, special cases like QRESYNC implying CONDSTORE, and
  extensions that are advertised but not rev2-implied. It wants its own
  authority (a shared `supports(capabilities, enabled, capability)`) and a
  capability-by-capability test matrix, not a fold.

## bifrost-sync

- **sync-F6.** (residuals of the closed F4+F5 throttle wiring) What
  bounds the now-wired `ThrottleBucket`:
  (a) This is the SINGLE home for the tenant-identity item; a near-identical
  duplicate filed under "Sync residuals" was deleted 2026-09-15, so do not
  re-file it there. `ThrottleScope::Tenant` degrades to the `Account` key because the
  error contract carries no tenant identity string - `ThrottleKey::
  Tenant(String)` exists but nothing can mint one, so a Graph tenant
  429 pauses only the observing account, not tenant siblings. Fixing it
  is a `bifrost-types` change (a tenant identity on the error or the
  advice) plus producer support in graph/net.
  (b) `Mailbox` keys are recorded (from `ErrorScope::Mailbox`) but
  excluded from the account-wide wait: the engine has no
  scope-to-mailbox mapping, so it cannot pause anything narrower than
  the account without widening a per-mailbox throttle to every scope.
  Needs a scope-to-mailbox channel (or a ruling that mailbox throttles
  stay advisory).
  (c) Cross-account enrollment is lazy (an account joins a shared
  `Provider` key only when its own error stream names the identity),
  so the FIRST provider-wide deadline is invisible to a sibling that
  has never failed. Attach-time enrollment needs the account's
  provider identity at attach - the same identity-channel shape as (a).

## A9 (directory search) follow-ups

- **a9-1 (carddav)** RFC 6352 directory-gateway leg. `directory_search`
  is `Unsupported(DirectorySearch)` on CardDAV; a server may advertise an
  optional read-only directory-gateway address book, but gateway discovery
  is a substantial provider-specific unknown with no ratatoskr precedent.
  Scoped out of A9 to keep the blast radius bounded.
- **a9-2 residual: the alias `$select` may cost directory reach.** The
  projection itself landed 2026-09-07. What it left open is a consent question
  nobody can settle hermetically: `otherMails` and `proxyAddresses` sit outside
  the property set `User.ReadBasic.All` grants, so a tenant holding only basic
  directory consent may now 403 the WHOLE `/users` query where it previously
  succeeded. It maps to `NoPermission` correctly, so nothing is misreported -
  but a tenant with working directory search can lose it, which is a reach
  regression rather than a graceful degradation. Confirming it needs a live
  ReadBasic-only tenant, because whether Graph actually refuses a `$select`
  overreach (as opposed to omitting the fields) is not something the hermetic
  suite can observe. If it does fire, the fix is a select-narrowing retry after
  the first 403 - drop the two alias fields and re-issue - NOT dropping them
  from the query outright, which would give every tenant the degraded
  projection to spare the minority.
- **a9-3 (graph)** `$search` (with `ConsistencyLevel: eventual`) as a
  richer substring directory match than the current `startswith` prefix
  `$filter`, if needed.

## Namespaced-container follow-ups

Surfaced while landing the namespaced-container surface (shared-mailbox and
public-folder containers, EWS public-folder hydration, allowlisted public-folder
scopes). Each was deliberately out of that brick's scope; none blocks the
container projection itself.

- **nc-2a (graph, filed while landing nc-2, not fixed)** A non-mail
  public-folder item now HYDRATES (the `GetItem` request is class-safe), but
  both projections are still mail-shaped: `hydrated_from_ews_item` reuses
  `item_to_inventory_entry` and `message_from_ews_item` mints a `Message`. A
  contact's `<t:DisplayName>` is not a subject and does not land anywhere, so
  a hydrated `IPM.Contact` is a near-empty `Message` carrying only its id,
  change key and memberships. Evidence: `ews::parse` fills `EwsItem` from a
  `<t:Contact>` with `item_id` / `change_key` / `item_class` and nothing else
  (`parse_get_item_response_contact`). Whether non-mail public-folder items
  should project onto a contact/event type at all is a product decision, not
  a defect - hence filed rather than built.
## Cross-crate items from the bug-hunt loop (2026-07-29)

Surfaced while working the per-crate bug-hunt ledgers of that wave (since
deleted) crate by crate. Each of
these was found from inside one crate but cannot be resolved there: the fix,
or the decision, belongs to a shared contract or to a second crate's API.
They are collected here rather than in the per-crate sections above so they
can be adjudicated together, from a higher vantage point, later. None is
blocking; each is a real defect or a real decision, not a cleanup.

- **xc-1 (graph + sync)** The push-teardown retry lane has no *scheduled*
  retrier. The engine side landed (2026-07 close pass):
  `SyncEngine::unsubscribe_push` retains a failed handle's registry record
  as `teardown_unconfirmed` and returns the error, reopen carries
  unconfirmed records across swaps and retries them, and `bifrost-graph`
  (G-15) keeps server subscription ids under the handle so those retries
  can land. What remains is that nothing retries *on its own*: an
  unconfirmed teardown waits for the next reopen or `unsubscribe_push`
  call, so an account that never reopens keeps its orphan until the
  provider expires it (24h for Graph). If that window matters, add a
  bounded engine-side retry timer for unconfirmed records.

## Open items folded in from the bug-hunt ledgers (2026-08-23)

Categories, as the ledgers used them: **C1** live defect, **C2** latent defect,
**C3** refactor opinion, **C4** product decision. **PUBLISHED SURFACE** means the
remedy removes, renames, or reshapes a published item - those are the repository
owner's call and must not be actioned without one, no matter how confident the
argument reads. Ledger findings were never verified against a running server;
confirm against the code before working any of them.

### Fenced for the repository owner (published surface)

- **google-B12. Retire `RATATOSKR_TEST_GCAL_ENDPOINT`.** [C3] Tail of google-B3,
  which landed 2026-08-23: `GoogleAccountFactory::with_calendar_api_base` now
  exists and takes precedence, the base is stored on `ClientInner`, the read
  happens once at construction instead of per request, and rate limits are
  registered from the configured bases rather than literal hostnames.

  The environment variable was KEPT on purpose. `ratatoskr` and `sæhrimnir` both
  set it, and deleting it would not fail their builds - it would silently stop
  redirecting and point their test traffic at the real Google Calendar API. Once
  both have migrated to `with_calendar_api_base`, delete `default_calendar_base`
  and have the constructors take `CALENDAR_API_BASE` directly. That removes the
  last `std::env::var` read in the workspace and the last place a bifrost crate
  names a downstream consumer. Coordinate with those two repos; there is nothing
  to do here until they are ready.
- **sync-B6. Repair is caller-driven, with no scheduler.** [C4]
  `SyncEngine::repair_debt(account, max_requests)` runs exactly one pass when a
  consumer asks. Nothing schedules it, so debt sits until someone calls. That is
  deliberate for now - repair is remote work against an account that may be
  throttled, paused or degraded, and the consumer knows better than the engine
  when to spend that budget - but a consumer that never calls it gets the
  pre-repair behaviour, which is the state this whole arc set out to leave. If
  it becomes a scheduled lane it needs the throttle-deadline and pause checks
  the backfill partition runner already does, and it should respect
  `OperatorBlocked` without re-arming it on a timer.

### Cross-crate shaping questions

- **No concurrency governor in `bifrost-net`.** Nothing bounds the number of
  simultaneously in-flight requests, per account or globally. JMAP's foreign
  probing at open solves it locally: `api_request_concurrency` (bound to a local
  `probe_concurrency` in `crates/jmap/src/sync/factory.rs`) bounds a
  `buffer_unordered` by the server's `maxConcurrentRequests` clamped to `[1, 8]`,
  serial when the core capability is unreadable, with results sorted by
  `accountId` before installation so topology and skip ordering stay
  deterministic. This is a new permit-pool feature with its own API and
  test-bite obligations, not a defect - it stays a recorded deferral until
  someone wants the feature.

  CORRECTED 2026-09-15 on two counts, and the correction strengthens the case.
  The symbol was named `foreign_probe_concurrency` here and does not exist. More
  importantly, the claim that the JMAP probe is the workspace's ONLY overlapping
  -request site is false: there are now at least four more - two google inventory
  fanouts (`buffer_unordered(HYDRATE_BATCH_SIZE)`), the jmap `filters::list`
  fanout that jmap-C4 itself describes, and the caldav/carddav client fanouts.
  So "any new concurrent call site has to solve it again from scratch" is not a
  prediction, it has already happened four times without anyone recording it.
- **Inventory exhaustion is an inferred count, not a declared flag.** The email
  inventory contract on both sides of the JMAP/sync boundary rests on "a
  partition yields zero entries only when the scope has no results past `from`".
  The live `OpenPages` walker stops only on `seen == 0` and `open_pages_resume`
  treats only the completion marker as exhaustion. It works, but the signal is
  inferred rather than declared, and a short-page-means-done inference has been
  reintroduced on the resume half once already. A declared exhaustion flag would
  remove the whole class - it reshapes a published stream contract.
### Refactor backlog

Nothing in this section misbehaves. None of it is a bug, and none of it blocks a
defect fix - in particular, do not let a unification proposal become a
prerequisite for the small local fixes above.

- **jmap page-cursor machinery is duplicated between `contacts.rs` and
  `calendar_ops.rs`.** Landed 2026-09-07 and immediately flagged by the agent
  that wrote the second copy. `PageCursor`, the `2:` prefix, `anchor_query`,
  `verify_query_state`, `next_cursor`, `decode_page_cursor`, the
  `anchorNotFound` mapping and the error constructors are near-identical,
  differing only in the id type, the query builder type and diagnostic wording -
  and both modules carry their own parallel unit tests. This is the exact shape
  the standing lessons name: a local restatement of a rule that lives elsewhere,
  kept alive by a test that exercises the copy rather than the original, so the
  copy and its test agree indefinitely while only the original disagrees. It is
  not hypothetical here - the absent-`total` truncation had to be fixed in three
  places in one day, and the same reviewer found the three walks answering one
  termination question three ways. A shared generic helper would make the next
  such fix one edit. Wants a ruling because it is a new module in `sync/`, not a
  local edit.

## Open items folded in from the second bug-hunt wave (2026-08-29)

The deferred tail of the August 2026 arcs. The same category labels and the
PUBLISHED SURFACE fence apply.

- **types-B1. `PimMethodSupport` is a hand-maintained mirror of the trait
  surface.** [C3, PUBLISHED SURFACE] `crates/types/src/capabilities.rs`. Sixty
  bools with no mechanical link to the 94-method `Account` trait. Six protocol
  crates x sixty bools is ~360 hand-maintained facts that can each be wrong in
  a way no test catches, and consumers must consult the mirror AND handle
  `Unsupported` anyway.

  The keep-it half LANDED 2026-08-29: `capability_contract_tests.rs` in all six
  protocol crates drives every gated entry point and asserts the flag agrees.
  It paid for itself immediately - three IMAP flags
  (`remove_from_container`, `draft_discard`, `thread_hydrate`) turned out not to
  gate their methods at all, while `search` / `draft_create` / `quota_get` in
  the same module read theirs correctly. The methods were fixed to match the
  flags, since each flag is derived from a server capability string and so was
  the true half.
  Coverage is FALSE-DIRECTION ONLY, by necessity rather than by choice - a
  `true` flag means the method reaches the network, and the test proves no wire
  contact by installing a seam that panics if reached (a never-called
  `DavTransport`, an empty script, a dropped connection half). Each file says so
  in its own doc comment. jmap is the hardest case and documents it: the
  `MailAccount = Account<ReqwestTransport>` alias means no scripted seam exists
  at the `Account` level at all (reasoned out at the alias itself, in
  `crates/jmap/src/sync/account.rs`).

  The REWRITE half is still fenced and still the owner's call: one runtime
  `fn supports(&self, op: AccountOperation) -> bool` defaulted from a per-impl
  `AccountOperation` set, so the capability answer and the error answer become
  the same value read twice, and a new trait method defaults to unsupported
  instead of needing a bool nobody sets. That DELETES a published struct.

- **send_message and send_as cannot both be honest about a JMAP account with
  foreign submission only.** [found 2026-09-15 by the cold review of the
  types-B1d work; PUBLISHED SURFACE] `send_message: support.submission` and
  `send_as: support.foreign_submission` are independent, and
  `JmapAccount::send_message` takes its send_as branch BEFORE the
  `self.submission` check. So a session whose primary account lacks Submission
  while a seeded foreign account has it yields `send_message == false` with
  `send_as == true`, and a send_as request through it genuinely works - while a
  consumer respecting `send_message` never offers the operation.
  The obvious remedy is WRONG and was rejected in the spar: widening to
  `send_message: support.submission || support.foreign_submission` makes the
  flag advertise a method that still refuses the ORDINARY request, since the
  personal path has no fallback to `foreign_mail`. That is the exact
  false-advertisement `capability_contract_tests.rs` exists to catch, so it
  trades a gap for a lie. Pinning `assert!(!send_as || send_message)` is also
  wrong: it asserts an invariant nobody has ruled on, and the state it forbids
  is one JMAP can legitimately be in.
  The real question is what `send_message` MEANS - does it gate the whole
  method, or describe ordinary personal sending? - and that is a published
  contract question for the owner, not something to settle inside a
  test-hardening pass. Note it may be answerable with documentation alone; the
  evidence does not force a struct change.

- **types-B2. `InventoryBatch::checkpoint` cannot express a withheld
  checkpoint.** [C2, PUBLISHED SURFACE] It is `Option<Checkpoint>`, with no way
  to distinguish "this page has no checkpoint" from "I stripped this
  checkpoint because of a barrier". The barrier signal rides only in
  `coverage`, which is precisely why the A2 divergence was easy to miss. A
  dedicated `PageCheckpoint::{Advance(..), Withheld}` would make the omission
  a compile error. The shared `InventoryWalk` makes both CURRENT front ends
  read `coverage` the same way, but nothing in the type system stops a third
  from ignoring it - which is the same risk sync-F3 records, one layer down
  and closable by a type. Carried out of the sync arc; `bifrost-types` was not
  touched there.

- **google-B13. `events_search_url` sends `singleEvents=true` with no
  `showDeleted`.** [C4] `crates/google/src/account/calendar.rs`, exactly as
  `events_in_range` did before G8. Deliberately NOT filed as the same defect,
  and a reflex copy of the G8 fix is the wrong move: a range reread is a
  COVERAGE question, where a missing tombstone is indistinguishable from a
  page boundary, whereas a SEARCH returning cancelled instances is a PRODUCT
  decision about what a query surface should answer.

  Considered 2026-09-07 and NOT ruled, deliberately: the choice belongs to the
  consumer, not to us, so picking a default either way is the error. See
  "Blocked on an unvalidated consumer contract" at the top of this file. The
  direction, when someone works it, is to stop deciding - an additive
  `include_cancelled: bool` on `EventSearchRequest`
  (`crates/types/src/calendar.rs`), defaulting to `false`, which is exactly
  today's behaviour, so nothing changes until a consumer asks. That is purely
  additive to a plain struct with a `new()` constructor, so it does not hit the
  published-surface fence. The prerequisite, and the reason it was not just
  done: `event_search` is on the shared `Account` trait, so a request field is
  a promise JMAP and CalDAV must honour too. Google is a one-parameter change;
  whether CalDAV's `calendar-query` filter and JMAP's `CalendarEvent/query` can
  express it is UNVERIFIED. A field that two of three backends silently ignore
  is a worse API than no field, so verify that before adding it.

  PREREQUISITE VERIFIED 2026-09-15, and it clears: all three backends can
  honour the field truthfully, so the "two of three silently ignore it"
  objection does not apply and the additive field is viable. The work is not
  symmetric, and it is inverted from what this item assumed.
  - JMAP CANNOT express it server-side, and not because the crate
    under-models draft-26: there is no `status` filter condition in the
    draft, since event status is a plain object property rather than a
    filterable condition. But `get_events` restricts no properties, so
    `status` is always hydrated and a client-side filter is CORRECT rather
    than lossy. Precedent sits in the same function: `search` already
    post-filters by `calendar_id` client-side. The one subtlety is that the
    precedent also suppresses `estimated_total`, and `include_cancelled:
    false` needs the same suppression or the total over-counts by the
    dropped events.
  - CalDAV is one line on an existing closure. RFC 4791 CAN express it (a
    sibling `prop-filter` on `STATUS` with `negate-condition`), but the crate
    has no general filter builder and, more to the point, already treats the
    server-side text-match as an advisory PREFILTER with the local match as
    authority - and a 403 `CALDAV:supported-filter` on any text leg degrades
    the whole search to an unfiltered walk, where a server-side status filter
    would be lost anyway. A CalDAV server returns cancelled events
    unconditionally, so `include_cancelled: false` is honestly a client-side
    filter here, which is the shape this lane already uses.
  - Google is the parameter alone, now that the page-poisoning projection
    defect that would have gated it was fixed on 2026-09-15 (cancelled
    tombstones project with empty times, and per-item projection failures
    ride `Page::failed_ids` instead of failing the page).

- **Absent event status reads as `Unknown` on all three backends.** Noticed
  2026-09-15 across the same three projections. Both iCalendar and JMAP
  draft-26 make the default for a missing status effectively confirmed, but
  all three `event_status` helpers map absent to `EventStatus::Unknown`.
  Harmless for an is-cancelled filter, but it means "no STATUS line" and
  "STATUS:X-WEIRD" are indistinguishable in the shared type everywhere.

## Open items folded in from the third bug-hunt wave (2026-09-04)

The deferred tail of the 2026-09-04 hunt. The same category labels and the
PUBLISHED SURFACE fence apply.

- **google-C3. `events_in_range` combines `orderBy=startTime` with
  `showDeleted=true`.** [C2, needs a live probe] `crates/google/src/account/
  calendar.rs`. Google's docs bless `showDeleted=true` with
  `singleEvents=true`, and `orderBy=startTime` requires `singleEvents=true`,
  but there are long-standing reports of the live API answering 400 "The
  requested ordering is not available for the particular query" for some
  `showDeleted` + `orderBy` combinations, and cancelled instances have no
  `start` to order by (only `originalStartTime`, which the projection
  substitutes). The hermetic suite cannot catch a live refusal. One live
  probe settles it; if Google rejects the combination, every production
  range read fails, a top-severity defect hiding behind a green suite. The
  only ledger item of the wave left open as a possible defect.

- **jmap-C4. `filters_list` blob traffic is invisible to any accounting
  surface.** Filed while closing jmap-C1 (2026-09-06). `sync::filters::list`
  downloads one sieve script body per filter (fanned out at
  `api_request_concurrency`), and those bodies can be large, but the door
  returns `Vec<ServerFilter>` rather than a `Batch`, so there is no
  `bytes_in` field to report into - the metering contract has nothing to
  violate here, and the traffic simply never appears. `Client::download`
  now records into the tally, so a metered handle would count it; what is
  missing is a place to put the number. This is a shape question about the
  `filters_list` return type, i.e. a published-surface decision, so it is
  filed rather than fixed. Same shape applies to `pim`'s upload paths, but
  those are outbound and the tally is inbound-only.

## The outbound cap is evadable by reconnecting

Found by the cold review of the 2026-09-07 throttle work; PRE-EXISTING, and
explicitly not introduced by it. `throttle_out` is per-stream while `ByteBucket`
is shared, and `poll_write` consults the bucket only AFTER the socket has
accepted bytes. So a fresh connection's first write is ungated whatever the
bucket owes: write one cap-sized chunk, drop the connection, dial again, and the
cap never binds. N concurrent connections can likewise each pass a chunk against
the same zero balance, because none of them reserved anything - ordinary async
interleaving is enough, no threads needed.

What that wave DID get wrong was the claim, since corrected in
`reference/smtp.md` and at `poll_flush`, that the next connection pays the
dropped connection's debt. It does not.

The fix is a mechanism this crate does not have: shared ADMISSION, meaning
permission held against the balance that a second connection cannot
simultaneously obtain. Querying the shared deficit is NOT sufficient - other
connections consume capacity between the query and the write - and the naive
placement is actively wrong: a shared gate consulted inside `poll_write` sits
inside `with_timeout` and re-creates smtp-CR11 one layer down, so admission
waiting has to stay outside the per-operation timer (and inside the absolute
deadline under a setup budget, with the write taking the recomputed slack).

Two shapes, each with a cost that wants a ruling rather than a pick:

- An exclusive lease held from the balance check through one socket write and
  its charge. Closes the race, but one connection stalled on a write - forever,
  under `timeout(None)` - then blocks every other connection. That is a new
  dependency between connections and must be judged deliberately.
- A byte reservation. Avoids exclusive ownership of the socket wait, but permits
  acquired over time can accumulate on stalled sockets and all be exercised at
  once, so a design promising bounded bursts has to bound outstanding admission
  or revalidate it when writes actually progress.

Either way: accepted-byte debt stays in the shared bucket and a dropped future
must neither erase it nor pay it twice; settlement of a short write has to
happen in the same poll that observes `Ready(Ok(n))`, before any await, since
refunding an accepted write on drop re-opens the reconnect escape; and the
connection-local `Sleep` can survive only as a cached wakeup, never as the
authoritative gate, because a stale one waits inside `with_timeout` and is
CR11 again.

Scope notes for whoever builds it. The async application-write funnel is
currently complete - every production async SMTP and LMTP write reaches
`write_stream_with_budget`, which drains before each write - but three things
sit outside it: the BLOCKING `NetworkStream::write` uses the same bucket
implementation and writes before charging, so a universal contract needs both
adapters; direct `AsyncNetworkStream` writes in tests bypass the connection
drain, and would need the admission mechanism wherever they claim to exercise
throttling; and TLS handshake, record overhead and shutdown traffic bypass
plaintext accounting entirely. The honest guarantee is therefore about
admission of METERED PLAINTEXT writes, not about every byte on the wire.

## Surfaced by the 2026-09-07 fix wave

Found while resolving the dav-F, smtp-CR and jmap-C2 items and the two cold
reviews over them. Everything the reviews found at P1 or P2 was fixed in that
wave; these are what was left. Verify before working any of them.

- **Two owner rulings, none blocking.**
  (a) Move `should_fallback_discovery` into `bifrost-dav-core`, parameterised by
  the existing `DavProtocol` (the only difference between the copies is
  `ResourceKind::Calendar` versus `Contact`, which `DavProtocol` already
  carries). Internal only, but it touches three crates. The case: this wave was
  the second time the twins needed the SAME edit, and the first time they needed
  DIFFERENT edits to reach the same behaviour - the CardDAV copy parsed the probe
  body inside its `Ok(response)` arm and lifted the failure with `?`, so the
  parse error never reached the predicate at all. One more measured divergence
  (the ordinals were retired on 2026-09-15; see `reference/carddav.md` on why
  the running tally stopped being maintainable).
  (b) Bound the STARTTLS handshake under `timeout(None)`. `AsyncSmtpConnection::
  starttls` passes `self.timeout` to `upgrade_tls`, so a transport built with no
  timeout has an unbounded TLS handshake on the explicit-STARTTLS path. Same root
  cause as the teardown cap that landed, but a handshake is a PROTOCOL operation,
  so the teardown reasoning ("nothing left to accomplish past this point") does
  not carry over and a default bound is a new policy. The mechanism underneath is
  `TimeoutBudget::SetupDeadline` yielding `None` slack when built from
  `AsyncDeadline::new(None)`; any future "everything is bounded" claim starts
  there.
- **Ledger: `record_attempt` charges an entry regardless of `is_open()`**, so a
  discharged-but-`Retrying` entry can still take charges. Pre-existing, and it is
  precisely what makes the lineage-root pin necessary. Worth deciding whether
  that is the intended contract or an accident the pin is now compensating for.

- **imap: SearchConsumer publishes surplus SEARCH/ESEARCH as events on the
  SUCCESS path.** [found 2026-09-15 while landing the infallible-finalize work]
  `reclassified_extras` emits extra tag-correlated ESEARCH beyond the first, all
  tagless ESEARCH when a correlated one won, and all legacy SEARCH. By the same
  classifier argument that settled the FAILURE path - SEARCH and ESEARCH are
  `OnlySolicited` inside a search command and `Impossible` outside one, so they
  are not asynchronous notifications and publishing them says they are - those
  arguably should be dropped too. The method's own doc comment rules
  deliberately the other way, which is why it was left alone: this is a
  published behaviour with a stated reason, not an oversight. Wants its own
  ruling. Note a pipelined foreign-tag-correlated ESEARCH is a third case again
  and probably wants correlation to the command its tag names rather than either
  answer.

- **imap: AppendConsumer narrows an untagged OK lossily on the SUCCESS path.**
  `on_response` swallows an untagged `OK [APPENDUID ...]` whole, keeping only
  its `ResponseCode` and discarding `text` and the rest of the `Status` variant.
  That is why its failure arm cannot surrender the response - there is no
  faithful `UntaggedResponse` left to publish - but the narrowing happens on the
  success path too, so an untagged OK that this consumer sees is silently not
  reclassified the way other untagged OKs are. `MultiAppendConsumer` and
  `CopyConsumer` have the same shape. Restructuring them to retain the whole
  response is available and was deliberately not done in the finalize pass.

- **smtp: a 421 masked by an earlier 550 in the same RCPT window.** Found
  2026-09-07 while documenting `DirectSmtpStage::RcptWindowReply`, which keeps
  only the first negative reply. `closing_channel` at `WindowClosing` therefore
  tests the RETAINED failure only, so a window whose first negative is a 550 and
  whose second is a 421 takes `Epilogue::reset` instead of the abort that
  commit e5a15b7a established for a 421 at every envelope boundary. NOT a
  correctness hole - `Epilogue::reset` aborts anyway when the peer does not
  positively acknowledge the RSET, so the connection still goes and the cost is
  one wasted command on a channel the server is closing. But it is a real
  divergence from the direct/batch symmetry, since `BatchSmtpStage::
  RcptWindowReply` tests every reply for it. Filed rather than fixed because
  making the direct path scan every reply for a 421 changes an observed
  termination nobody ruled on.

## Surfaced by the first per-feature check run (2026-09-15)

The sweeps landed the same day and their very first run found two things, which
is the argument for having them.

- **`bifrost-jmap` does not compile with `--no-default-features`, and never
  has.** `tokio` and `bifrost-types` are optional dependencies and the `mailbox`
  module is feature-gated, but `client.rs` uses `tokio` at four sites and
  `mailbox` at one unconditionally, `core/tests.rs` names `bifrost_types`
  directly, and four macros plus their re-export in `lib.rs` are consumed only
  by feature-gated modules so they read as unused. There is also an
  `irrefutable_let_patterns` in `event_source/stream.rs` under that
  configuration.
  NOT a regression, and not what the new sweep is for: the sweep was narrowed
  the same day to default-minus-`calendars`, which is the configuration the code
  actually branches on. Whether a zero-feature build should be SUPPORTED at all
  is the open question, and it is a product decision rather than a defect - the
  crate has never claimed it, no consumer has asked, and "make it compile" means
  either gating five more sites or making optional dependencies mandatory. Rule
  on the promise before anyone writes the `cfg`s.

## Surfaced by the 2026-09-15 fix wave

- **`WorkerRole` cannot distinguish the eight non-writer workers.** The enum is
  `{ AckWriter, Stream }`, and `take_ack_writer` removes the writer BEFORE the
  worker phase runs, so every worker reaching `await_worker_until` is
  `WorkerRole::Stream` and the `role` field now on its two warn lines is a
  constant. The field is free and correct but it does not deliver the
  diagnostic it looks like it delivers: all eight non-writer spawns in
  `attach.rs` (control applier, push reconciler, push forwarder, multiplexer,
  backfill orchestrator, deferred inventory, reopen listener, bandwidth feed)
  pass the same variant, so a wedged-detach log line still cannot say WHICH
  worker wedged. Getting that means splitting `Stream` into per-task variants,
  which RENAMES a variant and therefore stops and asks. It is crate-private
  (`pub(crate)`, not re-exported), the eight call sites are mechanical, and the
  only test churn is `engine/tests.rs`'s `else { WorkerRole::Stream }` becoming
  any non-writer variant while pinning the same property. Owner's call.

- **google: `end` falls back to `originalStartTime` but never to `start`.** So a
  live event carrying a start and no end is refused rather than projected as
  zero-length. Untouched by the 2026-09-15 tombstone work, which deliberately
  scoped itself to cancelled events; this is live-event behaviour and a
  candidate second reading of "malformed". Note that under the per-item
  isolation that landed with it, such an event now rides `failed_ids` instead of
  failing its page, so the blast radius is smaller than it was.

## Surfaced by the 2026-09-15 documentation pass

Found while discharging dav-F7, smtp-CR7/12/13, the two imap response-loop
divergences and the jmap push-arm items. Verify before working any of them.

- **The unbounded CalDAV page default is arguably a live risk, not a doc gap.**
  A `limit`-less `event_search` or `events_in_range` issues
  O(collection / `MULTIGET_BATCH_SIZE`) REPORTs and materializes the whole
  projected event set, and because a recurring resource expands into one event
  per override the item count is not even bounded by the resource count.
  Nothing between the account and the consumer clamps it. dav-F7 documented the
  default rather than changing it, because bounding a published method's
  no-`limit` behaviour is the owner's call - but if that alignment is ever
  ruled, this is the argument for bounding CalDAV rather than unbounding
  CardDAV.

## Notes

- The rules for working an item are at the top of this file. One more that
  earned its keep: an existing test you must modify is a red flag; justify
  it explicitly.
- Item keys: F-items came from the phase 5B/5C/5D error-model re-audits,
  N-items from the original post-phase-4 audit, B-items from the 2026-08
  bug-hunt ledgers, C-items from the 2026-09-04 hunt. All are tracked at
  the same level; none blocks ratatoskr.
