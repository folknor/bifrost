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

## Ruled 2026-09-29, not yet built

Twelve items were ruled in one serial pass on 2026-09-29 and all but one were
built the same day; a second pass the same day ruled the follow-ups below.
Several items had been ruled in earlier sessions without the ruling reaching
this file, so they were presented again as open; recording the ruling at the
item is what stops that.

- **imap: whether APPEND should also be a `Command`.** DEFERRED 2026-09-29:
  the owner judged the implications underpresented. The driver-built APPEND
  lives on the driver's `DriverCommandPayload::Append` (executed by
  `prepare_append` then `run_append_command`) rather than on `Command`, which
  the original ruling named. Behaviourally that ruling holds. Two facts to
  carry into any re-raise: `Command` is NOT published - `types` is a private
  module, and so is `AppendMessage`, contrary to how this item was first
  framed - so this is an internal-design question, not a published-surface
  one; and a re-raise must bring the full analysis first: how every
  `Command`-accepting path, the pipeline in particular (literal
  continuations interleaving with other commands), would treat APPEND, and
  what submits raw `Command`s at all.

- **errors: findings from the local-refusal audit (part c) still open.**
  Filed 2026-09-29; the rest of the audit's findings landed 2026-09-30.
  - JMAP `WebSocketNotConnected` (`sync/push.rs apply_push_set`) derives
    terminal `Unsupported(PushSubscribe)` for a transient race. Analysed
    2026-09-30: the sink is `None` only before the first handshake and is
    never cleared, so the error means "the reader has not connected yet". The
    engine does NOT read its kind: `subscribe_push` hands it to the consumer,
    and during a reopen (`engine/reattach.rs`) any error from the
    replacement's `push_subscribe` aborts the attempt, so a slow or down
    WebSocket endpoint during reopen can exhaust `restart_account`'s three
    attempts and pause the whole account, HTTP sync included. Reclassifying
    fixes none of that. Recommendation: treat it as deferred success (commit
    the desired set and return `Ok`; the reader re-applies `enabled` and
    `push_state` on every connect, and `close()` already treats the error as
    `Ok`; the lock order was traced and loses no update). Related: a send on a
    dead sink becomes `Protocol(PartialResponse)` stamped `Acknowledged` though
    nothing was acknowledged, which for non-idempotent `PushSubscribe` derives
    a reconcile with no target; honest is `Transport(Network)` (`Unsent` when
    the library refused before writing, else `InFlight`) with an idempotency
    override, needing its own sink-send error variant. Knock-on, unverified
    against `tokio_websockets`: the old account's `push_unsubscribe` on a dead
    sink fails, its record is marked `teardown_unconfirmed`, and unconfirmed
    records are never recreated, so a subscription can silently stop being
    recreated after a reopen that follows a WebSocket drop. Wants a ruling.
  - SMTP `starttls` on a server without STARTTLS is `Request(Malformed)`,
    while IMAP's `StartTlsUnavailable` is `Authorization(PolicyBlocked)`: a
    parity question with a security-policy flavour, left alone.
  - Graph public-folder `incomplete_walk_error` (`public_folder.rs`) is a
    `Transport(Network)` retried next poll by design (a page cap or stalled
    offset); whether the cap arm is `Internal(LimitExceeded)` is unexamined.
  - Graph OneDrive upload shutdown mid-PUT is conservatively `InFlight`
    (`cloud.rs upload_chunks`), including when the cancellation lands while
    bifrost-net is still waiting for rate-limit admission and nothing was
    dispatched: the dropped future loses its stage. The cost is an unneeded
    read-back, never a blind replay. Exact classification needs bifrost-net to
    take a cancellation signal into the request and report the stage it
    stopped at, a transport feature; sparred and accepted as conservative on
    2026-09-29, raised again by the cold review.

## Found 2026-09-30 by the laterals sweep, not fixed

Each was found while fixing a neighbour and is outside what was built; none
is ruled. Verify against the code before working any of them.

- **graph: `push_subscribe` can leak live subscriptions.**
  `finalize_push_outcomes` runs after the arm has registered the handle and
  started the renewal worker; if it fails, the caller gets `Err` with no
  handle, so the server subscriptions keep being renewed and can never be
  unsubscribed.
- **graph: calendar recurrence and patch gaps** (`calendar.rs`). An RRULE Graph
  cannot express is silently dropped on create and patch, making a one-off
  event (the test named `graph_event_create_rejects_unsupported_rrule_parts`
  pins the drop); a patch recurrence with no `start` builds its range start
  from an empty time; a patch setting attendees to an empty list sends nothing,
  so attendees cannot be cleared.
- **graph: OneDrive upload leftovers** (`cloud.rs`). A 307 or 308 on the
  anonymous chunk PUT goes through bifrost-net's redirect walk, unchecked for
  where it can lead or an http downgrade; an aborted or refused upload never
  deletes its upload session; the upload-URL admission duplicates part of
  `origin::validate`, whose natural home is `origin.rs`.
- **graph: Autodiscover leftovers** (`autodiscover.rs`). A missing setting, an
  in-body `ErrorCode` and "exceeded redirect limit" still map to
  `Request(Malformed)` though they are provider faults;
  `json_parse_to_account_error` labels every `GraphError::Json` a JSON parse
  failure, which now includes Autodiscover XML and 202 range failures; the
  EWS parsers (`ews/parse.rs`, `ews/xml_helpers.rs`) still use the lenient
  `push_general_ref` and a `push_text` that drops unescape errors, so an
  unknown entity silently vanishes from an EWS value (Autodiscover now uses
  the strict `try_push_general_ref`).
- **google: classification leftovers.** `changes.rs` reports a missing local
  `start_history_id` as `Protocol(MissingField)` though it is local stream
  state (`Internal(InvariantViolated)`); `missing_field` never records
  `Acknowledged` though most callers raise it after a complete response;
  `cloud.rs account_domain` refuses an account email with no domain as
  `Request(Malformed)` though it comes from the profile, and only after the
  upload, leaving an uploaded unshared file; `endTimeUnspecified` is not
  modelled, so Google's placeholder end is projected as real; a Drive
  resumable `Location` header that is present but not valid text reads as
  missing (`MissingField`) where `ParseFailed` is honest.
- **jmap:** `SessionState::derive` parses `eventSourceUrl` eagerly, so a server
  whose EventSource template this crate cannot parse cannot be opened at all
  though sync never uses EventSource; `session_template` checks only template
  syntax, so a relative `uploadUrl` / `downloadUrl` / `eventSourceUrl` such as
  `not-a-url/{accountId}` passes and later fails in the transport instead of
  as `MalformedSessionUrl` (a check must not refuse RFC 6570 templates that
  are legitimately absolute only after expansion); the `reader_pass` comment in `push.rs`
  says the re-enable path can produce `WebSocketNotConnected`, which it
  cannot.
- **imap:** `account/pim.rs`'s "SELECT missing UIDVALIDITY" sites (four, plus
  two in search) map to `pim_malformed`, blaming the caller for the server's
  omission (changes the recovery class, so wants a ruling); a tagged code
  (typically HIGHESTMODSEQ under QRESYNC) hides COPYUID on MOVE and COPY, which
  then survives only as an event; a missing mandatory response retires a
  connection whose framing is intact after the tagged OK; `ImapConnection::
  literal_mode` in `helpers.rs` is a dead copy of the driver's rule that its
  test exercises; the rev2 baseline may be too generous (from memory RFC 9051
  Appendix E folds in neither OBJECTID nor SAVEDATE, and only the FETCH side
  of BINARY); `Capability::StatusDeleted`'s doc cites RFC 9051 6.3.11 for a
  token that section does not define; an ESEARCH whose tag names an already
  finished command still becomes an event, where the surplus-results ruling
  suggests dropping it; `idle.rs` and `ImapConnection::next_event` mint
  `Error::driver_gone()` with no phase when the driver dies mid-IDLE, so an
  in-flight IDLE reads as `Unsent` (low stakes, IDLE mutates nothing).
- **dav:** a refused redirect to an unadmitted origin (`auth_headers`) is still
  `local_error` with no attempt stamp though the first hop was sent;
  `not_found_error` for a GET resource with no VEVENT carries no attempt stamp,
  while CardDAV's local `not_found_error`, built on `status_error`, now stamps
  `Acknowledged` as a side effect, so the two crates differ;
  `patch_to_ical` failures map to `Unsupported`, arguably the provider's fault
  when the server body has no place to splice the patch; the missing
  current-user-principal and home-set errors are `ParseFailed` where
  `MissingField` is more precise; the untrusted-origin check at the end of the
  redirect walk looks unreachable.

## Blocked on an unvalidated consumer contract

Recorded 2026-09-07, while working the open rulings serially. Several items in
this file are not engineering questions at all: they ask what a CONSUMER should
get from a surface no consumer uses. `ratatoskr` is the intended consumer of
every one of them, it has not wired them, and what it needs is open. Deciding
them now means guessing on the consumer's behalf and then defending the guess.

Known members of the class: the backfill `subsumed` history (see below),
**types-B1** (the rewrite half), **types-B2**, **sync-B6**. The tell is that the
remedy is a default, a shape, or a policy that only the caller can evaluate.

**google-B13 LEFT this class** by taking the first way out below: all three
calendar backends can honour an `include_cancelled` field truthfully, so the
surface stops deciding instead of guessing a default. It was ruled on
2026-09-29 and is listed above.

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
asked for. That review debt is still owed, and nothing else in this file
tracks it.

- **smtp: the blocking implicit-TLS handshake at connect time is unbounded
  even with a timeout configured.** Noticed 2026-09-29 while bounding STARTTLS.
  Documented as the blocking transport's structural difference, and its
  address resolution is unbounded too. Recorded so the STARTTLS bound is not
  mistaken for "every TLS handshake is bounded".

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

## Notes

- The rules for working an item are at the top of this file. One more that
  earned its keep: an existing test you must modify is a red flag; justify
  it explicitly.
- Item keys: F-items came from the phase 5B/5C/5D error-model re-audits,
  N-items from the original post-phase-4 audit, B-items from the 2026-08
  bug-hunt ledgers, C-items from the 2026-09-04 hunt. All are tracked at
  the same level; none blocks ratatoskr.
