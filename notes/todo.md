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

## Ruled sync work, sequenced

Three items ruled on 2026-09-06, sequenced 3, 1, 2: the receipt bound
reshapes the accounting the other two touch, so they are written against
the final shape. Each lands alone, with one scoped cold review under the
stopping rule in `AGENTS.md`. Two earlier structural landings of the same
week, the dav-core `ResponseParts` collapse and the smtp sans-I/O core,
never received the cold review their rulings asked for; that review debt
is listed under their crates below.

1. **sync: an explicit observer subscription on the change stream. RULED
    2026-09-06: PROCEED, as a separate method.** `account_changes_stream`
    supports any number of receivers with exactly one acknowledger, and until
    this lands the acknowledging receiver must subscribe FIRST, because three
    places in the engine see only a raw receiver count. The subscriber gate
    opens on an observer, which then takes pages it never acknowledges while
    the acknowledger joins at the ring's tail and cannot see them. "Reached a
    real subscriber" on each send counts an observer too, so once the
    acknowledger departs mid-walk every later page is judged delivered, stays
    charged, and can never be answered for. And an observer's lag abandons
    every registration on the account, which under the completion guarantee
    voids every in-flight walk. The build: a second public method returning an
    UNNUMBERED receiver, the shape `ChangesReceiver::new` already has behind
    `cfg(test)`, and three touch points that follow from it - the gate counts
    numbered receivers; a send that reached no numbered receiver counts as
    reaching nobody, decided by the delivery gate under its own lock since it
    holds the live set; an observer receiver carries no control handle, so its
    lag warns without abandoning. Once item 3 lands, an observer's read must
    NOT mark a page received. Observers are documented as never acknowledging.
    A separate method rather than a flag on the existing one, because the two
    are different roles and a flag lets a call site flip one into the other.
    The "subscribe first" sentence leaves `reference/sync.md` with this.
2. **sync: the teardown window in the completion guarantee. RULED
    2026-09-06: close by contract, no durable record.** A receiver dropped
    after `detach`'s drain of the outstanding discard requests records a
    request nobody can act on, because the writer is gone; a receiver dropped
    after `detach` returns runs no sweep at all, because its weak handle no
    longer upgrades. Same outcome either way, and it bites only when the
    consumer ends the attachment holding a page it received and never
    acknowledged while the completion marker is already durable - the same
    case as a consumer that crashes with pages in hand. The engine's guarantee
    repairs holes its own mechanics create, a receiver replaced INSIDE an
    attachment; the only full closure would be a durable "scope lost pages"
    row, a new method on the `CheckpointStore` trait every downstream store
    implements, bought for a consumer already outside the persist-then-
    acknowledge contract. The build: one contract line in `reference/sync.md`
    (a page delivered but unacknowledged at detach is the consumer's to have
    persisted or to forfeit, exactly as at a crash); a teardown flag that makes
    the departure sweep inert once `detach` has begun, so a drop in the window
    behaves like a drop after it; and the drain's writer awaits clamped to
    `detach_timeout`, which is the cold review's P3 in the same ten lines. THE
    OWNER'S CONDITION: the ruling must be documented at the code, a doc comment
    at the sweep's teardown check and at the drain that states the window, why
    it is the consumer's contract and not a defect, and what the closing
    alternative would have cost, so a later review reads the ruling there
    rather than re-filing the window as a bug.
3. **sync: replace the acknowledgement bound with a receipt bound. RULED
    2026-09-06: PROCEED; the cheap version (an accessor surfacing the
    maximum) is REJECTED.** The cold review's one P2 was that a consumer
    deferring acknowledgements by more than `lane_capacity` publications
    deadlocks with the parked producer, where before the bound it risked a
    lag. The contract was written down as an interim, but the maximum exists
    only because the bounded-lane ruling (landed 2026-09-06; "The bounded
    backfill lane" in `reference/sync.md`) chose the acknowledgement as the
    permit, and an
    acknowledgement is a promise about durability while what overruns a ring
    is pages nobody has READ. The engine observes reading without consumer
    cooperation: `ChangesReceiver::recv` and `try_recv` are engine code and
    the event carries its publication id. The build: a received bit per page
    (on the entry and on each subsumed tuple), set from the receiver's read
    path for numbered receivers, with `backfill_in_flight` summing unread
    pages instead of unacknowledged ones and receipt pulsing the capacity
    wake. The producer then runs exactly as far ahead as the consumer reads,
    a consumer may acknowledge on any schedule, ring protection is unchanged,
    and the completion guarantee is unchanged (a receiver departing with
    read-but-unacknowledged pages is still a recorded loss, the conservative
    reading it has now). What is given up: the bound no longer limits the
    consumer's unpersisted backlog, which becomes its own trade-off as on any
    channel. Departs from the letter of that ruling ("the existing backfill acks
    are the permit signal") and not its purpose. The cost is in the tests:
    about a dozen integration tests stage a parked producer by reading to the
    bound without acknowledging, and under a receipt bound reading is what
    frees it, so their staging becomes "hold the pages unread". Mechanical,
    but on the file that took seventeen rounds, and it wants its own scoped
    review. When it lands, the acknowledgement-window paragraph in
    `reference/sync.md` and on `BackfillConfig::lane_capacity` shrinks to the
    receipt rule, and the "should the engine defend" question below is moot.
    Everything below is P3 or P4 from the same review; verify against the
    code before working any of it. The rest of that list was worked on
    2026-09-06 and its entries deleted: the discard request left outstanding
    by a loss after a failed attempt (the scan now keeps a failed attempt's
    baseline and the rescan reopens on it), the ack-time ceilinged discard
    deleting a later attempt's rows (it now fences without deleting once the
    scope has minted a publication above the ceiling), `claim_checkpoint`
    answering `AlreadyPersisted` for a foreign segment, a completion marker
    acknowledged with `publication: None` (now `Unvouchable`; a PAGE with
    `None` still persists, deliberately), and the four P4 tidy-ups. One thing
    found on the way: once a LATER attempt's own completion marker is durable,
    a delayed marker acknowledgement from the earlier attempt resolves as
    `AlreadyPersisted` on the shared `Lane::Backfill(scope, completion)` key
    and never reaches the refusal at all, so the row-deleting variant of that
    P3 could only ever bite a later attempt's PAGE rows.
    - P3: `detach`'s discard drain awaits the writer with no deadline, while
      every other teardown step is clamped to `detach_timeout`. A store whose
      `delete_backfill` or `put_ledger` hangs now hangs `detach`.
    - Costs, not defects, both in the safe direction and both for item 1 to
      weigh: a lag on ANY receiver, an observer included, records losses for
      every in-flight backfill scope and requests their discard, a
      from-scratch re-walk per scope; and an abandonment retires a marker
      still in the ring, so a live burst that lags the consumer between a
      walk's last page and its marker acknowledgement re-walks that scope
      from scratch.

- **sync tenant throttle identity.** `ThrottleScope::Tenant` cannot be enforced
  across sibling accounts because `AccountError` carries no tenant identity.
  `bifrost-sync` can key mailbox throttles from `ErrorScope::Mailbox` and
  provider throttles from `AccountError::provider`, but tenant errors always
  fall back to the reporting account. This silently under-throttles siblings
  when a provider issues a tenant-wide 429. `bifrost-types` would need a
  bounded, non-secret tenant identity on `AccountError` or its throttle advice,
  protocol error translators would need to populate it, and sync could then
  enroll accounts under `ThrottleKey::Tenant`. Round 5 stopped widening a
  mailbox throttle with no mailbox identity to the whole account; tenant scope
  remains account-local until that cross-crate identity channel exists.

- **graph move concurrency verification.** `bulk_move` refreshes every missing
  message etag with a GET and sends `If-Match` on
  `POST /messages/{id}/move`, while Graph advertises
  `mutation.concurrency: StateBased`. Microsoft does not document `If-Match`
  for the move action, but that silence does not establish that the service
  ignores it. Verify against a live Graph mailbox by moving a message with a
  deliberately stale etag and observing whether the action rejects with a
  precondition failure before changing either the concurrency capability or
  removing the etag preflight. Until that experiment is recorded, the code
  retains the header and the capability is an explicitly unverified promise.

Open work surviving the close-out of the error-model project. Items
here were either explicitly deferred during phase 5 or are tail
cleanups the audit surfaced and the decisions doc marked "fix per
spec" without scheduling. Verify against current code before working
any item; some may already be obsolete.

## bifrost-jmap

- **jmap-D4.** Generic JMAP `Provider`. Wire `Provider::Fastmail` (and
  any other JMAP host the factory needs) when documented. Continue
  setting `Provider: None` until then.

## bifrost-imap

- **imap-G1.** (gap, feature-sized) Expose IMAP MIME-part downloads as
  real `BlobHandle`s. Symptom: the account used to advertise
  `BlobRangeSupport::Yes` and accept any `BlobHandle` in `open_blob` /
  `open_blob_range`, but nothing in inventory or hydration ever minted
  such a handle - no part identity, no part size, no encoding - so the
  only handles that reached the openers were ones a caller had to invent
  by hand from a private id encoding. What would have to ship: a
  BODYSTRUCTURE-to-part traversal, a stable part-handle encoding (folder,
  UIDVALIDITY, UID, part path, transfer encoding), and a consumer-facing
  projection that attaches those handles to hydrated MIME parts so
  `InventoryEntry::blob_id` and attachment metadata are populated.
  `bifrost-types::mime` now ships the decoded MIME part tree
  (`ParsedMessage` / `MimePart`) that any such traversal should reuse
  rather than recreating. What was done instead:
  the capability now reports `BlobRangeSupport::No` and both openers
  return `Unsupported`, with the private blob id codec and its openers
  deleted; `open_raw_rfc822` (whole-message `BODY.PEEK[]`) is unchanged
  and remains the supported byte path. What remains merely disclosed:
  consumers that want per-attachment streaming from IMAP still cannot
  have it - they now get an honest `Unsupported` instead of a handle
  shape they could not obtain.

## bifrost-caldav / bifrost-carddav

- **caldav-F1 (residual).** The snapshot lanes are now VEVENT-filtered on
  the server (`list_event_hrefs_filtered`, 2026-09-06), so a VTODO no
  longer enters the cursor at establish, inventory, or the polling
  changes fallback. What remains is the `sync-collection` lane: RFC 6578
  has no filter grammar, so a task resource created or modified between
  two token polls is still reported once as a created/updated event
  change that hydrates to nothing, and then sits in the snapshot.
  Closing it needs positive evidence that a newly-reported href is not a
  VEVENT, and the cheap shapes each open a hole: a comp-filter query run
  after the sync report cannot tell "not a VEVENT" from "created after
  the query ran", and dropping the href on that evidence loses a real
  event permanently, since the token has already advanced past it. The
  correct shapes are an unfiltered listing plus a filtered query (two
  round trips on any poll that creates, ordered listing-then-filter so a
  mid-flight creation lands in neither drop set), or a durable per-href
  classification mark in the cursor payload - which is a version-3
  envelope. Neither is obviously worth its cost against a leak this
  narrow; needs a ruling before it is built.
- **caldav-F2 (filed 2026-09-06, lateral).** `getcontenttype` is requested
  on every depth-1 event PROPFIND (`PROPFIND_EVENTS` in `client.rs`) and
  staged and committed by `EventProps` in `parse.rs`
  (`EventProps::content_type`), and then read by nothing at all - grep for
  `content_type` across `crates/caldav` and `crates/dav-core` returns only
  the declaration, the commit and the parse arm. So it is a property on
  the wire and a field in the parser that no lane consumes. Two ways out
  and they point opposite directions: drop it from the request and the
  `PropSet`, or USE it - RFC 4791 s5.2 lets a server answer
  `text/calendar; component=vevent`, which would be free component-type
  evidence for the caldav-F1 residual above on the servers that emit it
  (SabreDAV / Baikal do). Decide against F1 rather than in isolation.

## bifrost-sasl

- **sasl-F1.** Typed public auth-outcome surface. The SASL/channel-binding
  work landed the computation, mechanism selection, and downgrade protection,
  but deferred a typed public record of *which mechanism + channel binding
  were used* on success (useful for audit logs / enterprise debugging) and a
  typed failure reason (mechanism rejected, channel binding required but
  unavailable, credential rejected, server protocol violation). Today the
  protocol crates map into their existing error types and expose no such
  outcome record. Was step 5 ("Public API shape") of the deleted SASL plan;
  build it when a consumer (ratatoskr) needs the audit surface. Lives in
  `bifrost-imap` / `bifrost-smtp` (the public auth surfaces), not the private
  `bifrost-sasl` crate.

  Re-scoped 2026-07-31 against the question "shouldn't the error model
  already give us this?". Partly yes, and the item is smaller than filed:

  - FAILURE half, largely redundant. `crates/imap/src/error.rs` already
    carries `AuthPolicyFailure` with the offered mechanism list and
    per-mechanism `AuthMechanismRejection { mechanism, reason }`, which
    reaches `AccountError` as support-only diagnostic text. So "which
    mechanisms were rejected and why" is already available on the
    local-policy path. Verify what the SERVER-rejection paths carry, then
    close this half rather than building a parallel typed surface beside
    the error model.
  - SUCCESS half, genuinely unreachable that way. The error model only
    speaks when something fails; there is no error object to hang
    "authenticated with SCRAM-SHA-256 plus tls-exporter channel binding"
    on. No amount of improving error plumbing produces a success record.

  So the real remaining item is the success-path outcome record, and it
  should not be designed as a mirror of the failure surface that already
  exists. Still waiting on ratatoskr to need the audit trail.

## bifrost-graph

- **graph-A5b-1.** Public-folder deletion reconcile is side-table-free:
  the live-id baseline rides in the cursor (`PublicFolderCursor.live_ids`)
  hard-capped at `PUBLIC_FOLDER_LIVE_IDS_CAP` (10_000). Above the cap a
  folder degrades to additions-only (no `Destroyed` emission). Restore
  reconcile for huge folders with a `CheckpointStore`-backed deletion
  baseline once bifrost owns that side table. (A5b v1 follow-up.)
  BLOCKED (verified 2026-07-21): the side table does not exist and no
  account-reachable path to one does. `CheckpointStore` lives in
  `crates/sync/src/cursor/store.rs`, is held only by the engine, and is
  never handed to `Account` impls; the graph crate does not depend on
  `bifrost-sync` and so cannot name it; the trait exposes only change
  cursors + backfill checkpoints keyed by `(account, scope[, partition])`,
  no free-form key/value surface. Unblocking is a cross-crate prerequisite
  epic - extend `CheckpointStore` with a generic side-table put/get and
  thread a store handle into `Account::open` (touches `bifrost-types`,
  `bifrost-sync`, and every account crate). Do not schedule A5b-1 until
  that lands. Nothing is broken today: the degraded additions-only mode is
  correct and covered by tests; this is a capability upgrade, not a fix.

## bifrost-sync

- **sync-F6.** (residuals of the closed F4+F5 throttle wiring) What
  bounds the now-wired `ThrottleBucket`:
  (a) `ThrottleScope::Tenant` degrades to the `Account` key because the
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
  (d) No hermetic worker-level test proves a recorded deadline defers
  `changes_stream` or that two attached slots share a provider
  deadline; the bucket mechanics are unit-pinned in `recovery.rs`.
  No longer blocked on a stub (the `Account`/`AccountFactory` stub in
  `tests/attach_schema_recovery.rs` covers it), but blocked on a clock
  mismatch: `ThrottleBucket` deadlines are `SystemTime`, while the poll
  loop sleeps them off on tokio time. Under `start_paused` the sleep
  returns without `SystemTime::now()` having moved, so the re-check
  loop in `spawn_scope_poll` re-derives the full wait and spins - a
  virtual-time test of the deferral cannot terminate, and a real-time
  one would need a wall-clock `Retry-After`. Pinning this wants the
  bucket to carry a monotonic deadline (or an injectable clock) first.

## bifrost-types

Surfaced while authoring `reference/error-model.md` (a read of
`crates/types/src/error/`). All pre-existing, none blocking.


## A9 (directory search) follow-ups

- **a9-1 (carddav)** RFC 6352 directory-gateway leg. `directory_search`
  is `Unsupported(DirectorySearch)` on CardDAV; a server may advertise an
  optional read-only directory-gateway address book, but gateway discovery
  is a substantial provider-specific unknown with no ratatoskr precedent.
  Scoped out of A9 to keep the blast radius bounded.
- **a9-2 (graph)** `/users` `otherMails` / `proxyAddresses` into
  `DirectoryCard.additional_emails` (A9 ships the single `mail`).
- **a9-3 (graph)** `$search` (with `ConsistencyLevel: eventual`) as a
  richer substring directory match than the current `startswith` prefix
  `$filter`, if needed.

## C-3 (Graph send-as) follow-ups

Scoped out of C-3 (the Graph shared-mailbox send-as / send-on-behalf-of brick)
to keep its blast radius on the Graph send path. C-3 landed the typed
`SendRequest::send_as` surface and the Graph backend; the remaining item is a
provider capability ratatoskr may eventually wire, currently rejected with
`Unsupported(Send)`.

- **c3-2 (imap/smtp)** Shared-mailbox send over SMTP for IMAP-shaped accounts.
  C-3 rejects a `Some(send_as)` on IMAP because Graph-style mailbox routing has no
  SMTP analog. A shared-mailbox send over SMTP is the consumer setting
  `request.from` to the shared address and letting the relay's Send-As policy
  authorize it - that path already works and needs no `send_as`. If a consumer
  later wants `send_as` to map onto an SMTP `From:`/`MAIL FROM` choice (so the
  uniform surface carries shared-mailbox send for IMAP-shaped accounts too),
  decide whether IMAP honors `send_as` by translating it to a `from` override or
  whether it stays a deliberate `Unsupported`. Today: deliberate `Unsupported`.

## Namespaced-container follow-ups

Surfaced while landing the namespaced-container surface (shared-mailbox and
public-folder containers, EWS public-folder hydration, allowlisted public-folder
scopes). Each was deliberately out of that brick's scope; none blocks the
container projection itself.

- **nc-2 (graph)** Make the EWS `GetItem` property shape class-conditional by
  threading `EwsItem.item_class` from inventory; today a mixed-class pinned
  public folder fails hydration per item on every non-mail class (full context
  at `EwsClient::get_item`).
- **nc-4 (graph)** `well_known_folder_roles` is only correct for the primary
  mailbox, so shared-mailbox containers fall back to display-name matching and
  their Inbox / Sent carry no `FolderRole`. A correct fix costs about six extra
  round-trips per shared mailbox; decide whether the roles are worth it.

  Re-framed 2026-07-31 after asking what an end user actually loses. Not
  cosmetic. `FolderRole` (`Inbox | Sent | Drafts | Archive | Trash |
  Spam`, `crates/types/src/container.rs:87`) is documented as the canonical
  role a container plays in ratatoskr's UI, so without it on a shared
  mailbox the app holds names and no routing: Send does not know where to
  file the copy, Delete does not know which folder is Trash, Save-draft
  does not know Drafts, Not-spam has no target, and icons plus ordering
  fall back to alphabetical.

  The decisive part is the FALLBACK. Display-name matching is
  locale-dependent - a German tenant's shared mailbox is `Gesendete
  Elemente`, not `Sent` - so shared mailboxes work by accident on English
  tenants and degrade silently everywhere else. The real question is
  therefore not "are icons worth six round-trips" but "is correct
  destructive-action routing on non-English tenants worth six round-trips
  per shared mailbox AT OPEN" (not per operation). Framed that way it
  looks like a yes, but it is still unruled.

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

- **xc-2 (types + sync + every account crate)** Subscription teardown depends
  entirely on the caller, and the contract says so deliberately.
  `Account::close` is idempotent LOCAL teardown and explicitly does not delete
  durable server-side subscriptions (`reference/types.md`); engine detach
  cancels workers and calls `close`; the engine tells consumers to tear the
  subscriptions down themselves. So a consumer that detaches without
  unsubscribing strands live server subscriptions - for Graph, up to 24h, with
  provider-side expiry as the only backstop. This is the documented contract
  rather than a defect, and `bifrost-graph` correctly must NOT add best-effort
  deletion in `close`. The open question is whether the shared contract should
  keep placing that burden on the consumer at all.

  UNRULED as of 2026-07-31. Context below was verified against the code
  during a review pass; the ruling was explicitly deferred, and the
  reviewer disagreed with the recommendation recorded at the bottom, so
  treat that recommendation as one input rather than a plan of record.

  First, a naming trap worth knowing before reading any of this. There are
  TWO entries, one per layer, and their names are near-anagrams:
  - `Account::push_unsubscribe(handle)` - protocol-crate trait method
    (`crates/types/src/account.rs`), destroys ONE subscription.
  - `SyncEngine::unsubscribe_push(account_id)` - engine method
    (`crates/sync/src/engine/mod.rs`), takes the account's registry records
    and calls the trait method once per record.
  `Account::close`'s doc points at the first; `SyncEngine::detach`'s doc
  points at the second. Both are correct for their layer, and an app calls
  the engine one because an app holds an engine, not an `Account`. Read
  within a page of each other they look like a typo for one another.

  Two facts the original entry does not capture, both verified:

  1. The cleanup window closes SILENTLY. `SyncEngine::unsubscribe_push`
     looks up `self.accounts` first and returns `AccountNotAttached`, so
     after `detach` there is no API that can reach the handles - even
     though the engine still holds them. The consumer's only opportunity
     to do the job the contract assigns them ends at detach, with nothing
     enforcing or signalling that.
  2. Records outliving the account is FIXED: `detach` now takes the
     registry records, so a reattach of the same `AccountId` cannot
     inherit handles minted by a dead connection. Option D also landed in
     its low-cost form - a detach with records still registered logs on
     `bifrost.sync.push`. A structured `Warning` was not used: detach has
     already torn the change stream down, so no lane is left to carry one.
     Both are pure hygiene and foreclose none of the options below.

  Options considered, stated neutrally (A and the log-line form of D have
  landed; B and C remain open, and the contract question is UNRULED):
  - **A** Hygiene only: `detach` clears the registry, and the window is
    documented explicitly. Leaves the contract alone. Landed.
  - **B** `detach` always tears down. Fixes both. Argument against: push
    delivers to a consumer-owned endpoint (webhook, Pub/Sub topic), so an
    app that shuts down and wants events to queue for its next start is a
    legitimate pattern that unconditional teardown breaks silently.
  - **C** A, plus an explicit opt-in (`detach_with_teardown`, or a flag),
    leaving plain `detach` unchanged. Consumer states intent; neither
    pattern is penalised. Costs public API surface.
  - **D** A, plus a `Warning` emitted on detach when records were still
    live, so an app that forgot finds out. No API addition.

  The recommendation offered at the time was A + D, on the grounds that
  (2) is a defect regardless and that surface should wait until ratatoskr
  asks. That was disputed and is NOT settled - re-derive the choice rather
  than inheriting it. The two entry points are disambiguated in
  `reference/sync.md` under "Push reconciler", alongside the detach
  semantics above.

- **xc-4 (sync, maybe app). Rename `SyncEngine::reopen`.** The cadence
  ruling that produced this item (share-rediscovery cadence is consumer
  policy; the engine grows no rediscovery timer) is settled and recorded in
  `reference/sync.md` under the `reopen` paragraph, together with its
  rejected alternatives. What is left is the rename.

  The name undersells the operation and actively hides it from the consumer
  the ruling puts in charge: someone told "drive share rediscovery yourself"
  will search for something named `rediscover*` and find nothing. The method
  does a full staged reattach - re-runs scope and membership discovery,
  establishes newly-appeared scopes, drops vanished cursors, recreates push
  subscriptions, refreshes the capability snapshot, then swaps the handle.
  Candidate names: `rediscover_and_reattach` (most literal), `reattach`,
  `reopen_and_rediscover`. Not settled.

  Sizing, because it is bigger than it looks: `reopen` appears 105 times in
  `sync/src/engine/` and 35 times in `reference/sync.md`, and most of those
  are the reopen LANE (`reopen_tx`, `reopen_lock`, `ReopenRequest`, the
  reopen listener), not the public method - a blind rename would churn the
  internal vocabulary too. Decide whether the lane keeps its name.

  The sharper consequence: the capability flag
  `reopen_discovers_foreign_namespaces` NAMES the method. Renaming the
  method either drags the flag with it - a `bifrost-types` public API change
  touching all seven account crates plus every test stub - or leaves the flag
  naming a method that no longer exists. That coupling is the real cost of
  the rename and should be decided before starting, not discovered midway.

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
- **sync-B5. Ledger compaction and audit retention.** [C4] Discharged entries
  are retained forever for audit, so the ledger grows monotonically. Compaction
  needs to preserve the proved/waived distinction rather than flattening it -
  separate counters or audit roots for proved discharges, waived unresolved
  loss, and currently-open obligations. Not urgent at present volumes; it
  becomes real now that repair churns entries.

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

### Open defects

- **google-B2-residual. Drive session cleanup does not survive a dropped
  future.** [C2] The mid-upload abandonment is fixed (2026-08-29): every exit
  between `create_upload_session` and a completed upload now cancels the
  session, and a cancel that itself fails decorates the error as an abandoned
  session. What remains is the cancellation case - if the caller's future is
  dropped mid-upload, no cancel runs and Drive holds the partial for a week.
  A `Drop` guard is deliberately NOT the answer: `Drop` cannot await, and
  spawning the DELETE from `drop` trades an expiring server-side session for a
  detached task outliving the account handle. This is the same shape as the
  documented `close()`-dropped-mid-`users.stop` residual. Resume-on-reopen is
  also out: the session URI is per-call state that no `HostAttachment` request
  carries back in, so resumption needs a published surface change.

### Cross-crate shaping questions

- **No concurrency governor in `bifrost-net`.** Nothing bounds the number of
  simultaneously in-flight requests, per account or globally. The only
  overlapping-request site in the workspace is JMAP's foreign probing at open,
  which solves it locally: `foreign_probe_concurrency` bounds a
  `buffer_unordered` by the server's `maxConcurrentRequests` clamped to `[1, 8]`,
  serial when the core capability is unreadable, with results sorted by
  `accountId` before installation so topology and skip ordering stay
  deterministic. Any new concurrent call site has to solve it again from scratch.
  This is a new permit-pool feature with its own API and test-bite obligations,
  not a defect - it stays a recorded deferral until someone wants the feature.
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

- **google-B7.** `changes.rs`, `mutation.rs`, `inventory.rs::get_stream` and
  `scopes.rs::scope_lifecycle_stream` are four near-identical hand-rolled
  `stream::unfold` state machines, each with its own `finished`/`emitted_done`
  pair, its own batching, its own terminate-and-emit-`Done` dance. Relatedly,
  `terminates_mutation_stream` encodes a real fan-out policy in `error.rs` where
  it belongs, but only the mutation driver consults it: inventory always
  terminates, `get_stream` always fans per-item, and the lifecycle stream uses
  `is_terminal() || requires_engine_action()`. Three different answers to one
  question. A shared `BatchedStream` driver plus a `FailurePolicy::for(error, lane)`
  would collapse ~400 lines and make the boundary and terminate contracts
  enforceable in one place.
- **jmap-B1.** Three near-identical query/get/advance loops in the sync layer;
  `imap` has four copies of the untagged-response dispatch loop. Recorded for
  completeness with the other duplication findings; same standing as the above.

## Open items folded in from the second bug-hunt wave (2026-08-29)

The deferred tail of the August 2026 arcs. The same category labels and the
PUBLISHED SURFACE fence apply.

- **jmap-J13. Positional paging in the consumer-facing list and search
  paths.** [C3] `crates/jmap/src/contacts.rs`, `calendar_ops.rs` and `pim.rs`
  page by integer position over orders that are not total - the same
  unstable-order shape J1 fixed for the inventory walk. Deliberately NOT J1
  reopened: those are consumer-driven page-cursor APIs, not coverage-claiming
  walks, so churn-induced skip or duplication is ordinary list-API behaviour
  and nothing reports complete coverage off them. High confidence the paging
  is positional, LOW confidence it is a defect. The better mechanism exists
  and is known: carry an anchor id on the page cursor, as the inventory walk
  does, so a consumer paging a churning list gets stable continuation instead
  of positional drift.

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

- **types-B1b. `send_as` gates a request FIELD, not a method, and the crates
  disagree.** imap and google reject a `send_as` request with
  `Unsupported(Send)`; jmap's `route_send_as` answers `Request(Malformed)` for
  an unknown foreign id and `Unsupported(Send)` only for a known-but-not-
  submission-capable one. Not asserted anywhere and not expressible in the
  types-B1 test, which drives methods. Decide whether the uniform answer is
  worth it, or document the split. Related: c3-2.

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

- **types-B3. The `Reconcile` advice lane lacks the fields `Retry` has.** [C3]
  Both `retry_hint` and `throttle_scope` describe WHEN a failure applies and
  HOW WIDELY, regardless of which lane it lands in, so the two advice types
  want a shared carrier rather than one of them carrying the pair. Coupled to
  types-B1: the audit's premise that optional lanes have defaults is what
  makes the capability mirror necessary, so solving the capability problem is
  what would eventually make a narrower handle possible - not a supertrait
  split of the 94-method trait, which was considered and rejected on its own
  merits.

- **google-B13. `events_search_url` sends `singleEvents=true` with no
  `showDeleted`.** [C4] `crates/google/src/account/calendar.rs`, exactly as
  `events_in_range` did before G8. Deliberately NOT filed as the same defect,
  and a reflex copy of the G8 fix is the wrong move: a range reread is a
  COVERAGE question, where a missing tombstone is indistinguishable from a
  page boundary, whereas a SEARCH returning cancelled instances is a PRODUCT
  decision about what a query surface should answer. Wants a deliberate
  answer.

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

- **jmap-C2. The SSE stream tears down on one malformed event payload.** [C4]
  `crates/jmap/src/event_source/stream.rs` (`break 'events`). SSE's design
  intent is skip-and-continue; the crate reconnects and replays from
  `lastEventId` instead, on the argument that an undecodable state-change
  payload means the push contract is broken and dropping it would lose the
  change it carried. Defensible either way. The comment at the loop records
  the choice; this item is the decision to revisit it.

- **jmap-C3. Generic requests fall back to the lowest-capability primary
  account.** [C4] `Session::default_account_id` picks the lowest capability
  URI in `primaryAccounts`, so a session advertising only calendars serves a
  mail-shaped generic request off the calendar account. The empty-id half
  (no `primaryAccounts` at all) is refused before the wire; narrowing the
  fallback itself is a product decision, recorded at the function.

## Notes

- The rules for working an item are at the top of this file. One more that
  earned its keep: an existing test you must modify is a red flag; justify
  it explicitly.
- Item keys: F-items came from the phase 5B/5C/5D error-model re-audits,
  N-items from the original post-phase-4 audit, B-items from the 2026-08
  bug-hunt ledgers, C-items from the 2026-09-04 hunt. All are tracked at
  the same level; none blocks ratatoskr.
