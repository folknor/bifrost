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
**google-B13**, **types-B1** (the rewrite half), **types-B2**, **sync-B6**. The
tell is that the remedy is a default, a shape, or a policy that only the caller
can evaluate.

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

- **The ack writer shares the worker deadline, so a straggler starves its
  drain.** Found 2026-09-07 while clamping `Account::close()`, and it is the
  SAME SHAPE as that fix one step over: `detach` gives the stream workers and
  the ack writer one `deadline`, so a provider stream that stops yielding burns
  the whole `detach_timeout` and the writer is then aborted with
  `remaining == 0` - immediately, without draining. That is precisely the
  "writer aborted with unpersisted work" outcome the two-phase ordering and
  `take_ack_writer` exist to prevent, reached by a different route. The remedy
  is the one the close clamp already took: a fresh budget for the writer phase,
  on the same reasoning (detach is documented as worker awaits PLUS the trailing
  phases, and sharing one deadline silently zeroes whichever runs last). I
  believe this is settled by consistency rather than being a fresh decision, but
  it changes how long a detach can take in the worst case, so it is recorded
  here rather than assumed.

- **Three stream-poll loops have no shutdown arm**, which is what makes the
  above reachable: `drive_changes_stream`, `InventoryFusion::run_stream` and
  `BackfillRunner::run_partition` poll a provider stream with no select on the
  cancellation token, so a stream that neither yields nor ends parks its worker
  until detach aborts it. `reference/sync.md` documents that the DELAYS around
  the drive select on the token; the drive itself does not. Two tests now
  deliberately exploit this to hold a teardown window open, so closing it would
  break them - by design, and they say so.
- **Residuals of the bounded-backfill cold review.** P3 or P4 from that
    review; verify against the code before working any of it. The rest of
    that list was worked on
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
    - DEFERRED 2026-09-07, and the deferral is the ruling: this was put to the
      repository owner as a representation choice (tally, park, forfeit, or
      leave) and the choice was declined as premature. The reason is upstream of
      every option on the list. The whole publication ledger - receipts,
      per-publication acknowledgement, supersession, the boundary registration -
      is the price of ONE bargain offered to a consumer: persist-then-
      acknowledge, in exchange for a mirror that is provably complete or
      precisely annotated where it is not. No consumer has ever paid that price.
      `ratatoskr` is the intended one and has not wired it, and what it actually
      needs from sync is open. Optimizing the internals of a contract whose
      TERMS are unvalidated is the wrong activity: if the answer turns out to be
      "tell me when you are degraded and I will re-walk", most of this ledger
      evaporates and the memory question with it. Do not work this item, or
      re-file it as a defect, until ratatoskr's requirement is known.
      One observation banked from the read, because it survives whatever
      ratatoskr wants and sharpens the item if it comes back: `reference/sync.md`
      states that only ONE acknowledger is supported. With one acknowledger and
      one sequential producer per lane, acknowledgements are already effectively
      ordered - so the out-of-order acknowledgement that `subsumed` exists to
      serve is a shape the contract does not admit in the first place. If that
      holds under scrutiny (it was NOT verified against the code), the memory
      growth is a symptom and the disease is that the ledger carries a mechanism
      for a caller that is not allowed to exist. The candidate answer then is
      per-lane monotonic acknowledgement, which deletes `subsumed`,
      `SubsumedPage`, the `folded` map and the hot-path `absorb`, makes the
      per-page stamp invariant true by construction, and removes the
      `debt_only` demotion in `release_undelivered` - which is a live cost, not
      tidiness: an undelivered subsumed page currently destroys its survivor's
      clean proof and pays for it in re-walks. Its own tension, unresolved: a
      watermark acknowledgement needs a CUMULATIVE receipt per lane to replay
      across a writer restart, which wants `CoverageClaim` to merge reports by
      domain instead of appending them.
    - FILED by the receipt-bound landing, not fixed in it: a surviving
      backfill entry's `subsumed` history is no longer bounded by
      `lane_capacity`. It was, while the charge was the unacknowledged page -
      the producer parked once the charge reached the bound, so no partition
      could fold more than that many pages into one record. Under the receipt
      bound a READ page carries no charge, so a consumer that reads without
      acknowledging lets one partition's `subsumed` grow with the partition,
      each retained `PublicationId` holding an `Arc<PublicationReceipt>` (a
      checkpoint plus a whole coverage claim). Evidence:
      `PendingCoverage::retained_history` counts exactly this, and
      `reading_a_superseded_page_frees_the_charge_it_left_on_its_survivor`
      shows a read page staying in `subsumed` at zero charge. The pages
      cannot simply be dropped on receipt: the departure sweep and
      `abandon_checkpoints` judge the completion guarantee one page at a
      time, and a read-but-unacknowledged page whose reader leaves is still a
      recorded loss. So a fix is a product decision about what to do at a cap
      - forfeit the per-page loss record, or park the producer on retention -
      and wants its own ruling. Growth is bounded per attachment by the pages
      one partition publishes, and every path that retires the entry (ack,
      lag, reset, departure, detach) clears it.
      Two more faces of the same thing, found by the slowest-reader round and
      filed here rather than fixed in it. (a) `register_in` folds each
      superseded claim's `reports` into the survivor's claim via
      `CoverageClaim::absorb`, so the survivor carries not only one
      `Arc<PublicationReceipt>` per retained page but a report vector that
      grows with the partition too - memory grows with the partition until the
      last acknowledgement, not with `lane_capacity`. (b) the blast radius of a
      lag grew with it: the ack bound capped the pages a live-lane burst could
      abandon at `lane_capacity`, and under the receipt bound one burst that
      lags a read-without-acking consumer abandons an unbounded number of READ
      pages, each costing a re-walk. The CHEAP answer is written and needs no
      ruling: the consumer-facing note now on `BackfillConfig::lane_capacity`
      says coarse acking widens both memory and lag re-work. The STRUCTURAL
      answer is the one for the owner - compress read subsumed pages into a
      per-stamp `(delivered_at, count)` tally instead of retaining ids, since
      the departure sweep and `abandon_checkpoints` need only the stamp and a
      count, and an acknowledgement of a superseded id is already answered by
      the `folded` watermark rather than by the retained page. That would keep
      the per-page loss record while dropping the receipts, so it is not the
      forfeit-or-park choice above; it is a representation change with its own
      correctness argument and wants its own ruling.
    - Cost, not defect, in the safe direction: an abandonment retires a
      marker still in the ring, so a live burst that lags the acknowledging
      consumer between a walk's last page and its marker acknowledgement
      re-walks that scope from scratch. (The other half of this bullet, a lag
      on an observer doing the same, was retired by the observer
      subscription: an observer's lag abandons nothing.)
    - FILED by the last (2026-09-06) pass, not fixed in it - **the
      slowest-reader bound holds only on the READ path.**
      `backfill_in_flight` sums live entries only, and every acknowledgement
      path (`acknowledge_publication`, `acknowledge_checkpoint`,
      `retire_publication`, and the supersession fold in `register_in`)
      removes the entry, or strips the subsumed page, together with the
      `readers` set that recorded a slower receiver having not read it. So if
      the ACKNOWLEDGER is faster than a numbered observer: slow (seq 0) and
      fast (seq 1) subscribe; P1 and P2 are published; fast reads and acks
      both; `in_flight` is 0 while slow read nothing; the producer publishes
      `RING` more; slow is overwritten and gets `Lagged`; `on_lag` abandons
      every registration, including pages fast READ but did not ack; the
      marker is withheld, the walk restarts, and this repeats for as long as
      the observer stays slower. The structural close the reviewer proposed:
      keep a residual `(id, delivered_at, readers)` record when an entry
      leaves the ledger still unread by an eligible live receiver, sum it in
      `backfill_in_flight`, update it from `mark_received` and
      `receiver_departed`, and clear it on lag, on reset and on cap eviction.
      Deliberately NOT built in that pass (the owner ruled the round closed,
      and this is a second mechanism, not a correction to the one that
      landed). Scope note before anyone works it: the explicit observer
      subscription has since landed and an observer is unnumbered, so the
      case where the slow receiver is an observer no longer arises; what
      remains is two ACKNOWLEDGING receivers, which `reference/sync.md`'s
      consumer contract already declares unsupported. So this is most likely
      a residual the observer subscription retired rather than work to
      schedule; delete it once someone confirms no supported shape reaches
      it. What landed instead: `BoundaryEntry::readers`, the
      `sync.md` contract paragraph and `BackfillConfig::lane_capacity` now
      state the guarantee precisely ("the slowest live numbered reader among
      pages not yet acknowledged; an acknowledgement frees a page for every
      receiver") and name this failure.
    - Also filed there: drop a read subsumed page's receipt claim once every
      eligible reader has read it, as a bound on the unbounded `subsumed`
      history above. Same shape as the `(delivered_at, count)` tally, and it
      has the same obstacle - the departure sweep judges the completion
      guarantee one page at a time - so it wants the same ruling.

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

- **dav-F7 (P4, filed 2026-09-06, cold review).** Page-size default drift:
  CalDAV `event_search` and `events_in_range` use `usize::MAX` when `limit`
  is `None` (`account.rs`, two sites), CardDAV `contact_search` defaults to
  `CONTACT_PAGE_SIZE` (250). Neither reference states the CalDAV default, so
  a consumer omitting `limit` gets an unbounded page from one crate and a
  250-entry page from its twin with nothing documenting either.
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

## bifrost-smtp

Filed 2026-09-06 from the cold review of `client/core.rs`, both I/O adapters,
the pool and the batch callers. The two P2s from that review (the batch
DATA-final-negative `uncertain` lane, and the async body upload bounded by one
per-operation timeout) were fixed in the same pass; everything below is
verified against the code and awaiting its own ruling.

- **smtp-CR7 (P4, doc).** Two reference corrections, plus one unreachable
  shape. (a) The "Transport types" list of deliberate half-differences is now
  three, not two: the async setup deadline is ONE shared `AsyncDeadline` across
  DNS, connect, TLS, banner and EHLO, while the blocking half has a connect
  timeout plus per-operation `SO_RCVTIMEO`, so blocking setup can take roughly
  3x the configured timeout where async setup cannot exceed it. That is a real
  behavioural difference and it is documented only as two separate mechanisms.
  (b) `DirectSmtpStage::Start` on the pipelined path with an EMPTY recipient
  list calls `start_window(0)`, which immediately falls through to
  `after_envelope()` and writes `DATA` with no `MAIL FROM` ahead of it.
  Unreachable through `Envelope::new`, which requires at least one recipient,
  but the machine itself does not enforce it.

Filed 2026-09-06 from the third cold review of the same diff. Its two other
findings (the batch machine parking a 421'd connection, and a cap retune
re-pricing outstanding metering debt) were fixed in that pass.

- **smtp-CR11 (P3).** With a per-operation timeout of about one second or
  less, every capped write after the first waits its parked ~1 s of debt
  INSIDE `with_timeout` and times out. The offer clamp bounds parked debt at
  one second of cap, which is only helpful while the timeout is comfortably
  larger than a second. Fix direction: clamp the offer to a FRACTION of the
  timeout's worth of cap (the clamp already re-reads the cap per call, so it
  could read the timeout too), or document the constraint as a lower bound on
  a usable `timeout` under a cap.

- **smtp-CR12 (P3).** There is no TOTAL bound on a body write. The re-armed
  per-write timeout deliberately bounds only a write that made no progress, so
  a peer draining one byte per `timeout - epsilon` keeps an upload alive
  indefinitely. The read side explicitly closed the same shape - the per-reply
  deadline exists precisely because a peer trickling one line per period could
  stretch one reply - so the asymmetry is deliberate on one side and unstated
  on the other. Either add an outer bound or a minimum-progress rule, or state
  the asymmetry in `reference/smtp.md` beside the per-reply read deadline
  section.

- **smtp-CR13 (P4).** TLS record overhead is not counted. `charge` meters
  PLAINTEXT bytes, and under a small cap each clamped `poll_write` becomes its
  own TLS record, so the bytes actually on the wire exceed the cap by the
  per-record overhead - and the smaller the cap, the worse the ratio. Note
  only; charging ciphertext would need the meter below the TLS layer.

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
- **sync-B5. Ledger compaction and audit retention.** RULED 2026-09-07, not yet
  landed. Discharged entries are retained forever for audit, so the ledger grows
  monotonically, and repair churns entries faster than when the retention rule
  was written. Note that two neighbours are already bounded and must not be
  confused with this: `proved` prunes (a proof older than newly-raised debt
  cannot discharge it, and proofs whose load-bearing debt closed are discarded)
  and so do `barriers`. It is `entries` that never shrinks.

  Compact `Discharged` entries ONLY, into a per-scope count plus an audit root.
  Every `Unresolved` entry stays a live entry, WAIVED ONES INCLUDED. The line
  falls out of `ProofStatus`: `Discharged` is terminal, since something proved
  the coverage and nothing will transition it again, while a waiver leaves the
  entry `Unresolved` forever by design, and a later walk with a covering domain
  can still discharge it. Compacting a waived entry would destroy an
  `ObligationKey` an operator may still need and a proof may still land on.
  This satisfies the constraint the item names - preserve the proved/waived
  distinction rather than flattening it - by construction rather than by
  bookkeeping: only the proved side compacts, so the waived entries are exactly
  the ones left sitting there in full.

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

- **jmap-C3. Generic requests fall back to the lowest-capability primary
  account.** [C4] `Session::default_account_id` picks the lowest capability
  URI in `primaryAccounts`, so a session advertising only calendars serves a
  mail-shaped generic request off the calendar account. The empty-id half
  (no `primaryAccounts` at all) is refused before the wire; narrowing the
  fallback itself is a product decision, recorded at the function.

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

- **Four owner rulings, none blocking.**
  (a) Move `should_fallback_discovery` into `bifrost-dav-core`, parameterised by
  the existing `DavProtocol` (the only difference between the copies is
  `ResourceKind::Calendar` versus `Contact`, which `DavProtocol` already
  carries). Internal only, but it touches three crates. The case: this wave was
  the second time the twins needed the SAME edit, and the first time they needed
  DIFFERENT edits to reach the same behaviour - the CardDAV copy parsed the probe
  body inside its `Ok(response)` arm and lifted the failure with `?`, so the
  parse error never reached the predicate at all. Eleventh measured divergence.
  (b) Bound the STARTTLS handshake under `timeout(None)`. `AsyncSmtpConnection::
  starttls` passes `self.timeout` to `upgrade_tls`, so a transport built with no
  timeout has an unbounded TLS handshake on the explicit-STARTTLS path. Same root
  cause as the teardown cap that landed, but a handshake is a PROTOCOL operation,
  so the teardown reasoning ("nothing left to accomplish past this point") does
  not carry over and a default bound is a new policy. The mechanism underneath is
  `TimeoutBudget::SetupDeadline` yielding `None` slack when built from
  `AsyncDeadline::new(None)`; any future "everything is bounded" claim starts
  there.
  (c) Add `[check] consumer_features` to `brokkr.toml`. Two tests landed this
  wave under `#[cfg(not(feature = "calendars"))]` - the feature-off SSE path,
  which is exactly where the alert ruling was silently failing - and
  `brokkr check` runs `--all-features` with no second sweep, so their bodies are
  not even typechecked, let alone run. A cold reviewer's verdict: they earn their
  place as executable assertions and provide ZERO automated protection until a
  feature-off sweep exists. Deleting them would keep the gap and lose the
  assertions.
  (d) `PushNotification::CalendarAlert` carries no resume token, so a block
  containing ONLY alerts advances no checkpoint and is replayed in full after
  every reconnect. Inherent to the type and consistent with the ordering rule
  that puts the token-bearing notification last; closing it is a change to that
  type's shape.

- **Three rulings from the ledger-compaction landing (2026-09-07).**
  (a) After compaction a re-raised key becomes a NEW entry:
  `first_seen_unix_seconds` resets to now and the retry budget to zero, where an
  uncompacted discharged entry preserved both, since `upsert` deliberately keeps
  history on re-raise. Accepted and documented on the argument that the entry was
  proved covered before folding, so a re-raise after proof is a fresh gap and
  charging it the old budget charges it for failures that provably stopped. It is
  still a behaviour change to the "re-raising must not reset history" rule and it
  is reachable in production; preserving it means retaining keys, which reopens
  the growth compaction removes.
  (b) Multi-level lineages cannot arise in process - `replace_obligation`
  re-points every child at `lineage_root(parent)`, so live shapes are always
  flat. A chain is reachable only through `decode_ledger` / `from_parts`
  restoring a durable row written by another revision or by hand. The depth cap
  and the ancestor closure are therefore either cheap insurance or load-bearing
  depending on whether such rows are possible; the tests build chains that way
  deliberately and say so. Somebody should rule on it.
  (c) `record_attempt` charges an entry regardless of `is_open()`, so a
  discharged-but-`Retrying` entry can still take charges. Pre-existing, and it is
  precisely what makes the lineage-root pin necessary. Worth deciding whether
  that is the intended contract or an accident the pin is now compensating for.

- **A capped reply read's postponement is bounded but large.** The read-side
  refund pushes a reply deadline out by the throttle debt the read itself paid,
  and the bound on that is `MAX_RESPONSE_BYTES / cap` because a peer can only buy
  time by SENDING bytes and every byte is charged. `MAX_RESPONSE_BYTES` is 100 kB,
  so at a 100 B/s cap the worst case is on the order of 1000 s. Bounded, and the
  consumer chose the cap, so this is not a defect - but if that ever needs a
  ceiling it is a separate ruling and should not be folded into the refund.

- **imap: `wait_for_continuation` reads a FOREIGN tagged response as its own
  command's rejection.** Live defect, found 2026-09-07 while unifying the
  untagged-response arms, and left unfixed because the remedy changes an
  observed error class and a termination. Any tagged response ends the wait -
  `NO`/`BAD` become that command's error, `OK` becomes a protocol violation -
  which is right when one command is outstanding. But `run_pipeline` calls it
  per command, sequentially, with earlier commands already sent and their tags
  pending. On a server without LITERAL+/LITERAL-/rev2 (`literal_mode` is
  `Synchronizing`) a pipelined command carrying a literal can therefore consume
  command #1's tagged response: the batch aborts, #1's real result is destroyed,
  and a `NO` for #1 is reported as a failure of #2. Every other loop in the crate
  distinguishes its own tag from a foreign one. The narrow fix passes the
  expected tag in and ignores a foreign one; the design question, and the reason
  this wants a ruling, is whether the pipeline should instead PARK the foreign
  response and route it into the right command's result slot - ignoring it
  discards a result that was legitimately delivered, which is the actual loss.

- **imap: two smaller divergences between the response loops**, both found in
  the same pass and both left alone because each changes something observable.
  (a) `logout_best_effort` is the only loop whose tagged arm does not call
  `emit_tagged_response_code_events`, so an `ALERT` on the LOGOUT completion is
  dropped; low impact, since the sink is about to be torn down, but it is an
  undocumented deviation rather than a stated one. (b)
  `has_critical_response_code` excludes `UntaggedStatus::Bye` while
  `emit_untagged_response_code_events` publishes for it, though the two are
  meant to be exact complements. Latent only because the prologue makes a BYE
  fatal before any consumer sees it, so a BYE never reaches the one caller - but
  the exclusion is dead as written and becomes a double-emit the moment the BYE
  short-circuit moves after classification.

- **Two documentation gaps left by this wave.** (a) `DavDispatch::resolve_url`
  now resolves a relative id UNDER the configured base path (`join_base` restores
  the trailing slash before joining, where `Url::join` on a slashless base
  replaced the base's last segment). Neither DAV reference describes relative-id
  resolution at all, so nothing was false - but the behaviour belongs in the
  dav-core write-up. (b) `reference/net.md` may need the redirect-versus-
  `Destination` interaction now that `move_resource` rebases the destination on
  the hop the source took; nobody checked it.

- **smtp `DirectSmtpStage::RcptWindowReply` keeps only the FIRST negative reply**
  in a window (`failure: Option<Response>`) and drops any later ones. It looks
  deliberate on the direct path, whose output is a single `Response`, and it is
  the direct-path analogue of the accepted batch loss recorded at
  `BatchSmtpStage::WindowOpened` - but unlike that one it is written down
  nowhere. Either document it at the arm or decide it is a defect.

- **`flatten_push_object`'s `#[allow(unreachable_patterns)] _ => {}` arm now
  covers nothing** when both `mail` and `calendars` are on, since every
  `PushObject` variant has a real arm. It is still needed for feature-off builds,
  so it stays, but a reader can misread it as "something is still being dropped".
  Worth a clarifying comment or a cfg-shaped alternative.

## Notes

- The rules for working an item are at the top of this file. One more that
  earned its keep: an existing test you must modify is a red flag; justify
  it explicitly.
- Item keys: F-items came from the phase 5B/5C/5D error-model re-audits,
  N-items from the original post-phase-4 audit, B-items from the 2026-08
  bug-hunt ledgers, C-items from the 2026-09-04 hunt. All are tracked at
  the same level; none blocks ratatoskr.
