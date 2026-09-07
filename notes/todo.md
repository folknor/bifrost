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

## Sync residuals

The three items ruled on 2026-09-06 (the receipt bound, the explicit
observer subscription, the teardown window) have all landed. Two earlier
structural landings of the same week, the dav-core `ResponseParts` collapse
and the smtp sans-I/O core, never received the cold review their rulings
asked for; that review debt is listed under their crates below.

- **Filed from the teardown-window cold review (2026-09-07), not fixed in
  it.** (a) Lateral P2: `detach_inner` awaits `Account::close()` with no
  deadline, so a hanging close hangs `detach` despite every other step being
  clamped to `detach_timeout`; predates the change. Clamp it like the worker
  awaits, and decide what a timed-out close means for the handle. (b) P3:
  `a_departure_during_teardown_records_no_loss` calls `begin_teardown`
  directly, so deleting the production call from `detach_inner`, or moving it
  after worker shutdown, changes nothing the test observes. Pinning the
  lifecycle ordering wants a test through `SyncEngine::detach` with teardown
  held at a controlled await; the engine harness in
  `tests/backfill_lane_flow_control.rs` is the closest starting point.
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

- **dav-F3 (P3, filed 2026-09-06, cold review).** `multistatus.rs`
  `member_status_code` reports `failed_statuses.first()`, so a member
  answering `<propstat 404: getcontenttype>` then `<propstat 403: getetag>`
  reads as 404. 404 is `is_missing_resource`, so `classify_207` calls the
  lane `Usable` with no entries, and `establish_initial_cursor` /
  `inventory_stream` mint an EMPTY snapshot for a collection the server
  actually refused - a snapshot diff that destroys every resource in it.
  `failed_member` inherits the same code. Fix shape: with no success
  propstat, report the WORST failed code, preferring any non-404/410 over a
  404/410, rather than whichever came first in document order.
- **dav-F4 (P3, filed 2026-09-06, cold review).** A well-known probe that
  answers `200 text/html` - a front end that returns the index page for any
  method - fails `extract_href_property` with an XML parse error, which
  mints `Protocol(ParseFailed)`. `should_fallback_discovery` admits only
  `NotFound(Calendar)`, `Request(Malformed)` and `Server(Error{405})`, so
  the open FAILS instead of falling back to the configured base URL, which
  is what the same deployment answering an empty 207 gets. Either admit
  `Protocol(ParseFailed)` for the well-known leg specifically (not for the
  base leg, where a garbage document is a real contract violation), or
  gate the probe on a `text/xml`-ish content type.
- **dav-F5 (P3, filed 2026-09-06, cold review).** `post_schedule_reply`
  (caldav `client.rs`) drops the RFC 6638 `Originator` and `Recipient`
  headers silently via `if let Ok(value) = HeaderValue::from_str(..)`. A
  non-ASCII calendar-user address therefore goes out with no routing
  headers at all and the server's 400 surfaces as `ProviderRefused`. The
  seam to refuse locally already exists: routing the value through
  `DavRequest::header` records the rejection and `dispatch_once` refuses
  with `Request(Malformed)` before any I/O, which is what every other
  header on that request already does. Two lines.
- **dav-F6 (P3, filed 2026-09-06, cold review).** An empty-element
  `<D:status/>` inside a propstat arrives as `Event::Empty`, which routes to
  `sink.element` and never sets `staged_status` / `staged_success`, so
  `commit_propstat` sees `None` and commits under the absent-status-is-
  success rule. The stated rule in the `multistatus.rs` module doc is the
  opposite: a status that is PRESENT and unparseable is a failure. Same
  hole in `extract_href_properties` and `parse_collection_property`, which
  keep their own `staged_success` and only write it from `Event::End`.
- **dav-F7 (P4, filed 2026-09-06, cold review).** Page-size default drift:
  CalDAV `event_search` and `events_in_range` use `usize::MAX` when `limit`
  is `None` (`account.rs`, two sites), CardDAV `contact_search` defaults to
  `CONTACT_PAGE_SIZE` (250). Neither reference states the CalDAV default, so
  a consumer omitting `limit` gets an unbounded page from one crate and a
  250-entry page from its twin with nothing documenting either.
- **dav-F11 (P4, filed 2026-09-06, cold review).** `DavDispatch::resolve_url`
  joins a relative id two different ways: `Url::join` on the parsed base
  (which REPLACES the base's last path segment) and, when the base does not
  parse, a plain `format!` concatenation (which APPENDS). Only reachable for
  a consumer-supplied relative native id against an unparseable base, so it
  is latent - but the two branches should agree.
- **dav-F12 (P4, filed 2026-09-06, cold review).** `move_resource` keeps the
  original `Destination` header across a redirect OF THE SOURCE: the walk
  clones `request.headers` and rewrites only the url, so a server that
  moves `/cal/one.ics` to `/dav/cal/one.ics` gets a MOVE whose destination
  still names the pre-redirect collection namespace. Rebasing the
  destination on the same hop the source took is the fix, and it must stay
  inside the admitted-origin gate.
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

- **smtp-CR2 (P3).** `BatchLmtpStage::GroupOpened` has the wrong exit shape on
  `OpOutcome::Failed`: it returns `Step::Finish(Err((error, progress)))`, and
  it is reached only AFTER `BatchLmtpStage::BodyWritten` ran
  `set_body_finished()`. A batch-level `Err` means "nothing was transmitted"
  (`reference/error-model.md`), so this arm would claim that for a body that
  already left. Unreachable today - `open_reply_group` fails only on
  `verify()`, and a completed body write leaves the stream `Ok` - so it is a
  latent shape defect, not a live one. `BatchLmtpStage::FinalStatus`'s `Failed`
  arm has the right shape (`mark_uncertain_unresolved` + `Ok(progress)`).
  `BatchSmtp::WindowOpened` returns the same `Err` shape but sits on the clean
  side of DATA, where it is correct.

- **smtp-CR3 (P3).** The async `abort()` is bounded by
  `per_operation_budget()`, which is `TimeoutBudget::PerOperation(self.timeout)`
  - so a transport built with `timeout(None)` awaits `poll_shutdown`
  unbounded, and on a TLS peer that never answers `close_notify` the await
  never returns. `Pool::shutdown` runs closes concurrently but waits for them,
  so a single wedged TLS peer can hang shutdown. `reference/smtp.md` states
  "each close is bounded by the connection's operation timeout" flatly; it
  holds only when one is configured. Either bound the shutdown with a floor
  independent of the configured timeout, or say so in the reference.

- **smtp-CR4 (P4).** `DirectLmtpStage::BodyWritten` hardcodes
  `SmtpCommandPhase::DataBody` where `DirectSmtp::BodyWritten` uses
  `self.body.body_phase()`, so an LMTP BDAT chunk-write failure reports
  `DataBody` instead of `BdatBody`. One-line fix; the phase feeds
  `classify_response`, which does not currently split on it, so nothing
  observable changes today.

- **smtp-CR5 (P4).** `DirectSmtpStage::MailRejectedDrain` on a `Failed` drain
  read reports `phased(SmtpCommandPhase::RcptTo, error)`, dropping the
  `MAIL FROM` rejection it is carrying in `response` along with its reply text.
  The caller gets the transport failure of the drain and never learns why the
  transaction was doomed.

- **smtp-CR6 (P4).** `BatchSmtpStage::WindowOpened` and
  `BatchSmtpStage::WindowClosing` exit `Failed` as
  `Step::Finish(Err((error, progress)))`, and both transports discard the
  progress (`Err((e, _progress)) => Err(batch_level_error(e, ctx))`). So the
  RCPT answers already collected in earlier windows - including `550`s the
  server gave - fold into one `Unsent` batch-level retry. Contract-correct (no
  content was transmitted, the whole request is retryable) but lossy for
  diagnostics: a caller retrying learns nothing about the recipients the server
  had already refused.

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

- **smtp-CR9 (P4).** `SlowSinkPeer::new` arms its first `Sleep` at
  CONSTRUCTION rather than at the first `poll_write`, so the gap between
  building the peer and the first write is silently credited against the
  first chunk's delay. Harmless in the tests that use it (they build and
  write immediately, under paused time, where no virtual time elapses in
  between), but it makes the peer's contract "chunk bytes every gap, counted
  from whenever you happened to construct me", which is a trap for a future
  test that builds the peer during setup. Arm lazily on the first poll
  instead.

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

- **jmap-B1.** `imap` has four copies of the untagged-response dispatch loop.
  Recorded for completeness with the other duplication findings; same standing
  as the above. (The jmap half is landed: `sync/changes.rs`'s `email_changes`
  and `mailbox_changes` are now one generic `changes_walk` over the new
  `core::changes::ChangesMethod`. The inventory walk stayed separate - it is
  anchored query-then-get with its own no-`Done` exits, not a state walk - and
  the reason is written at `changes_walk`.)

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
