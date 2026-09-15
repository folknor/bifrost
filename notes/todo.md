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

- **Three stream-poll loops have no shutdown arm.** `drive_changes_stream`,
  `InventoryFusion::run_stream` and `BackfillRunner::run_partition` poll a
  provider stream with no select on the cancellation token, so a stream that
  neither yields nor ends parks its worker until detach aborts it at the
  deadline. `reference/sync.md` documents that the DELAYS around the drive
  select on the token; the drive itself does not. This is the actual GENERATOR
  of straggler workers, and the per-phase detach budgets that landed 2026-09-07
  bound the damage without removing it: every detach of a wedged provider still
  costs a full `detach_timeout` in the worker phase. Adding a shutdown arm to
  the three would turn that into a prompt teardown. Two tests deliberately
  exploit the current behaviour to hold a teardown window open and say so, so
  closing it means restaging them. Wants a ruling; it is a behaviour change to
  every detach, not a local fix.

  COSTED 2026-09-15, and the answer is to split the ruling three ways rather
  than take this as one item. Two corrections to the framing above came out of
  it. First, there are THREE such tests, not two, and none of them stages
  against the drive or the runner - all three park an inventory stream through
  fusion, because the deferred-inventory worker is the one detach actually
  joins. Second, and this changes the item's premise: **the drive is not a
  straggler, it is an ORPHAN.** Per-scope poll tasks are spawned with the
  `JoinHandle` discarded, so they are not in `slot.workers` and `detach`
  neither awaits nor aborts them. A poll wedged in `drive_changes_stream`
  therefore costs detach ZERO time and SURVIVES the detach entirely, holding an
  `Arc<dyn Account>`, a scheduler admission and a `ChangeDelivery`, still able
  to publish and to register publications against a `PendingCoverage` the
  detach has walked away from. So the cost here is an unbounded leak, not a
  bounded `detach_timeout`.
  - **`drive_changes_stream`: yes, highest value.** It already receives a
    `BoundaryView`, and `BoundaryView::changed()` is an async signal on the
    same watch `detach` publishes `Stop` on, so the arm needs no signature
    change and no new outcome variant (`ChangesEvent::Stopped` already exists).
    No test restaging. Contain the arm to the `stream.next()` poll; the body
    has no await to cut.
  - **`BackfillRunner::run_partition`: yes, conditionally.** `LaneGate` already
    owns the account shutdown token, so a crate-internal accessor plus an arm
    returns `Err(Error::ShuttingDown)`, a shape this function already produces
    and the orchestrator already handles. No test restaging. TWO HARD
    CONSTRAINTS: the arm goes around the poll only and never across the
    `publish_checkpoint` / `publish_backfill` pair, since cancelling between
    them mints a publication no acknowledgement can retire; and a cancelled
    partition must NEVER return `complete: true` or `RequestNextPartition`,
    because the orchestrator would then write a completion sentinel that makes
    the next attach skip the scope entirely. That is silent data invisibility,
    not re-work.
  - **`InventoryFusion::run_stream`: no, or not yet.** It is the only one whose
    fix moves published surface (a token field on a struct consumers construct
    by literal), and the only one that forces restaging all three tests. One of
    them, `a_straggler_worker_does_not_cost_the_ack_writer_its_drain`, names
    this exact change in its own doc as the ablation that breaks it, and it
    measures the stalled stream's destruction instant from inside `Drop`
    precisely because elapsed time cannot discriminate. A restaging that falls
    back to elapsed time passes against the bug the test exists to pin. The
    straggler cost being bought out is one `detach_timeout` in the worker
    phase, already fenced off from the writer and close phases by the per-phase
    budgets. If it is wanted anyway, it gets its own ruling, and the restaged
    test must keep the `Drop`-instant probe.
  Cancellation-safety check came back clean for all three: `StreamExt::next`
  borrows the stream so a dropped poll cannot swallow an item, and the
  `_activity` guards are function-scope bindings rather than constructed inside
  an async block - the shape that bit this crate before is not present.
  Arming the drive does move a stated contract: `reference/sync.md`'s "Do not:"
  list names all three loops and says a strict publication cutoff "would have to
  be named as one". Arming the drive moves toward that cutoff without reaching
  it, so the honest edit narrows the clause rather than deleting it.

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

  This costs more than "wire it when documented" implies, found 2026-09-15.
  `crates/net/src/account_error.rs` already has a LIVE branch keyed on
  `ctx.protocol == Protocol::Jmap && ctx.provider == Some(Provider::Fastmail)`
  awarding `ThrottleScope::Account`, and `ledger_envelope.rs` already
  serializes `Provider::Fastmail`. Because jmap sets `provider: None`, that
  branch cannot fire: a Fastmail JMAP 429 currently gets NO throttle scope at
  all. So this is not only a deferred nicety, it is dead throttle-classification
  code in a SHARED crate, kept alive by a decision in another one. Either wire
  the provider or delete the unreachable branch; leaving both is the worst of
  the three. Note the deletion half touches published surface and is the
  owner's call.

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
  `bifrost-types::mime` ships the decoded MIME part tree
  (`ParsedMessage` / `MimePart`), which describes octets already held where
  BODYSTRUCTURE describes octets not yet fetched - complementary, so the
  traversal reuses its encoding vocabulary rather than its tree.

  STAGE ONE LANDED 2026-09-07, deliberately UNWIRED. `account/parts.rs` has the
  BODYSTRUCTURE traversal and the versioned part-handle codec, with 22 tests
  over real wire bytes through the crate's own parser. `BlobRangeSupport` is
  still `No` and both openers still return `Unsupported`: a capability that
  claims a byte path before the projections mint handles is a promise the crate
  cannot keep. `open_raw_rfc822` remains the supported byte path.

  STAGE TWO, what is left:
  1. Add `BODYSTRUCTURE` to the hydration attribute selection - the account
     layer requests it NOWHERE today - and thread the parsed structure into the
     projection. That selector is shared by the generic and PIM projections.
  2. Mint a handle per part into `BlobHandle`, and decide there whether
     `message/rfc822` parts appear as attachments or only as byte-path targets.
  3. Restore `open_blob` / `open_blob_range` over `blob.rs::run_fetch` with
     `FetchAttr::BodySection { peek: true, section, partial }` - `run_fetch`
     already takes both, so the opener is thin. Route its existing UIDVALIDITY
     recheck through `verify_uidvalidity` rather than keeping two.
  4. Only then flip `BlobRangeSupport`.

  TWO THINGS STAGE TWO MUST RULE ON, both surfaced by stage one:
  (a) A range is over ENCODED octets - `MessagePart::size` is what the server
  reported - so a byte range on a base64 part is NOT a range over decoded
  content. The capability has to say which it means before it can honestly
  claim ranges.
  (b) An unmodelled transfer encoding now survives as a token beside the
  classified `TransferEncoding`, because `BlobEncoding` has no unknown variant
  and stage one refused to invent one from inside the imap crate. Stage two has
  to decide the `bifrost-types` surface: an unknown variant carrying the token,
  or such parts made ineligible for handles. Losing the token was the P2 that
  forced this; do not re-lose it by mapping it to something modelled.

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

  - SUCCESS half, genuinely unreachable that way. The error model only
    speaks when something fails; there is no error object to hang
    "authenticated with SCRAM-SHA-256 plus tls-exporter channel binding"
    on. No amount of improving error plumbing produces a success record.
    Still waiting on ratatoskr to need the audit trail; it should not be
    designed as a mirror of the failure surface.
  - FAILURE half, TRACED 2026-09-15 across both public auth surfaces. The
    2026-07-31 re-scope guessed "largely redundant" and asked someone to
    verify what the SERVER-rejection paths carry. They carry less than the
    guess assumed, and the premise was wrong in a way that matters:
    `AuthPolicyFailure`, `AuthMechanismRejection` and the whole IMAP `Error`
    enum are `pub(crate)`, so nothing typed about mechanism rejection is
    public in `bifrost-imap` at all. A consumer sees a support-only string on
    the local-policy path too. The redundancy question therefore has to be
    settled at the `AccountError` level, not at the "a typed struct exists"
    level. What the trace established: credential rejection works and the two
    crates agree on it (`Authentication(ReauthorizationRequired)`); channel
    binding unavailable reaches the same kind on both but is not separable
    from "no permitted mechanism at all" except by string; a SASL exchange
    violation is Protocol-class on both but the two crates pick DIFFERENT
    kinds for the identical condition (imap `ContractViolation`, smtp
    `ParseFailed`), and `reference/error-model.md` gives no rule making one
    right. Three gaps follow, all fixable inside the error model, none
    needing a parallel typed surface. They are listed as their own items
    below rather than left inside this one, since two are defects.

- **sasl-F3 (gap, imap).** A `NO` or `BAD` answering `AUTHENTICATE` is
  classified with no knowledge that the command was an auth command, so a
  server rejecting the mechanism is indistinguishable from one rejecting the
  credential (`NO`), or lands on the client-bug-flavoured `Request(Malformed)`
  (`BAD`). SMTP has a command phase for exactly this and imap has no analogue.
  `require_ok_auth` in `crates/imap/src/connection/dispatch/auth.rs` is
  already the single choke point, so the context has one place to go.
  Note that F4's mechanism threading landed through that same choke point on
  2026-09-15, so the plumbing a phase would ride is already there.

- **imap: `is_rev2` is restated in two places.** Found 2026-09-15.
  `driver/mod.rs::is_rev2(&ProtocolState)` and
  `connection/auth.rs::is_rev2_from_snapshot(&ConnectionStateSnapshot)` both
  implement the RFC 9051 dual-mode rule, over different inputs, with no
  cross-reference between them. The snapshot is DERIVED from the state, so this
  is a genuine live restatement rather than two views of one rule, and it is the
  real drift risk in the neighbourhood the now-deleted stale "Mirrors" comments
  were vaguely gesturing at. Unifying is a small refactor, not a comment fix.

- **imap: `UnavailableOnLiveSnapshot` is unreachable by construction.** The
  rung-fall-through that records it landed 2026-09-15 and is correct and
  defensive, but it cannot be pinned hermetically today. Reaching it needs the
  LIVE snapshot to lack a capability the ladder's profile snapshot had, and
  (a) the profile and the per-site gates are now provably governed by the same
  comparison, and (b) the snapshot only moves when a command completes, while a
  rung that completes either succeeds and returns or fails non-`MissingCapability`
  and aborts the ladder. The only real path is a second handle on the same driver
  refetching CAPABILITY concurrently, which is nondeterministic to script. Keep
  the code; do not delete it for want of a test, and do not write a test that
  fakes the shape - an earlier attempt at exactly that asserted a premise the
  code contradicts and had never been run.

- **sasl-F5 residual: the stale `offered` snapshot.** The vanishing rung was
  fixed 2026-09-15 (the `MissingCapability` fallthrough now records an
  `UnavailableOnLiveSnapshot` rejection before advancing). What survives is
  that `AuthPolicyFailure::Display` still renders `offered` from a snapshot
  taken BEFORE the ladder ran, so under skew the reported offer list can name a
  mechanism the live snapshot no longer advertises. Documented on the type:
  `rejected` is the authoritative per-rung record, and re-reading the profile at
  failure time would move the skew window rather than close it.

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
  (d) CLOSED 2026-09-07. The blocker really was the clock, not the test:
  `ThrottleBucket` deadlines were `SystemTime` while the poll loop slept
  them off on tokio time, so under `start_paused` the sleep returned
  without wall-clock moving and the re-check re-derived the full wait
  forever. The bucket is engine-owned in-memory state - no serde, never
  in a `Checkpoint` or the store - so a monotonic deadline is safe, and
  `tokio::time::Instant` matches what `AsyncDeadline` and `ByteBucket` in
  bifrost-smtp had to do for the same reason. `tests/throttle_defers_
  changes.rs` now pins both halves the item asked for. Note this changed
  five PUBLIC `ThrottleBucket` signatures from `SystemTime` to
  `tokio::time::Instant`; nothing was removed or renamed.

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

- **types-B1b. `send_as` gates a request FIELD, not a method, and the crates
  disagree.** imap and google reject a `send_as` request with
  `Unsupported(Send)`; jmap's `route_send_as` answers `Request(Malformed)` for
  an unknown foreign id and `Unsupported(Send)` only for a known-but-not-
  submission-capable one. Not asserted anywhere and not expressible in the
  types-B1 test, which drives methods. Decide whether the uniform answer is
  worth it, or document the split. Related: c3-2.

  AUDITED 2026-09-15, and **the premise above is wrong**: this is not jmap
  versus everyone. Graph draws exactly the same line and says so in its own
  rustdoc at `send_as_unknown_mailbox` ("The provider supports send-as; this
  specific mailbox is just not configured, so it is a caller error"). The split
  is two coherent groups separated precisely by the capability flag:
  `pim_methods.send_as == false` means the FEATURE is absent, so
  `Unsupported(Send)`; `== true` means the feature is present, so an unheld
  mailbox id is a bad ARGUMENT, hence `Request(Malformed)` with
  `RequestCause::InvalidArgument { field: Some("send_as.mailbox"), .. }`.
  imap and google genuinely cannot draw that line - with no routing table there
  is no known id to fail against - so `Unsupported` is the only honest answer
  available to them. RULING WANTED: document the split, do not unify it.
  Unifying has to break one of the two groups, and forcing graph/jmap to
  `Unsupported` would tell a consumer "this account cannot send-as" about an
  account that demonstrably can, while discarding the field pointer a UI needs.
  Both kinds classify terminal (`ClientBug` and `Unsupported`), so no automatic
  machinery branches on the difference; the distinction is for the consumer's
  remediation, correct-the-request versus reconfigure-the-feature.
  LANDED 2026-09-15: the governing statement is now the rustdoc on
  `SendRequest::send_as`, abbreviated on `PimMethodSupport::send_as`, with
  one-line branch pointers in `reference/imap.md`, `reference/google.md` and a
  corrected paragraph in `reference/graph.md`. Graph's unconditional `send_as:
  true` was fixed in the same pass (derived from the shared-client map, with a
  refresh at the one post-construction replacement site). One correction banked
  from that work: the audit claimed Graph already carried
  `RequestCause::InvalidArgument { field: Some("send_as.mailbox") }`. It did
  NOT - only JMAP did; Graph built a bare `Malformed { detail }`. Graph was
  brought up to the documented contract rather than the doc hedged down to
  Graph. What remains open here is only types-B1d below.

- **types-B1d. The published `send_as` contract contradicts two of six crates.**
  [found 2026-09-15] `crates/types/src/compose.rs` states the rejection is
  `Unsupported(Send)` on both `SendAs` and the field itself, and never mentions
  `Request(Malformed)` - which is what graph and jmap actually answer. Meanwhile
  `send_as_guard` is duplicated in structure between imap and google, each with
  its own local `send_as_rejected_unsupported` test exercising the copy. Textbook
  standing-lesson shape: the copies agree indefinitely while only the original
  disagrees, and nothing asks the original. `capability_contract_tests.rs` cannot
  reach it - its own comment concedes it drives methods, not request fields, and
  it always passes `send_as: None`. The structural close, additive: a
  `refuses_field!` arm beside `refuses!` that drives a `Some(send_as)` request
  and asserts the kind IMPLIED BY THAT CRATE'S OWN `pim_methods.send_as` flag.
  That tests the rule instead of the copies, is expressible in all six crates
  today, and would have caught types-B1c automatically.

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

- **Three owner rulings, none blocking.**
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
  (c) LANDED 2026-09-15, and it needs one clean verifying run. The gap was real:
  nothing compiled this workspace with features OFF, so the two tests under
  `#[cfg(not(feature = "calendars"))]` were never typechecked, and
  `flatten_push_object`'s `#[allow(unreachable_patterns)] _ => {}` arm existed
  for a configuration that was never built. `brokkr.toml` now declares three
  `[[check]]` sweeps, the featureless leg scoped to `bifrost-jmap` under
  `no_default_features` with `feature_unification = "selected"` so a sibling
  cannot donate `jmap/sync` back and make it a fourth copy of the default sweep.
  Doctests were turned on in the same pass; they default OFF, and 24 files carry
  `///` examples on a published API that had never been compiled.

  TWO THINGS TO RULE ON, both raised by the agent that wired it. Declaring ANY
  `[[check]]` entry REPLACES brokkr's implicit `--all-features` sweep, so those
  three entries have to reconstruct that coverage - the claim that they do is
  reasoning about the feature graph, not a measurement. And none of it is
  verified by a build: whether `jmap-featureless` compiles at all is unknown,
  and if it does not, that is the defect the sweep exists to surface rather than
  a config error.

  KEEP THE PHANTOM-KEY STORY, because it explains a class of error rather than
  one mistake. This item used to propose `[check] consumer_features` as "one
  line closes both". Brokkr rejects it: sweeps are the `[[check]]` ARRAY and the
  legacy table form is refused at parse. `AGENTS.md` asserted the key existed,
  and it got that from brokkr's OWN long help text, which still repeats it while
  the man page and the parser both disagree. So a stale upstream help string
  propagated into a binding project document, then into a filed ruling, then
  into a recommendation to the repository owner, with nothing in the loop
  checking it against the tool. Report the stale help text upstream to brokkr.
  The reusable lesson: a config-load failure is raised before any phase runs, so
  brokkr validates a config far more cheaply than a build does. Validate a key
  that way before writing it into any document.
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

  RE-VERIFIED 2026-09-15, holds, and the ruling is now easier because one
  option turns out not to exist. **The narrow fix is not safe standalone.**
  Ignoring a foreign tag leaves `run_pipeline_batch`'s step-4 tag-matching
  loop waiting forever for a tag the server has already answered, so the
  narrow fix converts a wrong error into a HANG unless it is combined with
  parking. Parking is therefore the only complete remedy. Its shape: build
  `tag_to_idx`, `consumers`, `targets`, `tags` and `results` BEFORE the send
  phase rather than after it, and give the send phase a sink (or a shared
  parked-response buffer that step 4 drains first). That restructures steps 2
  to 4 and leaves single-command dispatch and IDLE untouched.
  Also: the trigger condition above is too narrow. `LiteralMinus` is exposed
  too, because `patch_small_literals_to_plus_with_binary` only patches
  literals up to 4096 bytes and larger ones stay synchronizing, so a LITERAL-
  server still hits this on any pipelined APPEND-sized literal. Only the
  `LiteralPlus` arm is immune, since it is a single batched write with no
  waits. Independently, an untagged BYE during a pipelined literal send
  aborts the batch with the same total result loss, whatever the tag question
  is decided.

  DESIGNED 2026-09-15, ready to rule on. Two findings make the item bigger and
  more urgent than the text above.
  - **There is a second, INVISIBLE half of this defect.** The untagged arm of
    `wait_for_continuation` calls `process_untagged_as_event`, the shared
    consumerless arm, whose own doc says it is for loops with no consumer to
    route to. But the pipeline send phase HAS consumers, so a `* MYRIGHTS`,
    `* LIST` or `* ESEARCH` legitimately solicited by an earlier pipelined
    command is downgraded to an anonymous typed event and its owner never sees
    it. The batch then still COMPLETES, so the caller gets a silently short or
    empty result rather than an error. That is worse than the tagged case,
    which at least fails loudly, and the current inline comment does not name
    it. Parking must cover the untagged arm or the fix is half a fix.
  - **A naive parking implementation is a state-corruption regression.**
    `wait_for_continuation` calls `state.apply_side_effects` before it matches,
    and so does step 4. Park a bare `Response` and let step 4 treat it as
    freshly read, and side effects apply TWICE: a parked `* 3 EXPUNGE`
    decrements twice. So the parked record must be four fields -
    `{ resp, digest, notify_before, code_emitted }` - and the drain path must
    re-apply nothing. `notify_before` is load-bearing because step 4 reads
    `state.notify()` BEFORE applying effects and feeds it to classification;
    `code_emitted` is load-bearing because a parked untagged response has
    already had its code events emitted in the send phase. There is no smaller
    correct record. THE RULING SHOULD APPROVE THE FOUR-FIELD RECORD EXPLICITLY,
    not "the parking fix" in the abstract, since an implementer following the
    one-line sketch lands the regression.
  Shape: `wait_for_continuation` gains one `park: Option<&mut Vec<Parked>>`
  parameter; the three single-command call sites pass `None` and are unchanged.
  Do NOT route inside the send phase - that contagions five arguments into a
  function IDLE also calls. Step 4 drains the buffer before reading the socket.
  Buffer stays frame-local in `run_pipeline_batch`; hoisting it into
  `ProtocolState` would outlive the tag namespace that gives it meaning.
  Blast radius: IDLE verified genuinely unaffected (it sends one command and
  its own loop already discriminates `t.tag == tag`). Nothing published moves;
  everything involved is `pub(super)` or private. Size 60-80 lines net plus
  roughly 250 of tests.
  The reproducer is the fiddly part and is worked out: it needs `Synchronizing`
  mode, two commands of DISTINCT `CommandKind` (same kind gets split into
  sub-batches by `group_into_sub_batches` and the bug does not fire), and the
  SECOND command must carry a synchronizing literal. Most pipelinable commands
  cannot produce one, because mailbox names go through mUTF-7 and come out
  ASCII-quotable. The one that can is `LISTRIGHTS` with a non-ASCII identifier,
  which is passed raw. Greet with `* PREAUTH [CAPABILITY IMAP4rev1 ACL]` to
  force `Synchronizing`. Assert the POSITIVE post-fix shape including that the
  literal body bytes reached the wire - a narrow-fix implementation hangs there
  rather than passing, which is worth pinning too. The untagged test must
  assert on the payload, not `is_ok()`, because today it passes `is_ok()`.
  BYE stays unfixed deliberately: `short_circuit_on_bye` must stay ahead of the
  park, since a BYE means the connection is closing and delivering partial
  results while tearing down is a more confusing contract than failing. Record
  it as considered-and-declined rather than letting it look handled; partial-on
  -BYE needs `run_pipeline_batch` to return `(PipelineResults, Option<Error>)`,
  which ripples into `run_pipeline`'s sub-batch loop and the driver's fatal
  check.

- **imap: `emit_untagged_response_code_events` and `has_critical_response_code`
  are complements maintained by hand.** Both handle exactly `Alert` and
  `NotificationOverflow`, with no compiler link between them, so a third
  critical code added to one and not the other silently reproduces the
  dead-guard/double-emit bug the `Bye` exclusion already demonstrates. A
  shared helper returning `Option<TypedEvent>`, with the predicate defined as
  `.is_some()`, would make them structurally complementary. Refactor
  proposal, not a defect, so it wants a ruling rather than a fix.

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

- **The blocking SMTP transport did not compile without the `tokio` feature.**
  FIXED 2026-09-15. `error::timeout` carried a `#[cfg(feature = "tokio")]` since
  the per-reply-deadline commit, while the BLOCKING reader called it for the
  spent-deadline case (a zero `SO_RCVTIMEO` means block forever, so an expired
  deadline has to be an error rather than a zero re-arm). Nothing caught it
  because nothing had ever built `bifrost-smtp` without `tokio`; the in-workspace
  consumer is async, so unification always donated the feature. Worth keeping as
  the reference example of why "nothing calls it in this workspace" says nothing
  about a library crate: the blocking half is published API, it is deliberately
  independent of tokio, and it was broken in exactly the configuration no
  in-workspace consumer exercises.

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

- **CardDAV's `contact_search` saturates an unrepresentable `limit` to
  `usize::MAX`**, so an absurd explicit limit becomes unbounded on the crate
  whose DEFAULT is bounded. Unreachable on 64-bit, and left alone during
  dav-F7 because naming it `UNBOUNDED_PAGE_SIZE` would imply a constant that
  crate does not otherwise have. It is now the only bare `usize::MAX` page
  size across the twins.

- **`reference/smtp.md` states the write-timeout/cap interaction in three
  places** ("Transport types", "Per-reply read deadline", "Bandwidth
  metering") across 1062 lines. Not a defect, and the cross-references are
  deliberate, but it is the restatement shape the standing lessons name: any
  behaviour change there needs three edits and nothing enforces that.

- **`notes/todo.md` is running behind the code in bifrost-smtp.** Working the
  smtp items on 2026-09-15 found smtp-CR7(b) already FIXED (with a test
  pinning it), and smtp-CR7(a), CR12 and CR13 already absorbed into
  `reference/smtp.md`, in CR7(a)'s case in stronger and more accurate terms
  than the bullet carried. Check the remaining smtp bullets against the
  reference before working them.

## Notes

- The rules for working an item are at the top of this file. One more that
  earned its keep: an existing test you must modify is a red flag; justify
  it explicitly.
- Item keys: F-items came from the phase 5B/5C/5D error-model re-audits,
  N-items from the original post-phase-4 audit, B-items from the 2026-08
  bug-hunt ledgers, C-items from the 2026-09-04 hunt. All are tracked at
  the same level; none blocks ratatoskr.
