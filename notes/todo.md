# TODO

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
- **jmap-O2.** DONE (2026-07-31). The three named casualties are now
  pinned via the free-function extraction pattern `route_object_id`
  already used, rather than by threading the transport generic through the
  sync layer: `cursor_scopes_from_seeds`, `partitioning_for_scope`, and
  `establishment_for_seed`, each pure given its inputs, each with its
  trait method reduced to a one-line delegation. The extraction was
  behavior-preserving (the crate's 482 tests passed unchanged before any
  new test was added); the 7 tests added on top cover discovery order and
  its `HashMap` iteration-order independence, unseeded `Type` scopes not
  being discovered, `Email` as the only partitionable scope, the oversized
  `maxObjectsInGet` degrading to unbounded rather than truncated, and the
  seeded/unseeded establishment split including the cursor scope on the
  error. Verified sensitive by removing the foreign sort and confirming
  two tests fail.

  `JmapAccount` still hardwires `ReqwestTransport`, so the remaining
  surface (`capabilities()`, `describe_cursor`, the mutation doors as
  whole calls) is still undrivable hermetically. That is now a bounded,
  known residual rather than a blocker: the pattern for anything pure is
  established, and anything genuinely needing a scripted transport belongs
  with the xc-3 discussion.
- **jmap-S1-residual.** (nit; the confusable version-const pair it was
  filed for is fixed - the two are now `PAYLOAD_ENVELOPE_VERSION` and
  `OUTER_CURSOR_ENVELOPE_VERSION`, each documenting its axis)
  `JmapScopeRepr::from_cursor_scope` happily encodes `Type(Thread)` and
  `Query(_)` cursors that `changes::stream` then terminates
  `Unsupported` - a legal-but-dead codec path.

## bifrost-imap

- **imap-F2.** `pim_malformed` and `envelope::malformed` reach helper
  paths that don't know the op; both currently produce
  `Request(Malformed) -> ClientBug` so recovery class is op-independent
  but operation telemetry is degraded. Thread the operation when other
  pim/envelope refactoring happens.
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
- **imap-T3.** DONE (2026-07-31). The audit of `account/error.rs` against
  `reference/error-model.md` found and fixed two defects, both from the
  `with_folder_scope` migration leaving scope READERS behind:
  `id_from_scope` and `mailbox_throttle` matched only
  `ErrorScope::Mailbox { id }`, but every production folder producer builds
  `ErrorScope::Cursor(Folder(_))` - `with_mailbox` is called by tests only.
  So `ThrottleScope::Mailbox` was unreachable in production (every
  per-mailbox `[LIMIT]` widened to an account-wide pause) and the folder id
  was dropped from `RequestCause::NotFound` despite sitting in the scope.
  Both readers now accept either shape, pinned by folder-scoped tests
  alongside the pre-existing mailbox-scoped ones, which were passing
  precisely because they used the dead helper.

  Also fixed: `Translation::skip_attempt_cause` was documented as
  suppressing the `Transport(_)` + `Acknowledged` pair `try_build` rejects,
  but was never set to `true` - so the guarantee was a comment, and the
  pair would have panicked at the boundary's `.expect`. It is now a
  demotion to `InFlight` (dropping the cause would let `derive` read its
  `Unsent` default and blind-retry a non-idempotent op). Nothing builds the
  pair today, but `Error::with_attempt` accepts any state on the transport
  variants and the driver does apply `Acknowledged` after a tagged
  response.

  Left as dead-but-harmless: `ImapErrorContext::with_transmission_state` is
  never called (so `ctx.transmission_state` is always `None`), and
  `with_mailbox` is production-dead. Both are reasonable API surface; the
  scope readers now handle what they produce, so neither is a trap.

  Followed up by sweep-1 (2026-07-31): `#![warn(dead_code)]` on
  `account/error.rs` now enforces this residual rather than recording it,
  and turned up three more unreachable items the audit had missed
  (`with_scope`, `with_idempotency_override`, `strategy_failure`). All are
  annotated in place with the reason they are dead.

  The one finding NOT fixed is filed as imap-S1 below.

- **imap-S1.** DONE (2026-07-31). ManageSieve response codes (RFC 5804 1.3)
  are now parsed and mapped. `SieveResponseCode` + `split_response_code`
  lift the parenthesized code off the status line ahead of the message;
  `Error::Sieve { code, message }` carries it to the boundary and
  `classify_sieve` maps it. `TRYLATER` is retryable instead of terminal
  (the headline defect), `QUOTA[/*]` is `QuotaExhausted` + account
  throttle, `NONEXISTENT` is `NotFound(Filter)`, `ALREADYEXISTS` is
  `ConcurrencyConflict`, and the auth-refusal codes split policy-block
  from reauthorization. An absent, advisory, or unmodelled code keeps the
  old terminal default. `check_script` no longer reports a transient
  refusal as a validation verdict. Required one shared-crate addition,
  `ResourceKind::Filter` (`notfound.filter`), justified by the operation
  enum already treating filters as first class.
- **imap-T5.** (deferred, connection sweep rulings - revisit triggers,
  not work items) Hermetic STARTTLS needs a fake TLS handshake
  (`ImapStream::into_tcp` returns `None` for `Memory`, deliberately);
  ruled heavier machinery than the risk it retires. The B7 compile-level
  guarantee (a result oneshot that cannot answer without publishing
  state) was ruled not worth it while all completion arms live in one
  match in `driver_task`; revisit if a new state-changing driver command
  lane is added. Strongest proptest candidate if coverage is wanted:
  `buffer_may_contain_complete_response` against generated well-formed
  response streams, asserting the generator never lands on the fatal
  lane.

## bifrost-smtp

- **smtp-M1.** DONE (2026-07-31). Ruled: wire it, rather than document the
  limit. The deciding fact was not that SMTP lacked metering but that the
  cap was silently PARTIAL WITHIN ONE ACCOUNT - `ImapAccount` implements
  the required `set_bandwidth_cap` and honours it on fetch traffic, while
  its `submission.rs` built an `AsyncSmtpTransport` with no meter and no
  cap. So a consumer setting a cap had it enforced downstream and ignored
  upstream, on the one path most likely to saturate an uplink, with no way
  to learn that from the trait signature.

  Both socket funnels are metered (`AsyncNetworkStream` poll_read/write,
  `NetworkStream` blocking Read/Write) via a debt-returning bucket in
  `client/metering.rs`; `bandwidth_metering(sink, cap)` on every transport
  builder is the opt-in, and `open_submission` passes the IMAP account's
  own meter handle and cap atomic so one `set_bandwidth_cap` governs both
  halves. Design notes in `reference/smtp.md` under "Bandwidth metering".
  Verified end to end by driving a real send through a scripted peer and
  asserting the sink saw both directions; sensitivity checked by making
  the charge a no-op and confirming the test fails.
- **smtp-N2.** (minor, re-scoped after measuring - the filed symptom does
  not reproduce) A 600-char non-ASCII display name folds correctly at 73
  columns on every address path, typed and raw: RFC 2047 words are
  individually short and the writer folds between them, so the name's
  length never reaches the emitted line. What DOES emit an over-long line
  is an unbreakable ALLOWED token (all-ASCII, no spaces, ~600 chars) next
  to encoded words: `HeaderValueEncoder::format` splits on spaces and
  writes an allowed word verbatim, since folding inside an atom would
  change the value and RFC 2047-encoding an all-ASCII word is not this
  encoder's rule - the same deliberate behavior
  `format_ascii_with_folding_giant_word` pins for Subject. It misses the
  RFC 5322 SHOULD-78 and stays well inside the MUST-998. Both shapes are
  now pinned by `long_non_ascii_display_names_fold_on_every_address_path`.
  Open only as a decision: encode over-long allowed tokens when the header
  already carries encoded words, at the cost of changing that pinned test.

  RFC standing checked 2026-07-31, because the item cites the wrong
  document for the question it asks. Nothing here is superseded, and
  nothing is new:

  - RFC 2047 (1996, encoded-words) is current, but applies only to
    NON-ASCII text. The case at issue is an all-ASCII token, which 2047
    does not cover at all - so "2047-encoding an all-ASCII word" would be
    using the mechanism outside its remit to force a fold, not applying
    it.
  - The binding constraint is RFC 5322 (2008): line length SHOULD be <= 78
    and MUST be <= 998. Current. The existing behaviour misses the SHOULD
    and stays well inside the MUST, which is what makes this a preference
    rather than a conformance bug.
  - RFC 6532 (UTF-8 headers) over RFC 6531 (SMTPUTF8) is the modern escape
    from encoded-words, but it removes the need to ENCODE non-ASCII; it
    does nothing for folding an unbreakable ASCII atom. No RFC supersedes
    the folding problem.

  So the decision stands as filed, but it is a 5322 SHOULD-compliance
  judgement, and the argument for leaving it alone is stronger than the
  original wording suggests.
- **smtp-T1.** (coverage) Residual test-seam gaps. The batch entry
  points through the pool (sync and async), pool retirement / reuse end
  to end, RCPT-option sequencing on the wire, mailbox list parsing, a
  peer that closes mid-response, and `starttls` up to the handshake
  boundary are now pinned. What is left is deliberately out of scope or
  needs a production seam: a real TLS handshake and the socket-dialing
  paths in `client/net.rs` / `client/async_net.rs` cannot be exercised
  without a listener; a peer that pushes an unsolicited reply while no
  reply is owed is only observable through the coalesced-segment path,
  because SMTP has no surplus-reply detection outside the LMTP
  final-status drain (see the desync test in `connection.rs`); and the
  socket-listener tests still living in `transport.rs`,
  `async_transport.rs`, and `test_support.rs` (plaintext-auth refusal,
  the LMTP delivery servers) predate the `Transcript` harness and could
  be migrated onto it.

## bifrost-caldav / bifrost-carddav

- **dav-F5.** Shared DAV propstat/status parser. `is_success_status` and
  the propstat-success gating are robust in place but duplicated across
  both DAV crates; a shared parser module would remove the drift risk.
  Related accepted cost: the `DavTransport` / `DavResponse` test seam is
  duplicated in `caldav` and `carddav` rather than shared via
  `bifrost-net`, because net keeps its dispatcher crate-private and both
  DAV clients still own Basic auth and their own redirect policy - a
  shared seam would have to grow those first. Revisit when these clients
  move onto `AccountNet`.

  Re-scoped 2026-07-31: this is TWO items fused, and only one of them is
  blocked. The `AccountNet` gating is about the TRANSPORT seam
  (`DavTransport` / `DavResponse`). It does not gate the PARSER, which
  could be lifted today with no transport work at all.

  And the parser half is a live instance of the sweep-1 pattern, not a
  tidiness preference. `carddav/src/parse.rs:502` carries the comment
  "Matches CalDAV's `status_code` + `200..=299` check" - an explicit
  annotation that it is a copy - and the two `status_code` functions are
  byte-identical (`split_whitespace().find_map(parse::<u16>())`). A
  comment is the only thing keeping them in step. That is the same shape
  as the four defects the 2026-07 slices found (xc-3, xc-3a, imap-T3,
  imap-S1), three of which were live.

  Suggested split: parser unification is unblocked and should be judged on
  its own; the duplicated transport seam keeps the `AccountNet` trigger.

  PARSER HALF DONE (2026-07-31). `bifrost_net::status_line` now owns
  `status_line_code` / `status_line_is_success`; both DAV crates call it
  and their local copies are gone, including caldav's two different
  in-line spellings of the 2xx test (`matches!(code, 200..=299)` three
  times, `(200..=299).contains(&code)` once) and carddav's named wrapper.
  Three spellings became one.

  Consolidating immediately surfaced a real divergence the comment had
  denied: for a `<D:status>` that is PRESENT but unparseable, carddav
  failed closed while caldav mapped it to `None`, which
  `propstat_success.unwrap_or(true)` read as SUCCESS - so caldav would
  commit a property whose status it could not read. Both now fail closed,
  pinned by `a_present_but_unparseable_propstat_status_does_not_commit`.
  An ABSENT status is still success, which is the RFC 4918 s14.22 reading
  and unchanged.

  TRANSPORT HALF still open: the duplicated `DavTransport` / `DavResponse`
  seam keeps the `AccountNet` trigger described above.
- **caldav-J1.** (residual of the chrono -> jiff migration) chrono and
  chrono-tz are still in the dependency tree, reached only through
  `caldata` 0.16, which depends on both. No bifrost code names either
  crate any more, so the migration bought one time library in our own
  source, not a smaller tree. The one place the boundary is crossed is
  `event_end_from_duration` in `caldav/src/ical.rs`, which takes
  `caldata::types::parse_duration`'s chrono duration and converts via
  `.num_seconds()` rather than naming the type - deliberate, so caldav
  needs no chrono dependency, but it is a shim that would go quiet if
  caldata ever changed that return type to something else with a
  `num_seconds`. Close this if caldata drops chrono or is replaced;
  until then there is nothing to fix, only a fact to know.
- **caldav-J2.** (opportunity opened by the jiff migration, not a defect)
  Generated VTIMEZONE components still emit a single STANDARD block
  carrying the offset for the event's instant, which is approximate for a
  recurring event spanning a DST transition - off by the DST delta on the
  far side. That limit was accepted when the offset had to be resolved by
  hand-walking `LocalResult`; jiff exposes zone transitions directly, so
  emitting real STANDARD/DAYLIGHT transition rules is now substantially
  cheaper than it was. Still not obviously worth it - most servers
  re-resolve the TZID by name and ignore the supplied component - so this
  is a re-evaluation, not scheduled work. The accepted limit is documented
  in `reference/caldav.md`.
- **caldav-F1.** VTODO / VJOURNAL resources still occupy the event
  cursor. The snapshot and changes lanes key on the PROPFIND href
  listing, which does not carry the component type, so a task resource in
  a shared calendar collection is emitted as a created/updated event
  change whose hydration yields no events. Filtering needs either a
  component-type PROPFIND or a first-fetch classification cache.

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

## bifrost-gmail

- **gmail-A1.** (audit boundary, not a defect) The 2026-07 google+net
  bug sweep did not line-audit: Gmail MIME rendering, draft patching,
  search translation, identity and vacation mapping in
  `crates/google/src/account/pim.rs`; the already test-dense contacts,
  calendar, account-error, filters, and cloud modules;
  `crates/net/src/account_error.rs` beyond its integration suite and
  `trace.rs` beyond construction-level invariants; and bifrost-graph
  beyond its `attach_account` reattach path. Listed so a future auditor
  knows where coverage stops.

## bifrost-graph

- **graph-S1.** `GraphClient` carries a local per-client `Semaphore`
  for request concurrency. If a per-account concurrency limiter ever
  lands in `bifrost-net` or `bifrost-sync`, delete the local one.
  (Carried from the deleted unification plan's
  captured-but-not-decisions block.)
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
- **graph-A5b-4.** The public-folder incremental poll uses
  `DateTimeReceived` as its change watermark (`advance_watermark` /
  `incremental_added_ids` in `account/public_folder.rs`). An in-place edit
  that changes an item's change-key or read state WITHOUT moving its
  received time is never re-emitted: the `DateTimeReceived >= watermark`
  restriction excludes it, and the id-only full scan reconciles only
  deletions (and untimestamped additions), not in-place edits. This
  affects ALL public-folder classes, including `Message`, not just the
  non-mail classes A5b-3 added. Fixing it needs a modification signal
  (e.g. `LastModifiedTime`) or a different watermark model entirely - a
  known poll-model limitation, not a regression.
- **graph-T1.** (coverage; largely closed by xc-3a on 2026-07-31) The REST,
  aux, and download surfaces now script at the wire via
  `bifrost_net::test_support`, so retry, backoff, the rate-limit permit, the
  redirect walk, and the ranged-read contract all run below the script and
  are observable - `script_rest_with_retries` plus `wire_attempts()` pin
  attempt counts, and `a_transient_5xx_is_retried_below_the_graph_funnel`
  pins one funnel call against two wire attempts. Downloads moved once
  `Canned` grew streaming bodies (`Stream` / `StreamThenError`), which
  preserve chunk framing; that also surfaced that the old queue answered
  ranged reads without a 206 or a `Content-Range`, so ranged tests recorded
  a window the account never had to actually send.

  What REMAINS is EWS, the one seam still answering at a funnel
  (`EwsExecute`). `EwsClient::execute` does post through `AccountNet`, so it
  is migratable; it was deliberately left, because that double replaced
  three failed review-only rounds and caught four defects, and observing
  retry on SOAP posts does not justify rebuilding a working seam. Revisit
  if an EWS defect is ever traced to retry, backoff, or a redirect.
  (The blob byte streams, the pre-authed OneDrive chunk PUT, the
  Autodiscover POST including its in-body redirect chain, the renewal
  worker's SUCCESS leg across ticks, and the three-mailbox search walk
  with a shared mailbox's own `nextLink` are covered as of the
  download/aux seams.)
- **graph-S2-residual.** (smell; the autodiscover half is fixed - it now
  classifies with the real response headers and a scripted 3xx pins it)
  The chunk-PUT `_ =>` arm in `account/cloud.rs` is still reachable only
  by a passed-through 3xx, because bifrost-net returns `Err` for every
  4xx/5xx before the response surfaces. Unpinned, but it does classify
  with the real headers already.

## bifrost-sync

- **sync-F3.** `Reconcile` items requesting `DedupeByClientId` only
  (no `CheckTarget`) still queue `PendingReadback`. Counters surface
  the case and a `Warning::OperatorAttentionNeeded` fires, but the
  read-back guard runs anyway. Per sync-D4 plumbing is correct; revisit
  when a consumer (ratatoskr) starts using
  `MutationCounters::dedupe_by_client_id` to suppress the read-back.

  UNRULED as of 2026-07-31, but the premise above is WRONG and the item
  is much narrower than it reads. Verified against the code:

  - The described engine behaviour is real. At `engine.rs:1238-1281`
    (mirrored at 1580) the loop over `advice.guidance.actions` sets only
    `wants_dedupe`; the following `for id in &remaining` loop inserts
    `PendingReadback` unconditionally. `wants_dedupe` drives the counter
    and the warning, nothing else.
  - But `DedupeByClientId` WITHOUT `CheckTarget` cannot be produced.
    `RecoveryClass` is never set by producers - `try_build` always derives
    it through `recovery::derive` - and there are exactly two paths that
    yield a `Reconcile`: `transient_retry_or_reconcile` gives
    `TransportDropAfterSend` with `actions: [CheckTarget]`, and
    `derive_protocol`'s `PartialResponse` + non-idempotent arm gives
    `PartialCompletionSignal` with `actions: [CheckTarget,
    DedupeByClientId]` (`crates/types/src/error/recovery.rs:721-729`).
    Nothing outside `recovery.rs` constructs a `ReconcileAdvice`, and the
    builder exposes no way to inject one.
  - So `CheckTarget` is present in EVERY producible reconcile advice, and
    in the one case that also carries `DedupeByClientId` the read-back
    queue is exactly right rather than an over-reach. The division of
    labour already holds: `CheckTarget` is the engine's job (the read-back
    guard IS the target probe), dedupe-by-client-id is the consumer's, and
    the warning at `engine.rs:1272` says so.

  There is therefore no behaviour change to consider, and nothing to
  suppress. What is left is bookkeeping:

  1. Close as resolved-by-analysis, recording that the feared shape is
     unproducible and the reachable shape is correct.
  2. Close, plus a test pinning the reachable case - a `[CheckTarget,
     DedupeByClientId]` advice queues read-back AND warns - so the
     reasoning is enforced rather than only written down. LIKELY CHOICE,
     not yet ruled.
  3. Keep open as a guard against a future `ReconcileAction` variant.

  RULED AND DONE (2026-07-31): option 2, in the form that actually carries
  the weight. Re-verified first - the two producers are still the only ones
  (`recovery.rs:721` partial-response and `recovery.rs:751` transport drop;
  the third `RecoveryClass::Reconcile` hit in that file is a test), and the
  engine loop at `engine.rs:1238` is unchanged.

  The test landed in `bifrost-types`, not `bifrost-sync`, because the
  load-bearing claim is a PRODUCER invariant: every producible
  `ReconcileAdvice` contains `CheckTarget`. That invariant is what makes the
  engine's unconditional `PendingReadback` correct, and since `try_build`
  always routes through `derive` and producers cannot set `RecoveryClass`
  directly, pinning both `derive` arms pins the entire producible space.
  `every_producible_reconcile_requests_check_target` asserts both arms carry
  `CheckTarget` and that the partial-response arm is what makes
  `DedupeByClientId` reachable at all. Verified sensitive by dropping
  `CheckTarget` from that arm and confirming the failure.

  The engine half (drive a real mutation and observe the queue plus the
  warning) was NOT built: it needs an attached account and a failing
  non-idempotent mutation, and it would be pinning a behaviour that is only
  correct BECAUSE of the producer invariant now pinned above. Worth adding
  if the mutation-loop harness ever grows for another reason.

  Point 3 stands as filed and still belongs to sync-N1, not here.

  On 3: `ReconcileAction` is `#[non_exhaustive]` and the match at
  `engine.rs:1244` ends in `_ => {}`, so a new variant is silently ignored
  here. That is a real concern but it is the same shape as sync-N1
  (after-exhaustive wildcards in cross-crate matches) and belongs there,
  not in this item.
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
- **sync-N1.** (partially addressed) `directive_target_scope` and other
  `_ => None` after-exhaustive arms route a new scope-bearing
  `EngineDirective` variant account-wide instead of failing to compile.
  A5c reduced the footgun for the known variants: `directive_target_scope`
  now names `DisableScope` (and the other scope-bearing variants)
  explicitly before the wildcard, so every *current* variant routes
  per-scope. The residual stands: the `_ => None` wildcard could NOT be
  dropped - `EngineDirective` is `#[non_exhaustive]` in `bifrost-types`
  and the match is in `bifrost-sync`, so a cross-crate match requires a
  catch-all even when every variant is named (the existing comment at the
  arm states this). Full compile-time enforcement remains impossible
  without dropping `#[non_exhaustive]` from the `pub`, re-exported enum -
  a broader API-stability change out of A5c scope. Until then a future
  scope-bearing variant still defaults account-wide here and needs a human
  to add its arm. Close-pass ruling (2026-07): the flag is sufficient -
  the account-wide default is the conservative direction (never narrower
  than the directive asks), `DirectiveKey::Other` bounds dedupe
  coarseness to the old behavior, and `handle_engine_directive`'s
  required fallback logs the unhandled variant.

## bifrost-types

Surfaced while authoring `reference/error-model.md` (a read of
`crates/types/src/error/`). All pre-existing, none blocking.

- **types-N2.** (smell) `Transport + Acknowledged` is rejected three
  ways: `try_build` returns `TransportAcknowledged`, and
  `recovery::derive` *additionally* re-checks it with a `debug_assert!`
  (debug panic) plus a release-mode demotion to `InFlight`. Since
  `derive` only runs from inside `try_build` *after* that branch
  already returned `Err`, the `derive`-side check is dead in normal
  flow (reachable only by calling `pub(crate) derive` directly, as the
  tests do). Belt-and-suspenders, but the debug-panic-vs-release-demote
  fork is a real behavior split worth being aware of.

## Stage 3/4 (contacts + calendar) review

Open findings from the contacts/calendar review wave, carried from the
deleted stage 3/4 review. The fix wave there closed all six
bugs and the tractable gaps; these survive. Labels: **gap** (silent
intent loss or pending design decision), **smell**, **nit**. The
deliberate "accepted fidelity limits" from that doc are documented in
`reference/*.md` and are not tracked here.

Smells:

- **s34-S3 (graph)** `contacts.rs` - `ContactEmail.kind` maps
  to/from Graph `emailAddress.name`, a display name, not a type label.
  Round-trip is consistent (no loss) but semantically conflated.

Nits:

- **s34-N1 (caldav)** `account.rs` - `discover_calendar_user_email`
  and `discover_schedule_outbox_url` run unconditionally on every
  `open` (4+ PROPFIND round-trips, errors swallowed), even for
  accounts that never RSVP. Lazy discovery needs interior mutability
  and touches the open happy path; skipped during the fix wave.

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

- **nc-2 (graph)** `get_item_body` is message-shaped: it requests
  `message:ToRecipients` and friends, so a mixed-class public folder hydrates
  its mail correctly and then fails per item on `Contact` / `CalendarItem` with
  `ErrorInvalidPropertyRequest`. The class is not knowable at request time from
  a bare item id; thread `EwsItem.item_class` through from the inventory pass
  and make the requested property shape conditional. Consumers that drop
  non-mail scopes before hydration do not hit this.
- **nc-3 (graph)** EWS `GetItem` fans out one request per item because the
  per-folder routing headers differ. Items sharing a public folder could batch
  into a single `<m:ItemIds>` list; worth doing once a pinned folder is large.
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
- **nc-8 (jmap)** `pim::containers_list` reports `Container::rights` for the
  primary account from `Mailbox/myRights`, but a foreign account's mailboxes go
  through the same `container_from_mailbox`, so a share whose `Mailbox/get`
  omits `myRights` silently projects as unreported rather than as a
  degradation. The `ContainerList::skipped_scopes` lane (which closed nc-1)
  could now carry it, but nothing classifies the omission today.

## Cross-crate items from the bug-hunt loop (2026-07-29)

Surfaced while working the per-crate bug-hunt ledgers of that wave (since
deleted; unrelated to the current `notes/bugs-*.md`) crate by crate. Each of
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
    (`crates/sync/src/engine.rs`), takes the account's registry records
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
  2. FIXED (2026-07-31). The registry records used to OUTLIVE the account:
     `detach` forgot the sink, the scheduler budget, the backfill registry,
     throttles, and the bandwidth meter, but never touched
     `self.subscriptions`, so re-attaching the same `AccountId` inherited
     the previous incarnation's handles and a later `unsubscribe_push` or
     reopen would present handles minted by a dead connection to the
     provider as though they were live. Fixed independently of the contract
     question, because it was wrong under every option below. `detach` now
     takes the records; dropping loses nothing retryable, since after
     detach `unsubscribe_push` rejects with `AccountNotAttached` and reopen
     only runs on an attached slot, so nothing could reach them anyway.
     Pinned by
     `detach_drops_push_records_so_a_reattach_cannot_reuse_dead_handles`,
     which reproduces the original defect when the fix is reverted.

     Option D also landed in its low-cost form: a detach with records still
     registered logs on `bifrost.sync.push` rather than absorbing the case,
     since it means `unsubscribe_push` was never called and the provider
     will hold live subscriptions until its own expiry. A structured
     `Warning` was NOT used - detach has already torn the account's change
     stream down, so there is no lane left to carry one.

     Still UNRULED, and untouched by this: whether the shared contract
     should keep placing server-side teardown on the consumer at all
     (options A-D below). The fix above is pure hygiene and forecloses
     none of them.

  Options considered, stated neutrally (A is now DONE; B/C remain open,
  and D landed as a log line rather than a typed warning):
  - **A** Hygiene only: `detach` clears the registry, and the window is
    documented explicitly. Fixes (2), leaves the contract alone.
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
  than inheriting it.

  The `reference/sync.md` note disambiguating the two entry points is
  DONE (2026-07-31), under "Push reconciler", alongside the detach
  semantics above.

- **xc-3 (net + graph, related in jmap)** RESOLVED on the net side
  (2026-07-31). `bifrost-net` now publishes the wire seam under a
  `test-support` feature: `bifrost_net::test_support` exports `Canned`,
  `ScriptedDispatch`, `RequestSnapshot`, `canned` / `canned_with_headers`,
  and `scripted_net` / `scripted_account`. The `Dispatch` trait stays
  crate-private - its signature is in reqwest types, and keeping reqwest
  out of the public API is why the request wrapper exists - so what is
  published is the double, not the trait. `request.rs`'s own unit tests
  were migrated onto the published double so there is one definition
  rather than a private copy plus a downstream copy, and
  `tests/test_support_seam.rs` exercises it as a separate crate (which is
  what catches a private-type leak or a mis-gated item that in-crate tests
  would not). It also pins the two contract facts each hand-rolled double
  had been re-deriving: a 4xx never surfaces as `Ok(Response)`, and an
  exhausted script panics instead of reaching the network.

  **xc-3a (graph)** is DONE (2026-07-31). Graph's REST and aux surfaces now
  script at the wire: `script_rest` / `script_aux` install a
  `ScriptedDispatch` and bind an `AccountNet` to it, so responses travel the
  production retry loop. `into_net_outcome` - the local restatement of
  bifrost-net's status contract - is deleted. Only request RECORDING stayed
  local, since Graph's recorded shape (parsed JSON body, lifted `If-Match` /
  `Prefer`) is richer than `RequestSnapshot`; that kept all 62 scripting call
  sites working unchanged. Deleting the restatement immediately paid: the old
  helper answered a 401 with `AuthLost` directly, hiding that bifrost-net
  forces a refresh and reissues on a separate budget first, so a Graph path
  meeting a transient 401 recovers with no error at all. Now pinned.

  Consumer migration elsewhere remains optional and unscheduled; no other
  crate has a comparable restatement.

  Original statement, for context on why several `bifrost-graph`
  paths are pinned only at the level of extracted pure decision functions,
  with the surrounding request/response sequencing left unpinned and named as
  such at the time: partial webhook-creation rollback, the
  inventory neither-link branch, the unsubscribe DELETE loop as a loop, a
  mixed reaction batch actually reaching `$batch`, and the renewal leg past
  `due_renewals`. `bifrost-jmap` hit the same wall and solved it locally by
  introducing a two-method `PushTransport` trait over the transport it owns,
  which worked precisely because jmap owns that transport - Graph does not.
  That choice - a Graph-local transport trait versus promoting net's seam -
  is settled: net's seam was promoted, per the resolution above. Related:
  `jmap-O2`, the jmap sync layer hardwiring `ReqwestTransport`, which is the
  same testability problem one crate over and is NOT closed by this: jmap
  pins its own `ReqwestTransport` in a type alias rather than riding an
  `AccountNet`, so it needs the transport generic threaded through (or the
  free-function extraction), not a net-side seam.

- **xc-4 (sync, maybe app)** Nothing schedules share-rediscovery reopens
  automatically. `AccountCapabilities::reopen_discovers_foreign_namespaces`
  (adjudicated during the jmap round: it means reopen-time discovery
  POTENTIAL - IMAP derives it from NAMESPACE, JMAP is constitutively true)
  tells a consumer that a share granted after open surfaces only through a
  reopen, and `SyncEngine::reopen` is the public staged-reattach entry that
  performs the rediscovery - but no component ever calls it on a cadence.

  RULED 2026-07-31: cadence stays CONSUMER POLICY. The engine will not
  grow a rediscovery timer. Not yet implemented - this is a decision, and
  the documentation and rename below are the remaining work.

  Why, so the ruling is not re-litigated from scratch: the right interval
  depends on things the engine cannot see (app foregrounded, metered
  connection, whether shares are common in the deployment), a `reopen` is
  a full staged reattach with real wire cost, and ratatoskr is the only
  consumer - a `tokio::time::interval` on its side is ~10 lines. Rejected
  alternatives were an optional `EngineConfig::rediscovery_interval`
  (default-off would go unused, default-on would be wrong for most
  deployments) and an `accounts_awaiting_rediscovery()` accessor that
  exposes the candidate set without owning the clock. The second is the
  one to revisit first if consumer footwork turns out to be the problem.

  Verified while ruling: nothing in `crates/sync/src` reads the flag -
  every occurrence there is a test stub setting it `false` - and no
  internal caller of `reopen` exists. Also relevant to the shape of the
  fix: `EngineConfig` has NO interval-shaped field today (every knob is a
  cap, a count, or a timeout; `PushConfig` is an empty struct), so adding
  one would have introduced the first engine-owned wall clock rather than
  extending an existing pattern.

  Remaining work:

  1. Document the pairing so the flag does not read as a promise the
     engine keeps. `reference/sync.md` should say plainly that the flag is
     advisory TO THE CONSUMER and that the engine never schedules on it;
     a consumer reads the flag and drives the call itself.
  2. Rename `SyncEngine::reopen`. The name undersells the operation and
     actively hides it from the consumer this ruling puts in charge:
     someone told "drive share rediscovery yourself" will search for
     something named `rediscover*` and find nothing. The method does a
     full staged reattach - re-runs scope and membership discovery,
     establishes newly-appeared scopes, drops vanished cursors, recreates
     push subscriptions, refreshes the capability snapshot, then swaps the
     handle. Candidate names: `rediscover_and_reattach` (most literal),
     `reattach`, `reopen_and_rediscover`. Not settled.

     Sizing, because it is bigger than it looks: `reopen` appears 105
     times in `sync/src/engine.rs` and 35 times in `reference/sync.md`,
     and most of those are the reopen LANE (`reopen_tx`, `reopen_lock`,
     `ReopenRequest`, the reopen listener), not the public method - a
     blind rename would churn the internal vocabulary too. Decide whether
     the lane keeps its name.

     The sharper consequence: the capability flag
     `reopen_discovers_foreign_namespaces` NAMES the method. Renaming the
     method either drags the flag with it - a `bifrost-types` public API
     change touching all seven account crates plus every test stub - or
     leaves the flag naming a method that no longer exists. That coupling
     is the real cost of the rename and should be decided before starting,
     not discovered midway.

  UPDATE (commit 6829767): the EWS half is solved Graph-locally. Every EWS
  request goes through one funnel, `EwsClient::execute`, so a crate-private
  `EwsExecute` trait plus a scripted in-crate double made the whole streaming
  worker loop hermetically drivable - and immediately paid for itself by
  verifying four defects that three prior review-only rounds had each failed
  to prevent. That is evidence for the general shape of the fix, and it
  narrows this item rather than closing it: the REST paths above still have
  no seam, because they funnel through `ClientInner::execute_request` against
  a concrete `AccountNet` rather than through a trait. The open question is
  unchanged - Graph-local `GraphTransport` (now with a working precedent one
  module over) or promote net's `Dispatch`.

  UPDATE 2: the REST half is now solved Graph-locally too, with the
  `#[cfg(test)]` response queue rather than a trait. Every REST helper -
  including the one raw-MIME body that used to build its own request -
  funnels through one `GraphClient::execute_wire`, which adapts the
  production response into a Graph-local wire shape; every path named above
  is now pinned end to end. Two things had to be true for that to be worth
  anything, and both cost a round to get right: a scripted status has to
  take the shape bifrost-net's retry loop would have produced (it returns
  `Ok(Response)` for 2xx and a passed-through 3xx ONLY), and an exhausted
  script has to fail loudly instead of falling through to the network.
  Getting the first right surfaced a live defect the seam had been hiding
  in plain sight: because a 4xx never arrives as a response, Graph's typed
  `error.code` classification and its `subscription_is_gone` predicate were
  both dead on the live path. That is the argument for promoting net's
  `Dispatch` instead: an in-crate double has to re-derive the transport's
  status contract, and every crate that builds one re-derives it
  separately. This item stays open on the net side; the Graph consumer no
  longer blocks on it.

- **xc-5 (types + sync, surfaced from the DAV crates)** DECIDED and closed
  (2026-07-31). The engine now announces page-level loss instead of
  accumulating it: `announce_page_loss` emits a `SyncEvent::Warning`
  (`OperatorAttentionNeeded`, `next_action` pointing at the lanes, counts
  only - `failed_ids` holds native ids and is not user-safe text) on the
  account change stream when one of the four forwarded query methods
  returns a page with `failed_ids` or `skipped_scopes` non-empty. A clean
  page stays silent.

  The item's original framing was wrong in a way that changed the answer,
  and the correction is worth keeping: "an app that routes everything
  through the engine never sees the loss" is not true. `Page` appears only
  on on-demand query surfaces, never in the sync pipeline, and the engine
  either forwards the page verbatim (`contacts_list`, `directory_*`) or
  does not expose the method at all (`search`, `search_messages`,
  `contacts_search`, the calendar walks) - so the consumer always holds
  both lanes. Nothing was ever dropped. The real gap was an ASYMMETRY:
  open-time skips got an accessor and a log line, page-time skips got
  silence, so a consumer had to already know to look.

  Rejected: folding page skips into a queryable lane beside
  `open_skipped_scopes`. A page lane is true of one walk at one moment and
  has no healing point, so it would need an invented expiry, dedupe key,
  and cap; and because the engine does not expose every query surface, the
  accessor would report "no skips" while a direct `Account` call had just
  quarantined three scopes. A surface that looks authoritative and is
  systematically incomplete is worse than none, since it invites consumers
  to stop reading the pages. Reasoning recorded in `reference/sync.md`
  under "Page loss lanes".

## Workspace sweep: local copies of a contract (2026-07-31)

- **sweep-1 (workspace)** Sweep every crate for the failure mode the
  xc-3 / xc-3a / imap-T3 / imap-S1 slices each hit independently. It has
  one shape: **a local restatement of a rule that lives somewhere else,
  kept alive by a test that exercises the copy rather than the original.**
  The copy and its test agree with each other indefinitely; only the
  original disagrees, and nothing asks it. These do not surface as
  failures - they surface as tests passing - which is why review rounds
  keep missing them and why this wants a deliberate sweep rather than
  another read-through.

  The four found so far, as calibration for what to look for:

  - `bifrost-graph` `ScriptedRestResponse::into_net_outcome` reimplemented
    bifrost-net's status contract. It answered a 401 with `AuthLost`
    directly, hiding that the transport forces a token refresh and
    reissues on a separate budget - so a Graph path meeting a transient
    401 recovers with no error at all, and every test asking about a 401
    was asking the copy.
  - `bifrost-graph`'s download queue answered ranged reads with neither a
    206 nor a `Content-Range`, so a ranged test recorded a `ByteRange` the
    account never had to actually put on the wire. It asserted the range
    it LOGGED, not the range it SENT.
  - `bifrost-imap` `id_from_scope` / `mailbox_throttle` matched only
    `ErrorScope::Mailbox { id }` after producers had migrated to
    `Cursor(Folder(_))`. `ThrottleScope::Mailbox` became unreachable in
    production while its test kept passing, because the test used the
    now-production-dead `with_mailbox` helper.
  - `bifrost-imap` `Translation::skip_attempt_cause` documented a guard it
    never armed (never assigned `true`), so a `try_build` invariant was
    upheld by comment only.

  What that suggests looking for, in rough order of yield:

  1. **Re-derived contracts.** Any crate-local function that decides what
     another layer would have decided: status-to-error mappings, retry or
     backoff simulations, idempotency or transmission-state inference,
     capability gating restated away from the capability source. The tell
     is a comment of the form "mirrors X" / "same as X" / "what X would
     have produced" with no mechanism keeping the two in step. `grep` for
     `mirrors`, `same shape as`, `would have`, `equivalent to`.
  2. **Partially-migrated readers.** A producer changed its shape and only
     some consumers followed. The tell is a `match` on an enum where a
     sibling arm handles a case this one silently drops to `_ => None` /
     a default. `resource_from_scope` handled both shapes; the two beside
     it did not, and nothing made that visible.
  3. **Test-only helpers used by no production path.** Every one is a
     potential fake target for a passing assertion. Enumerate helpers
     reachable only from `#[cfg(test)]` and check whether a test built on
     one is claiming something about production. (`with_mailbox` and
     `with_transmission_state` are the known pair; there are likely more.)
  4. **Documented guards.** Any comment promising an invariant is enforced
     - check the enforcement exists and is reachable. `skip_attempt_cause`
     was dead; the doc read as though it were not.
  5. **Doubles above the layer they describe.** A seam that intercepts
     above the component whose behavior the test names cannot observe that
     behavior. EWS (`EwsExecute`) is the known remaining one, deliberately
     kept (see graph-T1).

  Method note, learned the hard way: three of the four were found by
  DELETING the local copy and routing through the real thing, not by
  reading either. Reading the copy tells you what it claims; deleting it
  tells you whether the claim was true. Where a copy cannot be deleted
  outright, the cheaper version is to route ONE test through the real path
  and see whether it still passes.

  Not scheduled, no blast radius bound yet - sizing is part of the job.
  Deliverable is a findings list triaged bug / gap / smell / nit, not a
  fix wave; fixes get scheduled per finding.

  TELLS 1, 3, AND 4 ARE DONE (2026-07-31). Result: 0 bugs, 1 smell, 1 nit,
  both fixed in the same commit. Tell 2 remains, and is now the only part
  worth spending on.

  Why the yield was so much lower than the four calibration finds, since
  that is the reusable lesson: **drift needs movement.** All four earlier
  defects lived in code that had MOVED - a producer changed shape, a seam
  was promoted, a contract was restated across a layer boundary. Tell 1
  greps (`mirrors`, `same shape as`, `equivalent to`) mostly surface copies
  of FROZEN specs, which cannot rot. `types::mime::is_atom_phrase` and
  `smtp::is_valid_phrase` are byte-comparable encodings of RFC 5322 `atext`
  and were verified identical; the RFC has not moved since 2008. The string
  marks intent-to-copy, which is only weakly correlated with drift.

  Tell 3 (test-only helpers) was likewise near-empty: the suspicious ones
  (`autodiscover::parse_user_settings`, `decode::parse_response`) are honest
  thin wrappers that delegate to the real function, and their docs say so.

  What DID pay was tell 4, in a form worth reusing: **arm the compiler
  instead of reading.** `bifrost-imap` and `bifrost-jmap` both set a
  crate-wide `#![allow(dead_code)]`, which switches off exactly the signal
  that would have caught `skip_attempt_cause`. Measured before acting: 105
  warnings in imap, 195 in jmap, overwhelmingly legitimate unused PROTOCOL
  surface (command builders, response types, per-RFC method modules that
  consumers call and the crate does not). So removing the blanket allow is
  NOT a cheap win and was not done.

  The bounded version was: re-arm the lint on the ERROR-TRANSLATION
  boundaries only, where a dead item is a hole in a contract rather than
  unused API. `#![warn(dead_code)]` now sits on
  `crates/imap/src/account/error.rs` and `crates/jmap/src/sync/error.rs`;
  `bifrost-smtp` needed nothing (no blanket allow, so already armed). It
  found five unreachable items in imap and two in jmap on the first run,
  three of which were not previously recorded anywhere. All seven proved
  dead-but-intentional and are now annotated with the REASON they are
  unreachable, so the next reader gets the ruling instead of re-deriving it.
  Verified the guard bites by adding a dead function and confirming the
  build fails.

  The one with teeth for the future: `imap::account::error::strategy_failure`
  is unreachable because the strategy ladder always has somewhere to land
  (QRESYNC -> CONDSTORE -> Basic, and Basic is plain FETCH), so downgrades
  report as `WarningKind::StrategyDowngraded` and continue. That is a
  property of the CURRENT ladder, not of the error model - a future strategy
  with no weaker peer needs it.

  - **sweep-1a (graph, smell) FIXED.** `public_folder_containers`
    (`account/pim.rs`) degrades four ways when a folder has no
    `public_folder_meta`: display name falls back to the raw EWS folder id,
    content class / parent / rights go `None`. Production cannot reach it -
    `seed_and_scope` writes BOTH maps unconditionally - but the test-only
    `seed_public_folder_for_tests` wrote routing alone, so a test could
    drive branches production never takes. The seeder now writes both.
  - **sweep-1b (smtp, nit) FIXED.** `message_error_to_account_error` has no
    production caller (nothing in the workspace builds an SMTP `Message`;
    IMAP submission takes raw RFC822 bytes, and the live boundary
    `into_account_error` dispatches on `SmtpError::kind`). Its doc asserted
    as present fact that send pipelines "feed validation failures through
    this function so the single-translation-boundary rule holds". Reworded
    to say what is true and why it is kept.

## Open items folded in from the bug-hunt ledgers (2026-08-23)

The eight `notes/bugs-*.md` ledgers and `notes/carry-forward.md` were closed out
and deleted on 2026-08-23. Everything durable from them moved to `reference/*.md`
or to inline comments at the code it describes; everything still open moved here.

Categories, as the ledgers used them: **C1** live defect, **C2** latent defect,
**C3** refactor opinion, **C4** product decision. **PUBLISHED SURFACE** means the
remedy removes, renames, or reshapes a published item - those are the repository
owner's call and must not be actioned without one, no matter how confident the
argument reads. Ledger findings were never verified against a running server;
confirm against the code before working any of them.

### Fenced for the repository owner (published surface)

- **dav-B2. Cursor sync only ever covers one collection.** [C1] **Partly
  addressed 2026-08-23; the model fix itself is still open and still fenced.**
  `establish_initial_cursor` / `inventory_stream` / `changes_stream` all read
  `default_calendar_url` (CalDAV) or `default_addressbook_url` (CardDAV), and
  `discover_cursor_scopes` yields a single `CursorScope::Type(CalendarEvent)` /
  `Type(Contact)`. An account with three calendars enumerates all three in
  `calendars_list` but syncs only the first: objects in the others never appear
  in inventory or changes and never get an update or a delete. Note the PIM
  primitives are unaffected - they route by the caller's `calendar_id` /
  `address_book_id`, and `event_get` derives the collection from the event's own
  URL - so it is specifically SYNC that is single-collection.

  What landed: the uncovered collections are now reported at open as
  `SkippedScope` entries (`ErrorScope::Calendar`/`Contact` plus an
  `Unsupported(DiscoverCursorScopes)` error), in both crates and through
  `bifrost-imap`'s composed path, which previously discarded a successful DAV
  open's skip lane outright. That removes the "looks complete, silently is not"
  trap without touching the cursor model.

  What remains: the honest model is a `CursorScope::Folder(href)` per
  collection, so every calendar and address book actually syncs. That reshapes
  the published cursor model and the stored envelope, forces an envelope bump,
  and costs every DAV account a second full re-sync (after the v1 -> v2 href
  correction). Owner's call. It must land in both crates together or it becomes
  another drift entry.
- **dav-B10. `default_*_url` falls back to the collection home.** [C2] Split out
  of dav-B3, whose list-side phantom was removed 2026-08-23. This is the
  symmetric half and it is present in BOTH crates: when discovery finds zero
  collections, `default_calendar_url` / `default_addressbook_url` fall back to
  `client.resolve_url(&home)`, so the cursor, inventory and changes lanes target
  the home collection - the same phantom by another name, one layer down. A
  spec-correct server 404s those queries.

  It is not simply removable the way the list phantom was: the field is not an
  `Option`, and the lanes need an answer for "no collection exists". The likely
  shape is an open-time `SkippedScope` (the machinery now exists, see dav-B2)
  plus empty inventory and change streams, which is enough design to deserve its
  own decision rather than riding along with a one-line deletion. Fix both
  crates together.
- **dav-B11. Implement cross-collection moves in the DAV crates.** [C4, feature]
  Split out of dav-B4, whose silent-drop half was fixed 2026-08-23: CalDAV now
  refuses a cross-calendar `event_update` the way CardDAV already refused a
  cross-address-book `contact_update`, so neither crate can drop a relocation
  request on the floor any more. Neither can perform one.

  Doing it means WebDAV `MOVE` with a `Destination` header - one request, atomic
  where the server supports it - with a fallback for servers that do not:
  GET + PUT-to-new + DELETE-from-old, which is non-atomic and needs the
  `Protocol(PartialResponse)` + `TransmissionState::Acknowledged` treatment that
  `event_rsvp` already uses for its own non-atomic sequence, so a consumer can
  tell "not moved" from "copied but not cleaned up".

  This is a feature request with a design, not a latent bug: nothing is silently
  wrong while it is absent. If it happens it should land in both crates together,
  since a move that works for events and refuses for contacts is a new
  asymmetry rather than a fixed one.
- **dav-B5. The CalDAV/CardDAV duplication.** [C4] **Deliberately left open on
  2026-08-23; not resolved, and not to be acted on without the owner.**

  Roughly 1500 duplicated lines across `client.rs` (transport, redirect policy,
  `auth_headers`, `escape_xml`, etag handling, the raw request helpers, the
  ~120-line `status_error` ladder), `parse.rs` (the whole propstat state machine,
  href resolution, multiget classification) and `account.rs` (the cursor codec,
  the snapshot diff, `put_condition`, URL comparison, and ~400 lines of
  `Unsupported` stubs each crate carries for the other's domain). The genuinely
  protocol-specific parts are the property names, the query XML, and the body
  projection.

  The duplication is real and the drift it causes is measured, not theoretical:
  SEVEN divergences have been found between these two crates, three of them
  fixed on 2026-08-23 (the `contact_snapshot` ctag path, the phantom collection,
  the silently-dropped calendar move). Every one was a case of a fix landing in
  one crate and not its twin.

  Three options, none picked:

  **A. Collapse to a single `bifrost-dav`** parameterized over the
  collection/resource kind, with CalDAV and CardDAV as thin projection layers
  (`ical.rs` / `vcard.rs`) plus their prop constants and query bodies. Removes
  the duplication outright. Highest blast radius: it reshapes two PUBLISHED
  pre-1.0 crates. Note the original argument for it - "pre-1.0 and crate-private
  below a factory, so the blast radius is small" - is a claim about THIS
  workspace, and both crates are published with consumers outside it by
  definition. That is the same reasoning that produced two public-API deletions
  which had to be reverted; see the standing lessons in `AGENTS.md`.

  **B. Extract the protocol-neutral half into a PRIVATE shared crate**, leaving
  both published surfaces exactly as they are. `bifrost-sasl` is the existing
  precedent in this workspace: a private shared computation layer consumed by a
  protocol crate and not published as public API. Kills the drift without
  touching either crate's contract. Real work, no user-visible benefit.

  **C. Leave it, and rely on the drift rule.** `reference/caldav.md` and
  `reference/carddav.md` now both state that these crates are near-duplicates
  and that any fix to shared-shape code must be checked against the other.
  Defensible: after the 2026-08-23 round the drift-prone code is mostly
  unified already (one `PropStat` struct, one href-resolution rule, the ctag
  path fixed), and what remains duplicated is stable code - `escape_xml`,
  `status_error`, the cursor codec - which has drifted rarely because it
  changes rarely.
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
- **google-B4. `calendars_list` returns a `Vec` with no streaming.** [C4]
  **CLOSED 2026-08-23 as a considered non-defect. Do not re-file without new
  evidence.** The shape argument does not survive the numbers: Google paginates
  at 250/page under a page budget with a repeated-token guard, and a real
  account has tens of calendars, so it is one page. The scenario the finding
  describes needs 250+ calendars. Against that, `calendars_list` is a published
  `Account` trait method with six real implementors and five test stubs, plus
  every out-of-workspace consumer. Note also that the trait already
  distinguishes these cases deliberately - `contacts_list` returns
  `Page<ContactCard>` with a page cursor because contacts number in the tens of
  thousands, and calendars do not. The inconsistency is considered, not an
  oversight.

  Auditing it did surface a real defect in a different crate, which is fixed:
  `bifrost-graph` had SIX unbounded `@odata.nextLink` loops with no page budget
  and no repeated-link guard, plus an unguarded folder-parentage descent. See
  `reference/graph.md`, "Bounded `nextLink` traversal".
- **sync-B4. Work off inventory coverage debt (the repair path).** [C2] Tail of
  google-B5, whose mechanism landed 2026-08-23. Inventory now has an
  `InventoryCoverage` model: a walk that cannot represent an object records an
  `InventoryObligation` and keeps going, every checkpoint from that point
  declares the gap, cursor and coverage are persisted in ONE atomic record, the
  backfill completion sentinel is withheld, and a warning is raised. So the
  scope converges live-and-degraded and nothing is silently lost.

  What does NOT exist yet is anything that works the debt off. Obligations sit
  in the durable record until a later walk of that scope reports `Complete`.
  For a Gmail account that means the object stays missing until the next full
  inventory, and for a scope whose backfill has finished there may be no next
  walk at all.

  The design, argued out with the cold reviewer and recorded in
  `reference/sync.md`: a repair pass reads the obligations back and retries
  them, using the account's own opaque `repair` / `replay` tokens rather than
  ordinary `get_stream` - only the protocol crate knows what a provider-native
  re-read needs, and a `Region` obligation names a page to replay rather than
  an id to fetch. A later success removes the obligation and emits the recovered
  entry; a definitive absence removes it without one. Obligation states worth
  having: `Pending` (retry normally), `OperatorBlocked` (a retry budget expired;
  stop trying automatically but stay visible), and `Waived` (an operator
  explicitly accepts the omission). **Only `Waived` is true abandonment, and it
  must never happen because a local retry counter ran out** - the account layer
  classifies evidence, it does not choose the user's acceptable-loss policy.

  Note the `Region` case has a hard boundary: the main cursor may advance past
  a replayable region only if that region can later be replayed INDEPENDENTLY of
  the advanced cursor. If Graph's delta API cannot replay a page after the delta
  link moves on, there is no honest discharge and that page has to remain a
  checkpoint barrier.

### Open defects

- **google-B2. The Drive resumable session is abandoned on a mid-upload `Net`
  error.** [C2] `upload_file_chunked` now rejects stalled, backward and
  impossible resume offsets under a finite attempt budget, so it cannot hang. But
  a `Net` error mid-upload aborts the function and abandons the resumable
  session; Drive keeps the partial upload for a week. There is no cleanup and no
  resume-on-reopen. The module doc acknowledges "a stray uploaded-but-unlinked
  file is the worst failure mode" for the link step but not for the upload step.
- **dav-B9. All DAV traffic bypasses `bifrost-net`.** [C2] Both crates run their
  own `ReqwestDavTransport` behind the `DavTransport` seam, so DAV legs get no
  retry, no rate limiting, no bandwidth metering and no observability, and
  `set_priority` / `set_bandwidth_cap` are silent no-ops in both. An IMAP account
  composed `with_caldav` / `with_carddav` and given a `BandwidthMeter` silently
  does not meter or cap its DAV legs. The two enablers shipped in the
  `bifrost-net` round-1 work (`AccountNet::request(Method, &str)` and an optional
  `AccountSpec::token_source`) deliberately without the migration; `Dispatch`
  staying crate-private was assessed and is correct. `reference/net.md` scopes
  the sharing claim to exclude these two crates rather than overclaiming.

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
- **A single unrepresentable object fails its whole hydration page.** JMAP's
  `event_from_jmap` returns `Unsupported` for an event with an unrepresentable
  recurrence, participant role or participation status, which fails the page that
  contains it rather than reporting that one event individually. That is the
  right direction against silent lossiness; whether the engine wants a per-item
  lane is a `BatchOutcome` shaping question for `bifrost-sync`, not a JMAP bug.
  Same shape as google-B5.

### Refactor backlog

Nothing in this section misbehaves. None of it is a bug, and none of it blocks a
defect fix - in particular, do not let a unification proposal become a
prerequisite for the small local fixes above.

- **sync-B1.** `crates/sync/src/engine.rs` is 5278 lines mixing five concerns:
  lifecycle, ~900 lines of recovery dispatch free functions, the ~500-line
  backfill orchestrator, the ~500-line mutation pipeline, and ~1200 lines of 1:1
  passthrough forwarders that invent no semantics (every one is `live_account(id)?`
  then forward, with an identical doc comment shape - a macro or a blanket
  forwarding trait, not 60 hand-written methods). `recovery.rs` exists but holds
  only the helpers while the dispatch stays in `engine.rs`, so the split is in
  the wrong place. `reference/sync.md`'s file map already describes the intended
  layout aspirationally and the code does not match it.
- **sync-B2.** `drive_changes_stream` still takes `_account_id` and `_ack_tx` and
  threads them from four call sites through `spawn_scope_poll_inner`. Dead
  parameters that obscure the actual data flow.
- **google-B6.** `inventory.rs::hydrate_one` issues both `get_message(id, "raw")`
  and `get_message(id, "full")` for `Projection::FullWithBlobs`. For a message
  with a 20 MB attachment that is ~40 MB of transfer and 10 quota units to obtain
  data the `raw` fetch already contains - `full` adds only the attachment ids,
  which are derivable from the MIME structure in the raw bytes or more cheaply
  from a `format=metadata` call. The single most expensive line in the crate's
  read path. The answer is correct, just expensive.
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
- **google-B8, smaller observations.** All [C3]: `push.rs` builds the
  transient-failure `Warning` with a constant `.with_retry_count(1)` regardless
  of how many consecutive failures occurred, while the renewer already tracks the
  `disconnected` state it could count from; `push_subscribe` emits
  `WatchEvent::Reconnected` before any consumer can have called `push_stream()`,
  and `broadcast` drops messages with no receivers, so the stream's first
  observable state is undefined; `client.rs::execute` sets
  `Content-Type: application/json` on bodyless GET and DELETE requests;
  `calendar.rs::search`'s clipped-tail comment ("any clipped tail is
  recoverable") is load-bearing but unverified - if Google ever returns more
  items than `maxResults` without a `nextPageToken` the tail is dropped silently
  and the cursor advances, so a `debug_assert` or an explicit `Warning` would
  make the assumption visible; `mutation.rs::post_empty_json` re-implements URL
  assembly that `GmailClient::api_url` already owns, so the raw-builder and typed
  paths can drift; `flags.rs::patch_for_set` names every user label in the
  account in `removeLabelIds`, which on an account with a few hundred labels
  ships a several-KB body per batch (the engine's read-back guard already fetches
  current state, so this is the site that would benefit most from
  read-back-then-diff).
- **dav-B8.** `event_search`'s empty-query branch lists and hydrates every
  resource in the collection before applying `request.limit`, and
  `events_in_range` likewise truncates to `limit` only after full hydration and
  projection. CardDAV's `contact_search` reruns the entire remote search and
  rehydrates everything for every page - documented as intentional and it does
  make `failed_ids` per-page honest, but it is O(collection) per page.
- **jmap-B1.** Three near-identical query/get/advance loops in the sync layer;
  `imap` has four copies of the untagged-response dispatch loop. Recorded for
  completeness with the other duplication findings; same standing as the above.

### Ledger residuals recorded as accepted, not open

Listed so they are not re-filed as untouched work. Each has its reasoning in the
`reference/` doc for its crate.

- The JMAP calendar and contacts audit was **static**, against the RFCs and the
  in-crate types. No server was involved, per the project's testing rules, so "a
  conforming server accepts this" is a reading of the spec, not an observation.
  The all-day `DATE` defect that round found is exactly the class an in-process
  round trip cannot catch.
- JMAP's recurrence mapping still covers only `FREQ`, `INTERVAL`, `COUNT`,
  `UNTIL`, `BYDAY`, `BYMONTH`, `BYMONTHDAY`; everything else rejects loudly.
  Widening it is tracked in `reference/jmap/DEFERRED.md`.
- JMAP `contacts.rs` outside postal addresses and titles (emails, phones, notes,
  media, name) still skips values it cannot parse rather than rejecting.
- JMAP's inventory overshoot is unbounded in principle - a bounded window that
  emits nothing keeps walking - and ends at the first surviving message in
  practice.
- Google's per-poll `users.getProfile` round trip is **kept deliberately**. The
  cost argument is correct (doubled request count and failure surface on the
  30-second poll), but it is the only thing that catches a rotated token now
  pointing at a different Google account before its history is mixed into the
  existing slot. It comes out when a token-source identity binding exists
  upstream, not before. Do not re-file this as free savings.
- Google's `get_stream` marks `PageBoundary::Final` only when the id stream
  closes during the batch drain, so a last batch that fills exactly to
  `HYDRATE_BATCH_SIZE` stays `Page` with the following `Done` as terminator.
  **Do not "finish" this with a one-item lookahead** - the ids come from a
  backpressured producer, so polling for the next id before hydrating the batch
  in hand deadlocks both sides. `bifrost-sync` reads `Final` in no hydration
  path, so the boundary is advisory.
- Google's cross-calendar event move stays non-atomic; the provider exposes the
  move and the field PATCH as separate requests. A second-leg failure returns
  `Protocol(PartialResponse)` scoped to the event in its destination calendar
  with acknowledged first-leg evidence, which is what makes it legible. No
  compensating move is attempted - that adds another blind write and another
  partial-failure window.
- A `close()` future dropped mid-`users.stop` leaves the Gmail-side watch running
  until it expires. The local half is cancellation-safe; retrying the remote stop
  needs a transport the close has already shed.
- Google's `open_blob_range` returns a classified `Unsupported(OpenBlobRange)`
  for every input including a forged handle claiming `supports_range`. That is a
  decision that Gmail attachments have no byte-range transport, not a stub.
- `read_capped_response_body` applies the read timeout per chunk, so a server
  trickling one byte per interval can stretch a terminal-status drain to roughly
  `STATUS_BODY_CAP` intervals. Bounded by the 4 KB cap; not worth a second
  deadline.
- CalDAV/CardDAV's credential-origin allowlist makes a request to an untrusted
  origin fail locally rather than go out unauthenticated. A consumer whose server
  names hrefs on a third origin - neither the configured base nor a discovered
  home - now gets a hard local error where it previously got a credential leak.
  Intended trade.
- The DAV cursor v1 -> v2 bump costs consumers one full re-sync per DAV account,
  once. `changes_from_cursor`'s token-retention path has no direct test and the
  missing-`sync_token` warning is log-only.
- RSVP is non-atomic by nature. Every failure path after the acknowledged outbox
  POST, including the local encoding steps between the POST and the PUT, is
  wrapped `Protocol(PartialResponse)` with `TransmissionState::Acknowledged`.
- IMAP's NOTIFY-runtime-rejection misreport: a folder admitted to the IDLE budget
  whose `NOTIFY SET` is rejected at runtime was already reported as pushed.

## Rules for agents working bug-hunt items

These earned their keep during the 2026 fix slices - keep applying them
to any item in this file:

1. Contract docs (`reference/*.md`, above all `error-model.md` /
   `sync.md`) and existing tests are authoritative; a finding that
   contradicts them loses. Don't rewrite a contract or a passing test to
   match a finding.
2. Confirm against the contract, not the finding's own rationale.
3. Fix only what is named and confirmed; don't generalize one case into
   a sweeping rule.
4. An existing test you must modify is a red flag - justify it
   explicitly.
5. A shared-crate change (`types`/`net`/`sasl`/`sync`) is pinned by
   downstream tests; run the full-workspace `brokkr check`, never `-p`.

## Notes

- The error-model design docs and the
  phase 4 audit / decisions docs were deleted at the end of phase 5.
  The contract lives in code; this file holds the residual cleanup
  tail.
- The orchestration, unification, and stage 3/4 review plans were
  deleted once their phases/stages
  fully merged - same convention as the error-model and Phase 0 plans.
  Their "what" lives in code + `reference/*.md`; their resolved-decision
  "why" lives in git history. Open items they still carried were folded
  into this file (the `*-T1`, `smtp-M1`, `graph-S1`, and `s34-*` items
  above).
- F-items came from phase 5B/5C/5D re-audits; N-items came from the
  original post-phase-4 audit. Both are intentionally tracked at the
  same level here - none are blocking ratatoskr.
