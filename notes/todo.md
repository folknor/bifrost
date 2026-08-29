# TODO

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
- **jmap-O2-residual.** `JmapAccount` hardwires `ReqwestTransport`, so
  `capabilities()`, `describe_cursor` and the mutation doors as whole
  calls are undrivable hermetically. Bounded, known residual: the
  free-function extraction pattern covers anything pure, and anything
  genuinely needing a scripted transport belongs with `xc-3-residual`.
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

- **dav-F5-transport.** The `DavTransport` / `DavResponse` test seam is
  duplicated in `caldav` and `carddav` rather than shared via
  `bifrost-net`, because net keeps its dispatcher crate-private and both
  DAV clients still own Basic auth and their own redirect policy - a
  shared seam would have to grow those first. Revisit when these clients
  move onto `AccountNet` (see dav-B9). The parser half of this item is
  done: `bifrost_net::status_line` owns `status_line_code` /
  `status_line_is_success` and both crates call it.
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
- **graph-T1-residual.** (coverage) EWS is the one seam still answering at
  a funnel (`EwsExecute`) rather than at the wire; the REST, aux and
  download surfaces all script through `bifrost_net::test_support`.
  `EwsClient::execute` does post through `AccountNet`, so it is
  migratable. Deliberately left: that double replaced three failed
  review-only rounds and caught four defects, and observing retry on SOAP
  posts does not justify rebuilding a working seam. Revisit if an EWS
  defect is ever traced to retry, backoff, or a redirect.
- **graph-S2-residual.** (smell; the autodiscover half is fixed - it now
  classifies with the real response headers and a scripted 3xx pins it)
  The chunk-PUT `_ =>` arm in `account/cloud.rs` is still reachable only
  by a passed-through 3xx, because bifrost-net returns `Err` for every
  4xx/5xx before the response surfaces. Unpinned, but it does classify
  with the real headers already.

## bifrost-sync

- **sync-F3 (engine-half coverage).** RESOLVED BY ANALYSIS and pinned on
  the producer side (2026-07-31): every producible `ReconcileAdvice`
  carries `CheckTarget`, so the engine's unconditional `PendingReadback`
  is correct rather than an over-reach. `DedupeByClientId` without
  `CheckTarget` cannot be produced - `try_build` always routes through
  `recovery::derive`, and both `Reconcile` arms include `CheckTarget`.
  Pinned by `every_producible_reconcile_requests_check_target` in
  `bifrost-types`.

  What remains, and only if the mutation-loop harness grows for another
  reason: the engine half, driving a real failing non-idempotent mutation
  and observing both the read-back queue and the warning. It would pin a
  behaviour that is correct BECAUSE of the producer invariant already
  pinned, so it earns little on its own. The `_ => {}` wildcard over
  `#[non_exhaustive] ReconcileAction` belongs to sync-N1, not here.
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
    documented explicitly. Leaves the contract alone. DONE.
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

- **xc-3-residual (jmap)** The net-side seam is published
  (`bifrost_net::test_support`) and Graph rides it; consumer migration
  elsewhere is optional and unscheduled, and no other crate carries a
  comparable restatement of net's status contract. What is NOT closed by
  it is `jmap-O2-residual`: jmap pins its own `ReqwestTransport` in a
  type alias rather than riding an `AccountNet`, so it needs the
  transport generic threaded through (or more free-function extraction),
  not a net-side seam.

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

  Item 1 (document the pairing so the flag does not read as a promise the
  engine keeps) is DONE: `reference/sync.md`, under the `reopen`
  paragraph, now states that the flag is advisory to the consumer, that
  nothing in `bifrost-sync` reads it, that the engine will not grow a
  rediscovery timer, and what the rejected alternatives were.

  Remaining work:

  1. Rename `SyncEngine::reopen`. The name undersells the operation and
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

## Workspace sweep: local copies of a contract (2026-07-31)

- **sweep-1 (workspace), TELL 2 ONLY.** The sweep hunts one failure mode:
  **a local restatement of a rule that lives somewhere else, kept alive by
  a test that exercises the copy rather than the original.** The copy and
  its test agree with each other indefinitely; only the original
  disagrees, and nothing asks it. These surface as tests PASSING, which is
  why review rounds keep missing them.

  Tells 1, 3, 4 and 5 are done or ruled. The remaining one:

  2. **Partially-migrated readers.** A producer changed its shape and only
     some consumers followed. The tell is a `match` on an enum where a
     sibling arm handles a case this one silently drops to `_ => None` /
     a default. `resource_from_scope` handled both shapes; the two beside
     it did not, and nothing made that visible. That pair
     (`id_from_scope` / `mailbox_throttle`) made `ThrottleScope::Mailbox`
     unreachable in production while its test kept passing.

  Two method notes worth keeping, both learned the hard way:

  - **Delete the copy, do not read it.** Three of the four calibration
    defects were found by routing through the real thing, not by reading
    either side. Reading the copy tells you what it claims; deleting it
    tells you whether the claim was true. Where a copy cannot be deleted
    outright, route ONE test through the real path and see whether it
    still passes.
  - **Drift needs movement.** This is why tells 1 and 3 came back nearly
    empty: `mirrors` / `same shape as` greps mostly surface copies of
    FROZEN specs, which cannot rot (`types::mime::is_atom_phrase` and
    `smtp::is_valid_phrase` both encode RFC 5322 `atext`, verified
    identical, and the RFC has not moved since 2008). Look where a
    producer changed shape, a seam was promoted, or a contract crossed a
    layer boundary - not where a comment announces an intent to copy.

  Tell 4's residue is the reusable technique: **arm the compiler instead
  of reading.** `#![warn(dead_code)]` now sits on the error-translation
  boundaries (`crates/imap/src/account/error.rs`,
  `crates/jmap/src/sync/error.rs`), where a dead item is a hole in a
  contract rather than unused protocol API. Removing the crate-wide
  `#![allow(dead_code)]` from imap and jmap outright is NOT a cheap win -
  measured at 105 and 195 warnings, overwhelmingly legitimate unused
  protocol surface.

  TELL 2 IS DONE (2026-08-29), and so is the whole of sweep-1. Result: two
  live defects, both FIXED the same day, plus the bookkeeping items below.
  Thin, but not empty. Most cross-crate scope readers already enumerate every
  variant explicitly rather than falling through, and several carry comments
  naming the exact hazard - the sweep found the two that did not.

  The two defects, recorded because both are instructive rather than because
  anything is left to do:

  - `resolve_throttle_key` read only `ErrorScope::Mailbox { id }` while every
    production folder producer builds `Cursor(Folder(_))`, so
    `ThrottleKey::Mailbox` was unreachable in production and a per-folder IMAP
    throttle recorded no deadline at all. This was the ORIGINAL calibration
    defect one layer up: the IMAP-side fix repaired its own readers and left a
    comment naming the hazard, and nobody checked the engine's reader of the
    same identity. Its test passed against the synthetic shape only tests build.
  - Both engine inventory front ends absorbed `InventoryEvent::Warning` one arm
    below the `Terminated` case that forwards, silencing Graph's
    public-folder additions-only degrade and IMAP's QRESYNC downgrade.

  The reusable lesson, which is the same one in both: **fixing a reader is not
  finished until you check the OTHER readers of the same identity.** Both
  defects were a producer shape that had moved, with one consumer following and
  a second left behind - and in both cases a sibling arm in the very same match
  showed the correct treatment.

  The bookkeeping tail is also closed (2026-08-29). `ProtocolSalt::CalDav`
  exists and `default_salt_factory` names it, so the catch-all is reserved for
  protocols that do not exist yet rather than absorbing a live one - pinned by
  `every_live_protocol_has_its_own_salt`, which fails with `Imap` against
  `CalDav` when the arm is removed. The mutation campaign forwards
  `SyncEvent::Warning` instead of dropping it, on the same channel the engine
  already uses for its own campaign warnings. `scopes_for_hint`'s
  `SpecificMembership` arm is annotated as having no production producer, so it
  is not read as evidence that the push path exercises the membership index.
  Only sweep-2e was left, deliberately - see below.

- **sweep-2e (graph). Graph's scope readers return `None` for `Cursor(_)`
  where IMAP's read the folder.** NOT actioned as bookkeeping, and the reason
  is worth keeping: `resource_from_scope` gates the `NotFound` classification,
  so teaching it to read `Cursor(Folder(_))` would silently convert every
  folder-scoped Graph 404 from a generic server error into
  `NotFound(Mailbox)`, carrying a different recovery class. That is a
  behaviour change for consumers wearing the clothes of a diagnostics
  improvement, so it wants deciding on its own terms. The trade is annotated at
  the function. What is actually lost today: a Graph 403 on a shared or public
  folder derives `NoPermission { resource: None }` and names no id in the
  support export, where the same failure on IMAP names the mailbox.

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
  evidence.** Google paginates at 250/page under a page budget with a
  repeated-token guard, and a real account has tens of calendars, so it is
  one page; the finding's scenario needs 250+. The trait already
  distinguishes these cases deliberately - `contacts_list` returns
  `Page<ContactCard>` because contacts number in the tens of thousands,
  and calendars do not. Kept as a do-not-re-file marker only.
- **sync-B4b. Region repair has no live provider.** [C4] The repair path landed
  2026-08-23; see `reference/sync.md`, "Inventory coverage" / "Repair". The
  object lane is implemented end to end and Google implements it. The REGION
  lane is built and tested but no provider produces it: Graph's only region is a
  `CheckpointBarrier` by construction, so `RegionRepairProof::ExactReplay`,
  `Partitioned`, conservation across children, and lineage splitting are
  exercised only by in-crate tests.

  That is not a defect - the shapes are unreachable rather than wrong - but it
  means the region contract is validated against tests rather than a provider,
  and the first real implementor should be reviewed against it rather than
  assumed to fit. A candidate would need a replay token that survives its own
  enumeration, which is exactly what Graph lacks.

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
- **google-B8, smaller observations.** [C3] Four of the six are fixed
  (2026-08-29): the transient-failure `Warning` now counts consecutive
  renewal failures instead of a constant 1; `WatchEvent::Reconnected` is
  edge-triggered off the actor's `disconnected` latch, so a first
  subscribe no longer publishes an event no consumer can be holding a
  receiver for; `client.rs::execute` no longer announces
  `Content-Type: application/json` on bodyless GET and DELETE requests
  (`json` sets it where a body exists); and `mutation.rs::post_empty_json`
  calls `GmailClient::api_url` rather than restating the URL join. The
  `calendar.rs::search` clipped-tail case is documented in place as an
  accepted residual - resume is by calendar id and provider page token but
  not by position WITHIN an over-delivered page, which requires the
  provider to violate its own documented cap.

  What remains: `flags.rs::patch_for_set` names every user label in the
  account in `removeLabelIds`, which on an account with a few hundred
  labels ships a several-KB body per batch. The engine's read-back guard
  already fetches current state, so this is the site that would benefit
  most from read-back-then-diff.
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

## Open items folded in from the second bug-hunt wave (2026-08-29)

The nine `notes/bugs-*.md` ledgers and `notes/carry-forward.md` of the
August 2026 arcs (`bugs-types`, `bugs-net`, `bugs-jmap`, `bugs-smtp`,
`bugs-imap-sasl`, `bugs-dav`, `bugs-sync`, `bugs-graph`, `bugs-google`,
`bugs-cross-cutting`) were closed out and deleted on 2026-08-29, the same
convention as the 2026-08-23 close-out above. Everything durable from them -
machinery invariants, refutations, accepted residuals, testing traps - moved
to `reference/*.md` or to inline comments at the code it describes.
Everything still open is below.

Every one of those arcs closed with no open findings in its own document, so
this list is short by construction: it is the deferred tail, not a defect
backlog. The same category labels and the PUBLISHED SURFACE fence apply.

- **sync-F3 (residual, structural).** `BackfillRunner` and `InventoryFusion`
  share the safety-critical barrier and resume state through `InventoryWalk`
  (`crates/sync/src/inventory_walk.rs`), which is what closed the A2
  divergence. Checkpoint MINTING and terminal-`Done` handling are still two
  implementations, because the shapes genuinely differ - fusion forwards the
  account's checkpoint, backfill mints a positional `page:F:T`. No current
  loss path was found across five rounds and two close passes, so this is a
  re-divergence RISK, not a defect. Close it only if a third inventory front
  end appears, or if a defect is ever traced to the split.

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
  at the `Account` level at all (see `jmap-O2-residual`).

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

- **types-B1c (carddav, smell). Three existing tests assert against a helper,
  not the account.** `carddav_host_attachment_unsupported`,
  `carddav_directory_search_unsupported` and
  `carddav_open_raw_rfc822_unsupported` in `crates/carddav/src/account.rs` call
  `unsupported_future` / `unsupported_stream` directly and assert those helpers
  return what they were told to return. They never call the `Account` method, so
  they pass regardless of what the impl does. A textbook sweep-1 instance that
  survived the sweep-1 tell-3 pass. The new `capability_contract_tests.rs`
  covers the same ground for real, so these are now redundant as well as
  vacuous; deleting them is safe but is a deliberate test deletion, so it wants
  a nod. Worth checking the other five crates for the same shape.

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

- **graph-B1. `pim.rs` and `push.rs` are oversized.** [C3] 4,795 and 2,600
  lines. Low confidence as defects, high as maintenance risk. `pim.rs` holds
  message writes, drafts, send-as, search plus its own versioned cursor codec,
  folder CRUD, identities, vacation, and typed hydration in one file; the
  search cursor logic alone is a self-contained subsystem with its own wire
  format. No round has judged the churn worth it. Same standing as sync-B1 and
  the rest of the refactor backlog: it does not misbehave, and it must not
  become a prerequisite for a local fix.

- **sync-B7. `attach_schema_recovery.rs` still carries its own `HealAccount`
  double.** [C3] The reusable `StubAccount` seam now lives in
  `crates/sync/tests/common/mod.rs` and is the model for new tests. Migrating
  the older test onto it is optional cleanup, explicitly not owed.

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
  into this file (the `*-T1`, `graph-S1`, and `s34-*` items above).
- Completed items are DELETED, not annotated DONE. Their "what" is in the
  code and `reference/*.md`; their "why" is in git history. An item kept
  after it lands is kept only to stop it being re-filed, and says so.
- F-items came from phase 5B/5C/5D re-audits; N-items came from the
  original post-phase-4 audit. Both are intentionally tracked at the
  same level here - none are blocking ratatoskr.
