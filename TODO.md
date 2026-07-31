# TODO

Open work surviving the close-out of the error-model project. Items
here were either explicitly deferred during phase 5 or are tail
cleanups the audit surfaced and the decisions doc marked "fix per
spec" without scheduling. Verify against current code before working
any item; some may already be obsolete.

## bifrost-jmap

- **jmap-D4.** Generic JMAP `Provider`. Wire `Provider::Fastmail` (and
  any other JMAP host the factory needs) when documented. Continue
  setting `Provider: None` until then.
- **jmap-T2-residual.** (carried context from the closed jmap objects
  ledger, which deliberately left the thin query filter/comparator enums -
  `principal/availability.rs`, `principal/query.rs`,
  `share_notification/query.rs`, `sieve/query.rs`,
  `calendar_event_notification/query.rs`, `quota/query.rs` - untested:
  they are structurally identical to the pinned `email_query_wire`
  tables and a second copy of the same table was judged lower value.)
- **jmap-O2.** The jmap sync layer hardwires `ReqwestTransport`
  (`sync/account.rs` pins `type MailAccount = Account<ReqwestTransport>`),
  so `JmapAccount`'s own `Account` impl surface - `capabilities()`,
  `describe_cursor`, `discover_cursor_scopes`, `establish_initial_cursor`,
  `inventory_partitioning`, the mutation doors - cannot be driven
  hermetically. Closing jmap-T1 covered everything below that boundary;
  the three named casualties are `cursor_scopes()` (seed-filtered
  ordering + foreign sort), the `inventory_partitioning` scope table,
  and `establish_initial_cursor`'s unseeded-scope `Unsupported`. All
  three are pure given their inputs; unblocking needs either the
  transport generic threaded through (the xc-3 discussion) or the
  free-function extraction pattern `route_object_id` already uses.
- **jmap-S1.** (smell, found while closing jmap-T1) `sync/state.rs`
  holds two near-identically named version consts on different axes:
  `CHANGE_CURSOR_ENVELOPE_VERSION = 1` (outer `ChangeCursor` envelope)
  two lines from `ENVELOPE_VERSION_V2 = 2` (inner payload envelope);
  every call site must know which is which. A rename
  (`OUTER_`/`PAYLOAD_`) would remove the footgun. Related nit:
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
- **imap-S2.** (smell, found while closing imap-T1) `Pool::close()`
  awaits `logout()` serially per member with no timeout, and `close.rs`
  adds none, so a hung or silent server blocks `Account::close` for the
  whole pool. Everything else in the account layer goes through
  `command_timeout()`. Candidate: a bounded logout or `join_all` +
  timeout. Related nit: `checkout_for_folder`, `checkout_any`, and
  `dial_idle` carry three byte-identical dial blocks; a private
  `async fn dial(&self)` would collapse them - the duplication is
  exactly where a future meter/cap wiring change gets applied twice and
  forgotten once.
- **imap-B1.** (bug, found while closing imap-T2) `run_qresync`'s
  downgrade retries (`account/changes.rs:271`, `:450`) call
  `run_condstore_with_baseline(...).await` as a tail expression while
  the local `PooledConn` (and its semaphore permit) is still alive; the
  retry's `checkout_for_folder` re-acquires from the same semaphore. At
  `data_cap == 1` that deadlocks; above it, it needlessly dials a second
  connection while holding a usable one (the `MissingCapability` branch
  does not `discard()`). An explicit `drop(conn)` before the retry fixes
  both. This is also why those two downgrade paths could not be pinned
  hermetically in the scripted harness.
- **imap-B2.** (gap, found while closing imap-T2)
  `should_disable_qresync`'s `MissingCapability` arm is unreachable at
  its call site: the only producer of
  `MissingCapability("QRESYNC (not ENABLEd)")` is
  `uid_fetch_vanished_stream`, called behind `?` at
  `account/changes.rs:351-356`, before the loop whose `fallback_error`
  handling performs the downgrade. A server that ACKs `ENABLE QRESYNC`
  without echoing `* ENABLED QRESYNC` terminates the changes stream
  instead of downgrading to CONDSTORE. `select_for_sync` can also return
  `MissingCapability("ENABLE")` (QRESYNC advertised, ENABLE not,
  non-rev2), which `mentions_qresync_capability` does not match, so that
  also terminates. Related nit: `changes.rs:450` passes the original
  `known_uids_complete` alongside `fallback_known_uids`, which is
  complete by construction - the two arguments no longer describe the
  same set.
- **imap-T3.** (audit) `account/error.rs`, crate `error.rs`, and their
  recovery-mapping tests still merit a dedicated audit against
  `reference/error-model.md`. ManageSieve and submission were checked only
  for unsafe text ingress, not full logic. (Carried from the closed imap
  bug-hunt ledger.)
- **types-G1.** (gap, found while closing imap-T4) No cap on total
  decoded encoded-word output in `bifrost-types::mime::words`. Per-word
  expansion is bounded by the 998-byte `ENCODED_WORD_SCAN_LIMIT` window
  (a 100 KB single-word payload never finds its `?=` and is echoed
  verbatim - the original imap-T4 concern was already bounded), but
  chained, individually in-window words have no aggregate cap: ~100 KB
  of Shift_JIS words decodes to ~220 KB (worst realistic factor ~2.2x;
  halfwidth katakana beats base64's 4:3 contraction). The decoder
  inherits whatever cap the caller puts on the raw header. Pinned by
  `chained_encoded_words_expand_past_the_input_with_no_output_cap`.
- **imap-S1.** (smell, found while closing imap-T4) With the STATUS
  terminator set, `skip_tagged_ext_simple`'s atom fallback
  (`codec/decode/envelope_fetch.rs`) does not treat CR as a terminator,
  so it scans through the response-terminating CRLF into the next
  buffered response. Degrades to a parse failure, not a mis-parse, but
  every sibling helper (`skip_balanced_parens`, `skip_paren_group`,
  `skip_fetch_value`) has an explicit CR/LF guard; candidate one-line
  fix is a CR/LF exclusion in the atom arm. Pinned by
  `skip_tagged_ext_simple_status_atom_scans_past_the_response_boundary`.
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

- **smtp-M1.** SMTP raw-socket bandwidth metering is unwired. IMAP
  drives `bifrost_net::MeterSink` + a `bandwidth_cap` through
  `account/{factory,pool}.rs` and `connection/wire.rs`; SMTP has no
  equivalent (zero `MeterSink` references in `crates/smtp/src`).
  Decide: add a metering adapter the raw-socket transport drives, or
  document that bandwidth caps apply only to HTTP- and IMAP-shaped
  accounts. (Carried from the deleted `plans/unification.md` decision
  point 8, which was never stamped resolved.)
- **smtp-N2.** (minor) A pathological non-ASCII display *name* (~500+
  chars) in an address header can still have its RFC 2047 folds flattened
  by the address-header path; the encoded-word fix fully handles Subject.
  Names are near-universally short, so low impact.
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
  (Carried from the deleted `plans/unification.md`
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
- **graph-T1.** (coverage) What the REST/EWS/download/aux test seams
  still do not reach (see `reference/graph.md` for the seams themselves):
  anything whose behavior depends on bifrost-net's own retry, backoff,
  rate-limit permit, or redirect walk. Every seam answers at the funnel,
  below which none of that runs, so a test written on one of them can
  pin WHICH outcome a status produces but never how many attempts,
  how long they waited, or which host a 3xx chain ended on. Closing it
  needs a seam inside bifrost-net, not another one in bifrost-graph.
  (The blob byte streams, the pre-authed OneDrive chunk PUT, the
  Autodiscover POST including its in-body redirect chain, the renewal
  worker's SUCCESS leg across ticks, and the three-mailbox search walk
  with a shared mailbox's own `nextLink` are covered as of the
  download/aux seams.)
- **graph-B1.** (bug/smell, found while closing graph-T1) Byte-stream
  `Batch`es in `account/blob.rs` set `bytes_in: 0`, so blob and
  raw-RFC822 downloads report zero bytes to the engine's metering while
  the EWS attachment path reports the real length - inconsistent
  accounting on the same event type. Related nit: every streamed chunk
  is `PageBoundary::Page`; no chunk is ever `Final`, so a consumer
  keying off the boundary never sees a terminal page for a blob (only
  the trailing `Done`).
- **graph-S2.** (smell, found while closing graph-T1) Two near-dead
  non-2xx branches: the chunk-PUT `_ =>` arm in `account/cloud.rs` and
  the `!status.is_success()` branch in `account/autodiscover.rs` can
  only be reached by a passed-through 3xx, because bifrost-net returns
  `Err` for every 4xx/5xx before the response surfaces. The
  autodiscover one additionally passes `HeaderMap::new()` into
  `GraphResponseError::from_response`, discarding the real response
  headers (`Retry-After`, `WWW-Authenticate`) from classification.

## bifrost-sync

- **sync-F1.** Three-failed-reopen behavior test deferred. The wiring
  is correct (exponential backoff, then `SyncEvent::Terminated` plus
  `AccountControl::Pause(RetryBudgetExhausted)`) but not pinned by a
  focused test because the necessary `Account` / `AccountFactory` stub
  is ~400 lines. Either add the test alongside protocol-level reopen
  exercises, or extract the backoff helper into a sync-internal module
  that can be tested in isolation.
- **sync-F3.** `Reconcile` items requesting `DedupeByClientId` only
  (no `CheckTarget`) still queue `PendingReadback`. Counters surface
  the case and a `Warning::OperatorAttentionNeeded` fires, but the
  read-back guard runs anyway. Per sync-D4 plumbing is correct; revisit
  when a consumer (ratatoskr) starts using
  `MutationCounters::dedupe_by_client_id` to suppress the read-back.
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
  deadline - blocked on the same `Account`/`AccountFactory` stub
  sync-F1 wants; the bucket mechanics are unit-pinned in
  `recovery.rs`.
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
deleted `plans/stage-3-4-review.md`. The fix wave there closed all six
bugs and the tractable gaps; these survive. Labels: **gap** (silent
intent loss or pending design decision), **smell**, **nit**. The
deliberate "accepted fidelity limits" from that doc are documented in
`reference/*.md` and are not tracked here.

Smells:

- **s34-S3 (graph)** `contacts.rs` - `ContactEmail.kind` maps
  to/from Graph `emailAddress.name`, a display name, not a type label.
  Round-trip is consistent (no loss) but semantically conflated.
- **s34-S7 (carddav)** `parse.rs` - `ResponseParts` carries fields
  unused by each of its three parser call sites; shared staging/commit
  logic touches fields irrelevant per site. Mild maintenance tax.

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
- **nc-8 (jmap)** `pim::containers_list` reports `Container::rights` for the
  primary account from `Mailbox/myRights`, but a foreign account's mailboxes go
  through the same `container_from_mailbox`, so a share whose `Mailbox/get`
  omits `myRights` silently projects as unreported rather than as a
  degradation. The `ContainerList::skipped_scopes` lane (which closed nc-1)
  could now carry it, but nothing classifies the omission today.

## Cross-crate items from the bug-hunt loop (2026-07-29)

Surfaced while working the per-crate bug-hunt ledgers (the since-deleted
`plans/bugs-*.md` files) crate by crate. Each of
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
  cancels workers and calls `close`; the engine tells consumers to call
  `unsubscribe_push` themselves. So a consumer that detaches without
  unsubscribing strands live server subscriptions - for Graph, up to 24h, with
  provider-side expiry as the only backstop. This is the documented contract
  rather than a defect, and `bifrost-graph` correctly must NOT add best-effort
  deletion in `close`. The open question is whether the shared contract should
  keep placing that burden on the consumer at all.

- **xc-3 (net + graph, related in jmap)** `bifrost-net` exposes no in-process
  seam for staging a canned HTTP response, so account crates that ride it
  cannot hermetically test any path whose behavior depends on what the server
  returned. `bifrost_net::Response` is `#[non_exhaustive]` with no public
  constructor, and the crate's `Dispatch` / `ScriptedDispatch` are
  crate-private and test-only. Concretely this is why several `bifrost-graph`
  paths are pinned only at the level of extracted pure decision functions,
  with the surrounding request/response sequencing left unpinned and named as
  such at the time: partial webhook-creation rollback, the
  inventory neither-link branch, the unsubscribe DELETE loop as a loop, a
  mixed reaction batch actually reaching `$batch`, and the renewal leg past
  `due_renewals`. `bifrost-jmap` hit the same wall and solved it locally by
  introducing a two-method `PushTransport` trait over the transport it owns,
  which worked precisely because jmap owns that transport - Graph does not.
  The choice is between a Graph-local `GraphTransport` trait (or a
  `#[cfg(test)]` response queue on `ClientInner`), and promoting net's
  existing `Dispatch` seam to a supported test surface that every net-riding
  crate can use. The second is the smaller total amount of code and the larger
  API commitment. Related: `jmap-O2`, the jmap sync layer hardwiring
  `ReqwestTransport`, which is the same testability problem one crate over.

- **xc-4 (sync, maybe app)** Nothing schedules share-rediscovery reopens
  automatically. `AccountCapabilities::reopen_discovers_foreign_namespaces`
  (adjudicated during the jmap round: it means reopen-time discovery
  POTENTIAL - IMAP derives it from NAMESPACE, JMAP is constitutively true)
  tells a consumer that a share granted after open surfaces only through a
  reopen, and `SyncEngine::reopen` is the public staged-reattach entry that
  performs the rediscovery - but no component ever calls it on a cadence.
  The ruling for now is that cadence is consumer policy (nightly, on
  opening the folder list, on user action), so ratatoskr must drive it.
  The open question is whether `bifrost-sync` should grow an optional
  rediscovery interval (`EngineConfig`) that calls `reopen` for accounts
  advertising the flag, so every consumer does not reimplement the timer.

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

- **xc-5 (types + sync, surfaced from the DAV crates)** The engine is blind
  to page-level loss lanes. `bifrost-sync` consumes only the open-time
  `OpenedAccount::skipped_scopes` (surfaced via `open_skipped_scopes`);
  nothing engine-side reads `Page::failed_ids` or a search/range page's
  `Page::skipped_scopes`. For the DAV crates this means a walk that quietly
  loses resources - a vCard that will not parse, a resource refused inside a
  207, a search REPORT leg that met a 401 mid-walk - is visible only to a
  consumer that inspects the returned `Page` directly; an app that routes
  everything through the engine never sees the loss or the recovery class the
  degraded lane was built to preserve. The DAV account layers now populate
  both lanes correctly (2026-07 close pass), so the producer side is done;
  what is open is whether the shared contract should grow engine-side
  accounting for page-lane loss (fold page `skipped_scopes` into the same
  consumer surface as open skips, or at least a counter/warning), or whether
  "page lanes are the app's job" stays the documented ruling. A decision,
  not a bug: nothing is dropped silently at the crate boundary today.

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

- The error-model design docs (`plans/error-model-*.md`) and the
  phase 4 audit / decisions docs were deleted at the end of phase 5.
  The contract lives in code; this file holds the residual cleanup
  tail.
- `plans/orchestration.md`, `plans/unification.md`, and
  `plans/stage-3-4-review.md` were deleted once their phases/stages
  fully merged - same convention as the error-model and Phase 0 plans.
  Their "what" lives in code + `reference/*.md`; their resolved-decision
  "why" lives in git history. Open items they still carried were folded
  into this file (the `*-T1`, `smtp-M1`, `graph-S1`, and `s34-*` items
  above).
- F-items came from phase 5B/5C/5D re-audits; N-items came from the
  original post-phase-4 audit. Both are intentionally tracked at the
  same level here - none are blocking ratatoskr.
