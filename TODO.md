# TODO

Open work surviving the close-out of the error-model project. Items
here were either explicitly deferred during phase 5 or are tail
cleanups the audit surfaced and the decisions doc marked "fix per
spec" without scheduling. Verify against current code before working
any item; some may already be obsolete.

## bifrost-net

- **net-N1.** `RequestCause::Malformed.detail` is synthetic. Leave the
  body in support-text; accepted as-is.
- **net-N2.** `refresh_failed` loses the underlying body via `Display`.
  Accepted: the non-recursion rule on `source()` is binding.
- **net-N3.** Retry-then-classify ordering. Accepted: final
  classification is correct even if the intermediate shape isn't
  pretty.
- **net-N4.** `throttle_scope` per-provider coverage. Extend as
  providers document their throttle vocabulary; `None` is honest for
  unknowns.

(All four are "accepted, no action" calls. Listed for completeness so
re-auditors don't re-raise them.)

## bifrost-jmap

- **jmap-D4.** Generic JMAP `Provider`. Wire `Provider::Fastmail` (and
  any other JMAP host the factory needs) when documented. Continue
  setting `Provider: None` until then.
- **jmap-N1.** `terminated_unsupported(op, scope, msg)` should take the
  caller's `AccountOperation`. Pagination overflows reclassify as
  `Protocol(ContractViolation)`, not `Unsupported(Discover)`.
- **jmap-N2.** `SetErrorType::Other(String)` should mirror
  `MethodErrorType::Other(String)`. Stop synthesizing `"other"`.
- **jmap-N3.** `resource_from_scope` / `id_from_scope`: drop the
  `_ => None` catch-alls so `ErrorScope`'s `#[non_exhaustive]` enforces
  coverage.
- **jmap-N4.** `capabilities.rs` "core limits zero" path: reclassify
  as `Protocol(ContractViolation)`.
- **jmap-T1.** Account-layer conformance tests (cursor envelope
  round-trip, capability shape, error classification, scope-to-method
  wiring) were deferred during Phase 3 behind in-flight sync-engine
  `InventoryPartition` rework in `crates/jmap/src/sync/`. The other
  three protocols have these; jmap does not. Pick up once that tree
  settles. (Carried from the deleted `plans/orchestration.md` P3-A1.)

## bifrost-imap

- **imap-F2.** `pim_malformed` and `envelope::malformed` reach helper
  paths that don't know the op; both currently produce
  `Request(Malformed) -> ClientBug` so recovery class is op-independent
  but operation telemetry is degraded. Thread the operation when other
  pim/envelope refactoring happens.
- **imap-N1.** AUTH leg of `factory::open`: keep
  `AccountOperation::Discover`. AUTH is protocol-level idempotent.
  (Listed as "leave alone" - re-auditor reminder.)
- **imap-N2.** Boundary builders that attach scope: thread
  `Mailbox(folder)` / `Cursor(scope)` per the IMAP plan's table.
- **imap-N3.** `concurrency_conflict_error` / `store_failed_error`:
  take an `AccountOperation` parameter; thread from the caller's
  `MutationKind`.
- **imap-N4.** Delete `MAILBOX_UNAVAILABLE_KINDS` const,
  `let _ = &mut t;`, and the unused `with_thread_id` constructor.
- **imap-N5.** Consolidate `terminated_event` / `fatal_event` into a
  single helper taking `impl Into<AccountError>`.
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
  Coordinate with **types-G2** so decoded MIME structure is shared
  rather than recreated in the IMAP account. What was done instead:
  the capability now reports `BlobRangeSupport::No` and both openers
  return `Unsupported`, with the private blob id codec and its openers
  deleted; `open_raw_rfc822` (whole-message `BODY.PEEK[]`) is unchanged
  and remains the supported byte path. What remains merely disclosed:
  consumers that want per-attachment streaming from IMAP still cannot
  have it - they now get an honest `Unsupported` instead of a handle
  shape they could not obtain.
- **imap-T1.** Account-layer conformance test additions inside
  `crates/imap/src/account/` modules are still outstanding (the
  reference doc and code landed in P3-A2; the focused tests did not).
  (Carried from the deleted `plans/orchestration.md` P3-A2.)

## bifrost-smtp

- **smtp-N1.** Per-recipient address: attach as
  `DiagnosticText::support_only` on each failed/uncertain lane
  explicitly, not only as shared response text.
- **smtp-M1.** SMTP raw-socket bandwidth metering is unwired. IMAP
  drives `bifrost_net::MeterSink` + a `bandwidth_cap` through
  `account/{factory,pool}.rs` and `connection/wire.rs`; SMTP has no
  equivalent (zero `MeterSink` references in `crates/smtp/src`).
  Decide: add a metering adapter the raw-socket transport drives, or
  document that bandwidth caps apply only to HTTP- and IMAP-shaped
  accounts. (Carried from the deleted `plans/unification.md` decision
  point 8, which was never stamped resolved.)

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

- **gmail-N1.** `inventory.rs:196`: clone `id` before move into
  `hydrate_one`; preserve it in the error scope.
- **gmail-N2.** `recovery.rs:153-160`: attach blob `id` as
  `ErrorScope::Message { id }` (parent message scope) and as a
  diagnostic native code.
- **gmail-N3.** `client.rs:159` unreachable branch: replace with
  `unreachable!()`.
- **gmail-N4.** `terminates_mutation_stream`: fix the comment/code
  mismatch.
- **gmail-N5.** Drop the `shallow_clone` helper; call
  `into_account_error(error, ...)` directly.
- **gmail-N6.** Hoist `dailyLimitExceeded` classification into a single
  helper.
- **gmail-N7.** Rename `account/recovery.rs` to `account/error.rs`.

## bifrost-graph

- **graph-N2.** `wire_or_specific` and trailing `_ => {}` arms: delete
  them; let `#[non_exhaustive]` enforce coverage.
- **graph-N3.** `parse_retry_after_header`: add HTTP-date support.
- **graph-N4.** `ews_stream::scope_for_folder`: switch `try_read` to
  `.read().await`.
- **graph-N5.** `client.rs::execute` unreachable branch:
  `unreachable!()`.
- **graph-N6.** `GraphResponseError::from_response` empty-string
  `Unknown { code: "" }`: emit `WireCause::MalformedResponse`.
- **graph-N8.** Confirm `AccountNet` auto-injects the bearer for
  `execute` and `fetch_blob_stream`; if not, attach explicitly.
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
- **sync-F4.** `ThrottleBucket` is write-only. `apply_throttle`
  (`engine.rs:1740-1757`) records deadlines on `Retry` dispatch but no
  production path reads the bucket via `wait_for(...)` before driving
  work. Tenant- and provider-wide throttles recorded by one scope do
  not pause sibling scopes or sibling accounts. Wire
  `ThrottleBucket::wait_for(key, now)` into the poll loop
  (`multiplexer/mod.rs` per-scope drive) and the push reconciler
  (`push/reconciler.rs::reconcile`). Coupled with sync-F5.
- **sync-F5.** `apply_throttle` only resolves `Account` scope.
  `throttle_key_for(scope, ctx.account_id, None, None, None)` returns
  `None` for `Mailbox` / `Tenant` / `Provider` because
  `RecoveryContext` doesn't carry those identities. Extend
  `RecoveryContext` with `Option<MailboxId>`, tenant, and `Provider`;
  thread them through `apply_throttle`. Doing F5 alone has no
  observable effect because the read side (F4) isn't wired.
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
  to add its arm.
- **sync-N2.** `MutationBucket::BlockedByEngine` vs `FailedTerminal`:
  split into distinct counter fields.
- **sync-N3.** `wait_for_real_subscriber` 25ms hot-poll: switch to
  `Notify`.
- **sync-N4.** `broadcast_warning` scope default: pass directive target
  scope when present; account-default only for genuinely account-wide
  warnings.
- **sync-N5.** `EstablishCursorTerminated` variant: keep; wire to
  `plan_recovery`.
- **sync-N6.** Terminal-arm logging: emit `TelemetryView` structured
  fields rather than `?debug` format.
- **sync-N7.** `ReopenRequest` keeps `#[non_exhaustive]` (it is `pub`,
  re-exported from `lib.rs:85`). Listed as "leave alone" -
  re-auditor reminder.

## bifrost-types

Surfaced while authoring `reference/error-model.md` (a read of
`crates/types/src/error/`). All pre-existing, none blocking.

- **types-N1.** (smell) `AccountErrorBuildError::EmptyChain` is
  effectively unreachable: `AccountErrorBuilder::new` mandates a
  `primary_cause` and `try_build` always pushes it first, so the chain
  is never empty at build. Defensive variant for an invariant the type
  system already guarantees; the "never escapes" framing slightly
  oversells a check that cannot fire. Keep or drop deliberately.
- **types-N2.** (smell) `Transport + Acknowledged` is rejected three
  ways: `try_build` returns `TransportAcknowledged`, and
  `recovery::derive` *additionally* re-checks it with a `debug_assert!`
  (debug panic) plus a release-mode demotion to `InFlight`. Since
  `derive` only runs from inside `try_build` *after* that branch
  already returned `Err`, the `derive`-side check is dead in normal
  flow (reachable only by calling `pub(crate) derive` directly, as the
  tests do). Belt-and-suspenders, but the debug-panic-vs-release-demote
  fork is a real behavior split worth being aware of.
- **types-G1.** (gap) A `throttle_scope` attached to a non-rate/quota
  kind returns `AccountErrorBuildError::KindCauseMismatch { kind,
  primary_cause }` from `try_build`. That misdiagnoses: the kind and
  cause may match perfectly; the actual fault is the throttle scope.
  Add a dedicated `ThrottleScopeNotApplicable` build-error variant so
  the producer is pointed at the right thing.
- **types-G2.** (gap, cross-crate prerequisite) `bifrost-types::mime`
  has no inbound MIME parser: it serializes outgoing RFC 5322 messages
  only. Every account crate that hydrates a full message therefore has
  no shared way to turn fetched octets into decoded text, HTML, and
  attachments. Symptom in bifrost-imap (`account/pim.rs`,
  `attrs_for_hydration` / `fetch_to_message`): full hydration fetches
  `BODY[]` and stores the entire raw wire message, lossy-UTF-8 decoded,
  in `Message::body_text`; `body_html` is always `None` and attachments
  are always empty, so multipart, base64, and quoted-printable messages
  surface wire source instead of content. What bifrost-types would need
  to ship: a parser over raw RFC 5322 octets producing the header set,
  a decoded part tree (content-type, charset, disposition, filename,
  cid), transfer-decoding for base64 and quoted-printable, charset
  decoding to UTF-8, and a text/HTML body selection rule - the inbound
  mirror of the existing outbound serializer, so IMAP, JMAP, Graph, and
  Google all decode identically. What was done locally: nothing beyond
  documenting the defect; the bug is pinned by the bifrost-imap test
  `full_hydration_puts_the_whole_raw_message_in_body_text`. What remains
  wrong: full hydration still returns raw source. A narrower
  `BODY[TEXT]` change would only drop the headers and stay wrong for
  multipart and transfer encodings, so it was deliberately not taken.
  Do not build the parser as a side effect of an IMAP fix; it is a
  shared-crate design item.
- **types-N3.** (nit) `RequestCause::InvalidArgument` has no distinct
  `AccountErrorKind`: `kind_matches_cause` maps it onto
  `Request(Malformed)`, and message-key / recovery treat it
  identically. It is purely a richer cause payload - fine by design,
  but a reader expecting 1:1 cause-to-kind correspondence is briefly
  surprised. Document or accept.

## Stage 3/4 (contacts + calendar) review

Open findings from the contacts/calendar review wave, carried from the
deleted `plans/stage-3-4-review.md`. The fix wave there closed all six
bugs and the tractable gaps; these survive. Labels: **gap** (silent
intent loss or pending design decision), **smell**, **nit**. The
deliberate "accepted fidelity limits" from that doc are documented in
`reference/*.md` and are not tracked here.

Gaps:

- **s34-G2 (types)** `contact.rs` - `ContactCreate` has `photo_url`
  but no inline `photo` (unlike `ContactCard` / `ContactPatch`), so
  inline-photo contacts require create-then-update. Likely intentional
  (CardDAV/People treat photo upload as a separate step) but
  undocumented. Add the field or document the two-step requirement.

Smells:

- **s34-S2 (graph)** `calendar.rs` (`event_from_graph`) -
  `recurrence_id` is populated from `seriesMasterId` (master series
  id), but the shared model documents it as RECURRENCE-ID semantics
  (an overridden occurrence). Consumers following the docstring will
  misinterpret it. (Google resolved this correctly via
  `originalStartTime`.)
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
- **s34-N2 (caldav)** `account.rs` (`event_rsvp`) - the
  organizer-email guard is unreachable on the success path because
  `rsvp_reply_ical` already errors when the organizer is absent.
  Redundant, harmless.
- **s34-N5 (types)** `error/scope.rs` - `EventRsvp` is classified
  non-idempotent; setting a fixed RSVP status is semantically
  idempotent. Consistent with the patch-setter convention, so a
  convention call, not a defect.
- **s34-N6 (types)** `account.rs` - the `Account` trait is not
  `#[non_exhaustive]` despite the (now-deleted) unification plan
  stating it is. Practical impact low (the attribute on traits only
  restricts external impls).
- **s34-N7 (imap)** `mod.rs` - new delegation code uses imported
  `AccountOperation::` while 8 pre-existing sites stay fully qualified
  `bifrost_types::AccountOperation::`. Cosmetic.
- **s34-N8 (jmap)** `calendar_ops.rs` - `jmap_visibility` and
  `rsvp_value` retain the explicit-arm-then-identical-wildcard shape
  cleaned out of the other three mappers; left because the explicit
  arms read as intentional documentation.

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
- **nc-6 (jmap)** Hydration flushes its trailing per-target buffers in
  `HashMap` iteration order, so batch ordering across routing targets is
  nondeterministic. Per-item outcomes are unaffected; only the grouping order
  varies.
- **nc-7 (jmap)** Foreign inventory does not qualify `InventoryEntry::thread_id`,
  and neither does `message_hydrate`'s `qualify_foreign_message_ids` (it
  qualifies `id`, containers, and attachment blob ids, not `thread_id`), so a
  foreign account's thread ids reach the consumer bare. Consequences:
  a foreign thread id collides in the consumer's index with a primary thread
  that happens to share the id; `thread_hydrate` (which is therefore left
  unrouted on purpose - there is no encoded form to route) runs `Thread/get`
  against the primary account for a foreign thread; and, now that the
  mutation primitives route, every thread-taking MUTATION
  (`set_keyword`/`set_is_read`/`set_importance` on `MutationTarget::Thread`,
  `move_thread`, `delete_thread`) does the same. That last one is the sharp
  edge: a bare foreign thread id is indistinguishable from a primary one, so
  on a collision the primary `Thread/get` resolves an UNRELATED thread and
  the mutation is applied to its messages - `delete_thread` destroys them.
  Owner validation cannot catch it, because a bare id asserts primary
  ownership. Qualifying thread ids is a contract change on an id the consumer
  groups by, not a wiring fix, so it wants a decision rather than a patch.
- **nc-8 (jmap)** `pim::containers_list` reports `Container::rights` for the
  primary account from `Mailbox/myRights`, but a foreign account's mailboxes go
  through the same `container_from_mailbox`, so a share whose `Mailbox/get`
  omits `myRights` silently projects as unreported rather than as a
  degradation. The `ContainerList::skipped_scopes` lane (which closed nc-1)
  could now carry it, but nothing classifies the omission today.
- **nc-9 (graph)** `GraphClient::with_account_net` hardcodes
  `rate_limit_host = GRAPH_HOST` instead of deriving it from the supplied
  api-base, unlike every other constructor. A consumer injecting its own
  `AccountNet` against a redirected base therefore meters under the production
  Graph host bucket. Cosmetic today (the injected net owns its own limits) but
  it is an inconsistency waiting to mislead.

## Cross-crate items from the bug-hunt loop (2026-07-29)

Surfaced while working the `plans/bugs-*.md` files crate by crate. Each of
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
  such in `plans/bugs-graph.md`: partial webhook-creation rollback, the
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
