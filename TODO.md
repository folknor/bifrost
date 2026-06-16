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

- **graph-N1.** `remove_from_container` should report
  `Unsupported(RemoveFromContainer)`.
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
- **sync-N1.** `directive_target_scope` and other `_ => None`
  after-exhaustive arms: drop the catch-alls so
  `EngineDirective`'s `#[non_exhaustive]` enforces coverage at compile
  time.
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

- **s34-G1 (imap)** `account/scopes.rs`, `mod.rs`
  (`folder_from_scope`) - composed CardDAV/CalDAV sub-accounts are
  primitive-only: `discover_cursor_scopes` never emits their
  contact/calendar cursor scopes and `folder_from_scope` returns
  `Unsupported` for non-Folder scopes, so contact/calendar *sync*
  dead-ends in the IMAP account. `reference/imap.md` is honest about
  it. Decide: sync-integrated composition, or document primitive-only
  delegation as the contract.
- **s34-G2 (types)** `contact.rs` - `ContactCreate` has `photo_url`
  but no inline `photo` (unlike `ContactCard` / `ContactPatch`), so
  inline-photo contacts require create-then-update. Likely intentional
  (CardDAV/People treat photo upload as a separate step) but
  undocumented. Add the field or document the two-step requirement.

Smells:

- **s34-S1 (caldav)** `ical.rs` - TZID datetimes are read as
  wall-clock digits with a `Z` appended, so `EventTime.value`
  (documented RFC 3339 UTC) holds local time mislabelled as UTC - off
  by the zone offset for any consumer that trusts it. A consequence of
  the VTIMEZONE-stub limit, but the docs frame it only as
  "fixed-offset stubs", not mislabelled instants.
- **s34-S2 (graph)** `calendar.rs` (`event_from_graph`) -
  `recurrence_id` is populated from `seriesMasterId` (master series
  id), but the shared model documents it as RECURRENCE-ID semantics
  (an overridden occurrence). Consumers following the docstring will
  misinterpret it. (Google resolved this correctly via
  `originalStartTime`.)
- **s34-S3 (graph)** `contacts.rs` - `ContactEmail.kind` maps
  to/from Graph `emailAddress.name`, a display name, not a type label.
  Round-trip is consistent (no loss) but semantically conflated.
- **s34-S4 (imap)** `factory.rs`, `capabilities.rs` - capability
  flags key on `sub.is_some()`, never consulting the sub-account's
  `pim_methods`. Correct only because the DAV crates hardcode all
  flags `true`; if either ever conditionally disables a method, IMAP
  over-advertises.
- **s34-S5 (imap)** `factory.rs` - `open_carddav(...).await?` /
  `open_caldav(...).await?` mean a transient DAV outage fails the
  whole IMAP account open, taking mail sync down with it. Decide
  fail-hard vs fail-soft deliberately.
- **s34-S6 (carddav)** `account.rs` - the ctag is encoded into the
  cursor envelope but never compared, so `changes_stream` always does
  the full PROPFIND + diff; the natural "collection unchanged"
  short-circuit is absent.
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
- **s34-N3 (carddav)** `account.rs` - multiget failures (failed
  propstat for a requested href) silently drop the contact from list
  pages; no error, no Destroyed.
- **s34-N4 (carddav)** `account.rs` - an empty multiget result stamps
  `Unsupported(operation)` instead of `NotFound(Contact)`; the HTTP
  404 path maps NotFound correctly, so the taxonomy is inconsistent
  for the same condition.
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
