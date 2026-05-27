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

## bifrost-imap

- **imap-F2.** `pim_malformed` and `envelope::malformed` reach helper
  paths that don't know the op; both currently produce
  `Request(Malformed) -> ClientBug` so recovery class is op-independent
  but operation telemetry is degraded. Thread the operation when other
  pim/envelope refactoring happens.
- **imap-N1.** AUTH leg of `factory::open`: keep
  `AccountOperation::Discover`. AUTH is protocol-level idempotent.
  (Listed as "leave alone" — re-auditor reminder.)
- **imap-N2.** Boundary builders that attach scope: thread
  `Mailbox(folder)` / `Cursor(scope)` per the IMAP plan's table.
- **imap-N3.** `concurrency_conflict_error` / `store_failed_error`:
  take an `AccountOperation` parameter; thread from the caller's
  `MutationKind`.
- **imap-N4.** Delete `MAILBOX_UNAVAILABLE_KINDS` const,
  `let _ = &mut t;`, and the unused `with_thread_id` constructor.
- **imap-N5.** Consolidate `terminated_event` / `fatal_event` into a
  single helper taking `impl Into<AccountError>`.

## bifrost-smtp

- **smtp-N1.** Per-recipient address: attach as
  `DiagnosticText::support_only` on each failed/uncertain lane
  explicitly, not only as shared response text.

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
  re-exported from `lib.rs:85`). Listed as "leave alone" —
  re-auditor reminder.

## Notes

- The error-model design docs (`plans/error-model-*.md`) and the
  phase 4 audit / decisions docs were deleted at the end of phase 5.
  The contract lives in code; this file holds the residual cleanup
  tail.
- F-items came from phase 5B/5C/5D re-audits; N-items came from the
  original post-phase-4 audit. Both are intentionally tracked at the
  same level here — none are blocking ratatoskr.
