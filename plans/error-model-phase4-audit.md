# Error model phase 4 audit

Post-merge audit of the error-model convergence (commits `e7f35bc` through
`eeaa386`). Eight focused per-crate audits, one per workspace crate, plus
a mechanical sweep in the main thread. The mechanical greps passed; the
semantic audits did not.

Headline: the *shape* landed. The *model* is not wired end-to-end.

Audit scope per crate:
- Builder funnel is the only construction path.
- Central recovery mapping rows are wired.
- `AttemptCause` emitted at every transport boundary with accurate
  `TransmissionState`.
- No `AccountOperation` placeholders at call sites.
- `SyncEvent::Terminated(AccountError)` carries structured errors; no
  per-item failures duplicated as global terminations.
- `BatchOutcome` / `ItemOutcome` shapes correct.
- `DiagnosticText` visibility tagging.
- No orphaned `Cause` variants the mapping ignores.
- Old types / helpers actually deleted (not just unused).
- Lateral bugs and smells.

Findings are labeled `[bug]`, `[gap]`, `[smell]`, `[nit]` per the audit
protocol. Severity grouping at the bottom.

## Mechanical sweep (main thread)

Passed:
- `TransportCause::transmission_state` access: zero hits.
- `TransportCause { ... transmission_state ... }` literal: zero hits.
- `ServerCause::Error { status: 0... }` sentinel: zero hits.
- `ServerCause::Error { status: <bare int> }`: zero hits (all `Some(_)` /
  `None`).
- `SyncEvent::Fatal`: zero hits.
- `RetryDisposition::DoNotRetry` / `OutcomeUncertain`: zero hits.
- `AccountOperation::Mutate`: zero hits.
- `let _ = AccountError` / `let _account_error`: only legitimate test
  panic-assertion sites.
- `MutationResult` / `MutationOutcome` as types: zero hits (one false
  positive, a comment).

Fixed during this audit:
- Removed `set_access_token` from `crates/graph/src/client.rs:162`,
  `crates/jmap/src/client.rs:395`, `crates/jmap/src/sync/factory.rs:234`
  (and orphan `AccessToken` imports). The convergence plan explicitly
  required these removed; rotation goes through the `Arc<dyn TokenSource>`
  the consumer constructs the factory with.
- Removed the stale "helpers that used to live here" doc preamble at
  `crates/graph/src/account/error.rs:1-9`.

## bifrost-types

### Bugs

- **[bug]** `recovery::derive` for `Transport(_)` + `tx_state ==
  Acknowledged` uses `assert!` (not `debug_assert!`) at
  `crates/types/src/error/recovery.rs:206-209`. A misbehaving protocol
  crate pushing `Transport + Attempt(Acknowledged)` crashes the process
  inside the builder rather than degrading. The plan's
  `error-model-types.md:381-383` explicitly required `debug_assert!`
  with a defensive classification. The `builder.rs:184`
  `kind_matches_cause` `assert!` has the same problem.

- **[bug]** `derive_server` does not implement the
  `Server(Error { status: None })` rows for `tx_state == InFlight`.
  `recovery.rs:522-536` falls through to `ProviderRefused` for every
  `InFlight` case regardless of operation idempotency. The plan's
  amendment added these rows as correctness-load-bearing: idempotent
  ops should derive `Retry { SameRequest }`, non-idempotent should
  derive `Reconcile { TransportDropAfterSend }`. Both are silently
  classified terminal today.

- **[bug]** `into_builder` silently drops `idempotency_override`,
  `retry_not_before`, and `throttle_scope`. `from_rebuild` at
  `builder.rs:67-81` resets all three to `None`. The docstring admits
  it. A caller decorating a rate-limited error to add an `Attempt`
  cause loses the original `Retry-After` deadline and throttle scope on
  rebuild, silently changing the derived `RecoveryClass`. Either persist
  these on `AccountError` or re-derive from the chain.

- **[bug]** `BatchOutcome` lane `Vec`s are `pub` and mutable
  (`batch.rs:21-25`). Combined with the internal `order: Vec<BatchLane>`,
  callers can mutate the public Vecs (`outcome.succeeded.push(...)`)
  without going through `push_succeeded`, desyncing `order` and breaking
  `iter()`. Should be private with `&[BatchSuccess<T>]` read accessors.

- **[bug]** `recovery::suggest` returns engine actions
  (`RestartScope(scope)`, `RestartAccount`) as `RemediationAction`
  variants for `CursorInvalid` / `SchemaIncompatible`
  (`recovery.rs:289-294`). The plan defines `RemediationAction` as
  consumer-facing product guidance; engine restarts are not user actions.
  The plan's `Reconcile` example explicitly returns
  `suggested_remediation: None` for analogous engine-driven cases.
  Conflates the engine/consumer split.

- **[bug]** `recovery::derive` for `SyncStateErrorKind::CapabilityChanged`
  falls back to `CapabilityDelta::default()` when the chain has no
  `StateCause::CapabilityChanged { delta }` (`recovery.rs:560-564`). This
  ships exactly the dubious path the convergence plan flagged as an open
  decision (plan lines 1787-1794) instead of the recommended replacement
  (`RestartAccount`).

- **[bug]** `kind_matches_cause` for `(ConcurrencyConflict, Wire(_))`
  returns false. A JMAP `stateMismatch` reaches the builder as
  `kind: ConcurrencyConflict` + `Wire(Jmap(StateMismatch))` only; if the
  producer forgets to also push `State(ConcurrencyConflict)`, the
  release-mode `assert!` panics. The plan requires the producer to push
  `State(ConcurrencyConflict)` outermost, but this is undocumented in
  `builder.rs` as a precondition.

### Gaps

- **[gap]** `derive_protocol` for `ProtocolErrorKind::Unknown` returns
  `UnknownPermanent` unconditionally (`recovery.rs:596`). The plan said
  "`Protocol(_)` (other) | `UnknownPermanent` *unless paired with a
  higher-level retryable cause*." The pairing-walk is not implemented.

- **[gap]** `BatchOutcome` has no `.finalize(expected)` or any check
  that submitted items appear exactly once across the three lanes. The
  "every submitted item exactly once" invariant is convention, not
  contract. A protocol crate that forgets or double-counts an item
  produces a wrong `BatchOutcome` silently.

- **[gap]** `ItemOutcome` is not `#[non_exhaustive]` (`stream.rs:3`),
  inconsistent with the closed-lane stance the plan documents for
  `BatchItemOutcome`. Either both should be closed or both
  `#[non_exhaustive]`.

- **[gap]** No streaming invariants are enforced or pinned by tests in
  `bifrost-types`. The "every pulled item produces exactly one outcome"
  / "locally-invalid items don't poison the stream" rules are
  protocol-crate obligations with no helper, validator, or trait to opt
  into.

### Smells

- **[smell]** `Fatal(pub AccountError)` exposes the inner error as a
  `pub` field (`recovery.rs:170`). Callers can construct
  `Fatal(retryable_error)` directly, bypassing the `TryFrom`
  guarantee. The plan's exit criterion "`Fatal` collapses these at the
  engine boundary via `TryFrom`" is weakened by the public tuple field.

- **[smell]** `CauseChain::root` (`cause.rs:32-36`) uses `.last()
  .expect("...")` but `CauseChain::new` only `debug_assert!`s
  non-empty. Same in `into_builder`'s `causes.remove(0)`
  (`account_error.rs:170`): debug/release divergence on an empty chain.

- **[smell]** The four mutual-exclusivity helpers (`is_retryable` etc.)
  are implemented as `matches!`, with no test asserting exactly one
  returns true for every constructible variant. Adding a future variant
  requires manually keeping the `is_terminal` negation correct.

- **[smell]** All three support exports (`support_minimal`,
  `support_consented`, `support_internal`) are unconditionally public
  on `AccountError`. The plan describes them as "gated by consent
  tier" but at the type level any caller can invoke
  `support_internal()`. Consent gating is the consumer's
  responsibility; no consent-token scaffolding.

- **[smell]** `recovery::derive` ignores `idempotency_override` for
  paths that do not call `transient_retry_or_reconcile`. For
  `AuthErrorKind::RefreshTransient` (`recovery.rs:447-453`), the
  override is silently dropped.

- **[smell]** `TelemetryView::transmission_state`
  (`account_error.rs:231-237`) returns `None` when no `Attempt` cause
  is present, while the recovery classifier collapses absence to
  `Unsent`. Documented behavior matches; flagged because future
  contributors might assume parity.

- **[smell]** `AccountError::source()` returns the outermost `Cause`
  cast as `&dyn StdError`. Every concrete `*Cause` type implements
  `StdError` with `source() = None`, so standard walkers see the
  Display of `Cause` rather than the specific payload
  (e.g. `JmapMethod::StateMismatch`). Likely intentional; worth
  confirming.

- **[smell]** `EnhancedStatusCode::code: u16` is ambiguous; the
  canonical SMTP enhanced status is `X.Y.Z` and the dotted form lives
  in a separate `enhanced: Option<DiagnosticText>` field. Name it
  `class` or `reply`.

- **[smell]** `ServerCause::status` and `WireCause::status` are
  `pub(crate)` / private; downstream tooling cannot render
  "retry in N seconds" hints without going through `support_internal`.
  Observability ergonomic gap.

### Nits

- **[nit]** `AccessErrorKind::MailboxUnavailable { kind:
  MailboxUnavailableKind }` (`kind.rs:51`) is a struct variant with a
  nested enum payload; matching reads `MailboxUnavailable { kind:
  MailboxUnavailableKind::Transient }`. Consistent with the plan but
  visually noisy.

- **[nit]** `Warning` and `WarningKind` (`warning.rs:3, 54`) are not
  `Eq, PartialEq`, blocking dedup in warning queues and test
  comparisons. Other fields are trivially comparable.

- **[nit]** `recovery::suggest` for `MailboxUnavailable { Transient }`
  calls `retry_later(recovery)` (`recovery.rs:280-282`), which only
  returns `Some` if `recovery` is `Retry(_)`. Always true today, but
  inflexible.

## bifrost-net

Cleanest crate in the audit; exit criteria satisfied. Two smells, no
bugs.

### Smells

- **[smell]** `RequestCause::Malformed.detail` is set to a synthetic
  `"HTTP {code} malformed request"` at `account_error.rs:585-587`
  instead of the provider-supplied reason; the body text is shunted to
  a separate support-text diagnostic. Downstream support exports still
  get the body, but the structured `detail` slot loses fidelity.

- **[smell]** `refresh_failed` (`account_error.rs:223-225`) projects
  source errors via `Display`. For a wrapped `Error::Status { body, .. }`,
  Display is `"HTTP {code}"` and the OAuth endpoint body (often
  carrying `error="invalid_grant"`) is lost. Token-endpoint 401/403
  cases are saved by `arc_err_to_error` upstream; 429/503 retry-
  transient cases lose body context.

- **[smell]** Three legacy variants (`EncodeBody`, `InvalidHeader`,
  `NetSetup`) carry both `message: String` and a `Box<dyn Error>`
  source. The conversion sets both, and current call-site `message`
  fields already include the source's text. Source text duplicated in
  support exports. Low impact.

- **[smell]** `request.rs:443-447` retries network errors blindly
  without classifying them first. A TLS handshake misconfiguration
  burns the full retry budget before surfacing as `Tls`. Policy
  question, not an error-model violation.

- **[smell]** `request.rs:732-771` `send_error_to_error` checks
  `e.is_builder()` redundantly with `request.rs:437`.

- **[smell]** `throttle_scope` (`account_error.rs:639-650`) only sets
  `Tenant` for Graph/Microsoft and `Account` for Gmail/Fastmail-JMAP.
  Non-Fastmail JMAP and other providers get `None`. By design per the
  plan.

### Nits

- **[nit]** `Error::InvalidRequest.field: &'static str` forces string
  literals (`error.rs:212`). The type-system enforcement is good; the
  shape just looks restrictive.

- **[nit]** `maybe_support_text_from_option` exists for two callers;
  could inline as `value.and_then(maybe_support_text)`.

- **[nit]** `invalid_argument_builder` (`account_error.rs:402-414`)
  clones the `Option<DiagnosticText>` for double-use; wastes one clone
  of a potentially-large diagnostic.

- **[nit]** `support_cause_from_source` (`account_error.rs:653-707`)
  groups several variants in a final `|`-arm; `Error` is
  `#[non_exhaustive]` so adding a new variant won't fail to compile.
  Explicit list is more honest about intent.

## bifrost-jmap

### Bugs

- **[bug]** WebSocket pre-handshake errors classify wrong.
  `tokio_websockets::Error` raised inside `Client::connect_ws`
  (`crates/jmap/src/client_ws.rs:168`) flows through
  `Error::WebSocket` and into `websocket_runtime_error`, which returns
  `Protocol(PartialResponse)` + `Attempt(Acknowledged)`. Pre-handshake
  failures must classify as `Transport(Network)` + `Attempt(Unsent)`
  per `error-model-jmap.md:511-530`. The current shape happens to dodge
  the `Transport + Acknowledged` assertion only by misclassifying as
  Protocol, which then yields wrong recovery (`Reconcile` for non-
  idempotent, `Retry` for idempotent; correct is plain
  `Retry::SameRequest, reason: Transport`). The crate's `Error` enum
  has no Pre/Post-handshake distinction; either a new variant or an
  out-of-band marker is required.

- **[bug]** Push WebSocket reader drops every error on the floor
  (`crates/jmap/src/sync/push.rs:204-246`). `Err(_) => break` at line
  232; `Err(_) => let _ = tx.send(WatchEvent::Disconnected)` at lines
  238-240. The plan (`error-model-jmap.md:611-630`) required the
  reader to construct an `AccountError` via
  `JmapErrorContext::new(PushStream)` and emit it through the push
  stream. Today consumers learn nothing beyond `Disconnected`;
  auth-lost / server-gone / schema-mismatch mid-stream are erased.

- **[bug]** `terminated_unsupported(msg)` hard-codes
  `AccountOperation::Discover` for every caller
  (`crates/jmap/src/sync/error.rs:225-246`). Almost no caller is in a
  discovery path:
  - `inventory.rs:25-37, 54-58` (op should be `SyncInventory`).
  - `inventory.rs:143-157, 178-191` (i32/usize overflow during
    pagination; kind is wrong too: protocol-shape overflows are
    `Protocol(ContractViolation)` or `Request(Malformed)`, not
    `Unsupported(Discover)`).
  - `hydrate.rs:25-30` (op `Hydrate`).
  - `blob.rs:52-68` (op `OpenBlobRange`).
  Worse, the kind itself (`Unsupported(Discover)`) tells consumers
  "the JMAP account does not implement discovery," which is false.
  Per the convergence rule "Unsupported -> roadmap or fallback path;
  never a 'try again' UX", consumer routing will mis-route.

### Gaps

- **[gap]** `scope_lifecycle` loop swallows errors with `Err(_) =>
  sleep` (`crates/jmap/src/sync/discover.rs:188-190`). Auth-lost or
  capability-changed conditions keep retrying silently every 5 minutes
  instead of escalating.

- **[gap]** No `ConcurrencyConflict` item-level surface in the
  streaming mutation pipeline. `mutation.rs:136-147` retries
  `stateMismatch` once, then returns the raw `crate::Error` up to
  `apply_batch`, which emits `SyncEvent::Terminated(AccountError)` for
  the whole batch. `reference/jmap.md:246` claims an
  `ItemOutcome::Failed` with `AccountErrorKind::ConcurrencyConflict`
  is emitted. Doc and code disagree.

### Smells

- **[smell]** `crates/jmap/src/sync/account.rs:158-168`
  `establish_initial_cursor` builds the unsupported-seed error inline
  with `AccountErrorBuilder::new(...)` instead of calling the existing
  `super::error::unsupported_error(EstablishCursor, Some(scope), ...)`
  helper. Two divergent construction paths for the same outcome shape;
  risk of drift.

- **[smell]** `crates/jmap/src/sync/error.rs:812-816` in
  `convert_method` the `MethodErrorType::Other(code)` arm builds both
  the primary `Cause::Wire(...Unknown { code: code.clone() })` AND a
  separate `wire: JmapMethod::Unknown { code: code.clone() }`.
  `primary_was_wire` skips the second push. Dead computation, two
  clones for one usage.

- **[smell]** `resource_from_scope` and `id_from_scope`
  (`sync/error.rs:474-486, 488-501`) both have explicit handling for
  every `ErrorScope` variant *plus* a trailing `_ => None`. Defeats
  `ErrorScope`'s `#[non_exhaustive]` discipline. A new scope variant
  silently returns `None` instead of failing to compile.

- **[smell]** `capabilities.rs:10-23` `missing_core_capability` uses
  `AccountOperation::Discover` (legitimate for session-load context)
  but kind `SyncState(CapabilityChanged)` produces
  `EngineDirective::CapabilityChanged { delta: default }`. The "core
  limits were zero" case (line 48-53) is a `ProviderContractViolation`
  (server claimed conformance and lied), not a capability change. The
  recovery class drifts because the kind drifts.

- **[smell]** `sync/error.rs:225-241` `unsupported_error` builds via
  `AccountErrorBuilder::new(...)` directly rather than the
  `build_with(&ctx, ...)` helper. `provider` cannot be attached if/when
  wired for generic JMAP hosts.

- **[smell]** `sync/error.rs:373-376` `SetErrorType::Other` synthesizes
  `JmapMethod::Unknown { code: "other".to_string() }`. The plan is
  explicit (`error-model-jmap.md:316-318`): do not synthesize
  placeholder codes; pollutes `native_code()` with a value the server
  never sent. The companion fix for `MethodErrorType::Other(String)`
  was taken; the parallel fix for `SetErrorType::Other` was not.

### Nits

- **[nit]** `sync/error.rs:118` `Error::IdNotFound(id)` no-scope
  fallback message could be more diagnostic ("JMAP response omitted
  requested id" rather than "id not found in response: {id}").

- **[nit]** `sync/error.rs:629` `builder = builder.status(Some(status))`
  could be `builder.status(status_u16)` without the local `if let`.

- **[nit]** `sync/pim.rs:23-36` `schema_incompatible_search_cursor`
  position between `to_acct_err` and the `use` block is visually
  confusing.

## bifrost-imap

### Bugs

- **[bug]** Tagged `NO` / `BAD` responses never carry
  `AttemptCause(Acknowledged)`. `crate::Error::No` and `Error::Bad`
  have no `attempt` field (`error.rs:60-72`). `Error::with_attempt`
  only patches variants that have one and silently no-ops on
  `No`/`Bad` (`error.rs:299-314`). `connection/driver/mod.rs:594` does
  `consumer.finalize_erased(..).map_err(|e|
  e.with_attempt(TransmissionState::Acknowledged))`; when the consumer
  produces `Error::No`/`Error::Bad` the `with_attempt` is a no-op.
  `account/error.rs::classify_status` (line 430) never sets
  `t.attempt`. Recovery rows that depend on `Acknowledged`
  (e.g. `Server(Error { status: None }) + Acknowledged ->
  ProviderRefused`) collapse to the wrong arm. Only auth compensates
  (`classify_auth` hard-codes `Some(Acknowledged)`, error.rs:420).

- **[bug]** `account/inventory.rs:62, 106, 132, 136, 139` turn dropped
  output-channel sends into `crate::Error::closed()`, which funnels
  through `fatal_event` to emit `SyncEvent::Terminated`. Plan
  forbids: "If the output receiver is dropped, stop the task and
  return `Ok(())`. Do not emit a fatal event for output-channel
  closure." Same pattern in `account/changes.rs:345, 365, 411, 415,
  605, 609, 774, 790`, `account/get.rs:107, 114`, `account/blob.rs:164`.
  User-visible damage is contained (receiver is gone), but the
  synthesized error has the wrong kind and the rule was meant to keep
  consumer-dropped streams out of recovery telemetry.

- **[bug]** `account/mutate.rs::mutation_stream` emits trailing global
  `SyncEvent::Terminated` after per-item outcomes for prior folders
  (`mutate.rs:88-105`). On the first failing folder, `fatal_event(err,
  ...)` is sent and the function returns. Earlier folders' per-item
  outcomes have already been emitted. Phase 3 correctness blocker #7
  forbids exactly this. Fix: guarantee no per-item events before the
  first folder failure, or emit per-item `ItemOutcome::Uncertain` for
  the failing folder.

### Gaps

- **[gap]** `account/factory.rs::open` uses `AccountOperation::Discover`
  for every leg (connect, server_id, qresync, list). Defensible for
  ID/QRESYNC/LIST; a stretch for the AUTH leg. An InFlight transport
  drop during AUTH derives `Retry::SameRequest` instead of `Reconcile`.
  Authentication is genuinely idempotent at the protocol level, so this
  works today but conflates four distinct phases.

- **[gap]** Many account-boundary builders omit `.operation(...)`:
  `envelope.rs::malformed` (414-423), `pim.rs::pim_malformed`
  (1275-1284), `mutate.rs::uidvalidity_changed_error` (526-535),
  `mutate.rs::store_failed_error` (539-550),
  `mutate.rs::concurrency_conflict_error` (515-523). The central
  mapping can't choose the right `RetryDisposition` for non-idempotent
  ops without operation. `AccountError::operation()` returns `None`.

- **[gap]** `mutate.rs::concurrency_conflict_error` and
  `store_failed_error` hardcode `UpdateFlags` regardless of the calling
  `MutationKind`. `mutation_results` (453-496) is called from
  flag-mutation, move, and destroy paths but always tags `UpdateFlags`
  via the helper. `BulkDestroy` STORE Deleted hitting MODIFIED claims
  `operation = UpdateFlags`; mis-routes telemetry.

- **[gap]** `account/mutate.rs::mutation_results` UNCHANGEDSINCE
  conflict emits no `MutationSuccess::Skipped`. IMAP's MODSEQ cache
  is opportunistic and cannot observe "already in state" without a
  full SELECT+FETCH; documented in the IMAP plan as legitimate
  divergence. Surfacing for the audit record.

- **[gap]** `account/get.rs:33-40` discards a structured `AccountError`
  into a free-form `Warning` string when `decode_object_id` fails.
  `get_stream` returns `SyncEvent<HydratedObject>` (not
  `SyncEvent<ItemOutcome<HydratedObject>>`), so the trait doesn't carry
  a `Failed` lane. Per-item ambiguity is unobservable to the engine.

- **[gap]** `account/inventory.rs::inventory_stream` and
  `mutate.rs::fatal_event` do not attach scope. `inventory.rs:33-40`
  needs `Cursor(scope)`; `mutate.rs:95-99` needs `Mailbox(folder)`.
  Same in `changes.rs:70-76`, `blob.rs:44-51`, `get.rs:46-53`.

- **[gap]** `account/error.rs::uidvalidity_changed` / `modseq_reset`
  hardcode `AccountOperation::SyncChanges`. Current call sites are
  limited to changes; helper signature is missing the op parameter.

- **[gap]** `EngineDirective::DowngradeStrategy` is never derived at
  runtime; the `strategy_failure` helper is unused outside tests.
  `changes.rs` handles every QRESYNC -> CONDSTORE downgrade via
  `Warning::StrategyDowngraded` and continues. Likely intended design
  (every strategy falls back to Basic), in which case the helper is
  dead code reserved for "all strategies exhausted."

### Smells

- **[smell]** `account/error.rs::classify` for `Error::DriverPanicked`
  builds `Translation::new(...)` ignoring the `attempt` field and
  relies on `into_account_error` re-reading `error.attempt()`. Works
  but inconsistent with `Auth`/`FetchLimit` which set `t.attempt`
  explicitly.

- **[smell]** `MAILBOX_UNAVAILABLE_KINDS` const (`error.rs:933`) is
  dead code with `#[allow(dead_code)]` and a "reserved for future"
  comment.

- **[smell]** `account/error.rs::classify_response_code` ends with
  `let _ = &mut t;` (line 751) marked "Unused capture." No-op from a
  refactor.

- **[smell]** `account/changes.rs::ChangeError::Imap` carries
  `crate::Error` while the boundary is `AccountError` everywhere else.
  Same in `blob.rs` (`BlobError::Account`/`Imap`). Double-conversion
  smell.

- **[smell]** `account/error.rs::ImapErrorContext::operation`
  constructor name shadows the field name. Cosmetic;
  `for_operation` or `new` reads better.

- **[smell]** `factory.rs:91-104` four `.map_err(discover_err)?` in a
  row. Each leg has different attempt-state semantics (connect, AUTH,
  LIST) but all map through one helper. Lost granularity.

- **[smell]** `error_tests.rs` test fixtures default to
  `AccountOperation::Discover` for response-code classifier tests.
  Defensible (`Discover` is conveniently idempotent), but a few tests
  would be more meaningful pinned to their real op.

### Nits

- **[nit]** `account/error.rs::with_thread_id` constructor exists but
  is never called.

- **[nit]** `account/mod.rs::terminated_event` and `fatal_event` are
  near-duplicates; could consolidate into one helper taking
  `impl Into<AccountError>`.

- **[nit]** `account/error.rs::response_code_payload` swallows several
  parameterized payloads (`UnknownCte`, `MetadataNoPrivate`) under
  `_ => return None`.

## bifrost-smtp

### Bugs

- **[bug]** LMTP `DATA`-command failure is misclassified `Unsent` and
  returns batch-level `Err` after RCPT acceptances.
  `crates/smtp/src/transport/smtp/client/connection.rs:752-756` (sync)
  and `async_connection.rs:834-837` (async) do:
  ```rust
  if let Err(e) = self.command(Data) {
      self.abort();
      return Err((e.with_attempt(SmtpTransmissionState::Unsent), progress));
  }
  ```
  Two problems:
  1. **Wrong attempt state.** `command(Data)` writes `DATA\r\n` and
     reads the response. On a negative reply (503, 554) the server
     received and responded; that is `Acknowledged`, not `Unsent`.
     Even a true transport drop should be `InFlight`
     (`plans/error-model-smtp.md:283-284`).
  2. **Wrong outcome shape.** After RCPTs were accepted, collapsing
     per-recipient outcomes into a batch-level `Err` is forbidden by
     `error-model-convergence.md:69-71`. The non-pipelined SMTP path at
     `connection.rs:494-518` does this correctly: a negative DATA reply
     marks accepted recipients failed and returns `Ok(progress)`. LMTP
     bypasses via `self.command(Data)` instead of
     `self.command_accepting_status(Data)`.

  This is the SMTP-specific gate-4 correctness blocker the audit was
  asked to verify. A non-idempotent `Send` whose LMTP DATA was
  acknowledged-negative is reported as `Transport(Network) +
  Attempt(Unsent)`, classified `Retry::SameRequest`. The engine
  resends the same message after the server rejected it, to *every*
  recipient (batch-level `Err` discards per-recipient lanes).

  Fix: use `command_accepting_status(Data)`, route negative replies
  through `mark_accepted_rejected_with_response`, transport drops
  through `mark_accepted_uncertain` with attempt `InFlight`.

- **[bug]** AUTH `InvalidInput` (no compatible mechanism) misclassifies
  as `Request(Malformed)`. `connection.rs:1246-1248` constructs
  `error::invalid_input("No compatible authentication mechanism was
  found")`. Per `error-model-smtp.md:973-976`, this should be
  `Unsupported(Send)` (if credentials required) or
  `Authorization(PolicyBlocked)`. The generic `ErrorKind::InvalidInput`
  arm maps everything to `Request(Malformed)` with no phase-awareness.
  `SmtpCommandPhase` has no `Auth` variant. Result: AUTH mechanism
  mismatches surface as "malformed request" -> `ClientBug` (internal
  telemetry) instead of reauth/policy-change UX.

### Gaps

- **[gap]** `message_error_to_account_error` is missing entirely.
  `error-model-smtp.md:339-343` and the exit criteria
  (`1095-1126`) require an entry point so account-oriented callers can
  map message-builder errors (`MissingFrom`, `MissingTo`, `TooManyFrom`,
  `EmailMissingAt`, etc.) into `AccountError`. Today the batch helpers
  (`transport.rs:349`/`async_transport.rs:389`) accept already-encoded
  bytes plus `BatchItem<Address>`, sidestepping `crate::error::Error`.
  Any future `Account` impl that builds a `Message` will invent its
  own mapping, defeating the single-translation-boundary rule.

- **[gap]** `SmtpCommandPhase` is incomplete. Plan
  (`error-model-smtp.md:227-244`) lists 15 phases: `Connect`,
  `Greeting`, `Hello`, `StartTls`, `Auth`, `MailFrom`, `RcptTo`,
  `DataCommand`, `DataBody`, `BdatBody`, `LmtpFinalStatus`, `Noop`,
  `Vrfy`, `Expn`, `Rset`. Shipped enum (`error.rs:67-72`) has only
  four. MAIL FROM `with_attempt` calls in `connection.rs:429, 442,
  570, 578, 584` cannot tag a phase. AUTH-phase errors cannot be
  refined. Connect/Greeting/Hello/StartTls drops cannot be
  distinguished from in-send drops.

### Smells

- **[smell]** `SmtpAttempt`'s `Inner.attempt` field is on `Error::Inner`
  but the plan-specified `phase: Option<SmtpCommandPhase>`
  (`error-model-smtp.md:215-220`) was never added.  Phase always
  passes through `SmtpErrorContext::with_phase`. Easy to miss; a
  missed call silently degrades to "no phase".

- **[smell]** DATA-final-negative reply uses `with_phase(DataBody)`
  even though it is a final-reply classification (`batch.rs:182-187`).
  No `DataFinal` phase. The classifier doesn't currently care, but
  the conflation will bite any future phase-aware rule.

- **[smell]** `with_recipient_text` is a no-op despite the plan
  calling for the recipient address in support-only diagnostics
  (`batch.rs:239-247`, plan line 794). For DATA-final-negative fanned
  out to N accepted recipients, every failed lane shares the same
  response text; the per-recipient address is not in there.

- **[smell]** `SendProgress::resolve` falls into
  `partial_completion_error` for LMTP `Accepted` without `Final`
  (`batch.rs:177-193`). For LMTP this is a programming error
  (acknowledged at line 176); the defensive fallback silently
  downgrades it to "uncertain lane".

- **[smell]** `LmtpFinalStatus` phase tag is passed to the classifier
  but the classifier ignores it (`account_error.rs:319` only checks
  `RcptTo`). The plan justified the phase enum partly for LMTP
  routing; lever unused.

### Nits

- **[nit]** `BdatBody` listed in the plan's enum is absent and BDAT
  batch is silently not implemented. Only test is `502 command not
  implemented` (`batch.rs:571-594`).

- **[nit]** `partial_completion_error` formats the recipient address
  into a `support_only` `DiagnosticText` twice (`batch.rs:249-269`).

### Summary

Gate-4 question (the per-phase wiring for non-idempotent `Send`) is
**closed for SMTP** and **open for LMTP**. A server that responds
negatively to LMTP `DATA` after RCPT acceptances produces a
batch-level `Err` tagged `Transport(Network) + Unsent`, classified
`Retry::SameRequest`. The engine can resend the whole message.

## bifrost-gmail

### Bugs

- **[bug]** `account/inventory.rs:196` `get_stream` hydration failure
  calls `GmailErrorContext::hydrate_message("")` with an empty id. The
  id is moved into `hydrate_one` (line 191) before the error branch
  runs. `ErrorScope::Message { id }` is empty in support exports for
  every hydration failure on this stream. Fix: clone `id` before, or
  return `(ObjectId, Error)` on failure.

- **[bug]** `account/recovery.rs:153-160` `GmailErrorContext::open_blob(id)`
  accepts `id: impl Into<String>` then drops it (`let _ = id;`). Three
  call sites pass real blob ids (`blobs.rs:154, 158` and the
  `OpenBlobRange` context). Scope info collected and silently thrown
  away.

- **[bug]** `account/recovery.rs:738-758` `not_found_kind_cause` for
  `GmailResource::Account`/`Draft`/`Identity`/`Vacation`/`Blob`/`PubSubWatch`
  has `to_resource_kind()` returning `None`, then the fallback
  hard-codes `ResourceKind::Message`. A 404 on a draft surfaces as
  `NotFound(Message)`. Consumers routing on `NotFound(ResourceKind)`
  send missing drafts to the message-not-found UX.

- **[bug]** `client.rs:159` `Error::invalid_request(AccountOperation::Discover,
  "unsupported HTTP method: ...")` uses `Discover` as a placeholder.
  Branch is unreachable in practice (all internal callers pick
  GET/POST/PUT/PATCH/DELETE). `ClientBug`-class regardless of op so
  recovery unaffected, but the tag is wrong. Low severity (dead
  branch).

### Gaps

- **[gap]** `AttemptCause` / `TransmissionState` are constructed only
  by `bifrost-net`; Gmail does not push an `Attempt` cause on
  locally-constructed `AccountError`s. Correct for `Local(_)` and
  `JsonDecode`/`Base64` after 200, but for `Error::Response` (HTTP
  error reaching the boundary) the response was acknowledged. The
  central mapping's "absence treated as `Unsent`" fallback happens to
  give the right answer today for current rows (idempotent ops same
  verdict; non-idempotent + 503 currently yields `Retry::SameRequest`
  which matches the `Acknowledged` row). If a future row distinguishes
  acknowledged-vs-unsent for non-idempotent ops, Gmail will silently
  classify wrong.

- **[gap]** `account/push.rs:264-271` Pub/Sub renewer task catches
  `Err(error)` and emits only `WatchEvent::Disconnected`/`Reconnected`,
  logging `tracing::warn!("gmail Pub/Sub watch renewal failed: {error}")`.
  Plan: "Convert renewal failures to structured `AccountError` for
  tracing and optional stored health state." Subscription-deleted /
  expired signals should trigger a fresh `users.watch`; current code
  sleeps `RENEW_RETRY_AFTER` and retries the same call. Auth/authz
  failures log identically to transient 5xx. Engine never sees a
  terminal `SyncEvent::Terminated(AccountError)`. `push_stream`
  yields `Disconnected`/`Reconnected` indefinitely.

- **[gap]** `account/push.rs:188-189, 193` `push_unsubscribe` decodes
  the handle envelope but only stops the watch when active-handle set
  empties. A syntactically valid but unknown handle, passed right
  after open, triggers `stop_watch` even though the handle never
  appeared.

- **[gap]** `account/changes.rs:235-237` `let _ =
  AccountOperation::SyncChanges;` is a dead stub reference.

- **[gap]** `account/recovery.rs:1206-1211` test hand-builds
  `bifrost_net::Error::Network { transmission_state: Unsent }`. No
  test pushes `InFlight` and verifies `GmailErrorContext::send()`
  flipping recovery from `Retry` to `Reconcile`. The
  `send_uses_non_idempotent_context` test uses HTTP 500 response (no
  `AttemptCause`). Non-idempotent reconcile path is untested
  end-to-end. Would have surfaced the missing `AttemptCause` on
  `Response`.

### Smells

- **[smell]** `account/recovery.rs:380-386` `terminates_mutation_stream`
  excludes both `NotFound(_)` and `Server(Error { .. })` from
  terminating, with the comment "only NotFound is a clean per-id lane;
  everything else also fans out per-id but is still terminal." The
  code does the opposite. Comment stale or branch incorrect.

- **[smell]** `account/mutation.rs:251-268` `shallow_clone` exists
  because `GmailError` is non-Clone and the TRASH-fallback path needs
  to translate the primary failure twice. After reading the
  surrounding code, the original `error` is not used after
  `shallow_clone(&error)`, so `into_account_error(error, ...)` would
  work directly. Refactor opportunity, not a bug.

- **[smell]** `account/recovery.rs:489-538` `translate_response` builds
  `ServerCause::{QuotaExhausted,RateLimited,Unavailable}` with
  `retry_after: None` in the cause regardless. Retry-after `Duration`
  reaches `AccountError` only via the builder `retry_not_before`
  side-channel. If the central mapping reads `retry_after` off the
  `Cause::Server(_)` (the convergence doc shows `not_before:
  retry_after` in several rows), Gmail's retry hint is silently
  dropped.

- **[smell]** `account/recovery.rs:773-790` `throttle_scope_for` checks
  `signal: GmailSignal::Unknown { code } if code == "dailyLimitExceeded"`.
  The same string is also classified as `Server(QuotaExhausted)` at
  line 647. Two places to keep in sync.

- **[smell]** `account/recovery.rs:632` `InsufficientScope { needed:
  "gmail.modify" }` hardcoded regardless of operation. A
  `ContainerCreate` failing for lack of scope actually needs
  `gmail.labels`. Consumer-facing `needed` string is wrong for
  non-`gmail.modify` operations.

- **[smell]** `account/recovery.rs:589-591` `failedPrecondition`
  outside the history endpoint maps to `Request(Malformed)`. Plan
  allows context-dependent routing; hardcoded malformed silently
  misclassifies server-side precondition failures as client bugs.

### Nits

- **[nit]** `account/recovery.rs:1` module doc says "recovery" but the
  file is the translation boundary; plan suggested rename to
  `account/error.rs`.

- **[nit]** `account/changes.rs:9` imports `AccountOperation` used
  only in the dead `let _ = AccountOperation::SyncChanges;`.

- **[nit]** `account/mod.rs:316-365` three trait stubs (`set_keyword`,
  `set_category`, `set_extended_property`) call directly through
  `into_account_error(Error::unsupported(op), ...)` rather than the
  in-module PIM helper.

## bifrost-graph

### Bugs

- **[bug]** `idempotency_override(false)` is never called anywhere in
  the Graph crate. `GraphErrorContext` has no `idempotency_override`
  field (`account/graph_error.rs:30-34`); `base_builder` does not set
  one. Exit criterion (`error-model-graph.md:1083-1089`) explicitly
  required it on every non-idempotent write call site: `Send`,
  `DraftCreate`, `DraftUpdate`, `DraftSend`, `BulkMove`,
  `AddToContainer`, `RemoveFromContainer`, `AttachmentUpload`,
  `ContainerCreate`, `ContainerRename`, `ContainerMove`,
  `ContainerDelete`, `IdentityUpdate`, `VacationSet`. In-flight
  transport drops on `Send`/`BulkMove`/draft mutations fall back to
  the default and silently derive wrong recovery (the exact gate-4
  hazard).

- **[bug]** `account/mod.rs:322-326` `remove_from_container` returns
  `unsupported_account_error(AccountOperation::BulkMove)`. Consumers
  receive `Unsupported(BulkMove)` when they called
  `remove_from_container`. Should be
  `AccountOperation::RemoveFromContainer`.

- **[bug]** `client.rs::execute` (lines 254-296) never sets
  `Authorization: Bearer <token>` on outgoing requests. Only
  `Content-Type`. `bifrost-net`'s `AccountNet` builder may inject
  automatically; flagging for cross-check, but if not every Graph
  request is anonymous.

- **[bug]** `account/blob.rs:200-219` `fetch_blob_stream` uses
  `download_stream(url, range)` without an `Authorization` header for
  raw `$value` byte streams. Same caveat as above. If `AccountNet::
  download_stream` does not auto-inject auth, every attachment
  download is anonymous and will 401.

- **[bug]** EWS path is still `Result<_, String>` end to end (deferred
  from Phase 2.2, not closed in Phase 3). `ews/client.rs:17-39` returns
  `Result<String, EwsError>` where `EwsError` wraps strings
  (`EwsError::Transport(String)`, `EwsError::SoapFault { message:
  String }`, `ews/mod.rs:17-25`). `account/ews_stream.rs:284-292, 102,
  186` keep `Result<_, String>` boundaries. The EWS streaming worker
  (`ews_stream.rs:59-98`) only logs `tracing::warn!` and emits
  `WatchEvent::Disconnected`; no `AccountError` is built, no
  `Protocol::Ews` is stamped anywhere. Reference and plan claim
  otherwise. Phase 2.2 exit gate "All EWS fallback errors stamp
  `Provider::Microsoft` and `Protocol::Ews`" is unmet.

- **[bug]** `webhooks.rs:36-108` still returns `Result<_, String>` and
  flattens `GraphError` into `format!("{error:?}")` via
  `graph_error_text` (line 116). `account/push.rs::subscribe_graph`
  (76-88) re-wraps the string into `Protocol(ContractViolation)` via
  `transport_string_error`, discarding the original `GraphSignal`,
  status, headers, retry-after, request id, and inner-error evidence.
  A 401/403/429/503 during `push_subscribe` classifies as
  `Protocol(ContractViolation)` -> `ProviderContractViolation`
  (terminal) instead of `AuthLost`/`NeedsPolicyChange`/`Retry`. The
  canonical "loss of structured evidence before the account boundary"
  defect. Same path for `push_unsubscribe` and the `getrandom` error.

- **[bug]** Webhook renewal failures never produce structured
  telemetry. `account/push.rs:203-209` is `tracing::warn!(...)` only.
  Plan (`error-model-graph.md:879-901`) required conversion for
  telemetry/health without changing `WatchEvent`'s public shape. 401/403/429
  distinction lost.

- **[bug]** `mutate.rs::refresh_missing_etags` (227-243) constructs
  `unsupported_account_error(operation_for_kind(kind))` for a Graph
  response missing an etag. Plan says this is `Protocol(MissingField)`
  (`error-model-graph.md:830`). A `Warning` built immediately after is
  bound to `let _warning = ...` and never yielded; dead code (236-243).

- **[bug]** `pim.rs::submit_write_batch` 4xx/5xx item path does not
  route through `into_account_error`. `pim.rs:705-722` builds
  `ServerCause::Error { status }` directly with bare
  `AccountErrorBuilder::new` without `AttemptCause`, without
  `WireCause::Graph`, without parsing the item body for `GraphSignal`.
  Every PIM 429/503/401/403/412 becomes flat `Server(Error { status })`
  -> `ProviderRefused` (terminal). Should call the same `mutation_item_outcome`
  routine `mutate.rs` uses.

- **[bug]** `pim::submit_write_batch` always tags
  `AccountOperation::UpdateFlags` (`pim.rs:685`), regardless of whether
  the caller is patching flags, moving, or destroying. Callers
  `move_messages` and `destroy_messages` (`pim.rs:612-663`) report
  their failures as `UpdateFlags`.

- **[bug]** `pim::pim_protocol_error` hardcodes
  `AccountOperation::Hydrate` (`pim.rs:755`). Invoked from
  `object_id_from_value` and the move/destroy etag-missing path
  (`pim.rs:622-624`). A missing changeKey during `BulkMove`/`BulkDestroy`
  reports operation `Hydrate`. Phase 3 correctness blocker #3
  explicitly names this kind of placeholder as a gate item.

- **[bug]** `account/get.rs:107-113` per-item 4xx/5xx is silently
  demoted to a `Warning` (`Warning::support_only(WarningKind::Other,
  "Graph get for {} failed with HTTP {}")`) and the hydrated object is
  dropped. Plan (`error-model-graph.md:710-716`) required per-item
  failures to surface as `ItemOutcome::Failed`. Today the stream emits
  one less hydrated object than requested, with only a warning. No
  classification, no `NotFound`/`AuthLost` distinction. Violates the
  streaming invariant "every pulled item produces exactly one
  outcome".

### Gaps

- **[gap]** `Protocol::Ews` end-to-end (see EWS bug above). No
  `into_account_error` path stamps `Protocol::Ews`.

- **[gap]** `GraphErrorContext::ews(...)` constructor promised by the
  plan (`error-model-graph.md:431-434`) but `graph_error.rs` only
  exposes `::graph(...)` (line 38). Without it and EWS-specific SOAP-fault
  translation, the EWS classification table
  (`error-model-graph.md:929-958`) is dead-letter.

- **[gap]** `push_subscribe` does not honor "fail if any requested
  scope is unsupported" (`error-model-graph.md:867-872`). `push.rs:97-103`
  silently drops scopes whose `resource_for_scope` returns `None`, then
  proceeds with whatever resolved. Returns empty-success when zero
  scopes resolved.

- **[gap]** `mutation_item_outcome` only handles per-item status;
  `BatchResponseItem` headers reconstructed best-effort
  (`mutate.rs:153-167`). Headers that fail to parse silently produce
  no `not_before` deadline on a per-item 429.

- **[gap]** `Authorization(MailboxUnavailable { Transient })` does
  not pass through `apply_retry_deadline` (`graph_error.rs:391-403`),
  so a `MailboxStoreUnavailable` with `Retry-After` drops the
  deadline.

- **[gap]** Cursor scope absence on 410 Gone is silently fallen
  through to `Server(Error { status: 410 })` (`graph_error.rs:281-285`).
  Plan: "missing scope is a producer bug surfaced in the audit." No
  log, trace, or `debug_assert!`. Likewise `InvalidDeltaToken` /
  `SyncStateNotFound` at 300-306 map to `SyncState(CursorInvalid)`
  even without a cursor scope, then derive `Engine(RestartAccount)`.

- **[gap]** `account/inventory.rs:64-66` skips removed entries during
  the initial walk. Documented in reference, but the asymmetry vs.
  the changes-stream behavior (which surfaces removed as
  `ScopeChange::Removed`) could bite consumers.

- **[gap]** No `WireCause::MalformedResponse { protocol: Protocol::Ews,
  .. }` anywhere. Plan exit gate required it for EWS XML parse
  failures (`error-model-graph.md:1091-1093`). EWS parsers return
  `String` errors that get logged and discarded.

- **[gap]** `into_account_error` does not delegate the
  `bifrost_net::Error::RateLimited { final_response, .. }` body
  re-parse described in `error-model-graph.md:464-481`.
  `GraphError::Net(net)` dispatches straight to
  `bifrost_net::into_account_error` (`graph_error.rs:68`) with no
  Graph-side body inspection. Microsoft `error.code` discrimination
  lost for net-classified rate-limit failures.

### Smells

- **[smell]** `wire_or_specific` (`graph_error.rs:167-176`) is dead
  code. Every classify path returns a non-`Wire` cause. The
  subsequent `if !matches!(cause, Cause::Wire(_))` (line 128) ALWAYS
  pushes `WireCause::Graph(signal)`, even when `signal ==
  GraphSignal::Unknown { code: "" }` (empty body fallback,
  `error.rs:124-130`). Wire cause carrying a meaningless empty code
  on every status-fallback error.

- **[smell]** `GraphErrorContext::to_net_ctx` does not pass
  `idempotency_override` because `GraphErrorContext` has no such
  field. Compounds the idempotency bug.

- **[smell]** `classify` end (`graph_error.rs:307-309`) has both
  `GraphSignal::Unknown { .. } => {}` and a wildcard `_ => {}`. The
  wildcard is unreachable today but masks future additions; a new
  typed variant silently falls through to status mapping. Defeats the
  point of the typed enum.

- **[smell]** `body_diagnostic` (`graph_error.rs:488-496`) is
  uncapped. `STATUS_BODY_CAP` mentioned in plan
  (`error-model-graph.md:472`) but no cap enforced; multi-megabyte
  error bodies become multi-megabyte `DiagnosticText`.

- **[smell]** `parse_retry_after_header` (`graph_error.rs:498-505`)
  only handles integer seconds; ignores HTTP-date `Retry-After`
  values that Microsoft Graph documents as legal.

- **[smell]** `transport_string_error` in `account/push.rs:76-88` is a
  stringly-typed escape hatch. Its name and signature should not
  exist after Phase 2.2.

- **[smell]** `pim_protocol_error` (`pim.rs:747-759`) has no
  caller-supplied scope; a missing-changeKey for a specific message id
  doesn't surface in `ErrorScope::Message { id }`.

- **[smell]** `ews_stream::scope_for_folder` (402-414) uses
  `try_read()`; if a write lock is held, scope falls back to a
  synthetic email folder scope. Race-condition-driven
  misclassification.

- **[smell]** `client.rs::execute` `_ => return Err(... "Unsupported
  HTTP method")` (280-286) is unreachable from `pub(crate)` callers
  but builds a `bifrost_net::Error::Network { transmission_state:
  Unsent }`. A genuine `ClientBug` masquerading as transport.

- **[smell]** `error.rs:104-141` `GraphResponseError::from_response`
  constructs `GraphSignal::Unknown { code: String::new() }` for
  unparseable bodies. Empty-string sentinel flows through `classify`
  into `WireCause::Graph(Unknown { code: "" })`. Distinguishing "no
  envelope" from "envelope with empty code" is lost.

### Nits

- **[nit]** `graph_error.rs:308-309` catch-all `_ => {}` after
  exhaustive arms; remove for exhaustiveness diagnostics.

- **[nit]** `graph_error.rs:524-525, 544-545` `Some(_) | None => None`
  reads as `_ => None`.

- **[nit]** `reference/graph.md:62-67` still lists
  `graph_error_to_fatal`, `recovery_for_graph_error`,
  `mutation_outcome_for_status` as functions in `error.rs`. Gone.

- **[nit]** `reference/graph.md:421-428` describes
  `mutation_outcome_for_status` as the `$batch` projector; actual
  code uses `mutation_item_outcome`.

- **[nit]** `reference/graph.md:64-67` lists `account/error.rs` as
  containing classification helpers; today it contains only
  `warning_blob_not_byte_stream`.

- **[nit]** `mutate.rs:236-243` `_warning` discarded.

- **[nit]** `graph_error.rs::throttle_scope_for(_ctx)` always returns
  `Some(Tenant)`. Either drop the context parameter or implement
  per-mailbox refinement (plan 644-648).

- **[nit]** No negative test that `Server(Error { status: 0, .. })` is
  never constructed; would lock the no-sentinel rule.

### Cross-cutting

Phase 4 commit `eeaa386` updated `reference/graph.md` to describe a
converged state that is **not** actually achieved for the EWS
subsystem, the webhook subsystem, the PIM write batch, and the
get-stream per-item path. The reference is ahead of the code in
these four areas. `idempotency_override` is a Phase 3 exit criterion
missed entirely; latent correctness bug for `Send`/`BulkMove`/draft
mutations under in-flight transport drops.

## bifrost-sync

### Bugs

- **[bug]** `RecoveryClass` dispatch matches variants directly with a
  `_` arm, silently routing future non-terminal variants to
  "terminal":
  - `crates/sync/src/engine.rs:1513-1556` (handle_account_error)
  - `crates/sync/src/multiplexer/mod.rs:611-666`
  - `crates/sync/src/push/reconciler.rs:113-149`
  - `crates/sync/src/engine.rs:879-905` (`bulk_set_flags`)
  - `crates/sync/src/engine.rs:1937-1963` (`classify_item_outcome`)

  `RecoveryClass` is `#[non_exhaustive]`. Plan prescribed dispatch
  through `is_retryable`/`requires_reconciliation`/`requires_engine_action`/
  `is_terminal` or the `plan_recovery` helper with `Fatal::try_from`
  collapse so drift fails loudly. Neither is used. `rg
  "is_retryable|requires_reconciliation|requires_engine_action|is_terminal"
  crates/sync/src` returns zero hits. `rg
  "plan_recovery|RecoveryPlan|SurfaceTerminal"` also zero. The plan-
  recommended `RecoveryPlan` enum was never written.

- **[bug]** `Fatal::try_from(&error)` is never called anywhere in
  `crates/sync/`. Exit criterion "terminal account errors convert
  through `Fatal::try_from`" unmet. The terminal arm in
  `handle_account_error` (`engine.rs:1545-1555`) only logs; the
  comment at 1543 acknowledges the conversion-then-skip. The roundtrip
  safety test `plan_recovery_terminal_round_trip` named in the plan
  does not exist.

- **[bug]** `RetryAdvice::throttle_scope` is ignored. `rg
  "throttle_scope|ThrottleScope" crates/sync/src` returns zero hits.
  `retry_delay` consults only `not_before` and `min_delay`
  (`recovery.rs:24-33`). No tenant pause path, no bucket, no
  observability hook. A Graph tenant-wide throttle sleeps the one
  scope and goes back to hammering the tenant from every other scope
  and account.

- **[bug]** `bulk_set_flags` treats `RecoveryClass::Engine(_)`
  stream termination as a fatal campaign error instead of routing.
  `engine.rs:901-903`:
  ```rust
  RecoveryClass::Engine(_) => {
      return Err(Error::Account(err));
  }
  ```
  Plan: "engine directive: stop the campaign and route the directive."
  Returning to caller without `ReopenRequest::Recovery` means
  `RestartScope`/`SchemaIncompatible`/`CapabilityChanged`/`RestartAccount`
  raised mid-campaign never reach `handle_engine_directive`. Caller
  just sees `Error::Account`.

- **[bug]** `RestartAccount` failure is log-and-swallow.
  `engine.rs:1789-1809` `restart_account`: when `factory.open` fails,
  `tracing::warn!` and return. Slot left running against stale handle;
  directive consumed. No retry, no escalation, no
  `ReopenRequest` re-enqueue. A persistently failing reopen silently
  leaves the account dead.

- **[bug]** `restart_scope` failure during `SchemaIncompatible`
  recovery: same pattern. `engine.rs:1689-1707`: scope re-establishment
  failure -> loop continues to the next scope. Originating error
  gone, scope without cursor, no `ReopenRequest` for it. Wedged until
  external poke.

- **[bug]** `classify_item_outcome` inner `_` arm misclassifies any
  future `RetryDisposition` as needing read-back
  (`engine.rs:1938-1952`). `RetryDisposition` is `#[non_exhaustive]`.

- **[bug]** Mutation campaign read-back counter underflow.
  `engine.rs:929-948`: items in `retry_set` after `max_retries` are
  pushed to `readback_ids` AND set to `PendingRetry`. Then
  `totals.pending_retry.saturating_sub(outcome.skipped).saturating_sub(outcome.still_failed)`.
  If read-back saw items not in the `PendingRetry` bucket
  (`PendingReadback` from the stream loop, then resolved as
  `still_failed`), the subtract drains `pending_retry` for items it
  didn't represent. `PendingReadback` items never get their counter
  shifted to `failed_terminal`/`skipped` after read-back resolves;
  remain `pending` forever in reported counters.

- **[bug]** Push reconciler ignores recovery class on
  `WatchEvent::Disconnected` (`push/reconciler.rs:60-65`). Every
  disconnect synthesizes a generic "push transport disconnected"
  warning. `WatchEvent` carries no `AccountError` (because protocol
  crates erase it; see Gmail/JMAP bugs above), so there is nothing to
  consult. The reconciler doesn't pause its loop, doesn't raise
  `OperatorOverrideRequired`, doesn't interact with poll cadence.
  Push permanently dead vs push hiccup look identical to the engine.

### Gaps

- **[gap]** `EngineDirective` match in `handle_engine_directive` has
  `_ => warn!("unknown engine directive")` (`engine.rs:1725-1732`).
  Necessary for `#[non_exhaustive]` compilation but a new variant logs
  and is otherwise silently ignored. No metric, no surfacing, no
  escalation. Same `_ => Some(scope)` in `directive_target_scope`
  (`recovery.rs:38-48`) silently classes new scope-bound directives as
  account-wide.

- **[gap]** `RecoveryClass::Retry(_)` arriving through the reopen
  listener is silently honored without re-driving the affected scope
  (`engine.rs:1514-1520`). Sleep then return. No worker re-poll. In
  practice no current worker sends `Retry` through this listener, but
  the path is structurally broken.

- **[gap]** `RecoveryClass::Reconcile(_)` in `handle_account_error` is
  log-and-return (`engine.rs:1521-1532`). Reconcile-via-poll
  (`multiplexer/mod.rs:624-633` and `push/reconciler.rs:122-131`) just
  sleeps 1s and loops. No actual `CheckTarget`/`DedupeByClientId`
  probe. `ReconcileAction` is never read; `rg "ReconcileAction"` zero
  hits. Read-stream reconcile collapses to "sleep 1s and re-poll" with
  no probe. The mutation-stream read-back path
  (`engine.rs:1939`) correctly handles `Retry::AfterStateRefresh`
  -> read-back; that part works.

- **[gap]** `EngineDirective::CapabilityChanged { delta }` only emits
  a warning and reopens the account (`engine.rs:1619-1635`); does not
  re-run `discover_cursor_scopes`/`discover_memberships`/`push_subscribe`.
  Engine's view of which scopes exist remains the attach-time
  snapshot. Gained scope -> never spawned; lost subscription -> handle
  never torn down.

- **[gap]** `OperatorOverrideRequired` warns but does not pause the
  account (`engine.rs:1709-1724`). Plan: "Do not reopen automatically.
  Surface the original account error through the account stream so the
  consumer can pause or alert." Warning broadcast (good) but the
  engine keeps driving and presumably keeps failing.

- **[gap]** `discover_memberships` terminal error logged and
  discarded (`engine.rs:1422-1431`). Loop breaks,
  `link_discovered_memberships` returns `Ok(())`. Push reconciler then
  routes hints to "every registered scope" because the membership
  index is empty (`scopes_for_hint` returns `all_scopes()` for
  `Unknown`). A terminal auth error during membership discovery
  silently degrades push routing for the slot's lifetime.

- **[gap]** Reference and plan claim `Fatal` is "available at engine
  boundaries that specifically need 'the engine has nothing more to
  try'" but no boundary uses it. Engine boundaries that need
  terminal-only collapse (`OperatorOverrideRequired` alert surface,
  terminal `bulk_set_flags`, terminal multiplexer endings) none use
  `Fatal::try_from`. Remove from engine surface or wire to the surface
  it was created for.

- **[gap]** `EstablishCursorTerminated(AccountError)` is constructed
  only in `run_establish`'s inventory fallback
  (`engine.rs:1861-1863`); never matched in any caller. `rg
  "EstablishCursorTerminated"` returns definition + construction only.
  When `run_establish` returns it from `SchemaIncompatible`
  re-establishment, the caller logs the formatted error and continues.
  Structured `AccountError` preserved on the wire but no one reads it.

### Smells

- **[smell]** `ReopenRequest` has one variant (`Recovery`); the
  dispatcher (`engine.rs:518-533`) uses `match req { Recovery { ... }
  => ... }` with no other arms. `#[non_exhaustive]` future variants
  fail to compile here (good), but the `_ => {}` pattern used
  elsewhere in the file would hide it. Inconsistency.

- **[smell]** `directive_target_scope` has exhaustive enumeration and
  `_ => None` (`recovery.rs:38-48`). Plan called this exact pattern
  out: account-wide directives must pass `None`, and silently dropping
  a scope for a scope-bound directive is the inverse mistake.

- **[smell]** `MutationBucket::BlockedByEngine` and
  `MutationBucket::FailedTerminal` both record into `record_failed`
  (`engine.rs:1981-1983`). Plan distinguishes "campaign blocked by
  engine directive" from "item terminally failed"; counter collapses
  them. Telemetry pivoting on terminal-vs-blocked impossible.

- **[smell]** `wait_for_real_subscriber` polls every 25 ms
  (`engine.rs:1388-1403`) instead of using `Notify` or
  `broadcast::Sender::sender_count` events. Hot-spinning for a
  one-shot wait.

- **[smell]** `engine.rs:1953` discards `ReconcileAdvice`:
  `RecoveryClass::Reconcile(_) => { readback_ids.push(...); ... }`.
  Advice's `actions` (CheckTarget vs DedupeByClientId) lost. Same in
  `push/reconciler.rs:122`, `multiplexer/mod.rs:624`,
  `bulk_set_flags` (`engine.rs:884`). Dedupe-by-client-id should at
  minimum be recorded for telemetry.

- **[smell]** `broadcast_warning` defaults the scope to
  `CursorScope::Account` when none is supplied
  (`engine.rs:1816-1822`), even for warnings about specific scope-bound
  directives like `OperatorOverrideRequired` and `CapabilityChanged`
  where `fallback_scope` is the multiplexer worker's notion, not the
  directive's. Produces a `MultiplexerEvent` whose `scope: Account`
  may be wrong for a per-scope subscriber filter.

- **[smell]** `recovery.rs::directive_target_scope` is `#[must_use]
  pub(crate)`; the four-way match is open-coded in five places rather
  than centralized through the helper.

- **[smell]** `cursor/envelope.rs` `decode_envelope` returns
  `Error::SchemaIncompatible` but never builds the corresponding
  `AccountError` with `SyncStateErrorKind::SchemaIncompatible`. `rg
  "SyncStateErrorKind::SchemaIncompatible|StateCause::SchemaIncompatible"
  crates/sync/src` returns zero hits. The only path that processes an
  on-disk schema mismatch is `attach` (`engine.rs:996` via
  `get_change_cursor`), which propagates the engine `Error` straight
  back to consumer. The directive dispatch in `handle_engine_directive`
  works for directives coming *in* through `ReopenRequest::Recovery`,
  but nothing in sync ever *creates* a `SchemaIncompatible` directive
  from a cursor decode failure. The "envelope decode failure ->
  engine directive -> schema clear" loop is broken.

- **[smell]** The reopen listener's terminal-arm logging
  (`engine.rs:1545-1555`) does not tag with telemetry-friendly
  structured fields the way `TelemetryView` would; just
  `?debug`-formats `error.recovery()`.

### Nits

- **[nit]** `error.rs:8` doc references `Fatal` as engine-vocabulary;
  type is `bifrost_types::Fatal` only.

- **[nit]** `engine.rs:286` comment "ends with a recoverable Fatal" -
  vocabulary moved on.

- **[nit]** `lib.rs:53` comment "the engine-local `FatalAction` enum
  is gone" is historical noise.

- **[nit]** `recovery.rs:38-48` `_ => None` after exhaustive list is
  redundant; suppresses non-exhaustive warning instead of
  `#[allow]` with comment.

## Severity grouping

### P0 (silent wrong behavior; must fix)

Double-send / mis-retry hazards:
- smtp LMTP DATA misclassification (`connection.rs:752-756`,
  `async_connection.rs:834-837`).
- graph: `idempotency_override(false)` never called
  (`graph_error.rs:30-34`).
- gmail: no `AttemptCause` on HTTP `Response` errors.
- imap: tagged NO/BAD lacks `AttemptCause(Acknowledged)`
  (`error.rs:60-72, 299-314`).

Structured errors flattened to strings:
- graph webhooks `Result<_, String>` (`webhooks.rs:36-108`,
  `push.rs:76-88`).
- graph EWS end-to-end `Result<_, String>` (`ews/client.rs:17-39`,
  `ews_stream.rs`).
- graph PIM `$batch` items bypass `into_account_error`
  (`pim.rs:705-722`).
- graph `get_stream` per-item failures dropped to Warning
  (`get.rs:107-113`).

Engine drops directives / can't escalate:
- sync `bulk_set_flags` drops `Engine(_)` directives
  (`engine.rs:901-903`).
- sync `RecoveryClass` `_ => terminal` dispatch in 5 sites; helpers
  never called.
- sync `Fatal::try_from` never called.
- sync `throttle_scope`/`ReconcileAction` zero hits.
- sync `restart_account` / `SchemaIncompatible` re-establish failures
  log-and-swallow.
- sync cursor envelope decode never produces
  `SyncState(SchemaIncompatible)` `AccountError`.
- sync `OperatorOverrideRequired` doesn't pause.
- sync `EngineDirective::CapabilityChanged` doesn't re-run scope /
  membership / push discovery.
- jmap push WebSocket reader drops every error (`push.rs:204-246`).
- gmail Pub/Sub renewer only `warn!`s (`push.rs:264-271`).

Type-system gaps in `bifrost-types`:
- `assert!` in `recovery::derive` and `kind_matches_cause`.
- `Server(Error{status:None}) + InFlight` rows unimplemented
  (`recovery.rs:522-536`).
- `into_builder` drops idempotency override / retry deadline /
  throttle scope (`builder.rs:67-81`).
- `BatchOutcome` lane Vecs `pub`-mutable (`batch.rs:21-25`).

### P1 (wrong telemetry / wrong UX routing; recovery technically right)

- graph: `remove_from_container` reports `Unsupported(BulkMove)`
  (`mod.rs:322-326`).
- graph: `pim::submit_write_batch` tags `UpdateFlags`;
  `pim_protocol_error` tags `Hydrate`.
- graph: webhook renewal failures lack structured telemetry
  (`push.rs:203-209`).
- graph: 410/`InvalidDeltaToken`/`SyncStateNotFound` without cursor
  scope silently derive `RestartAccount`.
- gmail: `not_found_kind_cause` coerces every non-message resource to
  `ResourceKind::Message` (`recovery.rs:738-758`).
- gmail: `InsufficientScope::needed` hardcoded to `gmail.modify`
  (`recovery.rs:632`).
- gmail: `inventory.rs:196` and `recovery.rs:153-160` discard message
  id / blob id.
- jmap: `terminated_unsupported` hardcodes `Discover` for inventory /
  hydrate / blob; uses `Unsupported(Discover)` kind for
  protocol-shape overflows (wrong kind).
- jmap: `SetErrorType::Other` synthesizes
  `JmapMethod::Unknown { code: "other" }` placeholder
  (`sync/error.rs:373-376`).
- jmap: `scope_lifecycle` swallows errors (`discover.rs:188-190`).
- jmap: `capabilities.rs:10-23` mis-kinds "core limits zero" as
  `CapabilityChanged` instead of `ProviderContractViolation`.
- imap: `concurrency_conflict_error`/`store_failed_error` hardcode
  `UpdateFlags` for all callers.
- imap: `factory.rs::open` uses `Discover` for AUTH leg.
- imap: many account-boundary builders omit `.operation(...)`.
- imap: `inventory.rs`/`mutate.rs`/`changes.rs`/`blob.rs`/`get.rs`
  fatal events don't attach scope.
- imap: dropped-output-channel sends synthesize transport errors
  (`inventory.rs:62`, `changes.rs:345`, etc.).
- imap: `mutate.rs` emits trailing global `Terminated` after per-item
  outcomes for prior folders (Phase 3 blocker #7 violation).
- smtp: AUTH `InvalidInput` -> `Request(Malformed)` ->
  `ClientBug` instead of policy/reauth UX.
- smtp: missing `message_error_to_account_error` boundary.
- smtp: `SmtpCommandPhase` enum incomplete (4 of 15 phases).
- gmail: `terminates_mutation_stream` comment vs code disagree
  (`recovery.rs:380-386`).
- gmail: retry-after lives on builder side-channel, not in
  `ServerCause` (`recovery.rs:489-538`).
- gmail: `failedPrecondition` -> `Request(Malformed)` hardcoded.
- types: engine actions returned as `RemediationAction`
  (`recovery.rs:289-294`).
- types: `derive_protocol` Unknown ignores "higher-level retryable
  cause" pairing.
- types: `EngineDirective::CapabilityChanged { delta:
  default }` ships the path the plan flagged.

### P2 (smells / consistency / defensive coding)

Selected; not exhaustive. See per-crate sections above for all
`[smell]` and `[nit]` entries.

- types: `Fatal(pub AccountError)` field bypass.
- types: `BatchOutcome` no `.finalize(expected)`.
- types: `ItemOutcome` not `#[non_exhaustive]`.
- types: `CauseChain` non-emptiness debug/release divergence.
- types: 4-helper mutual-exclusivity not pinned by test.
- net: malformed-body source duplicated in support exports.
- net: `RequestCause::Malformed.detail` synthetic, body in
  separate support-text.
- net: refresh OAuth body lost via `Display`.
- jmap: divergent construction paths (`account.rs:158-168` vs
  `unsupported_error` helper).
- jmap: `_ => None` catch-alls defeating `ErrorScope`
  `#[non_exhaustive]` (`sync/error.rs:474-501`).
- imap: `ImapErrorContext::operation` constructor-name shadows field.
- imap: `ChangeError::Imap`/`BlobError::Imap` double-conversion.
- smtp: `phase` not on `Error::Inner`, threaded via side context.
- smtp: `partial_completion_error` fallback hides LMTP programming
  errors.
- gmail: `shallow_clone` exists for a code path that no longer needs
  it.
- gmail: `throttle_scope_for` and `Server(QuotaExhausted)`
  classification of `dailyLimitExceeded` in two places.
- graph: `wire_or_specific` dead code.
- graph: `body_diagnostic` uncapped.
- graph: `parse_retry_after_header` integer-seconds only.
- graph: catch-all wildcard after exhaustive `GraphSignal` arms.
- graph: `ews_stream::scope_for_folder` race on `try_read`.
- graph: `GraphResponseError::from_response` empty-string sentinel.
- sync: `Reconcile` advice actions discarded in 4 sites.
- sync: `MutationBucket::BlockedByEngine` and `FailedTerminal`
  collapsed in counters.
- sync: `wait_for_real_subscriber` hot-polls every 25 ms.
- sync: `broadcast_warning` defaults to `CursorScope::Account` for
  scope-bound directives.
- sync: terminal-arm logging not structured.

### Documentation drift

- `reference/graph.md:62-67, 421-428, 64-67` claims helpers and
  module contents that don't exist; describes EWS / webhook
  convergence that didn't land.
- `reference/jmap.md:246` claims item-level
  `ConcurrencyConflict` from `stateMismatch` that code doesn't emit.
- `reference/sync.md` claims `Fatal::try_from` used at engine
  boundaries; never called.

## Mitigating context

`bifrost-net` is clean modulo two smells. The single-target,
single-protocol idempotent-op happy path works end-to-end. Mechanical
greps pass. The shape of the contract is in place; what's missing is
the wiring at the boundaries (push streams, EWS, webhooks, PIM batch,
get_stream) and the engine-side dispatch surfaces that consume
`RecoveryClass`/`EngineDirective`/`ThrottleScope`/`ReconcileAction`.

Most P0 issues are either (a) producer-side: a single boundary
function not pushing `AttemptCause` or `idempotency_override`, or
(b) consumer-side: sync engine never reading helpers that exist. None
are deep architectural rework; they are wiring tasks against a
contract that is already specified.
