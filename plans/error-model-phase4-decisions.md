# Error model phase 4 - decisions

Triage decisions for every design ambiguity surfaced in
`plans/error-model-phase4-audit.md`. Locked. Execute per the phase
plan at the bottom.

The audit doc is frozen evidence; this decisions doc is the live
tracker. Resolved items get marked `[done]` inline. The audit doc is
deleted only when every item here is `[done]`. This explicitly
overrides the AGENTS.md "delete resolved items" rule for the pair.

Each item: a short **rec** with the decided shape; **why** when
non-obvious; sometimes **cascade** when the decision implies edits
elsewhere.

## Shared types and surface

### Decisions

- **types-D1.** `Fatal(pub AccountError)`.
  - **rec:** field private; add `Fatal::into_inner(self) -> AccountError`
    and `Fatal::as_ref(&self) -> &AccountError`. `TryFrom` remains the
    only constructor.
  - **why:** the `pub` field bypasses `TryFrom` and lets callers build
    `Fatal(retryable_error)`.

- **types-D2.** `EngineDirective::CapabilityChanged { delta }`.
  - **rec:** remove the directive variant. Capability shifts map to
    `EngineDirective::RestartAccount`.
  - **cascade:** `StateCause::CapabilityChanged` stays (forensic
    evidence) but loses the fake-default payload: see types-D2b.

- **types-D2b.** `StateCause::CapabilityChanged` payload.
  - **rec:** `StateCause::CapabilityChanged { delta:
    Option<CapabilityDelta> }`. `None` is honest when no producer
    computes a real delta. Recovery maps the cause to
    `Engine(RestartAccount)` regardless.
  - **why:** keeps forensic value without inviting
    `CapabilityDelta::default()` sentinels.
  - **test:** no producer constructs `Some(CapabilityDelta::default())`.

- **types-D3.** `BatchOutcome` mutability.
  - **rec:** `BatchOutcome` is immutable on return. Add
    `BatchOutcomeBuilder` with `push_succeeded` / `push_failed` /
    `push_uncertain` / `finalize(expected: &[BatchItemId]) ->
    Result<BatchOutcome, BatchInvariantError>`. Protocol crates use
    the builder; the boundary returns the immutable outcome.
  - **why:** mutable lane Vecs on a returned value are an invariant
    violation hazard regardless of accessor shape.

- **types-D4.** `ItemOutcome` closure.
  - **rec:** drop `#[non_exhaustive]`. Three-lane model is closed.
    Adding a fourth lane is an explicit breaking change.

- **types-D5.** Support-export consent gating.
  - **rec:** keep consent as the consumer's responsibility. No
    consent-token parameter. Add a doc note on `support_internal()`.

- **types-D6.** Round-trip preservation through `into_builder`.
  - **rec:** `idempotency_override` is removed entirely from the
    builder for default cases (see types-D6b). The `RetryHint` change
    (types-D7) makes wall-clock vs duration round-tripping
    structural rather than convention. There is no `retry_not_before`
    side-channel after this lands.

- **types-D6b.** `idempotency_override` semantics.
  - **rec:** `AccountOperation::is_idempotent()` is authoritative.
    `idempotency_override` exists only for the exceptional case where
    a normally-unsafe operation is made safe by a request-side guard
    (e.g. `UpdateFlags` with `If-Match`). Default value is "use the
    operation's intrinsic idempotency."
  - **cascade:** invalidates the audit's framing of graph-D1.
    Graph's non-idempotent-Send bug is fixed by graph-D-attempt
    (push `AttemptCause` on `Response` errors) and graph-D-pim-op
    (correct `AccountOperation` at PIM call sites), not by spraying
    `idempotency_override(false)`.

- **types-D7.** Retry hint shape.
  - **rec:** introduce
    ```rust
    pub enum RetryHint {
        After(Duration),
        At(SystemTime),
    }
    impl RetryHint {
        pub fn not_before(&self, now: SystemTime) -> SystemTime;
        pub fn min_delay(&self, now: SystemTime) -> Duration;
    }
    ```
    Carry `retry_hint: Option<RetryHint>` on:
    - `ServerCause::{Unavailable, RateLimited, QuotaExhausted}`,
    - `RetryAdvice`.
    Drop `not_before` and `min_delay` from `RetryAdvice` entirely.
    Drop `retry_not_before` from `AccountErrorBuilder`. Recovery
    derivation forwards the hint from the cause to the advice
    verbatim.
  - **why:** dual fields with exact-mapping convention is the same
    two-sources-of-truth smell that produced the side-channel bug.
    The hint is the value; wall-clock and duration are computed
    accessors.

- **types-D8.** `RemediationAction` engine actions.
  - **rec:** remove `RestartScope` / `RestartAccount` variants.
    `recovery::suggest` returns `None` for `SyncState(CursorInvalid)`,
    `SyncState(SchemaIncompatible)`, and any other engine-driven
    case.
  - **why:** engine actions are not consumer remediations. The
    convergence doc's `Reconcile` example already documents `None`
    here.

- **types-D9.** `Protocol(Unknown)` chain-pairing.
  - **rec:** drop the "unless paired with higher-level retryable
    cause" clause. `ProtocolErrorKind::Unknown` is unconditionally
    `UnknownPermanent`. Producers that have a retryable signal use
    that as the kind.
  - **cascade:** update convergence doc to remove the pairing
    language.

- **types-D10.** Construction failure surface.
  - **rec:** `AccountErrorBuilder::build` returns
    `Result<AccountError, AccountErrorBuildError>`. Variants:
    ```rust
    pub enum AccountErrorBuildError {
        EmptyChain,
        KindCauseMismatch { kind, primary_cause },
        TransportAcknowledged,
        CursorInvalidWithoutScope,
    }
    ```
    Protocol crates call `.expect("valid account error
    classification")` at their `into_account_error` boundary. The
    error never escapes a producer; tests at every boundary cover
    the four variants.
  - **why:** invalid builder state is a producer invariant bug.
    Letting it become runtime-recoverable contaminates `Result<T,
    AccountError>` with a second escape hatch.
  - **cascade:** removes the documented "missing cursor scope falls
    back to `RestartAccount`" rule (see types-D10b).

- **types-D10b.** Cursor scope is required on `CursorInvalid`.
  - **rec:** `SyncState(CursorInvalid)` without scope is a build
    error (types-D10's `CursorInvalidWithoutScope`). The old
    fallback to `Engine(RestartAccount)` goes away.
  - **why:** with no consumers and a clean-API priority, requiring
    scope is better than silently coarsening recovery.
  - **cascade:** update `plans/error-model-convergence.md` to remove
    the fallback language. Protocol crates that produce
    `CursorInvalid` must thread the cursor scope from the call site.

- **types-D11.** Mutual-exclusivity test for the four helpers.
  - **rec:** add a test that constructs one value per
    `RecoveryClass` variant and asserts exactly one of
    `is_retryable` / `requires_reconciliation` /
    `requires_engine_action` / `is_terminal` returns true.

- **types-D12.** `Fatal::try_from` round-trip test.
  - **rec:** add the `plan_recovery_terminal_round_trip` test
    convergence-plan called out. For every terminal `RecoveryClass`
    variant: `Fatal::try_from(error).is_ok()`. For every non-terminal
    variant: `Fatal::try_from(error).is_err()`.

- **types-D13.** `ResourceKind` widening.
  - **rec:** add `Draft`, `Identity`, `Vacation`, `PushSubscription`
    to `ResourceKind`. `PushSubscription` is the provider-neutral
    name covering Gmail Pub/Sub watch, Graph webhook, JMAP push,
    and any future IMAP IDLE abstraction.
  - **why:** Gmail's `not_found_kind_cause` silently coerces these
    to `Message` today; widening is the only correct fix.

- **types-D14.** `ThrottleScope` enum.
  - **rec:**
    ```rust
    pub enum ThrottleScope {
        CurrentOperation,
        Mailbox,
        Account,
        Tenant,
        Provider,
    }
    ```
    Only the last four map to `ThrottleKey` in sync (sync-D3).
    `CurrentOperation` is a hint for the caller: delay only this
    work item, do not pause a shared bucket. The convergence and
    sync reference docs spell out this distinction.

- **types-D15.** `WatchEvent::Terminated(AccountError)`.
  - **rec:** add the variant. Push streams that terminate on a
    classified error emit it before exiting.
  - **cascade:** Gmail Pub/Sub renewer (gmail-D3), JMAP WebSocket
    reader (jmap-D2), Graph webhook subscribe/renew (graph-D3) all
    emit through this variant.

- **types-D16.** `AccountControl`.
  - **rec:** add
    ```rust
    pub enum AccountControl {
        Pause(PauseReason),
        Resume,
    }
    pub enum PauseReason {
        OperatorOverrideRequired,
        ConsumerRequested,
        TenantThrottle,
        RetryBudgetExhausted,
    }
    ```
    Engine flips an account to `Pause` automatically on
    `EngineDirective::OperatorOverrideRequired` and on
    `RetryBudgetExhausted` (sync-D6). Consumer flips to `Resume`
    after intervention.
  - **why:** `OperatorOverrideRequired` needed a pause primitive;
    `Pause(reason: String)` would reintroduce raw strings we just
    eliminated. Bounded enum; no `DiagnosticText` here.

- **types-D17.** `get_stream` trait shape.
  - **rec:** change `Account::get_stream` return type to
    `AccountStream<SyncEvent<ItemOutcome<HydratedObject>>>`. Locally
    invalid ids emit `ItemOutcome::Failed`; transport drops emit
    `ItemOutcome::Uncertain`; success emits
    `ItemOutcome::Succeeded`. Trait change owned by Phase A.
  - **cascade:** imap-D4 and graph-D5 consume this surface.

- **types-D18.** WebSocket error variant split (JMAP).
  - **rec:** in `bifrost-jmap` (not types), split `Error::WebSocket`
    into `Error::WebSocketHandshake(tokio_websockets::Error)` and
    `Error::WebSocketRuntime(tokio_websockets::Error)`. Remove
    `From<tokio_websockets::Error>`. `connect_ws` -> Handshake,
    runtime loop -> Runtime.
  - **why:** kept here in the shared-surface section because the
    audit considered it an architectural correctness item.
    Implementation lives in jmap.

### No decision (fix per spec)

- types-N1. `Server(Error{status:None})` `InFlight` rows: add the
  two missing recovery-table rows (idempotent -> `Retry::SameRequest`,
  non-idempotent -> `Reconcile(TransportDropAfterSend)`).
- types-N2. `CauseChain::root()` and `into_builder`'s
  `causes.remove(0)`: switch to `assert!` with non-empty invariant
  on `CauseChain::new`. Pick whichever simplifies.
- types-N3. `EnhancedStatusCode::code: u16` rename to `reply`.
- types-N4. `recovery::derive` `assert!` -> `debug_assert!` for the
  Transport+Acknowledged check, and the kind/cause mismatch check
  in `kind_matches_cause`. Release-mode is covered by the
  `try_build` failure path (types-D10) since the same checks live
  inside `build`.

## bifrost-net

No decisions; smells only. Defer to a final cleanup pass.

- net-N1. `RequestCause::Malformed.detail`: leave synthetic; body
  in support-text is fine.
- net-N2. `refresh_failed` body loss via `Display`: accept; the
  non-recursion rule for sources is binding.
- net-N3. Retry-then-classify: leave; final classification is
  correct.
- net-N4. `throttle_scope` per-provider coverage: extend as
  providers document; `None` is honest for unknowns.

## bifrost-jmap

### Decisions

- **jmap-D1.** WebSocket pre/post handshake error split.
  - **rec:** implemented as types-D18 above. `WebSocketHandshake` maps
    to `Transport(Network) + Attempt(Unsent)`;
    `WebSocketRuntime` maps to `Protocol(PartialResponse) +
    Attempt(Acknowledged)`.

- **jmap-D2.** Push WebSocket reader emits `Terminated`.
  - **rec:** reader classifies every exit error via
    `JmapErrorContext::new(PushStream)` and emits
    `WatchEvent::Terminated(AccountError)` before exiting. Drops
    no errors.

- **jmap-D3.** `scope_lifecycle` swallowed errors.
  - **rec:** emit
    `SyncEvent::Terminated(AccountError)` on terminal classes; stop
    polling. Retry classes get classified and handed to sync via
    the normal channel. No protocol-side sleep-and-retry.
  - **why:** sync owns recovery policy; protocol owns classification.
    Splitting policy across protocol code and sync code is the
    smell that produced the audit gaps.
  - **cascade:** same shape applies to `discover_memberships` and
    any other long-running protocol-side loop.

- **jmap-D4.** Generic JMAP `Provider`.
  - **rec:** defer. Continue setting `Provider: None`. Wire
    `Provider::Fastmail` / others through the factory in a later
    commit when documented.

### No decision

- jmap-N1. `terminated_unsupported(op, scope, msg)`: take caller's
  `AccountOperation`. Pagination overflows reclassify as
  `Protocol(ContractViolation)`, not `Unsupported(Discover)`.
- jmap-N2. `SetErrorType::Other(String)`: mirror
  `MethodErrorType::Other(String)`. Stop synthesizing `"other"`.
- jmap-N3. `resource_from_scope` / `id_from_scope`: drop `_ => None`
  catch-alls so `ErrorScope::#[non_exhaustive]` enforces coverage.
- jmap-N4. `capabilities.rs:48-53` "core limits zero" reclassify as
  `Protocol(ContractViolation)`.

## bifrost-imap

### Decisions

- **imap-D1.** Tagged `NO` / `BAD` `AttemptCause`.
  - **rec:** add `attempt: Option<TransmissionState>` to
    `Error::No` and `Error::Bad`. Default at construction site:
    `Some(Acknowledged)`. `with_attempt` becomes a no-op only for
    variants that genuinely cannot carry an attempt state (e.g.
    pure parse errors before any wire activity).

- **imap-D2.** Output-channel-dropped error synthesis.
  - **rec:** on sender error from a dropped receiver, return
    `Ok(())` from the streaming task. Do not synthesize
    `Error::closed()`. Affected sites:
    `inventory.rs:62, 106, 132, 136, 139`, `changes.rs:345, 365,
    411, 415, 605, 609, 774, 790`, `get.rs:107, 114`, `blob.rs:164`.

- **imap-D3.** Trailing global `Terminated` after per-item outcomes.
  - **rec:** in `mutation_stream`, a per-folder fatal after per-item
    emissions emits `ItemOutcome::Uncertain` for remaining items in
    that folder (with the fatal as cause) and continues to the next
    folder. Stream-level `Terminated` reserved for errors that
    prevent any further folder attempts (auth lost, schema break).

- **imap-D4.** `get_stream` trait shape.
  - **rec:** consume the new `SyncEvent<ItemOutcome<HydratedObject>>`
    surface from types-D17.

- **imap-D5.** `MutationSuccess::Skipped` divergence.
  - **rec:** accepted; document in `reference/imap.md` that IMAP
    uses `ItemOutcome::Failed { kind: ConcurrencyConflict }` for
    UNCHANGEDSINCE conflicts. IMAP cannot observe "already in state"
    without defeating the opportunistic guard.

- **imap-D6.** `EngineDirective::DowngradeStrategy` runtime
  derivation.
  - **rec:** keep helper. Document in `reference/imap.md` that
    runtime derivation is reserved for "all strategies exhausted"
    and currently has no trigger because Basic strategy always
    succeeds where the others fall short.

### No decision

- imap-N1. AUTH leg of `factory::open`: keep `AccountOperation::Discover`.
  AUTH is protocol-level idempotent.
- imap-N2. Boundary builders attaching scope: thread `Mailbox(folder)`
  / `Cursor(scope)` per the IMAP plan's table.
- imap-N3. `concurrency_conflict_error` / `store_failed_error` take
  `AccountOperation` parameter; thread from caller's `MutationKind`.
- imap-N4. Delete `MAILBOX_UNAVAILABLE_KINDS` const, `let _ = &mut
  t;`, unused `with_thread_id` constructor.
- imap-N5. Consolidate `terminated_event` / `fatal_event` into one
  helper taking `impl Into<AccountError>`.

## bifrost-smtp

### Decisions

- **smtp-D1.** LMTP DATA-command failure.
  - **rec:** replace `self.command(Data)` with
    `self.command_accepting_status(Data)` (mirroring the SMTP non-
    pipelined path). Negative reply marks accepted recipients failed
    with `Acknowledged`; transport drop on command write/read uses
    `InFlight` with `DataCommand` phase; transport drop on body
    write uses `InFlight` with `DataBody` phase.

- **smtp-D2.** AUTH `InvalidInput` routing.
  - **rec:** with the full `SmtpCommandPhase` enum (smtp-D3), the
    `Auth` phase routes "no compatible mechanism" to
    `Authorization(PolicyBlocked)`. Generic `InvalidInput` outside
    `Auth` keeps `Request(Malformed)`.

- **smtp-D3.** `SmtpCommandPhase` completeness.
  - **rec:** ship the full 16-variant enum: `Connect`, `Greeting`,
    `Hello`, `StartTls`, `Auth`, `MailFrom`, `RcptTo`, `DataCommand`,
    `DataBody`, `DataFinal`, `BdatBody`, `LmtpFinalStatus`, `Noop`,
    `Vrfy`, `Expn`, `Rset`. `DataFinal` distinguishes the negative-
    final-reply path from a body transport drop (smtp-D5).
  - Thread the phase through every `with_attempt` / `with_phase`
    site so the enum carries real signal.

- **smtp-D4.** `phase` field location.
  - **rec:** add `phase: Option<SmtpCommandPhase>` to `Error::Inner`.
    `SmtpErrorContext::with_phase` becomes a wrapper that sets the
    field on the carried error.
  - **why:** "missed `with_phase` call silently degrades" is a
    recurring footgun; making phase part of the value not a
    context-time decoration closes it.

- **smtp-D5.** DATA-final-negative phase tag.
  - **rec:** use `SmtpCommandPhase::DataFinal` (added in smtp-D3),
    not `DataBody`.

- **smtp-D6.** `message_error_to_account_error`.
  - **rec:** add a boundary function that maps every
    `crate::error::Error` variant to `AccountError`. Without it,
    any future `Account` impl that builds a `Message` will invent
    its own mapping.

- **smtp-D7.** `partial_completion_error` LMTP fallthrough.
  - **rec:** `debug_assert!(protocol == Protocol::Smtp)` in the
    fallback. LMTP `Accepted` without `Final` at resolve time is
    a programming bug, not an uncertain outcome.

### No decision

- smtp-N1. Per-recipient address as `DiagnosticText::support_only`
  on each failed/uncertain lane explicitly; not just shared response
  text.

## bifrost-gmail

### Decisions

- **gmail-D1.** `AttemptCause` on HTTP `Response` errors.
  - **rec:** push `AttemptCause { Acknowledged }` at the Gmail
    response boundary on every `Error::Response`, and post-200
    decode failures (`JsonDecode`, `Base64`) also get
    `Acknowledged` (the response was received; decode failed
    after).

- **gmail-D2.** `NotFound` for non-message resources.
  - **rec:** route through the widened `ResourceKind` from types-D13
    (`Draft`, `Identity`, `Vacation`, `PushSubscription`). Drop the
    `ResourceKind::Message` fallback in `not_found_kind_cause`.

- **gmail-D3.** Pub/Sub renewer emission.
  - **rec:** classify every error; emit
    `WatchEvent::Terminated(AccountError)` on terminal classes; emit
    a structured `Warning` on transient classes. No bare
    `tracing::warn!` for protocol-classifiable errors.

- **gmail-D4.** `retry_after` source of truth.
  - **rec:** `ServerCause::{Unavailable, RateLimited,
    QuotaExhausted} { retry_hint: Option<RetryHint> }` is
    authoritative (types-D7). The builder side-channel is gone.

- **gmail-D5.** `InsufficientScope::needed` per-operation.
  - **rec:** add a small `gmail_scope_for(op: AccountOperation) ->
    &'static str` helper that maps each gmail-touching operation to
    the required scope (`gmail.modify`, `gmail.labels`, `gmail.send`,
    `gmail.compose`, etc.). Use at `translate_response` and any
    scope-deriving site.

- **gmail-D6.** `failedPrecondition` context-dependent routing.
  - **rec:** from the history endpoint, classify
    `SyncState(CursorInvalid)`. From elsewhere, classify
    `ConcurrencyConflict`. Drop the `Request(Malformed)` mapping.

### No decision

- gmail-N1. `inventory.rs:196`: clone `id` before move into
  `hydrate_one`; preserve in error scope.
- gmail-N2. `recovery.rs:153-160`: attach blob `id` as
  `ErrorScope::Message { id }` (parent message scope) and as
  diagnostic native code.
- gmail-N3. `client.rs:159` unreachable branch: `unreachable!()`.
- gmail-N4. `terminates_mutation_stream` comment/code mismatch:
  fix comment.
- gmail-N5. `shallow_clone` cleanup: drop helper, call
  `into_account_error(error, ...)` directly.
- gmail-N6. Hoist `dailyLimitExceeded` classification into one
  helper.
- gmail-N7. Rename `account/recovery.rs` to `account/error.rs`.

## bifrost-graph

### Decisions

- **graph-D1.** `idempotency_override` semantics.
  - **rec:** invalidates the audit's framing. Graph does not add
    `idempotency_override(false)` at every non-idempotent call site.
    The actual non-idempotent-`Send`/`BulkMove` bug is fixed by
    graph-D7 (push `AttemptCause` on `Response`) plus graph-D8
    (correct `AccountOperation` at PIM sites). The override
    surface stays per types-D6b: only for `If-Match`-guarded calls
    that flip a normally-unsafe op safe, etc.

- **graph-D2.** EWS `Result<_, String>` end-to-end.
  - **rec:** full conversion. `EwsError::Transport(String)` /
    `SoapFault { message: String }` become
    `Transport(bifrost_net::Error)` /
    `SoapFault { code: SoapFaultCode, detail: DiagnosticText }`.
    Add `GraphErrorContext::ews(...)`. Add
    `WireCause::MalformedResponse { protocol: Protocol::Ews, .. }`.
    Every EWS boundary stamps `Protocol::Ews`.

- **graph-D3.** Webhooks `Result<_, String>`.
  - **rec:** full conversion. `webhooks.rs` returns
    `Result<_, GraphError>`. `account/push.rs::subscribe_graph` /
    `unsubscribe_graph` / renewer route through
    `into_account_error`. Remove `transport_string_error`.

- **graph-D4.** PIM `$batch` 4xx/5xx routing.
  - **rec:** `pim::submit_write_batch` routes per-item failures
    through `mutation_item_outcome`. Per-item body parsed for
    `GraphSignal`.

- **graph-D5.** `get_stream` per-item failures.
  - **rec:** consume `SyncEvent<ItemOutcome<HydratedObject>>` from
    types-D17. Emit `ItemOutcome::Failed` with classified
    `AccountError` instead of dropping a `Warning`.

- **graph-D6.** Cursor scope absence on 410 / `InvalidDeltaToken` /
  `SyncStateNotFound`.
  - **rec:** producer pushes `CursorInvalid` with cursor scope.
    Missing scope is `AccountErrorBuildError::CursorInvalidWithoutScope`
    (types-D10). No silent fallback to `RestartAccount`. Producer
    bugs surface at build time.

- **graph-D7.** `AttemptCause` on HTTP `Response`.
  - **rec:** push `AttemptCause { Acknowledged }` at the Graph
    response boundary, matching gmail-D1. Local /
    decode-after-200 paths get `Acknowledged`.

- **graph-D8.** PIM `AccountOperation` correctness.
  - **rec:** `pim::submit_write_batch` takes caller's
    `AccountOperation` (not always `UpdateFlags`). `pim_protocol_error`
    takes a scope param and the caller's `AccountOperation` (not
    always `Hydrate`).

- **graph-D9.** Webhook scope partial-resolve.
  - **rec:** if any requested scope cannot resolve to a Graph
    resource, return `AccountError` kind `Unsupported(PushSubscribe)`.
    Do not silently subscribe to the resolvable subset.

- **graph-D10.** EWS body cap.
  - **rec:** enforce `STATUS_BODY_CAP` in `body_diagnostic`. Cap
    matches `bifrost-net`'s.

### No decision

- graph-N1. `remove_from_container` reports
  `Unsupported(RemoveFromContainer)`.
- graph-N2. `wire_or_specific` and trailing `_ => {}` arms:
  delete; let `#[non_exhaustive]` enforce coverage.
- graph-N3. `parse_retry_after_header`: support HTTP-date.
- graph-N4. `ews_stream::scope_for_folder` `try_read` -> `.read().await`.
- graph-N5. `client.rs::execute` unreachable branch: `unreachable!()`.
- graph-N6. `GraphResponseError::from_response` empty-string
  `Unknown { code: "" }`: emit `WireCause::MalformedResponse`.
- graph-N7. `reference/graph.md`: remove references to deleted
  helpers; reflect graph-D2 / graph-D3 once landed.
- graph-N8. Confirm `AccountNet` auto-injects the bearer for
  `execute` and `fetch_blob_stream`; if not, attach explicitly.

## bifrost-sync

### Decisions

- **sync-D1.** `RecoveryClass` dispatch surface.
  - **rec:** all five variant-direct match sites move to dispatch
    through `is_retryable` / `requires_reconciliation` /
    `requires_engine_action` / `is_terminal`, via a new
    `plan_recovery(&AccountError) -> RecoveryPlan` helper:
    ```rust
    pub enum RecoveryPlan {
        Retry(RetryAdvice),
        Reconcile(ReconcileAdvice),
        Engine(EngineDirective),
        Terminal(Fatal),
    }
    ```
    Every call site uses `plan_recovery`. Adding a future
    `RecoveryClass` variant fails to compile here, not silently
    routes to terminal.

- **sync-D2.** `Fatal::try_from` wiring.
  - **rec:** `RecoveryPlan::Terminal(Fatal)` carries the typed
    terminal. Operator-notification queue and persistent-failure
    dashboard surfaces consume it directly. No ad-hoc `Fatal`
    construction anywhere else.

- **sync-D3.** `ThrottleBucket` + `ThrottleKey`.
  - **rec:** add
    ```rust
    pub enum ThrottleKey {
        Mailbox(AccountId, MailboxId),
        Account(AccountId),
        Tenant(TenantId),
        Provider(Provider),
    }
    pub struct ThrottleBucket {
        waits: HashMap<ThrottleKey, SystemTime>,
    }
    ```
    Engine consults the bucket before driving work; a hit pauses
    affected work until `wait_until`. `Tenant` / `Provider` cross
    account boundaries. `ThrottleScope::CurrentOperation` (types-D14)
    never enters the bucket; the caller delays the single work item
    inline.

- **sync-D4.** `ReconcileAction` handling.
  - **rec:** `ReconcileAction::CheckTarget` routes through the
    read-back guard the mutation pipeline already uses.
    `ReconcileAction::DedupeByClientId` records a counter on
    `MutationCounters` and emits a `Warning::OperatorAttentionNeeded`;
    the dedupe itself happens at the consumer because the client-id
    space lives there.
  - **why:** engine can implement `CheckTarget`; consumer is the
    only place with the right context for `DedupeByClientId`.

- **sync-D5.** `bulk_set_flags` `Engine(_)` routing.
  - **rec:** the function takes a `reopen_tx: Sender<ReopenRequest>`
    (or accesses the shared one). On `Engine(_)`: sends
    `ReopenRequest::Recovery { scope: directive_target_scope(...),
    error }` and returns `Ok(BatchOutcome)` accounting for work
    so far. Caller sees a partial outcome; the directive reaches
    the engine.

- **sync-D6.** Reopen-failure backoff and terminal emission.
  - **rec:** `restart_account` and per-scope re-establishment use
    exponential backoff (initial 1s, cap 5m, jitter). After three
    consecutive failures: emit
    `SyncEvent::Terminated(last_error)` verbatim and flip the
    account to `AccountControl::Pause(PauseReason::RetryBudgetExhausted)`
    (types-D16). No new error kind. No `Fatal` wrapping. The
    classification of `last_error` is whatever the underlying
    producer emitted; the engine just records "tried 3 times" as
    telemetry.
  - **why:** `Fatal` is a classification ("terminal by recovery
    class"), not an engine budget marker. Conflating them was the
    audit pushback that landed this shape.

- **sync-D7.** Per-scope vs per-account escalation.
  - **rec:** `SchemaIncompatible` re-establish failure escalates
    per-scope; account continues for other scopes.
    `RestartAccount` failure escalates per-account; all scopes
    pause.

- **sync-D8.** Capability shifts.
  - **rec:** with `EngineDirective::CapabilityChanged` removed
    (types-D2), all capability shifts arrive as
    `EngineDirective::RestartAccount`. Account reopen already
    re-runs discovery (`discover_cursor_scopes`,
    `discover_memberships`, `push_subscribe`); no extra dispatch
    needed.

- **sync-D9.** `OperatorOverrideRequired` pause.
  - **rec:** directive flips the account to
    `AccountControl::Pause(PauseReason::OperatorOverrideRequired)`
    automatically. Account stream emits a `Warning::OperatorAttentionNeeded`
    carrying the underlying `AccountError`. Polls and pushes
    suspend until the consumer flips to `Resume`.

- **sync-D10.** Cursor envelope decode -> `SchemaIncompatible`
  directive.
  - **rec:** `cursor/envelope.rs::decode_envelope` failure on
    schema-below-min routes through a new
    `engine::cursor_decode_failure(&Error) -> AccountError`
    translator producing
    `AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)`.
    `attach`'s call site routes through
    `handle_engine_directive(SchemaIncompatible)` instead of
    propagating the bare `Error`.

### No decision

- sync-N1. `directive_target_scope` and other `_ => None` after-
  exhaustive arms: drop the catch-alls so
  `EngineDirective::#[non_exhaustive]` enforces coverage at
  compile time.
- sync-N2. `MutationBucket::BlockedByEngine` vs `FailedTerminal`:
  split into distinct counter fields.
- sync-N3. `wait_for_real_subscriber` 25ms hot-poll: switch to
  `Notify`.
- sync-N4. `broadcast_warning` scope default: pass directive
  target scope when present; account default only for genuinely
  account-wide warnings.
- sync-N5. `EstablishCursorTerminated` variant: keep, wire to
  `plan_recovery`.
- sync-N6. Terminal-arm logging: emit `TelemetryView` structured
  fields rather than `?debug` format.
- sync-N7. `ReopenRequest` keeps `#[non_exhaustive]` (it is `pub`,
  re-exported from `lib.rs:85`).

## Documentation

- **doc-D1.** Reference doc drift.
  - **rec:** update `reference/{graph,jmap,sync,gmail,imap}.md` as
    part of each crate's fix commit.

- **doc-D2.** Audit doc retention.
  - **rec:** `plans/error-model-phase4-audit.md` is frozen evidence
    until every decision here is `[done]`. Decisions doc is the live
    tracker. Resolved decisions get marked `[done]` inline.
  - **override:** the AGENTS.md "delete resolved items, only current
    gaps" rule does not apply to this specific pair of documents.
    Audit value is the snapshot of what was wrong post-merge.

- **doc-D3.** Convergence doc edits cascading from decisions.
  - Remove "missing cursor scope falls back to `RestartAccount`"
    language (types-D10b).
  - Remove `EngineDirective::CapabilityChanged` (types-D2).
  - Remove the "unless paired with higher-level retryable cause"
    clause on `Protocol(Unknown)` (types-D9).
  - Update the recovery table for `Server(Error{status:None}) +
    InFlight` rows (types-N1).
  - Replace `not_before` / `min_delay` references on `RetryAdvice`
    with `retry_hint: Option<RetryHint>` (types-D7).
  - Rename `EnhancedStatusCode::code` to `reply` (types-N3).
  - Document the
    `CurrentOperation` distinction from the `ThrottleKey` variants
    (types-D14).
  - Document the four `AccountErrorBuildError` variants
    (types-D10).

## Execution plan

### Phase A - shared surface (single agent, single commit)

Lands every shared type and trait change. Intentionally leaves
protocol implementations uncompilable; Phase B owns convergence back
to a buildable state. This is the same broken-branch pattern Phase 1
of the original convergence used.

Surface in scope for Phase A:
- All `types-D*` and `types-N*` items.
- `Account::get_stream` trait signature change (types-D17).
- `WatchEvent::Terminated(AccountError)` variant (types-D15).
- `AccountControl` + `PauseReason` (types-D16).
- `ResourceKind` widening (types-D13).
- `ThrottleScope` rename + `ThrottleKey` enum (types-D14, sync-D3
  type definitions - actual `ThrottleBucket` implementation is
  Phase B).
- `RetryHint` shape on `ServerCause` and `RetryAdvice` (types-D7).
- `AccountErrorBuildError` enum + `build()` -> `try_build()` rename
  + `.expect()` discipline documented (types-D10).
- `EngineDirective::CapabilityChanged` removal + cause-payload
  change (types-D2, types-D2b).
- `RemediationAction` variant removal (types-D8).
- `Fatal` field privatization (types-D1).
- `BatchOutcomeBuilder` (types-D3).
- Convergence doc edits (doc-D3).
- The two new tests (types-D11, types-D12).

Exit criterion: types crate compiles cleanly; protocol crates do
not. Decisions in this doc that touched a Phase A surface get marked
`[done]`.

### Phase B - sync alone (single agent, single commit)

Sync gets a dedicated phase because it owns the largest semantic
surface change (throttle bucket, pause/resume, recovery plan
dispatch, terminal escalation, cursor-decode translator,
reconcile-action routing). One agent, one commit, no compromise.

Agent prompt must explicitly forbid:
- compromise adapters between old and new RecoveryClass dispatch,
- compatibility shims for the removed `EngineDirective::CapabilityChanged`,
- stringly pause reasons (use `PauseReason` enum only),
- ad-hoc `Fatal` construction (only `RecoveryPlan::Terminal(Fatal)`
  via `Fatal::try_from`),
- silent error swallows in any long-running loop.

All `sync-D*` and `sync-N*` items. Sync compiles cleanly; protocol
crates still do not.

### Phase C - five protocol crates in parallel

Five agents: jmap, imap, smtp, gmail, graph. Strict file ownership
per AGENTS.md. No agent reads or writes outside its crate.
Orchestrator runs `brokkr check` between agents.

Each agent's prompt cites the relevant `*-D*` and `*-N*` items
from this doc. Same compromise-free discipline as Phase B.

Exit criterion: workspace compiles cleanly. Every decision in this
doc is `[done]`.

### Phase D - re-audit pass

Six agents (sync + five protocols) in parallel. Same per-crate
prompt shape as the original audit but asking "did the fix land
cleanly; are there residual gaps; report findings labeled bug / gap
/ smell / nit." Surfaces P2 items that were deferred plus anything
missed.

### Phase E - P2 cleanup

Smells and nits from the original audit + Phase D residuals. Done
as a focused batch rather than per-crate-sequential. `reference/*.md`
final pass runs alongside Phase D's re-audit reviews.

**Retention override:** `plans/error-model-phase4-audit.md` deletion
is gated on "no active `[bug]` / `[gap]` findings in the audit doc",
not on "every decision `[done]`". Several Phase 5B/5C follow-ups are
deferred-to-consumer-ask (gmail-F1, jmap-F1, sync-F3, etc.) and would
never resolve under the strict reading. The audit doc deletes after
Phase 5D confirms no live bug/gap findings remain.

## Open items

None blocking. Begin Phase A on confirmation.

## Phase 5D follow-ups (deferred, not blocking)

Findings surfaced by the Phase 5D re-audit that aren't already
covered by an existing `*-F*` item. Tracked here so they don't rot
in the audit reports.

- **sync-F4.** `ThrottleBucket` is write-only.
  - **status:** `apply_throttle` (`engine.rs:1740-1757`) records
    deadlines on `Retry` dispatch, but no production code path reads
    the bucket via `wait_for(...)` before driving work. sync-D3's
    intent ("engine consults the bucket before driving any work") is
    unmet. Tenant- and provider-wide throttles recorded by one scope
    do not pause sibling scopes or sibling accounts.
  - **plan:** wire `ThrottleBucket::wait_for(key, now)` consultation
    into the poll loop (`multiplexer/mod.rs` per-scope drive) and the
    push reconciler (`push/reconciler.rs::reconcile`). Deferred until
    a consumer actually needs cross-account pause coordination; the
    record side is already in place.

- **sync-F5.** `apply_throttle` only resolves `Account` scope.
  - **status:** `engine.rs:1740-1757` calls
    `throttle_key_for(scope, ctx.account_id, None, None, None)`.
    `Mailbox` / `Tenant` / `Provider` scopes return `None` from
    `throttle_key_for` and the bucket entry is dropped. `RecoveryContext`
    does not carry mailbox / tenant / provider identity.
  - **plan:** extend `RecoveryContext` with `Option<MailboxId>`,
    `Option<String>` (tenant), `Option<Provider>` and thread them
    through `apply_throttle`. Coupled with sync-F4; doing F5 alone
    has no observable effect because the read side isn't wired.

- **sync-F6.** `[done]` `link_discovered_memberships` no longer
  returns `Ok(())` on `SyncEvent::Terminated`. The Terminated arm
  now returns `Err(Error::Account(err))` so the structured error
  reaches the two callers (`establish` flow at `engine.rs:1499` and
  the recovery-path re-establish at `engine.rs:1957`), both of which
  already log structurally. Push reconciler no longer routes hints
  to "every registered scope" because the index never finished
  building; the engine surface gets the chance to act on the error
  instead of silently degrading routing for the session lifetime.
  Caller-side `reopen_tx` escalation for terminal/engine-action
  classes is a separate enhancement; the immediate behavior change
  is that the error is no longer silently swallowed.

- **gmail-F3.** `scope_lifecycle_stream` and `labels_for_flags`
  swallow with `tracing::warn!`.
  - **status:** `crates/gmail/src/account/scopes.rs:134-136` and
    `:152-155` bare-warn on `list_labels()` failures. An
    auth-lost loop forever instead of surfacing.
  - **plan:** classify through
    `into_account_error(_, GmailErrorContext::containers_list())` and
    break the loop on terminal classes. Trait-shape constraint same
    as `jmap-F1` (the stream element type is `ScopeLifecycle`, not
    `SyncEvent<_>`), so no `Terminated(AccountError)` emission until
    that's resolved. Mirror of `jmap-F1`.

- **smtp-F1.** Legacy non-batch SMTP/LMTP send paths don't tag phase.
  - **status:** `crates/smtp/src/transport/smtp/client/connection.rs:142+`
    (`send_with_options`), `:172+` (`send_bdat_with_options`), `:324`
    (`send_lmtp_with_options`), `:340+` (`send_lmtp_bdat_with_options`)
    use `self.command(...)` with no `with_phase` decoration. Async
    siblings have the same shape. No `Account` impl wires them yet,
    so the P0 double-send pattern is dormant rather than active.
  - **plan:** either (a) deprecate the legacy paths in favor of
    `send_lmtp_batch` once an `Account::send` impl lands and confirm
    the batch helper is the only public surface, or (b) thread phase
    tags + per-recipient lane resolution into them. Defer until a
    consumer wires `Account::send`.

- **smtp-F2.** AUTH-command wire failures inside `auth()` not
  phase-tagged.
  - **status:** `connection.rs:1307` (`command(auth)`) and
    `:1311-1318` (challenge `command(...)`) propagate via `try_smtp!`
    without `with_phase(SmtpCommandPhase::Auth)`. Async siblings
    same. Server `535` / `534` replies still reach `Permanent(response)`
    and classify as `Authentication(...)` / `Authorization(...)` via
    the response-code path, so the routing is correct today; what's
    lost is the AUTH-phase tag for non-reply failures (challenge
    formatting faults, etc.).
  - **plan:** wrap `command(auth)` / challenge commands with
    `.map_err(|e| e.with_phase(SmtpCommandPhase::Auth))`. Trivial
    once someone touches `auth()`.

- **graph-F5.** `SoapFaultCode` coverage gap for Microsoft EWS faults.
  - **status:** `crates/graph/src/ews/mod.rs:78-89` `SoapFaultCode::parse`
    only matches the four SOAP 1.1 names (`VersionMismatch`,
    `MustUnderstand`, `Client`, `Server`). Microsoft EWS in practice
    ships `<faultcode>` values like `a:ErrorAccessDenied`,
    `a:ErrorServerBusy`, `a:ErrorMailboxStoreUnavailable`,
    `a:ErrorImpersonateUserDenied`, etc. - all collapse to
    `SoapFaultCode::Unknown` and then to `Protocol(ContractViolation)`
    -> `ProviderContractViolation` (terminal). Recoverable faults
    (server busy, mailbox store unavailable) are misclassified as
    terminal.
  - **plan:** extend `SoapFaultCode` with `ErrorAccessDenied`,
    `ErrorServerBusy`, `ErrorImpersonateUserDenied`,
    `ErrorMailboxStoreUnavailable`, and any other commonly-seen
    Microsoft EWS faults. Map onto the same `AccessCause::*` /
    `ServerCause::*` / `Access(MailboxUnavailable)` shapes the REST
    path uses. Defer to when EWS is actually exercised in
    production; until then misclassification is latent.

## Phase 5C follow-ups (deferred, not blocking)

Items the Phase 5C protocol agents landed correctly but with explicit
caveats. Tracked here so they don't rot in commit messages.

- **jmap-F1.** `scope_lifecycle_stream` element type mismatch.
  - **status:** the decisions doc (`jmap-D3`) called for
    `SyncEvent::Terminated(AccountError)` on terminal classes but
    `Account::scope_lifecycle_stream` returns
    `AccountStream<ScopeLifecycle>`, not `AccountStream<SyncEvent<_>>`.
    JMAP classifies and breaks the loop on terminal / engine-action
    recovery classes today.
  - **plan:** decide whether `scope_lifecycle_stream` should be
    wrapped in `SyncEvent<_>` like the other long-running streams.
    That is a `bifrost-types` trait change; defer to a future phase
    where the trait surface can be revisited cleanly.

- **jmap-F2.** `[done]` `JmapMethod::NotJson` / `NotRequest`
  reclassified `Request(Malformed)` -> `Protocol(ContractViolation)`
  with `WireCause::MalformedResponse`. The local serializer catches
  any client-side malformed JSON before sending; if the server
  insists otherwise, that is a server conformance failure. Test
  `problem_not_json_maps_protocol_contract_violation` pins it.

- **jmap-F3.** `[done]` `MethodErrorType::Other(code)` two-clone
  smell dedup at `crates/jmap/src/sync/error.rs:919-925`. Constructs
  the `JmapMethod::Unknown { code }` once, clones once into the wire
  cause.

- **imap-F1.** Auto-promote `Mailbox(id) -> Cursor(Folder(id))` for
  `SyncState(CursorInvalid)` in `into_account_error`.
  - **status:** defensive shim added so `EXPUNGEISSUED`/`CLOSED`/etc.
    in PIM/folder paths build cleanly without threading cursor scope
    through every call site.
  - **plan:** centrally-recommended fix is to thread cursor scope
    through every PIM `op_err`. Phase 5D re-audit should flag this if
    not closed; otherwise Phase 5E.

- **imap-F2.** `pim_malformed` and `envelope::malformed` reach helper
  paths that don't know the op.
  - **status:** both produce `Request(Malformed) -> ClientBug` so
    operation telemetry is slightly degraded, but recovery class is
    op-independent here.
  - **plan:** thread operation when other pim/envelope refactoring
    happens; not blocking.

- **gmail-F1.** `GmailResource::Account` falls back to
  `ResourceKind::Message` in `not_found_kind_cause`.
  - **status:** every other `GmailResource` now maps to a specific
    `ResourceKind` (`Draft`, `Identity`, `Vacation`, `PushSubscription`)
    via Phase 5A widening. `Account` has no analogue.
  - **plan:** add `ResourceKind::Account` to `bifrost-types` if
    consumer routing needs to distinguish "account-level NotFound"
    from "message NotFound". Defer until a consumer asks.

- **gmail-F2.** Pub/Sub renewer transient classes use
  `tracing::warn!` instead of a structured `Warning`.
  - **status:** `WatchEvent` has no `Warning` variant; the renewer
    emits structured tracing fields (`kind`, `message_key`,
    `recovery`) alongside the `WatchEvent::Disconnected` health
    signal. Terminal classes emit `WatchEvent::Terminated(AccountError)`
    correctly.
  - **plan:** if a wire `Warning` variant is desired on `WatchEvent`,
    add it to `bifrost-types` and have the renewer emit it. Defer.

- **graph-F1.** `[done]` `STATUS_BODY_CAP` promoted to `pub` in
  `bifrost-net::error`, re-exported from `bifrost-net::STATUS_BODY_CAP`.
  Graph imports it directly; the duplicate constant is gone.

- **graph-F2.** `pim.rs::object_id_from_value` hardcodes
  `AccountOperation::Hydrate` for missing-id cases.
  - **status:** threading the caller's operation through ~10 call
    sites would have been disproportionate; the audit (graph-N10)
    only flagged the missing-etag path which is now correct.
  - **plan:** address in Phase 5E if the telemetry granularity bites.

- **graph-F3.** `[done]` New
  `submit_write_batch_with_targets(account, requests, targets, ...)`
  threads a parallel `&[ObjectId]` slice; per-item failures look up
  by `BatchRequestItem::id.parse::<usize>()` and carry
  `ErrorScope::Message { id }` instead of `ErrorScope::Account`.
  `patch_messages`, `move_messages`, `destroy_messages` all thread
  targets. The legacy zero-target `submit_write_batch` remains for
  callers without per-request ids.

- **graph-F4.** `mutate.rs:132` "Missing folder destination for Move"
  still uses `unsupported_account_error(BulkMove)`.
  - **status:** pre-existing pattern; not named in the decisions doc.
  - **plan:** Phase 5E cleanup if the classification reads wrong in
    practice.

## Phase 5B follow-ups (deferred, not blocking)

Items the Phase 5B sync agent landed correctly but with explicit
caveats that need attention in a later phase. Tracked here so they
don't rot in commit messages.

- **sync-F1.** Three-failed-reopen behavior test deferred.
  - **status:** wiring is correct (exponential backoff with three
    attempts, then `SyncEvent::Terminated(last_error)` plus
    `AccountControl::Pause(PauseReason::RetryBudgetExhausted)`).
    The behavior is reachable but not pinned by a focused test
    because constructing the necessary `Account` / `AccountFactory`
    stub requires either ~400 lines of test scaffolding or a
    pure-function harness around the backoff helper.
  - **plan:** add the test in Phase 5C alongside protocol-level
    reopen exercises, or extract the backoff helper into a
    sync-internal module that can be tested in isolation. Phase
    5D's re-audit should flag this if it isn't closed.

- **sync-F2.** `[done]` Push reconciler now consults the boundary
  pause. `Reconciler::run` parks while `boundary.peek() == Pause`
  via `boundary.changed().await`, alongside the shutdown token.
  `AccountControl::Pause(OperatorOverrideRequired)` and
  `Pause(RetryBudgetExhausted)` park pushes the same way they park
  polls; buffered `WatchEvent`s drain on resume.

- **sync-F3.** `Reconcile` items requesting `DedupeByClientId` only
  (no `CheckTarget`) still queue `PendingReadback`.
  - **status:** counters surface the case
    (`MutationCounters::dedupe_by_client_id`) and a
    `Warning::OperatorAttentionNeeded` fires, but the read-back guard
    runs anyway. Plumbing is correct per sync-D4 (engine owns
    `CheckTarget`, consumer owns `DedupeByClientId`).
  - **plan:** at consumer write-back time, a counter check can suppress
    the read-back for dedupe-only items if the consumer confirms the
    client-id was deduped. Out of scope for sync; revisit when a
    consumer (ratatoskr) starts using the counter surface.
