# Error model: bifrost-imap implementation plan

**Scaffold.** Fill in the details by reading `reference/imap.md` and
the `crates/imap/` source. Sections marked `<TODO>` are placeholders.

This is **Phase 2.2** of `plans/error-model-roadmap.md`, one of the
five consumer crates that migrate in parallel.

## Scope

`bifrost-imap` is an IMAP client. Per `CLAUDE.md`: Daaki-derived
driver-task model, Tokio + native-tls only. It does NOT route through
`bifrost-net` (no HTTP).

What changes:

- Internal error type → `AccountError` via the builder.
- IMAP response codes (`[ALERT]`, `[TRYCREATE]`, `[NOPERM]`, etc.) →
  structured causes via `Cause::Wire(WireCause::Imap(ImapResponseCode {
  kind: ... }))`.
- Strategy downgrades (QRESYNC → CONDSTORE → Basic) now flow as
  `RecoveryClass::Engine(EngineDirective::DowngradeStrategy(_))`.
- Native error signal (untagged responses, parse failures, TLS
  errors) classified per the convergence plan's table.

## Dependencies

- Phase 1 (`plans/error-model-types.md`).
- **NOT** dependent on Phase 2.1 (`bifrost-net`) — IMAP has its own
  TCP / TLS layer.

## Files to modify

`<TODO>` — discover. Likely candidates based on `reference/imap.md`:

- internal error type module
- the driver-task model (how errors propagate from socket I/O to
  the public surface)
- the response parser (where untagged responses become structured
  events)
- the strategy-downgrade logic (QRESYNC / CONDSTORE handling)
- the `Account` impl under `crates/imap/src/account/`
- the per-folder modseq cache and the `STORE UNCHANGEDSINCE`
  opportunistic path

## Files to delete

`<TODO>` — discover. Any IMAP-specific recovery helpers.

## Translation surface

```rust
pub(crate) fn into_account_error(
    error: imap::Error,
    ctx: ImapErrorContext,
) -> AccountError;
```

Implementation:

1. Classify by error variant:
   - Transport-layer (TCP / TLS) → `Cause::Transport(...)` with
     `Cause::Attempt(AttemptCause { transmission_state })`. `imap`
     owns the state machine here since it does not route through
     `bifrost-net`.
   - Server-rejected command (`NO` response with response code) →
     `Cause::Server(...)` (or `Cause::Access(...)` for `[NOPERM]`)
     plus `Cause::Wire(WireCause::Imap(...))`.
   - Server-rejected command (`BAD` response) → `Request(Malformed)`
     when the cause is a client construction error;
     `Protocol(ContractViolation)` if the server is misbehaving.
   - Untagged response indicating server policy
     (`* BYE [SERVERBUG]`, etc.) → context-dependent.
   - Parse failure → `Protocol(ParseFailed)`.
   - Connection closed unexpectedly → `Transport(Network)` with
     `TransmissionState::InFlight` or `Unsent` depending on
     whether a command was in flight.
2. Build with operation, scope, `provider: None` (IMAP is generic
   server, no fixed provider), `protocol: Imap`.

## Notable concerns

- **No `bifrost-net` dependency.** IMAP owns its own transport state
  machine, so `AttemptCause` emission is `bifrost-imap`'s
  responsibility, not delegated. The state machine must distinguish
  "command queued but not sent" / "command sent, response pending" /
  "response received."
- **Strategy downgrade is a recovery class now.** Per the
  convergence plan, IMAP strategy downgrades surface as
  `RecoveryClass::Engine(EngineDirective::DowngradeStrategy(_))`.
  The current `WarningKind::StrategyDowngraded` warning still fires
  for observability (the downgrade happened transparently) but
  permanent strategy failure that needs operator attention is the
  engine directive case.
- **`STORE UNCHANGEDSINCE` opportunistic path.** Per
  `reference/imap.md`, IMAP attempts `STORE UNCHANGEDSINCE` when the
  cache says modseq is current. If the server reports the modseq
  advanced (FAILED response), that's `ConcurrencyConflict` →
  `Retry(AfterStateRefresh)`. The mutation pipeline must consume
  this through the new `RecoveryClass` shape.
- **QRESYNC vs CONDSTORE vs Basic.** The strategy selector currently
  produces `RecoveryClass::DowngradeStrategy(...)` directly. Update
  to `RecoveryClass::Engine(EngineDirective::DowngradeStrategy(...))`.
- **IDLE / NOTIFY push.** Per `reference/imap.md`, IDLE-busy on
  another checkout produces `Error::IdleBusy`. Map to a
  `Reconcile`-ish or terminal classification — `<TODO>` confirm
  which is right from the engine's perspective.

## Tests

Per project rules. ~15-20 tests:

- One test per `[<response code>]` that the recovery table covers.
- TCP / TLS error classification with correct `TransmissionState`.
- `NO` / `BAD` / `BYE` response classification.
- Strategy downgrade emits `Engine(DowngradeStrategy(_))`.
- `STORE UNCHANGEDSINCE` modseq-advanced emits
  `Retry(AfterStateRefresh)`.

No live IMAP server. Construct synthetic response objects.

## Exit criteria

- Every IMAP response code in the recovery table has a defined
  classification.
- `into_account_error` exists and routes through `AccountErrorBuilder`.
- Strategy downgrade emits the new `EngineDirective` variant.
- `AttemptCause` is emitted by the driver-task model with correct
  `TransmissionState`.
- Old recovery helpers removed.
- Patches against this crate match this plan's exit criteria.
  Compilation and per-crate tests are not run at this phase;
  Phase 3 (workspace integration) is where `brokkr check` runs
  and tests execute.

## Discovery items for the agent

`<TODO>`:

1. Current `imap::Error` shape and the driver-task model's error
   propagation path.
2. Where the response parser turns untagged responses into structured
   events.
3. How the current strategy-downgrade signal flows to the sync
   engine (and where to redirect it to the new
   `EngineDirective::DowngradeStrategy` variant).
4. The set of `ImapResponseCode` variants needed for
   `WireCause::Imap`.
5. The transport state machine's existing notion of
   `(unsent | in-flight | acknowledged)`, if any.
