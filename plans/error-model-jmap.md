# Error model: bifrost-jmap implementation plan

**Scaffold.** Fill in the details by reading `reference/jmap.md` and
the `crates/jmap/` source. Sections marked `<TODO>` are placeholders.

This is **Phase 2.2** of `plans/error-model-roadmap.md`, one of the
five consumer crates that migrate in parallel after `bifrost-net`.

## Scope

`bifrost-jmap` implements RFC 8620 / 8621 / etc. JMAP. It dispatches
through `bifrost-net` for HTTP and emits its own structured errors
for JMAP method errors (`stateMismatch`, `forbidden`, parse failures,
etc.).

What changes:

- Every site that currently constructs an `Error` value (per the old
  `bifrost-types::Error`) switches to constructing an `AccountError`
  via `AccountErrorBuilder`.
- The JMAP method-error vocabulary maps cleanly into
  `AccountErrorKind` + `Cause::Wire(WireCause::Jmap(JmapMethod {
  kind: ... }))`.
- Any `to_recovery`, `recovery_for_*`, or `fatal_for_*` helpers
  delete; recovery derives centrally.
- Provider / protocol identifiers are stamped on every `AccountError`
  via the builder's `.provider(Provider::Fastmail | ...)` and
  `.protocol(Protocol::Jmap)`.

## Dependencies

- Phase 1 (`plans/error-model-types.md`).
- Phase 2.1 (`plans/error-model-net.md`) — JMAP routes HTTP through
  `bifrost-net`, so net's `into_account_error` and `AttemptCause`
  emission must be in place before JMAP can produce well-formed
  errors for the transport layer.

## Files to modify

`<TODO>` — discover by reading `crates/jmap/src/`. Likely candidates
based on `reference/jmap.md`:

- the internal error type module (probably `error.rs`)
- the method-error parser (where JMAP method-level error
  responses become structured types)
- the dispatch surface (request → response, possibly in a module
  named `transport.rs` or `client.rs`)
- the `Account` impl module (likely under `src/sync/` per
  `reference/jmap.md`)
- the WebSocket push handler
- the mutation pipeline (where idempotency and retry decisions
  currently get made)

## Files to delete

`<TODO>` — discover. The convergence plan calls out:

- JMAP-specific `to_recovery` helpers if any exist.
- Any string-based error classification.

## Translation surface

```rust
pub(crate) fn into_account_error(
    error: jmap::Error,
    ctx: JmapErrorContext,
) -> AccountError;
```

Where `JmapErrorContext` carries operation, scope, and any
JMAP-specific correlation IDs.

Implementation:

1. If the error is purely transport-level: delegate to
   `bifrost_net::into_account_error` with a `NetErrorContext`
   constructed from `JmapErrorContext`.
2. If the error is a JMAP method-level error: classify by method
   error code:
   - `stateMismatch` → `kind: ConcurrencyConflict`,
     `chain: [State(ConcurrencyConflict), Wire(JmapMethod {
     kind: StateMismatch })]`.
   - `forbidden` → `kind: Authorization(...)` matching the specific
     forbidden context (cross-reference JMAP RFC 8620 §3.6).
   - `accountNotFound` / `accountNotSupportedByMethod` →
     `kind: Unsupported(operation)`.
   - `unknownMethod` → `kind: Unsupported(operation)`.
   - `invalidArguments` / `invalidResultReference` →
     `kind: Request(Malformed)`.
   - `serverFail` / `serverPartialFail` / `serverUnavailable` →
     `kind: Server(...)`. `serverPartialFail` is the
     `Reconcile(PartialCompletionSignal)` case.
   - `requestTooLarge` → `kind: Request(Malformed)`.
   - Cancelled / quota — match per JMAP RFC.
3. Use the builder with operation, scope, provider, protocol set.
4. Method-error responses are always `Acknowledged` from the
   transport perspective (the server returned a response), but the
   `AttemptCause` is pushed by `bifrost-net` when the HTTP layer
   succeeded; JMAP doesn't need to re-emit it.

## Notable concerns

- **JMAP capabilities.** Per `reference/jmap.md`, JMAP supports RFC
  8620 / 8621 / 8887 / 9404 / 9425 / 9610 / 9670 and several drafts.
  Each capability set adds method errors. The classification has to
  cover every method-error code that the JMAP server can return for
  any advertised capability.
- **WebSocket push.** RFC 8887 WebSocket push has its own error
  signaling (close frames, ping timeouts). These map to
  `Cause::Transport(...)` with appropriate `TransmissionState`, and
  the `AccountErrorKind` depends on whether the disconnect is
  recoverable (transient) or permanent (auth lost).
- **Mutation pipeline.** Per `reference/jmap.md`, mutations go
  through `Email/set` with `ifInState`. State-mismatch returns
  `Retry { AfterStateRefresh, reason: ConcurrencyConflict }`. The
  pipeline must consume the new `RecoveryClass` shape.

## Tests

Per project rules. ~15-20 tests:

- One test per documented JMAP method-error code: input the
  error response, assert the produced `AccountError` has the
  expected kind + recovery + message_key.
- WebSocket close-frame classification.
- `stateMismatch` produces `Retry(AfterStateRefresh)`.
- `serverPartialFail` produces `Reconcile(PartialCompletionSignal)`.

No live JMAP server.

## Exit criteria

- Every JMAP method-error code has a defined `AccountErrorKind`
  classification.
- `into_account_error` exists and routes all internal jmap errors
  through `AccountErrorBuilder`.
- WebSocket push emits structured `AccountError` on disconnect with
  the appropriate `TransmissionState`.
- Mutation pipeline consumes new `RecoveryClass` helpers.
- Old recovery helpers removed.
- `cargo check -p bifrost-jmap` clean.
- Per-crate tests pass.

## Discovery items for the agent

`<TODO>`:

1. Current internal `jmap::Error` shape and where it's defined.
2. The exact method-error vocabulary used by the parser.
3. Where the WebSocket push handler lives and how it currently
   surfaces disconnects.
4. Whether the mutation pipeline currently has its own
   `RecoveryClass`-shaped reasoning (it does, per the convergence
   plan's mention of `recovery_for_*` helpers).
5. The set of `JmapMethod` variants that `WireCause::Jmap` will need
   to wrap.
