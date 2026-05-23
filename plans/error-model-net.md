# Error model: bifrost-net implementation plan

**Scaffold.** Fill in the details by reading `reference/net.md` and
the `crates/net/` source. Sections marked `<TODO>` are placeholders.

This is **Phase 2.1** of `plans/error-model-roadmap.md` — the first
consumer crate to migrate, because it sets the `AttemptCause`
emission convention that JMAP, Gmail, and Graph depend on.

## Scope

`bifrost-net` is the shared HTTP transport for `bifrost-jmap`,
`bifrost-gmail`, and `bifrost-graph`. It already exposes retry, rate
limiting, and observability hooks. Per the convergence plan, it owns
the `AttemptCause` emission convention: as bytes move through the
transport state machine, `bifrost-net` knows when they've left the
socket (`InFlight`), when no terminal response arrived
(`InFlight` persists), and when one did (`Acknowledged`).

What changes:

- `NetErrorContext` and the public conversion fn match the convergence
  plan's HTTP transport boundary section.
- Internal error types map cleanly into the new `AccountError`
  through `AccountErrorBuilder`.
- `AttemptCause` is pushed onto the cause chain with the correct
  `TransmissionState` based on wire-level state.
- Retry / rate-limit / observability hooks consume / produce the
  new types where they cross the boundary.

## Dependencies

- Phase 1 (`plans/error-model-types.md`) complete. `bifrost-types`
  provides `AccountError`, `AccountErrorBuilder`, `AttemptCause`,
  `TransmissionState`, `Cause`, all subkind enums.

## Files to modify

`<TODO>` — discover by reading `crates/net/src/`. Likely candidates
based on `reference/net.md`:

- `crates/net/src/error.rs` (or equivalent) — internal error type
- the public conversion boundary (the function that converts net's
  internal error into `AccountError`)
- the retry path — where `retry_after` and rate-limit signals are
  read off responses
- the transport state machine — where `TransmissionState` transitions

## Files to delete

`<TODO>` — discover. Any old recovery / classification helpers that
the convergence plan replaces.

## Translation surface

```rust
pub fn into_account_error(
    error: net::Error,
    ctx: NetErrorContext,
) -> AccountError;
```

Implementation:

1. Classify `net::Error` into an `AccountErrorKind` (transport class,
   server class, etc.).
2. Construct a `Cause::Transport(TransportCause { kind, message })`
   or `Cause::Server(...)` matching the error.
3. Push `Cause::Attempt(AttemptCause { transmission_state })` with
   the state derived from the transport state machine:
   - `Unsent` if no bytes ever left the socket (DNS, TLS handshake
     failed locally, connection refused).
   - `InFlight` if bytes left and no terminal response arrived
     (read/write timeout mid-stream, connection reset mid-stream).
   - `Acknowledged` if the server returned a terminal HTTP response
     (any status, even error responses).
4. Use `AccountErrorBuilder::new(kind, primary_cause)` and chain the
   rest, including:
   - `.operation(ctx.operation)` when present
   - `.scope(ctx.scope)` when present
   - `.provider(ctx.provider)`
   - `.protocol(ctx.protocol)`
   - `.request_id`, `.trace_id`, `.status`, `.native_code` from
     response headers / status
   - `.retry_not_before(parsed)` when the response carries
     `Retry-After`
   - `.throttle_scope(...)` when the response indicates a documented
     throttle domain for the provider (Graph tenant-wide, Gmail
     per-user — protocol-specific knowledge that lives here)
5. `.build()`.

JMAP, Gmail, and Graph call this when only transport-level signal is
available. When they have a more precise interpretation (parsed
method error, structured error body), they build their own
`AccountError` via the builder directly, not through this function.

## Notable concerns

- **Transmission-state detection is the load-bearing change.** The
  current `net::Error` probably does not distinguish "DNS failed"
  from "connection dropped mid-response." Phase 2.1 must wire this
  through, possibly by adding state to the transport state machine
  itself. `<TODO>`: confirm by reading the connection / retry code.
- **`retry_after` parsing.** RFC 7231 `Retry-After` is either an
  HTTP-date or a seconds count. Convert to `SystemTime`.
- **Throttle scope mapping.** Graph's throttling is documented
  tenant-wide for app-level requests; user-level for delegated.
  Gmail's is per-user-per-quota-unit. This crate knows the
  provider-level throttle vocabulary because it sees the response
  headers (`x-ms-throttle-scope`, `Retry-After`, etc.). The
  per-protocol crates rely on `bifrost-net` to populate
  `throttle_scope`; they do not duplicate this logic.
- **Observability hooks.** Existing logging / tracing / metrics
  emit on retry / failure. Update to emit `AccountError` fields
  (kind, recovery, message_key) instead of stringified internal
  error values.

## Tests

Per project rules. ~10-15 tests:

- `Unsent` classification: simulate connect-refused / DNS-fail
  paths and assert `TransmissionState::Unsent`.
- `InFlight` classification: simulate read-timeout-after-send and
  assert `TransmissionState::InFlight`.
- `Acknowledged` classification: simulate any HTTP response and
  assert `TransmissionState::Acknowledged`.
- `Retry-After` parsing: seconds form, HTTP-date form, missing.
- `throttle_scope` derivation per provider.
- Status-code mapping: 401 → `Authentication(_)`, 403 →
  `Authorization(_)`, 404 → `NotFound(_)`, 409 →
  `ConcurrencyConflict`, 410 → `SyncState(CursorInvalid)` only when
  context indicates a cursor scope, 429 → `Server(RateLimited)`,
  5xx → `Server(Unavailable)` or `Server(Error)`.

No live HTTP. Construct synthetic `net::Error` values directly.

## Exit criteria

- `into_account_error` exists, matches the convergence plan's
  signature.
- Every `net::Error` variant has a defined classification into
  `AccountErrorKind`.
- `AttemptCause::transmission_state` is set correctly for every
  variant by the transport state machine.
- `Retry-After` and `throttle_scope` populated where the wire
  protocol provides them.
- Old recovery / classification helpers removed.
- Patches against this crate match `plans/error-model-net.md`'s
  exit criteria. Compilation and per-crate tests are not run at
  this phase; Phase 3 (workspace integration) is where `brokkr
  check` runs and tests execute.

## Discovery items for the agent

`<TODO>`:

1. Where `NetErrorContext` is currently defined.
2. What the current `net::Error` variants are.
3. Whether the transport state machine already tracks transmission
   state or needs new state added.
4. Where retry / rate-limit logic currently lives (so it can be
   updated to emit through `AccountError`'s `RetryAdvice` rather
   than its own ad-hoc surface).
5. Whether `bifrost-net` currently has a notion of "throttle scope"
   that we can reuse, or whether it needs to be introduced.
