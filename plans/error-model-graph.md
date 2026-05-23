# Error model: bifrost-graph implementation plan

**Scaffold.** Fill in the details by reading `reference/graph.md` and
the `crates/graph/` source. Sections marked `<TODO>` are placeholders.

This is **Phase 2.2** of `plans/error-model-roadmap.md`, one of the
five consumer crates that migrate in parallel.

Per `CLAUDE.md`, `bifrost-graph` is listed as "planned (not yet
present)." Verify before starting.

## Scope

`bifrost-graph` is the Microsoft Graph account implementation. Per
`reference/graph.md`: delta-token sync, webhook push with renewal
health worker plus EWS streaming fallback, cursor envelope and
validation, `If-Match` etag mutations, error mapping.

What changes:

- **Substring-matching recovery is deleted.** Per the convergence
  plan, the current implementation matches on Graph error message
  strings to choose recovery; this is a documented anti-pattern
  going away. Recovery comes from structured error codes and
  the central mapping.
- Internal error type → `AccountError` via the builder.
- Graph error body (`error.code`, `error.message`,
  `error.innerError`) classified by code, not by message text.
- Delta-token rejection (`Gone` 410 on delta endpoint) routes to
  `RecoveryClass::Engine(EngineDirective::RestartScope(_))`.
- Webhook subscription health failures map to the appropriate
  recovery class.
- EWS streaming fallback path stays; only its error reporting
  changes.

## Dependencies

- Phase 1 (`plans/error-model-types.md`).
- Phase 2.1 (`plans/error-model-net.md`) — Graph routes HTTP through
  `bifrost-net`.

## Files to modify

`<TODO>` — discover. Likely candidates based on `reference/graph.md`:

- the substring-matching recovery helper (DELETE)
- the internal error type module
- the delta-token sync engine
- the webhook handler and renewal worker
- the EWS streaming fallback path
- the `If-Match` etag mutation pipeline

## Files to delete

`<TODO>` — discover. Confirmed targets from the convergence plan:

- the substring-matching recovery helper(s)

## Translation surface

```rust
pub(crate) fn into_account_error(
    error: graph::Error,
    ctx: GraphErrorContext,
) -> AccountError;
```

Implementation:

1. If purely transport / HTTP: delegate to
   `bifrost_net::into_account_error`.
2. If Graph-specific error body:
   - `error.code: "InvalidAuthenticationToken"` →
     `Authentication(Expired)` or `Authentication(Revoked)`
     depending on inner details.
   - `error.code: "AccessDenied" / "Forbidden"` →
     `Authorization(PermissionDenied)`.
   - `error.code: "AccessRestricted" / "ConditionalAccess..."` →
     `Authorization(ConditionalAccessBlocked)`.
   - `error.code: "AdminConsentRequired"` →
     `Authorization(AdminConsentRequired)`.
   - `error.code: "MailboxNotEnabledForRESTAPI"` →
     `Authorization(MailboxNotLicensed)`.
   - `error.code: "MailboxStoreUnavailable"` →
     `Access(MailboxUnavailable { kind: Transient })` or `Permanent`
     depending on inner details.
   - `error.code: "ResyncRequired"` →
     `SyncState(CursorInvalid)` producing
     `Engine(RestartScope(scope))`.
   - `error.code: "TooManyRequests"` or 429 →
     `Server(RateLimited)` with throttle scope per Graph's
     documented model (typically `ThrottleScope::Tenant` for app
     auth, `ThrottleScope::Account` for delegated).
   - `error.code: "GenericFileError"` or 5xx →
     `Server(Unavailable)`.
   - `error.code: "PreconditionFailed"` or 412 →
     `ConcurrencyConflict`.
   - 410 on delta endpoint → `SyncState(CursorInvalid)` →
     `Engine(RestartScope(scope))`.
   - 404 → `NotFound(...)`.
3. Stamp `provider: Provider::Microsoft`,
   `protocol: Protocol::Graph`.

## Notable concerns

- **Substring matching deletion is the headline change.** Every
  branch that currently reads `error.message` and tests substrings
  is replaced by a match on `error.code`. If the existing code has
  cases where the substring is the only available signal (Graph
  occasionally returns generic codes with detail only in the
  message), document them in the discovery items and decide per
  case whether to:
  (a) classify generically (`UnknownPermanent` if terminal,
      `ProviderRefused` if structured),
  (b) keep a narrowly-targeted substring match as the absolute
      last resort with a comment explaining why and a TODO for
      Graph to fix the upstream code, or
  (c) escalate to library bug status and request Graph documentation
      for a stable code.
  Default to (a) unless the upstream behavior is well-documented.
- **Delta-token rejection.** Graph's `@odata.deltaLink` can become
  invalid (410 Gone). Maps to `Engine(RestartScope(_))`.
- **Webhook push and renewal.** Webhooks have subscription
  lifetimes; the renewal worker watches health. Renewal failures
  map to `Reconcile` (probe subscription state) or
  `Engine(RestartScope)` (recreate forces a sync restart).
- **EWS streaming fallback.** When Graph webhook health degrades,
  the impl falls back to EWS streaming. The fallback path produces
  its own errors with different shapes; classification still goes
  through the same builder.
- **`If-Match` etag mutations.** Etag mismatches produce
  `ConcurrencyConflict` → `Retry(AfterStateRefresh)`. The
  mutation pipeline consumes this through the new `RecoveryClass`.
- **Throttle scope.** Graph documents tenant-wide throttles for
  app authentication and per-mailbox throttles for delegated
  authentication. Surface the right `ThrottleScope` per auth
  flow.

## Tests

Per project rules. ~15-20 tests:

- One test per `error.code` value in the mapping above.
- Delta-token rejection produces `Engine(RestartScope(_))`.
- 410 on delta vs 410 on other resources (delta is cursor-invalid;
  other 410 is `ProviderRefused`).
- Webhook subscription expiry produces the expected recovery class.
- `If-Match` 412 produces `Retry(AfterStateRefresh)`.
- Throttle scope per auth flow.
- EWS fallback path error classification.

No live Graph. Construct synthetic Graph error response payloads.

## Exit criteria

- All substring-matching recovery deleted.
- Every Graph `error.code` value has a defined classification (or
  a documented fallback to `ProviderRefused` / `UnknownPermanent`).
- `into_account_error` exists and routes through
  `AccountErrorBuilder`.
- Delta-token rejection emits the correct `EngineDirective`.
- Webhook renewal failure emits structured `AccountError`.
- EWS fallback path emits structured `AccountError`.
- Patches against this crate match this plan's exit criteria
  (or noted as not-yet-implemented if the crate is still
  planned). Compilation and per-crate tests are not run at this
  phase; Phase 3 (workspace integration) is where `brokkr check`
  runs and tests execute.

## Discovery items for the agent

`<TODO>`:

1. Whether `bifrost-graph` is implemented yet.
2. Current `graph::Error` shape if the crate exists.
3. Where substring matching is performed and all call sites.
4. The exhaustive set of `error.code` values the implementation
   handles (cross-reference against Microsoft Graph error docs).
5. The webhook renewal worker's failure-mode taxonomy.
6. Whether the EWS fallback shares error infrastructure with the
   primary Graph path or has its own.
7. The set of `GraphSignal` variants for `WireCause::Graph`.
