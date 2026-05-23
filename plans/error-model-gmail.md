# Error model: bifrost-gmail implementation plan

**Scaffold.** Fill in the details by reading `reference/gmail.md` and
the `crates/gmail/` source. Sections marked `<TODO>` are placeholders.

This is **Phase 2.2** of `plans/error-model-roadmap.md`, one of the
five consumer crates that migrate in parallel.

Per `CLAUDE.md`, `bifrost-gmail` is listed as "planned (not yet
present)." Verify whether it exists in `crates/gmail/` before
starting. If it does not, this scaffold is a forward design that
becomes the implementation plan when the crate is created.

## Scope

`bifrost-gmail` is the Gmail account implementation. Per
`reference/gmail.md`: history-id seeded sync, Cloud Pub/Sub push
with renewer and health stream, mutation pipeline with flag
canonicalization and TRASH fallback, error mapping to the recovery
taxonomy.

What changes:

- Internal error type → `AccountError` via the builder.
- The `account_error_from_template` helper (called out by the
  convergence plan as something that must go) is deleted; every
  construction goes through the new builder.
- History-id rejection (Gmail's "history not available" response)
  routes to `RecoveryClass::Engine(EngineDirective::RestartAccount)`.
- Cloud Pub/Sub subscription health failures map to the appropriate
  recovery class (most are `Reconcile` or `Engine`-action level).
- TRASH fallback path stays; only its error reporting changes.

## Dependencies

- Phase 1 (`plans/error-model-types.md`).
- Phase 2.1 (`plans/error-model-net.md`) — Gmail routes HTTP through
  `bifrost-net`.

## Files to modify

`<TODO>` — discover. Likely candidates based on `reference/gmail.md`:

- the `account_error_from_template` helper (DELETE)
- the internal error type module
- the history-id sync engine
- the Pub/Sub push handler and renewer
- the mutation pipeline (flag canonicalization, TRASH fallback)

## Files to delete

`<TODO>` — discover. Confirmed targets from the convergence plan:

- `account_error_from_template` (or equivalent template-based
  classification helper)

## Translation surface

```rust
pub(crate) fn into_account_error(
    error: gmail::Error,
    ctx: GmailErrorContext,
) -> AccountError;
```

Implementation:

1. If purely transport / HTTP: delegate to
   `bifrost_net::into_account_error`.
2. If Gmail-specific error body (`error.code` + `error.message` +
   structured details):
   - `400` + reason `invalidQuery` → `Request(Malformed)`.
   - `400` + reason `failedPrecondition` (on history-id mismatch) →
     `SyncState(CursorInvalid)` with scope, producing
     `Engine(RestartScope(scope))` or `Engine(RestartAccount)`
     depending on context.
   - `401` → `Authentication(...)`.
   - `403` + reason `quotaExceeded` →
     `Server(QuotaExhausted)` with throttle scope per Gmail's
     documented per-user-per-quota model.
   - `403` + reason `forbidden` (resource access) →
     `Authorization(PermissionDenied)`.
   - `404` → `NotFound(...)`.
   - `412` precondition failed → `ConcurrencyConflict`.
   - `429` → `Server(RateLimited)` with throttle scope.
   - `5xx` → `Server(Unavailable)` or `Server(Error)`.
3. Stamp `provider: Provider::Gmail`, `protocol: Protocol::Gmail`.

## Notable concerns

- **`account_error_from_template` deletion.** Per the convergence
  plan, this helper currently exists and constructs errors from
  strings / templates. Every call site needs replacement with
  builder usage. The pattern that drove the template — combining
  recurring text fragments — is replaced by stable `message_key`s
  and the structured `DiagnosticText` accessors.
- **History-id rejection.** Gmail's `historyId` can become stale
  beyond recovery (full re-sync required). This maps to
  `RecoveryClass::Engine(EngineDirective::RestartAccount)` when the
  cursor is account-wide, or `Engine(RestartScope(scope))` if
  scoped.
- **Pub/Sub push.** Pub/Sub subscriptions have separate failure
  modes — subscription deleted, expired, push endpoint unreachable.
  These map to `Reconcile` (probe Pub/Sub state, recreate if
  needed) or `Engine(RestartScope)` (subscription recreation
  forces a sync restart).
- **TRASH fallback.** Per `reference/gmail.md`, Gmail's mutation
  pipeline falls back to TRASH for certain ops. The fallback path
  shouldn't surface as an error; it's an internal recovery.
  Visible only if both the primary path and the TRASH fallback
  fail.
- **Throttle scope.** Gmail's quotas are per-user per
  quota-unit (mail send vs message read are separate quotas). When
  surfacing `Server(QuotaExhausted)`, the `throttle_scope` field
  should reflect the documented quota model — most often
  `ThrottleScope::Account`.

## Tests

Per project rules. ~15-20 tests:

- One test per Gmail-specific error reason code.
- History-id stale produces `Engine(RestartAccount)` or
  `Engine(RestartScope)`.
- `quotaExceeded` produces `Server(QuotaExhausted)` with the
  correct throttle scope.
- Pub/Sub subscription dead produces the expected recovery class.
- TRASH fallback path: primary fails, fallback succeeds → no
  error surfaced.
- TRASH fallback path: both fail → single combined `AccountError`
  with both attempts in the cause chain.

No live Gmail. Construct synthetic Gmail error response payloads.

## Exit criteria

- `account_error_from_template` deleted.
- Every Gmail error reason has a defined classification.
- `into_account_error` exists and routes through
  `AccountErrorBuilder`.
- History-id rejection emits the correct `EngineDirective`.
- Pub/Sub push handler emits structured `AccountError`s.
- TRASH fallback paths emit single, well-formed errors only when
  both attempts fail.
- Patches against this crate match this plan's exit criteria
  (or noted as not-yet-implemented if the crate is still
  planned). Compilation and per-crate tests are not run at this
  phase; Phase 3 (workspace integration) is where `brokkr check`
  runs and tests execute.

## Discovery items for the agent

`<TODO>`:

1. Whether `bifrost-gmail` is implemented yet (per `CLAUDE.md`,
   "Planned (not yet present)" — but the convergence plan
   references `account_error_from_template` as if it exists, so
   confirm).
2. Current `gmail::Error` shape if the crate exists.
3. Where `account_error_from_template` is defined and all its call
   sites.
4. The Gmail error response parser and the reason codes it covers.
5. The set of Pub/Sub push failure modes the renewer handles.
6. The set of `GmailSignal` variants for `WireCause::Gmail`.
