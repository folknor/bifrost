# Error model: bifrost-smtp implementation plan

**Scaffold.** Fill in the details by reading `reference/smtp.md` and
the `crates/smtp/` source. Sections marked `<TODO>` are placeholders.

This is **Phase 2.2** of `plans/error-model-roadmap.md`, one of the
five consumer crates that migrate in parallel.

## Scope

`bifrost-smtp` is an SMTP and LMTP client. Per `CLAUDE.md`:
Lettre-derived, native-tls only. Does NOT route through
`bifrost-net` (no HTTP). Per `reference/smtp.md`: PIPELINING, DSN
support, message builder.

What changes:

- Internal error type → `AccountError` via the builder.
- Enhanced status codes (RFC 3463) become structured causes via
  `Cause::Wire(WireCause::Smtp(EnhancedStatusCode { ... }))`.
- Multi-recipient sends become the canonical use case for the
  Vec-batch `Result<BatchOutcome<()>, AccountError>` shape — this
  is the place the convergence plan's batch contract actually
  ships first.
- LMTP per-recipient final-state reporting feeds directly into the
  per-item `BatchOutcome` lanes.

## Dependencies

- Phase 1 (`plans/error-model-types.md`).
- **NOT** dependent on Phase 2.1 (`bifrost-net`) — SMTP has its own
  TCP / TLS layer.

## Files to modify

`<TODO>` — discover. Likely candidates based on `reference/smtp.md`:

- internal error type module
- the SMTP transport layer (where wire-level state lives)
- the LMTP final-state collector
- the multi-recipient send path
- the message builder (if it produces local validation errors)

## Files to delete

`<TODO>` — discover. Any SMTP-specific recovery helpers.

## Translation surface

```rust
pub(crate) fn into_account_error(
    error: smtp::Error,
    ctx: SmtpErrorContext,
) -> AccountError;
```

Multi-recipient send returns a different shape entirely:

```rust
pub async fn send(
    &self,
    envelope: Envelope,
    recipients: Vec<BatchItem<Recipient>>,
) -> Result<BatchOutcome<()>, AccountError>;
```

The Vec-batch invariants from the convergence plan apply:

1. `Err(AccountError)` only when nothing crossed the side-effect
   boundary (TCP connect failed, TLS failed, MAIL FROM rejected,
   etc.).
2. `Ok(BatchOutcome)` accounts for every recipient exactly once
   across `succeeded` / `failed` / `uncertain`.
3. Transmission state for any individual recipient maps to the
   right lane:
   - RCPT TO accepted, server later confirmed (LMTP per-recipient
     250) → `Succeeded`.
   - RCPT TO rejected with permanent 5yz code → `Failed`.
   - RCPT TO rejected with transient 4yz code → `Failed` with the
     recovery class indicating retryability via `RecoveryClass` on
     the carried `AccountError`.
   - Connection dropped after some RCPT TOs accepted, before
     final DATA confirmation → all unconfirmed recipients in
     `Uncertain`.
4. Locally invalid recipients (malformed addresses,
   non-unique / empty `BatchItemId`) abort with `Err(AccountError {
   kind: Request(BatchInputInvalid), ... })`.

## Notable concerns

- **Enhanced status codes carry recovery signal.** RFC 3463 status
  codes like `5.1.1` (bad destination mailbox) vs `4.7.0` (transient
  policy reject) inform `RecoveryClass`. The classification table
  needs an entry per relevant status code class.
- **PIPELINING.** With PIPELINING, multiple commands fly before
  responses. Per-command transmission state needs careful tracking
  to distinguish "command sent, response pending" from
  "command sent, response received." Each command can resolve
  independently.
- **LMTP per-recipient finalization.** LMTP issues one DATA response
  per recipient (unlike SMTP's single DATA response for all). This
  maps cleanly to per-item lanes — easier than SMTP because the
  protocol literally yields per-recipient outcomes.
- **DSN delivery reports.** DSN-formatted bounce reports arrive
  asynchronously and are out of scope here — they aren't part of
  the send call's return. (Future scope: surface DSN reports as
  events on the account's stream.)
- **No `bifrost-net` dependency.** Transport state machine is
  `bifrost-smtp`'s own.

## Tests

Per project rules. ~15-20 tests:

- Enhanced status code mapping table.
- TCP / TLS error classification with correct `TransmissionState`.
- Multi-recipient: all-success.
- Multi-recipient: mixed success / fail.
- Multi-recipient: connection drop produces `Uncertain` for
  unconfirmed recipients.
- Multi-recipient: locally-invalid recipient (malformed address)
  produces `Err(Request(BatchInputInvalid))`.
- Multi-recipient: duplicate `BatchItemId` produces
  `Err(Request(BatchInputInvalid))`.
- LMTP per-recipient finalization populates lanes correctly.
- PIPELINING preserves per-command transmission state.

No live SMTP server. Construct synthetic server response streams.

## Exit criteria

- Every relevant RFC 3463 status code class has a defined
  classification.
- `into_account_error` exists for single-recipient / control-channel
  errors.
- Multi-recipient `send` returns
  `Result<BatchOutcome<()>, AccountError>` per the convergence
  plan.
- LMTP per-recipient finalization feeds the `BatchOutcome` lanes.
- Connection-drop-mid-stream produces `Uncertain` for unconfirmed
  recipients.
- Locally-invalid input rejected as `Err(BatchInputInvalid)`.
- Old recovery helpers removed.
- Patches against this crate match this plan's exit criteria.
  Compilation and per-crate tests are not run at this phase;
  Phase 3 (workspace integration) is where `brokkr check` runs
  and tests execute.

## Discovery items for the agent

`<TODO>`:

1. Current `smtp::Error` shape.
2. How LMTP per-recipient responses are currently surfaced (they
   may already have a per-recipient shape that just needs renaming).
3. The PIPELINING command-tracking implementation.
4. Whether the message builder currently performs local validation
   that produces structured errors we can hook `BatchInputInvalid`
   into.
5. The set of `EnhancedStatusCode` variants for `WireCause::Smtp`.
