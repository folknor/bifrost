# Orchestration carry-forward

State the bug-hunt loop needs but no agent in it can see. Every brief carries the
relevant slice forward, because each agent arrives with only its own round.

This is not a history. When a piece of machinery is superseded, replace the entry
rather than appending to it.

## Standing loop conventions

- A change may alter or replace a published API. It may never remove one - no
  public type, variant, field, method or function. Three cold reviewers have
  objected to shape changes on source-break grounds; the owner overruled all
  three. Shape changes are fine, removals are not.
- Closure by verification is not closure. "Not a current defect" is weaker than
  "fixed": the invariant must be enforced by a type, a constructor or a guard.
- An assert on account-authored data is the wrong tool. A recoverable resync
  beats a process abort.
- Refusing a finding with a reason is a good outcome. Two have been rejected on
  the merits so far, both recorded below.

## From the `bugs-types.md` arc (closed, 2004c2c..a2b5fb4)

Machinery later work may build on and must not break:

- **ErrorScope** serializes through a single `scope_fields` projection whose
  declared field count comes from the projection itself. A second parallel match
  computing a count WAS the original bug. Its resource fields are typed ids, not
  Strings. There is no `Deserialize`; serialization is one-way into support
  exports.
- **AccountError construction.** `CauseChain` construction is fallible and wired
  to `EmptyChain`, which is unreachable because `try_build` always pushes the
  primary cause first. `into_builder` preserves idempotency and throttle
  overrides. `set_telemetry_token` assigns unconditionally, so a rejected
  replacement clears rather than preserving a stale token; telemetry ids are the
  bounded `TelemetryToken`. `validate_batch_input` takes the operation as a
  required parameter, because centralizing the construction is what made losing
  that context possible.
- **Recovery.** Throttle reconciliation keeps `ThrottleScope` and `retry_hint`
  via `ReconcileReason::ThrottledMidFlight`; reconcile sleep comes from the
  shared `recovery::reconcile_delay`. `RecoveryClass::is_terminal` is an
  exhaustive match, not a negation. `handle_drive_outcome` routes
  `Err(Error::Account)` through `plan_recovery` rather than logging and
  re-polling an unchanged cursor forever.
- **Cursor envelope versioning.** Decode is the SINGLE migration boundary:
  `decode_change_payload` routes through `migrate_change_cursor`, applies the
  fixup chain and stamps `CHANGE_CURSOR_ENVELOPE_VERSION`. The codec is therefore
  the only code in the workspace that ever holds a non-current cursor, and
  `validate_envelope` stays strict equality everywhere else. Bumping
  `ENGINE_VERSION` means adding both a fixup and a byte fixture. Do not widen the
  gate to the migration window - that was considered and rejected, because it
  spreads knowledge of historical layouts across every consumer.
- **Coverage and inventory.** `InventoryCompletion::complete` demands an exact
  `CoverageDomain`; unsupported inventory emits only `Terminated`, never a `Done`
  carrying a full-scope complete claim. Degraded coverage is structurally
  non-empty. UID partitions are half-open with `u64` endpoints. Batch-boundary
  violations classify as `Protocol(ContractViolation)`, which is terminal, and
  publish `Terminated` to subscribers.
- **Fingerprints.** Flag hashing goes through the crate-owned
  `canonical_flags_hash`, comparable within one provider only (`\Seen` vs
  `$seen` vs `UNREAD`). `InventoryEntry::differs_from` is the written contract
  for what constitutes "changed".

Disclosed residuals, deliberately left:

- `Batch` and `InventoryBatch` have guarded `try_new` constructors, but their
  fields stay public because making them private would delete published fields.
  Enforcement is the consumer-side boundary check in sync, and
  `reference/types.md` says so honestly rather than claiming the constructor is a
  gate.

Decisions already ruled on - argue against them if you have a reason, but do not
silently reopen them:

- The idempotency table deliberately does not mark `ContainerRename`. IMAP
  `RENAME` keys on the old mailbox NAME, not a stable id, so replaying a
  dropped-but-landed rename addresses a mailbox that no longer exists and reports
  a spurious permanent failure for an operation that succeeded.
- Nor the Google Calendar composite move-then-patch path, which carries an
  explicit `idempotency_override(false)` because replaying it is unsafe.
- `ReconcileAdvice` has carried `#[non_exhaustive]` since the original
  error-model commit, and `ReconcileGuidance` is the plain constructible one. A
  finding claiming the reverse rested on an inverted premise.
