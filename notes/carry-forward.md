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

## From the `bugs-net.md` arc (closed, a2b5fb4..5407925)

The arc's own signature defect, worth stating first because it fired in six
consecutive rounds and once more in the close pass: **a fix gets wired on the
success path, or the path the finding named, and the error path or the adjacent
path keeps the old behaviour.** A deadline that bounded every wait except the
metering wrapper's; accounting that counted error-path bytes but never charged
them against the cap; a governor that gained a key dimension at registration but
not at selection; an EWS tally written only on success. Every one was caught by
the cold reviewer, never by the fix pass that wrote it.

Machinery later work may build on and must not break:

- **RequestDeadline** is a genuine overall request deadline surviving retries and
  redirects. Its instant is unreachable outside the impl; every wait routes
  through `bound` or `bound_body`. Do not reintroduce raw instant arithmetic.
  `AttemptBudget`, `AuthBudget` and `RedirectBudget` own their own arithmetic,
  and the redirect arm resets by construction.
- **The body-path ordering** is `record_bytes_in`, then `deadline.check_body`,
  then `deadline.bound_body` around `ByteBucket::consume`. Metering happens
  BEFORE the deadline check, deliberately: those bytes came off the wire whether
  or not we may hand them up. Both `wrap_metered` and the error-path drains in
  `read_capped_response_body` follow it, so every byte counted is also charged
  against the per-account cap. A mid-body deadline expiry reports
  `Timeout { Acknowledged }` mapping to `Protocol(PartialResponse)`, always as an
  `Err`, so no prefix is returned as a complete body.
- **Classification.** The header timeout returns `InFlight`, correctly, because
  it can fire after the body was written; `connect_timeout` reaches the client
  builder so `Unsent` comes from real evidence rather than a guess.
- **Auth.** The 401 refresh is single-flight and cancellation-safe: there is no
  await point between the `Refreshing` transition and the driver spawn, so a
  dropped request future strands no waiter and poisons no state.
- **The governor** is keyed on `(host, quota_scope)`.
  `RequestBuilder::quota_scope` names the bucket per request and wins for every
  hop; the account declaration is the default, first-wins with a warning naming
  the override. Every ticket path checks bucket generation against ticket
  generation, and a `RateDebit` captures its own host, scope and generation, so a
  redirect cannot refund the wrong bucket. Registration rejects `burst = 0`.
- **Per-request accounting.** A response carries the bytes actually read for that
  request. `RequestBuilder::count_bytes_into` hands the caller its own
  `RequestByteCounter`, which is the only way to read the number back after an
  `Err` - use it rather than reading a count off a response that error paths
  never produce. Consumers aggregate with a batch-scoped `ByteTally` per crate,
  recorded at that crate's wire funnel and cleared per batch. JMAP reaches the
  count through a defaulted `HttpTransport::api_request_measured`, which is why
  its scripted doubles needed no change.
- **Redirect passthrough** bodies reach the caller through `into_byte_stream` and
  are counted. `ScriptedDispatch` defers its build, snapshot and step pop into
  the async block, so an unpolled dispatch future no longer consumes a step.

Disclosed exclusions, deliberately left:

- OAuth issuer traffic through the caller's own `TokenSource` is neither metered
  nor capped; closing it changes a published contract. `reference/net.md`
  discloses it.
- Long-lived EWS streaming and `StreamingResponse`'s counter stay out of batch
  totals, because a stream's count is only as complete as the caller's draining
  and a partial number must never be published as a total.
- Three sites report `bytes_in: 0` correctly because they perform no request at
  all: Gmail's constant `discover_cursor_scopes`, JMAP's session-derived
  `cursor_scopes`, and the two locally-rejected mutation lanes. Each says so at
  the call site.
- `NetErrorContext` and `FinalResponse` are deliberately not `#[non_exhaustive]`:
  the former is consumer-constructed with no constructor, the latter is
  crate-produced evidence rather than configuration.

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
