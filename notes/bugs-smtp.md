# bifrost-smtp: hunt findings

Scope: `crates/smtp/` - transport types (async and blocking halves, both published
surface), connection pooling, PIPELINING, DSN, message builder, LMTP, TLS handling,
examples.

All findings are closed. The final open refactor was completed in round 3, and
the two shape differences surfaced by that work were closed in round 4.

## Round 1 rulings (findings 1-7, landed)

Findings 1 through 7 are fixed and removed. Two things about that round are worth
keeping, because they are not visible from the code alone:

- **Finding 4's blocking half was refused on the merits.** The finding said
  "both halves, worse on async". Only the async half was a defect. The blocking
  `abort()` is `Shutdown::Both` on a blocking socket - a syscall that returns
  immediately - where the async `poll_shutdown` sends TLS `close_notify` and
  waits for the peer's. There is nothing to bound on the blocking side and no
  timeout was added there. `reference/smtp.md` now says so, so the "held in
  step" claim stays true.
- **A defect the round's own fix introduced, caught by the cold reviewer.** The
  finding-6 fix made `max_size` a real bound with a semaphore, and the new
  admission gate did not participate in `shutdown()`. That left a checkout
  parked on admission unwakeable, and, once a pre-shutdown checkout was
  returned and released its permit, able to dial a new connection through a
  closed pool. Closed by `admission.close()` plus `notify_waiters()` in
  `shutdown()` and a pool-state recheck after admission and before dialing.
  The blocking pool's condvar equivalent was audited and ruled already correct;
  it gained only a `notify_all()` for the case where the checked-out connection
  is never returned at all. The close pass found that ruling true for shutdown
  but false for the routine release path: `release_live` notified the condvar
  without holding the connections mutex, and a checkout's failed `try_reserve`
  followed by `wait` is atomic only against notifiers holding that mutex, so a
  broken connection dropped concurrently could fire its notify into the window
  between the two and strand the waiter (a Condvar notification is not sticky;
  the async `Notify` stores a permit, so only the blocking half was exposed).
  Closed by taking the connections lock around the notify in `release_live`.
  No hermetic test pins it - the window is between an atomic and a `wait`
  inside `connection()` and cannot be instrumented without wall-clock waits,
  the same limit the blocking shutdown test already records.

## Final round rulings

Findings 8 through 13 are fixed and removed. Four things about how that landed
are worth keeping, because they are not visible from the code alone.

**Finding 8 was fixed twice.** The first pass added `with_phase` at the
pipelined call sites one by one and missed six of them plus a seventh
asymmetry: the *negative reply* paths - a server rejecting `MAIL FROM`, a
recipient or `DATA` - still returned a bare `error::status(response)` in both
halves, and the sync `MAIL FROM` rejection path drained its reply group through
an undecorated `?` where the async twin did not. Those are the paths that
actually run when a real relay rejects a recipient, and their phase feeds
`classify_response` including the recipient-lane split, so PIPELINING still
classified an ordinary rejection differently from the non-pipelined path. The
cold reviewer caught it; the round's own new tests only exercised a body
transport failure.

The second fix is structural rather than another sweep of call sites, because a
per-call-site fix is exactly what failed here. `send_pipelined` is now a
one-line funnel over `send_pipelined_inner`, whose error type is `PhasedError`
(`client/mod.rs`): no `From<Error>` impl, no phase-less constructor. `?` on an
undecorated SMTP result does not compile inside the driver, so a boundary added
later cannot ship undecorated. It also caught a site nobody had listed - the
post-write `state().verify()` - which had been an undecorated `?` in both
halves all along. Six assertions across two paired tests pin `MailFrom`,
`RcptTo`, `DataCommand` and `DataBody`; all six were ablated and fail against
the undecorated funnel.

**Finding 9 was already closed by round 1, not by this round.** Round 1's
surplus-reply work added the sync `Broken` bracket and `finish_reply_group` to
`send_pipelined` in aa8ec44. The final round changed nothing there and should
not have claimed to. The mirror was neither necessary nor harmful - it did not
happen.

**Finding 12's four sites are ten.** Counting both I/O halves there are five
all-recipients-rejected early returns each: direct LMTP DATA and BDAT sends,
`send_smtp_batch`, `send_smtp_batch_pipelined` (the one that was already
correct) and `send_lmtp_batch`. All ten now route through one
`reset_transaction` per half, which keeps the connection when the peer
positively acknowledges the `RSET` and aborts it otherwise. A rejected `MAIL
FROM` on the pipelined path deliberately does *not* reset: it opened no
transaction.

**Finding 11 was closed without an assert.** `merge_lmtp_statuses` returns
`error::internal` on either a short or a surplus status count rather than
`expect`-ing a server-controlled invariant, matching the sibling error four
lines above it. No `debug_assert` accompanies it, and that is deliberate: the
`batch.rs` `debug_assert`-plus-release-fallback idiom exists where a *lane* has
to resolve to something, whereas this is an internal count invariant with a
real error to return.

The rest of the round:

- Round 4 aligned recipient-command transport failures on the scope-less async
  shape. The failure is correlated through its batch item id and support-only
  envelope-recipient text; `ErrorScope::Account` falsely located it at the
  account, and no typed resource scope represents an SMTP envelope address.
  The blocking half's inert context-level `Unsent` override and vestigial
  discarded recipient-address binding were removed with the false scope.
  Removing the last writer left `SmtpErrorContext::scope` permanently `None`
  and its `apply_context` branch unreachable, so the field itself went too:
  the crate now has one context constructor and no way to attach a scope, and
  the halves agree by construction rather than by matching call sites. Both
  halves pin the shape with a transcript test, each ablated by reinstating the
  scope and observed to fail.

- The proposed sans-I/O rewrite is refused for this bug-fix round. It is a
  plausible future architecture, but replacing roughly 6,600 lines of mature
  sync and async protocol drivers is not a bounded fix for the verified defects
  and would put every lifecycle invariant at risk at once. This round instead
  moved the repeated transaction cleanup behind one `reset_transaction` guard
  per I/O half, moved LMTP status merging into one shared count-checked helper,
  made EHLO/LHLO validation a constructor invariant, and made the pipelined
  phase decoration a type-level guarantee. Paired transcript tests pin the
  repaired behavior on both sides.
- The pool redesign proposal is already superseded by round 1. `max_size` now
  has admission control, async recycle has no I/O or await point, and shutdown
  participates in admission. No further pool reshape remains from the argument.
- Round 3 closed the batch phase duplication without a broader `SendProgress`
  reshape. `SmtpErrorContext` no longer has a phase field or phase-decorating
  method. Low-level conversion reads the phase only from `SmtpError`, while a
  stored negative `Response` passes one explicit phase to its classifier. The
  batch conversion sites therefore cannot write two disagreeing phases (the
  round-3 ledger said fifteen; the enumeration that round actually found twenty,
  and a fixed count in a document nobody recounts is how that drift starts). In both
  I/O halves the duplication itself is closed at the type level, so that part
  admits no runtime ablation - the former bad state no longer compiles.
  What survives as behaviour is the one explicit phase argument a stored
  negative `Response` still passes to `classify_response`, and that is now
  pinned: `rejected_recipient_classifies_in_the_recipient_lane` drives a bare
  550 with no enhanced status code, which is the only classification arm the
  phase actually steers. It was ablated - flipping the argument to `DataFinal`
  reports `Authorization(PermissionDenied)` instead of
  `NotFound(Mailbox)`. The pre-existing recipient-rejection tests use a `5.1.1`
  reply, which the enhanced table classifies the same way for every phase, so
  they never bit on this.
- EHLO and LHLO validation now lives in their constructors, so a future command
  call site cannot bypass it.
- A fixed command-buffer reserve cannot close the documented zeroization gap:
  OAuth tokens and authentication payloads are not bounded by such a reserve,
  so an oversized command could still reallocate. Closing it structurally would
  require a bounded credential contract or a different serialization design.
  The proposed few-hundred-byte reserve would only move the threshold and is
  refused as false closure. The accurate disclosure remains in
  `reference/smtp.md`.
- `PoolConfig::test_on_checkout` remains intentionally shared and inert for
  LMTP. A debug assertion would reject valid shared configuration, while making
  the config protocol-aware would reshape a published type for no behavioral
  gain. The public documentation already states the LMTP behavior.
- The cleanup worker's worst-case delayed FD release is not a protocol or
  resource-retention defect: checkout enforces expiry, and shutdown releases
  all parked connections. Tightening the sweep cadence would trade wakeups for
  earlier best-effort cleanup without changing the contract, so it is declined.
- The private `error_from_status` passthroughs were removed from both drivers.
  Both were private one-line functions; no published item was removed, renamed
  or narrowed anywhere in this round. `Ehlo::new` / `Lhlo::new` are
  `pub(crate)`, so making them fallible is not a source break for consumers,
  and `ClientId::validate` (also `pub(crate)`) is still called - from the
  constructors rather than from `hello()`.
- The durable reference assertions were corrected to describe the actual phase
  enforcement and the repaired sync/async equivalence. `reference/smtp.md`
  briefly asserted a closure that was not yet true; that is worse than an open
  finding, because it stops anyone looking.

## Things checked that are correct

Recorded so a later pass does not re-derive them:

- **Dot-stuffing** is correct for bare LF, lone CR, chunk boundaries and a dot at
  byte zero.
- **`smtp_data_size`** excludes transparency dots and the terminator line and
  reuses an existing final CRLF.
- **Header injection via header names** is closed by `is_ftext`.
- **Header injection via header values** is closed by the RFC 2047 encoder:
  `allowed_char` excludes NUL, CR and LF, so hostile text cannot open another
  field.
- **VRFY, EXPN, MAIL FROM and RCPT TO** validate single-line arguments,
  including values made with the unchecked address constructor.
- **`MailParameter::OtherRaw`** validates its keyword and value.
- **`Tls::Required`** cannot be stripped. `Tls::Opportunistic` is strippable by
  definition.
- **`ServerInfo` is replaced** after STARTTLS and AUTH, so pre-TLS capabilities
  do not leak forward.
- **LMTP per-recipient accounting** preserves original order and RCPT failures;
  unresolved accepted recipients become uncertain.
- **The `Unsent` versus `Uncertain` split** is correct on every traced envelope
  and DATA boundary.
