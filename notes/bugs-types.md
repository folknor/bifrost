# bifrost-types: hunt findings

Scope: `crates/types/` - the `Account` trait surface, the `AccountError` model,
`BatchOutcome`, diagnostics tiers, cursor/checkpoint/stream envelopes, coverage
ledger, mutation result types.

Hunter note: `mime/` was not audited in depth - a self-contained parser/renderer
with its own tests, orthogonal to the contract questions asked. Worth a second
pass if wanted.

## 17. `FlagOp::Patch { add, remove }` permits the same flag in both sets

`mutation.rs:179`. The outcome is provider-dependent order-of-application; nothing
rejects it and nothing documents a precedence rule. Same class: `Add(empty)` is a
wire round-trip that means nothing.

**Confidence: medium.**

## 25. `ReconcileAdvice` is not `#[non_exhaustive]` but is unconstructable downstream anyway

`error/recovery.rs`. `ReconcileGuidance` is `#[non_exhaustive]` while
`ReconcileAdvice`, which owns a `guidance: ReconcileGuidance` field, is not. An
external consumer can therefore name `ReconcileAdvice` in a struct literal and
still never complete it, because it cannot construct the `guidance` value the
literal requires. The omitted marker buys nothing - the struct is effectively
sealed regardless - and it reads as a deliberate "this shape is stable" promise
that the nested type contradicts. Either mark `ReconcileAdvice`
`#[non_exhaustive]` too (making the seal explicit and honest) or give
`ReconcileGuidance` a public constructor so the un-marked struct is actually
buildable. Note `RetryAdvice` next door for the same question.

Surfaced during round 1 of this ledger; deliberately not fixed there, because
choosing between the two answers is a published-surface decision.

**Confidence: high** on the inconsistency, **medium** on which way to resolve it.

## 26. `bifrost-caldav` files a collection URL as a `CalendarId`

`crates/caldav/src/account.rs`. CalDAV builds `ErrorScope::Calendar { id }` with
the calendar *collection URL* as the id. Round 1 converted `ErrorScope`'s
id-bearing variants from `String` to the typed account ids, so this now types as
a `CalendarId` - and a `CalendarId` everywhere else in the workspace is the
provider's calendar identifier, not a href. The semantics are pre-existing and
unchanged; the newtype is what makes the mismatch visible. A consumer that
correlates `ErrorScope::Calendar` against ids from the calendar listing gets no
match on CalDAV accounts.

Resolution is a real decision, not a rename: either CalDAV's `CalendarId` IS its
collection URL everywhere (in which case the listing surface should be checked to
confirm it agrees, and the invariant written down), or the scope should carry the
listing's id and the URL should move to diagnostic text.

**Confidence: high** that the values differ in kind; **unknown** whether they
differ in practice until the CalDAV listing path is checked.

## 27. The fusion inventory-cursor agreement guard has no test reaching it

Round 3 (finding 14) added a bidirectional guard in
`crates/sync/src/multiplexer/fusion.rs` enforcing that `is_inventory_cursor` and
`inventory_resume_stream` agree: it errors both when the classifier says yes and
the resume hook says no, and when the reverse holds. The guard is real code, not
an observation, but nothing exercises it. `InventoryFusion` is not exported from
`bifrost_sync`, so the existing `RecorderAccount` integration harness cannot
reach it, and an in-file stub would mean hand-writing the roughly 34 required
trait methods. The round-3 agent judged that not worth doing for a two-branch
guard and flagged it as the one verification gap of that round.

This is the "seams beat review" shape: the coupling is exactly the kind that has
produced a defect in a later round elsewhere in this loop. The fix is a seam - a
minimal test double reachable from the fusion path, or a narrow `pub(crate)`
export plus an in-crate test - not more review.

**Confidence: high** that the gap exists; the guard's own correctness was read
and looks right.

## 28. No written contract for which `InventoryEntry` fields a consumer must diff

Round 3 narrowed JMAP's `flags_hash` to keyword state only, which is correct -
it previously overloaded the field with `mailboxIds`, `receivedAt` and `size`.
The consequence is that `InventoryEntry::memberships` is now the only carrier of
mailbox membership in the JMAP diff path.

`Fingerprint`'s own doc invites a consumer to diff on the fingerprint alone
("compares `local.fingerprint != server.fingerprint` to decide whether to
refetch"). A consumer that does exactly that sees no signal when a JMAP message
moves between mailboxes with no keyword change. The old overloading masked this;
the narrowing exposes it. `flags_hash` is also comparable within one provider
only (`\Seen` vs `$seen` vs `UNREAD`), which round 3 documented on the type after
the fix pass had wrongly called it cross-provider.

The defect is the missing consumer-side contract, not the narrowing: nothing
states which fields together constitute "changed". Two rounds have now touched
this area without writing it down.

**Confidence: high.**

## The structural story

Two things dominate, and both are bigger than any individual defect above.

**`PimMethodSupport` is a 60-field hand-maintained mirror of the trait surface
with no mechanical link to it.** `capabilities.rs:151-276`. Nothing checks that a
`false` flag implies the method returns `Unsupported`, or that a `true` flag
implies it does not; the mirror is already incomplete (`send_raw_message`,
`repair_inventory`, `bulk_move_from`, `open_blob_range` have documented gating
with no flag, or a flag on a different struct). Six protocol crates x sixty bools
is ~360 hand-maintained facts that can each be wrong in a way no test can catch,
and consumers must consult the mirror *and* handle `Unsupported` anyway, per the
capabilities doc. The rewrite the hunter would make: keep one runtime query -
`fn supports(&self, op: AccountOperation) -> bool` with a default derived from a
per-impl `AccountOperation` set - so the capability answer and the error answer
are the same value read twice, and adding a trait method automatically defaults to
unsupported instead of requiring a new bool nobody remembers to set. That deletes
a published struct, so it is filed rather than mandated; the keep-it version is a
`#[test]` in each protocol crate that drives every gated method and asserts the
flag agrees with the result, which is mechanical to generate and would have caught
the graph fingerprint split too.

Two smaller structural notes. The error model's `Reconcile` lane is missing the
fields `Retry` has (finding 6) - the two advice types should share a common
carrier for `retry_hint`/`throttle_scope`, since both describe *when* and *how
widely* the failure applies regardless of which lane it lands in. And the
94-method trait: the hunter agrees with the audit's conclusion for the reasons it
gives, but the audit's own premise ("optional lanes have defaults") is what makes
the capability mirror necessary, so the two decisions are coupled - solving the
capability problem is what would eventually make a narrower handle possible, not a
supertrait split.
