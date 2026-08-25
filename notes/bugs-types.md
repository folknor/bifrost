# bifrost-types: hunt findings

Scope: `crates/types/` - the `Account` trait surface, the `AccountError` model,
`BatchOutcome`, diagnostics tiers, cursor/checkpoint/stream envelopes, coverage
ledger, mutation result types.

Hunter note: `mime/` was not audited in depth - a self-contained parser/renderer
with its own tests, orthogonal to the contract questions asked. Worth a second
pass if wanted.

## Status: closed

All thirty findings are worked. The five landed rounds were 657f257 (4, 5, 6, 7,
20, 21), ea5d477 (11, 12, 13, 15, 16, 18), f874de0 (1, 2, 3, 14, 22, 23),
d5bc7af (8, 9, 10, 19, 24), and the final round (17, 25, 26, 27, 28, 29, 30).

Two of the last round's findings closed on evidence rather than on a code fix,
and both are worth recording because the evidence contradicted the finding:

- **25 rested on an inverted premise.** It reported `ReconcileAdvice` as lacking
  `#[non_exhaustive]` while `ReconcileGuidance` carried it. The code is and
  always was the other way round: `ReconcileAdvice` and `RetryAdvice` have both
  been `#[non_exhaustive]` since the original error-model commit, and
  `ReconcileGuidance` is a plain public struct with a public field, so it is
  genuinely constructible downstream. The state the finding asked us to choose
  between is the state that already exists - an explicit seal on the advice
  structs and a real constructor path for the nested guidance. Nothing to
  decide; `reference/error-model.md` now states the posture so it is not
  rediscovered as an accident a third time.
- **26 was a real question with a clean answer.** A CalDAV `CalendarId` IS the
  resolved collection href on the listing surface too: `map_calendar` files
  `collection.href` as both `id` and `native_id`, and the XML decode boundary
  rebases every href against its request URI before the account layer sees it,
  which makes `resolve_url` a no-op on any id that came out of the listing. So
  the ids a consumer correlates against `ErrorScope::Calendar` do match. The
  invariant is now pinned by a test rather than only asserted in prose, because
  it is an agreement between two files that would drift silently.

## The structural story

Two things dominate, and both are bigger than any individual defect the ledger
recorded.

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
