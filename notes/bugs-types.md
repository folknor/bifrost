# bifrost-types: hunt findings

Scope: `crates/types/` - the `Account` trait surface, the `AccountError` model,
`BatchOutcome`, diagnostics tiers, cursor/checkpoint/stream envelopes, coverage
ledger, mutation result types.

Hunter note: `mime/` was not audited in depth - a self-contained parser/renderer
with its own tests, orthogonal to the contract questions asked. Worth a second
pass if wanted.

## 1. `unsupported_inventory_stream` manufactures a full-scope COMPLETE coverage claim out of a refusal

`crates/types/src/account.rs:141-156`. It yields `Terminated(error)` followed by
`Done(InventoryCompletion::complete(scope, None))`. `InventoryCompletion::complete`
hard-codes `CoverageDomain::full(scope)` + `CoverageOutcome::Complete`. Two
independent problems:

(a) The default `inventory_partition_stream` (`account.rs:281-290`) routes *every
non-`Full` partition request* through it, so a refused `Time`/`Uid`/`Page`
partition returns a proof of complete coverage over the **whole scope** - exactly
the ledger lie `coverage.rs`'s module doc exists to prevent, and exactly the
hazard `lift_complete_walk`'s doc warns about ("a PARTITION walk must not pass a
full-scope domain").

(b) Emitting `Terminated` and `Done` in one stream contradicts `InventoryEvent`'s
own doc, where `Terminated` is "the walk cannot continue at all". A consumer that
reads the last event wins sees success; one that reads the first sees failure.

**Confidence: high.** Fix that keeps the helper: make it emit only `Terminated`,
or have it take a `CoverageDomain` and report `Degraded` with a barrier region.
Separately, `InventoryCompletion::complete(scope, cp)` should take a
`CoverageDomain`, not a `CursorScope` - it is the unsafe twin of the
deliberately-safe `lift_complete_walk`, and callers reach for it (graph's
`public_folder.rs` uses it at six early-exit points).

## 2. `ChangeCursor::envelope_version` gates nothing and is owned by nobody

`cursor.rs:103`. Its doc says this field means "the trait crate said its outer
cursor shape ticked" - but `bifrost-types` exports no constant and performs no
check. Five crates each invent their own value for it
(`OUTER_CURSOR_ENVELOPE_VERSION`, `CHANGE_CURSOR_ENVELOPE_VERSION`,
`ENVELOPE_VERSION`, `CURSOR_ENVELOPE_VERSION`, `ENGINE_VERSION`), so the same
"outer shape" is simultaneously several different numbers. Worse,
`sync/src/cursor/envelope.rs` overwrites the decoded value with `ENGINE_VERSION`
rather than round-tripping it (`sync/tests/envelope_roundtrip.rs:153-164` pins
exactly that), so a mismatch is undetectable by construction. Only the *inner*
`OpaqueChangeState::envelope_version` actually gates anything.

**Confidence: high.** Fix: `pub const CHANGE_CURSOR_ENVELOPE_VERSION: u32` in
types plus a `ChangeCursor::validate_envelope()` the engine calls before handing
a cursor back to a protocol crate.

## 3. `Fingerprint::flags_hash` is a cross-producer comparison field with no defined derivation

`mutation.rs:40-44`. The type is the documented cross-protocol diff primitive,
but the crate supplies no canonicalization and no hash function, so each producer
invents one: imap hashes a `Flag` list, graph hashes a JSON `Value` in
`inventory.rs` but uses `u64::from(is_read)` in `public_folder.rs`, google uses a
canonical label hash, and carddav/sync/test-support hard-code `0`. Two producers
for the same account therefore emit different fingerprints for identical state,
and the engine diffs them as permanently changed. The root cause is the missing
contract here, not in the protocol crates.

**Confidence: high** on the types-side gap; the graph two-hasher divergence is a
live symptom noticed in passing (out of scope,
`crates/graph/src/account/public_folder.rs:517` vs `inventory.rs:550`).

Cross-check: the graph hunter tested that symptom and **refuted it as stated** -
the two graph producers mint disjoint id namespaces, so no consumer can compare
their hashes. It found a different, worse defect at that site instead; see
`bugs-graph.md`. The types-side gap (no crate-owned derivation) is unaffected by
the refutation, and the carddav/sync/test-support hard-coded `0` producers were
not tested by anyone.

## 8. `PageBoundary::Partial` documents "checkpoint is `None`" and nothing enforces it

`events.rs:113-136`. `Batch` has all-public fields and no constructor, so
`Partial` + `Some(checkpoint)` is constructible and the consumer's persist rule
keys off `checkpoint.is_some()`.

**Confidence: high.** The structural fix is to move the payload into the
discriminant: `Partial`, `Page(Option<Checkpoint>)`, `Final(Option<Checkpoint>)`,
deleting `Batch::checkpoint`. That is the same "two encodings can disagree"
argument the current doc makes, applied in the direction that actually removes
the nonsense state.

## 9. `CoverageOutcome::Degraded { obligations: vec![] }` is constructible and unresolvable

`coverage.rs:420`. `InventoryCoverageReport::degraded` does not reject an empty
list, producing a scope that is permanently degraded with nothing to repair and
no way to discharge. `from_obligations` gets it right; the sibling constructor
does not.

**Confidence: high.** Fix: make `degraded` take a non-empty first obligation, or
funnel it through `from_obligations`.

## 10. `finalize`'s accounting invariant is weaker than advertised when ids repeat

`error/batch.rs:126-177`, and the test `a_duplicated_expectation_does_not_change_the_verdict`
(line 385) pins it: two submitted items both named `"a"` are satisfied by **one**
lane entry, and one real item silently receives no outcome. The stated contract is
"every submitted id appears in exactly one lane." `validate_batch_input` would
have caught the duplicate, but it is a separate function `finalize` does not
require, and `push_subscribe` deliberately allows repeated scopes (positions as
ids).

**Confidence: medium-high.** Fix: `finalize` should count expectations, not
set-membership them, or reject a duplicated `expected` outright.

## 14. `is_inventory_cursor` and `inventory_resume_stream` must agree, and the type system cannot make them

`account.rs:247-268`. The doc spends a paragraph on the failure mode and ends with
"implement the condition once and have both entry points read it" - which the
two-method shape prevents. By the doc's own rule that building the resume stream
is I/O-free and side-effect-free, `is_inventory_cursor` is redundant:
`inventory_resume_stream(c).is_some()` is the classification.

**Confidence: high.** This is a removal, so filed as a finding: delete
`is_inventory_cursor`, or keep it and have the default impl be exactly
`self.inventory_resume_stream(cursor.clone()).is_some()` so divergence requires
overriding a method that already answers correctly.

## 17. `FlagOp::Patch { add, remove }` permits the same flag in both sets

`mutation.rs:179`. The outcome is provider-dependent order-of-application; nothing
rejects it and nothing documents a precedence rule. Same class: `Add(empty)` is a
wire round-trip that means nothing.

**Confidence: medium.**

## 19. `validate_batch_input`'s empty-input case fabricates a sentinel id

`error/batch.rs:276-280` returns `BatchInputInvalidItem { id:
BatchItemId(String::new()), reason: Empty }` for an empty *input vec* - the same
shape it uses for an empty *id*, and precisely the "builders must not synthesize
sentinel values" rule `AccountErrorBuilder::status` states two files away. The two
conditions want separate carriers. Also, the function returns raw
`Vec<BatchInputInvalidItem>`, so six protocol crates each hand-assemble the
identical `Request(BatchInputInvalid)` error; it should return the `AccountError`.

## 22. `InventoryPartition` mixes range conventions inside one enum

`events.rs:55-72`: `Time` and `Page` are inclusive-exclusive, `Uid` is inclusive.
`CoverageCoordinate` inherits the same split. An off-by-one magnet for every
implementor.

## 23. Stale references to a removed variant, in three load-bearing doc comments

`capabilities.rs:5-7`, `capabilities.rs:443`, and `account.rs:165` all describe
`RecoveryClass::CapabilityChanged { delta }`, which no longer exists
(`error-model.md` records its removal). Also `reference/types.md`'s file map omits
`coverage.rs` and `repair.rs` entirely, and still claims "94 methods" against a
trait that has grown since (`repair_inventory`, `bulk_move_from`,
`send_raw_message`, `message_reactions`, `category_definitions_list`,
`inventory_resume_stream`, `is_inventory_cursor` are all in the file but absent
from the lane table).

## 24. `Page::single` undoes the guarantee `Page`'s own doc claims

`page.rs:71-79`. The type is deliberately not `#[non_exhaustive]` so a new lane
"must break every constructor" - but `single` defaults all four non-item lanes, so
any impl using it silently absorbs the new lane instead of answering for it. Same
pattern in `OpenedAccount::complete`.

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

**The coverage/obligation machinery is well-designed and one constructor away from
being defeatable.** `coverage.rs` reasons carefully about why a completeness claim
needs a stated extent, and `lift_complete_walk` refuses to be a `From` impl
precisely so nobody manufactures one by accident. Then
`InventoryCompletion::complete(scope, cp)` and `unsupported_inventory_stream` do
exactly that, and the trait's own default `inventory_partition_stream` is the
biggest caller. The fix is small and worth doing first: every constructor of a
`Complete` outcome should require a `CoverageDomain`, with no scope-only shortcut
anywhere in the crate.

Two smaller structural notes. The error model's `Reconcile` lane is missing the
fields `Retry` has (finding 6) - the two advice types should share a common
carrier for `retry_hint`/`throttle_scope`, since both describe *when* and *how
widely* the failure applies regardless of which lane it lands in. And the
94-method trait: the hunter agrees with the audit's conclusion for the reasons it
gives, but the audit's own premise ("optional lanes have defaults") is what makes
the capability mirror necessary, so the two decisions are coupled - solving the
capability problem is what would eventually make a narrower handle possible, not a
supertrait split.
