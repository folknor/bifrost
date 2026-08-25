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

## 4. `ErrorScope`'s hand-written `Serialize` declares a field count it never writes

`error/scope.rs:26` - `serialize_struct("ErrorScope", 4)` for arms that write 1,
2, or 3 fields. Self-describing formats (JSON) tolerate this; length-prefixed
ones (bincode, postcard, MessagePack in compact mode) produce corrupt output.
`ErrorScope` rides `SupportExportConsented`, which is exactly the thing you would
ship to a support pipeline in a binary format.

**Confidence: high** on the bug, medium on whether any current consumer uses a
non-self-describing format.

## 5. `into_builder` silently downgrades recovery classification

`error/account_error.rs:167` / `builder.rs:142-155`. `idempotency_override` and
`throttle_scope` are dropped on the round-trip and derived fields recompute from
what remains. A tenant-scoped `Server(RateLimited)` that any layer decorates with
an extra cause loses its `ThrottleScope`, so the engine never lifts it into a
`ThrottleKey` bucket and keeps hammering the tenant with sibling accounts' work.
It is documented as a caveat, but "the decoration path silently changes the
classification unless every caller remembers to reapply two setters" is a
defaulting choice, not a caveat.

**Confidence: high.** Fix: carry both in `RebuildParts`.

## 6. `ThrottleScope` and `retry_hint` are dropped whenever a throttle reconciles

`error/recovery.rs:734-766`. `transient_retry_or_reconcile` on `InFlight` +
non-idempotent produces `Reconcile`, which has no field for either. So a
rate-limit or quota failure hit during a non-idempotent send never enters the
throttle bucket at all. Compounding this, the `ReconcileReason` it reports is
`TransportDropAfterSend` - for a `QuotaExhausted` error. The test at
`recovery.rs:1053` pins that lie as correct behaviour. Throttling is orthogonal
to retry-vs-reconcile and the enum shape forces the producer to misdescribe what
happened.

**Confidence: high.** Fix: hoist `throttle_scope`/`retry_hint` onto
`RecoveryClass::Reconcile` (or beside it on the error), and add a
`ReconcileReason::ThrottledMidFlight` arm.

## 7. `AccountErrorBuildError::EmptyChain` is documented as an enforced invariant and is enforced nowhere

`builder.rs:27`; `reference/error-model.md:57` calls it one of "the five enforced
invariants". No code path returns it. The actual protection is `CauseChain::new`'s
`debug_assert!` (`cause.rs:21`) plus `outermost()` indexing `causes[0]` - in
release, an empty chain is an index panic, not a `BuildError`.

**Confidence: high** (grep-verified). Fix that keeps the variant: make
`CauseChain::new` fallible and have `try_build` return `EmptyChain`, or make the
chain a genuine non-empty type (`(Cause, Vec<Cause>)`) and delete the variant.

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

## 11. `TelemetryView`'s "no free-form text" guarantee is unenforced, and the test named for it is vacuous

`error/diagnostic.rs:62-80`, `account_error.rs:328`. `native_code`, `request_id`,
and `trace_id` are producer-supplied `String`s copied straight through, so "safe
to ship to metrics unconditionally" holds only by convention.
`telemetry_has_no_free_form_text` asserts two field values and checks nothing
about free-form text - it passes regardless.

**Confidence: high.** Fix: make `native_code` a bounded/validated type (or an
interned `&'static str` per provider vocabulary - `WireCause::code()` already
produces exactly that), and rewrite the test to assert over the serialized field
set.

## 12. `CauseSummary` discards most of what the internal export tier exists to carry

`cause.rs:73-132`. `TransportCause.kind` (Network/Timeout/Tls),
`AccessCause::PermissionDenied{resource}` and `InsufficientScope{needed}`,
`StateCause::StrategyFailure{downgrade}` and `CapabilityChanged{delta}`, and the
entire `RequestCause::BatchInputInvalid{items}` list all vanish. A batch-input
producer bug reaches support with zero detail. Relatedly, `StdError::source()`
bridges only to `chain.outermost()` and `Cause::source()` returns `None`, so an
`anyhow`-style walker reaches exactly one of N causes - the reference claims
walkers "reach the cause graph."

**Confidence: high.**

## 13. `MutationSuccess::Downgraded` carries no statement of what was actually done

`error/stream.rs:41`. The variant's whole justification is that reporting a
neighbour is "a wrong answer a consumer cannot detect" - but `Downgraded` itself
is a unit variant, so the consumer still cannot detect *what* state the target is
in and read-back is the only recourse. Give it a payload (the weaker operation
actually applied, or the resulting membership).

**Confidence: medium-high.**

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

## 15. Four distinct conveniences all report `Unsupported(UpdateFlags)`

`account.rs:1097, 1123, 1144, 1158` - `set_starred`, `mark_replied`,
`mark_forwarded`, `mark_mdn_sent` are indistinguishable to the consumer, directly
against `unsupported_error`'s own doc ("narrows the error so the consumer knows
which operation was rejected"). `AccountOperation` has no variants for them.

**Confidence: high.** Fix: add the four operations.

## 16. `AccountOperation::is_idempotent` contradicts its own stated rule for the `*Update`/`Rename` family

`error/scope.rs:197-254`. The comment says absolute-state writes are idempotent,
then places `DraftUpdate`, `ContactUpdate`, `EventUpdate`, `IdentityUpdate`,
`ContainerRename`, and `VacationSet` in the non-idempotent set. Each is a
set-to-this-value write against a known id - the same shape as `SetIsRead`, which
is on the idempotent side. Either the rule or the table is wrong; as it stands, an
in-flight drop on a contact edit reconciles where the identical operation on a
flag retries.

**Confidence: medium** (there may be a deliberate reason for the DAV/If-Match
cases, but nothing records it).

## 17. `FlagOp::Patch { add, remove }` permits the same flag in both sets

`mutation.rs:179`. The outcome is provider-dependent order-of-application; nothing
rejects it and nothing documents a precedence rule. Same class: `Add(empty)` is a
wire round-trip that means nothing.

**Confidence: medium.**

## 18. `RecoveryClass::is_terminal()` is defined by negation over a `#[non_exhaustive]` enum

`error/recovery.rs:53`. Any future non-terminal variant is silently classified
terminal and accepted by `Fatal::try_from` - the type-system collapse point that
is supposed to mean "the engine has nothing left to try" would swallow it. The
exclusivity test iterates a hand-maintained variant list, which catches nothing at
compile time.

**Confidence: medium-high.** Fix: exhaustive match in `is_terminal`.

## 19. `validate_batch_input`'s empty-input case fabricates a sentinel id

`error/batch.rs:276-280` returns `BatchInputInvalidItem { id:
BatchItemId(String::new()), reason: Empty }` for an empty *input vec* - the same
shape it uses for an empty *id*, and precisely the "builders must not synthesize
sentinel values" rule `AccountErrorBuilder::status` states two files away. The two
conditions want separate carriers. Also, the function returns raw
`Vec<BatchInputInvalidItem>`, so six protocol crates each hand-assemble the
identical `Request(BatchInputInvalid)` error; it should return the `AccountError`.

## 20. `message_key` for `Unsupported` breaks its own namespace convention

`error/message_key.rs:81` - bare `"unsupported"` with no family prefix, in a
namespace documented as "dotted, family-prefixed". Prefix-grouped dashboards
mis-bucket it. And the reference's claim that the keys are "exhaustively pinned by
`documented_message_keys_are_derived`" is false: `NotFound::{Draft, Identity,
Vacation, PushSubscription, Account}` - five of eleven arms - are not in the
test's case list.

## 21. `ErrorScope` is the only place in the crate where ids are stringly typed

`error/scope.rs:10-16` uses `Mailbox { id: String }`, `Message { id: String }`, …
while the crate ships `MailboxId`, `ObjectId`, `ThreadId`, `CalendarId`,
`ContactId` newtypes and uses them everywhere else. Producers stringify at the
error boundary and consumers cannot round-trip back.

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
