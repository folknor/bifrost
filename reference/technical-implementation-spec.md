# Technical implementation specification

The single document from which an open TODO item is built to completion without
re-deriving its design. Two implementers working from it independently produce
the same artifact.

## What it is

1. **Every brick.** It lays each step on the road from the current code to the
   finished item. No step is left to discover during implementation.
2. **Obstacles resolved inline.** Anything blocking the road is solved in the
   document, as part of it. An unresolved obstacle is a missing brick.
3. **No deferral.** Nothing in the originating TODO is pushed to "later" -
   deferred work is a hole in the road. (Work that belongs to a genuinely
   separate TODO is named and excluded; that is not deferral.)
4. **No shoehorning.** We do not fit the work into existing abstractions,
   structures, or conventions because they already exist. The structure that
   best serves the end goal is the one we build; whatever stands in its way is
   ripped out and rebuilt. Pre-1.0, breaking any internal API is legal.

## What it must also pin (or it is aspiration, not a spec)

5. **Verification per brick.** Every change names its gate, matched to what the
   change can break: a named `brokkr test -p <crate> <NAME>` case for any
   behavior a small deterministic unit test can pin (parser, encoder,
   type-level check, serde round-trip, error classification - the entire
   bifrost test scope); and `brokkr check` (gremlins + clippy + the
   changed-files test sweep) as the green-tree gate every landing must hold.
   Bifrost deliberately has no integration, end-to-end, mock-server, or
   performance-baseline gates and does not add them; if a behavior cannot be
   pinned by a small deterministic unit test, the spec says so explicitly and
   names the `brokkr check` outcome that stands in. A brick whose load is
   unproven is not laid. Per gate, the spec contains the EXACT command to run -
   copy-pasteable, flags and all, not "run the relevant tests". If no command
   exists that can verify a gate (no path exercises it, no test pins the
   behavior), building that instrument - the smallest deterministic unit test
   that pins the behavior - is itself a brick of the spec, specified to the
   same standard and laid before the brick it gates.
6. **A keep/revert path.** The implementation unit is one coherent, fully
   intrusive change that lands and is then kept or reverted on its gate
   results - never a tiny gated probe or an env-var experiment switch. The
   sequence of such landings is ordered so `brokkr check` stays green at every
   boundary between them. Complete-but-unorderable is a failed spec.
7. **The target as concrete artifacts.** "The ideal structure" is pinned to
   exact types, signatures, ownership, and data flow - buildable, not merely
   directional.
8. **A survey of the ground.** The current structure and everything depending on
   it is inventoried before the teardown, so the rip is precise and drops no
   load-bearing work. Specs authored as a batch reconcile their surveys against
   siblings covering the same ground before any is implemented; a sibling's
   survey may already state the fact that refutes this spec's premise.
9. **A stopping rule.** The rebuild has a bounded blast radius. Where the
   teardown stops, and what is out of scope, is stated explicitly.
10. **The standing references.** Every spec MUST cite, by path: this document
    (`reference/technical-implementation-spec.md`) as the contract it is
    written against; `reference/error-model.md`, the cross-cutting
    `AccountError` contract - ALWAYS required reading regardless of what the
    spec targets, because every `Account` method returns
    `Result<_, AccountError>` and any change is bound by it; the document the
    spec was spawned from (the TODO source naming the item - e.g. a `TODO.md`
    entry); AND the `reference/<crate>.md` for every crate the spec targets -
    the single source of truth for that crate's module layout, trait surfaces,
    and invariants. A spec citing these references must direct its reviewers
    and implementers to READ them, not merely name them - they are the ground
    the work is built on and judged against. A spec missing any of these is
    incomplete. Bifrost keeps no
    performance-baseline ledger, and the test rules forbid perf, integration,
    and mock-server gates, so a spec owes no performance record - correctness
    is the only measured axis, gated by `brokkr check` and named `brokkr test`
    cases.

## Stance

- **Structural over micro.** The spec pursues the structural change that
  materially moves the goal - real capability for feature work, real
  correctness for fix work - not local tweaks. Full rewrites are labeled
  as such, distinct from local changes.
- **Cleanliness is a deliverable.** No env-var scaffolding, benchmark knobs, or
  temporary routing switches left as the way forward.
- **Unlimited resources, aggressive internal rewrites assumed.** Old
  abstractions earn no protection from age; shared writer abstractions and
  generic reuse are not goals. Correctness and maintainability of the *result*
  still hold.
