# bifrost-graph bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/graph/`. Findings are unverified work
material. Line numbers are as of the hunt and will drift.

## How to read this document (triage pass 2026-08-23)

This is an unverified hunt ledger, not a work queue. Most of it is a record of fixed work; the only
live section is the one filed from the round-1 audit at the bottom. Categories used across the
`bugs-*.md` set: **C1 live defect**, **C2 latent defect**, **C3 refactor opinion**, **C4 product
decision** (owner's call, never the loop's), **STALE** (no longer reproduces). **PUBLISHED SURFACE**
is an orthogonal marker for any remedy that would remove, rename, or reshape a published item.

Result for this document: **no C1, no C2, no C4, and no published-surface finding.** Both remaining
open entries are C3-or-less, and one of them is now stale. There is nothing here for the loop to
work. That is the honest state, not a gap in the triage.

Fixed 2026-08-18 and removed from this document: the calendar webhook
subscriptions colliding on `/me/events`, `push_stream` swallowing broadcast
`Lagged`, the infallible `expirationDateTime` parser (and the hand-rolled
civil-date arithmetic beside it), and the empty flag PATCH reported as
`Applied`, `close()` stranding server-side subscriptions in both push modes,
the `decode_cursor` progress asymmetry, the missing scope attribution on a
terminal renewal failure, and the `with_push_endpoint` constructor whose
`clientState` no receiver could validate. `reference/graph.md` now states each
new rule.

Fixed 2026-08-22 and removed from this document: EWS streaming delivering in
thirty-minute batches (there is now a chunked response seam and an incremental
frame decoder), the frozen calendarView window (cursor envelope v3 records the
horizon and reseeds ahead of it), the unrejected contradictory `FlagOp::Patch`,
the EWS worker that parked forever after its last handle went away, and
`inventory_stream` never checkpointing mid-walk (envelope v4 plus the
`Account::inventory_resume_stream` hook). Three defects that pass introduced
were caught in review and fixed in the same commit: the EWS worker retiring
without clearing its slot (now one shared `worker_slot` discipline across both
push modes), the frame decoder assuming the literal `m:` namespace prefix, and
a classified SOAP response error inside the stream being downgraded to a
malformed-XML reconnect loop. A fourth was found while auditing the consumers:
the reopen path `run_establish` put a stored mid-inventory cursor straight into
the registry, where `changes_stream` would have walked an empty delta link.

Fixed 2026-08-22 in round 2 and removed from this document: `push_subscribe`
failing the whole request when one requested scope was refused. It is now a
per-scope `PushSubscription` over the shared three-lane `BatchOutcome`, and
bifrost-sync records and recreates only the accepted scopes. Round 2's own fix
pass shipped that contract but left the all-or-nothing bails that ran BEFORE
translation - the poll-only public-folder check in `push_subscribe`, the
unsubscribable-shape check in `subscribe_ews`, and the unresolvable-resource
check in `subscribe_graph` - so the user-visible symptom survived on every path
that mattered; all three now route through the same lane. The same pass added
`Account::is_inventory_cursor` but wrote it to check only
`inventory_in_progress` where `inventory_resume_stream` also requires
`scope_matches_payload`, which would have stranded a scope/payload-mismatched
cursor with no live cursor and no recovery path; both now read one
`resumable_inventory_payload` condition.

Recorded, not fixed: round 2's Finding B (the Graph flag alias table) was not a
defect. The projection helpers and `canonical_flag_name` already recognised the
same alias set, so the unification into `graph_flag_field` changes no behavior
and its alias-pair test passes against the pre-unification code - verified by
reverting. The refactor and the test are worth keeping as a drift guard; they
should not be described as fixing anything.

## Filed 2026-08-22 from the round-1 fix-and-commit audit

These were flagged by the agent that fixed round 1, noticed while auditing the
consumers of its own changes. They are recorded here rather than fixed in place
so the next round adjudicates them deliberately.

- **[C3, not a defect - and the entry says so itself]** Verified 2026-08-23: `StreamingFrameDecoder::push`
  does loop from the buffer start, but every path either drains the consumed prefix or truncates the
  buffer to a bounded tail, so the quadratic case needs a single frame larger than the whole
  transfer. Filed correctly as a known bound. No action.

  **The EWS frame decoder rescans its buffer from index 0 on every chunk**
  (`account/ews_stream.rs`). Buffers drain per frame, so this is fine in
  practice; it is quadratic only in a pathological single-huge-frame case. Filed
  as a known bound, not a defect.

- **[STALE - the one thing it asked for has landed]** The entry's only suggestion was "a comment
  naming the shared idiom". As of 2026-08-23 the bare `delivered <= 1` comparison is gone from
  `crates/sync/src/multiplexer/fusion.rs`: both sites now call the named shared helper
  `super::delivered_to_real_subscriber(delivered)`, which is a stronger version of the remedy than
  the comment. Nothing remains to do. Also note the entry is filed in the wrong document - the code
  it describes lives in `bifrost-sync`, not `bifrost-graph`.

  **`fusion.rs`'s `delivered <= 1` retire-checkpoint heuristic is correct** - it
  mirrors the idiom in `multiplexer/changes.rs` exactly. Recorded only because it
  reads like a bug on first encounter and will again to the next reader; if
  anything here is worth doing, it is a comment naming the shared idiom, not a
  change.
