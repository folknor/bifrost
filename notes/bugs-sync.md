# bifrost-sync bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/sync/` (engine, control, cancel,
multiplexer, backfill, push, mutation, cursor, recovery, scheduler, types). Findings are
unverified work material.

2026-08-07: four findings verified against the code and fixed, each with a regression test -
the cursor envelope outer version, the detach/re-attach teardown race, the scope-token cleanup
identity bug, and the latched `CheckpointNow`. Their sections are removed below; the behaviour
now lives in `reference/sync.md`. Everything still listed is unverified.

2026-08-23: the push-ordering defect from `Smaller observations` is fixed - each account now has
one forwarder task, so a later event cannot bypass an earlier one that is waiting for bounded
queue space, and `Disconnected`/`Reconnected` can no longer invert.

**A note on this document's remaining sections.** The findings below are structural and
stylistic judgments, not defects: nothing here produces a wrong answer, loses data, or hangs.
They are a refactor backlog. An earlier round of this hunt treated a section of the same kind
as a mandate and deleted public API on that basis, which was not its call to make. Anything
here that would remove or reshape a published surface is a proposal for the repository owner to
accept or reject, not work to be picked up automatically.

## Structural: engine.rs is 5278 lines and mixes five unrelated concerns

Lifecycle (attach/detach/reopen), recovery dispatch (~900 lines of free functions), the backfill
orchestrator (~500), the mutation pipeline (~500), and roughly 1200 lines of 1:1 passthrough
forwarders that invent no semantics. The passthrough cluster in particular is mechanical: every
method is `live_account(id)?` then forward, with the identical doc comment shape, and is a macro
or a blanket forwarding trait, not 60 hand-written methods. Splitting recovery into `recovery/`
(it already has a `recovery.rs` that holds only the helpers, while the dispatch lives in
`engine.rs`, so the split is in the wrong place), the orchestrator into
`backfill/orchestrator.rs`, and the campaigns into `mutation/campaign.rs` would leave a
`SyncEngine` that fits in a head. `reference/sync.md`'s file map already describes this layout
aspirationally; the code does not match it.

## Smaller observations

- `drive_changes_stream` still takes `_account_id` and `_ack_tx` and threads them from four call
  sites through `spawn_scope_poll_inner`; dead parameters that obscure the actual data flow.
