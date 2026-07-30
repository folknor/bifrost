# Bug hunt: bifrost-types + bifrost-sync (2026-07-28)

Scope: `crates/types/src/**`, `crates/sync/src/**`,
`crates/sync/tests/**`, `reference/error-model.md`, and
`reference/sync.md`.

## Current gaps

Found reviewing the quiescence / reattach pass (2026-07-29). The two
boundary-primitive defects from that review are fixed and pinned in
`control.rs`'s unit tests; what follows is what remains.

## Nits

- **N1. Push forwarder polls capabilities once a second while
  `PushCapability::None`** (`engine.rs`, forwarder task). It already
  selects on the reopen-generation watch, and capabilities only change
  across a reopen, so the 1s tick buys nothing. `reference/sync.md`
  describes the park as generation-driven and does not mention the
  tick - code and doc should agree on one of the two.
- **N2. `bifrost_sync_push_dropped_total` now counts deferrals as
  drops** (`push/mod.rs`). The counter increments before the lossless
  branch, so `Terminated` / `Warning` events that are subsequently
  delivered intact are still tallied as dropped. Either skip the
  increment on the lossless path or rename the metric to something
  like `push_overflow_total`.

## Fixed in this pass

- **R1-R6 plus the four P1 regressions the cold review found in them.**
  Per-item `Engine(_)` recovery now dedupes by directive identity
  (variant + target scope), not by target alone, so a mixed batch can no
  longer have an `OperatorOverrideRequired`, schema reset or scope
  disable swallowed by an unrelated earlier failure. Reopen registers
  the account's activity BEFORE `factory.open()`, closing the window
  where `pause` reported quiescence while a replacement connection was
  mid-open (and where the losing replacement was dropped without
  `close()`). The lifecycle reader parks on the account generation only
  for `RestartAccount` - the one directive that replaces its connection
  - and that park is bounded; every other directive is handed over once
  and the reader reconnects on backoff instead of waiting for a signal
  that never fires. Any push handle whose server-side teardown did not
  succeed, on either side of a reopen, stays in the registry flagged
  `teardown_unconfirmed`: it is never recreated on a replacement, is
  carried across swaps, and is retried by the next reopen or
  `unsubscribe_push`, so the correlated old-teardown-plus-failed-unwind
  case can no longer orphan a webhook.

- **Unbounded, never-pruned pending-checkpoint set.** `SyncControl`'s
  outstanding-broadcast set only ever shrank on an exact-identity ack,
  so a failed `CheckpointStore` write, a consumer acking coarsely, or
  a consumer dropping without acking left an entry that wedged every
  later `pause` / `checkpoint_now` on that account for the process
  lifetime - B5 again, in a form that survives its own cause - while
  the `Vec` grew one entry per broadcast batch forever. Entries now
  leave through `record_checkpoint` (acked + durable),
  `retire_checkpoint` (ack processed but nothing durable, or no real
  subscriber), or supersession by a newer broadcast on the same
  lane + scope, which bounds the set by scope count.
- **Register-after-publish race.** `expect_checkpoint` ran after the
  broadcast at all four producer sites, so a consumer that acked
  between the send and the registration stranded an entry no ack could
  match - same terminal symptom. Registration now precedes the send
  and is retracted when the batch reached only the slot's sentinel
  receiver.
