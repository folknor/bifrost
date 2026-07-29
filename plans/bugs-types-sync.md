# Bug hunt: bifrost-types + bifrost-sync (2026-07-28)

Scope: `crates/types/src/**`, `crates/sync/src/**`,
`crates/sync/tests/**`, `reference/error-model.md`, and
`reference/sync.md`.

## Current gaps

Found reviewing the quiescence / reattach pass (2026-07-29). The two
boundary-primitive defects from that review are fixed and pinned in
`control.rs`'s unit tests; what follows is what remains.

### R1. Per-item `Engine(_)` recovery is forwarded once per failing item

`crates/sync/src/engine.rs`, both bulk pipelines (the `SyncEvent::Batch`
arms feeding `classify_item_outcome`).

B8's fix is correct but unthrottled: every item whose recovery is
`Engine(_)` sends its own `ReopenRequest::Recovery`. A 500-target
`bulk_set_flags` whose items all fail with `Engine(RestartScope(...))`
pushes 500 requests through a 16-slot channel; the campaign blocks on
`send().await` while the reopen listener performs 500 sequential
scope/account restarts, each under `reopen_lock` with its own backoff
budget. The stream-level `Terminated` arm immediately below forwards
exactly once.

Fix: dedupe by directive target within the campaign - a
`HashSet<Option<CursorScope>>`, forward on first sight - so the
per-item path costs the same as the stream-level one.

### R2. Reopen widens push subscriptions when the recorded scopes vanish

`crates/sync/src/engine.rs`, `reattach_account`
(`scopes.clone_from(&discovered)`).

When none of a registered subscription's scopes survive discovery, the
replacement subscribes against EVERY discovered scope. A subscription
the consumer registered for one shared folder silently becomes
account-wide, and the consumer has no way to observe the widening.
Dropping the record is the honest behavior; the consumer can
re-subscribe against the new topology.

### R3. Reopen is a silent no-op while the account is paused

`crates/sync/src/engine.rs`, `reattach_account` +
`restart_account` + `SyncEngine::reopen`.

`reattach_account` opens with `begin_activity().ok_or(Error::Paused)?`,
and `restart_account` maps `Err(Error::Paused) => return`. On a paused
account a `RestartAccount` directive therefore does nothing: no retry
budget consumed, no `SyncEvent::Terminated` broadcast, no
`RetryBudgetExhausted` pause. It self-heals only because the poll loop
re-raises the same error after resume, which is implicit and untested.

The public `SyncEngine::reopen` inherits the same behavior and returns
`Error::Paused` on a paused account. Neither its doc comment nor
`reference/sync.md` records that. Decide whether reopen should queue
until resume, run regardless of the boundary (it is engine-initiated
recovery, not consumer work), or document the refusal; today it is
none of the three.

### R4. Terminal lifecycle errors now retry forever

`crates/sync/src/multiplexer/mod.rs`, lifecycle task.

`ScopeLifecycleEvent::Terminated` changed from `return` to `break` as
part of the B4 fix, so the task reconnects and re-sends
`ReopenRequest::Recovery` every <= 30s indefinitely against a
permanently-dead lifecycle stream. Arguably right - a reopen can
revive it - but it is an unrecorded behavior change with no attempt
budget mirroring `restart_account`'s three-strike rule, and nothing
distinguishes "auth is gone for good" from "transient".

### R5. `BudgetGate::new` no longer floors `global` at one permit

`crates/sync/src/scheduler/budget.rs`.

The `.max(1)` unmasking is right for `per_account` / mutation
(`mutation_permits()` keeps its own `.max(1)`, and the new
`mutation_permits() >= per_account` rejection guarantees
`sync_permits() >= 1`). `global` is the one that lost its floor
without gaining a guard on the same path: `BudgetGate::new` is `pub`,
and `ConcurrencyBudget { global: 0, .. }` there yields a
`Semaphore::new(0)` that blocks every acquisition forever.
`validate()` covers the `SyncEngineBuilder` path only. Latent while
the gate is unwired.

### R6. `unsubscribe_push` retires the handle before it knows teardown succeeded

`crates/sync/src/engine.rs:982`.

`SyncEngine::unsubscribe_push` does `self.subscriptions.take(account_id)`
*before* the per-handle loop, then only logs a warning when
`Account::push_unsubscribe` fails. The registry entry is gone either way,
so a failed teardown leaves no engine-side record to retry through.

This became load-bearing when `bifrost-graph` fixed its own half (G-15,
commit 578c1ff): Graph now deliberately keeps a subscription's server ids
registered after a failed DELETE precisely so the teardown can be retried,
and orphaned Graph subscriptions otherwise keep delivering to the webhook
endpoint until their 24h expiry. That retry lane currently has no retrier
on the engine side - only a consumer holding its own handle can drive it.

Either retain the registry entry until every `push_unsubscribe` reports
success, or surface the failure to the caller instead of swallowing it into
a log line. Found while reviewing the Graph teardown work, not by a review
of `bifrost-sync` itself.

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
