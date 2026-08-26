# bifrost-jmap: hunt findings

Scope: `crates/jmap/` - dispatch, transport, per-RFC module pattern, capability
negotiation, error model, and the `Account` impl under `crates/jmap/src/sync/`.

Final round status: all accepted findings are resolved. Two entries below remain
only because they were rejected on the merits, with the ruling preserved in
full; one records a regression the round-2 cold review caught in the J8 fix, and
the last is a residual observation left deliberately unfixed.

## J8 follow-up. The call-count guard read an absent limit as zero

The first cut of the J8 guard snapshotted `maxCallsInRequest` with
`map_or(0, ..)`, so a session with no `urn:ietf:params:jmap:core` block produced
`max_calls = 0` and every method call failed as `RequestCallLimit`. Account
opening was the casualty: `seed_account_state` issues its probes before
`sync::capabilities::build` validates the session, so a malformed or
capability-shifted session terminated as `Request(Malformed)` / `ClientBug` and
never reached `SyncState(CapabilityChanged)` / `RestartAccount` (absent core
capability) or `Protocol(ContractViolation)` (zero-valued limit) - exactly the
classifications those sessions depend on for recovery.

Fixed by making the three states distinct in the type rather than in a comment.
`CallLimit` is `Unadvertised`, `Invalid` (advertised zero, which RFC 8620
forbids) or `Advertised(NonZeroUsize)`, and only the third enforces. Capability
validation remains the single gate for both bad sessions. Two open-path tests in
`sync::factory` pin it: each drives `seed_account_state` over the scripted
transport for one of the bad sessions and asserts both that the probes still go
out and that `capabilities::build` still yields the intended classification.
Both were ablated against the `map_or(0, ..)` behaviour and failed with
`RequestCallLimit { max: 0 }`.

## Residual, not fixed: `reenable_current_push_set` reads its two values non-atomically

The reader's reconnect replay takes the `enabled` guard, drops it, then takes
the `push_state` guard, so the pair it replays is not read under one critical
section. Every mutator (subscribe, unsubscribe) holds both guards across its
apply and commits under them. The close pass sharpened the worst case beyond
what was first recorded: if a mutator commits between the reader's two reads,
the replay can re-apply a union the mutators have already superseded - after a
racing final unsubscribe, the wire briefly carries a subscription the state
says is empty, with a `None` position. The consequence is only extra push
frames and spurious invalidation hints (never a missed change - hints are
over-approximate by contract), it self-heals on the next reconnect or
reconfigure because the mutators' own frames are sent under all three guards,
and the reconciler treats the reconnect as a full `Unknown` reconcile
regardless.
Left alone because closing it means holding the `enabled` guard across the
`set_push_data_types` await in the reader, which introduces exactly the kind of
lock-across-await teardown change this arc has repeatedly seen open a new hole
one layer up. Recorded so a future round does not have to rediscover it.

## J13. Positional paging survives in the consumer-facing list and search paths

Surfaced by the close pass of this document's arc, and left open deliberately
rather than folded into it.

`contacts.rs`, `calendar_ops.rs` and `pim.rs` still page by integer position over
orders that are not total - the same unstable-order shape that J1 fixed for the
inventory walk. The close pass judged, correctly, that this is not J1 reopened:
those are consumer-driven page-cursor APIs rather than coverage-claiming walks,
so churn-induced skip or duplication there is ordinary list-API behaviour and not
silent data loss. Nothing reports complete coverage off them.

It is still the weaker mechanism where a better one exists. The suggested shape:
carry an anchor id on the page cursor, so a consumer paging through a churning
list gets stable continuation rather than positional drift.

**Confidence: high** that the paging is positional; **low** that it constitutes a
defect. This is an improvement with a known better answer, not a bug.

## J11. Rejected: the first reader pass sends a disable frame

`enabled` starts empty at `open()`, and `apply_push_set` maps an empty set to
`client.disable_push_ws()`. The first reader pass therefore disables push on a
connection nobody had enabled, then announces `Reconnected`.

This is not a contract defect. `WatchEvent::Reconnected` is explicitly a
connection-health transition in `bifrost-types`, not proof that at least one
scope is subscribed. The empty data-type set is the applied desired state, and
the disable frame makes that state explicit on every newly opened connection.
The sync reconciler deliberately turns every reconnect into a full `Unknown`
reconcile, including this first connection, so suppressing the event or skipping
the frame would create a second meaning for the same transition and make the
reader's applied-state guarantee conditional. The extra frame is cosmetic wire
traffic with no incorrect state or missing coverage, so it stays.

## Structural story. Rejected: collapse push state into one mutex-protected object

The original argument treated `WsState.enabled`, the subscriptions registry,
and the then-missing RFC 8887 `pushState` as three pieces of one object mutated
by two actors under two locks with no transaction boundary. It proposed a
single `PushSubscriptionState` mutex with apply as its only mutator.

That premise is now stale. `pushState` exists, and subscribe, unsubscribe, and
the reader use one lock order. Subscribe and unsubscribe hold the registry,
enabled-set, and push-position guards across the sole apply await, then commit
all three only after success; cancellation and apply failure therefore mutate
nothing. The reader can update the position only after acquiring the same final
guard, so it cannot interleave a new position into an in-flight reconfigure.

The three values also have deliberately different meanings and lifetimes. The
registry is the requested handle set. `enabled` is the last wire-applied union.
`pushState` is retained across a non-empty reconfigure but cleared when the
union becomes empty. Combining their storage would reduce the number of mutex
objects, but it would neither strengthen the existing transaction boundary nor
encode those lifetime rules in a type. It would instead rewrite working,
test-pinned cancellation and replay machinery with no surviving invariant gap.
The current state is kept.
