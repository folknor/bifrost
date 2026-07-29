# bifrost-graph bug hunt

Scope: `crates/graph/src/**`. This is the current-gap list after the
2026-07-29 repair pass. Resolved findings live in git history and are not
retained here.

No open bugs remain: every G-numbered finding from this hunt is fixed and
its regression pinned or explicitly named as unpinnable below. The
observations below are still OPEN work - design questions and ergonomics
risks, none of them a defect on their own, none of them started. This file
is not finished while that list has entries.

## Observations

**O-2 - An unconfigured foreign mailbox falls back to the primary client.**
`client_for_scope` / `client_for_owner` decode the foreign id but fall back to
`/me` when its owner is no longer configured. Folder-scoped calls can then
target a different namespace. Decide whether those call sites should fail
locally as stale configuration instead.

**O-4 - `etag_index` grows without bound.** The per-account map has no
eviction or destroy cleanup. It is an optimization, so an LRU bound or removal
after successful destroy would contain its memory use without affecting
correctness.

**O-6 - `describe_cursor` reports every decodable cursor as fresh now.**
The payload's `issued_at_unix_secs` is recorded but unread. Either use it for
`freshness` or remove it.

**O-7 - Subscription teardown depends entirely on the caller.** Settled as a
Graph question: `Account::close` is idempotent LOCAL teardown and explicitly
does not delete durable server-side push subscriptions
(`reference/types.md`); engine detach cancels workers and calls `close`, and
the engine tells consumers to call `unsubscribe_push` themselves
(`SubscriptionRegistry` exists for exactly that). So leaving subscriptions
live across `close` is the contract, not a defect, and Graph must not add
best-effort deletion there. What remains is an API ergonomics risk: a
consumer that detaches without unsubscribing strands subscriptions for up to
24h, and Graph's 24h expiry is the only backstop. Worth revisiting at the
shared-contract level, not in this crate.

**O-8 - `api_beta_base` is unused outside tests.** It remains a public
constructor parameter and is cloned through the client, but production code
does not read it. Wire a beta call site or remove the parameter.

**O-10 - EWS notification-to-scope mapping cannot match foreign folders.**
The mapper compares native notification folder ids with encoded scope ids.
Even after mailbox-routed EWS subscriptions are introduced, this must compare
against the native folder id in the appropriate mailbox group. Verify Graph
and EWS folder-id equivalence before relying on narrow invalidation hints.

**O-13 - `check_response_error` keeps `in_error_message` set.** Present EWS
response shapes make the first error win, which is acceptable, but the flag's
lifetime is non-obvious and future parser changes could turn it into a false
positive.

## Repair pass: 2026-07-29

Fixed in this pass:

- shared metadata hydration preserves encoded ids and mailbox membership;
- foreign tombstones use the native-folder fallback;
- inventory rejects delta pages lacking both pagination links;
- `$batch` responses are range-checked, deduplicated, and reconciled against
  the submitted ids on the hydration, bulk-mutation, and direct-write paths;
- an omitted `$batch` subresponse classifies `Protocol(PartialResponse)`
  with `Acknowledged` transmission evidence instead of a terminal
  `ContractViolation`, and an ambiguous bulk mutation rides the uncertain
  lane so the engine reads it back;
- partial webhook creation rolls back, and the handle is minted before the
  first create so an RNG failure cannot report `Unsent` over live
  subscriptions;
- EWS rejects unsupported and shared-mailbox subscription scopes;
- beta-base derivation stays on the configured origin;
- expiry parsing rejects numeric UTC offsets;
- membership discovery emits one owner tag per shared mailbox;
- (review follow-up, O-14) the reaction read's `classify_chunk` routes its
  unanswered ids through `graph_error::batch_response_missing` like the
  other three `$batch` consumers - it already used the uncertain lane, but
  the error it carried was a terminal `ContractViolation` with default
  `Unsent` evidence, telling a consumer that a re-read of an idempotent
  read was pointless.
- G-4: incremental category `FlagOp::Add`, `Remove`, and `Patch` now fail
  locally as `Unsupported(UpdateFlags)` before etag preflight or `$batch`
  construction. `Set` continues to replace the full categories array.
- G-5: EWS streaming subscription lifecycle response codes now classify as
  retryable unavailable failures, so the worker reconnects and re-subscribes.
- G-8: `GraphAccountFactory::with_push_endpoint_client_state(url, secret)`
  reuses one consumer-owned account-wide `clientState` for every resource,
  allowing the external webhook receiver to validate notifications.
- G-11: terminal renewal state is removed after its one event; a 404/410
  recreates the vanished subscription for the same resource.
- G-15: webhook unsubscribe snapshots its group but keeps it registered,
  deletes each server subscription first, and removes only confirmed
  deletions. A DELETE failure therefore leaves its id and all later ids
  reachable under the same handle for a retry instead of orphaning live
  subscriptions until expiry.
- O-9: `MESSAGE_SELECT` explicitly requests the documented `changeKey`.
- O-11: reaction reads now recognize public-folder item ids before `$batch`
  construction and return `Unsupported(MessageReactionsRead)`, because this
  Graph-only extended-property surface has no EWS implementation.
- O-12: an EWS worker with no active scopes waits on a `Notify` from
  subscribe/unsubscribe rather than polling the empty subscription map once
  per second.

Found by the review OF this pass, and fixed in it:

- G-16: webhook unsubscribe raced the renewal worker's recreate path -
  introduced by G-15 itself. Teardown now has to keep the group registered
  while it deletes (that is what makes a failed DELETE retryable), so
  "registered" stopped implying "live" and `install_replacement` happily
  installed a replacement into a group whose server-id snapshot had already
  been taken. Teardown deleted the stale id, retired the group, returned
  success - and the replacement stayed registered and delivering. The group
  now carries a `tearing_down` marker set in the same write-lock
  acquisition that snapshots the ids; `install_replacement` refuses a
  condemned group (the worker then deletes the subscription it minted) and
  `due_renewals` skips one entirely, so teardown intent is never extended
  by a renewal either. A marker rather than a shared lock: the decision is
  local and synchronous, whereas serializing the two paths would hold a
  lock across DELETE and create round trips.
- G-17: `push_subscribe([])` registered an empty group. The new removal
  loop had no ids to walk, so the group was never retired and the renewal
  worker it started was never stopped. An empty scope list is now
  `Request(Malformed)` in both push modes - it covers nothing, and the
  engine already skips empty lists at its own reattach boundary - rather
  than tolerating a nonsense registration that teardown has to special-case.
- G-18: the O-11 guard rejected the whole request. One public-folder id
  made `message_reactions` return a top-level `Unsupported`, discarding the
  outcomes of every ordinary Graph message in the same batch. This surface
  preserves per-item failures, so `partition_supported_ids` splits the
  public ids off, files each `Failed(Unsupported(MessageReactionsRead))`
  with its own `ErrorScope::Message`, and lets the supported ids continue
  through `$batch`.

Review follow-ups on G-11 (same pass):

- the replacement create now runs BEFORE the stale state is dropped. The
  old order removed the state first, so a failed create left the resource
  with no row at all: no later tick could see it as due, and webhook
  coverage stayed lost until reopen or an explicit resubscribe. A failed
  create also classifies the create error rather than logging it and
  re-reporting the already-known 404, so its recovery class decides whether
  another tick is worth it.
- the replacement installs through `install_replacement`, a `get_mut` that
  refuses an unregistered handle, instead of `entry().or_insert_with()`.
  The worker walks a snapshot, so a concurrent `push_unsubscribe` could
  retire the handle mid-create and have both the server subscription and
  the local group resurrected under it - teardown reported as successful
  while notifications kept arriving. The worker now deletes the
  subscription it just minted in that case.
- a successful replacement emits `Reconnected`. The resource had no live
  subscription for the whole gap and Graph does not replay it;
  `Reconnected` is the only event `sync::push::reconciler` turns into a
  full reconcile, so the changes missed in the gap otherwise waited for the
  ordinary poll interval (up to the 30-minute ceiling).

Regression-pinned hermetically: the `PartialResponse` classification across
idempotent and non-idempotent operations; hydration and mutation `$batch`
reconciliation (missing, out-of-range, unparsable, and duplicate response
ids) through the extracted pure `reconcile_hydration_responses` /
`reconcile_mutation_responses`; the EWS subscribable-scope predicate; owner-
tag dedup with two folders in one shared mailbox plus a second mailbox; the
foreign-membership native fallback; the shared-mailbox metadata projection;
beta-base derivation; expiry-offset rejection; the renewal worker's
`subscription_is_gone` recreate gate (404/410 only, never a throttle, auth,
or transport failure); `install_replacement`'s two rules - the replacement
swaps in place inside a live group, and an unregistered handle is never
resurrected; G-15's pure state transition, which preserves not-yet-deleted
server ids after an earlier deletion succeeds; G-16's teardown marker,
pinned as the full interleaving over pure state (condemn + snapshot, confirm
one DELETE, refuse the renewal worker's replacement, finish teardown and
retire the handle) plus the idempotence of re-condemning and `due_renewals`
skipping a condemned group; G-17's empty-scope rejection in both push modes,
asserting neither subscription map was touched; and G-18's public/Graph
partition, which pins the order-preserving split both lanes depend on.

**Not pinned, and why.** Partial webhook rollback, the inventory
neither-link branch, the renewal worker's end-to-end sequencing (the
`Reconnected` emission after a successful replacement, the cleanup DELETE
when the handle was unsubscribed mid-create, and the stale row surviving a
failed create into the next tick), `unsubscribe_graph`'s DELETE loop as a
loop (a mid-loop DELETE failure returning the error with the remaining ids
still registered), and the reaction read's mixed batch reaching `$batch`
with its Graph ids after the public ones were failed, are all reachable
only through a live
`GraphClient`. The decision rules were factored out - the pure
`install_replacement`, `mark_group_tearing_down`, `due_renewals`,
`remove_subscription_from_groups`, `partition_supported_ids`, and the
`subscription_is_gone` gate carry everything
that does not need a socket, and all are pinned above - but the worker
loop itself still calls `create_subscription` / `renew_subscription` /
`delete_subscription` directly, and those need a transport seam.
`GraphClient` owns a concrete `bifrost_net::AccountNet` behind
`Arc<ClientInner>` with every request funnelled through a private
`execute_request`, so there is no in-process seam to stage a canned
response against. Verified at review: `bifrost_net::Response` is
`#[non_exhaustive]` with no public constructor and the net crate's
`Dispatch` seam plus its `ScriptedDispatch` are crate-private/test-only,
so a Graph test cannot mint or inject responses today - the seam has to
be Graph-local (jmap's `PushTransport` precedent), not borrowed. A `#[cfg(test)] responses: Mutex<VecDeque<..>>` field on
`ClientInner` (or a `GraphTransport` trait) would unlock those two plus the
`Retry-After` throttle path and the `@odata.nextLink` walk in one move; it
is a deliberate follow-up, not an oversight.
