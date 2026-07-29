# bifrost-graph bug hunt

Scope: `crates/graph/src/**`. This is the current-gap list after the
2026-07-29 repair pass and the observation sweep that followed it. Resolved
findings live in git history and are not retained here.

No open bugs remain: every G-numbered finding from this hunt is fixed and
its regression pinned or explicitly named as unpinnable below. The
observations below are still OPEN work - design questions, latent risks, and
ergonomics gaps, none of them a defect on today's call paths, none of them
started. O-2, O-7, O-8 and O-10 predate the sweep; O-17, O-18 and O-19 were
raised by it. This file is not finished while that list has entries.

## Observations

**O-2 - An unconfigured foreign mailbox falls back to the primary client.**
`client_for_scope` / `client_for_owner` decode the foreign id but fall back to
`/me` when its owner is no longer configured. Folder-scoped calls can then
target a different namespace. Decide whether those call sites should fail
locally as stale configuration instead.

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

**O-17 - an Error-classed EWS response message carrying `NoError` still
reads as success.** `check_response_error` classifies any non-`NoError` code
on a `ResponseClass="Error"` message and passes `NoError` through. The shape
is self-contradictory and Exchange is not known to emit it, so this is a
decision to confirm rather than an observed defect: either it is a provider
quirk that genuinely means success, or it belongs with the missing-code case
as `MalformedXml`. Raised while fixing G-19.

**O-18 - the response-error scan is whole-response, not per item.** The
first errored `ResponseMessage` decides the outcome of the entire body, and
(since G-19) an incomplete one fails it too. Every EWS request this crate
sends today carries exactly one item - public-folder hydration deliberately
issues one `GetItem` per item because the routing headers differ - so the
granularity is currently exact. It stops being exact the moment a multi-item
`GetItem` / `DeleteItem` body is introduced: one item's `ErrorItemNotFound`
would discard the siblings' results, which is the same per-request-answer-on-
a-per-item-surface mistake G-18 fixed on the Graph `$batch` side. Latent, and
worth pinning before any batched EWS request lands.

**O-19 - `etag_index` eviction is arbitrary, not LRU.** At the 10,000 cap
`insert_etag` drops whatever key `HashMap::keys().next()` yields, which can be
the change key a mutation is about to use. Correctness is unaffected -
`SetFlags`/`Move` refresh a missing etag with `GET ?$select=id` and `Destroy`'s
`If-Match` is opportunistic - so the cost is one extra round trip per evicted
hot id. On an account whose working set exceeds the cap this degrades toward a
refresh per mutation. If that shows up, the fix is a real recency policy, not
a bigger number.

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

## Observation sweep: 2026-07-29

Five observations closed, none of them a G-numbered defect:

- O-4: `etag_index` is bounded at `ETAG_INDEX_MAX_ENTRIES` (10,000) through
  the single `insert_etag` door every writer now goes through, and change
  keys are evicted when their object dies - `@removed` tombstones in the
  changes stream, and confirmed destroys on both the bulk `$batch` funnel and
  the per-message `pim` write path. Eviction policy is arbitrary rather than
  recency-ordered; see O-19.
- O-6: `GraphCursorPayload::issued_at_unix_secs` is gone. It was written on
  every mint and never read - `describe_cursor` reports freshness from the
  cursor kind - so it was pure payload weight. No envelope bump: the field is
  absent from new payloads and ignored (no `deny_unknown_fields`) in old ones,
  so cursors persisted by an earlier build still decode.
- O-13: `check_response_error`'s error-scan state is now scoped to the
  response message it belongs to, so an incomplete error cannot annex the
  `ResponseCode` of a later warning or success message.
- O-15: `delete_subscription` accepts 410 Gone as well as 404 for an
  already-vanished subscription, sharing the `subscription_is_gone` predicate
  with the renewal worker's recreate gate rather than keeping a second,
  narrower private copy.
- O-16: the renewal worker exits when only condemned (`tearing_down`) groups
  remain, instead of ticking forever over groups retained purely for a
  DELETE retry.

Found by the review OF this sweep, and fixed in it:

- G-19: an incomplete EWS error was converted into SUCCESS - a defect the
  O-13 fix introduced. Clearing the per-message scan state also dropped the
  fact that the body contained a failed response, so `check_response_error`
  returned `Ok(())` and `EwsClient::execute` handed a failed 200-OK body to
  the operation parsers, several of which project an absent result set as an
  empty successful one: public folders or items silently disappear, and the
  public-folder deletion reconcile would then emit a mass-`Destroyed`. An
  error-classed message that closes with no `ResponseCode` is now
  `MalformedXml`. A complete error later in the same body still wins, because
  its code carries the real classification (`ErrorAccessDenied` quarantines
  just that scope) where `Protocol(ParseFailed)` would throw it away. The
  sweep had pinned the unsafe result as though it were the contract; that
  assertion is inverted, not preserved.
- G-20: a new webhook subscription could end up with NO renewal worker - a
  race the O-16 exit opened. The worker observed condemned-only groups and
  returned, but its `JoinHandle` stayed unfinished for the moment it took to
  unwind, so a concurrent `push_subscribe` that inserted a live group in that
  window saw a live handle, declined to spawn, and left the subscription
  unrenewed until some later subscribe. The exit now clears the
  `graph_worker` slot itself, BEFORE releasing the subscriptions read guard
  that made the decision: the insert cannot land until the slot is empty, so
  the subscriber's `ensure_graph_worker` always spawns a replacement. A
  persistent, `Notify`-wakeable worker would also close it, but it would undo
  O-16 (an idle account keeps a task alive forever) and add a second wakeup
  channel to keep consistent with the subscription map; making the exit and
  the spawn agree on one piece of state - the slot, ordered under the lock
  that already guards the decision - is the smaller invariant.
  `push_unsubscribe`'s abort path had the same shape (sample emptiness, then
  abort) and now holds its guard across the abort explicitly, as a named
  binding rather than relying on `if`-temporary scoping. Lock order is
  subscriptions-then-worker everywhere; nothing acquires them inverted.

Regression-pinned hermetically: the `insert_etag` capacity bound; the
inverted G-19 assertion (incomplete error -> `MalformedXml`, and it must not
borrow the following message's code), plus a complete error after an
incomplete one still classifying, plus a code-less `Warning` still passing;
`has_live_graph_subscription_group` over a condemned group; and G-20's slot
retirement, driven through a real spawned worker on paused time (a
condemned-only map reaches the exit without a single HTTP call) asserting
both that the slot is cleared and that the next `ensure_graph_worker` spawns
again.

**Not pinned, and why.** The renewal leg of the worker (everything after
`due_renewals` returns a non-empty list) still needs the transport seam
described above - `renew_subscription` is called directly, and
`bifrost_net::Response` remains `#[non_exhaustive]` with no public
constructor while net's `Dispatch` / `ScriptedDispatch` are crate-private, so
a Graph test cannot mint the 404 that drives the recreate path. The
etag-eviction call sites (tombstone, `$batch` destroy, `pim` destroy) are
likewise behind live requests; only the pure `insert_etag` door is pinned.
