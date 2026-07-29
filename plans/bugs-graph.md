# bifrost-graph bug hunt

Scope: `crates/graph/src/**`. This is the current-gap list after the
2026-07-29 repair pass. Resolved findings live in git history and are not
retained here.

## Bugs

### G-4 - Bulk category add/remove/patch can report an empty PATCH as applied

`patch_for_flags` can produce `{}` for category-only `FlagOp::Add`, `Remove`,
or `Patch`. The bulk pipeline sends that PATCH and turns a 2xx response into
`Applied` even though no category changed. Either do a read-modify-write of
the category array or reject the affected item as unsupported before sending a
request. At minimum, an empty PATCH must never be counted as applied.

### G-5 - Unknown EWS subscription lifecycle errors terminate streaming push

`SoapFaultCode::parse` maps unrecognised EWS response codes to `Unknown`, and
the Graph error adapter treats that as a terminal provider-contract violation.
Codes such as `ErrorSubscriptionNotFound`, `ErrorInvalidSubscription`, and
`ErrorInvalidWatermark` should instead cause the worker to reconnect and
subscribe again. The long-lived worker should not permanently stop for
forward-compatible EWS server vocabulary unless it is clearly auth or policy
terminal.

### G-8 - Webhook `clientState` cannot be validated by consumers

`create_subscription` generates a random client state for each resource and
discards it. The webhook receiver lives outside this crate, so it has no value
to validate. Expose a caller-supplied account-wide webhook secret through the
factory endpoint configuration and reuse it for every resource, or expose the
generated values on an appropriate public handle.

### G-11 - Webhook renewal never retires terminal or deleted subscriptions

The renewal worker emits `Terminated` for a terminal renewal error but leaves
the stale state in its map, causing repeated terminal events on every renewal
tick. A 404/410 subscription is similarly retried forever. Terminal entries
must be removed after one event; deleted server subscriptions should be
re-created for the same resource rather than patched again.

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

**O-9 - The message select relies on `@odata.etag`.** `MESSAGE_SELECT` omits
`changeKey`; current mutation concurrency depends on Graph supplying the
OData annotation. This is functional but load-bearing and should be made
explicit, preferably by selecting `changeKey`.

**O-10 - EWS notification-to-scope mapping cannot match foreign folders.**
The mapper compares native notification folder ids with encoded scope ids.
Even after mailbox-routed EWS subscriptions are introduced, this must compare
against the native folder id in the appropriate mailbox group. Verify Graph
and EWS folder-id equivalence before relying on narrow invalidation hints.

**O-11 - `message_reactions` lacks a public-folder EWS guard.** It routes a
folder-qualified public item into Graph REST, yielding a per-item 404. Match
the public-folder partition used by `get_stream` and `message_hydrate`.

**O-12 - An idle EWS worker wakes once per second.** `push_stream` can start
the worker before any subscription exists. Replace the 1Hz empty-scope loop
with notification-based wakeup or a materially longer idle wait.

**O-13 - `check_response_error` keeps `in_error_message` set.** Present EWS
response shapes make the first error win, which is acceptable, but the flag's
lifetime is non-obvious and future parser changes could turn it into a false
positive.

**O-14 - `classify_chunk` is a third `$batch` consumer outside the shared
reconciler.** `reactions.rs` does its own answered-set and range-filter
accounting and builds its own error rather than routing through
`graph_error::batch_response_missing`. Whether its missing-id classification
carries the same terminal-instead-of-ambiguous defect that was repaired in
`get.rs`, `mutate.rs`, and `pim.rs` was not checked. Audit it, and fold it
into the shared helper if it matches.

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
- membership discovery emits one owner tag per shared mailbox.

Regression-pinned hermetically: the `PartialResponse` classification across
idempotent and non-idempotent operations; hydration and mutation `$batch`
reconciliation (missing, out-of-range, unparsable, and duplicate response
ids) through the extracted pure `reconcile_hydration_responses` /
`reconcile_mutation_responses`; the EWS subscribable-scope predicate; owner-
tag dedup with two folders in one shared mailbox plus a second mailbox; the
foreign-membership native fallback; the shared-mailbox metadata projection;
beta-base derivation; and expiry-offset rejection.

**Not pinned, and why.** Partial webhook rollback and the inventory
neither-link branch are both reachable only through a live `GraphClient`.
`GraphClient` owns a concrete `bifrost_net::AccountNet` behind
`Arc<ClientInner>` with every request funnelled through a private
`execute_request`, so there is no in-process seam to stage a canned
response against. A `#[cfg(test)] responses: Mutex<VecDeque<..>>` field on
`ClientInner` (or a `GraphTransport` trait) would unlock those two plus the
`Retry-After` throttle path and the `@odata.nextLink` walk in one move; it
is a deliberate follow-up, not an oversight.
