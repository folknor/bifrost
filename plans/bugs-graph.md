# bifrost-graph bug hunt

Scope: `crates/graph/src/**`. This file carries the CURRENT gaps and nothing
else. Resolved findings live in git history - the commit that fixed one is
its record - and so do the per-pass repair logs; retaining either here means
maintaining a second, drifting copy of `git log`.

Open work, in full: O-2, O-7, O-18 and O-21. O-7 is a shared-contract question
rather than a Graph defect and is tracked as `xc-2` in `TODO.md`. This file
is not finished while that list has entries.

## Open findings

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

**O-18 - the response-error scan is whole-response, not per item.** The
first errored `ResponseMessage` that names a code decides the outcome of the
entire body, and an unclassifiable one (no code, or the contradictory
`NoError`) fails it too. Every EWS request this crate sends today carries
exactly one item - public-folder hydration deliberately issues one `GetItem`
per item because the routing headers differ - so the granularity is
currently exact. It stops being exact the moment a multi-item `GetItem` /
`DeleteItem` body is introduced: one item's `ErrorItemNotFound` would
discard the siblings' results. That is the same per-request-answer-on-a-
per-item-surface mistake the Graph `$batch` funnel already had to unlearn -
a single unsupported id must fail its own item, not the request. Latent, and
worth pinning before any batched EWS request lands.

**O-21 - the EWS Subscribe body can list one folder twice.**
`active_ews_scopes` unions every handle's scopes with no deduplication, and
`build_subscribe_request` emits one `<t:FolderId>` per entry. Two scopes can
now resolve to the SAME `ews_folder_id`: `push_subscribe` called twice for
overlapping scopes without an intervening `push_unsubscribe`, or two
`FolderType` scopes differing only in `ObjectType` over one container (the
translation request already deduplicates those, so both get one id back).
The result is a repeated `FolderId` in the Subscribe request; whether EWS
accepts it, ignores the duplicate, or rejects the whole subscription is not
determinable here, and nothing in the crate normalizes it either way. The
routing side has the matching imprecision: `scope_for_folder` returns the
FIRST scope whose `ews_folder_id` matches, so a notification for a
double-registered folder invalidates one arbitrary scope of the pair rather
than both. Dedup the union by `ews_folder_id` for the request, and route to
every matching scope rather than the first. Latent and low-severity - the
degradation on the routing side is a narrower invalidation, not a wrong one -
but it is now a byte-exact match rather than a best-effort one, so the
first-match shortcut has no remaining excuse.

## Test coverage: the standing seam

Everything hermetically pinnable in this crate is a decision rule that was
factored out of a request path: cursor and payload projections, `$batch`
reconciliation (`reconcile_hydration_responses` /
`reconcile_mutation_responses`), the webhook group state machine
(`install_replacement`, `mark_group_tearing_down`, `due_renewals`,
`remove_subscription_from_groups`, `subscription_is_gone`), the EWS response
scan, the foreign/public id codec, `partition_supported_ids`, the EWS
subscribable-scope predicate, the REST-to-EWS translation rules
(`translation_input_chunks` dedup + 1,000-id chunking,
`reconcile_translated_ews_scopes` pairing and its three failure arms, the
`convertIdResult` wire shape, and the translation context's idempotency
override run through `bifrost_net::Error` directly), and the bounded LRU
change-key cache.

What is NOT pinned is everything that only exists inside a live request:
partial webhook-creation rollback; the inventory neither-link branch; the
renewal worker's HTTP legs (create / renew / delete, the `Reconnected`
emission after a successful replacement, the cleanup DELETE when the handle
was unsubscribed mid-create, the stale row surviving a failed create, and the
`Retry-After` throttle path); the `@odata.nextLink` walk;
`unsubscribe_graph`'s DELETE loop as a loop; a mixed reaction batch actually
reaching `$batch` with its Graph ids after the public ones were failed; the
etag-eviction call sites; the `translateExchangeIds` POST itself (that a
multi-chunk fan-out issues N requests and accumulates their answers is
asserted only through the pure chunker, never over a wire); and the EWS
worker's subscribe / long-poll cycle.

The reason is one missing seam, not an oversight per finding. `GraphClient`
owns a concrete `bifrost_net::AccountNet` behind `Arc<ClientInner>` with
every request funnelled through a private `execute_request`, and nothing can
stage a response against it: `bifrost_net::Response` is `#[non_exhaustive]`
with no public constructor, and net's `Dispatch` / `ScriptedDispatch` are
crate-private and test-only, so the seam has to be Graph-local (jmap's
`PushTransport` is the precedent) rather than borrowed. A `#[cfg(test)]
responses: Mutex<VecDeque<..>>` on `ClientInner`, or a `GraphTransport`
trait, unlocks the whole list above in one move. It is a deliberate
follow-up.
