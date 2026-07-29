# bifrost-graph bug hunt

Scope: `crates/graph/src/**`. This file carries the CURRENT gaps and nothing
else. Resolved findings live in git history - the commit that fixed one is
its record - and so do the per-pass repair logs; retaining either here means
maintaining a second, drifting copy of `git log`.

Open work, in full: O-7. It is a shared-contract question rather than a
Graph defect and is tracked as `xc-2` in `TODO.md`. This file is not
finished while that list has entries.

## Open findings

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

## Test coverage: the standing seam

Everything hermetically pinnable in this crate is a decision rule that was
factored out of a request path: cursor and payload projections, `$batch`
reconciliation (`reconcile_hydration_responses` /
`reconcile_mutation_responses`), the per-item chunk routing split
(`batch_routing::partition_routable`, plus each site's builder run over a
mixed live/stale id set), the wholly-unroutable chunk end to end on three
surfaces (reaction read, bulk destroy, hydration) - which is hermetic
precisely because nothing routes, so no `$batch` leaves the process - both
cursor doors rejecting a stale foreign scope before any request
(`ScopeRevoked` -> scope-bearing `DisableScope`, at `initial_delta_url` and
at `changes_stream`'s delta-link resume), the webhook group state machine
(`install_replacement`, `mark_group_tearing_down`, `due_renewals`,
`remove_subscription_from_groups`, `subscription_is_gone`), the EWS response
scan, the foreign/public id codec, `partition_supported_ids`, the EWS
subscribable-scope predicate, the REST-to-EWS translation rules
(`translation_input_chunks` dedup + 1,000-id chunking,
`reconcile_translated_ews_scopes` pairing and its three failure arms, the
`convertIdResult` wire shape, and the translation context's idempotency
override run through `bifrost_net::Error` directly), the EWS notification
routing rules (`dedupe_by_ews_folder` for the Subscribe body,
`unique_scopes_for_folder` for the invalidation fan-out), the single-answer
request guard (`per_answer_request_ids` plus a per-builder sweep and the
`build_soap_envelope` debug assert), and the bounded LRU change-key cache.

What is NOT pinned is everything that only exists inside a live request:
partial webhook-creation rollback; the inventory neither-link branch; the
renewal worker's HTTP legs (create / renew / delete, the `Reconnected`
emission after a successful replacement, the cleanup DELETE when the handle
was unsubscribed mid-create, the stale row surviving a failed create, and the
`Retry-After` throttle path); the `@odata.nextLink` walk;
`unsubscribe_graph`'s DELETE loop as a loop; a MIXED batch actually reaching
`$batch` with its routable ids after the public / unroutable ones were failed
(true for the reaction read, hydration, and the mutation funnel alike - the
split itself is pinned, the subsequent round trip beside the surviving ids is
not); the etag-eviction call sites, including
`submit_write_batch_with_targets` draining past a failed subresponse so a
later destroy still evicts; and the `translateExchangeIds` POST itself
(that a multi-chunk fan-out issues N requests and accumulates their answers is
asserted only through the pure chunker, never over a wire).

The reason is one missing seam, not an oversight per finding. `GraphClient`
owns a concrete `bifrost_net::AccountNet` behind `Arc<ClientInner>` with
every request funnelled through a private `execute_request`, and nothing can
stage a response against it: `bifrost_net::Response` is `#[non_exhaustive]`
with no public constructor, and net's `Dispatch` / `ScriptedDispatch` are
crate-private and test-only, so the seam has to be Graph-local (jmap's
`PushTransport` is the precedent) rather than borrowed. The EWS half of
that seam now exists: `crate::ews::EwsExecute` abstracts the one funnel
every SOAP request passes through (`EwsClient` in production, a scripted
double in tests), and the streaming worker's subscribe / long-poll /
unsubscribe cycle is pinned through it. The REST half is still missing: a
`#[cfg(test)] responses: Mutex<VecDeque<..>>` on `ClientInner`, or a
`GraphTransport` trait, unlocks the whole list above in one move. It is a
deliberate follow-up.
