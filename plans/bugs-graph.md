# bifrost-graph bug hunt

Scope: `crates/graph/src/**`. This file carries the CURRENT gaps and nothing
else. Resolved findings live in git history - the commit that fixed one is
its record - and so do the per-pass repair logs; retaining either here means
maintaining a second, drifting copy of `git log`.

Open work, in full: O-7 and O-24. O-7 is a shared-contract question rather than a
Graph defect and is tracked as `xc-2` in `TODO.md`. This file is not
finished while that list has entries.

## Open findings

**O-24 - foreign-scope routing rides entirely in the URL prefix, and nothing
enforces that.** Inventory and changes requests for a foreign scope are
issued on the PRIMARY client, with the owning mailbox expressed only as the
`/users/{id}` prefix. That is correct today for exactly one reason: a client
built by `for_shared_mailbox` differs from the primary client in nothing but
`mailbox_id`. The moment it grows a distinct auth token, a routing header,
or its own rate-limit host, those two paths silently stop honoring it while
every other foreign path keeps working - the failure would look like a
mailbox-specific auth or throttling bug, not a routing bug. `reference/graph.md`
documents the design but not the invariant it rests on. Either state the
invariant at both sites and in the reference, or route these two paths
through the same per-owner client the object paths use.

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

The REST half now mirrors the EWS seam without changing bifrost-net: every
REST helper funnels through one `execute_wire`, which adapts the production
response into a Graph-local wire shape, while test builds script response
status, headers, and body at that funnel and inspect the requests it records.
The wire coverage pins webhook-create rollback, subscription DELETE
iteration, tolerating a subscription row the server already dropped, the
inventory absolute-nextLink walk and neither-link failure, mixed reaction
batches that send their surviving Graph request, translation's 1,000-id POST
fan-out, inventory tombstone ETag eviction, a write batch that drains past a
failed subresponse to evict a later confirmed destroy, and the raw-MIME
import's base64 `text/plain` body. The script is shared with every client
derived from the one a test holds (`for_shared_mailbox`, `with_outlook_base`),
the way the semaphore and `AccountNet` already are, so a foreign-mailbox
request - issued by a client minted inside `GraphAccount::new` - is scripted
and recorded alongside the primary's. The EWS seam remains the matching SOAP
half (`EwsExecute` with a scripted in-crate double).

The seam is pinned as an EQUIVALENCE, not just a convenience, because every
test built on it inherits its correctness: a scripted status takes the shape
bifrost-net's retry loop would have produced (2xx and passed-through 3xx are
the only responses; 4xx/401/429/5xx are typed errors, with the retry set read
off `RetryPolicy::default()` rather than restated), and an armed script that
runs out panics at the offending request instead of falling through to the
network. Closing that gap is what surfaced the two live defects this round
fixed: Graph's typed `error.code` table never reached an ordinary REST call,
and `subscription_is_gone` never matched the shape a REST 404 actually
arrives in.

Still not reached, and still pinned only as pure functions or not at all:
blob byte streams (`download_stream`), the OneDrive resumable chunk PUT
(pre-authed, no bearer, its own builder), the Autodiscover POST, the renewal
worker's timing loop as a loop (`due_renewals` and `install_replacement` are
pinned, the 10-minute tick is not), and anything whose behavior depends on
bifrost-net's own retry, backoff, or redirect walk - the seam answers at the
funnel, below which none of that runs.
