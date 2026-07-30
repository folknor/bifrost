# bifrost-graph bug hunt

Scope: `crates/graph/src/**`. This file carries the CURRENT gaps and nothing
else. Resolved findings live in git history - the commit that fixed one is
its record - and so do the per-pass repair logs; retaining either here means
maintaining a second, drifting copy of `git log`.

Open work, in full: O-25. The shared-contract subscription-lifecycle question
that used to sit here as O-7 is tracked as `xc-2` in `TODO.md` instead, since
it is a contract question rather than a Graph defect. This file is not
finished while the list below has entries.

## Open findings

**O-25 - thread-keyed operations are silently primary-mailbox only.** Every
per-message door decodes the foreign-encoded `ObjectId` and routes via
`client_for_owner`, so a shared-mailbox message reads and writes under
`/users/{owner}`. The thread-keyed doors cannot: a `ThreadId` is Graph's bare
`conversationId`, minted with no owner tag by `inventory_entry_from_value`
and by search, and `message_values_for_thread` builds its filter query off
`account.client.api_path_prefix()` unconditionally. So `thread_hydrate`,
`move_thread`, `delete_thread`, and every `MutationTarget::Thread` fan-out
resolve their members against `/me`, where a shared mailbox's conversation
does not exist.

The failure is silent in both directions, which is what makes it worth
filing rather than accepting: zero matches is not an error, so hydration
returns `ThreadHydration { messages: [] }` and a thread-targeted flag / move
/ destroy submits an empty batch and reports success having touched nothing.
Foreign scopes are minted for `ObjectType::Email` only, so this is exactly
the shared-mail case, and a consumer cannot see it coming - the `thread_id`
on a shared mailbox's `InventoryEntry` is indistinguishable from a primary
one.

Two honest closes. Either give `ThreadId` the same owner tag `ObjectId`
carries (mint it encoded at both projection sites, decode it in
`message_values_for_thread`, and route the filter query through
`client_for_owner`), which makes thread operations work per-mailbox and
keeps one wire form per logical thread; or leave the surface primary-only
and say so on the wire - reject a thread whose members cannot be located
rather than returning an empty success - plus in `reference/types.md`, since
no consumer can infer it. The first is the real fix; the second is only
acceptable if owner-tagging the thread id is judged too invasive for the id
codec.

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

That sharing has one consequence worth stating, because it silently weakens
any test written past it: a derived client answers from the primary's queue
and records into the primary's log, so the seam alone cannot say WHICH client
issued a request. Asserting the URL does not recover it either on the delta
paths - `initial_delta_url` builds its `/users/{mailbox}` prefix off
`client_for_scope` independently of which client then sends it, and a
`nextLink` / `deltaLink` is whatever Graph minted - so a walk that fell back
to the primary would produce byte-identical requests. The per-message write
paths do not have that problem: there the URL is built from the selected
client's own prefix, so the URL IS the routing evidence. Where the
distinction matters, the test roots the shared client in its own
`GraphClient` (`new_for_tests_with_shared_clients`) and arms the primary with
an EMPTY script, so a fallback hits the exhaustion panic instead of passing
quietly. Both foreign delta walks (inventory and changes-resume) are pinned
that way, over a `nextLink` continuation, and both fail against a reverted
routing change.

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
the thread-keyed fan-out's mailbox routing (reachable through the seam, but
pinning today's behavior would pin O-25's bug, so it waits on that decision),
blob byte streams (`download_stream`), the OneDrive resumable chunk PUT
(pre-authed, no bearer, its own builder), the Autodiscover POST, the renewal
worker's timing loop as a loop (`due_renewals` and `install_replacement` are
pinned, the 10-minute tick is not), and anything whose behavior depends on
bifrost-net's own retry, backoff, or redirect walk - the seam answers at the
funnel, below which none of that runs.
