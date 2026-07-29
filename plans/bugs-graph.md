# bifrost-graph bug hunt

Scope: `crates/graph/src/**`. This file carries the CURRENT gaps and nothing
else. Resolved findings live in git history - the commit that fixed one is
its record - and so do the per-pass repair logs; retaining either here means
maintaining a second, drifting copy of `git log`.

Open work, in full: O-2, O-7, O-10, O-18 and O-20. O-7 is a shared-contract
question rather than a Graph defect and is tracked as `xc-2` in `TODO.md`.
O-10 was closed by the 2026-07-29 follow-up pass and REOPENED by the review
of it, which found the closure rested on an id equivalence nobody had
established; O-20 is the same gap on a path where it is not survivable, and
was raised while reopening O-10. This file is not finished while that list
has entries.

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

**O-10 (REOPENED) - a Graph folder scope and an EWS notification name the
same folder in two different id formats.** `scope_for_folder` maps an EWS
streaming notification's `<t:ParentFolderId Id="...">` onto a subscribed
`CursorScope` by comparing id strings. The two sides do not carry the same
format:

- the scope's `FolderId` came from Graph REST (`/me/mailFolders`, the
  inventory and container walks), so it is a `restId`;
- the notification came from EWS, so it is an `ewsId`.

Microsoft documents these as distinct formats and `translateExchangeIds`
(`POST /me/translateExchangeIds`, `sourceIdType`/`targetIdType`) as the
supported conversion between them; the historical URL-safe-base64
relationship is explicitly not a contract. So byte equality is SUFFICIENT for
a match but not NECESSARY: an unequal pair can still be the same folder.

Consequence at this call site is bounded - a miss degrades to
`HintPayload::Unknown`, an account-wide re-check instead of a scoped one, so
the failure is imprecision, not lost mail. The same gap on the Subscribe
request is O-20 and is not bounded.

The follow-up pass "closed" this by stripping the foreign-mailbox prefix off
the scope id before comparing. That is not the gap: it made a foreign scope
matchable by a bare native id, and a notification carries no owning mailbox,
so nothing said WHICH mailbox's folder had matched. The review's test passed
only because it handed both sides the same fake id. Reverted:
`scope_matches_notification_folder` now identifies primary-mailbox scopes
only and documents both unknowns in place; a foreign scope never matches
(and is not subscribable anyway, per `ews_scope_is_subscribable`). Pinned by
`only_a_primary_scope_is_identified_by_a_bare_notification_folder_id`, which
asserts non-equivalence, not equivalence.

The real fix is id translation at the subscription boundary - one
`translateExchangeIds` call per subscribed folder, its result cached
alongside the subscription state so notifications map without a round trip.
That is a new Graph API call with real cost and belongs to its own piece of
work, together with O-20.

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

**O-20 - EWS Subscribe is handed Graph REST folder ids.** Same root as O-10,
opposite direction and no safe degradation. `build_subscribe_request` writes
each scope's native folder id into `<t:FolderId Id="...">`, but those ids are
`restId`s minted by the Graph container walk, and EWS `FolderId` expects an
`ewsId`. The subscribable predicate makes the inversion exact: the scopes
that DO carry EWS ids (public folders, discovered through EWS `FindFolder`)
arrive as `CursorScope::Folder` and are rejected, while every id EWS actually
receives comes from Graph. An id EWS cannot parse returns `ErrorInvalidId*`,
which `SoapFaultCode::parse` does not recognize, so it lands on `Unknown` ->
`Protocol(ContractViolation)` -> terminal, and the worker emits
`WatchEvent::Terminated` instead of establishing push at all.

Not observed against a live server - this crate has no live-server coverage -
and reachable only through the opt-in `with_ews_streaming()`, which is why it
is filed rather than patched blind. Fix it with O-10: translate once at
`subscribe_ews`, keep the `ewsId` next to the scope in `EwsSubscriptionState`,
and both the request and the notification mapping become exact.

## Test coverage: the standing seam

Everything hermetically pinnable in this crate is a decision rule that was
factored out of a request path: cursor and payload projections, `$batch`
reconciliation (`reconcile_hydration_responses` /
`reconcile_mutation_responses`), the webhook group state machine
(`install_replacement`, `mark_group_tearing_down`, `due_renewals`,
`remove_subscription_from_groups`, `subscription_is_gone`), the EWS response
scan, the foreign/public id codec, `partition_supported_ids`, the EWS
subscribable-scope and notification-mapping predicates, and the bounded LRU
change-key cache.

What is NOT pinned is everything that only exists inside a live request:
partial webhook-creation rollback; the inventory neither-link branch; the
renewal worker's HTTP legs (create / renew / delete, the `Reconnected`
emission after a successful replacement, the cleanup DELETE when the handle
was unsubscribed mid-create, the stale row surviving a failed create, and the
`Retry-After` throttle path); the `@odata.nextLink` walk;
`unsubscribe_graph`'s DELETE loop as a loop; a mixed reaction batch actually
reaching `$batch` with its Graph ids after the public ones were failed; the
etag-eviction call sites; and the EWS worker's subscribe / long-poll cycle.

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
