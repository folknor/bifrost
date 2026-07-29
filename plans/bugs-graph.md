# bifrost-graph bug hunt

Scope: `crates/graph/src/**`. This file carries the CURRENT gaps and nothing
else. Resolved findings live in git history - the commit that fixed one is
its record - and so do the per-pass repair logs; retaining either here means
maintaining a second, drifting copy of `git log`.

Open work, in full: O-2, O-7 and O-22. O-7 is a shared-contract question
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

**O-22 - a subscription change never reaches a live EWS stream.** The EWS
subscription is minted once per `run_get_events_loop` and the loop re-issues
`GetStreamingEvents` against that same id forever; it exits only on an HTTP
error, a parse failure, or shutdown. `subscribe_ews` / `unsubscribe_ews`
mutate `ews_subscriptions` and call `ews_subscription_changed.notify_one()`,
but the only await on that `Notify` is the idle branch of
`run_streaming_worker` - the one taken when the map is EMPTY - and
`ensure_ews_worker` deliberately does not restart a worker that is still
running. So a `push_subscribe` issued while the stream is live adds folders
that the live EWS subscription does not cover: no notification for them
arrives until something knocks the connection over, which the 30-minute
`ConnectionTimeout` does not (it returns a well-formed empty body and the
loop simply re-polls the SAME id). Those scopes fall back to the ordinary
poll interval with no signal that push is not covering them. The
unsubscribe direction degrades rather than breaks: the removed handle's
folders keep notifying, routing no longer resolves them, and each one
becomes an account-wide `HintPayload::Unknown` reconcile. The fix is to make
the loop cancellable on `ews_subscription_changed` and re-subscribe -
`select!` against it around the `execute` await, then return a
`Resubscribe`-style exit that skips the `Disconnected` emission, since this
is a local topology change and not a transport fault. Verifying it needs the
transport seam below.

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
