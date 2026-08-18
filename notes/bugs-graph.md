# bifrost-graph bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/graph/`. Findings are unverified work
material. Line numbers are as of the hunt and will drift.

Fixed 2026-08-18 and removed from this document: the calendar webhook
subscriptions colliding on `/me/events`, `push_stream` swallowing broadcast
`Lagged`, the infallible `expirationDateTime` parser (and the hand-rolled
civil-date arithmetic beside it), and the empty flag PATCH reported as
`Applied`. `reference/graph.md` now states each new rule.

## EWS streaming push is not actually streaming: it delivers in ~30-minute batches

`crates/graph/src/ews/client.rs` - `EwsClient::execute` does `req.send().await`, then reads
`resp.body` (a fully-buffered `bytes::Bytes`) and only then parses.
`crates/graph/src/account/ews_stream.rs` issues
`build_get_streaming_events_request(subscription_id, 30)`, a 30-minute `ConnectionTimeout`, and
awaits that same buffered `execute`.

EWS Streaming Notifications work by holding a chunked HTTP response open for the whole
`ConnectionTimeout` and emitting `<m:GetStreamingEventsResponse>` fragments as events occur.
Because the transport buffers the entire body, nothing is parsed until Exchange closes the
connection at the 30-minute mark. So `PushMode::EwsStreaming`, whose whole point is
`PushCapability::InProcess` low-latency invalidation, delivers invalidations with up to 30 minutes
of latency, worse than the ordinary poll interval it is meant to pre-empt, and every notification
arrives in one burst.

Worse, the capability surface advertises `push_in_process() == true`, so the engine may relax its
polling on the strength of a push channel that is effectively a 30-minute timer.

Fixing this is not a tweak: it needs a streaming-body seam (`bytes_stream`) plus an incremental XML
reader that emits notifications per response fragment, and correspondingly an `EwsExecute` variant
that yields a stream. Given `EwsClient::execute` is also the funnel that runs `check_soap_fault` /
`check_response_error` on a whole body, the honest move is to split EWS into two transports: the
one-shot request/response funnel every other op uses, and a dedicated streaming funnel for
`GetStreamingEvents` that frames per-envelope. Lowering `ConnectionTimeout` to e.g. 1 minute is a
stopgap that trades latency for a Subscribe/reconnect storm, not a fix.

Hunter confidence: high on the buffering; lower on whether Exchange flushes a complete parseable
envelope per fragment or a single envelope split across the connection (it is the latter for the
outer element, which is exactly why the incremental reader is required rather than "parse each
chunk").

## Calendar calendarView delta window is frozen at first seed and never slides

`crates/graph/src/account/inventory.rs` computes `startDateTime`/`endDateTime` as `now-90d` /
`now+365d` and bakes them into the initial delta URL. Graph carries the original window inside the
`@odata.deltaLink` it mints; `changes_stream` then follows that link forever.

`delta_token_expires_after: None`, `describe_cursor` reports a delta cursor as fresh
unconditionally, and nothing re-runs `establish_initial_cursor` for a live scope. So an account
running longer than a year stops seeing events past its frozen `endDateTime` entirely, and the
trailing edge never advances either. The failure is silent: no error, no scope disable, just a
calendar that goes blind at a horizon that recedes into the past.

There is no mechanism in the crate to age out a cursor. Options: store the window bounds in
`GraphCursorPayload` and have `changes_stream` force a reseed (`SyncState(CursorInvalid)` then
`RestartScope`) once `now` approaches the frozen end, or make `describe_cursor` report a calendar
cursor as stale past a threshold. Either way the cursor payload needs to carry the window it was
minted against, which is an envelope bump.

## Nothing retires server-side subscriptions on close() / reopen

`crates/graph/src/account/mod.rs` - `close()` cancels the shutdown token and aborts the EWS worker.
It does not delete any Graph webhook subscription still in `graph_subscriptions`, and does not send
an EWS `Unsubscribe` for the live streaming subscription.

For webhook mode, since reopen constructs a fresh `GraphAccount` and the engine resubscribes, each
reopen strands one live server subscription per resource for up to its ~24h expiry, still POSTing
to the consumer's HTTPS receiver. For EWS mode this is exactly the per-mailbox
streaming-subscription quota leak the code already goes out of its way to avoid on the
topology-handoff path (`release_subscription`), but the shutdown path drops it on the floor.
`StreamLoopExit::Shutdown` and `StreamLoopExit::Terminated` both return without
`release_subscription`, and `close()`'s `worker.abort()` means even a release placed there would not
run.

The abort-based teardown is the structural problem: a cancellation-token-driven graceful exit
(worker observes `shutdown`, releases, then returns; `close()` joins rather than aborts, with a
timeout) is the shape this wants. Webhook teardown needs `close()` to walk `graph_subscriptions` and
DELETE, or an explicit statement in the reference that the engine guarantees `push_unsubscribe`
before `close()`. The hunter could not find such a guarantee.

## FlagOp::Patch naming one flag in both sets has no rejection

`crates/graph/src/account/mutate.rs`. The empty-patch half of this finding is fixed; what remains is
that `FlagOp::Patch { add, remove }` naming the same flag in both sets resolves by evaluation order
(`apply_flag_removes` runs last, so remove wins) rather than being refused as a contradictory
request. Deterministic and pinned by a test, so this is a design question, not a live defect.

## unsubscribe_ews never retires the EWS worker; the two push modes have divergent worker lifecycles

`push.rs` removes the state and bumps topology. The worker then exits its `GetStreamingEvents` loop
as `Resubscribe`, releases the subscription, finds an empty scope map, and parks forever on
`topology.changed()`: one live task per account, for the account's whole lifetime, doing nothing.

The webhook mode built a careful protocol for exactly this (`has_live_graph_subscription_group`,
`retire_graph_worker_slot`, the subscriptions-then-worker lock order, the "clear the slot before
releasing the guard" ordering). The EWS mode has none of it. Two worker-lifecycle state machines,
one hardened and one absent, guarding the same `Arc<Mutex<Option<JoinHandle>>>` shape. This is the
duplicated machinery that most wants unifying in this file: one worker-slot abstraction with
`ensure`/`retire` and the lock ordering pinned once, parameterized by "is there live work".

Lower severity than it looks because the idle worker is cheap and correct, but it is the kind of
asymmetry that hides the next bug.

## inventory_stream never checkpoints mid-walk

`inventory.rs`: a page with a `nextLink` yields `PageBoundary::Page` with `checkpoint: None`, and
`current_url` advances only in memory. Only the final `deltaLink` page produces a
`Checkpoint::Change`.

`changes_stream` does the opposite: it checkpoints `advanced_through` at every page boundary, and
`GraphPageMarker` exists precisely to carry that. So a 200k-message initial sync that drops its
connection on the last page restarts from page one, while the incremental pass that follows it is
fully resumable. That is backwards; the initial walk is the expensive one.

The `GraphCursorPayload` for a mid-inventory position would need a `delta_link` it does not have
yet, which is presumably why this was skipped, but a payload variant carrying only a page marker
(and a `changes_stream` that refuses to resume one) is expressible, and `establish_initial_cursor`
already has a reseed path for anything it cannot honor.

## Smaller observations

- **`decode_cursor` asymmetry** (`cursor.rs`): the outer `ChangeCursor::advanced_through`, when
  present, overwrites the payload's own; when absent, the payload's survives. Two sources of truth
  for one field with a silent precedence rule. Since `encode_cursor` always writes both from the
  same value they cannot disagree today, but nothing enforces that, and the resume URL is chosen off
  whichever won.
- **`subscribe_graph` discards the scopes it grouped** (`push.rs`, `for (resource, _) in grouped`).
  A `GraphSubscriptionGroup` knows its server ids and resources but not which `CursorScope`s it
  covers, so a terminal renewal failure emits `Terminated` with no scope attribution: the engine
  cannot tell which scopes lost coverage. Given how carefully every other error path in this crate
  carries `ErrorScope`, this one is conspicuous.
- **Move sends `If-Match` on `POST /messages/{id}/move`** (`mutate.rs`). Graph likely does not honor
  a precondition on the move action; if so, the `Move` etag preflight (`refresh_missing_etags`, one
  GET per uncached id) is buying nothing while `mutation.concurrency: StateBased` implies it is.
  Worth verifying against the live service; hunter was not certain either way.
- **`translate_ews_scopes` fails the whole subscription on one refused id** (`push.rs`).
  `reconcile_translated_ews_scopes` collects into `Result`, so a single stale folder id kills
  `push_subscribe` for every other folder. Everywhere else in this crate a per-item failure on a
  multi-item surface is filed per item and the rest proceeds, but here `push_subscribe` answers per
  request, so the design is at least self-consistent. Still, the practical effect is that one
  deleted folder in the engine's scope list disables push entirely.
- **`generate_client_state()` is minted per resource and immediately discarded** (`webhooks.rs`)
  under the legacy `with_push_endpoint`. The reference doc is honest about it, but it means that
  constructor produces subscriptions no receiver can authenticate. Pre-1.0: delete
  `with_push_endpoint` and make the secret mandatory rather than keeping a constructor whose
  documented behavior is "your webhook receiver cannot validate anything".
