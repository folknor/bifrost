# Bug hunt: bifrost-jmap protocol layer

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope: `crates/jmap/`
excluding `crates/jmap/src/sync/` - dispatch, transport, module pattern,
capabilities, request/response types, WebSocket/EventSource plumbing.

Hunter's note: reran the one test whose mechanism it doubted
(`response_frame_is_rebuilt_into_a_response`) - passes, so that doubt is retired.

## Confident findings

(Findings 1 and 2 - the WebSocket request door's missing correlation and
missing session-state comparison - are fixed on the "finish it" branch, and
`send_ws` is kept. `WebSocketResponse` now decodes RFC 8887 `requestId` and
`WebSocketMessage::Response` carries it, so a response frame can be matched
to the request it answers; the id counter moved from the per-connection
`WsStream` to the `Client`, so ids no longer restart at 0 on reconnect (a
late response from the old connection carrying an id the new one is about to
reuse is worse than no correlation); and `frame_stream` runs the same
`Client::note_session_state` comparison the HTTP door runs, on every response
frame including one whose method responses fail to decode. Pinned by
`a_response_frame_carries_the_request_id_it_answers`,
`every_response_frame_reports_its_session_state` and
`websocket_request_ids_do_not_restart`; the first two ablated and confirmed
failing.

The await side is now built too: `PendingRequests` on the `Client` keys
waiters by `requestId`, `Client::send_ws_awaiting` /
`Request::send_ws_awaiting` register one before the frame is written
(under the sink lock, so a reconnect cannot slip between registration and
send), and `frame_stream` routes a matching `Response`, decode failure, or
id-carrying `RequestError` to its waiter instead of yielding it. `send_ws`
is kept as the fire-and-forget door. Unknown ids fall through to the
stream as before; a dropped `PendingResponse` deregisters; a reconnect
fails every waiter of the replaced connection with `WebSocketClosed`
(retryable), and an ended read stream fails only its OWN generation, so an
old stream drained after a reconnect cannot reap the live connection's
waiter. `send_ws` also grew the missing `maxSizeRequest` guard, on the
encoded frame, raising the new `Error::RequestSizeLimit`. Pinned by
`two_in_flight_requests_are_told_apart_on_the_read_stream`,
`a_response_for_an_unknown_id_still_reaches_the_stream`,
`a_dropped_waiter_leaves_no_registration_behind`,
`a_reconnect_fails_every_pending_waiter`,
`an_ended_stream_fails_only_its_own_generation`,
`an_ended_stream_fails_its_own_pending_waiters`,
`a_request_error_fails_the_waiter_it_names`,
`an_undecodable_response_frame_fails_its_waiter`,
`push_frames_are_unaffected_by_a_pending_request`,
`an_oversized_websocket_frame_is_refused_before_the_wire` and
`a_zero_max_size_request_is_not_enforced` - each ablated and confirmed
failing (the end-of-stream one ablates into a HANG, which is the defect
it guards, caught by brokkr's per-test timeout).)

(Finding 3 - no JSON Pointer escaping in dotted patch paths - is fixed
crate-wide: `core::set::escape_json_pointer_token` now guards every
`format!`-built path site (keywords, mailboxIds, shareWith, addressBookIds,
calendarIds, and the sync layer's RSVP participant path). Pinned by
`keyword_and_mailbox_patch_paths_escape_json_pointer_tokens` with
revert-and-confirm; the rule is stated in `reference/jmap.md`.)

(Finding 5 - `SetResponse::new_state()` fabricating `""` for an absent
`newState` - is fixed: `new_state()` and `into_new_state()` are `Option` at
the boundary, so absence and a genuinely empty state stay distinguishable
instead of colliding on the sentinel the state cache gives its own meaning.
Every caller advances the cache only on a present, non-empty state. Pinned by
`an_absent_set_new_state_is_not_an_empty_one`; its bite is type-level - the
pre-fix `&str` accessor cannot satisfy the assertion at all.)

(Finding 7's ordering half is fixed: `Response::get` removes in place
instead of `swap_remove`, so several responses under one call id are read in
the order the server sent them and later lookups of unrelated handles are not
displaced. Pinned by
`repeated_call_ids_are_read_in_the_order_the_server_sent_them`; note its FIRST
shape passed against the bug by coincidence - with four same-id responses the
swapped-in tail is observable, and the ablation then failed with "fourth"
where "second" was due. Modelling multiplicity explicitly - a `get_all`
returning every response for a handle - is deliberately NOT done: nothing
wired produces it, and an unused accessor is dead surface. The generic
envelope now at least does not scramble.)

(Finding 8 - the empty-string fallback account id riding out as
`"accountId": ""` - is fixed at the door that would have serialized it:
`Request::call` refuses an empty account id with
`Error::NoPrimaryAccount { capability }`, naming the method's own capability,
the same variant `primary_account::<C>()` already raises and the sync error
table already classifies. Every method this crate defines carries an
`accountId`, so the guard needs no per-method exception. Pinned by
`a_session_without_a_primary_account_refuses_to_build_a_request` with
revert-and-confirm. The wrong-account half of the finding - a session
advertising only calendars serving mail-ish generic requests off the calendar
account - is untouched: that is the documented `build()` fallback, and
narrowing it is a product decision, not a defect fix.)

## Suspicions / lesser notes

- ~~**Malformed capability objects downgrade to "absent".**~~ FIXED. The core
  block now parses into its own `Capabilities::CoreMalformed` variant, read
  through the three-state `Session::core_capability_state()`
  (`Absent`/`Malformed`/`Present`), so a present-but-unparseable block is
  `Protocol(ContractViolation)` at `capabilities::build` and `Invalid` at
  `CallLimit` instead of masquerading as unadvertised (which had the engine
  reopening forever). Every core limit is an `Option<usize>`, so an omitted
  field no longer zero-fills into "advertised 0"; `build` refuses omitted and
  zero alike as contract violations (RFC 8620 §2 makes them mandatory), and
  the WS `maxSizeRequest` guard and the foreign-probe concurrency reader take
  the `Option` explicitly. Pinned in `core/request.rs` (`CallLimit` lanes),
  `sync/capabilities.rs` (malformed and omitted-limit refusals) and
  `tests.rs`, each confirmed to bite.
- **WS Response decode double-round-trips** (`json!` Value rebuild then
  `from_value`) - works (verified), but it re-allocates every method response; a
  `RawValue`-preserving envelope would decode once.
- **`ByteTally` counts only `/jmap/api` responses.** Documented, but it means
  metered streams that also `download` (raw RFC822, sieve script bodies)
  under-report - blob bytes are typically the *bulk* of the traffic those paths
  cause.
- **SSE stream teardown on one malformed event payload** (`break 'events` in
  stream.rs) where SSE's design intent is skip-and-continue - defensible, but
  it means one garbled frame costs a reconnect and replay. (The parser's
  leading-BOM nit is fixed; the `MethodError` description/limit discard,
  `frame_stream` spin trap, `request_id` field, and `well_known_session_url`
  decision note are closed via fixes/comments.)

## Structural observation

The layer is in better shape than most: the CallLimit three-state, the atomic
`SessionState`, the RFC 6570 encoding, and the frame-level WS test seam are all
carefully reasoned. The one structural gap worth a real investment was the
**WebSocket request/response half** (findings 1-2). The owner chose "finish
it" over "delete `send_ws`", and that door is now complete: correlation,
session-state comparison, the pending-request map with its reconnect and
drop teardown, and the `maxSizeRequest` guard (the `maxCallsInRequest`
guard already came free via the shared `call` door). Nothing of this
finding is left open.

Files most relevant: `crates/jmap/src/client_ws.rs`,
`crates/jmap/src/core/set.rs`, `crates/jmap/src/email/set.rs`,
`crates/jmap/src/event_source/stream.rs`, `crates/jmap/src/client.rs`,
`crates/jmap/src/core/session.rs`, `crates/jmap/src/core/response.rs`.
