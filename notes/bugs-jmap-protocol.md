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
- ~~**The malformed lane covered only the core block.**~~ FIXED (lateral
  finding of the above). `try_cap!` still fell back to `Capabilities::Other`
  for every NON-core capability, so a present-but-malformed `websocket`,
  `mail`, `submission`, `sieve`, `quota`, `blob`, `calendars`, `contacts` or
  `principals` block read as unadvertised - concretely, a websocket block
  missing its mandatory `url` derived `PushCapability::None` with no
  diagnostic. The fallback is now the generic `Capabilities::Malformed`, so
  `Other` means only "a URI this crate does not model", and
  `session_cap_accessor!` generates a `CapabilityState` (`Absent` /
  `Malformed` / `Present`) reader for every capability from the one
  mechanism. Readers split by the Unadvertised-versus-Invalid rule: the
  REQUIRED blocks (core, mail) are `Protocol(ContractViolation)` at
  `capabilities::build` with the URI named; the optional ones degrade but
  warn by URI via `Session::malformed_capabilities()`, push through its own
  arm; and `connect_ws` separates `Absent` (`WebSocketNotConnected` ->
  `Unsupported`) from `Malformed` (the new `Error::MalformedCapability`,
  mapped to `Protocol(ContractViolation)` naming the URI). Pinned by
  `a_malformed_non_core_block_is_malformed_and_not_absent`,
  `every_modelled_capability_uri_has_a_malformed_lane`,
  `a_malformed_websocket_block_degrades_push_without_reading_as_absent`,
  `a_malformed_mail_block_is_a_named_contract_violation`,
  `a_malformed_optional_block_does_not_fail_open` and
  `a_malformed_capability_error_classifies_as_a_named_contract_violation`.
  Ablated by restoring the `Other` fallback: five tests fail. The error-map
  arm's bite is type-level (the match is exhaustive), and the push arm's own
  bite is the diagnostic, not the derived value - a malformed websocket block
  yields no push either way, which is why it went unnoticed.
- ~~**`CoreCapabilities::max_concurrent_upload` has no reader.**~~ CLOSED as
  deliberate, not wired. Every upload door in the crate awaits one upload at a
  time (`Account::upload` is a single request; `pim::attachment_upload`, the
  send path's attachment loops and `filters.rs`'s script upload all iterate
  sequentially), so there is no concurrency for the limit to govern and a
  reader would have to invent the fan-out first. The crate's only overlapping
  requests are the foreign-account probes, which are API calls already bounded
  by `maxConcurrentRequests`. The field and its `Option` shape stay so that
  adding concurrent uploads is a reader change, not a parser change; the
  decision and its condition are recorded in `reference/jmap.md`, and
  `core_max_concurrent_upload_is_parsed_even_though_nothing_reads_it` pins the
  decode (including omitted staying `None` rather than zero).
- ~~**A malformed Submission block silently disabled only `scheduled_send`.**~~
  FIXED. `submission_capabilities()` collapses `Absent` and `Malformed` into
  `None`, and `factory` read that as `max_delayed_send = 0` - so a server whose
  Submission object did not parse kept `send_message`, `draft_send` and the
  identity doors live off that same unparseable block while scheduled send went
  dark with no diagnostic. That is a malformed optional block degrading as a
  silently different VALUE, which the lane rule forbids; the reference's rule
  for an optional block is "the family degrades to off, by name". It now does:
  `resolve_optional_families` drops the Submission handle on `Malformed`, so
  the flag and the handle go together. `SubmissionCapabilities` also lost its
  container-level `#[serde(default)]`, for the same reason `CoreCapabilities`
  did - RFC 8621 §7 makes `maxDelayedSend` mandatory, so `{}` is a malformed
  block, not an advertised zero-second window. A well-formed `maxDelayedSend:
  0` still keeps the family with `scheduled_send` false. Pinned by
  `a_submission_block_without_max_delayed_send_disables_the_family` and
  `a_zero_delayed_send_window_keeps_the_submission_family`; the first ablated
  against both halves of the fix and confirmed failing.
- ~~**Family enable flags came from `primaryAccounts`, not from the block.**~~
  FIXED. Mail, calendars, contacts and sieve derived their family flag from
  `client.primary_account::<C>()`, which reads `primaryAccounts` alone, so a
  malformed block left the family fully advertised with only the generic
  `build` log line to show for it. `resolve_optional_families` now gates every
  optional handle on its own `CapabilityState`: `Malformed` drops the handle
  and warns with the URI and the family name, which takes the `PimSupport`
  flag (derived from `is_some()`) and the `JmapAccount` door (which reads the
  same `Option`) with it. `Absent` is deliberately left alone - that is the
  separate question of a session naming a primary account for a URI it does
  not advertise, and gating it here would drop families off working servers.
  Mail needed no change: a malformed mail block already fails
  `capabilities::build`, which fails the whole open, so the family cannot
  exist. Vacation has no typed block and so no malformed lane. Pinned by
  `a_malformed_optional_family_block_drops_its_own_handle_only`,
  `a_malformed_family_block_is_not_advertised_in_the_capability_snapshot` and
  the over-gating control `well_formed_optional_family_blocks_keep_every_handle`;
  ablated by making the gate always return `true`, and confirmed failing.
- ~~**Serde accepts a JSON ARRAY as a capability object.**~~ FIXED. Derived
  struct deserializers accept a sequence in field order, and
  `CalendarsCapabilities` / `ContactsCapabilities` default every field, so
  `"urn:ietf:params:jmap:calendars": []` parsed as PRESENT rather than landing
  in the malformed lane (and `["16"]` fed the core block a limit
  positionally). No per-struct visitor was needed: `try_cap!` now refuses any
  non-object value before the typed parse, so the one door every modelled URI
  passes through closes the array lane for all of them at once, per RFC 8620
  s2. Pinned by `a_capability_sent_as_an_array_is_malformed_and_not_present`,
  which sweeps every modelled URI and additionally asserts the core and
  calendars three-state readers; ablated by disabling the guard and confirmed
  failing.

- ~~**A refused open still paid for a full round of foreign probes.**~~ FIXED.
  `capabilities::build` ran after `seed_account_state` and the foreign-account
  fan-out, so a session the contract already condemns - malformed core or mail
  block, a zero or omitted mandatory core limit - still cost two primary probes
  plus one request per share against a server known non-conformant, every
  answer discarded with the refusal. The session-only half of the validation is
  now `capabilities::validate_session`, hoisted to the top of the new
  `validate_and_seed` stage that `open` calls; the half that needs probe
  results (the `PimSupport` gates, which depend on which shares seeded) stays
  in `build`, which still calls `validate_session` itself so the refusal does
  not depend on call order elsewhere. Pinned by
  `a_malformed_required_block_refuses_the_open_before_any_probe` - a scripted
  transport armed with NO replies, so a probe that does go out fails loudly -
  ablated by dropping the hoisted call and confirmed failing
  (`Transport(Network)` from the probe instead of
  `Protocol(ContractViolation)`). The two factory tests whose doc comments
  cited the old order (`an_absent_core_capability_still_probes_...`,
  `a_zero_call_limit_still_probes_...`) were NOT flipped: they pin the request
  BUILDER's refusal to read an absent or zero limit as a hard bound, drive
  `seed_account_state` directly, and that contract is unchanged - only their
  stale prose about `open`'s ordering was corrected.
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
