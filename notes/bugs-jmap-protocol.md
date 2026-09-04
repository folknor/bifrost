# Bug hunt: bifrost-jmap protocol layer

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope: `crates/jmap/`
excluding `crates/jmap/src/sync/` - dispatch, transport, module pattern,
capabilities, request/response types, WebSocket/EventSource plumbing.

Hunter's note: reran the one test whose mechanism it doubted
(`response_frame_is_rebuilt_into_a_response`) - passes, so that doubt is retired.

## Confident findings

### 1. The WebSocket request door cannot correlate responses - RFC 8887 `requestId` is dropped

`crates/jmap/src/client_ws.rs`: `WebSocketResponse` deserializes
`methodResponses`, `createdIds`, `sessionState` - but not `requestId`, the field
RFC 8887 s4.3.4 echoes so a client can match a `Response` frame to the request
it answered. `send_ws` dutifully assigns and returns a request id (`ws.req_id`),
but nothing downstream can ever use it: `WebSocketMessage::Response` carries no
id. Two in-flight WS requests are indistinguishable on the read stream. Today
this is latent (grep shows `send_ws` has no production caller outside
`core/tests.rs`), but the reference doc advertises "WebSocket requests through
the same `call` door" as a supported path, and the correlation half of that door
does not exist. Related: `connect_ws` resets `req_id` to 0 on every reconnect,
so ids also repeat across connections.

### 2. WS-delivered responses bypass session-divergence detection

`Client::send_request` (client.rs:571-577) compares `response.session_state()`
against the snapshot and bumps the `session_changes` watch - the mechanism the
whole scope-lifecycle `CapabilityChanged` story rests on. The WS path
(`frame_stream` -> `WebSocketMessage::Response`) rebuilds a `Response` and hands
it up without that comparison, and `send_ws` never sees the response at all. Any
future consumer of WS method calls silently loses staleness detection. Contract
asymmetry between the two doors of the same layer.

### 3. No JSON Pointer escaping in dotted-path patch keys

`email/set.rs`: `EmailPatch::keyword` builds `format!("keywords/{keyword}")` and
`mailbox_id` builds `format!("mailboxIds/{mailbox_id}")`. RFC 8620 s5.3 patch
paths are JSON Pointers, where `/` must be escaped as `~1` and `~` as `~0`.
Mailbox ids are safe (RFC 8620 id grammar is URL-safe), but IMAP-derived
keywords may legally contain `/` (IMAP atoms exclude parens, `%`, `*`, `"`,
`\` - not `/`). A keyword like `work/urgent` produces the pointer
`keywords/work/urgent`, which patches a nested key inside the keyword map's
`work` entry - the server rejects it (`invalidPatch`) or worse, on a lenient
server, does something else. Same shape applies anywhere in the crate that
builds dotted paths by `format!` (the sync layer does it too, e.g. RSVP's
participant path - outside this scope but same missing helper). The fix is one
`escape_json_pointer_token` used at every path-construction site. Work note:
this one fix covers the sync-layer sites too (grep the whole crate for
`format!`-built patch paths when applying it); do not file or fix the sync-side
instances separately.

### 5. `SetResponse::new_state()` fabricates an empty state string

`core/set.rs:302-308`: `new_state` is modeled `Option<String>` but the accessors
return `""` for absent. RFC 8620 s5.3 makes `newState` mandatory. An `""`
flowing out of here is indistinguishable from a real state to callers; the sync
layer's state cache documents special "explicitly empty" semantics for exactly
this shape, so a non-conforming server yields a silently poisoned cache entry
rather than a `ContractViolation`. The leniency in `GetResponse.not_found` is
documented and reconciled against; this one is neither.

### 7. `Response::get` picks an arbitrary response when call ids repeat

`core/response.rs:31-56`: RFC 8620 s3.2 explicitly allows a single method call
to produce *multiple* responses tagged with the same call id. `get` takes the
first positional match and `swap_remove`s (which also scrambles order for later
lookups). No currently-wired method does this, but the core envelope claims to
be the generic RFC 8620 layer and doesn't model it; a second `get` on the same
handle silently returns the next one, which some caller might even come to
depend on accidentally.

### 8. Empty-string fallback account id in `SessionState::derive`

`client.rs:105-108`: no `primaryAccounts` -> `AccountId::new("")`, and
`Request::new` bakes that into every generic `client.build()` request; the
method structs happily serialize `"accountId": ""`.
`primary_account::<C>()` errors properly with `NoPrimaryAccount`, but the
`Client::build()` path ships a malformed request instead of failing locally.
The reference even documents `build()`'s "lexicographically first primary
capability" fallback - which for a session advertising *only* calendars means
mail-ish generic requests quietly ride the calendar account.

## Suspicions / lesser notes

- **Malformed capability objects downgrade to "absent".** `session.rs`
  `try_cap!` falls back to `Capabilities::Other(value)` on a typed-parse
  failure, so `core_capabilities()` returns `None` and everything downstream
  (CallLimit, `capabilities::build`) treats a *present but malformed* core block
  as *unadvertised* - the reference carefully distinguishes `Unadvertised` from
  `Invalid`, but a core block whose `maxCallsInRequest` is `"16"` (string) lands
  in the wrong lane. Also, blanket `#[serde(default)]` on `CoreCapabilities`
  zero-fills any omitted limit, merging "absent field" and "advertised 0" - the
  two cases the reference says map to different recovery classes.
- **`MethodError` deserializes only `type`** (`core/error.rs`), discarding
  `description` and per-error extras (`limit` on
  `requestTooLarge`/`tooManyChanges`). The error-model docs prize evidence
  preservation; this drops the server's own explanation at the wire.
- **HTTP-side `ProblemDetails.request_id` can never populate** - the derived
  field is snake_case with no rename, and RFC 7807 JMAP bodies wouldn't carry it
  anyway; only the WS constructor fills it. Harmless but misleading.
- **`frame_stream` yields the error and keeps reading** after a
  `tokio_websockets::Error`. On a stream that errors persistently and never
  ends, a consumer that doesn't break on first error spins. The sync reader does
  break, so this is only a trap for future consumers.
- **WS Response decode double-round-trips** (`json!` Value rebuild then
  `from_value`) - works (verified), but it re-allocates every method response; a
  `RawValue`-preserving envelope would decode once.
- **`well_known_session_url` unconditionally appends `/.well-known/jmap`** to
  whatever the caller gave, so a caller holding the actual session URL cannot
  connect to it; and RFC 8620 puts the well-known at the origin root, not under
  an arbitrary path prefix. Deliberate-looking, but worth a decision note.
- **`ByteTally` counts only `/jmap/api` responses.** Documented, but it means
  metered streams that also `download` (raw RFC822, sieve script bodies)
  under-report - blob bytes are typically the *bulk* of the traffic those paths
  cause.
- **SSE parser** is genuinely solid (WHATWG corner cases tested); the one spec
  nit is no leading-BOM strip at stream start. Also a single malformed *event
  payload* tears down the whole SSE stream (`break 'events` in stream.rs) where
  SSE's design intent is skip-and-continue - defensible, but it means one
  garbled frame costs a reconnect and replay.

## Structural observation

The layer is in better shape than most: the CallLimit three-state, the atomic
`SessionState`, the RFC 6570 encoding, and the frame-level WS test seam are all
carefully reasoned. The one structural gap worth a real investment is the
**WebSocket request/response half** (findings 1-2): it is the only part of the
protocol layer that is wired but not honest - no correlation, no session-state
check, no `maxSizeRequest` guard on `send_ws`. Either finish it (a
pending-request map keyed by `requestId`, session-state comparison in
`frame_stream`) or delete `send_ws` down to push-only plumbing so the door stops
advertising what it can't deliver - that's an owner decision, not the hunter's
to make.

Files most relevant: `crates/jmap/src/client_ws.rs`,
`crates/jmap/src/core/set.rs`, `crates/jmap/src/email/set.rs`,
`crates/jmap/src/event_source/stream.rs`, `crates/jmap/src/client.rs`,
`crates/jmap/src/core/session.rs`, `crates/jmap/src/core/response.rs`.
