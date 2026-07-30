# bifrost-jmap protocol objects - bug hunt

Scope: `crates/jmap/src/` **excluding** `core/` and `sync/`. That is the
~20 protocol object modules plus `client.rs`, `client_ws.rs`,
`account.rs`, `transport_reqwest.rs`, `lib.rs`, `tests.rs`.

`core/` and `sync/` were read; where a finding in my scope
is only reachable through a `sync/` call site, that call site is named so
the triage has the whole path.

Everything below is either **BUG** (a wrong thing that a server will act
on), **GAP** (untested/unprotected behaviour that matters), **SMELL**
(works today, easy to hold wrong) or **NIT**.

Tests landed in this pass are listed at the end.

---

## S8 (SMELL) - two residual SSE-parser deviations from the WHATWG rules

**Where:** `crates/jmap/src/event_source/parser.rs`.

The substantive deviations are closed: an oversized field or an oversized
accumulated `data` buffer now errors once and resynchronises on the next
blank line; `push_bytes` appends to an unconsumed buffer instead of
clobbering it at a stale offset; a repeated `id` line replaces rather
than appends; the last-event-ID buffer persists across events and a
colonless `id` line clears it; and a block with no `data` field (a
comment-only keepalive) dispatches nothing at all.

What is left is cosmetic. `Init` silently ignores a leading space, where
SSE treats it as the first character of a field name; and a field name is
capped at `MAX_EVENT_SIZE` but a comment line is not, so a pathological
server could stream an unbounded comment. Neither is reachable from a
JMAP server behaving even approximately correctly.

---

## S9 (SMELL, wire-or-remove decision) - the EventSource path has no caller

**Where:** `crates/jmap/src/event_source/` and its `Client::event_source()`
entry point.

Surfaced by the round-2 fix-and-commit agent: `event_source()` has no
caller anywhere in the workspace. `sync/` push is WebSocket-only
(`reference/jmap.md` documents this), so the whole EventSource stack -
now spec-correct after the round-2 SSE fixes - is dead code from the
`Account` impl's point of view. The round-2 fixes are latent-correctness
for a path nothing reaches, and the Last-Event-ID resume semantics have
no live reconnect path to serve.

Decision needed: either wire EventSource in as the push fallback for
servers without RFC 8887 WebSocket support (its plausible purpose), or
remove the module. S8's residual parser deviations only matter under the
"wire it" branch.

---

## N1..N6 (NITs)

- **N1** `Address.parameters` (`email_submission/mod.rs:127`) has no
  `skip_serializing_if`, so every envelope address serialises
  `"parameters": null`. Legal per RFC 8621 §7.1, just noise on every
  submission. Pinned in `email_submission_wire::envelope_address_parameters`.
- **N2** `Thread` (`thread/mod.rs:14-18`) requires both `id` and
  `emailIds`; a `Thread/get` with a partial `properties` projection fails
  the decode. Nothing does that today. Pinned in
  `misc_mail_object_decode::thread_requires_both_properties`.
- **N3** `principal::Property::ShareWith = 14` - `Principal` has no
  `shareWith` property in RFC 9670 (it has `accounts`); the variant looks
  copy-pasted from `Mailbox::Property`.
- **N4** `contact_card::query::Filter::Nickname` serialises as
  `"nickname"`. I believe RFC 9610 §2.3.1 spells the filter condition
  `nickName` (matching the JSContact `nicknames` property), but I could
  not verify the RFC text offline. Worth one grep of the spec - it is a
  one-character fix if I am right and a silently-ignored filter if I am
  not. Deliberately **not** pinned by a test, since I would be pinning a
  guess.
- **N5** `URLPart::parse` accepts `"{{a}"` (a second `{` while already in
  a parameter is silently swallowed). Malformed input that decodes
  anyway.
- **N6** `Display for Header` (`email/mod.rs:652-659`) forwards the
  formatter to `self.name.fmt(f)`, so a `{:>20}` on a `Header` pads the
  name rather than the whole token. Cosmetic; `Header` is only ever
  `to_string()`d.

---

## Tests landed

All in files this pass owns. Every test pins behaviour **as it exists
today**; the ones documenting behaviour I believe is wrong say so in a
comment directly above them ("BUG, documented rather than endorsed" /
"Documented, not endorsed").

`crates/jmap/src/tests.rs` (new modules, appended):

- `method_name_and_capability_table` - `M::NAME` and `M::Cap::URI` for
  every method struct in the crate (~60 methods). Nothing else covered
  these; a typo in either is a runtime `unknownMethod` /
  `unknownCapability`, never a compile error. Also pins the
  non-obvious placements: Identity under `submission`, ShareNotification
  under `principals`, Blob/copy under `core`, `*/parse` under the
  `:parse` sub-capabilities.
- `email_header_property_grammar` - the `header:<name>[:<form>][:all]`
  grammar both directions, all seven forms, the malformed cases, and
  `Property` serde including `Other`.
- `email_object_decode` - `Email` decode, the header-form aliases, the
  flattened header map, and extension-property tolerance.
- `email_query_wire` - every `Email/query` filter condition's wire name,
  the `header` two-element form, UTCDate formatting, comparator
  flattening (incl. `hasKeyword`'s extra field), `collapseThreads`,
  filter-operator nesting.
- `email_set_patch_shapes` - dotted-path null/true semantics, the
  clearing rule in both directions (a path setter drops the wholesale
  property; a wholesale setter drops both the children and an exact raw
  entry, so a `null_property("keywords")` cannot survive alongside
  `keywords(...)` as a duplicate key), the raw/null escape hatches.
- `mailbox_wire` - role wire names + case folding, owned and escaped role
  decoding, the
  create sentinels, create-id references, `Mailbox` decode incl. rights
  defaulting.
- `email_submission_wire` - envelope/parameter shapes incl. the RFC 4865
  `holduntil` form, `#c0` create-id references on both onSuccess
  arguments, `undoStatus` patch, delivery-status decode.
- `settings_object_wire` - identity and vacation patch wire shapes,
  vacation decode, Sieve activation references, `SieveScript/validate`
  error decode.
- `patch_defaults` - empty default PatchObjects and create sentinels.
- `set_error_vocabulary` - all 25 known `SetErrorType` codes both
  directions plus the `Other(code)` gate-5 invariant and `Display`.
- `wire_enums_without_a_catch_all` - unknown wire-enum fallbacks.
- `data_type_wire` - Display/Serialize agreement for every `DataType`,
  `MDN` casing, and unknown-value round-tripping.
- `session_capability_fallbacks` - the WebSocket `supportsPush` default
  plus the `Other` passthrough.
- `url_template_parsing` - `URLPart::parse` happy paths and all four
  rejection cases, plus the blob parameter set.
- `blob_management_wire` - RFC 9404 `Blob/upload` create shape and
  `DataSource` concatenation, `Blob/get`'s named-vs-dynamic field split,
  `Blob/lookup` round trip.
- `quota_field_three_state`, `address_book_wire`, `calendar_wire` - the
  `Field<T>` three-state contract where it IS implemented correctly, plus
  RFC-shape decodes (`mayRSVP` casing, rights defaulting).
- `calendar_event_patch_nesting` - calendar membership dotted paths and
  no-overlap, `set_property` dotted paths, Get/Set argument flattening.
- `misc_mail_object_decode` - N2, `SearchSnippet/get` request shape,
  `Email/import` `iN` create-id keying.
- `push_subscription_wire` - the non-account-scoped `accountId` omission.
- `principal_acl_vocabulary` - the RFC 8621 `shareWith` property names
  across all ten variants.

`crates/jmap/src/event_source/parser.rs` (appended to the existing
`mod tests`): repeated-id replacement, unconsumed-buffer preservation,
bounded and resynchronising oversized fields and data, comment-only
blocks dispatching nothing, an empty `data` field still dispatching, the
last-event-ID buffer persisting across events, and a colonless `id` line
clearing it. The pre-existing `parse` transcript was re-pinned to spec
behaviour (the keepalive block no longer yields a phantom event, and the
event after an `id` carries that id forward).

`crates/jmap/src/event_source/stream.rs` (new `mod tests`, both
`#[tokio::test]`): malformed event payloads emit one error and terminate
the EventSource stream; comment heartbeats neither surface as events nor
tear the stream down, and the resume token survives them. Both drive a
stub `HttpTransport` + `SseTransport`, the same capture-and-assert
pattern `core/tests.rs::envelope` uses for the request envelope.

---

Round 3 additions (the fix pass's own pins, plus the three the cold
review turned up):

- `crates/jmap/src/blob/download.rs::tests` and
  `crates/jmap/src/blob/upload.rs::tests` - a capturing `HttpTransport`
  behind `Client::with_transport` renders a whole download / upload URL
  and asserts RFC 6570 §3.2.2 simple-string expansion: every character
  outside ALPHA / DIGIT / `-` / `.` / `_` / `~` percent-encoded. The
  first fix-pass attempt at S6 added `&` and `=` to a deny list, which
  left `+`, `;`, `@`, `!`, `,` and `$` through - a query-position
  `{type}` of `application/ld+json` reached the server as
  `application/ld json`. The deny list is gone; both call sites now
  share `core::session::encode_template_value`.
- `crates/jmap/src/client.rs::session_state_tests` -
  `refresh_replaces_the_session_and_everything_derived_from_it`. S2 gave
  custom-transport clients a session URL, but `refresh_session` replaced
  only the `Session`; `apiUrl`, the three URL templates and
  `default_account_id` stayed derived from the session it replaced, so a
  refreshed client reported the new server through `session()` while
  every request still went to the old one. All of it now lives in one
  `Arc<SessionState>` swapped under one lock. The test drives two
  distinct sessions through a stub transport and asserts the second
  request's `apiUrl` moved.
- `crates/jmap/src/tests.rs::participant_identity_wire::patch_omits_calendar_address_rather_than_nulling_it`
  - B9's patch modelled `calendarAddress` as `Field<String>`, so
  `calendar_address(None)` emitted `"calendarAddress": null`. draft-26 §3
  makes the property required and non-nullable, so that PatchObject is a
  guaranteed `invalidProperties` rejection. The patch field is now
  omitted-or-String and the setter takes a `String`.

---

## Not done, and why

- **`client_ws.rs` frame handling beyond the subprotocol check.** The
  `WebSocketMessage_` decode is partly covered by the existing
  `deserializes_single_type_state_change_frame`; the close/error/binary
  arms are inside the `async_stream::stream!` and need a stub WebSocket
  transport of the same shape as the G1 double.
- **`principal/availability.rs`, `principal/query.rs`,
  `share_notification/query.rs`, `sieve/query.rs`,
  `calendar_event_notification/query.rs`, `quota/query.rs`.** Read for
  bugs (the `NotificationType` catch-all is closed), not covered by new
  tests - they are thin filter/comparator enums structurally identical to
  the ones now pinned in `email_query_wire` and `tests.rs`'s existing
  `query_filter_serialization`, and I judged a second copy of the same
  table lower value than the findings above.
- **N4 (`nickName` vs `nickname`).** Needs the RFC 9610 text, which I do
  not have offline. Flagged, not pinned - pinning a guess is worse than
  leaving it open.
- **The `Email::size()` / `has_attachment()` / `EmailBodyPart::size()`
  sentinel collapse.** Already tracked as an open decision in
  `plans/jmap/DEFERRED.md` and `plans/jmap/API.md`; not re-litigated here.
