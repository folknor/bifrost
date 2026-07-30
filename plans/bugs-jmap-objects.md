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

## B9 (BUG, severity: medium, latent) - `ParticipantIdentity` models `sendTo`, which the calendars draft this crate targets may no longer define

**Where:** `crates/jmap/src/participant_identity/mod.rs:49-94`
(`ParticipantIdentity`, `ParticipantIdentityCreate`,
`ParticipantIdentityPatch`, `Property::SendTo`).

The original B9 was the default-patch leak on `PushSubscriptionPatch` and
`ParticipantIdentityPatch` (plus the two Create shapes missing their
sentinels). That half is closed: those Patch shapes are `Field<T>` now
and the creates install sentinels. What remains is the shape itself.

The crate targets draft-ietf-jmap-calendars-26. A review pass reports
that -26 section 3 defines `ParticipantIdentity` with a **required
`calendarAddress`** and no `sendTo` at all. I could not check the draft
text (this environment has no network), and per N4's precedent a guess is
not worth pinning, so nothing here has been renamed and no test asserts
either spelling.

If the report is right, three things follow: a compliant identity's
address is silently dropped on decode (serde ignores the unknown key, so
`ParticipantIdentity/get` still succeeds but `send_to()` is always
`None`); `ParticipantIdentityCreate` cannot express a valid create and a
server should answer `invalidProperties`; and `Property::SendTo` names a
property that will not round-trip through `properties`.

**Reachability:** the module is `#![allow(dead_code)]` with no `sync/`
call site, so this is latent until the calendar conveniences are wired.

**Next step.** Read draft-ietf-jmap-calendars-26 section 3 and, if it
says `calendarAddress`, rename the field on all three shapes (a required
`String` on Create, `Field<String>` or plain `Option<String>` on Patch),
rename the `Property` variant, and pin the wire name in
`tests.rs::patch_defaults` / a new participant-identity module.

---

## S7 (SMELL) - `IdentityPatch::reply_to(Some(<empty>))` / `bcc(Some(<empty>))` silently send nothing

**Where:** `crates/jmap/src/identity/mod.rs:93-99` (`skip_if_empty_list`),
`crates/jmap/src/identity/set.rs:51-67`.
**Live call site:** `crates/jmap/src/sync/pim.rs::identity_update` (~line 996).

Noticed while confirming that B2 does not reproduce - it does not:
`IdentityPatch` has a hand-written `Default` installing the
`Some(Vec::new())` sentinels the predicate wants, and `reply_to(None)`
does emit `null` and does clear. But the sentinel is also the encoding of
"an empty list", so the *value* `Some([])` is indistinguishable from the
default and is skipped. `identity_update` forwards
`bifrost_types::IdentityPatch::reply_to` straight through as
`item.reply_to(Some(values))`; if a caller expresses "no reply-to" as an
empty vector rather than `None`, the edit is a silent no-op.

Whether an empty vector is a legal way to say "clear" is a
`bifrost_types` contract question, which is why this is a smell and not a
bug. `Field<Vec<EmailAddress>>` would remove the ambiguity here the same
way it did on `VacationResponsePatch`.

---

## G1 (GAP) - the request envelope has no test double

**Correction.** The original G1 claimed this crate could not host an async
test because `crates/jmap/Cargo.toml` had no `[dev-dependencies]`. That
was **false when it was written**: line 64 of the manifest already
carries `tokio = { workspace = true, features = ["macros", "rt",
"test-util"] }`. Nothing about the manifest changed in either fix round;
the blocker was believed, not real. `#[tokio::test]` works today - see
`event_source/stream.rs::tests`, which drives the whole EventSource
stream through a stub `SseTransport`.

What actually remains is a **coverage** gap, not a tooling one. Nothing
exercises the request envelope: `Client::with_transport(stub, session)`
plus a `HttpTransport` stub that captures the outgoing body would pin
`using` array construction and de-duplication, the `methodCalls` tuple
encoding, `accountId` injection via `JmapMethod::set_account_id`,
`CallHandle` -> `Response::get` call-id matching, method-error routing,
and `send_methods` tuple extraction. None of that has a single test.

The stub is cheap - `event_source/stream.rs::tests` already writes the
`HttpTransport` half of one (every method `unreachable!()`) purely to
satisfy the trait bound. Turning that into a capture-and-assert double is
the highest-value follow-up in this file.

A byte-level transcript over `tokio::io::duplex` is likewise reachable,
but it buys less here: the crate's transport seam is `HttpTransport` /
`SseTransport`, not a socket, so a duplex would only re-test reqwest.

---

## S1 (SMELL) - `CalendarEventPatch` / `ContactCardPatch` reuse the Create setters verbatim

**Where:** `crates/jmap/src/calendar_event/set.rs` (`ce_setters!` applied
to both `CalendarEventCreate` and `CalendarEventPatch`),
`crates/jmap/src/contact_card/set.rs` (`cc_setters!`, same).

`calendar_id(id, false)` writes a **nested** object
`{"calendarIds": {"cal-1": null}}`. On a create that is at worst odd. On
a `/set update` it is a wholesale replacement of `calendarIds` with a map
containing a null, not the `"calendarIds/cal-1": null` dotted path
RFC 8620 §5.3 asks for - so it also drops every calendar membership the
caller did not name, which is exactly the class of bug the
`EmailPatch::submitted_to_sent` doc comment was written to prevent.

The existing test `tests.rs::patch_object_null_semantics` pins the Create
side, and per the standing bug-hunt rule an existing passing test wins,
so I have **not** touched it - I have only added the Patch-side
observation as
`tests.rs::calendar_event_patch_nesting::patch_calendar_id_nests_instead_of_using_a_dotted_path`.
The type-state split exists precisely so the two shapes can differ;
applying one macro to both throws it away. Whoever wires
`calendar_ops.rs` membership edits should split the macro first.

---

## S2 (SMELL) - `Client::with_transport` produces a client that cannot refresh its session

**Where:** `crates/jmap/src/client.rs:277-304`.

`with_transport` sets `session_url: String::new()`. `refresh_session()`
on such a client issues `GET ""`. Harmless today (only the reqwest path
constructs a session URL, and nothing calls `refresh_session`), but the
constructor is the documented custom-transport entry point and it hands
back a half-functional object. Either take the session URL as a
parameter or make `refresh_session` return `Error::InvalidUrl` when it is
empty.

Adjacent: `ClientBuilder::connect` builds `format!("{url}/.well-known/jmap")`
with no trailing-slash normalisation, so a configured base URL ending in
`/` yields `//.well-known/jmap`.

---

## S3 (SMELL) - `send_ws` / `enable_push_ws` / `disable_push_ws` send an empty frame on encode failure

**Where:** `crates/jmap/src/client_ws.rs:250-257, 277-283, 295-300`.

```rust
Message::text(serde_json::to_string(&frame).unwrap_or_default())
```

`unwrap_or_default()` turns an encode failure into an **empty text
frame** rather than an error. The payloads involved cannot currently fail
to serialise (`method_calls` is already a `Value`, `using` is
`&'static str`s, the push frames are two fields), so this is not a live
bug - but the crate's own `Error::RequestEncode` variant exists for
exactly this and the doc comment on it says outbound encode sites must
use it explicitly rather than relying on `?`. These three sites do
neither.

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

## S6 (SMELL) - blob download URL percent-encoding leaves `&` and `=` alone

**Where:** `crates/jmap/src/blob/download.rs:12-23` (`PATH_SEGMENT`),
`upload.rs:37-48` (same set).

The set encodes `/ ? # % { } < > " ` ` and space, but not `&` or `=`.
RFC 8620's download URL template routinely puts `{name}` and `{type}` in
a **query** position (the session fixtures in this repo use
`.../{name}?accept={type}`), and `name` comes from an attachment
filename, i.e. it is attacker-controlled. A filename containing `&`
appends parameters to the download request. `?` is encoded so a new query
string cannot be started, which caps the impact at "inject extra params
into an existing query" - but the set should just include `&` and `=`.

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
- `calendar_event_patch_nesting` - S1, `set_property` dotted paths,
  Get/Set argument flattening.
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
stub `HttpTransport` + `SseTransport` - the first async tests in this
crate, and the pattern G1 asks to be extended to the request envelope.

---

## Not done, and why

- **A transport-level test double** (G1). Still the highest-value thing
  in scope, but nothing blocks it: the manifest has always had the tokio
  dev-dependency, and `event_source/stream.rs::tests` now proves an async
  test with a stub transport compiles and runs here. A capturing
  `HttpTransport` stub behind `Client::with_transport` would pin the
  request envelope (`using` construction and de-duplication, `methodCalls`
  tuple encoding, `accountId` injection via `JmapMethod::set_account_id`,
  `CallHandle` -> `Response::get` matching, method-error routing,
  `send_methods` tuple extraction) and none of that has a single test
  today. It was left because this round was scoped to the decode and
  parser cluster.
- **`blob/download.rs` URL construction.** The templating + percent-encoding
  logic (S6) is inside an `async fn` that immediately calls the transport,
  so pinning it wants either the G1 stub or a pure
  `build_download_url(&[URLPart], &BlobRef) -> String` extraction. That
  extraction is a refactor, and the brief splits tests from fixes, so I
  left it. It is a five-line change and would make S6 verifiable.
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
