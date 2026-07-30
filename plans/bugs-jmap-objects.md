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

## B5 (BUG, severity: medium, latent-but-easy-to-trip) - one unknown property fails the whole `Email/get` decode

**Where:** `crates/jmap/src/email/mod.rs:156-158` (the flattened
`headers: HashMap<Header, Option<HeaderValue>>`), `:630-649`
(`Header::parse`).

`Email` collects every key it does not recognise into a `#[serde(flatten)]`
map whose KEY type is `Header`. `Header`'s deserializer calls
`Header::parse`, which returns `None` for anything that is not
`header:<name>[:<form>][:all]`, and the visitor turns that into a hard
serde error.

**Path to failure.** A server includes any property this struct does not
name - a vendor extension (`example.com:snoozedUntil`), a future RFC
property, or a JMAP extension the crate has not modelled - in an
`Email/get` response. `Response::get::<EmailGet>` returns
`Error::ResponseDecode`, which `sync/error.rs` maps to
`Protocol(ParseFailed)` / `ProviderContractViolation`. Not one property
is lost: the entire batch of hydrated emails is.

This is mitigated in practice because the sync layer always sends an
explicit `properties` list and RFC 8620 §5.1 says the server MUST honour
it. It is not mitigated for `Email/parse` (whose `properties` is
optional), for any future caller that omits `properties`, or for a server
that is merely sloppy.

**Proposed fix.** Give the flattened map a key type that cannot fail -
either reuse `Property` (which already has an `Other(String)` catch-all,
except that it *also* propagates `Header::parse` failure and would need
the same treatment), or attach a `deserialize_with` to the flattened
field that drops keys `Header::parse` rejects. A one-line alternative is
to make `Header::parse` fall back to `Header { name: value, form: Raw,
all: false }`, but that changes `Display` round-tripping and would let
non-header keys masquerade as headers.

Pinned by `tests.rs::email_object_decode::one_unknown_property_fails_the_entire_email_decode`
(with the positive control next to it).

---

## B6 (BUG, severity: low, latent) - `Role` cannot be decoded from a non-borrowable string

**Where:** `crates/jmap/src/mailbox/mod.rs:385-404`.

```rust
match <&str>::deserialize(deserializer)?.to_ascii_lowercase().as_str() { ... }
```

`&str`'s `Deserialize` only accepts `visit_borrowed_str`. Two real inputs
fail with `invalid type: string "inbox", expected a borrowed string`:

1. Any `serde_json::from_value` path (owned `Value` cannot lend `&'de`).
2. `serde_json::from_str` where the JSON string contains an escape -
   serde_json then has to unescape into a scratch buffer and calls
   `visit_str`, not `visit_borrowed_str`. `"inbox"` is a legal
   encoding of `"inbox"` and fails.

The whole `Mailbox` (hence the whole `Mailbox/get` response) fails, not
just the role.

Today's main path survives by luck: `Response::get` uses
`serde_json::from_str` over a `RawValue`, and role strings are
unescaped ASCII. But an `x-` role containing any escaped character, or
any future use of `from_value` on a `Mailbox`, trips it. Every other
hand-written deserializer in the crate (`Header`, `Property`,
`SetErrorType`, `define_open_property_enum!`) uses a `Visitor` with
`visit_str` or goes through `String`; `Role` is the outlier.

**Proposed fix.** `String::deserialize(deserializer)?` (one extra
allocation on a path that already allocates for `to_ascii_lowercase`), or
a `Visitor` implementing `visit_str`.

Pinned by `tests.rs::mailbox_wire::role_cannot_be_decoded_when_the_string_is_not_borrowable`.

Related, separately: the same deserializer lower-cases before matching,
so `Role::Other` does not round-trip byte for byte
(`"x-MyRole"` decodes to `Other("x-myrole")` and re-serialises
lower-cased). Pinned by `unknown_roles_survive_as_other_but_are_lower_cased`.

---

## B7 (BUG, severity: medium, latent) - the SSE stream's error handling breaks out of the wrong loop, and the parser then desynchronises

**Where:** `crates/jmap/src/event_source/stream.rs:68-112` and
`crates/jmap/src/event_source/parser.rs:40-47, 52-55, 148-151`.

```rust
loop {
    for event_result in parser.by_ref() {
        match event_result {
            ...
            Err(err) => { yield Err(err); break; }   // breaks the FOR
        }
        continue;                                     // no-op, last stmt
    }
    if let Some(result) = stream.next().await { parser.push_bytes(bytes); continue; }
    else { break; }
}
```

Every `break` inside the `match` leaves the **inner `for`**, not the
outer `loop`. Two consequences:

1. The intended "terminate the stream on a decode error" never happens.
   The stream yields the error, pulls more bytes and carries on. A server
   emitting malformed `data:` payloads produces an infinite error stream
   instead of a terminal failure.
2. Worse, the `break` leaves the parser mid-buffer. `EventParser::push_bytes`
   overwrites `self.bytes` **without resetting `self.pos`**, and nothing
   checks the `needs_bytes()` precondition the parser exposes for exactly
   this. The next poll resumes at a stale offset inside the *new* frame.

**Path to failure (2).** Frame A is `"data: one\n\ndata: two\n\n"` (22
bytes). One event is yielded at `pos = 11`. Something breaks the loop.
Frame B `"data: three\n\n"` (13 bytes) is pushed. Parsing resumes at index
11 of frame B, i.e. at its two trailing newlines: `two` is lost, `three`
is never seen, and a bogus empty `StateChange` event is emitted instead.
If frame B were shorter than 11 bytes, `bytes.get(self.pos..)` returns
`None`, `next()` returns `None` **without clearing `self.bytes`**, and the
loop spins pulling and discarding frames forever.

**Reachability:** `Client::event_source` has no caller - the Account layer
uses the WebSocket push path - so this is latent. It is the entire
correctness of the SSE path if it is ever switched on.

**Proposed fix.** Two independent edits: (a) label the outer loop and
`break 'outer`, or restructure so the error path returns; (b) make
`push_bytes` either assert `needs_bytes()`, append to the unconsumed
remainder, or reset `pos` and drop the old buffer explicitly. (b) alone
makes the parser safe against any caller.

Pinned (parser half only, since the stream half is async and this crate
has no async test harness - see G1) by
`event_source/parser.rs::tests::push_bytes_over_a_partially_consumed_buffer_resumes_at_a_stale_offset`.

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

## G1 (GAP) - this crate cannot host an async test at all

`crates/jmap/Cargo.toml` has **no `[dev-dependencies]` section**. `tokio`
is an optional *runtime* dependency without the `macros` feature, and the
workspace pins `futures = { default-features = false }`, so
`futures::executor::block_on` is not available either.

Consequence for this pass: the deliberate new capability the brief
described - a byte-level protocol transcript over `tokio::io::duplex` -
is not reachable. Neither is a `StubTransport` implementing the crate's
own `HttpTransport` trait, which is the higher-value double here:
`Client::with_transport(stub, session)` + `Account::call` would pin the
request envelope (`using` array construction, `methodCalls` tuple shape,
`accountId` injection, `Response::get` call-id matching, method-error
routing) end to end, in-process, with no network and no listener.

I did not add the dependency (the brief forbids editing `Cargo.toml`, and
the manifest is shared). **Ask:** add to `crates/jmap/Cargo.toml`

```toml
[dev-dependencies]
tokio = { workspace = true, features = ["macros", "rt"] }
```

which is what `crates/caldav` already does. With that one line the
transport-stub tests become writable and I would expect them to be worth
more than everything else in this pass combined - the request envelope is
currently entirely unproven.

(A hand-rolled `block_on` built on `std::task::Wake` would avoid the
dependency, but shipping a bespoke executor in a test module to dodge a
one-line manifest change is the wrong trade.)

---

## G2 (GAP) - `#[non_exhaustive]` wire enums with no `#[serde(other)]` arm

`DataType`, `Role`, `AlertTrigger` and `SetErrorType` all have a
catch-all: an unknown wire value degrades. These do not, and an
unrecognised value fails the decode of the whole containing response:

| type | file | RFC-defined values |
|---|---|---|
| `UndoStatus` | `email_submission/mod.rs:132` | pending / final / canceled |
| `Delivered` | `email_submission/mod.rs:155` | queued / yes / no / unknown |
| `Displayed` | `email_submission/mod.rs:168` | unknown / yes |
| `AlertAction` | `calendar_event/mod.rs:69` | display / email |
| `RelativeTo` | `calendar_event/mod.rs:78` | start / end |
| `IncludeInAvailability` | `calendar/mod.rs:183` | all / attending / none |
| `NotificationType` | `calendar_event_notification/mod.rs:75` | created / updated / destroyed |
| `principal::Type` | `principal/mod.rs:288` | individual / group / resource / location / domain / list / other |

Each is marked `#[non_exhaustive]`, which is the crate declaring that the
value set will grow - but the deserializers refuse to grow with it. The
calendars draft in particular is at -26 and still moving; a server
shipping a newer `alerts[].action` fails every `CalendarEvent/get`.

This is a judgement call rather than an outright bug (the RFC values are
closed today), so it is pinned as-is, with the divergence made explicit,
in `tests.rs::wire_enums_without_a_catch_all`. If the answer is "add
`#[serde(other)] Unknown` everywhere", `AlertTrigger` is the model.

Related, and slightly worse: `DataType::Other` is a deserialize-only
catch-all that nonetheless **serialises**, as the literal `"Other"`.
Anything that decodes a server's type name and echoes it back - the
`WebSocketPushEnable.dataTypes` union built in `sync/push.rs`, a
`PushSubscription.types` round-trip - will ask the server to subscribe to
a data type called `Other`. Pinned by
`tests.rs::data_type_wire::other_serialises_as_a_literal_that_is_not_a_jmap_type`.

---

## G3 (GAP) - a malformed capability object silently disables the feature

**Where:** `crates/jmap/src/core/session.rs:84-132` (`try_cap!`). Out of
my edit scope; reported because the failure is invisible.

`try_cap!` falls back to `Capabilities::Other(value)` on **any** parse
failure. `WebSocketCapabilities` has no `#[serde(default)]` and both of
its fields are required, so a server that advertises

```json
"urn:ietf:params:jmap:websocket": {"url": "wss://..."}
```

(no `supportsPush`) produces a session where `websocket_capabilities()`
is `None`. `sync/capabilities.rs` reads that as "no push", the account
opens with `push: None`, and nothing anywhere reports why. The same
shape applies to `BlobCapabilities` (has defaults, safe),
`SieveCapabilities` (has defaults, safe) and `CoreCapabilities` (has
defaults - which is why a `{}` core capability decodes to all-zero limits
rather than falling to `Other`).

**Proposed fix.** `#[serde(default)]` on `WebSocketCapabilities` (absent
`supportsPush` == false is the natural RFC 8887 reading), and/or make the
`Other` fallback observable.

Pinned by `tests.rs::session_capability_fallbacks`.

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

## S4 (SMELL) - the SSE parser's `data` accumulation is uncapped

**Where:** `crates/jmap/src/event_source/parser.rs:87, 134` (the
`MAX_EVENT_SIZE` guards) vs `:108-113` (the `data` join).

The 1 MiB guard bounds a single `field`/`value` pair. `self.result.data`
accumulates across every `data:` line of one event with no bound at all,
so a stream of 1 MiB-minus-epsilon `data:` lines with no blank line grows
the buffer without limit. Also, the guard errors without clearing the
offending field, so the parser cannot resynchronise (see B7). Both pinned
in `event_source/parser.rs::tests`.

---

## S5 (SMELL) - the SSE parser's `id` field concatenates instead of replacing

**Where:** `crates/jmap/src/event_source/parser.rs:104-106`.

`self.result.id.extend_from_slice(&self.value)`. The SSE spec says the
`id` field *sets* the last-event-id buffer, so a second `id:` line in one
event replaces the first. Here `"id: 1\nid: 2\n\n"` yields id `"12"` -
which is then what `Last-Event-ID` resumption would send back. Pinned by
`event_source/parser.rs::tests::repeated_id_fields_concatenate_instead_of_replacing`.

Two smaller deviations in the same state machine: `Init` silently ignores
a leading space (SSE treats it as the first character of a field name),
and a field name is capped but a comment line is not.

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
  flattened header map, and B5.
- `email_query_wire` - every `Email/query` filter condition's wire name,
  the `header` two-element form, UTCDate formatting, comparator
  flattening (incl. `hasKeyword`'s extra field), `collapseThreads`,
  filter-operator nesting.
- `email_set_patch_shapes` - dotted-path null/true semantics, the
  clearing rule in both directions (a path setter drops the wholesale
  property; a wholesale setter drops both the children and an exact raw
  entry, so a `null_property("keywords")` cannot survive alongside
  `keywords(...)` as a duplicate key), the raw/null escape hatches.
- `mailbox_wire` - role wire names + case folding, B6, the
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
- `wire_enums_without_a_catch_all` - G2.
- `data_type_wire` - Display/Serialize agreement for every `DataType`,
  `MDN` casing, and the `Other` serialisation hazard.
- `session_capability_fallbacks` - G3 plus the `Other` passthrough.
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
`mod tests`): S5, B7's parser half, S4's two halves.

---

## Not done, and why

- **A transport-level test double.** The highest-value thing in scope and
  blocked on G1 (no `[dev-dependencies]`, so no async test can compile in
  this crate). This is the one item I would put at the top of the
  follow-up list: `Client::with_transport` + a stub `HttpTransport` would
  pin the request envelope (`using` construction and de-duplication,
  `methodCalls` tuple encoding, `accountId` injection via
  `JmapMethod::set_account_id`, `CallHandle` -> `Response::get` matching,
  method-error routing, `send_methods` tuple extraction) and none of that
  has a single test today.
- **`blob/download.rs` URL construction.** The templating + percent-encoding
  logic (S6) is inside an `async fn` that immediately calls the transport,
  so it is untestable without either G1 or extracting a pure
  `build_download_url(&[URLPart], &BlobRef) -> String`. That extraction is
  a refactor, and the brief splits tests from fixes, so I left it. It is a
  five-line change and would make S6 verifiable.
- **`client_ws.rs` frame handling beyond the subprotocol check.** The
  `WebSocketMessage_` decode is partly covered by the existing
  `deserializes_single_type_state_change_frame`; the close/error/binary
  arms are inside the `async_stream::stream!` and need G1.
- **`principal/availability.rs`, `principal/query.rs`,
  `share_notification/query.rs`, `sieve/query.rs`,
  `calendar_event_notification/query.rs`, `quota/query.rs`.** Read for
  bugs (none found beyond G2's `NotificationType`), not covered by new
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
