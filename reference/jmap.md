# bifrost-jmap reference

Current architecture of the JMAP implementation crate. The only external surface is `bifrost_jmap::sync` (account factory + config types). The protocol client, typed wire objects, transport, method macros, error type, and helper facade are crate-internal.

## Public surface

With the `sync` feature enabled, consumers may use:

- `sync::JmapAccountFactory` - registered as `Arc<dyn AccountFactory>`.
- `sync::JmapAccountFactoryBuilder` - open-time factory builder.
- `sync::JmapCredentials` - Basic or bearer credentials. `bearer(token)`
  / `bearer_source(Arc<dyn TokenSource>)`; the source threads through
  `account_token_source()` into bifrost-net, read live per request.
  `Clone`, hand-written `Debug`, no `PartialEq`/`Eq`.
- `sync::ReconnectPolicy` - WebSocket reconnect backoff config.

Everything else in `crates/jmap/src/` is `pub(crate)` or narrower. Consumers reach JMAP by constructing the factory, opening an `Arc<dyn Account>`, and calling `bifrost-types::Account` methods.

## Internal method dispatch

Every JMAP method is a self-describing struct implementing `JmapMethod`:

```rust
pub(crate) trait JmapMethod: Serialize + Send {
    const NAME: &'static str;       // "Email/get"
    type Cap: Capability;           // capability::Mail
    type Response: DeserializeOwned; // GetResponse<Email<Get>>
}
```

Adding a new method: define a struct, use `define_get_method!` / `define_set_method!` etc. Zero central files touched.

`Request` snapshots the session's `maxCallsInRequest` when it is constructed and
refuses the first call that would exceed it, before serializing or appending
that method. The guard covers tuple batches, explicit result-reference flows,
and WebSocket requests through the same `call` door, so no call site can
accidentally put an oversized method batch on the wire.

The snapshot is a three-state `CallLimit`, not a number: `Unadvertised` (no
`urn:ietf:params:jmap:core` block, or a block that omits `maxCallsInRequest`),
`Invalid` (the block advertises `maxCallsInRequest: 0`, which RFC 8620 forbids,
or the block is present and does not parse at all), and `Advertised`, which is
the only state that enforces.

Both halves of that widening are deliberate. Every core limit is an
`Option<usize>`, so an OMITTED field reads as "advertised nothing" rather than
as the zero a blanket `#[serde(default)]` used to fill in - two different server
bugs with two different classifications. And a present-but-unparseable core
block (`"maxCallsInRequest": "16"`) takes the `Capabilities::Malformed`
variant, read through `Session::core_capability_state()` (`Absent` /
`Malformed` / `Present`); falling back to `Capabilities::Other` made it
indistinguishable from absent at every reader, so a server sending a
string-typed limit was classified `SyncState(CapabilityChanged)` and the engine
reopened forever against a session that will never change. `core_capabilities()`
still collapses `Absent` and `Malformed` into `None`, which is correct for any
reader that only declines to enforce a bound; every reader that CLASSIFIES a bad
session must use the three-state. Treating the first two as a limit of zero would make
every request unable to hold a single call - and any caller probing such a
session would then see `Request(Malformed)` /
`ClientBug`, preempting the classifications the engine actually needs:
`SyncState(CapabilityChanged)` / `RestartAccount` for an absent core capability,
`Protocol(ContractViolation)` for a zero-valued one. Capability validation is
the gate for both bad sessions; the request builder enforces only a limit the
server actually advertised.

The method-generation macros are crate-private. They are re-exported
inside the crate root only so sibling modules can keep the existing
`crate::define_*` call sites without exporting macro names to consumers.

## Internal request/response flow

```rust
let mut request = client.build();
let handle = request.call(EmailGet::new(&account_id))?;  // typed CallHandle<EmailGet>
let mut response = request.send().await?;
let result = response.get(&handle)?;  // compile-time safe extraction
```

`CallHandle<M>` validates call_id and method name. A successful response with
the wrong echoed method name is rejected and maps to
`Protocol(ContractViolation)` at the account boundary. `Response::get()`
returns `Error::Method` for JMAP method-level errors. RFC 8620 s3.2 lets one call produce several responses under the same call id; `get` consumes them in the order the server sent them, so repeated reads on one handle walk the sequence and unrelated handles are unaffected. (It removes in place rather than swapping the tail into the vacated slot, which reordered every later lookup. No currently wired method produces multiple responses, but this is the generic RFC 8620 envelope.)

## Internal transport abstraction

`Client<T: HttpTransport = ReqwestTransport>` is generic over transport.

- `HttpTransport` - api_request, upload, download, get_session (returns `Bytes`).
- `SseTransport` - open_sse (EventSource, with `last_event_id` support).
  `Client::event_source()` and `event_source/` (WHATWG-conformant SSE parser
  plus stream driver, hermetically pinned) are supported public API even
  though the `sync/` Account impl does not call them: eventSourceUrl is a
  mandatory RFC 8620 session property, unlike the RFC 8887 WebSocket
  extension `sync/` push relies on. Wiring EventSource in as the sync-layer
  push fallback for servers without WebSocket push is tracked in
  `reference/jmap/DEFERRED.md`. The long-lived SSE response explicitly suppresses
  the ordinary JMAP request deadline while retaining response-header and body
  inactivity bounds.
- `ReqwestTransport` - default implementation with a pooled reqwest::Client.
- `Client::with_transport(transport, session, session_url)` - crate-internal custom transport injection. The session URL is required and rejected when empty: a client built without one could never re-fetch its session, so `refresh_session` was a silent no-op against the wrong (empty) URL.
- WebSocket remains reqwest-specific (documented).

The session and everything derived from it (`apiUrl`, the upload / download /
EventSource templates parsed into `URLPart`s, and the default account id) live
together in one `Arc<SessionState>` behind a single lock. RFC 8620 §2 lets any
Session property change, so `refresh_session` republishes the whole derived set
atomically; readers take one `session_state()` snapshot and build a whole URL
from it, so a concurrent refresh cannot splice two sessions into one request.
At the sync boundary, a method response whose `sessionState` differs marks the
client stale and advances a watch generation. The always-driven scope lifecycle
stream selects on that generation and terminates promptly with
`SyncState(CapabilityChanged)`, causing the engine to reopen
the whole account. An in-place refresh is insufficient there because the
existing `JmapAccount` has already frozen limits, capability flags, primary and
foreign account routing, and push topology from the old session. The watch wakes
the lifecycle poll pause immediately, so stale derived URLs are not used for
another poll interval after divergence.
`refresh_session` exists but no sync path calls it - the flag latches false for
the life of a client, and a fresh client (with a freshly fetched session) is
what clears it.

Values substituted into the session's URI templates are percent-encoded per RFC
6570 §3.2.2 simple string expansion - everything outside ALPHA / DIGIT / `-` /
`.` / `_` / `~` (`core::session::encode_template_value`). A deny list is not
sufficient here: `+` in a query-position `{type}` would otherwise decode
server-side as a space, turning `application/ld+json` into `application/ld json`.

All convenience helpers are `impl<Tr: HttpTransport> Client<Tr>` so custom transports get the full API.
`Client::build()` uses the lexicographically first primary capability as its
stable generic fallback; account-scoped callers use `Account::build()` and
therefore select their required capability explicitly. A session advertising no `primaryAccounts` has no such fallback, so the derived default account id is empty; every method this crate defines carries an `accountId`, and `Request::call` refuses on the empty one with `Error::NoPrimaryAccount { capability }` naming the method's own capability. The fault is local and knowable before a byte moves, so it is reported locally rather than shipped as `"accountId": ""` and answered with an opaque server error.

A contact `update` that carries `address_book_id` moves the card, which is an add of the new membership plus a clear of the old - and the old book is knowable only from the current card. `contact_update` therefore reads the card first and FAILS the update, as a retryable `Protocol(PartialResponse)`, when that read materializes no card. Treating the missing read as "no old book" would silently degrade the move into an add and leave the contact in both books; the unanswered-id lane is a transient, not evidence that the card has no book.

The contact projection follows RFC 9553's current object shape: postal fields
are `AddressComponent` entries under `Address.components`, and job titles are
separate `Title` objects linked to an `Organization` by `organizationId`.
The accepted component kinds are exactly the projected ones: the street-line
kinds and `locality` / `region` / `postcode` / `country`. Any other kind returns
`Unsupported`, including kinds RFC 9553 does define (`district`, `subdistrict`,
`separator`) but the shared contact address has no field for, so a recognized
component is never accepted and then dropped.

The two halves of the card projection fail differently, and a consumer sees
both. Postal addresses REJECT: an unplaceable component kind fails the whole
card as `Unsupported`, because a recognized kind with no shared field would
otherwise be accepted and then dropped out of a value whose parts only mean
anything together. Everything else - emails, phones, notes, media, the name -
SKIPS: an entry whose required string is missing or whose value is not an
object is discarded and the rest of the card is returned. So a `ContactCard`
may carry fewer emails or phones than the server holds, with no signal that
one was dropped; these are unordered multi-valued collections where one junk
entry says nothing about the others, and failing the contact over a single
malformed phone would cost its name and its addresses too. Surfacing skipped
values would need a per-value failure lane on the shared `ContactCard`.

RFC 9553 `pref` is a 1-100 RANKING in which the lower number is the more
preferred entry, not a boolean spelled `1`. The email / phone / address
projections therefore mark as primary the entry with the LOWEST rank
present, ties resolved by position, and none at all when no entry carries
one - an absent `pref` is least preferred, never promoted. Reading
`pref == 1` reported no primary for a card whose best address was ranked
`10`, and several primaries for a card that ranked two entries `1`. A value
outside 1-100 is not a ranking and reads as absent, so `pref: 0` cannot
beat every legal rank.

The sync request helpers likewise take `Account<Tr>` / `Client<Tr>` with
`Tr: HttpTransport`; the engine-facing `JmapAccount` remains the reqwest
production specialization because WebSocket push is reqwest-specific. Factory
tests use an armed scripted transport that records exact API JSON and derives
each reply's error shape from the same decision table `crates/net/src/request.rs`
walks, so a fixture cannot pin behavior against a response shape production
cannot produce: 2xx bodies reach the protocol decoder, a passed-through 3xx
carries status + headers AND its body (bifrost-net's redirect `PassThrough` arm
hands the body up through the same `into_byte_stream` the redirects-disabled arm
uses, rather than discarding it; a `Location`-less redirect carrying an
explanatory document therefore arrives intact and `ReqwestTransport` preserves
it on the resulting `TransportError`), a second 401 is `AuthLost`, statuses
retryable under `RetryPolicy::default()` (429 plus the 5xx family, read off the
policy rather than restated) come back as `RateLimited` / `RetryBudgetExhausted`
with their final response preserved, and only a genuinely non-retryable 4xx is
`Error::Status`. An exhausted script reports the request ordinal rather than
falling through to a network. Despite the test module's location,
`AccountFactory::open` itself is not hermetically reachable: its `connect()`
call hardwires `ReqwestTransport`. The seam covers the generic sync request
helpers beneath that boundary, not factory connection establishment.

## Module pattern

Every JMAP object type under `crates/jmap/src/<type>/`:

- `mod.rs` - struct with `<State = Get>` phantom, Property enum, method struct definitions via `define_*_method!` macros.
- `get.rs` - getters on `T<Get>`, GetObject impl.
- `set.rs` - builder methods on `T<Set>`, SetObject + SetObjectCreatable impls.
- `query.rs` - Filter/Comparator enums, QueryObject impl.
- `helpers.rs` - `impl<Tr: HttpTransport> Client<Tr>` convenience methods.

## Two data models

**Typed structs** (Mailbox, Calendar, AddressBook, etc.): serde derive, `Field<T>` for nullable properties.

**JSON map backing** (CalendarEvent, ContactCard): `serde_json::Map` via `json_object_struct!` macro. Property enum has `Other(String)`. Extension properties preserved on round-trip.

## Key types

- `Field<T>` - three-state nullable: `Omitted` / `Null` / `Value(T)`. Use instead of `Option<Option<T>>`.
- `Id<T>` - phantom-typed string ID: `AccountId`, `BlobId`, `State`. Available for incremental adoption.
- `Account<Tr>` - internal account-scoped view of `Client`. Use `account.build()` for scoped requests inside the crate.
- `Capability` trait - typed URIs with associated `Config` type.
- `TransportError` - crate-owned, `#[non_exhaustive]`, carries response body (`Bytes`) for ProblemDetails parsing.

## Capabilities

`Capabilities` enum in `session.rs` uses `deserialize_capabilities_map` to dispatch on URI key string. When adding a new capability:

1. Add struct in `session.rs`.
2. Add variant to `Capabilities` enum (with `#[cfg]` if feature-gated).
3. Add match arm in deserializer (via `try_cap!`, which supplies the malformed lane).
4. Add `Capability` impl in `capability.rs` with `type Config`.
5. Add the `session_cap_accessor!` invocation, which generates both readers.

`Session::typed_capability::<C>()` is a convenience bridge (serde round-trip). Hand-written accessors are zero-cost and primary.

### Absent, Malformed, Present - for every capability, not just core

`try_cap!` falls back to `Capabilities::Malformed(value)`, never to
`Capabilities::Other`, so a URI this crate models whose object it refuses to
parse is never reported as unadvertised. It first refuses any value that is not
a JSON OBJECT, which is not redundant with the typed parse: serde's derived
struct deserializers also accept a SEQUENCE in field-declaration order, so a
block sent as `[]` deserialized into any capability struct whose fields all
default and read as a fully advertised Present block (the calendars block sent
as `[]` was the concrete case), while `["16"]` fed the core block a
limit positionally. RFC 8620 §2 makes every capability value an object, and the
guard sits at the one door every modelled URI passes through, so the array lane
is closed for all of them at once rather than per struct. `Other` now means exactly one thing: a
URI the crate does not model. `session_cap_accessor!` generates the two readers
each capability needs from one mechanism - `x_capabilities() -> Option<&T>` for
callers that only decline to act, and `x_capability_state() ->
CapabilityState<'_, T>` (`Absent` / `Malformed` / `Present`) for callers that
have to classify. `CoreCapabilityState` is the core block's alias of that type.

The Unadvertised-versus-Invalid rule then decides per reader, and the split is
whether the account DEPENDS on the block:

- **Required.** `urn:ietf:params:jmap:core` and `urn:ietf:params:jmap:mail`.
  A malformed block is `Protocol(ContractViolation)` at
  `sync::capabilities::validate_session`, refused at open BEFORE any probe goes
  out, with the URI in the diagnostic.
  Not `SyncState(CapabilityChanged)`: nothing about this session changes on a
  reopen, so that lane buys an endless reopen loop.
- **Optional.** Everything else the crate models (websocket, submission, sieve,
  quota, blob, calendars, contacts, principals, principals:owner). The family
  degrades to off, and `build` warns per malformed URI via
  `Session::malformed_capabilities()`. Push has its own arm on top of that
  warning, because a malformed `urn:ietf:params:jmap:websocket` block deriving
  `PushCapability::None` silently is indistinguishable from a server that never
  offered push. It still degrades - there is no url to connect to - but the
  reason is on the record.
- **The family gate is where "degrades to off" is actually enforced.** A
  family's enable flag is NOT `primaryAccounts` membership alone.
  `sync::factory::resolve_optional_families` reads each family's block state
  and, on `Malformed`, drops the `primary_account` handle it just resolved,
  warning with the URI and the family name. Dropping the HANDLE rather than
  only clearing the `PimSupport` flag is the point: the derived flag is
  `handle.is_some()` and every family door on `JmapAccount` reads the same
  `Option`, so one decision moves both and a live handle can never sit behind
  a `false` capability. `Absent` is deliberately not gated here - a session
  naming a primary account for a URI it omits from `capabilities` is a
  different question from one that names it and then describes it wrongly, and
  answering it in this gate would drop families off servers that work today.
  `urn:ietf:params:jmap:vacationresponse` has no typed block and therefore no
  malformed lane: `primaryAccounts` is its whole signal.
- **At the door.** `Client::connect_ws` separates the two states as well:
  `Absent` stays `Error::WebSocketNotConnected` (which maps to `Unsupported`),
  while `Malformed` raises `Error::MalformedCapability { capability }`, mapped
  by `sync::error` to `Protocol(ContractViolation)` naming the URI. Filing a
  server contradicting its own advertisement under "feature not offered" hides
  it.

### `maxConcurrentUpload` is parsed and deliberately unread

`CoreCapabilities::max_concurrent_upload` has no reader, and that is the
correct state, not an oversight. Every upload door in this crate awaits one
upload at a time - `Account::upload` is a single request, and its callers
(`pim::attachment_upload`, the inline/attachment loops in the send path, the
sieve script body upload in `filters.rs`) iterate sequentially with an `await`
per item. There is no concurrent upload for the limit to govern, so a reader
would have to invent the fan-out first. The two places the crate does put
overlapping requests on the wire - the foreign-account probes at open and
the `filters_list` script-blob downloads - are governed by
`maxConcurrentRequests` instead, which `api_request_concurrency` already
honours (clamped to `[1, 8]`) for both.

The field stays parsed, and stays an `Option<usize>` like every other core
limit, so that adding concurrent uploads later is a reader change rather than a
parser change - and so an omitted limit never zero-fills into "advertised 0".
`core_max_concurrent_upload_is_parsed_even_though_nothing_reads_it` pins the
decode. Anything that grows a concurrent upload path must read it there; unlike
the four limits `build` validates, an absent or zero `maxConcurrentUpload` is
NOT a contract violation, because RFC 8620 §2 mandates the member but a client
that never uploads concurrently has nothing to refuse.

## Feature gates

Per-RFC features: `mail`, `calendars`, `contacts`, `blob`, `quota`. Each gates:

- Internal module declarations in `lib.rs`.
- DataType enum variants (with `#[serde(other)]` catch-all).
- Capabilities enum variants + session accessors + deserializer arms.
- PushObject/PushNotification variants.
- Test modules.

The Account layer under `crates/jmap/src/sync/` wires optional PIM capabilities at open. `contacts.rs` maps AddressBook/ContactCard methods onto the shared contact primitives (incl. JSContact postal addresses); `calendar_ops.rs` maps Calendar/CalendarEvent onto list/range/get/create/update/delete/RSVP/search. Create payloads stamp the mandatory top-level `@type` (`Card` / `Event`) and the `@type` on the nested RFC-defined objects (Name, EmailAddress, Phone, Organization, Title, Address, AddressComponent, Note; Participant, Location, RecurrenceRule, NDay). JSContact photo media is written with `kind: "photo"` (resource role, not a URI marker) so self-written photos read back; the read path filters on that kind.
Range queries send a server-side `AND(inCalendar, after, before)` filter and reapply the local overlap predicate after hydration. Recurrence maps common RRULE fields to JSCalendar `recurrenceRules` and back; simple RDATE/EXDATE map through `recurrenceOverrides`. Unsupported outbound RRULE parts are rejected before Set construction, and the payload builders reject a failed conversion again on their own - a rule that does not convert is an error, never a cleared or omitted recurrence - so the refusal does not depend on `validate_shared_recurrence` having run first. Modified overrides, multiple rules, excluded rules, and unrecognized inbound recurrence components return `Unsupported` rather than being silently discarded. A `recurrenceOverrides` entry projects only when it is a bare addition or a bare `excluded`; anything that patches the occurrence itself fails hydration instead of handing back the master event with the modified occurrence missing.
Calendar page walks (`events_in_range`, `search`) reconcile their submitted ids the way `contacts.rs::reconcile_cards` does. Every id the `CalendarEvent/get` answer covered in neither `list` nor `notFound`, and every returned event `event_from_jmap` refuses (a modified recurrence override, multiple or excluded recurrence rules, an unknown participant role or status), rides `Page::failed_ids`; the rest of the page still returns. One unrepresentable event therefore costs its own row, not the walk - before this it failed the whole call, permanently, since the event does not go away. The single-event `get` door is unchanged: with no neighbours to protect it still returns the conversion error itself. `search`'s calendar filter is client-side, so when `EventSearchRequest.calendar_id` is set the server's `total` (which counts the unfiltered result set) is suppressed rather than reported as this walk's total; the cursor stays live, and a page whose every hit belonged to another calendar is legitimately empty with more pages behind it.

`UNTIL` conversion and the start/duration write both need the event start timezone and all-day flag, which an `EventPatch` need not carry. `update` reads the current event (`CalendarEvent/get`, before hydration, so an event whose own recurrence is unrepresentable does not block the patch) for exactly the fields the patch leaves unset: a recurrence-only patch carrying `UNTIL`, or any time patch that does not restate `is_all_day`. Omitting `is_all_day` therefore means "unchanged", not "timed". RFC 5545 `UNTIL` is converted, not copied: basic DATE and DATE-TIME syntax becomes an extended JSCalendar `LocalDateTime`, a zoned event's UTC UNTIL is resolved into the event timezone, and the inbound direction restores DATE for all-day events, floating DATE-TIME for floating events, or UTC DATE-TIME for zoned events. The inclusive bound is unchanged in both directions.
Event time updates: JSCalendar derives end from `start` + `duration`, so a patch must carry both `start` and `end` (recomputing `duration`); a one-bound patch is rejected Unsupported rather than dropping the change or keeping a stale duration. All-day ends follow the exclusive `EventTime` contract: inbound end is `start + duration` days and outbound `duration` is `end - start` days (a single all-day event is start D / end D+1 / `P1D`), uniform with caldav/google/graph. JSCalendar has no DATE type, so an all-day start is written as midnight on its date (`2026-06-02T00:00:00`) with `showWithoutTime` carrying the all-day sense, and an inbound all-day time is truncated back to the shared bare date.
JSCalendar `alerts` project onto the read-only `CalendarEvent.reminders` surface (OffsetTrigger -> relative offset with `relativeTo`; AbsoluteTrigger -> absolute `when`; alert `action` carried through). Privacy maps JSCalendar `privacy` <-> visibility (`secret` -> `Confidential`); status maps `EventStatus` <-> `status`. Organizers map to/from JSCalendar owner participants (created events write the organizer as `owner`). `freeBusyStatus` standardizes `free`/`busy`, so shared Tentative/OutOfOffice serialize as busy.
An attendee patch is a whole-map `participants` write, and JSCalendar keeps the organizer in that same map, so `update` reads the current event whenever the patch carries attendees (`event_patch_needs_current`) and merges the owner-roled participants back in under their existing keys: an attendee whose email matches an owner entry updates its name and participation status in place (keeping its owner role - the read path surfaces an owner as a `Chair` attendee, so a read-modify-write round trip stays stable), and fresh attendee keys never collide with a kept one.
RSVP resolves authenticated email aliases from Basic credentials and RFC 9670 Principal/get when available, then patches the matching participant's `participationStatus` by dotted path; without a known email it falls back to the single non-owner attendee, and ambiguous events return unsupported. Shared attendees write `expectReply: true`; `ROLE` / `CUTYPE` information represented by the shared attendee role maps through JSCalendar roles. Unknown outbound shared roles or statuses and unknown enabled inbound JSCalendar roles or participation statuses return `Unsupported`, rather than acquiring a plausible default. If the server lacks the relevant capability, flags are false and calls return JMAP-stamped `Unsupported`.

## Error model

The crate-internal JMAP error type uses structured variants. No
`Error::Internal(String)`:

- `CallNotFound`, `IdNotFound`, `EmptyResponse`, `NotParsable`, `InvalidUrl`, `MalformedCapability`, `WebSocketClosed`, `WebSocketNotConnected`.
- `Transport(TransportError)` - wraps transport errors, auto-parses ProblemDetails from body.
- `Method(MethodError)` - JMAP method-level errors.
- No `From<reqwest::Error>` - reqwest errors converted to TransportError at point of use.

## PatchObject null semantics

RFC 8620: `null` removes map keys, not `false`. Email `patch` field uses `HashMap<String, serde_json::Value>` with `Value::Null` for removals.

## JMAP-specific code style

- `#[serde(skip_serializing_if = "...")]` on optional fields.
- `Field::is_omitted` for `skip_serializing_if` on `Field<T>` fields (with `#[serde(default)]`).
- `SetObjectCreatable::new()` initializes optional fields to `None`/`Omitted`, not empty collections.
- Helper impl blocks use `impl<Tr: HttpTransport> Client<Tr>` (not bare `impl Client`).

## Account layer

The `bifrost_types::Account` impl under `crates/jmap/src/sync/` (gated on `sync`) is engine-facing only: the sync tree depends on `bifrost-types`, not `bifrost-sync`. It wraps the JMAP client (`Client<ReqwestTransport>` + a `Mail`-capability `Account` handle) and maps each `Account` trait method onto one or more JMAP calls.

Cursors are protocol-tagged opaque bytes (hand-rolled length-prefixed format), scope-aware in establishment and change streams. Push uses the JMAP WebSocket subprotocol with a single reader task and broadcast fan-out.

### Module layout

```
crates/jmap/src/sync/
  mod.rs           - public keep-list re-exports (JmapAccountFactory,
                     JmapAccountFactoryBuilder, JmapCredentials, ReconnectPolicy)
  account.rs       - pub(crate) JmapAccount struct + impl Account
  factory.rs       - JmapAccountFactory + builder + JmapCredentials
  capabilities.rs  - AccountCapabilities builder, CoreLimits
  state.rs         - cursor envelope (V1 tag/length format)
  state_cache.rs   - per-accountId Email/Mailbox state maps
  foreign.rs       - foreign-account codec: mailbox (encode_foreign/parse_foreign/owner_tag)
                     and object ids (encode_object/parse_object/native_object)
  discover.rs      - cursor_scopes / memberships / scope_lifecycle_stream
  inventory.rs     - per-scope inventory streaming
  changes.rs       - per-scope change stream dispatch
  hydrate.rs       - get_stream projection logic
  push.rs          - WebSocket push, ReconnectPolicy, subscribe/unsubscribe
  mutation.rs      - bulk_set_flags / bulk_move / bulk_destroy pipeline
  pim.rs           - unified PIM primitives: mail mutations, send,
                     drafts, search, containers, settings, hydration
  filters.rs       - SieveScript-backed server-side filter scripts
  blob.rs          - open_blob / open_blob_range / open_raw_rfc822 (Email/get blobId + download,
                     correlated on the submitted id rather than the head of the echoed list)
  error.rs         - to_recovery / to_account_error mapping
```

### `JmapAccount` / `JmapAccountFactory` shape and lifecycle

`JmapAccountFactory` carries a `JmapAccountFactoryBuilder` config (URL, `JmapCredentials::Basic`/`Bearer`, optional timeout, `accept_invalid_certs`, `ReconnectPolicy`). `AccountFactory::open(account_id)` connects a `Client`, passing the engine account id into the `bifrost-net` attachment so metering / priority / caps / trace use the real key on reopen. Open resolves the primary `Mail` account plus the optional `Submission`/`VacationResponse`/`Quota`/`Sieve`/`Contacts`/`Calendars` handles through `resolve_optional_families` (which gates each on its own capability block's lane; see "Absent, Malformed, Present" above), reads the session, and then runs `validate_and_seed`: the session-only half of the capability validation (`capabilities::validate_session` - the core block's lane and its mandatory limits, plus the required mail block's lane) FIRST, and only then the initial `Email/get` and `Mailbox/get` (including names) probes, batched into one request for the primary and one request per foreign account. The order is load-bearing, not incidental: validation used to run only inside `capabilities::build`, after the whole seeding stage, so an open destined to be refused as a contract violation first spent two primary probes plus one per share against a server already known non-conformant and discarded every answer. The half of the validation that genuinely needs probe results - the `PimSupport` gates, which depend on which shares actually seeded - stays in `build`, which still calls `validate_session` itself so the refusal does not depend on the caller having run it. `build` then produces `AccountCapabilities` + `CoreLimits`. It spawns the WebSocket reader with a `CancellationToken`, and returns `OpenedAccount` (the handle plus the foreign-account skip lane; see Foreign accounts below).

The batch is conditional on the session's own numbers. `maxCallsInRequest` and `maxSizeRequest` are hard limits (RFC 8620 §2) the server enforces by rejecting the whole request with a request-level `limit` error, and the commonly quoted 16 calls is the minimum a server is *recommended* to support, never a floor a client may assume. So `batched_open_probes` sends the two-call batch only when the advertised call count allows it and `Request::send_methods_within` finds the encoded request inside `maxSizeRequest`; otherwise nothing is sent and the probes go out one request each. `send_methods_within` takes the size limit as a required argument (and returns `Ok(None)` without sending when the batch does not fit) precisely so a future batching call site cannot forget the question.

`JmapAccount` (`pub(crate)`) owns the `Client`, the primary `Mail` `Account` handle, a `foreign_mail: Arc<HashMap<String, MailAccount>>` of shared/delegate-account handles keyed by JMAP `accountId`, optional `Submission`/`VacationResponse`/`Quota`/`Sieve` handles, the built capabilities, per-scope cursor seed states, the `WsState`, a subscription registry, and per-`accountId` Email/Mailbox state caches (each `Arc<Mutex<HashMap<String, Option<String>>>>` via `state_cache.rs`). The maps are keyed by accountId because JMAP state is per-`(accountId, type)`; the primary account's id is one ordinary key. Scope lifecycle has a separate primary-only Mailbox state map: delta polling and local `Mailbox/set` may fast-forward the general cache, but neither consumes the lifecycle poller's window. This lifecycle position is open-time process state, not an engine change cursor, so it does not enter the cursor envelope or alter its schema/version. `set_priority`/`set_bandwidth_cap` delegate to `bifrost-net::AccountNet`. `Client` also carries an optional `ByteTally`; `Client::metered()` / `Account::metered()` hand back a handle over the SAME transport and session state that reports every response's request-local `bytes_in` into a fresh accumulator, which each sync stream takes at construction and each emitted batch `take`s. `Client::metered_into(&tally)` / `Account::metered_into(&tally)` report into an EXISTING accumulator, for a batch whose traffic runs through two differently scoped handles. The tally lives outside `ClientInner` deliberately: forking the inner would fork the session-state mutex, and two clients with independent session state is a correctness bug rather than an accounting detail. The count reaches the client through TWO doors, `HttpTransport::api_request_measured` and `HttpTransport::download_measured`, both DEFAULTED trait methods - the default reports the decoded body length, which is all a transport handing back only `Bytes` can honestly know, and `ReqwestTransport` overrides both with bifrost-net's request-local counter, the real inbound total across retries, redirects and 401 recovery. Defaulting them is why none of the crate's scripted doubles had to grow a byte counter. The download door is metered because blob octets are the BULK of the traffic a blob or raw-message read causes: `Client::download` records into the tally exactly as `send_request` does, so `sync::blob::open` reports the transport's own inbound count for the blob, and `open_raw_rfc822` shares one accumulator across both of its legs - the `Email/get` that resolves the whole-message `blobId` runs on a metered ACCOUNT handle and the download on a metered CLIENT handle, and its batch reports the sum. Uploads stay unmetered: the tally is inbound-only, and an upload's inbound leg is the small JSON acknowledgement, already counted when it rides the API door. One batch is several requests on the mutation path in particular: a state probe, the `Email/set`, a re-probe after `stateMismatch`, and the retried set. Unlike bifrost-graph and bifrost-google, this crate has no error-path accounting gap to close: every path that emits a batch after an error emits it after a METHOD-level JMAP error carried inside a successful HTTP exchange, whose bytes are already recorded. A transport-level error terminates the stream without emitting a batch, so no batch here can report a count that omits traffic it caused. See "Foreign (shared/delegate) accounts" below.

Reopen is engine-delegated: on drop or `close()`, the engine calls `JmapAccountFactory::open` again. `close()` cancels the shutdown token (terminating the WebSocket reader and in-flight streams), disables push, and then JOINS the spawned reader task, so its return really is evidence the reader stopped - `WsState` retains the `JoinHandle` for exactly that. Both steps are bounded by `ReconnectPolicy::connect_timeout`: a WebSocket sink write and a task join both park indefinitely on a half-open socket, and teardown blocking there would block the engine's reopen on a connection that is already gone. An unsubscribe that times out is not a close failure (the server drops the subscription with the connection); a reader that outlives the bound is abandoned rather than aborted, since aborting at an arbitrary await point is no safer. The `closed` flag short-circuits later calls, and the join handle is consumed by the first close. Cancellation safety relies on the shared `CancellationToken` plus `tokio::select!`; no `Account` method holds non-cancel-safe state across an await.

### Capabilities advertised

`capabilities::build` reads the core and websocket capability STATES (see "Absent, Malformed, Present" above - it refuses a malformed required block, warns per malformed optional one, and derives push from the websocket three-state) to construct `AccountCapabilities`:

- `cursor_freshness: ServerIssued`; `blob_range: No`; `blob_digest_pre_download: false`.
- `push: InProcess` when the session advertises `urn:ietf:params:jmap:websocket` with `supportsPush: true`, else `None`.
- `mutation.concurrency: StateBased` (every `Email/set` gated by `ifInState`); `mutation.replay_safety: None`.
- `batching_policy`: `max_items = core.maxObjectsInSet` clamped `[1,500]`, `max_wait: 100ms`, `flush_on_input_close: true`.
- `rate_limit_class: Generous`; `quota_signal: None` (quota via `quota_get`); `requires_uidvalidity_recheck: false`; `historyid_/delta_token_expires_after: None`; `reopen_discovers_foreign_namespaces: true` unconditionally, because shared-account scopes are discovered from the session at open, there is no foreign scope-lifecycle stream, and no session signal can prove a server will never grant a share (the accounts list is only the current grants; RFC 9670 principals support is sufficient but not necessary evidence of sharing).
- `pim_methods` advertises mailbox membership add/remove, keyword/read-state mutation, `set_importance`, attachment upload, draft lifecycle, search, mailbox CRUD, identity list/update (Submission), vacation get/set (VacationResponse), quota get (Quota), and thread/message hydration. `scheduled_send` is true iff the Submission block is PRESENT, parseable, and advertises `maxDelayedSend > 0` (onto `PimSupport.max_delayed_send` for boundary validation). A malformed Submission block does not degrade to `maxDelayedSend = 0`: `SubmissionCapabilities` is not `#[serde(default)]` at the container level, so a block omitting the mandatory RFC 8621 §7 `maxDelayedSend` fails to parse and lands in the malformed lane, where the family gate takes the whole submission family (send, drafts send, identities, `scheduled_send`) off with a named warning. A well-formed block advertising `maxDelayedSend: 0` is a different server and keeps the family, with only `scheduled_send` false. Gmail labels, Graph categories/extended properties are false.
- `filter_rule_shape: Scripts` and every filter flag true with a primary Sieve account; without Sieve they are false and the shape is `None`.
- `conveniences`: `starred = Keyword`, `replied`/`forwarded`/`mdn_sent` via keyword true, extended-property routes false. `set_starred`/`mark_replied`/`mark_forwarded`/`mark_mdn_sent` map to `$flagged`/`$answered`/`$forwarded`/`$MDNSent`. `set_importance` is two-valued: `High` sets `$important`, else clears; read maps `$important` presence onto `Message.importance`.

`CoreLimits` holds `maxObjectsInGet` and `maxObjectsInSet` - the only two limits the JMAP `Account` impl actually reads. `build` reads the three-state `core_capability_state()` and refuses on three grounds, in two different lanes: an ABSENT core block is `SyncState(CapabilityChanged)` -> `RestartAccount` (the server dropped a capability it used to advertise, so a reopen may fix it), while a PRESENT block that does not parse, and a present block whose `maxCallsInRequest` / `maxObjectsInGet` / `maxObjectsInSet` / `maxSizeRequest` is zero OR omitted, are both `Protocol(ContractViolation)`. Omitted counts because RFC 8620 §2 makes every one of those a mandatory member; classifying either as a capability change would buy a reopen loop against a session that will come back identical. `maxCallsInRequest` and `maxSizeRequest` are validated and discarded.

### Cursor envelope

`OpaqueChangeState` for JMAP is tagged with `ProtocolKind::Jmap` and `envelope_version = PAYLOAD_ENVELOPE_VERSION` (currently `2`). `OUTER_CURSOR_ENVELOPE_VERSION` is the matching `ChangeCursor.envelope_version` - the two consts sit on different axes, hence the `PAYLOAD_`/`OUTER_` prefixes. v2 is an OBJECT-ID encoding change, not a payload-shape change, exactly like graph's v2: v1 minted foreign thread ids bare, and a bare id still parses - as PRIMARY - so no additive field can detect one. A v1 cursor is therefore refused: `state::decode` distinguishes an OLDER envelope (`SchemaIncompatible` - reseed) from a FUTURE one (`CursorEnvelopeUnknown`), the refusal is pinned at the `changes_stream` door with its derived `Engine(SchemaIncompatible)` directive, and the engine's schema-clear deletes both the change cursors and every backfill checkpoint (completion marker included) so the next attach re-walks inventory and re-mints the ids - reseeding is the migration. The door classifies only genuine schema drift that way: a MIS-KEYED row (wrong protocol tag, or a payload scope disagreeing with the `ChangeCursor`) is a consumer/store bug, classified `SyncState(CursorInvalid)` -> `Engine(RestartScope(scope))` so the one bogus row is deleted and re-established without paying the account-wide clear and full re-hydration. Within the clearing session itself the re-established cursor covers changes from the open-time state onward only; the consumer's stored bare thread ids heal at that next-attach re-walk.

The payload is hand-rolled, length-prefixed bytes (little-endian `u32` lengths, single-byte tags):

```
state-tag:u8        // STATE_TAG_V1 = 1
scope-tag:u8        // 1=Email 2=Mailbox 3=Thread 4=Query 5=Folder
[query-id:length-prefixed-utf8 when Query]
[account-id + mailbox-id:length-prefixed-utf8 (each) when Folder]
state-string:length-prefixed-utf8
```

`JmapCursorState::V1 { scope: JmapScopeRepr, state_string }` is the only current variant. Its envelope can decode `Email`, `Mailbox`, legacy `Thread`/`Query(String)`, and `Folder { account_id, mailbox_id }` (a foreign account; round-trips through the `foreign.rs` codec). The SEEDED foreign shape is account-level - `encode_foreign_account(accountId)`, an empty `mailbox_id` (unambiguous: RFC 8620 ids are 1-255 chars) - one scope per share, because `Email/changes` state is per `(accountId, type)`. A legacy per-mailbox `Folder` cursor (non-empty `mailbox_id`) still decodes and still drives correctly if handed to `changes_stream`, but is no longer seeded or discovered. Discovery only exposes Email, Mailbox, and foreign Folder scopes; legacy Thread and Query cursors terminate unsupported rather than silently taking an unseeded or undefined path. `SCOPE_TAG_FOLDER = 5` was additive when it landed (tags 1-4 kept decoding, no bump); the later v2 bump was forced by the thread-id encoding change, not by this tag.

Validation rules in `state::decode`:

- Wrong `ProtocolKind` returns `Error::CursorProtocolMismatch`.
- Unknown `envelope_version` returns `Error::CursorEnvelopeUnknown`.
- Any decode failure (unknown state tag, unknown scope tag, truncated payload, trailing bytes, non-UTF-8 string) returns `Error::SchemaIncompatible`.
- `decode_cursor` additionally rejects a payload whose embedded scope does not match the `ChangeCursor.scope`, returning `Error::Other`.

`establish_initial_cursor(scope)` returns `CursorEstablishment::Ready` with a cursor built from the seed state captured at `open()` (including the account-level foreign `Folder` seeds). Unseeded scopes return `Error::Unsupported` - deliberately, rather than a cursor over an empty state, which would present as a working cursor that returns nothing forever. `describe_cursor` reports `Cheap`/`ServerCursor` only for decodable cursors on scopes the change stream drives (Email, Mailbox, foreign `Folder`); legacy Thread/Query cursors still decode but terminate unsupported, so they - like undecodable cursors - report `Expensive`/`None` rather than promising a strategy that dies on its first poll.

### Per-scope inventory, changes, hydration

Supported scopes for `inventory_stream` and `changes_stream`:

- `CursorScope::Type(ObjectType::Email)` - inventory walks `Email/query` (`receivedAt` desc) then `Email/get` with the fixed inventory property set. The first query starts at position zero; every later page uses the previous page's last id as `anchor` with `anchorOffset: 1`. The walk also pins `queryState` across its pages: if it moves, the walk ends via `terminated_walk_superseded` (`SyncState(CursorInvalid)` -> `RestartScope`), never with `Done`, so a changing result set is neither reported as complete coverage nor treated as permanently fatal. Every other error path likewise ends the stream with a `Terminated` and no `Done`; `Done(None)` is reachable only from an anchored query that came back empty under an unchanged `queryState`. `inventory_partitioning` reports `Full`: a positional `PageCount` plan would lose the anchor between engine partition calls and reopen the deletion-shift skip at every boundary. Explicit page partitions are refused rather than claiming coverage. Primary and foreign-account inventory share this loop, with foreign errors and ids routed through the owner-aware path.
- `CursorScope::Type(ObjectType::Mailbox)` - inventory is a single `Mailbox/get` (Id, Name, ParentId, Role, SortOrder, totals, unread counts, IsSubscribed). Changes use `Mailbox/changes`.
- `CursorScope::Type(ObjectType::Thread)` and `CursorScope::Query(_)` are not discovered. A legacy cursor for either terminates unsupported: thread inventory derives from email inventory, and the v1 trait has no registered query definition to supply an `Email/queryChanges` filter/sort.
- `CursorScope::Folder(FolderId(encode_foreign_account(account_id)))` - a foreign (shared/delegate) account, one account-level scope per share. Inventory paginates an UNFILTERED `Email/query` against the foreign account handle (one walk per share); changes use that account's `Email/changes`, which is account-wide and cannot be filtered by mailbox - which is exactly why the topology is one scope per account, never one per mailbox (a per-mailbox topology streamed the identical change set once per mailbox and fanned every foreign push out M ways). Per-mailbox membership is learned at hydration from the qualified `mailboxIds`, the same model the primary `Type(Email)` scope uses. A legacy per-mailbox `Folder` cursor still decodes and takes an `inMailbox`-filtered inventory walk, but is never seeded. See "Foreign (shared/delegate) accounts".

Every change-stream batch carries a `Checkpoint::Change(ChangeCursor)` whose state string is the post-call `newState`. The loop continues until `hasMoreChanges` is false, then emits `SyncEvent::Done`. A forward-progress guard terminates the loop as a `Protocol(ContractViolation)` when a server answers `hasMoreChanges: true` with `newState == sinceState` (RFC 8620 s5.2 requires `newState` to reflect the served changes) - the guard fires before that page's batch, so nothing is lost: the state never moved and the next drive replays from it. A second guard covers the case the first cannot see: a server alternating between two states moves the state at every single step, so each step looks like progress while the walk paginates forever. Each change walk therefore remembers the states it has served and terminates the same way when one comes back. Both guards are one mechanism - `changes::ChangeWalkGuard` / `WalkFault` - shared by `email_changes`, `mailbox_changes`, and the scope-lifecycle poller, which paginates `Mailbox/changes` on a loop of its own and needs the identical bound (see `scope_lifecycle_stream` below); the caller supplies only the method name for the diagnostic, so the two shapes cannot drift apart per call site. The inventory walk has the analogous guard on its own loop shape: it pages `Email/query` by anchor with `anchorOffset: 1`, so the previous page's last id can never appear again under an unmoved `queryState`; a server that re-serves it never yields the empty page that ends the walk, and the walk terminates as a `Protocol(ContractViolation)` WITHOUT a `Done` - as with the superseded-state exit, coverage was never established, so the engine restarts the scope rather than recording a walk that skipped. The per-`accountId` `state_cache` maps advance compare-and-swap style (keyed by the scope's accountId) so a stale writer does not clobber a newer state.

The CAS is exact: an expected state advances only an entry containing that state. It cannot initialize an absent or explicitly empty entry. Only an advance with no expected state may initialize those shapes.

`get_stream` (hydration) supports `Projection::FlagsOnly` and `Metadata` for Email; raw-MIME projections fatal-unsupported (whole-message raw is `open_raw_rfc822`: one `Email/get` for `blobId` then `client.download`). Batches size at `max_objects_in_get`. Hydrated emails emit `ItemOutcome::Succeeded`; locally-invalid ids or transport-drop ambiguity flow through `Failed` / `Uncertain` rather than terminating the stream. A foreign-qualified id's `Metadata` entry is qualified exactly as the foreign inventory mints it - memberships re-encoded `Folder(encode_foreign(accountId, mailboxId))` plus the `Mailbox(accountId)` owner tag, entry and blob ids in the object namespace. This is the attribution channel for foreign changes: the account-level change stream emits only qualified ids, and hydration is where the consumer learns which shared folder a message sits in.

Each `Email/get` answer is reconciled against the ids the batch submitted (`hydrate::reconcile_hydration`, pure and unit-pinned), so every submitted id leaves on exactly one lane. Repeated submitted ids remain repeated submissions: reconciliation drives from the submitted slice, and one correlated response object supplies one outcome for every occurrence. `notFound` alone cannot carry that: an absent `notFound` decodes as empty (see "`/get` response leniency"), and a present one can still omit an id the server also left out of `list`. Outcomes are keyed by the id the CALLER submitted - a response object whose id was not requested, was already answered, or is missing entirely is discarded rather than minted into an outcome. Ids named in `notFound` emit `Failed` with `NotFound(Message)`; ids answered in neither list emit `Failed` with `Protocol(PartialResponse)` + `Attempt(Acknowledged)`, which the shared recovery mapping retries rather than dropping (a terminal contract violation would lose the id for a condition the next `Email/get` usually clears). `contacts::get_cards` reconciles the same way, routing both classes into `Page::failed_ids` so the consumer preserves the row instead of reading absence as a deletion.

#### `/get` response leniency

`GetResponse::not_found` carries `#[serde(default)]`. RFC 8620 s5.1 makes `notFound` mandatory, but implementations omit it when empty often enough that rejecting the body would fail every sibling call in the same request over one absent empty array. Decoding it as empty is leniency, not proof that every requested id was answered - any caller with a closed per-item accounting contract must reconcile against its own submitted ids.

`SetResponse::new_state()` / `into_new_state()` return `Option`, not a fabricated `""`. RFC 8620 s5.3 makes `newState` mandatory, so absence means a non-conforming server, and folding it into an empty string made that indistinguishable from a state a server really did report as empty - a distinction the per-`accountId` state cache gives its own meaning ("probed, empty" versus "never probed"). Every caller now advances the cache only on a present, non-empty state; absent and empty are both "nothing to record", stated as such rather than inferred from a sentinel.

### The WebSocket request door

`send_ws` puts an RFC 8887 `Request` frame on the socket and returns the `requestId` it assigned. Three properties that door needs, and now has:

- The id counter is on the `Client`, not on the per-connection `WsStream`, so ids do not restart at 0 on reconnect. A per-connection counter meant a late response from the dropped connection carried an id the new connection was about to reuse - correlation that is actively worse than none.
- `WebSocketResponse` decodes `requestId` (RFC 8887 s4.3.4) and `WebSocketMessage::Response` carries it, so a reader can match a response frame to the request it answers. It was previously dropped at decode, which made two in-flight WebSocket requests indistinguishable on the read stream.
- `frame_stream` runs the session-divergence comparison on every response frame's `sessionState`, through the same `Client::note_session_state` the HTTP door uses - including on a frame whose method responses fail to decode, whose session state was still truthful. The WS door previously handed responses up without ever looking, losing staleness detection on the connection that stays open longest.
- A `Response` frame decodes in a single pass. The frame is not an HTTP response body - it carries `@type` and `requestId` beside the envelope fields - so `WebSocketResponse` keeps `methodResponses` as a raw JSON slice and hands it to `Response::from_frame_parts`, which runs the same per-call success/`MethodError` split (`split_call_results`) the HTTP envelope deserializer runs. It previously decoded the array into a `serde_json::Value` tree, rebuilt a `json!` envelope from it, and deserialized that a second time, re-allocating every method response on every frame. The frame dispatch is hand-written (`decode_frame` reads the `@type` discriminator by itself, then deserializes the variant from the same bytes) rather than `#[serde(tag = "@type")]`, because an internally-tagged enum buffers the frame into serde's private `Content` tree before selecting a variant and a `RawValue` cannot survive that buffer - it re-emerges as the newtype token. The same trap applies to every `#[serde(tag)]` / `#[serde(untagged)]` type in this crate: none can hold a `RawValue`, and the failure is a runtime decode error, not a compile error. An unknown or absent `@type` remains a parse error on the same arm as before. Session divergence is still checked on the frame's own `sessionState` before the method responses are touched, so a frame whose method responses fail to decode still reports its truthful session state and still fails its own waiter with `ResponseDecode`.

- A `maxSizeRequest` guard (RFC 8620 s2) on the encoded frame, matching the HTTP door's refusal to send an oversized batch. It is measured on the FRAME, envelope and `requestId` included, because over WebSocket the frame is the request. Only an advertised, non-zero limit is enforced, for the same reason `CallLimit` refuses to enforce its two unusable states - and since the limit is an `Option`, an omitted `maxSizeRequest` declines to enforce without being confused for an advertised zero, which the session validator refuses separately. Over-limit raises `Error::RequestSizeLimit { max, size }` -> `Request(Malformed)` / `ClientBug`; the HTTP door can answer `Ok(None)` against a caller-supplied bound instead because `send_methods_within` has that contract, and `send_ws` has none - it either writes the frame or it does not.

The await side is `PendingRequests`, a map of waiters keyed by `requestId`, held on the `Client` (not the `WsStream`) so it outlives any one connection. `Client::send_ws_awaiting` (and `Request::send_ws_awaiting`, the WebSocket counterpart of `Request::send`) registers a waiter and returns a `PendingResponse` that resolves to the correlated `Response`. `send_ws` is unchanged and still fire-and-forget.

Four properties hold that map together:

- **Registration happens under the WebSocket sink lock, before the frame is written.** A server that answers immediately cannot beat the registration, and a reconnect cannot slip between the two, because `connect_ws` installs the new sink and opens the new pending generation under that same lock.
- **A routed frame is not also yielded.** `frame_stream` hands a `Response` - or a `RequestError` naming a `requestId`, or a response frame whose method responses fail to decode - to the waiter for that id, and yields nothing. The outcome belongs to whoever asked for it; yielding a decode error onto the push stream instead would park that caller until the connection died. Everything else is yielded exactly as before, so the push reader is unaffected: all push traffic, pongs, unrouted responses, and - importantly - a `RequestError` with no `requestId`, which is how a push-enable frame's asynchronous rejection arrives and what the reconnect logic reads.
- **An unknown id wedges nothing.** `resolve` hands the outcome back when no waiter is registered (an id nobody awaits, or one whose caller was dropped between lookup and send), and the reader yields it as an ordinary response frame.
- **Teardown is generation-stamped.** A waiter is dropped -> its registration goes with it (`PendingResponse` has a `Drop` impl), so cancelled or timed-out calls cannot grow the map. A reconnect (`begin_connection`) bumps the generation and fails every waiter of the connection being replaced with `Error::WebSocketClosed` - `Protocol(PartialResponse)` -> `Retry(SameRequest)`, retryable rather than hanging. A read stream that reaches its end fails the waiters of ITS OWN generation only: an old stream drained to EOF *after* a reconnect must not reap a waiter belonging to the live connection, which is the same leak moved one layer over rather than closed.

### Push and reconnect

Push runs through one reader task spawned at `open()` when the session advertises WebSocket push. It connects via `Client::connect_ws`, validates `Sec-WebSocket-Protocol: jmap`, re-applies the subscribed `DataType` union together with the last observed RFC 8887 `pushState`, emits `Reconnected`, and forwards `PushObject::StateChange` as `Invalidated` (`PushSource::JmapStateChange` + `HintPayload::SpecificCursorScope`). A foreign `Folder` scope subscribes as `DataType::Email`, because JMAP subscriptions are data-type-wide across every visible account. On the emit side, `StateChange.changed` is keyed by `accountId` (RFC 8620 s7.1) and the reader routes each entry through a `PushRouting` snapshot built at `open` from the seeded scopes: a primary entry maps its `DataType` to the matching `Type(_)` scope (unmapped types degrade to `Unknown`); a seeded foreign account's `Email` entry emits exactly ONE `SpecificCursorScope` hint - its account-level `Folder` scope (the routing map holds one scope per foreign account by construction, so a foreign push is one change-stream pass, not a per-mailbox fanout; the engine's reconciler resolves a `SpecificCursorScope` hint by exact cursor lookup and skips scopes with no registered cursor, so the routing needs no membership-index support and a hint for a quarantined scope is a no-op); a foreign `Mailbox`/`Thread` entry is dropped (no cursor tracks foreign mailbox/thread state, and count-only bumps ride alongside the Email entry that caused them); an unseeded accountId is dropped (JMAP state is per-(accountId, type), so no registered cursor moved). Stream errors emit `Disconnected` and fall to the reconnect loop.

`ReconnectPolicy { initial: 1s, max: 60s, connect_timeout: 30s, keepalive: 120s }` controls exponential backoff. The backoff resets only when a pass actually READ a message off the connection, never merely for having reached the read loop: the push-enable frame's only failure signal is an asynchronous `RequestError` on that stream, so a server that will never accept the subscription (unknown `dataTypes` value, capability withdrawn, quota) otherwise completes handshake and sink write, gets `Reconnected` announced, rejects, and repeats at `initial` forever - an unbounded 1 Hz handshake storm with a `Disconnected`/`Reconnected` pair per second on the broadcast channel. A frame the peer actually served (a pong counts) is the evidence; an error is not. Every reader exit error is classified through `into_account_error(_, PushStream)`: terminal classes emit `Terminated(AccountError)` and stop the reader for engine reopen; retry classes emit `Disconnected` and continue backoff.

Every await in the reader's lifecycle is cancellation-covered, so `close()` is prompt from any point in it. The two setup awaits - the WebSocket handshake and the push re-enable frame that follows it - run through `bounded`, a `select!` over the shutdown token, a `connect_timeout` sleep, and the operation; the read loop and the backoff sleep select on the token directly. Neither setup await has a protocol-level deadline of its own, and both can park on a half-open socket forever: unbounded, a wedged handshake stalls push for the life of the session without ever reaching the backoff, and a wedged re-enable additionally holds the client's WebSocket sink lock and blocks every `push_subscribe` caller. Exceeding the bound is a transient disconnect (backoff not reset); cancellation stops the reader and drops the in-flight future so no detached task or client resource outlives the account. A re-enable that fails fast ends the pass as a transient disconnect too: the frame's only failure modes are a dead sink or a vanished connection (a server-side rejection arrives as a `RequestError` on the read stream), so reading on would announce `Reconnected` over a link with no applied subscription - silent dead push. `Reconnected` is therefore emitted only after a pass reaches a live connection whose subscription applied. The read loop carries a liveness deadline of its own: it is where the reader spends essentially all of its life, and JMAP defines no application-level keepalive, so on a half-open TCP connection (NAT or firewall silently dropping state, the common fate of a long-lived idle WebSocket) the stream never errors and never closes and the reader parks forever - push dead for the life of the account, indistinguishable to the engine from a quiet mailbox. After `keepalive` of total silence the reader sends a WebSocket ping; the answering pong must arrive within `connect_timeout` or the connection is dropped as a transient disconnect. Pongs are surfaced to the reader as `WebSocketMessage::Pong` rather than swallowed with the other control frames, because they are the only evidence a silent link is still carrying bytes. Any successful frame clears the outstanding probe. The reader talks to the client through the `PushTransport` seam (`connect_push` / `set_push_data_types` / `ping_push`), which exists so this behavior is pinned in-process - the rest of the sync layer still hardwires `ReqwestTransport`.

`push_stream` is a thin broadcast subscriber. A `Lagged` slot emits a coalesced `Invalidated { source: Coalesced, payload: Unknown }` so the engine full-repolls rather than losing notifications.

`subscribe` and `unsubscribe` build the union of all live `SubscriptionHandle` -> `DataTypeSet` mappings and call `Client::enable_push_ws` / `disable_push_ws`. Registry and enabled-union state commit only after the frame send succeeds. Subscribe outcomes account for every submitted position, including repeated scopes; unmapped scopes occupy the failed lane. When NO requested scope maps, the whole call errors (nothing was subscribed) as `Request(Malformed)`, not `Unsupported(PushSubscribe)`: the account advertised `PushCapability::InProcess`, so the fault is these arguments, and `Unsupported` would invite a consumer keying off the kind to downgrade push for the account wholesale. `WebSocketNotConnected` maps to `Error::Unsupported` to signal the engine that push is unavailable.

`scope_lifecycle_stream` polls `Mailbox/changes` against the primary mailbox state, hydrating changed names before advancing its state cache, then emits `ScopeLifecycle::Created`/`Renamed`/`Deleted`. A transient name-hydration failure leaves the state unchanged for replay; terminal/engine classes emit `Terminated`. Renames emit only when the stored and hydrated names differ and carry both names even when the stable scope id is unchanged.

This poller paginates `Mailbox/changes` on its own loop, so it carries the same `ChangeWalkGuard` the change walks do, for the same two unbounded shapes - and with more at stake: the engine drives this one stream for the life of the account, so an unguarded spin is a permanent hot poll loop rather than one walk that dies. The guard covers the pagination BURST, not the poller's whole life: it is dropped at every poll pause, because a state legitimately recurring across two polls five minutes apart is not a spin. On a fault the stream yields `ScopeLifecycleEvent::Terminated(Protocol(ContractViolation))` and ENDS - the honest report of a non-conformant provider, and the termination is final for this account's lifecycle channel (a change walk can be restarted per scope; this stream cannot) rather than burning a poll loop forever. As in the change walks, the guard fires before anything from that page is emitted and before its state is committed, so the refused page costs nothing.

The follow-up `Mailbox/get` batches at `maxObjectsInGet` on its own. It was previously bounded only incidentally, by the `maxChanges` the changes call happened to carry - which is fed from the same limit, but that is a fact about the call site, not a bound the read may assume. Its `notFound` and its silent omissions are ONE case and take one path: reconciliation drives from the submitted ids, so an id in either lane lands in the created-then-deleted rule below. RFC 8620 §5.1 gives both the same meaning here - the mailbox is not there any more - and a `notFound` id is by construction absent from `list`.

Two shapes decide which event a change is, and both are about what the CONSUMER has already been told, not about which change collection the server filed the id under:

- **An id the poller holds no name for is a discovery, not a rename.** `updated` is not evidence of a prior announcement: the poller's window opens at the state seeded at `open`, so a mailbox created before that state and renamed after it arrives as an `updated` id the names map has never seen. It used to emit `Renamed { old_name: "" }`, asserting a previous name that never existed and handing the consumer a scope to move rather than one to create. It now emits `Created`, which is both the honest event and the only one that establishes the scope. An id in `created` is `Created` regardless of the names map.
- **A create that vanished before it could be read is surfaced as `Created` then `Deleted`.** An id named in `created`/`updated` that the follow-up `Mailbox/get` answers in neither `list` nor at all was destroyed between the two calls. The state commit cannot be withheld for it - the mailbox is gone, so no later poll will ever mention that id again and there is nothing to replay - which is exactly why the create must be surfaced here or be lost to the lifecycle stream forever. Emitting the pair keeps the engine's per-folder cursor model sound: the scope is established and then torn down, which is what happened on the server, whereas a lone `Deleted` names a scope the consumer never had. Ids the same response already reports under `destroyed` are left to that loop, and an id the poller never knew and that was only `updated` produces nothing at all - it was never announced, so it has no lifecycle to close. The 300s poll pauses select on the shutdown token, so the stream ends promptly for a consumer still polling after `close()` (pull-based, so it cannot leak either way). Foreign-account mailbox lifecycle is not polled (see Foreign accounts).

### Mutation pipeline

`bulk_set_flags`, `bulk_move`, and `bulk_destroy` share a `mutation_stream` engine. Targets batch at `max_objects_in_set` clamped to `[1, 500]`; each batch is one `Email/set` gated by `ifInState(current_state)`. On `stateMismatch` the pipeline probes current state via `Email/get` (empty ids), updates the cache, and retries the batch once. Other errors abort with `SyncEvent::Terminated(AccountError)`.

An empty additive/subtractive flag operation is a caller error, not a no-op.
`FlagOp::validate_for_account` runs before any routing or batching and rejects
it - along with a wholly empty patch and a patch naming the same flag in both
sets - as a single `SyncEvent::Terminated(Request(Malformed))`. Nothing is
routed, no state is probed, and nothing reaches the wire. The former
`SkipFlags` `mutation_stream` kind, which answered such operations with
per-target `MutationSuccess::Skipped`, is gone with the contract that required
it.

Each bulk target is routed independently: a registered foreign-qualified
object id selects that account's `Mail` handle and its own Email-state cache,
then carries only its native id in the account-scoped `Email/set`. A batch
never mixes accounts. The returned outcome still uses the original qualified
id. An id for an account no longer registered deliberately remains literal on
the primary route so the server supplies the normal not-found result.

`bulk_move` additionally validates the destination against each target's
owner and fails the mismatched target on the item lane as
`Request(Malformed)` (`RecoveryClass::ClientBug`) before anything is sent,
the same call bifrost-graph makes for a cross-mailbox move. The destination
is a single `MembershipScope` for the whole call, so with mixed-owner targets
it can be right for some and wrong for others; owners are compared as
`Option<accountId>` (a bare id is the primary account, and no bare id is ever
minted for a share), independent of whether the named account is still
registered. This is not a politeness check: `Email/set` resolves the message
and the mailbox inside ONE `accountId`, so a bare primary mailbox id sent to
a shared account is not rejected by the server - it names whatever mailbox
that account holds under the same id, and the move reports `Applied` after
filing the message somewhere the caller never asked for.

Per-owner batching is bounded on both axes (`RouteBuffers`). Draining empties
a route in place rather than removing it, so the owner list holds one entry
per account rather than one per input target. A partial batch is flushed once
`batch_size` further targets have gone to other owners, so a quiet share's
short batch cannot be held to end-of-input by a busy one; the wait is capped
at the same input volume a full batch already accepts. A route holding only
locally-rejected targets emits them without probing Email state.

`IdempotencyKey` is a wire no-op, so replay safety stays `None` (read-back guard protects against double-apply). Per-id outcomes flow from `SetResponse::updated`/`destroyed`; an id the server names in neither the success nor error collection becomes `Protocol(PartialResponse)` with `Attempt(Acknowledged)`, so idempotent flag work retries and possibly-applied moves/destroys reconcile rather than being fabricated as `NotFound`. A `stateMismatch` surviving retry emits `Failed(ConcurrencyConflict)`; real per-id set errors map through `classify_set_item`. Empty additive/subtractive flag operations, wholly empty patches, and patches naming the same flag in both sets are rejected up front as `Terminated(Request(Malformed))` rather than transmitting empty patches. `bulk_move` accepts only `MembershipScope::Mailbox`.

### PIM primitives and conveniences

`pim.rs` is the Stage 1 unified mail surface. Message/thread mutation resolves `MutationTarget::Thread` via `Thread/get`, then `Email/set` patches against `mailboxIds`/`keywords`/`$seen`, guarded by the cached `Email` state with one `stateMismatch` retry. Gmail labels, Graph categories/extended properties return `Unsupported`. `Thread.emailIds` decodes as optional, because a `/get` may project it away; both readers (`thread_hydrate` and the mutation-target expansion) reject a response that omits an explicitly requested `emailIds` rather than treating it as an empty thread. That expansion failure is reported under the MUTATION's `AccountOperation`, not `HydrateThread` - the thread lookup is an implementation detail of the mutation, and the operation drives the recovery class the engine derives.

`patch_mailbox_membership` (`add_to_container` / `remove_from_container`, and
the two legs of `move_thread` / `delete_thread`) applies the same owner check
`bulk_move` does, for the same reason: the account layer routes by the
TARGET's owner but takes the container id as given, so a bare container id on
a foreign route resolves in the share's namespace. Owners disagreeing is
`Request(Malformed)` raised before `resolve_target` runs. A thread's owner
is decoded from its owner-qualified id, exactly like a message's (the
thread-routing section below); other target shapes are left to
`resolve_target`'s `Unsupported`.

`attachment_upload` stores bytes through the upload URL, returns an opaque blob handle. `draft_create`/`update`/`discard` use `Email/set` against Drafts. `send_message` creates the draft `Email` + `EmailSubmission` in one result-referenced request, then `onSuccessUpdateEmail` to Sent (or `onSuccessDestroyEmail` when `save_to_sent == Some(false)`). `draft_send` submits an existing draft and moves it to Sent, resolving Sent and Drafts from a single `Mailbox/get` via `role_mailboxes`. A `SendRequest::send_as` routes both sets to a successfully seeded foreign account that advertises Submission, resolves a concrete foreign `Identity/get` identity, and forces its `identityId`; `As` forces From to that identity and `OnBehalfOf` adds the authenticated user's Sender when known. Scheduled foreign sends are rejected because their bare submission handles cannot be safely routed through cancel/reschedule.

Dotted patch paths are JSON Pointers (RFC 8620 s5.3 / RFC 6901), so every interpolated token routes through `core::set::escape_json_pointer_token` (`~` -> `~0`, `/` -> `~1`) at every path-construction site: `keywords/{keyword}` (IMAP keywords legally contain `/`), `mailboxIds/{id}`, `shareWith/{principal}`, `addressBookIds/{id}`, `calendarIds/{id}`, and the RSVP participant path. New `format!`-built paths must use the same helper.

The `onSuccessUpdateEmail` payload (`EmailPatch::submitted_to_sent`) is built entirely from dotted-path patches, matching RFC 8621 s7.5's own submission example: `mailboxIds/{draftsId}: null`, `mailboxIds/{sentId}: true`, `keywords/$draft: null`. Assigning `mailboxIds` as a whole value would be a *replacement* and would silently drop any unrelated membership the submitted message also holds (a label-as-mailbox filing, a shared folder). When the caller cannot resolve a Drafts mailbox the drafts path is omitted: the message gains Sent and keeps whatever it had, which is the recoverable degradation.

Scheduled send rides RFC 8621/4865 FUTURERELEASE: when `SendRequest::scheduled` is `Some(t)`, the boundary validates `t` against `max_delayed_send`, forces an envelope, and stamps `holduntil` (RFC 3339) as a `mailFrom` parameter. A scheduled `send_message` returns the **EmailSubmission id** (undo-addressable). `cancel_scheduled_send` sets `undoStatus: canceled`; `reschedule_send` cancel-and-resubmits (no in-place reschedule) with a new `holduntil`.

Search maps the shared `SearchRequest` AST to `Email/query` (query text as a JMAP `text` filter). `search_messages` returns native email ids; `search` uses `collapseThreads = true`, hydrates the emails' `threadId`, returns thread ids. That projection accounts for every submitted email id (`reconcile_search_threads`): an id declared `notFound`, an id answered in neither list, and an email returned without the mandatory `threadId` all ride `Page::failed_ids` instead of being dropped, which used to leave the page silently short of its own result count. Thread ids are deduplicated, since `collapseThreads` is not a guarantee that the server collapsed them. Every known `SearchFilter` variant is translated recursively; the required non-exhaustive catch-all returns `Unsupported(Search)` so a future shared variant cannot silently become an empty JMAP `AND` that matches everything. Page cursors are opaque position bytes, so the search query pins `receivedAt` desc exactly as the inventory walks do: RFC 8621 gives an unsorted `Email/query` a server-defined order with no cross-call stability guarantee, and paging an unstable order by integer position duplicates and skips results. `route_search` picks the account before the query is built, from the owners of the filter's `In` containers (`search_owner` walks And/Or/Not): no `In` or a primary `In` is primary-only, one foreign owner routes both legs to that share and re-qualifies the result ids, two owners or an unreachable owner are refused. The `In` arm therefore strips the owner qualifier on the wire - the native part is a mailbox id in exactly the account the query addresses - and the cursor carries the owner so a resume cannot cross accounts. See "Foreign (shared/delegate) accounts".

Container CRUD is `Mailbox/get`/`set`. Mailboxes surface as `ContainerKind::Folder` (native mailbox id); `Mailbox.role` maps to `FolderRole` (`inbox`->INBOX, `sent`->SENT, `drafts`->DRAFT, `trash`->TRASH, `junk`->SPAM). `container_delete` leaves `onDestroyRemoveEmails = false`, so non-empty deletion fails rather than dropping messages.

Settings use `Identity/get`/`set`, `VacationResponse/get`/`set` (`singleton` id), and `Quota/get`. `identity_update` supports name/signatures/reply-to; default-identity selection is unsupported.

`thread_hydrate` does `Thread/get` then `Email/get` in order. `message_hydrate` selects headers / preview / full projections and returns blob handles without pre-downloading. `move_thread`/`delete_thread` add-to-target then remove-from-source; deleting from Trash destroys the emails. The thread is expanded ONCE, before either leg, and both legs run over that fixed id set (`patch_mailbox_membership_of`); the cross-account container check runs for both containers before the resolve. Each leg used to run its own `Thread/get`, so a message delivered to the thread between the two resolves was removed from the source having never been added to the target - it lost its source membership and gained nothing. The two-leg non-atomicity itself is the documented cross-provider shape and remains; the window is now one `Email/set` pair over a fixed set.

### Server-side filter scripts

`filters.rs` maps Stage 2 filter primitives onto JMAP Sieve:

- `filters_list` runs `SieveScript/query`, hydrates with `SieveScript/get`, downloads each script's blob, returns `ServerFilter::Script`. The query's id list is however many scripts the account holds, so hydration is BATCHED at the session's `maxObjectsInGet` (`factory::max_objects_in_get`, the reader for doors that hold a bare `Account` rather than the `CoreLimits` built at open) - one unbounded `/get` is a call a server may refuse wholesale with `requestTooLarge`. Each answer is then RECONCILED against the ids that batch submitted, the way `reconcile_hydration` and `contacts::get_cards` do. An id the server leaves out of `list` used to vanish from the returned `Vec`, which reads to the consumer as "that filter does not exist" - a deletion the response never claimed, and one an absent `notFound` cannot disprove (see the `/get` leniency rule). This door has no per-item lane, so the whole page fails, retryably: `error::get_id_unresolved_after_query` classifies both an unanswered id and a declared `notFound` as `Protocol(PartialResponse)` with `Attempt(Acknowledged)`, the same class `get_id_unanswered` uses and the same answer `contact_update` gives for a read that materializes nothing. `notFound` deliberately shares that lane rather than taking the terminal `get_id_not_found` one: at page level the honest reading of a script the query named and the get disclaimed is a delete that raced the two calls, and the retry's fresh `/query` will not name it again. The downloads are independent of each other, so they run CONCURRENTLY under `factory::api_request_concurrency` (the server's `maxConcurrentRequests`, clamped `[1, 8]` - the same reading the open-time foreign probes use, deliberately one function rather than a second bound that could drift). Serially this was N+2 round trips for N scripts. It is `buffered`, never `buffer_unordered`: yielding in submission order is what keeps the error accounting identical to the serial loop's, where the first failing script in list order is the error the whole call returns. Arrival order would report whichever download happened to fail first.
- `filter_create` uploads the body as `application/sieve`, creates a `SieveScript`, and `onSuccessActivateScript` when asked active.
- `filter_update` patches name/body via `SieveScript/set` (body uploads a fresh blob first); `is_active` toggles via `onSuccessActivate/DeactivateScript`.
- `filter_delete` destroys the id; `filter_validate` uploads then `SieveScript/validate`, mapping set errors to `FilterValidation` diagnostics.

Typed `ServerFilterCreate::Rule` / `ServerFilterPatch::Rule` return `Unsupported`.

### HTTP redirect handling

`ReqwestTransport` delegates redirects to `bifrost-net` (JMAP trusted-host allowlist, five-hop limit). `bifrost-net` strips `Authorization` on every cross-host hop, including injected Basic headers. The bearer path routes through bifrost-net; only Basic and the WebSocket handshake build a header via async `header_value()`.

### Error translation

`sync/error.rs::into_account_error(error, ctx)` is the single translation boundary: it consumes `crate::Error` plus a `JmapErrorContext { operation, scope, .. }` and emits an `AccountError` via `AccountErrorBuilder`, routing `(AccountErrorKind, Cause)` plus operation, scope, and any `AttemptCause` through `bifrost-types::recovery::derive` (central `RecoveryClass`; no private `to_recovery` table).

Mapping highlights for the JMAP signals the central table reads:

- `Method(stateMismatch)` -> `ConcurrencyConflict` +
  `State(ConcurrencyConflict)` -> `Retry::AfterStateRefresh`.
- `Method(cannotCalculateChanges)` -> `SyncState(CursorInvalid)` ->
  `Engine(RestartScope)` when scope known, else `Engine(RestartAccount)`.
- `Method(serverUnavailable | serverFail | serverPartialFail)` ->
  `Server(Unavailable)` -> `Retry::SameRequest`;
  `Method(requestTooLarge | tooManyChanges)` -> `SyncState(CursorInvalid)`
  or `Request(Malformed)` by context.
- `Method(forbidden)` -> `Authorization(PermissionDenied)` ->
  `NoPermission`. For a foreign (shared) `Folder` scope,
  `shared_scope_error` intercepts this into `jmap_scope_revoked` ->
  `SyncState(ScopeRevoked)` -> `Engine(DisableScope(scope))` (quarantine
  one scope, not the account); a primary scope stays terminal.
- `Problem(limit)` -> `Server(RateLimited)` w/ `throttle_scope`;
  `Problem(unknownCapability)` -> `SyncState(CapabilityChanged)` ->
  `Engine(RestartAccount)`; `Problem(notJSON | notRequest)` ->
  `Protocol(ContractViolation)`. Bare-`Problem` HTTP fallbacks
  (401/403/429/5xx) map per the central rules.
- Which net error shape carried the problem document decides where its
  evidence lives, and the crate reads all of them. `Error::Status` is the
  *only* variant bifrost-net produces for a terminal HTTP status, and a
  429 or 5xx never takes that path (the retry loop consumes it and hands
  back `RateLimited` / `RetryBudgetExhausted` with the final response
  preserved), so `TransportError::from_net` lifts the body back out of
  that evidence and `retry_after_from_net` reads the hint from the
  parsed field, the recorded history, *and* the response's own
  `Retry-After` header. `AuthLost` is the deliberate exception: its 401
  classification and transmission evidence stay bifrost-net's.
- `Transport(_)` -> `Transport(Network)` with `AttemptCause::transmission_state` from where the wire failed; central mapping picks `Retry::SameRequest` for idempotent ops, `Reconcile` for non-idempotent ops caught mid-flight.
- WebSocket errors split by handshake position: `WebSocketHandshake`
  (pre-handshake) -> `Transport(Network)` + `Attempt(Unsent)`;
  `WebSocketRuntime` (post-handshake) -> `Protocol(PartialResponse)` +
  `Attempt(Acknowledged)`. No blanket `From<tokio_websockets::Error>`;
  call sites map explicitly.
- `NoPrimaryAccount` -> `Authentication(ReauthorizationRequired)` ->
  `AuthLost`. Local shape errors (`Parse`, `Set`, `CallNotFound`,
  `IdNotFound`, `EmptyResponse`, `NotParsable`, `InvalidUrl`) ->
  `Request(Malformed)` -> `ClientBug`.

Cursor-decode failures from `cursor::envelope` (protocol mismatch, unknown envelope, malformed payload) build their own AccountError with `SyncState(SchemaIncompatible)`, routed to `Engine(SchemaIncompatible)`.

Known JMAP `SetErrorType` vocabulary lands on typed
`WireCause::Jmap(JmapMethod::*)` variants. `SetErrorType::Other(code)` is
the only path to `JmapMethod::Unknown { code }`, carrying the actual wire
code (no synthesized `"other"` literal; string-matching unknown
vocabulary is forbidden by the gate-5 invariant).

`sync/error.rs` exposes three stream terminators, each taking the caller's
`AccountOperation`: `terminated_unsupported` (`Unsupported(op)`);
`terminated_contract_violation` (`Protocol(ContractViolation)`, for
response shapes the library cannot encode - terminal, so it is the wrong
answer for anything a normal server does routinely, and it has no
production caller since positional paging went away); and
`terminated_walk_superseded` (`SyncState(CursorInvalid)` on the cursor
scope, mapping to `EngineDirective::RestartScope`) for a walk whose result
set moved underneath it. The last is what a mid-walk `Email/query`
`queryState` change raises: one delivered message advances `queryState`,
so a terminal class there would let ordinary mail delivery permanently
kill a scope's inventory, while a `Done` would claim coverage the walk
never achieved.

### Foreign (shared/delegate) accounts

JMAP auto-discovers shared/delegate accounts from the session: at `open`, `foreign_mail_account_ids` selects session accounts with `isPersonal: false` advertising `urn:ietf:params:jmap:mail`, excluding the primary; each becomes a scoped `Account::new(client, accountId)` handle in `foreign_mail`. The accounts are probed concurrently, then results are sorted by accountId before installation so open latency is bounded by the slowest independent share rather than their sum while topology and skip ordering stay deterministic. The concurrency is the server's own `maxConcurrentRequests` (RFC 8620 §2), clamped to `[1, 8]` by `api_request_concurrency`, and a session with no readable core capability probes serially. Exceeding that limit earns a request-level `limit` error, which the probe path would classify as a failure and turn a healthy share into a skipped scope. `api_request_concurrency` is the crate's single answer to how wide any fan-out may go; `filters_list` reads the same function for its blob downloads, so a second bound cannot drift from the session. `seed_foreign_account` runs the primary's two probes (`Email`/`Mailbox` state), and `apply_foreign_seed` inserts the per-accountId `state_cache` entries and seeds exactly ONE `CursorScope::Folder(encode_foreign_account(accountId))` - the account-level scope, seeded from the account's Email state - into `seed_states`. One scope per account, never one per mailbox: `Email/changes` is account-wide, so per-mailbox cursors would each replay the identical change set (the original B9 defect) and a foreign push would fan out to every one of them. A successfully seeded foreign account that also advertises Submission is included in `foreign_submission`; only that same routing set enables `pim_methods.send_as`, so a revoked share never produces an advertised but unreachable send path. A probe failure - revoked grant (terminal `NoPermission`) and exhausted transient retry (retryable) alike - skips that foreign account and records a `SkippedScope` naming its accountId, with the classified error, on `OpenedAccount::skipped_scopes` (`seed_foreign_account_or_skip`). Open itself neither fails (initial attach does not retry `factory.open`, so failing would block the user's own primary mail on a delegate outage) nor skips silently (the original G8 defect); the recovery class on the skip tells the consumer whether a reopen can heal it. Cost is O(foreign accounts) round-trips at open. Since foreign account discovery happens only here and there is no foreign scope lifecycle stream, the capabilities flag advertises that reopening can discover a newly granted share.

Owner-email resolution for those accounts is a two-level RFC 9670 gate
(`owner_email_plans`): no session `urn:ietf:params:jmap:principals`
capability means no plans at all, and per account the
`...:principals:owner` capability supplies the principal id.
`Principal/get` is authoritative; the account name is only ever a
fallback, and a narrow one. `account_name_as_address` PARSES the name
rather than sniffing it for an `@`, accepting only a bare addr-spec with
a dotted domain - display-name forms like `Support <support@example.com>`
are rejected outright rather than having an address guessed out of them,
because a wrong owner is worse than no owner. `fetch_principal_email`
returns a three-way `PrincipalEmail` so a lookup that never completed is
distinguishable from one that completed with no address:
`owner_email_from_lookup` permits the name fallback only on `Absent`.
On `Unavailable` the owner email is left unset, so a transport blip or an
unimplemented `Principal/get` cannot overwrite real ownership with a
label.

The foreign accountId rides in the `Folder` scope's `FolderId` (`foreign.rs` codec, `\u{1f}` separator; the account-level scope leaves the mailbox part empty, which no real RFC 8620 id can produce); `Type(_)` scopes cannot carry an account (`Type(Email)` is identical across accounts and would collide in the engine index), so the variant-free `Folder` shape is used. `mail_for_scope` / `account_id_for_scope` / `owner_of_scope` route a `Folder` scope to its `foreign_mail` handle, state-map key, and `MailboxId(accountId)` owner tag; primary scopes route to `self.mail` and `None`. `cursor_scopes` appends the seeded account-level foreign `Folder` scopes; `discover::memberships` appends one `Mailbox(accountId)` owner tag per foreign account, and `inventory_stream` stamps that tag onto every foreign-scope item - and re-encodes each native mailbox membership as `Folder(encode_foreign(accountId, mailboxId))` - so a foreign account's native mailbox ids cannot be conflated with the primary's in the membership index. The request layer is already per-account (`Account<Tr>::build` stamps the accountId), so routing is "hand `mail_for_scope` instead of `self.mail`", not a `core/request.rs` change.

Foreign OBJECT ids are qualified with the same codec (`encode_object` /
`parse_object` / `native_object`, same `\u{1f}` separator). The foreign
inventory qualifies both `InventoryEntry::id` and its whole-message
`blob_id`; the foreign changes leg qualifies its `ObjectChange` ids. This is
what makes hydration and blob reads self-routing: `get_stream` and
`open_blob` receive only an id - no scope - so a bare native id would run
`Email/get` / `/download/{accountId}/{blobId}` against the PRIMARY account
and either 404 or resolve an unrelated same-id primary object.
`hydrate::route_for_id` buckets ids per routing target (one `Email/get` per
account, since the call is accountId-scoped), strips to the native id on the
wire, and re-qualifies on the way out so an outcome id is byte-identical to
the id the caller handed in. `blob::foreign_split` does the same selection
for `open_blob` and both legs of `open_raw_rfc822` (the `blobId` fetch AND
the download). An id naming an account this session cannot reach falls back
to the primary handle deliberately, and - like the mutation pipeline's
`wire_email_id` - stays LITERAL on that wire (`hydrate::wire_object_id`
strips the qualification only for the routed owner), so the miss surfaces
as a real not-found rather than either a fabricated local error or, on a
native-id collision, an unrelated primary object hydrated under the
foreign id.

`pim::message_hydrate` (the one-id door, and the one
`SyncEngine::message_hydrate` funnels into) routes on the SAME
`hydrate::route_for_id` decision. It previously did not, so a foreign-encoded
id reached the primary `Email/get` verbatim and came back as a `NotFound`
naming the encoded string. On the way out, `qualify_foreign_message_ids`
re-qualifies the hydrated `Message`: `id` and each attachment `BlobId` in the
OBJECT namespace (`encode_object`, what `open_blob` decodes), each
`ContainerId` in the FOLDER namespace (`encode_foreign`, what
`containers_list` and the cursor scopes key on). The two namespaces are not
interchangeable and the projection must not confuse them.

Thread ids ride the same object namespace (nc-7's fix): foreign inventory
and hydration qualify `InventoryEntry::thread_id` / `Message.thread_id`
through the one `qualify_foreign_ids` hook, so a foreign thread id carries
its owner everywhere the consumer sees it, and every thread-keyed door
decodes it - `thread_hydrate` selects the owning handle, sends the bare
native id, and re-qualifies each returned member's id/thread/containers/
attachments; `MutationTarget::Thread` fan-outs, `move_thread`, and
`delete_thread` route the same way, with `delete_thread` minting its
resolved Trash id in the thread's own namespace so the cross-account guard
and the already-in-Trash comparison see one account. The handle-selection
boundary itself (`route_object_id` / `route_mutation_target`) is
transport-generic and pinned over string handles, because `JmapAccount`
hardwires `ReqwestTransport` (xc-3) and the account door cannot be driven
scripted; the account methods are one-line delegations to keep that pin
meaningful.

Three more decisions are lifted out for the same reason (jmap-O2), each
pure given its inputs, each with its trait method reduced to a one-line
delegation: `cursor_scopes_from_seeds` (discovery order - seeded `Type`
scopes in a fixed order, then foreign `Folder` scopes SORTED, because
`seed_states` is a `HashMap` and an unsorted lane would hand the engine a
different scope order every run), `partitioning_for_scope` (every scope is one
`Full` walk, because Email pages by anchor and an engine partition
boundary would discard it), and `establishment_for_seed` (seeded ->
`Ready`, unseeded -> `Unsupported` carrying the cursor scope). Prefer this
pattern over threading the transport generic through the sync layer: it
costs one function and no API surface.

A thread id qualified for an account this session no longer
holds stays LITERAL on the primary route - same rule as message ids - so
the server reports the miss instead of a collision-prone local strip.

`containers_list` appends each foreign account's mailboxes with
`namespace = Shared`, `owner = MailboxId(accountId)`,
`native_id = encode_foreign(accountId, mailboxId)` (byte-identical to the
`MembershipScope::Folder` qualification inventory and hydration stamp on
that account's messages - the container/membership join key; the sync
scope itself is the coarser account-level form),
`owner_local_id` = the bare mailbox id,
and the parent re-encoded in the same namespace so a foreign child never
points at a same-id primary mailbox. A per-account `Mailbox/get` failure
degrades to a `SkippedScope` (naming the foreign accountId, carrying the
classified error) on the returned `ContainerList::skipped_scopes`, plus the
remaining containers: one unreachable share neither blanks the sidebar nor
vanishes from it silently. A primary enumeration failure still fails the
call.

A share whose `Mailbox/get` answers WITHOUT `myRights` takes the same lane
per mailbox. `Container::rights` gates submit and the read-only-share
distinction, and `None` there is indistinguishable from a protocol with no
rights notion, so the omission would otherwise project as unreported rather
than as a degradation. `fetch_foreign_containers` reads `my_rights()` before
projection (afterwards the absence is unrecoverable), still lists the
container - a share with unknown rights beats a share missing from the
sidebar - and pushes a `SkippedScope` naming that container's qualified id
with a `Protocol(MissingField)` / `Wire(MalformedResponse)` error.
`fetch_mailboxes` asks for `MyRights` by name and RFC 8621 §2 makes it
server-set, so an answer omitting it is a missing field, not an absent
feature. Primary mailboxes are deliberately not classified: rights on the
user's own mailbox are not consulted, so an omission there hides nothing.

Foreign object mutations route through the same registered-account decision as
hydration: bulk operations partition by owner, and single-message PIM
operations select the owning `Mail` handle before stripping the qualified id
for `Email/set`. Their state-cache key is that selected accountId. Foreign
mailbox lifecycle remains out of scope: `scope_lifecycle` polls only the
primary. Under the account-level scope this costs less than it used to: a
foreign mailbox created after `open` needs no new cursor (the account scope
already covers its mail, and its qualified membership appears at hydration);
only the container row itself waits for the next `containers_list` call, and
a whole new share still waits for reopen.

### Known limitations

- Live foreign-mailbox lifecycle is not wired. `get_stream`, `open_blob`,
  `open_raw_rfc822`, bulk mutation, and single-message PIM mutation route to
  the foreign account via the qualified object-id codec. Foreign submission is
  supported, but scheduled foreign submission is not.
- Raw-MIME projections unsupported; only `FlagsOnly` and `Metadata` work. Sync-layer push is WebSocket-subprotocol only; against a server without RFC 8887 the engine falls back to polling. The client-level EventSource API exists but is not wired in as a push fallback (deliberate; see `reference/jmap/DEFERRED.md`).
- `BlobRangeSupport::No`; `open_blob_range` fatals `Error::Unsupported` even when the handle advertises range support (no transport `Range` hook). Its one non-capability refusal, a `range.start` past the known blob size, is `Request(Malformed)` (-> `ClientBug`) instead: that is a caller argument fault, and reporting it as `Unsupported` would tell the engine the protocol has no ranged read at all.
- `MutationReplaySafety::None`; `IdempotencyKey` is a wire no-op (read-back guard is the only lost-update protection).
- `bulk_move` only `MembershipScope::Mailbox`; `inventory_partitioning` is `Full` for every scope, and any explicit
  `InventoryPartition` other than `Full` is refused as
  `Unsupported(SyncInventory)` without sending a request.
- Gmail labels, Graph categories/extended properties, and identity-default selection are unsupported. Attachment handles keep blob id + MIME but not uploaded filenames.
- Typed filter-rule CRUD is unsupported; JMAP exposes literal Sieve scripts instead.
- Mail search covers a share only when the caller names it. A
  `SearchFilter::In` whose `ContainerId` is the owner-qualified form
  `containers_list` mints routes both `Email/query` and the follow-up
  `Email/get` to that owner's handle with the native mailbox id, and the
  returned message and thread ids come back qualified in the object
  namespace. What stays unsupported is the implicit union: with no `In`, or
  with an `In` naming a primary container, a search is primary-only and never
  touches a share. Two `In` filters naming different owners are refused
  (`Request(Malformed)`), not unioned - one `Email/query` carries one
  `accountId` - and so is an `In` naming a share this session does not hold.
  That refusal is the one place foreign routing cannot use hydration's
  stay-literal fallback: the id is a filter operand, not the object identity
  the server can report a miss on, so the primary account would answer with
  an empty page instead of an error. The page cursor is qualified with the
  account it was minted against (the same object-id codec), and replaying it
  under a filter that routes elsewhere is refused rather than paging one
  account by another's offsets.
- The search page cursor payload is versioned: `2:<position>:<queryState>`,
  owner-qualified for a share. Two things ride in it because a bare position
  means nothing without either. The account, because the order is
  account-scoped (above); and the `queryState`, because the order is not
  stable across calls - `Email/query` recomputes it per call, so page 2 taken
  under a moved state slides the window and silently duplicates one hit while
  dropping another. A moved state is refused as `ConcurrencyConflict` ->
  `Retry(AfterStateRefresh)` ("repeat the search"), never `Done`-shaped
  silence: it is not the caller's bug (one delivered message moves the state)
  and it is not terminal. `SyncState(CursorInvalid)` is unavailable here on
  purpose - it requires an `ErrorScope::Cursor`, and a search page cursor is
  not an engine cursor scope. A v1 cursor (the bare integer position that
  carried no pin) is refused `SyncState(SchemaIncompatible)` rather than
  resumed, since resuming it is exactly the unpinned paging the bump exists
  to stop; the two shapes are unambiguous (a v1 payload is all digits).
- Whether a next page exists is decided by the response's echoed `position`
  plus `total`, not by the page having come back full. RFC 8620 permits a
  short non-final page, and reading fullness as "more remains" ended such a
  walk in `Done`-shaped silence with most of the hits unreported. The query
  always sets `calculateTotal`; page fullness survives only as the fallback
  for a server that omits `total` anyway.
