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
returns `Error::Method` for JMAP method-level errors.

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
  `plans/jmap/DEFERRED.md`.
- `ReqwestTransport` - default implementation with a pooled reqwest::Client.
- `Client::with_transport(transport, session, session_url)` - crate-internal custom transport injection. The session URL is required and rejected when empty: a client built without one could never re-fetch its session, so `refresh_session` was a silent no-op against the wrong (empty) URL.
- WebSocket remains reqwest-specific (documented).

The session and everything derived from it (`apiUrl`, the upload / download /
EventSource templates parsed into `URLPart`s, and the default account id) live
together in one `Arc<SessionState>` behind a single lock. RFC 8620 §2 lets any
Session property change, so `refresh_session` republishes the whole derived set
atomically; readers take one `session_state()` snapshot and build a whole URL
from it, so a concurrent refresh cannot splice two sessions into one request.

Values substituted into the session's URI templates are percent-encoded per RFC
6570 §3.2.2 simple string expansion - everything outside ALPHA / DIGIT / `-` /
`.` / `_` / `~` (`core::session::encode_template_value`). A deny list is not
sufficient here: `+` in a query-position `{type}` would otherwise decode
server-side as a space, turning `application/ld+json` into `application/ld json`.

All convenience helpers are `impl<Tr: HttpTransport> Client<Tr>` so custom transports get the full API.
`Client::build()` uses the lexicographically first primary capability as its
stable generic fallback; account-scoped callers use `Account::build()` and
therefore select their required capability explicitly.

The sync request helpers likewise take `Account<Tr>` / `Client<Tr>` with
`Tr: HttpTransport`; the engine-facing `JmapAccount` remains the reqwest
production specialization because WebSocket push is reqwest-specific. Factory
tests use an armed scripted transport that records exact API JSON and derives
each reply's error shape from the same decision table `crates/net/src/request.rs`
walks, so a fixture cannot pin behavior against a response shape production
cannot produce: 2xx bodies reach the protocol decoder, a passed-through 3xx
carries status + headers with a knowably EMPTY body (bifrost-net's redirect
`PassThrough` arm discards passthrough bodies, so the fixture drops any
scripted body too), a second 401 is `AuthLost`, statuses
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
3. Add match arm in deserializer.
4. Add `Capability` impl in `capability.rs` with `type Config`.
5. Add session accessor method.

`Session::typed_capability::<C>()` is a convenience bridge (serde round-trip). Hand-written accessors are zero-cost and primary.

## Feature gates

Per-RFC features: `mail`, `calendars`, `contacts`, `blob`, `quota`. Each gates:

- Internal module declarations in `lib.rs`.
- DataType enum variants (with `#[serde(other)]` catch-all).
- Capabilities enum variants + session accessors + deserializer arms.
- PushObject/PushNotification variants.
- Test modules.

The Account layer under `crates/jmap/src/sync/` wires optional PIM capabilities at open. `contacts.rs` maps AddressBook/ContactCard methods onto the shared contact primitives (incl. JSContact postal addresses); `calendar_ops.rs` maps Calendar/CalendarEvent onto list/range/get/create/update/delete/RSVP/search. Create payloads stamp the mandatory top-level `@type` (`Card` / `Event`) and the `@type` on the nested RFC-defined objects (Name, EmailAddress, Phone, Organization, Address, Note; Participant, Location, RecurrenceRule, NDay). JSContact photo media is written with `kind: "photo"` (resource role, not a URI marker) so self-written photos read back; the read path filters on that kind.
Range queries send a server-side `AND(inCalendar, after, before)` filter and reapply the local overlap predicate after hydration. Recurrence maps common RRULE fields to JSCalendar `recurrenceRules` and back; simple RDATE/EXDATE map through `recurrenceOverrides`. Unsupported outbound RRULE parts are rejected before Set construction; modified overrides and less common recurrence features stay outside the mapping. JSCalendar `until` is a LocalDateTime, so a UTC RFC 5545 `UNTIL=...Z` (needs the event timeZone to normalize) is rejected; floating and date-only UNTIL pass through.
Event time updates: JSCalendar derives end from `start` + `duration`, so a patch must carry both `start` and `end` (recomputing `duration`); a one-bound patch is rejected Unsupported rather than dropping the change or keeping a stale duration. All-day ends follow the exclusive `EventTime` contract: inbound end is `start + duration` days and outbound `duration` is `end - start` days (a single all-day event is start D / end D+1 / `P1D`), uniform with caldav/google/graph.
JSCalendar `alerts` project onto the read-only `CalendarEvent.reminders` surface (OffsetTrigger -> relative offset with `relativeTo`; AbsoluteTrigger -> absolute `when`; alert `action` carried through). Privacy maps JSCalendar `privacy` <-> visibility (`secret` -> `Confidential`); status maps `EventStatus` <-> `status`. Organizers map to/from JSCalendar owner participants (created events write the organizer as `owner`). `freeBusyStatus` standardizes `free`/`busy`, so shared Tentative/OutOfOffice serialize as busy.
RSVP resolves authenticated email aliases from Basic credentials and RFC 9670 Principal/get when available, then patches the matching participant's `participationStatus` by dotted path; without a known email it falls back to the single non-owner attendee, and ambiguous events return unsupported. If the server lacks the relevant capability, flags are false and calls return JMAP-stamped `Unsupported`.

## Error model

The crate-internal JMAP error type uses structured variants. No
`Error::Internal(String)`:

- `CallNotFound`, `IdNotFound`, `EmptyResponse`, `NotParsable`, `InvalidUrl`, `WebSocketClosed`, `WebSocketNotConnected`.
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
  blob.rs          - open_blob / open_blob_range / open_raw_rfc822 (Email/get blobId + download)
  error.rs         - to_recovery / to_account_error mapping
```

### `JmapAccount` / `JmapAccountFactory` shape and lifecycle

`JmapAccountFactory` carries a `JmapAccountFactoryBuilder` config (URL, `JmapCredentials::Basic`/`Bearer`, optional timeout, `accept_invalid_certs`, `ReconnectPolicy`). `AccountFactory::open(account_id)` connects a `Client`, passing the engine account id into the `bifrost-net` attachment so metering / priority / caps / trace use the real key on reopen. Open resolves the primary `Mail` account plus optional `Submission`/`VacationResponse`/`Quota`/`Sieve`, reads the session, builds `AccountCapabilities` + `CoreLimits`, then batches the initial `Email/get` and `Mailbox/get` (including names) probes into one request for the primary and one request per foreign account. It spawns the WebSocket reader with a `CancellationToken`, and returns `OpenedAccount` (the handle plus the foreign-account skip lane; see Foreign accounts below).

The batch is conditional on the session's own numbers. `maxCallsInRequest` and `maxSizeRequest` are hard limits (RFC 8620 §2) the server enforces by rejecting the whole request with a request-level `limit` error, and the commonly quoted 16 calls is the minimum a server is *recommended* to support, never a floor a client may assume. So `batched_open_probes` sends the two-call batch only when the advertised call count allows it and `Request::send_methods_within` finds the encoded request inside `maxSizeRequest`; otherwise nothing is sent and the probes go out one request each. `send_methods_within` takes the size limit as a required argument (and returns `Ok(None)` without sending when the batch does not fit) precisely so a future batching call site cannot forget the question.

`JmapAccount` (`pub(crate)`) owns the `Client`, the primary `Mail` `Account` handle, a `foreign_mail: Arc<HashMap<String, MailAccount>>` of shared/delegate-account handles keyed by JMAP `accountId`, optional `Submission`/`VacationResponse`/`Quota`/`Sieve` handles, the built capabilities, per-scope cursor seed states, the `WsState`, a subscription registry, and per-`accountId` Email/Mailbox state caches (each `Arc<Mutex<HashMap<String, Option<String>>>>` via `state_cache.rs`). The maps are keyed by accountId because JMAP state is per-`(accountId, type)`; the primary account's id is one ordinary key. `set_priority`/`set_bandwidth_cap` delegate to `bifrost-net::AccountNet`. See "Foreign (shared/delegate) accounts" below.

Reopen is engine-delegated: on drop or `close()`, the engine calls `JmapAccountFactory::open` again. `close()` cancels the shutdown token (terminating the WebSocket reader and in-flight streams) then awaits teardown; the `closed` flag short-circuits later calls. Cancellation safety relies on the shared `CancellationToken` plus `tokio::select!`; no `Account` method holds non-cancel-safe state across an await.

### Capabilities advertised

`capabilities::build` reads `session.core_capabilities()` and `session.websocket_capabilities()` to construct `AccountCapabilities`:

- `cursor_freshness: ServerIssued`; `blob_range: No`; `blob_digest_pre_download: false`.
- `push: InProcess` when the session advertises `urn:ietf:params:jmap:websocket` with `supportsPush: true`, else `None`.
- `mutation.concurrency: StateBased` (every `Email/set` gated by `ifInState`); `mutation.replay_safety: None`.
- `batching_policy`: `max_items = core.maxObjectsInSet` clamped `[1,500]`, `max_wait: 100ms`, `flush_on_input_close: true`.
- `rate_limit_class: Generous`; `quota_signal: None` (quota via `quota_get`); `requires_uidvalidity_recheck: false`; `historyid_/delta_token_expires_after: None`; `reopen_discovers_foreign_namespaces: true` unconditionally, because shared-account scopes are discovered from the session at open, there is no foreign scope-lifecycle stream, and no session signal can prove a server will never grant a share (the accounts list is only the current grants; RFC 9670 principals support is sufficient but not necessary evidence of sharing).
- `pim_methods` advertises mailbox membership add/remove, keyword/read-state mutation, `set_importance`, attachment upload, draft lifecycle, search, mailbox CRUD, identity list/update (Submission), vacation get/set (VacationResponse), quota get (Quota), and thread/message hydration. `scheduled_send` is true iff `Submission` advertises `maxDelayedSend > 0` (onto `PimSupport.max_delayed_send` for boundary validation). Gmail labels, Graph categories/extended properties are false.
- `filter_rule_shape: Scripts` and every filter flag true with a primary Sieve account; without Sieve they are false and the shape is `None`.
- `conveniences`: `starred = Keyword`, `replied`/`forwarded`/`mdn_sent` via keyword true, extended-property routes false. `set_starred`/`mark_replied`/`mark_forwarded`/`mark_mdn_sent` map to `$flagged`/`$answered`/`$forwarded`/`$MDNSent`. `set_importance` is two-valued: `High` sets `$important`, else clears; read maps `$important` presence onto `Message.importance`.

`CoreLimits` holds `maxObjectsInGet` and `maxObjectsInSet` - the only two limits the JMAP `Account` impl actually reads. `build` rejects a session whose advertised core limits (including `maxCallsInRequest` and `maxSizeRequest`) are zero, even though those latter two are validated and discarded.

### Cursor envelope

`OpaqueChangeState` for JMAP is tagged with `ProtocolKind::Jmap` and `envelope_version = ENVELOPE_VERSION_V2` (currently `2`). `CHANGE_CURSOR_ENVELOPE_VERSION` is the matching `ChangeCursor.envelope_version`. v2 is an OBJECT-ID encoding change, not a payload-shape change, exactly like graph's v2: v1 minted foreign thread ids bare, and a bare id still parses - as PRIMARY - so no additive field can detect one. A v1 cursor is therefore refused: `state::decode` distinguishes an OLDER envelope (`SchemaIncompatible` - reseed) from a FUTURE one (`CursorEnvelopeUnknown`), the refusal is pinned at the `changes_stream` door with its derived `Engine(SchemaIncompatible)` directive, and the engine's schema-clear deletes both the change cursors and every backfill checkpoint (completion marker included) so the next attach re-walks inventory and re-mints the ids - reseeding is the migration. Within the clearing session itself the re-established cursor covers changes from the open-time state onward only; the consumer's stored bare thread ids heal at that next-attach re-walk.

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

`establish_initial_cursor(scope)` returns `CursorEstablishment::Ready` with a cursor built from the seed state captured at `open()` (including the account-level foreign `Folder` seeds). Unseeded scopes return `Error::Unsupported`. `describe_cursor` reports `Cheap`/`ServerCursor` only for decodable cursors on scopes the change stream drives (Email, Mailbox, foreign `Folder`); legacy Thread/Query cursors still decode but terminate unsupported, so they - like undecodable cursors - report `Expensive`/`None` rather than promising a strategy that dies on its first poll.

### Per-scope inventory, changes, hydration

Supported scopes for `inventory_stream` and `changes_stream`:

- `CursorScope::Type(ObjectType::Email)` - inventory paginates `Email/query` (`receivedAt` desc) then `Email/get` with a fixed property set (Id, MailboxIds, ThreadId, BlobId, Size, Keywords, MessageId, References, InReplyTo, ReceivedAt). Changes use `Email/changes` emitting Created/Updated/Destroyed `ObjectChange`s. `inventory_partitioning` exposes `Page { from, to }` for Email only. Every inventory path advances its query position by ids consumed, rather than hydrated objects, and treats only an empty query page as end-of-inventory. This prevents a server query-page cap or a query-to-get deletion race from truncating a full or page-windowed backfill.
- `CursorScope::Type(ObjectType::Mailbox)` - inventory is a single `Mailbox/get` (Id, Name, ParentId, Role, SortOrder, totals, unread counts, IsSubscribed). Changes use `Mailbox/changes`.
- `CursorScope::Type(ObjectType::Thread)` and `CursorScope::Query(_)` are not discovered. A legacy cursor for either terminates unsupported: thread inventory derives from email inventory, and the v1 trait has no registered query definition to supply an `Email/queryChanges` filter/sort.
- `CursorScope::Folder(FolderId(encode_foreign_account(account_id)))` - a foreign (shared/delegate) account, one account-level scope per share. Inventory paginates an UNFILTERED `Email/query` against the foreign account handle (one walk per share); changes use that account's `Email/changes`, which is account-wide and cannot be filtered by mailbox - which is exactly why the topology is one scope per account, never one per mailbox (a per-mailbox topology streamed the identical change set once per mailbox and fanned every foreign push out M ways). Per-mailbox membership is learned at hydration from the qualified `mailboxIds`, the same model the primary `Type(Email)` scope uses. A legacy per-mailbox `Folder` cursor still decodes and takes an `inMailbox`-filtered inventory walk, but is never seeded. See "Foreign (shared/delegate) accounts".

Every change-stream batch carries a `Checkpoint::Change(ChangeCursor)` whose state string is the post-call `newState`. The loop continues until `hasMoreChanges` is false, then emits `SyncEvent::Done`. The per-`accountId` `state_cache` maps advance compare-and-swap style (keyed by the scope's accountId) so a stale writer does not clobber a newer state.

`get_stream` (hydration) supports `Projection::FlagsOnly` and `Metadata` for Email; raw-MIME projections fatal-unsupported (whole-message raw is `open_raw_rfc822`: one `Email/get` for `blobId` then `client.download`). Batches size at `max_objects_in_get`. Hydrated emails emit `ItemOutcome::Succeeded`; locally-invalid ids or transport-drop ambiguity flow through `Failed` / `Uncertain` rather than terminating the stream. A foreign-qualified id's `Metadata` entry is qualified exactly as the foreign inventory mints it - memberships re-encoded `Folder(encode_foreign(accountId, mailboxId))` plus the `Mailbox(accountId)` owner tag, entry and blob ids in the object namespace. This is the attribution channel for foreign changes: the account-level change stream emits only qualified ids, and hydration is where the consumer learns which shared folder a message sits in.

Each `Email/get` answer is reconciled against the ids the batch submitted (`hydrate::reconcile_hydration`, pure and unit-pinned), so every submitted id leaves on exactly one lane. `notFound` alone cannot carry that: an absent `notFound` decodes as empty (see "`/get` response leniency"), and a present one can still omit an id the server also left out of `list`. Outcomes are keyed by the id the CALLER submitted - a response object whose id was not requested, was already answered, or is missing entirely is discarded rather than minted into an outcome. Ids named in `notFound` emit `Failed` with `NotFound(Message)`; ids answered in neither list emit `Failed` with `Protocol(PartialResponse)` + `Attempt(Acknowledged)`, which the shared recovery mapping retries rather than dropping (a terminal contract violation would lose the id for a condition the next `Email/get` usually clears). `contacts::get_cards` reconciles the same way, routing both classes into `Page::failed_ids` so the consumer preserves the row instead of reading absence as a deletion.

#### `/get` response leniency

`GetResponse::not_found` carries `#[serde(default)]`. RFC 8620 s5.1 makes `notFound` mandatory, but implementations omit it when empty often enough that rejecting the body would fail every sibling call in the same request over one absent empty array. Decoding it as empty is leniency, not proof that every requested id was answered - any caller with a closed per-item accounting contract must reconcile against its own submitted ids.

### Push and reconnect

Push runs through one reader task spawned at `open()` when the session advertises WebSocket push. It connects via `Client::connect_ws`, validates `Sec-WebSocket-Protocol: jmap`, re-applies the subscribed `DataType` union, emits `Reconnected`, and forwards `PushObject::StateChange` as `Invalidated` (`PushSource::JmapStateChange` + `HintPayload::SpecificCursorScope`). A foreign `Folder` scope subscribes as `DataType::Email`, because JMAP subscriptions are data-type-wide across every visible account. On the emit side, `StateChange.changed` is keyed by `accountId` (RFC 8620 s7.1) and the reader routes each entry through a `PushRouting` snapshot built at `open` from the seeded scopes: a primary entry maps its `DataType` to the matching `Type(_)` scope (unmapped types degrade to `Unknown`); a seeded foreign account's `Email` entry emits exactly ONE `SpecificCursorScope` hint - its account-level `Folder` scope (the routing map holds one scope per foreign account by construction, so a foreign push is one change-stream pass, not a per-mailbox fanout; the engine's reconciler resolves a `SpecificCursorScope` hint by exact cursor lookup and skips scopes with no registered cursor, so the routing needs no membership-index support and a hint for a quarantined scope is a no-op); a foreign `Mailbox`/`Thread` entry is dropped (no cursor tracks foreign mailbox/thread state, and count-only bumps ride alongside the Email entry that caused them); an unseeded accountId is dropped (JMAP state is per-(accountId, type), so no registered cursor moved). Stream errors emit `Disconnected` and fall to the reconnect loop.

`ReconnectPolicy { initial: 1s, max: 60s, connect_timeout: 30s }` controls exponential backoff (reset only after a pass reaches a live, subscribed connection). Every reader exit error is classified through `into_account_error(_, PushStream)`: terminal classes emit `Terminated(AccountError)` and stop the reader for engine reopen; retry classes emit `Disconnected` and continue backoff.

Every await in the reader's lifecycle is cancellation-covered, so `close()` is prompt from any point in it. The two setup awaits - the WebSocket handshake and the push re-enable frame that follows it - run through `bounded`, a `select!` over the shutdown token, a `connect_timeout` sleep, and the operation; the read loop and the backoff sleep select on the token directly. Neither setup await has a protocol-level deadline of its own, and both can park on a half-open socket forever: unbounded, a wedged handshake stalls push for the life of the session without ever reaching the backoff, and a wedged re-enable additionally holds the client's WebSocket sink lock and blocks every `push_subscribe` caller. Exceeding the bound is a transient disconnect (backoff not reset); cancellation stops the reader and drops the in-flight future so no detached task or client resource outlives the account. A re-enable that fails fast ends the pass as a transient disconnect too: the frame's only failure modes are a dead sink or a vanished connection (a server-side rejection arrives as a `RequestError` on the read stream), so reading on would announce `Reconnected` over a link with no applied subscription - silent dead push. `Reconnected` is therefore emitted only after a pass reaches a live connection whose subscription applied, which is also the only point that resets the backoff. The reader talks to the client through the `PushTransport` seam (`connect_push` / `set_push_data_types`), which exists so this behavior is pinned in-process - the rest of the sync layer still hardwires `ReqwestTransport`.

`push_stream` is a thin broadcast subscriber. A `Lagged` slot emits a coalesced `Invalidated { source: Coalesced, payload: Unknown }` so the engine full-repolls rather than losing notifications.

`subscribe` and `unsubscribe` build the union of all live `SubscriptionHandle` -> `DataTypeSet` mappings and call `Client::enable_push_ws` / `disable_push_ws`. `WebSocketNotConnected` maps to `Error::Unsupported` to signal the engine that push is unavailable.

`scope_lifecycle_stream` polls `Mailbox/changes` against the primary mailbox state, hydrating changed names before advancing its state cache, then emits `ScopeLifecycle::Created`/`Renamed`/`Deleted`. A transient name-hydration failure leaves the state unchanged for replay; terminal/engine classes emit `Terminated`. Renames emit only when the stored and hydrated names differ. The 300s poll pauses select on the shutdown token, so the stream ends promptly for a consumer still polling after `close()` (pull-based, so it cannot leak either way). Foreign-account mailbox lifecycle is not polled (see Foreign accounts).

### Mutation pipeline

`bulk_set_flags`, `bulk_move`, and `bulk_destroy` share a `mutation_stream` engine. Targets batch at `max_objects_in_set` clamped to `[1, 500]`; each batch is one `Email/set` gated by `ifInState(current_state)`. On `stateMismatch` the pipeline probes current state via `Email/get` (empty ids), updates the cache, and retries the batch once. Other errors abort with `SyncEvent::Terminated(AccountError)`.

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

`IdempotencyKey` is a wire no-op, so replay safety stays `None` (read-back guard protects against double-apply). Per-id outcomes flow from `SetResponse::updated`/`destroyed`; an id the server names in neither the success nor error collection becomes `Protocol(PartialResponse)` with `Attempt(Acknowledged)`, so idempotent flag work retries and possibly-applied moves/destroys reconcile rather than being fabricated as `NotFound`. A `stateMismatch` surviving retry emits `Failed(ConcurrencyConflict)`; real per-id set errors map through `classify_set_item`. Empty additive/subtractive flag operations short-circuit locally with per-target `Skipped` outcomes rather than transmitting empty patches. `bulk_move` accepts only `MembershipScope::Mailbox`.

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

The `onSuccessUpdateEmail` payload (`EmailPatch::submitted_to_sent`) is built entirely from dotted-path patches, matching RFC 8621 s7.5's own submission example: `mailboxIds/{draftsId}: null`, `mailboxIds/{sentId}: true`, `keywords/$draft: null`. Assigning `mailboxIds` as a whole value would be a *replacement* and would silently drop any unrelated membership the submitted message also holds (a label-as-mailbox filing, a shared folder). When the caller cannot resolve a Drafts mailbox the drafts path is omitted: the message gains Sent and keeps whatever it had, which is the recoverable degradation.

Scheduled send rides RFC 8621/4865 FUTURERELEASE: when `SendRequest::scheduled` is `Some(t)`, the boundary validates `t` against `max_delayed_send`, forces an envelope, and stamps `holduntil` (RFC 3339) as a `mailFrom` parameter. A scheduled `send_message` returns the **EmailSubmission id** (undo-addressable). `cancel_scheduled_send` sets `undoStatus: canceled`; `reschedule_send` cancel-and-resubmits (no in-place reschedule) with a new `holduntil`.

Search maps the shared `SearchRequest` AST to `Email/query` (query text as a JMAP `text` filter). `search_messages` returns native email ids; `search` uses `collapseThreads = true`, hydrates the emails' `threadId`, returns thread ids. Page cursors are opaque position bytes.

Container CRUD is `Mailbox/get`/`set`. Mailboxes surface as `ContainerKind::Folder` (native mailbox id); `Mailbox.role` maps to `FolderRole` (`inbox`->INBOX, `sent`->SENT, `drafts`->DRAFT, `trash`->TRASH, `junk`->SPAM). `container_delete` leaves `onDestroyRemoveEmails = false`, so non-empty deletion fails rather than dropping messages.

Settings use `Identity/get`/`set`, `VacationResponse/get`/`set` (`singleton` id), and `Quota/get`. `identity_update` supports name/signatures/reply-to; default-identity selection is unsupported.

`thread_hydrate` does `Thread/get` then `Email/get` in order. `message_hydrate` selects headers / preview / full projections and returns blob handles without pre-downloading. `move_thread`/`delete_thread` add-to-target then remove-from-source; deleting from Trash destroys the emails.

### Server-side filter scripts

`filters.rs` maps Stage 2 filter primitives onto JMAP Sieve:

- `filters_list` runs `SieveScript/query`, hydrates with `SieveScript/get`, downloads each blob, returns `ServerFilter::Script`.
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

`sync/error.rs` exposes two stream terminators: `terminated_unsupported`
(`Unsupported(op)`) and `terminated_contract_violation`
(`Protocol(ContractViolation)`, for pagination overflows and
response-shape mismatches). Both take the caller's `AccountOperation`.

### Foreign (shared/delegate) accounts

JMAP auto-discovers shared/delegate accounts from the session: at `open`, `foreign_mail_account_ids` selects session accounts with `isPersonal: false` advertising `urn:ietf:params:jmap:mail`, excluding the primary; each becomes a scoped `Account::new(client, accountId)` handle in `foreign_mail`. `seed_foreign_account` runs the primary's two probes (`Email`/`Mailbox` state), and `apply_foreign_seed` inserts the per-accountId `state_cache` entries and seeds exactly ONE `CursorScope::Folder(encode_foreign_account(accountId))` - the account-level scope, seeded from the account's Email state - into `seed_states`. One scope per account, never one per mailbox: `Email/changes` is account-wide, so per-mailbox cursors would each replay the identical change set (the original B9 defect) and a foreign push would fan out to every one of them. A successfully seeded foreign account that also advertises Submission is included in `foreign_submission`; only that same routing set enables `pim_methods.send_as`, so a revoked share never produces an advertised but unreachable send path. A probe failure - revoked grant (terminal `NoPermission`) and exhausted transient retry (retryable) alike - skips that foreign account and records a `SkippedScope` naming its accountId, with the classified error, on `OpenedAccount::skipped_scopes` (`seed_foreign_account_or_skip`). Open itself neither fails (initial attach does not retry `factory.open`, so failing would block the user's own primary mail on a delegate outage) nor skips silently (the original G8 defect); the recovery class on the skip tells the consumer whether a reopen can heal it. Cost is O(foreign accounts) round-trips at open. Since foreign account discovery happens only here and there is no foreign scope lifecycle stream, the capabilities flag advertises that reopening can discover a newly granted share.

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
meaningful. A thread id qualified for an account this session no longer
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
- Raw-MIME projections unsupported; only `FlagsOnly` and `Metadata` work. Sync-layer push is WebSocket-subprotocol only; against a server without RFC 8887 the engine falls back to polling. The client-level EventSource API exists but is not wired in as a push fallback (deliberate; see `plans/jmap/DEFERRED.md`).
- `BlobRangeSupport::No`; `open_blob_range` fatals `Error::Unsupported` even when the handle advertises range support (no transport `Range` hook).
- `MutationReplaySafety::None`; `IdempotencyKey` is a wire no-op (read-back guard is the only lost-update protection).
- `bulk_move` only `MembershipScope::Mailbox`; `inventory_partitioning` only `Page { from, to }` for `Email`.
- Gmail labels, Graph categories/extended properties, and identity-default selection are unsupported. Attachment handles keep blob id + MIME but not uploaded filenames.
- Typed filter-rule CRUD is unsupported; JMAP exposes literal Sieve scripts instead.
