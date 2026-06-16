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

Everything else in `crates/jmap/src/` is `pub(crate)` or narrower: no public raw client APIs, method macros, wire model modules, or transport types. Consumers reach JMAP by constructing the factory, opening an `Arc<dyn Account>`, and calling `bifrost-types::Account` methods.

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

`CallHandle<M>` validates call_id and method name. `Response::get()` returns `Error::Method` for JMAP method-level errors.

## Internal transport abstraction

`Client<T: HttpTransport = ReqwestTransport>` is generic over transport.

- `HttpTransport` - api_request, upload, download, get_session (returns `Bytes`).
- `SseTransport` - open_sse (EventSource, with `last_event_id` support).
- `ReqwestTransport` - default implementation with a pooled reqwest::Client.
- `Client::with_transport(transport, session)` - crate-internal custom transport injection.
- WebSocket remains reqwest-specific (documented).

All convenience helpers are `impl<Tr: HttpTransport> Client<Tr>` so custom transports get the full API.

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

The Account layer under `crates/jmap/src/sync/` wires optional PIM capabilities at open time. `contacts.rs` maps JMAP AddressBook/ContactCard methods onto the shared contact primitives (incl. JSContact postal addresses). `calendar_ops.rs` maps JMAP Calendar/CalendarEvent methods onto the shared calendar primitives: list/range/get/create/update/delete/RSVP/search.
Create payloads stamp the mandatory top-level `@type` (`Card` / `Event`)
and the `@type` on the nested objects the RFCs define (Name,
EmailAddress, Phone, Organization, Address, Note; Participant,
Location, RecurrenceRule, NDay). JSContact photo media is written with
`kind: "photo"` (the resource role, not a URI marker) so self-written
photos read back; the read path filters on that kind.
Range queries send a server-side `AND(inCalendar, after, before)` filter and reapply the local overlap predicate after hydration as a guard.
Calendar recurrence maps common RRULE fields to JSCalendar
`recurrenceRules` objects and back. Simple shared RDATE/EXDATE values map
through JSCalendar `recurrenceOverrides`. Unsupported outbound RRULE
parts are rejected before Set payload construction; modified recurrence
overrides and less common JSCalendar recurrence features remain outside
the shared mapping. JSCalendar `until` is a LocalDateTime, so a UTC
RFC 5545 `UNTIL=...Z` (which would need the event timeZone to normalize)
is rejected with the other unsupported parts; floating and date-only
UNTIL pass through unchanged.
Event time updates: JSCalendar derives the end from `start` + `duration`,
so a patch must carry both `start` and `end` (the patch recomputes
`duration`). A patch with only one bound cannot be applied losslessly
without reading the current event and is rejected as Unsupported rather
than silently dropping the change or keeping a stale duration.
Privacy maps between JSCalendar `privacy` and shared visibility (`secret` -> `Confidential`); status maps `EventStatus` <-> JSCalendar `status` on read/create/update. Organizers map to/from JSCalendar owner participants; created events write the organizer as an `owner` participant. `freeBusyStatus` standardizes `free`/`busy`, so shared Tentative/OutOfOffice serialize as busy.
RSVP resolves authenticated email aliases from Basic credentials and RFC 9670 Principal/get when available, then patches the matching participant's `participationStatus` by dotted path. Without a known email it falls back to the single non-owner attendee; ambiguous events without an identity match return unsupported rather than guessing.
If the server lacks the relevant JMAP capability, the method flags are
false and calls return JMAP-stamped `Unsupported`.

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

`JmapAccountFactory` is the consumer-registered factory, carrying a `JmapAccountFactoryBuilder` config (URL, `JmapCredentials::Basic`/`Bearer`, optional timeout, `accept_invalid_certs`, `ReconnectPolicy`). `AccountFactory::open(account_id)` connects a `Client` and passes the engine account id into the `bifrost-net` attachment used by `ReqwestTransport`, so metering, priority, caps, and trace correlation use the real engine key on every reopen. Open resolves the primary `Mail` account plus optional `Submission`/`VacationResponse`/`Quota`/`Sieve`, reads the session, builds `AccountCapabilities` + `CoreLimits`, probes initial `Email`/`Mailbox`/`Thread` state to seed cursors, spawns the WebSocket reader with a `CancellationToken`, and returns `Arc<dyn Account>`.

`JmapAccount` (`pub(crate)`) owns the `Client`, the `Mail` `Account` handle, optional `Submission`/`VacationResponse`/`Quota`/`Sieve` handles, the built capabilities, per-scope cursor seed states, the `WsState`, a subscription registry, and shared `Mutex<Option<String>>` state caches for `email`/`mailbox`/`thread`. `set_priority`/`set_bandwidth_cap` delegate to `bifrost-net::AccountNet` rather than local atomics; the transport owns the canonical knobs.

Reopen is engine-delegated: on drop or `close()`, the engine calls `JmapAccountFactory::open` again. `close()` cancels the shutdown token (terminating the WebSocket reader and in-flight streams) then awaits teardown; the `closed` flag short-circuits later calls. Cancellation safety relies on the shared `CancellationToken` plus `tokio::select!` in the push stream; no `Account` method holds non-cancel-safe state across an await.

### Capabilities advertised

`capabilities::build` reads `session.core_capabilities()` and `session.websocket_capabilities()` to construct `AccountCapabilities`:

- `cursor_freshness: ServerIssued` - `state` strings are server-issued tokens; the engine persists them and resumes.
- `blob_range: BlobRangeSupport::No` - whole-blob downloads only; range support would need an HTTP `Range` request hook.
- `blob_digest_pre_download: false` - no content digest before download.
- `push: PushCapability::InProcess` when the session advertises `urn:ietf:params:jmap:websocket` with `supportsPush: true`, otherwise `PushCapability::None`.
- `mutation.concurrency: MutationConcurrency::StateBased` - the pipeline gates every `Email/set` with `ifInState`, so the server rejects on a state mismatch.
- `mutation.replay_safety: MutationReplaySafety::None` - no wire replay token; the read-back guard is the lost-update safety net.
- `batching_policy.max_items` - `core.maxObjectsInSet`, clamped to `[1, 500]`.
- `batching_policy.max_wait: 100ms`, `flush_on_input_close: true`.
- `rate_limit_class: RateLimitClass::Generous` - JMAP rate-limits per request size/session, not per second.
- `quota_signal: QuotaSignal::None` - quota is exposed via `quota_get` but feeds no retry/scheduler signal.
- `requires_uidvalidity_recheck: false`; `historyid_expires_after`/`delta_token_expires_after: None` (state strings not time-bound).
- `pim_methods` advertises real JMAP support for mailbox membership add/remove, keyword/read-state mutation, `set_importance`, attachment upload, draft lifecycle, search, mailbox CRUD, identity list/update (when `Submission` open), vacation get/set (when `VacationResponse` open), quota get (when `Quota` open), and thread/message hydration. `scheduled_send` is true iff the `Submission` session advertises `maxDelayedSend > 0` (threaded onto `PimSupport.max_delayed_send` and stored on the account for boundary validation). Gmail labels, Graph categories, and Graph extended properties are false (`Error::Unsupported`).
- `filter_rule_shape: Scripts` and every filter method flag is true
  when the session has a primary Sieve account. Without Sieve they
  are false and the shape is `None`.
- `conveniences` declares `starred = Keyword`, `replied_via_keyword`/`forwarded_via_keyword`/`mdn_sent_via_keyword` true, both extended-property routes false. So `set_starred`/`mark_replied`/`mark_forwarded`/`mark_mdn_sent` map to JMAP `$flagged`/`$answered`/`$forwarded`/`$MDNSent`. `set_importance` is two-valued: `High` sets `$important`, `Normal`/`Low` clear it (one keyword op); the read side maps `$important` presence onto `Message.importance` (`High` if set, else `Normal`).

`CoreLimits` holds `maxObjectsInGet` and `maxObjectsInSet` - the only two limits the JMAP `Account` impl actually reads. `build` rejects a session whose advertised core limits (including `maxCallsInRequest` and `maxSizeRequest`) are zero, even though those latter two are validated and discarded.

### Cursor envelope

`OpaqueChangeState` for JMAP is tagged with `ProtocolKind::Jmap` and `envelope_version = ENVELOPE_VERSION_V1` (currently `1`). `CHANGE_CURSOR_ENVELOPE_VERSION` is the matching `ChangeCursor.envelope_version`.

The payload is hand-rolled, length-prefixed bytes (little-endian `u32` lengths, single-byte tags):

```
state-tag:u8        // STATE_TAG_V1 = 1
scope-tag:u8        // 1=Email 2=Mailbox 3=Thread 4=Query
[query-id:length-prefixed-utf8 when Query]
state-string:length-prefixed-utf8
```

`JmapCursorState::V1 { scope: JmapScopeRepr, state_string }` is the only current variant. `JmapScopeRepr` mirrors the four supported scope shapes: `Email`, `Mailbox`, `Thread`, and `Query(String)`.

Validation rules in `state::decode`:

- Wrong `ProtocolKind` returns `Error::CursorProtocolMismatch`.
- Unknown `envelope_version` returns `Error::CursorEnvelopeUnknown`.
- Any decode failure (unknown state tag, unknown scope tag, truncated payload, trailing bytes, non-UTF-8 string) returns `Error::SchemaIncompatible`.
- `decode_cursor` additionally rejects a payload whose embedded scope does not match the `ChangeCursor.scope`, returning `Error::Other`.

`establish_initial_cursor(scope)` returns `CursorEstablishment::Ready` with a freshly encoded cursor built from the cached seed state captured at factory `open()` time. Scopes that are not seeded (anything outside the supported four) return `Error::Unsupported`. `describe_cursor` reports `CostClass::Cheap` and `SyncStrategy::ServerCursor` for valid cursors and `Expensive` / `None` otherwise.

### Per-scope inventory, changes, hydration

Supported scopes for `inventory_stream` and `changes_stream`:

- `CursorScope::Type(ObjectType::Email)` - inventory paginates via `Email/query` (sorted `receivedAt` desc) then hydrates with `Email/get` using a fixed property set (Id, MailboxIds, ThreadId, BlobId, Size, Keywords, MessageId, References, InReplyTo, ReceivedAt). Changes use `Email/changes` against the cached state and emit Created/Updated/Destroyed `ObjectChange`s. `inventory_partitioning` exposes a `Page { from, to }` partition for Email only.
- `CursorScope::Type(ObjectType::Mailbox)` - inventory is a single `Mailbox/get` (Id, Name, ParentId, Role, SortOrder, totals, unread counts, IsSubscribed). Changes use `Mailbox/changes`.
- `CursorScope::Type(ObjectType::Thread)` - changes use `Thread/changes`; inventory fatals (thread inventory derives from email inventory).
- `CursorScope::Query(_)` - changes use `Email/queryChanges` and surface `ScopeChange` events; inventory fatals (registered query definitions are out of scope for the v1 trait).

Every successful change-stream batch carries a `Checkpoint::Change(ChangeCursor)` whose state string is the post-call `newState`. The change loop continues until `hasMoreChanges` is false, then emits `SyncEvent::Done(Some(Checkpoint::Change(...)))`. Shared `Mutex<Option<String>>` state caches are advanced compare-and-swap style so a stale writer does not clobber a newer state.

`get_stream` (hydration) supports `Projection::FlagsOnly` and `Projection::Metadata` for Email. Raw-MIME projections emit a fatal-unsupported event - MIME assembly is outside this wave. (The dedicated whole-message raw read is `open_raw_rfc822` in `blob.rs`: one `Email/get` for `blobId` then `client.download`; it is independent of this hydration fatal.) Batches are sized at `max_objects_in_get`. Per-item lane: every hydrated email is emitted as `ItemOutcome::Succeeded(BatchSuccess { item, output: HydratedObject })` so the return type matches the unified `AccountStream<SyncEvent<ItemOutcome<HydratedObject>>>` trait signature. Locally-invalid input ids or transport-drop ambiguity flow through `ItemOutcome::Failed` / `Uncertain` on the same channel rather than terminating the entire stream.

### Push and reconnect

Push runs through a single reader task spawned at factory `open()` when the session advertises WebSocket push. It connects via `Client::connect_ws` (`dep:tokio-websockets/native-tls`), validates `Sec-WebSocket-Protocol: jmap`, re-applies the union of subscribed `DataType`s, emits `WatchEvent::Reconnected`, and forwards `PushObject::StateChange` as `WatchEvent::Invalidated` with `PushSource::JmapStateChange` + `HintPayload::SpecificCursorScope(...)`. Per-message and stream-level errors emit `WatchEvent::Disconnected` and fall through to the reconnect loop.

`ReconnectPolicy { initial: 1s, max: 60s }` controls exponential backoff (reset on success, saturating-double on failure). Every reader exit error (connect failure or mid-stream drop) is classified through `into_account_error(_, JmapErrorContext::new(PushStream))`. Terminal-class errors (auth lost, capability changed, schema break) emit `WatchEvent::Terminated(AccountError)` and stop the reader so the engine reopens; retry-class errors emit `WatchEvent::Disconnected` and continue the backoff loop.

`push_stream` is a thin broadcast subscriber. A `Lagged` broadcast slot emits a coalesced `WatchEvent::Invalidated { source: PushSource::Coalesced, payload: Unknown }` so the engine triggers a full re-poll rather than silently losing notifications.

`subscribe` and `unsubscribe` build the union of all live `SubscriptionHandle` -> `DataTypeSet` mappings and call `Client::enable_push_ws` / `disable_push_ws`. `WebSocketNotConnected` maps to `Error::Unsupported` to signal the engine that push is unavailable.

`scope_lifecycle_stream` polls `Mailbox/changes` against the cached mailbox state and emits `ScopeLifecycle::Created` / `Renamed { old, new }` / `Deleted` for membership scope churn. Poll errors are classified through `into_account_error`; terminal/engine-action classes break the polling loop so the engine reopens. The element type is `ScopeLifecycle` (not `SyncEvent<_>`), so there is no typed `Terminated` on this channel - ending the stream is the protocol-side signal.

### Mutation pipeline

`bulk_set_flags`, `bulk_move`, and `bulk_destroy` share a `mutation_stream` engine. Targets batch at `max_objects_in_set` clamped to `[1, 500]`; each batch is one `Email/set` gated by `ifInState(current_state)`. On `stateMismatch` the pipeline probes current state via `Email/get` (empty ids), updates the cache, and retries the batch once. Other errors abort with `SyncEvent::Terminated(AccountError)`.

`IdempotencyKey` is accepted on the API surface but unused at the wire level (JMAP has no idempotency token), so replay safety stays `MutationReplaySafety::None`; the engine's read-back guard protects against double-apply.

Per-id outcomes flow from `SetResponse::updated` / `destroyed`. A `stateMismatch` surviving the retry emits `ItemOutcome::Failed` with `AccountErrorKind::ConcurrencyConflict`; other errors map through `into_account_error` into `ItemOutcome::Failed(BatchFailure { error, .. })`.

`bulk_move` only accepts `MembershipScope::Mailbox`; other membership shapes emit a fatal-unsupported event before any wire write.

### PIM primitives and conveniences

`pim.rs` implements the Stage 1 unified mail surface on the same JMAP client. Message/thread mutation resolves `MutationTarget::Thread` through `Thread/get`, then issues `Email/set` patches against `mailboxIds`, `keywords`, or `$seen`, guarded by the cached `Email` state with one `stateMismatch` retry. Gmail labels, Graph categories, and Graph extended properties return `Error::Unsupported` (flags false).

Composition uses JMAP's native object model. `attachment_upload` stores bytes through the account upload URL and returns an opaque blob handle. `draft_create`/`draft_update`/`draft_discard` use `Email/set` against Drafts. `send_message` creates the draft `Email` and `EmailSubmission` in one request via a result reference, then moves it to Sent (`onSuccessUpdateEmail`) or destroys it (`onSuccessDestroyEmail`, when `save_to_sent == Some(false)`). `draft_send` submits an existing draft and moves it to Sent on success.

Scheduled send rides RFC 8621/4865 FUTURERELEASE on the submission envelope: when `SendRequest::scheduled` is `Some(t)` the boundary validates `t` against `max_delayed_send` (`bifrost_types::validate_scheduled`), forces an envelope, and stamps `holduntil` (RFC 3339) as a `mailFrom` address parameter. A scheduled `send_message` returns the **EmailSubmission id** (the undo-addressable handle) instead of the email id an immediate send returns. `cancel_scheduled_send` is an `EmailSubmission/set` update setting `undoStatus: canceled` on that handle; `reschedule_send` cancel-and-resubmits (JMAP has no in-place reschedule) against the same `emailId` with a new `holduntil`, returning the new submission id.

Search maps the shared `SearchRequest` AST to `Email/query` (query text as a JMAP `text` filter). `search_messages` returns native email ids; `search` uses `collapseThreads = true`, hydrates the emails' `threadId`, returns thread ids. Page cursors are opaque position bytes.

Container CRUD is `Mailbox/get` / `Mailbox/set`. Mailboxes surface as `ContainerKind::Folder`; `ContainerId`/`native_id` are the native mailbox id; `Mailbox.role` maps to `FolderRole` (`inbox`->INBOX, `sent`->SENT, `drafts`->DRAFT, `archive`->archive, `trash`->TRASH, `junk`->SPAM). `container_delete` leaves `onDestroyRemoveEmails = false`, so non-empty deletion fails rather than silently dropping messages.

Settings primitives use `Identity/get`/`set`, `VacationResponse/get`/`set` on the `singleton` id, and `Quota/get` when the capability has a primary account. `identity_update` supports name, signatures, and reply-to; default-identity selection is unsupported (no writeable field).

`thread_hydrate` does `Thread/get` then `Email/get` in thread order. `message_hydrate` selects headers, preview, or full body-value projections and returns attachment blob handles without pre-downloading. `move_thread`/`delete_thread` add to the target mailbox then remove from the source; deleting from Trash destroys the thread's emails.

### Server-side filter scripts

`filters.rs` maps Stage 2 filter primitives onto JMAP Sieve:

- `filters_list` runs `SieveScript/query`, hydrates with `SieveScript/get`, downloads each blob via the download URL, returns `ServerFilter::Script`.
- `filter_create` uploads the body as `application/sieve`, creates a `SieveScript` with that blob id, and uses `onSuccessActivateScript` when the create asks for an active script.
- `filter_update` patches name and body through `SieveScript/set` (body changes upload a fresh blob first); `is_active` toggles via `onSuccessActivateScript` / `onSuccessDeactivateScript`.
- `filter_delete` destroys the `SieveScript` id; active-script delete failures surface as normal set errors.
- `filter_validate` uploads the body and calls `SieveScript/validate`; set errors become `FilterValidation` diagnostics instead of storing a script.

Typed `ServerFilterCreate::Rule` / `ServerFilterPatch::Rule` are unsupported and return `Unsupported`.

### HTTP redirect handling

`ReqwestTransport` delegates redirects to `bifrost-net` with JMAP's trusted-host allowlist and five-hop limit; the engine account id tags the attached transport. `bifrost-net` strips `Authorization` on every cross-host hop, including Basic headers `ReqwestTransport` injects into the `HeaderMap`. The bearer path routes through bifrost-net; only Basic and the WebSocket handshake build a header via async `header_value()` (awaits `current()`).

### Error translation

`sync/error.rs::into_account_error(error, ctx)` is the single translation boundary: it consumes `crate::Error` plus a `JmapErrorContext { operation, scope, .. }` and emits an `AccountError` via `AccountErrorBuilder`, routing `(AccountErrorKind, Cause)` plus operation, scope, and any `AttemptCause` through `bifrost-types::recovery::derive` (central `RecoveryClass`; no private `to_recovery` table).

Mapping highlights for the JMAP signals the central table reads:

- `Method(stateMismatch)` -> `ConcurrencyConflict` kind +
  `State(ConcurrencyConflict)` cause -> `Retry::AfterStateRefresh`.
- `Method(cannotCalculateChanges)` -> `SyncState(CursorInvalid)` +
  `State(CursorInvalid)` -> `Engine(RestartScope)` when scope is
  known, otherwise `Engine(RestartAccount)`.
- `Method(serverUnavailable | serverFail | serverPartialFail)` ->
  `Server(Unavailable)` -> `Retry::SameRequest`.
- `Method(requestTooLarge | tooManyChanges)` -> `SyncState
  (CursorInvalid)` or `Request(Malformed)` depending on context.
- `Method(forbidden)` -> `Authorization(PermissionDenied)` ->
  `NoPermission`.
- `Problem(limit)` -> `Server(RateLimited)` with `throttle_scope`.
- `Problem(unknownCapability)` -> `SyncState(CapabilityChanged)` ->
  `Engine(RestartAccount)` (no directive; routes through full reopen).
- `Problem(notJSON | notRequest)` -> `Protocol(ContractViolation)`.
- HTTP-only status fallbacks (401/403/429/5xx) on bare
  `Problem` -> `Authentication` / `Server(RateLimited)` /
  `Server(Unavailable)` per the central rules.
- `Transport(_)` -> `Transport(Network)` with `AttemptCause::transmission_state` from where the wire failure occurred; the central mapping picks `Retry::SameRequest` for idempotent ops and `Reconcile` for non-idempotent ops caught mid-flight.
- WebSocket errors split by handshake position: `Error::WebSocketHandshake`
  (pre-handshake, from `Client::connect_ws`) -> `Transport(Network)` +
  `Attempt(Unsent)`; `Error::WebSocketRuntime` (post-handshake stream) ->
  `Protocol(PartialResponse)` + `Attempt(Acknowledged)`. No blanket
  `From<tokio_websockets::Error>` impl; call sites map explicitly to attach
  the right `TransportCause` + `AttemptCause`.
- `NoPrimaryAccount` -> `Authentication(ReauthorizationRequired)`
  -> `AuthLost`.
- Local shape errors (`Parse`, `Set`, `CallNotFound`, `IdNotFound`,
  `EmptyResponse`, `NotParsable`, `InvalidUrl`) -> `Request
  (Malformed)` -> `ClientBug`.

Cursor-decode failures from `cursor::envelope` (protocol mismatch, unknown envelope, malformed payload) build their own AccountError with `SyncState(SchemaIncompatible)`, routed to `Engine(SchemaIncompatible)`.

Known JMAP `SetErrorType` vocabulary lands on typed
`WireCause::Jmap(JmapMethod::*)` variants
(`StateMismatch`, `MailboxHasChild`, `MailboxHasEmail`,
`OverQuota`, `RateLimit`, ...). `SetErrorType::Other(code)` is the
only path to `JmapMethod::Unknown { code }`; the variant carries the
actual wire code (manual `Deserialize` impl mirrors
`MethodErrorType::Other(String)`), so the conversion boundary never
synthesizes a placeholder `"other"` literal. Matching unknown
vocabulary via string comparison is forbidden by the convergence
plan's gate-5 invariant.

The `sync/error.rs` boundary exposes two stream-side terminator
helpers: `terminated_unsupported(operation, scope, msg)` for
`Unsupported(op)` kinds, and `terminated_contract_violation(operation,
scope, msg)` for `Protocol(ContractViolation)` kinds. Pagination
overflows and other response-shape mismatches use the latter; the
former is reserved for operations the JMAP account genuinely cannot
perform. Both take the caller's `AccountOperation` so the kind is
not hard-coded to `Discover`.

### Known limitations

- `Thread` and `Query` inventory are not implemented and emit fatal-unsupported events. Thread changes and query changes are supported.
- Raw-MIME hydration projections are not supported; only `Projection::FlagsOnly` and `Projection::Metadata` work.
- Push is only available via the JMAP WebSocket subprotocol. There is no HTTP push or EventSource fallback in the account layer.
- `BlobRangeSupport::No`, `blob_digest_pre_download: false`. `open_blob_range` fatals `Error::Unsupported` even when the handle advertises range support (transport has no `Range` hook).
- `MutationReplaySafety::None`; `IdempotencyKey` is a wire no-op. The read-back guard is the only lost-update protection.
- `bulk_move` only supports `MembershipScope::Mailbox`; `inventory_partitioning` only `Page { from, to }` for `Email` (others fatal).
- Gmail labels, Graph categories/extended properties, and identity-default selection are unsupported in JMAP.
- Attachment handles keep blob id + MIME type, but uploaded filenames are not represented by `AttachmentHandle`.
- Typed filter-rule CRUD is unsupported; JMAP exposes literal Sieve scripts through the Account filter surface instead.
