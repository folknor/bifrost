# bifrost-jmap reference

Current architecture of the JMAP implementation crate. The only
external surface is `bifrost_jmap::sync` with the account factory and
its config types. The protocol client, typed wire objects, transport,
method macros, error type, and helper facade are internal to the crate.
Examples live in `crates/jmap/examples/` and demonstrate the
`AccountFactory` / `Account` surface only.

## Public surface

With the `sync` feature enabled, consumers may use:

- `sync::JmapAccountFactory` - registered as `Arc<dyn AccountFactory>`.
- `sync::JmapAccountFactoryBuilder` - open-time factory builder.
- `sync::JmapCredentials` - Basic or bearer credentials passed to the
  factory.
- `sync::ReconnectPolicy` - WebSocket reconnect backoff config.

Everything else in `crates/jmap/src/` is `pub(crate)` or narrower.
There are no public raw JMAP client APIs, public method macros, public
wire model modules, or public transport types. Consumers reach JMAP by
constructing the factory, opening an `Arc<dyn Account>`, and calling
methods from `bifrost-types::Account`.

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

The `bifrost_types::Account` implementation lives under `crates/jmap/src/sync/`, gated behind the `sync` feature. It is intentionally engine-facing only: the sync tree depends on `bifrost-types`, not on `bifrost-sync`. The implementation wraps the existing JMAP client (`Client<ReqwestTransport>` plus a `Mail`-capability `Account` handle) and maps every method on the `Account` trait onto one or more JMAP method calls.

Cursors are encoded as protocol-tagged opaque bytes via a hand-rolled length-prefixed format, scope-aware in both establishment and change streams. Push uses the JMAP WebSocket subprotocol with a single reader task and a broadcast fan-out.

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
  blob.rs          - open_blob / open_blob_range
  error.rs         - to_recovery / to_account_error mapping
```

### `JmapAccount` / `JmapAccountFactory` shape and lifecycle

`JmapAccountFactory` is the consumer-registered factory. It carries a `JmapAccountFactoryBuilder` config (URL, `JmapCredentials::Basic` or `JmapCredentials::Bearer`, optional timeout, `accept_invalid_certs`, `ReconnectPolicy`). `AccountFactory::open(account_id)` connects a `Client` and passes the engine account id into the `bifrost-net` attachment used by `ReqwestTransport`, so metering, priority, bandwidth caps, and trace correlation use the real engine key on every reopen. Open resolves the primary `Mail` account plus optional `Submission`, `VacationResponse`, `Quota`, and `Sieve` accounts, reads the session, builds `AccountCapabilities` and `CoreLimits`, and probes initial `Email` / `Mailbox` / `Thread` state strings to seed cursors. It spawns the WebSocket reader task with a `CancellationToken` and returns `Arc<dyn Account>`.

`JmapAccount` (`pub(crate)`) owns the `Client`, the `Mail`-capability `Account` handle, optional account handles for `Submission`, `VacationResponse`, `Quota`, and `Sieve`, the built capabilities, the per-scope cursor seed states, the `WsState`, a subscription registry, and shared `Mutex<Option<String>>` state caches for `email`, `mailbox`, and `thread`. `set_priority` and `set_bandwidth_cap` delegate to the underlying `bifrost-net::AccountNet` rather than storing local atomics; the transport owns the canonical knobs.

Reopen is delegated to the engine: when an account drops or `close()` returns, the engine calls `JmapAccountFactory::open` again. `close()` cancels the shutdown token (which terminates the WebSocket reader loop and any in-flight streams), then awaits a clean teardown. The `closed` flag short-circuits subsequent calls. Cancellation safety relies on the shared `CancellationToken` plus `tokio::select!` in the push stream; no `Account` method holds non-cancel-safe state across an await.

### Capabilities advertised

`capabilities::build` reads `session.core_capabilities()` and `session.websocket_capabilities()` to construct `AccountCapabilities`:

- `cursor_freshness: ServerIssued` - JMAP `state` strings are server-issued tokens; the engine can persist them and resume.
- `blob_range: BlobRangeSupport::No` - the existing JMAP transport exposes whole-blob downloads only. Range support would need a request hook for the HTTP `Range` header.
- `blob_digest_pre_download: false` - JMAP does not surface a content digest before download.
- `push: PushCapability::InProcess` when the session advertises `urn:ietf:params:jmap:websocket` with `supportsPush: true`, otherwise `PushCapability::None`.
- `mutation.concurrency: MutationConcurrency::StateBased` - the pipeline gates every `Email/set` with `ifInState`, which forces the server to reject the set on a state mismatch.
- `mutation.replay_safety: MutationReplaySafety::None` - JMAP has no wire replay token; the engine's read-back guard is the lost-update safety net.
- `batching_policy.max_items` - `core.maxObjectsInSet`, clamped to `[1, 500]`.
- `batching_policy.max_wait: 100ms`, `flush_on_input_close: true`.
- `rate_limit_class: RateLimitClass::Generous` - JMAP servers typically rate-limit per request size and per session rather than per second.
- `quota_signal: QuotaSignal::None` - JMAP quota is exposed through `quota_get` when available, but it does not feed a retry or scheduler signal.
- `requires_uidvalidity_recheck: false`.
- `historyid_expires_after: None` and `delta_token_expires_after: None` - JMAP state strings are not time-bound.
- `pim_methods` advertises real JMAP support for mailbox membership add/remove, keyword mutation, read-state mutation, attachment upload, draft lifecycle, search, mailbox CRUD, identity list/update when `Submission` is open, vacation get/set when `VacationResponse` is open, quota get when `Quota` is open, and thread/message hydration. Gmail labels, Graph categories, and Graph extended properties are false and return `Error::Unsupported`.
- `filter_rule_shape: Scripts` and every filter method flag is true
  when the session has a primary Sieve account. Without Sieve they
  are false and the shape is `None`.
- `conveniences` declares `starred = Keyword`, `replied_via_keyword = true`, `forwarded_via_keyword = true`, and both extended-property routes false. The default `set_starred`, `mark_replied`, and `mark_forwarded` therefore map to JMAP `$flagged`, `$answered`, and `$forwarded`.

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

- `CursorScope::Type(ObjectType::Email)` - inventory paginates via `Email/query` sorted by `receivedAt` descending then hydrates with `Email/get` using a fixed property set (`Id`, `MailboxIds`, `ThreadId`, `BlobId`, `Size`, `Keywords`, `MessageId`, `References`, `InReplyTo`, `ReceivedAt`). Changes use `Email/changes` against the cached state string and emit `Created` / `Updated` / `Destroyed` `ObjectChange`s. `inventory_partitioning` exposes a `Page { from, to }` partition for Email only.
- `CursorScope::Type(ObjectType::Mailbox)` - inventory is a single `Mailbox/get` call with the inventory properties (`Id`, `Name`, `ParentId`, `Role`, `SortOrder`, totals, unread counts, `IsSubscribed`). Changes use `Mailbox/changes`.
- `CursorScope::Type(ObjectType::Thread)` - changes use `Thread/changes`. Inventory is not implemented and emits a fatal-unsupported event explaining that thread inventory derives from email inventory.
- `CursorScope::Query(_)` - changes use `Email/queryChanges` and surface `ScopeChange` events; inventory emits a fatal-unsupported event because registered query definitions are out of scope for the v1 trait.

Every successful change-stream batch carries a `Checkpoint::Change(ChangeCursor)` whose state string is the post-call `newState`. The change loop continues until `hasMoreChanges` is false, then emits `SyncEvent::Done(Some(Checkpoint::Change(...)))`. Shared `Mutex<Option<String>>` state caches are advanced compare-and-swap style so a stale writer does not clobber a newer state.

`get_stream` (hydration) supports `Projection::FlagsOnly` and `Projection::Metadata` for Email. Raw-MIME projections emit a fatal-unsupported event - MIME assembly is outside this wave. Batches are sized at `max_objects_in_get`. Per-item lane: every hydrated email is emitted as `ItemOutcome::Succeeded(BatchSuccess { item, output: HydratedObject })` so the return type matches the unified `AccountStream<SyncEvent<ItemOutcome<HydratedObject>>>` trait signature. Locally-invalid input ids or transport-drop ambiguity flow through `ItemOutcome::Failed` / `Uncertain` on the same channel rather than terminating the entire stream.

### Push and reconnect

Push runs through a single reader task spawned at factory `open()` when the session advertises WebSocket push. The task connects to the JMAP WebSocket endpoint (`Client::connect_ws`, `dep:tokio-websockets/native-tls`), validates that the server accepted `Sec-WebSocket-Protocol: jmap`, re-applies the union of currently subscribed `DataType`s, emits `WatchEvent::Reconnected`, and forwards `PushObject::StateChange` notifications as `WatchEvent::Invalidated { hint: InvalidationHint { source: PushSource::JmapStateChange, payload: HintPayload::SpecificCursorScope(...) } }`. Per-message disconnects (and stream-level errors) emit `WatchEvent::Disconnected` and fall through to the reconnect loop.

`ReconnectPolicy { initial: 1s, max: 60s }` controls exponential backoff. Each successful reconnect resets `backoff` to `initial`; failures double `backoff` (saturating) up to `max`. Every exit error from the reader (connect failure or mid-stream drop) is classified through `into_account_error(_, JmapErrorContext::new(PushStream))`. Terminal-class errors (auth lost, capability changed, schema break) emit `WatchEvent::Terminated(AccountError)` and stop the reader so the engine reopens the account; retry-class errors emit `WatchEvent::Disconnected` and continue with the backoff loop. The previous shape (`Err(_) => break`/`disconnected`) erased every classification signal.

`push_stream` is a thin broadcast subscriber. A `Lagged` broadcast slot emits a coalesced `WatchEvent::Invalidated { source: PushSource::Coalesced, payload: Unknown }` so the engine triggers a full re-poll rather than silently losing notifications.

`subscribe` and `unsubscribe` build the union of all live `SubscriptionHandle` -> `DataTypeSet` mappings and call `Client::enable_push_ws` / `disable_push_ws`. `WebSocketNotConnected` maps to `Error::Unsupported` to signal the engine that push is unavailable.

`scope_lifecycle_stream` polls `Mailbox/changes` against the cached mailbox state and emits `ScopeLifecycle::Created` / `Renamed { old, new }` / `Deleted` for membership scope churn. Errors from the poll are classified through `into_account_error`; terminal classes (or engine-action classes) break out of the polling loop so the engine reopens the account, instead of the previous sleep-and-retry-silently behavior. The stream's element type is `ScopeLifecycle` (not `SyncEvent<_>`), so the protocol cannot emit a typed `Terminated(AccountError)` on this channel - ending the stream is the protocol-side signal the engine has to act on.

### Mutation pipeline

`bulk_set_flags`, `bulk_move`, and `bulk_destroy` share a `mutation_stream` engine. Targets are accumulated into batches sized at `max_objects_in_set` clamped to `[1, 500]`. Each batch is sent as a single `Email/set` call gated by `ifInState(current_state)`. On a `stateMismatch` method error the pipeline probes the current state via `Email/get` (empty ids), updates the cached state, and retries the same batch once. Other errors abort the stream with `SyncEvent::Terminated(AccountError)`.

`IdempotencyKey` is currently accepted on the API surface but not used at the wire level - JMAP exposes no idempotency token, so replay safety stays `MutationReplaySafety::None` and the engine's read-back guard is the protection against double-apply.

Per-id outcomes flow from `SetResponse::updated` / `destroyed`. An `ItemOutcome::Failed` with `AccountErrorKind::ConcurrencyConflict` is emitted when a `stateMismatch` survives the retry; other JMAP errors are mapped through `into_account_error` and surfaced as `ItemOutcome::Failed(BatchFailure { error, .. })`.

`bulk_move` only accepts `MembershipScope::Mailbox`; other membership shapes emit a fatal-unsupported event before any wire write.

### PIM primitives and conveniences

`pim.rs` implements the Stage 1 unified mail surface on the same JMAP client. Message and thread mutation primitives resolve `MutationTarget::Thread` through `Thread/get`, then issue `Email/set` patches against `mailboxIds`, `keywords`, or `$seen`; the set call is guarded with the cached `Email` state and retries once after `stateMismatch`. JMAP cannot express Gmail label membership, Graph categories, or Graph extended properties, so those primitives return `Error::Unsupported` and their capability flags are false.

Composition uses JMAP's native object model. `attachment_upload` stores bytes through the account upload URL and returns an opaque JMAP blob handle. `draft_create`, `draft_update`, and `draft_discard` use `Email/set` against the Drafts mailbox. `send_message` creates the draft `Email` and `EmailSubmission` in one JMAP request using a result reference, then either moves the message to Sent through `onSuccessUpdateEmail` or destroys it through `onSuccessDestroyEmail` when `save_to_sent == Some(false)`. `draft_send` submits an existing draft and moves it to Sent on success.

Search maps the shared `SearchRequest` AST to `Email/query`; provider-specific query text is submitted as a JMAP `text` filter. `search_messages` returns native email ids. `search` uses `collapseThreads = true`, hydrates the returned emails' `threadId`, and returns thread ids. Page cursors are opaque position bytes.

Container CRUD is `Mailbox/get` / `Mailbox/set`. JMAP mailboxes are surfaced as `ContainerKind::Folder`, `ContainerId` and `native_id` are the native mailbox id, and `Mailbox.role` maps to `FolderRole` as: `inbox` -> INBOX, `sent` -> SENT, `drafts` -> DRAFT, `archive` -> archive, `trash` -> TRASH, `junk` -> SPAM. `container_delete` leaves `onDestroyRemoveEmails = false`, so non-empty mailbox deletion fails instead of silently dropping messages.

Settings primitives use `Identity/get` / `Identity/set`, `VacationResponse/get` / `set` on the `singleton` id, and `Quota/get` when the corresponding JMAP capability has a primary account. `identity_update` supports name, signatures, and reply-to; setting a default identity is unsupported because JMAP has no matching writeable field.

`thread_hydrate` performs `Thread/get` followed by `Email/get` in thread order. `message_hydrate` selects headers, preview, or full body-value projections and returns attachment blob handles without pre-downloading bytes. `move_thread` and `delete_thread` are overridden because the trait defaults are unsupported: JMAP can add to the target mailbox and then remove from the source with the crate's owned handle; deleting from Trash destroys the thread's emails.

### Server-side filter scripts

`filters.rs` maps Stage 2 filter primitives onto JMAP Sieve:

- `filters_list` runs `SieveScript/query`, hydrates script metadata
  with `SieveScript/get`, then downloads each script blob through the
  account's download URL and returns `ServerFilter::Script`.
- `filter_create` uploads the script body as `application/sieve`,
  creates a `SieveScript` with that blob id, and uses
  `onSuccessActivateScript` when the shared create payload asks for
  an active script.
- `filter_update` patches name and body through `SieveScript/set`;
  body changes upload a fresh script blob first. `is_active` toggles
  through `onSuccessActivateScript` /
  `onSuccessDeactivateScript`.
- `filter_delete` destroys the `SieveScript` id. Active-script
  delete failures surface as normal JMAP set errors.
- `filter_validate` uploads the script body and calls
  `SieveScript/validate`; returned set errors become
  `FilterValidation` error diagnostics instead of storing a script.

Typed `ServerFilterCreate::Rule` and `ServerFilterPatch::Rule` are
not supported by JMAP and return `Unsupported`.

### HTTP redirect handling

`ReqwestTransport` delegates redirects to `bifrost-net` with JMAP's
trusted-host allowlist and five-hop limit. The factory-provided engine
account id tags the attached transport. `bifrost-net` strips
`Authorization` on every cross-host hop, including Basic-auth headers
that `ReqwestTransport` injects directly into the request `HeaderMap`.

### Error translation

`sync/error.rs::into_account_error(error, ctx)` is the single
translation boundary: it consumes `crate::Error` plus a
`JmapErrorContext { operation, scope, .. }` and emits an
`AccountError` via `AccountErrorBuilder`. The builder routes
`(AccountErrorKind, Cause)` plus operation, scope, and any
`AttemptCause` through `bifrost-types::recovery::derive` so the
final `RecoveryClass` is computed centrally - the JMAP crate
no longer carries a private `to_recovery` table.

Mapping highlights for the JMAP signals the central table reads:

- `Method(stateMismatch)` -> `ConcurrencyConflict` kind +
  `State(ConcurrencyConflict)` cause -> `Retry::AfterStateRefresh`.
- `Method(cannotCalculateChanges)` -> `SyncState(CursorInvalid)` +
  `State(CursorInvalid)` -> `Engine(RestartScope)` when scope is
  known, otherwise `Engine(RestartAccount)`.
- `Method(serverUnavailable | serverFail | serverPartialFail)` ->
  `Server(Unavailable)` -> `Retry::SameRequest, reason:
  ServerUnavailable`.
- `Method(requestTooLarge | tooManyChanges)` -> `SyncState
  (CursorInvalid)` or `Request(Malformed)` depending on context.
- `Method(forbidden)` -> `Authorization(PermissionDenied)` ->
  `NoPermission`.
- `Problem(limit)` -> `Server(RateLimited)` with `throttle_scope`
  from documented JMAP behavior.
- `Problem(unknownCapability)` -> `SyncState(CapabilityChanged)` ->
  `Engine(RestartAccount)`. `EngineDirective` has no
  `CapabilityChanged` variant; capability shifts route through full
  account reopen.
- `Problem(notJSON | notRequest)` -> `Protocol(ContractViolation)`
  -> `ProviderContractViolation`.
- HTTP-only status fallbacks (401/403/429/5xx) on bare
  `Problem` -> `Authentication` / `Server(RateLimited)` /
  `Server(Unavailable)` per the central rules.
- `Transport(_)` -> `Transport(Network)` with
  `AttemptCause::transmission_state` derived from where the wire
  failure occurred; the central mapping picks `Retry::SameRequest`
  for idempotent ops and `Reconcile` for non-idempotent ops
  caught mid-flight.
- WebSocket errors split by handshake position. The crate carries
  `Error::WebSocketHandshake(tokio_websockets::Error)` for
  pre-handshake failures from `Client::connect_ws` and
  `Error::WebSocketRuntime(tokio_websockets::Error)` for post-
  handshake stream failures. There is no blanket
  `From<tokio_websockets::Error>` impl; call sites map explicitly
  so the conversion boundary can attach the right `TransportCause` +
  `AttemptCause` pair. `WebSocketHandshake` -> `Transport(Network)` +
  `Attempt(Unsent)`; `WebSocketRuntime` -> `Protocol(PartialResponse)`
  + `Attempt(Acknowledged)`.
- `NoPrimaryAccount` -> `Authentication(ReauthorizationRequired)`
  -> `AuthLost`.
- Local shape errors (`Parse`, `Set`, `CallNotFound`, `IdNotFound`,
  `EmptyResponse`, `NotParsable`, `InvalidUrl`) -> `Request
  (Malformed)` -> `ClientBug`.

Cursor-decode failures from `cursor::envelope` (protocol mismatch,
unknown envelope, malformed payload) build their own AccountError
with `SyncState(SchemaIncompatible)`, which the central mapping
routes to `Engine(SchemaIncompatible)`.

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
- `BlobRangeSupport::No` and `blob_digest_pre_download: false`. `open_blob_range` returns a fatal `Error::Unsupported` even when the handle advertises range support, because the existing transport has no `Range` header hook.
- `MutationReplaySafety::None` and `IdempotencyKey` is currently a no-op on the wire. The engine's read-back guard is the only lost-update protection.
- `bulk_move` only supports `MembershipScope::Mailbox`.
- `inventory_partitioning` only supports `Page { from, to }` for `Email`; all other scope/partition combinations fatal as unsupported.
- Gmail label membership, Graph categories, and Graph extended properties are intentionally unsupported in JMAP.
- Identity default selection cannot be updated through JMAP.
- JMAP attachment handles preserve blob id and MIME type, but uploaded attachment filenames are not represented by the shared `AttachmentHandle` type.
- Typed filter-rule CRUD is unsupported; JMAP exposes literal Sieve
  scripts through the Account filter surface instead.
