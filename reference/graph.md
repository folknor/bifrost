# bifrost-graph reference

Current architecture of the Graph Account-layer code under
`crates/graph/src/account/`. The crate hosts a `GraphClient` for
Microsoft Graph's mail/calendar/contact REST API plus an
`Account` / `AccountFactory` pair that sits on top of it, driving
`@odata.deltaLink` delta-token sync, `/subscriptions` webhook
push with a renewal health worker, and an EWS streaming
notifications fallback for tenants where webhooks are not
reachable.

The same `Account` impl now owns Graph's Stage 1 PIM action
surface: message moves and flag/category writes, send/draft
lifecycle, search, folder CRUD, identities, out-of-office settings,
and one-shot message/thread hydration. Unsupported Graph gaps are
advertised explicitly through `pim_methods`.

Graph's change-tracking primitive is per-collection: messages
deltas are per `mailFolders/{id}/messages`, events deltas are per
`calendars/{id}`, contacts deltas are per `contactFolders/{id}`.
The account layer reflects that: cursors are scoped to
`CursorScope::FolderType { folder, ty }`, each cursor wraps a
single `@odata.deltaLink`, and `inventory_stream` / `changes_stream`
walk a chain of `@odata.nextLink` pages and terminate on the
final `@odata.deltaLink` page. Mutations route through
`POST /$batch` with `If-Match: <changeKey>` for optimistic
concurrency.

## Module layout

`crates/graph/src/account/`:

- `mod.rs` - `GraphAccount`, `GraphAccountFactory`, `PushMode`,
  the `impl Account` surface.
- `capabilities.rs` - `build_capabilities` per `PushMode`.
- `cursor.rs` - `GraphCursorKind`, `GraphCursorPayload`,
  `GraphPageMarker`, envelope encode/decode, scope/kind mapping.
- `scopes.rs` - `discover_cursor_scope_events`,
  `discover_membership_events`, `scope_lifecycle_events`,
  `CursorIndex`, `FolderTree`.
- `inventory.rs` - initial `delta?$select=...` walk, page
  pagination, inventory entry projection from Graph JSON.
- `changes.rs` - delta-token-driven change stream over the
  cached `@odata.deltaLink`.
- `get.rs` - `$batch`-backed hydration with per-projection
  `$select` lists.
- `push.rs` - `/subscriptions` webhook subscribe/unsubscribe,
  per-handle `GraphSubscriptionGroup`, the renewal health worker
  that re-issues expiring subscriptions and emits Disconnected /
  Reconnected on renewal failure.
- `push_stream.rs` - the broadcast-backed `push_stream` adapter
  that selects the receiver against the shutdown token and spawns
  the EWS streaming worker on demand.
- `ews_stream.rs` - EWS Streaming Notifications fallback:
  Subscribe / GetStreamingEvents XML, watermark tracking, scope
  recovery, the long-lived worker loop.
- `mutate.rs` - `bulk_set_flags` / `bulk_move` / `bulk_destroy`
  over `$batch` with `If-Match` and a `Retry-After`-aware
  throttle path.
- `pim.rs` - Stage 1 PIM primitives and Graph-specific
  conveniences: message move / read / category / extended-property
  writes, send/drafts, search, mail folder CRUD, identity snapshot,
  automatic replies, and typed hydration.
- `blob.rs` - `open_blob` / `open_blob_range` over Graph
  attachments (`/messages/{id}/attachments/{aid}/$value`),
  including the reference-attachment short-circuit.
- `error.rs` - `graph_error_to_fatal`,
  `recovery_for_graph_error`, `mutation_outcome_for_status`,
  and per-id warning helpers.

## `GraphAccount` / `GraphAccountFactory` shape and lifecycle

`GraphAccountFactory` carries a `GraphClient`, a `PushMode`, and
an optional `PushEndpoint` (the public HTTPS webhook URL).
`with_push_endpoint(url)` selects `PushMode::GraphSubscriptions`
and stores the webhook URL; `with_ews_streaming()` selects
`PushMode::EwsStreaming` and clears the endpoint. The default
factory shape is webhook-mode without an endpoint, in which case
`push_subscribe` returns `Error::MissingCoreCapability`.

`AccountFactory::open(account_id)` first attaches the `GraphClient`
to `bifrost-net` under the engine-provided `AccountId`, then
performs a `users/me`-shaped profile fetch (`get_profile`) to
validate the access token, constructs a `GraphAccount`, and runs
`list_mail_folders_recursive` to seed the in-memory `FolderTree`.
The factory does not pre-seed
cursors; cursors are minted lazily from `establish_initial_cursor`
plus the first `inventory_stream` page. The factory returns
`Arc<dyn Account>`.

`GraphAccount` owns:

- The shared `GraphClient` (clones are cheap; the inner state is
  `Arc`-shared).
- The built `AccountCapabilities` (cached at `new`).
- The push mode plus optional webhook endpoint.
- A `broadcast::Sender<WatchEvent>` whose receivers feed
  `push_stream`.
- An `Arc<RwLock<CursorIndex>>` (the discovered cursor scope
  list) and an `Arc<RwLock<FolderTree>>` (parent map for the
  folder hierarchy).
- A `HashMap<SubscriptionHandle, GraphSubscriptionGroup>` for
  webhook subscriptions plus an `Arc<Mutex<Option<JoinHandle>>>`
  for the renewal health worker.
- A `HashMap<SubscriptionHandle, EwsSubscriptionState>` plus a
  matching `JoinHandle` slot for the EWS streaming worker.
- A `CancellationToken` driving worker shutdown.
- An `etag_index: Arc<RwLock<HashMap<String, String>>>` of
  per-object change keys harvested from inventory, changes, and
  get responses; this powers `If-Match` on mutations.
- `set_priority` and `set_bandwidth_cap` delegate to the
  underlying `AccountNet`; the transport owns the canonical knobs.

Reopen is delegated to the engine: when the account is dropped
or `close()` returns, the engine calls
`GraphAccountFactory::open` again to mint a fresh `GraphAccount`
with empty in-memory caches and a fresh shutdown token. The
factory holds the credentials and client, so the new account
inherits whatever access token the client currently exposes.

`close()` cancels the shutdown token and aborts the EWS worker
join handle. The graph subscription worker observes
`shutdown.cancelled()` on its select arm and exits cleanly. The
push stream wraps the broadcast receiver in a `stream::unfold`
that selects against the same shutdown token.

## Capabilities

`build_capabilities(push_mode)` in `capabilities.rs`:

- `cursor_freshness: CursorFreshness::ServerIssued`. Graph mints
  `@odata.deltaLink` server-side; the engine can persist and
  resume against it.
- `blob_range: BlobRangeSupport::Conditional`. Range support is
  per-handle: Graph fileAttachments support HTTP `Range` against
  `/$value`; itemAttachments and referenceAttachments do not.
  `BlobHandle::capabilities::supports_range` carries the
  per-attachment decision.
- `blob_digest_pre_download: false`. Graph does not surface a
  content digest in attachment metadata.
- `push:` depends on `PushMode`:
  - `PushMode::GraphSubscriptions` -> `PushCapability::WebhookOrEwsStream`.
    Out-of-process: subscription CRUD lives on the Account; the
    actual HTTPS receiver is wired by the consumer and feeds the
    engine's `InvalidationSink`. `push_in_process()` is false.
  - `PushMode::EwsStreaming` -> `PushCapability::InProcess`. The
    EWS worker runs inside the process and forwards
    `WatchEvent::Invalidated` on the in-process `push_stream`.
    `push_in_process()` is true.
- `mutation.concurrency: MutationConcurrency::StateBased`. Every
  mutation that is not a `Destroy` sends `If-Match: <changeKey>`;
  the cached etag comes from inventory / changes / get and is
  refreshed from `messages/{id}?$select=id` on a cold cache.
- `mutation.replay_safety: MutationReplaySafety::None`. Graph has
  no documented client-mintable replay token; the engine's
  read-back guard is the lost-update safety net.
- `batching_policy: { max_items: 20, max_wait: 100ms, flush_on_input_close: true }`.
  The 20-item ceiling matches Graph's documented `/$batch` limit.
- `rate_limit_class: RateLimitClass::Tiered`. Graph documents a
  per-mailbox concurrency tier and per-application throttling
  budget.
- `quota_signal: QuotaSignal::RetryAfter`. Throttled responses
  carry an explicit `Retry-After` header that the mutation
  pipeline forwards into `RecoveryClass::Retry`.
- `requires_uidvalidity_recheck: false`. Graph has no UIDVALIDITY
  concept.
- `historyid_expires_after: None`. Graph has no historyId.
- `delta_token_expires_after: None`. Microsoft documents no fixed
  delta-token lifetime; expiry is handled reactively when the
  next call returns 410 Gone or a 400 InvalidDeltaToken.
- `pim_methods`: true for `add_to_container`, `set_category`,
  `set_extended_property`, `set_is_read`, send/draft lifecycle,
  search, mail folder CRUD, `identities_list`, vacation get/set,
  and typed thread/message hydration. False for
  `remove_from_container`, `set_keyword`, `set_label_membership`,
  standalone `attachment_upload`, `identity_update`, and
  `quota_get`.
- `conveniences`: `starred = Category`, implemented by treating the
  reserved `$flagged` category input as Graph `flag.flagStatus`.
  Replied and forwarded conveniences dispatch to
  `set_extended_property` with `PidTagLastVerbExecuted`
  (`Integer 0x1081`) values 102 and 104. Keyword-backed replied /
  forwarded flags are false.

## Cursor envelope

`OpaqueChangeState` for Graph is tagged with
`ProtocolKind::Graph` and `envelope_version = GRAPH_CURSOR_ENVELOPE_VERSION`
(currently `1`). `CHANGE_CURSOR_ENVELOPE_VERSION` is the matching
`ChangeCursor.envelope_version`.

`GraphCursorPayload` carries:

- `kind: GraphCursorKind`. One of `MessagesDelta { folder_id }`,
  `EventsDelta { calendar_id }`, or `ContactsDelta { folder_id }`.
- `delta_link: String`. The `@odata.deltaLink` returned by the
  final page; this is the resume point for the next changes pass.
- `issued_at_unix_secs: u64`. Stamped at envelope construction
  for observability.
- `advanced_through: Option<GraphPageMarker>`. Set when the
  cursor was checkpointed mid-walk. The marker carries the
  `@odata.nextLink` and an optional `last_seen_id` so a resumed
  walk continues at the same page boundary.

`encode_cursor(scope, payload)` serializes the payload to JSON in
`OpaqueChangeState::bytes`. If `advanced_through` is set, it is
also encoded into the outer `ChangeCursor::advanced_through` as
`OpaqueProgressBytes`.

`decode_cursor` validates in order:

- Wrong `ProtocolKind` returns `Error::CursorProtocolMismatch`.
- `envelope_version > GRAPH_CURSOR_ENVELOPE_VERSION` returns
  `Error::CursorEnvelopeUnknown`.
- `envelope_version < GRAPH_CURSOR_ENVELOPE_VERSION` returns
  `Error::SchemaIncompatible` (no migration path defined).
- JSON deserialization failure on either the payload or the
  page-marker progress bytes returns `Error::Other`.

`scope_matches_payload` is an additional cross-check in
`changes_stream`: if the decoded payload's `GraphCursorKind`
projects to a `CursorScope` that does not match
`cursor.scope`, the stream emits a Fatal carrying
`RecoveryClass::SchemaIncompatible` and exits.

`establish_initial_cursor(scope)` calls `kind_for_scope(&scope)`
to validate that the scope shape is one of the three
delta-eligible variants, then returns
`CursorEstablishment::EstablishViaInventory` so the engine runs
`inventory_stream` to mint the first cursor. Scopes outside the
`FolderType { folder, ty }` shape return `Error::Unsupported`;
within `FolderType`, types other than `Email`, `Event` /
`CalendarEvent`, and `Contact` also return `Error::Unsupported`.

`describe_cursor` validates the cursor via `decode_cursor`. On
success it reports `cost_class: Cheap`, `strategy: ServerCursor`,
and `freshness: Some(Instant::now())`. On failure (wrong
protocol, unknown envelope version, malformed payload) it
reports `cost_class: Expensive`, `strategy: None`, and
`freshness: None` so the engine reseeds via inventory.

## Per-scope inventory, changes, hydration

Supported scopes:

- `CursorScope::FolderType { folder, ty: ObjectType::Email }` ->
  initial URL is `/{prefix}/mailFolders/{folder}/messages/delta?$select=...&$top=50`
  with `MESSAGE_SELECT` projection. `inventory_entry_from_value`
  pulls id, conversationId (thread), change-key (etag),
  Message-ID / References / In-Reply-To from
  `internetMessageHeaders`, and a flags hash derived from
  `isRead`, `flag.flagStatus`, and `categories`. `size` is left
  `None` because Graph's message resource does not surface a
  stable byte count for this projection.
- `CursorScope::FolderType { folder, ty: ObjectType::Event | CalendarEvent }`
  -> initial URL is the calendarView delta over a [-90d, +365d]
  window with `EVENT_SELECT`. Events are surfaced as inventory
  entries through the same path; thread / message-id fields are
  empty.
- `CursorScope::FolderType { folder, ty: ObjectType::Contact }`
  -> initial URL is `/{prefix}/contactFolders/{folder}/contacts/delta?$select=...&$top=250`
  with `CONTACT_SELECT`.
- Any other scope / type combination causes
  `initial_delta_url` to return an error string that
  `inventory_stream` projects into a `SyncEvent::Fatal` with a
  default-retry recovery; downstream callers should rely on
  `establish_initial_cursor` to gate scopes before that point.

The inventory walk reads pages through
`GraphClient::get_json` for relative URLs and
`GraphClient::get_absolute` for the `@odata.nextLink` chain.
Each `ODataCollection<Value>` page yields a Batch with
`PageBoundary::Page` while a `next_link` is present, and a final
Batch with `PageBoundary::Final` plus a `Checkpoint::Change`
when the page returns a `delta_link` instead. Entries flagged
with `@removed` are skipped during the inventory pass. The
etag harvested from each entry is folded into
`account.etag_index` so the next mutation can issue
`If-Match` without a separate metadata round-trip.

`changes_stream(cursor)` decodes the payload, asserts
`scope_matches_payload`, and walks `delta_link` (or
`advanced_through.next_link` if resuming) the same way. For each
non-removed entry it emits both an
`ObjectChange { kind: Updated }` and a
`ScopeChange { kind: Added }` row (Graph delta does not
distinguish created vs. updated for non-removed entries). For
`@removed` entries it emits a single
`ScopeChange { kind: Removed }`. Each Page-boundary batch
checkpoints a `ChangeCursor` whose payload's `advanced_through`
points at the next link; the final page checkpoints a cursor
whose `delta_link` is the freshly minted resume point and
`advanced_through` is cleared.

`get_stream` is shared across projections. It chunks ids into
`batching_policy.max_items` blocks and fires a single `/$batch`
per chunk, then projects responses through `hydrated_from_value`:
`FlagsOnly` returns a `HashSet<String>` of canonical flags
(`\seen`, `\flagged`, `category:<name>`), `Metadata` re-runs
`inventory_entry_from_value`, and the raw-MIME projections
(`Headers`, `Preview`, `TextOnly`, `Full`, `FullWithBlobs`)
serialize the raw JSON value as bytes inside `HydratedObjectKind::RawMime`.
Attachment metadata is surfaced as `BlobHandle`s on the
hydrated object.

`scope_lifecycle_stream` currently emits an empty stream. Graph
does not expose a folder-lifecycle change notification surface,
and the engine's adaptive polling cadence has not been wired into
the protocol crate yet; discovery is re-run on account reopen.

## Push: webhooks plus EWS streaming fallback

Push has two modes, selected on the factory and reflected in the
capability surface.

### Webhook mode (`PushMode::GraphSubscriptions`)

`push_subscribe(scopes)` groups the requested scopes by
`/me/mailFolders/{folder}/messages` (or `/me/events`,
`/me/contactFolders/{folder}/contacts`) resource, issues one
`POST /subscriptions` per resource through the
`crate::webhooks` helpers, stores the resulting
`(server_id, resource, client_state, expires_at)` tuples on a
`GraphSubscriptionGroup` keyed by the returned
`SubscriptionHandle`, and emits a `WatchEvent::Reconnected` on
the broadcast channel.

`push_unsubscribe(handle)` removes the group, deletes each
underlying server-side subscription via
`DELETE /subscriptions/{id}`, and if the subscription map is
empty after removal also aborts the renewal worker.

`ensure_graph_worker` starts the renewal health worker on the
first subscribe and re-arms it if a previous handle has finished.
The worker loops on a `RENEWAL_CHECK_INTERVAL` (10 minutes)
sleep against the shutdown token: on each tick it walks every
group's subscriptions, identifies any whose `expires_at` is
within `RENEWAL_THRESHOLD_MINUTES` (30 minutes), and calls
`renew_subscription` for each. Successful renewals update the
in-memory `expires_at`. The worker tracks a `disconnected`
sticky flag: the first renewal failure in a healthy run emits
`WatchEvent::Disconnected`; the first fully successful pass
after a failed one emits `WatchEvent::Reconnected`. Both events
flow through the same broadcast as inbound invalidations, so the
engine sees streaming health on a single channel. The worker
exits cleanly on shutdown or when the subscription map drains.

The webhook receiver itself is not part of this crate. Consumers
mount an HTTPS endpoint at `PushEndpoint::webhook_url`, validate
`clientState` per the Microsoft Graph contract, and feed
invalidation notices into the engine's `InvalidationSink`. The
account's `push_stream` carries connection health only;
invalidations from the webhook receiver do not flow through it.

### EWS streaming mode (`PushMode::EwsStreaming`)

`push_subscribe` installs an `EwsSubscriptionState`
(scopes plus empty subscription id / watermark) keyed by a new
`SubscriptionHandle` and starts the EWS worker via
`push_stream::ensure_ews_worker`. `push_unsubscribe` drops the
state entry.

`run_streaming_worker` (in `ews_stream.rs`) runs an outer
reconnect loop:

1. Read the union of currently subscribed scopes.
2. Send an EWS `Subscribe` request carrying the folder ids plus
   the most recent watermark. Parse the response.
3. Record the returned `SubscriptionId` and `Watermark` against
   every active state.
4. Enter `run_get_events_loop`: long-poll
   `GetStreamingEvents` (30 minute timeout). Parse notifications,
   record the per-notification `Watermark`, project each
   notification's `ParentFolderId` to a `CursorScope::FolderType`
   (falling back to a synthetic Email scope when the scope is
   not in the subscribed set), and emit
   `WatchEvent::Invalidated { hint: { source: EwsStreaming, payload: SpecificCursorScope(...) } }`
   on the broadcast.

Network or parse failures emit
`WatchEvent::Disconnected` once per disconnect, sleep, and
reconnect; a successful resubscribe after a disconnect emits
`WatchEvent::Reconnected`. The worker checks `shutdown`
between every step.

`push_stream` is a `broadcast::Receiver<WatchEvent>` adapter
wrapped in a `stream::unfold` that selects against the shutdown
token. The EWS branch additionally re-spawns the worker on
demand so a consumer that subscribes to `push_stream` lazily
still sees events.

## Mutation pipeline

`bulk_set_flags`, `bulk_move`, and `bulk_destroy` share
`bulk_mutation_stream`. Targets accumulate into chunks of
`batching_policy.max_items` (20), and each chunk is submitted via
`submit_batch`. Submission proceeds:

1. Snapshot the etag cache, then refresh any missing etags for
   `SetFlags` / `Move` operations by issuing
   `GET /messages/{id}?$select=id` per missing id. Failures here
   produce per-id `MutationOutcome::Failed(Error::Transport(...))`.
   `Destroy` does not require an etag.
2. Build one `BatchRequestItem` per id: `PATCH` for `SetFlags`,
   `POST /messages/{id}/move` for `Move`, `DELETE` for `Destroy`.
   `If-Match: <changeKey>` is attached when an etag is available
   (mandatory for `SetFlags` / `Move`, opportunistic for
   `Destroy`).
3. Send the `/$batch` request. Per-response status drives
   `mutation_outcome_for_status`: 2xx -> `Applied`,
   404-on-destroy -> `Skipped`, 412 (precondition failed) ->
   `Skipped`, 429 -> `Failed(Transport(...))` plus a sticky
   `retry_after` capture, other -> `Failed(Transport(...))`.
4. If any item returned 429, emit a trailing
   `SyncEvent::Fatal(RecoveryClass::Retry { after })` with the
   `Retry-After` header value (or 30s default) so the engine
   backs off before resending the rest of the stream.

`bulk_set_flags` translates the `FlagOp` to a Graph PATCH body:
`isRead` for `\\seen` / `read`, `flag.flagStatus` for
`\\flagged` / `flagged` / `starred`, and a sorted
`categories` array for `category:<name>` flags. Unrecognized
flags are ignored. The `Set` op rewrites the full
`categories` array; `Add` / `Remove` / `Patch` only touch the
named fields.

`bulk_move` requires a `MembershipScope::Folder`; any other
destination shape is a fatal error before the request is built.

`IdempotencyKey` is currently accepted on the API surface but
not forwarded on the wire (Graph has no documented client
idempotency token), matching the
`MutationReplaySafety::None` capability.

## PIM primitives

Mail mutation primitives live in `pim.rs` and use per-message Graph
operations, fanning out a `MutationTarget::Thread` by querying
`/messages?$filter=conversationId eq ...`. `add_to_container` is
Graph's `POST /messages/{id}/move`; Graph has no symmetric
remove-from-folder operation, so `remove_from_container` is
unsupported. `set_is_read` patches `isRead`. `set_category` patches
`categories[]`, except the reserved `$flagged` / `flagged` /
`starred` inputs patch `flag.flagStatus`. `set_extended_property`
patches `singleValueExtendedProperties` when the value is `Some`;
the clear path (`value: None`) issues a batched
`DELETE /messages/{id}/singleValueExtendedProperties/<prop-id>` and
tolerates 404 so partial / never-set states resolve to `Ok`. The
convenience alias `PR_LAST_VERB_EXECUTED` maps to `Integer 0x1081`.
These writes send `If-Match` when Graph exposes `changeKey`.

Send and draft lifecycle use draft-backed Graph mail APIs so the
trait can return an object id: create a draft with `POST /messages`,
send it with `POST /messages/{id}/send`, and return the draft id.
`send_message` follows that same path. Inline file attachments are
encoded into Graph `fileAttachment` JSON. Standalone
`attachment_upload` is unsupported because Graph upload sessions are
message/draft scoped, not account scoped. `draft_update` patches the
draft's mutable message fields; attachment replacement in updates is
unsupported.

Search uses `/messages` with `$filter`, `$search`, `$top`, and
Graph's `@odata.nextLink` as the opaque page cursor. Message search
returns native message ids. Thread search deduplicates
`conversationId` values from the same result page.

Container CRUD maps to mail folders only. `containers_list` returns
native Graph folder ids with `Provenance { provider: Graph, kind:
Folder, native }`, refreshes the in-memory folder tree, and maps
well-known folders by fetching `inbox`, `sentItems`, `drafts`,
`deletedItems`, `junkEmail`, and `archive`. User folders remain
role-less. Create / rename / move / delete call the corresponding
`mailFolders` endpoints; root moves use the `msgfolderroot`
well-known destination.

Settings support is intentionally narrow. `identities_list` returns
the primary `me` profile as one default identity. Graph does not
expose a writable send-as/signature surface here, so
`identity_update` is unsupported. Vacation get/set maps to
`mailboxSettings.automaticRepliesSetting`. `quota_get` is
unsupported because the mail API does not expose a stable mailbox
quota resource.

Typed hydration is separate from the sync engine's `get_stream`.
`message_hydrate` fetches a single message at the requested
projection and maps Graph recipients, body, flags, parent folder,
thread id, headers, and attachment handles into
`bifrost_types::Message`. `thread_hydrate` queries all messages in a
conversation and returns them sorted by message date.

`move_thread` overrides the default to call Graph move directly
(move already removes the source folder). `delete_thread` resolves
Trash through `deletedItems`; a current Trash source destroys
messages, else they move to Trash. `apply_label` / `remove_label`
inherit the trait default, which routes Graph provenance to
`set_category` and `(Folder, non-Graph)` to `add_to_container` /
`remove_from_container`; everything else surfaces as `Unsupported`.

## Blobs

`open_blob` decodes the `BlobHandle::id` (a JSON
`GraphBlobLocator { message_id, attachment_id, kind }` payload),
short-circuits with a `BlobNotByteStream` warning when the kind
is `Reference`, fetches
`/messages/{mid}/attachments/{aid}/$value` with a bearer token,
and emits each `bytes_stream` chunk as a Batch.

`open_blob_range` extends the same path with a `Range` header.
A 200 instead of 206 in response to a Range request is fatal; a
405 in response to either call surfaces a `BlobNotByteStream`
warning so the engine can fall back to non-byte-stream handling.

`blob_handle_from_graph_attachment` sets
`supports_range = true` only for `fileAttachment` kinds; item-
and reference-attachments have `supports_range = false`. The
handle does not carry a digest (`digest_available_pre_download:
false` matches the account capability).

## Error mapping to the recovery taxonomy

`recovery_for_graph_error(message, scope)` in `error.rs` is the
shared message-shape classifier. It receives a `&str` because the
underlying `GraphClient` returns `Result<T, String>` end-to-end:
every method in `api.rs` and the HTTP plumbing in `client.rs`
formats the upstream status code and body into a string of the
shape `"Graph upload error {status}: {body}"`. The classifier
therefore matches on the lowercased message body rather than on
a structured status enum. The format is stable because the
client itself produces it, but the approach is fragile to any
upstream change to the error envelope shape or to localization
of the status text. Reconciling onto a structured per-call error
type with a typed `status: u16` is in scope for Phase 4 error
model convergence. Mappings:

- 410 / "gone" -> `RecoveryClass::RestartScope(scope.clone())`.
  The Graph delta token has been compacted past retention; the
  engine must mint a new cursor for this scope from inventory.
- 400 + ("invaliddeltatoken" | "invalid delta token" |
  "syncstatenotfound") -> `RecoveryClass::RestartScope(scope.clone())`.
  Same shape as 410, surfaced through 400 by some endpoints.
- 429 or "too many requests" -> `RecoveryClass::Retry { after: 30s }`.
- 503 / 504 -> `RecoveryClass::Retry { after: 30s }`.
- 401 / "unauthorized" -> `RecoveryClass::AuthLost`.
- Anything else -> `None`; the caller decides the default. In
  `graph_error_to_fatal` the default is `AuthLost` when the
  message smells like auth and `Retry { after: 30s }` otherwise.

`mutation_outcome_for_status(status, destroy, id)` projects
per-id `$batch` responses onto `MutationOutcome`: 2xx -> Applied,
404-on-destroy -> Skipped (idempotent delete), 412 -> Skipped
(etag mismatch; the engine reads back), 429 -> Failed(Transport),
other -> Failed(Transport).

`fatal_from_recovery(recovery, message)` is the constructor used
when the change stream detects a cursor protocol/envelope/schema
mismatch. Cursor decode errors map onto:

- `Error::CursorProtocolMismatch` -> `RecoveryClass::SchemaIncompatible`.
- `Error::CursorEnvelopeUnknown` -> `RecoveryClass::SchemaIncompatible`.
- `Error::SchemaIncompatible` -> `RecoveryClass::SchemaIncompatible`.
- Any other cursor decode error -> `RecoveryClass::Fatal`.

Non-byte-stream attachments emit
`Warning { kind: WarningKind::BlobNotByteStream, .. }` rather
than a Fatal, so the engine can continue past a
referenceAttachment in a mixed batch.

## Known limitations

- Discovery is mail-only. `discover_cursor_scope_events` only
  enumerates `mailFolders` and emits
  `CursorScope::FolderType { ty: ObjectType::Email }` rows.
  Event and contact cursors are valid if the engine constructs
  them by hand, but they are not surfaced through the cursor
  scope discovery API.
- `scope_lifecycle_stream` is empty. Graph has no folder-lifecycle
  notification surface and the polling-based scope refresh has
  not been wired in; folder creates / renames / deletes are
  observed only at account reopen.
- EWS streaming requires Exchange Web Services to be reachable
  on the tenant with an access token the server will accept on
  the EWS endpoint. Tenants that have disabled EWS or that block
  basic-auth-shaped EWS tokens cannot use the EWS fallback.
- Webhook mode requires a public HTTPS endpoint at
  `PushEndpoint::webhook_url`. Without it `push_subscribe`
  returns `Error::MissingCoreCapability`.
- Blob range support is per-handle. fileAttachment handles
  advertise `supports_range = true`; itemAttachment and
  referenceAttachment handles do not. The account capability is
  `BlobRangeSupport::Conditional` to reflect this split.
- Delta-token expiry is reactive. There is no proactive refresh
  loop; the first request that returns 410 Gone or 400
  InvalidDeltaToken collapses onto `RestartScope` and the engine
  rebuilds the cursor through inventory.
- `MutationReplaySafety::None`. `IdempotencyKey` is accepted on
  the API surface but not transmitted; the engine's read-back
  guard is the only lost-update protection beyond the
  `If-Match` etag gate.
- `remove_from_container`, keyword writes, Gmail-style label
  membership, standalone `attachment_upload`, `identity_update`, and
  `quota_get` are unsupported and flagged false in `pim_methods`.
- `send_message` / `draft_send` return the draft id because the
  send actions answer `202 Accepted` with no body. Callers needing
  the final Sent Items id rediscover it through sync or search.
- `draft_update` does not replace attachments; Graph attachment
  upload sessions need a larger primitive than Stage 1 exposes.
- `send_message` / `draft_create` / `draft_update` accept inline
  attachments embedded in the request (base64 `fileAttachment` via
  `graph_attachment_from_inline`) but reject pre-uploaded
  `AttachmentHandle`s with `Unsupported`, mirroring
  `attachment_upload` itself.
- `recovery_for_graph_error` is substring-based because
  `GraphClient` returns `Result<T, String>`; structured-error
  convergence is S1-W4 (error model) work.
