# bifrost-graph reference

Current architecture of the Graph Account-layer code under
`crates/graph/src/account/`. The public surface is
`bifrost_graph::account::{GraphClient, GraphAccountFactory}`:
`GraphClient` carries credentials / endpoints into the factory,
and consumers use the returned `Arc<dyn Account>`. Raw Microsoft
Graph REST helpers and wire types are crate-private.

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
- `filters.rs` - Stage 2 Inbox `messageRules` typed-rule
  list/create/update/delete plus local validation.
- `blob.rs` - `open_blob` / `open_blob_range` over Graph
  attachments (`/messages/{id}/attachments/{aid}/$value`),
  including the reference-attachment short-circuit.
- `error.rs` - blob-not-byte-stream warning helper.
  Classification helpers live in `graph_error.rs`.

## `GraphAccount` / `GraphAccountFactory` shape and lifecycle

The `account` module path remains public because the cross-crate
conformance test and existing consumers construct the factory
through it; helper modules and `GraphAccount` stay crate-private.
`GraphClient` is public only as factory input. Its public methods
configure credentials, API bases, a pre-attached `AccountNet`,
token rotation, or shared-mailbox scoping; request helpers stay
`pub(crate)`.

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
  carry an explicit `Retry-After` header that the central
  recovery mapping translates into `RecoveryClass::Retry`'s
  `not_before` deadline.
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
- `filter_rule_shape: Rules`; all five filter method flags are
  true. Graph Inbox `messageRules` are wired for
  list/create/update/delete, and `filter_validate` performs local
  shape validation before writes.
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

`GraphCursorPayload` carries a delta kind, final
`@odata.deltaLink`, issue timestamp, and optional page marker for
mid-walk checkpoints. Encoding stores the payload in
`OpaqueChangeState::bytes`; page markers also land in
`ChangeCursor::advanced_through`.

`decode_cursor` rejects wrong protocol, newer envelope versions,
older incompatible envelopes, and malformed JSON. `changes_stream`
also cross-checks that payload kind projects back to `cursor.scope`;
mismatches terminate with `SyncState(SchemaIncompatible)`.
`establish_initial_cursor` accepts only delta-eligible
`FolderType` scopes (email, event/calendar event, contact) and asks
the engine to mint the first cursor through inventory. Successful
`describe_cursor` is cheap/server-cursor/fresh; invalid cursors are
expensive and reseeded through inventory.

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
  `inventory_stream` projects into a `SyncEvent::Terminated
  (AccountError)` with kind `Unsupported(_)`; downstream callers
  should rely on `establish_initial_cursor` to gate scopes
  before that point.

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
per chunk, returning per-id `ItemOutcome<HydratedObject>` envelopes:
2xx -> `Succeeded(BatchSuccess { output: HydratedObject, .. })`,
4xx/5xx -> `Failed(BatchFailure { error, .. })` carrying a structured
`AccountError` built through `response_to_account_error_pub` so
`Protocol::Graph`, `AttemptCause(Acknowledged)`,
`WireCause::Graph(signal)`, and retry-hint / throttle-scope are all
preserved. 2xx-with-no-body is also `Failed` carrying
`Protocol(MissingField)`. Locally-invalid items emit `Failed` rather
than poisoning the rest of the batch; transport drops on the whole
`/$batch` request emit `SyncEvent::Terminated` at the stream level.
The per-item projector `hydrated_from_value` produces:
`FlagsOnly` -> a `HashSet<String>` of canonical flags
(`\seen`, `\flagged`, `category:<name>`); `Metadata` -> re-runs
`inventory_entry_from_value`; raw-MIME projections (`Headers`,
`Preview`, `TextOnly`, `Full`, `FullWithBlobs`) -> serialized JSON
inside `HydratedObjectKind::RawMime`. Attachment metadata is
surfaced as `BlobHandle`s on the hydrated object.

`scope_lifecycle_stream` currently emits an empty stream. Graph
does not expose a folder-lifecycle change notification surface,
and the engine's adaptive polling cadence has not been wired into
the protocol crate yet; discovery is re-run on account reopen.

## Push: webhooks plus EWS streaming fallback

Push has two modes, selected on the factory and reflected in the
capability surface.

### Webhook mode (`PushMode::GraphSubscriptions`)

`push_subscribe(scopes)` groups scopes by Graph subscription
resource (`mailFolders/{folder}/messages`, `events`, or
`contactFolders/{folder}/contacts`) and rejects the whole request if
any scope is not subscribable. Successful subscribes create one
server subscription per resource, store `(server_id, expires_at)` in
a `GraphSubscriptionGroup`, and emit `WatchEvent::Reconnected`.

`push_unsubscribe(handle)` deletes each server subscription and
aborts the renewal worker when no groups remain. The renewal worker
wakes every 10 minutes, renews subscriptions inside the 30 minute
threshold, emits `Disconnected` on the first retryable renewal
failure, `Reconnected` after recovery, and `Terminated(AccountError)`
for terminal auth / policy / permission failures.

The webhook receiver itself is not part of this crate. Consumers
mount an HTTPS endpoint at `PushEndpoint::webhook_url`, validate
`clientState` per the Microsoft Graph contract, and feed
invalidation notices into the engine's `InvalidationSink`. The
account's `push_stream` carries connection health only;
invalidations from the webhook receiver do not flow through it.

### EWS streaming mode (`PushMode::EwsStreaming`)

`push_subscribe` installs an `EwsSubscriptionState` carrying scopes
and the latest subscription id / watermark, then starts the EWS
worker. The worker subscribes to the union of active folders,
long-polls `GetStreamingEvents`, records watermarks, maps
notifications back to cursor scopes, and emits
`WatchEvent::Invalidated` on the account broadcast. EWS failures use
`ews_error_to_account_error`: terminal classes terminate the stream;
transient classes emit `Disconnected`, sleep, reconnect, then emit
`Reconnected`.

`push_stream` is a `broadcast::Receiver<WatchEvent>` adapter that
selects against shutdown. The EWS branch re-spawns its worker on
demand so lazy consumers still receive events.

## Mutation pipeline

`bulk_set_flags`, `bulk_move`, and `bulk_destroy` share
`bulk_mutation_stream`. Targets accumulate into chunks of
`batching_policy.max_items` (20), and each chunk is submitted via
`submit_batch`. Submission proceeds:

1. Snapshot the etag cache, then refresh any missing etags for
   `SetFlags` / `Move` operations by issuing
   `GET /messages/{id}?$select=id` per missing id. Failures here
   produce per-id `ItemOutcome::Failed(BatchFailure { error, .. })`
   carrying an `AccountError` of kind `Transport(_)`. `Destroy`
   does not require an etag.
2. Build one `BatchRequestItem` per id: `PATCH` for `SetFlags`,
   `POST /messages/{id}/move` for `Move`, `DELETE` for `Destroy`.
   `If-Match: <changeKey>` is attached when an etag is available
   (mandatory for `SetFlags` / `Move`, opportunistic for
   `Destroy`).
3. Send the `/$batch` request. Per-response status drives
   `mutation_item_outcome`: 2xx -> `Succeeded(Applied)`,
   404-on-destroy -> `Succeeded(Skipped)`, 412 / 429 / other failures
   -> `Failed(BatchFailure { error, .. })` carrying a structured
   `AccountError` built through the same `response_to_account_error`
   path the boundary uses, so per-item failures pick up
   `AttemptCause(Acknowledged)`, `WireCause::Graph(signal)`, and
   `Retry-After` -> `RetryHint::After` on the `ServerCause`. The
   central recovery mapping resolves the structured hint into
   `RetryAdvice::retry_hint`.

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

## Error translation

`graph_error::into_account_error(error, ctx)` converts the
crate-internal `GraphError` enum into an `AccountError` via
`AccountErrorBuilder::try_build`. `GraphErrorContext { protocol,
operation, scope }` threads the calling operation so
`bifrost-types::recovery::derive` computes `RecoveryClass`; the Graph
crate has no private recovery table. `GraphErrorContext::ews(op)` is
the matching constructor for EWS failures; the EWS-side helper
`graph_error::ews_error_to_account_error` routes `EwsError::Transport`
through `bifrost_net::into_account_error`, maps `EwsError::HttpStatus`
through the same `response_to_account_error` path REST uses,
classifies `EwsError::SoapFault { code, detail }` onto the
`SoapFaultCode` taxonomy (Server -> Unavailable, Client/MustUnderstand/
VersionMismatch -> 400, Unknown -> ContractViolation), and routes
`EwsError::MalformedXml` to `Protocol(ParseFailed)`. Every produced
error stamps `Protocol::Ews`.

Known Graph vocabulary lands on typed `WireCause::Graph
(GraphSignal::*)` variants - `InvalidAuthenticationToken`,
`AccessDenied`, `Forbidden`, `AccessRestricted`,
`ConditionalAccessBlocked`, `AdminConsentRequired`,
`MailboxNotEnabledForRestApi`, `MailboxStoreUnavailable`,
`ResyncRequired`, `TooManyRequests`, `GenericFileError`,
`PreconditionFailed`, `NotFound`, `InvalidDeltaToken`,
`SyncStateNotFound`, `Gone`. `GraphSignal::Unknown { code }` is
reserved for forward-compat fallback; matching unknown
vocabulary via string comparison is forbidden by the
convergence plan's gate-5 invariant.

Mapping highlights:

- `Gone` / 410 / `InvalidDeltaToken` / `SyncStateNotFound` ->
  `SyncState(CursorInvalid)` -> `Engine(RestartScope(scope))`.
- `TooManyRequests` / 429 -> `Server(RateLimited)` with
  `throttle_scope: Tenant` and `retry_hint: RetryHint::After(_)`
  parsed from `Retry-After` (both integer seconds and HTTP-date forms
  supported via `bifrost_net::parse_retry_after`).
- 503 / 504 -> `Server(Unavailable)` ->
  `Retry::SameRequest`.
- `InvalidAuthenticationToken` / 401 ->
  `Authentication(ReauthorizationRequired)` -> `AuthLost`.
- `AdminConsentRequired` -> `Authorization(AdminConsentRequired)`
  -> `NeedsAdminConsent`.
- `ConditionalAccessBlocked` / `AccessRestricted` /
  `MailboxNotEnabledForRestApi` -> `Authorization
  (ConditionalAccessBlocked | PolicyBlocked | MailboxNotLicensed)`
  -> `NeedsPolicyChange`.
- `AccessDenied` / `Forbidden` ->
  `Authorization(PermissionDenied)` -> `NoPermission`.
- `MailboxStoreUnavailable` -> `Authorization(MailboxUnavailable
  { Transient })` -> `Retry::SameRequest`.
- `PreconditionFailed` / 412 -> `ConcurrencyConflict` ->
  `Retry::AfterStateRefresh`.

`mutation_item_outcome` projects per-id `$batch` responses
onto `ItemOutcome`: 2xx -> `Succeeded(Applied)`, 404-on-destroy ->
`Succeeded(Skipped)` (idempotent delete), 412 -> `Failed(BatchFailure)`
classified as `ConcurrencyConflict` (the engine reconciles via its
read-back guard; per-item Skipped would mask a lost update against
a deleted message), 429 and other failures -> `Failed(BatchFailure
{ error })` carrying a structured `AccountError` with
`Protocol::Graph`, `AttemptCause(Acknowledged)`, and the wire signal
preserved on the cause chain. The same projector is used by
`mutate.rs` for `bulk_*` flows, by `pim::submit_write_batch` for the
PIM single-error contract (where per-item failures unwrap into the
function's single `Result<(), AccountError>` return), and by
`get.rs` for `get_stream` per-item hydration outcomes.

Cursor-decode failures (`CursorProtocolMismatch`,
`CursorEnvelopeUnknown`, `SchemaIncompatible`, malformed payload)
build an AccountError with `SyncState(SchemaIncompatible)`, which
the central mapping routes to `Engine(SchemaIncompatible)`.

Non-byte-stream attachments emit
`Warning { kind: WarningKind::BlobNotByteStream, .. }` rather
than a terminal error, so the engine can continue past a
referenceAttachment in a mixed batch.

## Known limitations

- Discovery is mail-only. `discover_cursor_scope_events` emits
  mail-folder Email scopes; event/contact cursors must be
  engine-constructed.
- `scope_lifecycle_stream` is empty. Folder creates / renames /
  deletes are observed only at account reopen.
- EWS streaming requires Exchange Web Services to be reachable
  with a token the EWS endpoint accepts.
- Webhook mode requires a public HTTPS endpoint at
  `PushEndpoint::webhook_url`; otherwise `push_subscribe` returns
  `Error::MissingCoreCapability`.
- Blob range support is per-handle. fileAttachment handles
  advertise `supports_range = true`; itemAttachment and
  referenceAttachment handles do not. The account capability is
  `BlobRangeSupport::Conditional` to reflect this split.
- Delta-token expiry is reactive. 410 Gone or 400
  InvalidDeltaToken collapses onto
  `Engine(RestartScope(scope))`.
- `MutationReplaySafety::None`. `IdempotencyKey` is accepted on
  the API surface but not transmitted; the engine's read-back
  guard is the only lost-update protection beyond the
  `If-Match` etag gate.
- `remove_from_container`, keyword writes, Gmail-style label
  membership, standalone `attachment_upload`, `identity_update`,
  and `quota_get` are unsupported (flagged false in `pim_methods`).
- `send_message` / `draft_send` return the draft id because the
  send actions answer `202 Accepted` with no body. Callers needing
  the Sent Items id rediscover via sync or search.
- `draft_update` does not replace attachments; Graph attachment
  upload sessions need a larger primitive than Stage 1 exposes.
- `send_message` / `draft_create` / `draft_update` accept inline
  base64 `fileAttachment` via `graph_attachment_from_inline` but
  reject pre-uploaded `AttachmentHandle`s with `Unsupported`.
- Graph inbox rules are conjunction-shaped. `FilterCondition::And`
  maps to Graph conditions, `Not(...)` maps to Graph exceptions,
  and `Or`, date ranges, provider expressions, remove-label,
  mark-unread, star/unstar, keyword, and reject actions are rejected
  by local validation.
