# bifrost-graph reference

Current architecture of the Graph Account-layer code under
`crates/graph/src/account/`. The public surface is
`bifrost_graph::account::{GraphClient, GraphAccountFactory}`:
`GraphClient` carries credentials / endpoints into the factory, and
consumers use the returned `Arc<dyn Account>`. Raw Graph REST helpers
and wire types are crate-private.

The same `Account` impl owns Graph's Stage 1 PIM action surface:
message moves, flag/category writes, send/draft lifecycle, search,
folder CRUD, identities, out-of-office, and one-shot message/thread
hydration, plus the Stage 3/4 contact and calendar primitives.
Unsupported gaps are advertised through `pim_methods`.

Graph change-tracking is per-collection: message deltas per
`mailFolders/{id}/messages`, event deltas per `calendars/{id}`, contact
deltas per `contactFolders/{id}`. Cursors are scoped to
`CursorScope::FolderType { folder, ty }`, each wraps one
`@odata.deltaLink`, and `inventory_stream` / `changes_stream` walk the
`@odata.nextLink` chain and terminate on the final `@odata.deltaLink`
page. Mutations route through `POST /$batch` with `If-Match: <changeKey>`
for optimistic concurrency.

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

Calendar/contact primitives live in `calendar.rs` and `contacts.rs`.
Graph calendar `color` is a provider enum token, not a CSS color value,
so it is not projected into the shared `Calendar.color` string.
Calendar reads request `Prefer: outlook.timezone="UTC"` so Graph event
date-times do not leak Windows timezone names on read. Calendar recurrence
maps common daily, weekly, monthly, and yearly Graph patterns to shared
RRULE strings and back. Unsupported outbound RRULE parts reject Graph
recurrence serialization instead of being written partially; unsupported
Graph recurrence shapes are still omitted on read. `relativeMonthly` /
`relativeYearly` RRULEs that carry neither BYMONTHDAY nor BYDAY are
rejected locally (Graph requires `daysOfWeek` on relative patterns and
400s otherwise). Outbound event times map a conservative table of
common IANA timezone names to Graph Windows timezone names, pass
already-Windows names through, and reject unknown IANA ids before
create/update payload construction instead of silently writing UTC.
Event-level `responseStatus` maps into shared `CalendarEvent.self_response`.
Graph RSVP uses the native
`accept` / `decline` / `tentativelyAccept` actions. Calendar and contact
update/delete fetch the current item and send `If-Match` when the
response carried a change key or ETag. Calendar and contact updates send
sparse PATCH bodies, so absent fields are left untouched and scalar
clears are encoded as JSON nulls (contact updates build a dedicated
patch body for this: a cleared display name / notes / organization emits
`null`, and a present repeated field replaces its Graph property,
emitting `null` / `[]` for buckets the new collection leaves empty).
Graph event organizer is server-derived on create; create payloads with
a shared organizer are rejected as unsupported. Graph event status is
also server-derived (`isCancelled` is set by cancellation actions, not a
writable field), so create / update payloads carrying any status other
than `Confirmed` are rejected before payload construction rather than
silently writing a confirmed event.
Graph contact home/work/other physical addresses map through the shared
`ContactAddress` model. Event search uses Graph Search API for
unscoped, non-empty default-mailbox searches, and falls back to local
filtering over Graph list pages for specific calendars, shared mailboxes,
empty searches, and cursor resumes. Composite `EventId`s normally embed
the hosting calendar (`{calendar}::{event}`), but the Search API spans
the whole mailbox without reporting each hit's calendar, so search hits
are minted with the `$mailbox` sentinel calendar segment; `event_url`
routes that segment through `/me/events/{id}` (Graph event ids are
mailbox-unique) so `event_get`/`update`/`delete` resolve without a
calendar, instead of 404-ing against a guessed default calendar. Contact search uses Graph's
documented exact email-address `$filter` for email-shaped queries, and
otherwise remains local filtering over Graph list pages because the
contacts endpoint only documents exact address filtering. Local search
paths scan pages until the requested number of matches is collected or
the server page chain ends.

## `GraphAccount` / `GraphAccountFactory` shape and lifecycle

The `account` module path remains public because the cross-crate
conformance test and existing consumers construct the factory
through it; helper modules and `GraphAccount` stay crate-private.
`GraphClient` is public only as factory input; request helpers stay
`pub(crate)`. `new` / `with_api_base*` take a raw token string;
`with_source` and `with_account_net` take a shared `Arc<dyn
TokenSource>`. The held source is what `attach_account` hands to
bifrost-net, read live per request. Other methods configure API bases or
shared-mailbox scoping.

`GraphAccountFactory` carries a `GraphClient`, a `PushMode`, and an
optional `PushEndpoint` (the public HTTPS webhook URL).
`with_push_endpoint(url)` selects `PushMode::GraphSubscriptions` and
stores the URL; `with_ews_streaming()` selects `PushMode::EwsStreaming`
and clears the endpoint. The default is webhook-mode without an
endpoint, where `push_subscribe` returns `Error::MissingCoreCapability`.

`AccountFactory::open(account_id)` attaches the `GraphClient` to
`bifrost-net` under the engine `AccountId`, validates the token with a
`users/me` profile fetch (`get_profile`), constructs a `GraphAccount`,
and runs `list_mail_folders_recursive` to seed the `FolderTree`. Cursors
mint lazily from `establish_initial_cursor` plus the first
`inventory_stream` page. Returns `Arc<dyn Account>`.

`GraphAccount` owns:

- The shared `GraphClient` (clones cheap; inner state `Arc`-shared).
- The built `AccountCapabilities` (cached at `new`).
- The push mode plus optional endpoint.
- A `broadcast::Sender<WatchEvent>` whose receivers feed `push_stream`.
- An `Arc<RwLock<CursorIndex>>` (discovered cursor scope list) and an
  `Arc<RwLock<FolderTree>>` (parent map for the folder hierarchy).
- A `HashMap<SubscriptionHandle, GraphSubscriptionGroup>` for webhook
  subscriptions plus an `Arc<Mutex<Option<JoinHandle>>>` for the renewal
  health worker.
- A `HashMap<SubscriptionHandle, EwsSubscriptionState>` plus a
  matching `JoinHandle` slot for the EWS streaming worker.
- A `CancellationToken` driving worker shutdown.
- An `etag_index: Arc<RwLock<HashMap<String, String>>>` of per-object
  change keys harvested from inventory, changes, and get responses; this
  powers `If-Match` on mutations.
- `set_priority` / `set_bandwidth_cap` delegate to the underlying
  `AccountNet`; the transport owns the knobs.

Reopen is engine-delegated: on drop or after `close()`, the engine calls
`GraphAccountFactory::open` again for a fresh `GraphAccount` with empty
caches and a fresh shutdown token. The factory holds the client, so the
new account reads the token source's current value.

`close()` cancels the shutdown token and aborts the EWS worker join
handle. The subscription worker observes `shutdown.cancelled()` on its
select arm and exits cleanly. The push stream wraps the broadcast
receiver in a `stream::unfold` selecting against the same token.

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
  `scheduled_send` (with native cancel/reschedule), search, mail
  folder CRUD, `identities_list`, vacation get/set, typed
  thread/message hydration, contact primitives, and calendar
  primitives. False for
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

`GraphCursorPayload` carries a delta kind, final `@odata.deltaLink`,
issue timestamp, and optional page marker for mid-walk checkpoints. The
payload lands in `OpaqueChangeState::bytes`; page markers also land in
`ChangeCursor::advanced_through`.

`decode_cursor` rejects wrong protocol, newer / older-incompatible
envelopes, and malformed JSON. `changes_stream` cross-checks that
payload kind projects back to `cursor.scope`; mismatches terminate with
`SyncState(SchemaIncompatible)`. `establish_initial_cursor` accepts only
delta-eligible `FolderType` scopes (email, event/calendar event,
contact) and mints the first cursor through inventory. A valid
`describe_cursor` is cheap/server-cursor/fresh; invalid cursors are
expensive and reseeded through inventory.

## Per-scope inventory, changes, hydration

Supported scopes:

- `FolderType { folder, Email }` -> initial URL
  `/{prefix}/mailFolders/{folder}/messages/delta?$select=...&$top=50`
  (`MESSAGE_SELECT`). `inventory_entry_from_value` pulls id,
  conversationId (thread), change-key (etag), Message-ID / References /
  In-Reply-To from `internetMessageHeaders`, and a flags hash from
  `isRead` / `flag.flagStatus` / `categories`. `size` is `None` (no
  stable byte count on this projection).
- `FolderType { folder, Event | CalendarEvent }` -> calendarView delta
  over a [-90d, +365d] window (`EVENT_SELECT`); thread / message-id
  fields are empty.
- `FolderType { folder, Contact }` -> initial URL
  `/{prefix}/contactFolders/{folder}/contacts/delta?$select=...&$top=250`
  (`CONTACT_SELECT`).
- Any other scope / type makes `initial_delta_url` return an error
  string that `inventory_stream` projects into
  `SyncEvent::Terminated(AccountError)` with `Unsupported(_)`;
  `establish_initial_cursor` gates scopes before that point.

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
`advanced_through.next_link` when resuming). Each non-removed entry
emits both `ObjectChange { Updated }` and `ScopeChange { Added }` (Graph
delta does not distinguish created from updated); `@removed` entries emit
a single `ScopeChange { Removed }`. Page-boundary batches checkpoint a
`ChangeCursor` whose `advanced_through` points at the next link; the
final page checkpoints the freshly minted `delta_link` resume point with
`advanced_through` cleared.

`get_stream` is shared across projections. It chunks ids into
`batching_policy.max_items` blocks and fires a single `/$batch` per
chunk, returning per-id `ItemOutcome<HydratedObject>` envelopes: 2xx ->
`Succeeded(BatchSuccess { output, .. })`, 4xx/5xx -> `Failed` carrying a
structured `AccountError` (via `response_to_account_error_pub`, so
`Protocol::Graph`, `AttemptCause(Acknowledged)`, `WireCause::Graph`, and
retry-hint / throttle-scope survive). 2xx-with-no-body is `Failed` with
`Protocol(MissingField)`. Locally-invalid items emit `Failed` rather than
poisoning the rest of the batch; a transport drop on the whole `/$batch`
request emits `SyncEvent::Terminated` at the stream level. The per-item
projector `hydrated_from_value` produces:
`FlagsOnly` -> a `HashSet<String>` of canonical flags
(`\seen`, `\flagged`, `category:<name>`); `Metadata` -> re-runs
`inventory_entry_from_value`; raw-MIME projections (`Headers`,
`Preview`, `TextOnly`, `Full`, `FullWithBlobs`) -> serialized JSON
inside `HydratedObjectKind::RawMime`. Attachment metadata is
surfaced as `BlobHandle`s on the hydrated object.

`scope_lifecycle_stream` is empty: Graph exposes no folder-lifecycle
notification surface and the engine's adaptive polling is not yet wired
into the protocol crate; discovery re-runs on account reopen.

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

`push_unsubscribe(handle)` deletes each server subscription and aborts
the renewal worker when no groups remain. The renewal worker wakes every
10 minutes, renews subscriptions inside the 30 minute threshold, emits
`Disconnected` on the first retryable failure, `Reconnected` after
recovery, and `Terminated(AccountError)` for terminal auth / policy /
permission failures.

The webhook receiver is not in this crate. Consumers mount an HTTPS
endpoint at `PushEndpoint::webhook_url`, validate `clientState`, and
feed invalidations into the engine `InvalidationSink`. The account
`push_stream` carries connection health only; webhook invalidations do
not flow through it.

### EWS streaming mode (`PushMode::EwsStreaming`)

`push_subscribe` installs an `EwsSubscriptionState` (scopes plus the
latest subscription id / watermark) and starts the EWS worker, which
subscribes to the union of active folders, long-polls
`GetStreamingEvents`, records watermarks, maps notifications back to
cursor scopes, and emits `WatchEvent::Invalidated`. Failures use
`ews_error_to_account_error`: terminal classes terminate; transient
classes emit `Disconnected`, sleep, reconnect, then `Reconnected`.

`push_stream` is a `broadcast::Receiver<WatchEvent>` adapter that
selects against shutdown. The EWS branch re-spawns its worker on
demand so lazy consumers still receive events.

## Mutation pipeline

`bulk_set_flags`, `bulk_move`, and `bulk_destroy` share
`bulk_mutation_stream`. Targets accumulate into chunks of
`batching_policy.max_items` (20), and each chunk is submitted via
`submit_batch`. Submission proceeds:

1. Snapshot the etag cache and refresh missing etags for `SetFlags` /
   `Move` via `GET /messages/{id}?$select=id` per missing id (failures
   become per-id `Failed` with `Transport(_)`). `Destroy` needs no etag.
2. Build one `BatchRequestItem` per id: `PATCH` (`SetFlags`),
   `POST /messages/{id}/move` (`Move`), `DELETE` (`Destroy`). `If-Match:
   <changeKey>` is attached when available (mandatory for `SetFlags` /
   `Move`, opportunistic for `Destroy`).
3. Send `/$batch`; status drives `mutation_item_outcome`: 2xx ->
   `Succeeded(Applied)`, 404-on-destroy -> `Succeeded(Skipped)`,
   412 / 429 / other -> `Failed` carrying a structured `AccountError`
   (same `response_to_account_error` path, so `AttemptCause(Acknowledged)`,
   `WireCause::Graph`, and `Retry-After` -> `RetryHint::After` survive
   for the central recovery mapping).

`bulk_set_flags` translates `FlagOp` to a PATCH body: `isRead` for
`\\seen` / `read`, `flag.flagStatus` for `\\flagged` / `flagged` /
`starred`, sorted `categories` for `category:<name>`. Unrecognized flags
are ignored. `Set` rewrites the full `categories` array; `Add` /
`Remove` / `Patch` only touch named fields.

`bulk_move` requires `MembershipScope::Folder`; any other shape is fatal
before the request is built. `IdempotencyKey` is accepted but not sent
(no Graph idempotency token), matching `MutationReplaySafety::None`.

## PIM primitives

Mail mutation primitives live in `pim.rs` and use per-message Graph
operations, fanning out a `MutationTarget::Thread` via
`/messages?$filter=conversationId eq ...`. `add_to_container` is
`POST /messages/{id}/move`; Graph has no symmetric remove, so
`remove_from_container` is unsupported. `set_is_read` patches `isRead`.
`set_category` patches `categories[]`, except reserved `$flagged` /
`flagged` / `starred` inputs patch `flag.flagStatus`.
`set_extended_property` patches `singleValueExtendedProperties` when
`Some`; the clear path (`None`) batches
`DELETE /messages/{id}/singleValueExtendedProperties/<prop-id>` and
tolerates 404. The alias `PR_LAST_VERB_EXECUTED` maps to
`Integer 0x1081`. These writes send `If-Match` when `changeKey` exists.

Send / draft lifecycle is draft-backed so the trait can return an id:
`POST /messages` to create, `POST /messages/{id}/send` to send, return
the draft id. Inline attachments encode into Graph `fileAttachment`
JSON. Standalone `attachment_upload` is unsupported (Graph upload
sessions are message/draft scoped). `draft_update` patches mutable
message fields; attachment replacement is unsupported.

Scheduled send is `PidTagDeferredSendTime` (`SystemTime 0x3FEF`): when
`SendRequest::scheduled` is `Some(t)` the boundary validates `t`
(future; Graph has no documented upper bound so it relies on server
rejection) and PATCHes that `singleValueExtendedProperty` (ISO-8601
UTC) onto the draft after create and before send. The returned draft
id is the cancel/reschedule handle. `cancel_scheduled_send` DELETEs the
deferred draft; `reschedule_send` PATCHes `PidTagDeferredSendTime` to
the new instant in place, returning the same id. `scheduled_send` is
unconditionally true for Graph mailbox accounts.

Search uses `/messages` with `$filter` / `$search` / `$top` and
`@odata.nextLink` as the opaque page cursor. Message search returns
native ids; thread search dedups `conversationId` per result page.

Container CRUD maps to mail folders only. `containers_list` returns
native Graph folder ids (`Provenance { Graph, Folder, native }`),
refreshes the folder tree, and maps well-known folders (`inbox`,
`sentItems`, `drafts`, `deletedItems`, `junkEmail`, `archive`); user
folders stay role-less. Create / rename / move / delete call the
`mailFolders` endpoints; root moves target `msgfolderroot`.

Settings support is narrow. `identities_list` returns the primary `me`
profile as the one default identity; `identity_update` is unsupported
(no writable send-as/signature surface). Vacation get/set maps to
`mailboxSettings.automaticRepliesSetting`. `quota_get` is unsupported
(no stable mailbox quota resource).

Typed hydration is separate from `get_stream`. `message_hydrate`
fetches one message at the requested projection and maps recipients,
body, flags, parent folder, thread id, headers, and attachment handles
into `bifrost_types::Message`. `thread_hydrate` queries the whole
conversation, sorted by message date.

`move_thread` overrides the default to call Graph move directly (move
removes the source folder). `delete_thread` resolves Trash through
`deletedItems`: a current Trash source destroys, else moves to Trash.
`apply_label` / `remove_label` inherit the trait default (Graph
provenance -> `set_category`, `(Folder, non-Graph)` ->
`add_to_container` / `remove_from_container`, else `Unsupported`).

## Blobs

`open_blob` decodes `BlobHandle::id` (a JSON
`GraphBlobLocator { message_id, attachment_id, kind }`), short-circuits
with a `BlobNotByteStream` warning for `Reference` kinds, fetches
`/messages/{mid}/attachments/{aid}/$value` with a bearer token, and
emits each `bytes_stream` chunk as a Batch. `open_blob_range` adds a
`Range` header; a 200 (not 206) to a Range request is fatal, and a 405
to either call surfaces a `BlobNotByteStream` warning for fallback.

`blob_handle_from_graph_attachment` sets `supports_range = true` only
for `fileAttachment` kinds; item / reference handles are false. The
handle carries no digest (`digest_available_pre_download: false`).

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
- `TooManyRequests` / 429 -> `Server(RateLimited)`, `throttle_scope:
  Tenant`, `retry_hint: After(_)` from `Retry-After` (integer-seconds
  and HTTP-date forms via `bifrost_net::parse_retry_after`).
- 503 / 504 -> `Server(Unavailable)` -> `Retry::SameRequest`.
- `InvalidAuthenticationToken` / 401 ->
  `Authentication(ReauthorizationRequired)` -> `AuthLost`.
- `AdminConsentRequired` -> `Authorization(AdminConsentRequired)` ->
  `NeedsAdminConsent`.
- `ConditionalAccessBlocked` / `AccessRestricted` /
  `MailboxNotEnabledForRestApi` -> `Authorization(ConditionalAccessBlocked
  | PolicyBlocked | MailboxNotLicensed)` -> `NeedsPolicyChange`.
- `AccessDenied` / `Forbidden` -> `Authorization(PermissionDenied)` ->
  `NoPermission`.
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

- Discovery is mail-only: `discover_cursor_scope_events` emits
  mail-folder Email scopes; event/contact cursors are
  engine-constructed.
- `scope_lifecycle_stream` is empty; folder creates / renames /
  deletes are observed only at reopen.
- EWS streaming requires EWS reachable with a token it accepts.
- Webhook mode requires a public HTTPS endpoint at
  `PushEndpoint::webhook_url`; otherwise `push_subscribe` returns
  `Error::MissingCoreCapability`.
- Blob range is per-handle (`BlobRangeSupport::Conditional`):
  fileAttachment handles `supports_range = true`, item/reference
  handles false.
- Delta-token expiry is reactive: 410 Gone / 400 InvalidDeltaToken
  collapses onto `Engine(RestartScope(scope))`.
- `MutationReplaySafety::None`. `IdempotencyKey` is accepted but not
  transmitted; the engine read-back guard is the only lost-update
  protection beyond the `If-Match` etag gate.
- `remove_from_container`, keyword writes, Gmail-style label
  membership, standalone `attachment_upload`, `identity_update`,
  and `quota_get` are unsupported (false in `pim_methods`).
- `send_message` / `draft_send` return the draft id because the send
  actions answer `202 Accepted` with no body; the Sent Items id is
  rediscovered via sync or search.
- `draft_update` does not replace attachments; Graph upload sessions
  need a larger primitive than Stage 1 exposes.
- `send_message` / `draft_create` / `draft_update` accept inline
  base64 `fileAttachment` via `graph_attachment_from_inline` but
  reject pre-uploaded `AttachmentHandle`s with `Unsupported`.
- Graph inbox rules are conjunction-shaped. `FilterCondition::And`
  maps to Graph conditions, `Not(...)` maps to Graph exceptions,
  and `Or`, date ranges, provider expressions, remove-label,
  mark-unread, star/unstar, keyword, and reject actions are rejected
  by local validation.
