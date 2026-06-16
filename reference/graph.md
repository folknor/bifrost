# bifrost-graph reference

Current architecture of the Graph Account-layer code under
`crates/graph/src/account/`. Public surface:
`bifrost_graph::account::{GraphClient, GraphAccountFactory}` -
`GraphClient` carries credentials / endpoints into the factory, consumers
use the returned `Arc<dyn Account>`. Raw REST helpers and wire types are
crate-private.

The same `Account` impl owns Graph's Stage 1 PIM action surface
(message moves, flag/category writes, send/draft lifecycle, search,
folder CRUD, identities, out-of-office, one-shot message/thread
hydration) plus the Stage 3/4 contact and calendar primitives.
Unsupported gaps are advertised through `pim_methods`.

Graph change-tracking is per-collection: message deltas per
`mailFolders/{id}/messages`, event deltas per `calendars/{id}`, contact
deltas per `contactFolders/{id}`. Cursors are scoped to
`CursorScope::FolderType { folder, ty }`, each wraps one
`@odata.deltaLink`, and `inventory_stream` / `changes_stream` walk the
`@odata.nextLink` chain to the final `@odata.deltaLink` page. Mutations
route through `POST /$batch` with `If-Match: <changeKey>`.

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
- `foreign.rs` - foreign (shared/delegate) mailbox folder codec:
  `encode_foreign` / `parse_folder` / `ParsedFolder` / `owner_tag`.
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
- `mutate.rs` - `bulk_set_flags` / `bulk_move` / `bulk_destroy` over
  `$batch` with `If-Match` and a `Retry-After`-aware throttle path.
- `pim.rs` - Stage 1 PIM primitives and Graph conveniences: message
  move / read / category / extended-property writes, send/drafts, search,
  mail folder CRUD, identity snapshot, automatic replies, typed hydration.
- `filters.rs` - Stage 2 Inbox `messageRules` typed-rule
  list/create/update/delete plus local validation.
- `blob.rs` - `open_blob` / `open_blob_range` over Graph
  attachments (`/messages/{id}/attachments/{aid}/$value`),
  including the reference-attachment short-circuit, plus
  `open_raw_rfc822` (whole message via `/messages/{id}/$value`).
- `cloud.rs` - `host_attachment`: OneDrive resumable upload + a
  `createLink` in one call. Session and link POSTs go through
  `GraphClient::post` (`/me/drive/root:/Attachments/{encoded}:/createUploadSession`,
  conflict `rename`); the pre-authed chunk PUT uses the raw
  `account_net()` builder with `.without_bearer_auth()` and resumes on
  `202 Accepted` (`offset = end`, no 308 path). `ShareScope` maps
  `Anyone -> anonymous`, `Organization -> organization`.
- `error.rs` - blob-not-byte-stream warning helper. Classification
  helpers live in `graph_error.rs`.

Calendar/contact primitives live in `calendar.rs` and `contacts.rs`.
Graph calendar `color` is a provider enum token, not projected into
`Calendar.color`. Calendar reads request `Prefer: outlook.timezone="UTC"`.
Recurrence maps common daily / weekly / monthly / yearly patterns to
RRULE and back; unsupported outbound parts reject serialization (no
partial writes), unsupported inbound shapes are omitted on read, and
`relativeMonthly` / `relativeYearly` lacking BYMONTHDAY and BYDAY are
rejected locally (Graph 400s without `daysOfWeek`). Outbound event times
map a conservative IANA -> Windows table, pass Windows names through, and
reject unknown IANA ids pre-payload. Event `responseStatus` maps to
`CalendarEvent.self_response`; RSVP uses native `accept` / `decline` /
`tentativelyAccept`. Calendar/contact update/delete fetch-then-`If-Match`
on a change key / ETag and send sparse PATCH bodies (absent untouched,
scalar clears as JSON null; contact updates emit `null` / `[]` for
emptied buckets). Event organizer and status are server-derived, so a
shared organizer or non-`Confirmed` status is rejected unsupported.
Contact addresses map through `ContactAddress`. Event search uses the
Graph Search API for unscoped non-empty default-mailbox searches, else
local filtering (specific calendars, shared mailboxes, empty searches,
resumes). Composite `EventId`s embed the calendar (`{calendar}::{event}`);
Search hits use the `$mailbox` sentinel and `event_url` routes them
through `/me/events/{id}`. Contact search uses exact email `$filter` for
email-shaped queries, else local filtering.

## `GraphAccount` / `GraphAccountFactory` shape and lifecycle

The `account` module path is public (the conformance test and
consumers construct the factory through it); helper modules and
`GraphAccount` stay crate-private. `GraphClient` is public only as
factory input; request helpers stay `pub(crate)`. `new` /
`with_api_base*` take a raw token; `with_source` / `with_account_net`
take a shared `Arc<dyn TokenSource>` that `attach_account` hands to
bifrost-net, read live per request.

`GraphAccountFactory` carries a `GraphClient`, a `PushMode`, an
optional `PushEndpoint` (the HTTPS webhook URL), and a
`shared_mailboxes: Vec<String>`. `with_push_endpoint(url)` selects
`PushMode::GraphSubscriptions`; `with_ews_streaming()` selects
`PushMode::EwsStreaming` and clears the endpoint. Default is webhook-mode
without an endpoint, where `push_subscribe` returns
`Error::MissingCoreCapability`. `with_shared_mailbox(id)` registers a
delegate/shared mailbox by its `/users/{id}` routing key (SMTP address or
user id); see "Foreign (shared/delegate) mailboxes" below.

`AccountFactory::open(account_id)` attaches the `GraphClient` to
`bifrost-net` under the engine `AccountId`, validates the token with a
`users/me` profile fetch (`get_profile`), constructs a `GraphAccount`,
and runs `list_mail_folders_recursive` to seed the `FolderTree`. Cursors
mint lazily from `establish_initial_cursor` plus the first
`inventory_stream` page. Returns `Arc<dyn Account>`.

`GraphAccount` owns:

- The shared primary `GraphClient` (cheap clone, `Arc`-shared inner)
  plus `shared_clients: Arc<HashMap<String, GraphClient>>`, one
  `for_shared_mailbox(id)` client per configured foreign mailbox (built
  once at `open`).
- The built `AccountCapabilities` (cached at `new`); the push mode plus
  optional endpoint; a `broadcast::Sender<WatchEvent>` feeding
  `push_stream`.
- An `Arc<RwLock<CursorIndex>>` (cursor scope list) and an
  `Arc<RwLock<FolderTree>>` (folder-hierarchy parent map).
- `HashMap<SubscriptionHandle, GraphSubscriptionGroup>` (webhook) +
  renewal-worker join slot; `HashMap<SubscriptionHandle,
  EwsSubscriptionState>` + EWS-worker join slot; a `CancellationToken`
  for worker shutdown.
- An `etag_index: Arc<RwLock<HashMap<String, String>>>` of per-object
  change keys harvested from inventory / changes / get, powering
  `If-Match` on mutations.
- `set_priority` / `set_bandwidth_cap` delegate to the underlying
  `AccountNet`.

Reopen is engine-delegated: on drop or after `close()`, the engine calls
`GraphAccountFactory::open` again for a fresh `GraphAccount` (empty caches,
fresh shutdown token); the factory holds the client, so the new account
reads the token source's current value.

`close()` cancels the shutdown token and aborts the EWS worker; the
subscription worker observes `shutdown.cancelled()` and exits. The push
stream wraps the broadcast receiver in a `stream::unfold` selecting
against the same token.

## Capabilities

`build_capabilities(push_mode)` in `capabilities.rs`:

- `cursor_freshness: ServerIssued`. Graph mints `@odata.deltaLink`
  server-side; the engine persists and resumes against it.
- `blob_range: Conditional` (per-handle: fileAttachments support `Range`
  against `/$value`, item/referenceAttachments do not;
  `BlobHandle::capabilities::supports_range` carries the decision);
  `blob_digest_pre_download: false` (no content digest in metadata).
- `push` depends on `PushMode`: `GraphSubscriptions` ->
  `WebhookOrEwsStream` (out-of-process: subscription CRUD on the Account,
  the HTTPS receiver wired by the consumer into the engine
  `InvalidationSink`; `push_in_process()` false); `EwsStreaming` ->
  `InProcess` (the EWS worker forwards `Invalidated` on `push_stream`;
  `push_in_process()` true).
- `mutation.concurrency: StateBased`. Every non-`Destroy` mutation sends
  `If-Match: <changeKey>`; the cached etag comes from inventory / changes
  / get, refreshed from `messages/{id}?$select=id` on a cold cache.
- `mutation.replay_safety: None`. No client-mintable replay token; the
  read-back guard is the lost-update net.
- `batching_policy: { max_items: 20, max_wait: 100ms, flush_on_input_close: true }`
  (the 20 ceiling matches Graph's `/$batch` limit).
- `rate_limit_class: Tiered` (per-mailbox concurrency tier + per-app
  throttle budget); `quota_signal: RetryAfter` (the `Retry-After` header
  becomes `Retry`'s `not_before` deadline).
- `requires_uidvalidity_recheck: false`; `historyid_expires_after: None`;
  `delta_token_expires_after: None` (expiry is reactive: 410 Gone / 400
  InvalidDeltaToken).
- `pim_methods`: true for `add_to_container`, `set_category`,
  `set_extended_property`, `set_importance`, `set_is_read`, send/draft
  lifecycle, `scheduled_send` (with native cancel/reschedule), search,
  mail folder CRUD, `identities_list`, vacation get/set, typed
  thread/message hydration, contact and calendar primitives, and
  `host_attachment` (OneDrive). False for `remove_from_container`,
  `set_keyword`, `set_label_membership`, standalone `attachment_upload`,
  `identity_update`, `quota_get`.
- `filter_rule_shape: Rules`; all five filter flags true (Graph Inbox
  `messageRules` list/create/update/delete, `filter_validate` does local
  shape validation before writes).
- `conveniences`: `starred = Category` (reserved `$flagged` input ->
  `flag.flagStatus`); replied / forwarded dispatch to
  `set_extended_property` with `PidTagLastVerbExecuted` (`Integer
  0x1081`) values 102 / 104 (keyword-backed flags false);
  `mdn_sent_via_keyword = false` (Graph `isReadReceiptRequested` is
  read-only, so `mark_mdn_sent` surfaces `Unsupported(UpdateFlags)`).

## Cursor envelope

`OpaqueChangeState` for Graph is tagged `ProtocolKind::Graph` with
`envelope_version = GRAPH_CURSOR_ENVELOPE_VERSION` (currently `1`).
`CHANGE_CURSOR_ENVELOPE_VERSION` is the matching
`ChangeCursor.envelope_version`.

`GraphCursorPayload` carries a delta kind, final `@odata.deltaLink`, issue
timestamp, and optional mid-walk page marker; it lands in
`OpaqueChangeState::bytes`, page markers also in
`ChangeCursor::advanced_through`.

`decode_cursor` rejects wrong protocol, incompatible envelopes, and
malformed JSON. `changes_stream` cross-checks that payload kind projects
back to `cursor.scope`; mismatches terminate with
`SyncState(SchemaIncompatible)`. `establish_initial_cursor` accepts only
delta-eligible `FolderType` scopes (email, event/calendar event, contact)
and mints the first cursor through inventory. A valid `describe_cursor` is
cheap/server-cursor/fresh; invalid cursors reseed through inventory.

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

The inventory walk reads pages via `get_json` (relative) /
`get_absolute` (the `@odata.nextLink` chain). Each
`ODataCollection<Value>` page yields a `PageBoundary::Page` Batch while a
`next_link` is present, then a `PageBoundary::Final` Batch plus a
`Checkpoint::Change` when the page returns a `delta_link`. `@removed`
entries are skipped on the inventory pass. Harvested etags fold into
`account.etag_index` for the next mutation's `If-Match`.

`changes_stream(cursor)` decodes the payload, asserts
`scope_matches_payload`, and walks `delta_link` (or
`advanced_through.next_link` on resume). Each non-removed entry emits
`ObjectChange { Updated }` + `ScopeChange { Added }` (Graph delta does not
distinguish created from updated); `@removed` emits one
`ScopeChange { Removed }`. Page batches checkpoint a `ChangeCursor` with
`advanced_through` at the next link; the final page checkpoints the fresh
`delta_link` with `advanced_through` cleared.

`get_stream` is shared across projections. It chunks ids into
`batching_policy.max_items` blocks, fires one `/$batch` per chunk, and
returns per-id `ItemOutcome<HydratedObject>`: 2xx ->
`Succeeded(BatchSuccess { output, .. })`, 4xx/5xx -> `Failed` carrying a
structured `AccountError` (via `response_to_account_error_pub`, so
`Protocol::Graph`, `AttemptCause(Acknowledged)`, `WireCause::Graph`, and
retry-hint / throttle-scope survive), 2xx-no-body -> `Failed
(Protocol(MissingField))`. Locally-invalid items `Failed` without
poisoning the batch; a transport drop on the whole request terminates the
stream. The projector `hydrated_from_value` produces `FlagsOnly` ->
canonical flag `HashSet` (`\seen`, `\flagged`, `category:<name>`);
`Metadata` and body-bearing projections (`Headers` / `Preview` /
`TextOnly` / `Full` / `FullWithBlobs`) -> `metadata_or_flags`. Graph's
JSON message is not assembled RFC822, so body-bearing projections degrade
to `Metadata` (stopgap until A1; assembled bytes come from
`open_raw_rfc822`). Attachment metadata surfaces as `BlobHandle`s.

`scope_lifecycle_stream` is empty: Graph exposes no folder-lifecycle
notification surface and adaptive polling is not yet wired in; discovery
re-runs on account reopen.

## Foreign (shared/delegate) mailboxes

A configured shared mailbox surfaces as ordinary
`CursorScope::FolderType { folder, Email }` scopes; the owning mailbox
identity rides inside the `FolderId` string via the `account/foreign.rs`
codec (`encode_foreign(mailbox, folder)` joins on `\u{1f}`,
`parse_folder` splits it back into `ParsedFolder::{Primary, Foreign}`).
No new `CursorScope` variant. `client_for_scope(scope)` selects the
`shared_clients` entry when the folder parses foreign, else the primary
client; `owner_of_scope(scope)` returns the `MailboxId` owner tag for a
foreign scope, `None` for primary. Discovery
(`discover_cursor_scopes_inner`) lists the primary folders, then each
shared mailbox's folders via its client, emitting foreign-namespaced
`FolderType` scopes; a per-mailbox permission denial is skipped with a
scoped `Warning` (`OperatorAttentionNeeded`) rather than failing the
whole discovery. `discover_memberships_inner` emits the foreign
`MembershipScope::Mailbox(owner)` owner tag alongside the folder
membership (the engine covering rule cannot form it - folder-id and
mailbox-id strings differ), and `inventory_stream` stamps that same tag
onto every foreign-scope item so a foreign mailbox's native folder ids
cannot be conflated with the primary's in the membership index. `initial_delta_url` reads the
prefix from `client_for_scope` and the native folder id from
`parse_folder`, so the
mailbox rides in `/users/{id}` and the native id in `/mailFolders/{id}`;
once the first cursor mints, the absolute `delta_link` is mailbox-correct
with no further change (`kind_for_scope` round-trips the encoded
`FolderId` through the opaque cursor).

Revocation isolation: `graph_shared_scope_error(error, scope, owner, ctx)`
in `graph_error.rs` quarantines just the foreign scope when the failure
classifies as `Authorization(PermissionDenied)` (AccessDenied / Forbidden
/ 403) and `owner.is_some()`, building `graph_scope_revoked` ->
`SyncState(ScopeRevoked)` -> `Engine(DisableScope(scope))`. A primary
scope (`owner == None`) flows through `into_account_error` and stays
terminal `NoPermission`. Wired at the `inventory_stream` /
`changes_stream` per-scope fetch-failure boundary.

Foreign-mailbox enumeration is config-supplied (`with_shared_mailbox`):
Graph REST has no "list my delegated mailboxes" call (the documented
routes are EWS `GetDelegate` / Autodiscover `alternativeMailboxes`,
neither present). Live delegate enumeration is A5b.

## Push: webhooks plus EWS streaming fallback

Push has two modes, selected on the factory and reflected in the
capability surface.

### Webhook mode (`PushMode::GraphSubscriptions`)

`push_subscribe(scopes)` groups scopes by Graph subscription
resource (`mailFolders/{folder}/messages`, `events`,
`contactFolders/{folder}/contacts`) and rejects the request if any
scope is not subscribable. It creates one server subscription per
resource, stores `(server_id, expires_at)` in a
`GraphSubscriptionGroup`, and emits `WatchEvent::Reconnected`.
`push_unsubscribe(handle)` deletes each subscription and aborts the
renewal worker when no groups remain. The worker wakes every 10 min,
renews inside the 30 min threshold, emits `Disconnected` on the first
retryable failure, `Reconnected` after recovery, and
`Terminated(AccountError)` for terminal auth/policy/permission
failures.

The webhook receiver is not in this crate: consumers mount an HTTPS
endpoint at `PushEndpoint::webhook_url`, validate `clientState`, and feed
invalidations into the engine `InvalidationSink`. The account `push_stream`
carries connection health only.

### EWS streaming mode (`PushMode::EwsStreaming`)

`push_subscribe` installs an `EwsSubscriptionState` (scopes plus the
latest subscription id / watermark) and starts the EWS worker: it
subscribes to the union of active folders, long-polls
`GetStreamingEvents`, records watermarks, maps notifications to cursor
scopes, and emits `WatchEvent::Invalidated`. Failures use
`ews_error_to_account_error`: terminal classes terminate; transient
ones emit `Disconnected`, sleep, reconnect, then `Reconnected`.

`push_stream` is a `broadcast::Receiver<WatchEvent>` adapter that
selects against shutdown. The EWS branch re-spawns its worker on
demand so lazy consumers still receive events.

## Mutation pipeline

`bulk_set_flags`, `bulk_move`, and `bulk_destroy` share
`bulk_mutation_stream`. Targets accumulate into chunks of
`batching_policy.max_items` (20), and each chunk is submitted via
`submit_batch`. Submission proceeds:

1. Snapshot the etag cache and refresh missing etags for `SetFlags` /
   `Move` via `GET /messages/{id}?$select=id` (failures -> per-id
   `Failed` with `Transport(_)`). `Destroy` needs no etag.
2. Build one `BatchRequestItem` per id: `PATCH` (`SetFlags`),
   `POST /messages/{id}/move` (`Move`), `DELETE` (`Destroy`).
   `If-Match: <changeKey>` is attached when available (mandatory for
   `SetFlags` / `Move`, opportunistic for `Destroy`).
3. Send `/$batch`; status drives `mutation_item_outcome` (see Error
   translation).

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
`remove_from_container` is unsupported. `set_is_read` patches `isRead`;
`set_category` patches `categories[]` (reserved `$flagged` / `flagged`
/ `starred` patch `flag.flagStatus`). `set_extended_property` patches
`singleValueExtendedProperties` when `Some`; the clear path (`None`)
batches `DELETE .../singleValueExtendedProperties/<prop-id>` and
tolerates 404. `PR_LAST_VERB_EXECUTED` aliases `Integer 0x1081`. These
writes send `If-Match` when `changeKey` exists.

`set_importance` patches the single-valued `importance` field
(`{ "importance": "low|normal|high" }`) in exactly one `If-Match`-
conditioned PATCH per message - one overwrite, never a clear-then-set
pair. The read side maps the same wire field back onto
`Message.importance` (`low`/`high` literal; absent or unrecognized ->
`Normal`).

Send / draft lifecycle is draft-backed so the trait can return an id:
`POST /messages` to create, `POST /messages/{id}/send` to send, return
the draft id. Inline attachments encode into Graph `fileAttachment`
JSON. Standalone `attachment_upload` is unsupported (Graph upload
sessions are message/draft scoped); large over-limit attachments use
`host_attachment` (`cloud.rs`) -> OneDrive. `draft_update` patches
mutable message fields; attachment replacement is unsupported.

Scheduled send is `PidTagDeferredSendTime` (`SystemTime 0x3FEF`): for
`SendRequest::scheduled = Some(t)` the boundary validates `t` (future, no
documented upper bound -> relies on server rejection) and PATCHes that
`singleValueExtendedProperty` (ISO-8601 UTC) onto the draft between create
and send. The draft id is the cancel/reschedule handle
(`cancel_scheduled_send` DELETEs, `reschedule_send` PATCHes in place).
`scheduled_send` is always true for Graph mailbox accounts.

Search uses `/messages` with `$filter` / `$search` / `$top` and
`@odata.nextLink` as the opaque page cursor. Message search returns
native ids; thread search dedups `conversationId` per result page.

Container CRUD maps to mail folders only. `containers_list` returns
native folder ids (`Provenance { Graph, Folder, native }`), refreshes
the folder tree, and maps well-known folders (`inbox`, `sentItems`,
`drafts`, `deletedItems`, `junkEmail`, `archive`); user folders stay
role-less. Create/rename/move/delete call `mailFolders`; root moves
target `msgfolderroot`.

Settings are narrow: `identities_list` returns the primary `me`
profile as the one default identity; `identity_update` is unsupported.
Vacation get/set maps to `mailboxSettings.automaticRepliesSetting`.
`quota_get` is unsupported.

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

`blob_handle_from_graph_attachment` sets `supports_range = true` only for
`fileAttachment` kinds (item / reference false); the handle carries no
digest (`digest_available_pre_download: false`).

## Error translation

`graph_error::into_account_error(error, ctx)` converts the
crate-internal `GraphError` into an `AccountError` via
`AccountErrorBuilder::try_build`. `GraphErrorContext { protocol,
operation, scope }` threads the calling operation so
`bifrost-types::recovery::derive` computes `RecoveryClass`; the crate
has no private recovery table. `GraphErrorContext::ews(op)` is the EWS
constructor; `ews_error_to_account_error` routes
`EwsError::Transport` through `bifrost_net::into_account_error`, maps
`EwsError::HttpStatus` through the same `response_to_account_error`
path REST uses, classifies `EwsError::SoapFault` onto `SoapFaultCode`
(Server -> Unavailable, Client/MustUnderstand/VersionMismatch -> 400,
Unknown -> ContractViolation), and routes `EwsError::MalformedXml` to
`Protocol(ParseFailed)`. EWS errors stamp `Protocol::Ews`.

Known Graph vocabulary lands on typed `WireCause::Graph(GraphSignal::*)`
variants (`InvalidAuthenticationToken`, `AccessDenied`, `Forbidden`,
`AccessRestricted`, `ConditionalAccessBlocked`, `AdminConsentRequired`,
`MailboxNotEnabledForRestApi`, `MailboxStoreUnavailable`,
`ResyncRequired`, `TooManyRequests`, `GenericFileError`,
`PreconditionFailed`, `NotFound`, `InvalidDeltaToken`,
`SyncStateNotFound`, `Gone`). `GraphSignal::Unknown { code }` is the
forward-compat fallback; string-matching unknown vocabulary is forbidden
(gate-5 invariant).

Mapping highlights:

- `Gone` / 410 / `InvalidDeltaToken` / `SyncStateNotFound` ->
  `SyncState(CursorInvalid)` -> `Engine(RestartScope(scope))`.
- `TooManyRequests` / 429 -> `Server(RateLimited)`, `throttle_scope:
  Tenant`, `retry_hint: After(_)` from `Retry-After` (seconds + HTTP-date
  via `bifrost_net::parse_retry_after`); 503 / 504 ->
  `Server(Unavailable)` -> `Retry::SameRequest`.
- `InvalidAuthenticationToken` / 401 ->
  `Authentication(ReauthorizationRequired)` -> `AuthLost`;
  `AdminConsentRequired` -> `Authorization(AdminConsentRequired)` ->
  `NeedsAdminConsent`.
- `ConditionalAccessBlocked` / `AccessRestricted` /
  `MailboxNotEnabledForRestApi` -> `Authorization(ConditionalAccessBlocked
  | PolicyBlocked | MailboxNotLicensed)` -> `NeedsPolicyChange`.
- `AccessDenied` / `Forbidden` -> `Authorization(PermissionDenied)` ->
  `NoPermission`; `MailboxStoreUnavailable` ->
  `Authorization(MailboxUnavailable { Transient })` ->
  `Retry::SameRequest`; `PreconditionFailed` / 412 ->
  `ConcurrencyConflict` -> `Retry::AfterStateRefresh`.

`mutation_item_outcome` projects per-id `$batch` responses onto
`ItemOutcome`: 2xx -> `Succeeded(Applied)`, 404-on-destroy ->
`Succeeded(Skipped)`, 412 -> `Failed(ConcurrencyConflict)` (engine
read-back reconciles), 429/other -> `Failed(BatchFailure { error })`
carrying `Protocol::Graph` + `AttemptCause(Acknowledged)` + the wire
signal. The same projector serves `mutate.rs` `bulk_*`,
`pim::submit_write_batch`, and `get_stream` hydration.

Cursor-decode failures (`CursorProtocolMismatch`,
`CursorEnvelopeUnknown`, `SchemaIncompatible`, malformed payload)
build an AccountError with `SyncState(SchemaIncompatible)`, which
the central mapping routes to `Engine(SchemaIncompatible)`.

Non-byte-stream attachments emit
`Warning { kind: WarningKind::BlobNotByteStream, .. }` rather
than a terminal error, so the engine can continue past a
referenceAttachment in a mixed batch.

## Known limitations

- Discovery is mail-only; event/contact cursors are engine-constructed.
- `scope_lifecycle_stream` is empty; folder creates / renames / deletes
  (including foreign mailboxes) are observed only at reopen. Foreign
  mailboxes are config-supplied (`with_shared_mailbox`); live delegate
  enumeration and shared-mailbox send-as are A5b / C-3.
- EWS streaming requires EWS reachable with a token it accepts; webhook
  mode requires a public HTTPS endpoint (else `push_subscribe` returns
  `Error::MissingCoreCapability`).
- Blob range is per-handle: fileAttachment `supports_range = true`,
  item/reference false. Delta-token expiry is reactive (410 / 400
  InvalidDeltaToken -> `Engine(RestartScope(scope))`).
- `MutationReplaySafety::None`. `IdempotencyKey` is accepted but not
  transmitted; the read-back guard is the only lost-update protection
  beyond the `If-Match` etag gate.
- `remove_from_container`, keyword writes, label membership, standalone
  `attachment_upload`, `identity_update`, and `quota_get` are
  unsupported (false in `pim_methods`).
- `send_message` / `draft_send` return the draft id (send answers
  `202 Accepted` with no body; the Sent Items id is rediscovered via
  sync or search). `draft_update` does not replace attachments: inline
  `fileAttachment` is accepted via `graph_attachment_from_inline`,
  pre-uploaded `AttachmentHandle`s rejected `Unsupported`.
- Graph inbox rules are conjunction-shaped: `And` -> conditions,
  `Not(...)` -> exceptions; `Or`, date ranges, provider expressions,
  remove-label, mark-unread, star/unstar, keyword, and reject actions
  are rejected by local validation.
