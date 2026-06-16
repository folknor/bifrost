# bifrost-graph reference

Current architecture of the Graph Account-layer code under
`crates/graph/src/account/`. Public surface:
`bifrost_graph::account::{GraphClient, GraphAccountFactory}` -
`GraphClient` carries credentials / endpoints into the factory, consumers
use the returned `Arc<dyn Account>`. Raw REST helpers and wire types are
crate-private.

The same `Account` impl owns Graph's Stage 1 PIM action surface plus the
Stage 3/4 contact and calendar primitives; unsupported gaps are advertised
through `pim_methods`.

Graph change-tracking is per-collection: message deltas per
`mailFolders/{id}/messages`, event deltas per `calendars/{id}`, contact
deltas per `contactFolders/{id}`. Cursors are scoped to
`CursorScope::FolderType { folder, ty }`, each wraps one `@odata.deltaLink`,
and `inventory_stream` / `changes_stream` walk the `@odata.nextLink` chain
to the final page. Public folders (opt-in) instead poll a watermark cursor
(no delta token). Mutations route through `POST /$batch` with `If-Match`.

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
- `public_folder.rs` - no-delta-token public-folder sync: the
  watermark + throttled deletion-scan poll, inventory/changes streams,
  `EwsItem` -> entry/change projectors, hierarchy discovery driver.
- `autodiscover.rs` - Exchange Autodiscover: `GetUserSettings`
  (public-folder routing, content-mailbox SMTP) and the tested-but-
  unwired `alternativeMailboxes` delegate parser, over `AccountNet`.

`crates/graph/src/ews/`: `client.rs`
(`EwsClient::execute(body, &EwsHeaders)`), `ops.rs` (`find_folder` /
`get_folder` / `find_items` / `get_item` + pure body builders),
`parse.rs` (quick-xml parsers returning `EwsError`, `decode_replica_list`),
`mod.rs` (`EwsError`, `SoapFaultCode`, `EwsHeaders` routing pair).
- `mutate.rs` - `bulk_set_flags` / `bulk_move` / `bulk_destroy` over
  `$batch` with `If-Match` and a `Retry-After`-aware throttle path.
- `pim.rs` - Stage 1 PIM primitives and Graph conveniences: message
  move/read/category/extended-property writes, send/drafts, search, mail
  folder CRUD, identity snapshot, automatic replies, typed hydration.
- `filters.rs` - Stage 2 Inbox `messageRules` typed-rule
  list/create/update/delete plus local validation.
- `blob.rs` - `open_blob` / `open_blob_range` over Graph
  attachments (`/messages/{id}/attachments/{aid}/$value`),
  including the reference-attachment short-circuit, plus
  `open_raw_rfc822` (whole message via `/messages/{id}/$value`).
- `cloud.rs` - `host_attachment`: OneDrive resumable upload +
  `createLink` in one call. Session/link POSTs go through
  `GraphClient::post` (conflict `rename`); the pre-authed chunk PUT uses
  the raw `account_net()` builder with `.without_bearer_auth()` and resumes
  on `202 Accepted`. `ShareScope`: `Anyone -> anonymous`, `Organization ->
  organization`.
- `error.rs` - blob-not-byte-stream warning helper. Classification
  helpers live in `graph_error.rs`.

Calendar/contact primitives live in `calendar.rs` and `contacts.rs`.
Graph calendar `color` is a provider token (not projected); reads request
`Prefer: outlook.timezone="UTC"`. Recurrence maps common daily/weekly/
monthly/yearly patterns to RRULE and back; unsupported outbound parts
reject serialization, unsupported inbound shapes are omitted, and
`relativeMonthly`/`relativeYearly` lacking BYMONTHDAY+BYDAY reject locally.
Outbound times map a conservative IANA -> Windows table (Windows names
pass through; unknown IANA ids reject pre-payload). `responseStatus` maps
to `self_response`; RSVP uses native `accept`/`decline`/`tentativelyAccept`.
Calendar/contact update/delete fetch-then-`If-Match` and send sparse PATCH
(scalar clears as null; contacts emit `null`/`[]` for emptied buckets).
Event organizer/status are server-derived. Event search uses the Graph
Search API for unscoped non-empty default-mailbox searches, else local;
composite `EventId`s embed the calendar (`{calendar}::{event}`), Search
hits use the `$mailbox` sentinel routed through `/me/events/{id}`. Contact
search uses exact email `$filter` for email-shaped queries, else local.

## `GraphAccount` / `GraphAccountFactory` shape and lifecycle

The `account` module path is public (the conformance test and consumers
construct the factory through it); helper modules and `GraphAccount` stay
crate-private. `GraphClient` is public only as factory input. `new` /
`with_api_base*` take a raw token; `with_source` / `with_account_net` take
a shared `Arc<dyn TokenSource>` that `attach_account` hands to bifrost-net.

`GraphAccountFactory` carries a `GraphClient`, a `PushMode`, an optional
`PushEndpoint`, a `shared_mailboxes: Vec<String>`, and a `public_folders`
flag. `with_push_endpoint(url)` selects `PushMode::GraphSubscriptions`;
`with_ews_streaming()` selects `PushMode::EwsStreaming` and clears the
endpoint; default webhook-mode without an endpoint makes `push_subscribe`
return `Error::MissingCoreCapability`. `with_shared_mailbox(id)` registers
a delegate/shared mailbox by its `/users/{id}` routing key.
`with_public_folders()` opts in to public-folder discovery/sync (default
off, so no existing account pays the Autodiscover round-trips).

`AccountFactory::open(account_id)` attaches the `GraphClient` to
`bifrost-net` under the engine `AccountId`, validates the token with a
`users/me` profile fetch (`get_profile`, whose `mail`/`userPrincipalName`
seeds the public-folder Autodiscover lookups), constructs a
`GraphAccount`, and runs `list_mail_folders_recursive` to seed the
`FolderTree`. Cursors
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
  change keys (powering `If-Match`), and a `routing_map:
  Arc<RwLock<HashMap<FolderId, PublicFolderRouting>>>` seeded by
  public-folder discovery (the discriminator for public-folder dispatch),
  plus `public_folders_enabled` and the discovered `user_email`.
- `set_priority` / `set_bandwidth_cap` delegate to `AccountNet`.

Reopen is engine-delegated: on drop or after `close()`, the engine calls
`GraphAccountFactory::open` again for a fresh `GraphAccount` (empty caches,
fresh shutdown token); the factory holds the client, so the new account
reads the token source's current value. `close()` cancels the shutdown
token and aborts the EWS worker; `push_stream` wraps the broadcast receiver
in a `stream::unfold` selecting against the same token.

## Capabilities

`build_capabilities(push_mode)` in `capabilities.rs`:

- `cursor_freshness: ServerIssued`. Graph mints `@odata.deltaLink`
  server-side; the engine persists and resumes it.
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
- `pim_methods`: true for the container/category/extended-property/
  importance/is-read writes, send/draft lifecycle, `scheduled_send`,
  search, mail folder CRUD, `identities_list`, vacation, typed hydration,
  contact/calendar primitives, and `host_attachment`. False for
  `remove_from_container`, `set_keyword`, `set_label_membership`,
  standalone `attachment_upload`, `identity_update`, `quota_get`.
- `filter_rule_shape: Rules`; all five filter flags true (Graph Inbox
  `messageRules` CRUD + local `filter_validate`).
- `conveniences`: `starred = Category` (reserved `$flagged` ->
  `flag.flagStatus`); replied/forwarded dispatch to `set_extended_property`
  with `PidTagLastVerbExecuted` 102/104; `mdn_sent_via_keyword = false`
  (Graph `isReadReceiptRequested` read-only -> `Unsupported(UpdateFlags)`).

## Cursor envelope

`OpaqueChangeState` for Graph is tagged `ProtocolKind::Graph` with
`envelope_version = GRAPH_CURSOR_ENVELOPE_VERSION` (currently `1`);
`CHANGE_CURSOR_ENVELOPE_VERSION` is the matching
`ChangeCursor.envelope_version`. `GraphCursorPayload` carries a kind, final
`@odata.deltaLink`, issue timestamp, and optional mid-walk page marker
(landing in `OpaqueChangeState::bytes`; page markers also in
`ChangeCursor::advanced_through`).

`decode_cursor` rejects wrong protocol, incompatible envelopes, and
malformed JSON. `changes_stream` cross-checks that payload kind projects
back to `cursor.scope`; mismatches terminate with
`SyncState(SchemaIncompatible)`. `establish_initial_cursor` accepts
delta-eligible `FolderType` scopes (email, event/calendar event, contact)
plus any `CursorScope::Folder` present in the public-folder routing map;
both mint the first cursor through inventory. A delta `describe_cursor` is
cheap/`ServerCursor`/fresh; a public-folder cursor is cheap/`Poll`/fresh;
invalid cursors reseed through inventory.

### Public-folder cursor (no delta token)

`GraphCursorKind::PublicFolder(PublicFolderCursor)` is additive, no
version bump (a v1 reader never wrote it; safe because bifrost is one
linked library per consumer build). The payload IS the sync state:
`folder_id`, `PublicFolderRouting` (the cold-resumable `X-AnchorMailbox` /
`X-PublicFolderMailbox` pair), a `DateTimeReceived` `watermark`, the
`last_full_scan_at` throttle clock, and a `live_ids` deletion baseline.
`public_folder_changes_stream`: incremental `find_items` since the
watermark (emit `Updated` + `Folder` and `Mailbox(content)` `Added` scope
changes, matching the inventory pass's dual membership), then a deletion
reconcile throttled to `FULL_SCAN_INTERVAL_SECS` (3600) - a full IdOnly
scan diffed against `live_ids` emits `Destroyed` for vanished ids.
`live_ids` is hard-capped at `PUBLIC_FOLDER_LIVE_IDS_CAP` (10_000); above
it the snapshot empties and the folder degrades to additions-only with one
scoped `Warning`. Dispatch: routing-map membership for `Folder` scopes
(establish/inventory), cursor kind for changes; a bare non-public `Folder`
keeps its reject-on-delta behavior. `push_subscribe` is
`Unsupported(PushSubscribe)` for any `Folder` scope (poll-only v1). Lost
rights surface as EWS `ErrorAccessDenied` and quarantine just that scope
via `ews_shared_scope_error` -> `ScopeRevoked` -> `DisableScope`
(owner = content mailbox).

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
- `Folder(folder)` in the public-folder routing map -> the
  `public_folder.rs` poll strategy (see "Public-folder cursor"); a
  `Folder` absent from the map stays `Unsupported`.
- Any other scope / type makes `initial_delta_url` return an error that
  `inventory_stream` projects into `Terminated(Unsupported(_))`;
  `establish_initial_cursor` gates scopes before that point.

The inventory walk reads pages via `get_json` / `get_absolute` (the
`@odata.nextLink` chain), yielding `PageBoundary::Page` Batches until the
page returns a `delta_link`, then a `Final` Batch + `Checkpoint::Change`.
`@removed` entries are skipped; harvested etags fold into
`account.etag_index` for the next `If-Match`.

`changes_stream(cursor)` decodes the payload, asserts
`scope_matches_payload`, and walks `delta_link` (or
`advanced_through.next_link` on resume). Each non-removed entry emits
`ObjectChange { Updated }` + `ScopeChange { Added }` (Graph delta has no
created/updated split); `@removed` emits one `ScopeChange { Removed }`.
Page batches checkpoint with `advanced_through` at the next link; the final
page checkpoints the fresh `delta_link`, `advanced_through` cleared.

`get_stream` chunks ids into `batching_policy.max_items` blocks, fires
one `/$batch` per chunk, and returns per-id `ItemOutcome<HydratedObject>`:
2xx -> `Succeeded`, 4xx/5xx -> `Failed` with a structured `AccountError`
(via `response_to_account_error_pub`), 2xx-no-body ->
`Failed(Protocol(MissingField))`. Locally-invalid items `Failed` without
poisoning the batch; a whole-request transport drop terminates. The
`hydrated_from_value` projector maps `FlagsOnly` -> canonical flag
`HashSet`; `Metadata`/body-bearing projections -> `metadata_or_flags`.
Graph JSON is not assembled RFC822, so body-bearing projections degrade to
`Metadata` (stopgap until A1; assembled bytes come from `open_raw_rfc822`).

`scope_lifecycle_stream` is empty: Graph exposes no folder-lifecycle
notification surface; discovery re-runs on account reopen.

## Foreign (shared/delegate) mailboxes

A configured shared mailbox surfaces as ordinary
`CursorScope::FolderType { folder, Email }` scopes; the owning mailbox
identity rides inside the `FolderId` string via the `account/foreign.rs`
codec (`encode_foreign(mailbox, folder)` joins on `\u{1f}`,
`parse_folder` splits it back into `ParsedFolder::{Primary, Foreign}`).
No new `CursorScope` variant. `client_for_scope(scope)` selects the
`shared_clients` entry when the folder parses foreign, else the primary
client; `owner_of_scope(scope)` returns the `MailboxId` owner tag for a
foreign scope, `None` for primary. `discover_cursor_scopes_inner` lists
the primary folders then each shared mailbox's folders via its client,
emitting foreign-namespaced `FolderType` scopes; a per-mailbox permission
denial skips with a scoped `Warning`. `discover_memberships_inner` emits
the foreign `MembershipScope::Mailbox(owner)` owner tag alongside the
folder membership, and `inventory_stream` stamps it onto every
foreign-scope item (the engine covering rule cannot form it; folder-id and
mailbox-id strings differ). `initial_delta_url` reads the prefix from
`client_for_scope` and the native id from `parse_folder`, so the mailbox
rides in `/users/{id}` and the native id in `/mailFolders/{id}`.

Revocation isolation: `graph_shared_scope_error(error, scope, owner, ctx)`
quarantines just the foreign scope when the failure is
`Authorization(PermissionDenied)` and `owner.is_some()` ->
`graph_scope_revoked` -> `ScopeRevoked` -> `Engine(DisableScope(scope))`. A
primary scope (`owner == None`) stays terminal `NoPermission`. Wired at the
`inventory_stream` / `changes_stream` per-scope fetch-failure boundary.

Foreign-mailbox enumeration is config-supplied (`with_shared_mailbox`):
Graph REST has no "list my delegated mailboxes" call. The Autodiscover
`alternativeMailboxes` parser + entry point land in `autodiscover.rs`
(tested) but unwired - delegate *enumeration* into the foreign seeding is a
named follow-up (`TODO.md`). The EWS twin `ews_shared_scope_error` applies
the same `ScopeRevoked` -> `DisableScope` isolation to public-folder scopes
(owner = content mailbox).

## Public-folder discovery (Autodiscover)

Opt-in via `with_public_folders()`. After the primary/shared mailboxes,
`discover_cursor_scopes_inner` calls
`public_folder::discover_public_folder_scopes`: resolve hierarchy routing
via `GetUserSettings` (`PublicFolderInformation` /
`InternalRpcClientServer`), browse the hierarchy recursively from
`find_folder("publicfoldersroot")` with hierarchy headers (each folder with
`child_folder_count > 0` re-enters the worklist; a `visited` dedup set plus a
browse-step cap bound the walk), read-gate via `effective_rights.read`
(`readable_folders`), and per folder resolve the content mailbox
(`get_folder` PR_REPLICA_LIST GUID -> `construct_replica_smtp` ->
`discover_content_mailbox`), seeding `routing_map` and emitting a
`CursorScope::Folder`. Per-folder failures skip with a scoped `Warning`; a
missing hierarchy skips the whole leg. Rights are advisory at discovery
only (`EwsEffectiveRights`, `pub(crate)`, no shared rights type per the A5c
precedent); the authoritative gate is the live `ErrorAccessDenied`.

## Push: webhooks plus EWS streaming fallback

Push has two modes, selected on the factory and reflected in the
capability surface.

### Webhook mode (`PushMode::GraphSubscriptions`)

`push_subscribe(scopes)` groups scopes by Graph subscription resource and
rejects the request if any scope is not subscribable, creates one server
subscription per resource, stores `(server_id, expires_at)` in a
`GraphSubscriptionGroup`, and emits `Reconnected`. `push_unsubscribe`
deletes each subscription and aborts the renewal worker when no groups
remain. The worker wakes every 10 min, renews inside the 30 min threshold,
and emits `Disconnected`/`Reconnected`/`Terminated` accordingly. The
webhook receiver is not in this crate: consumers mount an HTTPS endpoint at
`PushEndpoint::webhook_url`, validate `clientState`, and feed invalidations
into the engine `InvalidationSink`; `push_stream` carries health only.

### EWS streaming mode (`PushMode::EwsStreaming`)

`push_subscribe` installs an `EwsSubscriptionState` and starts the EWS
worker: it subscribes to the union of active folders, long-polls
`GetStreamingEvents`, records watermarks, maps notifications to cursor
scopes, and emits `Invalidated`. Failures use `ews_error_to_account_error`
(terminal terminates; transient emits `Disconnected`, sleeps, reconnects).
`push_subscribe` rejects any `Folder` (public-folder) scope as
`Unsupported(PushSubscribe)` in both modes. `push_stream` is a
`broadcast::Receiver<WatchEvent>` adapter that selects against shutdown;
the EWS branch re-spawns its worker on demand.

## Mutation pipeline

`bulk_set_flags`, `bulk_move`, `bulk_destroy` share
`bulk_mutation_stream`: chunk to `batching_policy.max_items` (20), then
`submit_batch` (1) snapshots the etag cache and refreshes missing
`SetFlags`/`Move` etags via `GET /messages/{id}?$select=id` (`Destroy`
needs none), (2) builds one `BatchRequestItem` per id - `PATCH` /
`POST .../move` / `DELETE` - attaching `If-Match: <changeKey>` (mandatory
for `SetFlags`/`Move`, opportunistic for `Destroy`), (3) sends `/$batch`,
status driving `mutation_item_outcome`.

`bulk_set_flags` translates `FlagOp` to a PATCH body (`isRead`,
`flag.flagStatus`, sorted `categories`); unrecognized flags ignored; `Set`
rewrites the `categories` array, `Add`/`Remove`/`Patch` touch named fields.
`bulk_move` requires `MembershipScope::Folder` (else fatal pre-request).
`IdempotencyKey` is accepted but not sent (`MutationReplaySafety::None`).

## PIM primitives

Mail mutation primitives live in `pim.rs`, per-message, fanning out a
`MutationTarget::Thread` via `/messages?$filter=conversationId eq ...`.
`add_to_container` is `POST /messages/{id}/move` (no symmetric remove, so
`remove_from_container` unsupported). `set_is_read` patches `isRead`;
`set_category` patches `categories[]` (reserved `$flagged`/`starred` ->
`flag.flagStatus`). `set_extended_property` patches
`singleValueExtendedProperties` when `Some`, else `DELETE`s it (404-tolerant)
(`PR_LAST_VERB_EXECUTED` = `Integer 0x1081`). All send `If-Match` when
`changeKey` exists.

`set_importance` patches the single-valued `importance` field in one
`If-Match`-conditioned PATCH (never clear-then-set); the read side maps
it back onto `Message.importance` (absent/unrecognized -> `Normal`).

Send / draft lifecycle is draft-backed so the trait returns an id: `POST
/messages` create, `POST /messages/{id}/send` send, return the draft id.
Inline attachments encode into `fileAttachment` JSON; standalone
`attachment_upload` is unsupported (over-limit -> `host_attachment` ->
OneDrive); `draft_update` patches mutable fields (no attachment replace).
Scheduled send PATCHes `PidTagDeferredSendTime` (`SystemTime 0x3FEF`,
ISO-8601 UTC) onto the draft between create and send for a future
`scheduled`; the draft id is the cancel/reschedule handle.

Search uses `/messages` (`$filter`/`$search`/`$top`, `@odata.nextLink` as
the opaque page cursor); message search returns native ids, thread search
dedups `conversationId` per page.

Container CRUD maps to mail folders only. `containers_list` returns
native folder ids (`Provenance { Graph, Folder, native }`), refreshes the
folder tree, and maps well-known folders; user folders stay role-less.
Create/rename/move/delete call `mailFolders`; root moves target
`msgfolderroot`.

Settings are narrow: `identities_list` returns the primary `me` profile as
the default identity (`identity_update` unsupported); vacation get/set maps
to `mailboxSettings.automaticRepliesSetting`; `quota_get` unsupported.

Typed hydration is separate from `get_stream`: `message_hydrate` fetches one
message at the requested projection into `Message`; `thread_hydrate` queries
the conversation, sorted by date.

`move_thread` calls Graph move directly. `delete_thread` resolves Trash via
`deletedItems` (Trash source destroys, else moves to Trash).
`apply_label`/`remove_label` inherit the trait default (Graph provenance ->
`set_category`, `(Folder, non-Graph)` ->
`add_to_container`/`remove_from_container`, else `Unsupported`).

## Blobs

`open_blob` decodes `BlobHandle::id` (a JSON
`GraphBlobLocator { message_id, attachment_id, kind }`), short-circuits
with a `BlobNotByteStream` warning for `Reference` kinds, fetches
`/messages/{mid}/attachments/{aid}/$value`, and emits each `bytes_stream`
chunk as a Batch. `open_blob_range` adds a `Range` header; a 200 (not 206)
to a Range request is fatal, a 405 surfaces `BlobNotByteStream` for
fallback. `blob_handle_from_graph_attachment` sets `supports_range` only
for `fileAttachment` (item/reference false); no pre-download digest.

## Error translation

`graph_error::into_account_error(error, ctx)` converts the crate-internal
`GraphError` into an `AccountError` via `try_build`. `GraphErrorContext
{ protocol, operation, scope }` threads the operation so
`recovery::derive` computes `RecoveryClass` (no private recovery table).
`GraphErrorContext::ews(op)` is the EWS constructor;
`ews_error_to_account_error` routes
`EwsError::Transport` through `bifrost_net::into_account_error`, maps
`EwsError::HttpStatus` through the same `response_to_account_error`
path REST uses, classifies `EwsError::SoapFault` onto `SoapFaultCode`
(Server -> Unavailable, Client/MustUnderstand/VersionMismatch -> 400,
Unknown -> ContractViolation), and routes `EwsError::MalformedXml` to
`Protocol(ParseFailed)`. EWS errors stamp `Protocol::Ews`.

Known Graph vocabulary lands on typed `WireCause::Graph(GraphSignal::*)`
variants (auth/access/throttle/cursor codes; see `classify`).
`GraphSignal::Unknown { code }` is the forward-compat fallback;
string-matching unknown vocabulary is forbidden (gate-5 invariant).
`ews_shared_scope_error` is the EWS twin of `graph_shared_scope_error`:
an `ErrorAccessDenied` on an owned (public-folder) scope quarantines via
`ScopeRevoked` rather than escalating account-wide.

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
  `Authorization(MailboxUnavailable { Transient })` -> `Retry::SameRequest`;
  `PreconditionFailed` / 412 -> `ConcurrencyConflict` ->
  `Retry::AfterStateRefresh`.

`mutation_item_outcome` projects per-id `$batch` responses onto
`ItemOutcome`: 2xx -> `Succeeded(Applied)`, 404-on-destroy ->
`Succeeded(Skipped)`, 412 -> `Failed(ConcurrencyConflict)`, 429/other ->
`Failed` carrying `Protocol::Graph` + `AttemptCause(Acknowledged)` + wire
signal. Shared by `mutate.rs` `bulk_*`, `pim::submit_write_batch`,
`get_stream`.

Cursor-decode failures (`CursorProtocolMismatch`,
`CursorEnvelopeUnknown`, `SchemaIncompatible`, malformed payload)
build an AccountError with `SyncState(SchemaIncompatible)`, which
the central mapping routes to `Engine(SchemaIncompatible)`.

Non-byte-stream attachments emit
`Warning { kind: WarningKind::BlobNotByteStream, .. }` rather
than a terminal error, so the engine can continue past a
referenceAttachment in a mixed batch.

## Known limitations

- Discovery is mail-only (+ opt-in public folders); event/contact cursors
  are engine-constructed.
- `scope_lifecycle_stream` is empty; folder creates/renames/deletes are
  observed only at reopen. Foreign mailboxes are config-supplied
  (`with_shared_mailbox`); live delegate enumeration (Autodiscover parser
  landed, unwired) and shared-mailbox send-as are follow-ups / C-3.
- Public folders are poll-only (no push) and side-table-free: the deletion
  baseline rides in the cursor, capped at 10_000 items/folder (above it:
  additions-only). A `CheckpointStore`-backed baseline, and item-class
  support beyond `<t:Message>` (so `IPF.Appointment`/`IPF.Contact` folders
  sync items, not just surface as scopes), are named follow-ups.
- EWS streaming requires EWS reachable with a token it accepts; webhook
  mode requires a public HTTPS endpoint (else `push_subscribe` returns
  `Error::MissingCoreCapability`).
- Blob range is per-handle: fileAttachment `supports_range = true`,
  item/reference false. Delta-token expiry is reactive (410 / 400
  InvalidDeltaToken -> `Engine(RestartScope(scope))`).
- `MutationReplaySafety::None`: `IdempotencyKey` accepted but not sent; the
  read-back guard is the only lost-update net beyond `If-Match`.
- `remove_from_container`, keyword/label-membership writes, standalone
  `attachment_upload`, `identity_update`, `quota_get` unsupported.
- `send_message`/`draft_send` return the draft id (send is `202` with no
  body; Sent Items id rediscovered via sync/search). `draft_update` accepts
  inline `fileAttachment` but rejects pre-uploaded handles.
- Inbox rules are conjunction-shaped (`And` -> conditions, `Not` ->
  exceptions); `Or`, date ranges, remove-label, mark-unread, star, keyword,
  reject actions are rejected by local validation.
