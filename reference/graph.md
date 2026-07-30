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
- `batch_routing.rs` - `partition_routable`, the pure per-item split every
  `$batch` chunk builder runs before it sends: ids whose subrequest was
  built, and ids whose subrequest could not be built at all (a stale
  shared-mailbox owner). The rejected half goes to the caller's failed lane
  and the chunk proceeds; an empty routable half skips the POST. Generic
  over the per-site URL builder, so the lane rule is pinned once while each
  site keeps its own classification.
- `push.rs` - `/subscriptions` webhook subscribe/unsubscribe, per-handle
  `GraphSubscriptionGroup`, the renewal health worker that re-issues expiring
  subscriptions and emits Disconnected/Reconnected on renewal failure, plus
  the EWS arm's `restId` -> `ewsId` translation
  (`translation_input_chunks` / `reconcile_translated_ews_scopes`).
- `push_stream.rs` - the broadcast-backed `push_stream` adapter, selecting the
  receiver against the shutdown token and spawning the EWS worker on demand.
- `ews_stream.rs` - EWS Streaming Notifications fallback:
  Subscribe / GetStreamingEvents / Unsubscribe XML, scope
  recovery, the long-lived worker loop - generic over the crate-private
  `EwsExecute` transport seam (`EwsClient` in production, a scripted
  double in tests). At `push_subscribe` (in `push.rs`),
  Graph REST folder `restId`s are translated once through
  `/me/translateExchangeIds` - deduplicated and chunked to Graph's 1,000-id
  request cap - to `ewsId`s and retained beside their `CursorScope` in
  subscription state: the SOAP Subscribe body uses those EWS ids and
  notification routing maps the returned EWS parent id back to every original
  scope without a second request. The worker deduplicates equal `ewsId`s for
  the Subscribe body while preserving every DISTINCT matching scope for
  invalidation.
- `public_folder.rs` - no-delta-token public-folder sync: the
  watermark + throttled deletion-scan poll, inventory/changes streams,
  `EwsItem` -> entry/change projectors, hierarchy discovery driver.
- `autodiscover.rs` - Exchange Autodiscover: `GetUserSettings`
  (public-folder routing, content-mailbox SMTP) and the
  `alternativeMailboxes` delegate parser, wired into `open` via
  `with_delegate_discovery()`, over `AccountNet`.

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
- `cloud.rs` - `host_attachment`: OneDrive resumable upload + `createLink` in one
  call. Session/link POSTs go through `GraphClient::post` (conflict `rename`); the
  pre-authed chunk PUT uses the raw `account_net()` builder with
  `.without_bearer_auth()` and resumes on `202 Accepted`. `ShareScope`: `Anyone ->
  anonymous`, `Organization -> organization`.
- `error.rs` - blob-not-byte-stream warning helper. Classification
  helpers live in `graph_error.rs`.

`GraphClient` owns the REST test seam. Every REST helper - raw MIME import
included - funnels through one private `execute_wire`, which adapts the
production `bifrost_net::Response` into a Graph-local wire response; test
builds enqueue scripted status, headers, and bodies there and inspect the
recorded method, URL, conditional/prefer headers, content type, and JSON or
raw body. This keeps Graph account tests hermetic without exposing or
changing bifrost-net's private dispatch seam. The production path is
unchanged: it still goes through `AccountNet` for bearer auth, retry, rate
limits, and metering. JSON bodies are serialized by the funnel rather than by
`RequestBuilder::json` (identical bytes and content type, plus the same
`EncodeBody` failure) so the funnel is non-generic and the one non-JSON body
can share it; `post_mime` previously built its own request on `AccountNet` and
was the one wire path the seam could not see.

Two rules make the seam an equivalence rather than an approximation:

- A scripted status takes the shape `bifrost-net` would have produced.
  Its retry loop returns `Ok(Response)` for 2xx and a passed-through 3xx
  ONLY: a terminal 4xx becomes `Error::Status`, a 401 surviving the forced
  refresh `Error::AuthLost`, and the retryable set (429 plus 5xx) becomes
  `Error::RateLimited` / `Error::RetryBudgetExhausted` once the budget is
  gone. The retry set is read off `RetryPolicy::default()` - the policy
  `attach_account` installs - not restated. Backoff, attempt count, and the
  concurrency permit are not simulated; they change how long production
  takes to reach an outcome, not which outcome it reaches.
- An ARMED script that runs out panics at the offending request. Falling
  through to `AccountNet` would let an unexpected request reach the network
  and would leave it out of the recorded list, so a test asserting "exactly
  N requests" could not detect the N+1th. Mirrors the EWS double.

Still outside the funnel, and so still unseamed: blob byte streams
(`download_stream`), the OneDrive resumable chunk PUT (pre-authed, no bearer),
and the Autodiscover POST. EWS has its own `EwsExecute` seam.

Calendar/contact primitives live in `calendar.rs` and `contacts.rs`. Graph
calendar `color` is a provider token (not projected); reads request `Prefer:
outlook.timezone="UTC"`. Recurrence maps common daily/weekly/monthly/yearly
patterns to RRULE and back; unsupported outbound parts reject serialization,
unsupported inbound shapes are omitted, and `relativeMonthly`/`relativeYearly`
lacking BYMONTHDAY+BYDAY reject locally. Outbound times map a conservative IANA
-> Windows table (unknown ids reject pre-payload). `responseStatus` maps to
`self_response`; RSVP uses native `accept`/`decline`/`tentativelyAccept`.
Calendar/contact update/delete fetch-then-`If-Match` and send sparse PATCH
(scalar clears as null; contacts emit `null`/`[]` for emptied buckets). Event
organizer/status are server-derived. Event search uses the Graph Search API for
unscoped non-empty default-mailbox searches, else local; composite `EventId`s
embed the calendar (`{calendar}::{event}`), Search hits use the `$mailbox`
sentinel routed through `/me/events/{id}`. Contact search uses exact email
`$filter` for email-shaped queries, else local.

## `GraphAccount` / `GraphAccountFactory` shape and lifecycle

The `account` module path is public (consumers construct the factory
through it); helper modules and `GraphAccount` stay crate-private.
`GraphClient` is public only as factory input. `new` / `with_api_base` take a
raw token; `with_source` / `with_account_net` take a shared
`Arc<dyn TokenSource>` that `attach_account` hands to bifrost-net. There is
one api-base: the client speaks `v1.0` only, and a beta endpoint is not
configurable because no call site reads one.

`attach_account` is called again on every reopen with the same engine
id. When the client owns a parent `Net` it mints a fresh `AccountNet`,
installs it, and then calls `detach()` on the handle it displaced.
`Net::attach_account` issues a distinct registration token per call and
does not unregister a previous attachment for the same id, so without
that explicit teardown each reopen would leak one meter attachment and
one governor attach count and the host bucket would never be reclaimed.
Detaching after the install keeps the shared counts from reaching zero,
so requests still in flight on the displaced handle continue to meter
against the same counters. When the client was built with
`with_account_net` there is no parent `Net` and the existing handle is
retagged instead, which moves its token rather than minting a new one.

`GraphAccountFactory` carries a `GraphClient`, a `PushMode`, an optional
`PushEndpoint`, a `shared_mailboxes: Vec<String>`, and
`public_folders: Option<PublicFolderScope>`.
`with_push_endpoint(url)` selects `GraphSubscriptions` with the historical
random `clientState` fallback. New webhook consumers should use
`with_push_endpoint_client_state(url, secret)`: it reuses the caller-owned
account-wide secret for every resource so the out-of-process receiver can
validate Graph notifications. `with_ews_streaming()` selects `EwsStreaming`
and clears the endpoint; default webhook-mode without an endpoint makes
`push_subscribe` return `Error::MissingCoreCapability`.
`with_shared_mailbox(id)` registers a delegate/shared mailbox by its
`/users/{id}` routing key. `with_public_folders(scope)` opts in to
public-folder discovery (default off) and says which folders may SYNC:
`PublicFolderScope::hierarchy_only()` projects the hierarchy and syncs
nothing, `PublicFolderScope::pinned(ids)` syncs exactly those folders. The
argument is required, not defaulted - an org can carry thousands of public
folders holding millions of items, so "sync everything discovered" is a
defect, not a convenience.

`GraphClient` also derives an Autodiscover/EWS origin (`outlook_base`) from
its Graph api-base: a base on `graph.microsoft.com` keeps the production
`https://outlook.office365.com`, any other host (a harness mock, a sovereign
cloud) becomes the origin for `/autodiscover/autodiscover.{xml,svc}` and
`/EWS/Exchange.asmx` too. `with_outlook_base(base)` overrides it explicitly.
Without this, redirecting the Graph api-base left Autodiscover and EWS
pointed at the real service, so the public-folder and EWS-streaming legs
could not be exercised against a mock at all.

`AccountFactory::open(account_id)` attaches the `GraphClient` to `bifrost-net`
under the engine `AccountId`, validates the token with a `users/me` profile
fetch (`get_profile`, whose `mail`/`userPrincipalName` seeds the public-folder
Autodiscover lookups), constructs a `GraphAccount`, and runs
`list_mail_folders_recursive` to seed the `FolderTree`. Cursors mint lazily from
`establish_initial_cursor` plus the first `inventory_stream` page. Returns
`Arc<dyn Account>`.

`GraphAccount` owns:

- The shared primary `GraphClient` plus `shared_clients: Arc<HashMap<String,
  GraphClient>>`, one `for_shared_mailbox(id)` client per foreign mailbox.
  Every foreign operation, including initial inventory and delta-link resume,
  selects that owner client before it builds or follows a URL; continuations
  retain the selected client even though Graph's `nextLink` and `deltaLink`
  are absolute. Foreign folder and object ids are routed only through an owner
  present in that map, and an EMPTY routing key never enters it (`with_shared_mailbox("")`
  is constructible and used to install a client whose prefix was the
  malformed `/users/`, turning a local configuration error into an opaque
  remote 400; `merge_shared_mailboxes` already applied the same rule on the
  Autodiscover leg). A persisted id whose owner was removed from
  configuration is rejected locally rather than stripped and sent to `/me`,
  which would address a different mailbox namespace. Where that rejection
  lands depends on the surface's answer shape:
  - **Both cursor doors**, per scope: `initial_delta_url` refuses before the
    first delta request and `changes_stream` refuses before resuming a
    persisted `@odata.deltaLink`. The second is not redundant - the delta
    link Graph minted is namespace-correct, so that walk keeps SUCCEEDING
    for a mailbox this account no longer configures, leaving a live scope
    whose every object id fails hydration and mutation. Both classify
    `SyncState(ScopeRevoked)` carrying `ErrorScope::Cursor`, so the engine
    disables exactly that scope.
  - **Per-item batch surfaces** (`get_stream` hydration, `message_reactions`,
    the `bulk_*` mutation funnel), per id: the id is filed
    `Failed(Request(Malformed))` on its own lane through
    `batch_routing::partition_routable` and the rest of the chunk goes to
    the wire. A top-level `Err` there would discard valid siblings, and on
    `message_reactions` would additionally claim nothing was transmitted
    while earlier chunks had already completed.
  - **Per-request surfaces** (`push_subscribe`, the single-message `pim`
    writes, blobs), per call: the call fails, carrying the scope of the
    offending id.
- The built `AccountCapabilities`; the push mode plus optional endpoint; a
  `broadcast::Sender<WatchEvent>` feeding `push_stream`.
- An `Arc<RwLock<CursorIndex>>` (scope list) and `Arc<RwLock<FolderTree>>`
  (parent map).
- `HashMap<SubscriptionHandle, GraphSubscriptionGroup>` (webhook) +
  `HashMap<SubscriptionHandle, EwsSubscriptionState>` (EWS) + worker join slots
  and a `CancellationToken`.
- A bounded LRU `etag_index` of change keys (powering `If-Match`; mutation
  preflight reads refresh recency, `MESSAGE_SELECT` explicitly requests
  `changeKey`, and tombstones and confirmed destroys evict their keys). It is
  a `HashMap<Arc<str>, _>` paired with a `BTreeMap<u64, Arc<str>>` of recency
  tickets, so insert / hit / evict are each one hash lookup plus a couple of
  tree nodes and never a scan: the write lock is taken once per hydrated page
  and once per mutation preflight, where a scanning policy would cost
  `messages * capacity` comparisons serialized behind it. Alongside it a
  `routing_map: Arc<RwLock<HashMap<FolderId,
  PublicFolderRouting>>>` from public-folder discovery, plus
  `public_folders_enabled` and the discovered `user_email`.
- `set_priority` / `set_bandwidth_cap` delegate to `AccountNet`.

Reopen is engine-delegated: on drop or after `close()`, the engine calls
`GraphAccountFactory::open` again for a fresh `GraphAccount` (empty caches, fresh
shutdown token); the factory holds the client, so the new account reads the
token source's current value. `close()` cancels the shutdown token and aborts
the EWS worker; `push_stream` wraps the broadcast receiver in a `stream::unfold`
selecting against the same token.

## Capabilities

`build_capabilities(push_mode)` in `capabilities.rs`:

- `cursor_freshness: ServerIssued`. Graph mints `@odata.deltaLink`; the engine
  persists and resumes it.
- `blob_range: Conditional` (per-handle: fileAttachments support `Range` against
  `/$value`, item/referenceAttachments do not, carried by
  `BlobHandle::capabilities::supports_range`); `blob_digest_pre_download: false`.
- `push` depends on `PushMode`: `GraphSubscriptions` -> `WebhookOrEwsStream`
  (out-of-process: subscription CRUD on the Account, HTTPS receiver wired by the
  consumer into the engine `InvalidationSink`; `push_in_process()` false);
  `EwsStreaming` -> `InProcess` (the EWS worker forwards `Invalidated` on
  `push_stream`; `push_in_process()` true).
- `mutation.concurrency: StateBased`. Every non-`Destroy` mutation sends
  `If-Match: <changeKey>`; the cached etag comes from inventory/changes/get,
  refreshed from `messages/{id}?$select=id` on a cold cache.
- `mutation.replay_safety: None`. No client-mintable replay token; the read-back
  guard is the lost-update net.
- `batching_policy: { max_items: 20, max_wait: 100ms, flush_on_input_close: true }`
  (the 20 ceiling matches Graph's `/$batch` limit).
- `rate_limit_class: Tiered` (per-mailbox concurrency + per-app throttle budget);
  `quota_signal: RetryAfter` (`Retry-After` becomes `Retry`'s `not_before`).
- `requires_uidvalidity_recheck: false`; `historyid_expires_after: None`;
  `delta_token_expires_after: None` (expiry is reactive: 410 Gone / 400
  InvalidDeltaToken).
- `pim_methods`: true for the container/category/extended-property/
  importance/is-read writes, send/draft lifecycle, `scheduled_send`,
  `send_as`, search, mail folder CRUD, `identities_list`, vacation, typed hydration,
  contact/calendar primitives, `directory_search` (org directory via
  `/users`), and `host_attachment`. False for
  `remove_from_container`, `set_keyword`, `set_label_membership`,
  standalone `attachment_upload`, `identity_update`, `quota_get`.
- `filter_rule_shape: Rules`; all 5 filter flags true (Graph Inbox
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
`@odata.deltaLink`, and optional mid-walk page marker
(landing in `OpaqueChangeState::bytes`; page markers also in
`ChangeCursor::advanced_through`).

`decode_cursor` rejects wrong protocol, incompatible envelopes, and malformed
JSON. `changes_stream` cross-checks that payload kind projects back to
`cursor.scope`; mismatches terminate with `SyncState(SchemaIncompatible)`.
`establish_initial_cursor` accepts delta-eligible `FolderType` scopes (email,
event/calendar event, contact) plus any `CursorScope::Folder` in the
public-folder routing map; both mint the first cursor through inventory. A delta
`describe_cursor` is cheap/`ServerCursor`/fresh; a public-folder cursor is
cheap/`Poll`/fresh; invalid cursors reseed through inventory.

### Public-folder cursor (no delta token)

`GraphCursorKind::PublicFolder(PublicFolderCursor)` is additive, no version
bump (a v1 reader never wrote it). The payload IS the sync state:
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
`Unsupported(PushSubscribe)` for any `Folder` scope (poll-only v1). Lost rights
surface as EWS `ErrorAccessDenied` and quarantine just that scope via
`ews_shared_scope_error` -> `ScopeRevoked` -> `DisableScope`.

## Per-scope inventory, changes, hydration

Supported scopes:

- `FolderType { folder, Email }` -> initial URL
  `/{prefix}/mailFolders/{folder}/messages/delta?$select=...&$top=50`
  (`MESSAGE_SELECT`). `inventory_entry_from_value` pulls id, conversationId
  (thread), change-key (etag), Message-ID / References / In-Reply-To from
  `internetMessageHeaders`, and a flags hash from `isRead` / `flag.flagStatus`
  / `categories`. `size` is `None` on this projection.
- `FolderType { folder, Event | CalendarEvent }` -> calendarView delta over
  a [-90d, +365d] window (`EVENT_SELECT`); thread/message-id fields empty.
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
`@odata.nextLink` chain), yielding `PageBoundary::Page` Batches until a
`delta_link`, then a `Final` Batch + `Checkpoint::Change`. A page with neither
link is a contract violation, never a successful cursor-less completion.
`@removed` entries are skipped and evict their cached change key; harvested
etags fold into the bounded LRU `account.etag_index` for the next `If-Match`.

`changes_stream(cursor)` decodes the payload, asserts `scope_matches_payload`,
and walks `delta_link` (or `advanced_through.next_link` on resume). Each
non-removed entry emits `ObjectChange { Updated }` + `ScopeChange { Added }`
(Graph delta has no created/updated split); `@removed` emits one `ScopeChange
{ Removed }`. Page batches checkpoint `advanced_through` at the next link; the
final page checkpoints the fresh `delta_link`, `advanced_through` cleared.

`get_stream` chunks ids into `batching_policy.max_items` blocks, fires one
`/$batch` per chunk, and returns exactly one per-id `ItemOutcome<HydratedObject>`:
2xx ->
`Succeeded`, 4xx/5xx -> `Failed` with a structured `AccountError` (via
`response_to_account_error_pub`), 2xx-no-body -> `Failed(Protocol(MissingField))`.
Locally-invalid items `Failed` without poisoning the batch; a whole-request
transport drop terminates. "Locally-invalid" includes an id that cannot be
ROUTED (`batch_routing::partition_routable` over `hydrate_url_for_id`): the
split runs before the POST, so a stale shared-mailbox id costs itself an
outcome and neither its valid REST siblings nor the EWS outcomes already
fetched for the same chunk. The subrequest index is assigned AFTER that
split, so it always indexes the ids actually sent.

`reconcile_hydration_responses` is the pure projector that enforces the
one-outcome-per-id contract, and `reconcile_mutation_responses` is its
mutation twin. Both discard a response id that does not parse as a submitted
index, parses out of range, or repeats an already-answered one - minting an
`ObjectId` from the response id would put an id the caller never submitted on
a lane while the real one stays unanswered. Both then sweep the ids Graph
never answered.

An unanswered subrequest classifies `Protocol(PartialResponse)` with
`TransmissionState::Acknowledged` (`graph_error::batch_response_missing`), on
all four `$batch` paths - hydration, the bulk mutation funnel,
`pim::submit_write_batch_with_targets`, and the reaction read's
`classify_chunk` (whose unanswered ids ride the `BatchOutcome` uncertain
lane). The outer envelope decoding is
evidence about the envelope only: a `move` or `DELETE` that committed and
lost its subresponse is indistinguishable from one that never ran, so the
error must not assert the item was left alone. `ContractViolation` would
derive `ProviderContractViolation`, which is terminal, and the engine would
file the mutation `failed_terminal` with no read-back. `PartialResponse`
instead retries the idempotent operations (hydration, `UpdateFlags`) and
routes the non-idempotent ones (`BulkMove`, `BulkDestroy`) to
`Reconcile(PartialCompletionSignal)`. In the bulk funnel the ambiguous id
also rides the **uncertain** lane rather than `failed`, so the engine's
read-back guard resolves it.

`pim::submit_write_batch_with_targets` is the one `$batch` path that answers
per REQUEST - its callers are single-`Result` trait methods - but it still
DRAINS every subresponse before returning. `$batch` response order is
Graph's, not the caller's, so returning on the first bad item left the etag
of a message that demonstrably was destroyed in the LRU whenever its
subresponse sorted after a failing sibling's, and a later conditioned write
on that id then sent an `If-Match` for a message that no longer exists. The
error surfaced is unchanged: the first failure in response order.

`message_reactions` (`reactions.rs`) reads the two Outlook
`singleValueExtendedProperties` (`OwnerReactionType`, `ReactionsCount`) over
the same chunked `$batch`, dedupes its ids, and answers every submitted id in
exactly one lane. Public-folder ids name EWS items this Graph-only
extended-property surface cannot address, so `partition_supported_ids` splits
them off and files each one `Failed(Unsupported(MessageReactionsRead))`
locally while the Graph ids of the same batch still go to `$batch` - a
top-level rejection would be a per-request answer on a per-item surface and
would discard the outcomes of every ordinary message beside the public id.
An unroutable id (stale shared mailbox) files the same way, through
`batch_routing::partition_routable` per chunk. Here the boundary contract
adds a second reason: past the 20-id chunk limit earlier chunks have already
been transmitted, so a top-level `Err` - which means "nothing was
transmitted" - would be false as well as lossy.

The `hydrated_from_value` projector maps `FlagsOnly`
-> canonical flag `HashSet`; `Metadata`/body-bearing -> `metadata_or_flags`.
Graph JSON is not assembled RFC822, so body-bearing projections degrade to
`Metadata` (assembled bytes come from `open_raw_rfc822`).

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
client; `owner_of_scope(scope)` returns the `MailboxId` owner for a foreign
scope, `None` for primary. `discover_cursor_scopes_inner` lists the primary
then each shared mailbox's folders, emitting foreign-namespaced `FolderType`
scopes; a per-mailbox permission denial skips with a `Warning`.
`discover_memberships_inner` emits the foreign `MembershipScope::Mailbox(owner)`
tag with the folder membership, and `inventory_stream` stamps it onto every
foreign-scope item (the engine covering rule cannot form it).
`initial_delta_url` reads the prefix from `client_for_scope` and the native id
from `parse_folder`, so the mailbox rides in `/users/{id}`, the id in
`/mailFolders/{id}`.

Per-message routing rides the same codec applied to the *message* id.
`foreign.rs::encode_message_id(scope, native)` foreign-encodes the message id
(`{mailbox}\u{1f}{native}`) when the scope parses foreign, and leaves a primary
message bare; `parse_message_id` -> `ParsedMessageId::{Primary, Foreign}` with
`.native_id()` / `.owner()`. The id is minted encoded at every projection site
(`inventory_entry_from_value`, and the `changes` Added/Updated/Removed ids) so
one logical message carries identical bytes everywhere - the consumer uses it
as a stable primary key, and a shared-mailbox message never also surfaces bare
via `/me`. Every per-message request decodes it and routes via
`client_for_owner(parsed.owner())` using `parsed.native_id()`: hydration
(`get.rs hydrate_url_for_id`), blob + raw (`blob.rs`), mutations (`mutate.rs`
flag/move/destroy and the `pim.rs` writes), and the typed `message_hydrate`. A
foreign id builds `/users/{owner}/messages/{native}`, a primary id
`/me/messages/{id}`.

The THREAD-keyed doors are the exception, and it is a gap rather than a
design: a `ThreadId` is Graph's bare `conversationId`, minted with no owner
tag, so `thread_hydrate` and every `MutationTarget::Thread` fan-out resolve
their member ids through `/me/messages?$filter=conversationId eq ...` -
`message_values_for_thread` reads `account.client.api_path_prefix()`
unconditionally. A thread id a consumer took off a shared mailbox's
`InventoryEntry` therefore resolves against the PRIMARY mailbox, where that
conversation does not exist, and the empty result is not an error: hydration
returns an empty thread and a thread-targeted write reports success having
touched nothing. Tracked as O-25 in `plans/bugs-graph.md`. `bulk_move` also decodes the destination
`FolderId` (the `destinationId` body must be the native id) and rejects a
cross-mailbox move - destination owner != source owner - as `Request(Malformed)`,
since one endpoint can't express it. The etag cache stays keyed by the encoded
id; only the URL uses the native id. `push.rs resource_for_scope` routes a
foreign subscription via `client_for_scope` + the native folder id, never
percent-encoding a raw `\u{1f}` id into the URL.

Revocation isolation: `graph_shared_scope_error(error, scope, owner, ctx)`
quarantines just the foreign scope when the failure is
`Authorization(PermissionDenied)` and `owner.is_some()` ->
`graph_scope_revoked` -> `ScopeRevoked` -> `Engine(DisableScope(scope))`. A
primary scope (`owner == None`) stays terminal `NoPermission`. Wired at the
`inventory_stream`/`changes_stream` per-scope fetch-failure boundary.

Foreign-mailbox enumeration is config-supplied (`with_shared_mailbox`) or,
opt-in, Autodiscover-enumerated: Graph REST has no "list my delegated
mailboxes" call, so `open` falls back to the EWS-era Autodiscover
`alternativeMailboxes` response when `with_delegate_discovery()` is set
(default off). `discover_shared_mailboxes` (`autodiscover.rs`) queries it for
the primary user's SMTP; `merge_shared_mailboxes` combines the result with
any `with_shared_mailbox` entries additively, empty-dropping and
exact-string-deduping across the whole merged set (config first, discovered
appended). Discovery is best-effort and non-fatal: malformed/truncated XML or
a request failure degrades to the config-supplied mailboxes rather than
failing `open`. The EWS twin `ews_shared_scope_error` applies the same
`ScopeRevoked` -> `DisableScope` isolation to public-folder scopes.

## Public-folder discovery (Autodiscover)

Opt-in via `with_public_folders(scope)`. After the primary/shared mailboxes,
`discover_cursor_scopes_inner` calls
`public_folder::discover_public_folder_scopes`: resolve hierarchy routing via
`GetUserSettings`, browse recursively from `find_folder("publicfoldersroot")`
with hierarchy headers (`child_folder_count > 0` re-enters the worklist; a
`visited` dedup set plus a browse-step cap bound the walk), read-gate via
`effective_rights.read`, and per folder resolve the content mailbox
(`get_folder` PR_REPLICA_LIST GUID -> `construct_replica_smtp` ->
`discover_content_mailbox`).

Neither Autodiscover leg may drop a folder. Both degrade instead, because
both depend on optional Exchange machinery that a tenant (or a harness) may
simply not serve, and dropping meant an EMPTY `routing_map`: zero public
containers, zero pinned scopes, and nothing but a support-only warning:

- No `PublicFolderInformation`: `hierarchy_routing_fallback` anchors the
  browse on the account's own mailbox (a real identity - the anchor becomes
  the owner `MailboxId` on every emitted item, so it must never be empty).
- No content-mailbox routing for a folder: `content_routing_or_hierarchy`
  seeds it with the hierarchy routing the browse already succeeded with.

Both helpers are pure and unit-pinned. A `find_folder` failure at the root is
still fatal to the leg (there is no hierarchy at all); a sub-folder browse
failure still skips that subtree.

`seed_and_scope` then splits discovery from sync. It ALWAYS seeds two maps -
`routing_map` (the content-mailbox routing the cursor payload carries) and
`public_folder_meta` (display name, `FolderClass`, parent, effective rights,
for the `containers_list` projection) - and emits a `CursorScope::Folder`
only when the configured `PublicFolderScope` names the folder.
`HierarchyOnly` emits none. Both maps are filled synchronously inside
`SyncEngine::attach`'s discovery call, which is what makes them populated by
the time `containers_list` runs; neither seeding may be deferred to a lazier
point. Rights stay advisory at discovery (the authoritative gate is the live
`ErrorAccessDenied`), but they are now also PROJECTED onto
`Container::rights` so a read-only public folder is distinguishable
downstream.

## Public-folder hydration (EWS `GetItem` / `GetAttachment`)

A public-folder item is a raw EWS `ItemId`; Graph REST has no route that can
address it. `public_folder.rs` therefore mints folder-qualified object ids
(`foreign::encode_public_item_id`, an RS-separated `"<folderId>\u{1e}<itemId>"`
distinct from the US-separated shared-mailbox form) in both the inventory
projection and the poll's `Added`/`Destroyed` emissions.

`get_stream` splits each chunk with `partition_ews_ids`: an id whose folder is
present in `routing_map` goes to `fetch_ews_outcomes` (one `GetItem` per item,
because per-folder routing headers differ), everything else to the Graph
`/$batch`. Both arms land in one `Batch`, so the consumer still sees exactly
one outcome per pulled id. `hydrated_from_ews_item` projects into the SAME
`HydratedObject` shape the REST arm returns: `Metadata` (the identical
inventory-entry projection) for the body-bearing projections, `FlagsOnly` for
`FlagsOnly`, plus one blob handle per attachment descriptor. Body-bearing
projections degrade to `Metadata` for the same reason the REST arm does -
`RawMime` means assembled RFC822, and `GetItem` returns a parsed HTML body.

Attachment bytes route through `open_blob`: the blob locator carries the
folder-qualified message id, so `GraphBlobKind::Ews` fetches via SOAP
`GetAttachment` (base64-inline, hence no range support) and
`GraphBlobKind::EwsItem` surfaces the `BlobNotByteStream` warning.

## Push: webhooks plus EWS streaming fallback

Push has two modes, selected on the factory and reflected in the
capability surface.

### Webhook mode (`PushMode::GraphSubscriptions`)

`push_subscribe(scopes)` rejects an EMPTY scope list as `Request(Malformed)`
before mode dispatch, so in both modes (a subscription covering nothing
registered a group teardown could never retire and started a renewal worker
nothing could stop; the engine already skips empty lists at its own reattach
boundary). It then groups by Graph
subscription resource and
rejects the request if any scope is not subscribable, creates one server
subscription per resource, and best-effort deletes any already-created
subscriptions if a later create fails. The `SubscriptionHandle` is minted
BEFORE the first create: `new_handle` is fallible and classifies its error
`TransmissionState::Unsent`, which is only honest while nothing has been
written, so minting it afterwards would let an RNG failure report
no-bytes-sent over live server-side subscriptions and invite a duplicating
retry. It stores `(server_id, expires_at)` in a
`GraphSubscriptionGroup`, and emits `Reconnected`. `push_unsubscribe`
deletes each server subscription BEFORE dropping its local state, so a failed
DELETE leaves that subscription and every not-yet-attempted sibling reachable
under the handle for a later retry. Keeping the group registered across those
awaits means "registered" no longer implies "live", so
`mark_group_tearing_down` sets a `tearing_down` marker on the group in the
same write-lock acquisition that snapshots its server ids: the renewal worker
skips condemned groups in `due_renewals` and `install_replacement` refuses
them, because a replacement created after the snapshot is one teardown can
never name - it would stay registered and delivering while
`push_unsubscribe` reported success. The marker is monotone (a later
`push_subscribe` mints a new handle) and a plain flag rather than a lock, so
neither path holds anything across a round trip. Teardown lets the renewal
worker exit once no live groups remain; condemned groups retained for a DELETE
retry do not keep it ticking. A worker that decides to exit clears its own
`graph_worker` slot BEFORE releasing the subscriptions guard that made the
decision, and `push_unsubscribe` holds that same guard across its abort: a
concurrent `push_subscribe` cannot install its live group until the slot is
empty, so its `ensure_graph_worker` always spawns a replacement. Without that
ordering a new subscription could observe a still-unfinished `JoinHandle`,
decline to spawn, and never be renewed. Lock order is subscriptions-then-worker
on every path. The webhook receiver is not in this crate: consumers mount an HTTPS
endpoint at `PushEndpoint::webhook_url` and feed invalidations into the
engine `InvalidationSink`; `push_stream` carries health only.

`clientState` validation is available through
`GraphAccountFactory::with_push_endpoint_client_state(url, secret)`, which
sends the caller-owned account-wide secret on every resource's subscription
so the out-of-process receiver can compare it. The older
`with_push_endpoint(url)` keeps the legacy behavior: `create_subscription`
mints a fresh random `clientState` per resource, sends it, and discards it,
and nothing on `GraphAccount`, `GraphSubscriptionGroup`, or the returned
`SubscriptionHandle` exposes the value - a receiver behind that constructor
still has no secret to check and must authenticate forged invalidations by
other means.

#### Renewal worker

The worker wakes every 10 min, renews everything inside the 30 min
threshold, and emits `Disconnected` / `Reconnected` / `Terminated`
accordingly. Three rules make the renewal path safe:

- A renewal that 404/410s (`subscription_is_gone`) is not retryable - Graph
  retains no deleted subscription to PATCH - so the worker creates a
  replacement for the same resource. The stale state stays installed until
  the replacement is in hand: dropping it first meant a failed create left
  the resource with no row at all, so no later tick could see it as due and
  coverage was lost until reopen or an explicit resubscribe. A failed
  create classifies **the create error**, not the already-known 404, so its
  recovery class decides whether another tick is worth it.
- `install_replacement` is a plain `get_mut`, never
  `entry().or_insert_with()`, and it also refuses a group marked
  `tearing_down`. The due list is a snapshot, so a concurrent
  `push_unsubscribe` can retire the handle - or condemn it and start
  deleting from its own snapshot - while the create is in flight;
  re-registering (or quietly joining) the group there would tell the caller
  teardown succeeded while notifications kept arriving. In both cases the
  worker deletes the subscription it just minted instead (best effort - the
  caller's `push_unsubscribe` may already have returned).
- A successful replacement emits `Reconnected`. The resource had NO live
  subscription between its disappearance and the create, and Graph does not
  replay notifications for that window; `Reconnected` is the only event the
  reconciler turns into a full reconcile across every registered scope, so
  without it the missed changes wait for the ordinary poll interval.

### EWS streaming mode (`PushMode::EwsStreaming`)

`push_subscribe` installs an `EwsSubscriptionState` and starts the EWS
worker: it subscribes to the union of active folders, long-polls
`GetStreamingEvents`, maps notifications to cursor scopes, and emits
`Invalidated`. Failures use `ews_error_to_account_error`
(terminal terminates; transient emits `Disconnected`, sleeps, reconnects).
Scope registrations ride a `watch`-channel generation counter
(`ews_topology`): the worker marks the generation seen immediately before
every read of the subscription map, so a change can never fall between a
read and a wait - a stored-permit `Notify` here turned the registration
that starts the worker into a phantom topology change and a guaranteed
redundant second subscription. Started before any scopes exist, the worker
waits on that generation instead of polling the empty map. A bump while
the stream is live cancels the in-flight `GetStreamingEvents`: the worker
retires the abandoned subscription with a best-effort EWS `Unsubscribe`
(Exchange holds streaming subscriptions against a per-mailbox quota, so
leaking one per scope change accrues until each times out), subscribes to
the current union, and then emits `Reconnected` - never `Disconnected`,
since nothing failed - because a change raised in the handoff window was
delivered to neither subscription and `Reconnected` is the engine's
full-reconcile trigger. Streaming subscriptions carry no resume state:
`StreamingSubscriptionRequest` admits only `FolderIds` and `EventTypes`,
so the Subscribe body is watermark-free (a `<t:Watermark>` there is a
schema violation that broke every resubscribe once a watermark had been
recorded), and gap coverage is always the `Reconnected` reconcile. The
subscribe / long-poll / unsubscribe cycle is pinned hermetically through
`EwsExecute` with a scripted transport.

`push_subscribe` rejects any `Folder` (public-folder) scope as
`Unsupported(PushSubscribe)` in both modes. The EWS arm narrows further via
`ews_subscribable_folder_id`: only a PRIMARY-mailbox `FolderType` scope is
accepted. A non-folder scope contributes nothing to the Subscribe body's
`FolderIds` (an empty element EWS rejects outright), and a foreign folder is
addressable only with its mailbox's routing headers while Subscribe sends
`EwsHeaders::default()`, so its native id would resolve against the primary
namespace. Both are refused locally rather than turned into a remote failure
that reads as a provider fault. The predicate returns the scope's native
`restId` instead of a bool, so the check and the extraction are one step and
no later phase can re-derive the id differently. `push_stream` is a
`broadcast::Receiver<WatchEvent>` adapter that selects against shutdown;
the EWS branch re-spawns its worker on demand.

#### REST-to-EWS id translation

A Graph folder scope carries a `restId`; EWS speaks `ewsId`, and the two are
distinct opaque formats whose only supported conversion is
`translateExchangeIds`. `subscribe_ews` therefore translates once, at the
subscription boundary, and retains the pair: `EwsSubscriptionScope { scope,
ews_folder_id }` is what `EwsSubscriptionState` stores, what
`build_subscribe_request` puts in `<t:FolderId>`, and what `scopes_for_folder`
matches a notification's parent id against to route back to the original
`CursorScope`s without a second request. Nothing downstream re-derives an id.
The mapping is many-to-one in both directions, so two pure rules sit under
it. `dedupe_by_ews_folder` collapses the union onto one entry per
`ews_folder_id` before the Subscribe body is built: overlapping
`push_subscribe` calls, or two `FolderType` scopes over one container
(translation already returns them one `ewsId`), would otherwise repeat a
`<t:FolderId>`, and whether EWS accepts, ignores, or rejects that is not
determinable here. `unique_scopes_for_folder` routes a notification to EVERY
distinct scope registered against the folder, not one arbitrary match: each
scope owns its own delta cursor, so invalidating one of a pair leaves the
other stale. Equal scopes collapse, because every repeat costs the
reconciler another full `changes_stream` run over a cursor it is already
reconciling.
Before this, Subscribe shipped `restId`s EWS cannot parse, so the whole
opt-in `with_ews_streaming()` path terminated on an opaque
`SoapFaultCode::Unknown` rather than establishing.

Three rules govern the translation call, all pinned as pure functions
because the request itself has no in-process seam:

- **Deduplicate, then chunk** (`translation_input_chunks`). A `restId` is per
  folder, not per scope, so scopes sharing a folder ask once; Graph caps
  `inputIds` at 1,000 strings and rejects the whole request above it, so a
  large mailbox fans out over several POSTs instead of failing before EWS
  setup is attempted. First-seen order is preserved, so chunk boundaries are
  deterministic.
- **Per-id answers, per-id errors** (`reconcile_translated_ews_scopes`).
  Graph's `convertIdResult` reports failure PER id inside an otherwise
  successful 200: a converted id carries `targetId`, a refused one carries
  `errorDetails` and no target. `targetId` is therefore optional on the wire
  type - requiring it made one bad id fail deserialization of the whole
  response and surface as a terminal `Protocol(ParseFailed)` naming nothing.
  A refusal classifies `Request(Malformed)` -> `ClientBug` via
  `graph_error::id_translation_refused`, carrying `ErrorScope::Cursor(scope)`
  and Graph's verbatim code as a typed `WireCause::Graph`. It does not route
  through `response_to_account_error`: that needs a status, and a
  synthesized 400 on a cursor scope would classify `SyncState(CursorInvalid)`
  and send the engine to `RestartScope`, which cannot fix an id the server
  declines to convert. An omitted answer, or one with neither a target nor
  error details, is `Protocol(ContractViolation)` with the same scope. No
  arm degrades to sending the untranslated `restId`.
- **Idempotent despite the operation** (`GraphErrorContext::idempotent`). The
  call keeps `operation: PushSubscribe` - that is what the caller asked for -
  but overrides the derived idempotency. `PushSubscribe` is non-idempotent,
  so an in-flight transport drop would derive
  `Reconcile(TransportDropAfterSend)`; this read-only POST runs before any
  handle, local state, or EWS subscription exists, so there is no target to
  probe and a plain `Retry(SameRequest)` is correct. The override rides on
  `GraphErrorContext` and is applied in `finish` for builder-constructed
  errors; the `bifrost-net` transport arm re-applies it through
  `AccountError::into_builder`, since `NetErrorContext` has no override
  channel and the builder-only override does not survive that round trip.

Notification routing is an exact `ews_folder_id` byte match against the ids
this crate itself received from Graph, so a miss no longer means "the
formats may disagree". A miss still degrades to `HintPayload::Unknown`
(account-wide re-check) rather than dropping the notification.

## Mutation pipeline

`bulk_set_flags`, `bulk_move`, `bulk_destroy` share
`bulk_mutation_stream`: chunk to `batching_policy.max_items` (20), then
`submit_batch` (1) reads the batch's own ids out of the etag cache (a
per-id lookup that marks them hot, not a clone of the whole map) and
refreshes missing `SetFlags`/`Move` etags via
`GET /messages/{id}?$select=id` (`Destroy` needs none), (2) builds one `BatchRequestItem` per id - `PATCH` /
`POST .../move` / `DELETE` - attaching `If-Match: <changeKey>` (mandatory
for `SetFlags`/`Move`, opportunistic for `Destroy`), (3) sends `/$batch`,
status driving `mutation_item_outcome`.

Step (2) can fail per id, and every such failure joins the chunk's outcome
list instead of aborting it: an id with no cached or refreshable etag, a
`Move` that does not resolve to a same-mailbox folder destination, and an id
whose shared mailbox is no longer configured. The last one only reaches
`request_for_mutation` on the `Destroy` path - `SetFlags` / `Move` are
filtered out one step earlier by the etag preflight, which already files
them per item - which is precisely why propagating it there was invisible
until a bulk destroy hit it. If step (2) leaves nothing routable, no
`/$batch` is sent and the chunk is answered entirely from those local
outcomes.

`bulk_set_flags` translates `FlagOp` to a PATCH body (`isRead`,
`flag.flagStatus`, sorted `categories`); unrecognized flags ignored; `Set`
rewrites the `categories` array, `Add`/`Remove`/`Patch` touch named fields.
`bulk_move` requires `MembershipScope::Folder` (else fatal pre-request).
`IdempotencyKey` is accepted but not sent (`MutationReplaySafety::None`).

## PIM primitives

Mail mutation primitives live in `pim.rs`, per-message, fanning out a
`MutationTarget::Thread` via `/messages?$filter=conversationId eq ...`.
`add_to_container` is `POST /messages/{id}/move` (no symmetric remove ->
`remove_from_container` unsupported). `set_is_read` patches `isRead`;
`set_category` patches `categories[]` (reserved `$flagged`/`starred` ->
`flag.flagStatus`). `set_extended_property` patches
`singleValueExtendedProperties` when `Some`, else `DELETE`s it (404-tolerant).
All send `If-Match` if `changeKey` set.

`set_importance` patches the single-valued `importance` field in one
`If-Match`-conditioned PATCH (never clear-then-set); the read side maps it back
onto `Message.importance` (absent/unrecognized -> `Normal`).

Send / draft lifecycle is draft-backed so the trait returns an id: `POST
/messages` create, `POST /messages/{id}/send` send (`202` no-body; the Sent
Items id is rediscovered via sync/search), returning the draft id.
Inline attachments encode into `fileAttachment` JSON; standalone
`attachment_upload` is unsupported (over-limit -> `host_attachment` ->
OneDrive); `draft_update` patches mutable fields (no attachment replace).
Scheduled send PATCHes `PidTagDeferredSendTime` (`SystemTime 0x3FEF`, ISO-8601
UTC) onto the draft between create and send; the draft id is the
cancel/reschedule handle.

`SendRequest::send_as` (gated by `pim_methods.send_as`; Graph and JMAP both
advertise it, see `reference/jmap.md` for the JMAP foreign-submission leg)
routes create/deferred-stamp/send through the shared mailbox's `shared_clients`
entry (keyed by `MailboxId`); the three helpers take an explicit
`&GraphClient`. `apply_send_as` stamps `from`/`sender`: `As` forces both to
the mailbox; `OnBehalfOf` keeps `from` = mailbox (honoring an explicit
`from`), `sender` = `user_email` (omitted when `None`). An unconfigured
mailbox is `Request(Malformed)`.

Search uses `/messages` (`$filter`/`$search`/`$top`, `@odata.nextLink` as
the opaque page cursor); message search returns native ids, thread search
dedups `conversationId` per page. Graph forbids `$search`+`$filter` together and
rejects `$filter` `contains()` on sender/recipient (400), so any
`SearchFilter::{From,To}` leaf (`filter_requires_search`) routes the whole
filter onto `$search`/KQL (`kql_filter`), AND-combining any `provider_query`;
`In` + From/To is `Request(Malformed)` (no KQL folder property). No-sender
searches keep the `$filter` path.

`directory_search` (organization directory / GAL, distinct from the personal
`/contacts` corpus `contact_search` scans) queries `/users` with a
`displayName,mail,businessPhones,companyName,jobTitle,department` `$select`,
`$top`, and (non-empty query) a provider-side `$filter`
`startswith(displayName,'q') or startswith(mail,'q')` (escaped + URL-encoded as
a whole) - unlike `contact_search`, which scans client-side and only
server-filters exact-email queries. Rows without `mail` are dropped;
`additional_emails` is empty; `@odata.nextLink` pages. A tenant lacking
`User.ReadBasic.All` / `User.Read.All` 403s -> `NoPermission` (an unauthorized
directory is an error, not an empty result).

Directory groups (`account/groups.rs`, Graph-only in the workspace):
`directory_groups_list` walks `{prefix}/memberOf/microsoft.graph.group`
filtered to `mailEnabled eq true`, classifying rows into
`DirectoryGroupKind` (`Unified` in `groupTypes` -> `Unified`, else
`securityEnabled` -> `MailEnabledSecurity`, else `DistributionList`);
mail-disabled groups are dropped. `directory_group_expand` walks
`/groups/{id}/transitiveMembers/microsoft.graph.user` (nested groups
flattened and cycle-checked server-side), projecting
`DirectoryGroupMember { email, display_name }` with `mail` preferred over
`userPrincipalName`, lowercased, memberless-of-both rows dropped. Both
page via `@odata.nextLink` bytes like `directory_search` and need
`GroupMember.Read.All`-class consent - a harder grant than the mail
scopes; an ungranted tenant 403s -> `NoPermission` at call time (the
capability flags state protocol support, not tenant consent).

Container CRUD maps to mail folders only. `containers_list` returns
native folder ids (`Provenance { Graph, Folder, native }`), refreshes the
folder tree, and maps well-known folders; user folders stay role-less. It then
appends two namespaced legs:

- Each `shared_clients` entry's folders, `namespace = Shared`,
  `owner = MailboxId(mailbox)`, `native_id = encode_foreign(mailbox, folderId)`
  (byte-identical to the `CursorScope::FolderType` string discovery emits, so
  a container joins its sync scope by id), `owner_local_id` = the bare Graph
  folder id. The primary role map is deliberately NOT applied here - a shared
  mailbox's well-known folder ids are not the primary's. A per-mailbox listing
  failure degrades to a `Warning` plus the remaining containers.
- Each `routing_map` public folder, `namespace = Public`, no owner,
  `content_class` from the EWS `FolderClass`, `rights` from the EWS
  `EffectiveRights`. Purely local (reads the discovery-seeded maps, no EWS
  round-trip) and includes folders that are visible but NOT pinned for sync -
  the consumer has to see a folder before it can pin it.

`containers_list` has no warning lane in the `Account` trait, so the
degradation `Warning`s are logged rather than yielded.

Create/rename/move/delete call `mailFolders`; root moves target
`msgfolderroot`.

Settings are narrow: `identities_list` returns the primary `me` profile as
the default identity (`identity_update` unsupported); vacation get/set maps
to `mailboxSettings.automaticRepliesSetting`; `quota_get` unsupported.

Typed hydration is separate from `get_stream`: `message_hydrate` fetches one
message at the requested projection into `Message`; `thread_hydrate` queries
the conversation, sorted by date.

`message_hydrate` makes the SAME transport decision the batch door makes
(`pim::ews_read_folder`, the pure discriminator `partition_ews_ids` also uses):
a public-folder id reads over EWS `GetItem` via `public_message_hydrate` +
`message_from_ews_item`, everything else over Graph REST. Without that split
the single-id door - the one a consumer reaches through the engine's hydration
passthrough - stripped the id down to its native EWS `ItemId` and asked
`/me/messages/{itemId}`, which 404s `ErrorItemNotFound` naming the bare item
id. The EWS projection keeps the folder-qualified id, maps `BodyPreview` /
HTML body onto `body_text` / `body_html`, tags the containing folder as the
only container, and carries `Importance::Normal` and no `thread_id` (EWS's
message shape reports neither).

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
`GraphError` into an `AccountError` via `try_build`.

The `GraphError::Net` arm re-decodes Graph's error envelope before
delegating. `bifrost-net` never hands a 4xx or 5xx back as a response - its
retry loop converts one into `Status` / `RateLimited` /
`RetryBudgetExhausted`, body preserved - so classifying those through
`bifrost_net::into_account_error` alone reads the HTTP status and throws the
typed `error.code` away. The whole `GraphSignal` table below then applied to
`$batch` subresponses and to nothing else: a live 400 `InvalidDeltaToken`,
the documented delta-expiry signal, classified `Request(Malformed)` ->
terminal `ClientBug` instead of `SyncState(CursorInvalid)` ->
`RestartScope`, and a 403 `AdminConsentRequired` classified
`PermissionDenied` instead of `NeedsAdminConsent`. `net_to_account_error`
refines when the body is Graph's - an envelope decoded, or there was no body
at all and the status table is the whole answer (a bare 412 is
`ConcurrencyConflict` here and an opaque server error in bifrost-net's
table). A NON-EMPTY body that is not an envelope stays on bifrost-net's
mapping on purpose: Graph did not write it, so reading it as
`Protocol(ContractViolation)` would turn an edge device's HTML 403 on the
Autodiscover leg into a terminal provider fault. `AuthLost` is never
refined - it means a 401 survived a forced refresh, stronger evidence than
the envelope, and Graph's own 401 arm derives the same recovery class.

The same transport fact governs control flow, not just classification:
`webhooks::subscription_is_gone` reads the status through
`GraphError::response_status`, which sees both the `Response` shape (`$batch`
subresponse, EWS HTTP failure, OneDrive chunk PUT) and the response evidence
a `bifrost_net::Error` preserved. Matching only `GraphError::Response` made it
permanently false on the live path, so `delete_subscription` failed on an
already-vanished row and the renewal worker never took its recreate branch.

`GraphErrorContext
{ protocol, operation, scope, idempotency_override }` threads the operation so
`recovery::derive` computes `RecoveryClass` (no private recovery table).
`idempotency_override` is `None` everywhere except the one call site that
needs it (the EWS id-translation POST, via `.idempotent()`): it corrects the
retry-vs-reconcile derivation for a read-only preflight issued under a
mutating operation, without renaming the operation the caller invoked.
`GraphError::Configuration` - the locally-detected stale-shared-mailbox
rejection, raised before any request - classifies `Request(Malformed)`
through the same `base_builder` / `finish` funnel as every other arm rather
than through `invalid_account_error`, which takes an operation and nothing
else: the rejection is always raised about a specific message, cursor scope,
or subscription resource, and dropping `ctx.scope` left an operator a bare
`request.malformed` with nothing to act on.
`GraphErrorContext::ews(op)`
is the EWS constructor; `ews_error_to_account_error` routes
`EwsError::Transport` through `bifrost_net::into_account_error`, `HttpStatus`
through the REST `response_to_account_error` path, `SoapFault` onto
`SoapFaultCode` (Server -> Unavailable, Client/MustUnderstand/VersionMismatch
-> 400, Unknown -> ContractViolation), and `MalformedXml` to
`Protocol(ParseFailed)`. EWS errors stamp `Protocol::Ews`.

`EwsClient::execute` screens every 200-OK body through `check_soap_fault` then
`check_response_error` before any operation parser sees it, because EWS reports
most operation failures inside a 200 as `ResponseClass="Error"` +
`<m:ResponseCode>`. That scan is per-response-message (a later warning or
success never donates its code to an earlier error), and an error-classed
message that cannot be classified is `MalformedXml`, never success - the
operation parsers read a missing result set as an empty successful one, so
passing it through made public folders or items silently disappear.
Unclassifiable means either NO `ResponseCode` or the self-contradictory
`NoError` on a `ResponseClass="Error"` message; the two share one path. A
CLASSIFIABLE error later in the same body outranks the malformed report,
since its code carries the real classification (`ErrorAccessDenied`
quarantines just that scope).

That scan is whole-RESPONSE: it produces one verdict for the body, exactly
as the operation parsers produce one result. EWS, however, answers per entry
of an `m:`-namespaced id collection (`m:ItemIds`, `m:FolderIds`,
`m:ParentFolderIds`, `m:AttachmentIds`, `m:SubscriptionIds`), so the two
agree only while a request names at most one such id - otherwise one item's
`ErrorItemNotFound` discards its siblings' results, the same
per-request-answer-on-a-per-item-surface mistake the `$batch` funnel had to
unlearn. Every read builder does name exactly one (public-folder hydration
deliberately issues one `GetItem` per item because the routing headers
differ), and that is ENFORCED rather than assumed: `build_soap_envelope` -
the single funnel `EwsClient::execute` puts every request through -
`debug_assert!`s `per_answer_request_ids(body) <= 1`, so a multi-item body
panics at construction in any dev or test build, no transport needed. The
Subscribe folder set is exempt by construction, not by exception: it rides
in `t:FolderIds` inside `m:StreamingSubscriptionRequest`, which EWS answers
with one `SubscribeResponseMessage` however many folders it names, and the
counter keys on the namespace prefix. Making the response scan per item is a
prerequisite for any batched EWS request, not a follow-up to one.

Known Graph vocabulary lands on typed `WireCause::Graph(GraphSignal::*)` variants
(auth/access/throttle/cursor codes; see `classify`). `GraphSignal::Unknown
{ code }` is the forward-compat fallback; string-matching unknown vocabulary is
forbidden (gate-5 invariant). `ews_shared_scope_error` is the EWS twin of
`graph_shared_scope_error`: an `ErrorAccessDenied` on an owned (public-folder)
scope quarantines via `ScopeRevoked` rather than escalating account-wide.

Mapping highlights:

- `Gone` / 410 / `InvalidDeltaToken` / `SyncStateNotFound` ->
  `SyncState(CursorInvalid)` -> `Engine(RestartScope(scope))`.
- `TooManyRequests` / 429 -> `Server(RateLimited)`, `throttle_scope: Tenant`,
  `retry_hint: After(_)` from `Retry-After` (seconds + HTTP-date via
  `bifrost_net::parse_retry_after`); 503 / 504 -> `Server(Unavailable)` ->
  `Retry::SameRequest`.
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

`mutation_item_outcome` projects per-id `$batch` responses onto `ItemOutcome`:
2xx -> `Succeeded(Applied)`, 404-on-destroy -> `Succeeded(Skipped)`, 412 ->
`Failed(ConcurrencyConflict)`, 429/other -> `Failed` carrying `Protocol::Graph`
+ `AttemptCause(Acknowledged)` + wire signal. Shared by `mutate.rs` `bulk_*`,
`pim::submit_write_batch`, `get_stream`.

Cursor-decode failures (`CursorProtocolMismatch`, `CursorEnvelopeUnknown`,
`SchemaIncompatible`, malformed payload) build an AccountError with
`SyncState(SchemaIncompatible)`, routed to `Engine(SchemaIncompatible)`; a
scope whose shared mailbox left the configuration
(`cursor::routing_error` -> `CursorError::Configuration`) builds
`SyncState(ScopeRevoked)`. `cursor_error_to_account_error` terminates EVERY
arm through `finish`, so `ctx.scope` is stamped on all of them. That is not
just telemetry for the revoked arm: `ScopeRevoked` derives
`DisableScope(scope)` with a scope and `RestartAccount` without one, and
account-wide rediscovery cannot restore a mailbox that left the
configuration - the scope-less form put the engine into a rediscovery loop
on every pass instead of quarantining one folder.

Non-byte-stream attachments emit a `BlobNotByteStream` `Warning` rather than a
terminal error, so the engine continues past a referenceAttachment in a batch.

## Known limitations

- Discovery is mail-only (+ opt-in public folders); event/contact cursors
  are engine-constructed.
- `scope_lifecycle_stream` is empty; folder creates/renames/deletes are
  observed only at reopen. Shared-mailbox routing is otherwise complete:
  send-as (C-3), read paths and message mutations (flag/move/destroy,
  drafts) all route to the owning mailbox via the encoded message id.
- Public folders are poll-only (no push) and side-table-free: the deletion
  baseline rides in the cursor, capped at 10_000 items/folder (above:
  additions-only). `CalendarItem` and `Contact` public folders sync at
  identity level alongside `Message`; `Task`, `DistributionList`,
  `PostItem`, and `MeetingRequest`/`Response`/`Cancellation` items are
  dropped with a scoped operator warning. A `CheckpointStore`-backed
  deletion baseline and support for those remaining item classes are named
  follow-ups.
- EWS streaming requires EWS reachable with an accepted token; webhook mode
  needs a public HTTPS endpoint (else `Error::MissingCoreCapability`). EWS
  streaming currently supports primary-mailbox folders only: shared-mailbox
  scopes need mailbox-specific EWS routing headers and are not yet grouped by
  owner. Folder ids ARE translated (`restId` -> `ewsId` once at
  `push_subscribe`, retained beside the scope), so Subscribe and notification
  routing both speak the format EWS expects. Webhook mode never needed it (it
  addresses folders over Graph REST).
- Blob range per-handle (fileAttachment only); delta-token expiry reactive (410
  / 400 InvalidDeltaToken -> `RestartScope`); `MutationReplaySafety::None`.
- Unsupported: `remove_from_container`, keyword/label writes,
  standalone `attachment_upload`, `identity_update`, `quota_get`.
- Inbox rules are conjunction-shaped (`And` -> conditions, `Not` -> exceptions);
  `Or`, date ranges, remove-label, mark-unread, star, keyword, reject actions
  are rejected by local validation.
