# bifrost-graph Account impl

How `bifrost-graph` implements the `Account` trait defined in
`plans/account-trait.md` and locked in `plans/account-trait-shape.md`.
This is the implementation map: what code goes where, what state
lives in the `GraphAccount` struct, how each trait method is realized
in terms of Graph's REST/$batch/delta/subscription endpoints and the
EWS SOAP streaming-notification fallback. The trait shape is not
re-specified here; references point at `account-trait.md` where they
matter.

## Module layout

The Account-trait surface lands as a new module tree alongside the
existing client + endpoint modules. The current crate exposes
`api.rs`, `calendar_sync.rs`, `client.rs`, `webhooks.rs`, `blob.rs`,
`ews/`, and the per-feature submodules; none of those move. The
Account implementation is additive.

```
crates/graph/src/
- lib.rs                       // add `pub mod account;`
- account/
  - mod.rs                     // GraphAccount, GraphAccountFactory,
                               // impl Account for GraphAccount
  - capabilities.rs            // build AccountCapabilities at open()
  - cursor.rs                  // GraphCursorPayload, envelope, codec
  - scopes.rs                  // discover_cursor_scopes,
                               // discover_memberships,
                               // scope_lifecycle_stream
  - inventory.rs               // inventory_stream per scope
  - changes.rs                 // changes_stream: delta drive per
                               // CursorScope::FolderType
  - get.rs                     // get_stream (Email, Event, Contact)
  - blob.rs                    // open_blob, open_blob_range
                               // (attachment $value / item bytes)
  - mutate.rs                  // bulk_set_flags, bulk_move,
                               // bulk_destroy via $batch + If-Match
  - push.rs                    // push_subscribe / push_unsubscribe
                               // (Graph /subscriptions CRUD)
  - push_stream.rs             // push_stream: in-process EWS
                               // streaming-notification reader
  - ews_stream.rs              // long-poll Subscribe + GetEvents
                               // SOAP, quick-xml incremental
  - error.rs                   // map graph + ews errors into
                               // RecoveryClass
```

`GraphAccount` wraps the existing `GraphClient` (HTTP, OAuth token,
retry, concurrency semaphore, folder-map cache). The Account-trait
methods sit on top; they do not duplicate transport.

## GraphAccount struct

State carried per open account:

```rust
pub struct GraphAccount {
    client: GraphClient,                       // existing transport
    capabilities: Arc<AccountCapabilities>,    // read once at open()
    push_endpoint: Option<PushEndpoint>,       // optional webhook
                                               // relay; None means
                                               // EWS streaming
                                               // fallback is the
                                               // only push path
    push_path: PushPath,                       // GraphSubscriptions
                                               // | EwsStreaming
    push_tx: broadcast::Sender<WatchEvent>,    // in-process fan-out
                                               // (EWS streaming
                                               // only; webhooks
                                               // feed InvalidationSink)
    cursor_index: Arc<RwLock<CursorIndex>>,    // FolderId -> known
                                               // CursorScope::FolderType
                                               // entries (cached
                                               // from
                                               // discover_cursor_scopes)
    membership_index: Arc<RwLock<FolderTree>>, // FolderId ->
                                               // parentFolderId map
                                               // for derived
                                               // Destroyed via
                                               // engine
    ews: OnceCell<EwsClient>,                  // lazily built EWS
                                               // client for the
                                               // streaming path
    shutdown: CancellationToken,               // close() trips this
}

enum PushPath {
    GraphSubscriptions {
        // The engine receives the webhook callbacks from an
        // out-of-process listener and feeds them into
        // InvalidationSink; this enum just records which path is
        // active per scope and tracks the server-side handles for
        // push_unsubscribe.
        active: DashMap<SubscriptionHandle, GraphSubscriptionState>,
    },
    EwsStreaming {
        // In-process long-poll handler owns its task and a
        // broadcast::Sender<WatchEvent>.
        worker: JoinHandle<()>,
        scope_map: DashMap<SubscriptionHandle, EwsSubscriptionState>,
    },
}

struct GraphSubscriptionState {
    server_id: String,         // POST /subscriptions response id
    resource: String,          // e.g. /me/mailFolders/{id}/messages
    client_state: String,      // hex secret for validate_notification
    expires_at: SystemTime,
    scopes: Vec<CursorScope>,  // what changes_stream the hint maps to
}

struct EwsSubscriptionState {
    ews_subscription_id: String,
    watermark: String,         // EWS watermark advances per chunk
    scopes: Vec<CursorScope>,
}
```

`GraphClient` is already cheap-cloneable (it holds `Arc<ClientInner>`).
`GraphAccount` is `Send + Sync` by construction. There is no
per-connection driver mutex; HTTP/2 multiplexing lives in `reqwest`
and the existing 3-permit semaphore on the client. See
`plans/account-trait-shape.md` Q2 for why this is enough; nothing in
the Graph protocol needs a daaki-style driver.

`GraphAccountFactory::open()` validates credentials, fetches profile
+ `mailFolders` to seed the folder tree, builds capabilities, and
hands back `Arc<dyn Account>`. The engine drives reopen cycles from
`RecoveryClass::CapabilityChanged` and `RestartAccount` via the
factory; nothing in `GraphAccount` self-reopens.

## Cursor model

Graph delta is per-folder per type. There is no account-wide delta
endpoint for mail; `mailFolders/{id}/messages/delta` is the unit.
Calendar uses `calendars/{id}/calendarView/delta`. Contacts use
`contactFolders/{id}/contacts/delta`. The trait expresses this as
`CursorScope::FolderType { folder: FolderId, ty: ObjectType }`:

- `ObjectType::Email` -> `mailFolders/{folder}/messages/delta`
- `ObjectType::Event` -> `calendars/{folder}/calendarView/delta`
  (calendar scopes use a calendar id where the trait says
  `folder: FolderId`; the trait's `FolderId` is the container
  identifier and is opaque to the engine)
- `ObjectType::Contact` -> `contactFolders/{folder}/contacts/delta`

`discover_cursor_scopes` enumerates folders per type and yields one
`CursorScope::FolderType` per (folder, type) pair. Cost is O(folders
x supported types); the engine caches the result and only re-runs
on `ScopeLifecycle` events from `scope_lifecycle_stream`.

`discover_memberships` enumerates the same folders as
`MembershipScope::Folder(_)`. `parentFolderId` from `GraphMailFolder`
populates the folder tree; the membership scope a message carries
is `MembershipScope::Folder(parentFolderId)` (singleton in
`InventoryEntry::memberships`). Engine derives `Destroyed` from the
per-object membership log per the trait contract; the protocol crate
emits `ScopeChange::Removed` when a delta page shows
`@removed.reason=changed` with a new `parentFolderId`, or
`@removed.reason=deleted` for hard-delete.

### Cursor payload format

The opaque `OpaqueChangeState::bytes` carries:

```rust
#[derive(Serialize, Deserialize)]
struct GraphCursorPayload {
    kind: GraphCursorKind,
    // The full @odata.deltaLink URL from the most recent
    // terminating page. Graph encodes folder + type + token inside
    // this URL; we do not parse it.
    delta_link: String,
    // Issued-at instant; capabilities pin
    // delta_token_expires_after = 30 days, so the engine knows when
    // to treat it as Expensive in describe_cursor.
    issued_at: SystemTime,
    // Optional mid-page resume marker. Set when we emit a
    // Checkpoint inside a Batch on a @odata.nextLink boundary;
    // None when the cursor advanced to a fresh @odata.deltaLink.
    advanced_through: Option<GraphPageMarker>,
}

#[derive(Serialize, Deserialize)]
enum GraphCursorKind {
    MessagesDelta { folder_id: String },
    EventsDelta { calendar_id: String },
    ContactsDelta { folder_id: String },
}

#[derive(Serialize, Deserialize)]
struct GraphPageMarker {
    next_link: String,         // the @odata.nextLink to resume on
    last_seen_id: Option<String>,
}
```

`advanced_through` is `Some(GraphPageMarker)` when the protocol
checkpoints mid-page, `None` once the page sequence terminates and
the next `delta_link` is the resumption point. Per `account-trait.md`
"advanced_through is protocol-owned": only the Graph impl interprets
`GraphPageMarker.next_link`. The engine persists the cursor and
hands it back verbatim.

### Envelope versioning

`OpaqueChangeState::envelope_version` ticks when the bytes layout
changes. Initial value 1. Bumps:

- `+1` when `GraphCursorKind` adds a variant (new object type
  surface). Older engines can still drive older variants; the
  engine treats unknown variants as `SchemaIncompatible` per the
  trait recovery vocabulary.
- `+1` when `GraphPageMarker` gains a load-bearing field that older
  resumers cannot ignore.
- No bump for additive optional fields read with `#[serde(default)]`.

The `cursor.rs` module exposes `migrate_v_n_to_v_n1` per bump where
forward migration is lossless; otherwise the read path returns
`RecoveryClass::SchemaIncompatible` and the engine restarts with a
fresh inventory.

`OpaqueChangeState::protocol = ProtocolKind::Graph`. Read path
rejects mismatched protocol tags before deserializing bytes.

## Trait method realization

### capabilities

`capabilities()` returns a cached `&AccountCapabilities` built once
at `open()`. Field values:

```rust
AccountCapabilities {
    cursor_freshness: CursorFreshness::Hybrid,
    // No inventory_is_change_cursor_establish field — see
    // plans/account-trait.md -> Cursor establishment.
    // Graph's delta endpoint, called without a token, paginates a
    // FULL sync of the folder before yielding @odata.deltaLink.
    // Subsequent calls with the deltaLink return incremental
    // changes. Cursor establishment is therefore identical to
    // inventory — same wire calls, same cost — so
    // establish_initial_cursor(scope) returns
    // CursorEstablishment::EstablishViaInventory for every Graph
    // scope. The engine runs inventory_stream first and reads
    // the cursor from its terminal Done event, matching the
    // IMAP-Basic / IMAP-CONDSTORE-only regime per
    // plans/bifrost-sync.md -> Inventory-fusion.

    blob_range: BlobRangeSupport::Conditional,
    // file attachments: $value supports Range; item attachments
    // return MIME/vCard/iCal bytes without Range; reference
    // attachments return 405. Per-handle truth lives in
    // BlobHandle.capabilities.supports_range; the conditional
    // flag tells the engine to consult it.
    blob_digest_pre_download: false,

    push: PushCapability::WebhookOrEwsStream,
    push_in_process: false,
    // True only when PushPath::EwsStreaming is selected. The
    // value the engine reads is the active path's value; capability
    // re-read on RecoveryClass::CapabilityChanged covers a switch
    // between paths.

    mutation: MutationCapabilities {
        concurrency: MutationConcurrency::StateBased,    // If-Match
        replay_safety: MutationReplaySafety::None,
        // Microsoft Graph documents neither JSON-batch `id` nor
        // `client-request-id` as a client-mintable dedup token:
        // the batch id is per-batch correlation only; the
        // client-request-id is debugging/support correlation
        // (per the dev-proxy troubleshooting docs). Ambiguous
        // transport failure resolves via the engine's read-back
        // guard (plans/bifrost-sync.md -> Read-back guard);
        // re-fetch via get_stream(Projection::FlagsOnly) and
        // reconcile applied vs not.
    },
    batching_policy: BatchingPolicy {
        max_items: 20,         // Graph $batch cap
        max_wait: Duration::from_millis(100),
        flush_on_input_close: true,
    },

    rate_limit_class: RateLimitClass::GraphTenant,
    quota_signal: QuotaSignal::RetryAfterHeader,

    requires_uidvalidity_recheck: false,
    historyid_expires_after: None,
    delta_token_expires_after: None,
    // Microsoft documents Outlook delta token lifetime as not
    // fixed — depends on server-internal token cache, can age out
    // in hours under load even though "typically" longer. A wall
    // clock value misleads the engine's scheduler. Expiry is an
    // event (410 Gone -> RestartScope), not a budget.
}
```

`push_in_process` is derived from the chosen `PushPath`. Switching
between webhook delivery (out-of-process) and EWS streaming
(in-process) is a capability transition: the protocol crate ends
active streams with `RecoveryClass::CapabilityChanged { delta }`
and the engine reopens. There is no live capability channel.

### describe_cursor

Any extant delta token is `CostClass::Cheap`: incremental delta
calls are O(changes), not O(folder). The prior draft's 24-hour /
30-day cost cliff was wrong — Graph delta token lifetime is not
deterministic per Microsoft docs (depends on server-internal cache,
can age out in hours under load). Expiry is an event (410 Gone →
`RestartScope`), not a budget the engine can plan against.

Strategy is `SyncStrategy::ServerCursor` for all extant tokens.
There is no "no cursor yet" case at the `describe_cursor` layer —
the cursor only exists after `inventory_stream`'s establishment
pass (see below), and `describe_cursor` is only called by the
engine on cursors it has persisted.

### discover_cursor_scopes / discover_memberships / scope_lifecycle_stream

`discover_cursor_scopes` must walk the **full mail-folder tree**,
not just root children. `/me/mailFolders` returns only top-level
folders per Microsoft's user-list-mailfolders docs; nested folders
(subfolders of Inbox, of Archive, of user-created folders) require
traversing `/me/mailFolders/{id}/childFolders` on each parent.
`GraphClient::list_mail_folders` in `crates/graph/src/api.rs:17`
today calls `/mailFolders` once — that's the root-only primitive,
not the recursive walk this method needs. A new helper
`list_mail_folders_recursive` lands alongside; it walks
breadth-first using `childFolderCount` to skip empty subtrees.

```text
fn discover_cursor_scopes():
    mail_folders   = list_mail_folders_recursive().await?;
    calendars      = list_calendars().await?;
    contact_folders = list_contact_folders().await?;
    // (Calendars and contact folders are typically flat in
    // Outlook tenants; revisit if shared/nested calendar trees
    // need the same recursive walk.)
    for folder in mail_folders:
        yield CursorScope::FolderType { folder: folder.id, ty: Email };
    for calendar in calendars:
        yield CursorScope::FolderType { folder: calendar.id, ty: Event };
    for folder in contact_folders:
        yield CursorScope::FolderType { folder: folder.id, ty: Contact };
```

Batches sized per `BatchingPolicy`; stream terminates with `Done`.
Cost is O(folders) including nested mail folders, depth-bounded by
the tenant's tree (Outlook typically <5 levels). The walk is done
once per attach and cached on `cursor_index`; subsequent calls
read the cache. Invalidation is driven by `scope_lifecycle_stream`
observing new/deleted folders.

`discover_memberships` yields one `MembershipScope::Folder` per
folder across all containers (mail + calendar + contact, all
nested). Independent stream because membership scopes can be
coarser than cursor scopes (one folder, multiple typed cursors).

`scope_lifecycle_stream` is polling-only on Graph. Microsoft's
Outlook change-notification docs cover `messages`, `events`, and
`contacts` resources; **`mailFolders` container lifecycle is not
a subscribable resource**, so the prior draft's webhook path for
folder create/rename/delete was unsupported. The stream drives:

1. `/mailFolders/delta` on the engine's adaptive cadence
   (default 30s, doubling on no-change, capped at 30min per
   `bifrost-sync.md`). Response items map to
   `ScopeLifecycle::Created` (new id), `Renamed` (id present,
   displayName changed), `Deleted` (removed from delta).
2. `/me/calendars` + `/me/contactFolders` periodic listing
   (no delta endpoint for these container types; full re-list
   on a slower 5-minute cadence, diff against prior snapshot).

EWS lifecycle events on the EWS streaming path can also feed
folder events when available, but only as an optimization layered
over the polling truth source; the polling cadence is what
guarantees eventual consistency.

### inventory_stream

On Graph, `inventory_stream` IS the cursor-establishing pass.
Microsoft's message-delta docs describe the first call (no token)
as a paginated full sync of the folder, with `@odata.deltaLink`
appearing only on the terminating page. Cursor establishment and
inventory are the same wire calls; the engine fuses them per
`plans/bifrost-sync.md` -> Multiplexer fusion (matching the
IMAP-Basic / IMAP-CONDSTORE-only regime). There is no separate
"delta from now" API on Graph that would let the engine skip the
full sync.

`inventory_stream(scope: CursorScope::FolderType { folder, ty })`:

- For `Email`: `GET /mailFolders/{folder}/messages/delta?$select=
  id,parentFolderId,internetMessageId,subject,
  receivedDateTime,isRead,categories,flag,changeKey,
  conversationId,internetMessageHeaders` paginated via
  `@odata.nextLink`, terminating with `@odata.deltaLink`. Each
  page becomes one `Batch<InventoryEntry>`. Note: `size` is NOT
  in `$select`. Microsoft Graph's message resource does not
  expose message size (the existing parser at
  `crates/graph/src/parse.rs:188` already records `raw_size: 0`
  for the same reason); selecting it returns 0 / undefined.
- For `Event`: `GET /calendars/{folder}/events/delta` with the
  calendar `$select` from `calendar_sync.rs`. Same delta-walk
  shape.
- For `Contact`: `GET /contactFolders/{folder}/contacts/delta`
  with the existing `CONTACT_SELECT` set in `types.rs`. Same
  shape.

`InventoryEntry::memberships = vec![MembershipScope::Folder(folder)]`
(singleton). `fingerprint = Fingerprint { server_version:
ServerVersion::ETag(change_key), flags_hash }` — **no size
field**. The ETag is the `changeKey` on Graph messages/events;
contacts ship `@odata.etag`. `flags_hash` canonicalizes Graph's
flag-like fields: `isRead`, `flag.flagStatus`, `categories`
(sorted, lowercased) hashed with FNV-1a per the trait's
within-scope canonicalization rule.

Threading headers: `message_id` from `internetMessageId`,
`references` and `in_reply_to` parsed out of
`internetMessageHeaders` by name (Graph does not expose them
structured). `thread_id = Some(ThreadId(conversation_id))` flagged
as opaque-within-protocol per the trait note about Graph
`conversationId` not being comparable across protocols.

**Cursor establishment.** When the delta walk terminates with
`@odata.deltaLink`, the terminal `SyncEvent::Done` carries the
`OpaqueChangeState` encoding the deltaLink as
`GraphCursorPayload { kind, delta_link, issued_at: now,
advanced_through: None }`. The engine persists this cursor via
`CheckpointStore::put_change_cursor` before honoring `Done`.
Subsequent `changes_stream(cursor)` calls drive incremental
deltas cheaply (see below).

Mid-walk crash recovery uses `BackfillCheckpoint` markers per
`@odata.nextLink` boundary (separate from the change cursor):
the engine resumes from the persisted nextLink, completes the
walk, and only then registers the change cursor from the
terminal deltaLink. Per
`plans/sync-engine.md` -> Checkpoint atomicity invariant, the
final batch's `(items, change_cursor_checkpoint)` is one
transaction.

### get_stream

`get_stream(ids, projection)`: collect ids into batches of 20 and
issue a `POST /$batch` (already typed in
`types.rs::BatchRequest/BatchResponse`) where each inner request is
`GET /messages/{id}?$select=...` widened per projection
(`Projection::Headers` -> the inventory select; `Projection::Full`
adds `body,attachments`; `Projection::FullWithBlobs` triggers
follow-up `attachments` enumeration but blob bytes still come
through `open_blob`). Per-item failures inside the batch surface as
`Warning` per the trait, not as a stream `Fatal`.

### changes_stream

Preconditions: `changes_stream(cursor)` requires a cursor that was
established by a prior `inventory_stream` pass (terminal
`@odata.deltaLink`). The engine never constructs a "fresh" Graph
cursor out of band — there is no cheap-establishment primitive on
Graph (`establish_initial_cursor(scope)` returns
`EstablishViaInventory` for every Graph scope).

1. Validate `cursor.server_state.protocol == ProtocolKind::Graph`
   and `envelope_version` is migratable; reject otherwise with
   `SchemaIncompatible`.
2. Deserialize `GraphCursorPayload`. If `advanced_through` is `Some`,
   resume on `advanced_through.next_link`; else use `delta_link`.
3. Drive page-by-page. Each page is a `Batch<Change>`:
   - Items become `ObjectChange::Created` for new ids,
     `ObjectChange::Updated` for in-place changes, and
     `ScopeChange::Removed` from `@removed.reason=deleted` or
     `parentFolderId` shift. The engine derives `Destroyed` per
     `account-trait.md` -> Change emission contract.
   - `Checkpoint` emission rules: `Some(Checkpoint)` on each
     `@odata.nextLink` boundary with
     `advanced_through = Some(GraphPageMarker { next_link, ... })`.
     `Some(Checkpoint)` on the terminating `@odata.deltaLink`
     boundary with the new `delta_link` baked into
     `cursor.server_state` and `advanced_through = None`. The
     engine persists `(items, checkpoint)` atomically per
     `sync-engine.md` -> Stream contract.
4. On `410 Gone` or `400 InvalidDeltaToken`, end the stream with
   `RecoveryClass::RestartScope(scope)`; the engine clears the
   cursor and restarts with a fresh inventory. On `429`, end with
   `RecoveryClass::Retry { after }` honoring `Retry-After`.

Calendar delta needs the `calendarView/delta` URL shape and a
`startDateTime/endDateTime` window on the first call; subsequent
calls use the issued delta link verbatim. The window is engine-
managed via `BackfillStrategy::TimeWindowed`; the protocol crate
accepts a `time_window: Option<(DateTime, DateTime)>` parameter on
the calendar scope (carried inside `CursorScope::FolderType` is
insufficient; the calendar-specific scope param lives in
`GraphCursorPayload` for the very first call and is then absorbed
into the issued delta link).

### push_subscribe / push_unsubscribe

Trait signature: `push_subscribe(&self, scopes: &[CursorScope]) ->
SubscriptionHandle`. Graph maps as follows.

#### Path A: Graph /subscriptions (default; `push_in_process = false`)

`push_subscribe`:

1. Group the requested `CursorScope::FolderType` entries by
   `(resource, ty)` where the **resource** depends on the type:
   - `Email`: `/me/mailFolders/{folder}/messages` (per-folder
     resource is supported for messages).
   - `Event`: `/me/events` (account-wide). Per Microsoft's
     Outlook change-notification docs, event subscriptions are
     listed at `/me/events` or `/users/{id}/events`; there is no
     per-calendar event subscription path. A single subscription
     covers events across all calendars, and the resulting hint
     must therefore reach all `CursorScope::FolderType { ty:
     Event, .. }` cursor scopes — the receiver maps the inbound
     notification to `HintPayload::SpecificCursorScope` per
     registered calendar (or `Unknown` if the cursor index is
     uncertain), and the engine's reconciler fans out.
   - `Contact`: `/me/contactFolders/{folder}/contacts` (per-folder
     is supported).
2. For each group, `POST /subscriptions` with body assembled per
   `webhooks.rs::GraphSubscription`:
   - `change_type = "created,updated,deleted"`.
   - `resource` per the mapping above.
   - `notification_url = self.push_endpoint.webhook_url`.
   - `expiration_date_time` = now + `DEFAULT_EXPIRATION_MINUTES`
     (24h; max 4230min for messages per the existing constant).
   - `client_state` = freshly minted 16-byte hex secret. Stored on
     `GraphSubscriptionState.client_state` and used by the engine's
     out-of-process webhook receiver to validate notifications
     before pushing them into `InvalidationSink`.
3. Mint a `SubscriptionHandle` (UUID), record
   `GraphSubscriptionState { server_id, resource, client_state,
   expires_at, scopes }` in `PushPath::GraphSubscriptions::active`,
   return the handle. For the event subscription, `scopes`
   contains the full set of `CursorScope::FolderType { ty: Event }`
   the consumer asked about — the receiver's hint-fanout uses
   this set.

Renewal is a separate background task driven by `push_renewer.rs`
(spawned from `open()`): every minute it scans
`PushPath::GraphSubscriptions::active`, calls
`webhooks::check_and_renew_subscriptions` for entries within the
30-minute renewal window. Failures bubble up as
`InvalidationHint::Disconnected` followed by `Reconnected` once
renewal succeeds; permanent failure (auth lost, tenant policy)
ends the stream with `RecoveryClass::CapabilityChanged` so the
engine reopens, re-discovers `push_in_process`, and potentially
falls back to the EWS path.

`push_unsubscribe(handle)`:

1. Look up `GraphSubscriptionState.server_id`.
2. `DELETE /subscriptions/{server_id}`. Treat `404 Not Found` as
   success per the existing `webhooks::delete_subscription`
   behavior; the server may have aged the subscription out.
3. Remove the handle from `active`.
4. Idempotent: missing handles return `Ok(())`.

`push_stream()` returns an empty stream on Path A. Out-of-process
webhook receivers feed `InvalidationSink` per `sync-engine.md`. The
engine's `push::reconciler` then calls `account.changes_stream(cur)`
for the affected scopes.

`HintPayload` produced by the webhook receiver (out of band from
this crate): the receiver matches `subscription_id` to the
`GraphSubscriptionState.scopes` list and emits
`HintPayload::SpecificCursorScope(scope)`. The receiver crate
imports the `GraphSubscriptionState` mapping via a small store
interface the engine provides; the mapping itself is owned here.

#### Path B: EWS streaming notifications (`push_in_process = true`)

Selected when `push_endpoint` is `None` or when tenant policy blocks
Graph `/subscriptions` (e.g. some on-prem hybrid configs, restricted
app registrations). The crate detects this either by configuration
(consumer pins `PushPath::EwsStreaming` at factory time) or by
failed `POST /subscriptions` with `403` / unsupported tenant errors
ending `RecoveryClass::CapabilityChanged` so reopen picks the EWS
path. EWS endpoint is the existing `EWS_URL` const in `ews/mod.rs`.

EWS streaming wire flow:

1. `Subscribe` SOAP request creates a `StreamingSubscription` over
   selected folder ids and event types
   (`NewMail/Created/Deleted/Modified/Moved/Copied`). Returns a
   `SubscriptionId`.
2. `GetStreamingEvents` SOAP request opens a long-poll HTTP
   connection with `ConnectionTimeout` up to 30 minutes (server cap
   per `plans/graph/streaming.md`). Server pushes a sequence of
   notification XML chunks down the connection; the client parses
   them incrementally with `quick-xml` (the dep is added per the
   streaming notes).
3. Each parsed notification becomes a `WatchEvent::Invalidated {
   hint: InvalidationHint { source: PushSource::EwsStreaming,
   payload: HintPayload::SpecificCursorScope(scope) } }` and is
   broadcast via `self.push_tx`. `push_stream()` returns
   `BroadcastStream::new(self.push_tx.subscribe())` mapped through
   `WatchEvent`. The multiplexer reads it like IMAP IDLE.
4. Reconnect on connection close: emit `Disconnected`, sleep with
   capped exponential backoff (start 1s, max 60s, +-25% jitter),
   reissue `GetStreamingEvents` on the same `SubscriptionId` if it
   is still valid (server can age it out at 30 minutes), else
   re-`Subscribe` and emit `Reconnected`. Watermarks (`Watermark`
   element on each notification) are persisted on
   `EwsSubscriptionState.watermark` and replayed on reconnect via
   the `Subscribe`'s previous-watermark field.
5. Scope mapping is dense: EWS notifications carry
   `ItemId.ChangeKey` and `ParentFolderId` directly; the parser
   produces `(FolderId, ObjectType::Email)` for the
   `HintPayload::SpecificCursorScope` without needing a separate
   resolver step.

`push_subscribe` on Path B starts the EWS worker if not running,
adds scopes to `EwsSubscriptionState.scopes`, returns a handle.
`push_unsubscribe` removes scopes and sends `Unsubscribe` only when
the last scope drops (the EWS subscription is per-folder-set on the
wire, but we keep one logical worker and modify its scope set
across handles).

Lifetime cap: 30 minutes for the server-side `GetStreamingEvents`
HTTP connection; the worker silently restarts inside that lifetime
without surfacing `Disconnected/Reconnected` (those are reserved
for outages that the engine could observe externally).

The two push paths are mutually exclusive per account. A consumer
that wants both (e.g. webhook for messages + EWS streaming for
public-folder mail) splits the account into two `Account` impls;
that is a future enhancement, not v1.

### push_stream

Returns `AccountStream<WatchEvent>` per the trait. On Path A
(webhook delivery, `push_in_process = false`), the stream is
empty — it yields no items and simply terminates. Out-of-process
push events reach the engine through `InvalidationSink`, not
through this stream; the engine reads health and invalidations
from the sink, never from `push_stream` on this path. On Path B
(EWS streaming, `push_in_process = true`) the stream yields the
EWS worker's `WatchEvent`s (Invalidated / Disconnected /
Reconnected) until the worker exits or `close()` trips. There is
no `SyncEvent::Done` sentinel — `AccountStream<WatchEvent>` yields
`WatchEvent` items directly and termination is the natural
stream-end.

### open_blob / open_blob_range

`open_blob(BlobHandle)`:

Per Microsoft's attachment-get docs, `$value` is documented for
both file AND item attachments. Per-type bytes shape:

- **File attachments** (`#microsoft.graph.fileAttachment`):
  `GET /messages/{message_id}/attachments/{attachment_id}/$value`
  returns the raw file bytes with the attachment's
  `contentType`. Bytes stream directly. The existing
  `client::get_attachment_bytes` is a one-shot variant; the
  streaming impl uses `reqwest::Response::bytes_stream()` and
  wraps it as `AccountStream<SyncEvent<Bytes>>`.
- **Item attachments** (`#microsoft.graph.itemAttachment`):
  `$value` returns the embedded item as MIME (for mail), vCard
  (for contact), or iCal (for event), per Microsoft's
  attachment-get response examples. The protocol emits these
  bytes verbatim with the appropriate `content_type`
  (`message/rfc822`, `text/vcard`, `text/calendar`). Consumers
  treat them as opaque byte streams; parsing into structured
  items is out of scope for the Account trait. The prior
  draft's "$value returns 400 for item attachments" claim was
  incorrect — it conflated reference attachments with item
  attachments.
- **Reference attachments** (`#microsoft.graph.referenceAttachment`):
  `$value` returns `405 Method Not Allowed` per Microsoft's
  docs (the bytes don't exist on the Graph server; the
  attachment is a link to OneDrive/SharePoint). Emit
  `Warning { kind: BlobNotByteStream }` and terminate the
  stream cleanly. Consumers wanting reference bytes follow the
  attached `sourceUrl` separately.
- `BlobHandle::capabilities.supports_range = true` only for
  file attachments. Item attachments stream their MIME/vCard/iCal
  body without `Range` support (Graph documents `Range` on
  `$value` for file content; item attachment responses do not
  advertise it). Reference attachments have no bytes at all.
  `Conditional` at the account level forces the engine to read
  the per-handle flag before issuing a range request.

`open_blob_range(handle, ByteRange { start, length })`: file
attachments only. Adds `Range: bytes={start}-{end}` header to the
`$value` GET. The crate validates `Content-Range` on the response
matches the requested window before pushing bytes downstream, per
`sync-engine.md` -> Body fetch (parallel range assembly correctness).
Range on non-file attachments fails synchronously with `Error::
RangeNotSupported`; the engine falls back to a single-stream
`open_blob` call.

### bulk_set_flags / bulk_move / bulk_destroy

Graph PATCH on a message updates `isRead`, `flag.flagStatus`,
`categories`, `importance`. The existing `GraphMessagePatch` in
`types.rs` is the body shape. Move is `POST /messages/{id}/move`
with `GraphMoveRequest`. Destroy is `DELETE /messages/{id}`.

Streaming implementation:

1. Buffer the input `AccountStream<ObjectId>` into batches sized
   per `BatchingPolicy::max_items = 20`, with `max_wait = 100ms`,
   `flush_on_input_close = true`.
2. For each batch, build a `BatchRequest` (the existing type in
   `types.rs`) with one inner request per id. Each inner request:
   - `method = "PATCH"` (set_flags) or `"POST"` (move) or
     `"DELETE"` (destroy).
   - `url = "/messages/{id}"` or `/move` suffix.
   - `headers = { "If-Match": etag_from_inventory }` —
     StateBased concurrency. The engine supplies the expected ETag
     out of the cursor's most recent `Fingerprint::ServerVersion::
     ETag` snapshot; bulk_* methods accept an `etag_oracle` closure
     of type `Fn(&ObjectId) -> Option<String>` so the protocol
     crate does not need to re-fetch.
   - **No client-mintable replay-token header.** The prior draft
     posited `client-request-id` as a dedup primitive, but
     Microsoft documents it as debugging/support correlation only
     (per the dev-proxy troubleshooting docs). The JSON-batch
     `id` field is per-batch request/response correlation, also
     not a dedup key. Graph has no documented replay token for
     mutation; ambiguous transport failure is resolved by the
     engine's read-back guard (`plans/bifrost-sync.md` ->
     Read-back guard).
3. `POST /$batch` and parse `BatchResponse`. Per-item outcomes:
   - `2xx` -> `MutationOutcome::Applied`.
   - `412 Precondition Failed` -> `MutationOutcome::Skipped`
     (ETag mismatch; engine re-reads and resubmits with fresh ETag).
   - `404 Not Found` on destroy -> `MutationOutcome::Skipped`
     (already gone; idempotent).
   - `429 Too Many Requests` -> end the batch with
     `RecoveryClass::Retry { after }` honoring the inner
     `Retry-After`. The engine resubmits the same batch body on
     retry; the read-back guard reconciles any in-flight applies.
   - Other 4xx/5xx -> `MutationOutcome::Failed(Error)`.
4. Emit one `Batch<MutationResult>` per `$batch` round-trip with a
   `Checkpoint` boundary (engine persists `(results, checkpoint)`
   so retried-but-applied mutations are not re-issued post-crash —
   the read-back guard provides the final ground truth).

`bulk_destroy` is the same shape; soft-delete (move to Deleted Items)
is the trait's default semantic. Hard-delete is a separate trait
extension not addressed here.

### close

`close(&self)` is idempotent local handle teardown per
`account-trait-shape.md` Q3:

1. Trip `self.shutdown`.
2. Abort the EWS streaming worker if running. Best-effort
   `Unsubscribe` SOAP on its `SubscriptionId`; ignore errors.
3. Drop in-flight `reqwest` requests via `tokio_util::sync::
   DropGuard` on the cancellation token (the existing client
   threads cancellation tokens through `get_bytes`-style calls).
4. Does NOT call `DELETE /subscriptions/{id}` on webhook
   subscriptions. Server-side handles are durable; engine destroys
   them via explicit `push_unsubscribe` on account removal.

Safe to call more than once. Composes with `Arc<dyn Account>`.

## Concurrency model

`GraphAccount` is `Send + Sync`. The trait-method futures and
streams hold `Arc<ClientInner>` (cheap clone). No per-method mutex
beyond the existing `category_lock: Mutex<()>` on `GraphClient` (a
narrow serialization gate around `outlook/masterCategories` writes;
unrelated to Account-trait dispatch).

The 3-permit `Semaphore` inside `GraphClient` already caps in-flight
HTTP. The engine's per-account `ConcurrencyBudget` sits on top per
`bifrost-sync.md`. There is no contention between trait-method
callers beyond what the semaphore enforces.

## Recovery mapping

Mapping from `bifrost-graph` errors and HTTP responses to
`RecoveryClass`:

- `429 Too Many Requests` with `Retry-After` -> `Retry { after }`.
- `503 Service Unavailable`, `504 Gateway Timeout` -> `Retry { after
  = backoff(attempt) }`.
- `410 Gone` on delta -> `RestartScope(scope)`.
- `400 Invalid delta token`, `400 SyncStateNotFound` ->
  `RestartScope(scope)`.
- `401 Unauthorized` after one token refresh attempt -> `AuthLost`.
- `403 Forbidden` on subscription create -> end change streams with
  `CapabilityChanged { delta: removed PushPath::GraphSubscriptions,
  added PushPath::EwsStreaming }`. Engine reopens; capabilities
  pick the EWS path.
- `403 Forbidden` on EWS too -> end with `CapabilityChanged
  { delta: removed all push paths }`. Engine reopens with
  `PushCapability::None`; only polling cadence remains.
- `404` on a folder mid-delta -> `ScopeLifecycle::Deleted` followed
  by `RestartScope(scope)` to clear the cursor cleanly.
- `412 Precondition Failed` on PATCH -> per-item
  `MutationOutcome::Skipped`; the engine re-reads and retries with a
  fresh ETag. Not a stream-ending error.
- Network errors mid-stream -> `Retry { after = jittered_backoff }`.

`CapabilityChanged` is the load-bearing recovery class: switching
push paths is a capability transition, not an internal state
shuffle, because `push_in_process` is observed by the engine.

## Risks / Opens

- **Subscription resource granularity.** Graph subscriptions on
  `/me/messages` (account-wide) vs `/me/mailFolders/{id}/messages`
  (per-folder) differ in scaling: there is a tenant-wide cap on
  active subscriptions per app. v1 default is per-folder; a future
  optimization uses one account-wide subscription and maps the
  notification's resource path back to scopes on the receiver side.
  Open until we have telemetry on cap pressure.

- **Lifecycle notifications.** Graph supports a separate
  `lifecycleNotificationUrl` for `subscriptionRemoved`,
  `missed`, and `reauthorizationRequired` events. v1 omits this
  (the existing `webhooks.rs` create call sets it to `None`); v2
  needs to wire it so the engine can react to `missed` with a
  forced reconcile instead of waiting on renewal. Open: does
  bifrost-graph own the lifecycle URL, or does the consumer's
  receiver crate?

- **EWS streaming on Exchange Online deprecation.** Microsoft has
  publicly deprecated EWS for cloud tenants; the streaming path
  works today and through the deprecation window but is not a
  permanent fallback. When EWS is retired, the only push paths
  left are Graph `/subscriptions` and polling. Track the
  retirement date and revisit before then.

- **`changeKey` semantics.** Graph `changeKey` is an opaque ETag
  that advances on any server-side mutation, but Outlook clients
  routinely modify items with `Prefer: outlook.allow-unsafe-html`
  or similar headers that bump `changeKey` without semantic
  change. `MutationConcurrency::StateBased` works regardless, but
  it can cause spurious `412` cycles. Engine policy on the retry
  cap is the mitigation; tune defaults after first production runs.

- **Subscription-health surfacing for webhook path.** Sustained
  renewal failure today degrades silently into poll-only behavior
  on the affected account. Same gap Gmail has with Pub/Sub; the
  trait extension that lands `push_health_stream` (or equivalent)
  should cover both. Cross-protocol coordination.

- **Calendar/contact folder recursion.** The discovery walk
  currently treats calendars and contact folders as flat. Outlook
  tenants with shared/nested calendar trees may need a recursive
  walk similar to mail folders. v1 ships flat; revisit if
  production reveals nested calendar/contact trees.

- **Message size in fingerprint.** Microsoft Graph does not
  expose `size` on the message resource (the existing parser at
  `crates/graph/src/parse.rs:188` records `raw_size: 0` for this
  reason). The fingerprint omits size on Graph. If a consumer
  needs message size for UI or quota tracking, they hydrate the
  body and measure client-side, or accept that Graph cannot
  provide it.

- **Calendar scope window.** `calendarView/delta` requires an
  initial date window; once issued, the delta link carries it.
  The window passes through `GraphCursorPayload` on the first
  call. If the engine ever needs to widen the window (user scrolls
  calendar further into past/future), the cursor must restart -
  there is no widen-window-in-place operation. Surface this as
  `RestartScope` rather than `Updated`.

- **Conversation id opacity.** Trait note: `conversationId` does
  not cross protocols. Confirmed; the impl never compares Graph
  thread ids against JMAP or Gmail. Threading is consumer-side
  via RFC 5322 headers carried in `InventoryEntry`. Open: does
  the consumer's threading code want a stable per-account
  conversation id for UI ("group by Outlook thread")? If yes,
  surface it as a metadata field, not a cross-protocol identifier.

- **Shared mailbox account identity.** `GraphClient::
  for_shared_mailbox` already exists; one `GraphAccount` per
  shared mailbox is the model. Engine multiplexes them as
  separate accounts. The `AccountFactory` for a shared mailbox
  wraps the primary account's token. Open: do we need a single
  factory that fans out to N shared-mailbox accounts, or does the
  consumer register them individually?

- **Batch partial throttling.** When `$batch` returns `200 OK`
  but a subset of inner responses are `429`, we currently treat
  the batch as success and surface per-item retries. This loses
  the global `Retry-After` advice from the inner responses.
  Aggregating to a single `RecoveryClass::Retry` if more than half
  the inner items are `429` is a future heuristic. v1 emits
  per-item `Failed` and lets the engine's retry policy on the
  next batch absorb the backoff.

- **Watermark persistence on EWS reconnect.** EWS streaming
  watermarks are short-lived (server can refuse them after
  ~24h). Persisting them across process restarts is futile; on
  cold start the worker issues a fresh `Subscribe` and accepts a
  small notification gap. The first post-cold-start `changes_stream`
  pass closes the gap. Document this in the EWS module docstring.
