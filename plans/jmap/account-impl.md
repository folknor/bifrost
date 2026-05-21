# bifrost-jmap Account trait implementation

Implementation plan for the `Account` trait (defined in
`plans/account-trait.md`, dispatch shape locked in
`plans/account-trait-shape.md`) against `bifrost-jmap`. This is a
binding map from trait surface to the existing crate primitives in
`crates/jmap/src/`, not a re-spec of the trait or the engine.

Scope: what code lands where, what state the `JmapAccount` struct
holds, how each trait method is realized on top of `Client`,
`Account<Tr>`, the typed method structs (`EmailGet`, `EmailSet`,
`EmailChanges`, `EmailQuery`, `EmailQueryChanges`,
`MailboxGet/Changes`, `PushSubscription{Get,Set}`), the WebSocket
transport in `client_ws.rs`, and the blob download surface in
`blob/download.rs`.

## Crate layout

The trait impl lives behind a `sync` feature on `bifrost-jmap` so
consumers that only want the wire client do not pull in the
`bifrost-types` trait surface. `bifrost-jmap` never depends on
`bifrost-sync` (the engine); only on `bifrost-types`, per the
workspace carve-out in `plans/orchestration.md` and
`plans/bifrost-sync.md` -> Workspace policy. Adds one subtree:

```
crates/jmap/src/sync/
  mod.rs            // re-exports; JmapAccountFactory entry point
  account.rs        // JmapAccount struct, Account trait impl
  state.rs          // OpaqueChangeState <-> JmapCursorState serde
  capabilities.rs   // capabilities() construction from Session
  discover.rs       // discover_cursor_scopes, discover_memberships,
                    // scope_lifecycle_stream
  inventory.rs      // inventory_stream: Email/query + Email/get pages
  changes.rs        // changes_stream: Email/changes + queryChanges
  hydrate.rs        // get_stream: Email/get with Projection mapping
  push.rs           // push_subscribe/unsubscribe + push_stream WS
  blob.rs           // open_blob, open_blob_range
  mutation.rs       // bulk_set_flags, bulk_move, bulk_destroy
  error.rs          // problem-doc + MethodErrorType -> RecoveryClass
  factory.rs        // JmapAccountFactory: AccountFactory impl
```

No protocol surface in `bifrost-jmap` changes. The impl consumes
the existing `Client<Tr>`, `Account<Tr>`, and method structs
through their public APIs.

## JmapAccount struct

```rust
pub struct JmapAccount {
    // Active typed accounts. Hot-swap on
    // RecoveryClass::CapabilityChanged is handled by the engine's
    // AccountSlot (per bifrost-sync.md: ArcSwap<Arc<dyn Account>>);
    // inside this struct the fields are plain. Reopen mints a fresh
    // JmapAccount via JmapAccountFactory::open, and the engine swaps
    // the outer Arc atomically.
    mail: Account<ReqwestTransport>,        // primary_account::<Mail>
    submission: Option<Account<ReqwestTransport>>,
    cal: Option<Account<ReqwestTransport>>,
    contacts: Option<Account<ReqwestTransport>>,

    // Cached capability summary (CoreCapabilities + per-cap limits)
    // computed once at open. Engine reads via Account::capabilities()
    // by reference, so this is owned data, not a snapshot.
    caps: AccountCapabilities,
    core_limits: CoreLimits,                // see below

    // WebSocket: shared sender; in-process push fan-out.
    ws: WsState,                            // see push.rs

    // Push subscription bookkeeping: handle -> registered DataType
    // set. WebSocket push is enabled via WebSocketPushEnable
    // (RFC 8887 §5), which is a "set"-semantics primitive: the
    // server starts pushing exactly the types listed and forgets the
    // previous set. So subscribe/unsubscribe must recompute the
    // union of all handles' DataTypes and reissue
    // enable_push_ws(union) on every change. No server-side
    // PushSubscription/* row is created; the WS connection itself is
    // the subscription. close() tears down the WS path locally.
    subscriptions: tokio::sync::Mutex<HashMap<SubscriptionHandle,
                                              HashSet<DataType>>>,

    // Local shutdown signal. close() trips this; downstream tasks
    // (WS reader, push fan-out) observe via WaitForCancellation.
    // Idempotent.
    shutdown: tokio_util::sync::CancellationToken,

    // Atomic flag so close() is observably idempotent without
    // taking the WS mutex on every call.
    closed: AtomicBool,
}

struct CoreLimits {
    max_calls_in_request: usize,    // caps batch fan-out
    max_objects_in_get: usize,      // caps Email/get id-window
    max_objects_in_set: usize,      // caps Email/set update-window
    max_size_request: usize,        // body-size guard pre-send
}
```

The struct is `Send + Sync` by composition (everything inside is).
There is no per-method `&mut self` state on the trait surface, so
`Arc<JmapAccount> as Arc<dyn Account>` falls out.

## OpaqueChangeState wire shape

Per `account-trait-shape.md` Q1, cursor state is a concrete tagged
blob. JMAP carries one `StateString` per cursor scope. Serialize
via bincode (no JSON, no field renames to maintain):

```rust
// crates/jmap/src/sync/state.rs
const ENVELOPE_VERSION_V1: u32 = 1;

#[derive(serde::Serialize, serde::Deserialize)]
enum JmapCursorState {
    V1 {
        scope: JmapScopeRepr,           // Email | Mailbox | Query(qid)
        state_string: String,           // raw JMAP State
    },
}

fn encode(state: &JmapCursorState) -> OpaqueChangeState {
    OpaqueChangeState {
        protocol: ProtocolKind::Jmap,
        envelope_version: ENVELOPE_VERSION_V1,
        bytes: bincode::serialize(state).expect("infallible serde"),
    }
}

fn decode(raw: &OpaqueChangeState) -> Result<JmapCursorState, Error> {
    if raw.protocol != ProtocolKind::Jmap {
        return Err(Error::CursorProtocolMismatch);
    }
    if raw.envelope_version != ENVELOPE_VERSION_V1 {
        return Err(Error::CursorEnvelopeUnknown);
    }
    bincode::deserialize(&raw.bytes).map_err(...)
}
```

`envelope_version` ticks when the in-bytes shape of
`JmapCursorState` changes: a new variant, a renamed field, a
narrowed type. The crate exposes a `MIN_MIGRATABLE` constant
matching `bifrost-sync`'s migration window; a `migrate_v1_to_v2`
helper lives next to the enum when v2 lands.

`advanced_through` is `None` for the JMAP impl in v1.
`Email/changes` cannot resume mid-page (the `state` string is the
page boundary, but each response IS a fresh boundary — see
"changes_stream" below for the per-response checkpoint rule).
`Email/queryChanges` has no mid-result resumption primitive in
RFC 8621 §4.5; resume is from `sinceQueryState` alone, and on
`CannotCalculateChanges` the impl falls back to a fresh
`Email/query` + client-side diff. Document this in the module
comment; revisit if a JMAP server ever exposes intra-page cursors.

## CursorScope mapping

```rust
// CursorScope::Type(ObjectType::Email) -> Email/changes
// CursorScope::Type(ObjectType::Mailbox) -> Mailbox/changes
// CursorScope::Type(ObjectType::Thread) -> Thread/changes
// CursorScope::Query(qid) -> Email/queryChanges keyed by qid
```

`ObjectType` is the engine's protocol-neutral enum
(`plans/sync-engine.md`). The JMAP impl supports `Email`,
`Mailbox`, and `Thread` in v1; `EmailSubmission`, `Calendar`,
`AddressBook` follow once those modules grow inventory passes.

`CursorScope::Account`, `Folder(_)`, and `FolderType{..}` are
never produced by `discover_cursor_scopes()` on JMAP - they map to
Gmail/IMAP/Graph. If `changes_stream` is invoked with one, return
a `Fatal::Unsupported`; the engine should never construct one for
a JMAP account, this is a defensive check.

The `discover_cursor_scopes` stream yields exactly the type-cursor
scopes the account advertises a capability for, plus one
`CursorScope::Query(_)` per consumer-registered query (registered
through a separate engine API not in scope here). For v1, the
implementation yields `Type(Email)` unconditionally,
`Type(Mailbox)` if `MailCapabilities` is in the session,
`Type(Thread)` likewise, and terminates with `Done`.

## MembershipScope mapping

JMAP `Email.mailboxIds` is a set per RFC 8621; a message can sit
in multiple mailboxes (Gmail-backed JMAP servers project labels
as mailboxes). Mapping:

```rust
// MembershipScope::Mailbox(MailboxId)  // canonical case
// MembershipScope::Query(QueryId)      // for registered queries
```

`discover_memberships()` calls `Mailbox/get` (no `ids` -> fetch
all), pages on `maxObjectsInGet`, emits one
`Batch<MembershipScope>` per page, then `Done`. No
`MembershipScope::Folder`/`Label` is ever produced.

`InventoryEntry::memberships` is populated from `mailboxIds`
verbatim. The keys of the JMAP `mailboxIds: {id: true}` map
become `Vec<MembershipScope::Mailbox(_)>`.

`scope_lifecycle_stream()` is fed by `Mailbox/changes`: a
mailbox-changes poll loop (in v1, driven by either the WebSocket
push wakeup or the engine's reconciler) emits `ScopeLifecycle`
`Created` from `created`, `Renamed` from `updated` where `name`
changed, `Deleted` from `destroyed`. Renames need a parallel
`Mailbox/get` on the updated ids to read the new name; the
implementation batches both into one request via `Request::call`
with a result reference (`#ids`).

## Capabilities

Hard-coded per `plans/account-trait.md` line 553 plus session
data:

```rust
// crates/jmap/src/sync/capabilities.rs
fn build(session: &Session) -> Result<(AccountCapabilities, CoreLimits), Error> {
    // Missing or malformed urn:ietf:params:jmap:core is fatal: every
    // size cap on the trait is derived from CoreCapabilities, and
    // Default produces zero limits which feed batching_policy
    // max_items = 0 (degenerate, cannot drive Email/get or Email/set).
    let core = session.typed_capability::<core::Core>()
        .ok_or(Error::MissingCoreCapability)?;

    let ws_present = session.websocket_capabilities()
        .map(|w| w.supports_push())
        .unwrap_or(false);

    let caps = AccountCapabilities {
        cursor_freshness: CursorFreshness::ServerIssued,
        // No inventory_is_change_cursor_establish field — see
        // plans/account-trait.md -> Cursor establishment.
        // JMAP returns CursorEstablishment::Ready(cursor) for all
        // top-level scopes via factory-cached state probes.
        blob_range: BlobRangeSupport::Conditional,  // server-dependent
        blob_digest_pre_download: false,            // JMAP never pre-discloses
        push: PushCapability::InProcess,
        push_in_process: ws_present,
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::StateBased,
            replay_safety: MutationReplaySafety::None,
        },
        batching_policy: BatchingPolicy {
            max_items: core.max_objects_in_set().min(500),
            max_wait: Duration::from_millis(100),
            flush_on_input_close: true,
        },
        rate_limit_class: RateLimitClass::Standard,
        quota_signal: QuotaSignal::None,
        requires_uidvalidity_recheck: false,
        historyid_expires_after: None,
        delta_token_expires_after: None,
    };

    let limits = CoreLimits {
        max_calls_in_request: core.max_calls_in_request(),
        max_objects_in_get: core.max_objects_in_get(),
        max_objects_in_set: core.max_objects_in_set(),
        max_size_request: core.max_size_request(),
    };
    Ok((caps, limits))
}
```

`push_in_process` falls back to `false` if the session does not
advertise a WebSocket URL; the engine then drives changes via
poll only (Gmail-style scheduling), which is correct rather than
broken.

`batching_policy.max_items` is the smaller of `maxObjectsInSet`
and 500; using the server's own limit prevents an avoidable
`requestTooLarge`/`tooManyChanges` on every batch.

## inventory_stream

Cold-start enumeration for `CursorScope::Type(Email)`:

```rust
// crates/jmap/src/sync/inventory.rs
fn inventory_stream(scope) -> AccountStream<SyncEvent<Batch<InventoryEntry>>>:
  match scope {
    Type(Email)  => email_inventory(),
    Type(Mailbox) => mailbox_inventory(),       // typically empty: mailboxes
                                                // surface via memberships
    Type(Thread) => return error_stream(Unsupported), // threads are derived
    Query(qid)   => query_inventory(qid),
    _            => fatal_unsupported(),
  }
```

`email_inventory()`:

1. Issue `Email/query` with `sort=[receivedAt DESC]`, paging by
   `limit = core.max_objects_in_get`, recording `position`. The
   query is `null` (no filter) for the unscoped account
   inventory; the engine schedules per-mailbox inventory passes
   through registered queries when it wants membership-scoped
   enumeration.
2. For each page of ids, issue a chained `Email/get` in the same
   `Request` via a result reference (`#ids`) requesting
   properties `[id, mailboxIds, threadId, blobId, size, keywords,
   messageId, references, inReplyTo, receivedAt]`. This is the
   `Projection::Inventory` shape from the trait.
3. For each `Email`, build an `InventoryEntry`:
   - `id`           = `EmailId` lifted into the engine's `ObjectId`
   - `memberships`  = `mailboxIds.keys().map(MembershipScope::Mailbox)`
   - `size`         = `size`
   - `blob_id`      = `Some(blob_id)`
   - `fingerprint`  = canonical hash over `(keywords sorted,
                       mailbox-ids sorted, size, receivedAt)`
                       fed through fnv-1a; same algorithm Gmail and
                       Graph will use.
   - `thread_id`    = `Some(thread_id)`
   - `message_id`   = `headers.MessageId`
   - `references`   = parsed `References` header (split on
                       whitespace)
   - `in_reply_to`  = `headers.InReplyTo`
4. Emit `SyncEvent::Batch(Batch { items: entries, checkpoint:
   None, page_boundary: PageBoundary::Item(position +
   page_len), .. })`. `checkpoint` is `None` because inventory
   does not advance a change cursor on JMAP (`cursor_freshness =
   ServerIssued`, established cheaply by the first
   `changes_stream` call).
5. After the last page, emit `SyncEvent::Done` with no checkpoint.

Backpressure: each page is one HTTP call; the stream is built
with `async_stream::try_stream!` and awaits the consumer pulling
the previous batch before issuing the next call. There is no
internal buffer.

On first open, the engine calls `inventory_stream` to seed the
`InventoryEntry` corpus and `changes_stream` to start live
tracking in parallel.

On `RecoveryClass::CapabilityChanged`, the engine reopens the
account (per `account-trait-shape.md` Q3 -> Q2-Q3 ownership) and
treats inventory as already-known unless the capability delta
removes a capability that affected the projection (e.g. server
dropped `urn:ietf:params:jmap:mail`, which is fatal anyway). The
JMAP impl does not re-run inventory itself; it returns
`SyncEvent::Done` immediately on a no-op delta when the engine
asks for inventory the second time. Engine policy decides whether
to ask.

## Initial cursor seeding

`Email/changes`, `Mailbox/changes`, and `Thread/changes` all require
a `sinceState` input. A fresh account has none. The first
`changes_stream(cursor)` call therefore cannot be invoked on a fresh
account without a seed cursor minted out of band.

The cheapest mint per RFC 8620 / RFC 8621: every `*/get` response
carries the current `state` field even when `ids: []`. One empty get
per scope is one round-trip; the response body is `~100 bytes`. The
factory pays this cost at `open()` time:

```rust
// crates/jmap/src/sync/factory.rs (inside open())
let email_state    = mail.email_get_state().await?;     // Email/get { ids: [] }
let mailbox_state  = mail.mailbox_get_state().await?;   // Mailbox/get { ids: [] }
let thread_state   = mail.thread_get_state().await?;    // Thread/get { ids: [] }
```

Each seed is encoded into an `OpaqueChangeState` and stored on the
`JmapAccount` struct in a `seed_states: HashMap<CursorScope,
OpaqueChangeState>` field. The engine has two interaction patterns:

1. **First attach.** The engine calls `discover_cursor_scopes()`,
   then for each yielded `CursorScope` it calls
   `seed_cursor(scope)` (helper on `JmapAccount`, not on the trait)
   to obtain a `ChangeCursor` whose `server_state` carries the seed
   from `open()`. It persists `(scope, cursor)` through
   `CheckpointStore::put_change_cursor` BEFORE starting
   `changes_stream(cursor)`. This pins the "any change after the
   seed is observed" guarantee.
2. **Reattach with persisted cursor.** Engine reads the stored
   `ChangeCursor` from `CheckpointStore` and calls
   `changes_stream(cursor)` directly. Seed states from this run's
   `open()` are unused (and discarded on detach).

Trait method: `Account::establish_initial_cursor(scope)` returns
`CursorEstablishment::Ready(cursor)` for every JMAP top-level
scope, wrapping the cached seed state. Per
`plans/account-trait.md` -> Cursor establishment.

Ordering during first attach:
1. `factory.open()` runs the seed probes + builds `AccountCapabilities`.
2. Engine calls `discover_cursor_scopes()` and for each scope
   `establish_initial_cursor(scope)`.
3. Engine persists `(scope, cursor)` and starts
   `changes_stream(cursor)` (live tracking from the seed onward).
4. Engine starts `inventory_stream(scope)` in parallel. Any change
   landing between the seed probe and the first inventory page is
   captured by the running change stream; the engine's
   `MembershipIndex` reconciles inventory-snapshot vs change-stream
   tail.

## changes_stream

```rust
// crates/jmap/src/sync/changes.rs
fn changes_stream(cursor) -> AccountStream<SyncEvent<Batch<Change>>>:
  let JmapCursorState::V1 { scope, state_string, query_anchor } =
      state::decode(&cursor.server_state)?;

  match scope {
    Email => email_changes_loop(state_string),
    Mailbox => mailbox_changes_loop(state_string),
    Query(qid) => query_changes_loop(qid, state_string, query_anchor),
    _ => fatal_unsupported(),
  }
```

`email_changes_loop(since)`:

1. Issue `Email/changes { sinceState: since, maxChanges: N }`
   where `N = caps.batching_policy.max_items`.
2. Parse `created`, `updated`, `destroyed`. For each id:
   - `created`   -> `ObjectChange { kind: Created }`
   - `updated`   -> `ObjectChange { kind: Updated }`
   - `destroyed` -> `ObjectChange { kind: Destroyed }`
   The JMAP impl emits `ObjectChange::Destroyed` directly because
   `Email/changes.destroyed` is authoritative (per
   `plans/account-trait.md` line 199); the engine does not derive
   destroyed-from-membership for JMAP.
3. To produce `ScopeChange::Added`/`Removed` for mailbox
   membership shifts (`mailboxIds` changed without create/destroy),
   chain `Email/get` on the `updated` ids requesting only
   `mailboxIds`. Diff against the engine-visible prior state...
   the engine does this derivation, not the protocol crate. The
   JMAP impl only emits `ObjectChange::Updated` and lets the
   engine's `MembershipIndex` (per `sync-engine.md` change
   taxonomy) derive `ScopeChange`. This is consistent with the
   "protocol emits, engine derives" rule for Gmail/IMAP/Graph.
4. Build a `Batch<Change>` with the changes. The `checkpoint`
   field is `Some` on **every** `Email/changes` response. Each
   response's `newState` is an authoritative resumption point per
   RFC 8621 §4.4: a client may call `Email/changes` again with
   `sinceState = newState` to continue. So every response is a
   cursor-advance boundary, not just the final one. The
   `hasMoreChanges` flag controls only whether the loop issues
   another call immediately; it does NOT control checkpointing.
   Without per-response checkpoints, a crash mid-drain replays
   every already-applied page.
5. Encode the new state via `state::encode` into a fresh
   `ChangeCursor` and place it on the checkpoint. Engine persists
   `(items, checkpoint)` atomically.
6. When `hasMoreChanges == false`, emit `SyncEvent::Done` with the
   most-recent checkpoint repeated for clarity (per
   `plans/sync-engine.md` -> Done carries the final checkpoint).

`mailbox_changes_loop` is structurally identical against
`Mailbox/changes`. Mailbox changes are emitted as
`ScopeChange` rather than `ObjectChange` (mailboxes are
containers, not objects in the engine's data taxonomy). Concretely:
`Mailbox/changes.created` -> `ScopeChange::Added` on a synthetic
"all-mailboxes" container; this is the impl detail that lets the
engine pick up new mailboxes without a separate poll. The detail
mirrors what `scope_lifecycle_stream` does and overlaps - the
engine consumes both, and `scope_lifecycle_stream` is the
canonical surface. Mailbox changes inside `changes_stream` may be
dropped; revisit once the engine's reconciler is wired.

`query_changes_loop(qid, since)` issues
`Email/queryChanges { sinceQueryState: since, sort, filter,
maxChanges }` per RFC 8621 §4.5. It uses the `added`/`removed`
arrays to produce `ScopeChange::Added` and `ScopeChange::Removed`
on `MembershipScope::Query(qid)`. There is no mid-result resume
primitive: the response is a complete diff between two query
states. On `MethodErrorType::CannotCalculateChanges`, the loop
ends with `Fatal { recovery: RecoveryClass::RestartScope }`; the
engine re-establishes by running a fresh `Email/query` and
diffing client-side against the inventory snapshot. Each
queryChanges response is a cursor-advance boundary (same
per-response checkpoint rule as `email_changes_loop`).

On error:
- `MethodErrorType::CannotCalculateChanges` -> stream ends with
  `Fatal { recovery: RecoveryClass::RestartScope }`; engine
  re-establishes by replaying `inventory_stream` + a fresh
  `changes_stream` with no `since`.
- `MethodErrorType::ServerUnavailable` -> `Fatal { recovery:
  Retry { after: Duration::from_secs(5) } }`.
- Transport-level 429 or `ProblemType::JMAP(JMAPError::Limit)` ->
  `Fatal::Retry { after: <Retry-After or 30s> }`.
- `accountNotFound`/`accountReadOnly`/`forbidden` ->
  `Fatal::RestartAccount` (re-fetch session; the primary account
  id likely shifted).
- See "Error mapping" below.

## get_stream

`Email/get` keyed by the projection enum:

```rust
fn project(p: Projection) -> Vec<email::Property> {
    match p {
        Projection::Inventory  => INVENTORY_PROPS,
        Projection::FlagsOnly  => vec![Property::Id, Property::Keywords,
                                        Property::MailboxIds],
        Projection::Headers    => HEADERS_PROPS,
        Projection::Full       => vec![/* canonical full set */],
        Projection::FullWithBlobs => FULL_PROPS_WITH_BLOB_IDS,
    }
}
```

Consume the input `AccountStream<ObjectId>` and batch into
`Email/get` calls of up to `core.max_objects_in_get` ids. Each
batch is one HTTP round-trip; `HydratedObject` carries the typed
`Email<Get>` plus the inventory-style metadata.

## push_subscribe / push_unsubscribe

WebSocket push (RFC 8887 §5) is the **only** push path JMAP wires
in v1. `PushSubscription/*` (RFC 8620 §7) is an HTTP-webhook
mechanism — the `url` field is required, the server delivers via
HTTP POST, and verification is a multi-step round-trip. Conflating
the two surfaces was a bug in the prior draft: a URL-less
`PushSubscription` is not how WebSocket push is enabled, and many
servers reject `url = null` outright.

The WebSocket push primitive is `WebSocketPushEnable`, sent as a
control message on the existing JMAP WebSocket connection (the one
opened by `ClientBuilder::connect_ws()` already in
`crates/jmap/src/client_ws.rs`). The wire shape is set-replace
semantics: the server starts pushing exactly the listed
`dataTypes` and forgets the prior set. Subscribe/unsubscribe must
therefore recompute the union of all live handles and reissue
`enable_push_ws(union)` on every change.

```rust
fn push_subscribe(&self, scopes: Vec<CursorScope>)
    -> AccountFuture<Result<SubscriptionHandle, Error>> {
    // 0. If the session does not advertise WebSocket push, error
    //    with Error::PushUnavailable. (capabilities() already says
    //    push_in_process = false in that case; the engine should
    //    not be calling this method, but defend explicitly.)
    // 1. Map CursorScope -> DataType:
    //      Type(Email)   -> DataType::Email
    //      Type(Mailbox) -> DataType::Mailbox
    //      Type(Thread)  -> DataType::Thread
    //      Query(_)      -> Skipped (queries do not surface via
    //                       WebSocketPushEnable; engine polls).
    // 2. Mint a SubscriptionHandle (UUID). Insert
    //      self.subscriptions[handle] = collected_types.
    // 3. Recompute union of all values in self.subscriptions.
    // 4. Call client.enable_push_ws(union, push_state = None).
    //    push_state stays None in v1 — the engine treats every
    //    WatchEvent::Invalidated as a poke; sequence-number
    //    coalescing is the engine's reconciler concern, not the
    //    protocol crate's.
    // 5. Return the handle.
}
```

No server-side `PushSubscription/*` row is created. There is no
verification-code dance: WebSocket push is authenticated by the
session that opened the WS connection.

HTTP-webhook push (URL-based `PushSubscription/*`) is deferred to
a future revision behind a separate `push_subscribe_http` API on
`JmapAccount`. Consumers wanting webhook delivery (out-of-process
listener pattern, mirroring Gmail Pub/Sub and Graph webhooks) wire
through that future API and the engine's `InvalidationSink`. Not
v1.

`push_unsubscribe(handle)`:

1. Remove `self.subscriptions[handle]`. Idempotent: missing handle
   -> `Ok(())`.
2. Recompute union of remaining values.
3. If empty: `client.disable_push_ws()`. Otherwise:
   `client.enable_push_ws(new_union, push_state = None)` to drop
   the just-removed types from the server-side push set.

`close()` calls `client.disable_push_ws()` once as part of local
teardown. It does NOT iterate `self.subscriptions` and call
`push_unsubscribe` per handle — that is the engine's job on
explicit detach-with-unsubscribe (per
`account-trait-shape.md` Q3). Since no server-side
`PushSubscription/*` row was ever created, there is nothing to
clean up beyond closing the WS.

## push_stream / WebSocket wiring

```rust
// crates/jmap/src/sync/push.rs
struct WsState {
    // broadcast::Sender, not watch::Sender: Disconnected and
    // Reconnected are state transitions the engine's reconciler must
    // observe individually. watch::Sender keeps only the latest
    // value, so a fast Invalidated -> Disconnected -> Reconnected
    // burst would collapse and the reconciler would miss the
    // transition pair.
    tx: tokio::sync::broadcast::Sender<WatchEvent>, // fan-out
    reader: tokio::task::JoinHandle<()>,
    reconnect: ReconnectPolicy,                    // see below
    // Default broadcast capacity 256. On overflow, oldest events
    // drop and lagging subscribers receive
    // RecvError::Lagged(n) -> the impl translates lag into a
    // synthetic WatchEvent::Invalidated { hint:
    //   HintPayload::Unknown } so the engine reconciles fully
    // rather than missing the lagged events silently.
}
```

On `JmapAccountFactory::open`:

1. After `ClientBuilder::connect(...)` finishes, inspect
   `session.websocket_capabilities()`.
2. If absent -> `push_in_process = false`, no WS reader. Engine
   will poll via `changes_stream`.
3. If present and `supports_push == true` -> spawn the reader
   task:

```rust
async fn ws_reader(client: Client, tx: watch::Sender<WatchEvent>,
                   shutdown: CancellationToken, policy: ReconnectPolicy) {
    let mut backoff = policy.initial;
    loop {
        if shutdown.is_cancelled() { break; }
        match client.connect_ws().await {
            Ok(stream) => {
                let _ = client.enable_push_ws::<DataType>(None, None).await;
                let _ = tx.send(WatchEvent::Reconnected);
                drive(stream, &tx, &shutdown).await;
                let _ = tx.send(WatchEvent::Disconnected);
            }
            Err(_e) => {
                let _ = tx.send(WatchEvent::Disconnected);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(policy.max);
            }
        }
    }
}

async fn drive(stream, tx, shutdown) {
    while let Some(msg) = stream.next().await {
        if shutdown.is_cancelled() { break; }
        match msg {
            Ok(WebSocketMessage::PushNotification(PushObject::StateChange { changed })) => {
                // `changed` is account_id -> { DataType -> StateString }.
                for (account_id, by_type) in changed {
                    for (data_type, _state) in by_type {
                        let hint = HintPayload::SpecificCursorScope(
                            scope_for_data_type(data_type));
                        let _ = tx.send(WatchEvent::Invalidated {
                            hint: InvalidationHint {
                                source: PushSource::JmapStateChange,
                                payload: hint,
                            },
                        });
                    }
                }
            }
            Ok(WebSocketMessage::PushNotification(PushObject::CalendarAlert(..))) |
            Ok(WebSocketMessage::PushNotification(PushObject::EmailPush(..))) |
            Ok(WebSocketMessage::PushNotification(PushObject::Group { .. })) => {
                // Treat as opaque invalidation; engine kicks reconcile.
                let _ = tx.send(WatchEvent::Invalidated {
                    hint: InvalidationHint {
                        source: PushSource::JmapStateChange,
                        payload: HintPayload::Unknown,
                    },
                });
            }
            Ok(WebSocketMessage::Response(_)) => { /* request/response,
              not push; ignored on this path */ }
            Err(_) => break,                 // breaks the inner loop;
                                             // outer loop reconnects
        }
    }
}
```

`push_stream()` returns a fresh `AccountStream<WatchEvent>` built
from `BroadcastStream::new(self.ws.tx.subscribe())`. Each caller
sees every event from subscription onward. `RecvError::Lagged(n)`
is translated to a synthetic
`WatchEvent::Invalidated { hint: HintPayload::Unknown }` so the
engine reconciles all scopes rather than silently missing lagged
events — `Invalidated` is a wake-up signal not a delivery
guarantee, but `Disconnected` and `Reconnected` are state
transitions and must not be coalesced.

`ReconnectPolicy` follows RFC 8887 guidance, exposed via
`bifrost_jmap::sync::SyncConfig`. Defaults: initial 1s, max 60s,
exponential, no failure cap (re-tries forever; engine surfaces
sustained `Disconnected` as a `Warning::PushOffline`).

The engine, per `bifrost-sync.md` push reconciler, treats
`Invalidated` as a poke to run `changes_stream` against the named
scope (or all scopes on `HintPayload::Unknown`).

## open_blob / open_blob_range

`open_blob`:

1. Construct a `BlobRef` from `BlobHandle`: `account_id` =
   `self.mail.id()`, `blob_id` = `handle.id`, `name`/`type` from
   `handle.content_type` if present, else `None`.
2. Call `client.download(&blob_ref).await`. Wrap the resulting
   `Bytes` into a one-batch `SyncEvent::Batch(Batch { items:
   vec![bytes], ... })` followed by `Done`. The trait's stream
   shape is `SyncEvent<Bytes>` directly, not batched bytes; revise
   to match whichever the engine settled on. The implementation
   should `yield` raw `Bytes` chunks if the underlying transport
   ever exposes a streaming download.

`open_blob_range`:

1. Compute the absolute byte range.
2. Issue the download request with a `Range: bytes=N-M` header.
   `ReqwestTransport::download` does not currently expose a
   range-header API: this requires extending the transport
   surface with `download_range(url, range)` or threading a
   `RequestBuilder` hook through `HttpTransport`. Open question.
3. Validate `Content-Range` on the response; emit
   `Fatal::Warning(MisalignedRange)` if mismatched (engine
   composes parallel ranges and trusts this validation).
4. Yield the bytes.

`BlobRangeSupport::Conditional` is the right declaration today
because not every JMAP server emits `Accept-Ranges: bytes`; the
impl probes on first range call and caches the result per
content-type or per blob. v1 ships `BlobRangeSupport::Yes` only
once the probe lands; otherwise advertise `No` to keep the engine
on whole-blob fetches.

## bulk_set_flags

```rust
fn bulk_set_flags(targets, flags, op, key)
   -> AccountStream<SyncEvent<Batch<MutationResult>>>:
```

1. Read the latest known `Email` state via a single
   `Email/get { ids: [], properties: [] }`-style state probe...
   actually, easier: cache `last_email_state` on the account
   (advanced by `changes_stream` and `bulk_set_flags` itself) and
   use that as the `ifInState` for the first batch.
2. Buffer `targets` into pages of
   `caps.batching_policy.max_items` (max 500, clamped to
   `core.max_objects_in_set`).
3. For each page:
   a. Build an `Email/set` request:
      - `account_id = self.mail.id()`
      - `if_in_state = Some(last_known_state)`
      - `update = { id: patch for id in page }`
      where `patch` is a `HashMap<String, serde_json::Value>` with
      the keywords field set via the
      `keywords/<flag>` PatchObject convention (RFC 8621 §4.6):
        - `op = FlagOp::Add`:    `{ "keywords/<flag>": true }`
        - `op = FlagOp::Remove`: `{ "keywords/<flag>": null }`
                                 // RFC 8620 null-removes
        - `op = FlagOp::Set`:    `{ "keywords": <new map>  }`
   b. Send. Three terminal cases:
      - 200 with `newState` -> update `last_email_state`, emit
        `MutationResult::Applied` for each updated id,
        `MutationResult::Failed(err)` for `notUpdated`, then loop
        the next page.
      - `Method::StateMismatch` -> re-fetch latest state via
        `Email/get { ids: [], properties: [] }` (an empty
        get with just the state echo) or via `Email/changes`
        from `last_known_state`, then retry the same page with
        the new state. Engine's `IdempotencyKey` guarantees this
        is the SAME logical batch, not a duplicate.
      - `Method::RequestTooLarge` / `tooManyChanges` -> split the
        page in half and retry both halves.
4. After draining `targets`, emit `SyncEvent::Done`.

`IdempotencyKey` is engine bookkeeping. JMAP has no native replay
token; capabilities advertise `replay_safety: None`. `ifInState`
is concurrency control, not replay protection: a `StateMismatch`
on retry means *some* state change happened, which could be our
prior submit landing OR any unrelated mutation by another client.
The protocol cannot disambiguate.

Ambiguous transport failure is therefore resolved by the engine's
read-back guard (per `plans/bifrost-sync.md` -> Read-back guard),
which re-fetches the affected ids via
`get_stream(Projection::FlagsOnly)` after a retried batch and
reconciles applied / skipped / failed against the intended
mutation. The Account impl's job on retry is just to re-submit
with the same `IdempotencyKey`; ground truth comes from read-back,
not from interpreting `StateMismatch`.

The `bifrost-sync.md` Read-back guard section MUST list JMAP
explicitly as a read-back protocol (the audit gap reviewers flagged
on the prior draft). Cross-document follow-up.

`bulk_move` is `Email/set` with `mailboxIds` patches; same shape,
same `ifInState` semantics.

`bulk_destroy` is `Email/set { destroy: [ids] }`; same shape.

## close

```rust
fn close(&self) -> AccountFuture<Result<(), Error>> {
    Box::pin(async move {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());                 // idempotent
        }
        self.shutdown.cancel();             // poke WS reader
        if let Some(client) = self.ws_client_for_close() {
            let _ = client.disable_push_ws().await;
            // tokio_tungstenite stream end is handled by the
            // reader observing shutdown.
        }
        Ok(())
    })
}
```

Server-side `PushSubscription/*` rows are NOT destroyed here. If
the consumer wants them gone, they call the engine's
`engine.unsubscribe_push(account_id)` per
`plans/bifrost-sync.md`, which forwards to
`Account::push_unsubscribe` for every handle in
`self.subscriptions`.

## Error mapping

```rust
// crates/jmap/src/sync/error.rs
fn to_recovery(err: &crate::Error) -> RecoveryClass {
    use core::error::{ProblemType, JMAPError, MethodErrorType};
    match err {
        crate::Error::Method(m) => match m.error_type() {
            MethodErrorType::CannotCalculateChanges => RestartScope,
            MethodErrorType::StateMismatch          => Retry { after: ZERO },
            MethodErrorType::AccountNotFound
            | MethodErrorType::FromAccountNotFound
            | MethodErrorType::AccountNotSupportedByMethod
            | MethodErrorType::AccountReadOnly      => RestartAccount,
            MethodErrorType::ServerUnavailable      => Retry { after: 5s },
            MethodErrorType::ServerFail
            | MethodErrorType::ServerPartialFail    => Retry { after: 30s },
            MethodErrorType::RequestTooLarge
            | MethodErrorType::TooManyChanges       => Warning, // split + retry
            MethodErrorType::InvalidArguments
            | MethodErrorType::InvalidResultReference
            | MethodErrorType::UnknownMethod
            | MethodErrorType::UnsupportedSort
            | MethodErrorType::UnsupportedFilter
            | MethodErrorType::Forbidden
            | MethodErrorType::AnchorNotFound
            | MethodErrorType::AlreadyExists
            | MethodErrorType::Other                => Fatal,
        },
        crate::Error::Problem(p) => match p.error() {
            ProblemType::JMAP(JMAPError::Limit)          => Retry { after: 30s },
            ProblemType::JMAP(JMAPError::UnknownCapability) =>
                CapabilityChanged { delta: <fingerprint diff> },
            ProblemType::JMAP(JMAPError::NotJSON)
            | ProblemType::JMAP(JMAPError::NotRequest)   => Fatal,
            ProblemType::Other(_)                        => match p.status() {
                Some(401 | 403) => RestartAccount,
                Some(429)       => Retry { after: parse_retry_after() or 30s },
                Some(500..=599) => Retry { after: 30s },
                _               => Fatal,
            },
        },
        crate::Error::Transport(t)         => match t.kind() {
            // bifrost-net surfaces classified transport errors;
            // map TLS / dns / refused -> Retry { after: 5s };
            // map http2 GOAWAY -> RestartAccount.
            ...
        },
        crate::Error::WebSocket(_)
        | crate::Error::WebSocketNotConnected => Warning, // reconnect loop handles it
        crate::Error::NoPrimaryAccount { .. } => RestartAccount, // session re-fetch
        crate::Error::Parse(_)
        | crate::Error::NotParsable
        | crate::Error::CallNotFound
        | crate::Error::IdNotFound
        | crate::Error::EmptyResponse
        | crate::Error::InvalidUrl(_)
        | crate::Error::Set(_)                            => Fatal,
    }
}
```

A `RecoveryClass::CapabilityChanged` is constructed by capturing
the prior `Fingerprint` (computed at open from
`session.capabilities() | account_capabilities()`) and the new
one (re-fetched via `Client::refresh_session()`); the delta is
the symmetric difference of the URI sets plus any value-change
on `CoreCapabilities`. Triggered by:
- `UnknownCapability` problem-doc.
- `Client::is_session_updated() == false` observed after a
  request - the server signaled a new session state; refresh and
  diff.

## AccountFactory

```rust
// crates/jmap/src/sync/factory.rs
pub struct JmapAccountFactory {
    builder_seed: BuilderConfig,    // url, credentials, timeout, etc.
    push_policy: PushPolicy,        // reconnect tuning
}

impl AccountFactory for JmapAccountFactory {
    fn open(&self) -> AccountFuture<Result<Arc<dyn Account>, Error>> {
        Box::pin(async move {
            let client = ClientBuilder::new()
                .credentials(self.builder_seed.creds.clone())
                .timeout(self.builder_seed.timeout)
                .connect(&self.builder_seed.url).await?;
            let mail = client.primary_account::<core::capability::Mail>()?;
            // submission, cal, contacts: optional, gated on session.

            // Missing/malformed Core is fatal — see capabilities::build.
            let (caps, limits) = capabilities::build(&client.session())?;

            // Seed initial change-cursor states for every top-level
            // scope this account exposes. One */get { ids: [] } per
            // scope (see "Initial cursor seeding" above).
            let mut seed_states = HashMap::new();
            seed_states.insert(
                CursorScope::Type(ObjectType::Email),
                state::encode(JmapCursorState::V1 {
                    scope: JmapScopeRepr::Email,
                    state_string: mail.email_get_state().await?,
                }),
            );
            if session_has(&client, MailboxCapability) {
                seed_states.insert(
                    CursorScope::Type(ObjectType::Mailbox),
                    state::encode(JmapCursorState::V1 {
                        scope: JmapScopeRepr::Mailbox,
                        state_string: mail.mailbox_get_state().await?,
                    }),
                );
            }
            if session_has(&client, ThreadCapability) {
                seed_states.insert(
                    CursorScope::Type(ObjectType::Thread),
                    state::encode(JmapCursorState::V1 {
                        scope: JmapScopeRepr::Thread,
                        state_string: mail.thread_get_state().await?,
                    }),
                );
            }

            let ws = push::spawn(&client, push_policy, shutdown.clone())?;
            Ok(Arc::new(JmapAccount {
                mail, caps, limits, ws, seed_states, /* ... */
            }) as Arc<dyn Account>)
        })
    }
}
```

The engine attaches via
`engine.attach(account_id, Arc::new(JmapAccountFactory::new(cfg)))`.
On `CapabilityChanged`, the engine calls `factory.open()` again
to obtain a fresh `Arc<dyn Account>` with updated capabilities,
and atomically swaps the old handle via the
`ArcSwap<Arc<dyn Account>>` in the engine's `AccountSlot` (per
`bifrost-sync.md`).

## Per-trait-method summary

| Trait method                | JMAP realization                                                          |
|-----------------------------|---------------------------------------------------------------------------|
| `capabilities`              | Return `&self.caps` (built at open from `Session` + hard-coded defaults). |
| `describe_cursor`           | `CursorDescriptor { cost_class: Cheap, strategy: Default, freshness }`.   |
| `discover_cursor_scopes`    | Yield `Type(Email)`, plus `Type(Mailbox)`, `Type(Thread)`, then `Done`.   |
| `discover_memberships`      | `Mailbox/get` paged; yield `Mailbox(_)` each batch; `Done`.               |
| `scope_lifecycle_stream`    | Mailbox-changes-driven `Created`/`Renamed`/`Deleted` events.              |
| `inventory_stream`          | `Email/query` + chained `Email/get` per page; whole-account.              |
| `get_stream`                | `Email/get` batched by `maxObjectsInGet`, properties from `Projection`.   |
| `changes_stream`            | `Email/changes` / `Mailbox/changes` / `Email/queryChanges` loop.          |
| `push_subscribe`            | `WebSocketPushEnable` (RFC 8887 §5) over the existing WS; set-replace union of all handles. No `PushSubscription/*`. |
| `push_unsubscribe`          | Recompute union of remaining handles; reissue `WebSocketPushEnable` or `WebSocketPushDisable` when empty.            |
| `push_stream`               | Broadcast-channel fan-out of `Invalidated`/`Disconnected`/`Reconnected`; lag becomes `Invalidated{Unknown}`.         |
| `open_blob`                 | `Client::download(BlobRef)` -> single batch + `Done`.                     |
| `open_blob_range`           | Range-header download (transport extension required).                     |
| `bulk_set_flags`            | Page -> `Email/set` with `ifInState`; keywords PatchObject; state-mismatch retry. |
| `bulk_move`                 | `Email/set` with `mailboxIds` patches; same shape.                        |
| `bulk_destroy`              | `Email/set { destroy: [...] }`; same shape.                               |
| `close`                     | Trip cancellation token; disable push WS; mark closed; idempotent.        |

## Risks and opens

- **Transport range support.** `ReqwestTransport::download` has no
  `Range` API today. `open_blob_range` cannot ship without
  extending `HttpTransport` (or carving a separate
  `download_range`). Pick one before phase 1 codes.

- **Threads as a change cursor.** RFC 8621 lists `Thread/changes`
  but in practice the thread state mostly follows email state.
  The plan above advertises `Type(Thread)` if the session
  capability is present, but downstream sync engines may want to
  treat threads as derived. Keep it advertised, document that it
  is opt-in for the engine.

- **MailboxId surfacing.** The engine's `MembershipScope::Mailbox`
  carries an opaque `MailboxId`. The current `bifrost-jmap`
  `mailbox::MailboxId` is a phantom-typed string. Lifting it into
  the engine's neutral `MailboxId` is one trait-shape question
  open in `plans/account-trait.md` (Cross-references -> open
  questions). Pin the conversion direction (string-newtype on
  the engine side) before phase 1.

- **PushSubscription verification.** WebSocket-only deployments
  skip the RFC 8620 `verificationCode` round-trip. If a server
  rejects URL-less creates (some implementations do), the impl
  must fall back to a URL-based subscription pointed at a
  consumer-supplied webhook, which is out of scope for v1.

- **Session state vs Email state.** `is_session_updated()` flips
  when ANY type's state shifts. Using it as the
  `CapabilityChanged` trigger is too coarse - normal mailbox
  activity will flip it. Either compare full capability URI sets
  on `refresh_session()` rather than reading the flag, or add a
  dedicated capability-fingerprint field on `Session`.

- **Initial-cursor seeding cost on wide accounts.** A JMAP account
  with many top-level scopes (Email + Mailbox + Thread, plus future
  CalendarEvent / ContactCard / EmailSubmission) pays one `*/get
  { ids: [] }` round-trip per scope at `factory.open()`. Three
  round-trips today, more once non-mail scopes land. Worth
  measuring before parallelizing; for v1 the seeds run
  sequentially inside `open()` to keep the factory simple.

- **Trait-shape extension: `establish_initial_cursor`.** This plan
  reserves the method name and shape (see "Initial cursor seeding"
  above), but the trait extension lands in
  `plans/account-trait.md` as a Phase 1 follow-up. If the trait
  shape changes (e.g. cursor establishment folded into
  `discover_cursor_scopes` returning `(scope, Option<cursor>)`),
  the JMAP impl tracks. Coordinated cross-protocol decision; this
  document is not the deciding doc.

- **Mailbox changes inside `changes_stream(Email)`.** The plan
  punts mailbox-membership-only updates to the engine's derivation
  (`MembershipIndex` over `mailboxIds` snapshots in inventory
  plus updated reads). That requires the JMAP `changes_stream`
  to also issue a `Email/get { properties: [mailboxIds] }` for
  every `updated` id, doubling round-trips. A smarter approach is
  to chain the get inside the same request and emit `ScopeChange`
  directly. Decide between "minimal protocol; engine derives" and
  "protocol-emits-both" before the impl lands.

- **Submission, calendar, contacts.** This plan covers mail only.
  Calendars and contacts impl trait surfaces (CalendarEvent/get,
  ContactCard/get, `*/changes`) need their own per-type cursor
  scopes (`Type(CalendarEvent)`, `Type(ContactCard)`); add when
  those crates' sync surfaces are spec'd.
