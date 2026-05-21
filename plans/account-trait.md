# Account trait

The Account trait is the load-bearing artifact of the sync engine.
It is the contract every protocol crate implements and the engine
drives. This document sketches the trait surface, defines the types
referenced from `sync-engine.md`, and pins the cross-cutting
contracts (cursor ownership, recovery vocabulary, capability
staleness, change emission, push wiring) the engine and protocols
must agree on.

This is a sketch, not a final API. The trait will iterate as the
first protocol implementation lands. What is pinned here is the
shape of the scope problem, the types that must be cross-protocol
agreeable, the per-protocol mapping that validates the trait is
implementable, and the ownership rules that prevent two
implementations from drifting silently.

## Scope: two distinct concepts

The biggest design risk in the engine is that the four protocols
disagree about what a cursor tracks AND what an object belongs to,
and these are not the same axis. Two enums:

```rust
// Cursor granularity. What does a change cursor track?
enum CursorScope {
    Account,                                  // Gmail historyId
    Type(ObjectType),                         // JMAP Email/Mailbox/...
    Query(QueryId),                           // JMAP queryChanges
    Folder(FolderId),                         // IMAP per-folder modseq
    FolderType { folder: FolderId, ty: ObjectType },  // Graph delta
}

// Object membership. What container does an object live in?
enum MembershipScope {
    Folder(FolderId),                         // IMAP, Graph
    Label(LabelId),                           // Gmail
    Mailbox(MailboxId),                       // JMAP mailboxIds
    Query(QueryId),                           // JMAP queryChanges result
}
```

Per protocol:

- **Gmail.** `CursorScope::Account` (singleton). `MembershipScope::
  Label(_)` (N labels per message, dynamic).
- **JMAP.** `CursorScope::Type(_)` per typed object;
  `CursorScope::Query(_)` for queryChanges. `MembershipScope::
  Mailbox(_)` from `mailboxIds`; `MembershipScope::Query(_)` for
  query-result membership.
- **IMAP.** `CursorScope::Folder(_)` and `MembershipScope::
  Folder(_)` coincide; one folder per message at the protocol level.
- **Graph.** `CursorScope::FolderType { folder, ty }` (finer than
  membership); `MembershipScope::Folder(_)`.

`ChangeCursor` carries `CursorScope`. `ScopeChange` and
`InventoryEntry::memberships: Vec<MembershipScope>` carry
`MembershipScope`. The engine multiplexes one `changes_stream` per
cursor scope; tracks per-object membership across membership scopes
to derive `Destroyed` (engine cannot see scopes the consumer does
not sync, which is the correct level of fallibility).

```rust
struct ChangeCursor {
    scope: CursorScope,
    server_state: OpaqueChangeState,          // protocol-owned, tagged
    advanced_through: Option<OpaqueProgressBytes>,  // protocol-owned
    envelope_version: u32,
}

// Concrete opaque cursor state. `protocol` and `envelope_version`
// let the protocol impl reject a cursor that was minted for a
// different protocol or an older schema. See plans/account-trait-
// shape.md (Q1).
struct OpaqueChangeState {
    protocol: ProtocolKind,
    envelope_version: u32,
    bytes: Vec<u8>,
}
```

## The trait surface

The trait is dyn-safe by construction: all stream and future
returns are erased through type aliases, the cursor state is a
concrete tagged blob rather than an associated type, and `close`
takes `&self` so it composes with `Arc<dyn Account>`. Rationale
and full erasure list in `plans/account-trait-shape.md` (Q1).

```rust
type AccountStream<T> = Pin<Box<dyn Stream<Item = T> + Send + 'static>>;
type AccountFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

trait Account: Send + Sync {
    fn capabilities(&self) -> &AccountCapabilities;

    // Cursor introspection. Account-aware because cost depends on
    // capability state, not the cursor in isolation.
    fn describe_cursor(&self, cursor: &ChangeCursor)
        -> CursorDescriptor;

    // Cursor-scope discovery: what scopes does the engine multiplex
    // changes_stream over? Bounded, terminates with Done.
    fn discover_cursor_scopes(&self)
        -> AccountStream<SyncEvent<Batch<CursorScope>>>;

    // Membership-scope discovery: what containers (folders, labels,
    // mailboxes, queries) can the consumer ask about? Bounded.
    fn discover_memberships(&self)
        -> AccountStream<SyncEvent<Batch<MembershipScope>>>;

    // Ongoing scope lifecycle.
    fn scope_lifecycle_stream(&self)
        -> AccountStream<ScopeLifecycle>;

    // Inventory: projection-only cold-start primitive.
    fn inventory_stream(&self, scope: CursorScope)
        -> AccountStream<SyncEvent<Batch<InventoryEntry>>>;

    // Hydrate: fetch known ids at a chosen projection.
    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<Batch<HydratedObject>>>;

    // Post-cursor diff.
    fn changes_stream(
        &self,
        cursor: ChangeCursor,
    ) -> AccountStream<SyncEvent<Batch<Change>>>;

    // Push subscription CRUD. Creates / destroys the server-side hook.
    fn push_subscribe(&self, scopes: &[CursorScope])
        -> AccountFuture<Result<SubscriptionHandle, Error>>;
    fn push_unsubscribe(&self, handle: SubscriptionHandle)
        -> AccountFuture<Result<(), Error>>;

    // In-process event stream. IMAP IDLE / JMAP WebSocket deliver
    // here. Gmail Pub/Sub and Graph webhooks deliver out-of-process;
    // the engine wires those via InvalidationSink (see sync-engine.md).
    fn push_stream(&self) -> AccountStream<WatchEvent>;

    // Blobs.
    fn open_blob(&self, handle: BlobHandle)
        -> AccountStream<SyncEvent<Bytes>>;
    fn open_blob_range(&self, handle: BlobHandle, range: ByteRange)
        -> AccountStream<SyncEvent<Bytes>>;

    // Mutations. Streaming input so engine-driven mutation pipelines
    // ("for each item in inventory, set this flag") backpressure
    // cleanly. Consumers with a static list adapt via
    // `stream::iter(vec).boxed()`. The protocol crate batches the
    // stream per BatchingPolicy from its capabilities.
    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        flags: FlagSet,
        op: FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<Batch<MutationResult>>>;
    fn bulk_move(&self, ...) -> ...;
    fn bulk_destroy(&self, ...) -> ...;

    // Graceful local-handle teardown. IMAP LOGOUT + pool drain,
    // JMAP WebSocket close, Graph subscription stream end, local
    // worker shutdown. Idempotent: takes &self so it composes with
    // Arc<dyn Account>; safe to call more than once. Does NOT
    // destroy durable server-side push subscriptions - those go
    // through push_unsubscribe explicitly. See
    // plans/account-trait-shape.md (Q3).
    fn close(&self) -> AccountFuture<Result<(), Error>>;
}

// Engine-facing factory. Consumers register one per account so the
// engine can perform reopen cycles (capability change, transport
// reset) without knowing protocol config. The engine owns the
// current open Arc<dyn Account> and calls open() when it needs a
// fresh one. See plans/account-trait-shape.md (Q3).
trait AccountFactory: Send + Sync + 'static {
    fn open(&self) -> AccountFuture<Result<Arc<dyn Account>, Error>>;
}

struct ByteRange {
    start: u64,
    length: Option<u64>,    // None = open-ended to end of blob
}

enum ScopeLifecycle {
    Created(MembershipScope),
    Renamed { old: MembershipScope, new: MembershipScope },
    Deleted(MembershipScope),
}
```

`Account` is single-account by contract. Multi-account multiplexing
is an engine concern; the engine holds N `Account` handles and
correlates them.

`Change` is the sum of `ObjectChange` and `ScopeChange` defined in
`sync-engine.md`. See **Change emission contract** below.

## Capabilities

Read once at account-open via `Account::capabilities()`. When a
capability-relevant transition is observed mid-session (IMAP
post-AUTHENTICATE capability re-advertisement, tenant policy
change, Gmail quota tier shift), the protocol crate ends affected
streams with `RecoveryClass::CapabilityChanged { delta }`. The
engine re-opens the account, reads capabilities fresh, and restarts
the streams. There is no live `capabilities()` channel; capability
change is a recovery event.

```rust
struct AccountCapabilities {
    cursor_freshness: CursorFreshness,           // ServerIssued | Hybrid
    inventory_is_change_cursor_establish: bool,  // IMAP Basic/CONDSTORE

    blob_range: BlobRangeSupport,
    blob_digest_pre_download: bool,

    push: PushCapability,
    push_in_process: bool,                       // true for IMAP IDLE,
                                                 // JMAP WS; false for
                                                 // Gmail Pub/Sub, Graph
                                                 // webhooks

    mutation: MutationCapabilities,
    batching_policy: BatchingPolicy,

    rate_limit_class: RateLimitClass,
    quota_signal: QuotaSignal,

    requires_uidvalidity_recheck: bool,
    historyid_expires_after: Option<Duration>,
    delta_token_expires_after: Option<Duration>,
}

enum BlobRangeSupport {
    Yes,
    No,
    Conditional,         // varies per handle; BlobHandle.
                         // capabilities.supports_range authoritative
}

// Mutation has two orthogonal concerns, separated.
struct MutationCapabilities {
    concurrency: MutationConcurrency,   // optimistic concurrency
    replay_safety: MutationReplaySafety, // safe retry on transient
}

enum MutationConcurrency {
    StateBased,          // JMAP ifInState, Graph If-Match: etag,
                         // IMAP STORE UNCHANGEDSINCE
    None,                // last-write-wins; engine reads back if
                         // it cares
}

enum MutationReplaySafety {
    ReplayToken,         // Gmail X-Goog-Request-Id,
                         // Graph batch request-id
    None,                // engine guards via read-back-after-retry
}

struct BatchingPolicy {
    max_items: usize,        // Graph $batch=20, JMAP ~500,
                             // Gmail batchModify=1000
    max_wait: Duration,      // flush trigger if batch not full
    flush_on_input_close: bool,  // always true for bulk_* surfaces
}
```

Two distinct concerns. `MutationConcurrency` prevents lost updates
when state changed under the client. `MutationReplaySafety` prevents
double-apply on retry. They are orthogonal: JMAP has StateBased
concurrency but no replay token; Gmail has ReplayToken but no
concurrency check; Graph has both; IMAP has both only after
`STORE UNCHANGEDSINCE` lands (see
`plans/imap/condstore-qresync.md`).

`inventory_is_change_cursor_establish` is the IMAP-on-Basic-or-
CONDSTORE-only case: establishing a baseline across hundreds of
folders is itself an inventory pass, and the engine schedules
backfill differently when this flag is true (the "start cheap,
backfill underneath" pattern does not apply because the cheap-
start step does not exist).

## Ownership and contracts

### advanced_through is protocol-owned

`advanced_through: Option<OpaqueProgressBytes>` is opaque to the
engine, set by the protocol at Checkpoint emission, used by the
protocol on resume:

- Protocol writes `advanced_through` when emitting `Checkpoint`
  inside a `Batch` (see `sync-engine.md` -> Stream contract).
- Engine treats it as opaque, persists with the rest of the cursor.
- On resume, engine passes the cursor (including `advanced_through`)
  back; protocol interprets and resumes from where it last marked.

The previous engine-managed design (engine sets from consumer acks
by ObjectId) assumed stable item-id ordering, which JMAP
`Email/changes`, Gmail `history.list`, and Graph delta pages do
not guarantee. Protocol-owned is the only correct model across
protocols.

For protocols where mid-page resumption is impossible (cursor
advances only at page granularity), `advanced_through` is always
`None` and `server_state` alone is the resumption point.

### Change emission: protocol observes, engine derives Destroyed

The protocol crate emits what the protocol observed; the engine
derives `Destroyed` from per-object membership tracking:

- **JMAP** emits `Destroyed` directly (`Email/changes` returns
  destroyed ids). Per-mailbox membership changes are
  `ScopeChange::Added` / `Removed` from `MembershipScope::
  Mailbox(_)`.
- **Gmail** emits `ScopeChange::Added` / `Removed` for label
  changes; `Destroyed` when history records the deletion.
- **IMAP** emits `ScopeChange::Removed` per folder. Engine tracks
  per-object memberships and derives `Destroyed` when the last
  known membership reports `Removed`. (Pure-IMAP servers have
  exactly one folder per message at the protocol level, so the
  derivation usually collapses to one step.)
- **Graph** emits `ScopeChange::Added` / `Removed` per folder;
  engine derives `Destroyed` likewise.

The engine's derivation is fallibly incomplete: messages in
unsynced folders or shared mailboxes the consumer does not sync
stay `Removed`-not-`Destroyed`. This is the correct level of
fallibility - the engine can only see scopes the consumer syncs.

### Recovery vocabulary

The protocol-to-engine recovery vocabulary. The engine translates
these into consumer-facing `Fatal` variants in `sync-engine.md` and
applies the recovery action.

```rust
enum RecoveryClass {
    Retry { after: Duration },             // network, 429, transient
    DowngradeStrategy(StrategyDowngrade),  // QRESYNC -> CONDSTORE
    DowngradeCapabilityForScope(CursorScope),  // Gmail folder modseq=0
    RestartScope(CursorScope),             // UIDVALIDITY, modseq reset
    RestartAccount,                        // stale historyId
    AuthLost,
    SchemaIncompatible,                    // cursor envelope_version
                                           // older than engine can
                                           // migrate
    CapabilityChanged { delta: CapabilityDelta },
    OperatorOverrideRequired { reason: String },
    Fatal,
}

enum StrategyDowngrade {
    QResyncToCondstore,
    CondstoreToBasic,
}

struct CapabilityDelta {
    added: Vec<CapabilityKey>,
    removed: Vec<CapabilityKey>,
    changed: Vec<(CapabilityKey, OldValue, NewValue)>,
}
```

## Load-bearing types

### Fingerprint

Cross-protocol inventory diff requires a shape the consumer can
compare without knowing the protocol. Three layers:

```rust
struct Fingerprint {
    server_version: ServerVersion,
    size: u64,
    flags_hash: u64,
}

enum ServerVersion {
    ModSeq(u64),                  // IMAP CONDSTORE / QRESYNC
    ETag(String),                 // Graph
    HistoryAt(u64),               // Gmail
    StateAt(String),              // JMAP
    Unavailable,                  // IMAP Basic; falls back to
                                  // (size, flags_hash)
}
```

Diff rule: `local.fingerprint != server.fingerprint` implies refetch.
On `ServerVersion::Unavailable` the diff degrades to
`(size, flags_hash)`, which is weaker (a flag-only change that
canonicalizes to the same hash is not detected) but matches what
the protocol can offer.

**`flags_hash` canonicalization.** Within-scope only. The protocol
crate canonicalizes its native flag set deterministically: system
flags lowercased, all flags sorted, hashed with a stable algorithm
(fnv-1a or xxhash). Cross-protocol equivalence (`\Seen` vs Gmail
`UNREAD` inverted polarity, IMAP keyword vs Gmail label) is engine
territory; the fingerprint does not bridge it.

### CursorDescriptor

Engine-facing introspection over an opaque cursor. Returned by
`Account::describe_cursor`:

```rust
struct CursorDescriptor {
    cost_class: CostClass,         // Cheap | Medium | Expensive
    strategy: SyncStrategy,        // QResync | Condstore | Basic |
                                   // ServerCursor | None
    freshness: Option<Instant>,    // when this cursor was last advanced
}

enum CostClass { Cheap, Medium, Expensive }
```

Account-aware (not pure-cursor) because cost depends on capability
state, not the cursor in isolation. The engine uses cost class to
schedule work (do not start a multi-hour Basic-state diff on
cellular network or battery).

### InvalidationHint

Push payload, type-erased to a protocol-agnostic shape:

```rust
struct InvalidationHint {
    source: PushSource,
    payload: HintPayload,
}

enum PushSource {
    JmapStateChange,
    ImapNotify,
    GmailPubsub,
    GraphSubscription,
    EwsStreaming,
}

enum HintPayload {
    SpecificCursorScope(CursorScope),
    SpecificMembership(MembershipScope),
    Unknown,
}
```

The engine treats `Unknown` and a specific hint identically in v1:
wake up and run the change stream. Specific hints let a future
optimizing engine restrict the reconcile pass.

### IdempotencyKey

Engine-side identifier for safe retry. Distinct from
`MutationConcurrency::StateBased`: that is optimistic concurrency
on the wire, this is engine bookkeeping for "did we already submit
this exact batch?"

```rust
struct IdempotencyKey {
    run_id: Uuid,         // consumer-minted; see ownership rule below
    sequence: u64,        // monotonic within run
    salt: ProtocolSalt,
}

enum ProtocolSalt {
    Jmap(String),         // optional; combines with ifInState
                          // (concurrency) where used
    Gmail(String),        // becomes X-Goog-Request-Id header
                          // (replay token)
    Graph(String),        // becomes batch request-id (replay token);
                          // combines with If-Match (concurrency)
                          // for per-item ops
    Imap,                 // no native replay token; engine guards
                          // via read-back. STORE UNCHANGEDSINCE
                          // is the optimistic-concurrency hook,
                          // separate from idempotency.
}
```

**`run_id` is consumer-minted and persisted across process
restarts** for a given mutation campaign. If `run_id` resets on
restart, retries from the previous process look new to the server
and double-apply. The consumer mints once per campaign and persists
alongside the campaign state.

For protocols without native replay tokens (IMAP) the engine reads
back the affected items after retry and skips items already in the
target state.

### Digest

Algorithm-tagged content hash for blob dedup:

```rust
struct Digest {
    algorithm: DigestAlgorithm,
    value: Vec<u8>,
}

enum DigestAlgorithm {
    Sha256,
    Sha1,
    Md5,                 // legacy; some providers still ship this
}
```

`BlobHandle::digest` is `Some` only when
`AccountCapabilities::blob_digest_pre_download` is true. Otherwise
the consumer must hash bytes after download and dedup against its
own digest store.

## Per-protocol mapping

A sketch of how each protocol's existing primitives map to the
trait methods. The point is to validate the trait shape is
implementable, not to specify the implementation.

### JMAP

- `discover_cursor_scopes()` yields one `CursorScope::Type(_)` per
  type the account advertises capability for; plus
  `CursorScope::Query(_)` for any registered queries.
- `discover_memberships()` yields `MembershipScope::Mailbox(_)`
  from `Mailbox/get`, plus `MembershipScope::Query(_)` for
  registered queries.
- `scope_lifecycle_stream()` yields `ScopeLifecycle` from
  `Mailbox/changes`.
- `inventory_stream(Type(Email))` -> `Email/query` paged with
  `Email/get` requesting only
  `[id, mailboxIds, threadId, blobId, size, keywords, messageId,
  references, inReplyTo]`. `mailboxIds` becomes
  `InventoryEntry::memberships`.
- `get_stream(ids, projection)` -> `Email/get` with `properties`
  expanded per projection.
- `changes_stream(cursor)` -> `Email/changes` from
  `cursor.server_state` (a `State` string), follows
  `hasMoreChanges`. `Email/queryChanges` for query cursors.
- `push_subscribe` / `push_stream` -> `WebSocketPushEnable` or SSE;
  `push_in_process = true`.
- `open_blob` / `open_blob_range` -> blob endpoint with `Range`
  header where the server supports it.
- `bulk_set_flags` -> batched `Email/set` with `ifInState` from
  `MutationConcurrency::StateBased`; `IdempotencyKey` is engine
  bookkeeping (no native replay token).
- `close()` -> close WebSocket cleanly.

Capabilities: `cursor_freshness = ServerIssued`,
`inventory_is_change_cursor_establish = false`,
`push_in_process = true`,
`mutation.concurrency = StateBased`,
`mutation.replay_safety = None`,
`batching_policy = { max_items: 500, max_wait: 100ms,
flush_on_input_close: true }`.

### Gmail

- `discover_cursor_scopes()` yields one `CursorScope::Account`.
- `discover_memberships()` yields `MembershipScope::Label(_)` from
  `labels.list`.
- `scope_lifecycle_stream()` yields `ScopeLifecycle` from
  `labels.list` polling and history events touching labels.
- `inventory_stream(Account)` -> `messages.list` paged +
  `messages.get?fields=id,threadId,sizeEstimate,labelIds,
  payload.headers(Message-ID,References,In-Reply-To)`. `labelIds`
  becomes `InventoryEntry::memberships`.
- `get_stream(ids, projection)` -> `messages.get?format=...` per
  projection.
- `changes_stream(cursor)` -> `history.list` from
  `cursor.server_state` (a `historyId` u64).
- `push_subscribe` -> Pub/Sub `users.watch`; `push_unsubscribe` ->
  `users.stop`. `push_in_process = false`; out-of-process Pub/Sub
  listener feeds the engine's `InvalidationSink`. `push_stream`
  yields nothing.
- `open_blob` -> `messages.attachments.get`;
  `blob_range = No` (base64url in JSON).
- `bulk_set_flags` -> `messages.batchModify` with
  X-Goog-Request-Id from `MutationReplaySafety::ReplayToken`.
- `close()` -> stop Pub/Sub watch.

Capabilities: `cursor_freshness = ServerIssued`,
`historyid_expires_after = Some(7 days)`,
`push_in_process = false`,
`mutation.concurrency = None`,
`mutation.replay_safety = ReplayToken`,
`blob_digest_pre_download = false`,
`batching_policy = { max_items: 1000, max_wait: 200ms,
flush_on_input_close: true }`.

### IMAP

- `discover_cursor_scopes()` yields `CursorScope::Folder(_)` from
  `LIST`.
- `discover_memberships()` yields `MembershipScope::Folder(_)` -
  same folders, different role.
- `scope_lifecycle_stream()` yields `ScopeLifecycle` from `LIST` +
  IDLE / NOTIFY mailbox events.
- `inventory_stream(Folder(f))` ->
  `UID FETCH 1:* (FLAGS MODSEQ RFC822.SIZE
  BODY.PEEK[HEADER.FIELDS (MESSAGE-ID REFERENCES IN-REPLY-TO)])`.
  `memberships` is `[Folder(f)]` (singleton). Doubles as change-
  cursor baseline establishment on Basic and CONDSTORE-only paths.
- `get_stream(ids, projection)` -> `UID FETCH <ids> (...)`.
- `changes_stream(cursor)` -> per
  `plans/imap/condstore-qresync.md`: QRESYNC `SELECT (QRESYNC ...)`
  + `CHANGEDSINCE`, or CONDSTORE `CHANGEDSINCE` + UID-list diff, or
  full UID-list diff.
- `push_subscribe` / `push_stream` -> IDLE / NOTIFY;
  `push_in_process = true`.
- `open_blob_range` -> `UID FETCH <uid>
  BODY[<section>]<offset.length>`.
- `bulk_set_flags` -> `UID STORE` with read-back guard;
  `UID STORE (UNCHANGEDSINCE m)` where per-message MODSEQ is
  parsed.
- `close()` -> `LOGOUT`.

Capabilities: `cursor_freshness = Hybrid`,
`inventory_is_change_cursor_establish = true` on Basic /
CONDSTORE-only, `requires_uidvalidity_recheck = true`,
`push_in_process = true`,
`mutation.concurrency = None` (or `StateBased` once
`STORE UNCHANGEDSINCE` lands),
`mutation.replay_safety = None` (engine reads back after retry).

### Graph

- `discover_cursor_scopes()` yields `CursorScope::FolderType
  { folder, ty }` from folder enumeration per type.
- `discover_memberships()` yields `MembershipScope::Folder(_)`
  from folder enumeration.
- `scope_lifecycle_stream()` yields `ScopeLifecycle` from folder
  delta queries.
- `inventory_stream(FolderType{folder, Email})` -> delta query with
  `$select=id,parentFolderId,internetMessageId,subject,changeKey,
  size,...`. `parentFolderId` becomes `memberships` (singleton).
- `get_stream(ids, projection)` -> `$select` widened per projection.
- `changes_stream(cursor)` -> delta query from `cursor.server_state`
  (a `@odata.deltaLink` URL).
- `push_subscribe` -> Graph `/subscriptions` CRUD;
  `push_unsubscribe` -> DELETE. `push_in_process = false` for
  webhook subscriptions; out-of-process webhook receiver feeds the
  engine's `InvalidationSink`. EWS `StreamingSubscription` is
  in-process when chosen, yielding `push_in_process = true`.
- `open_blob` / `open_blob_range` -> `$value` for file attachments
  (range supported); JSON-wrapped body for item attachments (range
  not supported).
- `bulk_set_flags` -> batched `PATCH` with `If-Match: <etag>`
  (StateBased concurrency) and request-id header on the batch
  envelope (ReplayToken).
- `close()` -> end EWS streaming subscription where in use.

Capabilities: `cursor_freshness = ServerIssued`,
`delta_token_expires_after = Some(30 days)`,
`blob_range = Conditional`,
`push_in_process` depends on subscription type,
`mutation.concurrency = StateBased`,
`mutation.replay_safety = ReplayToken`,
`batching_policy = { max_items: 20, max_wait: 100ms,
flush_on_input_close: true }`.

**Thread id quirks.** Graph's `conversationId` is per-tenant unique
but can change on resubmit; `conversationIndex` encodes position in
a thread, not identity. Treat `thread_id` as opaque-within-protocol:
do not compare Graph `conversationId` against JMAP `threadId` or
Gmail `threadId` directly. Cross-protocol threading is a consumer
concern, derived from `message_id` / `references` / `in_reply_to`
in `InventoryEntry`.

## Cross-references

Types referenced from this document and defined elsewhere:

- `SyncEvent`, `Batch`, `Progress`, `Checkpoint`, `Control`,
  `Priority`, `BackfillCheckpoint`, `Projection`, `BlobHandle`,
  `BlobCapabilities`, `WatchEvent`, `MessageChange`, `ObjectChange`,
  `ScopeChange`, `MutationResult`, `MutationOutcome`, `DownloadOpts`,
  `HydratedObject`, `FlagSet`, `FlagOp`, `InvalidationSink`
  -> `sync-engine.md`
- `BlobHandle::download_to(path, opts)` is an engine-level utility
  built on `Account::open_blob_range`, not a trait method.
  -> `sync-engine.md` -> Body fetch.
- IMAP capability fingerprinting, three-state cursor lifecycle,
  per-folder modseq downgrade, MOVE / RFC 6851 interaction, `STORE
  UNCHANGEDSINCE` for concurrency, per-session capability scoping
  -> `plans/imap/condstore-qresync.md`.
- Per-protocol streaming notes -> `plans/jmap/streaming.md`,
  `plans/gmail/streaming.md`, `plans/graph/streaming.md`,
  `plans/imap/condstore-qresync.md`.

## Open questions

- **Scope discovery cache TTL.** Cost ranges O(1) (Gmail) to
  O(folders x types) (Graph). Engine caches the result; cache TTL
  needs spec.
- **Multi-account scope identity.** `FolderId`, `LabelId`,
  `MailboxId` are per-account; the engine multiplexes scopes across
  accounts. The scope types carry no account identity; engine tags
  implicitly via the `Account` handle.
- **EWS path.** Lives inside `bifrost-graph`. One `GraphAccount`
  with internal selector, or two `Account` impls? Lean to one with
  internal selector; revisit when EWS streaming notifications land.
- **`STORE UNCHANGEDSINCE` for IMAP.** Requires per-message `MODSEQ`
  in FETCH responses, which `imap-proto` does not currently parse.
  Adding it unlocks IMAP `MutationConcurrency::StateBased`. See
  `plans/imap/condstore-qresync.md`.
- **Account lifecycle and ownership.** Resolved in
  `plans/account-trait-shape.md` (Q3). Consumer registers an
  `AccountFactory` per account, engine owns the current open
  `Arc<dyn Account>` and calls `factory.open()` for reopen cycles,
  `close(&self)` is idempotent local handle teardown only (does
  not destroy server-side push subscriptions).
- **`InvalidationSink` shape.** Out-of-process push (Gmail Pub/Sub,
  Graph webhooks) feeds events into the engine. Exact API of the
  sink (push channel, queue, callback?) needs spec in `sync-
  engine.md` before consumers wire receivers.
