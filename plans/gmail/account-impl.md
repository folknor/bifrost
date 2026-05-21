# Gmail Account impl

Implementation plan for the `Account` trait on Gmail. Trait shape and
type contracts live in `plans/account-trait.md` and
`plans/account-trait-shape.md`; engine driving is in
`plans/sync-engine.md` and `plans/bifrost-sync.md`; pre-existing Gmail
streaming constraints are in `plans/gmail/streaming.md`. This document
pins how Gmail-specific primitives realize each trait method, what
state lives on the `GmailAccount` struct, and where each piece of code
lands inside `crates/gmail/`.

## Code layout

New module tree inside `crates/gmail/src/`:

```
account/
  mod.rs              // GmailAccount, GmailAccountFactory, Account impl
  capabilities.rs     // const AccountCapabilities builder
  cursor.rs           // GmailChangeState, OpaqueChangeState envelope
  changes.rs          // history.list -> changes_stream
  inventory.rs        // messages.list -> inventory_stream
  scopes.rs           // labels.list -> scope discovery + lifecycle
  push.rs             // users.watch / users.stop + push_stream
  mutation.rs         // batched messages.modify, bulk_set_flags
  blobs.rs            // attachments.get -> open_blob{,_range}
  flags.rs            // label canonicalization + flags_hash
  idempotency.rs      // IdempotencyKey -> engine bookkeeping
                      // (no wire header; see "Idempotency posture")
  recovery.rs         // 404 historyId -> RecoveryClass mapping
```

Existing modules (`client.rs`, `api.rs`, `types.rs`, `blob.rs`,
`parse.rs`, `headers.rs`, `error.rs`) stay as the wire layer. The
`account/` module is the engine-facing layer; everything in it
composes existing client calls into the streamed trait surface.

## GmailAccount struct

```rust
pub struct GmailAccount {
    client: Arc<GmailClient>,              // existing reqwest wrapper
    // Capabilities are immutable for the lifetime of this handle.
    // Mid-session capability transitions end affected streams with
    // RecoveryClass::CapabilityChanged; the engine reopens via the
    // factory and AccountSlot atomically swaps in a fresh
    // GmailAccount with fresh capabilities. No mutable cell here.
    capabilities: Arc<AccountCapabilities>,
    profile: GmailProfile,                 // snapshot at open(); the
                                           // emailAddress is the only
                                           // load-bearing field for
                                           // cross-check on cursor decode
    pubsub: PubSubControl,                 // users.watch / users.stop CRUD
    seed_state: OpaqueChangeState,         // historyId from getProfile
                                           // at open() - engine's
                                           // initial cursor seed
                                           // (per "Initial cursor seeding"
                                           // section below)
    idempotency: IdempotencyState,         // run_id + sequence + salt
    scope_cache: ArcSwap<ScopeSnapshot>,   // last labels.list result;
                                           // ArcSwap is intentional -
                                           // labels.list is a live poll,
                                           // not a capability transition
    shutdown: CancellationToken,
}

struct PubSubControl {
    topic: String,                          // projects/<proj>/topics/<topic>
    last_history_id: ArcSwap<Option<String>>,
    expiry: ArcSwap<Option<SystemTime>>,    // server-returned expiration;
                                            // renewer task watches this
    renewer: OnceCell<JoinHandle<()>>,      // background watch-renewal
                                            // task; spawned at
                                            // push_subscribe
}

struct ScopeSnapshot {
    labels: Vec<GmailLabel>,
    fetched_at: Instant,
    etag: Option<String>,
}
```

The struct is `Send + Sync`. All state lives behind `Arc`/`ArcSwap`/
channel handles so trait methods take `&self` and the engine can hold
`Arc<dyn Account>` across multiplexer, push reconciler, and mutation
runner tasks (see `account-trait-shape.md` Q2).

`GmailAccountFactory` implements `AccountFactory`. It holds the OAuth
refresher and the Pub/Sub topic config. It does NOT hold a channel
to the consumer's Pub/Sub listener: out-of-process push events go
through the engine's `InvalidationSink`, not through GmailAccount -
the listener calls `sink.push(account_id, WatchEvent::Invalidated{..})`
directly. `factory.open()` mints a fresh `Arc<GmailAccount>`, calls
`users.getProfile` to seed `historyId` (the initial cursor anchor),
builds capabilities, and returns. Construction never calls
`users.watch`; that is `push_subscribe`'s job.

## Capabilities

`AccountCapabilities` is built once at `factory.open()` and lives
behind `Arc`. Field values:

```rust
AccountCapabilities {
    cursor_freshness: CursorFreshness::ServerIssued,
    // No inventory_is_change_cursor_establish field - see
    // plans/account-trait.md -> Cursor establishment.
    // Gmail returns CursorEstablishment::Ready(cursor) for the
    // single CursorScope::Account, wrapping the historyId from
    // factory.open()'s users.getProfile probe.
    blob_range: BlobRangeSupport::No,
    blob_digest_pre_download: false,
    push: PushCapability::OutOfProcessPubsub,  // see trait
                                               // reconciliation note
    push_in_process: false,
    mutation: MutationCapabilities {
        concurrency: MutationConcurrency::None,
        replay_safety: MutationReplaySafety::None,
    },
    batching_policy: BatchingPolicy {
        max_items: 1000,                  // batchModify cap per
                                          // Gmail REST docs
        max_wait: Duration::from_millis(75),
        flush_on_input_close: true,
    },
    rate_limit_class: RateLimitClass::QuotaUnits {
        units_per_second: 250,
        per_request_cost: GmailQuotaCosts::default(),
    },
    quota_signal: QuotaSignal::Http429OrForbiddenWithMessage,
    requires_uidvalidity_recheck: false,
    historyid_expires_after: None,
}
```

Notes that drive the field choices:

- `cursor_freshness = ServerIssued`: `users.getProfile` returns
  `historyId` in one round-trip; the engine establishes a cursor
  immediately (via the seed-state path below) and backfills
  underneath via `inventory_stream`.
- Cursor establishment is independent of the inventory pass:
  `establish_initial_cursor(Account)` returns `Ready(cursor)`
  wrapping the cached historyId.
- `blob_range = No`: attachments come down as base64url inside JSON
  (`attachments.get`); there is no `Range`-able byte stream. See
  `plans/gmail/streaming.md`.
- `push_in_process = false`: Pub/Sub is out-of-process. The consumer
  runs the listener and feeds the engine's `InvalidationSink`.
- `mutation.concurrency = None`: Gmail has no `ifInState` or `If-Match`
  equivalent on `messages.modify`. Last-write-wins, surfaced by the
  engine's mutation conflict policy (`plans/sync-engine.md`).
- `mutation.replay_safety = None`: Gmail's official REST docs do
  NOT document `X-Goog-Request-Id` (or any other header) as a
  client-mintable idempotency token for `messages.modify` /
  `messages.batchModify` / `messages.batchDelete`. The prior draft
  assumed Cloud-API idempotency conventions that simply do not
  apply to Gmail. Ambiguous transport failure is resolved by the
  engine's read-back guard (per `plans/bifrost-sync.md` ->
  Read-back guard): re-fetch affected ids via
  `get_stream(Projection::FlagsOnly)` and reconcile applied vs
  not. If Google ever documents a real per-request token for
  Gmail, revisit and bump capabilities; until then, `None` is the
  honest answer.
- `batching_policy.max_items = 1000`: per Gmail REST docs the
  `messages.batchModify` and `messages.batchDelete` ids array
  accepts up to 1000 entries. The prior draft's 100 was wrong.
- `historyid_expires_after = None`: Gmail docs say historyId is
  *typically* valid at least a week but can expire after only a
  few hours under load (Google's wording). A deterministic
  expiry value misleads the engine into wrong scheduling. `None`
  means the engine treats the cursor as live until it 404s; the
  404 path (see "changes_stream" below) restarts cleanly via
  `RestartScope`. This is more honest than a fake-deterministic
  budget.
- The mutation-method enable/disable booleans (`bulk_set_flags`,
  `bulk_move`, `bulk_destroy`) on the prior draft were Gmail-only
  inventions not in `MutationCapabilities`. They are removed.
  Gmail's "move" semantic is relabeling; the impl handles this
  by routing `bulk_move` callers through `bulk_set_flags` (or
  returning `Error::Unsupported` if the caller specifically
  wants atomic move).

## Cursor representation

Single cursor scope: `CursorScope::Account`. Gmail has one historyId
per mailbox; there are no per-folder cursors. Label membership is
modeled via `MembershipScope::Label(LabelId)`, where `LabelId` is the
opaque Gmail label id (system labels like `INBOX`, `UNREAD`, `STARRED`
and user labels share the same id space).

Native cursor:

```rust
struct GmailChangeState {
    history_id: u64,
    profile_email: String,                 // cross-check on resume
    schema_version: u8,
}

const PROTOCOL: ProtocolKind = ProtocolKind::Gmail;
const ENVELOPE_VERSION: u32 = 1;
```

Encoded into `OpaqueChangeState { protocol, envelope_version, bytes }`
with the bytes being a `bincode` or `serde_json` payload of
`GmailChangeState`. On read, the cursor module validates `protocol ==
Gmail` and `envelope_version <= ENVELOPE_VERSION`; mismatch yields
`RecoveryClass::SchemaIncompatible`.

`advanced_through` is always `None` for Gmail. `history.list` paginates
with `pageToken`, but Gmail does not guarantee that a half-consumed
page is replayable in a way that lines up with `historyId` advancement
that survives `historyId` expiry; resume is from the last fully-
applied `historyId` only. Mid-page checkpoint inside a `Batch` carries
a fresh `historyId` only when `historyId` actually advanced between
pages (which Gmail does indicate via the `historyId` field on each
`history.list` response).

## discover_cursor_scopes

Yields exactly one event:

```rust
SyncEvent::Batch(Batch {
    items: vec![CursorScope::Account],
    checkpoint: None,
    ..
})
```

then `SyncEvent::Done`. There is no per-folder cursor partitioning to
discover on Gmail. The stream is a one-shot `stream::iter(vec).boxed()`
on the trait return.

## discover_memberships / scope_lifecycle_stream

`discover_memberships()` is one `labels.list` call. Each `GmailLabel`
becomes a `MembershipScope::Label(label_id)`. Output is a single
`Batch` with all labels then `Done`.

`scope_lifecycle_stream()` polls `labels.list` on the same adaptive
cadence the engine multiplexer uses for non-IDLE poll loops (default
30s, doubles on no-change, capped at 30min per `bifrost-sync.md`).
Diff against the previous `ScopeSnapshot` to emit
`ScopeLifecycle::Created` / `Renamed` / `Deleted` per
`plans/account-trait.md` (the prior draft used `Added` / `Removed`,
which are `ScopeChange` variants for per-object membership shifts,
not for scope creation/destruction). System labels never change
ids, so renames are user-label only.

The snapshot is also consumed by `flags.rs` (canonicalization needs
the id-to-name map for system labels).

## inventory_stream

`inventory_stream(CursorScope::Account)`:

Gmail's `establish_initial_cursor(Account)` returns
`Ready(cursor)` from the cached historyId. Inventory does NOT
mint a ChangeCursor and does NOT call `getProfile` itself; the
seed lives on the `GmailAccount` struct, populated at
`factory.open()` time (see "Initial cursor seeding" below).

1. Drive `users.messages.list` with paging (`maxResults=500`,
   `pageToken`).
2. For each page of stub ids, fan out `users.messages.get?format=
   metadata&metadataHeaders=Message-ID&metadataHeaders=References&
   metadataHeaders=In-Reply-To&metadataHeaders=Subject` with an
   in-flight cap of 8 concurrent gets (per
   `plans/gmail/streaming.md`).
3. Build an `InventoryEntry` per message:

```rust
InventoryEntry {
    id: ObjectId::Gmail(message.id),
    memberships: message.label_ids.into_iter()
        .map(MembershipScope::Label).collect(),
    size: message.size_estimate.unwrap_or(0) as u64,
    blob_id: None,                       // attachments are per-part
    fingerprint: Fingerprint::from_history_id(message.history_id),
    thread_id: Some(ThreadId::Gmail(message.thread_id)),
    message_id: parse_header("Message-ID"),
    references: parse_references_header(),
    in_reply_to: parse_header("In-Reply-To"),
}
```

4. Emit batches at `users.messages.list` page boundaries
   (`page_boundary = PageBoundary::PageEnd`). The `checkpoint` on
   each batch is a `BackfillCheckpoint` carrying the next
   `pageToken` (so a crash mid-inventory resumes on the next
   page), NOT a `ChangeCursor`. Per `plans/bifrost-sync.md` ->
   Backfill, backfill checkpoints persist through
   `CheckpointStore::put_backfill`, separate from the
   change-cursor store. The final page emits a backfill
   checkpoint with no `pageToken` (terminal) followed by
   `SyncEvent::Done`.
5. The batch fanned `messages.get` calls back-pressure via the
   per-account semaphore reservation in `bifrost-sync`; inventory
   never opens more than the configured cap regardless of stream
   poll rate.

`flush_on_input_close` flushes any in-flight `messages.get` results
into a final partial batch before `Done`.

The Batch endpoint (`/batch` multipart) is explicitly deferred per
`plans/gmail/streaming.md`. Stay on individual `messages.get` calls
behind the in-flight semaphore.

## Initial cursor seeding

`users.history.list` requires `startHistoryId` (per Google docs). A
fresh account has none, so a seed must be minted out of band.

Gmail's seed primitive is `users.getProfile`, which returns the
account's current `historyId` in one round-trip (~hundreds of
bytes). The factory pays this cost at `open()` time:

```rust
// crates/gmail/src/account/factory.rs (inside open())
let profile = client.get_profile().await?;
let seed = encode_gmail_state(GmailChangeState {
    history_id: profile.history_id,
    profile_email: profile.email_address.clone(),
    schema_version: ENVELOPE_VERSION as u8,
});
```

The seed lives on `GmailAccount.seed_state`. Two engine interactions:

1. **First attach.** Engine calls `discover_cursor_scopes()`,
   gets `[CursorScope::Account]`, then asks GmailAccount for the
   initial cursor (via the future trait extension
   `establish_initial_cursor(scope)` - reserved here, pinned in
   `plans/account-trait.md`). GmailAccount returns a
   `ChangeCursor` wrapping `seed_state`. Engine persists
   `(scope, cursor)` BEFORE the first `changes_stream(cursor)`
   call - any change after the seed is observed.
2. **Reattach with persisted cursor.** Engine reads the stored
   `ChangeCursor` from `CheckpointStore` and calls
   `changes_stream(cursor)` directly. The seed from this run's
   `open()` is unused.

This pattern mirrors JMAP's `*/get { ids: [] }` seed and IMAP's
inventory-establishes-cursor for Basic-tier folders. The
cross-protocol trait shape is a Phase 1 coordination point.

## changes_stream

`changes_stream(cursor)`:

1. Decode `cursor.server_state` into `GmailChangeState`. Validate
   `protocol == Gmail` and `envelope_version`. On mismatch, end the
   stream with `RecoveryClass::SchemaIncompatible`.
2. Cross-check `profile_email` against `client.get_profile().email_
   address`. Mismatch is `RecoveryClass::Fatal { reason:
   AccountIdentityChanged }` (account swap underneath us).
3. Call `users.history.list?startHistoryId=<id>&historyTypes=
   messageAdded&historyTypes=messageDeleted&historyTypes=labelAdded&
   historyTypes=labelRemoved&maxResults=500` with paging.
4. For each `GmailHistoryItem`, translate. `ObjectChange` carries
   only `{ id, kind }` per `plans/sync-engine.md`; membership
   information rides as separate `ScopeChange` events. There is no
   `MembershipScope::AllOf` variant - the prior draft invented it.
   - `messages_added` -> emit `ObjectChange { id, kind: Created }`
     **plus** one `ScopeChange { id, membership:
     MembershipScope::Label(label_id), kind: Added }` per label
     the message landed with. The engine's `MembershipIndex`
     records the memberships from the ScopeChange events; no
     membership data rides on `ObjectChange`.
   - `messages_deleted` -> emit `ObjectChange { id, kind:
     Destroyed }`. Gmail's `history.messagesDeleted` is
     authoritative (Google docs name the deleted message ids
     explicitly), so the protocol emits `Destroyed` directly
     rather than relying on engine derivation. This matches
     JMAP's `Email/changes.destroyed` handling per
     `plans/account-trait.md` change emission contract.
   - `labels_added[*]` -> `ScopeChange { id, membership:
     MembershipScope::Label(label_id), kind: Added }`.
   - `labels_removed[*]` -> `ScopeChange { id, membership:
     MembershipScope::Label(label_id), kind: Removed }`.
5. Emit one `Batch` per `history.list` page. `checkpoint` is `Some`
   on every page boundary, carrying the new
   `OpaqueChangeState{history_id = response.history_id}`. Gmail's
   page boundary is the cursor advance boundary, which matches the
   `Batch`-with-checkpoint contract from `sync-engine.md`.
6. When `next_page_token` is absent, the stream emits `Done` with
   the final checkpoint.

### 404 historyId mapping (RestartScope)

When `client.get_history()` returns HTTP 404 (or the typed
`Error::HttpStatus { status: 404, .. }` from `error.rs`), the cursor
is dead - Gmail aged it out. This is **not** a capability change:
nothing about the account's surface shifted, the cursor token
itself just lapsed. The right recovery class is the same one Graph
uses for `410 Gone` on delta tokens - `RestartScope`:

```rust
SyncEvent::Fatal(Fatal::Recovery(
    RecoveryClass::RestartScope(CursorScope::Account)
))
```

Per `plans/account-trait.md`, `RestartScope` tells the engine to
clear the stored cursor for the scope and re-establish: re-seed via
`establish_initial_cursor`, then run a fresh inventory + change
stream in parallel. The account handle is **not** torn down (unlike
`RestartAccount` or `CapabilityChanged`); capabilities are
unchanged. The consumer sees a `Warning::CursorReset` plus the
fresh inventory underneath the live change stream.

This is the only path for historyId recovery. There is no in-stream
retry, no partial re-baseline. The classification lives in
`account/recovery.rs::classify_history_error`.

## push_subscribe / push_unsubscribe / push_stream

`push_subscribe(scopes)`:

1. Validate `scopes` is `vec![CursorScope::Account]`. Gmail watches
   the whole mailbox; per-scope watches do not exist.
2. POST `users.watch` with the configured topic and `labelIds` (omit
   for all-label watch; include for filtered watch when the consumer
   asks).
3. Server returns `{ historyId, expiration }`. Store both on
   `PubSubControl`. Return a `SubscriptionHandle` whose opaque bytes
   carry the topic + expiration so `push_unsubscribe` can target
   exactly that subscription.
4. Spawn (lazily, into `PubSubControl.renewer`) a background
   renewer task that re-POSTs `users.watch` ~6 days into the 7-day
   window. Renewal is internal to GmailAccount; the task does not
   emit `WatchEvent`s anywhere. On sustained renewal failure the
   task logs a warning and exits; the engine's lack of Pub/Sub
   wake-ups naturally falls through to poll-only behavior on the
   account, which is correct rather than broken.

`push_unsubscribe(handle)`:

1. POST `users.stop`. Returns 204 on success.
2. Mark `PubSubControl.expiry = None`. Stop the renewer task if
   running.

`push_stream()`:

`push_in_process = false`, so per `plans/sync-engine.md` -> Push
is invalidation, Gmail wake-ups arrive on the engine's
`InvalidationSink`, not on `push_stream`. The consumer's Pub/Sub
listener decodes Cloud Pub/Sub messages (`emailAddress`,
`historyId`) and calls
`InvalidationSink::push(account_id, WatchEvent::Invalidated { hint:
HintPayload::Unknown })` on the engine. GmailAccount is not in this
data path.

`push_stream()` therefore returns an **empty** stream - no items,
just natural termination. It does not carry `Reconnected` /
`Disconnected` / `Invalidated` for out-of-process push; the engine
reads those from the sink, not from this stream. There is no
`SyncEvent::Done` sentinel since `AccountStream<WatchEvent>`
yields `WatchEvent` items directly and stream-end is the natural
termination. The prior draft's `broadcast::Sender<WatchEvent>` and
watchdog emission into `push_stream` were inconsistent with the
in-process / out-of-process split pinned in
`plans/account-trait.md` and have been removed.

Subscription health surfacing (sustained renewal failure, expired
without renewal) is a known gap: the engine has no current way to
learn the account's push channel is unhealthy except via the
absence of `Invalidated` events. Tracked under "Risks / Opens"
below for a future trait extension.

This split is the load-bearing reason `push_in_process = false`. The
consumer must wire Pub/Sub credentials and the listener at the
binary level; bifrost-gmail only CRUDs the watch. See
`plans/gmail/streaming.md`.

## open_blob / open_blob_range

`open_blob(handle)`:

1. Parse `handle.id` as `(message_id, attachment_id)`.
2. Call `users.messages.attachments.get` (existing
   `client::get_attachment`). The current wire implementation
   parses the response into `GmailAttachmentData` - a struct with
   the full base64url string materialized in memory. Streaming
   decode requires a new wire method that pumps the JSON body as
   it arrives; not in scope for v1.
3. **v1 is full-buffered**: decode the entire base64url string
   via `BlobHash`/decode helpers in `blob.rs`, then emit a single
   `SyncEvent::Batch(Batch { items: vec![bytes], .. })` followed
   by `Done`. Memory footprint per call is roughly `1.33 *
   attachment_size` during decode (raw JSON + decoded bytes
   co-resident); ~33MB peak for a 25MB attachment.
4. Computed digest emitted at `Done`. Because
   `blob_digest_pre_download = false`, the digest is computed
   client-side via `BlobHash::hash` (existing in `blob.rs`).

A streaming-decode wire method (chunked base64url over a JSON
streaming parser) is tracked as a follow-up in Risks/Opens; v2.

`open_blob_range(handle, range)`:

Gmail does not support byte ranges on `attachments.get`. The
implementation downloads the full body and slices in-process,
with explicit bounds validation:

```rust
let full = open_blob(handle).collect_bytes().await?;
let total = full.len() as u64;
// Bounds: start must be <= total; start+length must be <= total.
if range.start > total {
    return stream::iter([Err(Error::RangeOutOfBounds {
        start: range.start, total
    })]).boxed();
}
let end = match range.length {
    Some(len) => range.start.checked_add(len)
        .filter(|e| *e <= total)
        .ok_or(Error::RangeOutOfBounds { start: range.start, total })?,
    None => total,
};
let sliced = full.slice(range.start as usize .. end as usize);
stream::iter([Ok(sliced)]).boxed()
```

This is technically wasteful, but the alternative is to refuse the
call, and the engine's `BlobHandle::download_to` convenience
(`sync-engine.md`) decides on parallel vs serial based on
`supports_range`. With `supports_range = false`, the engine never
calls `open_blob_range` from `download_to`; it falls through to
`open_blob`. We implement `open_blob_range` anyway for caller
correctness (consumer code that bypasses `download_to`), but it is
a single-shot full-download wrapper.

## bulk_set_flags

Gmail "flags" are labels: `\Seen` <-> not `UNREAD`, `\Flagged` <->
`STARRED`, `\Draft` <-> `DRAFT`. `bulk_set_flags` translates:

1. Consumer-supplied `FlagSet` is canonicalized in `flags.rs`
   (described below) into `add_label_ids` + `remove_label_ids`.
2. Stream `targets: AccountStream<ObjectId>` is chunked in
   batches of `batching_policy.max_items = 1000` (Gmail
   `batchModify` cap per Google's REST docs).
3. For each chunk, POST `users.messages.batchModify`. **No
   client-mintable idempotency header.** Gmail's REST docs do not
   document `X-Goog-Request-Id` (or any other header) as a
   per-request idempotency token for the messages endpoints. The
   prior draft assumed Cloud-API conventions that do not apply
   here. Retries re-submit the same request body; on ambiguous
   transport failure (network timeout where we don't know if the
   server applied) the engine's read-back guard (per
   `plans/bifrost-sync.md` -> Read-back guard) re-fetches via
   `get_stream(Projection::FlagsOnly)` and reconciles.
4. `batchModify` per Gmail docs returns 204 No Content on
   success; the request is atomic across the listed ids. Treat
   204 as `MutationOutcome::Applied` for every id in the chunk.
   `429` / `503` → `RecoveryClass::Retry { after }` honoring
   `Retry-After`; the runner re-submits the same body. Non-204
   2xx with field-level errors (rare) → per-item
   `MutationOutcome::Failed`.
5. Emit one `Batch<MutationResult>` per chunk with `checkpoint:
   Some` carrying the campaign sequence number so the engine's
   mutation runner can resume mid-campaign across process restarts
   (per `bifrost-sync.md::mutation/`).

`bulk_destroy`: POST `users.messages.batchDelete`, same chunking
and same no-replay-token posture. Destroy is irrevocable on Gmail;
`MutationConcurrency::None` means "the consumer says delete, we
delete" with no state-based safety. Surface
`MutationResult::Applied` per 204, then expect a corresponding
`ObjectChange::Destroyed` on the next `changes_stream` page
(Gmail's `history.messagesDeleted`).

`bulk_move` is expressed as relabeling. For consumers calling the
trait's `bulk_move(target, destination_scope)`, the impl translates
to `bulk_set_flags` with `add: {destination_label}` and
`remove: {INBOX, ...current_inbox_labels}`. Atomic move semantics
(succeed-or-rollback across the relabel pair) are not available on
Gmail; if the caller specifically asks for atomicity, return
`Error::UnsupportedAtomicMove`.

The `MutationCapabilities` struct no longer carries
`bulk_set_flags` / `bulk_move` / `bulk_destroy` booleans (the
prior draft invented them); per-operation feasibility surfaces
through the `Error::UnsupportedAtomicMove` return value where
relevant.

## Idempotency posture

Gmail has no documented client-mintable replay token for the
messages endpoints. `MutationReplaySafety` is `None`. On ambiguous
transport failure, the engine's read-back guard (per
`plans/bifrost-sync.md` -> Read-back guard) re-fetches affected
ids via `get_stream(Projection::FlagsOnly)` after a retried batch
and reconciles applied / failed against the intended mutation.

The Account impl on retry re-submits the same request body with
the same `IdempotencyKey` from the engine; the key serves engine
bookkeeping (campaign correlation, retry-queue dedup) but does
not become a wire header. The `IdempotencyKey::Gmail` variant
exists in the trait for future use if Google ever documents a
real token; today it carries the campaign id for engine-side
diagnostics only.

The `bifrost-sync.md` Read-back guard section MUST list Gmail
explicitly. Cross-document follow-up.

## Flag canonicalization

`flags.rs::canonical_flags(message)` produces a stable
`(Vec<String>, FlagsHash)` tuple from a Gmail message:

1. Take `message.label_ids`.
2. Translate the seen-polarity: if `UNREAD` is absent in
   `label_ids`, the canonical set includes `\Seen`; otherwise it
   does not. (Gmail uses unread-polarity; IMAP/JMAP use
   seen-polarity. The Account impl is responsible for putting the
   data in the engine's seen-polarity per `account-trait.md` flag
   canonicalization rule.)
3. Translate system labels to canonical names:
   - `STARRED` -> `\Flagged`
   - `DRAFT` -> `\Draft`
   - `IMPORTANT` -> `$Important` (RFC 5788 keyword)
   - `SENT`, `TRASH`, `SPAM`, `CHAT` -> Gmail-specific membership,
     not flags; do not emit as flags but do record memberships.
4. Lowercase the system flag set (per `account-trait.md`).
5. Pass user-defined label names through unchanged but quoted with
   their label id, since Gmail label ids are stable while names
   may change.
6. Sort the resulting set lexicographically.
7. Hash via FNV-1a / xxhash per `account-trait.md`.

Inverse mapping for `bulk_set_flags`:

- Incoming `FlagSet { add: {\Seen}, remove: {} }` -> Gmail
  `remove_label_ids: [UNREAD]`.
- Incoming `FlagSet { add: {\Flagged}, remove: {} }` -> Gmail
  `add_label_ids: [STARRED]`.
- Unknown flag -> `MutationResult::Skipped { reason:
  UnknownFlagForProtocol }`. Engine surfaces as warning.

The id-to-name map for system labels is hard-coded in `flags.rs`;
user-label translation goes through `ScopeSnapshot` for current
ids. If `ScopeSnapshot` is stale (last fetched > 5 minutes ago),
`bulk_set_flags` triggers a one-shot `labels.list` refresh before
chunking.

## get_stream

`get_stream(ids, projection)`:

Projection mapping:

- `Projection::FlagsOnly` -> `users.messages.get?format=minimal`.
- `Projection::Headers` -> `format=metadata` plus
  `metadataHeaders=Subject,From,To,Cc,Date,Message-ID,References,
  In-Reply-To`.
- `Projection::Full` -> `format=full`.
- `Projection::FullWithBlobs` -> `format=full` plus eager
  `attachments.get` fan-out per part. (Engine consumer normally
  prefers calling `open_blob` lazily; `FullWithBlobs` exists for
  one-shot exports.)

Fan-out concurrency is the same 8-in-flight semaphore as
`inventory_stream`. Output is `HydratedObject` batches per
`account-trait.md`.

## close

`close(&self)`:

1. Trip `shutdown` token.
2. Close `push_outbound` broadcast channel.
3. Do *not* call `users.stop`. Per `account-trait-shape.md`,
   `close` is local handle teardown; push subscription teardown is
   `push_unsubscribe`, called by the engine on detach-with-
   unsubscribe.
4. Drop the `reqwest::Client` (connection pool teardown is
   reqwest-internal).
5. Idempotent: a second call returns `Ok(())` immediately.

## describe_cursor

```rust
fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor {
    let _state = decode_gmail_state(&cursor.server_state)?;
    CursorDescriptor {
        // Any extant Gmail historyId is cheap to consume - one
        // history.list call delivers the delta in O(changes) cost.
        // The 5-day pre-expiry cliff the prior draft posited was
        // wrong: Gmail docs say historyId is *typically* valid at
        // least a week but can age out after only a few hours
        // under load, so a deterministic budget misleads the
        // engine. Expiry is an event (404 -> RestartScope), not a
        // capacity to plan against.
        cost_class: CostClass::Cheap,
        strategy: SyncStrategy::HistoryDelta,
        freshness: Some(Instant::now()),
    }
}
```

## Risks / Opens

- **History gaps inside Gmail's 7-day window.** Gmail occasionally
  reports gaps even when `historyId` is well inside the freshness
  window (observed on labels with millions of messages). Current
  plan treats every non-404 success as authoritative; if production
  reveals systematic drift we may need a periodic
  inventory-vs-cursor reconciliation pass. Engine has no hook for
  this today; would land as a new `RecoveryClass` variant.
- **Pub/Sub listener placement.** The consumer owns the listener.
  Bifrost cannot assert delivery-ordering or at-least-once
  semantics; we treat every wake-up as `HintPayload::Unknown` and
  let `changes_stream` reconcile. Worth documenting explicitly in
  the consumer integration guide; not a code concern in
  `bifrost-gmail`.
- **batchModify partial failure.** Gmail's docs say `batchModify`
  is all-or-nothing, but field reports show occasional
  per-message 404 inside an otherwise-204 response (deleted
  underneath the request). The current `MutationResult::Applied`
  blanket may misreport these. The engine's read-back guard
  catches drift on the next `changes_stream` page; defer detailed
  detection until we see this in production.

- **Subscription-health surfacing.** Out-of-process push means
  the engine learns about wake-ups via `InvalidationSink` but has
  no current channel for "the Pub/Sub subscription expired and
  renewal is failing" - sustained renewal failure today degrades
  silently into poll-only behavior. A future trait extension
  (e.g. `push_health_stream` separate from `push_stream`, or a
  health field on `SubscriptionHandle`) would let the engine
  surface this. Tracked cross-protocol since Graph webhooks have
  the same gap.

- **Attachment streaming decode.** The current wire API parses
  full JSON into `GmailAttachmentData`, materializing the entire
  base64url string before decode. v1 is documented as
  full-buffered (~33MB peak for a 25MB attachment). v2 needs a
  new wire method that pumps the JSON body incrementally and
  decodes base64url in chunks of 64KB; this requires extending
  the transport surface (touches `bifrost-net` once it lands).
- **Batch endpoint deferral.** `plans/gmail/streaming.md` already
  defers `/batch` multipart batching. The 8-in-flight concurrency
  cap may be the bottleneck for very large inventory passes; if
  so, `/batch` rejoins the table. Out of scope for this plan.
- **Trait-shape extension: `establish_initial_cursor`.** This
  plan reserves the method shape (see "Initial cursor seeding"
  above); the trait extension lands in
  `plans/account-trait.md` as a Phase 1 follow-up coordinated
  with JMAP and IMAP. If the trait shape changes (e.g. cursor
  establishment folded into `discover_cursor_scopes` returning
  `(scope, Option<cursor>)`), the Gmail impl tracks.
- **Cursor schema migrations.** `envelope_version = 1` today; once
  we ship, any new field on `GmailChangeState` needs a v2 with a
  `migrate_v1_to_v2` per `bifrost-sync.md::cursor/envelope.rs`
  conventions. No migration owner today; will land when the first
  schema change does.
- **Label id reuse.** Gmail's label ids appear stable, but the docs
  do not guarantee non-reuse after delete. If a deleted user label
  id is reused by a freshly-created label, the engine's per-object
  membership tracker may attribute messages to the wrong scope on
  the diff page that straddles the recreate. Mitigation candidate:
  include a (label_id, label_name_hash) tuple in the membership
  index; deferred until we observe collisions.
