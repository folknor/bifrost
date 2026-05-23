# bifrost-gmail reference

Current architecture of the Gmail Account-layer code under
`crates/gmail/src/account/`. The crate hosts a `GmailClient` for
the Gmail REST API plus an `Account` / `AccountFactory` pair that
sits on top of it, driving history-id-based sync, Cloud Pub/Sub
push, and Gmail-specific flag canonicalization.

Gmail has no UID model and no mailbox-scoped server state. A
single `historyId` walks the account-wide change log, and labels
play the role of folders. The Account layer reflects that: one
`CursorScope::Account` cursor per account, a single
`changes_stream` that pages `users.history.list`, and mutations
that route through `messages.batchModify` / `batchDelete`.

## Module layout

`crates/gmail/src/account/`:

- `mod.rs` - `pub(crate) GmailAccount`, public `GmailAccountFactory`, `impl Account`.
- `capabilities.rs` - `AccountCapabilities` builder.
- `cursor.rs` - `GmailChangeState`, envelope encode/decode,
  `cursor_from_state`.
- `scopes.rs` - `discover_cursor_scopes`, `discover_memberships`,
  `scope_lifecycle_stream`, `ScopeCache` / `ScopeSnapshot`.
- `inventory.rs` - inventory pass and `get_stream` hydration.
- `changes.rs` - history-id driven change stream.
- `push.rs` - Cloud Pub/Sub `watch`/`stop`, `PubSubConfig`,
  `PubSubControl`, renewer task.
- `mutation.rs` - `bulk_set_flags`, `bulk_move`, `bulk_destroy`.
- `pim.rs` - Phase 3.6 unified PIM primitives: message and
  thread label mutations, MIME send and drafts, search translation,
  container CRUD, identities, vacation responder, and typed
  message/thread hydration.
- `flags.rs` - Gmail-label-to-IMAP-flag canonicalization and the
  reverse `LabelPatch` translation used by mutations.
- `blobs.rs` - `open_blob` / `open_blob_range` over Gmail
  attachments.
- `recovery.rs` - error classification onto `RecoveryClass`.

## GmailAccount / GmailAccountFactory

`GmailAccountFactory` carries an `Arc<GmailClient>` and an
optional `PubSubConfig`. `open(account_id)` first asks the client
for an account-scoped clone attached to `bifrost-net` under the
engine supplied `AccountId`, then does one `users.getProfile`
round-trip, parses `profile.historyId` into a `u64`, and stores
the resulting `GmailChangeState` as `seed_state`. The opened
`GmailAccount` retains:

- `client: Arc<GmailClient>`.
- `capabilities: AccountCapabilities` snapshotted at open.
- `profile: GmailProfile` for `email_address` and history-id
  identity checks downstream.
- `seed_state: OpaqueChangeState` used by
  `establish_initial_cursor` to mint a cursor without a second
  network call.
- `pubsub: Arc<PubSubControl>` holding the optional config, the
  last-known watch `historyId` and `expiration`, a renewer
  `JoinHandle`, the active-handle set, and a `broadcast::Sender<WatchEvent>`.
- `scope_cache: Arc<RwLock<ScopeSnapshot>>` for the label list.
- `shutdown: CancellationToken` for the renewer and the
  lifecycle stream.
- `set_priority` and `set_bandwidth_cap` delegate to the
  underlying `AccountNet`; the transport owns the canonical knobs.

Clients constructed through the default token constructors retain
their parent `Net`, so `open(account_id)` mints a fresh
`AccountNet` under the engine id on every reopen. Clients built
via `GmailClient::with_account_net` have no parent `Net`; the
factory calls `AccountNet::retag(account_id)` on the pre-attached
handle so per-account metering and host bookkeeping move under
the engine id without losing in-flight clones.

`AccountFactory::open(account_id)` returns `Arc<dyn Account>`.
`reopen` flows from the engine: the engine drops the previous
`Arc` and calls the factory again with the same `AccountId`. The
factory holds the credentials and client, so the new `GmailAccount`
carries a fresh
`shutdown`/`pubsub`/`scope_cache` and reads the current profile
at open time.

`close()` is idempotent. It marks `closed`, cancels `shutdown`,
and aborts the Pub/Sub renewer task. The renewer task selects on
`shutdown.cancelled()` and exits cleanly. `push_stream` exits
on the same cancellation token.

## Capabilities

`gmail_capabilities()` in `capabilities.rs`:

- `cursor_freshness: CursorFreshness::ServerIssued`. The
  `historyId` is server-issued and monotone per account; the
  engine can trust it as a freshness signal without a local
  clock.
- `blob_range: BlobRangeSupport::No`. Gmail attachments arrive
  base64url-encoded inside a JSON envelope. There is no HTTP
  Range surface against attachment downloads; `open_blob_range`
  enforces the unsupported case early.
- `blob_digest_pre_download: false`. The attachment metadata
  does not carry a digest separately from the body.
- `push: PushCapability::OutOfProcessPubsub`. Push lives on
  Google Cloud Pub/Sub, not on a connection bifrost owns.
  `push_in_process()` is false; consumers wire their own
  Pub/Sub subscriber and feed `InvalidationSink` from there.
- `mutation.concurrency: MutationConcurrency::None`. Gmail has
  no server-side optimistic-concurrency primitive on
  `batchModify`. The engine's read-back guard is the
  lost-update safety net.
- `mutation.replay_safety: MutationReplaySafety::None`. Gmail
  does not document a client-mintable dedup token, so the
  shared `IdempotencyKey` is accepted and held engine-side
  rather than wired onto the request.
- `batching_policy: BatchingPolicy { max_items: 1000, max_wait:
  75ms, flush_on_input_close: true }`. 1000 matches Gmail's
  `batchModify` cap.
- `rate_limit_class: RateLimitClass::Tiered`. Gmail uses
  per-user quota units rather than a uniform rps cap.
- `quota_signal: QuotaSignal::QuotaUnits`.
- `requires_uidvalidity_recheck: false`. Gmail has no
  UIDVALIDITY model.
- `historyid_expires_after: None`. Gmail does not document a
  fixed retention window for `historyId`. The Account layer
  detects expiry reactively via `classify_history_error`
  (404/410 -> `RestartScope`) rather than scheduling on a
  timer. `describe_cursor` reports `CostClass::Expensive` only
  when the cursor envelope no longer decodes against the open
  account's email-address; that flips
  `SyncStrategy::ServerCursor` to `SyncStrategy::None` and
  `freshness` to `None`, prompting the engine to re-establish.
- `delta_token_expires_after: None`. Gmail has no delta token.
- `pim_methods`:
  - Supported: `add_to_container`, `remove_from_container`,
    `set_label_membership`, `set_is_read`, `send_message`,
    `draft_create`, `draft_update`, `draft_discard`, `draft_send`,
    `search`, `search_messages`, `containers_list`,
    `container_create`, `container_rename`, `container_delete`,
    `identities_list`, `identity_update`, `vacation_get`,
    `vacation_set`, `thread_hydrate`, `message_hydrate`.
  - Unsupported: `set_keyword`, `set_category`,
    `set_extended_property`, `attachment_upload`,
    `container_move`, `quota_get`.
- `conveniences.starred: LabelMembership`. The default
  `set_starred` convenience dispatches to Gmail's `STARRED` label.
  Replied and forwarded convenience flags are false because Gmail
  derives that state from messages rather than exposing a writeable
  flag.

## PIM primitives and conveniences

`pim.rs` implements the S1-W2 Gmail shape for the unified Account
trait.

Mail mutation primitives use Gmail label modification:

- `add_to_container` and `remove_from_container` dispatch to
  `users.messages.modify` or `users.threads.modify` depending on
  `MutationTarget`.
- `set_label_membership` is the same add/remove label operation.
- `set_is_read` flips Gmail's `UNREAD` label with inverted polarity.
- `set_keyword`, `set_category`, and `set_extended_property` return
  `AccountError::Unsupported`.
- The explicit Archive container is synthetic. Adding a target to
  Archive removes `INBOX`; removing from Archive is a no-op because
  archive is the absence of the Inbox label, not a native label.

Composition primitives build RFC 5322 MIME locally and send the
base64url raw message through Gmail:

- `send_message` calls `users.messages.send`. Inline attachments are
  encoded into the MIME tree. Pre-uploaded attachment handles are
  unsupported because Gmail has no separate upload primitive for
  message attachments.
- `draft_create`, `draft_update`, `draft_discard`, and `draft_send`
  call Gmail drafts endpoints. `draft_update` fetches the current
  draft in `full` format, projects editable headers/body/attachments
  into the shared draft document, applies the partial patch, then
  replaces the draft with a new raw MIME body.
- `attachment_upload` returns `Unsupported`.

Search translates the shared `SearchRequest` AST into Gmail query
strings and uses `users.threads.list` for thread-shaped search and
`users.messages.list` for message-shaped search. `provider_query` is
appended verbatim so consumers can use Gmail-specific operators such
as `larger:5M`.

Container CRUD treats Gmail labels as `ContainerKind::Label` and
returns native Gmail label ids. System labels `INBOX`, `SENT`,
`DRAFT`, `TRASH`, and `SPAM` map to the matching `FolderRole`.
Archive is surfaced as a synthetic label-shaped container with
native id `archive` and `FolderRole::Archive`. Creating, renaming,
and deleting labels call Gmail label endpoints. Moving containers is
unsupported because Gmail labels are flat.

Settings primitives map to Gmail settings endpoints:

- `identities_list` and `identity_update` use
  `users.settings.sendAs`.
- `vacation_get` and `vacation_set` use
  `users.settings.vacation`.
- `quota_get` returns `Unsupported`; the Gmail API profile exposes
  message counts but not storage quota bytes.

Hydration primitives use Gmail's `full` and `metadata` message
formats. `thread_hydrate` calls `users.threads.get` and projects
each message. `message_hydrate` chooses the cheapest Gmail format for
the requested `HydrationProjection`, parses common address and
threading headers, maps label ids to containers and canonical flags,
and surfaces attachment blob handles for `FullWithBlobs`.

The Gmail overrides for multi-call conveniences are:

- `move_thread` adds the target container first, then removes the
  source container when supplied. The implementation lives in the
  crate so it can own a cloned client inside the boxed future.
- `delete_thread` moves to `TRASH` unless the caller says the thread
  is already in Trash, in which case it calls `users.threads.delete`
  for permanent deletion.

Other conveniences inherit the trait default. `set_starred` routes
through `set_label_membership` because capabilities advertise
`LabelMembership`. `apply_label` and `remove_label` use the default
provenance dispatch for Gmail label ids.

## Cursor envelope

`GmailChangeState` carries:

- `history_id: u64`.
- `profile_email: String`.
- `schema_version: u8` pinned at `1`.

`encode_gmail_state` serializes to JSON and wraps as
`OpaqueChangeState { protocol: ProtocolKind::Gmail,
envelope_version: 1, bytes }`. `decode_gmail_state` rejects
wrong protocol, wrong envelope version, and wrong schema
version, mapping each to `AccountError::SchemaIncompatible`.
JSON deserialization errors map to `AccountError::Other`.

`decode_gmail_state_for_profile` layers an
identity check on top: the decoded `profile_email` must equal
the open account's `email_address`. A mismatch returns
`AccountError::Other` describing both sides; this is the
account-identity check that prevents a checkpoint from one
Google account being replayed into another (for example after
the consumer rotates accounts under the same persistence key).

`cursor_from_state` projects to
`ChangeCursor { scope: CursorScope::Account, server_state,
advanced_through: None, envelope_version: 1 }`. There is no
`advanced_through`; Gmail does not page over time intervals
in the cursor.

## Per-scope inventory / changes / hydration

`CursorScope::Account` is the only scope. `inventory_stream`,
`establish_initial_cursor`, `push_subscribe`, and the change
stream each reject any other scope with a Fatal carrying
`RecoveryClass::Fatal` (inventory) or
`AccountError::Unsupported` (cursor establishment, push).

`inventory_stream` walks `users.messages.list` in pages of 500
ids, then hydrates each page through `users.messages.get` with
the `metadata` format under `buffer_unordered` concurrency of
32. The final page emits an `inventory_checkpoint` derived from
`users.getProfile` so the engine can commit a cursor anchored to
the historyId observed at the end of the inventory pass. Page
boundaries are `PageBoundary::Page` for intermediate batches and
`PageBoundary::Final` for the last batch that carries the
checkpoint.

`get_stream` consumes a stream of `ObjectId`s in batches of 32
and dispatches per `Projection`:

- `FlagsOnly` - `format=minimal` then `flag_set` over the
  label list.
- `Metadata` - `format=metadata` projected through
  `inventory_entry_from_message`.
- `FullWithBlobs` - `format=raw` for the bytes plus
  `format=full` to enumerate attachment blob handles.
- `Headers` / `Preview` / `TextOnly` / `Full` - `format=raw`,
  with `HydratedObjectKind::RawMime`.
- Any other variant falls back to `format=metadata`.

`changes_stream` decodes the cursor, re-verifies
`profile_email`, then `users.getProfile`-checks that the open
account still matches the cursor's recorded identity. A drift
yields a Fatal with `RecoveryClass::Fatal`. With the identity
confirmed, the stream pages `users.history.list` from
`startHistoryId`. Each page emits a `Batch` whose `checkpoint`
is a `Checkpoint::Change(cursor_for_history(history_id,
email))`, and the final page (no `nextPageToken`) terminates
with `SyncEvent::Done(None)`.

History entries map to `Change` variants in
`changes_from_history`:

- `messagesAdded` -> `ObjectChange::Created` plus one
  `ScopeChange::Added` per label on the new message.
- `messagesDeleted` -> `ObjectChange::Destroyed`.
- `labelsAdded` / `labelsRemoved` -> `ScopeChange` rows scoped
  per label.

## Push: Cloud Pub/Sub

Push is out-of-process. `PubSubConfig` holds a `topic` and an
optional `label_ids` filter. Wiring is opt-in: an account opened
without `with_pubsub_config` rejects `push_subscribe` with
`AccountError::Unsupported`.

`push_subscribe` issues `users.watch` with the configured
topic, stores `(historyId, expiration)` on `PubSubControl`,
emits `WatchEvent::Reconnected` on the broadcast channel, and
spawns the renewer task. The returned `SubscriptionHandle` is a
JSON envelope `{ topic, history_id, expiration }`.

The renewer task in `start_renewer`:

- Sleeps until `renewal_delay(expiration)` - one day before
  the expiration timestamp from Gmail, or `DEFAULT_RENEW_AFTER`
  (six days) if no expiration was returned.
- Re-issues `users.watch` and updates the stored expiration.
- On failure: emits `WatchEvent::Disconnected` (once, not
  repeatedly), retries after `RENEW_RETRY_AFTER` (five
  minutes), and emits `WatchEvent::Reconnected` when the
  next attempt succeeds.
- Selects on `shutdown.cancelled()` between every sleep and
  every watch call so `close()` cuts the loop promptly.

`push_unsubscribe` decodes the handle envelope, removes the
handle from the active-handle set, and only when the set
empties does it call `users.stop`, clear the stored expiration
and last-history-id, and abort the renewer. The active-handle
set lets multiple subscribers share one Gmail watch.

`push_stream` is a `broadcast::Receiver<WatchEvent>` adapter
with shutdown wiring. `Lagged` is treated as a skip rather than
an error.

The Pub/Sub message payload itself is not parsed by this crate;
the consumer subscribes to the Pub/Sub topic out of process,
decodes the JSON envelope, and feeds the change hint into the
engine's `InvalidationSink`. The Account layer only owns the
watch lifecycle.

## Mutation pipeline

`bulk_set_flags`, `bulk_move`, and `bulk_destroy` share a
single `mutation_stream` driver:

- Drain up to 1000 ids per round (the Gmail `batchModify` /
  `batchDelete` cap).
- Translate the operation once per stream: `SetFlags` runs
  through `translate_flag_op` against the cached label list;
  `Move` builds an add/remove `LabelPatch` from the destination
  label; `Destroy` carries no translation.
- Post the batch:
  - `SetFlags` / `Move` -> `users.messages.batchModify`.
  - `Destroy` -> `users.messages.batchDelete`. On 403 it falls
    back to a label patch that moves the messages into `TRASH`,
    matching the scopes Gmail OAuth tokens with the
    `gmail.modify` scope can perform but `gmail.metadata` cannot.

Result classification:

- All ids in a successful batch -> `MutationOutcome::Applied`.
- An empty patch (label op with no changes) or an
  unsupported-flag patch -> `MutationOutcome::Skipped` per id.
- A retry-class or auth-class error -> stream-level
  `SyncEvent::Fatal` so the engine can re-issue. The driver
  does not split applied vs failed across the same batch on
  HTTP failure.
- Any other error -> per-id `MutationOutcome::Failed(error)`,
  carrying the projected `AccountError`.

Flag canonicalization in `flags.rs`:

- Gmail labels project to IMAP-style flags: `UNREAD` toggles
  `\Seen` (presence of `UNREAD` removes `\Seen`), `STARRED` is
  `\Flagged`, `DRAFT` is `\Draft`, `IMPORTANT` is
  `$Important`. Folder-like labels (`INBOX`, `SENT`, `TRASH`,
  `SPAM`, `CHAT`) drop out. User labels project to
  `$gmail-label:<id>:<name>`.
- The reverse direction in `translate_flag_op` handles `Add`,
  `Remove`, `Patch`, and `Set`. `Set` re-derives an add/remove
  patch over the canonical four flags plus the user label
  vocabulary; any unrecognized flag is added verbatim and any
  unknown `Set` flag falls into `unsupported_flags`. The
  driver emits `Skipped` for ids in a patch with non-empty
  `unsupported_flags`.
- A canonical flag set is hashed (FNV-1a) into the
  `Fingerprint.flags_hash` field of an `InventoryEntry`.

Gmail does not accept a client-mintable idempotency header on
these endpoints. The mutation impls take the shared
`IdempotencyKey` argument and hold it engine-side rather than
wiring it onto the request, keeping the no-wire-token posture
explicit at the call sites.

## Blobs

`open_blob` decodes the `BlobHandle.id` (a JSON
`{ message_id, attachment_id }` payload), fetches the
attachment, base64url-decodes the body, and emits a single
`Batch { items: vec![bytes], page_boundary: Final }` followed
by `Done`.

`open_blob_range` short-circuits with a Fatal carrying
`AccountError::RangeNotSupported` when the handle's
`supports_range` is false. The handle builder always sets
`supports_range: false` for Gmail blobs, so this path is the
authoritative "no" rather than a runtime probe. The
range-supporting branch slices the already-decoded buffer; it
exists for symmetry with the trait surface but is unreachable
under the capability shape.

`blob_handles_for_message` walks the MIME tree, surfacing any
part whose `body.attachment_id` is set. Inline bodies (no
attachment id) are not surfaced as blob handles.

## Error mapping

`recovery.rs` exposes two classifiers over `crate::Error`:

- `classify_general_error`:
  - `GmailError::Auth { .. }` -> `RecoveryClass::AuthLost`.
  - `GmailError::QuotaExhausted { .. }` (HTTP 429 or 403 with
    a quota-shaped body) -> `RecoveryClass::Retry { after: 1s }`.
  - `GmailError::Transport(err)` where `err.is_timeout()` or
    `err.is_connect()` -> `RecoveryClass::Retry { after: 1s }`.
  - `GmailError::HttpStatus { status, .. }` where
    `status.is_server_error()` -> `RecoveryClass::Retry { after: 1s }`.
  - Everything else -> `RecoveryClass::Fatal`. This covers 4xx
    bodies that are not quota-shaped, JSON decode failures,
    base64 decode failures, and malformed payloads.
- `classify_history_error` overrides 404 and 410 on the
  history endpoint to `RecoveryClass::RestartScope(CursorScope::Account)`.
  Gmail returns 404 or 410 when the supplied `startHistoryId`
  has been compacted past the server's retention window; the
  engine must re-establish from a fresh `getProfile`
  `historyId`. All other history-endpoint errors fall through
  to `classify_general_error`.

`account_error_from_gmail` projects `GmailError` onto
`AccountError` for use in `Fatal.source` and per-id
`MutationOutcome::Failed`:

- `Auth { service, body, .. }` -> `AccountError::Auth`.
- `Transport(err)` -> `AccountError::Transport(err.to_string())`.
- `QuotaExhausted { service, body, .. }` and
  `HttpStatus { service, status, body }` -> `AccountError::Transport(...)`
  so the engine treats both as transport-shaped failures.
- `Json`, `Base64`, `MalformedPayload`, `InvalidInput` ->
  `AccountError::Other`.

`fatal_for_error(error, recovery)` and
`fatal_for_account_error(error, recovery)` are the two `Fatal`
constructors used across the account layer. The first preserves
the Gmail-side error message; the second is used when the
failure originates from envelope or schema checks rather than
the wire.

## Known limitations

- Only `CursorScope::Account`. There is no thread scope, label
  scope, or query scope. Inventory, push, and cursor
  establishment all return `Unsupported` / Fatal for non-Account
  scopes.
- No blob range support. `BlobRangeSupport::No` is advertised;
  `open_blob_range` enforces it.
- Push requires a consumer-supplied Pub/Sub topic.
  `with_pubsub_config` is the only entry point; without it
  `push_subscribe` is `Unsupported`.
- `historyId` expiry is handled reactively. The capability set
  does not advertise a TTL; `classify_history_error` upgrades
  404/410 on the history endpoint to a scope restart.
- Mutation concurrency is `None` and replay safety is `None`.
  Gmail has no `If-Match` analogue on `batchModify` and no
  documented dedup header. The engine's read-back-after-retry
  path is the lost-update safety net.
- The Pub/Sub message body is parsed out of process, not
  inside this crate. The Account layer owns the watch lifecycle
  and the `WatchEvent::Reconnected` / `Disconnected` signal;
  it does not decode incoming Pub/Sub envelopes.
- Gmail has no independent attachment upload primitive for messages;
  callers send inline attachment bytes in `SendRequest` /
  `DraftPatch`.
- Gmail labels are flat; `container_move` is unsupported.
- Gmail profile data does not expose storage quota bytes;
  `quota_get` is unsupported.
- Replied and forwarded state are not writeable Gmail flags through
  this API. The corresponding convenience dispatch flags are false.
