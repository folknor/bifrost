# bifrost-imap reference

Current architecture of the IMAP client crate. Daaki-derived. Tokio + native-tls only.

The public consumer surface is intentionally small after S1-W3:
`ImapAccountFactory`, `ImapAccountConfig`, `ImapConfig`,
`Credentials`, and `AuthPolicy`. Consumers construct the factory and
use it through `Arc<dyn bifrost_types::AccountFactory>`; raw IMAP
connections, parser types, command types, protocol errors, and sync
helpers are crate-internal implementation detail.

## Driver-owned I/O

One tokio task owns the socket, parser state, and a watch channel exposing connection state. `ImapConnection` is a cheap internal handle around the driver. Connection methods take `&self` and talk to the driver over channels.

The driver model is load-bearing for:

- Cancellation safety.
- Atomic stream upgrades (STARTTLS, COMPRESS).
- Typed event queue draining without contending with command I/O.
- Streaming FETCH without giving callers raw stream access.

There is no boxed-transport escape hatch. Supported transport is `tokio::net::TcpStream` plus optional `tokio_native_tls::TlsStream`.

## Connection state machine

State is `Ok` / `Broken` / `Closed`. The state machine is the cancel-safety mechanism:

- Every async write, flush, and read sets state to `Broken` before the await; only success restores `Ok`.
- `abort()` transitions to `Closed` without sending LOGOUT.
- A dropped future leaves the connection `Broken`; pool discards it.

LMTP-style multi-response paths and the LMTP final delivery-status loop hold `Broken` across the entire post-DATA recipient loop.

## Typed events

`TypedEvent` carries every unsolicited response (EXISTS, EXPUNGE, FETCH, FLAGS, STATUS, METADATA, NOTIFY-routed events). `drain_events()` and `next_event()` give callers ordered access.

`TypedEvent::impact()` returns `EventImpact` for cache-coherence decisions: `SelectedMailboxChanged`, `SelectedMailboxResync`, `MailboxStateChanged`, `ServerMetadataChanged`, `Heartbeat`, `Extension`, `None`. RECENT maps to `None` (informational; deprecated by IMAP4rev2). ServerMetadataChange is server-scoped, not selected-mailbox scoped.

## Streaming FETCH

Streaming uses an **unbounded** mpsc channel. The previous bounded `try_send` silently dropped responses under load; that is fixed. Slow consumers create memory pressure rather than data loss.

Three entry points:

- `uid_fetch_streaming()` / `fetch_streaming()` - raw stream; caller drains the receiver.
- `uid_fetch_each(...)` - callback-based wrapper.
- `uid_fetch_limited(budget, ...)` - hard client-side byte budget enforced inside the driver consumer; returns `Error::FetchLimit` when crossed. Driver keeps reading until tagged OK, so parser/TCP buffers may continue past the limit.

Buffered `uid_fetch()` uses the driver buffer path directly, not the streaming channel. `uid_fetch_full_messages(budget, ...)` requires an explicit budget and routes through the limited path.

## Capability and ENABLE state

`ServerProfile` answers capability questions without ad-hoc CAPABILITY parsing in every caller. Built from `state_rx.borrow()` at call time. **Snapshot semantics**: it does not auto-refresh. Refetch after STARTTLS, AUTH, or ENABLE.

`rev2_implies` bakes in the RFC 9051 baseline: ESEARCH, IDLE, MOVE, NAMESPACE, SASL-IR, SEARCHRES, UIDPLUS, UNSELECT, LIST-EXTENDED, STATUS=SIZE, STATUS=DELETED, OBJECTID, SAVEDATE, BINARY, LITERAL+, LITERAL-, SPECIAL-USE, ENABLE. STATUS=DELETED encoding flows through the same gate.

## Auth

`Credentials` is a public opaque wrapper with `password(username,
password)` and `oauth2(identity, access_token)` constructors. The
stored secret material remains in the internal `SecretString` wrapper,
which zeroizes and redacts under `Debug`; consumers no longer handle
or import that wrapper directly. No `From<(String, String)>`.

Internal `AuthMechanism`: PLAIN, LOGIN, XOAUTH2, OAUTHBEARER,
CRAM-MD5, SCRAM-SHA-1, SCRAM-SHA-256.

`AuthPolicy` TLS-gates cleartext mechanisms by default. PLAIN and LOGIN refuse over plaintext unless `allow_cleartext_without_tls` is set. CRAM-MD5 is opt-in (`with_cram_md5`) AND TLS-gated, because a MITM can pick the challenge and brute-force `HMAC-MD5(password, challenge)` offline. LOGIN-the-IMAP-command is opt-in (`with_login`).

`authenticate_best(credentials, policy)` intersects server-advertised, policy-allowed, and credentials-supported mechanisms, then runs the strongest match. SASL-IR is used when advertised or implied by IMAP4rev2. Malformed mechanism names are rejected before any wire write. `AuthOutcome` carries the selected mechanism.

## Typed IDs and sets

Newtypes with explicit `::new` constructors. No `From<u32>`/`From<u64>` to prevent accidental mixing across UID, sequence, message-id, and thread-id contexts:

- `Uid`, `Seq` - `NonZeroU32`-backed.
- `UidValidity` - `NonZeroU32`.
- `ModSeq` - `u64` (0 means "no CONDSTORE state").
- `GmailMessageId`, `GmailThreadId` - `u64`.

`UidSet` and `SeqSet` wrap the validated sequence-set encoder. Construct via `from_uids`, `from_seqs`, `all()`, `saved_search()` (`$`), or range constructors. `parse(&str)` exists as an escape hatch and bypasses the typed discipline.

## Configuration and connect

`ImapConfig` centralizes TLS mode (Implicit / StartTls / Plaintext),
connect timeout, command timeout, keepalive, and optional native-tls
connector. Ports default per mode (993 / 143 / 143). Public
constructors are `tls`, `starttls`, and `plaintext`; public builders
cover port, timeouts, keepalive disablement, and custom TLS
connectors. The raw `connect` / `connect_authenticated` entry points
were deleted in S1-W3.

Internally, `connect_authenticated_metered(credentials, policy, meter,
cap)` composes connect + STARTTLS-if-needed + `authenticate_best` for
the account factory and pool. It returns `(ImapConnection,
AuthOutcome)` inside the crate only.

## Sync helpers

`SyncSelectOptions` / `SyncSelectResult` wrap SELECT/EXAMINE with CONDSTORE and QRESYNC parameters (UIDVALIDITY, last-known MODSEQ, known UIDs, VANISHED parsed off the SELECT response).

`SyncFetchRequest` / `SyncFetchResult` wrap UID FETCH with `CHANGEDSINCE` and `VANISHED`. `sync_fetch()` drives the call and surfaces both VANISHED ranges and per-UID FETCH responses.

`SelectedMailbox` carries `highest_mod_seq`, `uid_validity`, `uid_next`, `no_mod_seq`, and the SELECT-side VANISHED list. A missing `UIDVALIDITY` is a protocol error rather than a silent 0; a `MODSEQ 0` in a FETCH response is rejected for the same reason.

## Account layer

The shared `bifrost_types::Account` implementation lives under `crates/imap/src/account/`. `ImapAccountFactory::open(account_id)` opens a connection pool, lists folders, builds capabilities, and threads the engine account id into optional raw-socket bandwidth metering. `ImapAccount` backs both the sync methods and the Stage 1 PIM primitives.

The public account module re-exports only `ImapAccountFactory` and
`ImapAccountConfig`, preserving the existing conformance-test path
`bifrost_imap::account::{...}`. The crate root also re-exports the
factory and config types directly. `ImapAccount` itself is
`pub(crate)`.

Submodules:

- `factory.rs` - `ImapAccountConfig`, `AccountFactory::open(account_id)`, optional `BandwidthMeter` / `MeterSink` wiring, `ID` probe, QRESYNC negotiation, initial folder LIST.
- `pool.rs` - per-folder connection checkout. Push lane reserves one slot; data lanes share the rest. Every dialed connection receives the account-scoped `MeterSinkHandle` and shared bandwidth-cap atomic when configured.
- `folder_registry.rs` - mailbox map plus per-folder cursor cache and per-folder MODSEQ cache (`record_modseq`, `modseq`, `clear_modseqs`). Cache is keyed by `(folder, uidvalidity, uid)`, stores LIST delimiter / attributes for PIM containers, and clears on UIDVALIDITY change, delete, or rename.
- `envelope.rs` - `FolderCursor` (QResync / Condstore / Basic) plus `encode_cursor`/`decode_cursor` over `OpaqueChangeState`.
- `capabilities.rs`, `inventory.rs`, `changes.rs`, `get.rs`, `blob.rs`, `mutate.rs`, `push.rs`, `close.rs`, `scopes.rs` - one file per `Account` method group.
- `pim.rs` - Stage 1 mail action surface: container membership, keyword/read mutations, search, folder CRUD, quota, draft create/discard, message/thread hydration, and IMAP-specific thread move/delete conveniences.

### CONDSTORE / QRESYNC strategy

`FolderCursor` has three variants and `changes_stream()` dispatches diff strategy by variant:

- `QResync { uidvalidity, modseq, known_uids, known_uids_complete }`. Change diff uses `UID FETCH ... CHANGEDSINCE ... VANISHED` against the cached MODSEQ, plus SELECT-side VANISHED and changed-FETCH data.
- `Condstore { uidvalidity, modseq, known_uids }`. Flag diff via `CHANGEDSINCE`; expunge detection via UID-list diff against `known_uids`.
- `Basic { uidvalidity, uidnext, known_uids }`. Neither extension. UID-list diff for both flags and expunges.

Negotiation in `factory.rs`:

1. `ID` probe runs when advertised; failures are non-fatal. iCloud is preconfigured off via exact-match `name` checks against `"icloud"` / `"icloud imap"`; substring matches do not trip the downgrade.
2. If `enable_qresync` and the server advertises QRESYNC, the factory runs `ENABLE QRESYNC`. The `ENABLED` reply must echo `QRESYNC` (or `ServerProfile::enabled` must confirm it); otherwise the session continues in CONDSTORE-only mode with a one-shot `OperatorAttentionNeeded` warning emitted on first changes-stream attach.
3. The QRESYNC capability check is exact, not substring, to avoid false positives on capabilities that contain `QRESYNC` as a suffix.

Runtime downgrades:

- A QRESYNC SELECT response that fails to parse calls `disable_qresync_for_session()` (one-shot, no retry on the same session), discards the suspect pooled connection, and retries on the CONDSTORE path.
- A QRESYNC cursor that lacks a complete UID baseline (`known_uids_complete == false`) seeds the baseline via `UID SEARCH ALL` before the first `CHANGEDSINCE` round-trip and emits a benign downgrade warning.
- A CONDSTORE cursor with an incomplete baseline seeds the same way before the first UID-list diff.
- VANISHED and FETCH may report the same UID in non-conformant servers. The change stream de-duplicates so a single message does not surface as both expunge and update.

### Mutations

`bulk_set_flags` and `bulk_destroy` partition targets by cached MODSEQ. Targets with a cache hit go out under `STORE UNCHANGEDSINCE <modseq>`; cache misses fall back to unprotected STORE. Protected batches are ordered before unprotected batches so a successful protected pass updates cache entries before the unprotected pass runs.

- The MODSEQ cache is populated from inventory, get, changes, mutation SELECT data, and push IDLE FETCH events.
- Successful flag mutations, expunges, VANISHED notifications, moves, and folder delete/rename clear the affected entries.
- `bulk_destroy` partial-failure accounting separates conflicts (UNCHANGEDSINCE rejected) from expunge failures; expunge failures only apply to the UIDs being expunged in that round, not the whole batch.

Capabilities still advertise `MutationConcurrency::None`. The MODSEQ cache is opportunistic - cold cache means unprotected STORE - so promoting to `StateBased` would let the engine assume UNCHANGEDSINCE is always wired up when it is not. The engine's read-back-after-retry path remains the lost-update safety net.

### PIM primitives

`capabilities.rs` fills `AccountCapabilities::pim_methods` and `conveniences` at open time. IMAP advertises real support for:

- Container membership: `add_to_container` via UID COPY, `remove_from_container` via `+FLAGS.SILENT \Deleted` plus UID EXPUNGE.
- Keywords: `set_keyword` via UID STORE, with the convenience keywords `$flagged`, `$answered`, and `$seen` mapped to `\Flagged`, `\Answered`, and `\Seen`. `$forwarded` remains an IMAP keyword.
- Read state: `set_is_read` via `\Seen`.
- Search messages: UID SEARCH across selectable folders, with `SearchFilter::In` restricting the selected mailbox. Thread search is advertised only when `THREAD=REFERENCES` is available and returns synthetic IMAP thread ids containing folder, UIDVALIDITY, and member UIDs.
- Containers: LIST-backed folder enumeration plus CREATE, RENAME-as-rename, RENAME-as-move, and guarded DELETE. DELETE first checks `STATUS MESSAGES` and refuses non-empty mailboxes.
- Quota: `GETQUOTAROOT`, mapped from STORAGE units to bytes when QUOTA is advertised.
- Hydration: one-shot message FETCH and synthetic thread hydration.
- Draft create/discard: APPEND to the Drafts folder with `\Draft` when Drafts and UIDPLUS are present; discard deletes the draft object id.

Unsupported PIM methods return `Error::Unsupported` and have false capability flags: SMTP send, attachment upload, draft update/send, Gmail label membership, Graph categories and extended properties, identities, identity update, vacation get/set. IMAP identities and vacation responders are external configuration or Sieve-shaped and are not exposed in Stage 1.

`ConvenienceShape` declares IMAP starred/replied/forwarded as keyword-shaped. `move_thread` and `delete_thread` override the trait defaults: they use the crate's cloneable account handle to do add-then-remove, and delete moves to the Trash role unless the current container is already Trash, in which case it expunges the thread from that mailbox.

Containers use native mailbox paths as primitive ids and provenance-native ids. `containers_list` maps SPECIAL-USE attributes to `FolderRole` (`\Sent`, `\Drafts`, `\Archive`, `\Trash`, `\Junk`, and custom `\Inbox`) and falls back to name-based INBOX / Sent / Drafts / Archive / Trash / Spam detection for servers without SPECIAL-USE.

### Bandwidth metering

`ImapAccountConfig` can carry either a process `BandwidthMeter` or a generic `MeterSink`. The factory builds a `MeterSinkHandle` with the real engine `AccountId` on every open and passes it to the initial connection plus pool dials. `WireReader` records bytes read and written on every connection-level read/write path. The shared bandwidth-cap atomic is read per chunk; inbound and outbound I/O use byte buckets so `set_bandwidth_cap(None)` is unlimited and `Some(0)` is clamped to 1 B/s with a warning.

### Folder lifecycle

`FolderRegistry::apply_mailbox_event` handles delete, rename, and delete-then-recreate. Delete and rename drop the in-memory entry. A recreate (fresh UIDVALIDITY at the same name) installs a fresh `FolderEntry` with empty modseq cache and cursor, so a recreated mailbox cannot reuse the prior epoch's state.

## Error model

`Error` is `pub(crate)` and `#[non_exhaustive]`. Variants include
`AuthPolicy(String)`, `FetchLimit { estimated, limit }`, plus
protocol/transport/capability variants. The account boundary converts
protocol errors into `bifrost_types::AccountError` via
`error::into_account_error(error, ctx)`; consumers never see the
crate-internal IMAP error taxonomy.

`ImapErrorContext` carries the calling `AccountOperation` (now a
required field, not `Option`), optional `ErrorScope`, `Provider`,
explicit `transmission_state`, and an `idempotency_override`. Every
public `pim.rs` method threads its operation via a local `op_err`
closure that stamps `ImapErrorContext::operation(<op>)` on every
`map_err`; multi-call helpers (`copy_messages`, `delete_messages`,
`set_flag`, `hydrate_decoded`, `refresh_folders`, `folder_from_scope`)
take an explicit `op: AccountOperation` parameter so they emit errors
tagged with the caller's operation rather than a generic default.
This is what lets the central recovery mapping distinguish
`Reconcile` from `Retry::SameRequest` for non-idempotent ops like
`AddToContainer` and `DraftCreate`.

`Error::response_code()` returns the structured `ResponseCode` from
`[CODE ...]` brackets (first only); the central recovery mapping
in `bifrost-types::recovery` consumes those response codes via the
typed `ImapResponseCode` wire variants.

## Module layout

```
crates/imap/src/
|-- codec/           - parser + encoder (nom 8)
|-- connection/      - driver, auth, lifecycle, dispatch
|   |-- auth.rs      - PLAIN/LOGIN/XOAUTH2/OAUTHBEARER/CRAM-MD5/SCRAM wire
|   |-- config.rs    - ImapConfig, internal account dial path
|   |-- dispatch.rs  - command dispatch, FETCH consumer
|   |-- driver/      - driver task
|   |-- ergonomics.rs - uid_fetch_each, _limited, _full_messages
|   |-- helpers.rs   - server_profile, drain_events, ...
|   |-- lifecycle.rs - greeting, STARTTLS, ENABLE
|   |-- pipeline/    - command pipelining
|   |-- seq_ops.rs   - sequence-number command surface
|   `-- uid_ops.rs   - UID command surface
|-- account/         - bifrost_types::Account implementation
|   |-- blob.rs            - open_blob, open_blob_range
|   |-- capabilities.rs    - AccountCapabilities builder
|   |-- changes.rs         - QRESYNC / CONDSTORE / Basic diff dispatch
|   |-- close.rs           - graceful shutdown
|   |-- envelope.rs        - FolderCursor encode/decode
|   |-- factory.rs         - ImapAccountConfig, QRESYNC negotiation
|   |-- folder_registry.rs - mailbox map, cursor cache, MODSEQ cache
|   |-- get.rs             - hydration
|   |-- inventory.rs       - inventory + initial cursor establishment
|   |-- mod.rs             - ImapAccount trait impl, helpers
|   |-- mutate.rs          - bulk_set_flags, bulk_move, bulk_destroy
|   |-- pim.rs             - unified PIM primitives + conveniences
|   |-- pool.rs            - per-folder connection checkout
|   |-- push.rs            - IDLE-driven WatchEvents
|   `-- scopes.rs          - folder discovery, lifecycle stream
|-- types/           - internal protocol types plus public Credentials / AuthPolicy
`-- error.rs         - internal Error, ErrorCategory, Recovery
```

## IMAP-specific code style

- Connection methods take `&self`.
- Internal raw protocol operations take an explicit `Duration` or document the timeout policy.
- Public enums/structs are limited to the factory/config surface and are `#[non_exhaustive]` unless there is a strong reason not.
- Credentials and SASL intermediate strings use `Zeroizing` and redact under `Debug`.
- Malformed SASL mechanism names are rejected before any wire write.
