# bifrost-imap `Account` implementation

How the IMAP protocol crate realizes the `Account` trait. The
trait shape, dispatch model, cursor envelope, and capability
taxonomy are pinned in `plans/account-trait.md` and
`plans/account-trait-shape.md`. This document does not re-spec
them; it pins what code goes where, what state lives in the
`ImapAccount` pool struct, and how each trait method is realized
against a checkout.

Backpressure semantics for the data plane are pinned in
`plans/imap/backpressure.md`. Cursor lifecycle and per-folder
capability downgrade are pinned in
`plans/imap/condstore-qresync.md`. Both are cited, not restated.

## Crate-level sequencing

Three changes land in order. Each predecessor is a hard blocker
for the next.

1. **Backpressure rework** (`plans/imap/backpressure.md`).
   `BoundedStreamingFetchConsumer`,
   `BoundedStreamingFetchVanishedConsumer`,
   `DriverConsumer::StreamingRegular`, and the
   `(Receiver, Future)` shape of `uid_fetch_stream` /
   `fetch_stream`. Without bounded streaming, the `Account` impl
   cannot expose a `Stream` to the engine that honors the
   sync-engine bounded-buffering contract.
2. **CONDSTORE / QRESYNC tier**
   (`plans/imap/condstore-qresync.md`). The three-state
   `FolderCursor` (QResync / Condstore / Basic), the per-folder
   downgrade rule on `HIGHESTMODSEQ=0`, the iCloud detection
   path, and the VANISHED drain rule. Account-side code reads
   `ServerProfile` and per-folder SELECT response to pick the
   tier at SELECT time; without this taxonomy, change-stream
   strategy has nowhere to live.
3. **`ImapAccount` pool plus trait impl** (this document). Land
   the pool struct, the registry tables, the trait surface, and
   the per-method dispatch against a checkout. Depends on (1)
   and (2). Includes the `ImapAccountFactory` (`AccountFactory`
   impl) the engine sees.

A code change that crosses two of these is rebased onto the
later one. No interleaving.

## Pool shape: `Account` is the pool

`bifrost-imap::Account` (the trait impl, hidden behind a public
type `ImapAccount`) IS the connection pool. Each `ImapConnection`
is a checkout unit, never shared across tasks. This is forced by
two facts pinned in `account-trait-shape.md`:

- `ImapConnection` is `Send` but not `Sync`; a `tokio::sync::Mutex`
  on `cmd_rx` lets `&self` drain but does not let two tasks
  drive commands concurrently.
- The engine multiplexes IDLE, backfill, and reconcile against
  one account, which forces `N >= 2` live sessions. IDLE owns
  one checkout exclusively; reconcile and backfill take separate
  checkouts.

### `ImapAccount` struct

Lives in `crates/imap/src/account/mod.rs`. Internally:

```text
pub struct ImapAccount {
    // Pool primitives
    config:        Arc<ImapAccountConfig>,
    // Capabilities are immutable for the lifetime of this handle.
    // Mid-session capability transitions end affected streams with
    // RecoveryClass::CapabilityChanged; the engine reopens via the
    // factory and AccountSlot atomically swaps in a fresh ImapAccount
    // with fresh capabilities. No mutable cell here.
    server_caps:   Arc<AccountCapabilities>,
    pool:          Arc<Pool>,                 // bounded checkout pool
    idle_owner:    Mutex<Option<IdleHolder>>, // at most one IDLE
    push_tx:       broadcast::Sender<WatchEvent>,
    push_run:      OnceCell<JoinHandle<()>>,  // single IDLE driver

    // Per-folder state
    folders:       Arc<FolderRegistry>,       // RwLock<HashMap<MailboxName, FolderEntry>>

    // Push bookkeeping. Each SubscriptionHandle records the scope set
    // it asked for; the IDLE driver attends to the union of all
    // handles' scopes. Drop a handle, recompute the union, possibly
    // shut the driver down when empty.
    push_scopes:   Mutex<HashMap<SubscriptionHandle, HashSet<CursorScope>>>,

    // Lifecycle
    shutdown:      CancellationToken,
    closed:        AtomicBool,
}
```

`Pool` separates **slot capacity** (max concurrent connections
allowed) from **idle availability** (currently parked, ready-to-use
connections). Conflating the two — as the prior draft did with a
single semaphore over `Vec::pop()` — produces either a pool that
cannot grow past its initial size, or a pool whose checkouts
acquire-then-find-nothing.

```text
struct Pool {
    cap: usize,                                   // max total
                                                  // connections
    permits: Arc<Semaphore>,                      // initialized with
                                                  // `cap` permits
    idle: Arc<Mutex<Vec<PooledConnection>>>,      // parked, warm
                                                  // connections
    dialer: Arc<ConnectionDialer>,                // dials + does
                                                  // handshake/AUTH/
                                                  // ENABLE/CAPABILITY
}

async fn checkout(&self) -> Result<PooledConn<'_>, Error> {
    // Permit guarantees we won't exceed `cap` live connections.
    let permit = self.permits.clone().acquire_owned().await?;
    let conn = {
        let mut idle = self.idle.lock().await;
        idle.pop()
    };
    let conn = match conn {
        Some(c) => c,                              // reuse parked
        None => self.dialer.dial().await?,         // grow lazily
    };
    Ok(PooledConn { conn, permit, pool: self })
}
```

Default `cap = 4` (one IDLE slot + three data slots); configurable
on `ImapAccountConfig`. `factory.open()` parks one primed
connection into `idle`; further connections are dialed on demand
up to `cap`. Permits are held for the lifetime of `PooledConn`; on
drop, the guard returns the connection to `idle` (or discards on
`Broken`/`Closed`) and releases the permit. Connections enter the
pool in `SessionState::Authenticated` (post-handshake,
post-`ENABLE`, post-`CAPABILITY` cache). On checkin, the pool
inspects `state_rx`: `Broken` connections are discarded, `Closed`
are dropped, `Ok` is parked.

The IDLE slot uses its own dedicated dial path (`checkout_idle`,
below) and does not draw from `permits`; the data pool reserves
all `cap` permits for data work.

The IDLE holder is exclusive: at most one connection at any time
is bound to IDLE. Acquiring the IDLE checkout uses a separate
slot (`idle_owner: Mutex<Option<IdleHolder>>`) so the data pool
remains usable while IDLE is parked.

### `ImapAccountFactory`

Lives in `crates/imap/src/account/factory.rs`. Implements
`AccountFactory`:

```text
pub struct ImapAccountFactory { cfg: Arc<ImapAccountConfig> }

impl AccountFactory for ImapAccountFactory {
    fn open(&self) -> AccountFuture<Result<Arc<dyn Account>, Error>>;
}
```

Construction is consumer-side. The engine never sees IMAP config;
it sees only the factory trait and the opened
`Arc<dyn Account>`. `open()` does one priming connect to read
capabilities, runs `LIST "" "*" RETURN (SPECIAL-USE)`, primes the
`FolderRegistry`, parks the connection in the pool, builds
`AccountCapabilities`, and returns the handle. The pool is then
warmed lazily as work arrives.

`ImapAccountConfig` carries:

- `ImapConfig` (transport, host, port, TLS).
- `Credentials` + `AuthPolicy`.
- Pool cap (default 4).
- IDLE timeout (default 29 minutes; servers terminate after 30).
- QRESYNC opt-out gate (per-account operator override, surfaces
  `Warning::OperatorAttentionNeeded` on toggle - see
  `plans/imap/condstore-qresync.md`).
- Throttle defaults: `FLAG_SYNC_INTERVAL_SECS = 300`,
  `DELETION_CHECK_INTERVAL_SECS = 600` (overridable).

### `FolderRegistry`

```text
struct FolderEntry {
    name:            MailboxName,
    attributes:      Vec<MailboxAttribute>,    // \Inbox, \Sent, \Junk, \Drafts, \All, \Archive
    cursor:          ArcSwap<FolderCursor>,    // tier and watermarks
    last_seen:       AtomicU64,                // unix seconds of last poll
    membership:      MembershipScope,          // derived once, see below
}
```

`FolderCursor` is the three-state lifecycle from
`plans/imap/condstore-qresync.md`:

```text
enum FolderCursor {
    QResync   { uidvalidity: u32, modseq: u64 },
    Condstore { uidvalidity: u32, modseq: u64, known_uids: CompactUidSet },
    Basic     { uidvalidity: u32, uidnext: u32, known_uids: CompactUidSet },
}
```

Downgrade is in-place via `cursor.store(...)`. UIDVALIDITY change
clears `known_uids` and surfaces
`Fatal(ScopeInvalidated { scope })` for that folder; the engine
re-establishes via inventory.

Basic-tier expunge detection is a direct diff of `known_uids`
against a fresh `UID SEARCH ALL` result on the
`DELETION_CHECK_INTERVAL_SECS` cadence. `known_uids` is the
persistent state — it lives inside `OpaqueChangeState.bytes` for
`FolderCursor::Basic` and is restored on resume. A precomputed
hash over the UID set saves nothing in practice (the SEARCH
response is what we're comparing against, so we need the live
UID set in hand regardless), so the prior `UidListDigest`
intermediate is dropped.

### Pool checkout API

```text
impl ImapAccount {
    async fn checkout(&self) -> PooledConn<'_>;
    async fn checkout_for_folder(&self, folder: &MailboxName) -> PooledConn<'_>;
    async fn checkout_idle(&self) -> Result<IdleConn<'_>, Error>;
}
```

`checkout_for_folder` is the affinity-aware path: it scans
parked connections for one already SELECTed on `folder` (so the
CONDSTORE baseline is local), falls back to any free connection
and issues `SELECT (CONDSTORE)` / `SELECT (QRESYNC ...)` per
the folder's cursor tier. SELECT cost amortizes across calls
since the parked connection retains state until reused for a
different folder.

`checkout_idle` errors with `Error::IdleBusy` if `idle_owner`
already holds. The engine layer above mediates - there is at most
one IDLE per account in practice.

`PooledConn<'a>` is a guard. On drop it returns the connection
to the pool, or discards on `Broken` / `Closed`.

## `MembershipScope::Folder` derivation from LIST

At `open()` the factory runs
`LIST "" "*" RETURN (SPECIAL-USE)`. Each `MailboxInfo` becomes a
`FolderEntry` with `membership = MembershipScope::Folder(name)`.
Pure IMAP has one folder per message at the protocol level, so
`InventoryEntry::memberships` is a singleton `Vec<MembershipScope>`
of length 1 per entry. Engine derivation of `Destroyed` then
collapses to one step (the engine's per-object membership tracker
sees `Removed` from the single membership and emits `Destroyed`,
fallibility as documented in `plans/sync-engine.md`).

`discover_cursor_scopes()` yields one `CursorScope::Folder(_)`
per `FolderEntry` (modulo \Noselect entries, which yield no
scope).

`discover_memberships()` yields the same set as
`MembershipScope::Folder(_)` for the consumer-facing folder
tree.

`scope_lifecycle_stream()` watches the typed event queue for
`* LIST` updates (NOTIFY MailboxName events when subscribed,
LIST diff on the periodic refresh otherwise) and emits
`ScopeLifecycle::Created` / `Renamed` / `Deleted` events. Source
of truth is a periodic
`LIST "" "*"` refresh (default 5 minutes); NOTIFY events are an
optimization, not the truth.

## `changes_stream` realization

```text
fn changes_stream(&self, cursor: ChangeCursor)
    -> AccountStream<SyncEvent<Batch<Change>>>;
```

Cursor decode: the cursor's `OpaqueChangeState.bytes` decodes to a
`FolderCursor` plus the `MailboxName` (from
`ChangeCursor.scope`). `protocol` is validated as
`ProtocolKind::Imap`; `envelope_version` is checked against the
crate's envelope version constant. Mismatch surfaces
`Fatal(SchemaIncompatible)`.

Dispatch by tier:

- `QResync { uidvalidity, modseq }`: checkout, `SELECT (QRESYNC
  (<uidvalidity> <modseq>))`. The SELECT response itself carries
  the full delta from `<modseq>` to the server's current
  HIGHESTMODSEQ: VANISHED for expunges plus untagged FETCH for
  flag/modseq changes. The existing `SelectedMailbox` state already
  captures both (`crates/imap/src/connection/mod.rs` already
  records QRESYNC vanished + changed_messages from SELECT). Drain
  those in-memory through `BoundedStreamingFetchVanishedConsumer`,
  emit one `Batch<Change>` per `PageBoundary`, advance the cursor
  to the new HIGHESTMODSEQ from the SELECT response, and end with
  `Done`. **No follow-up `UID FETCH ... CHANGEDSINCE <modseq>`** —
  that would re-fetch the same delta. Ongoing detection past this
  point is the IDLE driver's job, not this `changes_stream` call.
  `advanced_through` is `None` (one SELECT, one drain — mid-page
  resumption not applicable).
- `Condstore { uidvalidity, modseq, known_uids }`: checkout,
  `SELECT (CONDSTORE)`, `UID FETCH 1:* (FLAGS MODSEQ)
  (CHANGEDSINCE <modseq>)` for flag changes. Compute expunge
  diff via `UID SEARCH ALL` vs `known_uids` on the
  `DELETION_CHECK_INTERVAL_SECS` cadence (or on every run if the
  consumer's `Priority` is `Foreground`).
- `Basic { uidvalidity, uidnext, known_uids }`: checkout,
  `SELECT`, periodic `UID FETCH <known_uids> (FLAGS)` for flag
  reconciliation on the `FLAG_SYNC_INTERVAL_SECS` cadence;
  `UID SEARCH ALL` vs `known_uids` for expunge detection on the
  `DELETION_CHECK_INTERVAL_SECS` cadence. New mail is detected
  via the `uidnext` watermark plus EXISTS.

Each variant emits one `Batch<Change>` per cursor advance, with
`checkpoint: Some(_)` carrying the updated cursor as
`OpaqueChangeState.bytes`. Mid-page items batch without
checkpoint. Stream terminates with `Done(checkpoint)` when the
delta is fully drained.

The `Change` variants this method emits:

- `ObjectChange::Updated` for FLAGS / MODSEQ changes.
- `ScopeChange::Removed` per folder for VANISHED / SEARCH-diff
  expunges. The engine derives `Destroyed` from per-object
  membership tracking; the protocol does not emit `Destroyed`.
- `ScopeChange::Added` for new UIDs observed past `uidnext`
  (the consumer hydrates via `get_stream` afterward).

## `inventory_stream` realization

Per-folder establishment via
`Account::establish_initial_cursor(scope)`:
`CursorEstablishment::Ready(cursor)` for QRESYNC folders (probe
SELECT yields the modseq baseline);
`CursorEstablishment::EstablishViaInventory` for Basic and
CONDSTORE-only folders (inventory FETCH IS the cursor
establishment). The multiplexer fuses or parallelizes
accordingly per `plans/bifrost-sync.md` -> Inventory-fusion.

Single implementation under the hood:

```text
fn inventory_stream(&self, scope: CursorScope)
    -> AccountStream<SyncEvent<Batch<InventoryEntry>>>;
```

`scope` decodes to `MembershipScope::Folder(name)`. Checkout via
`checkout_for_folder`. SELECT with the highest-tier capability
the folder supports (QRESYNC, CONDSTORE, or plain SELECT).

For QRESYNC-tier SELECT: `UID FETCH 1:* (UID FLAGS MODSEQ
RFC822.SIZE BODY.PEEK[HEADER.FIELDS (MESSAGE-ID REFERENCES
IN-REPLY-TO)])`. The resulting cursor anchors at the SELECT
response's `HIGHESTMODSEQ`. The `Batch<InventoryEntry>` carries
`checkpoint: Some(_)` only on the final page; the `Done` event
carries the final cursor wrapped in `OpaqueChangeState`.

For CONDSTORE-only / Basic: same FETCH but `MODSEQ` may be absent;
the cursor is constructed from `UIDVALIDITY` + `UIDNEXT` plus the
collected UID set for the `known_uids` field. Establishment cost
equals the inventory pass exactly; capability flag is honored.

`InventoryEntry::memberships` is always the singleton
`[MembershipScope::Folder(name)]`. `fingerprint` is computed
from `(UID, MODSEQ?, FLAGS_HASH)` where `FLAGS_HASH` is the
canonicalization rule pinned in `plans/account-trait.md` (system
flags lowercased, all flags sorted, hashed via xxhash).
`message_id` / `references` / `in_reply_to` are parsed from the
header fields. `blob_id` is `None`: IMAP has no blob id;
hydration is by UID + BODY section.

Backpressure: data plane runs through
`BoundedStreamingFetchConsumer`; the per-FETCH-response data flows
through the bounded channel, the driver applies TCP-level
backpressure when the consumer is slow (the mechanism pinned in
`plans/imap/backpressure.md`). VANISHED that arrives unsolicited
during the SELECT-drain goes to the connection's event_sink, not
the data channel.

## `get_stream` realization

```text
fn get_stream(
    &self,
    ids: AccountStream<ObjectId>,
    projection: Projection,
) -> AccountStream<SyncEvent<Batch<HydratedObject>>>;
```

`ObjectId` for IMAP encodes `(MailboxName, Uid, Uidvalidity)`.
The implementation groups incoming ids by `MailboxName`, then
per-folder runs `UID FETCH <set> <fetch_atoms>` where
`<fetch_atoms>` is derived from `Projection`:

- `Projection::Metadata`: `(UID FLAGS RFC822.SIZE INTERNALDATE
  ENVELOPE)`.
- `Projection::Preview(n)`: metadata plus
  `BODY.PEEK[TEXT]<0.n>` for the text/plain part if
  `BODYSTRUCTURE` indicates one.
- `Projection::TextOnly`: metadata plus
  `BODY.PEEK[<text-section>]` after a `BODYSTRUCTURE` round
  (single fetch via two-pass on first call per UID, cached on the
  hydrated object).
- `Projection::Full`: metadata plus `BODY.PEEK[]` (whole
  message).
- `Projection::FullWithBlobs`: same as `Full` since IMAP delivers
  attachments inline with the message.

UIDVALIDITY mismatch on the incoming `ObjectId` surfaces a
per-item `MutationOutcome::Skipped` wrapped in a `Warning`
(folder reselect would be wrong - the engine owns the
`ScopeInvalidated` recovery decision).

Per-folder dispatch runs on its own checkout. Folders are
processed concurrently up to the pool cap minus one (IDLE
reservation). Order across folders is not guaranteed; order
within a folder follows the input stream.

## `open_blob_range` realization

```text
fn open_blob_range(&self, handle: BlobHandle, range: ByteRange)
    -> AccountStream<SyncEvent<Bytes>>;
```

`BlobHandle.id` for IMAP encodes `(MailboxName, Uid, Uidvalidity,
SectionSpec)` where `SectionSpec` is the BODYSTRUCTURE-derived
section path (e.g. `"2.1"` for the first part of the second
multipart). Range fetch wires to `UID FETCH <uid>
BODY.PEEK[<section>]<offset.length>` per RFC 3501 / 9051.

`length` of `None` (open-ended to end of blob) translates to
`<offset.<remaining>>` where `<remaining>` is derived from the
`BlobHandle.size` value, fetched once via BODYSTRUCTURE at handle
construction. If `size` is unknown (handle constructed without
BODYSTRUCTURE pre-fetch), the implementation issues
`UID FETCH <uid> BODYSTRUCTURE` first.

`BlobCapabilities` for IMAP:

- `supports_range = true` (BODY[section]<offset.length>).
- `supports_parallel = false` (one driver per connection;
  parallel range fetches require multiple checkouts and the
  engine arbitrates).
- `digest_available_pre_download = false` (no IMAP primitive
  exposes a content digest; the consumer post-hashes).
- `encoding = Raw7Bit` or `Base64` per BODYSTRUCTURE's
  declared transfer encoding; the IMAP server may BINARY-decode
  if `BINARY` was negotiated (RFC 3516).

The stream emits `SyncEvent<Bytes>` chunks as they arrive over
the wire. Cancellation by drop leaves the connection `Broken`
mid-literal (pool reclaims by discard, per
`plans/imap/backpressure.md` cancellation rules).

## `bulk_set_flags` realization

```text
fn bulk_set_flags(
    &self,
    targets: AccountStream<ObjectId>,
    flags: FlagSet,
    op: FlagOp,
    idempotency: IdempotencyKey,
) -> AccountStream<SyncEvent<Batch<MutationResult>>>;
```

Per-folder grouping mirrors `get_stream`. Per folder, the
implementation runs `UID STORE <set> <op> (<flags>)` where `<op>`
maps to `+FLAGS.SILENT` / `-FLAGS.SILENT` / `FLAGS.SILENT` from
`FlagOp`. Group size: default 1024 UIDs per `UID STORE`,
configurable via `ImapAccountConfig`.

CONDSTORE-aware concurrency: when the folder's cursor is
`Condstore { modseq, .. }` or `QResync { modseq, .. }` AND the
server's per-message MODSEQ parsing is supported (gated on
`imap-proto` capability - see `plans/imap/condstore-qresync.md`),
the dispatch uses `UID STORE <set> (UNCHANGEDSINCE <cutoff>)
<op> (<flags>)`.

**Cutoff source.** `<cutoff>` is the folder cursor's
last-synchronized HIGHESTMODSEQ — the `modseq` field on the
`FolderCursor` snapshot captured at the **start of this
mutation campaign**. It is NOT an opportunistically newer value
sampled at STORE time (sampling later would weaken the test for
concurrent writers who landed between sample and submit), nor a
value drawn from any state unrelated to the target UID
selection. Per RFC 7162 §3.1.2.1 / §3.1.3, HIGHESTMODSEQ at last
sync is the correct cutoff for "apply only if this message has
not changed since the state I synced." The MODSEQ value lives on
`FolderCursor` and is captured once when the campaign opens; the
engine's `IdempotencyKey::run_id` ties retries to the same
captured value across process restarts.

**MODIFIED response code handling.** Per RFC 7162 §3.1.3, the
`MODIFIED <uids>` response code MAY appear on tagged **OK or
NO**, not just OK. The implementation parses MODIFIED off any
tagged status and treats listed UIDs as
`MutationOutcome::Failed(Error::ConcurrencyConflict)`:

- `OK` + no `MODIFIED` -> every dispatched UID is `Applied`.
- `OK` + `MODIFIED <set>` -> UIDs in `<set>` are
  `ConcurrencyConflict`, the rest `Applied`.
- `NO` + `MODIFIED <set>` -> UIDs in `<set>` are
  `ConcurrencyConflict`. The UIDs not in `<set>` are
  **ambiguous** — the server returned NO so we cannot assert
  they were applied. Mark them `pending_retry` and let the
  engine's read-back guard (per
  `plans/bifrost-sync.md` -> Read-back guard) resolve their
  state via `get_stream(Projection::FlagsOnly)`.
- `NO` + no `MODIFIED` -> whole-batch failure, all UIDs
  `Failed`.
- `BAD` -> parse-level failure, all UIDs `Failed`.

When per-message MODSEQ parsing is not available, the call
falls back to the non-UNCHANGEDSINCE form and
`MutationCapabilities::concurrency` is `None`. The engine then
relies on read-back-after-retry for safety, per
`plans/account-trait.md`.

`IdempotencyKey` is engine bookkeeping; IMAP has no native
replay token, so `MutationReplaySafety` is `None`. The
implementation does not consume the key on the wire; it is
preserved on each `MutationResult` for engine readback
correlation.

Partial success: the tagged response of a `UID STORE` does not
itemize per-UID success; the implementation correlates by
emitting one `MutationResult` per input UID. Outcomes are
determined by the MODIFIED-handling table above (covering OK,
OK+MODIFIED, NO+MODIFIED, NO without MODIFIED, and BAD).

## `push_subscribe` / `push_unsubscribe` / `push_stream`

IMAP push is in-process via IDLE. `push_subscribe` and
`push_unsubscribe` are server-side hooks; for IMAP they are
effectively `Ok(())` because IDLE is opportunistic, not a
persistent subscription. The implementation does:

```text
fn push_subscribe(&self, scopes: Vec<CursorScope>)
    -> AccountFuture<Result<SubscriptionHandle, Error>>;
```

Records the scope set in `ImapAccount.push_scopes` (a
`Mutex<HashSet<CursorScope>>`) for the IDLE driver to attend to,
hands back a `SubscriptionHandle` carrying a `u64` token. If the
IDLE driver is not yet spawned, spawn it now.
`push_unsubscribe` removes the scope set; if empty, stop the
IDLE driver.

```text
fn push_stream(&self) -> AccountStream<WatchEvent>;
```

Returns a `BroadcastStream` wrapping `ImapAccount.push_tx`. The
IDLE driver is the producer.

### IDLE driver

A single `tokio::task` per account, spawned lazily on first
`push_subscribe`. Workflow:

1. `account.checkout_idle().await` reserves the IDLE
   connection (separate from the data pool).
2. SELECT the most-active folder (heuristic: most recent
   `last_seen` across the union of `push_scopes` values). On
   NOTIFY-capable servers, issue `NOTIFY SET ...` covering the
   rest of the scope union and skip the per-folder SELECT
   cycling.
3. Drive `connection.idle(timeout=29min, cancel)` in a loop.
   `connection.idle()` returns `IdleEvent` (per
   `crates/imap/src/connection/idle.rs`); the explicit mapping
   `IdleEvent -> WatchEvent` lives in `push.rs::map_idle_event`:

   ```text
   IdleEvent::Exists { mailbox, .. }
       | IdleEvent::Expunge { mailbox, .. }
       | IdleEvent::Vanished { mailbox, .. }
       | IdleEvent::Fetch { mailbox, .. }
       | IdleEvent::Recent { mailbox, .. }
       => WatchEvent::Invalidated {
              hint: InvalidationHint {
                  source: PushSource::ImapNotify,
                  payload: HintPayload::SpecificCursorScope(
                      CursorScope::Folder(mailbox)),
              },
          }

   IdleEvent::Metadata { .. }
       | IdleEvent::ServerStatus { .. }
       | IdleEvent::Notify(NotifyEvent::AnyOther(_))
       => WatchEvent::Invalidated {
              hint: InvalidationHint {
                  source: PushSource::ImapNotify,
                  payload: HintPayload::Unknown,
              },
          }

   IdleEvent::Heartbeat
       | IdleEvent::Timeout
       | IdleEvent::None
       => Ignored

   IdleEvent::Extension(_)
       => Ignored in v1; reserved for future hint mapping.
   ```

   Variants whose exact name lives in `imap-proto` may differ in
   spelling once wired; the dispatch shape (per-mailbox events
   become `SpecificCursorScope`, account-wide become `Unknown`,
   liveness pings are dropped) is what's load-bearing.
4. On any `Error::Network`/transport failure: emit
   `WatchEvent::Disconnected`, discard the checkout, sleep with
   backoff, re-checkout, re-issue NOTIFY/SELECT, emit
   `WatchEvent::Reconnected`. Standard reconnect loop.
5. Shutdown via `ImapAccount.shutdown` cancellation token. Sends
   DONE, returns the connection.

The IDLE driver does NOT feed `WatchEvent::Invalidated` with
authoritative VANISHED data even when QRESYNC-IDLE delivers UIDs
in-band. Per `plans/sync-engine.md` -> "Push is invalidation",
the IDLE event is a wake-up; the engine's reconciler runs
`changes_stream` against the cursor for the affected scope. The
in-band UID data is dropped at the bandwidth cost noted in the
sync-engine plan (v1 behavior).

The data channel for `push_tx` is bounded (default capacity 128).
Overflow drops the oldest event and increments a drop counter;
the engine's `Unknown` hint handler covers the loss (re-reconcile
all scopes).

## `close`

```text
fn close(&self) -> AccountFuture<Result<(), Error>>;
```

Per `account-trait-shape.md`, idempotent local teardown only.
The implementation:

1. `closed.swap(true)` - return `Ok(())` immediately if already
   closed.
2. Trip `shutdown` cancellation token.
3. Stop the IDLE driver (DONE then LOGOUT on the IDLE
   connection).
4. Drain the data pool: each parked connection gets a LOGOUT
   with a short timeout; broken/timed-out connections are
   dropped.
5. Drop `push_tx` to close `push_stream` subscribers.

Does NOT call `push_unsubscribe` on the engine's behalf; IMAP
has no server-side push subscription to revoke.

## `AccountCapabilities` field values

`capabilities()` returns `&AccountCapabilities` per the trait
shape (`plans/account-trait.md`). Internally the struct holds
`Arc<AccountCapabilities>` and returns `&*self.server_caps`; the
value is **immutable** for the lifetime of this `ImapAccount`.
Mid-session capability transitions end affected streams with
`RecoveryClass::CapabilityChanged { delta }`; the engine reopens
via the factory, the new `ImapAccount` carries fresh capabilities,
and the engine's `AccountSlot` swaps the outer `Arc<dyn Account>`
atomically. There is no live capability channel and no
`ArcSwap<AccountCapabilities>` on the handle.

Per-tier values:

### QRESYNC tier

```text
AccountCapabilities {
    cursor_freshness: CursorFreshness::Hybrid,
    // No inventory_is_change_cursor_establish field — see
    // plans/account-trait.md -> Cursor establishment.
    // Per-folder establishment via establish_initial_cursor(scope):
    // QRESYNC folders return Ready(cursor); CONDSTORE-only and
    // Basic-downgraded folders return EstablishViaInventory.
    blob_range: BlobRangeSupport::Yes,
    blob_digest_pre_download: false,
    push: PushCapability {
        push_in_process: true,
        push_authoritative: false,   // see Push is invalidation
    },
    mutation: MutationCapabilities {
        concurrency: MutationConcurrency::StateBased, // STORE UNCHANGEDSINCE
        replay_safety: MutationReplaySafety::None,
    },
    batching_policy: BatchingPolicy {
        max_items: 1024,
        max_wait: Duration::from_millis(50),
        flush_on_input_close: true,
    },
    rate_limit_class: RateLimitClass::PerConnection,
    quota_signal: QuotaSignal::ServerAdvertised, // QUOTA extension
    requires_uidvalidity_recheck: true,
    historyid_expires_after: None,
}
```

The account-level capability values above are tier-independent:
they represent the BEST tier the server advertises. Per-folder
downgrade (`HIGHESTMODSEQ=0` on an otherwise CONDSTORE / QRESYNC
server) does NOT alter `AccountCapabilities`; the per-folder
`FolderCursor::Basic` variant carries the downgrade locally,
`describe_cursor` for that folder returns `CostClass::Expensive`,
and `establish_initial_cursor(scope)` for that folder returns
`EstablishViaInventory` instead of `Ready(cursor)`. The engine
schedules each folder independently per the per-scope
`CursorEstablishment` reply. Mixed-tier accounts (QRESYNC plus a
Basic-downgraded folder) work correctly without the conservative
account-level fallback the prior draft proposed.

`mutation.concurrency` is `StateBased` only when per-message
MODSEQ parsing is actually wired (gated). Until then,
`MutationConcurrency::None`.

### CONDSTORE-only tier

Same as QRESYNC except:

- `mutation.concurrency` is gated on per-message MODSEQ parsing
  as above.
- `establish_initial_cursor(scope)` returns `EstablishViaInventory`
  (CONDSTORE-only servers have no QRESYNC SELECT probe that
  yields the cursor cheaply; the inventory FETCH IS the
  cursor-establishment pass).

### Basic tier

Same as CONDSTORE-only except:

- `mutation.concurrency: MutationConcurrency::None`.
- `blob_range` unchanged (BODY[section]<offset.length> works
  without MODSEQ).
- `establish_initial_cursor(scope)` returns
  `EstablishViaInventory`.

## `describe_cursor`

```text
fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor;
```

Decodes `OpaqueChangeState.bytes`, returns:

- `FolderCursor::QResync { .. }` -> `CursorDescriptor {
  cost_class: Cheap, strategy: SyncStrategy::ModseqDiff,
  freshness: Some(last_seen) }`.
- `FolderCursor::Condstore { .. }` -> `Medium`,
  `SyncStrategy::ModseqPlusUidListDiff`.
- `FolderCursor::Basic { .. }` -> `Expensive`,
  `SyncStrategy::UidListDiff`.

`freshness` is the registry's `last_seen` for the folder.

## File layout

```text
crates/imap/src/account/
  mod.rs            // ImapAccount, public re-exports
  factory.rs        // ImapAccountFactory (AccountFactory impl)
  pool.rs           // Pool, PooledConn, IdleConn, checkout primitives
  folder_registry.rs// FolderEntry, FolderRegistry, CompactUidSet
  envelope.rs       // OpaqueChangeState <-> FolderCursor codec
  capabilities.rs   // AccountCapabilities builder per tier
  changes.rs        // changes_stream tier dispatch
  inventory.rs      // inventory_stream FETCH wiring
  get.rs            // get_stream per-folder grouping
  blob.rs           // open_blob_range BODY[section]<offset.length>
  mutate.rs         // bulk_set_flags UNCHANGEDSINCE wiring
  push.rs           // IDLE driver, push_tx fan-out
  scopes.rs         // discover_cursor_scopes, discover_memberships,
                    // scope_lifecycle_stream
  close.rs          // shutdown sequence
```

Trait impl lives in `mod.rs`; method bodies are thin and delegate
to the sibling modules above. No public surface escapes outside
`account::*` except `ImapAccount`, `ImapAccountFactory`, and
`ImapAccountConfig`.

## Tests

Per AGENTS.md testing scope: parser, encoder, type-level,
validation, error classification, envelope round-trip. Tests
this crate adds:

- `envelope.rs` round-trip: `FolderCursor` <->
  `OpaqueChangeState` for each tier, with envelope_version drift
  rejection.
- `folder_registry.rs`: LIST parse to `FolderEntry`, SPECIAL-USE
  attribute mapping, `CompactUidSet` round-trip + diff
  correctness (added/removed against a synthetic SEARCH ALL).
- `capabilities.rs`: builder reads `ServerProfile` and emits the
  correct per-tier `AccountCapabilities`.
- `changes.rs`: tier dispatch (synthesized `FolderCursor` ->
  expected FETCH atom set, without running a real connection).
- `mutate.rs`: `FlagOp` -> wire atom mapping, MODIFIED response
  parsing into per-UID `MutationOutcome`.

No live-server tests, no mock servers, no Docker harnesses
(`AGENTS.md` testing rules).

## Risks / Opens

- **Per-message MODSEQ parsing in `imap-proto`.** Gates
  CONDSTORE-aware `UID STORE UNCHANGEDSINCE`. Until landed,
  `MutationConcurrency` is permanently `None` and the engine's
  lost-update protection relies on readback-after-retry. Tracked
  in `plans/imap/condstore-qresync.md`. If upstream stalls, the
  fallback is hand-rolled MODSEQ parsing in
  `crates/imap/src/codec/` - one quarter of the cost of vendoring
  the crate, but it bifurcates parser maintenance.
- **iCloud QRESYNC detection lag.** The detection path is
  "advertised but did not ENABLE" plus "FETCH carries malformed
  shape." Both signals are observable only mid-session. A naive
  cold-start path uses QRESYNC against iCloud and corrupts the
  first inventory pass. Mitigation: detection-first probe at
  `open()` - one `SELECT` of `INBOX` with QRESYNC params, then
  inspect the response. If detection trips, downgrade the whole
  account session to CONDSTORE-only before the factory returns.
  Open: does the probe cost (one extra SELECT round-trip) justify
  the safety, or do we let mid-session detection handle it and
  eat the first corrupted inventory? Lean toward the probe.
- **Pool cap interaction with backfill priority.** Default pool
  cap of 4 means one IDLE plus three data connections. A
  backfill campaign on a 5M-message account at `Priority::Bulk`
  can saturate all three data connections, starving foreground
  fetches. Either the pool needs a foreground reservation (one
  permit always available to `Priority::Foreground`), or the
  engine's scheduler must enforce the priority asymmetry. The
  scheduler is the right layer (per `plans/bifrost-sync.md` ->
  Starvation guards), but the pool must surface priority to the
  scheduler. Open: shape of the `Priority` hint through
  `checkout_for_folder`.
- **IDLE drop on long pauses.** Server-side IDLE timeouts vary
  (5-30 minutes). The IDLE driver cycles every 29 minutes by
  default, but the data pool also has a stale-connection
  timer (default 25 minutes) to prevent half-broken parked
  connections. The two timers are independent today; a unified
  keepalive policy might simplify configuration.
- **NOTIFY support detection and fallback.** NOTIFY (RFC 5465)
  multiplexes IDLE across folders, eliminating
  SELECT-cycling. Detection is via `ServerProfile`. Where NOTIFY
  is absent, the IDLE driver SELECTs the most-active folder and
  cycles on inactivity. Cycling cadence and "most active"
  heuristic are not pinned; v1 picks `last_seen` and a 5-minute
  cycle, but production tuning is needed.
- **OBJECTID interaction with UIDVALIDITY recheck.** OBJECTID
  (RFC 8474) gives stable per-message EMAILID across
  UIDVALIDITY changes. If advertised, the engine could avoid
  full re-inventory on UIDVALIDITY change. Not in v1; tracked as
  a future
  `requires_uidvalidity_recheck = false` path conditional on
  OBJECTID.
- **`scope_lifecycle_stream` cadence.** Periodic LIST refresh
  (5 minutes default) is the floor. NOTIFY MailboxName events
  ride on top when available. Open: should the cadence be
  configurable per account, or fixed? Fixed is simpler, but
  large enterprise accounts with hundreds of shared mailboxes
  may want a slower refresh.
- **Pool cap discovery vs. user config.** Some servers advertise
  per-user connection limits via the IMAP ID extension or
  out-of-band documentation (Gmail IMAP caps at 15 per account,
  Outlook.com at 20). The factory could clamp pool cap against
  observed server limits. v1 trusts user config; v2 reads
  server hints where available.
