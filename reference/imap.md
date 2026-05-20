# bifrost-imap reference

Current architecture of the IMAP client crate. Daaki-derived (see `plans/imap-daaki-migration.md` for migration history). Tokio + native-tls only.

## Driver-owned I/O

One tokio task owns the socket, parser state, and a watch channel exposing connection state. `ImapConnection` is a cheap handle around the driver. Public methods take `&self` and talk to the driver over channels.

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

`Credentials` is enum: `Password { username, password: SecretString }` or `OAuth2 { identity, access_token: SecretString }`. No `From<(String, String)>`.

`AuthMechanism`: PLAIN, LOGIN, XOAUTH2, OAUTHBEARER, CRAM-MD5, SCRAM-SHA-1, SCRAM-SHA-256.

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

`ImapConfig` centralizes TLS mode (Implicit / StartTls / Plaintext), connect timeout, command timeout, keepalive, and optional native-tls connector. Ports default per mode (993 / 143 / 143).

`connect_authenticated(credentials, policy)` composes connect + STARTTLS-if-needed + `authenticate_best`. Returns `(ImapConnection, AuthOutcome)`.

## Sync helpers

`SyncSelectOptions` / `SyncSelectResult` wrap SELECT/EXAMINE with CONDSTORE and QRESYNC parameters (UIDVALIDITY, last-known MODSEQ, known UIDs, VANISHED).

`SyncFetchRequest` / `SyncFetchResult` wrap FETCH with CHANGEDSINCE and VANISHED. `sync_fetch()` drives the call.

## Error model

`Error` is `#[non_exhaustive]`. Variants include `AuthPolicy(String)`, `FetchLimit { estimated, limit }`, plus protocol/transport/capability variants.

- `Error::category()` returns `ErrorCategory`: `Auth`, `AuthPolicy`, `Authorization`, `Capability`, `MailboxState`, `Limit`, `ServerRejected`, `Transport`, `Protocol`, `Tls`, etc.
- `Error::recovery()` returns `Recovery`: `RetryOrReconnect`, `Reconnect`, `Reauthenticate`, `ResyncMailbox`, `Transient`, `DoNotRetry`. Transient RFC 5530 codes (Unavailable, InUse, TempFail, Corruption, ExpungeIssued, NotificationOverflow, Referral) report retry-safe outcomes.
- `Error::response_code()` returns the structured `ResponseCode` from `[CODE ...]` brackets (first only).

## Module layout

```
crates/imap/src/
├── codec/           - parser + encoder (nom 8)
├── connection/      - driver, auth, lifecycle, dispatch
│   ├── auth.rs      - PLAIN/LOGIN/XOAUTH2/OAUTHBEARER/CRAM-MD5/SCRAM wire
│   ├── config.rs    - ImapConfig, connect_authenticated
│   ├── dispatch.rs  - command dispatch, FETCH consumer
│   ├── driver/      - driver task
│   ├── ergonomics.rs - uid_fetch_each, _limited, _full_messages
│   ├── helpers.rs   - server_profile, drain_events, ...
│   ├── lifecycle.rs - greeting, STARTTLS, ENABLE
│   ├── pipeline/    - command pipelining
│   ├── seq_ops.rs   - sequence-number command surface
│   └── uid_ops.rs   - UID command surface
├── types/           - public types (auth, ids, profile, secret, sync, events)
└── error.rs         - Error, ErrorCategory, Recovery, ResponseCode
```

## IMAP-specific code style

- Connection methods take `&self`.
- Every public operation takes an explicit `Duration` or documents the timeout policy.
- Public enums/structs are `#[non_exhaustive]` unless there is a strong reason not.
- Credentials and SASL intermediate strings use `Zeroizing` and redact under `Debug`.
- Malformed SASL mechanism names are rejected before any wire write.
