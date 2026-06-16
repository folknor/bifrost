# bifrost-imap reference

Current architecture of the IMAP client crate. Daaki-derived. Tokio + native-tls only.

The public consumer surface is intentionally small after S1-W3:
`ImapAccountFactory`, `ImapAccountConfig`, `ImapConfig`,
`ManageSieveConfig`, `Credentials`, and `AuthPolicy`. Consumers
construct the factory and use it through
`Arc<dyn bifrost_types::AccountFactory>`; raw IMAP, ManageSieve,
parser, command, protocol-error, and sync helper surfaces are
crate-internal implementation detail.
`ImapAccountConfig::with_carddav(CardDavConfig)` composes the
standalone `bifrost-carddav` account for contact primitives when an
IMAP mail account has paired DAV settings.

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
password)`, `oauth2(identity, access_token)` (raw string), and
`oauth2_source(identity, Arc<dyn TokenSource>)` (shared rotation source)
constructors. The OAuth variant holds an `Arc<dyn TokenSource>`
(bifrost-net's trait), read via `current().await` at connect and on
every reconnect through the same per-connect point in
`authenticate_best` (and the ManageSieve auth path) - so a token rotated
on the shared source is presented fresh on reconnect, closing the
stale-token-on-reconnect path. Password secret material remains in the
internal `SecretString` wrapper, which zeroizes and redacts under
`Debug`. `Credentials` is `Clone` only - the `PartialEq`/`Eq` it once
derived is dropped (a live token source is not `Eq`; nothing compares
credentials). Unlike SMTP's `Credentials`, IMAP's was never
serde-derived, so that drop is the whole derive fallout. No
`From<(String, String)>`.

Internal `AuthMechanism`: PLAIN, LOGIN, XOAUTH2, OAUTHBEARER,
CRAM-MD5, SCRAM-SHA-1, SCRAM-SHA-256, and the channel-bound
SCRAM-SHA-1-PLUS / SCRAM-SHA-256-PLUS. Each variant's `name()` is the
wire token; the PLUS variants match the `-PLUS` advertisement and the
SASL crate (`ScramHash::mechanism_name`) is the single authority for the
token actually emitted on the socket.

SCRAM and CRAM-MD5 computation (the `scram_client_final` /
`verify_server_final` transitions, the per-hash proofs, and the CRAM-MD5
response) lives in the private `bifrost-sasl` crate; the dispatch consumers
(`AuthenticateScramConsumer`, `AuthenticateCramMd5Consumer`) drive the `+`
continuation flow and invoke it, mapping `bifrost_sasl::SaslError` back into
the IMAP error model at the call boundary.

`AuthPolicy` TLS-gates cleartext mechanisms by default. PLAIN and LOGIN refuse over plaintext unless `allow_cleartext_without_tls` is set. CRAM-MD5 is opt-in (`with_cram_md5`) AND TLS-gated, because a MITM can pick the challenge and brute-force `HMAC-MD5(password, challenge)` offline. LOGIN-the-IMAP-command is opt-in (`with_login`).

`authenticate_best(credentials, policy)` intersects server-advertised, policy-allowed, and credentials-supported mechanisms, then runs the strongest match. SASL-IR is used when advertised or implied by IMAP4rev2. Malformed mechanism names are rejected before any wire write. `AuthOutcome` carries the selected mechanism.

The password ladder is a pure helper `password_mechanism_ladder(profile, policy, is_encrypted)` returning `PasswordCandidate::{Attempt, Reject}` in the fixed preference order SCRAM-SHA-256-PLUS > SCRAM-SHA-1-PLUS > SCRAM-SHA-256 > SCRAM-SHA-1 > PLAIN > CRAM-MD5 > LOGIN. RFC 5802 Section 6 downgrade protection lives in the helper: when the server advertises `SCRAM-SHA-N-PLUS`, the matching unbound `SCRAM-SHA-N` rung becomes `Reject(ChannelBindingUnavailable)` so a MITM cannot strip the binding. `authenticate_best` walks the ladder; for a PLUS `Attempt` it resolves the `tls-server-end-point` binding once via `resolve_scram_binding()` (peer-cert DER fetch + `bifrost_sasl::tls_server_end_point`) and threads it into `authenticate_scram_with_binding`. A binding that cannot resolve (plaintext, EdDSA leaf cert, unsupported sig-alg) becomes a `ChannelBindingUnavailable` rejection; combined with the downgrade skip, an EdDSA-cert server advertising a `-PLUS` variant disables SCRAM entirely and falls through to PLAIN-over-TLS (RFC-compliant, credential stays encrypted). A direct `authenticate_scram_with_binding(.., TlsServerEndPoint, None, ..)` call surfaces the same failure as a typed `Error::AuthPolicy`.

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

The public account module re-exports only `ImapAccountFactory`,
`ImapAccountConfig`, and `ManageSieveConfig`, preserving the
existing conformance-test path `bifrost_imap::account::{...}`. The
crate root also re-exports the factory and config types directly.
`ImapAccount` itself is
`pub(crate)`.

Submodules:

- `factory.rs` - `ImapAccountConfig`, `AccountFactory::open(account_id)`, optional `BandwidthMeter` / `MeterSink` wiring, optional CardDAV contact account open, `ID` probe, QRESYNC negotiation, initial folder LIST.
- `pool.rs` - per-folder connection checkout. Push lane reserves one slot; data lanes share the rest. Every dialed connection receives the account-scoped `MeterSinkHandle` and shared bandwidth-cap atomic when configured.
- `folder_registry.rs` - mailbox map plus per-folder cursor cache and per-folder MODSEQ cache (`record_modseq`, `modseq`, `clear_modseqs`). Cache is keyed by `(folder, uidvalidity, uid)`, stores LIST delimiter / attributes for PIM containers, and clears on UIDVALIDITY change, delete, or rename.
- `envelope.rs` - `FolderCursor` (QResync / Condstore / Basic) plus `encode_cursor`/`decode_cursor` over `OpaqueChangeState`.
- `capabilities.rs`, `inventory.rs`, `changes.rs`, `get.rs`, `blob.rs`, `mutate.rs`, `push.rs`, `close.rs`, `scopes.rs` - one file per `Account` method group.
- `pim.rs` - Stage 1 mail action surface: container membership, keyword/read mutations, search, folder CRUD, quota, draft create/discard, message/thread hydration, and IMAP-specific thread move/delete conveniences.
- `sieve.rs` - optional ManageSieve client plus Stage 2 literal
  script filter list/create/update/delete/validate.

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
- Draft create/discard: APPEND to the Drafts folder with `\Draft` when Drafts and UIDPLUS are present; discard deletes the draft object id. Draft bodies are built by the shared `bifrost-types::mime` assembler (inline attachments supported; uploaded-attachment handles still rejected as `AttachmentUpload`), so the `Bcc:` header is preserved in the saved draft.
- Send / draft-send: real when `ImapAccountConfig::with_submission(SmtpSubmissionConfig)` is set (see "Submission" below); `Unsupported` otherwise.

Unsupported PIM methods return `Error::Unsupported` and have false capability flags: attachment upload, `host_attachment` (cloud hosting is Gmail/Graph only), draft update, Gmail label membership, Graph categories and extended properties, identities, identity update, vacation get/set, and `scheduled_send` (cancel/reschedule too). `scheduled_send` is statically false even with submission configured because FUTURERELEASE is a per-connection EHLO truth unknown at open; a scheduled send is attempted and the relay's EHLO at send time decides (see "Submission"). SMTP send and draft-send are unsupported only when no submission config is present. IMAP identities and vacation responders (external config or Sieve-shaped) are not exposed in Stage 1.

### Submission (SMTP send)

`ImapAccountConfig::with_submission(SmtpSubmissionConfig)` makes the account
own a `bifrost-smtp` transport (`crates/imap/src/account/submission.rs`,
`SubmissionTransport`). The factory builds it at open and stores it on
`ImapAccountInner.submission`; `build_capabilities(.., submission_configured)`
flips `send_message` / `draft_send` true together with the impls (one atomic
flag/behavior flip - the flag never lies). `attachment_upload` stays false (A6).

- `SmtpSubmissionConfig` carries host, `SubmissionTls` (Implicit 465 /
  StartTls 587 / Plaintext 587, mapped to SMTP `relay` / `starttls_relay` /
  `builder_dangerous`), optional port/timeout/pool, the default From address,
  a `save_to_sent_default`, and optional `SubmissionCredentials`. It is `Clone`
  but not `Eq`/serde (a live token source is neither).
- Credential reuse: when `credentials` is `None`, the SMTP credentials are
  derived from the IMAP `Credentials` via `credentials.kind()` -
  password->password, or OAuth identity + the *same* `Arc<dyn TokenSource>`
  (A1) cloned across the boundary. An override supplies explicit submission
  auth.
- The send path stays in `bifrost_types::Address` space. The only
  `Address -> bifrost_smtp::Address` conversion is the bare addr-spec
  reverse-path / recipient conversion at `SubmissionTransport::send_rfc5322`,
  which drives `AsyncSmtpTransport::send_raw_batch_with_options` and returns
  SMTP's already-translated `AccountError` (no IMAP `Smtp` error variant; a
  submission *build* failure maps to `InvalidInput`). `draft_send` re-stamps
  the returned error's operation to `DraftSend`.
- Scheduled send is one-shot SMTP FUTURERELEASE: `SendRequest::scheduled`
  threads a `hold: Option<SystemTime>` into `send_rfc5322`, which builds
  `SendOptions::hold_until(rfc3339(t))` (absolute-time, so the boundary never
  races `now()`; the relay computes the delay). No IMAP-side pre-validation -
  the relay's EHLO at send time is authoritative. A relay that did not
  advertise FUTURERELEASE yields a bifrost-smtp `FeatureUnsupported` already
  mapped to `Unsupported(Send)`; the IMAP boundary only re-stamps
  operation/protocol (it cannot change the kind), so the consumer sees a stable
  `Unsupported(Send)`. A HOLD over the relay's advertised max arrives as
  `Request(Malformed)`. RFC 4865 has no recall verb, so `cancel_scheduled_send`
  and `reschedule_send` are `Unsupported`.
- MIME assembly is the shared `bifrost-types::mime` serializer
  (`send_request_to_rfc5322` for send, `render_rfc5322` for drafts), lifted
  from Google's `MailDocument` so Google and IMAP share one path. It emits
  text/html/`multipart/alternative` bodies, a `multipart/mixed` wrapper for
  inline attachments, RFC 2047 display names, and a controlled-domain
  `Message-ID` (sender domain, never `hostname::get()`).
- `ObjectId` rule: `send_message` / `draft_send` return the real
  APPENDUID-derived id from the Sent APPEND when UIDPLUS yields one, else the
  generated/parsed `Message-ID` (`imapmsgid1:<id>`). The send result is
  authoritative for `Ok`; the id depends only on whether a real APPENDUID was
  obtained.
- Sent-APPEND contract: after a committed SMTP send, a failed (or
  no-UIDPLUS, or no-Sent-folder) APPEND is non-fatal - SMTP is never
  re-driven - but not silent: it logs an uncertain-Sent reconcile warning and
  falls back to the generated `Message-ID`.
- `draft_send` is "fetch + send + discard": a net-new one-shot raw `BODY[]`
  full-message fetch (bounded by `DRAFT_FETCH_BUDGET`, 64 MiB, through the
  `FetchLimit` guard - never `usize::MAX`) recovers the verbatim draft
  octets (hydration only returns a parsed projection), the headers build the
  envelope, and the `Bcc:` header is folded into RCPT recipients but
  stripped from the transmitted body. A failed post-send discard is logged,
  not fatal.
- Sent-copy Bcc retention: the body transmitted over SMTP strips `Bcc:`
  (blind recipients are never disclosed on the wire), but the copy APPENDed
  to the Sent folder retains it - the sender's Sent copy is their own record
  of who was blind copied. `send_message` uses the assembler's `sent_copy`
  variant (a Bcc-bearing render, present only when the request has a Bcc);
  `draft_send` APPENDs the original saved-draft octets, which already carry
  the `Bcc:` header.

`bifrost-imap` now depends on `bifrost-smtp` (feature `tokio`;
`account-error` rides in as a default feature). SMTP has no path back to
IMAP, so this is acyclic.

When `ImapAccountConfig::with_manage_sieve(ManageSieveConfig)` is
set, IMAP advertises `filter_rule_shape: Scripts` and all five
filter method flags true. `sieve.rs` speaks ManageSieve over
implicit TLS, STARTTLS, or plaintext, reuses the account credentials
with SASL PLAIN or XOAUTH2, and maps scripts to
`FilterScript { language: Sieve }`. Without ManageSieve config, the
filter flags remain false and calls return `Unsupported`.

When `ImapAccountConfig::with_carddav(CardDavConfig)` is set, IMAP
opens a sibling `bifrost-carddav` account during factory open,
advertises all contact method flags true, and delegates
`address_books_list`, `contacts_list`, `contact_get`,
`contact_create`, `contact_update`, `contact_delete`, and
`contact_search` to that account. Without CardDAV config, contact
flags remain false and calls return `Unsupported`.

When `ImapAccountConfig::with_caldav(CalDavConfig)` is set, IMAP
opens a sibling `bifrost-caldav` account during factory open,
advertises all calendar method flags true, and delegates
`calendars_list`, `events_in_range`, `event_get`, `event_create`,
`event_update`, `event_delete`, `event_rsvp`, and `event_search` to
that account. Without CalDAV config, calendar flags remain false and
calls return `Unsupported`.

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

`Error::No` and `Error::Bad` both carry `attempt:
Option<ImapAttempt>`; constructors (`no_with_code`, `bad_with_code`)
default this to `Some(Acknowledged)` because a tagged `NO` / `BAD` is
by definition a server-acknowledged terminal response. Without this,
recovery rows that key on `Acknowledged` (e.g. `Server(Error { status:
None }) + Acknowledged -> ProviderRefused`) collapse to the `Unsent`
arm and misclassify provider refusals as retryable transport drops.

### Concurrency conflicts and UNCHANGEDSINCE

IMAP diverges from the `MutationSuccess::Skipped` lane that other
provider crates use when the engine's "already in this state" probe
short-circuits a mutation. IMAP's MODSEQ cache is opportunistic: a
cold-cache STORE goes out without `UNCHANGEDSINCE`, so we cannot
observe "already in state" without a full SELECT+FETCH that would
defeat the purpose of the guard. Concretely, `STORE UNCHANGEDSINCE
<modseq>` rejecting with `MODIFIED` surfaces as `ItemOutcome::Failed
{ kind: ConcurrencyConflict }` for the conflicting UIDs - never as
`Succeeded(Skipped)`. `concurrency_conflict_error` /
`store_failed_error` / `uidvalidity_changed_error` each take the
caller's `AccountOperation` so flag / move / destroy paths emit
their own op (the central recovery mapping picks
`Retry::AfterStateRefresh` regardless, but the operation tag drives
telemetry and per-op retry budgets).

### Strategy downgrade derivation

`EngineDirective::DowngradeStrategy` is reserved for the "all
strategies exhausted" case. IMAP today never derives that directive
at runtime: `changes.rs` handles every QRESYNC -> CONDSTORE ->
Basic downgrade inline via `Warning::StrategyDowngraded` plus a
direct retry on the lower strategy. The `strategy_failure` helper
in `account/error.rs` is wired through the builder funnel and
covered by tests so the directive is producible when needed, but
the runtime path that reaches it does not exist while Basic remains
the universal fallback.

### Per-folder mutation failure contract

`mutate::mutation_stream` does not emit a trailing global
`SyncEvent::Terminated` once a folder fails mid-batch. A per-folder
fatal after per-item emissions surfaces as `ItemOutcome::Uncertain`
for every remaining target in the failing folder (carrying the
classified `AccountError`), and the loop continues to the next
folder. Stream-level `Terminated` is reserved for failures that
prevent any further folder attempt - auth lost, schema /
capability break. `stream_terminating` is the gate.

### Output-channel-dropped contract

Every streaming task (`inventory_stream`, `changes_stream`,
`get_stream`, `open_blob`, `mutation_stream`) treats a `tx.send`
failure on a dropped output receiver as silent termination: the
task returns without synthesizing any `crate::Error` and without
emitting a fatal `SyncEvent::Terminated`. The error funnel is
reserved for wire failures and structural invariant breaks; a
consumer that walks away from its stream is not an error.

### Terminated-event helper

`account/mod.rs` exposes a single `terminated_event::<T, _>(cause)`
helper for surfacing fatal stream causes as
`SyncEvent::Terminated`. It accepts any `Into<TerminatedCause>`:
- a pre-built `AccountError` (UIDVALIDITY change, modseq reset,
  pre-classified failures), or
- an `(Error, ImapErrorContext)` pair to classify on the way out
  (the legacy `fatal_event(err, ctx)` alias keeps reading naturally
  at call sites).
Callers do not need to choose which lane to dispatch through.

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
|   |-- blob.rs            - open_blob, open_blob_range, open_raw_rfc822 (BODY.PEEK[])
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
|   |-- sieve.rs           - ManageSieve script filters
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
