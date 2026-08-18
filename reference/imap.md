# bifrost-imap reference

Current architecture of the IMAP client crate. Daaki-derived. Tokio + native-tls only.

The public consumer surface is intentionally small after S1-W3: `ImapAccountFactory`, `ImapAccountConfig`, `ImapConfig`, `ManageSieveConfig`, `Credentials`, `AuthPolicy`. Consumers construct the factory and use it through `Arc<dyn bifrost_types::AccountFactory>`; raw IMAP, ManageSieve, parser, command, protocol-error, and sync-helper surfaces are crate-internal. CardDAV/CalDAV composition (`with_carddav`/`with_caldav`) is under "Account layer".

## Driver-owned I/O

One tokio task owns the socket, parser state, and a watch channel exposing connection state. `ImapConnection` is a cheap internal handle around the driver. Connection methods take `&self` and talk to the driver over channels.

The driver model is load-bearing for cancellation safety, atomic stream upgrades (STARTTLS, COMPRESS), typed event-queue draining without contending with command I/O, and streaming FETCH without raw stream access.

No boxed-transport escape hatch. Supported transport is `tokio::net::TcpStream` plus optional `tokio_native_tls::TlsStream`.

Before the nom parser runs, `WireReader` asks a framing pre-check whether the buffer can hold a whole response, because the parser is in `complete` mode and cannot distinguish "truncated" from "malformed". The pre-check has three answers, not two: complete, need more bytes, and *unreachable*. Unreachable is a literal count this process can never be handed the octets for  -  above the RFC 9051 `number64` ceiling, or above `usize` on a narrow target, or a chain of counts whose running offset leaves the addressable buffer. Those return `Error::Parse` (connection-fatal, `ProviderContractViolation`) rather than "need more bytes", which would park the read loop until the peer closed. The bound matches `codec::decode::literal`, so the pre-check never accepts a count the decoder would reject and never rejects one the decoder would take.

Send-side literal scanning (`connection/literals.rs`) shares one marker parser with the read side and is length-aware in both directions: an already non-synchronizing (`{N+}`) body is skipped by its declared length, so marker-shaped bytes inside an APPEND payload are never mistaken for command syntax. The shared parser answers three ways, mirroring the framing pre-check: a counted marker (count guaranteed `<= i64::MAX`), a well-formed marker whose count is above the RFC 9051 `number64` ceiling, and "not a marker". Each call site decides what the middle answer means for it - fatal framing on the read path, ordinary text on the send path, where the buffer is one we built. A digit run too long even for `u64` is not recognizable framing at all and is handed to the decoder.

ESEARCH `ALL` sets are normalized before expansion into the flat compatibility `SearchResult`: `*` is refused wherever it appears (RFC 9051 `seq-number = nz-number / "*"`), not only as a range endpoint, because its concrete value is unknowable from the response; and legal overlap or repetition is sorted and merged, so both the 1e6 safety cap and the expanded ids count each UID once. An expansion that cannot be completed is `Error::SearchResultTruncated`, never a partial list presented as complete, and the solicited ESEARCH stays consumed by the command rather than being republished as an asynchronous event.

APPEND resolves APPENDLIMIT (RFC 7889) before writing anything: a numeric `APPENDLIMIT=<n>` capability is a global limit, a bare `APPENDLIMIT` sends a preflight `STATUS <mailbox> (APPENDLIMIT)`, and a server advertising both gets the most restrictive of the two. The STATUS reply is read from `items` *and* `ambiguous`, since NOTIFY STATUS makes the solicited reply wire-identical to a notification. Because that STATUS is a preflight, its failures are restamped `TransmissionState::Unsent` relative to the APPEND, as is a deadline that expires before submission: APPEND is non-idempotent, and leaking the helper's `InFlight` / `Acknowledged` evidence would send `DraftCreate` recovery down the reconcile path for a message no server ever saw.

## Connection state machine

The driver-owned command channel is the connection liveness signal.
`ImapConnection::is_alive()` reports whether the driver still owns the
receiver; this is separate from RFC session state, which can still say
`Selected` after a socket failure.

Driver-side I/O loss, BYE, parse failure, and protocol desynchronization
are connection-fatal. Before returning such an error, the driver closes
the command receiver and moves the session to `Logout`. The pool filters
dead parked members during checkout and never returns a dead checked-out
member to the idle list. Tagged NO/BAD responses and local validation or
capability errors leave the connection reusable.

A timed-out or cancelled command is a third case: the driver is
cancellation-safe and keeps executing the command after the caller's
`tokio::time::timeout` fires, so the connection is neither dead nor idle.
Every submit helper holds an `InFlightGuard` that marks the connection
`abandoned` when its future is dropped before the driver's result arrives,
and `PooledConn::drop` parks only a member that `is_reusable()` (alive AND
not abandoned). Without this, the next checkout queues behind the still
running command and times out in turn, so one slow command cascades into a
run of spurious timeouts on the same connection.

An untagged `* BYE` is fatal in every driver read loop - regular command,
prebuilt command, pipeline batch, IDLE (and its drain), literal
continuation wait, and best-effort LOGOUT - through one shared guard
(`short_circuit_on_bye`) fed by `ProtocolState::apply_side_effects`'s
`SideEffectDigest`. The digest is `#[must_use]` and its `had_bye` field is
private, so a new loop cannot silently ignore it. BYE is recognized from
the status tag alone, independently of the response code it carries, so
`* BYE [CAPABILITY ...]` cannot be routed away by the capability
side-effect arm. The error preserves the BYE text and structured response
code (`Error::bye_with_code`), which is what the account layer maps to a
specific `RecoveryClass`; response-code events (e.g. ALERT) are emitted
before the short-circuit fires. Command results are published to the state
watch channel *before* the caller's oneshot is answered, so a caller that
observes the result always sees the matching snapshot.

The codec draws the line that decides which of those two buckets a bad
response lands in. A response opening with a keyword the codec claims to
parse cannot degrade to `UntaggedResponse::Unknown`: it becomes
`nom::Err::Failure`, which the connection surfaces as `Error::Parse` and
the account boundary maps to `Protocol(ParseFailed)` /
`ProviderContractViolation`. Inside FETCH the gate is per attribute
(`codec/decode/envelope_fetch.rs::has_closed_grammar`): attributes with a
closed grammar (`UID`, `FLAGS`, `RFC822.*`, `INTERNALDATE`, `MODSEQ`,
`SAVEDATE`, `PREVIEW`, `EMAILID`, `THREADID`, `X-GM-*`, the sectioned
`BODY[...]` / `BINARY[...]` / `BINARY.SIZE[...]` forms) hard-fail.
`ENVELOPE`, `BODYSTRUCTURE`, and bare `BODY` additionally gate their safe
fixed prefixes (ENVELOPE's ten fields; single-part and multipart body
prefixes, applied recursively to every multipart child up to the typed
decoder's depth cap) *and* the balanced close of the outer structure, since
an extension tail is open in its contents but never in its framing. They
retain tolerance for the contents of those tails at every nesting level;
only fixed-arity violations hard-fail. Any entirely unmodelled attribute
also stays on the tolerant skip path: a failure there is at least as likely to
be a modelling gap of ours, and parse failure is connection-fatal. Separator
handling is uniformly multi-space for the same reason.

The strict lane must therefore never fire on a shape a real server sends.
`X-GM-LABELS` is the worked example: Gmail sends system labels as bare
backslash-prefixed atoms (`(\Inbox \Sent Important "Muy Importante")`), which
`astring` cannot express, so the label grammar is `"\" atom / astring` and
`X-GM-MSGID` / `X-GM-THRID` accept the quoted decimal some proxies emit.
Labels are decoded into `FetchResponse::gmail_labels` (modified UTF-7 decoded
for astring labels, system labels verbatim); requesting them needs
`X-GM-EXT-1`, and they count toward the buffered-FETCH byte estimate. The
generic IMAP account layer assigns them no folder semantics.

Cancellation safety comes from socket ownership: dropping a caller's
future does not cancel an in-flight wire exchange. The driver completes
the command and preserves framing before accepting the next command.

## Typed events

`TypedEvent` carries every unsolicited response (EXISTS, EXPUNGE, FETCH, FLAGS, STATUS, METADATA, NOTIFY-routed events). `drain_events()` and `next_event()` give callers ordered access.

`TypedEvent::impact()` returns `EventImpact` for cache-coherence: `SelectedMailboxChanged`, `SelectedMailboxResync`, `MailboxStateChanged`, `ServerMetadataChanged`, `Heartbeat`, `Extension`, `None`. RECENT maps to `None` (informational; deprecated by IMAP4rev2). ServerMetadataChange is server-scoped, not selected-mailbox scoped.

## Streaming FETCH

Streaming never drops responses. The driver-side consumer is a bounded pipe (`BoundedStreamingPipe`, default capacity 64) that pre-reserves an `OwnedPermit` *before* each socket read, so a slow consumer applies TCP backpressure instead of either buffering without limit or silently discarding under a failed `try_send`.

Three entry points:

- `uid_fetch_streaming()` / `fetch_streaming()` - caller supplies an unbounded sender; the bounded receiver is drained onto it internally.
- `uid_fetch_each(...)` - callback-based wrapper over the same bounded receiver.
- `uid_fetch_limited(budget, ...)` - hard client-side byte budget enforced inside the driver consumer; returns `Error::FetchLimit` when crossed. Driver keeps reading until tagged OK, so parser/TCP buffers may continue past the limit.

**Early consumer stop.** When a callback returns an error (or the caller's own sender is gone), the shared drain helper *drops* the bounded receiver rather than closing and draining it. The distinction is load-bearing: with an `OwnedPermit` outstanding, `recv()` on a merely closed receiver stays pending forever, so a server that stalls mid-response would outlive the command timeout and hang the joined future. Dropping makes the driver's next `reserve_owned` fail; it marks the pipe drained and keeps reading through the tagged completion so IMAP framing stays synchronized. Items already buffered when the consumer stopped are discarded, and the callback's error is the returned error.

**Termination is decided by the future, never by the channel.** A caller
driving a `(fetch_rx, fetch_fut)` pair through `tokio::select!` sees the
item receiver close *before* the future resolves - the driver drops the
consumer synchronously and only then answers the oneshot - so the two
branches are both ready and the choice between them is random. Every such
loop (`run_inventory`, `run_qresync`) therefore awaits `fetch_fut` on the
`recv() -> None` arm and propagates its result unconditionally. Reading
whatever `select!` happened to have stored would let a tagged NO, a read
error, or a timeout break on the success path and checkpoint a truncated
mailbox as complete.

Buffered `uid_fetch()` uses the driver buffer path directly, not the streaming channel. `uid_fetch_full_messages(budget, ...)` requires an explicit budget and routes through the limited path.

## Capability and ENABLE state

`ServerProfile` answers capability questions without ad-hoc CAPABILITY parsing, built from `state_rx.borrow()` at call time. **Snapshot semantics**: no auto-refresh - refetch after STARTTLS, AUTH, or ENABLE.

`rev2_implies` bakes in the RFC 9051 baseline: ESEARCH, IDLE, MOVE, NAMESPACE, SASL-IR, SEARCHRES, UIDPLUS, UNSELECT, LIST-EXTENDED, STATUS=SIZE, STATUS=DELETED, OBJECTID, SAVEDATE, BINARY, LITERAL+, LITERAL-, SPECIAL-USE, ENABLE (STATUS=DELETED encoding flows through the same gate).

## Auth

`Credentials` is a public opaque wrapper with `password(username, password)`,
`oauth2(identity, access_token)` (raw string), and
`oauth2_source(identity, Arc<dyn TokenSource>)` (shared rotation source)
constructors. The OAuth variant holds an `Arc<dyn TokenSource>` (bifrost-net's
trait), read via `current().await` at every connect/reconnect (in
`authenticate_best` and the ManageSieve auth path), so a rotated token is
presented fresh on reconnect. Password material stays in the internal
`SecretString` wrapper (zeroizes + redacts under `Debug`). `Credentials` is
`Clone` only (no `PartialEq`/`Eq` - a live token source is not `Eq`), no
`From<(String, String)>`.

Internal `AuthMechanism`: PLAIN, LOGIN, XOAUTH2, OAUTHBEARER, CRAM-MD5, SCRAM-SHA-1, SCRAM-SHA-256, and the channel-bound SCRAM-SHA-1-PLUS / SCRAM-SHA-256-PLUS. Each variant's `name()` is the wire token; the PLUS variants match the `-PLUS` advertisement and the SASL crate (`ScramHash::mechanism_name`) is the single authority for the token emitted on the socket.

SCRAM and CRAM-MD5 computation (`scram_client_final` / `verify_server_final`
transitions, per-hash proofs, CRAM-MD5 response) lives in the private
`bifrost-sasl` crate; the dispatch consumers (`AuthenticateScramConsumer`, `AuthenticateCramMd5Consumer`) drive the `+` continuation flow and map `bifrost_sasl::SaslError` into the IMAP error model at the boundary.

`AuthPolicy` TLS-gates cleartext mechanisms by default: PLAIN/LOGIN refuse over plaintext unless `allow_cleartext_without_tls` is set. CRAM-MD5 is opt-in (`with_cram_md5`) AND TLS-gated (a MITM can pick the challenge and brute-force `HMAC-MD5(password, challenge)` offline). LOGIN-the-IMAP-command is opt-in (`with_login`).

`authenticate_best(credentials, policy)` intersects server-advertised, policy-allowed, and credentials-supported mechanisms, then runs the strongest. SASL-IR is used when advertised or implied by IMAP4rev2; malformed mechanism names are rejected before any wire write. `AuthOutcome` carries the selected mechanism.

The password ladder is a pure helper `password_mechanism_ladder(profile, policy, is_encrypted)` returning `PasswordCandidate::{Attempt, Reject}` in the fixed order SCRAM-SHA-256-PLUS > SCRAM-SHA-1-PLUS > SCRAM-SHA-256 > SCRAM-SHA-1 > PLAIN > CRAM-MD5 > LOGIN. RFC 5802 Section 6 downgrade protection: when the server advertises `SCRAM-SHA-N-PLUS`, the matching unbound `SCRAM-SHA-N` rung becomes `Reject(ChannelBindingUnavailable)` so a MITM cannot strip the binding. For a PLUS `Attempt`, `authenticate_best` resolves the `tls-server-end-point` binding once via `resolve_scram_binding()` (peer-cert DER + `bifrost_sasl::tls_server_end_point`); a binding that cannot resolve (plaintext, EdDSA leaf cert, unsupported sig-alg) becomes a `ChannelBindingUnavailable` rejection, so an EdDSA-cert server advertising `-PLUS` disables SCRAM and falls through to PLAIN-over-TLS (credential stays encrypted).

## Typed IDs and sets

Newtypes with explicit `::new` constructors. No `From<u32>`/`From<u64>` to prevent accidental mixing across UID, sequence, message-id, and thread-id contexts:

- `Uid`, `Seq` - `NonZeroU32`-backed.
- `UidValidity` - `NonZeroU32`.
- `ModSeq` - `u64` (0 means "no CONDSTORE state").
- `GmailMessageId`, `GmailThreadId` - `u64`.

`UidSet` and `SeqSet` wrap the validated sequence-set encoder (`from_uids`, `from_seqs`, `all()`, `saved_search()` (`$`), or range constructors). `parse(&str)` is an escape hatch bypassing the typed discipline.

## Configuration and connect

`ImapConfig` centralizes TLS mode (Implicit / StartTls / Plaintext), connect/command timeouts, keepalive, and optional native-tls connector. Ports default per mode (993 / 143 / 143). Public constructors are `tls`, `starttls`, `plaintext`; builders cover port, timeouts, keepalive disablement, and custom TLS connectors. The raw `connect` / `connect_authenticated` entry points were deleted in S1-W3.

Internally, `connect_authenticated_metered(credentials, policy, meter, cap)` composes connect + STARTTLS-if-needed + `authenticate_best` for the factory and pool, returning `(ImapConnection, AuthOutcome)` crate-only.

## Sync helpers

`SyncSelectOptions` / `SyncSelectResult` wrap SELECT/EXAMINE with CONDSTORE and QRESYNC parameters (UIDVALIDITY, last-known MODSEQ, known UIDs, VANISHED parsed off the SELECT response).

`SyncFetchRequest` / `SyncFetchResult` wrap UID FETCH with `CHANGEDSINCE` and `VANISHED`; `sync_fetch()` surfaces both VANISHED ranges and per-UID FETCH responses.

`SelectedMailbox` carries `highest_mod_seq`, `uid_validity`, `uid_next`, `no_mod_seq`, and the SELECT-side VANISHED list. A missing `UIDVALIDITY` is a protocol error rather than a silent 0; a `MODSEQ 0` in a FETCH response is rejected for the same reason.

## Account layer

The shared `bifrost_types::Account` impl lives under `crates/imap/src/account/`. `ImapAccountFactory::open(account_id)` opens a connection pool, lists folders, builds capabilities, and threads the engine account id into optional raw-socket bandwidth metering. `ImapAccount` backs both the sync methods and Stage 1 PIM primitives.

The public account module re-exports only `ImapAccountFactory`, `ImapAccountConfig`, and `ManageSieveConfig` (preserving the conformance-test path `bifrost_imap::account::{...}`); the crate root re-exports the factory and config types directly. `ImapAccount` itself is `pub(crate)`.

Submodules:

- `factory.rs` - `ImapAccountConfig`, `AccountFactory::open(account_id)`, optional `BandwidthMeter`/`MeterSink` wiring, fail-soft CardDAV/CalDAV sub-account open (`DavAttach`), `ID` probe, QRESYNC negotiation, folder LIST, and A5c NAMESPACE/ACL shared-folder discovery (`discover_shared_folders`).
- `pool.rs` - per-folder connection checkout. Push lane reserves one slot; data lanes share the rest. Every dialed connection receives the account-scoped `MeterSinkHandle` and shared bandwidth-cap atomic when configured.
- `folder_registry.rs` - mailbox map plus per-folder cursor and MODSEQ caches (`record_modseq`, `modseq`, `clear_modseqs`), keyed by `(folder, uidvalidity, uid)`, storing LIST delimiter/attributes and the A5c `shared_owner` tag, clearing on UIDVALIDITY change, delete, or rename.
- `envelope.rs` - `FolderCursor` (QResync/Condstore/Basic) plus `encode_cursor`/`decode_cursor` over `OpaqueChangeState`.
- `capabilities.rs`, `inventory.rs`, `changes.rs`, `get.rs`, `blob.rs`, `mutate.rs`, `push.rs`, `close.rs`, `scopes.rs` - one file per `Account` method group. `mod.rs` holds `route_scope`/`ScopeHandler` (sub-account dispatch); `scopes.rs` the discovery fan-in.
- `pim.rs` - Stage 1 mail actions: container membership, keyword/read mutations, search, folder CRUD, quota, draft create/discard, message/thread hydration, IMAP-specific thread move/delete conveniences.
- `sieve.rs` - optional ManageSieve client plus Stage 2 literal script filter list/create/update/delete/validate.

### CONDSTORE / QRESYNC strategy

`FolderCursor` has three variants and `changes_stream()` dispatches diff strategy by variant:

- `QResync { uidvalidity, modseq, known_uids, known_uids_complete }`: change diff via `UID FETCH ... CHANGEDSINCE ... VANISHED` against the cached MODSEQ, plus SELECT-side VANISHED and changed-FETCH data.
- `Condstore { uidvalidity, modseq, known_uids }`: flag diff via `CHANGEDSINCE`; expunge detection via UID-list diff against `known_uids`. `CHANGEDSINCE` returns arrivals as well as updates, so a UID absent from `known_uids` is not emitted as `Updated` - the baseline diff reports it as `Added`, once. The QRESYNC path guards the same hazard through `record_fetch_change` / `fetch_change_seen`.
- `Basic { uidvalidity, uidnext, known_uids }`: neither extension; UID-list diff for both flags and expunges.

Negotiation in `factory.rs`:

1. `ID` probe runs when advertised; failures are non-fatal. iCloud is preconfigured off via exact-match `name` checks (`"icloud"` / `"icloud imap"`); substring matches do not trip the downgrade.
2. If `enable_qresync` and the server advertises QRESYNC, the factory runs `ENABLE QRESYNC`; the `ENABLED` reply must echo `QRESYNC` (or `ServerProfile::enabled` confirms it), else the session continues CONDSTORE-only with a one-shot `OperatorAttentionNeeded` warning on first changes-stream attach.
3. The QRESYNC capability check is exact (not substring) to avoid false positives on capabilities with `QRESYNC` as a suffix.

Runtime downgrades:

- A QRESYNC SELECT response that fails to parse calls `disable_qresync_for_session()` (one-shot), discards the suspect pooled connection, and retries on CONDSTORE.
- A QRESYNC or CONDSTORE cursor lacking a complete UID baseline (`known_uids_complete == false`) seeds via `UID SEARCH ALL` before the first diff and emits a benign downgrade warning.
- VANISHED and FETCH may report the same UID on non-conformant servers; the change stream de-duplicates so a message never surfaces as both expunge and update.

### Mutations

`bulk_set_flags` and `bulk_destroy` partition targets by cached MODSEQ: cache hits go out under `STORE UNCHANGEDSINCE <modseq>`, misses fall back to unprotected STORE, and protected batches run first so their success updates the cache before the unprotected pass.

- The MODSEQ cache is populated from inventory, get, changes, mutation SELECT data, and push IDLE FETCH events; cleared by successful flag mutations, expunges, VANISHED, moves, and folder delete/rename.
- `bulk_destroy` partial-failure accounting separates conflicts (UNCHANGEDSINCE rejected) from expunge failures; an expunge failure applies only to that round's UIDs, not the whole batch.
- A two-sided `FlagOp::Patch` (non-empty add and remove) is two STOREs, and every id is accounted per UID across both. The guard rides the first (add) STORE; UIDs it rejects with `MODIFIED` are `Failed(ConcurrencyConflict)` and are excluded from the second STORE. Only the add-applied subset receives the unguarded remove, and only that subset may report `Succeeded(Applied)` - a second STORE that errors or comes back non-applied leaves those ids `Uncertain`, never succeeded. A wire error on the first STORE is confined to its own MODSEQ group: outcomes already established for earlier groups survive it and are never downgraded by the folder-level uncertain path.
- `bulk_move` validates the destination mailbox once, before any source folder is opened. An unusable destination is our own request fault, so every target fails `Request(Malformed)` rather than passing through the per-folder uncertain path.

Capabilities advertise `MutationConcurrency::None`: the MODSEQ cache is opportunistic (cold cache = unprotected STORE), so `StateBased` would let the engine assume UNCHANGEDSINCE is always wired. The engine's read-back-after-retry path is the lost-update safety net.

### PIM primitives

`capabilities.rs` fills `AccountCapabilities::pim_methods` and `conveniences` at open time, and sets `AccountCapabilities::reopen_discovers_foreign_namespaces` from the open-time NAMESPACE response (`namespaces_advertise_foreign`, pure, `factory.rs`): true iff any `other`/`shared` descriptor carries a non-empty prefix, independent of whether any folders are currently shared. IMAP emits no scope-lifecycle events and discovers foreign folders only at open, so consumers read this flag to decide whether a periodic reopen/reattach (the only way a post-open ACL grant becomes visible) can ever surface anything on this server. IMAP advertises real support for:

- Container membership: `add_to_container` via UID COPY, `remove_from_container` via `+FLAGS.SILENT \Deleted` plus UID EXPUNGE.
- Keywords: `set_keyword` via UID STORE; convenience keywords `$flagged`/`$answered`/`$seen` map to `\Flagged`/`\Answered`/`\Seen`. `$forwarded` and `$MDNSent` stay IMAP keywords; `mdn_sent_via_keyword` is true, so `mark_mdn_sent` flips `$MDNSent`.
- Read state: `set_is_read` via `\Seen`.
- Importance: `set_importance` maps onto the `$important` keyword (no native IMAP field). Two-valued/exclusive: `High` sets it, `Normal`/`Low` clear it (one STORE); the read side maps `$important` presence to `Message.importance`.
- Search messages: UID SEARCH across selectable folders, `SearchFilter::In` restricting the selected mailbox. Thread search is advertised only with `THREAD=REFERENCES`, returning synthetic thread ids containing folder, UIDVALIDITY, and member UIDs.
- Containers: LIST-backed enumeration plus CREATE, RENAME-as-rename, RENAME-as-move, and guarded DELETE (checks `STATUS MESSAGES`, refuses non-empty mailboxes).
- Quota: `GETQUOTAROOT`, mapped from STORAGE units to bytes when QUOTA is advertised. Hydration: one-shot message FETCH and synthetic thread hydration.
- Draft create/discard: APPEND to Drafts with `\Draft` when a delimiter-aware Drafts role and UIDPLUS or active IMAP4rev2 are present; discard is advertised only when UID EXPUNGE is available. Draft bodies use the shared `bifrost-types::mime` assembler (inline attachments supported; uploaded-attachment handles rejected as `AttachmentUpload`), preserving the `Bcc:` header in the saved draft.
- Send / draft-send: real when `ImapAccountConfig::with_submission(SmtpSubmissionConfig)` is set (see "Submission" below); `Unsupported` otherwise.

Unsupported PIM methods return `Error::Unsupported` with false flags: attachment upload, `host_attachment` (Gmail/Graph only), draft update, Gmail label membership, Graph categories/extended properties, identities + identity update, vacation get/set, `scheduled_send` (cancel/reschedule too). `scheduled_send` is statically false even with submission because FUTURERELEASE is a per-connection EHLO truth unknown at open; the send is attempted and the relay's EHLO decides. SMTP send/draft-send are unsupported only without submission config. IMAP identities and vacation responders are not in Stage 1.

### Submission (SMTP send)

`ImapAccountConfig::with_submission(SmtpSubmissionConfig)` makes the account own a `bifrost-smtp` transport (`submission.rs`, `SubmissionTransport`), built at open and stored on `ImapAccountInner.submission`. `build_capabilities(.., submission_configured)` flips `send_message` / `draft_send` true together with the impls. `attachment_upload` stays false (A6).

- `SmtpSubmissionConfig` carries host, `SubmissionTls` (Implicit 465 / StartTls 587 / Plaintext 587, mapped to SMTP `relay` / `starttls_relay` / `builder_dangerous`), optional port/timeout/pool, default From, `save_to_sent_default`, optional `SubmissionCredentials`. `Clone`, not `Eq`/serde.
- Credential reuse: when `credentials` is `None`, SMTP creds derive from the IMAP
  `Credentials` via `credentials.kind()` (password->password, or OAuth identity +
  the *same* `Arc<dyn TokenSource>` cloned across the boundary); an override
  supplies explicit submission auth.
- The send path stays in `bifrost_types::Address` space; the only
  `Address -> bifrost_smtp::Address` conversion is the bare addr-spec
  reverse-path / recipient conversion at `SubmissionTransport::send_rfc5322`,
  which drives `AsyncSmtpTransport::send_raw_batch_with_options` and returns
  SMTP's already-translated `AccountError` (a build failure maps to
  `InvalidInput`). `draft_send` re-stamps the operation to `DraftSend`.
- Scheduled send is one-shot SMTP FUTURERELEASE: `SendRequest::scheduled`
  threads `hold: Option<SystemTime>` into `send_rfc5322` ->
  `SendOptions::hold_until(rfc3339(t))` (absolute-time, never races `now()`).
  No IMAP-side pre-validation - the relay's EHLO at send time is authoritative.
  A relay without FUTURERELEASE yields a stable `Unsupported(Send)`; a HOLD over
  the advertised max arrives as `Request(Malformed)`. RFC 4865 has no recall
  verb, so `cancel_scheduled_send` / `reschedule_send` are `Unsupported`.
- MIME assembly is the shared `bifrost-types::mime` serializer
  (`send_request_to_rfc5322` for send, `render_rfc5322` for drafts; shared with
  Google's `MailDocument`): text/html/`multipart/alternative` bodies, a
  `multipart/mixed` wrapper for inline attachments, RFC 2047 display names, and a
  controlled-domain `Message-ID` (sender domain, never `hostname::get()`).
- `ObjectId` rule: `send_message` / `draft_send` return the APPENDUID-derived id
  from the Sent APPEND when UIDPLUS yields one, else the generated/parsed
  `Message-ID` (`imapmsgid1:<id>`). The send result is authoritative for `Ok`.
- Sent-APPEND contract: after a committed send, a failed (or no-UIDPLUS, or
  no-Sent-folder) APPEND is non-fatal (SMTP is never re-driven), logs an
  uncertain-Sent reconcile warning, and falls back to the generated `Message-ID`.
- `draft_send` is "fetch + send + discard": a one-shot raw `BODY[]` fetch
  (bounded by `DRAFT_FETCH_BUDGET` 64 MiB, through `FetchLimit`) recovers the
  verbatim octets, the headers build the envelope, `Bcc:` is folded into RCPT but
  stripped from the body. A failed post-send discard is logged, not fatal.
- Sent-copy Bcc retention: the SMTP body strips `Bcc:` but the Sent APPEND
  retains it. `send_message` uses the assembler's `sent_copy` variant (Bcc-bearing,
  only when the request has a Bcc); `draft_send` APPENDs the saved-draft octets,
  which already carry `Bcc:`.

`bifrost-imap` depends on `bifrost-smtp` (feature `tokio`; `account-error` is a
default feature). SMTP has no path back to IMAP, so this is acyclic.

With `ImapAccountConfig::with_manage_sieve(ManageSieveConfig)`, IMAP advertises `filter_rule_shape: Scripts` and all five filter flags true; `sieve.rs` speaks ManageSieve over implicit TLS / STARTTLS / plaintext, reuses the account credentials with SASL PLAIN or XOAUTH2, and maps scripts to `FilterScript { language: Sieve }`. Without it, filter flags stay false and calls return `Unsupported`.

With `with_carddav(CardDavConfig)` / `with_caldav(CalDavConfig)`, IMAP opens a sibling `bifrost-carddav` / `bifrost-caldav` account at factory open and delegates the contact / calendar PIM primitives to it. Without the config, the matching flags stay false and primitives return `Unsupported`.

Composition is first-class for sync, not just primitives (subs are full `Arc<dyn Account>`s): `build_capabilities` copies the sub's real contact/calendar `pim_methods` subsets; `route_scope` maps a `CursorScope` to `ScopeHandler::{Folder, Delegate}` and the four sync entry points forward to the sub on `Delegate`; discovery fans sub scopes in (sub errors fold to `Warning`). Routing + fan-in use the reusable `bifrost_types::account_compose` helpers. `open_carddav`/`open_caldav` are fail-soft (`DavAttach::Degraded`, classified via `RecoveryClass`): a DAV open failure degrades to IMAP-only for the cycle and the next reopen retries, so a DAV outage never takes mail offline. The degradation is recorded twice on purpose: as the discovery-surfaced `Warning` (live signal) and as a `SkippedScope` on `OpenedAccount::skipped_scopes` (`ContactCollection` / `CalendarCollection` scope + the classified error), the queryable open-time record.

`ConvenienceShape` declares IMAP starred/replied/forwarded as keyword-shaped. `move_thread`/`delete_thread` override the trait defaults via the cloneable account handle (add-then-remove); delete moves to the Trash role unless already in Trash, where it expunges from that mailbox.

Containers use native mailbox paths as primitive/provenance ids. `containers_list` maps SPECIAL-USE attributes to `FolderRole` (`\Sent`, `\Drafts`, `\Archive`, `\Trash`, `\Junk`, custom `\Inbox`), falling back to name-based INBOX/Sent/Drafts/Archive/Trash/Spam detection on the leaf selected with the LIST hierarchy delimiter. `folder_registry::leaf_name` is the single owner of that split, shared with the `draft_create` Drafts probe so a `.`-delimited server cannot advertise a draft capability whose APPEND target `role_folder` then fails to resolve; a NIL delimiter keeps the legacy `/` fallback rather than treating the whole name as the leaf. When several folders map to one role, SPECIAL-USE wins over name fallback and mailbox path breaks remaining ties deterministically.

`container_from_folder_entry` also projects the shared-namespace metadata: a folder with a `shared_owner` gets `namespace = Shared`, `owner = MailboxId(owner)`, and `owner_local_id` = the full mailbox path (IMAP has no separate per-owner id space - the path IS the native id in every namespace, so `native_id` stays byte-identical to the `CursorScope::Folder` string discovery emits). `rights` projects the MYRIGHTS set `discover_shared_folders` captured at open (`rights_from_myrights`, RFC 4314 Section 4: `l`+`r` -> read, `i` -> add, `t` -> remove, `s` -> seen, `w` -> keywords, `k`/`c` -> create child, `x`/`d` -> rename+delete, `p` -> submit). The rights set is now RETAINED on `FolderEntry`, not just used as the discovery read-gate - without it a read-only share is indistinguishable from a writable one downstream. `None` means unreported (a personal folder we never probed, or a server without ACL), which is distinct from an explicit empty rights set. A rename/recreate inherits both the owner tag and the rights, since a LIST/IDLE `MailboxInfo` carries neither.

### Hydration accounting (`get.rs`)

`get_stream` groups requested ids by folder and answers every requested id exactly once.

- Ids whose UIDVALIDITY no longer matches the selected mailbox are `Failed(Request(Malformed))` and are published *before* the hydration FETCH is issued. They are known truth already; buffering them behind a fallible command would relabel them uncertain whenever that command fails. The per-folder error path in turn only downgrades ids that still lack a published outcome (`run_folder_get` prunes the caller's unresolved set as it publishes), so a FETCH failure after the stale batch cannot put one id in two lanes.
- A requested UID the server never returns is `Failed(NotFound(Message))` rather than being silently dropped.
- FETCH responses are merged per UID before conversion. A server may follow the solicited response with unsolicited FLAGS-only FETCHes for the same UID; the merge adopts later `FLAGS` / `MODSEQ` and fills gaps, but never blanks a data item or body section the earlier response carried, so a trailing partial response cannot turn a complete hydration into an empty one.
- `Projection::Preview` and `Projection::TextOnly` both ask for `BODY.PEEK[]` (whole message, not the TEXT section) - preview bounded to a prefix, text-only unbounded. `BODY[TEXT]` is never requested by either: the hydrated value is raw MIME the consumer parses, and the TEXT section of a multipart message is boundaries and base64 with no headers to decode them by. `get.rs` and `pim.rs` use the same 64 KiB preview floor (`PREVIEW_FETCH_BYTES`), raised to the caller's limit when that is larger.
- Full and preview hydration parse the fetched RFC 5322 source through
  `bifrost-types::mime`. Full exposes text and HTML plus attachment metadata;
  `FullWithBlobs` carries decoded attachment bytes inline because IMAP has no
  redeemable per-part handle. Preview fetches a whole-message prefix (at least
  64 KiB, or the requested limit when larger) rather than `BODY[TEXT]`, so
  multipart framing is not shown as prose - in `pim.rs` and `get.rs` alike.
  RFC 2047 and RFC 2231 decoding now
  lives in `bifrost-types::mime`, shared with this parser.

### Blob openers

`AccountCapabilities::blob_range` is `BlobRangeSupport::No` and `open_blob` / `open_blob_range` return `Unsupported`. IMAP can fetch a `BODY[]` section by range, but nothing in inventory or hydration mints a `BlobHandle` for a MIME part, so there is no handle a caller could hand back to an opener. `blob.rs` therefore serves only `open_raw_rfc822`, which streams the whole message via `BODY.PEEK[]`. Decoded attachment bytes nevertheless reach `FullWithBlobs` inline; that is not a resumable blob lane. Building the real capability (BODYSTRUCTURE traversal, a stable part-handle encoding, a consumer-facing projection that attaches handles) is filed as imap-G1.

### Bandwidth metering

`ImapAccountConfig` carries a process `BandwidthMeter` or generic `MeterSink`. The factory builds a `MeterSinkHandle` with the engine `AccountId` on every open and passes it to the initial connection plus pool dials. `WireReader` records bytes read/written on every read/write path; the shared bandwidth-cap atomic is read per chunk (`set_bandwidth_cap(None)` unlimited; `Some(0)` clamps to 1 B/s with a warning). The token bucket honors the cap exactly: a chunk larger than one second of budget owes `bytes / cap` seconds in total, slept in slices of at most 60 s (the 60 s value bounds a single timer, never the total debt), so a very low cap makes big reads proportionally slow rather than being silently exceeded. The bucket keeps time with `tokio::time::Instant`, so the metering arithmetic is testable under `tokio::time::pause()`.

### Folder lifecycle

`FolderRegistry::apply_mailbox_event` handles delete, rename, and delete-then-recreate: delete/rename drop the in-memory entry; a recreate (fresh UIDVALIDITY at the same name) installs a fresh `FolderEntry` with empty modseq cache and cursor, so a recreated mailbox cannot reuse the prior epoch's state.

A same-name LIST/IDLE re-announcement is the existing mailbox epoch, but its
attributes are live lifecycle state. The registry refreshes them (`\Noselect`,
`\NonExistent`, SPECIAL-USE) so a folder whose access or role changed does not
keep syncing under a stale projection until reopen. The refresh happens IN
PLACE: attributes, delimiter, and the derived `selectable` flag live behind
`FolderEntry::listing` (a `RwLock<FolderListing>`, read through
`selectable()` / `delimiter()` / `attributes()`), and the map keeps the same
`Arc<FolderEntry>`. Copying the state into a replacement entry and swapping
the map would drop every write a task made through an `Arc` it cloned before
the swap - a sync run holds its entry across awaits and commits a cursor and
MODSEQs at the end, so the lost-update window is real, not theoretical.

Mailbox management on a mailbox this account has SELECTed goes through
`PooledConn::deselect_target`: an affinity-free `checkout_any` can hand back a
connection still selected on the RENAME/DELETE target, and RFC 3501 lets a
server reject either for the selected mailbox (RFC 2683 2.2.2 asks the client
to close first). UNSELECT is preferred - it keeps the checkout on its
connection. A pre-UNSELECT server falls back to replacing the member, and the
old connection is LOGOUT'd BEFORE the replacement is dialed: dialing first
would hold `pool_cap + 1` physical connections across the handshake, which a
server enforcing a per-user connection limit rejects - precisely on the old
servers that need the fallback.

### Shared / other-user folders (A5c)

`factory::open` issues NAMESPACE after the personal LIST and enumerates each non-personal `other`/`shared` prefix via `LIST "" "<prefix>*"` (`discover_shared_folders`, prefix in the PATTERN - RFC 3501 6.3.8 leaves reference/pattern concatenation implementation-defined and a server that ignores the reference answers the reference form with the personal namespace), tagging each with its owning `MailboxId`. An other-user root (`#user/`) carries one descriptor for all users, so the principal is read per folder (`mailbox_owner_from_other_user_path`: the segment after the root, e.g. `#user/alice/INBOX` -> `alice`); a shared root collapses to itself (`mailbox_owner_for`). Under ACL, candidates are probed with MYRIGHTS and the parsed set rides out on the entry; a MYRIGHTS failure is non-fatal and defers to SELECT. Discovery no longer DROPS an unreadable candidate: the personal `LIST "" "*"` may echo the same path (RFC 2342), so dropping it left that echo behind as a bare personal entry and a read-only share presented downstream as a writable personal folder. The read decision moves one layer down to `shared_folder_is_selectable` (pure, `folder_registry.rs`): `\Noselect`/`\NonExistent` or a reported rights set without `l`+`r` clears `FolderEntry::selectable()`, which is exactly the flag `discover_cursor_scopes` filters on, so an unreadable share keeps its owner / `Shared` namespace / rights in `containers_list` and still never becomes a cursor scope. The owner rides on `FolderEntry.shared_owner` (via `FolderRegistry::from_lists`). Shared folders are ordinary `CursorScope::Folder` scopes (no new variant, no `route_typed_scope` change); discovery and inventory also emit `MembershipScope::Mailbox(owner)` per shared entry - explicitly, because `scope_covers_membership` does not link them (FolderId and MailboxId strings differ).

Two registry rules keep the shared tagging (and therefore the owner and the rights) attached to the entry:

- **Overlap precedence** (`shared_overrides_personal`, pure). RFC 2342 leaves `LIST "" "*"` free to include the non-personal namespaces and several servers do, so the same path can arrive from both the personal LIST and its own namespace LIST. `ingest_shared` lets the namespace candidate WIN that overlap when its path actually lies under its non-empty NAMESPACE prefix - the server's own declaration that the path is not personal. Skipping the overlap (the prior rule) left the folder registered bare: no owner, no `Shared` namespace, and `Container::rights == None` for a folder whose MYRIGHTS had been parsed and then discarded. A candidate NOT under its prefix is still skipped, which is what stops a reference-ignoring server from demoting a real personal INBOX.
- **Personal-only refresh** (`FolderRegistry::replace_personal`). A mid-session re-LIST after a create / rename / move / delete enumerates only the personal root, so `refresh_folders` must not `replace_all`: clearing the map would drop every shared entry, its owner tag, and its rights, blanking the shared half of `containers_list` until the next reopen.

Revocation quarantines, not escalates. A shared-folder SELECT permission denial (in `inventory.rs` establish and the three `changes.rs` strategy runners) routes through `error::shared_folder_error` to `SyncState(ScopeRevoked)` scoped to `Cursor(Folder(id))`, deriving `Engine(DisableScope(scope))`: the engine deletes that scope's cursor and broadcasts a scoped warning while siblings keep syncing. The same denial on a personal folder (`shared_owner == None`) stays the account-level terminal `NoPermission`. ACL rights are advisory (gate discovery only); `NO [ACL]` stays authoritative per-mutation.

## Error model

`Error` is `pub(crate)` and `#[non_exhaustive]` (variants: `AuthPolicy(String)`, `FetchLimit { estimated, limit }`, plus protocol/transport/capability). The account boundary converts protocol errors into `bifrost_types::AccountError` via `error::into_account_error(error, ctx)`; consumers never see the crate-internal taxonomy.

`ImapErrorContext` carries the calling `AccountOperation` (required), optional `ErrorScope`, `Provider`, explicit `transmission_state`, and `idempotency_override`. Every `pim.rs` method threads its operation via a local `op_err` closure stamping `ImapErrorContext::operation(<op>)` on every `map_err`; multi-call helpers (`copy_messages`, `delete_messages`, `set_flag`, `hydrate_decoded`, `refresh_folders`, `folder_from_scope`) take an explicit `op: AccountOperation`. This lets recovery distinguish `Reconcile` from `Retry::SameRequest` for non-idempotent ops like `AddToContainer` and `DraftCreate`.

`Error::response_code()` returns the structured `ResponseCode` from `[CODE ...]` brackets (first only); recovery consumes those via the typed `ImapResponseCode` wire variants.

The scope readers inside `classify_response_code` (`resource_from_scope`, `id_from_scope`, `mailbox_throttle`) must each accept BOTH mailbox scope shapes. Folder producers build `ErrorScope::Cursor(Folder(id))` via `with_folder_scope`, not `ErrorScope::Mailbox { id }` - `with_mailbox` is reached only from tests. A reader matching just the latter degrades on every real folder-scoped failure while its mailbox-scoped test keeps passing, which is exactly how `ThrottleScope::Mailbox` became unreachable (widening per-mailbox `[LIMIT]` to account-wide pauses) and how the folder id was dropped from `RequestCause::NotFound`. Both fixed and pinned folder-side; keep new readers symmetric.

ManageSieve carries its own response-code vocabulary (RFC 5804 1.3), parsed into `SieveResponseCode` and surfaced as `Error::Sieve { code, message }` - deliberately NOT folded into the IMAP `ResponseCode`, since recording a code the server never sent would corrupt the wire evidence support exports exist to preserve. The code rides out as `native_code`. `split_response_code` lifts the parenthesized code off the status line BEFORE the human-readable string, which is the ordering RFC 5804 specifies and the step whose absence caused the original defect: the code stayed inside the message as opaque text, so every rejection reached `fallback_status` and derived terminal `ProviderRefused`.

`classify_sieve` overrides that default only where it is actively wrong: `TRYLATER` -> `Server(Unavailable)` (retryable - it explicitly asks to be retried, and was previously permanent), `QUOTA[/MAXSCRIPTS|/MAXSIZE]` -> `Server(QuotaExhausted)` + account throttle, `NONEXISTENT` -> `NotFound(Filter)`, `ALREADYEXISTS` -> `ConcurrencyConflict`, `ACTIVE` -> `Request(Malformed)` (a precondition the caller must satisfy), `AUTH-TOO-WEAK` / `ENCRYPT-NEEDED` -> `Authorization(PolicyBlocked)`, `TRANSITION-NEEDED` / `SASL` -> `Authentication(ReauthorizationRequired)`, `REFERRAL` -> `Unsupported(op)`. An absent, advisory (`WARNINGS`, `TAG`), or unmodelled extension code keeps the terminal default - "the server refused and did not say why" - and `Other(_)` preserves the raw spelling for diagnostics.

CHECKSCRIPT is the one path where a `NO` is normally not an error: it IS the validation verdict, so it becomes `FilterValidation` diagnostics. `validation_outcome` makes the transient codes the exception - a `TRYLATER` means the server never compiled the script, so reporting it as a validation failure would tell the user their script is broken when it may be fine, and would discard the retryable classification.

A `Transport(_)` kind must never carry an `Acknowledged` attempt - `try_build` rejects the pair (`TransportAcknowledged`) and this boundary `.expect`s, so producing it panics. `into_account_error` demotes it to `InFlight` rather than dropping the cause, since dropping would leave `derive` on its `Unsent` default and blind-retry a non-idempotent operation. Nothing builds the pair today, but `Error::with_attempt` permits it on the transport variants and the driver stamps `Acknowledged` after a tagged response.

`Error::No` and `Error::Bad` carry `attempt: Option<ImapAttempt>`; `no_with_code`/`bad_with_code` default it to `Some(Acknowledged)` because a tagged `NO`/`BAD` is a server-acknowledged terminal response. Without this, recovery rows keyed on `Acknowledged` (e.g. `Server(Error { status: None }) + Acknowledged -> ProviderRefused`) collapse to the `Unsent` arm and misclassify provider refusals as retryable drops.

`ImapErrorContext::with_extra_text` appends support-only diagnostics the call site knows and the wire does not, WITHOUT touching the classification. The destroy path uses it: a UID EXPUNGE that fails after its `UID STORE +FLAGS.SILENT \Deleted` was acknowledged leaves a confirmed partial effect (those messages stay flagged `\Deleted`), but the failure itself keeps its own kind and cause chain. Collapsing it into `Protocol(PartialResponse)` would flatten an auth loss, an ACL denial, a quota throttle and a capability loss into one generic reconcile - the exact classification `reference/error-model.md` requires the producer to preserve.

### Push lifecycle

The IDLE task is account-owned, not subscription-owned: `ensure_idle_task` spawns one for the account's lifetime, and `push_unsubscribe` never cancels it. With no subscribed scopes the loop parks on the `resubscribe` `Notify` (plus the cancel/shutdown tokens) instead of sleeping or exiting, so a subscribe/unsubscribe pair cannot race a cancelling task into two IDLE loops or none. `choose_idle_folder` sorts subscribed folder scopes by name before picking, so a `HashMap`/`HashSet` iteration order never decides which mailbox is watched, then falls back to INBOX.

A scope-set change cancels the in-flight IDLE round through a child token so the folder choice can be re-evaluated. `ImapConnection::idle` gives cancellation strict priority over queued server events, so the round returns `Cancelled` while events may still be queued. The DONE drain itself does not throw those away - `drain_idle_responses` emits them to the event sink and they survive into the next `idle()` round - but the loop breaks to re-choose a folder and redials, which is where they are lost. It therefore absorbs whatever `idle()` did return and then emits a coarse `HintPayload::Unknown` invalidation (`signal_idle_interrupt_loss`). Reconfiguring subscriptions degrades to a broader reconcile, never to silent push loss for the scopes that stayed.

The DONE handshake carries the account's `command_timeout`, the same bound every other command has. A peer whose TCP is alive but which never answers the tagged OK for IDLE would otherwise park the push loop past cancellation, past `account.shutdown`, and past `close()` - the IDLE connection is dialed via `dial_idle` and sits outside the pool's idle list, so `Pool::close`'s bounded drain never reaches it. The bound turns that into an `Error::Timeout`, a `Disconnected` event, and a redial.

A failed dial or SELECT backs off from 5s, doubling to a 300s ceiling, and resets on the first successful SELECT. The wait races the cancel and shutdown tokens rather than sleeping through them, and `Disconnected` is emitted once per disconnection rather than once per failed attempt.

### Concurrency conflicts and UNCHANGEDSINCE

IMAP diverges from the `MutationSuccess::Skipped` lane other crates use for the engine's "already in this state" short-circuit: the opportunistic MODSEQ cache sends cold-cache STOREs without `UNCHANGEDSINCE`, so "already in state" cannot be observed without a full SELECT+FETCH that defeats the guard. So `STORE UNCHANGEDSINCE <modseq>` rejecting with `MODIFIED` surfaces as `ItemOutcome::Failed { kind: ConcurrencyConflict }` for the conflicting UIDs, never `Succeeded(Skipped)`. `concurrency_conflict_error` / `store_failed_error` / `uidvalidity_changed_error` each take the caller's `AccountOperation` so flag/move/destroy paths emit their own op (recovery picks `Retry::AfterStateRefresh` regardless, but the op tag drives telemetry and per-op retry budgets).

### Strategy downgrade derivation

`EngineDirective::DowngradeStrategy` is reserved for "all strategies exhausted", which IMAP never derives at runtime: `changes.rs` handles every QRESYNC -> CONDSTORE -> Basic downgrade inline via `Warning::StrategyDowngraded` plus a direct retry on the lower strategy. The `strategy_failure` helper is funnel-wired and tested so the directive is producible, but no runtime path reaches it while Basic is the universal fallback.

### Per-folder mutation failure contract

`mutate::mutation_stream` emits no trailing global `SyncEvent::Terminated` when a folder fails mid-batch: a per-folder fatal after per-item emissions surfaces as `ItemOutcome::Uncertain` for every remaining target in that folder (carrying the classified `AccountError`), and the loop continues to the next folder. Stream-level `Terminated` is reserved for failures that prevent any further folder attempt (auth lost, schema/capability break); `stream_terminating` is the gate.

### Output-channel-dropped contract

Every streaming task (`inventory_stream`, `changes_stream`, `get_stream`, `open_raw_rfc822`, `mutation_stream`) treats a `tx.send` failure on a dropped output receiver as silent termination: the task returns without synthesizing any `crate::Error` or fatal `Terminated`. The error funnel is reserved for wire failures and structural invariant breaks; a consumer walking away from its stream is not an error.

### Terminated-event helper

`account/mod.rs` exposes one `terminated_event::<T, _>(cause)` helper for surfacing fatal stream causes as `SyncEvent::Terminated`. It accepts any `Into<TerminatedCause>`: a pre-built `AccountError` (UIDVALIDITY change, modseq reset, pre-classified failures - including the shared-folder `ScopeRevoked`), or an `(Error, ImapErrorContext)` pair to classify on the way out (the `fatal_event(err, ctx)` alias). Callers do not choose a lane.

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
|   |-- blob.rs            - open_raw_rfc822 (BODY.PEEK[]); no blob openers
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
- Public enums/structs are limited to the factory/config surface and are `#[non_exhaustive]` absent a strong reason.
- Credentials and SASL intermediate strings use `Zeroizing` and redact under `Debug`; malformed SASL mechanism names are rejected before any wire write.
