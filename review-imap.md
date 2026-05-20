# IMAP review - wave 2

Review of uncommitted IMAP changes in `crates/imap/` against `plans/imap-daaki-migration.md`. Five parallel reviewers across types/IDs/secrets, auth, config+profile+events, sync+ergonomic fetch+streaming fix, and error model.

Scope covered (per the user-stated highlights):

- Typed IDs and sets: `Uid`, `Seq`, `UidSet`, `SeqSet`, `UidValidity`, `ModSeq`, Gmail IDs.
- Public `SecretString`, `Credentials`, `AuthPolicy`, `AuthMechanism`, `authenticate_best()`.
- `ImapConfig` and `connect_authenticated()`.
- `ServerProfile` and `ImapConnection::server_profile()`.
- Sync helpers: `SyncSelectOptions`, `SyncFetchRequest`, `select_for_sync()`, `sync_fetch()`.
- Ergonomic fetch helpers: `uid_fetch_each()`, `uid_fetch_limited()`, `uid_fetch_full_messages()`.
- `TypedEvent::impact()` and `EventImpact`.
- `ErrorCategory`, `Recovery`, `Error::response_code()`, `Error::AuthPolicy`, `Error::FetchLimit`.
- Streaming FETCH fix: unbounded sender, buffered uid_fetch uses driver buffer path directly, limited fetch enforces budget in driver consumer.

## BLOCKERS (fix before commit)

1. **CRAM-MD5 leaks credentials over cleartext** - `connection/auth.rs:67-73`, default policy at `types/auth.rs:117`. PLAIN refuses without TLS, but CRAM-MD5 only checks `allow_cram_md5` (default `true`) - no TLS gate. A MITM picks the challenge and brute-forces `HMAC-MD5(password, challenge)` offline. Plan explicitly says CRAM-MD5 should be policy-gated like other cleartext. Add the TLS guard AND flip the default to false.

2. **Gmail message/thread IDs interchangeable** - `types/ids.rs:80-84`. `From<u64>` is impl'd for `GmailMessageId`, `GmailThreadId`, *and* `ModSeq`, so `GmailMessageId::from(thread_id.get())` silently compiles. Plan's stated goal is "prevent accidental mixing." Drop `From<u64>`; force explicit `::new(...)` constructors.

3. **Recovery returns the wrong answer on transient response codes** - `error.rs:282-296`, `error.rs:330-352`. `Unavailable`, `InUse`, `TempFail`, `Corruption`, `ExpungeIssued`, `Closed` all RFC 5530-transient, but `from_response_code` has no arms for them and they fall through to `ServerRejected` → `Recovery::DoNotRetry`. That's the literal opposite of correct. Plan asked for "stable policy hooks without string matching server text" - this is currently lying to callers. Also missing: `ContactAdmin` (Authorization), `AlreadyExists`/`NonExistent` (MailboxState), `TooBig`/`MetadataMaxSize` (Limit), `Referral`, `NotificationOverflow`. No `Recovery::Transient` / `Recovery::RetryAfter` variant exists at all.

4. **`ServerProfile::rev2_implies` is incomplete** - `types/profile.rs:116-129`. Lists only `Esearch, Idle, Move, Namespace, SaslIr, SearchRes, UidPlus, Unselect`. RFC 9051 also baselines: `LIST-EXTENDED`, `STATUS=SIZE`, `STATUS=DELETED`, `OBJECTID`, `SAVEDATE`, `BINARY`, `LITERAL+`/`LITERAL-`, `SPECIAL-USE`, `ENABLE`. A caller asking `profile.supports(Capability::ObjectId)` on an IMAP4rev2-only server gets `false` and silently skips RFC 8474 features.

5. **`ServerProfile` is a snapshot but documented as a live query** - `types/profile.rs` + `helpers.rs:36-39`. Built from `state_rx.borrow()` at call time. Clone/Eq derives invite caching. But capabilities mutate after STARTTLS / AUTH / ENABLE (RFC 9051 §6.1.2.1). `authenticate_best` happens to recompute it, but any user code that stashes a profile across those boundaries gets stale answers. Either return a guard tied to the watch channel, or doc-warn very loudly.

6. **`uid_fetch_full_messages` OOM footgun** - `connection/ergonomics.rs:135-146`. Calls buffered `uid_fetch` with no budget. A 100k-message mailbox pulled "for sync" allocates all of it in RAM. Plan flagged `uid_fetch_limited` as the safe path; this helper bypasses it entirely. Either take a budget arg or route through a streaming/limited path.

7. **`EventImpact` mapping has known bugs and zero tests** - `types/events.rs:65-75`. `ServerMetadataChange` mapped to `SelectedMailboxChanged` is wrong (server-level annotations, RFC 5464 - not selected-mailbox scoped). `Recent` mapped to `SelectedMailboxChanged` will trigger needless refreshes (RECENT is informational and deprecated in IMAP4rev2). No tests at all for the ~12 → ~11 dispatch table.

## CONCERNS

### Auth

- **XOAUTH2 has no TLS check** - `connection/auth.rs:31-39`. Bearer tokens over cleartext are exfiltratable. Apply `allow_cleartext_without_tls` equally.
- `profile.supports_auth(Login)` returns `!supports(LoginDisabled)` - but LOGIN-the-command and `AUTH=LOGIN`-the-SASL-mech are different. Rename or split (`profile.rs:62-70`).
- `Credentials` carries no `authzid` - locks out delegated/shared-mailbox flows (Dovecot master users). Document as deliberate or extend.
- `Error::AuthPolicy(String)` loses structure - the only producer (`auth.rs:87`) emits a generic string. Plan said carry mechanism + reason. This is the string-matching anti-pattern the plan said to avoid.
- Mechanism intersection diagnostic is lost on empty-set failure - no list of "tried X, refused Y because Z".
- `password.to_owned()` materializes a non-zeroizing intermediate `String` before re-wrapping in `SecretString` (`auth.rs:170, 337`). Original Zeroizing storage preserved at the destination, but the intermediate isn't.

### Types

- **No `#[repr(transparent)]`** on any typed ID - plan's stretch goal calls for it; without it the FFI/zero-cost guarantees aren't real.
- `UidSet::parse(&str)` / `SeqSet::parse(&str)` accept any string and bypass the typed-API discipline entirely (`ids.rs:148, 219`). Make `parse` `#[doc(hidden)]` or rename to `from_raw_unchecked`.
- `SecretString` derives `PartialEq` → `String`'s variable-time compare. Negligible client-side; flag if it ever appears in a credential-comparison hot path.
- `SecretString` `Deref<Target = str>` and `as_str()` give unredacted access - doc the policy (`format!("{}", &*secret)` is legal). Currently silent.
- `from_uids` / `from_seqs` silently sort+dedup; loses caller order (`ids.rs:135-145, 206-216`).

### Config & Profile

- `ImapConfig` mixes `#[non_exhaustive]` with public fields. Either private + builder, or public + non-non_exhaustive (`config.rs:7-22`).
- `with_keepalive(Option<TcpKeepalive>)` reads awkwardly; prefer `with_keepalive(...)` + `without_keepalive()`.
- `command_timeout` on `ImapConfig` only used during auth - misleading; either thread it as the per-op default or rename.
- `append_limit: Option<Option<u64>>` - `Some(None)` means "no limit", `None` means "no APPENDLIMIT advertised". Use a dedicated enum.
- `profile_tests.rs` is 21 LOC - IMAP4rev2 implications, APPENDLIMIT parsing, THREAD= parsing, ENABLE state all uncovered. Would have caught BLOCKER #4.
- No end-to-end test for `connect_authenticated`.

### Sync & streaming

- `uid_fetch_limited` skips `require_state(Selected)` - direct `submit_regular` bypasses the guard other fetch paths use (`ergonomics.rs:107-132`).
- Limited fetch is not actually memory-bounded - when over budget, the driver keeps reading until tagged OK; parser arena and TCP buffers continue to grow (`dispatch.rs:1488-1493`). Document this honestly.
- `uid_fetch_each` callback errors get masked by transport/timeout errors - `tokio::join!` ordering picks fetch_result first (`ergonomics.rs:91-104`).
- `SyncFetchRequest` doesn't validate QRESYNC was ENABLEd before allowing VANISHED. Server will reject; cleaner to fail client-side.
- `SyncQResyncOptions::known_uids` drops the `(known-uids known-seqnums)` second element (RFC 7162 §3.2.5).
- **The streaming-FETCH fix has zero tests for the actual loss scenario.** Add a test that pumps >256 responses through `uid_fetch_streaming` to a slow consumer and asserts no drops.

### Error model

- No `ErrorCategory::Connection` or `Tls` variant - `Closed`, `DriverGone`, `DriverPanicked`, TLS handshake errors all bucket as `Transport`. Defeats the "category enum" purpose.
- `Error::Bye { code: None }` falls into `ServerRejected` - should be `Transport`/`Reconnect`.
- `StartTlsUnavailable` mapped to `Capability`/`DoNotRetry` - that's a security policy outcome, not a feature gap. Separate bucket.
- `Error::FetchLimit` carries `estimated`/`limit` but no UID/seq position - can't resume.
- `response_code()` returns only the first code; RFC 9051 §7.1 permits multiple. Doc it or return `&[ResponseCode]`.
- `ErrorRepr` in `serde_support` not `#[non_exhaustive]` - adding an Error variant will silently break serde round-trip.
- `lib.rs` exports very wide for a pre-1.0 crate - `IntoSecretString`, Gmail IDs, sync request/result pairs each deserve a "is this stable?" decision.
- Zero tests for `category()` / `recovery()` / `response_code()` mappings. For policy hooks this stable, exhaustive coverage is the most valuable test.

## NITs

- `ids_tests.rs` is 27 LOC - missing `*`, `$`, single-element, max-value, parse round-trip, compile-fail "UidSet ≠ SeqSet" check.
- `AuthOutcome` is a one-field struct - inline to `AuthMechanism` until a second field arrives.
- `EventImpact::Extension` carries no payload; if drained via `drain_event_impacts` the raw event is gone.
- `FetchUpdate(Box<FetchResponse>)` clones to produce impact; consider `Arc` for high-volume IDLE.
- `Recovery` variants undocumented; `RetryOrReconnect` vs `Reconnect` is opaque.
- `tls_active: AtomicBool` uses `Relaxed`; cross-task readers will be racy (`lifecycle.rs:309-312`).

---

The CRAM-MD5 cleartext gap and the Recovery-on-transient bug are the two items most worth addressing first - both undermine claimed security/policy contracts. The IMAP4rev2 implication gap is a functional regression vs daaki. The OOM footgun in `uid_fetch_full_messages` is the easiest blocker to fix.
