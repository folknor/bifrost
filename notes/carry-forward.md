# Orchestration carry-forward

Machinery established by bug-hunt arcs that later rounds may build on and must
not break. This is loop state: no agent in the loop can see any round but its
own, so every brief carries the relevant slice of this forward.

Keep this current as of the last close pass. It records invariants and couplings,
not history - when something here stops being true, change it rather than
appending to it.

## From the bifrost-jmap arc (closed 2026-08-22, `e866377..` five commits plus close pass)

- **The email inventory walks are ONE loop, and its zero-entries contract is
  load-bearing on both sides.** `inventory.rs::email_inventory_loop` is the
  primary full walk, the foreign walk, and the bounded `Page` walk, chosen by
  filter/owner/window parameters; the owner parameter is the only behavioral
  difference (foreign error routing via `shared_scope_error`, foreign id and
  membership qualification). The contract: a partition yields zero entries
  ONLY when the scope has no results past `from` - the loop advances by ids
  CONSUMED from `Email/query`, and a bounded window that fills without
  emitting anything (ids deleted between query and get, id-less objects
  dropped) keeps walking past the window until it emits or the query runs
  dry. The engine side holds up the other half: `bifrost-sync`'s live
  `OpenPages` walker stops only on `seen == 0`, and `open_pages_resume`
  treats ONLY the completion marker as exhaustion - a short acked page
  resumes at `to`, never skips. Do not reintroduce a short-page-means-done
  inference on either side; the resume half of exactly that inference
  survived one round and was removed by the close pass. The exhaustion
  signal is still an inferred count, not a declared flag - `TODO.md` carries
  that cross-crate writeup.
- **Foreign probing at open is the crate's only overlapping-request site.**
  `foreign_probe_concurrency` bounds the `buffer_unordered` by the server's
  `maxConcurrentRequests` clamped `[1, 8]`, serial when the core capability
  is unreadable; results sort by accountId before installation so topology
  and skip ordering stay deterministic. A new call site that issues
  concurrent requests must solve the same problem again (no bifrost-net
  governor exists; `TODO.md`).
- **The scope-lifecycle poller owns a private `lifecycle_mailbox_states`
  map.** Nothing else may write it: the shared `mailbox_states` cache is
  fast-forwarded by delta passes and local `Mailbox/set`, which silently eats
  the poller's unread window. It is open-time process state, not a cursor -
  it must not enter the cursor envelope. `state_cache::advance` is an exact
  CAS: an expected state never initializes an absent or empty entry.
- **`sessionState` divergence ends in a reopen, through the lifecycle
  stream.** The always-driven lifecycle stream checks
  `client.is_session_updated()` at the top of each poll and terminates with
  `SyncState(CapabilityChanged)` -> `RestartAccount`. An in-place
  `refresh_session` cannot heal a live account (limits, capabilities,
  routing, push topology are frozen from the old session document); the poll
  pacing is what bounds the reopen rate against a churning server.
- **The push reader's health evidence is traffic, nothing else.**
  `reset_backoff` requires a frame the peer actually served (a pong counts);
  reaching the read loop is not evidence, because a push-enable rejection
  arrives as an asynchronous `RequestError` on that stream - resetting on
  loop entry is a 1 Hz handshake storm. The read loop pings after
  `ReconnectPolicy::keepalive` of silence and drops the connection if the
  pong misses `connect_timeout`. `WsState` retains the reader `JoinHandle`;
  `close()` bounds both the push-disable write and the join by
  `connect_timeout`, abandoning (not aborting) a reader that outlives it.
  The reader talks through the `PushTransport` seam so all of this is
  pinned in-process.
- **Empty flag ops ride `mutation_stream` as `MutationKind::SkipFlags`.**
  One batching engine: owner routing, batch bound, tail flush, `Done`. Do
  not grow a second stream for a "simple" case; the last one missed owner
  routing.
- **The PIM mapping rejects what it cannot represent - ruled on, not open.**
  Unknown attendee roles, participation statuses, address-component kinds
  (including RFC-defined kinds the shared types cannot hold), non-simple
  recurrence overrides, multiple or excluded recurrence rules, and RRULE
  parts outside FREQ/INTERVAL/COUNT/UNTIL/BYDAY/BYMONTH/BYMONTHDAY all
  return `Unsupported` instead of a plausible partial answer. The payload
  builders reject a failed RRULE conversion THEMSELVES (never clear or omit
  the recurrence), so the refusal does not depend on
  `validate_shared_recurrence` call order. Widening the recurrence mapping
  is tracked in `reference/jmap/DEFERRED.md`, not a bug.
- **Calendar conversions read the current event when the patch cannot supply
  context.** `event_patch_needs_current` gates one `CalendarEvent/get` in
  `update`: UNTIL conversion and start/duration writes need the start
  timezone and all-day flag, and an attendee patch needs the current owner
  participants because JSCalendar keeps the organizer in the same
  `participants` map a whole-list attendee write replaces
  (`merge_owner_participants` carries owners over, updating a matching
  attendee in place). Omitting `is_all_day` in a patch means "unchanged".
  All-day starts are midnight `LocalDateTime` + `showWithoutTime` on the
  wire (JSCalendar has no DATE type); the shared layer sees bare dates with
  exclusive all-day ends.
- **An object without a usable id is not an item, anywhere.** Email and
  mailbox inventory, hydration, container discovery, and contact-card
  reconciliation all drop the shape rather than minting `ObjectId("")` /
  `ContactId("")`; the submitted id then travels the failed/partial lane
  under the closed per-item accounting.
- **Search pins `receivedAt desc`** (page cursors are integer positions into
  the query order, and RFC 8621 gives an unsorted query no stable order),
  and the `SearchFilter` non-exhaustive catch-all is `Unsupported(Search)`,
  never an empty `AND`.
- Accepted residuals, on the record: the audit was static (no live server);
  `event_from_jmap` fails the whole page when one event is unrepresentable
  (a `BatchOutcome` shaping question for bifrost-sync); the inventory
  overshoot is unbounded in principle (ends at the first surviving message
  in practice); contacts readers outside addresses/titles still skip
  unparseable values. `notes/bugs-jmap.md` records these; do not re-open
  them as findings without new evidence.

## From the bifrost-graph arc (closed 2026-08-22, `f0d7d99` and `b460cdb`)

These are cross-crate. They bind every protocol crate, not just `bifrost-graph`.

- **`push_subscribe` answers per scope, not per request.** It returns
  `PushSubscription` (in `crates/types/src/account.rs`), which carries an
  optional handle plus a validated per-scope `BatchOutcome` over the shared
  three-lane model in `reference/error-model.md`. `Err` is reserved for genuine
  whole-request faults: an empty scope list, no webhook endpoint configured,
  nothing subscribable at all, or a create failure that was rolled back. A single
  refused scope must never fail its siblings, and that applies to every bail path,
  including ones that run before any translation or resolution step. The ids in
  the outcome are submission positions.

  Non-Graph implementors were audited, not assumed: caldav, carddav and the IMAP
  `StubAccount` refuse push outright; google (`users.watch` is per mailbox) and
  jmap (per-account PushSubscription) genuinely cover the entire requested list
  when they return `Ok`, so `all_succeeded` is correct for each. **imap no
  longer does** - see the IDLE budget below.

- **`Account::is_inventory_cursor` must share ONE condition with
  `Account::inventory_resume_stream`.** A predicate that merely agrees with the
  hook today can drift from it, and that drift strands a scope with neither a live
  cursor nor a recovery path - it was a live defect in round 2, caught in review.
  Graph derives both from `resumable_inventory_payload`. Overriding either means
  overriding both from a single condition; the trait doc says so.

- **Constructing an `Account` change/inventory stream must be I/O-free and
  side-effect-free.** `crates/sync/src/engine.rs` no longer builds and drops
  streams to classify a cursor (that is what `is_inventory_cursor` replaced), but
  the contract stands for implementors.

- **`CursorError::InventoryInProgress` maps to `CursorInvalid`, not
  `SchemaIncompatible`.** A mid-inventory cursor arriving where a changes cursor
  belongs is well-formed and misrouted, so the recovery restarts that scope; it
  must not clear the account's whole stored schema.

- **One worker-slot discipline, shared by both push modes.**
  `crates/graph/src/account/worker_slot.rs` owns `ensure_worker` /
  `retire_worker_slot` / `take_worker` and pins the lock ordering once:
  registration writes state under the registration lock, drops it, then ensures;
  an exiting worker retires its slot while STILL holding the registration guard
  that decided to exit. Two divergent worker lifecycles guarding the same shape
  produced two separate bugs before this existed. Do not fork it back apart.

- **The EWS frame decoder matches the LOCAL XML name**
  (`GetStreamingEventsResponseMessage`), never a literal namespace prefix.
  Prefixes are aliases; matching `<m:` silently discarded valid responses and
  disabled push while still advertising `push_in_process()`.

- **Typed protocol errors survive the streaming path.** `EwsError` is preserved
  through the frame consumer so a terminal recovery yields
  `StreamLoopExit::Terminated`. Stringifying it turned access-denied into an
  endless reconnect loop.

- Cursor envelope is at **v4**.

## From the bifrost-imap arc (round 2, 2026-08-22)

- **IMAP push coverage is bounded and partial.** Without RFC 5465 NOTIFY the
  account runs at most `ImapAccountConfig::idle_connection_budget` (default 4)
  dedicated IDLE sessions, one per distinct pushed folder. Folders past the
  budget come back in `PushSubscription.outcomes.failed` as
  `Unsupported(PushSubscribe)`, distinguished from a non-folder scope only by
  the diagnostic text. This is the first `Account` impl whose `push_subscribe`
  legitimately partially fails, so it is the live test of the per-scope
  contract above: `Err` stays reserved for whole-request faults, a refusal
  never fails a sibling, and a subscribe that accepts nothing returns no
  handle at all. Admission accepts exactly what worker assignment can watch:
  a folder name `MailboxName::new` rejects goes to the failed lane, since the
  assignment side (`subscribed_idle_folders`) silently drops such names.

  The coupling that makes this safe is that **bifrost-sync never suppresses
  polling on push coverage.** `Engine::subscribe_push` records only
  `outcomes.succeeded()` in the subscription registry, and that registry is
  teardown bookkeeping only - no scheduler path consults it to skip a poll.
  A refused folder therefore stays polled rather than becoming invisible. Any
  future change that lets push coverage relax polling must first make the
  uncovered-scope lane explicit, or it silently reintroduces that hole.

- **`Pool::close()` means no session of that account is still connected when
  it returns.** Every session the pool mints is registered weakly; `close`
  gates the pool, closes the permit semaphore, drains the registry, requests
  LOGOUT on all of them concurrently under one `command_timeout`, then aborts
  any surviving driver and drops its transport. This holds for a checkout
  whose command is still in flight - it is terminated, not waited for - and
  the promise is about the transport, not about a graceful LOGOUT, which is
  best effort. The `sessions` mutex is the linearization point: `close` stores
  the flag before draining, registration re-reads it while holding the lock,
  so a dial or a permit acquisition that completes after `close` refuses
  rather than landing a live session past the drain. Registration also prunes
  dead weak entries, because a reconnecting IDLE worker would otherwise grow
  the registry by one entry per dial for the life of the account.

- **Multi-worker wakeups must latch.** The IDLE workers are woken by a
  `watch` generation counter, not a `Notify`: `notify_one` wakes one of N
  workers and leaves the rest on a stale assignment, and `notify_waiters`
  stores nothing, so a worker between deciding it has no folder and awaiting
  the wake parks forever. Anything that fans out to several workers here needs
  the same latching property. The seen-mark lives at exactly ONE site, before
  `choose_idle_folder` reads the scope set; a second mark later in the round
  discards bumps latched during the dial/SELECT/`NOTIFY SET` awaits and leaves
  the worker on a stale assignment for a full `idle_timeout` - the close pass
  removed exactly such a mark.

- **Get and mutation streams flush at `TARGET_BUFFER_ITEMS` (256) decoded
  targets.** Output ordering is therefore input-window order, then lexical
  folder order within a window; it used to be lexical folder order over the
  whole input. bifrost-sync tolerates this because it keys results by
  `ObjectId` and `BatchOutcome` carries its own submission-order index. A
  consumer that starts depending on cross-window ordering breaks this.

## From the bifrost-net arc (round 1, 2026-08-22)

These bind every protocol crate, because `bifrost-net` sits under all of them.

- **`AccountSpec::token_source` is optional.** `None` means the account
  does not use bearer auth (Basic-auth DAV is the motivating case) and its
  requests must opt out with `without_bearer_auth()`. Leaving bearer auth on
  with no source fails LOCALLY, before dispatch, as `Error::InvalidRequest {
  field: "bearer_auth" }` -> `Request(Malformed)` -> `RecoveryClass::ClientBug`.
  No path may send an unauthenticated request in place of an authenticated
  one; that is a configuration bug, not a transient condition.

- **The refresh backoff bounds the issuer call RATE, not the failure count.**
  A no-fallback refresh failure parks the refresher in `Backoff`, and every
  call during the interval fails locally off the shared cached error.
  Transient failures escalate 1s -> 60s by doubling; an authoritative issuer
  refusal (401/403 from the token endpoint, or `AuthLost`) takes 60s
  immediately; any success resets. Forced refreshes obey the same interval -
  the state decides, not the caller. Do not re-document this as "prevents one
  refresh call per request": that overclaim is what this round replaced.

- **Rate-governor tickets name a bucket INSTANCE, not a host.** Admission is
  FIFO, each waiter holds its own `Notify`, and every `HostBucket` carries a
  `generation` while `next_waiter_id` restarts at zero per instance. A waiter
  whose generation no longer matches the bucket under its host, or whose
  ticket is gone from that queue, completes as UNMETERED; the cancellation
  guard mutates only its own generation. Matching on the host name alone
  stranded a waiter forever when another account re-registered the same host
  in the window between the final `unregister` waking it and it resuming -
  ordinary detach/open churn. Anything added to this queue must keep the
  generation check, or the strand comes back.

- **`status_line_code` reads the status POSITION.** The first token, or the
  token after an `HTTP/` version (case-insensitive), and only in `100..=599`.
  It never scans prose for a later number. Both DAV crates consume it, and
  the caldav `propstat_success.unwrap_or(true)` hole stays closed because
  unparseable is still `Some(false)` while ABSENT stays `None` into success.
  Tightening the parser moves inputs into the unreadable bucket, never out of
  it, so that hole cannot reopen from this direction.

## From the bifrost-net / bifrost-types arc (2026-08-22)

`crates/net` and `crates/types` sit under every protocol crate, so all of these
bind the whole workspace.

- **`NetConfig` is process-wide ONLY; per-account settings live on
  `AccountSpec`.** The shared half owns the reqwest client: pool, keepalive and
  TLS trust. The per-account half owns request/connect/read timeouts, User-Agent,
  redirect policy, max buffered response, and token max-age. A protocol crate
  that calls `Net::new` per account silently un-shares the connection pool, the
  governor and the meter, which is exactly the defect this split fixed for JMAP.
  Attach to a shared `Net` unless you genuinely own the transport.

- **There is no client-level read timeout any more.** It is per-account
  (`AccountSpec::read_timeout`). Anything that drains a response body must go
  through `read_capped_response_body` with `account.read_timeout()`, never
  `response.bytes()`. Two terminal-status drains were left on `bytes()` by the
  split and would have blocked forever against a server that sends headers and
  then stalls; google and graph accounts have no total request deadline to
  rescue them. The capped reader also bounds memory, which `bytes()` does not.

- **TLS trust selects WHICH shared client an account attaches to.** It cannot be
  per-account, because it is a property of the reqwest client. Use
  `Net::shared_for_tls(accept_invalid_certs)`. Never accept a caller's TLS
  choice and discard it: JMAP did, so a self-signed deployment failed every HTTP
  request while its WebSocket - which builds its own connector - succeeded, and
  the option appeared to work and not.

- **Redirects are followed inside bifrost-net, not by reqwest.**
  `Policy::none()` is installed at client construction and that is the only
  handoff point. reqwest's own policy cannot rewrite methods per RFC 7231, strip
  `Authorization` across a host boundary, or carry a per-account trusted-host
  allowlist - and a shared client could not carry the allowlist regardless.

- **The rate governor is FIFO with generation-scoped tickets.** Every
  `HostBucket` carries a generation; a ticket names a bucket INSTANCE, not a
  host name. A waiter whose generation no longer matches, or whose ticket has
  left the queue it joined, completes UNMETERED rather than parking. Burst
  validation and enqueue happen under one lock. Two separate permanent-strand
  bugs came out of getting this wrong, both during account detach/open churn.
  `WaiterGuard::drop` is generation-scoped for the same reason, because
  `next_waiter_id` restarts at zero per bucket.

- **Token refresh backoff on the no-fallback path** escalates 1s to 60s by
  doubling, resets on any success, and goes straight to the 60s floor on an
  authoritative refusal (401/403 from the token endpoint, or `AuthLost`). This
  bounds the ISSUER CALL RATE, not the number of failing requests - state it
  that way, since the doc previously overclaimed here.

- **The 94-method `Account` trait stays unified.** Audited lane by lane: a split
  is object-safe but buys impl and accessor churn without removing a single
  dependency. The grouping evidence is in `reference/types.md`. Do not reopen
  this without new evidence.

## From the bifrost-smtp / bifrost-sasl arc (closed 2026-08-22, `cfd9f46` + `0613bde` + `dd92738` + `d20816c` + `488ffd7` plus close pass)

Decisions already ruled on - do not silently relitigate:

- **The crate is async-only, permanently.** The blocking transport half
  (`SmtpTransport`, `LmtpTransport`, `Transport`, the blocking pool and
  socket funnel, `oauth2_token_blocking`) and the `tokio` cargo feature are
  deleted, not gated. A build without Tokio has no transport, so a feature
  gating one described a build that cannot exist. The fifteen invariants only
  the blocking tests had pinned were re-pinned as async transcript tests; a
  restoration of the blocking half would have to re-earn all of that.
  `TODO.md` carries the release-note obligation.
- **SMTP has NO auth retry ladder, deliberately, and IMAP keeps one,
  deliberately.** SMTP's `password_mechanism` returns exactly one mechanism
  and a wire 535 is final: walking down to an unbound mechanism or PLAIN
  after a rejection is the silent-downgrade class this arc spent rounds
  refusing. IMAP's `password_mechanism_ladder` returns an ordered
  `Attempt`/`Reject` list, but its driver walks past LOCAL rejections only
  (policy gates, `ChannelBindingUnavailable`, capability skew surfacing as
  `MissingCapability`); any wire rejection aborts the ladder. Collapsing the
  two shapes into one, in either direction, reopens a downgrade.
- **DATA reuses the message's final CRLF.** The RFC 5321 terminator's leading
  CRLF is the message's own final CRLF; the writer appends one only when the
  buffer does not end in CRLF (chunked writers track the last two bytes
  across iterator items), and `smtp_data_size` matches the transmitted bytes
  exactly. `Message::formatted()` is byte-identical to the delivered data
  section for CRLF-terminated messages. `body_raw()` still appends an
  unconditional CRLF for DKIM; that is verified-safe because RFC 6376 simple
  and relaxed body canonicalization both strip trailing empty lines, so
  signing and delivery agree in every terminator case. Restoring the
  unconditional `\r\n.\r\n` re-adds a phantom empty line to every delivery
  and breaks the SIZE arithmetic.

Machinery a future arc may build on and must not break:

- **Channel-binding policy is one rule in two crates: absence skips, presence
  binds or hard-errors.** The pure seams (`resolve_scram_binding` in SMTP,
  `scram_binding_from_der` in IMAP) both feed
  `bifrost_sasl::tls_server_end_point`. Only a MISSING peer certificate may
  skip the PLUS rung; a certificate that is present but unusable (EdDSA leaf,
  unknown OID, malformed DER, ambiguous RSASSA-PSS params) is a hard typed
  error that must never fall through to unbound SCRAM or PLAIN. RFC 5802
  Section 6 additionally drops the unbound SCRAM-SHA-N rung whenever
  SCRAM-SHA-N-PLUS is advertised. The two crates must not diverge here; they
  did once (an `.ok()?` in each), and the prose got ahead of the IMAP code
  for a round.
- **The connection-level binding gate is pinned through a test seam, not
  review.** The transcript harness has no TLS, so `AsyncNetworkStream` has a
  test-only injected peer-certificate DER
  (`from_transcript_with_peer_certificate`). Three transcript tests pin the
  `plus_candidate` gate in both directions; before the seam, neutering the
  gate to `false` left all 393 crate tests green while answering a
  PLUS-advertising server with `AUTH PLAIN`. Keep the seam; it is the only
  hermetic eye on those few lines of plumbing.
- **Envelope commands are built before the transaction opens.**
  `build_transaction_commands` / `build_recipient_commands` construct and
  validate the whole `Mail` + `Rcpt` set before `MAIL FROM` is written, on
  every send path. This is a connection-state invariant: a validation error
  raised mid-transaction unwinds through `?` past the `try_smtp!` abort,
  leaves the connection `Ok` inside an open transaction, and poisons the
  pool. Any new validation on an envelope value must run in these builders,
  not at the write site. (`build_recipient_commands` zips addresses with the
  options slice; every call site builds both from the same recipient list,
  which is what keeps the zip lossless.)
- **bifrost-sasl is strict on both wire boundaries and zeroizes key
  material.** Duplicate or malformed SCRAM attributes are protocol errors
  even for keys the client never reads; server-supplied `i=` is bounded to
  [4096, 100,000,000] (the floor is downgrade defence - `i=1` makes the
  captured proof brute-forceable offline); the server nonce must strictly
  extend the client nonce; SASLprep (RFC 4013, via `stringprep`) runs on
  SCRAM usernames (`prepare_scram_username`, the only public path to an `n=`
  value - the bare escaper is crate-private on purpose) and independently on
  passwords before PBKDF2; verifier comparison is `subtle::ConstantTimeEq`;
  salted keys, client/stored/server keys, proofs and the CRAM-MD5 buffer are
  `Zeroizing`. `hmac_digest` / `xor_bytes` return `Zeroizing<Vec<u8>>` so
  the next intermediate cannot be missed.
- **AUTH payload hygiene at the SMTP boundary.** PLAIN refuses NUL in
  username or password (RFC 4616: NUL is the field separator, so a NUL in
  the username is an authzid injection). `HeaderName` enforces RFC 5322
  `ftext` on both constructors (header-name CRLF injection). LOGIN answers
  challenges by POSITION (`challenge_index`), never by prompt-text matching.
  AUTH command buffers are `Zeroizing` through the socket write.
- **`MAIL FROM` / `RCPT TO` reject control characters at command
  construction**, which is the wire-boundary defence for
  `Address::new_dangerous` values; VRFY/EXPN share the same single-line
  check.

Accepted residuals, on the record (all disclosed in `TODO.md`): the
`account-error` feature gates nothing and `--no-default-features` alone does
not build (the gate to run per commit is
`brokkr check -p bifrost-smtp --no-default-features --features account-error`);
`AsyncLmtpTransportBuilder` has no `bandwidth_metering` (LMTP is local
delivery); the async-only deletion and the DATA byte change need release
notes.

## Standing lessons this project has paid for

- **Audit new tests for bite, mechanically.** Revert the production change,
  confirm the test fails, restore. Three tests in this project have been caught
  passing against the bug they were written for - most recently an entire
  "exhaustive" alias-pair suite that passed against the pre-fix code, which also
  proved the finding it came from was never a defect.

  **Uniform inputs are how a concurrency test fails to bite.** A FIFO
  admission test whose waiters all had cost 1 passed against a governor with
  no queue at all, because single-threaded scheduling order alone reproduced
  the answer. It only bit once the head was made expensive and the follower
  cheap, so an overtake was observable. Same shape as the 500,000-element
  test that never produced a single-element result.

  **Check the name filter before believing a PASS.** `brokkr test -p X <NAME>`
  is a substring match; a filter that matches none of the tests you meant
  reports PASS. Two ablation runs in this arc were read as "the test does not
  bite" when the test had simply not run.

- **The second half of a fix-and-commit stage is never cold-reviewed.** That
  stage fixes review findings and commits in one step, so its own work ships
  unreviewed. The close pass looks hardest there, and in both arcs that had a
  close pass it found real defects in exactly that half.

- **Which arcs have had a close pass, and which have not.** `bugs-graph`,
  `bugs-imap`, `bugs-jmap`, and `bugs-smtp-sasl` were closed properly.
  `bugs-net-types` was NOT: both rounds ran,
  but the arc-level review never did. Anything later that leans on
  `bifrost-net` machinery from that arc should treat it as reviewed once, not
  twice. The gap is recorded in the document itself.

- **The recurring defect shape is a fix that opens a new hole one layer up.**
  Check what a fix does to its consumer, not only to the unit test in front of it.

- **A refactor that MOVES a field must move the tests that pinned it.** The
  `NetConfig` split deleted three tests along with the fields they described,
  which was locally correct and left three defaults silently unpinned. A falling
  test count after a refactor is the signal; chase it rather than accepting it.

- **A `review` prompt over roughly 8k characters is rejected by the permission
  layer.** Two ~10k briefs were denied outright; the same content trimmed to ~7k
  went through unchanged. Budget brief length accordingly - this bites hardest on
  exactly the large documents whose briefs most want to be long.

- **`git add -N` every untracked source file before the cold review.** The fix
  pass leaves new modules untracked, and the cold review reads the unstaged diff,
  so a round whose central deliverable is a NEW FILE gets reviewed with that file
  invisible. This bit the bifrost-imap round 1: the shared hydration module that
  was the entire point of the unification did not appear in `git diff` at all.
  Intent-to-add makes it visible without staging its content.

- **The fix pass sometimes verifies with a scoped `-p <crate>` run.** That is
  exactly why the orchestrator gate at stage 2 is unconditional: scoped runs miss
  cross-crate breakage, and feature unification makes them non-equivalent to the
  real thing.
