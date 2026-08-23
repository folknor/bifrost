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

- **The blocking transport half STAYS. This entry previously said the
  opposite and was wrong.** `d20816c` deleted `SmtpTransport`,
  `LmtpTransport`, `Transport`, the blocking pool and socket funnel,
  `oauth2_token_blocking`, the seven blocking examples, and the `tokio` cargo
  feature, on the reasoning that the only IN-WORKSPACE consumer is async. That
  reasoning does not hold for a library crate, whose consumers are by
  definition outside this workspace, and the removal was never the loop's call
  to make. All of it was restored in `e632ab9`, with the `488ffd7` DATA-framing
  and auth-ladder fixes mirrored into the restored blocking writers so the two
  halves do not diverge. The fifteen invariants that only the blocking tests
  had pinned are now covered on BOTH sides; that duplication is the intended
  end state, not debt to pay down. Do not re-delete this surface.
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

## From the bifrost-caldav / bifrost-carddav arc (closed 2026-08-23, `4f6e4cb` + `b7b86d6` + `5d4de38` + `ddc749f` plus close pass `0796b29`)

- **Multistatus hrefs resolve against the EFFECTIVE request URI, never `base_url`.** RFC 4918
  makes an href relative to the request URI, and the request URI is the POST-REDIRECT one:
  `DavResponse` carries `response.url()`, `DavBody` pairs it with the body out of
  `propfind_raw` / `report_raw` / `send_body_request`, and every parser takes it as its
  resolution base. No `resolve_href(&self.base_url, ...)` survives in either crate.
  `client.resolve_url` still uses `base_url` correctly - it runs id-to-URL on already-absolute
  native ids and short-circuits on the scheme guard. Reintroducing a base-relative resolution
  mints ids on the wrong host, which the credential allowlist then passes and which 404.
- **Both DAV cursor payloads are at v2, and v1 is rejected, not decoded.** A v1 payload is
  byte-identical in SHAPE to a v2 one, so a permissive decode hands back base-relative ids and
  the snapshot diff emits every object as a delete plus a create. The rejection is classified
  `SyncState(SchemaIncompatible)` and deliberately NOT a scope restart: only the
  schema-incompatible directive also deletes the backfill checkpoint, and without the re-walk
  the already-backfilled objects keep their pre-correction id spelling forever. Consumers take
  one full re-sync per DAV account on upgrade, once. Do not reclassify this to `RestartScope`;
  that makes the migration half-happen.
- **Common-deployment ids did not move, and that is pinned against a literal.** The
  pre-migration base was `base_url` with its trailing slash trimmed, and an absolute-path href
  resolves identically against that, against the collection URI, and against a post-redirect
  URI. The stability tests assert the literal id, so a regression cannot pass by changing both
  sides at once. This is the invariant that makes the v2 bump a clean migration rather than a
  silent id change; anything touching resolution must keep it.
- **Redirects: same-origin inside reqwest, cross-origin by hand.** reqwest's
  `remove_sensitive_headers` strips `Authorization`, `Cookie` and `Proxy-Authorization` on any
  scheme, host or effective-port change, and the redirect policy closure runs earlier with no
  header access - so a cross-origin hop followed INSIDE reqwest arrives unauthenticated and
  401s. The policy therefore follows same-origin hops only (exact scheme/host/effective-port,
  the credential gate's own origin definition), and a cross-origin 3xx surfaces to
  `send_raw_request` for hop-bounded manual re-dispatch with fresh `auth_headers` for the
  target. Method and body are preserved on 301/302/307/308; 303 is terminal. `auth_headers`
  refuses unadmitted origins, so a server-controlled `Location` can SPEND trust that
  authenticated discovery admitted but can never widen it. Moving the hop above the transport
  seam is also what makes the leak hermetically observable - the reason this beat simply
  refusing cross-origin hops.
- **Discovery stages origins; `open` finalizes them before the client is shared.** Both trust
  gates - credential and redirect - use the same exact-origin definition. A discovery transport
  failure FAILS THE OPEN rather than silently degrading a capability: CalDAV discovery is one
  principal lookup plus one multi-property PROPFIND (a test asserts the request count is
  exactly 2), and a blip no longer bakes `event_rsvp = false` into an immutable field for the
  account's lifetime.
- **The propstat state machine commits on success only, and EVERY property obeys it.** The
  collection marker stages with its propstat (`mark_collection`) and is promoted in
  `commit_propstat`; promoting it immediately discards a resource whose own properties
  succeeded but whose `resourcetype` block failed. `ResponseParts` is ONE `PropStat` struct
  cleared with `mem::take` in both crates - CalDAV's eleven hand-maintained `propstat_*` twins,
  reset in three places, were one forgotten field away from leaking a previous propstat's
  value. The `propstat_success.unwrap_or(true)` hole stays closed because an UNPARSEABLE status
  is still `Some(false)` while an ABSENT one stays `None` into success; `bifrost_net::status_line_code`
  reading the status POSITION is what keeps tightening the parser from reopening it.
- **Resource identification is not extension-based.** Listings accept extensionless resource
  names in both crates, and CardDAV requests `resourcetype` so collections are excluded from
  the success AND failure lanes. Extension-only gating silently dropped sync-collection
  deletion entries and wedged the cursor in a permanent no-observation state.
- **RSVP is non-atomic by nature, and says so.** Every failure path after the acknowledged
  outbox POST - including the local encoding steps between the POST and the PUT - is wrapped
  `Protocol(PartialResponse)` with `TransmissionState::Acknowledged`, so a consumer knows the
  organizer was already told. Three of the four paths were left unwrapped by the first pass.
- **These two crates are near-duplicates and DRIFT IS THE DEFECT.** Five separate bugs in this
  one arc were CalDAV/CardDAV divergence: `escape_xml` quoting, the immediate collection-marker
  promotion, the propstat twins, the phantom-collection asymmetry, and finally the close pass's
  own find - `as_fetched_vcard` missing the `is_collection` guard its CalDAV twin got in round 1,
  surfacing an echoed collection as a phantom card. Any fix to one crate's shared-shape code
  must be checked against the other. Where the asymmetry is real - CardDAV has no
  `sync-collection` path and discovers one property - it is design, not oversight.
- Accepted residuals, on the record: RSVP's non-atomicity itself; the one-time re-sync from the
  v2 bump; `changes_from_cursor`'s token-retention path having no direct test and the
  missing-`sync_token` warning being log-only; `bifrost-caldav` taking `tracing` without `std`,
  matching google and graph but not imap/sync/net/types (filed as a C2 in
  `notes/bugs-cross-cutting.md`). Do not reopen these as findings without new evidence.
- **Fenced, awaiting the repository owner, NOT closed:** the recurrence-override `EventId`
  contract (C1, `event_delete` on one instance destroys the whole series), the single-collection
  cursor scope (C1), the CardDAV phantom address book (C2), the silently-ignored CalDAV calendar
  move (C1), and the `bifrost-dav` collapse (C4). Each would remove, rename, or reshape
  published API or behavior. `notes/bugs-dav.md` holds them in full with their categories.

## From the bifrost-google arc (closed 2026-08-23, `10f4034` + `a060489` + `74bc3ca` plus close pass `d8eb9fe`)

- **Hydration must NEVER poll the id stream past the batch in hand.** A one-item lookahead to
  determine `PageBoundary::Final` deadlocks a backpressured producer: `get_stream` streams ids
  precisely so the producer can wait on output, so polling for id 33 before hydrating the first
  32 parks both sides forever. `get_stream` marks `Final` from a flag set INSIDE the drain loop -
  a batch cut short by a closed id stream is `Final`, a batch that fills exactly stays `Page`
  with `Done` as terminator. Pinned by a paused-clock stall test. The residual (a last batch that
  fills exactly is unmarked) is deliberate and the ledger carries an explicit
  "do not finish this with a lookahead" note; `bifrost-sync` reads `Final` in no hydration path.
- **Inventory absorbs the deletion race and nothing else.** A Gmail 404 for a message listed and
  then deleted before `users.messages.get` is routine on a large mailbox, and it used to
  terminate the whole backfill partition, discarding every page already emitted. Only a
  precisely classified `NotFound(Message)` is absorbed; every other failure still terminates.
  There is deliberately NO per-item failed lane here: `Account::inventory_stream` returns
  `AccountStream<SyncEvent<InventoryEntry>>` with no `ItemOutcome` wrapper, so adding one is a
  published trait change and is fenced for the owner.
- **Quota is a dated table consulted through one lookup, never a hardcoded cost.** Gmail bills in
  quota units, not requests; the old calibration was 250 requests/s at `cost_default: 1` while
  the capability surface advertised a `QuotaUnits` model the transport did not implement. The
  bucket is now 100 units/s with real per-method costs (source: published Gmail usage limits,
  May 2026 revision, rechecked August 2026 - the note in `gmail_quota_cost` carries the
  provenance, and the instruction is to re-read the table rather than re-derive from 429s). The
  catch-all is 20 units, NOT 1: a cheap default was the original defect, so genuinely cheap
  endpoints are listed explicitly instead. `messages.send` and `users.watch` sit exactly at the
  burst ceiling, so those constants must move together - a test pins that no method costs more
  than the burst. Non-Gmail hosts (Calendar, Drive, People) keep `cost_default: 1` in the
  smaller bucket, so per-request billing is right for them. Both build sites consult the lookup.
- **`close()` is cancellation-safe at every await, and the guard is built before the future.**
  `closed` and `shutdown.cancel()` happen synchronously before the future exists, so the renewer
  and the push/lifecycle streams retire whatever the caller does with it; the bifrost-net detach
  lives in a `DetachOnDrop` guard CONSTRUCTED BEFORE `Box::pin` and moved in, because a guard
  built inside the async block is never built at all if the future is dropped before its first
  poll - and with `closed` already set, `Drop` skips the detach too, leaking the rate-limiter
  registration unreclaimably. That was the close pass's find, and it is the same shape as the
  bug the guard was added to fix. Only `users.stop` remains inside the future, since it needs
  the transport the detach sheds.
- **The watch renewer cannot outlive teardown.** It rechecks `shutdown` under the lifecycle
  mutex, because it could otherwise win its `select!` just before close cancelled the token and
  re-issue `users.watch`, resurrecting delivery for another seven days. `clear_watch_state`
  aborts it while holding that mutex. Push handles persist beyond the in-memory set, so
  `push_unsubscribe` after a restart is no longer a silent no-op, and a failed stop on the `Last`
  path restores the handle rather than leaving an empty set beside a live renewer.
- **An unparseable 403 is not evidence about delete scope.** `is_batch_delete_scope_failure`
  returning `true` on a body it could not parse meant a 403 from a proxy or policy layer
  silently downgraded a permanent delete into a trash. It now follows ordinary classified
  failure handling. The remaining half - that a SUCCESSFUL permission fallback still reports
  `MutationSuccess::Applied` for a message that was only trashed, which is a permanent engine
  reconcile loop - is fenced for the owner, since the remedy adds a published variant.
- **A reclassification carries the evidence the consumer needs to act.** A cross-calendar move
  whose follow-up PATCH fails returns `Protocol(PartialResponse)` scoped to the event in its
  DESTINATION calendar (the caller's `source::` composite id no longer addresses it), copying
  provider, protocol, HTTP status, request and trace ids, native code, both diagnostic tiers,
  and the whole original cause chain as secondary evidence, with `Attempt(Acknowledged)` pushed
  first so `derive` reads it. `into_builder` is decoration-only and exposes no kind-changing
  path, so a fresh builder plus hand-copying is the contract-correct route, not a shortcut.
  Telling a consumer to reconcile without naming what to reconcile is barely better than silence.
- **Every unbounded loop against a remote gets a budget.** The Drive chunk loop was
  `while offset < total` with no attempt counter and could spin forever; it now rejects stalled,
  backward and impossible resume offsets under a finite attempt budget. `calendars_list`
  paginates with a repeated-token guard AND a page budget, because a server handing out a fresh
  token every page slips the repeated-token check and only the budget stops it.
- Accepted residuals, on the record: the per-poll `users.getProfile` call is KEPT deliberately -
  it stops a rotated token pointing at a different Google account from mixing data into the
  existing slot, so do not re-file it as free savings; the cross-calendar move stays non-atomic
  with the classification making it legible; `calendars_list` returns a `Vec` with no streaming
  (filed C4, published surface). `notes/bugs-google.md` records these across three dated
  "Residuals from round N" sections, deliberately kept separate rather than consolidated.
- **Fenced, awaiting the repository owner, NOT closed:** the `bulk_destroy` distinct-outcome half
  (published `MutationSuccess` variant) and the Calendar endpoint override (published
  `with_calendar_api_base` constructor). Both additive rather than removals.

## From the bifrost-sync arc (closed 2026-08-23, `54290b2` plus close pass)

- **Reattach's abort discipline: never write what an abort would have to restore.** Freshly
  created cursors are staged in memory and persisted pre-teardown only when the scope had no
  stored row; abort compensation is plain deletion of rows the reattach itself created;
  vanished-scope rows are deleted only after the cutover commits, best-effort. A late ack
  re-persisting a deleted row is a leak a later rediscovery validly resumes from, never loss.
- **The close pass's find, same shape as the ledger's ten:** the committed code classified
  "preexisting vs freshly created" with a separate pre-check `get` that swallowed store errors
  (`.is_ok_and`), so a transient read failure demoted a preexisting prior-session row to
  "created" and the abort path deleted it - the exact P1 the redesign existed to eliminate,
  resurfacing through the error path of a duplicated read (plus a TOCTOU between the two
  reads). Fixed by having `run_establish` report the origin off its own single store read
  (`EstablishOrigin`); a store error now aborts the reattach before any durable write. Pinned
  by `transient_get_failure_cannot_demote_preexisting_cursor_to_created`, ablation-verified
  red against the pre-close-pass code. Lesson: a classification that feeds a destructive
  compensation must come from the same read the action uses, and must fail closed on error.
- A compare-and-swap primitive on `CheckpointStore` was considered and deliberately NOT taken
  (published trait); it remains a possible owner-level proposal, not loop work.

## Standing lessons this project has paid for

- **The loop does not get to delete public API. Ever. Ask the owner.** This is
  the most expensive mistake made so far, and it was made twice in one session:
  `d20816c` deleted bifrost-smtp's entire blocking transport half and
  `603d146` deleted bifrost-sync's `Scheduler`, `ConcurrencyBudget`,
  `SchedulerConfig`, `MutationConfig`, `LiveSupersedes`,
  `BackfillCheckpointWriter` and `mutation::fanout`. Both were restored
  (`e632ab9`, `7e7184d`) at the owner's instruction. Three things went wrong
  and each is worth naming separately.

  **"Nothing calls it" was established by grepping this workspace.** For a
  library crate that is close to meaningless: its consumers are outside the
  workspace by definition. "The only in-workspace consumer is async" is a fact
  about the workspace, not about who uses `SmtpTransport`. Likewise
  "documented as deliberately unwired, with no dated plan" describes somebody's
  plan; a missing date is not evidence of abandonment.

  **The bug documents mix four different kinds of finding under one heading
  level**: live defects, latent defects, refactor opinions, and product
  decisions about what the public surface should be. Only the first two are
  bugs. The loop's unit of work is "the document until it has zero open
  entries", and its standing rules push toward action - `build, don't defer`,
  and `closure by verification is not closure`, which explicitly rates "this is
  not actually a problem" as a weaker outcome than changing code. So an
  aesthetic judgment written into a `bugs-*.md` file gets laundered into a
  mandate. Before working any remaining document, separate the defects from the
  proposals.

  **Deletion was often not even the only fix on offer.**
  `MutationConfig::retry_queue_cap` reading as a bound while the queue is an
  unbounded `Vec` is a real defect - and "make the field actually cap the
  queue" is at least as good an answer as "delete the field". Where a finding
  proposes removing something, look for the fix that keeps it.

  The rule going forward: a change that removes or renames a published item
  stops and asks the repository owner, no matter which document recommends it,
  no matter how confident the argument. `build, don't defer` settles
  build-versus-defer. It does not settle delete-versus-keep, and that
  distinction was noticed at the time and overridden anyway.

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

- **Which arcs have had a close pass.** `bugs-graph`, `bugs-imap`,
  `bugs-jmap`, `bugs-smtp-sasl`, `bugs-net-types` (close pass run
  2026-08-22, after its two rounds), `bugs-dav` (close pass `0796b29`,
  2026-08-23, after four rounds), `bugs-google` (close pass `d8eb9fe`,
  2026-08-23, after three rounds), and `bugs-sync` (close pass run
  2026-08-23, after its single round `54290b2`) are all closed properly. The
  `bugs-net-types` close pass re-read the reconstructed `rate.rs` as new code
  and the un-cold-reviewed half of round 2, found no code defect, and fixed
  three doc-only drifts in `reference/net.md`; its residuals are recorded in
  `notes/bugs-net-types.md`.

- **The recurring defect shape is a fix that opens a new hole one layer up.**
  Check what a fix does to its consumer, not only to the unit test in front of it.

  This is now measured, not impressionistic. Across the dav and google arcs
  (2026-08-23) it fired ten times, and EVERY one was caught by the cold review
  or the close pass, never by the fix pass's own tests. The sharpest cases are
  worth naming because they rhyme: fixing a `PageBoundary::Final` contract gap
  introduced a hydration deadlock; fixing a push-watch leak made `close()`
  cancellation-unsafe, creating a different unreclaimable leak; and the guard
  added to fix THAT was constructed inside the async block, so a future dropped
  before its first poll leaked the same registration one poll earlier. A fix
  aimed at a resource leak has produced a new resource leak three times running.
  When the change is to teardown, ordering, or a lifetime, assume the hole moved
  rather than closed, and go looking for where.

  The corollary for the loop's structure: the cold review's value is entirely in
  its ignorance. It is the only stage that does not share the orchestrator's
  priors, and it has a perfect record on this defect shape. Every sentence of
  context added to that prompt is a prior installed in the one reviewer that
  should not have any.

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
