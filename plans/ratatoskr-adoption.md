# Bifrost readiness for ratatoskr adoption

The bifrost-side governing plan for the ratatoskr migration. ratatoskr's
`plans/bifrost-migration.md` is the cross-repo source-of-record; its "Track A"
is the work bifrost must land before ratatoskr can adopt it. This document is
bifrost's local articulation of that track: the large bricks, grounded in the
current tree.

It is not a technical-implementation-spec. Each brick below becomes ONE spec,
verified against current code at spec time and run through the seven steps in
`reference/orchestrate.md`. This plan lays the bricks and their order; it does
not pin per-spec green-tree sequencing. The loop is not yet running; this plan
precedes it.

## 1. Goal

Make bifrost adoptable: ratatoskr depends on the bifrost crates and speaks
bifrost's `Account` / `AccountError` / `SyncEngine` language natively, with no
provider special-cases leaking up. "Adoptable" means every capability ratatoskr
ships today is reachable through the uniform surface - either as an `Account`
method that behaves identically across providers, or as a clean
`AccountCapabilities` flag ratatoskr reads declaratively. This is
feature-preserving: the bricks add and unify bifrost surface; they do not change
what a client can do.

## 2. Governing principle

Bifrost exists to serve ratatoskr. The migration is written against an IDEAL
bifrost, and wherever bifrost's current shape is sub-optimal for ratatoskr,
bifrost is fixed FIRST, here, before the matching ratatoskr work. ratatoskr is
never contorted around a bifrost wart. Provider-reality differences (Gmail has
no separate attachment-upload endpoint; IMAP has no native send) are absorbed
behind the uniform `Account` surface or expressed as capability flags. A
genuinely immutable provider limit becomes a flag the consumer consults, never a
code branch.

## 3. The seam

Bifrost is a service-side dependency. `Account` (the two-tier
primitives-plus-conveniences trait in `crates/types/src/account.rs`),
`AccountError` / `RecoveryClass`, and the `SyncEngine` are the only API. The
cursor state crosses as opaque versioned envelopes. ratatoskr keeps its DB,
stores, local tantivy search, the application sync layer (threading, bundling,
filters, notifications), discovery, OAuth authorization, the action pipeline,
and the entire app. Bifrost persists nothing: the engine emits a change stream
and ratatoskr owns the DB write plus a `CheckpointStore`.

## 4. Baseline the bricks build on (verified)

What already exists, so the bricks are framed as deltas, not from-scratch work:

- The `Account` trait is dyn-safe, two-tier (primitives with no default impl;
  conveniences defaulted in terms of primitives), and already carries the send,
  draft, search, container, settings, filter, contact, calendar, and mutation
  surfaces.
- `AccountFactory::open(account_id)` is the engine-facing reopen contract. It
  threads only an `AccountId`; all credential/transport config lives in the
  concrete factory, not on the trait.
- `bifrost-net` already provides the token-rotation mechanism: a `TokenSource`
  trait, `StaticTokenSource` (rotatable via `.set()`), `AccessToken`, and
  `OAuthRefresher` (single-flight refresh cache, itself a `TokenSource`).
- `AccountCapabilities` (`crates/types/src/capabilities.rs`) carries
  per-method `PimMethodSupport`, mutation/batching/rate-limit shapes, and
  `ConvenienceShape` (the star/replied/forwarded dispatch). New capability
  flags extend this struct.
- `discover_cursor_scopes` / `discover_memberships` are the scope-discovery
  surface every protocol implements, including standalone `caldav` / `carddav`.

## 5. The bricks

Labels mirror the migration plan's Track A for cross-repo walk. Each is one
future spec. "Current" reflects a read of the tree as of this writing; re-verify
at spec time.

### A1 - Uniform token rotation (first domino) - LANDED

- Intent: one generic refresh-plus-DB-write-back path drives every provider, so
  ratatoskr never special-cases rotation.
- Delivered: every concrete provider factory now accepts an
  `Arc<dyn TokenSource>` (bifrost-net's trait) at construction, and every
  transport reads `source.current()` at each wire authentication. Shipped per
  crate: Graph (`GraphClient::with_source` / `with_account_net`), Google
  (`GmailClient::with_source`, `GoogleAccountFactory::from_token_source`), JMAP
  (`JmapCredentials::bearer_source`, async `header_value`, `set_bearer_token`
  removed), SMTP (`Credentials::oauth2_source`, per-connect token read; async
  read in the async transport, single-poll resolve in the blocking transport;
  serde/`Eq` derives dropped from `Credentials`/`Auth`), IMAP
  (`Credentials::oauth2_source`, per-connect/per-reconnect read closing the
  stale-token reconnect bug, `PartialEq`/`Eq` dropped), CalDAV/CardDAV
  (`CalDavCredentials::bearer_source` / `CardDavCredentials::bearer_source`,
  per-request token read). Each crate has a `*_reads_current_token` /
  `*_threads_token_source` unit test pinning the rotation read point. The
  string-convenience constructors (`bearer` / `oauth2` / `from_access_token` /
  `new`) survive, wrapping a `StaticTokenSource` so existing call sites are
  unchanged.
- Note: SMTP and CalDAV/CardDAV gained a `bifrost-net` dependency (they had
  none); no cycle, bifrost-net does not depend on them.
- Depends on: nothing. Blocks all of Track B.
- TODO resolved: the "Token rotation asymmetry across factories" cross-cutting
  item is closed by this brick.

### A2 - IMAP send - LANDED

- Intent: one uniform send surface; no IMAP-plus-SMTP composition leaks up.
- Delivered: `ImapAccountConfig::with_submission(SmtpSubmissionConfig)` builds an
  owned `bifrost-smtp` transport inside the IMAP account. With submission
  configured, `send_message` and `draft_send` are real and the capability flags
  report `true`; without it they return `Unsupported` and the flags stay `false`.
  Submission auth reuses the IMAP `Credentials` (the A1 `Arc<dyn TokenSource>`
  threads straight across) unless `SubmissionCredentials` overrides it. MIME
  assembly moved to a shared `bifrost-types::mime` serializer
  (`send_request_to_rfc5322` / `render_rfc5322`) lifted from Google's
  `MailDocument`; Google, IMAP send, and IMAP `draft_create`/`draft_patch` now
  share one composition path. `send_message` returns the real APPENDUID-derived
  `ObjectId` from the Sent APPEND when available, else a controlled-domain
  generated `Message-ID`. A failed Sent-APPEND after a committed send is
  non-fatal (never resend) but logs an uncertain-Sent reconcile warning.
  `draft_send` adds a net-new raw `BODY[]` fetch plus Bcc-into-envelope /
  strip-from-body handling. `attachment_upload` stays `Unsupported` (A6).
- Depends on: A1 (shared token source).
- TODO: `smtp-M1` adjacent (raw-socket bandwidth metering parity) - not folded
  in; the submission transport uses the SMTP crate's own pooling.

### A3 - Raw RFC822 hydration - LANDED

- Intent: real assembled MIME bytes, uniform across all providers, for the body
  store, attachment dedup, and forwarding.
- Delivered: a new `Account` primitive `open_raw_rfc822(&self, message: ObjectId)
  -> AccountStream<SyncEvent<Bytes>>` yielding the verbatim server-assembled
  RFC822 octets (`Bytes`, never lossy-UTF8), gated by the new
  `PimMethodSupport.open_raw_rfc822` flag and the new
  `AccountOperation::OpenRawRfc822` (an idempotent read, outside the
  `is_idempotent` exclusion set). Native on all four mail protocols via each
  provider's existing server-assembled whole-message endpoint - no client-side
  MIME assembler: IMAP `BODY.PEEK[]` (new helper in `account/blob.rs`), Gmail
  `messages.get?format=raw` decoded by `inventory::raw_bytes`, JMAP `Email/get`
  for `blobId` then `client.download`, Graph `GET /messages/{id}/$value` via
  `download_stream`. The two DAV crates return
  `Unsupported(OpenRawRfc822)` with the flag `false` (inherited from
  `PimMethodSupport::default()`), gating the consumer's raw-source UI button.
  Folded-in Graph contract fix: `get.rs:hydrated_from_value` no longer ships
  serialized JSON inside `HydratedObjectKind::RawMime`; the body-bearing
  projections (`Headers`/`Preview`/`TextOnly`/`Full`/`FullWithBlobs`) now degrade
  to `Metadata` (falling back to `FlagsOnly`), an accepted stopgap until A1 owns
  Graph's body-projection path. The JMAP `hydrate.rs` raw-projection fatal is
  deliberately left in place (A1 owns it); A3 only opened the dedicated raw read.
- Depends on: A1 (landed after).
- TODO: none.

### A4 - Scheduled send [LANDED]

- Intent: first-class scheduled send where providers support it (Graph,
  JMAP, IMAP-via-relay); a flag where they do not (Gmail).
- Delivered: `SendRequest::scheduled: Option<SystemTime>`,
  `PimMethodSupport::scheduled_send`, `cancel_scheduled_send` /
  `reschedule_send` trait primitives, `AccountOperation::{CancelScheduledSend,
  RescheduleSend}`, a shared `bifrost_types::validate_scheduled` boundary
  helper, JMAP `holduntil` envelope + submission-id handle + undo/resubmit
  cancel/reschedule, Graph `PidTagDeferredSendTime` stamp + delete/patch
  cancel/reschedule, Gmail `Unsupported`, IMAP one-shot FUTURERELEASE
  (HOLDUNTIL) with cancel/reschedule `Unsupported`, and an smtp
  FUTURERELEASE-unsupported / over-limit error discriminator.
- Depends on: A2 (send surface settled).
- TODO: none.

### A5 - Shared mailboxes plus public folders - LANDED (A5a/A5b/A5c all landed)

- Intent: first-class bifrost scopes - Graph (EWS / Autodiscover) and IMAP
  (NAMESPACE / ACL) - surfaced through `discover_cursor_scopes` /
  `discover_memberships` and fully sync-integrated, not CRUD-only.
- A5 split into three sub-bricks; **all three (A5c, A5a, A5b) are LANDED**, so
  A5 overall is LANDED:
  - **A5c (IMAP NAMESPACE/ACL shared folders) - LANDED.** See below.
  - **A5a (Graph delegate mailboxes + JMAP shared accounts) - LANDED.** Graph
    config-supplied foreign mailboxes (`with_shared_mailbox`, per-mailbox
    `for_shared_mailbox` clients selected by `client_for_scope`); JMAP
    session-auto-discovered non-personal accounts (`foreign_mail`,
    `mail_for_scope`). Both surface as cursor-resident scopes (Graph
    `FolderType` namespaced via a per-crate `foreign.rs` codec, JMAP
    `Folder { account_id, mailbox_id }` with an additive `SCOPE_TAG_FOLDER`
    envelope variant), emit `MembershipScope::Mailbox(owner)` tags, and
    quarantine a foreign-scope permission denial through `graph_scope_revoked`
    / `jmap_scope_revoked` -> `ScopeRevoked` -> `DisableScope` while a primary
    denial stays terminal. Implementation lives in git history (the A5a landing
    commit) and the reference docs; the spec was retired at landing. Deliberate
    A5a-scoped-out follow-ups: **foreign-account mutations** (the JMAP
    per-accountId state store is shaped for `ifInState` but the mutation path is
    unwired) and **live foreign-mailbox lifecycle** (discovery is seeded once at
    open; Graph is config-seeded at construction, JMAP's `scope_lifecycle`
    worker polls only the primary, so a foreign mailbox added after open appears
    at the next reopen). Graph delegate auto-discovery (EWS `GetDelegate` /
    Autodiscover) landed under A5b - the `alternativeMailboxes` parser is in the
    tree, but wiring its enumeration into foreign seeding stays a named A5b
    follow-up; shared-mailbox send-as is C-3.
  - **A5b (Graph EWS public folders + Autodiscover) - LANDED.** The size-L
    unknown, landed in three sub-specs: (1) EWS read ops
    (`FindFolder`/`GetFolder`/`FindItem`/`GetItem` over `AccountNet` with the
    `X-AnchorMailbox`/`X-PublicFolderMailbox` routing pair, quick-xml parsers
    returning `EwsError`); (2) the no-delta-token cursor strategy
    (`GraphCursorKind::PublicFolder`, watermark timestamp-poll + throttled
    full-id deletion reconcile whose baseline rides in the cursor capped at
    10_000 items/folder, additive `SyncStrategy::Poll`, poll-only push,
    `ews_shared_scope_error` quarantine); (3) Autodiscover wiring
    (`GetUserSettings` public-folder routing + content-mailbox SMTP, the tested
    `alternativeMailboxes` delegate parser, `with_public_folders()` opt-in
    discovery seeding the routing map). See `reference/graph.md`. Named
    follow-ups: a `CheckpointStore`-backed deletion baseline restoring reconcile
    past the 10_000-item cap, and wiring delegate `alternativeMailboxes`
    enumeration into A5a's foreign-mailbox seeding (parser landed, unwired).
- Settled model (decided during A5c; A5a/A5b inherit it): a shared mailbox is
  **cursor-resident**, surfaced as an ordinary `CursorScope::Folder` - NO new
  `CursorScope` variant (a new variant would be silently mis-serviced by the
  scope routers). The **owner tag is the existing `MailboxId`**, emitted as a
  `MembershipScope::Mailbox(owner)` on each shared item's
  `InventoryEntry.memberships` / `ScopeChange` (no new consumer surface). The
  ratatoskr consumer maps that tag to `shared_mailbox_id` and treats an unknown
  tag as a hard error. Revocation **quarantines, not escalates**: a per-scope
  permission denial routes through `EngineDirective::DisableScope` (deletes that
  scope's cursor, broadcasts a scoped warning) while siblings keep syncing - it
  is NOT account-wide auth loss.
- A5c delivered (see `reference/imap.md` "Shared / other-user folders (A5c)",
  `reference/sync.md` `DisableScope`, `reference/error-model.md` `ScopeRevoked`):
  NAMESPACE-driven shared/other-user folder discovery with per-principal owner
  derivation, advisory ACL/MYRIGHTS read-gating at discovery, owner-tagged
  membership emission, and the `SyncState(ScopeRevoked)` ->
  `EngineDirective::DisableScope` quarantine recovery path. Implementation lives
  in git history (the A5c landing commit), not in a plan doc - the spec was
  retired at landing.
- Depends on: nothing structurally; A5c landed first as the cheapest, highest-
  ratio leg. A5a/A5b survey-first.
- TODO: `s34-S4` adjacent (IMAP capability flags key on `sub.is_some()`).
  `sync-N1` partially addressed by A5c's explicit `DisableScope` routing (see
  TODO.md).

### A6 - Cloud-storage attachments - LANDED

- Intent: large-attachment hosting plus share-link generation (Google Drive,
  Microsoft OneDrive), uniform and capability-flagged.
- Delivered: a new `Account` primitive `host_attachment(&self, bytes: Bytes,
  meta: CloudUploadMeta) -> AccountFuture<Result<HostedAttachment,
  AccountError>>` that uploads an over-limit attachment to the account's cloud
  drive and returns a shareable link in one call - upload + link are atomic from
  the caller's view, so a successful upload with a failed link step still returns
  `Err`. Shared types live in the new `bifrost-types::cloud` module
  (`CloudUploadMeta { file_name, mime, size, scope }`, `HostedAttachment {
  share_url, provider_file_id }`, `ShareScope::{Anyone, Organization}`). Gated by
  the new `PimMethodSupport.host_attachment` flag (`true` on Gmail and Graph,
  `Default` `false` elsewhere) and classified by the new non-idempotent
  `AccountOperation::HostAttachment` (an interrupted upload may have created a
  partial Drive item, so no blind retry). Google hosts via a Drive resumable
  session (`uploadType=resumable`), 256-KiB-aligned chunked PUTs, then a
  two-round-trip link (POST permission `{anyone|domain}`, GET `webViewLink`).
  Graph hosts via a OneDrive `createUploadSession` under a de-branded
  `Attachments` folder (was ratatoskr's `Ratatoskr Attachments`),
  320-KiB-aligned chunks resuming on 202, then a one-round-trip `createLink`. The
  four non-hosting protocols (JMAP, IMAP, CalDAV, CardDAV) return
  `Unsupported(HostAttachment)`. Two folded-in fixes ported from the ratatoskr
  sources: (1) a bifrost-net change - `classify_redirect` now returns
  `PassThrough` instead of `MalformedRedirect` when a followed-redirect status
  carries no `Location` header, so Drive's `308 Resume Incomplete` (no
  `Location`, only `Range`) reaches the chunk loop instead of failing every
  multi-chunk upload; a present-but-malformed `Location` stays a hard error. (2)
  The Drive chunk loop fails on an unparseable/absent 308 `Range` header instead
  of falling back to `offset = end`, closing a latent gap-skip upload-corruption
  bug in the ratatoskr source.
- Depends on: A1 (landed after; the AccountNet token source supplies the Bearer,
  with `.without_bearer_auth()` for the pre-authed chunk PUT).
- TODO: none. Closes A8 wart A-1 (the consumer-side
  `supports_cloud_upload(provider) = matches!(Graph | Gmail)` is replaced by the
  `host_attachment` capability flag).

### A7 - DAV as first-class synced accounts - LANDED

- Intent: CalDAV / CardDAV emit cursor scopes and sync-integrate, and compose
  into any account, not just IMAP.
- Current: standalone `caldav` / `carddav` implement `discover_cursor_scopes`,
  but composed-into-IMAP sub-accounts are primitive-only - their contact /
  calendar cursor scopes never propagate and `folder_from_scope` returns
  `Unsupported` for non-Folder scopes, so DAV sync dead-ends inside IMAP.
- Spec delivers: sync-integrated composition (sub-account cursor scopes
  propagate through the composing account) and composition beyond IMAP; decide
  fail-soft vs fail-hard for a DAV outage during open.
- Depends on: nothing structurally.
- TODO: `s34-G1` (the core gap), `s34-S4`, `s34-S5` (open fails hard on DAV
  outage), `s34-S6` (carddav ctag short-circuit).
- Status: LANDED. `s34-G1` (scope router `route_scope`/`ScopeHandler` + the
  four sync entry points + discovery fan-in), `s34-S4` (capabilities copy the
  sub's real `pim_methods`), `s34-S5` (fail-soft `DavAttach` open), and
  `s34-S6` (carddav ctag short-circuit) are all done, plus the two empty-207
  destroy-suppression / failed-uri-preservation robustness ports in the
  caldav+carddav PROPFIND-snapshot diffs. The composition router + discovery
  fan-in are extracted to `bifrost_types::account_compose`
  (`route_typed_scope` / `merge_scope_streams`) so JMAP/Graph composition is a
  later wiring task, not a copy-paste. `s34-S1` (TZID-as-UTC) is carried
  forward as its own standalone item (needs the `chrono-tz` tzdata dependency,
  absent from `Cargo.lock`); full CardDAV `sync-collection` parity with CalDAV
  is a named follow-up.

### A8 - Provider-wart absorption sweep - LANDED (closed)

- Intent: anywhere a wart would otherwise force a ratatoskr special-case,
  bifrost absorbs it behind the uniform surface or expresses a clean capability
  flag. Immutable provider limits become flags, never consumer branches.
- A8 is a *sweep* spanning the port-map's Groups A-D, not a single landing. The
  first slice of independent/unblocked warts landed first; C-3 (the send-path
  tail, gated on A5a) was the last bifrost-side brick and is now landed, so A8 is
  **closed**. The only residue is **A-6** (Gmail `CATEGORY_*` bundling - explicit
  consumer policy, not bifrost work), Group D (already-absorbed transport quirks),
  and the per-crate N-item cleanups already tracked in `TODO.md` (`graph-S1`,
  `graph-N3`, etc.) - none of which are A8 deliverables.
- LANDED (first slice): the independent warts unblocked by A1-A7. Delivered as
  one coherent landing:
  - **graph-N1 / B-2** - Graph `remove_from_container` returns
    `Unsupported(RemoveFromContainer)` with `remove_from_container: false`, now
    pinned by a method-behavior test (not just the flag). Closes `graph-N1`.
  - **B-1 (importance leg)** - a uniform three-valued `Importance` enum in
    `bifrost-types::hydration` (re-exported from `lib.rs`), a `Message.importance`
    read field populated at all four mail-crate construction sites, and a
    `set_importance` mutation primitive gated by `PimMethodSupport.set_importance`
    and `AccountOperation::SetImportance`. Graph absorbs its single-valued
    exclusivity inside one `If-Match`-conditioned PATCH (one overwrite, never
    expand-into-two); JMAP/IMAP map `High` <-> `$important`; Gmail returns
    `Unsupported`. The consumer never expands one importance change into two
    intents.
  - **B-4 (MDN `$MDNSent`)** - a `mark_mdn_sent` convenience backed by
    `set_keyword`, gated by the new `ConvenienceShape.mdn_sent_via_keyword` hint
    (true on JMAP/IMAP, false on Gmail/Graph where the read-receipt bit is
    read-only; the false case reports `Unsupported(UpdateFlags)`). Rides the same
    `ConvenienceShape` dispatch as `mark_replied`/`mark_forwarded` - no new
    operation variant.
  - **B-3 (draft-update new-id)** - confirmed uniform by construction:
    `draft_update` returns `Result<(), _>` and surfaces no post-update id, so the
    new-id contract is identical across all four mail crates (the consumer keeps
    its `DraftHandle`; impls remap internally).
  - **A-3 / A-4 / A-5 dispatch** - verified (not rebuilt): the uniform
    `vacation_*` / `contact_*` / `event_*` trait surface already absorbs
    ratatoskr's `match source:&str` / `match CalendarProvider` / six-per-provider
    free fns now that A7 landed the CalDAV/CardDAV legs. Resolved on the
    bifrost side; the consumer rewrite is Track B (B4/B6/B8).
- LANDED (final slice - C-3, the send-path tail):
  - **C-3** - Graph shared-mailbox send-as / send-on-behalf-of. Gated on A5a's
    foreign-mailbox routing (landed); the send-as leg landed as: a typed
    `SendAs::{As, OnBehalfOf}(MailboxId)` enum + `SendRequest::send_as` +
    `PimMethodSupport.send_as` flag (`bifrost-types`); the Graph backend resolves
    the routed `shared_clients` `&GraphClient` up front and threads it through
    `create_draft_message` / `stamp_deferred_send_time` / `send_draft_message`,
    with an `apply_send_as` helper stamping `from`/`sender` (`As` forces both to
    the mailbox; `OnBehalfOf` keeps `from` = mailbox and `sender` = `user_email`,
    omitted when `None`) and a `send_as_unknown_mailbox` -> `Request(Malformed)`
    for an unregistered mailbox; IMAP/Google each reject a `Some(send_as)` with
    `Unsupported(Send)` (a one-line guard at the top of `send_message`). The
    draft-backed path is kept (not `/users/{id}/sendMail`), so the port-map's
    inline-only-attachments note never applies. Scoped-out follow-up filed:
    `c3-2` (shared-mailbox send over SMTP for IMAP-shaped accounts) in
    `TODO.md`. JMAP native foreign-account submission (formerly `c3-1`) landed
    separately: JMAP now honors `send_as` by routing the draft `Email/set` +
    `EmailSubmission/set` to a seeded, submission-capable foreign account. The
    specs were retired at landing; durable record is git history +
    `reference/graph.md` / `reference/jmap.md`.
- STILL OPEN (NOT A8 deliverables - residue only):
  - **A-6** - Gmail `CATEGORY_*` bundling priority. POLICY: the ML categories are
    surfaced uniformly already; the bundling heuristic stays a ratatoskr consumer
    decision. No bifrost surface change - explicitly NOT bifrost work.
  - **Group D** - already-absorbed transport quirks (`$batch <= 20`, `If-Match`,
    push capability, 429/Retry-After, blob range); nothing to land.
  - **graph-S1 / graph-N3 and the per-crate N-item cleanups** - tracked in
    `TODO.md`, not A8 deliverables; none block ratatoskr.
- Depends on: known warts independent; C-3 needed A5a (landed). All landed.
- TODO: `graph-S1`, `graph-N3`, `c3-2`, and the broader per-crate N-item
  cleanups (`graph-N1` is closed and removed from `TODO.md`).

### A9 - Global Address List / directory search

- Intent: the org-directory address-list primitive that A-2 (`handlers/gal.rs`
  `match provider`) needs - split out of A8 because it is a new search primitive
  with two substantial, asymmetric provider backends (Graph directory `/users`;
  Google People `listDirectoryPeople` / `searchDirectoryPeople`), not a flag +
  thin convenience. The directory corpus is a *different corpus* from the user's
  personal address books (`contact_search`), which is why it earns its own
  primitive.
- Spec delivers: a `directory_search` primitive on `Account` returning a page of
  directory cards, a `directory_search` flag in `PimMethodSupport`, the Graph and
  Google directory backends, and the `Unsupported(DirectorySearch)` default for
  accounts without a directory.
- Depends on: nothing structurally - standalone, can land before or after the A8
  tail. A8 deliberately does not add a `directory_search` flag (a `false` flag
  with no primitive would be dead surface).
- LANDED. `directory_search(query, limit, page_cursor) -> Page<DirectoryCard>`
  on `Account`; new `DirectoryCard` (`crates/types/src/directory.rs`),
  `PimMethodSupport.directory_search`, `AccountOperation::DirectorySearch`
  (idempotent). Graph backend: `/users` with `startswith` `$filter`. Google
  backend: `listDirectoryPeople` (empty query) / `searchDirectoryPeople`
  (non-empty, with warmup), 403 directory-absence swallowed to an empty page for
  `PermissionDenied`/`InsufficientScope`, `PolicyBlocked` propagated. JMAP, IMAP,
  CalDAV, CardDAV are `Unsupported(DirectorySearch)`. The Google backend uses the
  People directory endpoints (`listDirectoryPeople` for the empty-query
  enumeration, `searchDirectoryPeople` for a non-empty lookup); the Graph backend
  uses `/users` with a `startswith` `$filter`. One refinement of the brick text
  above: the CardDAV directory-gateway leg is scoped out as a named follow-up
  (`TODO.md` a9-1), not delivered here.

## 6. Sequencing

A1 is the literal first task - it gates everything. Then:

- A2 needs A1; A4 needs A2.
- A3 and A6 land after A1; otherwise independent.
- A5 is survey-first and the biggest unknown; all three sub-bricks - A5c (IMAP),
  A5a (Graph delegate + JMAP shared), and A5b (EWS public folders, size L) - have
  landed, so A5 is closed.
- A7 is independent.
- A8's known warts are independent; its first slice landed (importance, MDN,
  `remove_from_container` polish, draft-update confirm, contacts/calendar/vacation
  verification) and its final brick C-3 (send-as, gated on A5a) has now landed, so
  A8 is closed. A9 (GAL) is standalone and landed.

**All Track A bricks (A1-A9) are LANDED.** C-3 was the last adoption brick; with
it, every capability ratatoskr ships is reachable through the uniform `Account`
surface or a declarative `AccountCapabilities` flag, with no provider special-case
leaking up. The remaining named follow-ups (A8's A-6 consumer policy, the
per-crate `TODO.md` N-items, the A5b/A7/A9/C-3 scoped-out follow-ups) are
non-blocking; bifrost is adoptable. Track B (the ratatoskr-side rewrite onto this
surface) is the cross-repo work that follows, tracked in ratatoskr's
`plans/bifrost-migration.md`.

Downstream stakes (Track B dependents, from the migration plan): A1 unblocks all
of Track B; A2/A4 -> send and drafts (B5); A3 -> the sync consumer body store
(B3) and attachments/forwarding (B9); A5 -> shared-mailbox scoping (B12); A6 ->
cloud attachments (B9); A7 -> calendar (B7) and contacts (B8) over DAV; A8 ->
wart-free action dispatch (B4/B6/B9).

## 7. Methodology

Bricks are processed serially, the tree green at every boundary, nothing
deferred. For each: verify the brick against current code, author one
technical-implementation-spec, run it through `reference/orchestrate.md`'s seven
steps, land it green. When a spec lands, update this plan (strike or annotate
the brick) and file any residual into `TODO.md`. Per repo convention this plan
is never committed as a standalone markdown change - it rides along with the
first code landing it relates to.

The `TODO.md` items named under each brick are opportunistic bonuses, not
requirements. When a brick's spec already has the relevant code open, folding in
an adjacent item is free cleanup and worth doing. But a brick is never blocked,
delayed, or scope-expanded to chase one: none of those items block ratatoskr
(`TODO.md` says so itself), so if an item does not fall naturally inside the
brick's landing, it stays in `TODO.md`.
