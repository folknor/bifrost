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

### A5 - Shared mailboxes plus public folders

- Intent: first-class bifrost scopes - Graph (EWS / Autodiscover) and IMAP
  (NAMESPACE / ACL) - surfaced through `discover_cursor_scopes` /
  `discover_memberships` and fully sync-integrated, not CRUD-only.
- Current: near-greenfield - no `SharedMailbox` / `PublicFolder` scope or
  capability. `CursorScope` / `MembershipScope` exist to extend. This is the
  largest Track A unknown.
- Spec delivers: TBD - size after a dedicated current-state survey. Likely
  splits (Graph EWS path; IMAP NAMESPACE/ACL path), each sync-integrated.
- Depends on: nothing structurally; survey first.
- TODO: `s34-S4` adjacent (IMAP capability flags key on `sub.is_some()`).

### A6 - Cloud-storage attachments

- Intent: large-attachment hosting plus share-link generation (Google Drive,
  Microsoft OneDrive), uniform and capability-flagged.
- Current: absent.
- Spec delivers: a bifrost surface for cloud upload plus share-link generation,
  behind a capability flag; providers without it advertise false.
- Depends on: A1.
- TODO: none.

### A7 - DAV as first-class synced accounts

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

### A8 - Provider-wart absorption sweep

- Intent: anywhere a wart would otherwise force a ratatoskr special-case,
  bifrost absorbs it behind the uniform surface or expresses a clean capability
  flag. Immutable provider limits become flags, never consumer branches.
- Current: scattered and partly tracked - Graph `remove_from_container` should
  report `Unsupported(RemoveFromContainer)`; `BlobRangeSupport` exists for
  blob-range uniformity; Gmail attachment inlining sits behind
  `attachment_upload`.
- Spec delivers: the enumerated wart fixes, each as a capability flag or
  internal absorption. The known ones can land independently; the full list is
  confirmed once Track B exercises the surface.
- Depends on: known warts independent; B-driven warts land last.
- TODO: `graph-N1`, `graph-S1`, and the broader per-crate N-item cleanups.

## 6. Sequencing

A1 is the literal first task - it gates everything. Then:

- A2 needs A1; A4 needs A2.
- A3 and A6 land after A1; otherwise independent.
- A5 is survey-first and the biggest unknown; start its survey early so its
  size is known before the schedule firms up.
- A7 is independent.
- A8's known warts are independent; its B-driven tail closes near the end.

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
