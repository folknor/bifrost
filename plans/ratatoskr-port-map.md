# Ratatoskr -> bifrost port map

Companion to `plans/ratatoskr-adoption.md`. That plan lays the bricks; this doc
records, per brick, what already exists in the cloned ratatoskr tree
(`research/ratatoskr/`) and how it ports. It is the evidence base for sizing and
sequencing each brick's spec. The adoption plan stays the source-of-record; this
is a working map, not a commitment.

## Governing fact: presence = needed

ratatoskr is mid-migration and unfinished. **If code exists in the clone, it is
required surface that simply has not been wired yet** - not dead code, not
abandoned scaffolding. Several provider features (cloud-attachment upload,
scheduled send + cancel + reschedule, send-as/on-behalf, raw-message fetch, GAL)
are present but have zero internal callers in the clone. The correct reading is:
these are the roadmap. ratatoskr will wire them against bifrost's uniform
surface, so bifrost must provide the ideal primitive for each. The one genuine
upside of "unwired" is freedom: with no consumer depending on the old shape,
bifrost can define the *ideal* uniform surface with no back-compat constraint.

Concrete confirmations from the user: ratatoskr will ship a "show raw message
source" action (drives A3), and large-attachment hosting is a real feature
(drives A6).

## Method

Seven read-only agents, one per brick group, each grounded in
`reference/error-model.md` + `reference/sync.md` plus the relevant per-crate
reference, cross-reading both trees. Classification legend used throughout:

- **COPY-DIRECT** - lift nearly verbatim (pure parsers, tested helpers).
- **COPY-AND-ADAPT** - lift, then reshape error boundary / transport / types.
- **RESHAPE** - concept reusable, code rewritten onto bifrost's uniform surface.
- **ALREADY-IN-BIFROST** - bifrost already has it (sometimes ahead of ratatoskr).
- **RATATOSKR-KEEPS** - lives above the seam (DB, policy, UX); bifrost persists
  nothing.

The structural through-line: ratatoskr already has the uniform seam bifrost is
replacing. `crates/common/src/ops.rs::ProviderOps` is the analog of bifrost's
`Account`; `crates/{gmail,graph,imap,jmap}` ops are the analogs of the per-crate
`Account` impls. So most absorption is "the wart already lives behind a trait
method; bifrost needs the matching capability flag or primitive." The warts that
most force consumer `match provider` are the ones that escaped that trait: cloud
attachments, GAL, scheduled send, auto-responses, the `LabelKind` taxonomy.

---

## A1 - Uniform token rotation (first domino) - LANDED

The whole mechanism exists in both trees, split around a seam A1 preserved.

| ratatoskr | bifrost dest | class |
|---|---|---|
| `common/src/token.rs:refresh_oauth_token` (RFC 6749 refresh POST) | body of a ratatoskr `TokenSource::refresh()` impl | RESHAPE |
| `common/src/token.rs:TokenState`/`needs_refresh` | `bifrost_net::auth::AccessToken` (richer) | ALREADY-IN-BIFROST |
| `common/src/token.rs:get_refresh_lock` global registry | `OAuthRefresher` single-flight | ALREADY-IN-BIFROST |
| per-provider `ensure_valid_token`/`do_refresh` (gmail/graph/jmap/imap) | `OAuthRefresher` wrapping the injected source | RESHAPE (collapses) |
| `jmap:rebuild_client_with_token` (reconnect on rotate) | bifrost reads the live token from the shared `Arc<dyn TokenSource>` in place (no reconnect, no crate-internal setter) | DELETE (obsolete) |
| `db/queries.rs:persist_refreshed_token`, encrypt+write, read+decrypt | DB write-back | RATATOSKR-KEEPS |
| `core/src/oauth.rs` (PKCE, callback listener, initial grant) | initial authorization | RATATOSKR-KEEPS |

**Where the seam falls.** `bifrost_net::TokenSource` *is* the seam. ratatoskr
supplies an `Arc<dyn TokenSource>` whose `refresh()` does lock -> read+decrypt ->
POST token endpoint -> encrypt+persist -> return `AccessToken`. bifrost
single-flights it in exactly one `OAuthRefresher` and consumes `current()` /
`force_refresh()`. The refresh network call and the persist both live inside
ratatoskr's impl; bifrost only ever sees the trait. **A1's deliverable is the
trait ingress on every factory, not the refresh logic.**

**Reshaping demands.** Every concrete factory accepts `Arc<dyn TokenSource>` at
construction (none does today; all three HTTP providers bury a private
`StaticTokenSource` built from a raw token string, and IMAP/SMTP bake a static
token into opaque `Credentials`). HTTP providers feed it to `AccountNet`;
IMAP/SMTP call `source.current()` at the auth leg of each new connection. The one
genuinely new code ratatoskr must write: classify a refresh failure into
`bifrost_net::Error` (transient -> `RefreshFailed` -> `Retry(AfterAuthRefresh)`;
invalid_grant 401/403 -> `AuthLost`, terminal). Its current `Result<_, String>`
loses that distinction the engine needs.

**Order within A1 (lowest blast radius first):** JMAP (bearer swap already in
place) -> Graph/Google (swap private `StaticTokenSource` ctor for injected
source) -> IMAP (new OAuth2-via-`TokenSource` credential kind, per-connection
resolve) -> SMTP (transport-builder ingress, no Account surface yet, pairs with
A2).

**Depends on:** nothing. **Blocks:** A2 and all of Track B.

**Bugs / cautions found (resolved at landing):**
- **Stale-token bug (the one the plan alluded to):** bifrost IMAP/SMTP
  `Credentials::oauth2` froze the token at construction, so a long-lived factory
  authenticated new pooled connections with an expired token until the consumer
  rebuilt the whole factory. A1 closed it: IMAP/SMTP now hold an
  `Arc<dyn TokenSource>` and re-read `current()` at each connection's auth leg.
- **Cross-provider single-flight:** ratatoskr serializes IMAP+SMTP refresh for
  one account under a single global lock. The A1 ingress lets ratatoskr hand the
  *same* `Arc<dyn TokenSource>` (one `OAuthRefresher`) to both transports of a
  composed IMAP+SMTP account, so the two legs single-flight together rather than
  racing - the consumer-side discipline A2 relies on.
- **Earlier framing, now moot:** the originating TODO and the spec draft once
  framed this as "expose `set_access_token` on every factory"; no factory ever
  did, and A1 took the other branch (every factory accepts an
  `Arc<dyn TokenSource>` at construction; no crate-internal setter survives).

---

## A2 - IMAP send - LANDED

The composition that "leaks up" is real and lands almost directly.

| ratatoskr | bifrost dest | class |
|---|---|---|
| `imap/ops.rs:send_email` (send -> APPEND-to-Sent -> return id) | imap `account/pim.rs` `send_message` | COPY-AND-ADAPT |
| `smtp/client.rs:build_transport` (lettre from config) | bifrost-smtp transport built inside the imap account | RESHAPE (bifrost-smtp is stronger) |
| `smtp/client.rs:extract_envelope` (re-parse raw to find recipients) | envelope from the structured `SendRequest` | RESHAPE (step disappears) |
| `imap/ops.rs:create/delete_draft` | bifrost `draft_create`/`draft_discard` | ALREADY-IN-BIFROST |
| `imap/ops.rs:mark_send_intent` (Reply->Answered, Fwd->$Forwarded) | existing `mark_replied`/`mark_forwarded` conveniences | RATATOSKR-KEEPS |
| `imap/ops.rs:test_connection` SMTP leg | validate at open/first-send | RATATOSKR-KEEPS |

**Reshaping demands.** Add an SMTP config + the A1 `Arc<dyn TokenSource>` to
`ImapAccountConfig`; construct the transport inside the account (SMTP XOAUTH2
reuses the *same* rotated token as IMAP). Gate `pim_methods.send_message` /
`draft_send` true iff an SMTP config is present (mirrors how caldav/carddav/sieve
gate their flags). Build RFC822 from the structured `SendRequest`, send, optional
APPEND-to-Sent honoring `save_to_sent`, return the Sent-folder `ObjectId`. Error
mapping is already end-to-end (bifrost-smtp's `account_error.rs` yields
`AccountError`).

**Depends on:** A1. bifrost-smtp is otherwise feature-complete for this.

**As landed.** `ImapAccountConfig::with_submission(SmtpSubmissionConfig)` builds
an owned `bifrost-smtp` transport inside the IMAP account; `send_message` /
`draft_send` are real and the capability flags report `true` only when submission
is configured, else `Unsupported` and `false`. Submission auth reuses the IMAP
`Credentials` (the A1 `Arc<dyn TokenSource>` threads straight across) unless
`SubmissionCredentials` overrides it. RFC822 assembly did **not** route through
`bifrost_smtp::Message::builder` (which consumes its own `Mailbox`/`Address`
types, forcing a header-level `bifrost_types::Address -> Mailbox` conversion onto
the critical path). Instead Google's `MailDocument` serializer was lifted into a
shared `bifrost-types::mime` assembler (`send_request_to_rfc5322` /
`render_rfc5322`) that operates on `bifrost_types::Address` natively; Google, IMAP
send, and IMAP `draft_create`/`draft_patch` now share that one composition path,
and the narrow bare-addr-spec conversion to SMTP's `Envelope` is confined to the
submission boundary.

**Cautions (resolved at landing):**
- Synthetic message id avoided: `send_message` returns the real APPENDUID-derived
  `ObjectId` from the Sent APPEND (UIDPLUS) when available, falling back to an
  explicitly-built controlled-domain `Message-ID` only when no Sent APPEND
  succeeded - never ratatoskr's `imap-sent-{ts}-{hex}` synthetic id.
- A failed Sent-APPEND after a committed SMTP send is non-fatal (the send is
  authoritative, never resend) but **not** silent: it logs an uncertain-Sent
  reconcile warning rather than a bare log-and-drop.
- `draft_update` was left out of scope deliberately (ratatoskr's delete+recreate
  is not on the send surface A2/A4/B5 need); it stays `Unsupported` in bifrost
  IMAP. If later implemented it is a clean delete+recreate on the Drafts APPEND
  primitive.
- Read-receipt / MDN injection was left above the seam: A2 did not grow
  `SendRequest` with an MDN field (a cross-provider `types` change out of A2's
  blast radius), so ratatoskr injects `Disposition-Notification-To` above this
  seam, or a follow-up adds the field for all providers at once. The capability is
  preserved, not silently dropped.
- `draft_send` adds a net-new raw `BODY[]` full-message fetch (hydration returns a
  parsed projection, not verbatim octets) plus deliberate Bcc-into-envelope /
  strip-from-body handling so blind recipients are delivered without leaking the
  `Bcc:` header.
- `attachment_upload` stays `Unsupported` (A6 territory); uploaded-attachment
  handles in a `SendRequest` are rejected. `smtp-M1` (raw-socket bandwidth
  metering parity) was not folded in - it stays in `TODO.md`.

---

## A3 - Raw RFC822 hydration - LANDED

Recalibrated for presence=needed: ratatoskr's `common/ops.rs:fetch_raw_message`
is an IMAP-only impl behind an otherwise-`Unsupported` trait default, currently
unwired. That is the **required "show raw message source" primitive**, not dead
code - it exists precisely because ratatoskr will wire that action. A3 builds the
uniform version.

| ratatoskr | bifrost dest | class |
|---|---|---|
| `imap/client/mod.rs:fetch_raw_message` (`BODY.PEEK[]`) | imap `account/get.rs` raw arm | COPY-AND-ADAPT |
| `common/ops.rs:fetch_raw_message` (trait default = unsupported) | new `Account` primitive + capability flag | RESHAPE |
| Gmail/Graph/JMAP raw reads | each crate's `account/get.rs` | NET-NEW (single endpoint each) |

**The de-risk.** The plan's "JMAP/Google/Graph assemble from parts" is wrong:
*all four* providers expose a server-assembled whole-message endpoint - IMAP
`BODY[]`, JMAP `Email.blobId` download, Gmail `messages.get?format=raw`, Graph
`GET /messages/{id}/$value`. **There is no client-side MIME assembler to build.**
bifrost's `get_stream` already issues `format=raw` for Gmail and already fetches
JMAP `BlobId`, so two of the four wire calls exist; Graph `$value` and the JMAP
blob-download primitive are the only new endpoints.

**Surface shape.** Mirror `open_blob`:
`fn open_raw_rfc822(&self, message: ObjectId) -> AccountStream<SyncEvent<Bytes>>`.
**`Bytes`, never `String`** - the IMAP path's `from_utf8_lossy` corrupts 8-bit /
binary parts and would render garbage in the raw-source viewer and break dedup
hashes. Streaming serves the body-store/dedup consumers; a viewer can buffer.
Capability flag `pim_methods.open_raw_rfc822`: true for all four mail providers,
false only for CalDAV/CardDAV-only composed accounts (which gates the UI button).

**Depends on:** A1 (landed after).

**As landed.** The uniform `open_raw_rfc822` primitive ships on all four mail
protocols (IMAP `BODY.PEEK[]`, Gmail `format=raw`, JMAP `Email/get blobId` +
`client.download`, Graph `$value`); the two DAV crates return
`Unsupported(OpenRawRfc822)` with the `open_raw_rfc822` flag `false`. `Bytes`
throughout - no `from_utf8_lossy`. The de-risk held: no client-side MIME
assembler was built.

**Bugs fixed at landing:**
- **Graph contract violation (fixed):** bifrost Graph no longer puts serialized
  JSON into `HydratedObjectKind::RawMime`. `hydrated_from_value`'s body-bearing
  projections (`Headers`/`Preview`/`TextOnly`/`Full`/`FullWithBlobs`) now degrade
  to `Metadata` (falling back to `FlagsOnly`); the assembled bytes come solely
  through `open_raw_rfc822` (`GET /messages/{id}/$value`). The degradation is a
  recorded stopgap until A1 owns Graph's body-projection path.
- **JMAP self-block (left in place by design):** the `hydrate.rs` raw-projection
  fatal was NOT removed - it belongs to A1. A3 instead opened the dedicated raw
  read (`Email/get blobId` + `client.download`), the cheapest provider to wire,
  leaving the hydration fatal untouched.

---

## A4 - Scheduled send [LANDED]

Landed decisions worth recording:

- **IMAP one-shot.** IMAP scheduled send is fire-and-submit via SMTP
  FUTURERELEASE (`HOLDUNTIL`, absolute-time so the boundary never races
  `now()`). RFC 4865 has no recall verb, so `cancel_scheduled_send` /
  `reschedule_send` are `Unsupported` for IMAP and `scheduled_send` is a
  per-connection truth defaulted `false` (the relay EHLO at send time is
  authoritative, no probe-at-open).
- **smtp discriminator (4.6a).** bifrost-smtp gained
  `ErrorKind::{FeatureUnsupported, ParameterOverLimit}`: FUTURERELEASE-
  unsupported maps to `Unsupported(Send)`, HOLDFOR-over-limit to
  `Request(Malformed)`, so the IMAP boundary can tell them apart without
  string-matching a consent-gated diagnostic.
- **JMAP submission-id handle contract.** A scheduled JMAP send returns the
  EmailSubmission id (the undo-addressable object) as the cancel/reschedule
  handle; an immediate send still returns the email id. Reschedule is
  cancel-and-resubmit (JMAP has no in-place reschedule).
- **Graph in-place reschedule.** Cancel = DELETE the deferred draft;
  reschedule = PATCH `PidTagDeferredSendTime`, returning the same id.

Every wire primitive already exists in **bifrost** (smtp FUTURERELEASE,
JMAP `holduntil`, Graph `PidTagDeferredSendTime`). A4 is a `SendRequest`
field + a flag.

Native support matrix: **JMAP** yes (RFC 4865, gated by `maxDelayedSend > 0`),
**Graph** yes (`PidTagDeferredSendTime` 0x3FEF), **Gmail** no (flag false),
**IMAP** yes iff the relay advertises FUTURERELEASE (else false).

| ratatoskr | bifrost dest | class |
|---|---|---|
| `jmap/ops.rs:schedule_send_jmap` (`holduntil` + maxDelayedSend check) | fold into jmap `send_message` `submit.envelope` | COPY-AND-ADAPT |
| `graph/ops:create_draft_with_deferred_time` + `schedule_send` | graph `send_message` (already draft-then-send) | COPY-AND-ADAPT |
| SMTP FUTURERELEASE | imap `send_message` via existing `SendOptions::hold_until` | RESHAPE / NEW |
| Gmail | flag false | nothing to port |

**Reshaping demands.** Add `scheduled: Option<SystemTime>` to `SendRequest`, a
`scheduled_send: bool` capability flag, validation at the provider boundary
(future + within `maxDelayedSend`/`future_release_max_interval` -> over-limit/past
map to `Request(Malformed)`; unsupported -> `Unsupported(Send)`).

**Scope gap, now firm given presence=needed.** ratatoskr ships **cancel** and
**reschedule** for both JMAP and Graph scheduled sends
(`cancel_scheduled_send_jmap` via `undoStatus`, Graph delete/patch). These are
required surface ratatoskr wires. A4 landed `cancel_scheduled_send` and
`reschedule_send` primitives gated by the same flag (not fire-and-forget),
which is why no provider special-case is needed.

**IMAP capability is post-EHLO.** Unlike JMAP (`maxDelayedSend` in the session
object) and Graph (always-on), the IMAP flag depends on the relay's EHLO
FUTURERELEASE advertisement, unknown until connect, while `AccountCapabilities`
is an open-time snapshot. Landed resolution: default the flag false and surface
a runtime `Unsupported` (no probe-at-open), per the IMAP one-shot decision
above.

**Depends on:** A2 (uniform send surface settled first).

---

## A5 - Shared mailboxes + public folders (largest brick)

Substantial ratatoskr code (~110 KB / 6 files) but most is CRUD + ad-hoc
poll-loop-into-SQLite - the shape the plan warns against. The hard, novel work is
reshaping that onto `CursorScope`/`changes_stream`, which has no ratatoskr
analog. Splits **three** ways, not two.

**Gating fork (resolve before any porting).** ratatoskr models a shared mailbox
as a *cloned client* (`for_shared_mailbox`), not a scope. bifrost must choose:
(a) new `CursorScope` variants carrying foreign-mailbox / public-folder routing,
threaded through the cursor envelope + `ErrorScope` + `scope_covers_membership`;
or (b) a separate `Account` instance per shared mailbox with the engine attaching
N accounts. Deep blast radius either way; this gates all three sub-bricks.

### A5c - IMAP NAMESPACE/ACL shared folders (size M, best first landing)

Highest value-to-effort: shared IMAP folders are ordinary folders behind a
NAMESPACE prefix, so once scoped they reuse the entire CONDSTORE/QRESYNC
`changes_stream` with **no new sync code**.

| ratatoskr | bifrost dest | class |
|---|---|---|
| `imap/public_folders.rs:parse_rights` (RFC 4314) + tests | imap codec/account | COPY-DIRECT |
| `discover_namespaces` (raw NAMESPACE byte-scan) | bifrost nom-8 codec NAMESPACE production | COPY-AND-ADAPT |
| `discover_myrights` (MYRIGHTS) | typed command/response | COPY-AND-ADAPT |
| `list_shared_folders` | extend `account/scopes.rs` discovery | RESHAPE |
| `sync_imap_public_folder` (SEARCH SINCE loop) | existing `changes_stream` Basic strategy | RESHAPE (mostly free) |
| `build_uid_set` | bifrost typed `UidSet` | ALREADY-IN-BIFROST |

### A5a - Shared mailboxes, Graph delegate + JMAP shared accounts (size S-M)

Graph plumbing (`for_shared_mailbox`/`api_path_prefix`/`is_shared_mailbox`/
`mailbox_id`) is **ALREADY-IN-BIFROST** (`graph/client.rs:161-195`). Real work:
a scope shape carrying the foreign mailbox identity, discovery emitting those
scopes, inventory/changes honoring `api_path_prefix()`, JMAP threading a foreign
`accountId`. Beyond CRUD: per-scope recovery isolation (a 403 on one shared
mailbox must not kill the account).

### A5b - Graph EWS public folders + Autodiscover (size L, the real unknown)

| ratatoskr | bifrost dest | class |
|---|---|---|
| `graph/ews/parsers.rs`, `xml_helpers.rs` (quick-xml) | graph `ews/` (exists, streaming-only today) | COPY-DIRECT |
| `autodiscover.rs` parsers (`parse_alternative_mailboxes`, `parse_user_settings`) | new graph `autodiscover.rs` | COPY-DIRECT |
| `ews/client.rs` FindFolder/FindItem/GetItem/CreateItem | graph `ews/` | COPY-AND-ADAPT (-> AccountNet + `ews_error_to_account_error`) |
| `autodiscover.rs` HTTP entry points | same | COPY-AND-ADAPT |
| `public_folder_sync.rs` poll loop + SQLite | new bifrost sync strategy + `CheckpointStore` | RESHAPE / RATATOSKR-KEEPS (DB) |

The single biggest piece of net-new sync engineering in all of Track A: public
folders have **no delta token**, so a timestamp-poll-plus-periodic-full-deletion-
scan must become a first-class bifrost cursor strategy (`establish_initial_cursor`
-> `EstablishViaInventory`; `changes_stream` re-polls by `DateTimeReceived` and
periodically does a full-id reconcile). The throttle state (`last_full_scan_at`)
and the routing context (hierarchy/content mailbox, `X-PublicFolderMailbox`
header) must live **inside the opaque cursor**, reconstructable on a cold resume -
not a side table. bifrost's EWS today is only the streaming-fallback skeleton
(client + envelope helpers); FindFolder/FindItem are net-new. The error-mapping
path (`ews_error_to_account_error`, `SoapFaultCode`) already exists, which
de-risks the adapt.

**Top risks:** (1) the scope-vs-Account fork; (2) the no-delta-token strategy;
(3) routing context must be cursor-resident; (4) no shared rights/permission type
exists (ratatoskr has three divergent ones - `ImapFolderRights`,
`EwsEffectiveRights`, `can_*` columns; bifrost needs one, and a decision: advisory
hint vs enforced pre-flight reject); (5) EWS operation surface maturity; (6) no
push for shared/public scopes (poll-only acceptable v1).

**Re-home:** `group_sync.rs` (Graph distribution-list membership) is **not** A5 -
it is contact-group membership, shares no machinery, belongs in the contacts work.

**Depends on:** the scope-model decision. Otherwise structurally independent.

---

## A6 - Cloud-storage attachments

Recalibrated for presence=needed: ratatoskr's `gmail/gdrive.rs` and
`graph/onedrive.rs` are complete upload+share-link modules that are present but
unwired in the clone. That is the **confirmed large-attachment-hosting feature**,
not dead code; the lack of callers is just ratatoskr's incompleteness.

| ratatoskr | bifrost dest | class |
|---|---|---|
| `gmail/gdrive.rs` (Drive resumable upload, sharing permission, webViewLink) | new google `account/cloud.rs` | COPY-AND-ADAPT |
| `graph/onedrive.rs` (OneDrive resumable upload, createLink) | new graph `account/cloud.rs` | COPY-AND-ADAPT |
| share-link scope vocab (Anyone/Domain vs anonymous/organization) | shared `ShareScope { Anyone, Organization }` | RESHAPE |
| `core/cloud_attachments.rs:supports_cloud_upload` (match provider) | `capabilities.host_attachment` flag | RESHAPE (also closes A8 wart) |
| 25 MB threshold + warn-vs-host UX + `UploadStatus` state machine | consumer | RATATOSKR-KEEPS |
| `detect_cloud_links` / inbound enrichment | consumer (enrichment optionally a thin Account method later) | RATATOSKR-KEEPS |

**Surface shape (upload + link in one call** - a stray uploaded-but-unlinked file
is the worst failure mode):
`fn host_attachment(&self, bytes, meta: CloudUploadMeta) -> Result<HostedAttachment, AccountError>`
with `CloudUploadMeta { file_name, mime, size, scope: ShareScope }` and
`HostedAttachment { share_url, provider_file_id, web_view_link }`. Behind it stays
all provider divergence (256 vs 320 KiB chunk alignment, `Location` header vs JSON
body, 308 vs 202 resume, one vs two round-trips for the link). Add a net-new
non-idempotent `AccountOperation::HostAttachment` so the error model classifies an
interrupted upload correctly (`AccountOperation` has no cloud variant today).

**Seam.** bifrost owns the mechanism (can-host flag, upload/link wire protocol,
error classification). ratatoskr owns the policy (the 25 MB threshold, warn-vs-host
UX, inserting `share_url` into the body, the `UploadStatus` persistence).

**Depends on:** A1 (must route through the shared `TokenSource` + bifrost-net, not
ratatoskr's bare `reqwest` + hand-built Bearer). Otherwise independent of A2/A4.

**Cautions:**
- New shared types land in `crates/types` first (trait method, `ShareScope`,
  `HostedAttachment`, `CloudUploadMeta`, `host_attachment` flag, the
  `AccountOperation` variant) - both impls depend on them.
- De-brand the hardcoded `"Ratatoskr Attachments"` OneDrive folder name.
- **Latent upload-corruption bug** in `gdrive.rs:upload_file_chunked`: on a 308
  with an unparseable Range header it falls back to `offset = end`, silently
  skipping a gap if the server accepted fewer bytes. Fix during the adapt; do not
  copy the optimism (OneDrive sibling avoids it).
- COPY-AND-ADAPT must be exercised by new bifrost unit tests (the chunk-range /
  serde tests copy cleanly).

---

## A7 - DAV as first-class synced accounts

Almost no port: the DAV protocol clients and iCal/vCard projection **already
moved** into bifrost, which is **ahead** of ratatoskr (bifrost added WebDAV
`sync-collection` to CalDAV; ratatoskr never had it). A7 is net-new bifrost
*composition wiring* inside the IMAP account. ratatoskr never composed DAV under
IMAP (it branched on a provider string column), so there is no composition
mechanism to copy.

| ratatoskr | bifrost dest | class |
|---|---|---|
| `core/{caldav,carddav}/client*`, parse | `crates/{caldav,carddav}` clients/projection | ALREADY-IN-BIFROST (bifrost ahead) |
| `calendar/src/sync.rs` orchestration loop + DB | engine + consumer | RATATOSKR-KEEPS |
| empty-207 deletion-suppression guard (`sync.rs:670,685`) | DAV `changes_stream` diff | COPY-AND-ADAPT |
| stale-URL clear+rediscover recovery (`sync.rs:407`) | DAV factory `open` + `RestartAccount` | COPY-AND-ADAPT (shape only) |
| IMAP scope fan-in + scope router | `imap/account/{scopes,mod,changes,inventory}.rs` | RESHAPE (net-new) |
| fail-soft DAV open | `imap/account/factory.rs` | RESHAPE (net-new) |

**The dead-end (s34-G1).** `imap/account/scopes.rs:discover_cursor_scopes` builds
scopes only from the IMAP folder registry, never touching `inner.contacts`/
`inner.calendars`; and `folder_from_scope` matches only `CursorScope::Folder(_)`,
returning `Unsupported` for `Type(Contact)`/`Type(CalendarEvent)` - which
`changes_stream`/`inventory_stream` call first thing. So composed DAV scopes are
rejected before any work.

**The fix.** (1) `discover_cursor_scopes` concatenates IMAP folder scopes with the
sub-accounts' `discover_cursor_scopes()` (drain, merge, single terminal `Done`);
same for memberships/lifecycle. (2) Replace the `folder_from_scope` gate with a
scope **router**: `Folder/FolderType` -> self; `Type(Contact)` -> `inner.contacts`;
`Type(CalendarEvent)` -> `inner.calendars`; each of `establish_initial_cursor`/
`inventory_stream`/`changes_stream`/`describe_cursor` dispatches through it and
delegates the whole call to the sub-account `Arc<dyn Account>` (already a full
Account - no new trait surface). The engine already namespaces cursors by
`ProtocolKind`, so CalDav/CardDav cursors cannot collide with IMAP's. Push is
poll-only for DAV (`push_subscribe` Unsupported) - the multiplexer just polls.
Capabilities should read the sub-account's real `pim_methods` (s34-S4), not
`sub.is_some()`. For "compose beyond IMAP," extract the router + fan-in as a
reusable helper rather than copy-pasting into JMAP/Graph.

**Fail-soft vs fail-hard (s34-S5).** Today `factory.rs:open` does
`open_caldav(...).await?` - a transient DAV outage fails the *entire* IMAP account
open, taking mail down. That is accidental, not designed. **Recommend fail-soft:**
attach IMAP-only degraded, omit the DAV scopes from discovery; the engine re-runs
`discover_cursor_scopes` on every reopen, so a later reopen picks DAV up by
design. Distinguish transient (`Transport`/`Unavailable` -> retry on reopen) from
permanent (`Authorization`/`NotFound` -> still attach, but surface a terminal
warning to fix config). Requires the factory to hold the DAV *config*, not just
the opened handle. Mirrors ratatoskr's own clear-and-rediscover recovery intent.

**Robustness catches (the most important COPY-AND-ADAPT in A7):**
- **Empty-207 deletion suppression.** ratatoskr's DAV sync skips deletes when the
  server returns zero hrefs but the local cache is non-empty (suspected transient).
  bifrost's snapshot-diff lacks this - a flaky DAV server returning an empty
  multistatus makes bifrost emit a `Destroyed` change for every contact/event and
  the consumer wipes its store. Port the guard into the diff.
- **Failed-uri preservation** (`sync.rs:685`): same destroy-on-transient risk
  class; preserve local copies for hrefs the server reported failed within a 207.

**Carried-forward bug:** s34-S1 - CalDAV `ical.rs` reads TZID datetimes as
wall-clock digits with `Z` appended, so `EventTime.value` (documented UTC) holds
local time off by the zone offset. Real correctness bug; same files.

**Other:** s34-S6 - CardDAV `changes_stream` encodes the ctag into the cursor but
never compares it (always full PROPFIND + diff). CalDAV already does proper
`sync-collection`; CardDAV is the laggard. Fold in either a ctag short-circuit or
full `sync-collection` parity while the code is open.

**Depends on:** nothing structurally.

---

## A8 - Provider-wart absorption sweep

ratatoskr's `ProviderOps` already hides most warts behind a trait method, so most
of A8 is "add the matching capability flag / primitive in bifrost." The warts that
escaped the trait force a consumer `match provider` and carry the highest adoption
value. Group A below is that high-value set.

### Group A - provider identity leaking ABOVE the seam (consumer `match`)

| # | ratatoskr leak | absorption | status | brick |
|---|---|---|---|---|
| A-1 | `cloud_attachments.rs:supports_cloud_upload` = `matches!(Graph\|Gmail)` | `host_attachment` primitive + `host_attachment` flag | NEEDS | A6 |
| A-2 | `handlers/gal.rs` `match provider` for GAL | directory/GAL primitive + `directory_search` flag | NEEDS | A8 (candidate own brick) |
| A-3 | `auto_responses.rs` six per-provider free fns | `vacation_get`/`set` flags exist; audience+date model-mapping into impls | PARTIAL | A8 |
| A-4 | `actions/contacts.rs` `match source:&str` + two body builders | `contact_*` flags exist; per-provider mapping into impls | PARTIAL | A8 + A7 (CardDAV) |
| A-5 | `calendar/actions.rs` `match CalendarProvider` | `event_*` flags exist; mapping into impls | PARTIAL | A8 + A7 (CalDAV) |
| A-6 | `bundling.rs` Gmail CATEGORY_* top priority | surface ML categories uniformly; heuristic stays consumer | POLICY | A8 |

GAL (A-2) is arguably under-served by "just a flag" - ratatoskr has real Graph and
Google directory impls that need a home; it may deserve its own small brick rather
than riding A8.

### Group B - model-mapping warts already behind `ProviderOps`

| # | ratatoskr wart | absorption | status |
|---|---|---|---|
| B-1 | `LabelKind` provider-typed taxonomy; Graph importance is single-valued/exclusive so `actions/label.rs` **expands one add into two intents** | importance-exclusivity belongs inside the bifrost-graph `set_category`/importance primitive; consumer never expands. Needs a uniform importance representation | PARTIAL (densest wart) |
| B-2 | Graph `remove_from_container` move-only | `Unsupported(RemoveFromContainer)` + flag false (`graph-N1`) | ALREADY (error polish only) |
| B-3 | Graph draft update = delete+recreate, "returns possibly-new id" | confirm bifrost `draft_update` new-id contract is uniform across all four | PARTIAL |
| B-4 | MDN `$MDNSent` - JMAP/IMAP only (Graph `isReadReceiptRequested` read-only) | `mark_mdn_sent` via `set_keyword` + flag; Gmail/Graph false | NEEDS |
| B-5 | replied/forwarded send-intent (`PR_LAST_VERB`) | `ConvenienceShape.replied_via_*`/`forwarded_via_*` | ALREADY |

### Group C - send-path warts

C-1 IMAP-send-via-SMTP = **A2**. C-2 scheduled send (three unwired provider entry
points off the uniform trait; JMAP `maxDelayedSend` gate is the textbook
"immutable limit -> flag") = **A4**, including the cancel/reschedule gap above.
C-3 Graph shared-mailbox send (`send_as_shared_mailbox`, `send_on_behalf_of`,
inline-only attachments on `/users/{id}`) ties to **A5** + a send-as identity
parameter on the send surface.

### Group D - transport quirks, mostly ALREADY absorbed

Graph `$batch <= 20` -> `BatchingPolicy`; `If-Match` -> `MutationConcurrency::StateBased`;
webhook/EWS-stream/Autodiscover -> `PushCapability`; 429/Retry-After -> `QuotaSignal`
(`graph-N3` HTTP-date polish); per-client `Semaphore` -> `graph-S1` (delete if a
per-account limiter lands in net/sync); blob range per-handle -> `BlobRangeSupport`.
Gmail historyId expiry / batchDelete-403 TRASH fallback / flat-labels / synthetic
Archive / Pub/Sub topic -> all ALREADY in bifrost-google. IMAP UIDVALIDITY /
QRESYNC-CONDSTORE-Basic / IDLE / client-side threads -> ALREADY (shared mailboxes
= A5). JMAP `ifInState` / WebSocket / Sieve -> ALREADY.

### A8 priority (most forces a consumer `match provider`)

1. Cloud attachments (A-1 / A6). 2. GAL (A-2). 3. `LabelKind` + Graph-importance
intent expansion (B-1, densest). 4. Scheduled send (C-2 / A4). 5. IMAP send (C-1 /
A2). 6. Contacts/calendar/auto-response dispatch (A-3/4/5, mostly resolved once the
bifrost primitives + A7 land).

**Independence:** standalone now - A-2 (GAL flag), B-2 (`graph-N1`), B-4 (MDN
flag), B-1 (importance, modulo a representation decision), A-6 (policy). Needs A1 -
A6, A3, A2. Chained - A4 needs A2; C-3 needs A5; A-4/A-5 CalDAV/CardDAV legs need A7.

---

## Cross-cutting findings

**The scope-vs-Account fork (A5) is the one decision that gates the most.** Resolve
how a foreign/shared mailbox is modeled before any A5 porting.

**Unwired-but-required surface (the roadmap).** Present in the clone, zero callers,
all things ratatoskr will wire against bifrost's ideal surface: cloud-attachment
upload (A6), scheduled send + cancel + reschedule (A4), send-as / on-behalf (A5/C-3),
raw-message fetch (A3), GAL (A8 A-2). Upside: no back-compat constraint - design the
ideal uniform surface.

**Bugs worth fixing inside the relevant brick (not separate work):**
- IMAP/SMTP stale-token-at-construction (A1).
- Graph `RawMime` carrying JSON (A3, fixed - body projections degrade to
  `Metadata`); JMAP raw fatal left to A1 (A3 added the dedicated raw read).
- DAV empty-207 destroy-everything + failed-uri loss (A7).
- CalDAV TZID-as-UTC offset bug, s34-S1 (A7).
- gdrive 308-resume gap-skip corruption (A6).
- Graph `add_label` category read-modify-write lost-update window (A8/B-1).

**Duplicate/loose typing bifrost already fixes:** ratatoskr's `CalendarProvider`
enum parallels `MailProviderKind` (divergence risk); contact dispatch keys on a
`&str` source. bifrost's single typed `Provider`/scope/provenance model removes both.

## Suggested sequencing (refines plan section 6)

1. **A1** - first domino, JMAP -> Graph/Google -> IMAP -> SMTP.
2. **A2** then **A4** (A4 includes cancel/reschedule).
3. **A3** and **A6** after A1, independent of each other.
4. **A5** - start the scope-vs-Account decision early; **A5c (IMAP NAMESPACE)**
   first as the cheapest, highest-ratio landing; **A5b (EWS)** is the size-L unknown.
5. **A7** - independent; fold the DAV robustness guards in.
6. **A8** - standalone warts anytime; B-driven tail closes near the end. Consider
   pulling **GAL (A-2)** out as its own small brick.
