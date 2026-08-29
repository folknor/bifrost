# bifrost-google reference

Current architecture of the Google Account-layer code under
`crates/google/src/account/`. Public surface is
`bifrost_google::account::{GoogleAccountFactory, PubSubConfig}`;
everything else is crate-private behind `Account` / `AccountFactory`.
The mail path uses a `GmailClient` for Gmail REST history-id sync,
Cloud Pub/Sub push, and flag canonicalization; People contacts and
Google Calendar ride alongside it.

Gmail has no UID model and no mailbox-scoped server state. A single
`historyId` walks the account-wide change log, and labels play the
role of folders: one `CursorScope::Account` cursor per account, a
`changes_stream` paging `users.history.list`, and mutations routing
through `messages.batchModify` / `batchDelete`.

## Module layout

Public modules:

- `account` - public `GoogleAccountFactory` and `PubSubConfig`; the
  opened account itself is returned inside `OpenedAccount`.

Internal modules:

`crates/google/src/account/`:

- `mod.rs` - crate-private `GoogleAccount`, public
  `GoogleAccountFactory`, `impl Account`.
- `capabilities.rs` - `AccountCapabilities` builder.
- `cursor.rs` - `GmailChangeState`, envelope encode/decode,
  `cursor_from_state`.
- `scopes.rs` - `discover_cursor_scopes`, `discover_memberships`,
  `scope_lifecycle_stream`, `ScopeCache` / `ScopeSnapshot`.
- `inventory.rs` - inventory pass and `get_stream` hydration.
- `changes.rs` - history-id driven change stream.
- `push.rs` - Cloud Pub/Sub `watch`/`stop`, `PubSubConfig`, and the
  `PubSubControl` watch actor.
- `mutation.rs` - `bulk_set_flags`, `bulk_move`, `bulk_destroy`.
- `pim.rs` - Phase 3.6 PIM primitives: message/thread label
  mutations, MIME send and drafts, search translation, container
  CRUD, identities, vacation responder, message/thread hydration.
- `filters.rs` - Gmail settings filter list/create/delete and
  typed-rule mapping.
- `flags.rs` - Gmail-label-to-IMAP-flag canonicalization and the
  reverse `LabelPatch` translation used by mutations.
- `blobs.rs` - `open_blob` / `open_blob_range` over Gmail
  attachments, plus `open_raw_rfc822` (whole message via `format=raw`,
  decoded by `inventory::raw_bytes`).
- `cloud.rs` - `host_attachment`: Drive resumable upload + sharing
  permission, one call. Session POST uses the raw `account_net()`
  builder (sets `X-Upload-Content-*`, which typed `post` cannot); the
  pre-authed chunk PUT skips auth (`.without_bearer_auth()`) and
  resumes on 308 via `Range: bytes=0-N` - an absent/unparseable 308
  `Range`, a non-advancing/backward cursor, or a cursor beyond the bytes just
  sent is a hard failure, never a silent gap-skip. The loop also bounds total
  chunk attempts to the expected chunk count plus eight, so malicious tiny
  progress cannot hold the caller forever. Then a
  `permissions` POST (`type: anyone` / `type: domain` + account domain)
  and a `webViewLink` GET. The 308 reaches the loop only via
  bifrost-net's missing-`Location` passthrough.
- `error.rs` - translation boundary from `crate::Error` to
  `AccountError` via the central `AccountErrorBuilder`.

`crates/google/src/api.rs` is the Gmail wire wrapper used by these
modules. Every dynamic label, thread, message, attachment, draft,
filter, send-as, and page-token component is passed through
the matching `bifrost_net::url` path or query encoder before URL
assembly. The Drive hosting path applies the path rule to returned
file ids.

## GoogleAccount / GoogleAccountFactory

Consumers construct `GoogleAccountFactory` with
`from_access_token(token)` (raw) or `from_token_source(Arc<dyn
TokenSource>)` (shared source, read live per request), then optionally
attach a `PubSubConfig` via `with_pubsub_config`/`with_pubsub_topic`.
The factory is the only public entry point; the raw `GmailClient`,
Gmail wire DTOs, and crate-local `Error` are `pub(crate)`.

### Three independent API bases

The crate talks to three Google surfaces, and each base is configured
separately because none can be derived from another:

| Surface | Production base | Override |
|---|---|---|
| Gmail mail | `www.googleapis.com/gmail/v1/users/me` | `from_access_token_with_api_base` / `from_token_source_with_api_base` |
| People (contacts, directory) | `people.googleapis.com/v1` | `with_people_api_base` |
| Calendar | `www.googleapis.com/calendar/v3` | `with_calendar_api_base` |

Calendar shares a HOST with Gmail but not a path root, so it cannot ride the
Gmail base; People differs in both. All three compose, so a harness can redirect
any subset.

`RATATOSKR_TEST_GCAL_ENDPOINT` still sets the Calendar base when
`with_calendar_api_base` is not called. It is **legacy, kept working
deliberately**: downstream harnesses set it, and removing it would not fail
their builds - it would silently stop redirecting and send their test traffic to
the real Google Calendar API. It is now read ONCE per client construction
(`default_calendar_base`) rather than on every request. It was previously a
`std::env::var` read per call, which meant a `getenv` on a hot path,
process-global state read from inside a library, no way to point two accounts at
two Calendar endpoints in one process, and a bifrost crate naming its downstream
consumer in an identifier. An explicit `with_calendar_api_base` always wins; a
variable set after a client is built does not affect that client.

Rate limits are registered against the hosts derived from these configured
bases, not against literal production hostnames - a redirected base was
otherwise completely unmetered. Each declaration carries the engine
`AccountId` as its `quota_scope`, so Gmail's per-user quota is per account
rather than pooled across every account in the process that happens to share
`www.googleapis.com`. The same `(host, quota_scope)` key is registered once:
Gmail and Calendar share `www.googleapis.com` in production, and registering it
twice would install a second bucket for one key so the effective limit became
whichever registration won. Because this account declares exactly one scope per
host, its requests need no `RequestBuilder::quota_scope` override - the
account-level default resolves unambiguously. A base that fails to parse falls
back to the production host, so a malformed override meters the real host rather
than nothing.

### Per-batch byte accounting

`GmailClient` carries an optional `ByteTally`. `metered()` hands back a handle
that is one `Arc` bump over the same client and reports every request's
request-local `bytes_in` into a fresh accumulator; each engine
stream takes one at construction and each emitted batch `take`s it, so
consecutive batches partition the traffic. The record sits at `send_recorded`,
the single point every buffered request leaves through, which is what keeps
`delete`, `post_no_content` and caller-built `execute_builder` requests counted
rather than free. It records from the request-local counter, not from the
`Response`, so a FAILED request contributes too: the mutation lane turns a
refused `batchModify` / `batchDelete` into per-item failures and still emits a
batch, and the `batchDelete` permission fallback's refused primary call is pure
error-path traffic.

This matters because one batch is routinely many requests: an inventory batch
covers a `users.messages.list` page plus up to 32 concurrent
`users.messages.get` calls, and a mutation batch covers `batchModify` plus any
label refresh and per-id TRASH fallback. The two remaining zero-valued sites are
genuinely synthetic - `discover_cursor_scopes` returns a constant, and a
`discover_memberships` cache hit performs no request - and say so at the call
site. Blob batches keep their own exact transferred size and do not use the
accumulator. The Drive resumable-upload session in `cloud.rs` builds its own
requests off `account_net()` and so sits outside the accumulator; it feeds no
`bytes_in` field, so no lane reports a number it did not measure.

`GoogleAccountFactory` carries an `Arc<GmailClient>` and an optional
`PubSubConfig`. `open(account_id)` asks the client for an
account-scoped clone attached to `bifrost-net` under the engine
`AccountId`, does one `users.getProfile` round-trip, parses
`profile.historyId` into a `u64`, and stores the `GmailChangeState` as
`seed_state`. The opened `GoogleAccount` retains:

- `client: Arc<GmailClient>` (crate-private REST wrapper).
- `capabilities: AccountCapabilities` snapshotted at open.
- `profile: GmailProfile` for `email_address` and history-id
  identity checks downstream.
- `seed_state: OpaqueChangeState` used by
  `establish_initial_cursor` to mint a cursor without a second
  network call.
- `pubsub: Arc<PubSubControl>` holding the watch actor command sender,
  shutdown token, and a `broadcast::Sender<WatchEvent>`. The actor task owns the
  optional config, lifecycle state, and active-handle set exclusively.
- `scope_cache: Arc<ScopeCacheState>` for the shared, refresh-on-stale label
  vocabulary used by canonicalization: an `RwLock<ScopeSnapshot>` for the
  snapshot plus a `tokio::sync::Mutex` that makes the refresh single-flight.
  Lifecycle diff state is private to each lifecycle stream and never reads this
  cache.
- `shutdown: CancellationToken` for the watch actor and the
  lifecycle stream.
- `set_priority` / `set_bandwidth_cap` delegate to the underlying `AccountNet`.

Clients (`from_access_token` or `from_token_source`) retain their parent
`Net` without attaching a placeholder account. `open(account_id)` is
the first point that mints an `AccountNet`, under the engine id. A
failed open detaches that registration before returning the error.
There is no public custom-`Net` constructor after S1-W3; callers use
the factory and the shared `Account` trait.

`AccountFactory::open(account_id)` returns `OpenedAccount` with an
always-empty skip lane (single-namespace account; open probes only the
principal's own profile). `reopen`
flows from the engine: it drops the previous `Arc` and calls the factory
again with the same `AccountId`. The factory holds the credentials and
client, so the new `GoogleAccount` carries a fresh
`shutdown`/`pubsub`/`scope_cache` and reads the current profile at open.

`close()` is idempotent and cancellation-safe. It marks `closed` and cancels
`shutdown` synchronously, before the returned future exists, so the watch actor
and the push and scope-lifecycle streams are retired whatever the caller does
with that future. The future then makes a best-effort `users.stop` call for a
locally active Gmail watch - it needs the transport, so it cannot precede the
detach - and clears the watch state; the `bifrost-net` detach runs from a drop
guard constructed before the future is returned, and therefore happens on
completion, on mid-poll cancellation, and when the future is dropped
without ever being polled. A stop
failure is classified and logged but cannot keep a closing account alive. The
one thing a dropped close future cannot guarantee is the remote half: Gmail
may keep delivering until the 7-day watch expires.
`Drop` also cancels and detaches as a fallback when a consumer omits
`close()`. The watch actor and `push_stream` both select on
`shutdown.cancelled()` and retire cleanly.

## Capabilities

`gmail_capabilities()` in `capabilities.rs`:

- `cursor_freshness: CursorFreshness::ServerIssued`. The `historyId` is
  server-issued and monotone per account; trusted without a local clock.
- `blob_range: BlobRangeSupport::No`. Gmail attachments arrive base64url inside
  a JSON envelope with no HTTP Range surface; `open_blob_range` enforces it.
- `blob_digest_pre_download: false`. Attachment metadata carries
  no digest separate from the body.
- `push: PushCapability::OutOfProcessPubsub`. Push lives on Cloud Pub/Sub, not a
  connection bifrost owns; `push_in_process()` is false and consumers wire their
  own subscriber feeding `InvalidationSink`.
- `mutation.concurrency: MutationConcurrency::None`. No
  optimistic-concurrency primitive on `batchModify`; the engine's
  read-back guard is the lost-update safety net.
- `mutation.replay_safety: MutationReplaySafety::None`. No client-mintable
  dedup token; the shared `IdempotencyKey` is held engine-side.
- `batching_policy: BatchingPolicy { max_items: 1000, max_wait:
  75ms, flush_on_input_close: true }`. 1000 = Gmail's `batchModify`
  cap.
- `rate_limit_class: RateLimitClass::Tiered`. Per-user quota units,
  not a uniform rps cap. Gmail publishes a per-user rate of 250 quota units
  per second; the Gmail host bucket is deliberately registered below it, at
  100 units/s with a 100-unit burst, so a backfill leaves headroom for the
  interactive traffic sharing the same per-user budget.
- `quota_signal: QuotaUnits`. Every Gmail request debits the published
  per-method cost, applied in `GmailClient::execute` and, for the raw batch
  mutation builder, by calling `GmailClient::gmail_quota_cost` directly.
  Method costs are transcribed from Google's published quota table (May 2026
  revision, rechecked August 2026) in `client.rs::gmail_quota_cost`, which
  carries the provenance note. A Gmail method absent from that table is billed
  at 20 units rather than 1, so an unchecked method cannot under-charge. The
  host `cost_default` of 1 unit applies only to the non-Gmail Google surfaces
  sharing the transport (Calendar, Drive, People).
- `requires_uidvalidity_recheck: false`. Gmail has no
  UIDVALIDITY model.
- `historyid_expires_after: None`. No documented `historyId` retention window;
  expiry is detected reactively via `classify_history_error` (404/410 ->
  `RestartScope`), not on a timer. `describe_cursor` reports
  `CostClass::Expensive` only when the cursor envelope no longer decodes against
  the open account's email-address; that flips `SyncStrategy::ServerCursor` and
  `freshness` to `None`, prompting re-establish.
- `delta_token_expires_after: None`.
- `pim_methods`:
  - Supported: `add_to_container`, `remove_from_container`,
    `set_label_membership`, `set_is_read`, `send_message`,
    `draft_create`, `draft_update`, `draft_discard`, `draft_send`,
    `search`, `search_messages`, `containers_list`,
    `container_create`, `container_rename`, `container_delete`,
    `identities_list`, `identity_update`, `vacation_get`,
    `vacation_set`, `thread_hydrate`, `message_hydrate`,
    `address_books_list`, `contacts_list`, `contact_get`,
    `contact_create`, `contact_update`, `contact_delete`,
    `contact_search`, `contact_autocomplete`, `directory_search`,
    `calendars_list`,
    `events_in_range`, `event_get`, `event_create`,
    `event_update`, `event_delete`, `event_rsvp`, `event_search`,
    `event_autocomplete`, and `host_attachment` (Drive hosting).
  - Unsupported: `set_keyword`, `set_category`,
    `set_extended_property`, `set_importance` (Gmail has no
    message-importance field), `attachment_upload`, `container_move`,
    `scheduled_send`, and `quota_get`. The Gmail REST API has no
    scheduled-send lever (web-UI only), so a scheduled `SendRequest` is
    rejected `Unsupported(Send)` before any wire call, and
    `cancel_scheduled_send` / `reschedule_send` are `Unsupported`.
- `filter_rule_shape: Rules`. Gmail filters are wired for
  list/create/delete plus local validation; `filter_update` is
  unsupported (no update/replace endpoint).
- `conveniences.starred: LabelMembership`. The default
  `set_starred` convenience dispatches to Gmail's `STARRED` label.
  Replied and forwarded convenience flags are false because Gmail
  derives that state from messages rather than exposing a writeable
  flag. `mdn_sent_via_keyword` is false: Gmail's read-receipt model is
  read-only, so `mark_mdn_sent` returns `Unsupported(MarkMdnSent)`.
  Hydrated `Message.importance` is always `Normal`.

## PIM primitives and conveniences

`pim.rs` implements the S1-W2 Gmail shape for the unified Account
trait.

Mail mutation primitives use Gmail label modification:

- `add_to_container` and `remove_from_container` dispatch to
  `users.messages.modify` or `users.threads.modify` depending on
  `MutationTarget`.
- `set_label_membership` is the same add/remove label operation.
- `set_is_read` flips Gmail's `UNREAD` label with inverted polarity.
- `set_keyword`, `set_category`, `set_extended_property`, and
  `set_importance` are `Unsupported` (see the capability list above).
- The Archive container is synthetic: adding to Archive is lowered as
  a relocation (see below), never as a label to apply; removing from
  Archive is a no-op (archive is the absence of a display container,
  not a native label).
- `move_thread` is ONE `threads.modify`: the add and every removal
  (the exclusive display containers plus the caller's `source` label)
  ride the same request, so there is no window where the thread sits
  in both containers.

One relocation rule, shared by `flags::move_placement_patch` between
the bulk `batchModify` driver and the single-object builders in
`pim.rs`, so a consumer cannot get different wire semantics depending
on which entry point it reached. `INBOX` / `SPAM` / `TRASH` are Gmail's
mutually exclusive display containers: Gmail renders a message under
whichever of them it carries, whatever else is also attached. "Move
into `destination`" therefore lowers to *add `destination`, remove
every exclusive container that is not `destination`*:

- `INBOX` -> add INBOX, drop SPAM + TRASH (un-spam / un-trash).
- a user label -> add it, drop INBOX + SPAM + TRASH, so filing a
  spammed or trashed message actually takes it out of Spam / Trash.
- `archive` -> add nothing (it is not a Gmail label id, and Gmail
  rejects a modify that tries to apply it), drop all three.
- `SPAM` / `TRASH` -> add it, drop the other two.

Case-insensitive matches for the three system destinations are normalized to
their canonical Gmail wire spelling before entering `addLabelIds`; user label
ids retain the caller's exact casing.

An optional `source` is the one part the destination cannot imply - a
user label being filed out of - and it joins the same
`removeLabelIds`. It is skipped when it is the synthetic `archive`,
equal to the destination, or already implied.

Composition renders MIME through the shared `bifrost-types::mime`
serializer (`render_rfc5322`, shared with IMAP send) and sends the
base64url message through Gmail. `MailDocument` keeps Gmail
orchestration (draft-patch apply, hydration) and emits `Bcc:` so Gmail
learns blind recipients:

- `send_message` calls `users.messages.send`. Inline attachments are
  encoded into the MIME tree. Pre-uploaded attachment handles are
  unsupported (Gmail has no separate message-attachment upload).
- `draft_create`, `draft_update`, `draft_discard`, `draft_send` call
  Gmail drafts endpoints. `draft_update` fetches the draft in `full`,
  projects editable fields into `MailDocument`, applies the patch, then
  replaces the draft with the re-rendered raw MIME.
- `attachment_upload` returns `Unsupported`; large over-limit
  attachments use `host_attachment` (`cloud.rs`) -> Google Drive.

Search translates the shared `SearchRequest` AST into Gmail query
strings and uses `users.threads.list` for thread-shaped search and
`users.messages.list` for message-shaped search. `provider_query` is
appended verbatim so consumers can use Gmail-specific operators such
as `larger:5M`. Both list endpoints explicitly send
`includeSpamTrash=true`; inventory and search therefore see the same
first-class Spam and Trash containers that history changes can name.
The inventory page walk calls the same `mail_list_query` builder as
search, including its page-token query encoding.

Container CRUD treats Gmail labels as `ContainerKind::Label` and returns
native label ids. System labels `INBOX`, `SENT`, `DRAFT`, `TRASH`, `SPAM`
map to the matching `FolderRole`; Archive is a synthetic label-shaped
container with native id `archive` and `FolderRole::Archive`. Create,
rename, and delete call Gmail label endpoints. Moving containers is
unsupported because Gmail labels are flat.

Settings primitives map to Gmail settings endpoints:

- `identities_list` and `identity_update` use
  `users.settings.sendAs`.
- `vacation_get` and `vacation_set` use
  `users.settings.vacation`.
- `quota_get` returns `Unsupported`; the Gmail API profile exposes
  message counts but not storage quota bytes.

Hydration primitives use Gmail's `full` and `metadata` formats.
`thread_hydrate` calls `users.threads.get` and projects each message.
`message_hydrate` picks the cheapest format for the requested
`HydrationProjection`, parses common address and threading headers, maps
label ids to containers and canonical flags, and surfaces attachment blob
handles for `FullWithBlobs`.

Google Calendar maps shared lifecycle status, availability, visibility,
attendees, and recurrence on create/update. Organizer is server-derived
for created events; create payloads carrying a shared organizer are
rejected as unsupported rather than silently dropped.

The Gmail overrides for multi-call conveniences are:

- `move_thread` adds the target container first, then removes the
  source container when supplied. The implementation lives in the
  crate so it can own a cloned client inside the boxed future.
- `delete_thread` moves to `TRASH` unless the caller says the thread
  is already in Trash, in which case it calls `users.threads.delete`
  for permanent deletion.

Other conveniences inherit the trait default. `set_starred` routes
through `set_label_membership` because capabilities advertise
`LabelMembership`. `apply_label` and `remove_label` use the default
provenance dispatch for Gmail label ids. The synthetic `archive`
container is label-shaped for round trips, but applying it is the
documented relocation operation: it removes INBOX, SPAM, and TRASH
rather than adding a native Gmail label.

People contacts use `people/me/connections`, `people:get`,
`people:createContact`, `people:updateContact`, and `people:deleteContact`.
Auto-collected addresses (People `otherContacts.list`) are surfaced as a
distinct corpus: a synthetic read-only address book `google:other-contacts`
(no create / update / delete) appears in `address_books_list` carrying
`ContactCorpus::OtherAutoCollected`, and a `contacts_list` scoped to that
book id routes to `/v1/otherContacts` (read-mask limited to
names / emails / phones / metadata), stamping each card
`OtherAutoCollected`. Every other book, group, and card is
`ContactCorpus::Main`; `contact_get` derives the corpus from the resource
name (`otherContacts/*` -> auto-collected). The discriminator lets the
consumer route the two corpora to distinct local stores without matching
on the provider.
Update fetches the raw `Person`, requires the server ETag, and replaces only
fields named by the shared `ContactPatch`. Display-name updates rewrite the
first modeled name's given/family split while preserving unmodeled People name
fields and additional name entries. Custom email and phone labels use People
`type=custom` plus `formattedType`; postal addresses map through the shared
`ContactAddress` model. Contact search sends the People empty-query warmup
before `searchContacts` so the cache is populated after mutations. The account
exposes the synthetic `google:contacts` address book plus People
`contactGroups.list` groups. List/search accept no id, `google:contacts`, or
`contactGroups/*` ids; group-scoped queries filter returned contacts by
`contactGroupMembership`, and group-scoped creates include the matching People
membership. Contact photos are read-only through the shared `photo_url`; URL
update attempts return unsupported because People `updateContactPhoto` requires
image bytes, not a URL. Raw `ContactPatch.photo` updates call
`updateContactPhoto` (its field mask travels in the body, not the URL), and
clearing that field calls `deleteContactPhoto`.

`directory_search` (organization directory / GAL, distinct from the personal
`contact_search` corpus) returns `Page<DirectoryCard>`. An empty query
enumerates via `people:listDirectoryPeople` (no warmup); a non-empty query runs
`people:searchDirectoryPeople`, preceded by an empty-query warmup like the
`searchContacts` family. Both use `sources=DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE`
and a `names,emailAddresses,phoneNumbers,organizations` readMask; email-less
rows are dropped, the first email is the key and the rest fill
`additional_emails`. Accounts with no directory (personal Gmail, or missing the
`directory.readonly` scope) answer 403; the impl swallows the directory-absence
classifications (`PermissionDenied`, `InsufficientScope`) to an empty page,
while `PolicyBlocked` (a real admin refusal) propagates.

Google Calendar RSVP fetches the raw event, updates only the matching
account attendee's `responseStatus`, and sends the attendee array back
with unmodeled attendee fields preserved. Event status maps through Google
Calendar `status` on read, create, and update. Event search uses the
requested calendar when supplied; otherwise it walks the calendar list and
searches each, using an internal cursor that records the calendar id plus
the provider page token. `calendars_list` walks every `calendarList` page at
the provider's 250-item maximum, rejects repeated page tokens, and has a
finite 10,000-page request budget. Each page is one ordinary request
against `www.googleapis.com`, so it debits the host `cost_default` of one
unit through the same governor as before; the paginated walk costs units
in proportion to the pages the provider actually hands back, and no
Calendar traffic rides for free. Calendar ids occupy path components on
event resource URLs, while the `events.move` destination occupies a query
value and uses the corresponding encoder. The two encoders differ only on
the complete `.` and `..` components, which the path encoder
double-escapes so the URL parser cannot resolve a provider id as path
navigation; in a query value dots carry no structural meaning. Cross-calendar
search respects the requested `limit`, never over-fetching at a calendar
boundary and clipping any loose provider page. `events_in_range` requests
`showDeleted=true` alongside `singleEvents=true`, so cancelled recurring
instances remain visible as `EventStatus::Cancelled` instead of disappearing
from a range reread. Google may return such tombstones with only
`originalStartTime`; the projection uses that value for both required time
fields so the stable instance id and cancelled status can cross the shared
`CalendarEvent` surface. `is_all_day` is read off that EFFECTIVE start rather
than off `event.start`, because a cancelled instance of an all-day recurrence
carries only `originalStartTime.date` - reading `event.start` alone paired
date-valued times with `is_all_day: false`, which the shared surface treats as
a timed event. Calendar update
uses `events.move` when `EventPatch.calendar_id` targets a different
calendar, then applies any remaining field patch against the destination.
Patch-local validation runs before the move, so a patch that could never
be applied cannot strand the event in the destination calendar.
That provider operation is non-atomic. If the move succeeds and the field
PATCH fails, the returned error is `Protocol(PartialResponse)` with an
acknowledged attempt cause, which directs the consumer to reconcile the moved
event. Because the primary kind changes, the reclassification builds a
fresh `AccountErrorBuilder` rather than taking the `into_builder`
decoration path, and copies across by hand everything a consumer or a
support export would otherwise lose: the `ErrorScope::Calendar` naming
the event under its *destination* calendar, the provider and protocol,
the HTTP status, request and trace ids, native code, the tagged
diagnostic text, and the entire original cause chain as secondary
evidence. There is no compensating move because that would add another
blind write and another partial-failure window.
A present `EventPatch.recurrence` always writes the `recurrence` key; an
empty `EventRecurrence` sends `[]` (the Google idiom for clearing
RRULE/RDATE/EXDATE), while an absent recurrence patch omits the key.
Google carries the timed/all-day distinction in the `start`/`end` shape
(`date` vs `dateTime`), not a flag, so an `is_all_day` flip is rejected
as unsupported unless the patch also carries both `start` and `end`.

Calendar mutations advertise `MutationConcurrency::None` and write blind:
`event_update`/`event_delete`/`event_rsvp` send PATCH/DELETE without
`If-Match`, so the read `GoogleEvent.etag` does not guard the write.
`event_rsvp` reads the event, mutates one attendee's `responseStatus`, and
writes the whole attendee array back, so a concurrent attendee edit between
read and write is clobbered (a lost-update window). This matches the
no-optimistic-concurrency posture; the engine's read-back guard is the
safety net, and wiring `If-Match` is a deferred concurrency-model decision.

## Cursor envelope

`GmailChangeState` carries:

- `history_id: u64`.
- `profile_email: String`.
- `schema_version: u8` pinned at `1`.

`encode_gmail_state` serializes to JSON and wraps as
`OpaqueChangeState { protocol: ProtocolKind::Gmail,
envelope_version: 1, bytes }`. Serialization is an invariant boundary:
the data-only state must serialize, and an impossible encoder failure
panics instead of minting an empty poisoned cursor. `decode_gmail_state` rejects
wrong protocol, wrong envelope version, and wrong schema
version, mapping each to `AccountError::SchemaIncompatible`.
JSON deserialization errors map to `AccountError::Other`.

`decode_gmail_state_for_profile` layers an identity check on top: the decoded
`profile_email` must equal the open account's `email_address` under an
ASCII-case-insensitive comparison. A mismatch
returns `AccountError::Other` describing both sides, preventing a checkpoint
from one Google account being replayed into another (for example after the
consumer rotates accounts under the same persistence key).

`cursor_from_state` projects to
`ChangeCursor { scope: CursorScope::Account, server_state,
advanced_through: None, envelope_version: 1 }`. There is no
`advanced_through`; Gmail does not page over time intervals.

## Per-scope inventory / changes / hydration

`CursorScope::Account` is the only scope. `inventory_stream`,
`establish_initial_cursor`, `push_subscribe`, and the change
stream each reject any other scope with a
`SyncEvent::Terminated(AccountError)` whose kind is
`Unsupported(_)` and recovery is `Unsupported(_)` - the central
recovery mapping resolves an `Unsupported` kind to the terminal
`Unsupported` `RecoveryClass`.

`inventory_stream` samples `users.getProfile` before the first list
page, then walks `users.messages.list` in pages of 500 ids and
hydrates each page through `users.messages.get` with the `metadata`
format under `buffer_unordered` concurrency of 32. Page boundaries are
`PageBoundary::Page` for intermediate batches and `PageBoundary::Final`
for the last batch. The final batch is emitted even when it has no items,
so an empty mailbox or a terminal page whose listed messages all vanished
still exposes the boundary and checkpoint. A message-level
`NotFound(Message)` during hydration is discharged as the ordinary list/get
deletion race - the id came from this walk's own listing and the cursor is
anchored before the walk, so absence is the correct inventory state. Every
OTHER classified hydration failure becomes an
`InventoryObligation::Object` (key `gmail:message:<id>`, stable across
walks so the same unreadable message re-raises the same key; no
provider-native repair token, since a Gmail message is re-readable from
its id alone) and the walk CONTINUES. Terminating instead used to discard
every page already emitted for one unreadable object, then hit the same
object on the retry, forever. Each emitted batch and the terminal
completion carry an `InventoryCoverageReport` over
`CoverageDomain::full(scope)`: complete while no obligation exists,
degraded and naming every unresolved obligation from the first one onward
- a checkpoint certifies the results before it, so a page checkpoint
claiming `Complete` would let the cursor advance past an object nothing
recorded. A LIST-page failure (as opposed to a per-object hydration
failure) still terminates the walk.

The checkpoint that final batch and the terminal `Done` carry is
derived from the pre-walk profile sample, and **the engine does not
read it**. Gmail answers `establish_initial_cursor` with
`CursorEstablishment::Ready`, not `EstablishViaInventory`, so the
change cursor is anchored at `open()` from `seed_state` and
`changes_stream` starts from it immediately, in parallel with the
inventory walk - a message that races the walk is caught there, not by
anything inventory emits. Gmail's inventory therefore only ever feeds
the backfill path (the default `inventory_partition_stream` resolves
`InventoryPartition::Full` to `inventory_stream`), and
`BackfillRunner::run_partition` substitutes its own
`Checkpoint::Backfill` on every page and discards the terminal `Done`
checkpoint outright.

Sampling the profile before rather than after the walk is kept because
it is the honest ordering for a value that claims to describe the start
of the pass, but it is presently inert: nothing downstream consumes it.
Emitting a `Checkpoint::Change` at all is a mild deviation from the
`inventory_stream` contract, which reserves the terminal checkpoint for
scopes that returned `EstablishViaInventory`. Revisit both if Gmail
ever moves to inventory-established cursors.

Inventory and hydration resolve the label vocabulary through
`labels_for_flags` before canonicalizing flags, never through
`snapshot` directly. `ScopeSnapshot.fetched_at` is an
`Option<Instant>`, so a cache that has never been populated is stale
regardless of age while a successful fetch that returned no labels is
fresh for the usual five minutes. If the initial refresh fails with
nothing cached, the operation fails rather than canonicalizing against
an empty vocabulary; a populated stale cache stays usable when a later
refresh fails.

`scope_lifecycle_stream` owns a private `last_emitted: Option<ScopeSnapshot>`.
Its first successful label fetch seeds that state and emits no Created events.
Later polls diff only against the last snapshot observed by that stream, so a
flag mutation, hydration batch, inventory prelude, or membership discovery that
refreshes the shared vocabulary cannot consume lifecycle changes. Rename events
carry the same stable label id in `old` and `new`; they are invalidation
signals telling the consumer to re-read container metadata, not a
carrier for the old and new display names.

The poll cadence is a separate piece of state, `next_delay: Option<Duration>`,
and NOT a function of `last_emitted`. `None` means no poll has been attempted
yet and the first one runs immediately; every later attempt waits. Keying the
delay off the baseline snapshot instead conflated "a poll has happened" with "a
poll has succeeded", so a retryable `labels.list` failure before the first
successful snapshot re-issued the request with no delay - an outage became a hot
request loop. A retryable failure doubles the wait from
`LIFECYCLE_POLL_INTERVAL` up to `LIFECYCLE_MAX_BACKOFF`; the reset happens only
on a `labels.list` that actually completed, the same rule bifrost-graph's EWS
reconnect backoff follows for the same reason. The wait is a `select!` against
the shutdown token, so `close()` ends the stream where it happens rather than at
the end of the backoff. Terminal and engine-action classes still end the stream
with `ScopeLifecycleEvent::Terminated` instead of backing off.

`get_stream` consumes a stream of `ObjectId`s in batches of 32
and emits `AccountStream<SyncEvent<ItemOutcome<HydratedObject>>>`.
Per-item hydration outcomes flow as `ItemOutcome::Succeeded` for a
hydrated message and `ItemOutcome::Failed(BatchFailure)` carrying
the classified `AccountError` for an id that Gmail refused or that
parsed badly. A single bad id no longer poisons the stream. The
boundary a hydration batch carries is derived only from what draining
the id stream already revealed: a batch cut short because the id stream
closed is `PageBoundary::Final`, every other batch is `PageBoundary::Page`,
and an empty input emits only `Done(None)`. A last batch that happens to
fill exactly to the 32-id hydration width therefore stays `Page`, with the
terminal `Done(None)` as its only end marker. This is deliberate and must
not be "fixed" with a one-item lookahead: the ids arrive from a
backpressured producer that may be waiting on hydration output before it
yields again, so polling for one more id before hydrating the batch
already in hand deadlocks the two sides against each other. The
dispatch per `Projection` is:

- `FlagsOnly` - `format=minimal` then `flag_set` over the label list.
- `Metadata` - `format=metadata` via `inventory_entry_from_message`.
- `FullWithBlobs` - `format=raw` for bytes plus `format=full` to
  enumerate attachment blob handles.
- `Headers` / `Preview` / `TextOnly` / `Full` - `format=raw`, with
  `HydratedObjectKind::RawMime`.
- Any other variant falls back to `format=metadata`.

A hydration batch is issued CONCURRENTLY at width 32 (`buffer_unordered`),
matching the fan-out `inventory_stream` already used, and each future carries
its own id alongside its result so a per-item failure still names the object it
could not represent. The label vocabulary is resolved once per batch and
projected into a `LabelNameIndex` (id -> name) once, rather than rebuilt inside
`canonical_flags` for every message; `flags::canonical_flags` and
`flags::flag_set` remain as the by-slice entry points over the same index.
`labels_for_flags` is single-flight: a stale cache is refreshed under a
`tokio::sync::Mutex` in `ScopeCacheState`, with the staleness re-checked after
the lock, so 32 concurrent hydrations issue one `labels.list` rather than 32.

### Shutdown observation

`inventory_stream`, `get_stream`, `changes_stream` and the mutation driver all
take the account's `CancellationToken` and select on it around every provider
call and every wait on an input stream. Without it, a stream that outlived
`close()` kept calling Gmail through a deregistered `AccountNet` - unmetered,
and reporting the result as a transport fault rather than as a closed account.

The rule for the READ streams is that cancellation ends them SILENTLY: they
return without a `Done`, a `PageBoundary::Final`, a checkpoint or a coverage
report. A dropped in-flight read loses nothing, and the one thing that would be
unsafe - a partial walk published as complete - is exactly what is excluded.
Hydrated items already collected for an unfinished page are discarded with it.

The mutation driver cannot follow that rule, and does not; see the mutation
pipeline below.

`changes_stream` decodes the cursor, re-verifies
`profile_email`, then `users.getProfile`-checks that the open
account still matches the cursor's recorded identity. A drift
yields a `SyncEvent::Terminated(AccountError)` with kind
`SyncState(SchemaIncompatible)`, which the central mapping
resolves to `Engine(SchemaIncompatible)`. Email identity comparisons
are ASCII-case-insensitive in both cursor decoding and the live profile
check - `getProfile` returning different casing must not wedge the account into
a permanent `SchemaIncompatible`-clear-re-establish cycle.

This per-poll round trip is **kept deliberately**, and the cost objection to it
has already been raised and answered. It genuinely doubles the request count and
the failure surface on the 30-second poll path. It is also the only thing today
that catches a rotated token now pointing at a DIFFERENT Google account before
that account's history is mixed into the existing slot, and there is no
token-source identity binding upstream to lean on. It comes out when that
binding exists, not before; do not re-file it as free savings.

With the identity confirmed, the stream pages `users.history.list` from
`startHistoryId`. Intermediate pages emit `checkpoint: None`, because Gmail's
opaque page token is not represented in the cursor. Each response `historyId`
is the mailbox's current position when that page is requested, not a marker for
how far the response snapshot has been consumed. The walk therefore retains the
first page's `historyId` and uses that value for the final
`Checkpoint::Change`. Records arriving while later pages are fetched may replay
on the next walk, but cannot fall below a checkpoint the walk did not earn. The
stream then terminates with `SyncEvent::Done(None)`.

History entries map to `Change` variants in
`changes_from_history`:

- `messagesAdded` -> `ObjectChange::Created` plus one
  `ScopeChange::Added` per label on the new message.
- `messagesDeleted` -> `ObjectChange::Destroyed`.
- `labelsAdded` / `labelsRemoved` -> `ScopeChange` rows scoped
  per label.

## Paging inventory

Google has no universal paging helper, but every internally traversed walk has
its own repeated-token detection and 10,000-page refusal budget. The production
paging sites are:

- Gmail `search` and `search_messages` each request one provider page and return
  its `nextPageToken`. They are bounded by one request per call and resume at a
  page boundary, not inside an over-delivered page.
- Gmail `inventory_stream` traverses `users.messages.list` with repeated-token
  detection and a 10,000-page budget. It has no durable mid-walk resume token;
  cancellation or refusal terminates without `Done`, a final boundary, or a
  checkpoint, so partial enumeration can never claim full or degraded coverage.
- Gmail `changes_stream` traverses `users.history.list` with repeated-token
  detection and a 10,000-page budget (`MAX_HISTORY_PAGES`). It cannot resume
  mid-walk, and only its final batch checkpoints, using the first page's history
  boundary as described above - so a refusal must terminate rather than truncate,
  or the walk would emit a checkpoint for ground it never read. Both guards
  therefore produce `SyncEvent::Terminated` carrying
  `Protocol(ContractViolation)`, discarding the page in hand; the next walk
  restarts from the same unchanged `startHistoryId`.
- People `address_books_list` traverses `contactGroups.list` with repeated-token
  detection and a 10,000-page budget. It returns `Err` on refusal, never the
  accumulated prefix as a complete address-book list.
- People personal-contact list, other-contact list, contact search,
  other-contact search, autocomplete, and directory search each request one
  provider page and return its token. They are bounded by one request per call
  and resume at page boundaries. None clips an over-delivered page, so none needs
  an intra-page offset to preserve returned items.
- Calendar `calendars_list` traverses the calendar list with repeated-token
  detection and a 10,000-page budget. It returns only after the traversal, so it
  cannot resume mid-walk.
- Calendar `events_in_range` and a search constrained to one calendar each
  request one provider page and return its token. They are bounded by one request
  per call and resume at page boundaries.
- Cross-calendar event search first runs the bounded `calendars_list`, then
  walks the finite calendar vector until the requested result limit or a provider
  page boundary. Its cursor records calendar id plus provider page token, so it
  resumes at either boundary. It does not encode an intra-page offset; the code
  relies on Google's `maxResults` contract and defensively clips a loose page.

## Repairing inventory coverage debt

Gmail is the first `Account::repair_inventory` implementor, and it exercises the
object lane only - it has no region obligations to repair.

`repair_inventory` re-reads each requested message with
`users.messages.get(format=metadata)` and rebuilds its `InventoryEntry` from
that fresh read. Rebuilding is the point, not a side effect: the obligation was
raised because the object could not be REPRESENTED, so confirming it still
exists proves only half of what is owed. The rebuilt entry crosses the Account
boundary as proof and is then discarded by the engine, which publishes only the
id - the same thing a successful walk publishes.

A `NotFound` becomes `DefinitiveIrrelevance::AbsentUnderCursorBridge`, and the
reason is the cursor model rather than the status code. The scope cursor is
anchored at the `historyId` sampled BEFORE the walk that raised the obligation,
so any deletion since then is carried by the change stream the consumer is
already reading; absence is therefore the correct inventory state and nothing is
owed. Under a different pagination or cursor model the identical `NotFound`
would NOT be dischargeable, which is why the conclusion is typed and carries its
authority rather than being inferred from the error kind. This is the same
argument the inventory walk already uses to discharge a message deleted between
`users.messages.list` and `users.messages.get`.

Everything else defers, costing one attempt against the lineage budget. A region
request reaching this account is an engine bug; it is answered with `Deferred`
rather than dropped, so the one-terminal-outcome-per-request contract holds even
then.

The label map is fetched once per pass. If that fetch fails no per-request
conclusion is possible, so the stream terminates and the engine converts every
outstanding attempt to a local deferral - it must not record a conclusion the
account never reached.

## Push: Cloud Pub/Sub

Push is out-of-process. `PubSubConfig` holds a `topic` and an
optional `label_ids` filter. Wiring is opt-in: an account opened
without `with_pubsub_config` rejects `push_subscribe` with
`AccountError::Unsupported`.

`push_subscribe` sends a command to the watch actor, which issues `users.watch`
with the configured topic and atomically enters
`Watched { history_id, expiration }`. The returned `SubscriptionHandle` is a
JSON envelope `{ topic, history_id, expiration }`.

A subscribe emits `WatchEvent::Reconnected` only when the actor's
`disconnected` latch is set - that is, only when it genuinely reconnects.
A FIRST subscribe is silent: it is the consumer's opening act, so
announcing a reconnect from there published an event before any consumer
could hold a `push_stream()` receiver, and `broadcast` drops messages with
no receivers, making the stream's first observable state a function of
task scheduling rather than of the watch.

One actor task owns the four-state lifecycle (`Unwatched`, `Watched`, `Renewing`,
`Retired`), the active handles, subscription commands, renewal timing, and close.
There is no renewer handle for another task to clear or restart. Its renewal arm:

- Sleeps until `renewal_delay(expiration)` - one day before
  the expiration timestamp from Gmail, or `DEFAULT_RENEW_AFTER`
  (six days) if no expiration was returned. Computed renewal delays
  have a five-minute floor, including already-expired timestamps, so
  a short or unchanged watch expiration cannot create a successful
  hot renewal loop.
- Re-issues `users.watch` and replaces history id and expiration together in one
  state transition.
- On failure: classifies via
  `error::into_account_error(_, GmailErrorContext::push_subscribe())`
  and routes on `RecoveryClass::is_terminal()`. Terminal classes
  (auth lost, policy block, account disabled, schema break) emit
  `WatchEvent::Terminated(AccountError)` and returns to `Unwatched`, so a later
  subscribe command starts a fresh watch and renewal schedule. Transient classes
  emit a structured
  `WatchEvent::Warning` per failure (support-only text carrying the
  message key) plus `WatchEvent::Disconnected` (once) and retry after
  `RENEW_RETRY_AFTER` (five minutes); the next success emits
  `WatchEvent::Reconnected`. Every failure goes
  through the classifier first. The warning's `retry_count` is the number
  of CONSECUTIVE renewal failures, cleared by any completed request
  (subscribe or renewal) and by a terminal classification, so an operator
  reading the lane can tell one stumble from a sustained outage.
- Selects on `shutdown.cancelled()` against the renewal sleep, biased so that a
  cancelled account retires rather than renewing when both are ready in the same
  poll. Cancellation moves the actor to `Retired`.
- Deliberately does **not** race `shutdown.cancelled()` against the `users.watch`
  request itself, on either the renewal or the subscribe path. An unbiased
  `select!` between the token and the request can pick the completed request, and
  any commit after that point installs a watch nobody renews and no `close()`
  retires - the orphan-watch failure the shutdown check exists to prevent. The
  request runs to completion and `commit_watched` decides afterwards whether we
  are still allowed to own it. It is the single place a response becomes
  `Watched`; if the account retired underneath the request it pins `Retired`,
  issues a best-effort `users.stop` for the watch it just refused, and reports
  failure. A check on the committed value cannot lose that race, where a check in
  a `select!` arm can.

  Subscribe additionally checks shutdown before queueing the command and again on
  entry to the actor, so the common post-close case costs no wire traffic at all.
  The handle envelope is encoded before the commit: an encode failure after it
  would leave `Watched` installed with no handle in the set, so `close()` would
  skip `users.stop` and the renewer would keep the orphan alive.

A successful subscribe clears the `Disconnected` latch so `Reconnected` stays
edge-triggered, but deliberately does not clear the transient-failure
`retry_after` damper: a backoff reset by a *subscribe* rather than by a completed
request is how a subscribe-then-die loop escapes its backoff. The damper also
survives unrelated actor traffic: the per-loop delay computation reads it
without consuming it, and it is cleared only where the renewal timer actually
fires or a fresh lifecycle state is installed. Consuming it during the
computation let any command landing inside the five-minute backoff erase it,
and for a watch with no expiration the recomputed delay then fell back to the
six-day default.

`push_subscribe`, renewals, `push_unsubscribe`, and close are actor messages or
actor-owned transitions, so no lifecycle state is assembled from independently
locked fields. A renewal cannot recreate a watch after a successful stop, and a
concurrent subscribe cannot be stopped by the preceding teardown.
`push_unsubscribe` decodes the handle envelope, and on a `Retired` lifecycle
returns `Ok(())` with no wire call - `Retired` is absorbing, `close()` has
already stopped the watch and cleared the handle set, so a late unsubscribe is
idempotent teardown rather than a reason to issue a post-close `users.stop` or
to overwrite `Retired` with `Unwatched`. Otherwise it removes the handle from
the active-handle set. A non-last known handle, or an unknown handle while other
known handles remain, is a no-op. When the set is empty, including on a fresh
process receiving a persisted handle, it calls `users.stop`. A failed stop for
the last known handle re-inserts that handle and leaves the `Watched` state
intact so the engine can retry. A successful stop
enters `Unwatched`. The active-handle set lets multiple
subscribers share one Gmail watch; correctness after restart does not depend on
that in-memory set surviving.

`push_stream` is a `broadcast::Receiver<WatchEvent>` adapter
with shutdown wiring. `Lagged` is treated as a skip rather than
an error.

The Pub/Sub message payload itself is not parsed by this crate;
the consumer subscribes to the Pub/Sub topic out of process,
decodes the JSON envelope, and feeds the change hint into the
engine's `InvalidationSink`. The Account layer only owns the
watch lifecycle.

## Mutation pipeline

`bulk_set_flags`, `bulk_move`, `bulk_move_from`, and `bulk_destroy`
share a single `mutation_stream` driver:

- Drain up to 1000 ids per round (the Gmail `batchModify` /
  `batchDelete` cap).
- Translate the operation once per stream: `SetFlags` runs
  through `translate_flag_op` against the cached label list;
  `Move` builds an add/remove `LabelPatch` via `move_patch` from the
  destination and the optional source (see the relocation rule above);
  `Destroy` carries no translation.
- Gmail is the reason `Account::bulk_move_from` exists: `batchModify`
  carries `addLabelIds` and `removeLabelIds` in one request, so the
  source detach is free here, where the destination-only `bulk_move`
  forces a consumer into one `remove_from_container` PER ID. `bulk_move`
  is `bulk_move_from` with `source: None`.
- Post the batch:
  - `SetFlags` / `Move` -> `users.messages.batchModify`.
    A 404 from a multi-id request is ambiguous because Gmail rejects the whole
    request when any one message is absent. The driver bisects the batch, keeping
    successful sub-batches in the success lane and reducing 404 sub-batches to
    singletons, so only genuinely absent ids receive `NotFound(Message)`. Bisection
    is used instead of per-id replay because the common sparse-failure case takes
    logarithmic rounds rather than one extra request per message.
    Every non-404 sub-batch error inside the bisection goes back through
    `mutation_error`, exactly as the unsplit call does, so a rate limit,
    transport fault, auth loss, or engine directive arriving mid-bisection still
    terminates the stream instead of being laundered into per-id failures. The
    sub-batches already resolved when that happens are emitted as one final
    non-final page ahead of `Terminated`, so no id the driver resolved loses its
    lane; ids never transmitted stay unreported, which is what `Terminated` has
    always meant.

Shutdown of a mutation stream follows the TRANSMISSION EVIDENCE, not the loop
that caught it. Before dispatch, cancellation ends the stream silently: no byte
has crossed the side-effect boundary for the batch in hand, so nothing is owed.
Once `batchModify` / `batchDelete` has been dispatched, dropping the future
would lose writes Gmail may already have applied, and reporting the ids
`Failed` would assert they did not land. The driver instead emits every id of
that batch as `ItemOutcome::Uncertain`, on a `PageBoundary::Page` batch (never
`Final` - the operation did not complete), followed by `SyncEvent::Terminated`
carrying `Transport(Network)` with a secondary
`AttemptCause(TransmissionState::InFlight)`. That is the same rule
`bifrost-imap` applies to an `InFlight` drop, and it is what routes a
non-idempotent mutation to reconciliation rather than to a blind retry. The
select is `biased` toward the request so an answer that has already arrived is
classified normally even when the token fires in the same poll, and the
driver's pending-event slot is drained BEFORE the shutdown check so the
terminator parked behind the uncertain lanes cannot be swallowed by the token
that produced it. A mid-bisection shutdown reports every id of the batch as
`Uncertain`, including sub-batches already resolved - an over-approximation on
the safe side, since `Uncertain` queues for read-back.
  - `Destroy` -> `users.messages.batchDelete`. On a parsed 403 Gmail reason of
    `forbidden` or `insufficientPermissions` it falls
    back to a label patch that moves the messages into `TRASH`,
    matching the scopes Gmail OAuth tokens with the
    `gmail.modify` scope can perform but `gmail.metadata` cannot.
    An unparseable 403 body follows ordinary classified failure handling and
    never triggers the downgrade.

    A successful fallback reports `MutationSuccess::Downgraded { actual:
    MovedToContainer(TRASH) }`, never
    `Applied`: those messages moved to Trash and still exist. Reported as
    `Applied` they came back on the next inventory or history pass, were
    destroyed again, and the account sat in a permanent reconcile loop - on
    the ordinary `gmail.modify` scope, which triggers this fallback by design
    rather than by misconfiguration. `bifrost-sync` read-back-verifies the
    `Downgraded` lane instead of trusting it, and a trashed message still
    hydrates, so the guard files it `still_failed`: honest, and terminal.

    Only ids the trash patch reported as SUCCEEDED are downgraded
    (`downgrade_succeeded_outcomes`). An id whose trash patch failed was not
    downgraded - it was not mutated at all - and keeps its classified failure.
    That helper is the crate's only producer of `Downgraded`, so removing the
    call site fails the build as dead code; the helper's own semantics,
    including the lane-preservation rule, are pinned by
    `the_destroy_trash_fallback_downgrades_only_what_it_trashed`. The scripted
    transport test `destroy_scope_failure_sends_trash_fallback_and_reports_downgraded`
    drives the production 403 classification, asserts both request bodies, and
    pins the downgraded outcome without a socket.
- The driver reads one item ahead at the 1000-item boundary. Every
  final mutation batch is marked `PageBoundary::Final`, including a
  stream whose item count is exactly divisible by 1000, followed by
  `SyncEvent::Done(None)`.

Result classification:

- All ids in a successful batch -> `ItemOutcome::Succeeded` with
  `MutationSuccess::Applied`.
- A legitimate empty patch (label op with no changes) ->
  `ItemOutcome::Succeeded` with `MutationSuccess::Skipped` per id.
- A `Destroy` that fell back to a TRASH patch for scope reasons ->
  `ItemOutcome::Succeeded` with `MutationSuccess::Downgraded { actual:
  MovedToContainer(TRASH) }` per
  successfully-trashed id (see the `Destroy` bullet above).
- A non-label move scope is malformed caller input and produces
  `ItemOutcome::Failed` per id with a `Request(Malformed)` account error.
- Unsupported flags do not prevent representable flags in the same operation
  from reaching Gmail. Successfully modified ids produce a
  `MutationSuccess::Downgraded { actual: FlagsPartiallyApplied { unsupported } }`
  outcome, preceded by a `WarningKind::StrategyDowngraded` warning. This replaces
  the earlier deliberate all-or-nothing policy.
- An operation with NO representable half sends no request and changes nothing,
  so it takes no success lane at all: every id gets `ItemOutcome::Failed` with
  `Unsupported(UpdateFlags)` -> `RecoveryClass::Unsupported`, alongside the same
  warning. `Downgraded` is not available here - its published contract in
  `crates/types/src/error/stream.rs` requires that the target actually changed,
  and `bifrost-sync` files every downgrade as `PendingReadback`, which would
  schedule a read-back for a mutation that never ran. `Skipped` is equally wrong:
  it claims the target was already in the requested state.
- A retry-class or auth-class error -> stream-level
  `SyncEvent::Terminated(AccountError)` so the engine can re-issue
  via `error.recovery()`. The driver does not split applied vs
  failed across the same batch on HTTP failure.
- Any other error -> per-id `ItemOutcome::Failed(BatchFailure {
  error, .. })`, carrying the projected `AccountError`.

Flag canonicalization in `flags.rs`:

- Gmail labels project to IMAP-style flags: `UNREAD` toggles
  `\Seen` (presence of `UNREAD` removes `\Seen`), `STARRED` is
  `\Flagged`, `DRAFT` is `\Draft`, `IMPORTANT` is
  `$Important`. Provider system-label comparisons are
  ASCII-case-insensitive. Folder-like labels (`INBOX`, `SENT`,
  `TRASH`, `SPAM`, `CHAT`) drop out. Every other label id - user labels and
  the `CATEGORY_*` system labels alike - projects to
  `$gmail-label:<id>:<name>`.
- The reverse direction in `translate_flag_op` handles `Add`,
  `Remove`, `Patch`, and `Set`. A `$gmail-label:` flag resolves on
  its label id alone; the embedded display name is advisory, so a
  server-side rename does not invalidate a queued mutation. The
  lookup deliberately does not filter on `labelType`, because
  canonicalization mints this spelling for system category labels
  too and a flag this crate itself produced has to translate back.
- `Set` re-derives an add/remove patch over the writable canonical flags
  plus the *user* label vocabulary. Since the patch is built without
  knowing what the target messages currently carry, an exact set
  names every known user label it omits, so `remove_label_ids`
  scales with the account's user label count rather than with the
  incoming flag set. `CATEGORY_*` labels are exempt from that
  re-derivation in both directions - Gmail's classifier owns them,
  so they resolve without poisoning the patch but are never added or
  removed by a `Set`.
- `DRAFT` and `SENT` are read-only projections: Gmail answers 400 for either id
  in `addLabelIds` or in `removeLabelIds`, so neither ever reaches the wire from
  any reverse translation path, including a crafted `$gmail-label:SENT:...`
  spelling. Excluded from the wire is NOT the same as absent from the report,
  though - a flag the driver cannot send is a flag it did not apply - so
  `\Draft` and a read-only `$gmail-label:` flag land in `unsupported_flags` and
  are reported through the partial-application path above rather than dropped.
  Their OMISSION from a `FlagOp::Set` is deliberately not reported: draft-ness
  and sent-ness are structural in Gmail rather than toggles, so every ordinary
  exact set omits them, and reporting each one as partially applied would make
  `Applied` unreachable for `Set` while telling a consumer nothing actionable.
  The residual gap - a `Set` omitting `\Draft` against a message that really is
  a draft - needs a read-back the translation layer does not have.
- Any other unrecognized flag falls into `unsupported_flags`, while its
  representable siblings are still applied as a downgraded mutation.
- A canonical flag set is hashed (FNV-1a) into the
  `Fingerprint.flags_hash` field of an `InventoryEntry`.

Gmail accepts no client-mintable idempotency header on these endpoints, so the
mutation impls take the shared `IdempotencyKey` and hold it engine-side rather
than wiring it onto the request.

## Blobs

`open_blob` decodes the `BlobHandle.id` (a JSON
`{ message_id, attachment_id }` payload), fetches the
attachment, base64url-decodes the body, and emits a single
`Batch { items: vec![bytes], page_boundary: Final }` followed
by `Done`. The batch's `bytes_in` is the exact buffered JSON response-body
length transferred, not the smaller decoded attachment size.

`open_blob_range` always short-circuits with a Fatal carrying
`Unsupported(OpenBlobRange)`. Gmail attachment bodies are base64url inside
JSON and expose no byte-range transport, and a forged handle claiming range
support cannot bypass that provider capability.

`attachments_for_message` walks the MIME tree, surfacing any part
whose `body.attachment_id` is set as a `MessageAttachment` with
`source: AttachmentSource::Blob(handle)`. Inline bodies (no attachment
id) are not surfaced. `message_from_gmail` calls it under `Full` and
`FullWithBlobs`, matching the cross-provider rule that `Full` reports
attachment metadata without bytes and only `FullWithBlobs` implies
fetchable content. `blob_handles_for_message` remains as the
`Vec<BlobHandle>`-only helper `open_blob`'s tests build against.

## Error translation

`account/error.rs::into_account_error(error, ctx)` is the single
boundary converting `crate::Error` (Net / Response / JsonDecode /
Base64 / Local) into an `AccountError`. `GmailErrorContext` carries
the calling operation so `bifrost-types::recovery::derive` produces a
precise `RecoveryClass`; the crate keeps no local classification
tables.

Mapping highlights:

- Stable Gmail reason codes from `GmailErrorEnvelope.primary_reason()`
  route to typed `WireCause::Gmail(GmailSignal::*)` variants and
  the appropriate `AccountErrorKind` (e.g.
  `quotaExceeded`/`rateLimitExceeded` -> `Server(RateLimited)` ->
  `Retry::SameRequest, reason: RateLimited` with the documented
  `throttle_scope`).
- Authentication failures (HTTP 401 or `authError`) ->
  `Authentication(ReauthorizationRequired)` -> `AuthLost`.
- Authorization failures (HTTP 403 outside the quota path) ->
  `Authorization(PermissionDenied)` -> `NoPermission`. The
  `InsufficientScope::needed` carrier is selected per `AccountOperation` via
  `gmail_scope_for` (`gmail.send`/`compose`/`labels`/`modify`/`readonly`/
  `metadata`/`settings.basic`, and `directory.readonly` for `DirectorySearch`).
- Transport network failures (DNS, TLS, timeout) ->
  `Transport(_)` with an `AttemptCause` whose
  `transmission_state` carries the wire-level evidence; the
  central mapping picks `Retry::SameRequest` for idempotent ops
  and `Reconcile` for non-idempotent ops caught mid-flight.
- 5xx and `internalError` -> `Server(Unavailable)` ->
  `Retry::SameRequest, reason: ServerUnavailable`.
- Every `Error::Response` and every post-200 decode failure
  (`JsonDecode`, `Base64`) pushes
  `AttemptCause(Acknowledged)` onto the chain so the central
  mapping never falls back to "absence treated as Unsent" for an
  actually-acknowledged request.
- `failedPrecondition` from the history endpoint ->
  `SyncState(CursorInvalid)`. `failedPrecondition` elsewhere ->
  `ConcurrencyConflict` (etag / version mismatch semantics),
  routed through `Retry::AfterStateRefresh`.
- 404 / 410 on the history endpoint -> `SyncState(CursorInvalid)`
  -> `Engine(RestartScope(scope))`; the scope is always
  `CursorScope::Account`, so this reseeds from `getProfile`.
- 404 on non-message resources routes to the appropriate
  `ResourceKind`: `Draft`, `Identity`, `Vacation`,
  `PushSubscription` (for Pub/Sub watch). Blob 404 maps to the
  parent message's `NotFound(Message)`.
- `Retry-After` is wrapped with `RetryHint::After(Duration)` on the
  originating `ServerCause::{Unavailable, RateLimited, QuotaExhausted}`
  and forwarded to `RetryAdvice::retry_hint`; no separate
  `retry_not_before` side-channel.
- Local validation failures (`GmailLocalError::*`) ->
  `Request(Malformed)` or `Request(InvalidArgument)` ->
  `ClientBug`. The identity-mismatch and cursor-envelope
  variants produce `SyncState(SchemaIncompatible)` ->
  `Engine(SchemaIncompatible)` so the engine clears the cursor.

`mutation_error(ids, error, ctx)` is the ordinary per-id fan-out for batched mutations:
it translates the crate-level error once via `into_account_error` and produces
one `ItemOutcome::Failed(BatchFailure { error: account_error.clone(), .. })` per
id - `AccountError` is `Arc<Inner>`-backed, so the clones share storage. The
mutation driver intercepts ambiguous multi-id `batchModify` 404s before this
fan-out and isolates them by bisection.

## Known limitations

- Only `CursorScope::Account`; non-Account scopes return `Unsupported`.
- No blob range (`BlobRangeSupport::No`); push requires a consumer-supplied
  Pub/Sub topic (`with_pubsub_config`), else `push_subscribe` is `Unsupported`.
- `historyId` expiry is reactive (no advertised TTL); mutation concurrency and
  replay safety are `None` (no `If-Match`, no dedup header on `batchModify`,
  calendar writes blind) - the read-back-after-retry path is the safety net.
- No independent message attachment-upload primitive (callers send inline bytes;
  over-limit attachments go through `host_attachment` -> Drive); flat labels, so
  `container_move` is unsupported; no storage-quota bytes, so `quota_get` is
  unsupported; replied/forwarded are not writeable Gmail flags.
- Server-side Gmail filters map to typed rules. Direct criteria
  (`from`/`to`/`subject`/attachment/size) plus native query strings surface as
  `FilterCondition::ProviderExpression`. Writes reject names, disabled rules,
  stop-processing, and unstorable actions. `filter_update` is unsupported.

### Accepted residuals

Assessed and deliberately left as they are. Open work sits in `notes/todo.md`;
these are decisions, not gaps.

- **Cross-calendar event update is non-atomic.** Google exposes the move and the
  field PATCH as separate requests. A second-leg failure returns
  `Protocol(PartialResponse)` scoped to the event in its DESTINATION calendar
  (the caller's `source::` composite id no longer addresses it), carrying
  provider, protocol, HTTP status, request and trace ids, native code, both
  diagnostic tiers and the whole original cause chain as secondary evidence,
  with `Attempt(Acknowledged)` pushed first so `derive` reads it. No
  compensating move is attempted - that adds another blind write and another
  partial-failure window. Note that a reclassification must carry the evidence
  the consumer needs to act: a `Reconcile` directive that does not name its
  target is barely better than the silent partial write it replaces. Because
  `into_builder` is decoration-only and exposes no kind-changing path, a fresh
  builder plus hand-copied evidence is the contract-correct route here, not a
  shortcut.
- **A `close()` future dropped mid-`users.stop` leaves the Gmail-side watch
  running until it expires.** The local half is cancellation-safe at every await
  - `closed` and `shutdown.cancel()` happen synchronously before the future
  exists, and the bifrost-net detach lives in a drop guard constructed BEFORE
  `Box::pin` and moved in. (A guard built inside the async block is never built
  at all if the future is dropped before its first poll, and with `closed`
  already set `Drop` would skip the detach too, leaking the rate-limiter
  registration unreclaimably - the same shape as the bug the guard was added to
  fix.) Only `users.stop` remains inside the future, because it needs the
  transport the detach sheds, so retrying it after a cancelled close is not
  possible.
- **`open_blob_range` always returns a classified `Unsupported(OpenBlobRange)`**,
  including for a forged handle claiming `supports_range`. That is a decision
  that Gmail attachments have no byte-range transport, not a stub awaiting
  implementation. The earlier "defensive" branch returned `stream::empty()`,
  which is a silently-ended stream for any consumer awaiting bytes.
- **`get_stream` marks `PageBoundary::Final` only from information the drain
  already has.** A batch cut short by the id stream closing is `Final`; a last
  batch that fills exactly to `HYDRATE_BATCH_SIZE` stays `Page` with the
  following `Done` as terminator. **Do not "finish" this with a one-item
  lookahead.** The ids come from a backpressured producer that may be waiting on
  hydration output before it yields again, so polling for id 33 before hydrating
  ids 1-32 parks both sides forever - and short of a deadlock it holds every
  partial batch until one more id arrives. `bifrost-sync` reads `Final` in no
  hydration path, so the boundary is advisory; losing it on one alignment is far
  cheaper than a stall.
- **Inventory has no per-item failed lane; unreadable objects travel as
  coverage debt instead.** `InventoryEvent` carries no `ItemOutcome` wrapper.
  The list/get deletion race (`NotFound(Message)`) is discharged outright, and
  every other classified hydration failure is recorded as an
  `InventoryObligation` on the batch and completion coverage reports (see the
  inventory section above) and repaid through `repair_inventory` - not
  surfaced per item, and no longer a reason to terminate the walk.
