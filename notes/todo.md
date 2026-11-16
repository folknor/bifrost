# TODO

## What belongs here, and what does not

This file holds OPEN work: defects to fix, decisions the repository owner
has not made, and ruled work that has not landed. Nothing else.

- **A completed item is DELETED**, never annotated DONE or CLOSED. Its
  "what" is in the code and `reference/`; its "why" is in git history.
- **A known limit, an accepted non-defect, a deliberate trade, a coverage
  boundary, or a "do not re-file this" marker does NOT go here.** It goes
  to an inline comment at the code it describes - the function, constant,
  match arm or test whose behaviour it explains - written so the next
  reader of that code understands the limit, why it is accepted, and what
  would change it. Where a consumer relies on it, it is also stated in the
  crate's `reference/` doc. A bullet in this file is invisible from the
  code, and every past close-out has had to move such bullets there by
  hand; put them there directly.
- **A rejected proposal gets the same treatment**: a comment at the code
  the proposal would have changed, stating the ruling and its condition
  for re-raising. Code comments never cite `notes/` paths.
- Verify every item against the code before working it. This file has no
  truth guarantee; `reference/` and passing tests are authoritative and a
  finding that contradicts them loses. Fix only what is named and
  confirmed; breaking, renaming or moving published API is fine, but a
  change that removes a capability entirely stops and asks the owner,
  whatever the argument. A shared-crate change (`types`,
  `net`, `sasl`, `sync`) is verified with the full-workspace `brokkr
  check`, never `-p`.
- An existing test you must modify is a red flag; justify it explicitly.

The order below is a priority triage of the findings taken at face value,
not verified against the code. Verification happens when an item is worked.

## 1. Priority

Items that leak something or report success for something that did not
happen, plus one surface gap that matters if the consumer relies on it.

- **`BatchOutcome::iter()` yields builder push order, not submission order.**
  The docs now say so; whether consumers need submission order is open.
  **Recommendation (owner-agreed, not a ruling):** no change. Every entry
  carries its `item` key and the builder enforces each submitted id appears
  exactly once, so consumers pair by key, never by position. Close-out owes
  the `iter()` doc a sentence saying so (order is stable but carries no
  meaning).
- **`AccountFactory` has no permanent-removal notification.** JMAP's
  per-factory method refusal history (which keeps a server that refuses an
  advertised method from looping account restarts) is pruned only through
  `forget_account`, which a caller retaining the factory must invoke on
  detach; clearing it on an ordinary account drop would reopen the restart
  loop. Whether the shared factory trait gains a removal hook is an API
  decision.
  **Recommendation (owner-agreed, not a ruling):** add
  `AccountFactory::forget_account(&self, id: &AccountId)` with a default no-op,
  and a `SyncEngine` permanent-removal door beside `detach` that detaches and
  then calls it. Plain `detach` and reattach keep the history: removing an
  account forgets it, a reconnect keeps what was learned.
- **`UnsupportedReason::LabelKind` has no producer.** Its only producers were
  JMAP's label overrides, replaced by declared `LabelShape`s. Keep or remove is
  the owner's call (published variant).
  **Recommendation (owner-agreed, not a ruling):** remove. Its role moved on
  purpose to declared `LabelShape`s; a variant nothing produces offers
  consumers nothing. Reintroduce with a producer if one appears.

## 2. Blocked on a live probe

Nothing hermetic can settle these.

- **Google `events_in_range` combines `orderBy=startTime` with
  `showDeleted=true`.** `crates/google/src/account/calendar.rs`. Google's docs
  bless `showDeleted=true` with `singleEvents=true`, and `orderBy=startTime`
  requires `singleEvents=true`, but there are long-standing reports of the live
  API answering 400 "The requested ordering is not available for the particular
  query" for some `showDeleted` + `orderBy` combinations, and cancelled
  instances have no `start` to order by (only `originalStartTime`, which the
  projection substitutes). If Google rejects the combination, every production
  range read fails: top severity if real.
- **Graph directory search: the alias `$select` may cost directory reach.**
  `otherMails` and `proxyAddresses` sit outside the property set
  `User.ReadBasic.All` grants, as do `businessPhones`, `companyName`,
  `jobTitle` and `department`. A tenant holding only basic directory consent
  may now 403 the WHOLE `/users` query where it previously succeeded. It maps
  to `NoPermission` correctly, but a tenant with working directory search can
  lose it. Confirming needs a live ReadBasic-only tenant. If it fires, the fix
  is a select-narrowing retry after the first 403 (narrow all properties
  outside the basic set and re-issue), NOT dropping them from the query, which
  would give every tenant the degraded projection.
- **Graph event delta without `$select` may not return `changeKey`.** Event
  inventory and changes pages no longer send `$select` (Graph documents it as
  unsupported on event delta) and receive Graph's default delta property set.
  If that set omits `changeKey` or `recurrence`, the event fingerprint degrades
  to `Unavailable`.
- **Graph default contacts book syncs through `/me/contacts/delta`.** Discovery
  now emits the default book under its synthetic `contacts` id, and
  `GraphScope::delta_path` maps it to `{prefix}/contacts/delta`. Graph's
  contact delta page documents only `contactFolders/{id}/contacts/delta`; the
  SDK metadata binds `delta` to the contacts collection, but it is unprobed.
  If it 400s, resolve the default folder's real id (for example from a
  child folder's `parentFolderId`) and use the folder path.
- **Graph KQL `category:` may not be exact category membership.**
  `crates/graph/src/account/pim/search.rs`. `SearchFilter::Labeled` is
  `categories/any(c:c eq ...)` on `$filter` but `category:"..."` on `$search`
  (forced by a From/To, attachment-name or raw-KQL sibling). Graph's message
  `$search` property list does not name `category`. Probe a mailbox with
  categories such as "Red", "Dark Red", "red" and a multi-word name, and
  record for `category:"Red"` and `NOT category:"Red"`: exact, false
  positives (phrase or word match), false negatives, or inconclusive (a few
  passing cases do not prove arbitrary names exact). Exact: nothing to do.
  Otherwise the remedies are refusing `Labeled` on the KQL path (removes the
  "from X with category Y" search, so the owner's call) or a superset query
  that drops the label restriction, fetches `categories` and evaluates the
  original tree exactly, counting raw rows against the 1,000-result cap.

## 3. Blocked on downstream

- **Retire `RATATOSKR_TEST_GCAL_ENDPOINT`.**
  `GoogleAccountFactory::with_calendar_api_base` exists and takes precedence.
  The environment variable was KEPT on purpose: `ratatoskr` and `sæhrimnir`
  both set it, and deleting it would not fail their builds - it would silently
  point their test traffic at the real Google Calendar API. Once both have
  migrated, delete `default_calendar_base` and have the constructors take
  `CALENDAR_API_BASE` directly. That removes the last `std::env::var` read in
  the workspace and the last place a bifrost crate names a downstream
  consumer.

## 4. Owner rulings

Product, API-shape and error-model decisions. None is a live defect on its
face.

### Error model and published surface

- **An IMAP send with no acknowledged recipient but an uncertain one is `Err`.**
  `send_rfc5322` (`crates/imap/src/account/submission.rs`) returns the uncertain
  lane's error (InFlight evidence, `Application::Uncertain`) when nothing landed
  in the succeeded lane, and the call skips the Sent APPEND and, for
  `draft_send`, the draft discard, although the message may have been accepted.
  A consumer dispatching on `error.recovery()` alone can also get terminal
  advice: a malformed final reply in that lane is `ProviderContractViolation`.
  The other recipients' failures are dropped from that `Err`. Decide whether
  "acceptance uncertain, none acknowledged" becomes a receipt state, and if so
  what Sent filing and draft cleanup do for a maybe-sent message.
  **Recommendation (owner-agreed, not a ruling):** no IMAP-only receipt state;
  every provider returns `Err` for a send of unknown acceptance. Bring IMAP to
  Graph's shape (`send_progress_error` in `graph/src/account/pim/send.rs`): a
  partial-commit record with `ReplaySafety::AfterObservation`, every
  recipient's outcome including the failures, never terminal advice for an
  uncertain final reply, draft discard still skipped. Whether "maybe sent"
  becomes an `Ok` receipt state for all providers is a separate cross-provider
  question, unbuilt unless the consumer wants that UI state.

- **Gmail and Graph ignore `save_to_sent = false`.** Both file the Sent copy
  server-side unconditionally (`_save_to_sent` is unused in
  `crates/google/src/account/pim.rs` and `crates/graph/src/account/pim/send.rs`),
  and their `SendReceipt` says `ProviderManaged`. Decide whether a declined Sent
  copy should be refused as `Unsupported`, honoured by a post-send delete, or
  documented as a provider limit on `SendRequest::save_to_sent`.
  **Recommendation (yardstick, not a ruling):** document it as a provider limit on
  `SendRequest::save_to_sent` and keep `ProviderManaged` on the receipt. An
  Outlook user on Exchange or Gmail always gets a Sent copy; refusing the send
  would block mail, and a post-send delete is a destructive compensation that
  can race the server's own filing.
- **`SendReceipt::id` multiplexes three identities.** It is a message id, a
  scheduled-send handle (JMAP returns the EmailSubmission id when scheduled), or
  an IMAP `imapmsgid1:` correlation token that names no addressable object.
  Separate fields (message id, correlation id, scheduled handle) would let
  `cancel_scheduled_send` / `reschedule_send` take the handle by type. Also
  `draft_send` has no per-call `save_to_sent`; IMAP uses the account default.
  The November 2026 design hunt (types scope) proposed the same split as
  `SendReceipt::id: SubmissionRef` (`Message(ObjectId)`,
  `Scheduled(ScheduledSendHandle)`, `Correlation(String)`), with the scheduled
  primitives taking `ScheduledSendHandle`, and noted that `DraftHandle` exists
  "so the type system catches mix-ups" while `impl From<DraftHandle> for
  ObjectId` undoes it. The imap-account scope of the same hunt reached the same
  shape from `append_to_sent_or_fallback`, which returns either an
  APPENDUID-derived id or `ObjectId("imapmsgid1:<Message-ID>")` that
  `decode_object_id` rejects, and noted `SentCopy::Filed { id: Option<..> }`
  already carries the addressable case separately.
  **Recommendation (owner-agreed, "type it properly"; not a ruling):** each
  identity gets its own type, and nothing converts between them implicitly.

  ```rust
  #[non_exhaustive]
  pub enum SubmissionRef {
      /// The sent message, addressable through the message doors.
      Message(ObjectId),
      /// A pending scheduled send; the only key the scheduled doors take.
      Scheduled(ScheduledSendHandle),
      /// A correlation token naming no addressable object (IMAP's
      /// Message-ID based token). Not an `ObjectId`; never decodes as one.
      Correlation(CorrelationToken),
  }
  pub struct ScheduledSendHandle(/* opaque, provider-minted */);
  pub struct CorrelationToken(/* opaque */);
  ```

  `SendReceipt::id: SubmissionRef`; `cancel_scheduled_send(ScheduledSendHandle)`
  and `reschedule_send(ScheduledSendHandle, ..)`; delete
  `impl From<DraftHandle> for ObjectId` so a draft handle cannot reach a
  message door. `draft_send` gains a per-call `save_to_sent` matching
  `send_message`.
- **A JMAP send's leftover staging Email has no routable handle on the
  receipt.** When the implicit `Email/set` refuses or does not confirm the
  Sent move, the created/imported Email (or, for `draft_send`, the submitted
  draft, which now looks sendable) may remain in Drafts with `$draft`; when it
  refuses or does not confirm the `save_to_sent = false` destroy, the staging
  Email may survive. The receipt says `NotFiled` / `Uncertain` / `NotRequested`
  and the destroy case is only logged (`implicit_sent_copy` in
  `crates/jmap/src/sync/pim.rs`). Reusing `DraftDiscardFailure` was rejected:
  no discard was attempted on the move path, and `draft_discard` routes only
  through the primary draft account, so a foreign send-as staging Email cannot
  be addressed by a `DraftHandle`. Decide whether `SendReceipt` grows a field
  naming the retained or possibly retained Email with an owner-preserving
  handle, distinguishing refused, uncertain and failed-destroy.
  **Recommendation (owner unsure, not endorsed; not a ruling):** an optional
  `SendReceipt` field naming the leftover Email by a properly typed,
  owner-preserving handle (see the `SubmissionRef` entry), with state
  `Retained` / `PossiblyRetained` / `DestroyFailed`, so a sent message never
  sits sendable in Drafts unseen. The low-priority `.ok()` staging-Email entry
  would fold in as the same record on the failure path. Revisit with the owner.
- **A proxy 407 maps to `AuthLost`, implying the wrong remedy.** JMAP
  classifies net's typed `TunnelError` (407 ->
  `Authentication(ReauthorizationRequired)` / `AuthLost`, proxy TLS ->
  `Transport(Tls)`, other refusals by status). The error model has no
  proxy-credential kind, so a 407 asks the consumer to reauthorize the mail
  account when the proxy's credentials are wrong; only support detail says so.
  Decide whether the taxonomy gains a proxy-credential kind or a different
  existing kind fits better.
  **Recommendation (yardstick, not a ruling):** not `AuthLost`. Asking for the mail password when
  the proxy refused is the wrong remedy and invites a lockout loop. Gains a
  distinct proxy-authentication kind (Transport-side, no reauthorization
  advice). Adding the kind is a types change, still build work.
- **`MutationConcurrency::StateBased` has no producer left.** Keep or remove
  is the owner's decision, to be made later. JMAP, CalDAV, CardDAV and Graph
  moved off it, so no provider advertises it. The `IdempotencyKey` doc in
  `crates/types/src/mutation.rs` still contrasts itself with `StateBased` as
  "optimistic concurrency on the wire". The November 2026 design hunt (types
  scope) confirmed it is constructed nowhere and folded the question into a
  proposal to replace `ExpectedVersion`'s eight bools plus `None` with one
  doors bitset (filed in the hunt's `structural.md`).
  **Recommendation (owner-agreed, not a ruling):** remove; every provider left
  it on purpose. Reword the `IdempotencyKey` doc to stand alone. The bitset
  reshape is independent.
- **Engine-internal requests on the published `ReopenRequest`.**
  `ScopeDeleted`, `ScopeCreated`, `EstablishIfAbsent` and `SchemaReset` are
  engine plumbing whose epochs and generations mean nothing to a consumer, on
  a `pub` enum re-exported from `lib.rs`. Moving them to a crate-private
  request enum removes published variants. Same ruling: `PushRecovery` is now
  identical in meaning to `Recovery { generation: Some(_) }` (both are
  stamped with the producing incarnation's generation and declined the same
  way), so merging it into `Recovery` also removes a published variant.
  The November 2026 design hunt (sync-engine scope) proposed one variant
  carrying a stamp type, `Stamp::{Incarnation(AccountGeneration), Injected}`,
  so the type says only the sink is unstamped, and argued `ReopenRequest`
  belongs to the per-account supervisor rather than to `multiplexer`.
  **Recommendation (owner tentatively agreed, less sure; not a ruling):** move
  the four plumbing variants to a crate-private request enum; merge
  `PushRecovery` into `Recovery` with `Stamp::{Incarnation(AccountGeneration),
  Injected}`; move `ReopenRequest` to the per-account supervisor.
- **Caller-version residue.** Ruled (2026-10-21) and largely landed. Still
  open: JMAP PIM reads expose no caller type-state field, so a consumer has
  nothing to hand back, although JMAP capabilities already advertise
  `ExpectedVersion` on those doors (advertising an unusable guarantee is a
  product question); IMAP versioned flag writes are still submitted per
  target; other mutation families have no caller-version surface.
  **Recommendation (owner-agreed, not a ruling):** JMAP reads return the
  version its update doors enforce, so the advertised `ExpectedVersion` is
  usable; where no per-object version exists, that door stops advertising it
  (check how the doors enforce today first). IMAP per-target versioned writes
  are performance only: batch later by `UNCHANGEDSINCE`. Other families wait
  for a consumer; nothing is advertised there.
- **BulkDestroy PartialResponse derives DedupeByClientId** even though destroy
  has no client-id deduplication. This protects Graph/JMAP's usual ambiguous
  destroy answers from reassertion. An operation-aware derivation requires an
  error-model ruling. The same derivation now reaches the JMAP single-id
  doors that read an unanswered set id as `PartialResponse`: an unanswered
  `CancelScheduledSend`, the `RescheduleSend` cancel leg (no replacement
  create was attempted), `ContainerDelete`, `FilterDelete` and `DraftDiscard`
  all carry `DedupeByClientId`. A reschedule-only partial-commit record with a
  `Cancelled` Uncertain primary would drop it there, but was not built ahead
  of the general ruling.
  **Recommendation (owner-agreed, not a ruling):** make the central derivation
  operation-aware. An unanswered destroy-shaped operation means "re-issue the
  destroy; already gone counts as done" (the absent-target rule), not
  `DedupeByClientId`. One ruling for the whole destroy family. Give the
  `RescheduleSend` cancel leg its own partial-commit record.
- **A dedupe-bearing stream termination leaves same-round per-item
  SameRequest/AfterAuthRefresh blind retry candidates eligible.** Whether
  whole-submission dedupe should suppress these retries needs a ruling.
  **Recommendation (owner-agreed, low stakes; not a ruling):** the
  whole-submission "observe first" overrides same-round per-item blind retry
  candidates. The owner notes duplicates are also acceptable, so this is
  hygiene, not urgency.

### Write outcomes and compensation

- **A JMAP create answered in both `created` and `notCreated` reads as
  success.** `IdAnswer::from_parts` (crates/jmap/src/core/answer.rs) prefers
  the success entry and drops the refusal. With a usable id that was already
  success; since the create doors return `Created`, an id-less contradictory
  entry is now an unidentified success too. A contradiction is a provider
  defect that proves nothing either way, so it arguably belongs with
  unreadable answers (Uncertain) for every set reader, not only creates.
- **JMAP filter create ignores its activation answer.** `filters::create`
  with `is_active` rides `onSuccessActivateScript` in the same request, but
  never reads the created script's `isActive`; draft-14 ignores an invalid
  activation reference, so an identified script the server left inactive
  reports plain success. `Created::with_warning` can now carry "created but
  not activated".
- **Graph BulkMove caches an empty Deleted Items folder id.** The
  `deletedItems` lookup in `pim/containers.rs` refuses an answer without an
  `id`, but accepts and caches `{"id":""}`, so later thread deletes route to a
  folder id that names nothing instead of failing at the lookup.
- **IMAP draft_create still requires UIDPLUS.** The gate
  (`capabilities.rs`, `draft_create: has_drafts && can_identify_append`)
  existed because the door needed APPENDUID; an APPEND without one is now an
  unidentified success, so the gate could admit every server with a Drafts
  role. Whether a draft save that never yields an id is worth advertising is a
  product decision. The APPEND path also does not consult `UIDNOTSTICKY`
  (same RFC 4315 condition as BUG-048).
- **IMAP ManageSieve filter_create is two requests with no record.**
  `sieve.rs` `filter_create` runs `put_script` then `set_active`; a
  `set_active` failure after a committed `put_script` returns a bare error for
  a script that exists. `Created::with_warning` could report it as created
  but not activated.
- **ManageSieve data responses are read line by line, not as records.**
  `read_response` (`crates/imap/src/sieve.rs`) pushes a data literal as an
  item and the rest of its line (the CRLF, or ` ACTIVE` after a literal
  LISTSCRIPTS name) as a separate `Line`, so `list_scripts` fails with
  "invalid LISTSCRIPTS line" on a literal script name, and a GETSCRIPT literal
  is allocated at the server's count with no cap. Status lines are already
  read as whole records (`read_status`); data responses need the same.
- **ManageSieve PUTSCRIPT / SETACTIVE `OK (WARNINGS)` text is discarded.**
  `SieveStatus::Ok` now keeps the code and text, but `ensure_ok` drops them
  and `filter_create` / `filter_update` have no channel for warnings
  (`Created::with_warning` exists for create). Product question whether a
  saved-with-warnings script should say so.
- **IMAP container operations return a refresh_folders failure after the
  write commits.** The partial-commit record makes this honest; whether to
  return success with a stale registry is a separate API decision.
  **Recommendation (yardstick, not a ruling):** return success. The folder was created, renamed or
  deleted; reporting failure invites a duplicate retry. The stale registry is
  repaired by the next refresh, and the refresh failure is logged, not
  returned.
- **Graph send_message leaves an orphan draft after a refused send;** each
  automatic retry can create another. Accepted Repeatable clutter, bounded by
  retry budgets. Orphan cleanup would be a new compensation; none exists in
  `pim/send.rs`.
  **Recommendation (owner: "obviously"; not a ruling):** a retry re-sends the
  draft it already created instead of creating another; on final failure
  exactly one draft remains, so the user keeps the message and nothing piles
  up.
- **Mutation outcomes carry no per-id detail.**
  `MutationCounters::pending_retry` is a single count that lumps unsent work,
  exhausted retries, read-back mismatches and inconclusive read-backs, with no
  object ids. A consumer that wants to replay or report precisely can only
  re-run the whole campaign.
  **Recommendation (yardstick, not a ruling):** the outcome lists the affected
  ids grouped by reason (unsent, retries exhausted, unconfirmed), counts kept
  as a summary, as `BatchOutcome` already does per batch.

### Engine behaviour

- **Push terminal event semantics.** A terminal push unit failure broadcasts
  account-scoped `SyncEvent::Terminated` while polling continues; its error
  carries the failing unit's attribution. Decide whether this needs a distinct
  structured notification; a folder-scoped termination could incorrectly imply
  that folder polling died. Matters if a consumer UI reads it as "account
  dead".
  **Recommendation (yardstick, not a ruling):** a push-unit failure while polling continues must not
  read as "account dead". It becomes a degraded-push notification (a warning
  that live updates fell back to polling), not `Terminated`. Build work.
- **Push and lifecycle pause policy.** Lifecycle streams continue polling
  while the account boundary is paused; the multiplexer owns their pacing but
  does not consult the boundary. Push sessions gate only directive submission;
  registration maintenance may need to keep server watches alive, while
  Connection lanes retain live transports. Decide their pause semantics
  together.
  **Recommendation (yardstick, not a ruling):** pause means no new user-visible
  work and no new traffic beyond keeping server registrations alive: lifecycle
  polling stops, push stops submitting, watch renewal continues (so resuming
  does not cost a full resubscribe), idle connections may be released.
  Resume picks up without a resync.
- **Scope disable and push subscriptions.** DisableScope quarantines a cursor
  but does not narrow desired push subscriptions. An IMAP folder excluded after
  SELECT refusal remains in the subscription registry until consumer
  unsubscribe or account replacement. Decide whether engine scope quarantine
  should also change desired coverage.
  **Recommendation (yardstick, not a ruling):** yes. A disabled scope drops out
  of desired push coverage and rejoins when re-enabled; holding a server watch
  for a folder the engine will not sync wastes a provider subscription and
  produces notifications nothing acts on.
- **Uncertain mutation items carrying Engine recovery are observed but do not
  forward the directive** (`classify_item_outcome` in sync engine bulk). The
  reassertion path protects these ids; forwarding is separate work.
  **Recommendation (yardstick, not a ruling):** forward it. An item whose
  error asks the engine to restart a scope or resync is evidence the engine's
  state is stale; observing it without acting leaves the user looking at that
  stale state.

### New capability

- **Caller-supplied composition `Date` on `SendRequest`.** The shared
  renderer stamps `Date:` with the submission instant. An offline outbox that
  submits later would want the moment the user pressed Send instead; a
  `SendRequest` field (defaulting to now) would carry it. Surfaced by the
  codex spar on bz#1; not built because it is a new consumer-facing knob.
  **Recommendation (yardstick, not a ruling):** build it, optional, defaulting
  to now. Outlook stamps a message queued offline with when the user pressed
  Send.
- **Gmail `draft_send` and `Date`.** Google drafts are rendered without a
  `Date` and handed to drafts.send as stored. If a live probe shows Gmail
  keeps a missing `Date` missing in the sent copy, `draft_send` could fetch
  the draft and pass replacement `message.raw` stamped with the send time
  in the same drafts.send call.
  **Recommendation (yardstick, not a ruling):** if the probe shows the Date
  stays missing, do it; a sent message without a Date shows wrong or blank in
  recipients' clients. Still blocked on the probe.

- **Graph contact moves.** `crates/graph/src/account/contacts.rs`. A folder
  change is refused as `Unsupported(ContactUpdate)` before I/O. Supporting it
  means folder-aware move routing (Graph lists `parentFolderId` as an update
  field).
  **Recommendation (yardstick, not a ruling):** build it. Moving a contact
  between folders is ordinary Outlook behaviour, and the other providers that
  have folders support it.

- **Graph public-folder mail writes over EWS.** `crates/graph/src/account/mutate.rs`,
  `pim/messages.rs`. Flag writes, moves, destroys and the typed writers refuse
  public-folder items as `Unsupported` before I/O, because Graph REST cannot
  address them and the EWS client has no write operations. Building it means an
  EWS arm (`UpdateItem` for `IsRead` / `Flag` / `Categories`, `MoveItem`,
  `DeleteItem`) routed by the folder's `routing_map` headers, with change-key
  preconditions, replacement ids after a move, etag-cache and ambiguous-
  completion handling that meet the mutation contracts, and a capability
  story for a namespace-specific write gap. Not built: nobody has ruled
  public folders writable.
  **Recommendation (yardstick, not a ruling):** build it. In an Exchange shop,
  Outlook lets users flag, categorize, move and delete public-folder items
  where their permissions allow; read-only public folders are a visible gap.
  Large: its own wave.
- **Undiscovered public-folder move destinations.** A public folder id is a
  bare EWS `FolderId`; only discovery (`is_known_public_folder`) tells it
  apart from a primary folder id, so a move into an undiscovered public
  folder still goes to Graph REST and fails there.
  **Recommendation (yardstick, not a ruling):** on a Graph REST refusal of an
  id discovery did not know, run public-folder discovery once and retry the
  route; part of the public-folder write wave above.

## 5. Low priority

Narrow edge cases, audits with no known defect, and design work.

- **`reference/types.md` implies IMAP search cursors are unbound.** It says
  other providers keep their existing cursor formats, which reads as though
  IMAP does not bind its cursors to the query; it does. Reword.
- **SMTP builder: a single-part root whose payload ends in CRLF gains a blank
  line** (small). `Message` with a `SinglePart` body, `SinglePart::formatted`
  and `MultiPart::formatted` append one CRLF after the root entity
  unconditionally, so a 7bit or quoted-printable text payload `hello\r\n`
  decodes as `hello\r\n\r\n`. Pre-existing, kept byte-identical when the
  shared entity serializer landed (DKIM tests pin these bytes). The renderer
  already uses the decode-safe rule (append only when the serialized entity
  does not end in CRLF); adopting it here changes wire bytes, so rule on it.
- **Two outbound-relevant address-list parsers remain** (medium).
  `bifrost-mime`'s tolerant `parse_address_list(_checked)` and smtp's nom
  `Mailbox` / `Mailboxes` `FromStr` (all-or-nothing, validated through
  `email_address` plus IDNA) have different contracts (tolerant inbound
  versus validated construction). Whether they should share a grammar is a
  separate question from the now-unified writer.

- **Concurrent `close` calls do not share one teardown.** Found in the spar of
  the closed-account gate. Google's `close` swaps its closed flag and returns
  `Ok` at once on every later call, while the first may still be awaiting its
  watch teardown; Graph's `PushTasks::join` takes the task handles out, so a
  concurrent close sees an empty registry and finishes without waiting, and
  dropping the first waiter detaches the rest. Not a defect under the current
  `Account::close` contract (idempotent, safe to call more than once), which
  promises nothing about completion. Decide whether awaiting any `close` should
  certify that teardown has finished; if so, it is a provider contract plus a
  per-provider fix, since `ClosingAccount` does not own teardown.

- **`WarningKind::Throttled` may have no producer.** Noted by the scout of the
  clock-skew warning item, unverified. If confirmed, it is the same shape as
  `ClockSkew`: a published warning nothing emits. Check before treating it as
  wired.

- **`advertised_partitioning_maps_to_its_windows` does not bite on its
  time-window arm.** In `crates/sync/src/backfill/plan.rs`, the time arm
  compares `backfill_plan_for`'s result against `time_windows` itself, so a
  broken `time_windows` passes it (forcing `time_windows` to return an empty
  Vec left it green; the UID arm did fail under the same ablation). Pin the
  time arm to literal expected windows. The planner's own unit tests do bite.

- **JMAP local transport failures lost their reason in diagnostics.**
  `convert_transport` in `crates/jmap/src/sync/error.rs` exports a fixed
  "local JMAP transport failure" for `InvariantViolated` and `LimitExceeded`,
  though those messages are bifrost-authored, not foreign. Decide whether
  support needs the local reason back.
  **Recommendation (not a ruling):** yes, as `support_only` text. The messages are bifrost-authored,
  so there is nothing foreign to leak, and support needs the reason.
- **Lateral findings from the review of the primary-cause builder rewrite.**
  - Several producers mint a scope-directed state cause with a placeholder
    `CursorScope` and rely on a later `.scope(ErrorScope::Cursor(_))` to rebind
    it (Gmail's history arms use `CursorScope::Account`). Where the context
    carries no cursor scope the placeholder now routes recovery silently; the
    old builder rejected that shape. Correct for Gmail history today, but the
    placeholder pattern hides a missing scope elsewhere.
- **Lateral findings from the review of the November 2026 resolution batch.**
  - JMAP: shared-account contacts and calendars have no routing door: the
    optional families live once on `Registry::families` and select the primary
    account (capability question).
    **Recommendation (yardstick, not a ruling):** build routing for them, as
    mail already has. Outlook shows a colleague's shared calendar and contacts
    once access is granted.
- **Lateral findings from deleting the JMAP transport generic.**
  - JMAP: `sync/capability_contract_tests.rs` pins only the false direction
    (a `false` flag refuses locally). A whole `JmapAccount` now opens over a
    scripted net (`a_whole_account_opens_and_answers_over_a_scripted_net` in
    `sync/factory.rs`), so the true direction - a `true` flag reaches the wire
    and the method works - is reachable per method. Design work: one scripted
    server per gated method, driven through the opened account.
  - JMAP: the same file's module doc still calls `PimMethodSupport` a
    hand-maintained mirror of sixty bools; it is macro-generated with a gate
    per operation.
  - JMAP: `transport_reqwest.rs` carries a module-wide `#![allow(dead_code)]`
    justified as a "standalone constructor kept for consumers", but the type
    is crate-private. Find what the allowance still covers and scope it.
  - JMAP: `sync/push.rs` `WsState::new` takes a `Client` and a shutdown token
    it ignores; every caller builds and passes both.

- **Lateral findings from the review of the design-hunt batch, unverified.**
  Each wants its own check and, where marked, a ruling.
  - SMTP: a rejected batch DATA reports per-recipient RCPT rejections. Batch
    sends have no BDAT entry point (ruling). Header lines are settled: the
    builder now writes through the shared `bifrost-mime` writer, which
    encodes where RFC 2047 is legal and refuses an indivisible token past
    998 octets. What remains open is body lines: a caller `7bit`/`8bit`
    body line past 998 octets is not re-encoded or refused.
    **Recommendation (yardstick, not a ruling):** batch DATA rejection is
    reported against every recipient of that batch as a message rejection,
    not as their RCPT results. BDAT for batches: build when CHUNKING is
    advertised (it is the better path for large mail). 998 octets: encode an
    over-long body line (quoted-printable or base64) rather than refuse it;
    servers silently mangle or
    bounce over-long lines, which a user sees as a broken message.
  - sync: the operator-override warning moved from `user_safe` to
    `support_only` (ruling). **Recommendation (yardstick, not a ruling):** keep `support_only`. An
    operator override is admin configuration; an enterprise user is not shown
    their IT department's tuning, and support is.
  - Calendars: CalDAV and JMAP normalize floating times to UTC.
    **Recommendation (yardstick, not a ruling):** a defect. A floating 09:00 event must show at
    09:00 wherever the user is, as Outlook does; preserve `Floating` end to
    end (the shared `EventTime` already models it).

- **No persistence contract for a `ChangeCursor` held outside the engine.**
  bifrost#3 offered it as the alternative to the UID polling surface; the
  surface landed instead. The cursor codec lives in bifrost-sync and nothing
  documents how a direct `Account` consumer persists and migrates a cursor.
  Design work, no consumer asking today.

- **A refused JMAP send drops an unanswered Email create or import.** In
  `send_message` / `send_raw_message` the failure branch reads the Email
  result with `.ok()`, so an Email create answered in neither map, beside a
  proven submission refusal, becomes `email: None` and `failed_submission`
  returns the bare refusal: nothing records that a staging Email may exist.
  The same `.ok()` swallows an Email created with no usable id (the
  `require_created_email_id` MissingField leaf), where the Email certainly
  exists.
  Recording it needs an uncertain, unidentified staging-Email step, which sits
  next to the owner-ruling entry on leftover staging Emails having no
  routable handle.
- **Consumer-injected terminations cannot be attributed to an incarnation.**
  `InvalidationSink::push` (`route_watch_event` in
  `crates/sync/src/push/mod.rs`) sends `ReopenRequest::Recovery` with
  `generation: None` by policy, so a delayed termination from an obsolete
  external watch (a webhook or Pub/Sub subscription the consumer runs) still
  restarts, resets or disables the replacement after a reattach. Closing it
  needs optional incarnation attribution on the consumer-facing push API
  (`WatchEvent` or `SinkPush` side) while keeping the unattributed door;
  sampling the current generation at push time was rejected, since it labels
  an old event with the new identity.
- **`unsubscribe_push` cancelled after taking the registry discards in-flight
  and unattempted handles.** Marking orphans in place instead risks the
  retrier racing the delete to `NotFound`; needs a design.
- **IMAP literals are not emitted incrementally.** FETCH body and binary
  literals are `Bytes` slices of the framed response, but a FETCH response is
  fully framed and parsed before delivery. Standalone parsers without an owned
  backing still copy, generic string fields still parse owned, and non-literal
  syntax and parser overhead are outside the literal ceiling. Incremental
  delivery needs a chunk-aware parser and dispatch contract: design work,
  suggested for the technical-specification track.
- **Account schema reset follow-ups.**
  - `BackfillCheckpointWriter::persist` bypasses completion refusal and every
    reset fence: the attached route accepts any consumer-supplied
    `BackfillCheckpoint`, completion partition included, with no publication;
    the direct route has no engine at all. The account reset excludes this
    consumer assertion path.
  - Ledger-only writer requests from pre-reset producers: `RecordBarrier` and
    `CrossWaivedBarriers` sent by a walk that predates a scope or account
    reset are accepted; a stale barrier fails closed (it blocks the
    replacement walk and asks an operator) and needs its own policy.
  - `ack_checkpoint` with no publication bypasses every fence: a change cursor
    or backfill page acknowledged with `publication: None` skips the claim
    lookup and every reset fence in `persist_ack_request`; after an account
    reset such a backfill page becomes a resume position past windows the
    replacement walk never re-mints. Nothing in the engine sends one. Two
    answers to rule on: name it as a consumer assertion path, or refuse a
    `None`-publication backfill acknowledgement (always, by the reasoning
    `completion_refusal` already records, or once an account fence is raised).
  **Recommendation for all three (not a ruling):** a pre-reset request must
  never write into post-reset state. Fence `BackfillCheckpointWriter::persist`
  like every other write; refuse ledger barriers from a walk older than the
  reset instead of accepting and failing closed; refuse a `None`-publication
  backfill acknowledgement once an account fence is raised (change-cursor
  acks without a publication stay a consumer assertion path, documented as
  such). A reset that old work can undo is a reset the user has to repeat.

## Coverage gaps from the October 2026 hunt

Not findings; areas the hunt did not reach in depth, for the next pass.

- Graph: public-folder EWS, autodiscover, cloud upload and search.
- Sync: `cursor/coverage.rs`, `cursor/ledger.rs`, `cursor/ledger_envelope.rs`,
  `backfill/*`, `engine/lane.rs` and `repair.rs`, the largest files.
