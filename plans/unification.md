# Unification: bifrost as ratatoskr's cross-provider PIM client

This document is **Phase 3.6** in the bifrost sequence (see
`plans/orchestration.md`). It supersedes the previously-deferred
Phase 4 (error model convergence) by absorbing it as Stage 1 Wave 4.
Internal stage and wave labels below (S1 through S5) are local to
this document.

Bifrost becomes the unified cross-provider PIM client, not just the
unified sync engine.

## Premise

Phases 0 through 3.5 built a unified *sync* surface. `bifrost-sync`
drives `bifrost-types::Account` across JMAP, IMAP, Gmail, and Graph
for one operation shape: "list and watch the email index, hydrate
bodies, mutate flags." That is real work and the bones are sound.

It is also one shape out of about a dozen that a real email client
performs. Sending, searching, draft lifecycle, folder CRUD, label
CRUD, thread state toggles, mark-replied / mark-forwarded on send,
identities, vacation responder, server-side filter rules, contacts,
calendars - none of these are in the trait. Every one of them is
currently written per-provider in ratatoskr.

The unification ratatoskr was promised does not yet exist. This plan
delivers it - as a single API that any consumer (ratatoskr or not)
uses to talk to JMAP, IMAP, Gmail, or Graph.

### Non-negotiables

These are the operating principles for the work in this document.
Anywhere a phase decision conflicts with one of these, the principle
wins.

- **Breaking APIs is good when it produces a better API.** Bifrost
  is pre-1.0. There is no external semver pressure. The "we can't
  break this" gravity that paralyses mature libraries does not
  apply.
- **No "someone might want this" code.** Every public item exists
  because a named consumer calls it. Items with no current named
  consumer are deleted, not deprecated, not flagged with
  `#[doc(hidden)]`, not hidden behind a feature.
- **One API surface.** `Account` and `AccountFactory` are the
  bifrost public API. Protocol crates (`bifrost-jmap`,
  `bifrost-imap`, `bifrost-gmail`, `bifrost-graph`, `bifrost-smtp`)
  are implementation detail: everything `pub(crate)` except the
  factory and its config types. No raw protocol surfaces alongside
  the trait. A consumer wanting "the only good Graph client in the
  world" gets it by depending on `bifrost-graph` and using its
  `Account` impl - not by reaching for a parallel raw Graph API.
- **Bifrost owns the cross-provider translation.** Whenever
  ratatoskr currently has a `match provider { Jmap => ..., Imap =>
  ..., Gmail => ..., Graph => ... }` for a single PIM operation,
  that match moves into bifrost. Ratatoskr calls one method and
  bifrost dispatches.
- **Two-tier trait: primitives and conveniences.** The trait has
  opinion-neutral primitives (wire-level operations, typed,
  expose provider-native model with provenance metadata) and
  opinionated conveniences (high-level operations encoding
  ratatoskr's product opinions). Conveniences have default impls
  expressed in terms of primitives, so protocol impls only have
  to implement primitives. A consumer who disagrees with
  ratatoskr's defaults uses the primitives directly. A consumer
  who is happy with ratatoskr's defaults uses the conveniences and
  gets one-call ergonomics.
- **Ratatoskr's defaults are bifrost's defaults.** The convenience
  layer's default impls match what ratatoskr's glossary
  prescribes: folder/label classification, canonical system-folder
  IDs, label-ID prefixes, move-vs-apply semantics. A different
  consumer who wants different defaults overrides conveniences in
  their own wrapper - bifrost does not host alternate policies.
- **Hard problems get unified; opaque-by-design things stay
  opaque.** Where two providers expose genuinely non-isomorphic
  shapes (Sieve scripts vs Gmail filters, OData operators outside
  the canonical search AST, JSCalendar vs iCalendar quirks),
  bifrost picks a primitive that exposes the wire shape and lets
  consumers branch. It does not pretend to unify what is not
  unifiable.
- **No escape hatch.** `Account` is the complete API for every
  provider. If a wire-level operation is not modelled by a typed
  primitive, that is a gap to close by adding the primitive - not
  to paper over with a generic `execute_raw`. Escape hatches
  contradict the one-complete-API premise and accumulate as the
  thing people use to bypass the design. The genuinely-divergent
  cases each have a better typed answer: Sieve scripts via
  `FilterScript { language, body }`, Gmail-specific search
  operators via `SearchRequest.provider_query: Option<String>` at
  the primitive level, Graph extended properties via
  `set_extended_property`.

## Scope of the unification

The `Account` trait gets two kinds of methods.

### Primitives (Layer 1, opinion-neutral)

Every protocol impl implements these. Operations are *what the
protocol does*, typed but not opinionated. Return values carry
provenance and role metadata so consumers can branch on provider
model without a `match`.

#### Mail mutation primitives

| Method | JMAP | IMAP | Gmail | Graph |
|---|---|---|---|---|
| `add_to_container(target, container_id)` | `Email/set` mailboxIds patch (add) | `COPY` | `users.messages.modify` addLabels | `POST /messages/{id}/move` |
| `remove_from_container(target, container_id)` | `Email/set` mailboxIds patch (remove) | `UID STORE +FLAGS (\Deleted)` + `EXPUNGE` | `users.messages.modify` removeLabels | n/a (move replaces source) |
| `set_keyword(target, keyword, value)` | `Email/set` keyword set/clear | `UID STORE +/-FLAGS (keyword)` | n/a | n/a |
| `set_label_membership(target, label_id, value)` | n/a | n/a | `users.messages.modify` add/remove labels | n/a |
| `set_category(target, category, value)` | n/a | n/a | n/a | `PATCH` `categories[]` |
| `set_extended_property(target, prop_id, value)` | n/a | n/a | n/a | `singleValueExtendedProperties` PATCH |
| `set_is_read(target, bool)` | `$seen` keyword | `\Seen` flag | UNREAD label | `isRead` PATCH |

`target` is a typed enum `MutationTarget::Thread(id) | Message(id)`.
Providers that only support per-message mutation fan out internally.
The engine's read-back-after-retry guard applies to every mutation.

#### Mail composition primitives

| Method | Notes |
|---|---|
| `send_message(SendRequest)` | RFC 5322 message + provider routing. JMAP `EmailSubmission/set`; Gmail `users.messages.send`; Graph `POST /me/sendMail`; IMAP via configured `bifrost-smtp` transport. |
| `attachment_upload(stream, mime)` -> `AttachmentHandle` | Streaming upload. |
| `draft_create(DraftPatch)` -> `DraftHandle` | New draft. |
| `draft_update(DraftHandle, DraftPatch)` | Modify. |
| `draft_discard(DraftHandle)` | Delete. |
| `draft_send(DraftHandle)` | Convert + send, atomically where supported. |

#### Search primitive

| Method | Notes |
|---|---|
| `search(SearchRequest)` -> `Page<ThreadId>` | Structured filter. The `SearchRequest` AST is the canonical intersection of provider operators; provider-specific operators are reached via the typed `SearchRequest.provider_query: Option<String>` field at the primitive level. JMAP `Email/query`; IMAP `UID SEARCH`; Gmail query strings; Graph OData. |
| `search_messages(SearchRequest)` -> `Page<MessageId>` | Same AST, message-shaped results. |

#### Container CRUD primitives

| Method | Notes |
|---|---|
| `containers_list()` -> `Vec<Container>` | Returns `Container { kind: Folder \| Label, role: Option<FolderRole>, provenance: Provenance, native_id: String, name: String, parent: Option<ContainerId> }`. Consumer reads metadata to decide rendering. |
| `container_create(kind, name, parent: Option<ContainerId>)` -> `ContainerId` | Where supported by `kind`. |
| `container_rename(ContainerId, name)` | Where supported. |
| `container_move(ContainerId, new_parent)` | Folder-kind only. |
| `container_delete(ContainerId)` | Where supported. Contract: non-empty containers either fail or move contents to Trash, never silently delete messages. |

A `Container` carries both `kind` (Folder | Label) and `role`
(`Inbox | Sent | Drafts | Archive | Trash | Spam | None`). Consumers
that want a folder-binary UI read `kind`. Consumers that want a
labels-everywhere UI ignore `kind` and read `role`.

#### Settings primitives

| Method | Notes |
|---|---|
| `identities_list()` -> `Vec<Identity>` | JMAP `Identity/get`; Gmail `users.settings.sendAs.list`; Graph `me` + `mailboxSettings`; IMAP n/a (configured externally). |
| `identity_update(IdentityId, IdentityPatch)` | Display name, signature, default, reply-to. |
| `vacation_get()` -> `Option<VacationConfig>` | Out-of-office. JMAP `VacationResponse/get`; Gmail `getVacation`; Graph `mailboxSettings/automaticRepliesSetting`; IMAP via Sieve. |
| `vacation_set(VacationConfig)` | Update. |
| `quota_get()` -> `Option<QuotaInfo>` | Storage usage. |

#### Threading and hydration primitives

| Method | Notes |
|---|---|
| `thread_hydrate(ThreadId)` -> `ThreadHydration` | All messages in the thread with metadata. JMAP `Thread/get` then `Email/get`; IMAP `THREAD REFERENCES` + per-message FETCH; Gmail `users.threads.get`; Graph conversation API. |
| `message_hydrate(MessageId, projection: HydrationProjection)` -> `Message` | Single-message fetch. `projection` selects headers-only / preview / full. |

### Conveniences (Layer 2, ratatoskr policy, default impls)

These methods encode ratatoskr's product opinions: folder/label
classification, move-removes-source semantics, starred-collapses-to-
single-bit, replied-derives-from-keyword-or-extended-property. Each
has a default impl in terms of primitives. Protocol impls override
only when the default is wrong for that provider.

| Method | Default impl |
|---|---|
| `move_thread(thread, target, source?)` | `add_to_container(target)` + `remove_from_container(source)` per ratatoskr's archive-removes-INBOX semantics |
| `apply_label(target, label_id)` | Dispatch to `set_keyword` / `set_label_membership` / `set_category` based on the label's `provenance` |
| `remove_label(target, label_id)` | Same dispatch, value = false |
| `set_read(target, bool)` | Alias for `set_is_read` (the primitive is already opinion-neutral; this convenience exists only for naming symmetry with the others) |
| `set_starred(target, bool)` | Per-provider: Gmail STARRED label, IMAP `\Flagged`, Graph `flag.flagStatus`, JMAP `$flagged`. Collapses provider semantics to single bit, dropping Graph follow-up dates. |
| `mark_replied(message)` | Per-provider: `set_keyword(target, $answered, true)`, `set_extended_property(target, PR_LAST_VERB_EXECUTED, 102)`, Gmail no-op (derived on sync) |
| `mark_forwarded(message)` | Same shape, `$forwarded` / 104 |
| `delete_thread(thread)` | If not in Trash: `move_thread(thread, Trash, current)`. If in Trash: `remove_from_container` + EXPUNGE / `users.messages.delete` / equivalent |

A consumer who needs Outlook follow-up dates preserved overrides
`set_starred` (or skips it and calls `set_extended_property`
directly with the full follow-up payload). A consumer who wants
Gmail system labels rendered as labels rather than folders ignores
the role metadata and renders by `kind`.

### Canonical IDs

Bifrost surfaces a default ratatoskr-aligned canonical-ID projection
*through the conveniences*. The primitives return `native_id`
verbatim with `Provenance { provider: Provider, kind: ContainerKind,
native: String }`. The conveniences expose a derived `canonical_id`
view that applies ratatoskr's existing prefix conventions:

- System folder roles -> `INBOX`, `SENT`, `DRAFT`, `archive`,
  `TRASH`, `SPAM`
- IMAP keywords / JMAP keywords -> `kw:{keyword}`
- Exchange categories -> `cat:{name}`
- Exchange importance -> `importance:high|low`
- Gmail user labels -> native ID, no prefix
- Graph user folders -> `graph-{guid}`
- JMAP user mailboxes -> `jmap-{id}`
- IMAP user folders -> `folder-{path}`

`canonical_id` is a derived view, not a stored field. Consumers who
disagree with the convention ignore it and store `(provider,
native_id)` themselves. Ratatoskr uses `canonical_id` directly as
the storage key for `folders.id` / `labels.id`.

Per-provider role normalisation (mapping JMAP `Mailbox.role`, IMAP
SPECIAL-USE, Gmail system labels, Graph well-known folder names to
`FolderRole`) is done inside each protocol impl when populating
`Container::role`. That mapping is wire-shape translation, not
policy.

### Contacts (Stage 3)

Address book and contact card CRUD primitives, with the same
primitive-vs-convenience split.

### Calendar (Stage 4)

Calendar list, events, RSVP, recurrence. Recurrence is the hard
part - bifrost canonical recurrence is RFC 5545 RRULE + RDATE +
EXDATE + recurrence-id overrides as a primitive. Provider-specific
shapes (JSCalendar) are translated in protocol impls.

### Server-side filter rules (Stage 2+)

Sieve, Gmail filters, Outlook rules. Bifrost exposes:

- `FilterRule { conditions: Vec<Condition>, actions: Vec<Action> }`
  primitive - the typed intersection of provider operators.
- `FilterScript { language: ScriptLanguage, body: String }`
  primitive - for accounts that accept literal Sieve.

`AccountCapabilities::filter_rule_shape` advertises which shape the
account supports. There is no convenience layer here - filter rules
are too divergent across providers to land on a single ratatoskr-
shaped wrapper.

## Trait shape

One trait. `Account` already exists; Stage 1 grows it with primitives
and conveniences. `AccountFactory` already exists; no change.

Trait shape rules:

- Every primitive is a trait method with no default impl. Each
  protocol impl must implement every primitive (or return
  `Err(AccountError::Unsupported)` and advertise the gap in
  `AccountCapabilities`).
- Every convenience is a trait method with a default impl in terms
  of primitives. Protocol impls override only when the default is
  wrong for that provider.
- `AccountCapabilities` advertises per-operation support. Ratatoskr
  reads capabilities to disable UI affordances per account.
- Trait is `#[non_exhaustive]` and dyn-safe.

## Stages

### Stage 1: Unified action surface for mail

Goal: every operation in the primitive and convenience tables above
exists on `Account` with real per-protocol implementations.
Absorbs the previously-deferred Phase 4 (error model convergence)
as Wave 4. Ratatoskr's adoption of the new API is a downstream
concern handled in ratatoskr's own plan, not gating bifrost.

#### Sequencing

Four waves; each blocks on the prior.

- **Wave 1: trait surface (S1-W1)**. **Merged.** Trait carries
  27 primitives and 8 conveniences in `crates/types/src/account.rs`;
  `AccountCapabilities` grew `PimMethodSupport` and
  `ConvenienceShape`; `AccountFactory::open(AccountId)` is the
  factory signature; `bifrost-net` owns the method-aware redirect
  loop (`redirect.rs`) and the raw-socket `MeterSinkHandle`
  (`bandwidth.rs`); `crates/sync/src/` compiles against the new
  trait shape. `crates/sync/tests/cross_crate_conformance.rs` was
  stubbed back to dyn-safety only during W1 and restored in W2.
- **Wave 2: protocol impls (S1-W2)**. **Merged.** Four agents in
  parallel, one per protocol crate. Each landed a `pim.rs`
  implementing all 27 primitives (real wire calls for the
  protocol's supported set, `Err(Unsupported)` for the rest),
  declared `PimMethodSupport` + `ConvenienceShape` flags, overrode
  conveniences only where the default was wrong, and consumed the
  `AccountId`-receiving `open` signature. IMAP drove the
  `MeterSinkHandle` through `connection/{wire,lifecycle,pool}` and
  the STARTTLS / COMPRESS swap path in `connection/driver/upgrade`.
  JMAP deleted its manual redirect loop from
  `transport_reqwest.rs`. Audit follow-ups merged in the same wave:
  cross-crate conformance test restored against all four factories;
  Graph `apply_label` / `remove_label` overrides dropped in favor
  of the trait default after `(Label, Graph)` was added to
  `dispatch_label` so non-Graph provenance still routes correctly;
  Graph `set_extended_property(_, _, None)` clears via batched
  `DELETE` on `singleValueExtendedProperties` instead of returning
  `Unsupported`; `bifrost-net::AccountNet::retag` lets factories
  re-mint pre-attached `AccountNet` handles under the engine id;
  cross-host redirect strip widened to remove caller-set
  `Authorization` headers (covers JMAP Basic-auth, not just
  bearer-injection).
- **Wave 3: protocol crate contraction (S1-W3)**. Four agents in
  parallel. Each makes everything in its protocol crate
  `pub(crate)` except the factory and its config types. Examples
  that demonstrated the raw client API are deleted; new examples
  consume `Account`. This is the final `pub` audit for the
  protocol crates after the trait surface has been implemented
  end-to-end.
- **Wave 4: error model convergence (S1-W4)**. Single agent. Folds
  `plans/error-model-convergence.md` into Stage 1: all `Account`
  methods return `Result<_, AccountError>`, per-protocol error
  types become `pub(crate)` and convert at the boundary, the
  shared HTTP error -> `RecoveryClass` adapter lands in
  `bifrost-net`, and `bifrost-types::Error` gets whatever
  duplication shape (derive `Clone`, `Arc`-wrap non-Clone
  payloads, or an explicit `duplicate()` method) the reshape
  settles on - resolving the Gmail `account_error_from_template`
  workaround.

#### File ownership

Per AGENTS.md coordination rules:

- **S1-W1**: `crates/types/src/`, `crates/types/Cargo.toml`,
  `crates/sync/src/`, `crates/net/src/`, `crates/net/Cargo.toml`.
- **S1-W2-jmap, -imap, -gmail, -graph**: `crates/<protocol>/src/`
  and `crates/<protocol>/Cargo.toml` per protocol.
- **S1-W3**: same per-protocol ownership as wave 2.
- **S1-W4**: `crates/types/src/error.rs`, `crates/net/src/` (for
  the recovery adapter), plus a sweep of every protocol crate's
  error module. Single agent because changes must land coherently
  across all four protocol crates and the two shared crates.

#### Exit criteria

Stage 1 is done when all of these hold:

- Every primitive in the tables exists on `Account` with a real
  implementation in every protocol crate. `Err(Unsupported)` is
  acceptable only where the protocol genuinely cannot express the
  operation.
- Every convenience exists with a default impl. Protocol crates
  override conveniences where the default produces wrong behaviour.
- `AccountCapabilities` advertises per-method support.
- `AccountFactory::open` takes the engine `AccountId` and every
  protocol impl consumes it.
- `bifrost-net` owns the method-aware redirect loop; no protocol
  crate carries its own.
- IMAP drives `MeterSink` for bandwidth metering.
- All protocol crates are `pub(crate)` except their factory and
  config types. The factory plus `Arc<dyn Account>` is the only
  way any consumer talks to a provider.
- All `Account` methods return `Result<_, AccountError>`; the
  shared HTTP error -> `RecoveryClass` adapter lives in
  `bifrost-net`.
- Examples in each crate consume `Account`, not raw clients.
- `brokkr check` is clean workspace-wide.

### Stage 2: Server-side filter rules

Adds `filters_*` primitives plus the `FilterRule` / `FilterScript`
shapes. No conveniences - filter rules are too divergent for a
ratatoskr-canonical wrapper. Three waves: trait surface (W1),
protocol impls (W2), protocol crate contraction (W3). No error
convergence wave - that landed in S1-W4 and applies workspace-wide.

### Stage 3: Contacts

Adds address-book and contact-card primitives, plus conveniences for
ratatoskr's contact-list UI.

**First action (W1 prep):** rename `bifrost-gmail` to `bifrost-google`
(the crate stops being mail-only here), and create the `bifrost-carddav`
skeleton crate. Workspace `Cargo.toml`, dependent crates, and reference
docs updated to match. Lands as the leading patch of W1 rather than its
own wave - too small to warrant separate sequencing.

Then the standard wave structure:

- **W1: trait surface.** Contacts primitives in `bifrost-types::Account`
  (`address_books_list`, `contacts_list`, `contact_get`, `contact_create`,
  `contact_update`, `contact_delete`, `contact_search`), plus conveniences.
  `AccountCapabilities` grows the contacts-support flags.
- **W2: protocol impls** (four agents in parallel):
  - JMAP: native via the JMAP contacts draft already wired into
    bifrost-jmap.
  - Google (formerly bifrost-gmail): adds Google People API support
    alongside the existing Gmail mail code.
  - Graph: native `me/contacts`.
  - IMAP-via-CardDAV: the new `bifrost-carddav` crate implements the
    CardDAV protocol client; the IMAP `Account` impl pulls it in as a
    dependency and dispatches contacts primitives through it when the
    account is configured with CardDAV credentials, returning
    `Unsupported` otherwise. `bifrost-carddav` also ships its own
    `CardDavAccountFactory` for DAV-only consumers (Radicale, Apple
    iCloud Calendar standalone, etc.) - same one-API premise as the
    other protocol crates. The initial implementation of
    `bifrost-carddav` is sourced from ratatoskr's existing CardDAV
    code; this is a one-time code transfer happening inside the W2
    bifrost-carddav agent, not a separate wave.
- **W3: protocol crate contraction.** Same as Stage 1 W3, now
  including the two newly-touched crates (`bifrost-google`,
  `bifrost-carddav`).

### Stage 4: Calendar

Calendar primitives + conveniences for ratatoskr's calendar UI.
Recurrence canonicalised to RFC 5545 (RRULE + RDATE + EXDATE +
recurrence-id overrides). JSCalendar / iCalendar / Google translation
happens inside protocol impls.

**First action (W1 prep):** create the `bifrost-caldav` skeleton crate.
Workspace `Cargo.toml` and reference docs updated. Same shape as Stage 3's
prep, leading patch of W1.

Then the standard wave structure:

- **W1: trait surface.** Calendar primitives (`calendars_list`,
  `events_in_range`, `event_get`, `event_create`, `event_update`,
  `event_delete`, `event_rsvp`, `event_search`), plus conveniences.
- **W2: protocol impls** (four agents in parallel):
  - JMAP: native via the JMAP calendar draft already wired into
    bifrost-jmap.
  - Google: adds Google Calendar API support alongside the existing
    Gmail + People code.
  - Graph: native `me/events`.
  - IMAP-via-CalDAV: `bifrost-caldav` implements the CalDAV protocol
    client; same composition pattern as `bifrost-carddav` in Stage 3.
    `CalDavAccountFactory` is published for DAV-only consumers. The
    initial implementation is sourced from ratatoskr's existing
    CalDAV code, same one-time code transfer pattern as Stage 3.
- **W3: protocol crate contraction.** Same as Stage 1 W3, plus
  `bifrost-caldav`.

## Decision points the user needs to resolve before launch

1. **bifrost-gmail rename to bifrost-google.** **Resolved:**
   rename. The crate covers Gmail + People (Stage 3) + Calendar
   (Stage 4); "Gmail" becomes misleading once People/Calendar
   land. Pre-1.0, single PR, no real cost. Different hosts
   (`gmail.googleapis.com`, `people.googleapis.com`,
   `calendar.googleapis.com`) reinforce that "Google" is the
   honest scope. Rename lands as the first action of Stage 3 -
   matching the moment the crate's content stops being mail-only.
2. **bifrost-carddav / bifrost-caldav as separate crates, or
   embedded in bifrost-imap.** **Resolved:** separate crates,
   migrated out of ratatoskr where the implementations currently
   live. Same pattern bifrost-graph and bifrost-gmail followed -
   protocol code moves to bifrost, app-level concerns stay in
   ratatoskr. CardDAV/CalDAV are WebDAV-based (HTTP/XML), not
   IMAP-based (line protocol, raw TCP/TLS); embedding them in
   bifrost-imap would put two unrelated transport stacks under
   one identity. Each new crate gets its own AccountFactory;
   DAV-only consumers (Apple iCloud Calendar standalone,
   Radicale, etc.) use them directly, while IMAP-shaped mail
   accounts with DAV co-configured use ImapAccountFactory which
   composes the DAV clients internally for contacts/calendar
   primitives - same shape as bifrost-imap composing bifrost-smtp
   for send today. Lands in Stages 3 and 4.
3. **Per-account vs per-host rate buckets in `bifrost-net`.**
   **Resolved:** no prescriptive per-account bucket layer. The
   rate-limit subject is the authenticated principal (OAuth
   subject / Basic Auth user), not a bifrost `Account` - a
   shared mailbox spends the acting principal's quota, not the
   mailbox's; three E3/E5 accounts on the same `@domain.com`
   are three separate principals with separate per-user quotas;
   Gmail delegated access spends the acting principal's quota.
   "Per-account" is the wrong bucket key, and we do not reliably
   know any provider's actual limits. Keep the per-host token
   bucket as a coarse safety net and let `bifrost-net`'s
   `Retry-After` honor (landed in P2-A5) drive the actual
   backoff. If bulk-backfill 429 storms become a problem, the
   backfill orchestrator throttles itself based on observed 429
   rates - a sync-engine concern, not a transport concern.
4. **`AccountFactory::open` should receive the engine account id.**
   **Resolved:** extend the trait method signature. JMAP currently
   attaches `bifrost-net` with placeholder `AccountId("jmap")`;
   Gmail and Graph have analogous shapes. Every factory needs the
   real id for at least one of: `bifrost-net` attach (JMAP, Gmail,
   Graph), `MeterSink` bandwidth metering (IMAP, SMTP - per the
   Phase 3.5 IMAP finding), trace/log correlation, error/recovery
   correlation. No factory benefits from ignoring it. Lands in
   S1-W1.
5. **Manual redirect loop migration to bifrost-net.** **Resolved:**
   move into `bifrost-net`. RFC 7231 §6.4 method rewriting
   (301/302/303 convert POST -> GET and drop body; 307/308
   preserve method and body), `Authorization` stripping on
   cross-host hops, trusted-host allowlist with redirects to
   non-allowlisted hosts rejected, works for both buffered and
   streaming responses. JMAP's manual loop in
   `crates/jmap/src/transport_reqwest.rs` is deleted at the
   same time. Lands in S1-W1.
6. **`bifrost-types::Error` should derive `Clone`** (or otherwise
   support cheap duplication). **Deferred to S1-W4.** Gmail's
   `account_error_from_template` workaround exists because
   `Error` cannot be cloned, but the error model is going to be
   reshaped dramatically in the error-convergence wave anyway -
   structured Graph errors, shared HTTP -> recovery adapter,
   unified taxonomy across all four protocols. Decision about
   cloneability rolls into that broader work rather than getting
   locked in against today's shape.
7. **Shared HTTP error -> recovery taxonomy adapter.** Graph still
   classifies recoveries by substring-matching HTTP text in
   `recovery_for_graph_error`, and `GraphClient` returns
   `Result<T, String>` rather than a structured error. A shared
   `bifrost_net::Error -> RecoveryClass` adapter that preserves
   `Retry-After` would let all three HTTP protocol crates (JMAP,
   Gmail, Graph) delete their local substring classifiers. Lands
   in S1-W4 (error model convergence).
8. **Per-protocol bandwidth metering.** IMAP stores
   `set_bandwidth_cap()` but does not enforce it or report bytes
   through `bifrost_net::MeterSink`. The HTTP protocol crates feed
   the shared meter via `AccountNet`; IMAP and SMTP need their own
   wiring because they own raw TCP/TLS. Decision: add a metering
   adapter that both raw-socket transports can drive, or accept
   that bandwidth caps apply only to HTTP-shaped accounts. Lands
   in S1-W1 or S1-W2 depending on which option.
9. **`MutationConcurrency` capability shape.** **Resolved:** leave
   as `None`; do not add `OpportunisticStateBased`. The current
   behavior is already correct - IMAP opportunistically uses
   `STORE UNCHANGEDSINCE` when the modseq cache is warm, and the
   engine's read-back guard provides the lost-update safety net
   regardless. A richer variant would either require new engine
   complexity to know whether the per-mutation optimistic check
   ran (significant cost for a small per-mutation latency win) or
   be purely informational (no behavioral difference). Capability
   shape is additive: if behavioral pressure ever justifies the
   variant, it lands then alongside the engine code that takes
   advantage of it. Today's `None` posture forecloses nothing.
10. **`execute_raw` escape hatch.** **Resolved: not adding one.**
    `Account` is the complete API for every provider; an
    unmodelled wire-level operation is a gap to close by adding
    a typed primitive. Bifrost already implements 5 more JMAP
    RFCs than Stalwart's client library and is the most
    featureful Graph client outside Microsoft - there is no
    real category of "unmodelled wire operation" left. Where
    primitive-level escapes are useful (provider-specific
    search operators), they live on the typed request structs
    as optional fields, not as a generic raw-call surface.

### Captured-but-not-decisions

Phase 3.5 audits surfaced two findings that need no decision now
but are worth recording so Stage 1 agents do not rediscover them:

- **`GraphClient` has a local per-client `Semaphore`** for
  concurrency. If Stage 1 grows a per-account request concurrency
  limiter in `bifrost-net` or `bifrost-sync`, the local Semaphore
  is deleted.
- **`reference/jmap.md`, `reference/gmail.md`, `reference/graph.md`,
  `reference/smtp.md` were updated** during the Phase 3.5
  commits to match the new visibility and shape. Stage 1 work
  that touches these surfaces should refresh them again at the
  end of each wave rather than batch the doc churn.

S1-W2 agents surfaced these protocol-side known limitations,
already documented in the matching `reference/*.md`. Recorded here
so S1-W3 (`pub(crate)` contraction) and S1-W4 (error convergence)
agents do not re-open them:

- **IMAP `send_message` is unsupported until the IMAP factory has
  an SMTP transport handle to call.** `bifrost-smtp` exists; the
  composition pattern is `ImapAccountFactory` carrying an
  `Option<SmtpClient>` plumbed in by the consumer. Not a Stage 1
  goal; tracked for Stage 2 or later.
- **IMAP `draft_update` needs MIME parse + merge to do partial
  updates correctly.** Stage 1 advertises `draft_update: false` for
  IMAP. Closing the gap is a draft-model primitive beyond the
  current scope.
- **IMAP `identity_update` and vacation responder are external
  config or Sieve-shaped**, not exposed in Stage 1. Sieve script
  primitive lives in Stage 2 (server-side filter rules); identity
  storage is a consumer responsibility today.
- **IMAP `container_delete` refuses non-empty mailboxes** with
  `AccountError::Other` rather than moving contents to Trash. The
  trait contract permits either; the IMAP impl picks "fail loudly"
  because move-to-Trash semantics differ across servers. Document
  in `reference/imap.md`; not a defect.
- **IMAP `quota_get` reports only the STORAGE resource** from
  `GETQUOTAROOT`. Other resources (MESSAGE, MAILBOX, etc.) are
  silently dropped. Trait surface returns a single `QuotaInfo`;
  multi-resource quota would need a richer return type.
- **Graph send/draft return the draft id** because the Graph send
  actions answer 202 Accepted with no body. Final Sent Items id
  is rediscovered through sync or search. Documented in
  `reference/graph.md`.
- **Graph `send_message` / `draft_create` / `draft_update` accept
  inline attachments embedded in the request but reject
  pre-uploaded `AttachmentHandle`s** (which can only come from a
  successful `attachment_upload`, itself unsupported on Graph).
  Capability flags advertise the no-pre-uploaded-attachment shape
  as supported; a future `attachment_upload` for Graph upload
  sessions would relax this.
- **Graph `draft_update` does not replace attachments.** Same
  upload-session limitation.

## Coordination rules

Same as `plans/orchestration.md`:

- All agents work in the same git tree. No worktrees.
- Agents read `AGENTS.md` and `CLAUDE.md` first, plus this
  document and `ratatoskr/reference/glossary/folders-labels.md`.
- Agents do not run `brokkr`, `cargo`, or any build/test
  commands. The orchestrator validates between waves.
- Each agent owns exactly the files listed. No two agents ever
  own the same file. Cross-crate findings go to the orchestrator.
- Subagents always run in the foreground.

## Phase gates

Between stages (and between waves inside Stage 1) the orchestrator
runs the 3-pass audit from orchestration.md:

1. **Domain-specific verification.** Per agent, does the
   delivered work match the brief? Primitives exist on the trait,
   are implemented per protocol, dispatch into real provider
   calls, return real responses with provenance metadata.
   Conveniences either inherit the default or override with
   documented reasoning.
2. **Cross-cutting reconciliation.** Does the new wiring actually
   reach ratatoskr through `Arc<dyn Account>`? Are
   capability flags accurate per protocol? Have all per-provider
   match arms in ratatoskr's action service been replaced? Are
   the primitives sufficient to express the conveniences without
   reaching outside the trait?
3. **Editorial.** Clippy clean workspace-wide. `brokkr fmt`. No
   orphan files. Reference docs (`reference/{net,sync,jmap,imap,
   gmail,graph}.md`) reflect the new surface.

Do not trust agent claims of completion. Verify trait method
existence, per-protocol implementation existence, ratatoskr-side
removal, and end-to-end dispatch with a synthetic account.

## What this plan deliberately does not do

- **No second API.** `Account` is the only public surface. Protocol
  crates expose only the factory and its config. There is no raw
  JMAP / IMAP / Gmail / Graph client API alongside the trait. A
  consumer wanting the world's-best Graph client uses
  `GraphAccountFactory` and the trait; the quality of that
  deliverable rests on the quality of the `Account` impl, not on
  exposing the underlying machinery.
- **No alternate policy layer.** Bifrost ships one set of
  conveniences - ratatoskr's. Consumers who disagree with a
  default override the convenience in their own wrapper or skip it
  and use primitives directly. Bifrost does not host alternate
  policies.
- **No SMTP in the public trait surface.** SMTP is the send
  transport for IMAP-shaped accounts; `bifrost-smtp` stays a
  separate crate, pulled in by the IMAP Account impl's
  `send_message` implementation. Ratatoskr does not depend on
  `bifrost-smtp` directly.
- **No shared-mailbox abstraction.** Per-provider delegation
  semantics differ enough (Graph shared mailboxes vs JMAP
  principals vs Gmail delegated access) that bifrost exposes one
  `Account` per delegate the user has access to, rather than a
  cross-provider "delegate" abstraction.
- **No back-port to pre-Phase-4 surfaces.** Pre-Phase-4
  `bifrost-jmap` consumers (the examples and `crates/sync/tests/
  cross_crate_conformance.rs`) get rewritten against `Account`.
  No deprecation period.
