# Unification: bifrost as ratatoskr's cross-provider PIM client

This document is **Phase 3.6** in the bifrost sequence (see
`plans/orchestration.md`). It supersedes the previously-deferred
Phase 4 (error model convergence) by absorbing it as Stage 1 Wave 5.
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
- **Typed escape hatch.** A primitive `execute_raw(ProviderRequest)
  -> ProviderResponse` exists for genuinely-unmodelled wire-level
  calls. The request/response types are protocol-tagged sum types,
  not unstructured bytes. Consumers reach for it rarely; its
  existence prevents the "we need a second API" pressure from ever
  arising.

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
| `search(SearchRequest)` -> `Page<ThreadId>` | Structured filter. The `SearchRequest` AST is the canonical intersection of provider operators; unmodelled operators go via `execute_raw`. JMAP `Email/query`; IMAP `UID SEARCH`; Gmail query strings; Graph OData. |
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

#### Escape hatch

| Method | Notes |
|---|---|
| `execute_raw(ProviderRequest)` -> `ProviderResponse` | Typed sum type per protocol; lets consumers reach the wire for genuinely-unmodelled operations (Sieve script upload, Gmail-specific search operators, Graph extended-property queries that don't fit `set_extended_property`, etc.). Returns the provider's native response shape with the recovery taxonomy applied to errors. |

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
- `execute_raw` is a primitive; the request/response sum types live
  in `bifrost-types` and have one variant per protocol.
- Trait is `#[non_exhaustive]` and dyn-safe.

## Stages

### Stage 1: Unified action surface for mail

Goal: every operation in the primitive and convenience tables above
exists on `Account`. Ratatoskr's action service has zero per-provider
match arms for mail operations after this stage. Absorbs the
previously-deferred Phase 4 (error model convergence) as Wave 5.

#### Sequencing

Five waves; each blocks on the prior.

- **Wave 1: trait surface (S1-W1).** Single agent extends
  `bifrost-types::Account` with all primitives (no default impls)
  and all conveniences (default impls in terms of primitives).
  Grows `AccountCapabilities`. Adds request/response/identity
  types. `bifrost-sync` updated to compile.
- **Wave 2: protocol impls (S1-W2).** Four agents in parallel,
  one per protocol crate. Each implements every primitive.
  Conveniences inherit the default impl unless the default is
  wrong (e.g. Gmail's `mark_replied` is a no-op rather than the
  default keyword-set).
- **Wave 3: ratatoskr migration (S1-W3).** Single ratatoskr-side
  agent removes per-provider mail dispatch in the action service
  and `provider-sync` crate, replacing each match arm with a
  single `account.method(...)` call. Removes per-provider files
  that no longer have callers.
- **Wave 4: protocol crate contraction (S1-W4).** Four agents in
  parallel. Each makes everything in its protocol crate
  `pub(crate)` except the factory and its config types. Examples
  that demonstrated the raw client API are deleted; new examples
  consume `Account`.
- **Wave 5: error model convergence (S1-W5).** Single agent. Folds
  `plans/error-model-convergence.md` into this phase: all
  `Account` methods return `Result<_, AccountError>`. Per-protocol
  error types become `pub(crate)`, converted at the boundary.

#### File ownership

Per AGENTS.md coordination rules:

- **S1-W1**: `crates/types/src/`, `crates/types/Cargo.toml`,
  `crates/sync/src/`.
- **S1-W2-jmap, -imap, -gmail, -graph**: `crates/<protocol>/src/`
  and `crates/<protocol>/Cargo.toml` per protocol.
- **S1-W3**: ratatoskr-side, out of this orchestrator's direct
  scope. Bifrost orchestrator signals "wave 2 merged" and
  ratatoskr migration begins.
- **S1-W4**: same per-protocol ownership as wave 2.
- **S1-W5**: `crates/types/src/error.rs` plus a sweep of every
  protocol crate's error module. Single agent because changes
  must land coherently across all four crates.

#### Exit criteria

Stage 1 is done when all of these hold:

- Every primitive in the tables exists on `Account` with a real
  implementation in every protocol crate. `Err(Unsupported)` is
  acceptable only where the protocol genuinely cannot express the
  operation.
- Every convenience exists with a default impl. Protocol crates
  override conveniences where the default produces wrong behaviour.
- `AccountCapabilities` advertises per-method support; ratatoskr
  reads it to disable UI.
- Ratatoskr has zero per-provider match arms for mail operations.
- All protocol crates are `pub(crate)` except their factory and
  config types. The factory plus `Arc<dyn Account>` is the only
  way ratatoskr (or any other consumer) talks to a provider.
- All `Account` methods return `Result<_, AccountError>`.
- Examples in each crate consume `Account`, not raw clients.
- `brokkr check` is clean workspace-wide.

### Stage 2: Server-side filter rules

Adds `filters_*` primitives plus the `FilterRule` / `FilterScript`
shapes. No conveniences - filter rules are too divergent for a
ratatoskr-canonical wrapper. Same wave structure as Stage 1.

### Stage 3: Contacts

Adds address-book and contact-card primitives, plus conveniences for
ratatoskr's contact-list UI. Per-protocol implementations:

- JMAP: native via the contacts draft already in bifrost-jmap.
- Graph: native `me/contacts`.
- Gmail: requires Google People API. Decision point: rename
  `bifrost-gmail` to `bifrost-google` if it covers Gmail + People
  + Calendar, or split into a separate crate.
- IMAP: no native contacts. CardDAV via a new `bifrost-carddav`
  crate; the IMAP account's contacts primitives dispatch to a
  CardDAV client if the account was configured with CardDAV
  credentials, else return `Unsupported`.

Same wave structure.

### Stage 4: Calendar

Calendar primitives + conveniences for ratatoskr's calendar UI.
Recurrence canonicalised to RFC 5545. JSCalendar / iCalendar /
Google translation happens inside protocol impls. IMAP -> CalDAV
via `bifrost-caldav`, same pattern as Stage 3.

Same wave structure.

### Stage 5: Final ratatoskr migration and audit

By Stage 4 ratatoskr should be calling `Account` for every PIM
operation. Stage 5 is cleanup:

- Ratatoskr's `crates/provider-sync/*` is deleted.
- Ratatoskr's per-provider action-service files are deleted.
  Action service becomes a thin orchestrator over `Arc<dyn Account>`.
- Ratatoskr keeps: app-level DB schema, action-service
  orchestration (local DB write before bifrost call, reconcile
  after), label-group concept, universal-folders aggregation,
  smart-folder operators.
- Final `pub` audit per protocol crate. The only public items are
  the factory and its config types.

This stage is mostly ratatoskr-side; the bifrost-side work is the
final `pub` audit (one agent per crate).

## Decision points the user needs to resolve before launch

1. **bifrost-gmail rename to bifrost-google.** If the crate covers
   Gmail + People + Calendar, the name should change. Decision:
   rename or keep `bifrost-gmail` as-is.
2. **bifrost-carddav / bifrost-caldav as separate crates, or
   embedded in bifrost-imap.** Recommendation: separate crates.
   CardDAV/CalDAV are not IMAP; the only thing they share is "users
   often configure them alongside an IMAP mail account."
3. **Per-account vs per-host rate buckets in `bifrost-net`.**
   Carryover from Phase 3.1, narrowed by the Phase 3.5 audits:
   the per-host bucket carryover reproduces in Gmail and Graph
   (`Net::shared_default()` shares one `www.googleapis.com` /
   `graph.microsoft.com` bucket across accounts, but the actual
   quota is per-user) and does *not* reproduce in JMAP (which
   builds a per-client Net and registers no host rate bucket) or
   IMAP (no HTTP). Stage 1 forces a decision because the new
   operations hit the same hosts as sync. Recommendation:
   per-account bucket layer below per-host, scoped to the HTTP
   protocol crates only.
4. **`AccountFactory::open` should receive the engine account id.**
   JMAP currently attaches `bifrost-net` with placeholder
   `AccountId("jmap")`; Gmail and Graph have analogous shapes.
   Per-account metering and per-account bucket layering (point 3)
   are both blocked on this API contract change. Either extend
   `AccountFactory::open` to take an engine-minted `AccountId`, or
   add a shared account-net injection path the engine can wire.
   Lands in S1-W1.
5. **Manual redirect loop migration to bifrost-net.** Carryover
   from Phase 3.1. S1-W1 is the right time to move the redirect
   loop into `bifrost-net` with proper RFC 7231 method rewriting
   and trusted-host allowlist support. JMAP is the only current
   caller; Stage 1's HTTP-protocol convergence makes it shared.
6. **`bifrost-types::Error` should derive `Clone`** (or otherwise
   support cheap duplication). Gmail's Phase 3.5 audit flagged a
   local `account_error_from_template` workaround that exists
   solely because `Error` cannot be cloned, so per-id mutation
   failures need a hand-rolled duplicator. Derive `Clone` (or
   model the cloneable shape explicitly) and the local helper
   goes away.
7. **Shared HTTP error -> recovery taxonomy adapter.** Graph still
   classifies recoveries by substring-matching HTTP text in
   `recovery_for_graph_error`, and `GraphClient` returns
   `Result<T, String>` rather than a structured error. A shared
   `bifrost_net::Error -> RecoveryClass` adapter that preserves
   `Retry-After` would let all three HTTP protocol crates (JMAP,
   Gmail, Graph) delete their local substring classifiers. Lands
   in S1-W5 (error model convergence).
8. **Per-protocol bandwidth metering.** IMAP stores
   `set_bandwidth_cap()` but does not enforce it or report bytes
   through `bifrost_net::MeterSink`. The HTTP protocol crates feed
   the shared meter via `AccountNet`; IMAP and SMTP need their own
   wiring because they own raw TCP/TLS. Decision: add a metering
   adapter that both raw-socket transports can drive, or accept
   that bandwidth caps apply only to HTTP-shaped accounts. Lands
   in S1-W1 or S1-W2 depending on which option.
9. **`MutationConcurrency` capability shape.** IMAP opportunistically
   uses `STORE UNCHANGEDSINCE` (the modseq cache may be cold-
   startable), so it advertises `MutationConcurrency::None`. A
   richer variant - e.g. `OpportunisticStateBased` - would let
   the engine rely on the optimistic-concurrency check when the
   cache is warm without assuming it is always wired. Genuine
   modeling decision: add the variant and refine the engine's
   read-back guard accordingly, or leave the IMAP situation as-is.
10. **`execute_raw` request/response shape.** Per-protocol typed
    sum type vs serde-json blobs vs serialised wire bytes.
    Recommendation: typed sum types per protocol (`JmapRequest`,
    `GraphRequest`, `ImapCommand`, `GmailRequest`) with matching
    response types. Costs more upfront; pays off in consumer
    ergonomics and keeps the recovery taxonomy applicable to
    errors.

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
  end of each wave rather than batch the doc churn for Stage 5.

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
