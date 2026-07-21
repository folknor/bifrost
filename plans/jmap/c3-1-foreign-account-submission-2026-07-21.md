# c3-1: JMAP native foreign-account submission (send-as)

Date: 2026-07-21

Technical implementation specification for TODO item **c3-1 (jmap)**. Wire
JMAP's `send_as` support so `Account::send_message` honors a
`SendRequest::send_as` natively, submitting on behalf of a shared/delegate
mailbox via an `EmailSubmission/set` against the foreign `accountId`,
instead of the current boundary rejection (`Unsupported(Send)`).

## Required reading (standing references)

This spec is written against, and MUST be judged against, the following.
Reviewers and implementers: **read them before working the bricks** - they
are the ground the work stands on.

- `reference/technical-implementation-spec.md` - the contract this
  document is written to (the ten points; the keep/revert landing
  discipline; the verification-per-brick rule).
- `reference/error-model.md` - the cross-cutting `AccountError` contract.
  ALWAYS required: every `Account` method returns
  `Result<_, AccountError>`, so the new `Unsupported` / `Malformed`
  boundary rejections and the wire-error mappings this spec adds are bound
  by it.
- `reference/jmap.md` - the single source of truth for bifrost-jmap module
  layout, the `sync/` account layer, the foreign-account (A5a) read/sync
  slice, and the send pipeline. Read especially the "PIM primitives and
  conveniences" and "Foreign (shared/delegate) accounts" sections.
- `TODO.md`, item **c3-1 (jmap)** (under "C-3 (Graph send-as) follow-ups")
  - the source this item is built from. Also read **c3-2** (the sibling
  IMAP/SMTP exclusion) and the A5a foreign-mutation note it is coupled to.

The only crates this spec targets are `bifrost-jmap` (all changes) and, as
consumed contract, `bifrost-types` (`SendRequest::send_as` / `SendAs`,
already landed by C-3 - no change). Read `reference/jmap.md` for the
former; the `SendAs` contract lives in `crates/types/src/compose.rs`.

## Survey of the ground

### What C-3 already landed

- `bifrost_types::SendRequest.send_as: Option<SendAs>` and the
  `SendAs { As(MailboxId), OnBehalfOf(MailboxId) }` enum
  (`crates/types/src/compose.rs`). `SendAs::mailbox()` returns the
  routing-key `&MailboxId` regardless of mode. `As` forces `from` to the
  mailbox (overriding any consumer `from`); `OnBehalfOf` keeps the
  consumer `from` as author and marks the authenticated user as `Sender`.
- `PimMethodSupport.send_as: bool` gate
  (`crates/types/src/capabilities.rs`).
- The Graph backend implements it (`crates/graph/src/account/pim.rs`,
  `apply_send_as` + `send_as_unknown_mailbox`) - the cross-provider
  reference contract this spec mirrors on the From/Sender stamping and the
  unknown-mailbox rejection.

### JMAP current state (the code being torn into)

- `crates/jmap/src/sync/pim.rs`
  - `send_as_guard` (lines ~185-198): rejects any `Some(send_as)` with
    `Unsupported(Send)`. Called first in `send_message`.
  - `send_message` (lines ~200-342): resolves the account's Drafts/Sent
    role mailboxes on the passed `mail` handle, builds the draft
    `EmailCreate` via `build_email_create_from_send`, issues one
    result-referenced batch (`Email/set` create + `EmailSubmission/set`),
    `onSuccessUpdateEmail` -> Sent (or `onSuccessDestroyEmail` when
    `save_to_sent == Some(false)`), CAS-advances the per-`accountId` email
    state, and returns the email id (immediate) or the submission id
    (scheduled). The submission's `identityId` is set **only if**
    `request.identity` is `Some`; otherwise omitted (server default).
    The envelope `mailFrom` is stamped only when `request.from` is `Some`
    and there is at least one recipient.
  - `build_email_create_from_send` (lines ~1560-1600) -> delegates to
    `build_email_create_from_draft`, which sets `from` from `patch.from`
    and (existing setter `EmailCreate::sender`, `crates/jmap/src/email/set.rs:79`)
    can stamp `sender`.
  - `role_mailbox(&mail, role, op)` (line ~1324): resolves a role mailbox
    on **whatever `MailAccount` it is handed** - already per-account, so
    handing it a foreign handle resolves that foreign account's roles with
    no change.
  - `identities_list` (lines ~774-815): the `Identity/get` shape (Id, Name,
    Email, ReplyTo, signatures) on a `MailAccount`. A `MailAccount` can
    issue `IdentityGet` regardless of which account it is scoped to.
  - `send_as_rejected_unsupported` (lines ~2294-2309) is the *only*
    existing guard test; the absent-`send_as` case is an inline assertion
    inside it (lines ~2307-2308), not a separate test. This test is
    removed. `send_as_absent_is_none` named later in this spec is a **new**
    test introduced by this work, not a rename of an existing one.
- `crates/jmap/src/sync/account.rs`
  - `JmapAccount` fields: `foreign_mail: Arc<HashMap<String, MailAccount>>`
    (keyed by JMAP `accountId`), `self_emails: Vec<String>`,
    `submission: Option<MailAccount>`, `email_states: StateMap`,
    `max_delayed_send: usize`. `JmapAccount::new` (line ~73) is called from
    exactly one site (`factory.rs`).
  - `send_message` (lines ~569-591): rejects when `self.submission` is
    `None`; otherwise routes to the `Submission`-capability account handle
    and delegates to `pim::send_message`. `draft_send` / `send_raw_message`
    take no `send_as` (out of scope, see stopping rule).
  - `foreign_mail` routing helpers (`mail_for_scope`,
    `account_id_for_scope`, `owner_of_scope`) already exist for read/sync.
- `crates/jmap/src/sync/foreign.rs`: the `Folder`-scope codec and
  `owner_tag(account_id) -> MembershipScope::Mailbox(MailboxId(accountId))`.
  **This is the load-bearing fact for routing:** the owner tag a consumer
  receives for a foreign account is `MailboxId(accountId)`. So a
  `SendAs::As(MailboxId(x))` the consumer hands back carries
  `x == the foreign JMAP accountId`, i.e. `send_as.mailbox().0` is a
  `foreign_mail` key directly.
- `crates/jmap/src/sync/factory.rs`
  - `foreign_mail_account_ids(client, primary_id)` (lines ~376-394):
    session non-personal accounts advertising `urn:ietf:params:jmap:mail`,
    minus the primary.
  - Foreign accounts are seeded (lines ~239-270) into `foreign_mail`
    **after** `capabilities::build` is called (line ~173). But the
    session - which is all the capability gate needs - is available before
    the build (line ~158).
  - `self_emails` is fetched (line ~159, `fetch_self_emails`) and already
    passed to `JmapAccount::new`.
- `crates/jmap/src/sync/capabilities.rs`
  - `PimSupport` struct + `build(session, support)`. `send_as: false`
    hard-coded (line ~137). Tests deserialize a `Session` from JSON via the
    `session(json)` helper (lines ~204-206) and assert on the built caps.
- `crates/jmap/src/sync/error.rs`: `unsupported_error(op, scope, detail)`,
  the `Request(Malformed)` builders, and `into_account_error` (the wire
  mapping every `Email/set` / `EmailSubmission/set` / `Identity/get` error
  already flows through).

### Reconciliation with sibling scope

There is no batch-sibling spec over this ground. The coupled follow-ups
(A5a foreign *mutation*, c3-2 IMAP/SMTP send-as) are named exclusions, not
refutations: this spec wires exactly the send leg of the A5a foreign-
mutation slice, deliberately not the bulk flag/move/destroy leg (stopping
rule below).

## Obstacles, resolved inline

1. **How a `SendAs` `MailboxId` maps to a JMAP submission target.** JMAP
   has no `/users/{id}` routing dimension like Graph. Resolution: the
   `MailboxId` IS the foreign JMAP `accountId` (established above via
   `foreign.rs::owner_tag`). Route the draft `Email/set` and the
   `EmailSubmission/set` to `foreign_mail[send_as.mailbox().0]`. Both calls
   target the foreign account because the draft `Email` and its submission
   must live in the same account (RFC 8621 EmailSubmission is per-account).

2. **A foreign account that lacks the submission capability.** `foreign_mail`
   is populated for read/sync and does not imply submission. Resolution:
   compute, at open, the subset of foreign `accountId`s whose session
   `accountCapabilities` advertise `urn:ietf:params:jmap:submission`, store
   it as `foreign_submission: Arc<HashSet<String>>` on `JmapAccount`, and
   reject a `send_as` targeting an id outside it. This also drives the
   capability gate (obstacle 5).

3. **The `EmailSubmission` `identityId` and the shared-mailbox From
   address.** RFC 8621 requires the submission's identity to match the
   message `From`; for a foreign send that identity must belong to the
   foreign account, and (for `As`) its email IS the shared-mailbox From
   address we must stamp. Resolution: before building the draft, issue one
   `Identity/get` against the foreign `MailAccount` handle. Pick the
   identity: if `request.identity` is `Some`, use that id (the consumer's
   explicit choice - a wrong id is a server-side `forbidden`/
   `invalidProperties` mapped through `into_account_error`); else the
   default (first-returned) identity. Capture `(identity_id, email, name)`.
   The submission always carries the resolved `identity_id` (no longer
   optional on the foreign path). If `Identity/get` returns no identities,
   reject `Unsupported(Send)` ("foreign account advertises no sending
   identity") - there is no address to submit as.

4. **From / Sender stamping (mirror the Graph contract).** With the
   resolved identity `email`/`name` (`ident`) and the authenticated user's
   address `self` (first `self_emails` entry, or `None`):
   - `As(_)`: `From = ident` (overrides any consumer `request.from`);
     `Sender` omitted (author == sender).
   - `OnBehalfOf(_)`: `From = request.from` if the consumer set one, else
     `ident`; `Sender = self` when known, else omitted (matches Graph's
     "omit sender without user_email").
   - `SendAs` is `#[non_exhaustive]`: a future arm falls back to the
     conservative `As` stamping (author == sender == mailbox).
   The envelope `mailFrom` follows the resolved `From` (so the relay's MAIL
   FROM is the shared address, and the FUTURERELEASE `holduntil` still
   stamps correctly for a scheduled foreign send).

5. **The capability gate ordering.** `pim_methods.send_as` must be `true`
   iff at least one foreign submission-capable account exists. This is a
   pure function of the session (available before `capabilities::build`).
   Resolution: a `foreign_submission_available(session, primary_id) -> bool`
   predicate in `factory.rs`, threaded into `PimSupport.foreign_submission`
   and set as `send_as: support.foreign_submission` in `capabilities.rs`.
   No reordering of the foreign-account seeding is required.

6. **State CAS on the foreign account.** The post-set state advance keys on
   the *foreign* `accountId` (already the correct behavior: `send_message`
   takes an `account_id` param and CASes `email_states` on it - the caller
   passes the foreign id). The per-`accountId` `state_cache` maps already
   hold a foreign entry from A5a seeding, so no new state plumbing.

## Review consolidation (R1 opus + R2 codex)

Two independent reviews (R1 = opus, R2 = codex) audited this spec against
the code. Every finding below was re-validated against the tree at review
time and folded in. Findings are grouped by the obstacle/artifact they
amend; the two brand-new obstacles (7, 8) are additions the reviews
forced. Rejected / downgraded findings are listed at the end with reasons.

### Amendment to obstacle 2 (capability bool vs routing set can diverge)

*(R1 gap; significant.)* The `foreign_submission` routing set is built
**post-probe**, inside the foreign-seed loop (`factory.rs:241-270`): a
foreign account whose `seed_foreign_account` probe returns `Err(_skip)`
(permission-denied or transient) is absent from both `foreign_mail` and the
set. But `foreign_submission_available(session, primary_id)` (obstacle 5)
is a **pure function of the session**, so it can return `true` for exactly
that skipped account. Result: `pim_methods.send_as` is advertised `true`,
yet a send to the skipped account hits `foreign_mail.get(id) -> None ->
send_as_unknown_account` = `Request(Malformed)` / terminal `ClientBug` - a
*transient* open-time skip misclassified as a *permanent* client bug.

Resolution: derive the capability bool from the **same post-seed set** the
router consults, not from a second pure-session pass. Concretely, drop
`foreign_submission_available` as an independent predicate; set
`PimSupport.foreign_submission = !foreign_submission.is_empty()` where
`foreign_submission` is the `HashSet<String>` accumulated in the seed loop
(insert `foreign_id` when its session caps advertise submission AND its
probe succeeded). This also collapses obstacle 5's ordering concern
(next). If a future design must keep the pure-session predicate, then the
"advertised-but-not-seeded" send must map to a **retryable** error
(`Unsupported`/transient), never `Request(Malformed)`.

### Amendment to obstacle 5 (primary_id binding ordering)

*(R1 nit.)* The plan asserts "No reordering of the foreign-account seeding
is required" - true - but omits that `primary_id` is bound at
`factory.rs:226`, **after** `PimSupport` is constructed (line 164) and
`capabilities::build` runs (line 173). Threading the foreign-submission
signal into `PimSupport` therefore requires either hoisting the
`let primary_id = mail.id_str().to_string();` binding above line 164, or
using `mail.id_str()` directly at the `PimSupport` site. If obstacle 2's
resolution is taken (bool derived from the post-seed `HashSet`), the
`PimSupport` construction moves below the seed loop anyway, which resolves
this ordering as a side effect - call that out in the factory brick.

### Amendment to obstacle 3 (identity selection - underspecified)

*(R1 gap + R2 P1; the most substantive correctness gap.)* Obstacle 3 as
written only handles the empty-list case and defers a bad `request.identity`
to a server `forbidden`/`invalidProperties`. That is impossible for `As`:
the identity **email** must be stamped into `From` **client-side, before**
the batch is built (there is no address to construct the draft with), so a
`request.identity` that matches no returned row cannot be deferred to the
server. Additional facts the plan missed:

- **Discovery gap.** `identities_list` (`pim.rs:774`) only issues
  `Identity/get` against the **primary** submission account (the account
  layer passes `self.submission`; `account.rs:800`). A consumer therefore
  has no supported way to *discover* a foreign account's identity ids, so a
  foreign `request.identity` is largely a guess unless the consumer scraped
  it elsewhere. Either widen identity discovery to the foreign account
  (out of this spec's stated surface) or document that on the foreign route
  `request.identity` is best-effort and the default path is the norm.
- **No RFC default field.** `is_default: idx == 0` (`pim.rs:810`) is a
  bifrost heuristic, not an RFC 8621 guarantee - `Identity/get` does not
  order by or flag a default. "first-returned identity" is therefore a
  convention, not a contract; state it as such.
- **Wildcard identity emails.** RFC 8621 identities may carry `email`
  values like `*` or `*@domain` (send-from-any). Stamping such a literal
  into `From` produces an invalid header. The selection algorithm must
  reject / skip wildcard-email identities as a `From` source (fall through
  to a concrete identity, else reject).
- **Missing requested id/email is a provider contract violation**, a
  distinct class from an empty identity collection - it maps through
  `into_account_error` as a server/malformed-response error, not the
  `Unsupported(Send)` "no sending identity" rejection.

Resolution: the spec must pin an **exact** selection algorithm:
1. Issue `Identity/get` on the foreign `mail` handle. Prefer a **by-id**
   get when `request.identity` is `Some` (a by-id miss is then a clean,
   unambiguous rejection), else fetch-all.
2. If `request.identity` is `Some` and the row is absent -> reject
   (explicit-identity-not-found; user-safe `Malformed`/`Unsupported` per
   the error-model derive, not a silent server deferral).
3. Select the identity: the requested id, else the first row with a
   **concrete** (non-wildcard) `email`.
4. If the chosen row has no usable `email` (empty or wildcard-only) ->
   reject with the "no sending identity" `Unsupported(Send)`.
The by-id-vs-fetch-all decision, the wildcard rule, and the
required-field classification are all part of the concrete artifact, not
implementer's choice.

*(R1 smell, acceptable-with-note.)* Even with an explicit override
available, the default ("first concrete identity") may be a non-canonical
alias when a foreign account exposes several identities. No better signal
exists; acknowledge this one-line in the obstacle so it is a known,
accepted limitation rather than a silent surprise.

*(R1 observation.)* The inline foreign `Identity/get` duplicates the
property list and `idx == 0` default logic already in `identities_list`;
factor a shared helper rather than hand-rolling a second `IdentityGet`.
Note also the added per-foreign-send round-trip (an `Identity/get` before
the `Email/set`+`EmailSubmission/set` batch) is a real latency cost the
original plan did not call out - acceptable, but stated.

### Amendment to obstacle 4 (From/Sender + envelope)

*(R1 smell - wording.)* "direct analog of Graph's `apply_send_as` tests"
overstates the mirror. Graph's `As` arm stamps `sender = from = mailbox`
(`graph .../pim.rs:412-414`); this spec's `As` returns `(ident, None)` -
**Sender omitted** (RFC 5322-correct, since Sender is redundant when equal
to From). The JMAP `As` test therefore asserts the *opposite* of Graph's
`apply_send_as_as_sets_from_and_sender`. Behaviorally intended; just don't
call it a "direct analog" - it is a deliberate divergence.

*(R1 smell - wording, prevents a real bug.)* `envelope_from` is captured at
`pim.rs:229`, **before** the draft build at line 244. Step 2's "set
`request.from` before `build_email_create_from_send`" points at line 244;
the binding constraint is actually line 229. Retighten the wording to
"set `request.from` before the envelope capture (`pim.rs:229`)" so an
implementer does not mutate `from` after the envelope snapshot and stamp a
stale MAIL FROM.

*(R2 P1 - reclassified to design-note; see rejected list.)* The public
`SendAs::OnBehalfOf` doc (`compose.rs:115-118`) reads "the mailbox is the
author (`From`)", while this spec preserves a consumer-set `from`
(honoring divergence). This is **not** a plan bug: it deliberately mirrors
Graph's shipped `apply_send_as`, whose own doc (`graph .../pim.rs:400-405`)
documents `OnBehalfOf` as honoring a consumer-set `from`
(`entry("from").or_insert_with`). The real action item is doc-consistency:
either tighten `compose.rs`'s enum doc to match the shipped cross-provider
behavior, or explicitly note the "fill-when-absent" nuance - and pin the
behavior with the `resolve_foreign_headers` tests (both branches). Second,
the **envelope `mailFrom` for `OnBehalfOf`** is a genuine open decision:
RFC 8621's server-generated envelope derives MAIL FROM from `Sender` when
present, whereas this spec forces `mailFrom` from the resolved `From`. For
`OnBehalfOf` the bounce/return-path arguably belongs to the authenticated
`Sender`, not the shared-mailbox `From`. Decide explicitly and pin it:
either keep `mailFrom == From` (simplest, document the choice) or set
`mailFrom` from `Sender` when `OnBehalfOf` stamped one.

### Obstacle 7 (NEW - scheduled foreign send cannot be canceled/rescheduled)

*(R2 P1; significant, no analog in the original plan.)* A scheduled send
returns the bare `EmailSubmission` id (`pim.rs:336-337`). But
`cancel_scheduled_send` (`account.rs:684-700`) and `reschedule_send`
(`account.rs:702-722`) route **unconditionally to `self.submission`** - the
**primary** submission account. A submission id minted on a *foreign*
account is not addressable there: the `EmailSubmission/get`+`/set` will
query/mutate the wrong account and fail (or worse, silently no-op). Worse
still, `reschedule_send`'s recreate (`pim.rs:2024-2034`) rebuilds the
replacement submission with only `undo_status` + `email_id` + `envelope` -
it does **not** restamp `identityId`, which the foreign path made
mandatory (obstacle 3). Rescheduling a foreign scheduled send would thus
recreate an identity-less submission against the primary account.

Resolution (pick one, spec must commit):
- **(a) Reject `scheduled + send_as`** at the boundary in Brick 1
  (`Unsupported(Send)`, "scheduled foreign send is not supported"). Cheap,
  honest, and keeps the return/undo contract sound. Preferred unless the
  consumer needs scheduled foreign sends now.
- **(b) Owner-encode the returned handle** so `cancel`/`reschedule` can
  recover the foreign `accountId` and route the follow-up
  `EmailSubmission/set` to `foreign_mail[id]`, and make `reschedule_send`
  **preserve `identityId`** on the recreated submission (fetch it back via
  `EmailSubmission/get` alongside `email_id`/envelope, and re-`identity_id`
  it). This is materially more work and touches three account-layer methods
  and `pim::reschedule_send`.

The original stopping rule ("Only `send_message` carries `send_as`") does
**not** cover this: `cancel`/`reschedule` take an `ObjectId`, not a
`SendRequest`, so a foreign scheduled send silently produces an
un-cancelable handle unless (a) or (b) is chosen. Add the chosen path to
the Bricks and the stopping rule.

### Obstacle 8 (NEW - per-account delayed-send window)

*(R2 P1; significant.)* The plan passes `self.max_delayed_send`
(`account.rs:588`, plan step) to the foreign `send_message`. That scalar is
sourced from the **session-level** submission capability
(`factory.rs:160-163`, `session.submission_capabilities().max_delayed_send()`).
Per RFC 8621 the session-level submission capability object is empty and
`maxDelayedSend` lives in each account's `accountCapabilities`; foreign
accounts may therefore have a **different** window, including `0` (no
scheduled send). Using the primary scalar can (i) let a scheduled foreign
send through a window the foreign account does not actually offer, or (ii)
reject a legal one. It also couples the global `pim_methods.scheduled_send`
flag to the primary account only.

Resolution: capture per-target submission metadata at open - e.g. store
`foreign_submission: HashMap<String, ForeignSubmissionMeta>` (or extend the
routing set to a map) carrying each foreign account's `maxDelayedSend` read
from **its** `accountCapabilities`, and pass the **target account's**
window into `pim::send_message` for a foreign send. If obstacle 7 path (a)
(reject `scheduled + send_as`) is chosen, this reduces to "reject when
scheduled", but the per-account window is still the correct source once
scheduled foreign sends are ever supported. Define how the global
`scheduled_send` capability flag reads against a selected foreign target.

### Amendment to Target artifacts + Bricks (verification design)

*(R2 P1 - partly valid; see rejected list for the part downgraded.)*
- **Package token.** All gate commands used `-p jmap`; the cargo package is
  `bifrost-jmap` (`crates/jmap/Cargo.toml:2`) and brokkr passes `-p`
  straight to `cargo test -p`, which needs the exact package name. Fixed
  throughout to `-p bifrost-jmap`.
- **Add deterministic encoder tests.** The plan leaned entirely on
  `brokkr check` (type-check/clippy) + downstream for the wire shape. But
  the repo test rules explicitly bless small encoder / serde round-trip
  tests, and the load-bearing wire facts here - the draft `Email/set` +
  `EmailSubmission/set` targeting the **foreign** `accountId`, the forced
  `identityId`, the resolved `From`/`Sender`, the envelope `mailFrom`, and
  the `/created/{id}/id` result reference - are all serializable and can be
  pinned by an encoder test **without a mock server**. Add such a test to
  Brick 1's gates (serialize the built batch, assert the JSON shape). This
  is additive to `brokkr check`, which remains the green-tree gate.
- **Concrete routing helper.** `resolve_send_target` was left as
  "implementer's choice of exact shape" (plan step under `account.rs`),
  contrary to the technical-implementation-spec's concrete-artifact rule.
  Pin one shape, e.g.:
  ```rust
  enum SendTarget<'a> {
      Personal(&'a MailAccount),                 // self.submission path
      Foreign { mail: &'a MailAccount, id: String, mode: SendAs },
  }
  fn resolve_send_target(&self, req: &SendRequest)
      -> Result<SendTarget<'_>, AccountError>;
  ```
  so `send_as_absent_is_none`, the unknown-account, and the
  not-submission-capable gates assert on a concrete return, not prose.

### Rejected / downgraded findings

- **R2 "`OnBehalfOf` conflicts with the public contract" - downgraded from
  P1 bug to a doc-consistency note.** The spec's `OnBehalfOf` From-handling
  is not wrong: it deliberately mirrors Graph's shipped, documented
  `apply_send_as` behavior (honor a consumer `from`, fill the mailbox only
  when absent). The cross-provider contract *is* the shipped behavior, not
  the terser `compose.rs` enum sentence. Folded as an obstacle-4 amendment
  (reconcile the doc + pin with tests + decide the envelope `mailFrom`
  question), not as a plan defect.
- **R2 "`brokkr check` cannot prove the wire shape" - accepted as
  *additive*, rejected as a *replacement*.** `brokkr check` remains a valid
  green-tree gate; the correct fix is to **add** deterministic encoder
  tests (folded above), not to treat the existing gate as wrong.
- **Nothing was rejected outright.** Every other R1/R2 finding was
  validated against the tree and folded above. The `send_message` `None`
  path remains byte-identical to today; all amendments are confined to the
  `foreign.is_some()` branch, the account-layer routing, the factory
  capability derivation, and the verification/doc scope.

## Target artifacts (concrete)

### `bifrost-jmap` `sync/pim.rs`

Delete `send_as_guard`. Introduce a foreign-submission routing input and a
pure header-resolution helper:

```rust
/// Foreign (shared/delegate) submission context resolved by the account
/// layer before `send_message` runs. The `mail` handle passed to
/// `send_message` is ALREADY the foreign account's handle; this carries
/// the mode and the authenticated user's address for `OnBehalfOf`.
pub(crate) struct ForeignSubmission {
    pub(crate) mode: bifrost_types::SendAs,
    pub(crate) self_address: Option<bifrost_types::Address>,
}

/// Pure From/Sender resolution. `ident` is the foreign account's chosen
/// sending identity; `consumer_from` is `request.from`. Mirrors Graph's
/// `apply_send_as`.
fn resolve_foreign_headers(
    mode: &bifrost_types::SendAs,
    ident: &bifrost_types::Address,             // identity email (+ name)
    consumer_from: Option<bifrost_types::Address>,
    self_address: Option<bifrost_types::Address>,
) -> (bifrost_types::Address /* From */, Option<bifrost_types::Address> /* Sender */)
```

`resolve_foreign_headers` rules (per obstacle 4):
- `As` => `(ident.clone(), None)`.
- `OnBehalfOf` => `(consumer_from.unwrap_or(ident.clone()), self_address)`.
- non-exhaustive fallback => `(ident.clone(), None)`.

`send_message` new signature (add one param; the guard call is removed):

```rust
pub(crate) fn send_message(
    mail: MailAccount,               // primary OR the foreign handle
    email_states: StateMap,
    account_id: String,              // primary OR foreign accountId
    max_delayed_send: usize,
    foreign: Option<ForeignSubmission>,
    request: bifrost_types::SendRequest,
) -> AccountFuture<Result<ObjectId, AccountError>>
```

Body changes, `foreign.is_some()` branch only (the `None` path is
byte-identical to today):
1. `Identity/get` on `mail`; select identity per obstacle 3; on empty list
   return `unsupported_error(Send, None, "...no sending identity")`.
2. `let (from, sender) = resolve_foreign_headers(&fs.mode, &ident_addr,
   request.from.take(), fs.self_address);` Set `request.from = Some(from)`
   before `build_email_create_from_send` (so `from` and the envelope
   `mailFrom` both derive from it).
3. After building `create`, if `sender.is_some()`
   `create.sender([address_to_jmap(sender)])`.
4. Force the submission `identityId` to the resolved identity id
   (unconditional on the foreign path), rather than the current
   `if let Some(identity)` gate.
Sent-move / destroy and state CAS are unchanged. The scheduled-send
window check, envelope stamping, and return-value contract are **not**
simply "unchanged" on the foreign path - see the Review consolidation
below (the foreign delayed-send window is per-account, not the primary
scalar, and a scheduled foreign submission id is not addressable by the
`self.submission`-routed cancel/reschedule). Obstacles 7 and 8 resolve
both.

Replace the guard tests with:
- `resolve_foreign_headers_*` unit tests (the As/OnBehalfOf/fallback matrix
  - direct analog of Graph's `apply_send_as_*` tests).
- `send_as_unknown_account_is_malformed` (new error helper below).
- `send_as_absent_is_none`: retained meaning - a `None` `send_as` produces
  no `ForeignSubmission` at the account layer (assert via the account-layer
  routing helper, see below).

### `bifrost-jmap` `sync/error.rs`

```rust
/// A `send_as` targeting a `MailboxId` that is not a known foreign
/// submission-capable account. A client bug (the consumer got the
/// accountId from `memberships`), so `Request(Malformed)`, mirroring
/// Graph's `send_as_unknown_mailbox`.
pub(crate) fn send_as_unknown_account(mailbox: &bifrost_types::MailboxId) -> AccountError
```
`Request(Malformed)`, operation `Send`, user-safe detail naming the
unknown account. Derives terminal `ClientBug` recovery per the central
`derive` table.

### `bifrost-jmap` `sync/account.rs`

- New field `pub(crate) foreign_submission: Arc<HashSet<String>>`; new
  `JmapAccount::new` param (factory is the only caller).
- `send_message` becomes:
  ```rust
  fn send_message(&self, request: SendRequest) -> ... {
      match request.send_as.as_ref() {
          None => { /* existing self.submission path, foreign = None */ }
          Some(send_as) => {
              let id = send_as.mailbox().0.clone();
              let Some(mail) = self.foreign_mail.get(&id) else {
                  return err(send_as_unknown_account(send_as.mailbox()));
              };
              if !self.foreign_submission.contains(&id) {
                  return err(unsupported_error(Send, None,
                      "foreign account does not advertise submission"));
              }
              let self_address = self.self_emails.first()
                  .map(|e| bifrost_types::Address::bare(e.clone()));
              pim::send_message(
                  mail.clone(), Arc::clone(&self.email_states), id,
                  self.max_delayed_send,
                  Some(pim::ForeignSubmission { mode: send_as.clone(), self_address }),
                  request,
              )
          }
      }
  }
  ```
  The `None` arm keeps the current `self.submission`-not-available guard
  and passes `foreign = None`.
- A small pure routing helper (testable without a client) so
  `send_as_absent_is_none` and the unknown-account path have deterministic
  gates, e.g.
  `fn resolve_send_target<'a>(...) -> Result<Option<&'a MailAccount>, AccountError>`
  or expose the decision as an enum the test asserts on. (Implementer's
  choice of exact shape; the gate below names what it must pin.)

### `bifrost-jmap` `sync/factory.rs`

- `fn foreign_submission_available(session: &Session, primary_id: &str) -> bool`:
  any session account `!= primary_id`, `!is_personal()`, whose
  `capabilities()` include both `urn:ietf:params:jmap:mail` and
  `urn:ietf:params:jmap:submission`.
- Build `foreign_submission: HashSet<String>` alongside the existing
  `foreign_mail` loop (insert the id when its session caps advertise
  submission), pass to `JmapAccount::new`.
- Thread `foreign_submission: foreign_submission_available(&session, &primary_id)`
  into `PimSupport`.

### `bifrost-jmap` `sync/capabilities.rs`

- Add `pub(crate) foreign_submission: bool` to `PimSupport`.
- `send_as: support.foreign_submission` (was `false`). Update the comment
  from "not yet wired" to describe the gate.

## Bricks (ordered landings)

Each lands as one coherent change kept/reverted on its gate. The order
keeps `brokkr check` green and never advertises `send_as` before the path
exists.

### Brick 1 - Wire the foreign submission path (capability still false)

All of `pim.rs`, `account.rs`, `error.rs`, plus the `factory.rs` +
`account.rs` `foreign_submission` field/param plumbing (the set is
computed and stored, and consulted by routing, but `PimMethodSupport.send_as`
stays `false` this landing, so no consumer issues a `send_as` yet). Removes
`send_as_guard` and its two tests; adds `resolve_foreign_headers`,
`ForeignSubmission`, `send_as_unknown_account`, and the account-layer
routing.

Why green: nothing advertises `send_as`, so the new path is reachable only
by the new unit tests. No existing test asserts the guard's presence once
its own tests are removed in the same landing.

**Gates.**
- Named unit tests (the pieces a deterministic test can pin - the wire
  round-trip is exercised downstream, not here, matching the pre-existing
  `send_message`, which has no wire-path unit test):
  - `resolve_foreign_headers` As/OnBehalfOf/fallback matrix, incl.
    "OnBehalfOf omits Sender without self_address" and "OnBehalfOf honors
    explicit consumer from" and "As overrides consumer from".
  - `send_as_unknown_account_is_malformed` (kind ==
    `Request(Malformed)`, operation `Send`, terminal recovery).
  - The account-layer routing decision: `send_as` `None` yields the
    personal path (no `ForeignSubmission`); a `MailboxId` absent from
    `foreign_mail` yields `send_as_unknown_account`; present-but-not-
    submission-capable yields `Unsupported(Send)`. Assert on the concrete
    `resolve_send_target` return (Review consolidation), not on prose.
  - Deterministic **encoder test** (blessed by the repo test rules; no mock
    server): serialize the built `Email/set` + `EmailSubmission/set` batch
    for a foreign `As` send and assert the JSON pins the foreign
    `accountId`, the forced `identityId`, the resolved `From` (and omitted
    `Sender`), the envelope `mailFrom`, and the `/created/{id}/id` result
    reference. This covers the load-bearing wire shape the pure-decision
    tests cannot reach.
  - Identity selection (obstacle 3 amendment): explicit `request.identity`
    absent from `Identity/get` rejects; a wildcard-only `email` is not
    stamped as `From`; empty identity list rejects `Unsupported(Send)`.
  - Command: `brokkr test -p bifrost-jmap send_as`
  - Command: `brokkr test -p bifrost-jmap resolve_foreign_headers`
- Green-tree gate (covers the wiring the unit tests cannot reach - the
  `Identity/get` + result-referenced `Email/set`/`EmailSubmission/set`
  batch is validated by type-checking and clippy here, end-to-end
  downstream): `brokkr check`

### Brick 2 - Flip the capability gate

`capabilities.rs` (`PimSupport.foreign_submission` field + `send_as`
wiring) and `factory.rs` (`foreign_submission_available` predicate +
`PimSupport` thread-through). After this landing the feature is live: a
session with a non-personal submission-capable account advertises
`send_as == true` and the Brick 1 path serves it.

**Gates.**
- Named unit tests:
  - In `capabilities.rs`: extend the existing session-JSON tests -
    `send_as` follows `support.foreign_submission` (true and false cases).
  - In `factory.rs` (add a `#[cfg(test)]` module with a `session(json)`
    fixture mirroring `capabilities.rs`): `foreign_submission_available`
    is `true` for a non-personal mail+submission account, `false` when the
    foreign account lacks submission, is personal, or is the primary.
  - Command: `brokkr test -p bifrost-jmap foreign_submission`
  - Command: `brokkr test -p bifrost-jmap send_as`
- Green-tree gate: `brokkr check`

## Stopping rule / out of scope

- **Only `send_message` carries `send_as`.** `SendRequest::send_as` is the
  sole surface; `draft_send(DraftHandle)` and `send_raw_message(raw, ...)`
  take no `send_as` and are untouched. A foreign draft-then-send or foreign
  raw send is a separate item if ever wanted.
- **Scheduled foreign sends: decide per obstacle 7.** `cancel`/`reschedule`
  take an `ObjectId`, not a `SendRequest`, so they fall outside the
  "only `send_message` carries `send_as`" line above - yet a scheduled
  foreign send would mint an un-cancelable handle (routed to the primary
  submission account). This spec must commit to obstacle 7 path (a) reject
  `scheduled + send_as`, or (b) owner-encode the handle and route
  follow-ups. Path (a) is the recommended default and keeps this stopping
  rule tight.
- **No foreign bulk mutations.** The A5a foreign-mutation exclusion for
  `bulk_set_flags` / `bulk_move` / `bulk_destroy` stays closed; this spec
  wires only the submission leg.
- **No live foreign-mailbox lifecycle.** Unchanged (foreign mailboxes
  appear at reopen).
- **No new `bifrost-types` surface.** `SendAs` / `send_as` / the
  capability flag already exist from C-3; this is purely the JMAP backend.
- **No integration / mock-server test.** Per the bifrost test rules, the
  `Identity/get` + `Email/set` + `EmailSubmission/set` wire round-trip is
  proven downstream in ratatoskr, not here; the named unit tests pin the
  pure decision/mapping pieces and `brokkr check` is the green-tree gate
  for the wiring.

## Documentation

Update `reference/jmap.md` (the "PIM primitives and conveniences" and
"Foreign (shared/delegate) accounts" / "Known limitations" sections) to
record that JMAP now honors `send_as` via foreign-account
`EmailSubmission/set`, and remove c3-1 from the foreign-mutation "not
wired" limitation.

**Public `bifrost-types` doc comments must also change (no type-shape
change, doc-only).** They currently name Graph as the sole send-as
provider; once JMAP advertises the capability that is stale:
- `crates/types/src/capabilities.rs` (`PimMethodSupport.send_as`, the
  "`true` only on Graph; `false` everywhere else" doc, ~line 190) - widen
  to "Graph and JMAP (foreign submission-capable accounts)".
- `crates/types/src/compose.rs` (`SendRequest.send_as`, the "Honored only
  where ... is `true` (Graph)" doc, ~line 185) - same widening.
These are a `bifrost-types` edit, so they do **not** violate the "no new
`bifrost-types` surface" stopping rule (surface = types/signatures, not
doc text), and they land with Brick 2 (the capability-live landing).

Per the repo git rules, bundle all of this markdown/doc-comment change with
the Brick 2 code commit (the landing that makes the capability live); do
not commit it alone. Once landed, strike **c3-1** from `TODO.md`.
