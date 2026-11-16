# Bugs - design hunt, November 2026

Defects the design-hunt hunters turned up on the way. They were not what the
hunt asked for; they are the findings a resolution loop can land first. Each
entry is the hunters' account, unverified; the fix pass verifies and may find
an entry wrong.

1. An entry is removed entirely when completely resolved. No historical record
   stays here.
2. Stable IDs never change and are never reused; removal leaves a gap.
3. An entry adjudicated against, verified incorrect, or whose outcome is that no
   action is taken owes comments at the code sites it names - and, where the
   claim touches a documented contract, the relevant `reference/` or `docs/`
   page - before the entry is removed, so the finding is not hunted again.
4. Once all findings are resolved, the file gets deleted.
5. Finding IDs are never written into the code or other documents. They are
   stable only until this document is drained; the next hunt writes new ones,
   and they are never deduplicated through git history. Carry the context
   inline instead.

Each entry names the hunt scope(s) that reported it. The raw reports are in the
commit that precedes this file's.

## BUG-176 - Open rulings and design residue from the wave 10 to 16 lateral findings

**Reported by:** wave 10 to 16 fixers (lateral); everything fixable in those
bundles landed by wave 16. What remains needs a ruling or a design.

Rulings:

- ManageSieve errors are stamped `Protocol::Imap` because the shared
  `Protocol` enum has no ManageSieve variant (documented at
  `imap/src/account/error.rs::oauth_token_error`). Decide whether ManageSieve
  gets its own protocol identity.
  **Recommendation (owner-agreed, not a ruling):** yes, add
  `Protocol::ManageSieve` and stamp it on the ManageSieve error path, so filter
  failures are never attributed or throttled as IMAP mail failures. Additive on
  a non-exhaustive enum.
- IMAP `native_code` for an unknown response code is the server's own
  spelling, giving that telemetry field unbounded cardinality. Decide whether
  to bound it (for example a fixed `OTHER` with the spelling in diagnostics).
  **Recommendation (owner has no preference; not a ruling):** bound it. Known
  codes keep their canonical name, unknown codes become `OTHER`, and the server
  spelling moves to `support_only` text. Check other providers' `native_code`
  for the same shape.
- Closed-account guards are not universal: the shared `AccountClosed` kind
  replaced existing refusals, but many provider methods have no lifecycle gate.
  **Recommendation (yardstick, not a ruling):** every method gains one. A closed account must refuse
  uniformly with `AccountClosed` before I/O; a door that silently still works,
  or fails with a transport error, after close is not what a user who removed
  an account expects. Would be build work once ruled.
- Gmail: a malformed cursor payload maps to `SchemaIncompatible` (account
  clear) through `CursorDecodeFault::Malformed`, where it used to be
  `CursorInvalid` with a scope restart. Confirm.
  **Recommendation (yardstick, not a ruling):** confirm. Gmail has one cursor
  scope, `CursorScope::Account`, so both mappings end in the same full resync.
  A draft comment at the mapping in `google/src/account/error.rs` and a
  corrected line in `reference/google.md` are in the tree, uncommitted.
- Sync acknowledgement tokens: a wrong or rotated key and a corrupted token
  decode to the same error; a key-identifier byte in the header would separate
  them.
  **Recommendation (owner-agreed, not a ruling):** add the key-id byte under a
  header version bump (old tokens decode as "key id unknown" and keep working).
  Key mismatch and corruption report distinctly, and it opens keyring rotation
  (accept the previous key, issue under the new) so a secret rotation does not
  force every account to resync.

Design:

- Sync `PendingCoverage::supersede` (standalone public form) cannot update a
  token a consumer already saved, and `AckToken` receipts hold the full
  cumulative claim; bounding either needs a new durable evidence owner or an
  acknowledgement contract (documented in `cursor/coverage.rs`).
- CalDAV and CardDAV unconditional deletes: the client absorbs 404 and 410
  under its replay contract before account code sees them, so the shared
  absent-target policy cannot be applied there without moving that decision
  (documented in both `account.rs` files).
- CalDAV and CardDAV updates sent without a strong validator can recreate a
  resource deleted after it was fetched.
- Sync `engine/reattach.rs` closes the replaced account (`close_owed`) without
  a time bound, and `retire_stranded` awaits provider cleanup without one;
  bounding either means deciding when to abandon an obligation cancellation
  hands over (attach and detach use `engine::close_within`).

## BUG-048 - IMAP sync lanes cycle on a `UIDNOTSTICKY` folder

**Reported by:** imap-account; reopened by the wave 1 review.

Inventory and changes on a `UIDNOTSTICKY` folder mint cursors the next SELECT
voids, so the scope cycles CursorInvalid -> re-inventory. Refusing the folder
(as the poller does) was tried and reverted: it would make the folder's mail
vanish from the user's view, which removes a capability. The remaining fix is a
cursor-free sync mode for such folders (re-inventory on each poll, no durable
UID cursor, no CursorInvalid loop); the inventory site and `reference/imap.md`
say so. It cannot be built inside IMAP alone: it needs a shared
`CursorEstablishment` variant in bifrost-types and engine scheduling of
repeated authoritative inventory (today `NoCursor` schedules no further poll),
plus safe hydration and mutation semantics for UID handles that do not survive
a re-SELECT. A wave of one owning types, sync and imap.

## BUG-190 - Graph webhook subscriptions collide with Graph's duplicate rule across handles and on reattach

**Reported by:** the Graph calendar and contact scope discovery work (spar review).

Graph refuses a second subscription with the same `resource` and `changeType`
with 409 Conflict ("Subscription Id <> already exists for the requested
combination", subscription-post-subscriptions, Duplicate subscription
behavior). The webhook arm owns subscriptions per handle, so two cases fail:

- Reattach (`crates/sync/src/engine/reattach/transaction.rs`) creates the
  replacement subscriptions BEFORE deleting the old handle's rows, so every
  replacement create is a duplicate of a live row and 409s; the whole request
  fails and the old handle survives. Affects mail folder resources too, not
  only calendars. Not reproduced live; inferred from the documented rule.
- Two subscribe calls whose scopes share a resource collide. Calendar and
  contact scopes now all map to the mailbox-wide `/events` and `/contacts`
  resources (the only documented ones), so two calls over disjoint calendars
  hit it; before, only two calls over the same mail folder did.

`create_and_register` treats the 409 as whole-request failure with rollback,
which keeps the old coverage alive but makes the second subscribe or the
reattach fail. A fix needs subscription rows owned per account and shared by
handles (reference-counted teardown, renewal once per row), or a reattach that
adopts the old rows instead of recreating them. Laning the 409 per resource
was tried and rejected: reattach would then commit the other groups and delete
the old row, silently dropping working coverage.

## BUG-191 - A Graph claims challenge's `claims` value never reaches sign-in

**Reported by:** the Graph claims-challenge classification fix (spar review).

Graph now classifies a Bearer `WWW-Authenticate` challenge with
`error="insufficient_claims"` as `Authentication(ReauthorizationRequired)`
(`is_claims_challenge` in `crates/graph/src/account/graph_error.rs`). That is
the right kind, but recovery is incomplete: Microsoft requires the challenge's
`claims` value to be passed into the authorization request, and for an
authentication-context (`acrs`) step-up an ordinary sign-in can succeed without
the required claim, so the resource challenges again - a reauth loop. Nothing
carries the value: `AccountError` has no consumer-readable slot for it,
`TokenSource` has no refresh-with-claims, and on a 401 bifrost-net spends its
forced refresh without claims and builds `AuthLost` without reading the
header (the 401 header survives only as response evidence). A fix needs a
contract across bifrost-types (structured payload and its export tier),
bifrost-net (do not silent-refresh a claims 401; keep the claims), the token
source, and ratatoskr's MSAL call. Covers both the 401 and 403 paths.

## BUG-199 - Graph's KQL search route has two pre-existing gaps

**Reported by:** the attachment-search contract fix (spar, lateral). More
filters now take the route (`Attachment(Name)` and `ProviderQuery` leaves join
`From`/`To`), so these bite more often. (Its KQL date range, which used the
received clock, was moved to send time in the same fix.)

In `crates/graph/src/account/pim/search.rs`: (1) `In` plus any KQL-only leaf
is refused `Request(Malformed)`, yet Graph supports `$search` on a folder's
own message collection (`/mailFolders/{id}/messages`), so a positive `In`
conjunct could become the endpoint scope; (2) `$search` returns at most 1,000
results, and the search pages hardcode `truncated: false`, so hitting the
ceiling reads as an exhaustive end.

## BUG-200 - JMAP has no attachment-name search

**Reported by:** the attachment-search contract fix (spar, lateral).

`SearchFilter::Attachment(Name)` is refused `Unsupported` on JMAP because
RFC 8621 has no attachment-name condition (`text` searches other fields). An
Outlook user on a JMAP account therefore cannot search attachments by name.
Candidates: a server extension condition when the session advertises one
(Cyrus/Fastmail-style `attachmentName`), or client verification of a positive
top-level conjunct against `bodyStructure` names. Verification is only exact
in that position, and `hasAttachment` is not a proven candidate superset of
named parts, so neither path is a drop-in.

## BUG-201 - IMAP search counts named inline parts as attachments; raw-query literal doc mismatch

**Reported by:** the attachment-search contract fix (spar, lateral).

`parts::attachment_entities` counts any part with a filename, so a signature
logo sent `inline` with a name makes `Attachment(Any)` match on IMAP, while
Graph's `hasAttachments` excludes inline attachments (an Outlook user's
paperclip). Whether IMAP should follow the paperclip is a product decision
over the entity rule ruled on for the BODYSTRUCTURE evaluator. Separately,
`reference/imap.md` says raw SEARCH criteria "retain caller-written
literals", but `validate_provider_query` in `crates/imap/src/account/search.rs`
refuses any IMAP literal in `provider_query` (and now in a `ProviderQuery`
leaf); one of the two is wrong.

