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

**Reported by:** wave 10 to 16 fixers (lateral). The rest of the bundle landed
or was refused; two design items remain.

- Closed-account guards are not universal. Only Google gates every future;
  IMAP's pool answers a retryable `Transport(Network)` after close; Graph and
  JMAP gate only push; CalDAV and CardDAV gate nothing. Sparred design, a
  chain: a shared wrapper in bifrost-types with generated forwarding and
  admission at first poll, in `notes/closed-account-gate.md` with its link
  plan.
- Sync `PendingCoverage::supersede` (standalone public form) cannot update a
  receipt a consumer already saved, and `AckToken` receipts hold the full
  cumulative claim. Bounding either needs a durable evidence owner and a new
  acknowledgement contract. The limitation is now in `supersede`'s rustdoc and
  `reference/sync.md`.

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

## BUG-190 - A Graph subscription row this process does not know still 409s every subscribe for its resource

**Reported by:** the Graph calendar and contact scope discovery work (spar
review); narrowed by the shared-row fix.

The in-process cases are fixed: rows live in a factory-shared `WebhookStore`
(`crates/graph/src/account/push/registry.rs`) and handles lease them, so a
reopen and overlapping subscribes attach instead of creating a duplicate. The
residue is a row the store cannot see: one left by a previous process (a crash,
or a teardown that never ran; it lives up to the seven-day expiry we request),
or one made by a second `GraphAccountFactory` for the same credentials. Graph
answers 409 to every create for that resource until it expires, and the
subscribe fails whole-request, so the account has no push for up to a week.
A fix means listing `/subscriptions` on 409 and adopting or deleting the row
whose resource and notification URL match. Adopting keeps a clientState that
may have been rotated; deleting can kill another live process's identical
subscription (the trade-off `run_subscribe`'s doc records). Product decision
before build. Not reproduced live; the 409 rule is documented
(subscription-post-subscriptions, Duplicate subscription behavior).

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

## BUG-208 - Graph KQL compiler drops empty fragments and clamps dates

**Reported by:** the Graph search request-binding fix (spar, lateral).

In `crates/graph/src/account/pim/search.rs` `kql_compile`, an unbounded
`DateRange { after: None, before: None }` (constant true) compiles to an
empty string. `kql_join` drops empty parts, which is right under `And` but
wrong under `Or` (a true disjunct vanishes, narrowing the result), and a
bare `Not` of it emits `NOT ()`. The `$filter` path's `join_filters` has the
same shape. Separately, `utc_civil` in `pim/common.rs` clamps an
unrepresentable instant to the epoch, which can move a KQL date bound in the
direction that drops matches. Candidate: Boolean constants through
composition, and refusing unrepresentable bounds as `Request(Malformed)`.

## BUG-209 - Graph KQL `category:` equivalence to `Labeled` is unverified

**Reported by:** the Graph search request-binding fix (spar, lateral).

On the `$filter` path `Labeled` is exact category membership
(`categories/any(c:c eq ...)`); on the KQL path, which a raw query or a
From/To sibling forces, it is `category:"..."`. Graph's searchable-property
table does not list category and Exchange's KQL docs describe it loosely, so
whether `category:` matches exactly the same messages (and so stays correct
beneath `Not`) is unestablished. Needs a live probe or a documented
statement; refuse `Labeled` on KQL if it is not exact.

## BUG-210 - Gmail search cursor is not bound to the query

**Reported by:** the Graph search request-binding fix (spar, lateral).

`crates/google/src/account/page_cursor.rs` records only the surface tag and
Gmail's `pageToken`; `search` rebuilds `q` from the resuming request. A
cursor resumed under a changed filter or `provider_query` sends the old
query's `pageToken` with a new `q`, whose meaning Gmail does not define.
IMAP (`imapsearch3:` binding) and now Graph refuse that as
`invalid_page_cursor`; Gmail should bind the same way (filter plus trimmed
raw query, not `limit`).

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


