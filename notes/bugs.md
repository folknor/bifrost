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
or was refused; one design item remains.

- Sync `PendingCoverage::supersede` (standalone public form) cannot update a
  receipt a consumer already saved, and `AckToken` receipts hold the cumulative
  claim. Narrowed: `CoverageClaim::absorb` now collapses adjacent identical
  `Complete` proofs, so a clean single-domain walk is one proof per token and
  linear retained evidence. Residue: degraded evidence, and clean pages over
  distinct domains, still accumulate per page. A hard bound needs a durable
  evidence owner (a store row tokens reference) and a new acknowledgement
  contract. A cheaper in-process alternative, unbuilt: share report prefixes
  between receipts (persistent list) so memory is linear while each token
  still encodes standalone; serialized size stays cumulative.

## BUG-048 - IMAP sync lanes cycle on a `UIDNOTSTICKY` folder

**Reported by:** imap-account; reopened by the wave 1 review.

Inventory and changes on a `UIDNOTSTICKY` folder mint cursors the next SELECT
voids, so each poll publishes `Terminated(CursorInvalid)` and re-inventories.
Worse than noise: the restart never retires the old generation's ids, and
hydration and mutation of any id fail against their own fresh SELECT, so on a
server that rotates UIDVALIDITY per selection the folder's mail is listed but
unreadable. Refusing the folder is ruled out (removes a capability). The
recognition defect (the RFC's `* NO [UIDNOTSTICKY]` form was ignored) is
fixed. The rest is a chain - stickiness observation, an access model for
handles that do not survive a selection, then the engine contract it needs -
in `notes/imap-uidnotsticky.md`, which also records why recurring inventory
alone, a generation roll in changes, and a Message-ID identity bridge were
rejected.

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

