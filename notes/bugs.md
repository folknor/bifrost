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
- IMAP `native_code` for an unknown response code is the server's own
  spelling, giving that telemetry field unbounded cardinality. Decide whether
  to bound it (for example a fixed `OTHER` with the spelling in diagnostics).
- Closed-account guards are not universal: the shared `AccountClosed` kind
  replaced existing refusals, but many provider methods have no lifecycle gate.
  Decide whether every method gains one.
- Gmail: a malformed cursor payload maps to `SchemaIncompatible` (account
  clear) through `CursorDecodeFault::Malformed`, where it used to be
  `CursorInvalid` with a scope restart. Confirm.
- Sync acknowledgement tokens: a wrong or rotated key and a corrupted token
  decode to the same error; a key-identifier byte in the header would separate
  them.

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

## BUG-177 - JMAP projects three distinct set-error types onto `JmapMethod::Forbidden`

**Reported by:** the provider-vocabulary consolidation (spar review).

`set_error_type_to_jmap_method` in `crates/jmap/src/sync/error.rs` maps
`SetErrorType::ForbiddenFrom`, `ForbiddenMailFrom` and `ForbiddenToSend` to
`JmapMethod::Forbidden`, although the shared vocabulary has a variant for each.
The wire cause, and so `CauseSummary.native_code`, reports `forbidden` for a
`forbiddenMailFrom` refusal; an admin reading support exports cannot tell a
sender-identity refusal from a plain permission refusal. The primary
classification may stay collapsed; the wire evidence should map each type to
its own variant, with a test over every `SetErrorType` and `MethodErrorType`
spelling against `JmapMethod::code()`. Not fixed with the vocabulary change
because the file was mid-edit by another item.

## BUG-178 - IMAP `[METADATA ...]` native codes fail the telemetry token check

**Reported by:** the provider-vocabulary consolidation (scout, by reading).

`ImapResponseCode::code()` spells the four METADATA codes with an embedded
space (`METADATA LONGENTRIES` and so on), and IMAP account translation uses
that as `native_code`. `TelemetryToken::new` admits only `is_ascii_graphic`
bytes, so the code is dropped from telemetry and demoted to support text. Not
executed; confirm with a test, then pick a token spelling for the telemetry
field (the wire cause can keep the RFC spelling).
`create_and_register` treats the 409 as whole-request failure with rollback,
which keeps the old coverage alive but makes the second subscribe or the
reattach fail. A fix needs subscription rows owned per account and shared by
handles (reference-counted teardown, renewal once per row), or a reattach that
adopts the old rows instead of recreating them. Laning the 409 per resource
was tried and rejected: reattach would then commit the other groups and delete
the old row, silently dropping working coverage.

## BUG-191 - Graph classifies a Conditional Access claims challenge as `PermissionDenied`

**Reported by:** the Graph calendar and contact scope discovery work (spar review).

Graph answers 403 with `insufficient_claims` (and a `WWW-Authenticate` claims
challenge) when the token needs extra claims; that is recovered by
re-authenticating, not by consent. `GraphSignal` has no variant for it, so it
falls to the 403 status and becomes `Authorization(PermissionDenied)`, which
reads to the engine and to the user as missing permission. Since the webhook
arm now files a `PermissionDenied` create as a per-resource lane failure, a
challenge confined to one resource would drop that resource's push coverage
until the next subscribe. Fix in `crates/graph/src/account/graph_error.rs`:
recognise the challenge and map it to an authentication kind. Not fixed with
the discovery change because that file was mid-edit by another item.

## BUG-196 - `SearchFilter::Has` means three different things across providers

**Reported by:** the IMAP attachment-search fix (lateral).

The shared doc in `crates/types/src/search.rs` says the value matches "either
the filename suffix or the MIME type depending on protocol". IMAP now matches
an empty value as any attachment, a value with `/` as a MIME type, anything
else as a filename substring. JMAP (`sync/pim/search.rs`) sends
`hasAttachment AND text(value)`, a full-text match on any attachment-bearing
message. Gmail (`account/query.rs`) maps `attachment` to `has:attachment` and
anything else to `filename:`. Graph (`account/pim/search.rs`) supports only
the empty value and refuses a non-empty one as `Unsupported`. A consumer
cannot know what `Has("pdf")` will return. Decide one cross-provider meaning
(the yardstick suggests the IMAP one: an Outlook user typing a filename or a
type expects attachments that have it), write it on the variant, and bring the
other providers to it.

