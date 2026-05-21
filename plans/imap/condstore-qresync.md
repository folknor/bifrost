# IMAP CONDSTORE and QRESYNC

Engineering reference for bifrost-imap's CONDSTORE / QRESYNC
implementation. Distilled from ratatoskr's production experience,
which ships CONDSTORE but not the QRESYNC `VANISHED` consumption
path.

Bifrost-imap implements fresh against its own driver-based codec
rather than porting ratatoskr's `async-imap` code. What ports is the
semantics and the quirk catalog below, not the implementation.

## Three-state cursor lifecycle

The folder cursor knows which sync path it was created under and the
change stream picks the matching diff strategy:

- **QRESYNC-negotiated.** `MODSEQ` diff via `CHANGEDSINCE` for flag
  and content changes; `VANISHED` for expunges. Cheap. One round-trip
  for both axes of change. Not yet shipped in ratatoskr.
- **CONDSTORE-only.** Capability advertised but QRESYNC did not
  enable, or only CONDSTORE was advertised. `MODSEQ` diff for flag
  changes; UID-list diff required for expunge detection. Medium cost.
  Shipped path in ratatoskr.
- **Neither.** Full UID-list diff for every change category.
  Expensive on folders with many messages. Shipped path in ratatoskr.

The three-state distinction must surface to the consumer as a cost
class on the cursor, not as an internal hidden choice. The sync
engine uses cost class to schedule work (do not start a multi-hour
Basic-state diff on cellular network or battery).

## Quirk catalog

Per-server behavior, validated against production traffic:

- **Gmail.** CONDSTORE-only; QRESYNC not advertised. Reliable
  CONDSTORE semantics otherwise. Per-folder watch: some Gmail
  folders return `HIGHESTMODSEQ=0` after SELECT even with CONDSTORE
  negotiated session-wide; treat as "no persistent mod-sequences for
  this folder" and downgrade that folder to Basic without touching
  other folders.
- **Dovecot.** Reference implementation. Full QRESYNC + CONDSTORE
  with no known quirks. Validate against Dovecot first when shipping
  QRESYNC changes.
- **Cyrus.** Powers Fastmail. Full RFC 7162 compliance.
- **Stalwart.** Rust IMAP4rev2 server. Mandatory CONDSTORE,
  maintains mod-sequence changelog for QRESYNC.
- **iCloud.** Advertises QRESYNC but is buggy on two axes:
  - The `ENABLE QRESYNC` round-trip does not produce the required
    `ENABLED QRESYNC` reply. Detect via "advertised but did not
    ENABLE" check and downgrade the session to CONDSTORE-only.
  - FETCH responses during QRESYNC SELECT can carry **negative
    sequence numbers** or other malformed shapes. On parse failure
    or out-of-range sequence numbers during the response drain,
    disable QRESYNC for the session (one-shot signal, do not retry)
    and continue in CONDSTORE-only mode.
- **Yahoo / AOL.** Full QRESYNC + CONDSTORE. CONDSTORE and OBJECTID
  available on all mailboxes.
- **Zimbra.** Full RFC 7162 support. Tested by Thunderbird.
- **Exchange / O365 IMAP.** No CONDSTORE, no QRESYNC. Microsoft
  steers tenants to Graph API. Forces the Basic cursor path on
  bifrost-imap.
- **Courier.** No CONDSTORE, no QRESYNC. No MOVE. Legacy, declining
  usage.
- **hMailServer.** No CONDSTORE, no QRESYNC. Windows-only, minimal
  extension support.

Capability detection should fingerprint the server (`ID` response,
greeting banner) and pre-configure the cursor strategy where possible
rather than relying on advertised capabilities alone.

## Implementation footguns

Real-world hazards that show up only after the happy path works:

- **Drain untagged `VANISHED` on every command in a QRESYNC session.**
  Untagged `VANISHED` can arrive at any time during a QRESYNC session,
  not just after a `SELECT (QRESYNC ...)`. Every command's response
  handler must consume them.
- **Cross-check UID count vs. `Mailbox.exists` after delta application.**
  After applying a diff, the local UID count must match the server's
  `EXISTS` value. Mismatch means the diff missed expunges. Treat as
  a cursor invalidation, not a panic.
- **Treat modseq reset at the same UIDVALIDITY as forced resync.**
  Some servers reset `HIGHESTMODSEQ` without changing UIDVALIDITY.
  This is not strictly a CONDSTORE-spec violation but it requires
  discarding cached `MODSEQ` and resyncing flags fully.
- **Tolerate iCloud's malformed FETCH responses by disabling QRESYNC
  for the session on parse failure.** Do not abort the session; mark
  the cursor as CONDSTORE-only and continue.

## Validated throttle intervals

Production-tuned for non-CONDSTORE / non-QRESYNC fallback paths:

- `FLAG_SYNC_INTERVAL_SECS = 300` (5 minutes) for periodic flag-
  state reconciliation when MODSEQ is not available.
- `DELETION_CHECK_INTERVAL_SECS = 600` (10 minutes) for UID-list-
  diff expunge detection on the Basic cursor path.

These intervals balance freshness against server load on folders
with hundreds of thousands of messages. Surface them as configurable
defaults on the cursor; do not hard-code in the diff strategy.

## Per-folder capability downgrade

Capability negotiation is session-scoped (`CondstoreQresyncState
{ condstore, qresync }` lives on the connection, not the account or
folder). But even with CONDSTORE negotiated session-wide, an
individual folder can return `HIGHESTMODSEQ=0` (or absent) after
SELECT. That signal means "this folder does not support persistent
mod-sequences here" - downgrade that folder to the Basic cursor
path without touching the session-wide capability state or other
folders.

The downgrade does not surface to the consumer as Fatal; it is
internal to the protocol crate and reflected in the per-folder
cursor's `FolderCursor` variant (defined in
`plans/sync-engine.md`). The engine sees `cost_class()` rise from
`Cheap` to `Expensive` for that folder and schedules accordingly.

A `Warning::StrategyDowngraded { from: Condstore, to: Basic,
reason: "HIGHESTMODSEQ=0 after SELECT" }` lets observability catch
the transition.

## MOVE (RFC 6851) interaction with QRESYNC

When a server supports both QRESYNC and MOVE (Dovecot, Cyrus,
Stalwart, Gmail, iCloud, Zimbra), QRESYNC sends `* VANISHED <uids>`
(not `* EXPUNGE`) for moved messages. Servers with UIDPLUS
additionally send `COPYUID <uidvalidity> <src-uids> <dest-uids>` to
map old UIDs to new UIDs in the destination folder.

Without MOVE (Courier, hMailServer), the engine must observe
`COPY + DELETE + EXPUNGE` as a synthetic move. This matters for the
engine's change taxonomy: a moved message should not surface as
`ObjectChange::Destroyed` in the source plus `Created` in the
destination - both events refer to the same `ObjectId` from the
consumer's point of view. With MOVE + UIDPLUS, the engine can
correlate via `COPYUID`. Without it, the engine falls back to
correlating by `message_id` (RFC 5322) carried in inventory.

For QRESYNC sessions, the protocol crate must drain `* VANISHED`
events on every command's response stream (see Implementation
footguns), not just after SELECT and FETCH.

## STORE UNCHANGEDSINCE for IMAP concurrency

`STORE UNCHANGEDSINCE <modseq>` (RFC 7162) is IMAP's native lost-
update protection: the STORE fails if the message's `MODSEQ` has
advanced past the supplied value. This is the IMAP analogue of
JMAP `ifInState` and Graph `If-Match: <etag>`, and the right hook
for `MutationConcurrency::StateBased` on IMAP (see
`plans/account-trait.md` -> Capabilities).

Concurrency and replay safety are distinct concerns (see
`plans/account-trait.md` -> Capabilities). IMAP has no native
replay token regardless of `STORE UNCHANGEDSINCE`, so
`MutationReplaySafety` stays `None` and the engine guards retries
via read-back-after-retry.

Requires per-message `MODSEQ` in FETCH responses, which `imap-proto`
does not currently parse. Bifrost-imap's own driver-based codec can
model this natively. Until it lands, IMAP mutations use
`MutationConcurrency::None`.

The matrix:

| State                              | MutationConcurrency  |
|------------------------------------|----------------------|
| `imap-proto` lacks per-msg MODSEQ  | `None`               |
| Per-msg MODSEQ + CONDSTORE         | `StateBased`         |
| Per-msg MODSEQ + QRESYNC           | `StateBased`         |
| Server lacks CONDSTORE             | `None`               |

`MutationReplaySafety` is `None` across all rows.

## CONDSTORE-only exit ramp

If QRESYNC `VANISHED` consumption keeps regressing during
implementation, the cursor degrades cleanly to CONDSTORE-only with
no consumer-visible behavior change beyond a higher cost class. The
sync engine treats the cursor opaquely; only the protocol crate
cares which path is in use. This is the rationale for shipping
CONDSTORE-only first and QRESYNC as an optimization layer on top.

Delta Chat (same `async-imap` stack ratatoskr uses) made this call
deliberately: they do not implement QRESYNC at all. Their stance is
the strongest "you can stop at CONDSTORE-only" argument available.
Bifrost's 150-300 GB cached-mailbox / multi-year-history target is
the reason we do not take that exit by default - but if QRESYNC
keeps regressing in field testing, CONDSTORE-only +
`run_deletion_detection`-equivalent UID-list-diff is a viable end
state, not a failure. Decide consciously rather than discover by
attrition.

## Rollout

Land QRESYNC behind a runtime gate, off by default. Validate
against Dovecot (gold standard), Stalwart (Rust, RFC-strict), and
Cyrus before flipping the default. Keep the CONDSTORE-only path
reachable as a panic-button fallback per-account, not just
per-capability - operator override via consumer config, surfaced as
`Warning::OperatorAttentionNeeded` when QRESYNC field-failure rates
cross a threshold.

This is the Thunderbird "years to ship" pattern, applied
deliberately rather than discovered by attrition.

## Codec note

`imap-codec` (duesee) has an `ext_condstore_qresync` feature that
would be the architecturally correct path: type-safe CONDSTORE /
QRESYNC with fuzz-tested parsing. As of May 2026 it is marked
"Unfinished" and is not viable for production. Bifrost-imap's own
driver-based codec can model these as first-class commands and
responses without waiting; revisit `imap-codec` when the feature
matures.

The `async-imap` / `imap-proto` stack ratatoskr uses ships the
workaround for `CHANGEDSINCE` (append the modifier to the
`uid_fetch` query string) and exposes `Response::Vanished { earlier,
uids }`. Per-message `MODSEQ` in FETCH is not yet parsed by
`imap-proto`, which gates `STORE UNCHANGEDSINCE` (see above).

## Sources

- RFC 7162 (CONDSTORE / QRESYNC).
- RFC 6851 (IMAP MOVE).
- Ratatoskr's CONDSTORE production implementation. External to this
  repo; do not cross-link from source comments.
- Thunderbird bugs 1124569 (VANISHED-without-IDLE drops), 1123094
  (folder contents drift), 885220 (Gmail CONDSTORE flakes) for
  field-tested footguns.
- chatmail/async-imap #130 (closed not-planned; documents the
  `CHANGEDSINCE` workaround).
