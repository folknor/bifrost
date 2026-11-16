# IMAP partitioned inventory - design and link plan

Status: design, not built. Tracks the STR-082 entry in `notes/structural.md`.
Sparred with codex 2026-11-16; the recommendation below is the converged
position.

## What is true today

- `ImapAccount` overrides neither `inventory_partitioning` nor
  `inventory_partition_stream`, so every IMAP folder plans `Full` and is read by
  one `UID FETCH 1:*` (`crates/imap/src/account/inventory.rs::run_inventory`),
  oldest UID first, with every UID pushed into a `Vec<u32>` so the cursor can
  carry the complete known-UID baseline (`cursor_from_select(.., Some(known))`).
- First inventory of a folder: `establish_initial_cursor` returns
  `EstablishViaInventory`, so the walk runs in the fusion path
  (`multiplexer/fusion.rs`, `account.inventory_stream`) and the folder cursor is
  published only at the walk's terminal `Done`.
- The backfill orchestrator DOES reach IMAP folders: `BackfillScan::select`
  skips only the first sighting of a fusion-owned scope, and a stored cursor
  resumes as `Ready` without calling `establish_initial_cursor`. Those passes go
  through `inventory_partition_stream`, whose trait default resolves `Full` to
  `inventory_stream` - the same unbounded `1:*` walk.
- `uid_windows` (bifrost-sync `backfill::partitioner`) already plans
  newest-first half-open `uid:{uidvalidity}:{from}:{to}` windows for any
  account that advertises `InventoryPartitioning::UidRange { max_uid: Some(_),
  uid_validity }`; the key grammar, coverage containment (`CoverageDomain`,
  epoch-scoped) and resume are in place. `max_uid: None` falls back to `Full`.
- `inventory_partitioning` is synchronous: it can only report what the account
  already knows.

What a user loses today: on a large folder the first useful message arrives
after the server has streamed the oldest mail, and a backfill pass is one
request whose failure throws away the whole walk.

## Candidate shapes

1. **Fusion-owned bounded enumeration (first inventory).** Keep
   `EstablishViaInventory`. Inside `run_inventory`: SELECT once and keep that
   selection; take UIDNEXT - 1 (or a UID SEARCH ALL worklist) as the
   enumeration ceiling; issue bounded `UID FETCH hi-chunk:hi` bands from the top
   down; accumulate the known-UID baseline and obligations as today; publish the
   cursor only after the full walk through the existing fusion contract.
   Bounded requests and later-assigned mail first, with no change to the
   cursor's publication timing. Does not by itself give durable positional
   resume (fusion refuses `Backfill` checkpoints); if that is wanted later, the
   provider-owned resumable inventory cursor hooks (`inventory_resume_stream`,
   `is_inventory_cursor`, used by Graph) are the route.
2. **Early `Ready` plus partitioned orchestrator backfill.** Establish the
   cursor from SELECT plus UID SEARCH ALL, then let the orchestrator walk UID
   partitions. Rejected: the cursor would go live before any inventory entry is
   published, so a buffered inventory `Created` can land after a live removal
   and resurrect the message (`LiveSupersedes` documents that nothing populates
   it), and a restart with a stored cursor bypasses the establishment that would
   have populated the planning hint.
3. **UID producer for the passes that already reach the orchestrator.**
   `inventory_partitioning` returns `UidRange { max_uid, uid_validity }` from
   registry-held SELECT/STATUS facts and `inventory_partition_stream` serves
   `InventoryPartition::Uid` as `UID FETCH from:to-1`.

**Recommendation: (1) plus (3); (2) rejected.**

## Requirements for (3)

- **The ceiling must cover the resumed baseline.** A cached maximum below a UID
  already in the stored change cursor would leave unchanged messages
  permanently unrequested (changes need not announce them again). The hint must
  come from a fresh authoritative observation, or be proven to be at least the
  baseline's maximum. An epoch match alone is not enough.
- **Hint availability is explicit.** Say where the SELECT/STATUS facts enter the
  folder registry on a resumed attach, and plan `Full` (today's behaviour) when
  they are absent rather than guessing.
- **A partition walk never commits a folder cursor.** It reports partition
  coverage and completion only; it must not reuse `finish_inventory`'s cursor
  finalisation with a window's partial known-UID set.
- **Epoch mismatch invalidates the walk.** A window whose `uid_validity` is not
  the selected UIDVALIDITY is refused and drives scope recovery; it never
  returns successful empty coverage.
- **UIDNOTSTICKY folders advertise `Full`** and keep their existing path (see
  the UIDNOTSTICKY comment in `inventory.rs`).
- (3) keeps the orchestrator's existing inventory-vs-live publication ordering
  limitation; epoch checks prevent identity confusion, not stale inventory after
  a live removal.

## Notes common to both

- Higher UID bands are later-ASSIGNED mail, not necessarily newer dates, and
  FETCH response order inside a band is not guaranteed (RFC 9051 permits any
  order). Strict newest-first presentation needs bounded per-band sorting.
- The `known` baseline stays complete for the cursor; bounding request size
  does not bound baseline memory. `UidSet` ranges are compact, but the current
  collection path builds individual numbers first.

## Link plan

1. bifrost-imap: bounded descending UID bands inside `run_inventory` (shape 1),
   with a scripted-transcript test that the walk issues bounded bands and
   publishes the cursor only after the last band.
2. bifrost-imap: registry-held UIDNEXT/UIDVALIDITY hint satisfying the ceiling
   rule, `inventory_partitioning` returning `UidRange` (or `Full` when unproven
   or UIDNOTSTICKY), and `inventory_partition_stream` for `Uid` windows with the
   epoch refusal and no cursor commit. Tests: windows requested, epoch mismatch
   refused, no cursor written by a partition walk.
3. reference/imap.md: record both behaviours and the ceiling rule at the code
   sites.
