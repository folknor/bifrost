# IMAP partitioned inventory - design and link plan

Status: links 1 and 2 built; link 3 remains. Tracks the STR-082 entry in `notes/structural.md`.
Sparred with codex 2026-11-16; the recommendation below is the converged
position.

## Starting state before link 1

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

1. DONE (2026-11-16). bifrost-imap: bounded descending UID bands inside `run_inventory` (shape 1),
   with a scripted-transcript test that the walk issues bounded bands and
   publishes the cursor only after the last band.
2. DONE (2026-11-16). bifrost-imap: registry-held UIDNEXT/UIDVALIDITY hint satisfying the ceiling
   rule, `inventory_partitioning` returning `UidRange` (or `Full` when unproven
   or UIDNOTSTICKY), and `inventory_partition_stream` for `Uid` windows with the
   epoch refusal and no cursor commit. Tests: windows requested, epoch mismatch
   refused, no cursor written by a partition walk.
3. reference/imap.md: record both behaviours and the ceiling rule at the code
   sites.

## Link 1 implementation rulings (2026-11-16)

- Bands span 65,536 UID positions, descending from the frozen SELECT UIDNEXT
  minus one. The selection stays checked out throughout. UIDNEXT 1 completes
  without FETCH. Missing UIDNEXT uses UID SEARCH ALL and its maximum as the
  ceiling (zero for an empty result), rather than guessing or sending `*`.
  This keeps tolerated missing metadata working while bounding FETCH requests.
- Each non-final band's residual entries are flushed as a checkpoint-free
  page, so sparse bands also deliver later-assigned mail before older requests.
  Response order within a band stays server-defined; no date ordering is promised.
- UIDNOTSTICKY retains the existing `1:*` walk, per the existing recurring
  inventory limitation. The complete known-UID baseline and obligations remain
  accumulated across bands. Only the existing terminal finalizer writes a cursor.
- Scripted duplex tests cover explicit and absent UIDNEXT, descending requests,
  complete baseline publication, and failure in a later band without a cursor.

Remaining work: link 3 is a small documentation link for partition behaviours,
SELECT provenance on resumed attach, and the ceiling rule. Link 1's inventory
behaviour is already recorded in the durable reference. No decision handed back.

Validation for link 1: `brokkr test -p bifrost-imap inventory_descending_bands`
passed all three tests in workspace and install-shape resolution. Reverting the
production inventory change made all three fail in both resolutions; production
was restored. `brokkr check -p bifrost-imap -p bifrost-sync` stopped at gremlins
in a concurrent MIME test edit (U+2603 in `crates/mime/src/writer_tests.rs`), before
compilation. The separate gate agent still owns final validation and commit.

## Link 2 implementation rulings (2026-11-16)

- `FolderEntry` holds an incarnation-local SELECT/EXAMINE UIDVALIDITY and
  UIDNEXT-minus-one hint. The common `ImapAccount::select_folder` path records
  it only after successful selection. Factory LIST does not provide these
  facts. On resumed attach, planning stays Full until an inventory, changes,
  read or mutation selects that folder in this account incarnation. This avoids
  adding per-folder discovery round trips or making attach fail on an optional
  planning probe. No old incarnation's hint is restored from storage.
- A fresh selection's UIDNEXT covers UIDs assigned before attach in that epoch
  (including the stored baseline). Later SEARCH/FETCH can observe arrivals
  beyond that selection, so planning also takes the maximum UID of the
  registry's completed same-epoch baseline. An epoch disagreement with that
  baseline plans Full until recovery replaces it. UIDNOTSTICKY and absent
  UIDNEXT clear the hint and plan Full; epoch equality alone never creates one.
- UID partitions select once and issue the exact inclusive `from:to-1` FETCH,
  with a zero lower endpoint normalized to UID 1. Empty windows (including the
  exclusive endpoint above u32::MAX) validate the epoch then complete without
  FETCH. This handles every window the shared type admits without wildcard
  operands, underflow or truncated endpoint conversion.
- Epoch mismatch or UIDNOTSTICKY on redemption terminates with scoped
  CursorInvalid/RestartScope before FETCH, even for an empty window. Full walks
  retain their existing UIDNOTSTICKY behaviour. A user must never receive
  successful coverage of identities that now name different messages.
- Partition pages and completion report the requested UID coverage domain and
  object obligations, with no change checkpoint or folder cursor write.
  Out-of-window unsolicited responses are ignored. FETCH command completion
  remains authoritative; a failed command cannot emit Done. Partial walks do
  not accumulate a known-UID baseline. Composed DAV scopes delegate planning and
  partition requests to their owning account.
- Hermetic tests cover request bounds, omitted unsolicited UIDs, unchanged
  cursor/checkpoints, epoch and UIDNOTSTICKY refusal, absent UIDNEXT, empty
  windows and maximum endpoint arithmetic, empty-folder hint, and the completed
  baseline extending beyond SELECT's ceiling.

Validation for link 2: `brokkr test -p bifrost-imap inventory_partition` passed
all seven tests in workspace and install-shape resolution. Temporarily removing
the partition finalizer guard made
`inventory_partition_window_does_not_commit_cursor` fail in both resolutions
because a change checkpoint was exposed; the guard is restored.
`brokkr check -p bifrost-imap -p bifrost-sync` passed the convention phases but
stopped at clippy compilation with E0425 for `is_status_line` and `read_status`
in the concurrent `crates/imap/src/sieve.rs` edit. That gate ran no tests.
`git diff --check` passed. The separate gate agent owns final validation and
commit. Link 3 remains small; no link 2 implementation work or open decision
is handed back.
