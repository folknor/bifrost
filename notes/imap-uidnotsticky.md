# IMAP UIDNOTSTICKY folders: design chain

Status: chain, not started beyond link 0. Owner of the item: BUG-048 in
`notes/bugs.md`.

## Problem

A mailbox the server marks UIDNOTSTICKY (RFC 4315 Section 3) gets a different
UIDVALIDITY on each selection. Bifrost object ids encode `(folder,
uidvalidity, uid)`, and `select_folder` always sends a fresh SELECT/EXAMINE, so:

- Every changes poll fails `UidValidityChanged` -> `CursorInvalid`, published
  to consumers as `SyncEvent::Terminated`, and `RestartScope` re-runs
  inventory. The restart succeeds, so no retry budget is spent: the folder
  re-inventories once per poll interval.
- That cycle does not retire the old generation. `reset_scope_for_restart`
  clears cursor rows only, and IMAP inventory emits no removals for ids it did
  not enumerate. Example: generation G holds A, generation H is empty; nothing
  tells the consumer A is gone.
- Hydration (`get.rs`, `message.rs`, `parts.rs`) and mutation (`mutate.rs`)
  compare the id's UIDVALIDITY against their own fresh selection, so on a
  server that rotates per selection no message in the folder can be read or
  changed. Pooled connection affinity does not help: the pool picks a
  connection but the SELECT is still issued.
- Refusing the folder (as `ImapPoller` does) is ruled out for the account
  path: hiding a folder's mail removes a capability.

Link 0 (landed with this document): the SELECT consumer now recognizes the
RFC-prescribed `* NO [UIDNOTSTICKY]` form. Before it only the OK form set
`uid_not_sticky`, so on a conforming server the flag was never seen - the
poller did not refuse, and inventory used bounded UID bands and advertised UID
partitions for the folder. With the flag recognized, inventory takes its
existing unbounded walk and partition planning stays `Full`.

## Rejected shapes

- **Recurring inventory alone** (a new `CursorEstablishment` variant plus
  engine scheduling of repeated authoritative inventory). Removes the
  per-poll `Terminated`, but ships a cross-crate contract that still leaves
  the folder unreadable, and it may be the wrong scheduling contract: a
  retained-selection model (below) would want repeated enumeration within one
  generation, and a fresh authoritative inventory per poll destroys exactly
  the context being retained.
- **IMAP-local generation roll** (changes treats the UIDVALIDITY change as
  expected and emits Removed for every known old id plus Added for every new
  UID). Same unreadable-folder hole; its removals are only as complete as the
  cursor's baseline (legacy incomplete QRESYNC baselines exist); and retiring
  a generation before the new enumeration succeeds needs a partial-failure
  contract.
- **Identity bridge by Message-ID** (resolve a stale id with `UID SEARCH
  HEADER Message-ID`). Unsound: if A (Message-ID M) disappears and B carrying
  M arrives, the single new match is B, and a queued delete of A deletes B.
  Messages may lack a Message-ID or share one; SEARCH HEADER is substring
  matching; resolving on one selection and acting after another reintroduces
  the defect; after an uncertain COPY or move a single remaining match cannot
  say whether replay is safe. Uniqueness in the new generation does not prove
  continuity from the old one.

## Link plan

1. **Stickiness observation in the folder registry.** Today
   `FolderEntry::observe_selection` folds not-sticky into `inventory_hint =
   None`, the same value as "never observed". Record an explicit observation
   (unobserved / sticky-as-of-last-selection / not-sticky). "Sticky" promises
   nothing about UIDVALIDITY staying put; cached observations never replace
   epoch validation when redeeming a partition; an older selection finishing
   after a newer one must not overwrite it. Behaviour-preserving: nothing
   branches on it yet. First establishment is unobserved, so later links must
   handle discovering non-stickiness during the first inventory.
2. **Access model.** Decide and prove how a not-sticky folder's messages are
   enumerated, hydrated and mutated. Leading candidate: retained selection -
   enumerate, hydrate and operate through one held selection, with explicit
   expiry on disconnect or reselection. Questions it must answer:
   - Pool pressure: more not-sticky folders than sessions starves others if
     each holds a selection; eviction expires handles, so the model must say
     how a user reads an expired item and how queued mutations settle.
   - EXAMINE-then-SELECT for a mutation is itself a reselection; hold the
     selection read-write where mutation is expected, or handle the switch.
   - Routing: the selection outlives the inventory future, so ownership lives
     behind the `Account` interface, not inside one stream.
   - Generation retirement: who emits removals for the old generation, and
     generation fencing across fusion, backfill (which selects on its own),
     hydration results and acks so a late old-generation page cannot
     resurrect retired objects.
   - Mutation campaigns: stale-generation failures become `BlockedByEngine`
     and a scope restart does not translate campaign ids; read-back and retry
     need the same treatment as first dispatch.
   This link may itself need engine/types changes.
3. **Engine/types lifecycle contract** that link 2 requires - recurring
   inventory if that is what it selects, otherwise whatever scheduling and
   expiry contract it does need. Not committed to a shape until link 2 lands.

## Open question for the owner

None blocks link 1. Link 2 contains a product question an Outlook-shaped
answer may settle (what a user sees when they open a message whose handle
expired), recorded here rather than decided.
