# bifrost-jmap `core/` + `sync/` sweep

Scope: implementation fixes primarily concern `crates/jmap/src/core/**` and
`crates/jmap/src/sync/**`, with their directly-owned wire types where needed;
supporting plans and references track their current state.

Read first: `reference/jmap.md`, `reference/error-model.md`,
`plans/bug-hunt-2026-06-17.md`, `plans/jmap/*`. Nothing below re-reports a
finding already closed there.

This file is NOT the whole open-gap picture for bifrost-jmap: it holds only
what this sweep found and has not yet closed. Standing jmap gaps that predate
or outlive it live in `TODO.md` as `nc-*` (currently nc-6 hydration flush
ordering, nc-7 unqualified foreign thread ids, nc-8 unreported foreign
container rights). nc-7 in particular is a live correctness hazard on the
mutation paths this sweep just wired.

---

## 1. Bugs

No open findings. B9 (every foreign mailbox scope replayed the whole
account-wide `Email/changes`) closed by collapsing the foreign cursor
topology to one account-level `Folder` scope per shared account - option (a)
of the two it proposed - which also collapsed the push-routing fanout to one
hint per foreign notification and surfaced a hydration defect on the way:
`get_stream`'s `Metadata` projection returned foreign entries with bare
native `mailboxIds` / entry / blob ids, which would have broken the folder
attribution the new topology relies on (and already misrouted foreign blob
reads). Both fixed in the same commit.
