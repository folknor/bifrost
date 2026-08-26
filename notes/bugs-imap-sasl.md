# bifrost-imap and bifrost-sasl: hunt findings

Scope: `crates/imap/` and `crates/sasl/`.

## Smaller / lower confidence

- **`StoreResult`'s visibility contradicts `Connection::uid_store`'s. Rejected.**
  `ImapConnection`, its `connection` module, and the entire raw command surface
  are crate-private. The public-looking method and result fields are internal
  visibility within private modules, so no external consumer can reach
  `uid_store` or need to name `StoreResult`. Re-exporting the result at the crate
  root would expose one isolated wire type without making the command surface
  usable and would contradict the intentionally small public account API in
  `reference/imap.md`. No code change is warranted.

## Structural read

The proposed async `Strategy` runner was not adopted as stated. QRESYNC owns a
mid-stream downgrade rule that is legal only before any page escapes, including
connection discard and a retry on a separately checked-out CONDSTORE path.
CONDSTORE has a fallible bounded FETCH stream but no VANISHED lane. Basic has no
change-source stream at all and can avoid SEARCH when UIDNEXT and EXISTS prove
the membership snapshot unchanged. Hiding those lifetimes and fallback states
behind one async trait would make the runner own strategy-specific wire policy,
not merely the two hooks the proposal allows.

The shared policy was consolidated at the narrower stable seam instead:
`flush_page` owns the `BATCH_ITEMS` boundary for CONDSTORE and both Basic entry
paths, while QRESYNC uses the same boundary and its existing stream-specific
dedup state. All three now page, all cursor variants require an exact baseline,
and every Basic server-authored `changed_messages` UID is classified against
both the prior baseline and the live snapshot before it can become `Updated`.
The first cut of that guard failed the cursor whenever the UID was merely absent
from the baseline, which is the ordinary case for an arrival on the `[NOMODSEQ]`
downgrade path - a QRESYNC SELECT returns FETCH data for every message above the
client's MODSEQ, new ones included - so it would have restarted the scope on
routine traffic. Only a UID in neither set is contradictory. The remaining
separate loops are
therefore wire and downgrade orchestration, not three copies of baseline and
buffering policy.
