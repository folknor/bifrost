# bifrost-imap bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/imap/` including
`crates/imap/src/account/`. Findings are unverified work material.

Fixed 2026-08-18 and removed from this document: the streaming-FETCH
termination race, the CONDSTORE double-report, the laundered `AccountError` in
`inventory.rs`, the unbounded IDLE DONE handshake, the timed-out command
returning to the pool, `get.rs` preview/text-only hydration, `CompactUidSet`
expanding on every `diff`, and the flat push-reconnect sleep. The reference doc
now states each new rule.

Fixed 2026-08-22 (round 2) and removed from this document: push watching one
folder on a server without NOTIFY (now a bounded budget of dedicated IDLE
sessions, with the excess reported in the failed lane and left polling-only),
the unbounded buffered target sets in `get_stream` and the mutation streams
(now a 256-target flush window), and `Pool::close` being unable to log out an
outstanding checkout (the pool now registers every session it mints and
`close` reaches all of them). The same round closed two holes the fix itself
opened - a dial or permit acquisition completing after `close` could register
a live session past the drain, and the session registry retained a dead entry
per dial forever - plus a lost-wakeup regression from waking several IDLE
workers with a `Notify`. `reference/imap.md` states each new guarantee and
`notes/carry-forward.md` carries the invariants forward.

Close pass 2026-08-22: reviewed both rounds in full, with the never-cold-reviewed
halves hardest. Found and fixed two defects in round 2's push worker machinery:
the in-round `resubscribe.mark_unchanged()` discarded a generation bump landing
during the dial/SELECT/`NOTIFY SET` awaits (a stale worker assignment for up to
one `idle_timeout` - the same lost-wakeup class round 2 fixed elsewhere, and a
direct contradiction of the discipline `reference/imap.md` documented), and
admission accepted folder scopes whose names `MailboxName::new` rejects, so an
unsendable name was reported as pushed and burned a budget slot while
`subscribed_idle_folders` silently dropped it. Verified independently: the
ordering weakening is tolerated (bifrost-sync keys mutation outcomes by
`ObjectId`, and the read-back guard is order-insensitive counting), the
subscription registry is teardown/reopen bookkeeping only with no scheduler
path suppressing polling on push coverage, and the reopen replay handles
partial acceptance via `accepted_push_scopes`. The NOTIFY-runtime-rejection
misreport residual is accepted as documented.

No open findings remain. What follows are decisions, not defects.

## Settled decisions

- `*` sentinel collides with a legal UID. `codec/decode/flags_caps.rs::seq_number` maps `*` to
  `u32::MAX`, and `connection/mod.rs::expand_uid_ranges` treats any `u32::MAX` endpoint as `*`
  and returns `Error::SearchResultTruncated`. UID 4294967295 is a legal `nz-number`. Assessed
  2026-08-18 and deliberately left: the collision fails safe (a refused expansion, never a wrong
  one), it needs a mailbox that has reached the last UID of its UIDVALIDITY epoch to trigger, and
  moving the sentinel out of band means changing `UidRange` itself, which every codec, encoder,
  and cursor path touches.
- `map_idle_event`'s `IdleEvent::Bye` arm is unreachable in the push loop; `event_closes_connection`
  breaks first. Kept deliberately: the mapping is total over a `#[non_exhaustive]` enum, and the
  arm costs nothing.
