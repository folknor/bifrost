# bifrost-imap bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/imap/` including
`crates/imap/src/account/`. Findings are unverified work material.

Fixed 2026-08-18 and removed from this document: the streaming-FETCH
termination race, the CONDSTORE double-report, the laundered `AccountError` in
`inventory.rs`, the unbounded IDLE DONE handshake, the timed-out command
returning to the pool, `get.rs` preview/text-only hydration, `CompactUidSet`
expanding on every `diff`, and the flat push-reconnect sleep. The reference doc
now states each new rule.

## Push still watches one folder on a server without NOTIFY

`NOTIFY SET` now covers every subscribed folder on one connection when the server advertises
NOTIFY (2026-08-18), and the no-NOTIFY case logs a warning instead of going silent. What is
still missing is coverage on servers without NOTIFY: the remaining option is one IDLE connection
per hot folder (bounded), which costs connections and wants a deliberate decision about the
budget.

## Unbounded memory: the buffered target sets

`mutation_stream` and `get_stream` both fully drain their input
`AccountStream<ObjectId>` into a `HashMap` before issuing a single command.
Nothing is emitted until the producer finishes, and the whole target set is
resident. Streaming per folder as ids arrive would fix both, but changes
batching and ordering, so it wants a deliberate design pass rather than a
patch.

(The MODSEQ cache is bounded and body-bearing hydration has a byte budget as
of 2026-08-18.)

## Smaller / lower-confidence

- `*` sentinel collides with a legal UID. `codec/decode/flags_caps.rs::seq_number` maps `*` to
  `u32::MAX`, and `connection/mod.rs::expand_uid_ranges` treats any `u32::MAX` endpoint as `*`
  and returns `Error::SearchResultTruncated`. UID 4294967295 is a legal `nz-number`. Assessed
  2026-08-18 and deliberately left: the collision fails safe (a refused expansion, never a wrong
  one), it needs a mailbox that has reached the last UID of its UIDVALIDITY epoch to trigger, and
  moving the sentinel out of band means changing `UidRange` itself, which every codec, encoder,
  and cursor path touches.
- `Pool::close` cannot log out an outstanding checkout. It is outside the idle list; the
  checkout's `Drop` correctly refuses to re-park it, but nothing LOGOUTs it. It relies on the
  driver task's `logout_best_effort` after the handle drops, which is fine but means
  `Account::close()` returning does not mean that session is gone. (The IDLE connection is
  registered with the pool as of 2026-08-18 and is drained by `close()`.)
- `map_idle_event`'s `IdleEvent::Bye` arm is unreachable in the push loop; `event_closes_connection`
  breaks first. Kept deliberately: the mapping is total over a `#[non_exhaustive]` enum, and the
  arm costs nothing.
