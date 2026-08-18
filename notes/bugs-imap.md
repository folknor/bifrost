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

## Two parallel hydration implementations

`Projection` in `get.rs` and `HydrationProjection` in `pim.rs` are two
attribute-selection paths over the same FETCH surface, one parsing through
`bifrost-types::mime` and one returning raw bytes. They drift: the preview
whole-message-prefix fix landed in `pim.rs` first and had to be applied to
`get.rs` separately (done 2026-08-18). They want unifying behind a single
attribute-selection + decode function.

## Unbounded memory: the buffered target sets

`mutation_stream` and `get_stream` both fully drain their input
`AccountStream<ObjectId>` into a `HashMap` before issuing a single command.
Nothing is emitted until the producer finishes, and the whole target set is
resident. Streaming per folder as ids arrive would fix both, but changes
batching and ordering, so it wants a deliberate design pass rather than a
patch.

(The MODSEQ cache is bounded and body-bearing hydration has a byte budget as
of 2026-08-18.)

## CompactUidSet is range-compressed in name only

`diff` is now a linear merge and `contains` a binary search over the ranges
(2026-08-18), but `to_uids` callers still expand: `run_qresync`'s `live_uids`
`BTreeSet`, `run_condstore_with_baseline`'s seeded-baseline path, and
`select_options`' QRESYNC known-UID list. A CONDSTORE cycle on a large mailbox
still materialises the baseline and re-coalesces it. Invisible on a test
mailbox, dominant on a real one.

## Smaller / lower-confidence

- `*` sentinel collides with a legal UID. `codec/decode/flags_caps.rs::seq_number` maps `*` to
  `u32::MAX`, and `connection/mod.rs::expand_uid_ranges` treats any `u32::MAX` endpoint as `*`
  and returns `Error::SearchResultTruncated`. UID 4294967295 is a legal `nz-number`. Vanishingly
  rare, but the sentinel should be a distinct variant rather than an in-band value.
- Duplicated dispatch loop. `run_one_command` and `run_prebuilt_command` in `driver/mod.rs` are
  ~70 near-identical lines (the classification/BYE/continuation loop); `pipeline.rs` and
  `idle.rs` carry third and fourth copies of the untagged-response handling. The
  `short_circuit_on_bye` guard was clearly added to paper over exactly this. One loop
  parameterised by "how do I send" and "what do I do with a `+`" would remove the class of bug
  the guard defends against.
- `Pool::close` cannot log out an outstanding checkout. It is outside the idle list; the
  checkout's `Drop` correctly refuses to re-park it, but nothing LOGOUTs it. It relies on the
  driver task's `logout_best_effort` after the handle drops, which is fine but means
  `Account::close()` returning does not mean that session is gone. (The IDLE connection is
  registered with the pool as of 2026-08-18 and is drained by `close()`.)
- `map_idle_event`'s `IdleEvent::Bye` arm is unreachable in the push loop; `event_closes_connection`
  breaks first. Kept deliberately: the mapping is total over a `#[non_exhaustive]` enum, and the
  arm costs nothing.
