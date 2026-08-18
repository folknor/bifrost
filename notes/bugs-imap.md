# bifrost-imap bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/imap/` including
`crates/imap/src/account/`. Findings are unverified work material.

Fixed 2026-08-18 and removed from this document: the streaming-FETCH
termination race, the CONDSTORE double-report, the laundered `AccountError` in
`inventory.rs`, the unbounded IDLE DONE handshake, the timed-out command
returning to the pool, `get.rs` preview/text-only hydration, `CompactUidSet`
expanding on every `diff`, and the flat push-reconnect sleep. The reference doc
now states each new rule.

## Push watches exactly one folder, silently

`crates/imap/src/account/push.rs::choose_idle_folder` / `subscribed_idle_folder`.

Subscriptions are collected from all handles, sorted by name, and `.next()` is taken. An account
that subscribes ten folder scopes gets IDLE on `Archive` and no push at all for the other nine:
no warning, no capability signal, nothing in the reference doc. The doc only claims the choice
is deterministic, which hides the fact that everything else is dropped. The crate already models
NOTIFY (RFC 5465) end to end in the connection layer (`NotifySet`, `NotifyFlags`, STATUS/LIST
routing) and it is unused by the account layer. The right move is a rewrite of `idle_loop` to
use `NOTIFY SET` with the subscribed mailbox set when advertised, and one IDLE connection per
hot folder (bounded) otherwise, not a tweak.

## Two parallel hydration implementations

`Projection` in `get.rs` and `HydrationProjection` in `pim.rs` are two
attribute-selection paths over the same FETCH surface, one parsing through
`bifrost-types::mime` and one returning raw bytes. They drift: the preview
whole-message-prefix fix landed in `pim.rs` first and had to be applied to
`get.rs` separately (done 2026-08-18). They want unifying behind a single
attribute-selection + decode function.

## Unbounded memory, two places

- `FolderEntry::modseq_by_uid` (`folder_registry.rs`) is a `HashMap<u32, u64>` that grows one
  entry per UID seen by inventory/get/changes/IDLE and is only ever pruned by expunge/VANISHED or
  a UIDVALIDITY change. A 500k-message mailbox holds a permanent multi-MB map per folder for an
  opportunistic cache. It wants an LRU bound or to be dropped for UIDs outside the current
  mutation working set.
- `mutation_stream` and `get_stream` both fully drain their input `AccountStream<ObjectId>` into
  a `HashMap` before issuing a single command. Nothing is emitted until the producer finishes,
  and the whole target set is resident. `Projection::Full` in `get.rs` additionally uses the
  buffered `uid_fetch` (not `uid_fetch_limited`), so a hydration batch of large messages is
  materialised in full with no byte budget, while the reference explicitly notes that
  `uid_fetch_full_messages` requires one.

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
- `bulk_destroy` has no non-UIDPLUS path. It always issues `UID EXPUNGE`; on a server without
  UIDPLUS the batch is left flagged `\Deleted` and every item fails. `draft_discard` is
  capability-gated on UID EXPUNGE, but the sync-side destroy is not.
- `Pool::close` cannot log out an outstanding checkout or the IDLE connection. Both are outside
  the idle list; the checkout's `Drop` correctly refuses to re-park it, but nothing LOGOUTs it.
  It relies on the driver task's `logout_best_effort` after the handle drops, which is fine but
  means `Account::close()` returning does not mean the sessions are gone.
- `map_idle_event`'s `IdleEvent::Bye` arm is dead code; `event_closes_connection` breaks first.
