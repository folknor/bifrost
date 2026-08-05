# bifrost-imap bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/imap/` including
`crates/imap/src/account/`. Findings are unverified work material.

## Streaming-FETCH errors lost on a 50/50 race; truncated inventory checkpointed as complete

`crates/imap/src/account/inventory.rs` (`run_inventory` select loop) and
`crates/imap/src/account/changes.rs` (`run_qresync` select loop).

Both drive a `(fetch_rx, fetch_fut)` pair with an unbiased `tokio::select!`. The failure
handling is:

```rust
None => {
    if let Some(result) = fetch_result.take() { result?; }
    break;
}
```

The bounded sender lives in the consumer, which the driver drops synchronously before it sends
the result on the oneshot (`run_one_command` finalizes/drops the consumer, then
`publish_then_answer`). So when a FETCH ends in a tagged NO/BAD, a read error, or a timeout, the
caller wakes with both branches ready and `select!` picks at random. If it picks
`fetch_rx.recv() -> None` first, `fetch_result` is still `None`, the loop breaks on the success
path, and:

- `run_inventory` commits `Checkpoint::Change(...)` for a partial mailbox. Messages that were
  never fetched are permanently absent from the engine's view until UIDVALIDITY changes.
- `run_qresync` commits a cursor at the SELECT HIGHESTMODSEQ having processed only part of the
  CHANGEDSINCE/VANISHED stream.

This is the most serious thing in this scope. The fix is not just `biased;`: after `recv()`
returns `None` the code must await `fetch_fut` to completion and propagate its result
unconditionally. Hunter did not execute this, so treat the exact probability as unverified, but
the ordering (sender dropped before oneshot send) is clear in `driver/mod.rs`.

## CONDSTORE change stream reports every new message twice

`crates/imap/src/account/changes.rs::run_condstore`.

`UID FETCH 1:* (FLAGS MODSEQ) (CHANGEDSINCE n)` returns every message with modseq > n, which
includes messages that arrived since the cursor. Each of those is pushed as `updated_change`
(`ObjectChange::Updated`), and then the `known_uids.diff(&live_set)` pass pushes the same id
again as `added_change` (`ScopeChange::Added`), with `Updated` ordered before `Added`. The
QRESYNC path has an elaborate dedup for exactly this hazard (`record_fetch_change` /
`live_uids` / `fetch_change_seen`); the CONDSTORE path has none. Either the dedup should be
shared across strategies, or the CHANGEDSINCE loop should skip UIDs not already in `known_uids`.

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

## idle() can wedge the push loop forever, ignoring cancellation

`crates/imap/src/connection/idle.rs`.

Once the select breaks (cancel, timeout, or event), the code does `done_tx.send(())` and then
`result_rx.await` with no timeout and no cancellation branch. The driver is meanwhile in
`drain_idle_responses`, a bare `read_one().await` loop with no deadline. A peer whose TCP is
alive but which never answers the tagged OK for IDLE parks the account's push loop indefinitely:
`cancel` cannot break it, `account.shutdown` cannot break it, and `close()` cannot either (the
IDLE connection is dialed via `dial_idle` and is not in the pool's idle list, so `Pool::close`'s
bounded drain never sees it). The DONE handshake needs the same `command_timeout` bound every
other command has.

## get.rs preview hydration is the un-fixed twin of pim.rs, and the reference documents only the fixed one

`crates/imap/src/account/get.rs::attrs_for_projection` vs
`crates/imap/src/account/pim.rs::attrs_for_hydration`.

`reference/imap.md`, in the section titled "Hydration accounting (`get.rs`)", states: "Preview
fetches a whole-message prefix (at least 64 KiB, or the requested limit when larger) rather than
`BODY[TEXT]`, so multipart framing is not shown as prose." That is true of `pim.rs`
(`section: None`, `partial: (0, limit.max(PREVIEW_FETCH_BYTES))`). `get.rs` still emits
`BODY.PEEK[TEXT]<0.count>` with no 64 KiB floor, the exact shape the doc says was replaced, so a
`Projection::Preview` on a multipart message hands the consumer MIME boundaries and base64 as
"preview text". `Projection::TextOnly` has the same shape.

More broadly: there are two parallel hydration implementations (`Projection` in `get.rs`,
`HydrationProjection` in `pim.rs`) with independently drifting attribute selection, one parsing
through `bifrost-types::mime` and one returning raw bytes. That duplication is the reason the fix
landed in one and not the other. They want unifying behind a single attribute-selection + decode
function.

## inventory.rs launders a classified AccountError into Error::Protocol

`inventory.rs`: `folder_from_scope(&scope, ...).map_err(|e| crate::Error::Protocol(e.to_string()))`.

`folder_from_scope` already returns a properly classified `AccountError` (`Unsupported(op)` for a
non-folder scope, `Request(Malformed)` for an unsendable name). Stringifying it into
`Error::Protocol` re-classifies it at the boundary as `Protocol(ParseFailed)` /
`ProviderContractViolation`, i.e. an unsupported scope is reported to the engine as the server
violating its contract. `changes.rs` gets this right (plain `?` on the `AccountError`). This is
precisely the "producer must preserve classification" rule in `reference/error-model.md`.

## A timed-out command returns its connection to the pool

`crates/imap/src/error.rs::is_connection_fatal` excludes `Timeout`, and no account-layer call
site discards the checkout on timeout.

The driver is cancellation-safe by design: after the caller's `tokio::time::timeout` fires, the
driver is still executing that command. The `PooledConn` drops, is parked (alive, not `Logout`),
and the next checkout submits a command that queues behind the still-running one, so the next
caller's timeout is very likely to fire too. One slow command cascades into a run of spurious
timeouts on the same connection. A timed-out checkout should be `discard()`ed, or the driver
should be given a way to report "still busy".

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

Every operation (`to_uids`, `diff`, `uid_count` callers, `select_options`' QRESYNC known-UID
list) expands the ranges into `Vec<u32>`/`BTreeSet<u32>`. A CONDSTORE cycle on a large mailbox
expands the baseline at least three times per run and then re-coalesces it. `diff` in particular
should be a linear merge over the sorted range lists. This is the kind of thing that is invisible
on a test mailbox and dominant on a real one.

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
- Push reconnect has no backoff. `idle_loop` sleeps a flat 5s and re-emits
  `WatchEvent::Disconnected` on every failed dial/select, forever, without an intervening
  `Reconnected`.
- `bulk_destroy` has no non-UIDPLUS path. It always issues `UID EXPUNGE`; on a server without
  UIDPLUS the batch is left flagged `\Deleted` and every item fails. `draft_discard` is
  capability-gated on UID EXPUNGE, but the sync-side destroy is not.
- `Pool::close` cannot log out an outstanding checkout or the IDLE connection. Both are outside
  the idle list; the checkout's `Drop` correctly refuses to re-park it, but nothing LOGOUTs it.
  It relies on the driver task's `logout_best_effort` after the handle drops, which is fine but
  means `Account::close()` returning does not mean the sessions are gone.
- Reference imprecision. The doc says the IDLE DONE drain "discards" in-flight events;
  `drain_idle_responses` actually emits them to the event sink, and they survive into the next
  `idle()` round. They are only lost when the loop breaks to redial (the resubscribe case
  `signal_idle_interrupt_loss` covers). Worth correcting so the next reader does not add a
  redundant invalidation.
- `map_idle_event`'s `IdleEvent::Bye` arm is dead code; `event_closes_connection` breaks first.
