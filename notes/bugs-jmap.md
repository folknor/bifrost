# bifrost-jmap bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/jmap/` including
`crates/jmap/src/sync/`. Findings are unverified work material.

Tree was clean at hunt time (`brokkr check -p bifrost-jmap`: 489 tests pass, zero
clippy/gremlins), so everything below is behavior the suite does not cover.

Fixed 2026-08-22 and removed from this document: the unsorted `Email/query`
behind the positional search cursor, the inventory walks minting an
`ObjectId("")` for an object the server returned without an id,
`open_raw_rfc822` taking the head of the echoed `Email/get` list instead of
correlating on the submitted id, and `open_blob_range`'s out-of-range start
reported as `Unsupported` rather than `Request(Malformed)`.
`reference/jmap.md` states each new rule.

Fixed 2026-08-22 in a second round and removed from this document: the push
read loop having no liveness deadline (there is now a `keepalive` ping with a
`connect_timeout` pong deadline, and `WebSocketMessage::Pong` exists so the
reader can see the answer - which also gives the previously dead `ws_ping` its
caller), the persistently-rejected push-enable becoming a 1 Hz reconnect storm
(`reset_backoff` is now gated on the pass having read a message, not on having
reached the loop), and `close()` being unbounded and unable to prove the
reader stopped (`WsState` retains the `JoinHandle`; `close()` bounds both the
push-disable and the join by `connect_timeout`, which also makes the
reference's "awaits teardown" claim true).

## scope_lifecycle and the Mailbox change stream share one cursor

`crates/jmap/src/sync/discover.rs` (`scope_lifecycle`), `crates/jmap/src/sync/changes.rs`
(`mailbox_changes`), `crates/jmap/src/sync/pim.rs` (`container_create/rename/move/delete`).

`scope_lifecycle` takes its `sinceState` from `state_cache::get(&mailbox_states, account_id)`,
i.e. the shared per-accountId Mailbox state map is its cursor of record. But three other
writers advance that same map:

- `changes::mailbox_changes` (`state_cache::advance(&mailbox_states, ..., newState)`), which
  the engine drives for `CursorScope::Type(Mailbox)`, on push and on every poll.
- all four `pim::container_*` functions, each doing an unconditional
  `state_cache::set(&mailbox_states, ..., Mailbox/set newState)`.

JMAP `Mailbox/changes` state is a linear per-`(accountId,type)` sequence. Whoever advances it
first consumes the window. Concretely: a shared folder is created server-side; the engine's
Mailbox changes stream polls, sees it as an `ObjectChange`, and CAS-advances the cache to
`newState`. 300s later `scope_lifecycle` wakes, reads the new state, calls `Mailbox/changes`
from there, gets an empty set, and never emits `ScopeLifecycle::Created`. Same for
`Renamed`/`Deleted`. A local `container_create` is worse: it jumps the state forward past any
unconsumed server-side mailbox lifecycle in that window.

The careful "commit only after every follow-up succeeded" logic in `scope_lifecycle` is
defending an invariant that a co-writer breaks anyway. The fix is structural: `scope_lifecycle`
needs its own private state slot, not the shared `mailbox_states` map. That map exists to serve
`ifInState` for `Mailbox/set` and to seed the changes cursor, two roles that tolerate
fast-forwarding. A lifecycle poller cannot.

## Unhandled SearchFilter variant silently becomes "match everything"

`crates/jmap/src/sync/pim.rs` (`search_filter_to_jmap`), final arm:
`_ => query::Filter::and(Vec::new())`.

`SearchFilter` is `#[non_exhaustive]`, so a catch-all is required, but an empty JMAP `AND`
matches every message. Today's variants are all handled, so this is latent, not live; the
moment `bifrost-types` grows a variant, JMAP search starts returning the entire mailbox for a
filtered query, and under `Not(new_variant)` returns nothing. Both are wrong answers presented
as success. This should be `Unsupported`, propagated out of `search_filter_to_jmap` as a
`Result`. The gate-5 "never string-match unknown vocabulary" discipline applied elsewhere in
this crate is exactly the same instinct.

## Session-state divergence is detected and then thrown away

`crates/jmap/src/client.rs` sets `session_updated = false` when a response's `sessionState`
differs from the cached session. `is_session_updated()` is read by nothing outside
`core/tests.rs`, and `refresh_session()` is called by nothing outside a unit test.

RFC 8620 sections 2 and 3.4 make every Session property mutable and tell the client to re-fetch
on `sessionState` change. Everything the account layer derives from the session is frozen at
`open`: `apiUrl`, `downloadUrl`/`uploadUrl` templates, `maxObjectsInGet`/`InSet`, the advertised
capability set, the primary-account ids, and the whole foreign-account list. So a newly granted
share, a revoked capability, a rotated download URL, or a tightened limit is invisible until
the engine happens to reopen for an unrelated reason. The flag is a fully wired detector with
no consumer: either drive `refresh_session()` off it, or surface it as
`SyncState(CapabilityChanged) -> Engine(RestartAccount)`, but the current shape is a dead
invariant that reads as if it were live.

## Design observations

- `skipped_flag_stream` duplicates `mutation_stream`'s batching by hand. Two independent copies
  of "accumulate to `max_objects_in_set`, yield a `Batch`, flush the tail, yield `Done`", with
  different `PageBoundary` handling and no shared code. The empty-`FlagOp` short-circuit is
  worth keeping; the second batching loop is not.
- `email_inventory`, `foreign_email_inventory`, and `email_inventory_page` are three
  near-identical query/get/advance loops (~110 lines each) differing only in the filter, the
  window bound, and whether errors route through `shared_scope_error`. All three repeat the same
  `i32::try_from` / `checked_add` overflow ceremony verbatim. One parameterized loop taking
  `(filter, owner, window)` would remove ~200 lines and the risk of the three drifting, which
  they already have: only the foreign loop applies `qualify_foreign_ids`, only the page loop
  stops on a short window.
- Foreign-account seeding at `open` is strictly sequential (`factory.rs`, the
  `for foreign_id in foreign_ids` loop), two round trips per share. With ten shares on a 200ms
  link that is four seconds of serialized open latency before the user's own mail is reachable.
  These probes are independent; `FuturesUnordered` would make it one round-trip's worth. The doc
  calls out the O(n) cost as accepted, but accepting the count is different from accepting the
  serialization.
- `state_cache::advance`'s CAS is weaker than it reads. `advance(map, acct, Some(expected), new)`
  writes unconditionally when the entry is absent or `Some(None)`, which makes "probed, empty"
  indistinguishable from "never probed" for guarding purposes despite the module doc
  distinguishing them. Combined with the shared-cursor finding above, the guard mostly documents
  intent rather than enforcing it.

## Coverage gap in this hunt

`calendar_ops.rs` and `contacts.rs` (~3k lines of JSCalendar/JSContact mapping) were not audited
in depth. The RRULE/`UNTIL`, all-day exclusive-end, and RSVP claims in the reference are
unverified and would be worth a dedicated pass.
