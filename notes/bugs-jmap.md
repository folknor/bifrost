# bifrost-jmap bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/jmap/` including
`crates/jmap/src/sync/`. Findings are unverified work material.

Tree was clean at hunt time (`brokkr check -p bifrost-jmap`: 489 tests pass, zero
clippy/gremlins), so everything below is behavior the suite does not cover.

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

## Push read loop has no liveness deadline; ws_ping is dead code

`crates/jmap/src/sync/push.rs` (`reader_pass` read loop), `crates/jmap/src/client_ws.rs`.

`bounded(..., connect_timeout, ...)` covers exactly two awaits: the handshake and the re-enable
frame. The read loop itself is `select!(shutdown, ws.next())` with no idle timeout and no
keepalive. `Client::ws_ping` exists and is called from nowhere in the crate. On a half-open TCP
connection (NAT/firewall silently dropping state, the common case for a long-lived idle
WebSocket) the reader parks on `ws.next()` forever: no error, no close frame, no reconnect.
Push is dead for the life of the account and the engine cannot tell it apart from a quiet
mailbox, which is precisely the failure mode the `connect_timeout` work was introduced to
prevent, left uncovered on the await that is parked 99.9% of the time. What is needed is an
idle deadline in the read `select!` that fires a ping, or just treats the silence as a
disconnect.

## Persistently rejected push-enable becomes a 1 Hz reconnect storm

`crates/jmap/src/sync/push.rs`.

The push-enable frame's only failure signal is a `RequestError` arriving asynchronously on the
read stream (push.rs says so explicitly). So the sequence for a server that rejects the
subscription (unknown `dataTypes` value, capability withdrawn, quota) is: connect OK, sink
write OK, `Reconnected` emitted, `RequestError` read, non-terminal classification, `break`,
`ReaderStep::Retry { reset_backoff: true }`. Because the read loop was reached, the backoff is
reset to `policy.initial` (1s) on every pass. The result is an unbounded 1-request-per-second
handshake loop against a server that will never accept the subscription, plus a
`Disconnected`/`Reconnected` event pair per second on the broadcast channel. `reset_backoff`
should be gated on the pass having produced useful work, or at least on not having ended in a
`RequestError`, not merely on having reached the loop.

## close() is unbounded and not cancellation-covered

`crates/jmap/src/sync/account.rs`.

`close()` cancels the shutdown token and then `client.disable_push_ws().await`, a WebSocket
sink write, holding `self.ws` lock, with no timeout and no `select!` on anything. On the same
half-open socket as above, `close()` hangs indefinitely, blocking the engine's teardown/reopen.
This is the identical hazard `bounded` was added for, on the shutdown path.

Related doc divergence: `reference/jmap.md` says `close()` "cancels the shutdown token ... then
awaits teardown". It does not. `WsState::spawn` drops the `JoinHandle`
(`let _reader = tokio::spawn(...)`), so nothing is ever awaited. The reader is detached;
`close()` returning is not evidence it has stopped.

## Email/query for search is sent with no sort, then paged by position

`crates/jmap/src/sync/pim.rs` (`search_email_ids`).

`search` and `search_messages` build `EmailQuery::new().collapse_threads(..).position(..).limit(..)`
and never call `.sort(..)`. RFC 8621 leaves the order of an unsorted `Email/query`
server-defined and gives no stability guarantee across calls. The page cursor is a bare integer
position into that undefined ordering, so paging a search can duplicate and skip results on any
server whose default order is not stable, and gives a different order per server. Every other
query site in the crate (`inventory.rs`, all three loops) correctly pins `receivedAt desc`.
Search should too.

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

## Inventory mints entries with an empty ObjectId when the server omits id

`crates/jmap/src/sync/inventory.rs`, two sites:
`email.id().map(ToString::to_string).unwrap_or_default()`.

An `Email`/`Mailbox` object arriving without `id` produces `InventoryEntry { id: ObjectId("") }`,
which is emitted into the batch as a real inventory row, and in the foreign case gets qualified
into `"acct-9\u{1f}"`. `hydrate::reconcile_hydration` handles the same condition correctly: it
drops the object and lets the submitted id fall through to the `PartialResponse` lane.
`discover::memberships` and `seed_account_state` also correctly filter `id.as_str().is_empty()`.
Inventory is the one path that manufactures a bogus row instead. Given the crate's own "closed
per-item accounting" doctrine this looks like an oversight, not a decision.

## open_raw_rfc822 takes the first object in list without correlating the id

`crates/jmap/src/sync/blob.rs`: `response.into_list().into_iter().next()`.

One id was requested, so this is currently safe, but it is the same "trust the echoed list
positionally" pattern that `reconcile_hydration` was written to eliminate. A server echoing an
unrelated object would have its `blobId` downloaded and returned as the caller's message body.
One `id ==` check closes it.

## open_blob_range's out-of-range check is mislabeled

`crates/jmap/src/sync/blob.rs`. A `range.start >= handle.size` is reported as
`Unsupported(OpenBlobRange)`. That is a caller argument error (`Request(InvalidArgument)` /
`ClientBug`), not a capability gap, and the engine's recovery derivation reads the difference.
Low impact today because `BlobRangeSupport::No` makes every path terminate unsupported anyway,
which raises the design question of why the two dead pre-checks exist at all ahead of the
unconditional fatal.

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
