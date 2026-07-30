# bifrost-jmap `core/` + `sync/` sweep

Scope: implementation fixes primarily concern `crates/jmap/src/core/**` and
`crates/jmap/src/sync/**`, with their directly-owned wire types where needed;
supporting plans and references track their current state.

Read first: `reference/jmap.md`, `reference/error-model.md`,
`plans/bug-hunt-2026-06-17.md`, `plans/jmap/*`. Nothing below re-reports a
finding already closed there.

Ordering inside each section is by blast radius, not by discovery order.

---

## 1. Bugs

### B9 - Every foreign mailbox scope replays the whole account-wide `Email/changes`

**Where:** `sync/changes.rs::stream` (100-108) -> `email_changes` (113-181).

**Mechanism.** `JmapScopeRepr::Folder { .. }` routes to `email_changes` with
no `inMailbox` restriction, because `Email/changes` is account-wide and cannot
be filtered. The account seeds **one `Folder` scope per mailbox** of each
foreign account (`factory.rs:238-252`). So a shared account with M mailboxes
produces M cursor scopes, each of which streams the *same* account-wide change
set, each qualifying ids with the *same* accountId.

**Path to failure.** A delegate account with 12 folders. One message arrives in
its Inbox. The engine drives all 12 `Folder` cursors. Each emits an
`ObjectChange { id: "acct-9\u{1f}M123", kind: Created }`. The engine sees 12
identical change events for one message, attributed to 12 different scopes,
11 of which do not contain it. Hydration then runs up to 12 times for the same
id (the per-scope streams are independent), so the wire cost is O(mailboxes)
per change.

Related, and worse for correctness: `email_changes` emits only
`Change::ObjectChange`, never `Change::ScopeChange`, so nothing tells the
engine which of the 12 folders the message actually landed in - membership is
only learned at hydration via `mailboxIds`. (The dispatch comment in
`changes.rs` used to claim a compensating scope-change mechanism that does
not exist; it now states this reality instead.)

**Interaction with push routing (added after the routing fix landed).**
`push.rs::emit_state_change` now routes a foreign `StateChange` onto one exact
`SpecificCursorScope` hint per seeded `Folder(accountId, *)` scope, because
that is the only correct answer while the cursor topology is per-mailbox: JMAP
state is per-(accountId, type), so all M of that account's cursors really did
move, and hinting fewer would leave cursors stale. The consequence is that a
single foreign push now drives M change-stream passes for an M-mailbox share -
the fanout is *correct*, but it pays B9's pre-existing O(mailboxes) wire cost
once per notification rather than once per poll interval, so B9's cost is now
push-rate-driven. Whoever takes B9 should weigh the two together: option (a)
below collapses the topology to one `Folder` scope per foreign account, which
collapses the push fanout to a single hint at the same time. Option (b) keeps
the fanout as-is. This is an argument for (a), not a new bug.

**Proposed fix.** Two options, both larger than a patch:
(a) seed one `Folder` scope per foreign *account* rather than per mailbox, and
    derive per-mailbox membership from the hydrated `mailboxIds`; or
(b) keep per-mailbox scopes but have the foreign changes leg hydrate
    `mailboxIds` for each changed id and emit `ScopeChange`s, so the engine
    can attribute the change.
This is a design call, not a mechanical fix.

## 2. Gaps and smells

### G3 - Bulk mutations hand a foreign-qualified id to the primary account verbatim

Known and documented (`reference/jmap.md` "Known limitations"), but the exact
failure mode is worth naming because it is not a clean error: `mutation.rs`
does `EmailId::new(id.0.clone())` on whatever `ObjectId` it is given
(191, 294, 298, 303). A foreign-qualified id is
`"acct-9\u{1f}M123"`, so the literal control character goes on the wire inside
the `Email/set` `update` key. The server answers `notFound` for a nonsense id,
and `apply_batch` reports `Failed(NotFound)` naming the encoded string. The
read path (`hydrate::route_for_id`, `blob::foreign_split`) already has the
decode; `mutation.rs` needs the same three-line selection plus the
per-accountId state key (which `state_cache` is already shaped for).

Same for `pim.rs::add_to_container` / `remove_from_container` / `set_keyword` /
`set_is_read` / `set_importance`, all of which take `self.mail` and
`self.mail.id_str()` from `account.rs` (477-589).

### G8 - A transient probe failure silently drops a shared account for the whole session

`factory.rs:233-236`: `Err(_skip) => { /* Permission-denied or transient:
skip */ }`. The comment is accurate about intent but the two cases are not
equivalent. A permission-denied probe means the share is gone and skipping is
right. A 503 or a connection reset on one of the two probes means the share
is fine and the user's shared mailbox vanishes from `containers_list`, from
`cursor_scopes`, and from `send_as` routing until the account is reopened -
with no warning anywhere, because `open` returns `Ok`. Classifying through
`into_account_error` and retrying (or at least surfacing) the retry classes
would separate them.

### G9 - The SSE leg classifies a status shape `bifrost-net` cannot hand it

`transport_reqwest.rs::open_sse` calls `request.send_streaming()` and then
branches on `response.status().is_success()`, building a bare
`TransportError::new(format!("SSE: HTTP {status}"))` for anything else. But
`send_streaming` only returns `Ok` for 2xx and for passed-through 3xx
(304/305/306, `Location`-less redirects); every 4xx/5xx has already become a
typed `bifrost_net::Error` by then. So that branch fires only for a 3xx, and
when it does it discards the body and the net evidence both - the resulting
`TransportError` has `net: None`, which `convert_transport` can only classify
as a generic `Transport(Network)`.

This is the same defect class the API leg had (fixed in this round for
`api_request` / `handle_response`): a branch written against a response shape
the production stack never delivers. It is currently unreachable in practice -
the `Account` impl drives WebSocket push, not EventSource, and
`ReqwestByteStream` is `#[allow(dead_code)]` - so it is filed rather than
fixed. Whoever wires EventSource must route the non-2xx leg through
`TransportError::from_net` the way `send`/`handle_response` does, or the first
real SSE failure arrives with no status, no body, and no recovery signal.

---
