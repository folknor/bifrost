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

### G1 - `foreign_namespaces_advertised: false` contradicts what JMAP actually does

`sync/capabilities.rs:196` sets the flag false with the comment *"JMAP foreign
accounts arrive through the session resource, a per-request surface, not an
open-time namespace discovery."* That is not what the code does:
`factory.rs::foreign_mail_account_ids` reads the non-personal accounts out of
the session **at open** and seeds their scopes there, and
`reference/jmap.md` records that *"Foreign-account mailbox lifecycle is not
polled ... a foreign mailbox added after `open` appears at the next reopen."*

Which is precisely the flag's stated semantics
(`types/src/capabilities.rs:372-383`): "foreign namespaces whose folder set is
discovered ONLY at account open ... the account emits no scope-lifecycle
events, so a share granted after open becomes visible only when the consumer
re-opens the account." Consumers read the flag to decide whether a rediscovery
reattach is worth its wire cost.

Consequence: a JMAP share granted after open never surfaces, because the
consumer has been told a reattach could not possibly find anything.

The counter-authority is the types doc itself, which lists "JMAP session
accounts" in the `false` bucket. Per the bug-hunt rules, contract docs win, so
this is a **decision for the orchestrator**: either the types doc's
parenthetical is stale and JMAP should advertise `true`, or the flag's
semantics are narrower than its doc-comment and the JMAP comment should say
why. Not a mechanical fix either way.

### G2 - `Type(Thread)` is probed and seeded at open but never discovered, so the probe is pure cost

`factory.rs` runs `probe_thread_state` for the primary account (181) and for
**every** foreign account (`seed_foreign_account`, 431), seeds
`CursorScope::Type(ObjectType::Thread)` (203-206), and populates
`thread_states` per account (221, 238). But `JmapAccount::cursor_scopes`
(`account.rs:170-196`) only ever offers `Type(Email)`, `Type(Mailbox)`, and
the foreign `Folder` scopes. `Type(Thread)` is never discovered, so
`changes::thread_changes` and the whole `thread_states` map are unreachable.

Cost: one `Thread/get` round trip per account at every open, for state nobody
reads. `reference/jmap.md` lists Thread under "Supported scopes for
`inventory_stream` and `changes_stream`" without noting that discovery never
offers it, so the doc reads as if thread changes sync.

Either add `Type(Thread)` to `cursor_scopes` (and accept that its inventory
fatals) or drop the probe, the seed, and `thread_states` and say so in the
reference.

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

### G7 - `query_changes` ignores the query definition entirely

`sync/changes.rs::query_changes` (307-362) builds
`EmailQueryChanges::new(since_state)` with **no filter and no sort**. The
`query_id` is used only to label the scope and the membership. So a
`CursorScope::Query("unread-in-inbox")` cursor reports added/removed against
the *unfiltered, unsorted* email query and tells the engine those are
membership changes of "unread-in-inbox".

`reference/jmap.md` says registered query definitions are out of scope for the
v1 trait, and query *inventory* correctly fatals with exactly that reason
(`inventory.rs:49-55`). The changes leg does not fatal - it answers with
plausible-looking wrong data. If query scopes are out of scope, this leg
should fatal too rather than silently mis-attributing every message in the
account to whatever query id it was handed.

### G8 - A transient probe failure silently drops a shared account for the whole session

`factory.rs:256-260`: `Err(_skip) => { /* Permission-denied or transient:
skip */ }`. The comment is accurate about intent but the two cases are not
equivalent. A permission-denied probe means the share is gone and skipping is
right. A 503 or a connection reset on one of the four probes means the share
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

## 3. Doc contradictions found

1. `sync/capabilities.rs:194-196` - the comment claims JMAP foreign accounts are not open-time discovery. They are. See G1.
