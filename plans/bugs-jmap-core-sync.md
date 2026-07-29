# bifrost-jmap `core/` + `sync/` sweep

Scope: `crates/jmap/src/core/**` and `crates/jmap/src/sync/**` only. Edits
confined to those trees; everything outside is reported, not touched.

Read first: `reference/jmap.md`, `reference/error-model.md`,
`plans/bug-hunt-2026-06-17.md`, `plans/jmap/*`. Nothing below re-reports a
finding already closed there.

Ordering inside each section is by blast radius, not by discovery order.

---

## 1. Bugs

### B1 - Every `Mailbox/set update` and `Identity/set update` this crate sends carries removal-nulls

**Where:** `sync/pim.rs::container_rename` (752), `sync/pim.rs::container_move`
(784), `sync/pim.rs::identity_update` (952).

**Mechanism.** All three build their patch through `SetRequest::update`, which
is `entry(id).or_default()` (`core/set.rs:278`). The default patch is *not*
empty:

- `MailboxPatch::default()` -> `{"role": null, "shareWith": null}`.
  `role`'s skip predicate is `role_not_set` = `matches!(Some(Role::None))` and
  `shareWith`'s is `skip_if_empty_map` = `matches!(Some(empty))`. Both return
  `false` for a plain `None`, so `None` reaches the wire as an explicit null.
- `IdentityPatch::default()` -> `{"replyTo": null, "bcc": null}`, same shape via
  `skip_if_empty_list`.

RFC 8620 s5.3 reads an explicit `null` in a PatchObject as *remove this
property*. `MailboxCreate` escapes this only because `SetCreate::new`
hand-installs the sentinels; the Patch types use `#[derive(Default)]`, which
cannot.

**Path to failure, mailbox.** User renames their Sent folder to "Sent Mail".
`container_rename` sends
`Mailbox/set {"update": {"mb-sent": {"name": "Sent Mail", "role": null, "shareWith": null}}}`.
Two possible outcomes, both wrong:

1. Permissive server (role is settable per RFC 8621 s2.5): the rename succeeds
   and the mailbox's `role` becomes null and its `shareWith` map is emptied.
   Now `role_mailboxes` (`pim.rs:1537`) can no longer find `FolderRole::Sent`,
   so `send_message` (263, 388), `draft_send` (584), and `draft_create` (484,
   for Drafts) all fail with `missing_role_mailbox` ->
   `Unsupported("JMAP required mailbox role not found")`. Renaming the Trash
   folder breaks `delete_thread` (1323) the same way. Renaming *any* folder
   also drops its `FolderRole` in `containers_list`, so the consumer's sidebar
   loses its special-folder identity. And every ACL on the renamed mailbox is
   gone - a shared team folder becomes private on rename.
2. Strict / sharing-aware server: `shareWith` is admin-only (RFC 8621 s2:
   settable only with `mayAdmin`), so an ordinary user renaming a folder they
   do not administer gets `notUpdated: {id: {type: "forbidden" |
   "invalidProperties", properties: ["shareWith"]}}`. `unwrap_update_errors`
   turns that into an error and the rename fails outright for a property the
   user never mentioned.

**Path to failure, identity.** `identity_update(id, IdentityPatch { name:
Some("New Name"), .. })` sends `{"name": "New Name", "replyTo": null, "bcc":
null}`. Renaming an identity silently wipes its Reply-To addresses and its
default Bcc. A user who set "always Bcc my archive address" loses it the next
time they edit the signature.

**Proposed fix.** The patch types are the other agent's files, so the fix
belongs there: give `MailboxPatch` / `IdentityPatch` a hand-written `Default`
that installs the same sentinels `SetCreate::new` does (`Some(Role::None)`,
`Some(HashMap::new())`, `Some(Vec::new())`), or move the fields to
`Field<T>` (defaulting to `Omitted`), which is what `Calendar` /
`AddressBook` already do for nullable properties. No sync-side change is then
needed; the call sites are correct once the default is.

**Status:** the type-level half is already pinned in `crates/jmap/src/tests.rs`
(`an_empty_mailbox_patch_still_clears_role_and_share_with`, which even
replicates the `container_rename` construction verbatim). I did not duplicate
that test. Confirmed independently against the sync call sites; the
`role_mailboxes` / `identity_update` consequences above are the sync-side
half and are not covered by those tests.

### B2 - `container_move(id, None)` silently does nothing and reports success

**Where:** `sync/pim.rs::container_move` (784-813).

**Mechanism.** `new_parent.map(|id| MailboxId::new(id.0))` is `None`, so
`MailboxPatch::parent_id(None)` sets `self.parent_id = None`. That field is
`#[serde(skip_serializing_if = "Option::is_none")]`, so the only field that
expresses the caller's intent is dropped from the wire. What actually goes out
is `{"update": {"mb-1": {"role": null, "shareWith": null}}}`.

**Path to failure.** User drags a nested folder to the top level of the folder
tree. The consumer calls `container_move(ContainerId("mb-1"), None)`. The
server receives a patch that says nothing about `parentId`, applies the
role/shareWith removals from B1, and answers `updated: {"mb-1": null}`.
`unwrap_update_errors` sees an empty `notUpdated`, so the function returns
`Ok(())`. The mailbox state cache is advanced. The consumer marks the move
applied. The folder never moved, and it lost its role and its ACLs on the way.

Note the asymmetry: `MailboxCreate::parent_id(None)` *does* emit `null`
(`skip_if_empty_id` only skips `Some("")`), so creating a top-level mailbox
works and moving one to the top level does not.

**Proposed fix.** Same as B1 - `Field<MailboxId>` on `MailboxPatch::parent_id`
so `Field::Null` is expressible and distinct from `Field::Omitted`. Until then
`container_move(_, None)` should not report success; but the honest fix is the
field type, not a guard at the call site.

### B3 - Foreign inventory (and full primary inventory) truncate on the first short page

**Where:** `sync/inventory.rs::foreign_email_inventory` (90-211) and
`sync/inventory.rs::email_inventory` (248-360).

**Mechanism.** Both loops do:

```
let ids = query_response.ids().to_vec();      // from Email/query
...
let items = <hydrated from Email/get>;
let batch_len = items.len();                  // HYDRATED count
if batch_len < limit { yield Done; break }
position += batch_len;
```

`batch_len` counts objects `Email/get` returned, not ids `Email/query`
returned. It is smaller than `limit` in two routine cases:

1. The server caps its query page below `maxObjectsInGet`. This is not
   hypothetical - `email_inventory_page` (362-515) exists precisely because of
   it and says so in its own comment: *"a server whose query page cap is below
   the requested window width (saehrimnir caps queries below its get cap)
   returns fewer ids than asked for, and the orchestrator reads a short window
   as end-of-inventory and silently drops every later page."*
2. An id vanished between the query and the get (it lands in `notFound`, which
   both loops discard). `email_inventory_page` also names this: *"advance by
   the number of ids consumed from the query - not by the count of hydrated
   objects, which can be smaller when an id vanished between query and get."*

So the fix landed in the *partitioned* path and not in the two *unpartitioned*
paths that share the identical shape.

**Path to failure.** A shared mailbox with 5000 messages, `maxObjectsInGet =
256`, server query page cap 100. `foreign_email_inventory` asks for 256, gets
100 ids, hydrates 100, sees `100 < 256`, yields `SyncEvent::Done(None)`. The
engine records a complete inventory. 4900 messages in that shared folder are
never enumerated, and nothing reports an error. `inventory_partitioning`
returns `Full` for every non-`Type(Email)` scope, so **every** foreign
(shared/delegate) scope takes this path unconditionally - the truncation is
guaranteed on such a server, not merely possible.

The same holds for `email_inventory` whenever the engine chooses
`InventoryPartition::Full` for the primary Email scope. A single deleted
message racing the backfill is enough to end it early.

**Proposed fix.** Port the `email_inventory_page` logic into both loops:
advance `position` by `ids.len()`, and terminate only on an *empty query page*,
never on a short hydration batch. Optionally surface the `Email/get`
`notFound` ids so a vanished message is a per-item signal rather than silence.

### B4 - WebSocket push discards the `accountId`, so shared-account pushes invalidate the primary scope

**Where:** `sync/push.rs::emit_push` (291-322).

**Mechanism.** RFC 8620 s7.1 shapes `StateChange.changed` as
`{accountId: {DataType: newState}}`. `emit_push` iterates
`changed.values()` and never looks at the key. Every `Email` state change,
whoever it belongs to, becomes
`HintPayload::SpecificCursorScope(CursorScope::Type(ObjectType::Email))` -
the *primary* account's scope.

**Path to failure.** A delegate drops a message into a shared mailbox
`acct-foreign`. The server pushes
`{"changed": {"acct-foreign": {"Email": "s2"}}}`. The engine is told the
primary `Type(Email)` scope is invalid, repolls the primary account's
`Email/changes` (which returns nothing new), and never repolls the
`Folder(acct-foreign, *)` scopes. The shared mailbox stays stale until the
next account reopen - which is exactly the failure push exists to prevent.
The primary scope also eats a spurious poll for a change that did not happen
in it.

**Proposed fix.** Thread the accountId through `emit_push`. When it names a
registered foreign account, emit one `SpecificCursorScope` hint per seeded
`Folder(accountId, *)` scope (or a single `SpecificMembership(Mailbox(accountId))`
hint if the engine's covering rule accepts the owner tag); when it names the
primary account, keep today's behavior. `emit_push` currently takes only
`(PushObject, &Sender)`, so it needs the foreign registry - the reader loop
already holds a `Client` and could hold an `Arc<HashMap<String, _>>` alongside
`enabled`.

Pinned by `a_foreign_account_state_change_is_announced_as_a_primary_scope_change`
in `sync/push.rs` (documents current behavior, explicitly labelled a bug).

### B5 - A foreign `Folder` scope cannot be push-subscribed at all

**Where:** `sync/push.rs::data_type_for_scope` (149-156), consumed by
`push::subscribe` (106-117).

**Mechanism.** `data_type_for_scope` returns `Some(_)` only for
`Type(Email|Mailbox|Thread)`. `CursorScope::Folder(_)` falls into `_ => None`.
`subscribe` filter-maps the caller's scopes and then hard-errors when the
resulting set is empty.

**Path to failure.** An engine that subscribes per scope calls
`push_subscribe(&[Folder(acct-9 + inbox)])`. It gets back
`Unsupported("JMAP push subscribe requires at least one supported scope")`,
so the consumer records "push unavailable" for that scope. A mixed call
(`[Type(Email), Folder(...)]`) succeeds but silently drops the foreign half,
which then never appears in the `enabled` union either.

This compounds B4: even if `emit_push` were fixed to route by account, nothing
would be subscribed for the foreign scopes to begin with. JMAP WebSocket push
is subscribed *per DataType* and delivered for every visible account, so
`Folder(_)` should simply map to `DataType::Email`.

**Proposed fix.** `CursorScope::Folder(_) => Some(DataType::Email)` in
`data_type_for_scope`. `scope_for_data_type` stays as-is (it is the reverse
map used for hints, and B4's fix supersedes it).

Pinned by `a_foreign_folder_scope_maps_to_no_push_data_type`.

### B6 - `scope_lifecycle` advances the mailbox state before hydrating names, so a transient failure loses folder events permanently

**Where:** `sync/discover.rs::scope_lifecycle` (138-183).

**Mechanism.** Order of operations inside the `Ok(response)` arm:

```
let created  = response.created().to_vec();
let updated  = response.updated().to_vec();
state_cache::set(&mailbox_states, &account_id, response.new_state());  // line 143
if (...) && let Ok(fetched) = fetch_mailboxes(&mail, created.chain(updated)).await  // 147
```

The since-state is committed *before* the follow-up `Mailbox/get`, and the
`if let Ok` swallows every failure of that get.

**Path to failure.** The user creates a folder "Receipts" on their phone.
`Mailbox/changes` reports `created: ["mb-9"]` and `newState: "s2"`.
`state_cache::set` writes `s2`. The follow-up `Mailbox/get` for `mb-9` hits a
503 (or a connection reset, or a rate limit). `if let Ok(...)` skips the whole
block, no `ScopeLifecycle::Created` is emitted, and the next poll asks for
changes *since s2* - which no longer includes `mb-9`. The folder is invisible
to the consumer until the account is reopened. The event is not delayed; it is
gone.

Note the poll interval is 300s, so "until reopen" can be a very long time.

**Proposed fix.** Move `state_cache::set` after the successful fetch, and
classify the `fetch_mailboxes` error through `into_account_error` instead of
discarding it: `continue` without advancing on a retry class, and emit
`ScopeLifecycleEvent::Terminated` on a terminal class. Destroyed ids come
straight off the changes response and can be emitted before the fetch.

### B7 - `scope_lifecycle` reports a rename on every mailbox update

**Where:** `sync/discover.rs::scope_lifecycle` (165-181).

**Mechanism.**

```
let old_name = replace_mailbox_name(&mailbox_names, id.clone(), name).await;
if old_name.is_some() {
    yield ScopeLifecycle::Renamed { old: scope.clone(), new: scope };
}
```

`replace_mailbox_name` is `HashMap::insert`, which returns the *previous
value* - `Some(_)` for any mailbox already in the map, whether or not the name
changed. The new name is never compared to the old one.

**Path to failure.** A message arrives in the Inbox. `Mailbox/changes` reports
`updated: ["mb-inbox"]` (RFC 8621 s2.4 lets the server report a mailbox
updated when only its counts changed - and `ChangesResponse::updated_properties`
exists in `mailbox/mod.rs` precisely to describe that case, though this code
never reads it). The follow-up get returns name "Inbox". `insert` returns
`Some("Inbox")`. A `ScopeLifecycle::Renamed { old: Mailbox(mb-inbox), new:
Mailbox(mb-inbox) }` is emitted. Every incoming message produces a spurious
folder-rename event, and `old == new` so the event carries no information even
when the rename is real.

**Proposed fix.** Emit only when `old_name.as_deref() != Some(name.as_str())`.
Additionally consider gating the whole created/updated fetch on
`response.updated_properties()` containing something other than the count
properties (`Property::is_count` already exists for this and is currently
unused by the lifecycle worker) - that removes the `Mailbox/get` round trip
entirely for the count-only case.

### B8 - `Response::get` matches on the call id only; the method name is never checked

**Where:** `core/response.rs::get` (31-49); the handle field it ignores is
`core/request.rs::CallHandle::method_name` (20).

**Mechanism.** The lookup is
`self.raw.iter().position(|(_, _, id)| id == &handle.call_id)`. The first
tuple element - the method name the server echoed - is discarded. The handle
stores `method_name`, and `reference/jmap.md` line 46 asserts
*"`CallHandle<M>` validates call_id and method name"*, but nothing compares
them.

**Path to failure.** A server (or a proxy, or a version skew) answers call
`s0` with a different method's result. Every JMAP `/get` response has the
identical envelope (`accountId` / `state` / `list` / `notFound`) and every
field on this crate's object structs is `Option`, so a `Mailbox/get` body
deserializes cleanly into `GetResponse<Email>`: `id` maps across (both spell
it `"id"`), everything else lands as `None`. `hydrate::fetch_batch` then emits
one `ItemOutcome::Succeeded` per object - N "hydrated messages" keyed by
*mailbox* ids, with empty keyword sets and blank metadata. The engine records
them as successfully hydrated. No error anywhere, and the resulting flags-hash
mismatch looks like ordinary drift on the next inventory pass.

This also silently masks the more mundane case: a server that renumbers or
reuses call ids.

**Proposed fix.** Compare the stored `name` against `handle.method_name` in
`Response::get` and return a contract-violation error on mismatch (the `error`
name is already special-cased above, so the comparison only needs to run on
the success branch). One line, and it makes the doc true.

Pinned by `response_get_matches_the_call_id_only_and_ignores_the_method_name`
in `core/tests.rs` (documents current behavior, explicitly labelled a bug).

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

Related, and worse for correctness: the code comment at 94-99 claims *"the
per-mailbox membership rides on the change items' scope changes rather than
filtering the changes call"* - but `email_changes` emits only
`Change::ObjectChange`, never `Change::ScopeChange`. The compensating
mechanism the comment names does not exist, so nothing tells the engine which
of the 12 folders the message actually landed in.

**Proposed fix.** Two options, both larger than a patch:
(a) seed one `Folder` scope per foreign *account* rather than per mailbox, and
    derive per-mailbox membership from the hydrated `mailboxIds`; or
(b) keep per-mailbox scopes but have the foreign changes leg hydrate
    `mailboxIds` for each changed id and emit `ScopeChange`s, so the comment
    becomes true and the engine can attribute the change.
Either way the comment must stop describing behavior the code does not have.
This is a design call, not a mechanical fix.

### B10 - An empty or unrecognized `FlagOp` sends an empty patch and is reported as applied

**Where:** `sync/mutation.rs::apply_flags` (312-337), consumed by `send_set`
(284-310) and `apply_batch` (140-232).

**Mechanism.** `apply_flags` writes into an `EmailPatch` obtained from
`set.update(id)`. `FlagOp::Add(empty_set)` writes nothing; the `_ => {}` arm
(there for `FlagOp`'s `#[non_exhaustive]`) writes nothing either. The patch
serializes to `{}`. `Email/set` accepts `{"update": {"m1": {}}}` as a valid
no-op and answers `updated: {"m1": null}`. `apply_batch` reads that through
`response.updated(&email_id)` and emits
`ItemOutcome::Succeeded(MutationSuccess::Applied)` for every id.

**Path to failure.** A future `FlagOp` variant (the type is
`#[non_exhaustive]` precisely to allow one) reaches this Account impl. Every
target is reported `Applied`, the engine's read-back guard sees flags it never
asked to change, and the mutation is silently dropped. The same shape is
reachable today via `FlagOp::Add(HashSet::new())`, `Remove(empty)`, and
`Patch { add: empty, remove: empty }`.

**Proposed fix.** Have `apply_flags` return whether it produced any patch
entry; an operation that produced none is either a boundary rejection
(`Request(Malformed)` for an unknown variant) or a short-circuited local
success (for a genuinely empty set) - both are honest, and `Applied` for an
unhandled operation is not.

Pinned by `an_empty_flag_op_sends_an_empty_patch_that_reads_back_as_success`
in `sync/mutation.rs` (documents current behavior, explicitly labelled a bug).

### B11 - The push reader task does not race the shutdown token against its blocking reads

**Where:** `sync/push.rs::reader_loop` (204-281).

**Mechanism.** `shutdown.is_cancelled()` is polled at the top of the reconnect
loop and after each received message, but the two places the task actually
parks are not raced against it:

- `futures::StreamExt::next(&mut ws).await` (224) - blocks until the server
  sends something or the socket errors.
- `tokio::time::sleep(backoff).await` (278) - up to `policy.max` (60s by
  default).

**Path to failure.** `JmapAccount::close()` cancels the token and awaits
`disable_push_ws()`. If the socket is already gone (the common case - the
account is closing because the connection dropped), the reader is sitting in
`sleep(60s)` and keeps the `Client`, its `AccountNet` handle, and the
broadcast sender alive for up to a minute past close. On a reopen loop that
churns accounts, reader tasks accumulate. `reference/jmap.md` states
*"`close()` cancels the shutdown token (terminating the WebSocket reader and
in-flight streams)"* - the reader is not terminated, only asked.

**Proposed fix.** Wrap both awaits in `tokio::select!` against
`shutdown.cancelled()`, matching the pattern `push::stream` already uses
(62-87). Low risk, both sites are cancel-safe.

---

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

### G4 - `GetResponse` requires `notFound`, and a missing one is misclassified

`core/get.rs:46-47`: `not_found: Vec<O::Id>` has no `#[serde(default)]`. RFC
8620 s5.1 does require the property, but implementations omit it when empty
often enough to matter. When it is missing, the whole `Response::get`
deserialization fails with `crate::Error::ResponseDecode`, which
`sync/error.rs` maps to a protocol parse failure - correct classification for
a genuinely malformed body, but it takes down the entire batch (every sibling
call in the same request) over one absent empty array. `#[serde(default)]` on
`not_found` (and arguably on `list`) makes the decode tolerant without
weakening anything real. Same consideration for
`ChangesResponse`/`QueryResponse` list fields - not audited in detail.

### G5 - `Client::default_account_id` is nondeterministic when the session lists more than one primary account

`client.rs:235-239` and `278-282` both do
`session.primary_accounts().next()` on a `HashMap<String, String>`. A session
that lists mail + calendars + contacts primaries (the normal case) yields an
arbitrary one, so `Client::build()` stamps an arbitrary `accountId` into every
method it carries.

Latent today: nothing in the crate calls `client.build()` directly - every
sync call site goes through `Account::build()`, which overrides. But
`Client::build` is `pub(crate)` and the next person to reach for it gets a
random account with no compiler complaint. Either sort the primaries and take
the lowest, prefer the `mail` URI explicitly, or delete `Client::build` in
favor of `Account::build`. (`client.rs` is outside my scope; reporting only.)

The test fixtures I added deliberately list at most one primary account so
this cannot make them flaky, and say so in a comment.

### G6 - Feature-dependent `Email` header decoding

`crates/jmap/src/email/mod.rs` gates the header-form aliases
(`header:From:asAddresses` etc.) behind `cfg_attr(not(feature = "debug"), ...)`.
Validation runs `--all-features`, so `debug` is on and the aliases vanish -
meaning the shape that is tested is not the shape that ships by default.

Impact on my scope is small: neither `hydrate.rs` nor `pim.rs` ever *requests*
the header form (they use `Property::From` etc.), so a conforming server
answers with the canonical names either way. The exposure is a server that
echoes back the header spelling it was not asked for; under the default
feature set that decodes into `from`, under `--all-features` it silently lands
in the flattened `header` catch-all and `from` stays `None`, so a hydrated
message loses its sender. Worth knowing that the crate has two decode
behaviors selected by a feature that is supposed to be diagnostic-only.

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

---

## 3. Optimization opportunities

### O1 - Account open does `3 + 4F` serial round trips where `1 + F` (or fewer) would do

`factory.rs::open` (164-260), sequentially and each on its own request:

- primary: `Email/get` (state probe), `Mailbox/get` (state + names),
  `Thread/get` (state probe) - 3 round trips.
- plus `fetch_self_emails` - 1.
- per foreign account `seed_foreign_account` (424-440): `Email/get`,
  `Mailbox/get`, `Thread/get`, `Mailbox/get` (enumeration) - 4 round trips,
  serially, and the outer `for foreign_id in foreign_ids` loop is serial too.

With 5 shared mailboxes that is 24 sequential HTTP round trips before the
account is usable. On a 150ms RTT link, ~3.6 seconds of pure latency.

`Request::send_methods` exists for exactly this (`core/request.rs:243-255`,
tuples up to 8 methods) and is used nowhere in `sync/`. The three primary
probes are one `send_methods((EmailGet, MailboxGet, ThreadGet))`. Each foreign
account's four calls are one batch. The foreign accounts themselves could go
in the same request up to `maxCallsInRequest` (validated at 82-87 and then
discarded), or at minimum `futures::future::join_all` over the per-account
batches. Realistic result: 24 round trips -> 2, or 1 on a server with a
generous `maxCallsInRequest`.

Note the two `Mailbox/get` calls per foreign account are literally the same
call with different property sets; even without batching, merging them halves
that leg. Same for the primary: `discover::fetch_mailbox_names` already
returns the state, so the separate mailbox state probe is redundant there -
and that merge is already done for the primary but not mirrored into
`seed_foreign_account`.

Nothing here changes behavior; it is pure round-trip elimination on the
open path, which is the path a user waits on.

### O2 - The sync layer hardwires `ReqwestTransport`, so none of it is testable in-process

Nine files under `sync/` each declare
`type MailAccount = crate::account::Account<ReqwestTransport>;`
(account, blob, changes, discover, hydrate, inventory, mutation, pim, and
factory's variant). `Client<T>` and `Account<Tr>` are both generic over
`HttpTransport`, and `Client::with_transport` is `pub(crate)` - the seam
exists and the sync layer opts out of it.

Consequence for this sweep: B2 (silent no-op move), B3 (inventory
truncation), B6 (lost lifecycle events), B9 (duplicate change fan-out), and
B10's read-back half are all *behavioral* bugs in async streams that a stub
transport would pin in a dozen lines each, and none of them can be tested
today. I could only pin their pure sub-parts.

Making the alias a generic parameter (`fn stream<Tr: HttpTransport>(mail:
Account<Tr>, ...)`) is mechanical but touches every file in the tree, so it
is a decision rather than a drive-by. It is the single highest-leverage change
available for this crate's testability, and the `core/tests.rs` stub I landed
is the working proof that the transport seam holds.

### O3 - `inventory.rs` re-collects ids it already owns

`let ids = query_response.ids().to_vec();` then `.ids(ids.clone())` in both
`email_inventory` (281-288) and `foreign_email_inventory` (129-136). The clone
is only needed because `batch_len`/`consumed` is read afterwards; taking
`ids.len()` first removes both the `to_vec` and the `clone` on the hot
backfill path. Small, but it is per page of every backfill.

---

## 4. Doc contradictions found

1. `reference/jmap.md:46` - *"`CallHandle<M>` validates call_id and method
   name"*. It validates the call id only. See B8.
2. `reference/jmap.md:171` - *"`close()` cancels the shutdown token
   (terminating the WebSocket reader ...)"*. The reader is not terminated
   promptly; it parks on an unraced read or sleep. See B11.
3. `sync/changes.rs:94-99` - the comment claims foreign per-mailbox
   membership *"rides on the change items' scope changes"*. No `ScopeChange`
   is ever emitted on that path. See B9.
4. `sync/capabilities.rs:194-196` - the comment claims JMAP foreign accounts
   are not open-time discovery. They are. See G1.
5. `reference/jmap.md:217` - the short-page discipline is described as a
   property of the Email inventory generally; it holds only in
   `email_inventory_page`. See B3.
6. `reference/jmap.md:219` - lists `Type(Thread)` changes as supported
   without noting that discovery never offers the scope. See G2.

I did not edit `reference/jmap.md`: it is outside my scope, and per the
project rule the doc update should ride with whichever fix commit lands.

---

## 5. Tests landed

All deterministic and in-process. No listener, no port, no daemon, no clock
dependence.

**`crates/jmap/src/core/tests.rs`** - new `mod envelope`, built on a stub
`HttpTransport` (`StubTransport`) that records the exact JSON body the client
would have POSTed and replays a FIFO of canned replies. This is the
request/response envelope's first test coverage.

- `request_serializes_using_method_calls_and_injected_account_id` - the RFC
  8620 3-tuple encoding, `using` seeded with core, `createdIds` omitted when
  unset, and `accountId` injected by `Request::call` rather than by the method
  constructor.
- `using_accumulates_each_capability_exactly_once` - a Mail-capability method
  between two Core ones appends `urn:ietf:params:jmap:mail` once; call ids are
  positional and monotonic. (Needed a second test method on a different
  capability - `TestMailGet` - since every pre-existing test method rides
  Core, which `Request::new` already seeds.)
- `account_scoped_and_explicitly_overridden_account_ids_reach_the_wire` -
  `Account::build` stamps the capability's primary account; an explicit
  `Request::account_id` beats the client default.
- `send_methods_batches_in_order_and_returns_typed_responses` - one batch is
  one round trip, tuple order matches call order.
- `responses_returned_out_of_order_still_match_their_handles` - extraction is
  by call id, not by position (RFC 8620 permits any response order).
- `result_references_serialize_as_hash_prefixed_arguments` - `#ids` shape and
  the mutual exclusion with literal `ids`.
- `a_diverging_session_state_marks_the_cached_session_stale` - both
  directions of the `session_updated` flag.
- `a_transport_failure_surfaces_as_error_transport` and
  `an_unparseable_response_body_is_a_response_decode_error` - the
  transport-vs-decode error split is not laundered.
- `response_get_matches_the_call_id_only_and_ignores_the_method_name` -
  **documents B8, does not endorse it.**

**`crates/jmap/src/sync/push.rs`** - new test module (the file had none).

- `a_primary_email_state_change_invalidates_the_email_cursor_scope`,
  `a_data_type_with_no_cursor_scope_degrades_to_an_unknown_hint`,
  `a_grouped_push_fans_out_to_one_event_per_entry` - `emit_push` projection.
- `a_foreign_account_state_change_is_announced_as_a_primary_scope_change` -
  **documents B4, does not endorse it.**
- `a_foreign_folder_scope_maps_to_no_push_data_type` - **documents B5, does
  not endorse it.**
- `data_type_and_scope_mappings_are_inverse_for_the_supported_types`,
  `the_enabled_data_type_union_spans_every_live_subscription`.
- `the_push_stream_ends_when_the_shutdown_token_is_cancelled` and
  `a_lagged_broadcast_slot_coalesces_into_an_unknown_invalidation` - the
  subscriber's two contract points, driven through a real `broadcast` channel
  and a real `CancellationToken`.

**`crates/jmap/src/sync/changes.rs`** - new test module (the file had none).

- `primary_change_ids_stay_bare`, `foreign_change_ids_carry_their_owning_account`,
  `an_empty_change_list_yields_no_changes` - the foreign id-qualification on
  the changes leg, which is what makes a changed shared message hydrate
  against its owning account.
- `every_supported_scope_can_build_its_checkpoint_cursor` - `checkpoint_for`
  unwraps, so every scope reachable from a change loop (including the
  codec-encoded foreign `Folder` shape) must encode and decode again.

**`crates/jmap/src/sync/mutation.rs`** - added to the existing module.

- `every_batch_is_gated_by_if_in_state` - the `MutationConcurrency::StateBased`
  claim in `capabilities.rs` is actually on the wire.
- `flag_add_and_remove_use_dotted_keyword_paths` - RFC 8620 s5.3 removal is an
  explicit `null` key, not `false` and not an omission.
- `flag_set_replaces_the_whole_keyword_map`,
  `a_flag_patch_applies_removals_after_additions`,
  `a_multi_id_batch_carries_one_update_entry_per_id`,
  `a_move_assigns_the_destination_as_the_only_mailbox`.
- `an_empty_flag_op_sends_an_empty_patch_that_reads_back_as_success` -
  **documents B10, does not endorse it.**

**`crates/jmap/src/sync/state.rs`** - added to the existing module.

- `rejects_a_cursor_whose_payload_scope_disagrees_with_its_envelope` and
  `a_folder_scope_naming_a_different_mailbox_is_also_rejected` - the
  `decode_cursor` scope-equality guard, which had no coverage.
- `an_unsupported_scope_cannot_be_encoded_at_all` - a `FolderId` with no
  foreign separator has no account to route to and must not mint a cursor.

**`crates/jmap/src/sync/hydrate.rs`** - added to the existing module.

- `each_projection_requests_the_properties_it_actually_reads` - dropping
  `Keywords` from `FlagsOnly` would hydrate every message with an empty flag
  set instead of failing; `Metadata` must request exactly
  `inventory_properties()` or hydrated and inventory fingerprints diverge.
- `a_foreign_route_and_a_primary_route_never_share_a_batch_buffer` - the
  `HydrationRoute` keys `stream`'s per-request buffers, so they must not
  compare equal.

**Deliberately not duplicated.** `crates/jmap/src/tests.rs` (another agent's
file) already pins the `MailboxPatch::default()` / `parent_id(None)` wire
shapes, including a verbatim replay of the `container_rename` construction.
Adding the same assertions under `sync/` would be redundant, so B1 and B2 are
reported here with their sync-side consequences and left pinned there.

---

## 6. Not done, and why

- **No test for B2, B3, B6, B7, B9, B11.** Every one of them lives inside an
  `async_stream` that takes a concrete
  `Account<ReqwestTransport>`. There is no seam to inject a stub through
  without changing the nine `type MailAccount = ...` aliases in `sync/`,
  which is a structural change, not a test. See O2 - unblocking this is the
  highest-value follow-up in the crate. I pinned the pure sub-parts I could
  reach (`emit_push`, `object_changes`, `apply_flags`, `properties_for_projection`,
  `checkpoint_for`, the cursor guards) and left the stream bodies uncovered.

- **`sync/error.rs` (68 KB) not audited.** It has the largest existing test
  module in the tree and `reference/error-model.md` plus
  `plans/bug-hunt-2026-06-17.md` both indicate it was worked over recently. I
  read its call sites from the streams rather than the mapping table itself.
  A dedicated pass on the `(AccountErrorKind, Cause) -> RecoveryClass`
  routing against `error-model.md` is still owed.

- **`sync/calendar_ops.rs` (69 KB) and `sync/contacts.rs` (35 KB) not
  audited.** In scope by directory, but they are PIM surfaces layered on the
  same primitives rather than the request/cursor/change machinery the task
  named, and both already carry test modules. Reading them properly needs the
  JSCalendar / JSContact mappings in `reference/jmap.md` s"PIM primitives"
  cross-checked against the RFCs, which is its own sweep.

- **`sync/pim.rs` (121 KB) audited selectively.** I read the container CRUD,
  identity, role resolution, foreign owner-email gate, and hydration
  qualification. The send / draft / scheduled-send / search / filters legs I
  read only far enough to confirm the `role_mailboxes` dependency chain for
  B1. The send path in particular (result-referenced `Email/set` +
  `EmailSubmission/set` + `onSuccessUpdateEmail`) deserves its own read.

- **`sync/filters.rs` and `sync/foreign.rs` not re-audited.** `foreign.rs` is
  small, pure, and already well covered. `filters.rs` is a thin Sieve mapping
  with an existing test module and no cross-cutting invariants.

- **`core/query.rs`, `core/copy.rs`, `core/parse.rs`, `core/changes.rs`,
  `core/query_changes.rs` read but not deeply audited.** No defect surfaced on
  the read; the request-envelope work took the budget. `core/session.rs`
  I read in full (it is load-bearing for the test fixtures) and found nothing
  beyond G5, which lives in `client.rs`.

- **Nothing outside `core/` and `sync/` was edited.** The B1/B2 fixes both
  belong in `mailbox/mod.rs` and `identity/mod.rs`; B8's fix is one line in
  `core/response.rs` and is in scope, but it is a fix, not a test, so it is
  reported rather than applied per the task's split.
