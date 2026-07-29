# bifrost-graph bug hunt

Scope: `crates/graph/src/**`. Read `reference/graph.md`,
`plans/bug-hunt-2026-06-17.md` and `plans/graph/` (empty) first; nothing
below re-reports a finding those already closed.

Tests landed alongside this document pin behavior as it exists today.
Where a test documents behavior I believe is wrong, its doc comment opens
with "Documents a defect, NOT the intended contract" and explains the
failure inline - it does not name this file, per the workspace rule that
source comments must never point at `plans/`. Each finding below names the
test that pins it. **No fix in this document has been applied.**

---

## Bugs

### G-1 - EWS `Subscribe` puts the raw foreign-encoded `FolderId` on the wire

`crates/graph/src/account/ews_stream.rs:259` (`build_subscribe_request`).

Every other request builder decodes a foreign (shared-mailbox) `FolderId`
before it reaches the wire - `push::resource_for_scope` (fixed explicitly
for this, per its own comment), `inventory::initial_delta_url`,
`mutate::request_for_mutation`. `build_subscribe_request` does not: it
interpolates `folder.0` through `xml_escape`, which covers only the five
XML metacharacters.

Path to failure:

1. Factory configured `with_ews_streaming()` and `with_shared_mailbox("shared@contoso.com")`.
2. `discover_cursor_scopes` emits `FolderType { folder: FolderId("shared@contoso.com\u{1f}AAMk"), Email }`.
3. Engine calls `push_subscribe` with that scope; `subscribe_ews` stores it and the worker calls `active_ews_scopes`.
4. `build_subscribe_request` emits `<t:FolderId Id="shared@contoso.com\u{1f}AAMk"/>`.

U+001F is not a legal XML 1.0 character, so the SOAP body is rejected
outright (or, if a lenient parser accepts it, names a folder id the
authenticated mailbox does not own). The failure surfaces as an EWS
`ResponseCode` that `SoapFaultCode::parse` does not know, which by G-5
below is classified terminal - so EWS streaming push is dead for the whole
account, not just for the shared folder.

Secondary: even with the id decoded, no `X-AnchorMailbox` routing header is
sent (`EwsHeaders::default()`), so the subscription would target the
primary mailbox's folder namespace.

Proposed fix: decode with `foreign::parse_folder(folder).native_id()` the
way `resource_for_scope` does, and either group scopes by owning mailbox
and issue one Subscribe per mailbox with that mailbox's `EwsHeaders`, or
explicitly skip foreign scopes with a `Warning` until per-mailbox EWS
subscriptions are implemented. Silently emitting a broken id is the worst
of the three.

Test: `subscribe_request_leaks_the_foreign_separator_into_the_folder_id`
(ews_stream.rs).

---

### G-2 - Metadata hydration strips the shared-mailbox owner off the inner entry

`crates/graph/src/account/get.rs:421` (`metadata_or_flags`), reached from
`hydrated_from_value` for `Metadata`/`Headers`/`Preview`/`TextOnly`/`Full`/
`FullWithBlobs`.

`metadata_or_flags(value)` takes only the Graph JSON. It synthesizes a
`CursorScope::FolderType { folder: FolderId(parentFolderId), Email }` and
hands that to `inventory_entry_from_value`. `parentFolderId` is always a
NATIVE id, so the synthesized scope never parses as foreign, so:

- `encode_message_id` leaves the entry id BARE, and
- `membership_from_value` emits a bare `MembershipScope::Folder(native)`.

Path to failure: shared mailbox `shared@contoso.com`, message `AAMkmsg` in
its `inbox`.

| stage | id | membership |
| --- | --- | --- |
| `inventory_stream` | `shared@contoso.com\u{1f}AAMkmsg` | `Folder(shared@contoso.com\u{1f}inbox)` + `Mailbox(shared@contoso.com)` |
| `get_stream(Metadata)` | outer `shared@contoso.com\u{1f}AAMkmsg`, **inner `AAMkmsg`** | **`Folder(inbox)`** |

This is exactly the conflation `inventory::membership_from_value`'s own
comment says must not happen ("a foreign native id colliding with a primary
id (e.g. `inbox`) would conflate into the primary mailbox's membership
set"), and it breaks the documented invariant that one logical message
carries identical bytes everywhere. A consumer reconciling the hydrated
`InventoryEntry` sees a second, primary-looking key for the same message,
and a shared-mailbox `inbox` merged into the primary `inbox`.

Proposed fix: pass the caller's `ObjectId` into `metadata_or_flags`
(`hydrated_from_value` already holds it). Recover the owner with
`foreign::parse_message_id(&id).owner()` and build the scope with
`foreign::encode_foreign(owner, parentFolderId)`, then push
`MembershipScope::Mailbox(MailboxId(owner))` onto the entry the way
`inventory_stream` does. A public-folder id (`ParsedMessageId::Public`)
already goes down the EWS arm, so only the `Foreign` case needs handling.

Test: `metadata_projection_strips_the_shared_mailbox_owner_from_the_inner_entry`
(get.rs).

---

### G-3 - Foreign membership is double-encoded whenever `parentFolderId` is absent

`crates/graph/src/account/inventory.rs:188` (`membership_from_value`).

```rust
let fallback = match scope { FolderType { folder, .. } | Folder(folder) => folder.0.clone(), .. };
let native = value.get("parentFolderId")...unwrap_or(&fallback);
let folder = match parse_folder(folder).foreign() {
    Some(foreign) => encode_foreign(&foreign.mailbox, native),   // <- native is already encoded
    None => FolderId(native.to_string()),
};
```

The fallback is the scope's ALREADY-ENCODED folder id, and the foreign arm
encodes it a second time. For a foreign scope the result is
`mailbox\u{1f}mailbox\u{1f}folder`.

Path to failure - reachable on **every shared-mailbox deletion**, not an
edge case: a Graph delta `@removed` tombstone carries only
`{"id": ..., "@removed": {...}}`; `MESSAGE_SELECT` is irrelevant because
Graph does not ship properties on tombstones. `changes.rs:76` calls
`membership_from_value` for the `Removed` branch, so the emitted
`ScopeChange` is

```
ScopeChange { id: "shared@x\u{1f}m1",
              membership: Folder("shared@x\u{1f}shared@x\u{1f}AAMkRoot"),
              kind: Removed }
```

while the matching `Added` used `Folder("shared@x\u{1f}inbox")`. The engine
cannot reconcile a removal against a folder scope that discovery never
emitted, so the message stays in the consumer's index forever.

The primary-mailbox case is correct (no re-encoding), which is why this has
gone unnoticed.

Proposed fix: derive the fallback from the parsed folder, not the raw one:

```rust
let parsed = parse_folder(folder);
let native = value.get("parentFolderId")...unwrap_or(parsed.native_id());
```

then encode once. Note the tombstone case also cannot recover the item's
real parent folder - the scope's own folder is the best available answer,
which is fine, it just must be the NATIVE one.

Test: `foreign_scope_membership_double_encodes_when_parent_is_absent`
(inventory.rs), plus `membership_falls_back_to_the_scope_folder_when_parent_is_absent`
for the correct primary behavior.

---

### G-4 - `FlagOp::Add`/`Remove`/`Patch` drop category flags into an empty PATCH reported as applied

`crates/graph/src/account/mutate.rs:412` (`patch_for_flags`).

Only `FlagOp::Set` writes `categories`. `apply_flag_adds` /
`apply_flag_removes` handle `\seen`/`read` and `\flagged`/`flagged`/
`starred` and nothing else, so a `category:` flag (or any unrecognized
flag) contributes no field at all.

Path to failure:

1. Consumer calls `bulk_set_flags([m1], FlagOp::Add({"category:Work"}), key)`.
2. `patch_for_flags` returns `{}`.
3. `request_for_mutation` builds `PATCH /me/messages/m1` with `If-Match` and an EMPTY JSON body.
4. Graph accepts an empty PATCH with `200 OK`.
5. `mutation_item_outcome(200, ..)` returns `ItemOutcome::Succeeded(MutationSuccess::Applied)`.

The consumer is told the category was applied. Nothing was. The engine's
read-back guard is the only thing that would eventually notice, and only if
the consumer runs one.

Note this is not merely "unsupported": `pim_methods.set_category` is true
and the per-message `pim::set_category` path handles categories correctly.
It is only the BULK flag path that drops them.

Proposed fix (either is defensible, pick one deliberately):

- Make `Add`/`Remove` read-modify-write `categories` the way `Set` does,
  which requires the current array and therefore a pre-read - expensive but
  correct; or
- Detect that the op carried only fields Graph cannot express in this shape
  and return `ItemOutcome::Failed(Unsupported(UpdateFlags))` for that id
  rather than issuing an empty PATCH. Cheap, honest, and consistent with
  how `remove_from_container` is handled.

At minimum, an empty patch body must never be sent as a successful
mutation.

Test: `add_and_remove_silently_drop_category_flags_into_an_empty_patch`
(mutate.rs).

---

### G-5 - An unrecognized EWS `ResponseCode` is terminal, so EWS push dies on routine subscription expiry

`crates/graph/src/ews/mod.rs:143` (`SoapFaultCode::parse`),
`crates/graph/src/account/graph_error.rs:195` (`Unknown` ->
`Protocol(ContractViolation)`), `crates/graph/src/account/ews_stream.rs:401`.

`SoapFaultCode::parse` types seven `ErrorXxx` codes and collapses everything
else onto `Unknown`, which maps to `Protocol(ContractViolation)` ->
`RecoveryClass::ProviderContractViolation` -> `is_terminal() == true`.

Path to failure:

1. `PushMode::EwsStreaming`; `run_get_events_loop` long-polls `GetStreamingEvents`.
2. The streaming subscription lapses (idle timeout, mailbox move, server restart, an admin unsubscribe).
3. EWS answers HTTP **200** with `ResponseClass="Error"` and `<m:ResponseCode>ErrorSubscriptionNotFound</m:ResponseCode>` (or `ErrorInvalidSubscription`, `ErrorSubscriptionUnsubscribed`, `ErrorInvalidWatermark`).
4. `check_response_error` correctly surfaces it as `EwsError::SoapFault { code: Unknown }`.
5. `ews_error_to_account_error` -> terminal.
6. `run_get_events_loop` returns `StreamLoopExit::Terminated`; `run_streaming_worker` emits `WatchEvent::Terminated` and **returns**.

The worker's own outer loop would have re-`Subscribe`d and recovered in one
iteration. Instead in-process push is dead for the life of the account (the
worker is only respawned by `ensure_ews_worker`, which is called on
`push_subscribe` and from `push_stream`, not on the worker exiting).

The same terminal path swallows genuinely transient codes -
`ErrorInternalServerTransientError`, `ErrorTimeoutExpired`,
`ErrorTooManyObjectsOpened`. "Unknown provider vocabulary is terminal" is
the wrong default for a long-lived worker.

Proposed fix, in order of value:

1. Add the subscription-lifecycle codes to `SoapFaultCode` and map them to a
   dedicated non-terminal class (a `SyncState`-ish "resubscribe" signal, or
   simply `Server(Unavailable)`), so the worker's `Disconnected` +
   re-Subscribe path handles them.
2. Independently, make `run_streaming_worker` not exit on a terminal exit
   from the events loop unless the class is auth/policy-terminal - a
   provider contract violation should reconnect with backoff, exactly as
   the malformed-XML branch already does.

Test: `unrecognized_ews_response_codes_are_terminal_contract_violations`
(graph_error.rs), plus
`ews_mailbox_move_in_progress_is_retryable_not_terminal` for the classified
counter-example.

---

### G-6 - `get_stream`'s `$batch` never reconciles missing or out-of-range response ids

`crates/graph/src/account/get.rs:295` (`fetch_batch`).

`mutate::submit_batch` tracks `seen_indices` and emits a
`ContractViolation` failure for every submitted request that got no
response, with the comment "every id accounted for exactly once".
`reactions::classify_chunk` does the same via `answered` plus an explicit
`.filter(|i| *i < chunk.len())`. `fetch_batch` does neither, even though its
own doc comment claims "the consumer still sees exactly one outcome per
pulled id".

Two concrete failures:

1. **Dropped id.** Graph returns 19 responses for a 20-request `$batch`
   (documented behavior under partial failure / throttling). The 20th id
   appears on no lane - not `Succeeded`, not `Failed`. The engine's
   hydration accounting sees `N-1` outcomes for `N` pulled ids.
2. **Fabricated id.** An `item.id` that does not parse as `usize`, or
   parses out of range, hits
   `.unwrap_or_else(|| ObjectId(item.id.clone()))` at get.rs:300. That
   injects an `ObjectId` the caller never asked for into the batch, while
   the real id is still missing. `reactions.rs` guards this with a range
   filter; `fetch_batch` does not.

Proposed fix: port the `mutate::submit_batch` sweep verbatim - a
`HashSet<usize>` of seen indices, a range filter on the parsed index
(`skip` an out-of-range response instead of fabricating an id), and a
trailing loop emitting `ItemOutcome::Failed(ContractViolation)` for every
unanswered index. The EWS arm already answers per id, so only the REST arm
needs it.

Untested today; I did not add a test because it needs a stub `post_batch`
seam that does not exist on `GraphClient` (see "Not reached", below).

---

### G-7 - A partially-failed `push_subscribe` leaks server-side subscriptions

`crates/graph/src/account/push.rs:108-119` (`subscribe_graph`).

```rust
for (resource, _) in grouped {
    let response = create_subscription(...).await.map_err(...)?;   // <- early return
    subscriptions.push(...);
}
```

Path to failure: the engine subscribes 3 scopes that group into 3 resources.
The first two `create_subscription` calls succeed; the third is throttled
(429) or hits a per-app subscription quota. The `?` returns `Err`, the
local `subscriptions` vec is dropped, and no `SubscriptionHandle` is ever
minted - so nothing on the account, and nothing the engine can hand to
`push_unsubscribe`, references the two live server-side subscriptions.

They stay alive for `DEFAULT_EXPIRATION_MINUTES` (24h), delivering
notifications to the consumer's webhook endpoint for a subscription the
account has no record of, and any retry of `push_subscribe` creates two
more. The `clientState` for each is also gone (G-8), so the receiver cannot
even distinguish them.

Proposed fix: on error, best-effort `delete_subscription` every id created
so far before returning, mirroring the rollback `unsubscribe_graph` already
performs. Failing that, store the partial group under a handle and return
the error with the handle attached so teardown is possible.

---

### G-8 - `clientState` is generated, sent, and discarded, so the documented receiver validation is impossible

`crates/graph/src/webhooks.rs:51`, `crates/graph/src/webhooks.rs:23`
(`SubscriptionResponse`).

`create_subscription` mints a fresh 16-byte random `clientState` per
resource and posts it. `SubscriptionResponse` decodes only `id` and
`expirationDateTime`, and `subscribe_graph` stores only
`GraphSubscriptionState { server_id, expires_at }`. There is no accessor
anywhere on `GraphAccount`, `GraphSubscriptionGroup`, or the returned
`SubscriptionHandle` that exposes the value.

`reference/graph.md` tells consumers: "consumers mount an HTTPS endpoint at
`PushEndpoint::webhook_url`, validate `clientState`, and feed invalidations
into the engine `InvalidationSink`." That is not currently possible. The
`clientState` is the only thing standing between the consumer's public
webhook endpoint and forged invalidations, so the doc is describing a
control that does not exist. Worse, each RESOURCE gets a different random
value, so a receiver could not even use a single shared secret.

Proposed fix: let the consumer supply the secret. Add it to `PushEndpoint`
(`with_push_endpoint(url, client_state)` or a
`with_webhook_client_state(s)` builder), thread it through
`create_subscription`, and use one value for all resources on the account.
Keeping the random generation but surfacing it on the handle also works,
but it forces the consumer to plumb a per-subscription lookup into an HTTP
handler that has not run `push_subscribe`, which is worse ergonomics for
the same security.

Test: `each_client_state_is_a_distinct_unexported_secret` (webhooks.rs)
pins the mechanics.

---

### G-9 - A delta page with neither link silently live-locks `inventory_stream`

`crates/graph/src/account/inventory.rs:125`.

`changes_stream` treats "neither `@odata.nextLink` nor `@odata.deltaLink`"
as a contract violation, with a comment explaining exactly why: "Emitting
`Final` + `Done(None)` here would drop the cursor advance, so the engine
would re-issue the same final page on every poll forever."

`inventory_stream` reaches the identical wire condition through the
identical `ODataCollection` decode - and does precisely what that comment
forbids:

```rust
} else {
    if !entries.is_empty() { yield batch(entries, PageBoundary::Final, None); }
    yield SyncEvent::Done(None);
    return;
}
```

Path to failure: a corporate proxy or gateway strips OData annotations from
the response body (a real and common failure mode with content-rewriting
middleboxes). Every `inventory_stream` run yields the full first page,
`Done(None)`, no `Checkpoint`. The engine never gets a cursor, so
`establish_initial_cursor` -> `EstablishViaInventory` -> the same page ->
forever, re-emitting every entry each cycle at full bandwidth cost, with no
error anywhere.

Proposed fix: mirror the changes-side branch - emit
`protocol_violation(ContractViolation, SyncInventory, Some(Cursor(scope)),
...)` before `Done(None)`. The asymmetry is unjustified: both streams walk
the same delta collection.

---

### G-10 - EWS `Subscribe` silently drops non-`FolderType` scopes, producing an empty `FolderIds`

`crates/graph/src/account/ews_stream.rs:261`.

`for scope in scopes { if let CursorScope::FolderType { folder, .. } = scope { .. } }`
- anything else contributes nothing and raises nothing. A subscription
request built from only such scopes ships `<t:FolderIds></t:FolderIds>`,
which EWS rejects with `ErrorInvalidSubscriptionRequest`. That code is not
in `SoapFaultCode::parse`, so by G-5 the whole worker terminates.

Note `push_subscribe` already rejects `CursorScope::Folder` up front, so the
reachable shapes are `Account`/`Query`/`Mailbox` scopes - which the webhook
path rejects loudly (`subscribe_partial_resolve_returns_unsupported`) and
the EWS path accepts silently. The two modes should agree.

Proposed fix: apply the same "fail loudly on an unresolvable scope" rule
`subscribe_graph` uses, in `subscribe_ews` before the worker starts.

Test: `subscribe_request_silently_drops_non_folder_scopes` (ews_stream.rs).

---

### G-11 - The renewal worker re-emits `Terminated` every tick and never abandons a dead subscription

`crates/graph/src/account/push.rs:173-257` (`run_graph_subscription_worker`).

Two distinct problems in one loop:

1. **`Terminated` is not terminal.** When `renew_subscription` fails with a
   terminal class (401 -> `AuthLost`), the worker sends
   `WatchEvent::Terminated(error)` and then *continues looping*. The
   subscription stays in `graph_subscriptions` with its stale `expires_at`,
   so it is still `is_expiring_soon` next tick, renewal fails again, and
   another `Terminated` goes out - every 10 minutes, forever, until the
   account is closed. A `Terminated` event that repeats is a contract
   smell at best; a consumer that tears down on the first one will see N-1
   events for a channel it already dropped.

2. **A server-deleted subscription is retried forever.** Graph deletes a
   lapsed subscription; renewal then 404s. With no `ErrorScope` on the
   context, `resource_from_scope(None)` is `None`, so 404 maps to
   `Server(Error { status: 404 })` - not terminal. The worker emits one
   `Disconnected`, then re-renews the same dead `server_id` every 10
   minutes indefinitely. Push never recovers and the dead entry is never
   dropped from the map.

Proposed fix: on a terminal class, remove the affected group (or the
affected `GraphSubscriptionState`) from `graph_subscriptions`, emit
`Terminated` once, and return when no groups remain. On a 404/410
specifically, drop the state and re-`create_subscription` for the same
resource rather than re-`PATCH`ing an id the server no longer has.

---

## Observations

**O-1 - `derive_beta_base` falls back to the production Graph host.**
`crates/graph/src/client.rs:61`. Unlike `derive_outlook_base`, which
correctly follows any non-`graph.microsoft.com` api-base, `derive_beta_base`
only rewrites a base ending in `/v1.0` and otherwise returns the literal
`https://graph.microsoft.com/beta`. A harness or sovereign-cloud base shaped
differently would send beta traffic to the real service - the exact bug the
`derive_outlook_base` doc comment was written to prevent. Latent, because
`api_beta_base` is read only from `#[cfg(test)]` code today (see O-8).
Pinned by `beta_base_falls_back_to_production_for_a_base_without_the_v1_suffix`.

**O-2 - An unconfigured foreign mailbox degrades to the primary mailbox
rather than reporting stale config.** `client_for_scope` /
`client_for_owner` fall back to the primary client when a mailbox is not in
`shared_clients`, while `parse_folder` still strips the owner. A persisted
`FolderType { folder: "other@contoso.com\u{1f}AAMk" }` scope that outlives
its `with_shared_mailbox` entry therefore subscribes/reads
`/me/mailFolders/AAMk/...`. The `client_for_owner` doc comment argues for
this ("the subsequent request will surface the real `/me` 404, which is
more honest than a silent local error"), which is a defensible call for a
message id, but for a FOLDER-scoped subscribe/delta it silently points at a
different mailbox's namespace instead of 404ing. Worth revisiting for the
scope-shaped call sites specifically. Pinned by
`an_unconfigured_foreign_mailbox_subscribes_against_the_primary_prefix`.

**O-3 - Expiry parsing silently ignores a numeric UTC offset.**
`webhooks.rs:175`. `parse_iso8601_to_unix` strips a trailing `Z` and knows
nothing about `+05:00`; the offset digits are dropped by
`filter_map(parse)` and the timestamp is read as UTC. Graph documents
`expirationDateTime` as UTC-with-`Z`, so this is not live - but the failure
direction is the dangerous one (a `+05:00` expiry reads five hours later
than it is, so the worker renews too late and the subscription lapses).
Unreadable values collapse to epoch 0, which fails open to "renew now" -
safe, but it re-renews every tick instead of reporting the bad value once.
Pinned by `a_numeric_utc_offset_is_silently_dropped_rather_than_rejected`
and `unreadable_expiry_reads_as_epoch_zero_and_forces_renewal`.

**O-4 - `etag_index` grows without bound.** `GraphAccount::etag_index` is
an `Arc<RwLock<HashMap<String, String>>>` written by `inventory_stream`,
`changes_stream`, `get.rs::fetch_batch`, `mutate::refresh_missing_etags`
and `pim::cache_etag_for`. Nothing ever removes an entry - not on
`Destroy`, not on scope disable, not on a size ceiling. A 200k-message
mailbox with base64 ids and change keys is tens of MB of live map per open
account, held for the whole session. Bounding it (LRU, or eviction on
successful destroy) would be a pure win; the cache is an optimization, and
`refresh_missing_etags` already exists as the cold path.

**O-5 - `discover_memberships_inner` emits one duplicate owner tag per
foreign folder.** `scopes.rs:196`: the `seen` set is keyed on the FOLDER,
so a shared mailbox with 40 folders pushes 40 identical
`MembershipScope::Mailbox(owner)` entries into the returned vec. Harmless
if the engine dedups, wasteful if it does not, and it makes the
`assert_eq!(.. count(), 1)` in the existing test read as a guarantee it is
not.

**O-6 - `describe_cursor` reports every decodable cursor as fresh "now".**
`mod.rs:449`: `freshness: decoded.is_some().then(Instant::now)`. The
payload carries `issued_at_unix_secs`, which is never consulted. Consistent
with `delta_token_expires_after: None` and the "expiry is reactive"
contract, so this is probably intentional - but it means the engine has no
way to prefer a fresh scope over one whose token was minted six weeks ago,
and the recorded issue timestamp has no reader at all. Either wire it into
`freshness` or delete the field.

**O-7 - `close()` does not tear down server-side webhook subscriptions.**
`mod.rs:1093`: `close` cancels the shutdown token and aborts the EWS
worker. Graph subscriptions created by `push_subscribe` are left live on
the server for up to 24h, still POSTing to the consumer's endpoint. On
reopen the factory mints fresh ones, so subscriptions accumulate across
reopen cycles until they expire. The engine may be calling
`push_unsubscribe` first - worth confirming against `reference/sync.md`
rather than assuming.

**O-8 - `api_beta_base` is dead outside tests.** `client.rs`: the field is
stored, cloned through `for_shared_mailbox` / `with_outlook_base`, and read
only by `#[cfg(test)] fn api_beta_base`. Two public constructors
(`with_api_bases`, `with_source`) require callers to supply it. Either wire
the beta endpoint up or drop the parameter; carrying an unused public API
argument that also has the O-1 trap baked in is the worst of both.

**O-9 - The message `$select` never asks for `changeKey`.**
`types.rs:144`: `MESSAGE_SELECT` lists 24 fields, none of them `changeKey`.
The whole `If-Match` chain for inventory/changes/metadata-hydrated messages
therefore rides on `graph_etag`'s `@odata.etag` fallback annotation. That
works (Graph ships the annotation regardless of `$select`) and the value is
a valid `If-Match`, but it is load-bearing and undocumented. Pinned
explicitly by `the_metadata_select_relies_on_the_odata_etag_annotation`
(get.rs) and `the_message_select_carries_every_field_the_projection_reads`
(inventory.rs).

**O-10 - EWS notification-to-scope mapping cannot match a foreign scope.**
`ews_stream.rs:465` (`scope_for_folder`) compares the notification's
`ParentFolderId` against `folder.0` - the ENCODED scope folder id. A
foreign scope's `folder.0` is `mailbox\u{1f}native`, so it can never equal
a native EWS folder id, and every notification for a shared folder degrades
to `HintPayload::Unknown` (an account-wide re-check). Moot while G-1 stands
(the subscription cannot be created at all), but it is the second half of
the same fix. Separately: Graph REST folder ids and EWS `FolderId`s are
related but not byte-identical encodings, so this comparison is suspect
even for the primary mailbox - worth verifying against a live tenant before
trusting the specific-hint path at all.

**O-11 - `message_reactions` has no public-folder guard.**
`reactions.rs:92` builds `message_batch_url(&account, id, ..)` for every id.
A folder-qualified public-folder `ObjectId` (RS-separated) is not decoded by
`parse_message_id` into anything the REST path can address, so it
percent-encodes into a `/me/messages/{folder%1Eitem}` URL and 404s. It
lands on the `failed` lane rather than corrupting anything, so this is a
quality issue, not data loss - but `get_stream` and `message_hydrate` both
learned to partition on `ews_read_folder` and this door did not.

**O-12 - The EWS worker busy-polls at 1 Hz when no scopes are subscribed.**
`ews_stream.rs:77-81`: `if scopes.is_empty() { sleep(1s); continue; }`.
`ensure_ews_worker` is called from `push_stream` as well as
`push_subscribe`, so a consumer that opens a push stream without
subscribing spins a task waking once a second for the life of the account.
A notify/condvar on the subscription map, or simply a longer idle sleep,
would cost nothing.

**O-13 - `check_response_error` never resets `in_error_message`.**
`ews/xml_helpers.rs:197`: the flag is set when any element carries
`ResponseClass="Error"` and is never cleared. Combined with the
"first errored ResponseMessage wins" break, the behavior is correct for
every shape EWS actually emits, but a body whose FIRST message is an error
and whose later messages are successes would still be classified by the
first error - which is the intended reading. Noted only because the flag's
lifetime is not obvious from the code and a future edit could easily
introduce a false positive.

---

## What I did not get to, and why

- **`public_folder.rs` (103KB) got a read-through, not an audit.** It is by
  far the best-tested module in the crate - 30+ tests covering the
  watermark, boundary ids, the cap/degrade transition, the deletion
  reconcile, incomplete walks, and the routing fallbacks - and every
  invariant I probed was already pinned. Weighting my time toward the
  thinner modules was the better trade, but a dedicated pass on the
  incremental-poll/full-scan interaction is still worth someone's time.

- **`calendar.rs` (75KB) and `contacts.rs` (40KB) were checked only for the
  null-as-removal defect class** the brief called out. Both use explicit
  `Option<Value>` sparse-PATCH bodies with `skip_serializing_if` and a
  documented `Some(Value::Null)` = clear convention, and `GraphContactPatch`
  (create) is separate from `GraphContactPatchBody` (update) precisely so
  create cannot emit clears. That class is clean. The recurrence/RRULE
  mapping and the timezone table were not audited.

- **`autodiscover.rs`, `filters.rs`, `cloud.rs`, `groups.rs`,
  `ews/parse.rs`** were read for structure only. `ews/parse.rs` (78KB of
  quick-xml) is the largest unaudited surface in the crate and is where I
  would look next.

- **No transcript-level tests for the `$batch` seam.** The JMAP crate's
  stub-transport model (`crates/jmap/src/core/tests.rs`) works because JMAP
  has a transport trait to substitute. `GraphClient` has no such seam: it
  owns a concrete `bifrost_net::AccountNet` behind
  `Arc<ClientInner>`, every request method goes through the private
  `execute_request`, and there is no injection point short of a new trait or
  a `#[cfg(test)]` response-queue field on `ClientInner`. That is why G-6
  (the `$batch` reconciliation gap) ships as a finding with no regression
  test, and why the mutation and hydration paths are only tested at the
  request-BUILDER level. Introducing a `GraphTransport` trait (or a
  `#[cfg(test)] responses: Mutex<VecDeque<Response>>` on `ClientInner`,
  which is far cheaper) would unlock byte-level tests for `$batch`
  reconciliation, the `Retry-After` throttle path, the `@odata.nextLink`
  walk, and the EWS `GetItem` fan-out in one move. It is a
  `crates/graph/src/client.rs` change and squarely in scope, but it is a
  production-code change and the brief says findings get reported, not
  applied.

- **No `Cargo.toml` changes**, per the brief. Nothing I wrote needs a
  dev-dependency the crate does not already have (`tokio` with the `macros`
  and `rt` features, `futures`, `serde_json` are all in use by existing
  tests).
