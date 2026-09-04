# Bug hunt: bifrost-graph

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/graph/` - delta-token sync, webhook push with EWS fallback, cursor
envelope, `If-Match` mutations, error mapping. Core (lifecycle, push, sync
streams, mutation, error boundary, client funnel, codecs) read in full, rest
spot-checked; findings verified against source lines and `reference/graph.md` /
`reference/sync.md`.

## Confident defects

### 1. `changes_stream` silently drops an id-less delta value and advances the cursor - the exact "silent permanent loss" the inventory walk was hardened against

`crates/graph/src/account/changes.rs`, page loop (~lines 145-184). A
non-removed value without a usable `id` matches neither the `is_removed` branch
nor the `if let Some(id)` branch, so it contributes nothing, and the page's
checkpoint (`advanced_through` / new `delta_link`) is still emitted, crossing
the page. `inventory.rs` (lines 151-194) treats the identical condition as a
`Region` obligation with `RegionRecovery::CheckpointBarrier` - with an
extensive comment explaining why silently dropping is a checkpoint-contract
violation - and `reference/graph.md` ("An id-less delta value is a checkpoint
barrier") states the rule for "a delta page" generically. The changes lane has
no obligation vocabulary in `SyncEvent<Change>`, but the honest fallback exists
in the same file: terminate before emitting the checkpoint (as the
neither-link arm at lines 229-247 does), so the cursor never crosses the page.
As written, a malformed value in a delta page after initial sync is
unrecoverable, unreported loss for that scope.

### 2. Bad move destination misclassified as a terminal provider fault

`crates/graph/src/account/mutate.rs` lines 206-230. When
`request_for_mutation` returns `Ok(None)` for a `Move` (destination is not a
folder, or a cross-mailbox move), the outcome is built with
`protocol_violation(ProtocolErrorKind::ContractViolation, ...)` - i.e.
`Protocol(ContractViolation)` with `Cause::Wire(MalformedResponse)`, which per
the crate's own documentation (graph_error.rs lines 1187-1189) derives
terminal `ProviderContractViolation`. The comment directly above (lines
214-217) says "Classify as `Request(Malformed)` so recovery routes to
`ClientBug` rather than the misleading `Unsupported(BulkMove)` shape", and
`reference/graph.md` says `bulk_move` "rejects a cross-mailbox move ... as
`Request(Malformed)`". The code matches neither: it blames Microsoft (wrong
provider attribution in telemetry, and possibly wrong engine recovery) for a
caller-side malformed request, and it fabricates a `Cause::Wire(MalformedResponse)`
for a failure raised before any wire traffic. Three-way disagreement between
comment, reference, and code; the code is the odd one out.

### 3. `reference/graph.md` claims `push_subscribe` without a webhook endpoint returns `Error::MissingCoreCapability`; the code returns `Unsupported(PushSubscribe)`

`push.rs` `subscribe_graph` (line 260-261) returns `unsupported_push_error()`;
`grep MissingCoreCapability crates/graph/src` finds nothing. The reference
makes the claim twice (factory section and "Known limitations"). One of them
must change; since `reference/` is binding, this is a live doc defect, and
possibly a behavioral one if bifrost-sync branches on the kind.

## Suspected defects (verify against the engine)

### 4. `get_stream` can only hydrate messages, but Graph establishes event and contact cursor scopes

`get.rs` `hydrate_url_for_id` (lines 596-605) always builds
`{prefix}/messages/{id}`, and `select_for_projection` returns message-shaped
`$select` lists. Event/contact scopes produce inventory entries and change ids
through the same generic pipeline; if the engine hydrates those object ids
through `get_stream` (its generic path), every event and contact hydrates as a
per-item 404 (`/me/messages/{eventId}`). If the engine instead never calls
`get_stream` for non-email scopes, this is fine but undocumented - neither
`reference/graph.md` nor the code says which. Worth pinning either way.

### 5. Mutation batches never carry `PageBoundary::Final`

`mutate.rs` `submit_batch` emits every batch as `PageBoundary::Page` and ends
with `Done(None)`. `get.rs` was explicitly reworked (lines 30-36) because "a
hydration that fired chunks tagged only `Page` never told the engine where the
stream terminated", matching inventory/changes. The mutation stream has
exactly the shape that comment calls a bug. `reference/sync.md` doesn't
obviously require it for mutation lanes, so this may be benign - but the
asymmetry with the hydration fix is unexplained.

## Smells / minor findings

- **`changes.rs` page-marker `last_seen_id` is the bare Graph id** (captured at
  line 146 before foreign encoding), while inventory's marker records the
  encoded id (`entries.last`, line 222). Also captured from `@removed` values
  in changes but only from surviving entries in inventory. Harmless today
  (resume uses `next_link` only) but the two walks disagree on what the field
  means.
- **`is_expiring_soon` warns on every renewal tick for an unparseable expiry** -
  the comment itself (`webhooks.rs` lines 160-167) says it "should be logged
  once per subscription, not once per tick". Known, unfixed.
- **`run_get_events_loop`'s `Resubscribe` arm has no backoff** - deliberately
  flagged in-code (`ews_stream.rs` lines 147-154) as accepted: a server that
  answers every long poll with a resubscribe directive spins full round-trips.
  The hunter agrees it's defensible; noting it stays a documented spin.
- **`attach_account` swallows a poisoned `RwLock`** (`client.rs` line 466-469:
  `Err(_) => None`) - a poisoned lock silently skips the detach of the
  displaced handle, leaking one meter attachment. Vanishingly unlikely
  (nothing panics while holding it), but the leak the surrounding comment
  exists to prevent.
- **`open` lists mail folders twice per attach** - `GraphAccountFactory::open`
  runs `list_mail_folders_recursive` to seed the folder tree, and
  `discover_cursor_scopes_inner` runs it again (and re-seeds the same tree)
  when the engine calls discovery moments later. One recursive folder walk per
  open is pure duplicate traffic.
- **`message_reactions` and `get_stream` `$batch` POSTs always go through the
  primary client** while subrequest URLs carry `/users/{owner}` prefixes -
  correct against Graph's account-global `/$batch`, but it means the routing
  evidence the reference praises for per-message writes ("the URL IS the
  routing evidence") is subrequest-level only here; fine, just worth knowing
  when writing tests.

## Structural observations (pre-1.0, unlimited-resources posture)

- **`pim.rs` at 4,825 lines is the crate's dumping ground** - search (plus its
  cursor codec), drafts/send/scheduled-send, containers, identities, vacation,
  thread doors, trash cache, typed hydration. The thread/trash owner-routing
  logic and the search walk are each intricate enough to deserve their own
  modules; the current file makes the reference's per-topic narratives the only
  navigable map.
- The per-item lane discipline (`batch_routing`, `reconcile_*`,
  `BatchOutcomeBuilder`) is now implemented four times with slight variations
  (get, mutate, reactions, push). `resolve_batch_responses` /
  `reconcile_hydration_responses` duplicate the invalid/duplicate-index rules;
  a single validated `$batch` projection type would pin the rule once. Finding
  2 is precisely the kind of drift this duplication invites.
- Everything else read - the worker-slot lifecycle, teardown/renewal race
  handling, the EWS frame decoder, the cursor envelope, the etag LRU, the
  wire-level test seam - is unusually carefully built and densely test-pinned;
  no unsound concurrency or cancellation behavior found beyond what's flagged
  above.

Files with findings: `crates/graph/src/account/changes.rs`,
`crates/graph/src/account/mutate.rs`, `crates/graph/src/account/get.rs`,
`crates/graph/src/account/push.rs`, `crates/graph/src/webhooks.rs`,
`crates/graph/src/client.rs`, `reference/graph.md`.
