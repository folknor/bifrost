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
