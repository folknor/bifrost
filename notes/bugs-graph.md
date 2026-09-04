# Bug hunt: bifrost-graph

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/graph/` - delta-token sync, webhook push with EWS fallback, cursor
envelope, `If-Match` mutations, error mapping. Core (lifecycle, push, sync
streams, mutation, error boundary, client funnel, codecs) read in full, rest
spot-checked; findings verified against source lines and `reference/graph.md` /
`reference/sync.md`.

## Confident defects

(Finding 1 - `changes_stream` silently dropping an id-less delta value while
still emitting the page's checkpoint - is fixed: an id-less value (removed or
not) now delivers the page's decoded siblings on a checkpoint-less batch and
terminates as `Protocol(ContractViolation)`, mirroring the neither-link arm,
so the cursor never crosses the page. Pinned by
`an_idless_delta_value_terminates_without_crossing_the_page` with
revert-and-confirm; the rule is documented for the changes lane in
`reference/graph.md`.)

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
