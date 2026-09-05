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

(Finding 4 - `get_stream` hydrating only messages while Graph also establishes
event and contact cursor scopes - is answered, and the answer is that the
routing is correct: `bifrost-sync` never routes event or contact scope ids into
`get_stream`. Its only internal callers are the mutation read-back paths, which
hydrate ids the caller just put through the mail mutation API;
`SyncEngine::get_stream` is otherwise a consumer-facing passthrough, and the
standalone calendar/contact account crates (caldav, carddav) answer the same
trait method with `unsupported_stream(Hydrate)`. Calendar events and contacts
are read through their own typed surfaces (`calendar::get` by `EventId`, the
contacts API), which address `/events/{id}` and `/contacts/{id}` themselves. An
`ObjectId` carries no type marker, so this lane could not route by scope kind in
any case. Pinned by
`every_hydration_projection_selects_message_fields_on_a_messages_url`
(revert-and-confirmed: adding a non-mail `$select` field fails it) and stated in
`reference/graph.md` plus the `hydrate_url_for_id` doc comment, so the next
reader does not re-derive it.)

(Finding 5 - mutation batches never carrying `PageBoundary::Final` - is
answered: the engine does not need it. `bifrost-sync`'s mutation consumer loops
on `stream.next()` and matches only `Batch` / `Terminated`, treating the stream
ending as the terminus; it never reads `page_boundary` on this lane, and a
mutation batch carries `checkpoint: None` so there is no cursor for a `Final`
tag to close. The hydration lane differs because its reader has to know which
chunk closes the stream before the stream ends. No code change; the rule is
stated in `reference/graph.md` and in `bulk_mutation_stream`'s doc comment, and
pinned by `mutation_batches_are_paged_and_the_stream_end_is_the_terminus`
(revert-and-confirmed by tagging the batch `Final`).)

## Smells / minor findings

- (The **double folder listing on `open`** is fixed: `open`'s
  `list_mail_folders_recursive` result is handed forward in a one-shot
  `open_folder_seed` slot, and the first `discover_cursor_scopes_inner` consumes
  it instead of walking the hierarchy again. The slot is one-shot so a discovery
  re-run still lists for real - noticing folders created since open is what a
  second discovery is for. Pinned by
  `discovery_reuses_the_listing_open_seeded_and_lists_again_on_re_run`,
  revert-and-confirmed; documented in `reference/graph.md`.)
- **`message_reactions` and `get_stream` `$batch` POSTs always go through the
  primary client** while subrequest URLs carry `/users/{owner}` prefixes -
  correct against Graph's account-global `/$batch`, but it means the routing
  evidence the reference praises for per-message writes ("the URL IS the
  routing evidence") is subrequest-level only here; fine, just worth knowing
  when writing tests.

## Structural observations (pre-1.0, unlimited-resources posture)

- (**`pim.rs` at 4,825 lines as the crate's dumping ground** - DONE: split into
  `account/pim/` as a pure move, no behaviour change. `mod.rs` declares the
  modules and re-exports the `pub(crate)` doors the rest of `account/` calls;
  everything else is `pub(super)` inside `pim`. `messages.rs` (per-message
  writes plus the shared `$batch` write pipeline), `send.rs` (send / send-as /
  scheduled-send handle codec), `drafts.rs`, `search.rs` (the KQL and OData
  builders, the shared-mailbox walk, the versioned opaque search cursor),
  `containers.rs` (container CRUD across the three namespaces, well-known
  roles, `trash_container_id` and its per-mailbox cache), `identities.rs`
  (identity snapshot, vacation), `threads.rs` (`thread_hydrate` / `move_thread`
  / `delete_thread` and their owner routing), `hydrate.rs` (typed hydration and
  the Graph-JSON / EWS projections onto `Message`), `common.rs` (only the
  helpers two or more of them share), and `tests.rs` kept as one module so the
  suite stays flat. The reference's module map now names each topic, so the
  thread owner-routing and the search walk are findable by module name. The
  four `$batch` lane-discipline implementations were moved untouched; their
  unification is the separate item below.)
- (The per-item lane discipline (`batch_routing`, `reconcile_*`,
  `BatchOutcomeBuilder`) implemented four times with slight variations
  (get, mutate, reactions, push), with `resolve_batch_responses` /
  `reconcile_hydration_responses` duplicating the invalid/duplicate-index
  rules - NOT APPROVED, ruling 2 in `notes/todo.md`: one recorded drift is
  thin evidence for restructuring four working lanes. Re-raise if a future
  hunt finds a second drift between them.)
- Everything else read - the worker-slot lifecycle, teardown/renewal race
  handling, the EWS frame decoder, the cursor envelope, the etag LRU, the
  wire-level test seam - is unusually carefully built and densely test-pinned;
  no unsound concurrency or cancellation behavior found beyond what's flagged
  above.

Files with findings: `crates/graph/src/account/changes.rs`,
`crates/graph/src/account/mutate.rs`, `crates/graph/src/account/get.rs`,
`crates/graph/src/account/push.rs`, `crates/graph/src/webhooks.rs`,
`crates/graph/src/client.rs`, `reference/graph.md`.
