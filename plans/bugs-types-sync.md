# Bug hunt: bifrost-types + bifrost-sync (2026-07-28)

Scope: `crates/types/src/**`, `crates/sync/src/**`, `crates/sync/tests/**`.
Every file in scope was read; `reference/error-model.md`, `reference/sync.md`,
and `plans/bug-hunt-2026-06-17.md` were used as the contract baseline. Items
that doc already marks resolved or deliberately-kept are not re-reported.

Tests landed in this pass (behavior pinned as it exists today):

- `crates/sync/tests/envelope_hardening.rs` - byte-level decoder rejection
  matrix (reserved bytes, unknown kind/scope/objtype/protocol tags, the
  reserved `0xFF` tag mapping to `SchemaIncompatible` rather than a plain
  decode error, invalid UTF-8, every strict prefix of both envelope kinds,
  oversized length prefixes, trailing-garbage tolerance, multibyte folder
  ids).
- `crates/sync/tests/attach_schema_recovery.rs` - attach against a store
  that reports a row as `SchemaIncompatible`: the attach survives, the
  unreadable row is deleted, the scope is re-established from the live
  account, and a sibling scope is untouched. Pins the half of the
  schema-clear contract that has no reopen listener to route to.
- `crates/sync/tests/registry_hints.rs` - `CursorRegistry` membership index
  (dedupe, shared-membership pruning on delete, put-replaces), the
  `scopes_for_hint` routing table, and `membership_to_cursor_scope`.
- `crates/sync/tests/checkpoint_store.rs` - `InMemoryCheckpointStore`
  contract: account/scope isolation, replace-on-put, delete semantics, and
  the `get_backfill` "latest by items_done" selection the backfill resume
  logic rides on (ties deliberately not pinned; see O3).
- `crates/sync/tests/control_boundary.rs` - `SyncControl` generation gating
  (stale checkpoints never satisfy a later `pause`/`checkpoint_now`; each
  request needs its own post-request record), boundary compose rules
  (`checkpoint_now` restores Run / restores a pre-existing Pause; `pause`
  stays paused), concurrent-pause preservation, snapshot accessors, and
  canonical watch values after every receiver is dropped. The remaining
  test that documents behavior believed defective is
  `pause_with_no_checkpoint_traffic_parks` (the B5 hang).
- `crates/sync/tests/lane_budget.rs` - `LaneQueue` DropOldest shedding +
  shed counter + capacity floor; `ConcurrencyBudget` permit arithmetic,
  clamping, and `validate` rejection of zero fields. The
  `sync_permits() == 0` pin documents the masking noted in O4.
- `crates/sync/tests/convenience_dispatch.rs` - the `Account` trait's
  convenience dispatch table via a recorder stub: `set_starred` across all
  four `StarredFlagShape`s (including the `"$flagged"` Category sentinel
  Graph's `is_starred_category` depends on), `mark_replied` /
  `mark_forwarded` / `mark_mdn_sent` capability dispatch and precedence,
  the `apply_label` / `remove_label` provenance matrix (Gmail engine-id vs
  Graph/JMAP native-id is load-bearing and was untested), `set_read`
  aliasing, autocomplete limit threading, the multi-call convenience
  defaults, and `inventory_partition_stream`'s Full-vs-rejected contract.

## Bugs

### B4. Account reopen: no re-discovery, dead lifecycle stream, leaked old handle

`crates/sync/src/engine.rs` `restart_account` (~line 3501) and
`SyncEngine::reopen` (~line 805); `crates/sync/src/multiplexer/mod.rs`
lifecycle task (~line 196).

Three related defects around `EngineDirective::RestartAccount`:

1. **No re-discovery.** `restart_account` opens the new handle, reapplies
   priority/bandwidth, stores it, and returns. It never re-runs
   `discover_cursor_scopes`, `discover_memberships`, or `push_subscribe`.
   `reference/sync.md` explicitly claims the opposite ("account reopen
   already re-runs discover_cursor_scopes / discover_memberships /
   push_subscribe" - the stated justification for removing the
   `CapabilityChanged` directive). Since capability change IS the canonical
   `RestartAccount` trigger, the one flow that most needs fresh discovery
   (a scope appearing or vanishing on the new handle) never gets it: new
   scopes are never established, and the membership index keeps routing
   push hints against the pre-reopen topology.
2. **Lifecycle stream dies permanently after the first reopen.** The
   multiplexer's lifecycle task captures
   `lifecycle_account.load_full()` ONCE, before its loop, and `return`s
   when that stream ends. After a reopen swaps the handle, the old
   account's `scope_lifecycle_stream` ends (or worse, keeps polling a dead
   connection) and the task exits; nothing respawns it. From then on,
   folder create/rename/delete events are invisible for the rest of the
   session - on every protocol, not just IMAP (whose no-lifecycle behavior
   is the documented exception). The inline comment in `restart_account`
   ("discovery runs ... by way of the multiplexer's lifecycle stream")
   relies on exactly the mechanism this kills.
3. **The old handle is never closed.** `ctx.current.store(Arc::new(next))`
   drops the previous `Arc<dyn Account>` (once workers release clones)
   without ever calling `close()`. For IMAP that skips LOGOUT + pool
   drain; for JMAP it strands the WebSocket. One leaked connection per
   reopen, and reopen loops are exactly where connections are already
   scarce. Same in the public `SyncEngine::reopen`.

Proposed fix: make reopen a real re-attach of the slot internals: close the
old handle (best-effort), re-run scope discovery + establishment +
membership linking against the new handle, and either respawn the lifecycle
task or restructure it to reload `load_full()` and resubscribe whenever its
current stream ends (with a backoff so a dead handle does not spin).

### B5. `pause()` / `checkpoint_now()` hang forever on an idle account

`crates/sync/src/control.rs` + the ack-writer contract
(`crates/sync/src/engine.rs::ack_writer`).

`record_checkpoint` fires only from the ack writer, after a consumer ack.
Path to failure: consumer calls `pause()` on a quiet account (no change
traffic). The boundary flips to `Pause` immediately, so every poll loop
parks before producing a batch; no batch means no consumer ack, no ack
means no `record_checkpoint`, so the `pause()` future never resolves. The
same happens to `checkpoint_now()` whenever there is no in-flight batch at
request time, and to a busy account whose consumer stops acking after
requesting the pause (persist-then-ack ordering makes this easy to hit).
This is a self-deadlock: the very act of pausing prevents the event that
would complete the pause. Pinned (as current behavior, explicitly not
endorsed) by `control_boundary.rs::pause_with_no_checkpoint_traffic_parks`.

Proposed fix direction: the waiter needs a second completion source -
"already at a safe boundary". E.g. have workers report quiescence (parked
at Pause with no batch in flight) and let `wait_for_checkpoint_at_or_after`
resolve with the last recorded checkpoint (or a synthetic
no-checkpoint-yet marker) once all workers for the account are parked.
Alternatively document loudly that `pause()` resolves only after the next
consumer-acked batch and provide a non-waiting `pause_hint()`.

### B8. Per-item `Engine(_)` mutation failures never reach the reopen listener

`crates/sync/src/engine.rs`, `classify_item_outcome` (~line 3790).

Both the `bulk_set_flags` doc comment and `reference/sync.md` state that an
`Engine(_)` recovery increments `blocked_by_engine` AND forwards the
directive through `ReopenRequest::Recovery`. The code does that only for a
stream-level `SyncEvent::Terminated`; a per-item `ItemOutcome::Failed`
whose recovery is `Engine(RestartScope(...))` (a protocol reporting cursor
invalidation against one item without terminating the batch) only sets the
counter. The directive is dropped - no restart, no schema clear - and the
consumer sees `blocked_by_engine > 0` with an engine that took no action.
Either forward the directive from `classify_item_outcome` (it would need
the reopen sender threaded in) or fix the two docs to say stream-level
only. Today's protocol crates likely terminate the stream alongside, which
is why this has not bitten; it is a contract hole, not an observed failure.

### B10. Push overflow coalescing demotes `Terminated` / `Warning` to a plain wakeup

`crates/sync/src/push/mod.rs`, `InvalidationSinkInner::push` +
`coalesced_event` (~lines 94-137), and the identical path in the engine's
push forwarder.

When the per-account watch channel is full, ANY rejected event - including
`WatchEvent::Terminated(auth-lost)` and `WatchEvent::Warning` - is
replaced by an `Invalidated { Unknown }` reconcile request. The terminal
classification is lost at the moment the account is busiest; the engine
only learns about the auth loss later, when the reconcile's
`changes_stream` fails on its own. Secondary issue in the same function:
if `tokio::runtime::Handle::try_current()` fails (a consumer's Pub/Sub or
webhook receiver thread outside any tokio runtime - the exact callers the
`InvalidationSink` exists for), the coalesced wakeup is dropped entirely
with only a counter increment. Proposed: preserve `Terminated` by
prioritizing it (e.g. try_send the coalesced Unknown first, but always
deliver Terminated via the blocking-send task without the 100 ms cap), and
document the runtime requirement on `InvalidationSink::push` or capture a
runtime handle at engine construction.

### B11. Reopen listener serializes recovery behind inline `Retry` sleeps

`crates/sync/src/engine.rs`, `handle_account_error` `Retry` arm +
`handle_retry` (~line 3175).

The reopen listener processes `ReopenRequest`s one at a time. A
`RecoveryPlan::Retry` reaching it (e.g. a `scope_lifecycle_stream`
terminating with a retryable class, or a mutation campaign forwarding one)
sleeps for the full `retry_delay` - potentially a provider `Retry-After`
of minutes - ON the listener task, then does nothing else (there is no
re-run of whatever failed). Meanwhile the channel (capacity 16) backs up:
a real `RestartScope` or `SchemaIncompatible` raised during the sleep
waits behind it, and once 16 requests queue, workers block on
`reopen_tx.send(...)`. Proposed: never sleep on the listener - for Retry
verdicts either spawn the delayed action or drop it with a structured log
(the current sleep provides zero recovery value since nothing is retried
afterward).

### B12. Lifecycle `Created`/`Renamed` synthesizes folder cursor scopes on type-cursor protocols

`crates/sync/src/multiplexer/mod.rs`, `membership_to_cursor_scope`
(~line 721) + the lifecycle `Created` arm.

`MembershipScope::Mailbox(id)` is mapped to
`CursorScope::Folder(FolderId(id))` unconditionally. On a protocol whose
cursors are type-scoped (JMAP: `Type(Email)` already covers every
mailbox), a mailbox-created lifecycle event synthesizes a
`RestartScope(Folder(mailbox-id))` recovery, and the engine dutifully
calls `establish_initial_cursor` for a folder scope the protocol never
advertised in `discover_cursor_scopes`. Depending on the protocol's
handling that either errors on every new-mailbox event (backoff loop +
operator warning for a non-problem) or establishes a rogue per-folder
cursor that double-syncs mail already covered by the type cursor. The
mapping needs to consult the registered scope shapes (if an account-wide
or type cursor exists, a new mailbox needs no establishment at all - at
most a membership re-link). Pinned as-is in `registry_hints.rs` with a
pointer here.

### B13. Failed `attach` and `detach`-during-teardown never close the opened account

`crates/sync/src/engine.rs`, `attach_inner`.

If `attach_inner` fails after `factory.open` succeeds (scope discovery
error, non-scope-local establishment failure, membership discovery
error), the opened `Arc<dyn Account>` is dropped without `close()` -
whatever the protocol crate spun up at open (IMAP connection pool, JMAP
WebSocket, background workers) is orphaned until its own internals notice.
Cheap fix: on every early-error path after `open`, call
`opened.close().await` best-effort before returning.

## Doc-vs-code contradictions (beyond those folded into bugs above)

- **D1.** `reference/sync.md` push section: "`Reconciler::warning_event`
  synthesizes a MultiplexerEvent carrying
  `WarningKind::Other("push:disconnected" | "push:reconnected")` and
  Reconnected additionally triggers a full reconcile". Code: `WarningKind::
  Other` is a unit variant (carries no string); the Disconnected warning
  message is "push transport disconnected"; and `Reconnected` emits NO
  warning at all - it only reconciles (`push/reconciler.rs` ~line 88).
- **D2.** `multiplexer/fusion.rs` module header says inventory batches are
  forwarded "as a `ScopeChange::Added` membership signal"; the code
  forwards `ObjectChange::Created` (and drops memberships entirely).
- **D3.** `reference/sync.md` says the multiplexer drives "in-process push
  ... on the most-active cursor scope". Nothing does: `IdleHolder` is
  never constructed anywhere (dead code; only the re-export references
  it), `Multiplexer.poll` and `Multiplexer.watch_tx` are destructured into
  `_` in `run`, and in-process push is drained scope-agnostically by the
  engine's push-forwarder task.
- **D4.** `reference/sync.md` presents `LaneShedPolicy::DropNewest` as a
  configurable alternative; no constructor or config field can select it -
  `LaneQueue::with_capacity` hardcodes `LaneShedPolicy::default()`.
- **D5.** `SyncEngine::detach` doc says it "awaits the in-flight
  Batch-with-checkpoint via the safe-boundary primitive"; the body sends
  `Stop` and then IMMEDIATELY cancels the shutdown token (deliberately,
  per its inline comment), so workers parked on the token exit without
  waiting for a boundary. The method doc and inline comment disagree;
  the inline one matches the code.
- **D8.** `reference/sync.md`'s hydration section reads as though
  `InventoryEntry` (fingerprint, threading headers, memberships) reaches
  the consumer on the broadcast stream. It does not, anywhere: both
  `InventoryFusion::forward_inventory_batch` and
  `BackfillRunner::run_partition` down-convert every inventory entry to a
  bare `Change::ObjectChange { id, Created }`. The fingerprint /
  membership / thread data the protocol already fetched is discarded, and
  a consumer must re-fetch it per id via `get_stream(Metadata)` - doubling
  the wire cost of cold start. Worth an explicit design decision: either
  extend `MultiplexerEvent` (or a sibling event) to carry inventory
  entries, or make the docs state plainly that cold-start signals are
  id-only.
- **D9.** There is no `reference/types.md`. `bifrost-types` is the contract
  crate every other crate implements against - `account.rs` alone is 1113
  lines of two-tier trait surface with real dispatch logic in the default
  impls - and it is the only crate without a reference doc. The error
  model has `error-model.md`; the other 20 files of the crate (Account
  trait tiers, capabilities semantics, cursor-vs-membership scope split,
  event vocabulary, container/label provenance rules) have nothing.

## Observations, smells, latent traps

- **O1. Reconciler `Reconnected` hint source is hardcoded
  `PushSource::JmapStateChange`** for every protocol
  (`push/reconciler.rs` ~line 91) - telemetry mislabels IMAP/Graph/EWS
  reconnect reconciles as JMAP.
- **O2. `InventoryFusion.store` field is dead** - constructed and cloned
  at both call sites, never read. Either the durable write it implies is
  missing or the field should go.
- **O3. `get_backfill` "latest" ties are nondeterministic** in the
  in-memory store (strictly-greater comparison over HashMap iteration
  order). For an open-pages walk where several full windows share
  `items_done == chunk`, resume may restart from an EARLIER window than
  the furthest acked one - harmless (idempotent re-walk) but wasteful,
  and any consumer store that copies the trait comment inherits the same
  ambiguity. Related latent trap: the per-partition checkpoint's
  `items_done` is the POST-filter `kept_total`
  (`backfill/runner.rs` ~line 265) while `open_pages_resume` treats
  `items_done < width` as "inventory exhausted inside this window; skip".
  Today kept == seen because `LiveSupersedes` is never populated; the day
  a producer populates it, a fully-populated window with filtered entries
  reads as a short page and cold-start silently skips the remainder of
  the scope. If the supersedes set ever gains a producer, the checkpoint
  must switch to pre-filter counts (or record both).
- **O4. `ConcurrencyBudget::sync_permits()` can be 0** (`per_account = 1`)
  and `BudgetGate::register` masks it with `.max(1)`, quietly granting
  `per_account + 1` effective permits. Pinned in `lane_budget.rs`.
  `validate()` could reject `per_account == 1` or the accessors could
  guarantee a nonzero split.
- **O5. `AccountSlot` carries ten `#[allow(dead_code)]` fields**
  (`types.rs`) - capabilities, backfill/push/mutation handles, cursors,
  checkpoints, priority channels, throttles. Some are genuinely
  future-wiring (throttles is tracked as sync-F2), but the volume
  suggests the slot shape and the actual wiring have drifted. The
  `priority_rx` and `bandwidth_cap_rx` fields are receiver keepalives,
  but canonical snapshots no longer depend on them.
- **O6. Thread-convenience defaults reuse bulk operation tags** -
  `move_thread` reports `Unsupported(BulkMove)` and `delete_thread`
  `Unsupported(BulkDestroy)` (`types/src/account.rs`); there is no
  `MoveThread`/`DeleteThread` `AccountOperation`, so telemetry attributes
  thread-level refusals to the bulk surface. Pinned as-is in
  `convenience_dispatch.rs`.
- **O7. The push forwarder reconnect loop** (`engine.rs` attach, ~line
  446) re-creates `push_stream` every 50 ms if the stream ends
  immediately. For out-of-process-push accounts whose `push_stream`
  yields nothing and closes, that is 20 stream constructions per second
  for the session. Protocol crates currently return pending-forever
  streams, so latent; a backoff (or gating the forwarder on
  `push_in_process()` plus health-only expectations) would remove the
  trap.
- **O8. `warning_event` / terminal broadcasts pick an arbitrary scope** -
  `cursors.all_scopes().into_iter().next()` (reconciler ~line 129) tags
  push warnings/terminations with whatever scope HashMap iteration
  yields first. Consumers filtering events by scope will attribute
  account-wide push failures to a random folder. `CursorScope::Account`
  as the deliberate account-wide marker (as `broadcast_warning` uses)
  would be honest.
- **O10. `run_backfill_orchestrator` snapshots `cursors.all_scopes()`
  once**, after the subscriber wait. Deferred-inventory scopes that
  establish later (the fusion worker runs concurrently) are absent from
  the snapshot, so they get no partitioned backfill pass this session.
  Benign today (fusion itself broadcast the cold-start data), but the
  orchestrator and fusion racing on who covers a scope is implicit; a
  comment or an explicit scope handoff would prevent someone "fixing"
  the missing scopes into a double walk.
- **O11. Cross-crate conformance tests never poll the attach future**
  (`cross_crate_conformance.rs` constructs and drops it). Fine as a
  type-level witness, but under the new hermeticity policy an in-memory
  duplex transcript could now drive one real attach end-to-end for at
  least one protocol; noted as an opportunity, not a defect.
- **O12. Oversized cursor-envelope fields corrupt their length prefix.**
  `pack_envelope` / `write_bytes` use
  `u32::try_from(len).unwrap_or(u32::MAX)`, which for a payload larger
  than 4 GiB writes a false length prefix instead of failing. This is
  unreachable in practice but remains the codec's one silent-corruption
  path.

## What I did not get to

- **Engine-level integration tests with a full stub account** (attach ->
  broadcast -> ack -> checkpoint persistence; the backfill
  consumer-ack-completion loop; the disable-scope quarantine driven
  through a real `ReopenRequest`). `attach_schema_recovery.rs` now covers
  the establish-loop half of this with a full stub plus a custom
  `CheckpointStore`, but the ack / broadcast / backfill-completion
  sequencing is still unpinned: each such test is a ~700-line stub plus
  careful subscriber sequencing, and pure logic with zero coverage came
  first. The five stub accounts across the suite are now near-identical
  boilerplate; a shared `tests/common` module would pay for itself before
  the sixth.
- **A full-engine reproduction test for B5** beyond the control-level
  pin, which wants either a quiescence seam in `SyncControl` or the full
  engine harness above.
- **Verifying B12's blast radius inside the JMAP crate** (what
  `establish_initial_cursor(Folder(...))` actually does there) - outside
  my file scope; flagged for whoever owns `crates/jmap`.
