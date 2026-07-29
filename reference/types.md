# bifrost-types reference

`bifrost-types` is the protocol-neutral contract crate shared by every
account implementation and by `bifrost-sync`. It contains no transport
and no engine. Protocol crates implement `Account` and
`AccountFactory`; consumers program against the types in this crate.

The shared structured error contract is documented separately in
`reference/error-model.md`.

## Async object-safe surface

`AccountFuture<T>` and `AccountStream<T>` are boxed, `Send`, and
`'static`. `Account` and `AccountFactory` are object-safe and used as
`Arc<dyn Account>` / `Arc<dyn AccountFactory>`. The workspace is
async-only.

`AccountFactory::open(account_id)` creates one live account handle.
`Account::close()` is idempotent local teardown. It closes protocol
connections and workers but does not delete durable server-side push
subscriptions, which use `push_unsubscribe`.

## Account trait tiers

The required protocol surface in `account.rs` covers:

- capability and scheduling controls;
- cursor-scope and membership discovery;
- lifecycle, inventory, partitioned inventory, and changes streams;
- hydration and blob access;
- push subscription CRUD and the in-process push stream;
- bulk mutation primitives;
- compose, draft, search, container, filter, settings, contact,
  directory, and calendar operations;
- typed message and thread hydration;
- graceful close.

Default implementations form the convenience tier. They either:

- dispatch through capability shapes, such as `set_starred`,
  `mark_replied`, `mark_forwarded`, and `mark_mdn_sent`;
- dispatch by identifier provenance, such as `apply_label` and
  `remove_label`;
- compose aliases, such as `set_read` over `set_is_read`;
- adapt a broad request to a narrower result, such as autocomplete;
- return a structured `Unsupported(AccountOperation)` when a safe
  object-safe default cannot compose multiple async calls.

`move_thread` and `delete_thread` are in the last group and report
`AccountOperation::MoveThread` and `AccountOperation::DeleteThread`.
Protocol crates with an internal `Arc<Self>` handle override them.

## Capabilities

`AccountCapabilities` is the declarative dispatch contract. Important
fields include:

- `cursor_freshness`;
- `push: PushCapability`;
- mutation concurrency and replay safety;
- batching and rate-limit hints;
- blob-range support;
- protocol expiry hints;
- `pim_methods` for optional compose, contact, calendar, filter, and
  settings methods;
- `filter_rule_shape`;
- `conveniences`, including starred, replied, forwarded, and MDN
  dispatch shapes;
- foreign namespace advertisement.

Consumers should inspect capabilities before presenting an operation.
An implementation must still return a structured error if an advertised
operation fails or support changes at runtime.

## Cursor scopes and memberships

`CursorScope` answers where change state advances:

- Gmail: `Account`;
- JMAP: commonly `Type(ObjectType)` or `Query(QueryId)`;
- IMAP: `Folder(FolderId)`;
- Graph: `FolderType { folder, ty }`.

`MembershipScope` answers where an object belongs:
`Folder`, `Mailbox`, `Label`, or `Query`. The two enums must not be
collapsed. One account-wide cursor can cover many memberships, and one
object can have several memberships.

`ChangeCursor` combines a scope, protocol-tagged opaque server state,
an optional progress marker, and an outer envelope version.
`CursorEstablishment` is either `Ready(ChangeCursor)` or
`EstablishViaInventory`. In the latter case the terminal inventory
`Done` event carries the new change checkpoint.

## Stream vocabulary

`SyncEvent<T>` is the common stream protocol:

- `Batch(Batch<T>)`;
- `Progress`;
- `Warning`;
- `Done(Option<Checkpoint>)`;
- `Terminated(AccountError)`.

A batch carries items, page-boundary metadata, wire observations, and
an optional checkpoint. The item set and checkpoint are one consumer
transaction. A checkpoint must not be persisted unless the matching
items were persisted.

`ScopeLifecycleEvent` carries created, renamed, and deleted membership
events or a structured termination. `WatchEvent` carries invalidation,
connection-health, warning, and termination signals. `InvalidationSink`
is synchronous so an out-of-process webhook or Pub/Sub receiver can
feed the engine from a non-async thread.

`Control::pause` and `Control::checkpoint_now` return
`Result<Option<Checkpoint>, AccountError>`. `None` means the stream is
at a safe boundary but has never produced a durable checkpoint.

## Mutation outcomes

Batch and streaming mutations use the same closed three-lane model:

- succeeded;
- failed;
- uncertain.

`BatchOutcomeBuilder::finalize` enforces that every submitted
`BatchItemId` appears exactly once. A whole-operation `Err` means
nothing was transmitted. `Ok(BatchOutcome)` means every item is
accounted for. `ItemOutcome::Uncertain` means the write may have landed
and must be read back rather than replayed blindly.

`AccountOperation::is_idempotent` is the authoritative retry input for
the error model. Absolute-state writes are idempotent. Sends, creates,
moves, destroys, uploads, and other side-effecting writers are not.

## Provenance and container vocabulary

Identifiers are typed newtypes. `Container`, `Label`, and related
values preserve provider-native provenance so convenience defaults can
choose the correct primitive:

- Gmail engine labels route through label membership;
- Graph categories route through category mutation;
- JMAP and Graph native container ids route through container
  membership;
- keyword-like shapes route through keyword mutation.

The `"$flagged"` category sentinel is reserved by the convenience
dispatch contract for providers whose starred state is represented by
a category.

## Hydration and blobs

`Projection` controls bulk `get_stream` hydration. Typed
`HydrationProjection` drives single-message hydration.
`HydratedObject`, `Message`, and `ThreadHydration` keep parsed content
separate from inventory and change signals.

Blob methods return byte streams. `open_blob_range` is only usable when
advertised by `blob_range`; `open_raw_rfc822` returns verbatim
server-assembled message bytes.

## File map

```
crates/types/src/
  lib.rs              public module and re-export surface
  account.rs          Account, AccountFactory, convenience defaults
  capabilities.rs     declarative feature and dispatch shapes
  cursor.rs           cursor scope, membership scope, cursor state
  events.rs           SyncEvent, Batch, Checkpoint, Control, push hints
  mutation.rs         mutation targets, flags, labels, fingerprints
  hydration.rs        projections and hydrated object vocabulary
  blob.rs             blob handles, byte ranges, range support
  container.rs        containers, labels, roles, provenance
  compose.rs          send and draft request vocabulary
  account_compose.rs  compose validation and helpers
  search.rs           message search requests and results
  filter.rs           server-filter model
  settings.rs         identities, vacation, quota
  contact.rs          contacts and address books
  directory.rs        directory search and groups
  calendar.rs         calendars and events
  cloud.rs            hosted attachment vocabulary
  mime.rs             MIME-facing shared values
  ids.rs              typed identifiers
  page.rs             page helpers
  error/              structured error, recovery, batch, warning model
```
