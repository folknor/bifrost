# bifrost-jmap pre-1.0 API redesign (archived)

Status: archived ADR. The redesign described here has shipped. The
current architecture is documented in `reference/jmap.md`; this file
exists to record *why* the surface looks the way it does and to track
the remaining deliberate gaps.

Do not edit this document as a live plan. Code changes update
`reference/jmap.md`. New design proposals get their own file.

## What shipped

### Account scoping (B++ protocol layer + curated facade)

`Account<Tr>` is owned, `Arc`-clone-cheap, and capability-aware. The
old ~90 `Client` helpers and 107 `default_account_id().to_string()`
allocations are gone. The shape is:

- `Client::primary_account::<C: Capability>() -> Result<Account<Tr>>` -
  per-capability primary selection. JMAP allows different primary
  account IDs per capability; passing the capability marker forces an
  explicit choice instead of silently using whichever primary the
  session lists first.
- `Account::call<M: JmapMethod>(method) -> Result<M::Response>` - the
  single-method protocol entry point. Method-struct constructors do
  not take an `accountId`; `call` and `build` inject it.
- `Account::build() -> Request` - batch builder for cross-method
  flows (result references, multiple calls per round-trip).
- `Account::mail() -> Mail` - the curated workflow facade. Only
  `mail()` ships; `calendar()` and `contacts()` were deliberately not
  built on speculation. The protocol layer is sufficient for those
  capabilities until a real ratatoskr workflow demands a shape.

Cross-account methods (`Email/copy`) take a second `&AccountId` (the
source); the destination is the calling `Account`.

The facade in `mail.rs` is hand-curated workflows, not 1:1 protocol
mirroring. Methods are added when a real consumer flow demands one,
never on speculation.

### Typed IDs (`Id<T>`) everywhere

`Id<T>` with phantom markers is adopted across helpers, builders,
filter values, changes APIs, and result references. Per-object
typedefs (`EmailId`, `MailboxId`, `BlobId`, `AccountId`, ...) are
public; the marker types are module-private. `Object::Id` is the
associated type threaded through every generic core request/response
(Get, Set, Changes, Query, QueryChanges, Copy, Parse).

JSON-map-backed objects (`CalendarEvent`, `ContactCard`) return owned
`XxxId` from `id()` / `take_id()` since their storage cannot lend a
borrowed reference.

### `NonZeroUsize` for `max_changes`

`Changes` and `QueryChanges` builders accept `NonZeroUsize`. The
runtime `Error::InvalidArgument` path for `max_changes: 0` was
deleted; the invalid value is now a compile error.

### Value-builders

Every method-struct builder is `fn x(self) -> Self` (consume + return).
The earlier `&mut self` form is gone. `Account::call(EmailGet::new()
.ids([id]))` reads as one expression.

### Response extraction: `take_*` → `into_*`

`Response::get(&handle)` returns a result by value; `into_list()`,
`into_ids()` consume `self`. The pre-redesign `take_*` mutating
boundary on the result type is removed. The outer `let mut response`
remains because `Response::get` still uses `swap_remove` internally;
that wasn't worth unwinding.

### Typed batch results: `Request::send_methods((m1, m2, ...))`

The §6 stretch shipped, but with a different name and placement than
the original proposal. It is `Request::send_methods` on the request
builder, not `Batch::send` on a separate type. Tuple sizes 1-8 are
supported via a `MethodTuple` trait.

```rust
let (query, get) = account
    .build()
    .send_methods((email_query, email_get))
    .await?;
```

Result-reference flows are intentionally excluded from this path.
When method N needs a `CallHandle` from method N-1 to build an
`ids_ref` / `mailbox_ids_ref`, the explicit
`request.call(m)?` + `response.get(&h)?` path is the only option,
because the handles cannot cross the tuple boundary by design.

### `BlobRef` and asymmetric download/upload placement

`BlobRef { account_id, blob_id, name, content_type }` replaces bare
`BlobId` at API boundaries that need URL construction. The previous
download path silently substituted `default_account_id()` (wrong for
cross-account blobs) and hardcoded `name` and `type` to placeholders.

Placement is intentionally asymmetric:

- `Client::download(&BlobRef)` - `BlobRef` already carries the account
  ID, so routing through `Account` would duplicate or be misleading.
- `Account::upload(data, content_type)` - the upload URL itself
  contains the account, so consumers should not have to reconstruct
  one. The returned `BlobRef` is bound to the uploading account.

### Type-state split for typed JMAP objects

`Email`/`EmailCreate`/`EmailPatch` and `Mailbox`/`MailboxCreate`/
`MailboxPatch` (and the rest of the typed objects) are three distinct
struct shapes per object. `SetCreate` is a separate trait from
`SetObject`. Destroy-only objects (`ShareNotification`,
`CalendarEventNotification`) declare `Create`/`Patch` as uninhabited
enums and gate `SetRequest::create` / `update` on bounds that those
uninhabited types do not satisfy - the spec's read-only semantics are
enforced at compile time.

JSON-map-backed objects (`CalendarEvent`, `ContactCard`) use the
extended `json_object_struct!` macro to emit the trio. Patch shapes
allow dotted-path keys for nested patches.

### `Field<T>` partially exposed

`Field<T>` (three-state nullable: `Omitted` / `Null` / `Value(T)`) is
the storage layer for nullable properties. `_field()` accessors are
exposed on JSON-map-backed objects (`Calendar`, `Quota`, ...) where
the three-state distinction matters for patch generation.

This is the only redesign item that did not land in full. See
"Remaining gaps" below.

## Deliberate departures from the proposal

### `CreatedId<T>` did not ship

The proposal called for a typed wrapper for consumer-supplied
create-ids (`#draft1`). The shipped design keeps create-ids as `String`
in `SetResponse::created` and `SetResponse::not_created`. Create-ids
are not server IDs, are never compared with server IDs, and have no
useful invariant a typed wrapper would carry. The doc comment on
`core/set.rs` records the choice.

### `Batch::send` became `Request::send_methods`

The proposal sketched a separate `Batch` type. In practice `Request`
already had the builder shape needed, and adding a parallel `Batch`
type would have meant two ways to build a batch with no real
disambiguation. `send_methods` is a method on the existing request
builder. The name reads less clean than `Batch::send`, but the type
hierarchy stays flat.

### No `calendar()` / `contacts()` facades

The proposal was explicit about this and we held the line. The
protocol layer is sufficient and ratatoskr has not driven a concrete
shape for either capability.

## Remaining gaps

### `Field<T>` getter consistency

Typed structs that hold `Field<T>` properties expose only the
ergonomic `Option<T>`-style getter, not the three-state `_field()`
sibling, on `Email` and `Mailbox`. `Email` additionally has sentinel
getters that hide the omitted/null distinction: `size()` returns
`usize` with `unwrap_or(0)` and `has_attachment()` returns `bool` with
`unwrap_or(&false)`. The proposal called for both getters per
property *and* removing sentinel collapses; neither is in for `Email`
or `Mailbox`.

Open questions:

- Is the JSON-map vs typed-struct split the right place to draw the
  `_field()` line? Today JSON-map objects (`Calendar`, `Quota`,
  `CalendarEvent`, `ContactCard`) expose `_field()`; typed structs
  mostly do not.
- Do `Email::size` and `Email::has_attachment` need a migration to
  `Option<usize>` / `Option<bool>` plus `_field()` accessors, or do
  the sentinels stay because ratatoskr never branches on
  omitted-vs-zero?

Resolving this is one decision and a mechanical sweep, but it has not
been done.

### Typed create-id / result-reference handles

`CreatedId<T>` is settled as "not shipping" (see above). The adjacent
proposal was to give *result-reference values* (`#/ids` and friends)
typed handles instead of `String`. That part also did not ship -
result references are built from `CallHandle` at request-construction
time, which is typed, but the JSON-level reference path
(`#/ids`) is stringly-typed in the request payload. No bug has been
filed against this; revisit if a real consumer hits a typo here.

### `send_methods` naming

The shipped name is fine but not load-bearing. If a clearer name
emerges before 1.0 the rename is mechanical. No decision required
unless a consumer reports the ergonomics are wrong.

### `Response` still requires `let mut`

`Response::get` does `swap_remove` on an internal `Vec`, which
requires the outer binding to be `mut`. The §6 stretch
(`send_methods`) sidesteps this for tuple-shaped batches, but the
explicit `request.call(m)?` + `response.get(&h)?` path still pays the
`let mut`. Worth fixing if we can rework `Response` to consume on
extract, but not blocking.

## See also

- `reference/jmap.md` - current architecture, single source of truth.
- `reference/jmap/DEFERRED.md` - accepted micro-optimizations and the
  one open API-cleanup item.
