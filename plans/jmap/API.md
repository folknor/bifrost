# bifrost-jmap pre-1.0 API redesign

A side-by-side of today's surface and where we think it should land, with the
tradeoffs spelled out. The goal is to commit (or consciously reject) each shift
before doing the work - not to drift into a half-migration.

This is the long-form companion to `TODO.md` § "API ergonomics (pre-1.0 breaking
changes)". `TODO.md` lists the five ergonomics fixes consumer feedback called
out; this doc places them inside the larger structural decisions and folds in
three rounds of external review.

> **Read order matters.** §2 is the load-bearing decision. §1 is shaped by the
> §2 outcome - read §2 first, then §1.

---

## 2. The central decision: helpers vs. builder vs. domain facade

### Today

Two parallel vocabularies for every JMAP method:

| Operation | Helper | Builder |
|---|---|---|
| Get one email | `client.email_get(id, props)` | `EmailGet::new(acc).ids([id])` + send + take |
| Get all mailboxes | *(no helper - `mailbox_get` is single)* | `MailboxGet::new(acc)` + send + take |
| Create mailbox | `client.mailbox_create(name, parent, role)` | `MailboxSet::new(acc).create()...` |

Plus a *third* vocabulary for cross-account variants (`email_import` +
`email_import_account`). The helper layer has accreted three conventions in
parallel: singular (`email_get`), plural (`quota_get_all`), and
`_account`-suffixed pairs.

### Options on the table

#### B - bare builder + sugar

Delete the helper layer entirely. Every operation goes through the builder. Add
`Account::call(M)` as the build-send-extract single-method path.

```rust
let account = client.primary_account::<cap::Mail>()?;
let emails = account.call(EmailGet::new().ids([id])).await?;

// batching unchanged:
let mut request = account.build();
let h1 = request.call(EmailQuery::new().filter(...))?;
let h2 = request.call(EmailGet::new().ids_from(&h1))?;
let response = request.send().await?;
```

**Pros:** one vocabulary; adding a method is just the method struct; no
plurality drift; matches the protocol shape exactly. **Cons:** every
existing helper call site changes; common single-method flows are slightly
more verbose than today's helpers.

#### D - type-namespaced scopes

One sub-scope per JMAP type. Each owns a small set of methods.

```rust
account.email().get(id).await?;
account.email().get_many(&ids).await?;
account.mailbox().create("Drafts", None, Role::Drafts).await?;
account.calendar_event().query(filter).await?;
```

**Pros:** plurality drift impossible by shape; folds out `_account` suffix
(the scope already carries the account); domain name (`email`, `mailbox`)
mirrors the spec. **Cons:** ~16 sub-scope structs, each with an audited
method set; adding a new method still touches a per-type scope; mirrors the
protocol, not consumer workflows.

#### B++ - bare builder + curated workflow facade (recommended)

B's protocol layer plus a small domain facade for actual workflows - *not* a
1:1 helper per JMAP method, but a curated API around what consumers actually do.

```rust
// Protocol layer (B):
let account = client.primary_account::<cap::Mail>()?;
let emails = account
    .call(email::get().ids_from(q.ids()).properties([email::Prop::Subject]))
    .await?;

// Workflow facade (the "++"):
let mail = account.mail();
let messages = mail.emails()
    .in_mailbox(inbox_id)
    .latest(50)
    .fetch([email::Prop::Subject, email::Prop::From])
    .await?;
```

The facade is hand-curated workflows, not generated method-by-method. Most
JMAP methods do *not* get a facade entry - only flows we observe consumers
actually doing.

**Pros:** protocol is fully accessible (B); common workflows get a
purpose-built API; facade is small and growable; doesn't double the surface
forever (we add facade methods only when a real workflow demands one).
**Cons:** "what's a real workflow?" requires judgment; facade and protocol
layer can drift apart in shape; we have to discover the right facade by
using it (probably with ratatoskr as the proving ground).

#### Rejected: A (keep helpers, normalize) and C (helpers as primary API)

All three reviewers rejected A. The "stay disciplined under a written rule"
hope behind A is the kind of bet that loses on a multi-year horizon. C was
already rejected in the prior pass - doubles the surface forever, makes
batching consumers second-class.

### Decision: B++

Ship B as the protocol layer this release; introduce one small `mail()`
facade with the obvious workflows (list inbox, fetch message bodies, mark
read, move to mailbox, send, search). Grow the facade as ratatoskr demands.

**Only `mail()` ships in this release.** `calendar()` and `contacts()`
facades are not shipped until a real ratatoskr workflow demands a specific
shape - shipping facades on speculation reintroduces the same accretion
problem we're getting out of. The protocol layer (B) is fully sufficient
for calendar and contacts work in the meantime.

D was the defensible alternative - protocol-shaped vs. task-shaped. We
optimize for the actual consumer (task-shaped) over the hypothetical
protocol-reader (spec-shaped). One consumer, one shape.

§1 (next) collapses substantially - there are no helpers to relocate.

#### Constructor rule

With B++, account-scoped method-struct constructors no longer take an
`accountId` parameter. `Account::call(M)` and `Batch` inject the account
ID at request-build time. Today:

```rust
EmailGet::new(account_id_string).ids([id])      // before
```

Becomes:

```rust
EmailGet::new().ids([id])                        // after
account.call(EmailGet::new().ids([id])).await?   // account injected here
```

Cross-account methods (`Email/copy`) take a *second* `&AccountId` (the
source account); the calling `Account` is the destination, injected by
`call`/`Batch`.

#### Builder style: value builders, not `&mut self`

Today's builder methods are `&mut self -> &mut Self` - chaining requires
`let mut x = ...; x.a(...); x.b(...);` and breaks the fluent
`EmailGet::new().ids([id]).fetch_text_body_values(true)` shape used in
every example in this doc.

**Decision:** flip method-struct builder methods to value builders -
`fn ids(self, ...) -> Self`. Chains compose without `let mut`.

```rust
// before:
let mut get = EmailGet::new(&acc);
get.ids([id]);
get.arguments().fetch_text_body_values(true);

// after:
let get = EmailGet::new()
    .ids([id])
    .fetch_text_body_values(true);
```

Tradeoffs:
- **Pro:** the fluent shape works; no `let mut`; matches every other
  modern Rust builder (reqwest, sqlx-query, axum router).
- **Pro:** `Account::call(EmailGet::new().ids([id]))` reads as one
  expression, which is the whole point of the redesign.
- **Con:** consumers who *want* to mutate piecemeal (build a method
  struct over several conditional branches) lose the `&mut self` form.
  Rebinding (`let q = q.filter(...);`) covers this; mildly more verbose.
- **Con:** mechanical rewrite of every builder method on every method
  struct in the crate. Large but straightforward.

---

## 1. Account scoping (post-§2)

### Today

`AccountScope` exists in `account.rs` but only owns `build()`. The ~90 helper
methods live on `Client` and call `request.default_account_id().to_string()`
107 times across the crate.

Two latent problems beyond ergonomics:

- **Implicit default account** - every helper silently uses
  `default_account_id()`. Consumers can't tell from the call site which
  account they hit.
- **Capability-per-account.** JMAP allows different account IDs per
  capability. One login can have a mail account ID and a different calendar
  account ID. `default_account_id()` flattens that into one ID and uses it
  everywhere - true on most servers, not spec-guaranteed.

### Target

Rename `AccountScope` → `Account`. Make it owned, cheap-clone (backed by an
`Arc<ClientInner<Tr>>`), no public lifetime parameter:

```rust
pub struct Account<Tr: HttpTransport = ReqwestTransport> {
    client: Client<Tr>,        // already cheap-clone via Arc<ClientInner>
    account_id: AccountId,
}
```

Make `Client` itself cheap-clone - `Client<Tr>` becomes `Arc<ClientInner<Tr>>`.
This is the standard shape (reqwest, sqlx, every async client of consequence).
Lifetime parameters in handles passed to long-lived structs are friction with
no upside.

Capability-aware account selection:

```rust
let mail_account     = client.primary_account::<cap::Mail>()?;
let calendar_account = client.primary_account::<cap::Calendars>()?;
// May be the same AccountId, may not. The type system stops assuming.
```

`Account` is parameterized only by transport, not by capability - a single
account can carry multiple capabilities. The capability marker on
`primary_account` is the *selection criterion*, not a permanent type tag.

`Account` exposes:
- `call<M>(method) -> Result<M::Response>` - single-method protocol path.
- `build()` - batch builder (unchanged).
- `upload` (see §7); `download` lives on `Client`.
- `mail()` - the one workflow facade entrypoint shipped this release,
  feature-gated under `mail`. No `calendar()` / `contacts()` - protocol
  layer suffices for those until ratatoskr drives a concrete shape.

If §2 = B, drop the facade entrypoints entirely.

### Tradeoffs

- **Pro:** kills the 107 `.to_string()` allocations.
- **Pro:** explicit scoping; capability-per-account is now visible at the
  type level instead of lurking in a runtime assumption.
- **Pro:** `Arc<ClientInner>` shape lets consumers store, clone, spawn, and
  pass `Account` and `Client` around without lifetime acrobatics.
- **Con:** `Client` becoming `Arc<ClientInner>` is a structural refactor -
  not large, but touches client.rs, transport plumbing, and tests.
- **Con:** capability-per-account changes the session inspection API.
  `client.default_account_id()` either disappears or gains a capability
  parameter. Existing call sites all change.

### Open question (resolved)

Borrow vs. Arc: **Arc.** All three reviewers agreed; the borrow-based
`AccountScope<'a>` was unsalvageable for long-lived consumers.

---

## 3. ID and value typing

### Today

`Id<T>` exists in `core/id.rs` with three markers (`Account`, `BlobMarker`,
`StateMarker`) - and with three different naming conventions. Everything else
is `&str` / `String`. `max_changes` is `usize` - `0` is a valid value to
construct, invalid per spec, rejected at runtime.

### Target

#### Per-object ID markers, marker types private

Add a marker per JMAP object. Make the marker enum module-private; export
only the typedef:

```rust
// in crates/jmap/src/email/mod.rs
mod marker { pub enum Email {} }
pub type EmailId = crate::core::id::Id<marker::Email>;
```

Consumers spell `EmailId`, `MailboxId`, `IdentityId`, etc. They never see
the marker. Resolves the `EmailMarker` ugliness without a naming-convention
debate.

While breaking everything, rename existing markers for consistency:
`Account` → private; `BlobMarker` → private; `StateMarker` → private. Only
`AccountId`, `BlobId`, `State` remain public.

#### Adopt `Id<T>` everywhere, including filters

Method-struct builder methods, facade APIs, filter values that take IDs
(`inMailbox`, `hasAttachment` references), changes APIs, and result
references - all take `&Id<T>`.

#### Type the rest of the protocol identity layer

- `BlobRef { account_id: AccountId, blob_id: BlobId, name: Option<String>,
  content_type: Option<String> }` - see §7. Replaces bare `BlobId` at API
  boundaries that need URL construction.
- `CreatedId<T>` - typed wrapper for create-id references (`#draft1`).
- Result-reference values (`#/ids` etc.) get typed handles.

#### `NonZeroUsize` for `max_changes`

```rust
pub fn max_changes(self, n: NonZeroUsize) -> Self;
```

`0` becomes a compile error, not a runtime `Error::InvalidArgument`. This
subsumes TODO §4 entirely; the runtime validation never ships.

### Tradeoffs

- **Pro:** the entire bug class reviewer feedback flagged
  (`email_submission_create(email_id, identity_id)` confusable, etc.) is
  gone at compile time, not "fixed" by one signature.
- **Pro:** invalid `max_changes: 0` is structurally impossible.
- **Pro:** `BlobRef` un-corrupts the download API (see §7).
- **Con:** consumers building IDs from server responses do `Id::from(s)`.
  Mild noise, but visible.
- **Con:** ~16 marker types is a lot. Module-private resolves naming, but
  the modules still each grow a `marker` submodule.

### Open question (resolved)

Marker naming: **markers are private; only typedefs are public.** Resolves
OQ#3.

---

## 4. Getter shape

### Today

Mixed. From `mailbox/get.rs`:

| Getter | Returns | Behavior on absent |
|---|---|---|
| `name()` | `Option<&str>` | `None` |
| `parent_id()` | `Option<&str>` | `None` |
| `role()` | `Role` | `Role::None` (sentinel) |
| `total_emails()` | `usize` | `0` (sentinel - indistinguishable from real 0) |
| `is_subscribed()` | `bool` | `false` (sentinel) |

The sentinels are a bug: JMAP lets the server omit any property, so "totalEmails: 0"
and "server didn't include totalEmails" are reported the same.

### Target

Two getters per property - ergonomic and explicit:

```rust
impl Mailbox {
    pub fn role(&self) -> Option<Role>;
    pub fn role_field(&self) -> &Field<Role>;     // omitted vs null vs value

    pub fn total_emails(&self) -> Option<usize>;
    pub fn total_emails_field(&self) -> &Field<usize>;
}
```

Or a generic `presence`-style accessor: `mb.field(Property::Role) -> &Field<Role>`.
Either shape works; pick whichever generates less boilerplate.

Default ergonomic getter (`role()`) collapses Omitted+Null → `None`.
`*_field()` exposes the three-state version for consumers who need it (sync
engines, patch generators).

The `Some(Role::None)` ambiguity (a JMAP-defined "no specific role" value
distinct from "server omitted role") gets a doc-level callout. We do *not*
introduce a third state in the type to disambiguate it; that's worse than
the docs.

### Tradeoffs

- **Pro:** matches `Field<T>` storage layer; the getter stops lying about
  three-state data.
- **Pro:** ergonomic path is `Option<T>` - most consumers never touch
  `*_field()`.
- **Con:** twice the getters per property. Mechanical, but verbose.
- **Con:** consumers migrate sentinel-comparison code (`if mb.role() ==
  Role::None`) to `Option` shape. ~few hundred call sites in ratatoskr.

### Open question (resolved)

Expose `Field<T>` at the getter layer? **Yes - both.** Disagreement among
reviewers; we side with "expose both" because ratatoskr's patch path needs
the three-state distinction and YAGNI doesn't apply when the consumer
already exists.

---

## 5. Lifted method arguments

### Today

```rust
let mut get = EmailGet::new(&account_id);
get.ids([id]);
get.arguments().fetch_text_body_values(true);   // hidden
get.arguments().max_body_value_bytes(1024);     // hidden
```

`.arguments()` is a serde implementation artifact, not a user-facing
boundary.

### Target

Lift everything that has a method on the inner arguments struct to the outer
method struct. Mechanical, no per-method judgment:

```rust
let get = EmailGet::new()
    .ids([id])
    .fetch_text_body_values(true)
    .max_body_value_bytes(1024);
```

`.arguments()` stays as the escape hatch for direct field access in tests.

No tradeoffs. Recommendation stands.

---

## 6. Response extraction

### Today

```rust
let mut response = request.send().await?;     // let mut required
let mut result = response.get(&handle)?;       // let mut required
let id = result.take_id();
let emails = result.take_list();
```

Two mutating boundaries.

### Target

#### Baseline (this release)

Rename `take_*` → `into_*`, consume `self`:

```rust
let mut response = request.send().await?;
let result = response.get(&handle)?;
let emails = result.into_list();
let id = result.into_id();
```

Drops the inner `let mut`. The outer one stays - `Response::get` does
`swap_remove` on an internal Vec, which is an implementation choice we don't
unwind in this release.

#### Stretch (this release if scope allows)

Typed batch result via tuple destructuring:

```rust
let (query_result, get_result) = batch
    .send((email_query, email_get))
    .await?;
```

`Batch::send` takes a tuple of method structs and returns a tuple of typed
results. No `Response`, no `Handle`, no `let mut`, no `swap_remove`.

This is a significant refactor of the request envelope - possibly bigger
than the rest of this release combined. Two reviewers flagged it; one said
"v2 territory," one said "do it now." We file it as a stretch goal: do it
if `Account` + `Arc<ClientInner>` + facade work goes faster than expected;
defer otherwise.

### Tradeoffs

- **Pro (baseline):** drops one `let mut`, matches Rust convention.
- **Pro (stretch):** removes the response-as-mutable-bag entirely.
- **Con (stretch):** result-reference flows (`#/ids`) need a typed
  representation that survives the tuple boundary. Non-trivial.

---

## 7. Blob handling: `download` is wrong, not just imperfect

### Today

```rust
// crates/jmap/src/blob/download.rs
pub async fn download(&self, blob_id: &str) -> Result<Bytes> {
    let account_id = self.default_account_id();   // silently
    // ... fills URL template:
    URLParameter::Name => download_url.push_str("none"),                       // hardcoded
    URLParameter::Type => download_url.push_str("application/octet-stream"),   // hardcoded
    // ...
}
```

RFC 8620 §6 specifies that download URLs require `accountId` ("the account
to which the record with the blobId belongs"). Today's API:

1. Takes a bare `blob_id`.
2. Silently substitutes `default_account_id()`. Wrong if the blob belongs
   to a different account (cross-account email, calendar attachment in a
   shared account, etc.).
3. Hardcodes `name` and `type` to placeholder strings - losing the
   consumer's ability to specify either.

`TODO.md` files this under "explicitly not changing - current shape is
correct." That's wrong. Reviewer #3 caught it; verified against the source.

### Target

```rust
pub struct BlobRef {
    pub account_id: AccountId,
    pub blob_id: BlobId,
    pub name: Option<String>,
    pub content_type: Option<String>,
}

impl<Tr: HttpTransport> Client<Tr> {
    /// Download a blob. The account is identified by the BlobRef.
    pub async fn download(&self, blob: &BlobRef) -> Result<Bytes>;
}

impl<Tr: HttpTransport> Account<Tr> {
    /// Upload a blob to this account. Returns a BlobRef bound to this account.
    pub async fn upload(&self, data: Bytes, content_type: Option<&str>) -> Result<BlobRef>;
}
```

Asymmetric placement is intentional:

- **`download` lives on `Client`** because `BlobRef` is self-describing -
  it already carries the account ID. Routing through `Account` would either
  duplicate that information (and require a mismatch check) or silently
  ignore the `Account` and use the ref's account anyway. Both are worse
  than just putting it on `Client`.
- **`upload` lives on `Account`** because the upload URL is templated with
  `accountId` - there's no `BlobRef` yet to pull it from. The upload's
  account context comes from the `Account` handle, and the returned
  `BlobRef` records that binding for subsequent reads.

`BlobRef` is the unit that travels through the API. Functions returning blob
references (`Email/get` for attachments, `Email/import` results, etc.) hand
back `BlobRef`, so consumers don't reconstruct one.

### Tradeoffs

- **Pro:** correct under RFC 8620; cross-account blob access works.
- **Pro:** `name` and `type` reach the URL template instead of being
  silently dropped.
- **Con:** every consumer that calls `client.download(blob_id)` migrates.
- **Con:** `BlobRef` becomes load-bearing across the API. Worth it.

### Update to `TODO.md`

Remove `download(blob_id)` from "Explicitly not changing." It is changing.

---

## ✅ STATUS

§8 (type-state split), §6 stretch (typed batch results), and §3
(Id<T> call-site adoption sweep) are landed. The sweep added a
required `Object::Id` associated type, threaded `O::Id` through
every generic core request/response (Get, Set, Changes, Query,
QueryChanges, Copy, Parse), and converted method-struct builder
parameters, filter constructors, and per-object getters/setters
to typed IDs. JSON-map-backed objects (`CalendarEvent`,
`ContactCard`) return owned `XxxId` from `id()`/`take_id()` since
their JSON-map storage cannot lend a `&XxxId`.

## 8. Type-state on object types: open structural question

### Today

Every JMAP object struct is parameterized by a phantom state: `Email<Get>`,
`Email<Set>`, `Mailbox<Get>`, `Mailbox<Set>`. The state controls which
serde fields are active and which methods compile.

### The critique (reviewer #3)

The phantom-state pattern leaks a serde mechanic into the user-facing type
name. Consumers see `Email<Get>` and `Email<Set>` and have to learn what the
phantom state means. The cleaner shape is distinct types per role:

```rust
pub struct Email          { /* fields a server returns */ }
pub struct EmailCreate    { /* fields a client sends on create */ }
pub struct EmailPatch     { /* fields a client sends on update */ }

pub struct Mailbox        { /* ... */ }
pub struct MailboxCreate  { /* ... */ }
pub struct MailboxPatch   { /* ... */ }
```

### Tradeoffs

- **Pro:** user-facing types stop carrying serde implementation detail in
  their names.
- **Pro:** create/patch shapes can omit fields the server would never
  accept (server-assigned IDs, computed totals, etc.) - the type system
  enforces it.
- **Con:** ~10 typed JMAP objects × 3 roles = ~30 new public types. Big
  surface change.
- **Con:** code reuse between roles either gets macros or duplication.
  `Email<State>` shares one struct definition; `Email`/`EmailCreate`/
  `EmailPatch` need either a macro or three separate definitions.
- **Con:** every existing call site (helpers, builder, getters, set
  methods) changes.

### Decision: implemented this release

The split landed: `Mailbox`/`MailboxCreate`/`MailboxPatch` and so on
across every typed JMAP object. ShareNotification and
CalendarEventNotification declare `Create`/`Patch` as uninhabitable
enums, so `SetRequest::create()` / `update()` do not resolve at
compile time for those destroy-only types - the spec's read-only
semantics are enforced by the type system rather than documentation.

`SetCreate` (the new contract for the create-shape input type) is a
separate trait from `SetObject` (which lives on the canonical type).
`SetRequest::create()` is gated on `O::Create: SetCreate` and
`SetRequest::update()` on `O::Patch: Default`, so destroy-only objects
opt out cleanly.

JSON-map-backed objects (`CalendarEvent`, `ContactCard`) use the
extended `json_object_struct!` macro which now emits a trio. The
patch shape's setters allow dotted-path keys for nested patches;
the create shape uses plain property names.

---

## 9. Things explicitly *not* changing

Pruned from `TODO.md`'s "Explicitly not changing" list, with rationale:

- **`changes.created()` returning `&[String]`** - matches storage;
  `.map(String::as_str)` is idiomatic. Stays.
- **Filter type inference requiring an explicit binding** - a generics
  limitation; fixing it would need a less-generic API. Stays.
- **`take_id()` / `take_list()` requiring `let mut`** - addressed in §6
  via `into_*` consuming `self`. Removed from "not changing."
- **`download(blob_id)` signature** - wrong; addressed in §7. Removed
  from "not changing."

---

## Aggregate scope

If we accept the recommendations above, the pre-1.0 release contains:

1. **`Client` becomes `Arc<ClientInner<Tr>>`** - cheap-clone, no public
   lifetime.
2. **`AccountScope` → `Account`** - owned, cheap-clone, capability-aware
   selection (`primary_account::<cap::Mail>()`).
3. **§2 = B++** - delete helpers, add `Account::call<M>()`, ship one
   `mail()` workflow facade. `calendar()` and `contacts()` facades are
   *not* in this release - protocol layer suffices until ratatoskr proves
   a concrete workflow shape.
4. **`Id<T>` adoption everywhere** - markers private, typedefs public,
   ~16 typed IDs across helpers/builders/filters/results.
5. **`NonZeroUsize` for `max_changes`** - supersedes runtime validation.
6. **`BlobRef`** - replaces bare `BlobId` at API boundaries; fixes
   `download` correctness (§7).
7. **`Option<T>` getters + `*_field()` accessors** - sentinel bugs gone,
   `Field<T>` exposed where needed.
8. **Lifted method arguments** (§5).
9. **Value-builder method structs** - `fn ids(self, ...) -> Self`,
   not `&mut self -> &mut Self` (see §2 builder-style rule).
10. **`take_*` → `into_*`** (§6 baseline).
11. **Stretch:** typed batch results (§6 stretch). Defer if blocking.

### What this release does *not* do

- **Type-state split** (`Email<Get>` → `Email`/`EmailCreate`/`EmailPatch`).
  Deferred to a follow-up breaking release before 1.0 - see §8.
- MDN (RFC 9007), S/MIME (RFC 9219). See `MDN.md`, `SMIME.md`.
- Optimization items 1, 3, 4 from `TODO.md` (capability_config round-trip,
  CallHandle.call_id, SSE Bytes copy) - accepted trade-offs.
- JSON-map vs typed-struct unification for CalendarEvent / ContactCard.
  Acknowledged as **structural debt**, not "intentional design": the
  typed-struct path silently drops vendor extension properties on
  round-trip. Worth fixing post-1.0; not in this release because it
  touches every typed JMAP object similarly to §8.

---

## Decisions (final)

1. **§2 = B++.** Bare builder + one `mail()` workflow facade.
   `calendar()` / `contacts()` deferred until ratatoskr drives the shape.
2. **`Account` is owned, `Arc<ClientInner>`-backed.** No public lifetime.
3. **`Id<T>` markers are module-private, typedefs are public.**
4. **`Field<T>` exposed at the getter layer.** Both `role() -> Option<Role>`
   and `role_field() -> &Field<Role>`.
5. **§8 (type-state split) deferred.** Post-1.0 will need one more
   breaking release dedicated to it. This is the release's one conscious
   compromise.
6. **Account-scoped method-struct constructors don't take `accountId`.**
   `Account::call`/`Batch` injects it (see §2 constructor rule).
7. **Method-struct builders are value builders** - `fn ids(self) -> Self`
   throughout (see §2 builder-style rule).
8. **`download` lives on `Client`, `upload` on `Account`** (see §7).

### Still open

- **Typed batch results (§6 stretch).** Bundle if scope allows, defer
  otherwise. Decide during implementation, not now - it depends on how
  much budget the rest of the release consumes.
