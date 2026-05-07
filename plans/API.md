# bifrost-jmap pre-1.0 API redesign

A side-by-side of today's surface and a utopia version, with the tradeoffs spelled
out. The goal is to commit (or consciously reject) each shift before doing the
work — not to drift into a half-migration.

This is the long-form companion to `TODO.md` § "API ergonomics (pre-1.0
breaking changes)". `TODO.md` lists the five ergonomics fixes consumer feedback
called out; this doc places them inside the larger structural decisions.

---

## 1. Account scoping: today vs. utopia

### Today

Two parallel ways to scope a request to an account:

```rust
// (a) Free-function helpers on Client — implicitly use default_account_id()
client.email_get(id, None).await?;
client.mailbox_create("Drafts", None, Role::Drafts).await?;

// (b) Builder via Client::build() or AccountScope::build()
let mut request = client.build();        // implicit default account
let handle = request.call(EmailGet::new(request.default_account_id().to_string()))?;

let account = client.account_scope(account_id);
let mut request = account.build();        // explicit account
```

`AccountScope` exists (`account.rs`) but only owns `build()`. The ~90 helper methods
(`email_get`, `mailbox_create`, `calendar_event_query`, …) live on `Client`, not on
`AccountScope`, and each one calls `request.default_account_id().to_string()`
(TODO optimization #2 — counted: 107 occurrences).

Cross-account helpers don't exist; if you need a non-default account in a helper, you
drop down to the builder.

### Utopia

Helpers move to `AccountScope`. `Client` stops being a method bag.

```rust
let inbox = client.account(account_id);    // or client.default_account()
inbox.email_get([id]).await?;
inbox.mailbox_create("Drafts", None, Role::Drafts).await?;

// builder still works, scoped:
let mut request = inbox.build();
let handle = request.call(EmailGet::new(inbox.id()))?;
```

Properties:
- `Client` keeps connection-level methods (`build`, `session`, `upload`, `download`,
  `connect_websocket`, …) and nothing else.
- `AccountScope` owns every per-account method. Cross-account ops (`Email/copy`)
  take a second `AccountScope` or `&AccountId`.
- `default_account_id().to_string()` disappears — `AccountScope` already owns the
  `AccountId`, so method-struct constructors take `&AccountId` (or `Id<Account>`).

### Tradeoffs

- **Pro:** every consumer call site grows one short prefix (`client.default_account().`)
  and in exchange gets explicit scoping, kills the 107 allocations, and makes
  cross-account flows look like cross-account flows.
- **Pro:** drops the "implicit default account" footgun. Accounts that have multiple
  data types (mail + calendar + contacts) currently work because *one* account_id is
  used for all — true for most servers, but not guaranteed by JMAP. Explicit scoping
  forces the consumer to think about which account.
- **Con:** every downstream call site changes. ratatoskr is the only consumer, but
  it's a wide blast radius.
- **Con:** `AccountScope` borrows `&Client`. Holding one across `.await` points or
  storing one in a struct is fine but constrains lifetimes.

### Open question

Should `AccountScope` be `Copy` (carrying `&Client` + `&AccountId`) or owned
(carrying `&Client` + `AccountId`)? Today it's the latter. `Copy` would let
consumers freely re-derive scopes; owned is what we have.

---

## 2. Helpers vs. builder: today vs. utopia

### Today

Both exist for every method. Helpers are convenience wrappers that:
1. Build a single-method request,
2. Send it,
3. Extract the result.

The builder path is for batching multiple calls in one HTTP round-trip (the actual
reason JMAP's request envelope exists).

So today's surface is two complete vocabularies for the same operations:

| Operation | Helper | Builder |
|---|---|---|
| Get one email | `email_get(id, props)` | `EmailGet::new(acc).ids([id])` + send + take |
| Get all mailboxes | *(no helper — `mailbox_get` is single)* | `MailboxGet::new(acc)` + send + take |
| Create mailbox | `mailbox_create(name, parent, role)` | `MailboxSet::new(acc).create()...` |

The plural-naming gotcha (TODO §2) is a direct symptom: we picked one of "get one" /
"get many" per helper without writing it down, so `email_get` is single and
`quota_get_all` is plural and `principal_get` takes a list.

### Utopia options

#### A. Keep both, make them consistent

Every `<type>_get` takes `Option<&[Id]>` (None = all). Every `<type>_get_one(id)`
exists as a thin shim. Helper signatures audited end-to-end for plurality
consistency. Builder unchanged.

- **Pro:** smallest churn for consumers; preserves the convenience layer.
- **Con:** still two vocabularies. Surface area still doubles for every new method.
  We'll have this same conversation when MDN lands.

#### B. Delete helpers, keep builder

Every operation goes through the builder. We add a few sugar methods on the
builder/response so the common single-method case isn't painful:

```rust
// before:
client.email_get(id, None).await?

// after:
client.account(acc).call_one(EmailGet::new().ids([id])).await?
//                  ^^^^^^^^ build + send + extract, returns M::Response
```

- **Pro:** one vocabulary. Adding a method is just the method struct — no helper.
- **Pro:** the builder is already the more powerful API; helpers just hide it.
- **Con:** every existing helper call site changes, even ones that didn't have
  ergonomics complaints. Bigger consumer migration.
- **Con:** convenience flows get more verbose. `mailbox_create("Drafts", None, Drafts)`
  becomes `mailbox_set().create().name("Drafts").role(Drafts).send_one()` or similar.

#### C. Helpers as the public API, builder as the escape hatch

Inverse of B. Make helpers complete (every JMAP method has one), document the
builder as "use this for batching." Most consumers never touch the builder.

- **Pro:** simplest call sites; matches how most consumers actually use JMAP
  (one method per request, batching is rare).
- **Con:** doubles the surface forever. Every new method is a helper *and* a method
  struct. We just signed up for ~10 more methods (MDN, S/MIME).
- **Con:** batching consumers (us, eventually, for the sync engine) are second-class.

### Decision required: A or B (not both)

This is an either-or pick. Option C is rejected (doubles the surface forever,
makes batching consumers second-class). Options A and B are both defensible and
mutually exclusive — we ship one of them, not a blend.

**The case for A — keep helpers, fix them.**
The asymmetry (helpers exist but are inconsistent) is the actual consumer
complaint. Deleting helpers is more churn than fixing them. Most call sites are
single-method-per-request flows where the helper is a genuine convenience, not
just a wrapper. ratatoskr is the only current consumer and a 5-line helper is
cheap to maintain per type. The surface-doubling cost is paid in our crate, not
the consumer's.

If we pick A, the rules are:
- `<type>_get(ids: Option<&[Id]>, props)` — `None` = all, slice = specific.
- `<type>_get_one(id, props)` — convenience for the single case, returns `Option<T>`.
- Every `<type>_query` takes `Option<Filter>` and `Option<Comparator>` (today's
  shape; OK).
- Every `<type>_changes(since, max)` errors on `max == 0` (TODO §4).
- The `_account` suffix variants (`email_import_account`, …) are folded into
  the primary helper by taking `&AccountScope` directly.

**The case for B — delete helpers, sugar the builder.**
Pre-1.0 is the only window where breaking the helper layer is free. Picking A
means accepting "we'll have this same conversation when MDN lands" as a
permanent outcome — every new method needs a helper *and* a method struct, and
the plurality/naming/`_account`-suffix audits repeat for every addition. Today's
codebase already has three helper conventions in flight (`email_get` singular,
`quota_get_all` plural, `email_import` + `email_import_account` paired); that's
evidence the helper layer accretes inconsistency faster than we fix it.

If we pick B, the shape is:
- `AccountScope::call_one<M>(method) -> Result<M::Response>` — the
  build-send-extract single-method path.
- `AccountScope::build()` — unchanged, for batching.
- Method structs grow first-class builder methods (this is §5 anyway, so it's
  not extra work — it's work we were doing for the helper layer that now serves
  the only layer).
- No `<type>_*` functions on `Client` or `AccountScope`.

### Recommendation

Deferred — caller's choice. Both are coherent. The decision pivots on a single
question: **how much do we believe the helper layer will keep accreting
inconsistency vs. stay disciplined under a written rule?** If "stay disciplined"
is plausible, A. If we expect this same conversation in 12 months, B.

---

## 3. ID typing: today vs. utopia

### Today

`Id<T>` exists in `core/id.rs` with phantom marker types:
```rust
pub enum Account {}      pub type AccountId = Id<Account>;
pub enum BlobMarker {}   pub type BlobId    = Id<BlobMarker>;
pub enum StateMarker {}  pub type State     = Id<StateMarker>;
```

Three markers. Everything else is `&str` or `String`. Helper signatures look like:
```rust
pub async fn email_submission_create(&self, email_id: &str, identity_id: &str)
pub async fn email_set_mailbox(&self, id: &str, mailbox_id: &str, set: bool)
pub async fn mailbox_get(&self, id: &str, ...)
```

CLAUDE.md says `Id<T>` is "available for incremental adoption." Incremental
adoption means today's signatures all use raw strings.

### Utopia

Markers for every JMAP object type that has an ID. Helper and method-struct
signatures take `&Id<T>` or `impl Into<Id<T>>`:

```rust
pub enum Email {}    pub type EmailId    = Id<Email>;
pub enum Mailbox {}  pub type MailboxId  = Id<Mailbox>;
pub enum Identity {} pub type IdentityId = Id<Identity>;
// ... + Calendar, CalendarEvent, AddressBook, ContactCard, Thread,
//       EmailSubmission, SieveScript, Principal, Quota, ShareNotification,
//       PushSubscription, VacationResponse, ParticipantIdentity, CalendarEventNotification

pub async fn email_submission_create(
    &self,
    email_id: &EmailId,
    identity_id: &IdentityId,
) -> Result<EmailSubmission>;
```

Methods that produce IDs return `Id<T>`. `created_ids()` keys remain `String`
(JMAP-level), but typed extractors hand back `Id<T>`. Filters that take IDs
(e.g. `email_query` `inMailbox`) take `&MailboxId` not `&str`.

### Tradeoffs

- **Pro:** the `email_submission_create(email_id, identity_id)` confusion (TODO §5)
  becomes a compile error across every method, not just one.
- **Pro:** filter and changes APIs get type-safe IDs too — the bug class disappears.
- **Pro:** `&Id<Mailbox>` and `&str` coerce alike at most call sites because of
  `AsRef<str>` — most internal code is unchanged.
- **Con:** consumers building IDs from server responses need `Id::from(s)` or
  `Id::new(s)`. Not painful, but visible.
- **Con:** ~16 marker types is a lot of namespace. Co-locate them with their type
  modules (`email::EmailId`) instead of dumping them all into `core/id.rs`.
- **Con:** the marker types collide naturally with object types (`Email` the marker
  vs `Email<Get>` the struct). Need a naming convention — either `pub enum
  EmailMarker {}` or move markers to `id` submodules.

### Recommendation

**Adopt fully** in the same pre-1.0 release. Half-typed IDs are worse than fully
typed *or* fully untyped because consumers can't tell which form a given API
expects. Use `<Type>Marker` for the marker enums to avoid name collisions.

---

## 4. Getter shape: today vs. utopia

### Today

Mixed. Snapshot from `mailbox/get.rs`:

| Getter | Returns | Behavior on absent |
|---|---|---|
| `name()` | `Option<&str>` | `None` |
| `parent_id()` | `Option<&str>` | `None` |
| `role()` | `Role` | `Role::None` (sentinel) |
| `sort_order()` | `u32` | `0` (sentinel) |
| `total_emails()` | `usize` | `0` (sentinel — indistinguishable from real 0) |
| `unread_emails()` | `usize` | `0` (sentinel) |
| `is_subscribed()` | `bool` | `false` (sentinel) |
| `my_rights()` | `Option<&MailboxRights>` | `None` |

Half the surface uses `Option<T>`, half uses sentinels. The sentinels are a bug:
JMAP's `Mailbox/get` lets the server omit any property, so "totalEmails: 0" and
"server didn't include totalEmails" are both reported as `0`.

Same pattern is likely in `email/get.rs`, `calendar/get.rs`, etc. — needs an
audit.

### Utopia

Every getter that wraps a property the server can omit returns `Option<T>`.
Booleans included — `is_subscribed()` returns `Option<bool>`, not `false`-as-default.
Defaults move to documentation, not into the type.

```rust
pub fn role(&self) -> Option<Role>;
pub fn total_emails(&self) -> Option<usize>;
pub fn is_subscribed(&self) -> Option<bool>;
```

### Tradeoffs

- **Pro:** matches `Field<T>` machinery internally — getter layer stops flattening
  it away.
- **Pro:** consumers who want a default just `.unwrap_or(0)`. Consumers who want
  to detect absence finally can.
- **Con:** call sites grow `.unwrap_or_default()` noise where the consumer didn't
  care. ratatoskr will have a few hundred of these.
- **Con:** `Role::None` already exists as a JMAP-defined value (per RFC 8621 §2 —
  "no specific role"). So `Option<Role>` with `Some(Role::None)` is a real,
  distinct state from `None`-the-Option. We need to make sure we're representing
  "server omitted role" vs. "server said role: null" vs. "server said role:
  whatever" correctly. `Field<Role>` is the actual right type at the storage
  layer; the getter should expose it as `Option<Role>` collapsing
  Omitted+Null → None.

### Recommendation

**Audit and convert all sentinel-returning getters to `Option<T>`** in the same
release. Document at the module level that `None` collapses both
"omitted" and "explicit null" — consumers who need to distinguish have access to
the underlying `Field<T>`.

---

## 5. Method-struct argument visibility: today vs. utopia

### Today

Common arguments are buried on `.arguments()`:

```rust
let mut get = EmailGet::new(&account_id);
get.ids([id]);
get.arguments().fetch_text_body_values(true);   // hidden
get.arguments().fetch_html_body_values(true);   // hidden
get.arguments().max_body_value_bytes(1024);     // hidden
```

The `.arguments()` boundary is a serde implementation artifact — JMAP method
structs split into "common" and "method-specific" args server-side, but consumers
don't care about that boundary, they care about discovering options.

### Utopia

Lift frequently-used arguments to first-class methods on the method struct.
`.arguments()` becomes the escape hatch for rarely-touched fields.

```rust
let mut get = EmailGet::new(&account_id);
get.ids([id])
    .fetch_text_body_values(true)
    .fetch_html_body_values(true)
    .max_body_value_bytes(1024);
```

### Tradeoffs

- **Pro:** discoverable. IDE autocomplete on `EmailGet` shows the option.
- **Pro:** matches how every other Rust builder works.
- **Con:** "frequently-used" requires judgment per method. Each method needs an
  audit of what to lift; we'll get some wrong on the first pass.

### Recommendation

**Lift everything that has a fluent builder method on the underlying arguments
struct.** If it was worth giving a method to in `EmailGetArguments`, it's worth
exposing on `EmailGet`. Mechanical, no per-method judgment needed.

---

## 6. Response extraction: today vs. utopia

### Today

```rust
let mut response = request.send().await?;        // let mut required
let mut result = response.get(&handle)?;          // let mut required
let id = result.take_id();                        // mutating accessor
let emails = result.take_list();                  // mutating accessor
```

Two mutating boundaries. Consumers report this as awkward (the `let mut` on the
response). TODO marks it "explicitly not changing — ownership semantics, correct
as-is."

### Utopia

`Response::get` consumes its slot (it already does — `swap_remove`), so it can
return owned data. The `let mut result` is mostly cosmetic; the real friction is
that `take_id`/`take_list` *look* like getters but mutate.

Two micro-improvements without touching the ownership model:

1. Rename `take_id`/`take_list` → `into_id`/`into_list` and consume `self`
   instead of `&mut self`. Makes the ownership transfer obvious and removes the
   inner `let mut`.
2. Add `Response::take<M>(handle)` returning the fully-extracted value (e.g.
   `Vec<Email>` for `EmailGet`) for the common case.

### Tradeoffs

- **Pro:** removes one of the two `let mut` requirements with no semantic change.
- **Pro:** `into_*` matches Rust convention (`String::into_bytes`,
  `Vec::into_iter`).
- **Con:** every response-extraction call site changes. But this is the most
  mechanical change in the doc.

### Recommendation

**Rename `take_*` → `into_*` and consume `self`.** Don't pretend to fix the outer
`let mut response` — that one is genuinely required by `Response::get`'s
mutation, and changing it would require interior mutability or a fundamentally
different envelope model.

---

## 7. Things explicitly *not* changing

Documented in `TODO.md` under "Explicitly not changing":

- `download(blob_id)` — consumer expected the wrong signature. Current shape is
  correct (BlobId fully identifies a blob; account context is in the URL).
- `changes.created()` returning `&[String]` — matches storage; `.map(String::as_str)`
  is idiomatic.
- Filter type inference requiring an explicit binding — generics limitation. Fix
  would need a less-generic API; not worth it.

These come up in feedback but aren't bugs.

---

## Aggregate scope

If we accept the recommendations above, the pre-1.0 release contains:

1. **Account scoping** (§1) — move helpers to `AccountScope`, drop free
   functions on `Client`. Eliminates 107 `.to_string()` allocations on the way.
2. **Helper layer disposition** (§2) — either A (keep helpers, normalize
   plurality and `_account` variants) or B (delete helpers, sugar the builder
   with `AccountScope::call_one`). Decision pending; the rest of the release
   does not depend on which we pick.
3. **`Id<T>` adoption** (§3) — markers for every object type;
   helper + method-struct + filter signatures take `&Id<T>`.
4. **`Option<T>` getters** (§4) — audit and convert sentinel returns.
5. **Lifted method arguments** (§5) — `.arguments()` becomes the escape hatch.
6. **`take_*` → `into_*`** (§6) — consuming response accessors.
7. **`max_changes: 0` validation** (TODO §4) — return `Error::InvalidArgument`.

This is a single coherent breaking release. It is *much* larger than the five
items in `TODO.md`'s ergonomics list, but doing it incrementally means consumers
re-migrate three or four times. Bundle and ship once.

### What this release does *not* do

- MDN (RFC 9007) and S/MIME (RFC 9219) — see `plans/MDN.md`, `plans/SMIME.md`.
  Add after the API stabilizes; they should be additive, not breaking.
- Optimization items 1, 3, 4 from `TODO.md` (capability_config round-trip,
  CallHandle.call_id, SSE Bytes copy) — documented as accepted trade-offs.
- Reworking the JSON-map backing for CalendarEvent / ContactCard. The two-data-
  model split is intentional (preserves extension properties on round-trip);
  not on the table.

---

## Open questions for review

1. §2: A or B? Pick one — they're mutually exclusive and the rest of the
   release lands either way.
2. `AccountScope`: borrow-based (`&Client`) or owned (`Arc<Client>`)? Today's
   answer is borrow-based. ratatoskr stores clients in long-lived structs; if
   that's painful, we move to `Arc`.
3. `Id<T>` marker naming — do we live with `EmailMarker` ugliness, or rename
   the existing types to something like `email::Identifier` to reuse the
   short name?
4. The `Field<T>` → `Option<T>` collapse at the getter layer — do we want a
   separate `field()` accessor that exposes the three-state version for
   consumers who care, or hide it entirely?
