# bifrost-jmap protocol objects - bug hunt

Scope: `crates/jmap/src/` **excluding** `core/` and `sync/`. That is the
~20 protocol object modules plus `client.rs`, `client_ws.rs`,
`account.rs`, `transport_reqwest.rs`, `lib.rs`, `tests.rs`.

`core/` and `sync/` were read but not edited; where a finding in my scope
is only reachable through a `sync/` call site, that call site is named so
the triage has the whole path.

Everything below is either **BUG** (a wrong thing that a server will act
on), **GAP** (untested/unprotected behaviour that matters), **SMELL**
(works today, easy to hold wrong) or **NIT**.

Tests landed in this pass are listed at the end. Fixes were deliberately
NOT applied.

---

## B1 (BUG, severity: high, live) - every `Mailbox/set update` clears `role` and `shareWith`

**Where:** `crates/jmap/src/mailbox/mod.rs:152-181` (`MailboxPatch`),
`crates/jmap/src/mailbox/set.rs:120` (`role_not_set`),
`crates/jmap/src/core/set.rs:507` (`skip_if_empty_map`).
**Live call sites:** `crates/jmap/src/sync/pim.rs::container_rename`
(~line 751), `container_move` (~line 784).

`MailboxPatch` is `#[derive(Default)]`. Two of its fields use skip
predicates that return `false` for `None`:

```rust
#[serde(skip_serializing_if = "role_not_set")]  role: Option<Role>,
// role_not_set(r) == matches!(r, Some(Role::None))
#[serde(skip_serializing_if = "skip_if_empty_map")] share_with: Option<HashMap<..>>,
// skip_if_empty_map(m) == matches!(m, Some(m) if m.is_empty())
```

So `None` is not skipped; serde emits it as JSON `null`. A
default-constructed patch therefore serialises to:

```json
{"role": null, "shareWith": null}
```

RFC 8620 §5.3: a `null` value in a PatchObject **removes** the property.

**Path to failure.** User renames a folder. `container_rename` does
`set.update(mailbox).name(name)` and sends

```json
{"update": {"mb1": {"name": "Renamed", "role": null, "shareWith": null}}}
```

The server sets `role` to null and empties `shareWith`. If the renamed
mailbox was the Inbox (or Sent/Drafts/Trash/Junk), it loses its role -
which is exactly the value `sync/pim.rs::role_mailboxes` and
`containers_list` use to resolve Sent/Drafts for sending and to map
`FolderRole`. A shared mailbox loses every ACL grant. Neither is
recoverable from the client. `container_move` sends the same two nulls
on top of its own bug (B2).

**Why `MailboxCreate` is fine and `MailboxPatch` is not:** `MailboxCreate`
has a hand-written `SetCreate::new` that installs the sentinels
(`Some(Role::None)`, `Some(HashMap::new())`) the predicates look for.
`MailboxPatch` uses `#[derive(Default)]`, which cannot.

**Proposed fix.** Move the nullable Patch properties to `Field<T>`
(default `Field::Omitted`, `skip_serializing_if = "Field::is_omitted"`),
which is already the pattern `Calendar`/`AddressBook` use and which also
fixes B2 and B3 in the same edit. A narrower fix is a hand-written
`impl Default for MailboxPatch` installing the same sentinels
`MailboxCreate::new` does, but that leaves the type easy to hold wrong
again.

Pinned by `tests.rs::mailbox_wire::an_empty_mailbox_patch_still_clears_role_and_share_with`
and `tests.rs::patch_defaults::patches_that_leak_property_removals`.

---

## B2 (BUG, severity: high, live) - every `Identity/set update` clears `replyTo` and `bcc`

**Where:** `crates/jmap/src/identity/mod.rs:87-108` (`IdentityPatch`).
**Live call site:** `crates/jmap/src/sync/pim.rs::identity_update`
(~line 952).

Same root cause as B1, different predicate: `replyTo` and `bcc` use
`skip_if_empty_list`, which returns `false` for `None`. A default
`IdentityPatch` serialises to `{"replyTo": null, "bcc": null}`.

**Path to failure.** `identity_update` only touches the fields the
`bifrost_types::IdentityPatch` names. A caller who edits just the display
name (`patch.name = Some(..)`, `patch.reply_to = None`) produces

```json
{"update": {"i1": {"name": "Alice", "replyTo": null, "bcc": null}}}
```

RFC 8621 §6 types both as `EmailAddress[]|null`, so the server clears
them. The user's configured reply-to address and auto-Bcc silently
disappear on the next name/signature edit.

**Proposed fix.** As B1: `Field<Vec<EmailAddress>>` on the Patch shape.
Note the fix cannot be "make `skip_if_empty_list` skip `None`" - a
deliberate `reply_to(None)` genuinely means "clear it", and
`IdentityCreate` relies on the current semantics. The two intents need
two states, which is what `Field` is for.

Pinned by `tests.rs::settings_object_wire::an_empty_identity_patch_still_clears_reply_to_and_bcc`.

---

## B3 (BUG, severity: medium, live) - `container_move(container, None)` silently does nothing

**Where:** `crates/jmap/src/mailbox/mod.rs:158-160`
(`MailboxPatch::parent_id` is `skip_serializing_if = "Option::is_none"`),
`crates/jmap/src/mailbox/set.rs:61`.
**Live call site:** `crates/jmap/src/sync/pim.rs::container_move` (~line 795).

`MailboxPatch::parent_id` takes `Option<impl Into<MailboxId>>`, so `None`
is the only way to express "move this mailbox to the top level". But the
field is skipped when `None`, so the intent never reaches the wire.

**Path to failure.** Engine calls `container_move(ContainerId("mb1"),
None)` to un-nest a folder. The request is
`{"update": {"mb1": {"role": null, "shareWith": null}}}` (per B1 the two
nulls are there too). The server reports success, `unwrap_update_errors`
passes, the function returns `Ok(())`, and the mailbox has not moved -
but has lost its role and shares.

Note the asymmetry: `MailboxCreate::parent_id(None)` DOES emit
`"parentId": null` correctly, because Create uses `skip_if_empty_id`
(which returns `false` for `None`). The two halves of the same object
disagree about what `None` means.

**Proposed fix.** `Field<MailboxId>` on `MailboxPatch`, same edit as B1.

Pinned by `tests.rs::mailbox_wire::patching_parent_id_to_none_emits_no_parent_id_at_all`.

---

## B4 (BUG, severity: medium, latent) - `MailboxPatch::acl_set` emits a property name no server has

**Where:** `crates/jmap/src/mailbox/set.rs:112-117`,
`crates/jmap/src/principal/mod.rs:261-284` (serde names) vs `:355-370`
(`Display`).

`ACL` has two incompatible string renderings:

| variant | `Serialize` | `Display` |
|---|---|---|
| `ReadItems` | `mayReadItems` | `readItems` |
| `Administer` | `mayShare` | `administer` |
| `SetSeen` | `maySetSeen` | `setSeen` |
| ... | `may*` | bare verb |

`MailboxPatch::acl_set` builds its dotted patch path with `Display`:

```rust
self.acl_patch.insert(format!("shareWith/{id}/{acl}"), ACLPatch::Set(set));
```

producing `"shareWith/u1/readItems": true`. RFC 8621 §2 names the
`shareWith` sub-properties `mayReadItems`, `mayShare`, ... - so the path
addresses a property that does not exist. A strict server answers
`invalidPatch`/`invalidProperties`; a lenient one creates a junk key.
`MailboxPatch::acl` (whole-map form, same struct) uses the serde name and
is correct, so the two builders on one type disagree.

**Reachability:** `acl_set` has no caller in `sync/` today, so this is
latent. It is a loaded gun for the sharing-aware stage the module header
says is coming.

**Proposed fix.** Build the path from the serde name. Cheapest is an
`ACL::as_str()` returning the `may*` spelling and having both `Display`
and `Serialize` delegate to it; the divergent `Display` vocabulary looks
like a leftover and has no other consumer (`Mailbox::acl_list` returns
typed `ACL`s, not strings).

Pinned by `tests.rs::mailbox_wire::acl_set_builds_a_patch_path_the_server_will_not_recognise`
and `tests.rs::principal_acl_vocabulary::acl_display_does_not_match_the_wire_names`.

---

## B5 (BUG, severity: medium, latent-but-easy-to-trip) - one unknown property fails the whole `Email/get` decode

**Where:** `crates/jmap/src/email/mod.rs:156-158` (the flattened
`headers: HashMap<Header, Option<HeaderValue>>`), `:630-649`
(`Header::parse`).

`Email` collects every key it does not recognise into a `#[serde(flatten)]`
map whose KEY type is `Header`. `Header`'s deserializer calls
`Header::parse`, which returns `None` for anything that is not
`header:<name>[:<form>][:all]`, and the visitor turns that into a hard
serde error.

**Path to failure.** A server includes any property this struct does not
name - a vendor extension (`example.com:snoozedUntil`), a future RFC
property, or a JMAP extension the crate has not modelled - in an
`Email/get` response. `Response::get::<EmailGet>` returns
`Error::ResponseDecode`, which `sync/error.rs` maps to
`Protocol(ParseFailed)` / `ProviderContractViolation`. Not one property
is lost: the entire batch of hydrated emails is.

This is mitigated in practice because the sync layer always sends an
explicit `properties` list and RFC 8620 §5.1 says the server MUST honour
it. It is not mitigated for `Email/parse` (whose `properties` is
optional), for any future caller that omits `properties`, or for a server
that is merely sloppy.

**Proposed fix.** Give the flattened map a key type that cannot fail -
either reuse `Property` (which already has an `Other(String)` catch-all,
except that it *also* propagates `Header::parse` failure and would need
the same treatment), or attach a `deserialize_with` to the flattened
field that drops keys `Header::parse` rejects. A one-line alternative is
to make `Header::parse` fall back to `Header { name: value, form: Raw,
all: false }`, but that changes `Display` round-tripping and would let
non-header keys masquerade as headers.

Pinned by `tests.rs::email_object_decode::one_unknown_property_fails_the_entire_email_decode`
(with the positive control next to it).

---

## B6 (BUG, severity: low, latent) - `Role` cannot be decoded from a non-borrowable string

**Where:** `crates/jmap/src/mailbox/mod.rs:385-404`.

```rust
match <&str>::deserialize(deserializer)?.to_ascii_lowercase().as_str() { ... }
```

`&str`'s `Deserialize` only accepts `visit_borrowed_str`. Two real inputs
fail with `invalid type: string "inbox", expected a borrowed string`:

1. Any `serde_json::from_value` path (owned `Value` cannot lend `&'de`).
2. `serde_json::from_str` where the JSON string contains an escape -
   serde_json then has to unescape into a scratch buffer and calls
   `visit_str`, not `visit_borrowed_str`. `"inbox"` is a legal
   encoding of `"inbox"` and fails.

The whole `Mailbox` (hence the whole `Mailbox/get` response) fails, not
just the role.

Today's main path survives by luck: `Response::get` uses
`serde_json::from_str` over a `RawValue`, and role strings are
unescaped ASCII. But an `x-` role containing any escaped character, or
any future use of `from_value` on a `Mailbox`, trips it. Every other
hand-written deserializer in the crate (`Header`, `Property`,
`SetErrorType`, `define_open_property_enum!`) uses a `Visitor` with
`visit_str` or goes through `String`; `Role` is the outlier.

**Proposed fix.** `String::deserialize(deserializer)?` (one extra
allocation on a path that already allocates for `to_ascii_lowercase`), or
a `Visitor` implementing `visit_str`.

Pinned by `tests.rs::mailbox_wire::role_cannot_be_decoded_when_the_string_is_not_borrowable`.

Related, separately: the same deserializer lower-cases before matching,
so `Role::Other` does not round-trip byte for byte
(`"x-MyRole"` decodes to `Other("x-myrole")` and re-serialises
lower-cased). Pinned by `unknown_roles_survive_as_other_but_are_lower_cased`.

---

## B7 (BUG, severity: medium, latent) - the SSE stream's error handling breaks out of the wrong loop, and the parser then desynchronises

**Where:** `crates/jmap/src/event_source/stream.rs:68-112` and
`crates/jmap/src/event_source/parser.rs:40-47, 52-55, 148-151`.

```rust
loop {
    for event_result in parser.by_ref() {
        match event_result {
            ...
            Err(err) => { yield Err(err); break; }   // breaks the FOR
        }
        continue;                                     // no-op, last stmt
    }
    if let Some(result) = stream.next().await { parser.push_bytes(bytes); continue; }
    else { break; }
}
```

Every `break` inside the `match` leaves the **inner `for`**, not the
outer `loop`. Two consequences:

1. The intended "terminate the stream on a decode error" never happens.
   The stream yields the error, pulls more bytes and carries on. A server
   emitting malformed `data:` payloads produces an infinite error stream
   instead of a terminal failure.
2. Worse, the `break` leaves the parser mid-buffer. `EventParser::push_bytes`
   overwrites `self.bytes` **without resetting `self.pos`**, and nothing
   checks the `needs_bytes()` precondition the parser exposes for exactly
   this. The next poll resumes at a stale offset inside the *new* frame.

**Path to failure (2).** Frame A is `"data: one\n\ndata: two\n\n"` (22
bytes). One event is yielded at `pos = 11`. Something breaks the loop.
Frame B `"data: three\n\n"` (13 bytes) is pushed. Parsing resumes at index
11 of frame B, i.e. at its two trailing newlines: `two` is lost, `three`
is never seen, and a bogus empty `StateChange` event is emitted instead.
If frame B were shorter than 11 bytes, `bytes.get(self.pos..)` returns
`None`, `next()` returns `None` **without clearing `self.bytes`**, and the
loop spins pulling and discarding frames forever.

**Reachability:** `Client::event_source` has no caller - the Account layer
uses the WebSocket push path - so this is latent. It is the entire
correctness of the SSE path if it is ever switched on.

**Proposed fix.** Two independent edits: (a) label the outer loop and
`break 'outer`, or restructure so the error path returns; (b) make
`push_bytes` either assert `needs_bytes()`, append to the unconsumed
remainder, or reset `pos` and drop the old buffer explicitly. (b) alone
makes the parser safe against any caller.

Pinned (parser half only, since the stream half is async and this crate
has no async test harness - see G1) by
`event_source/parser.rs::tests::push_bytes_over_a_partially_consumed_buffer_resumes_at_a_stale_offset`.

---

## B8 (BUG, severity: low, latent) - `VacationResponsePatch` setters cannot clear a property

**Where:** `crates/jmap/src/vacation_response/mod.rs:84-113`,
`crates/jmap/src/vacation_response/set.rs:5-42`.

The mirror image of B1/B2 on the same type family. Every
`VacationResponsePatch` setter takes an `Option`, but the fields are
`skip_serializing_if = "Option::is_none"`, so `subject(None)`,
`to_date(None)`, `text_body(None)`, `html_body(None)`, `from_date(None)`
emit nothing at all instead of `null`. The Create shape uses
`skip_if_empty_str` / `skip_if_zero_date` and gets it right.

The tell that this is a known-broken API: `sync/pim.rs::vacation_set`
does not use the setters for the clearing case at all - it calls
`patch.null_property("subject")` etc. by hand for all five nullable
properties. So the bug is worked around rather than fixed, and the next
caller will not know to work around it.

**Proposed fix.** `Field<T>` here too, then delete the `null_property`
workaround in `vacation_set`.

Pinned by `tests.rs::settings_object_wire::vacation_patch_setters_cannot_clear_a_property`.

---

## B9 (BUG, severity: low, latent) - the same default-patch leak on `PushSubscriptionPatch` and `ParticipantIdentityPatch`

Same mechanism as B1/B2, no live call site today.

- `PushSubscriptionPatch::default()` -> `{"types": null}`. RFC 8620 §7.2
  reads a null `types` as "notify me about **every** data type", so an
  update that only sets `verificationCode` also widens the subscription
  to everything.
- `ParticipantIdentityPatch::default()` -> `{"sendTo": null}`.

And two Create shapes whose `SetCreate::new` forgot the sentinel the
predicate needs, so a fresh create carries a stray null:

- `AddressBookCreate::new(_)` -> `{"name": null}` (`name` is a
  non-nullable String in RFC 9610; the server should answer
  `invalidProperties`). Harmless only because every caller sets a name.
- `ParticipantIdentityCreate::new(_)` -> `{"sendTo": null}`.

The whole family is pinned as one table in
`tests.rs::patch_defaults` (three tests: the shapes that are correct, the
four patches that leak, the two creates that leak). That test is the
regression net for the B1/B2/B3/B8/B9 fix.

---

## G1 (GAP) - this crate cannot host an async test at all

`crates/jmap/Cargo.toml` has **no `[dev-dependencies]` section**. `tokio`
is an optional *runtime* dependency without the `macros` feature, and the
workspace pins `futures = { default-features = false }`, so
`futures::executor::block_on` is not available either.

Consequence for this pass: the deliberate new capability the brief
described - a byte-level protocol transcript over `tokio::io::duplex` -
is not reachable. Neither is a `StubTransport` implementing the crate's
own `HttpTransport` trait, which is the higher-value double here:
`Client::with_transport(stub, session)` + `Account::call` would pin the
request envelope (`using` array construction, `methodCalls` tuple shape,
`accountId` injection, `Response::get` call-id matching, method-error
routing) end to end, in-process, with no network and no listener.

I did not add the dependency (the brief forbids editing `Cargo.toml`, and
the manifest is shared). **Ask:** add to `crates/jmap/Cargo.toml`

```toml
[dev-dependencies]
tokio = { workspace = true, features = ["macros", "rt"] }
```

which is what `crates/caldav` already does. With that one line the
transport-stub tests become writable and I would expect them to be worth
more than everything else in this pass combined - the request envelope is
currently entirely unproven.

(A hand-rolled `block_on` built on `std::task::Wake` would avoid the
dependency, but shipping a bespoke executor in a test module to dodge a
one-line manifest change is the wrong trade.)

---

## G2 (GAP) - `#[non_exhaustive]` wire enums with no `#[serde(other)]` arm

`DataType`, `Role`, `AlertTrigger` and `SetErrorType` all have a
catch-all: an unknown wire value degrades. These do not, and an
unrecognised value fails the decode of the whole containing response:

| type | file | RFC-defined values |
|---|---|---|
| `UndoStatus` | `email_submission/mod.rs:132` | pending / final / canceled |
| `Delivered` | `email_submission/mod.rs:155` | queued / yes / no / unknown |
| `Displayed` | `email_submission/mod.rs:168` | unknown / yes |
| `AlertAction` | `calendar_event/mod.rs:69` | display / email |
| `RelativeTo` | `calendar_event/mod.rs:78` | start / end |
| `IncludeInAvailability` | `calendar/mod.rs:183` | all / attending / none |
| `NotificationType` | `calendar_event_notification/mod.rs:75` | created / updated / destroyed |
| `principal::Type` | `principal/mod.rs:288` | individual / group / resource / location / domain / list / other |

Each is marked `#[non_exhaustive]`, which is the crate declaring that the
value set will grow - but the deserializers refuse to grow with it. The
calendars draft in particular is at -26 and still moving; a server
shipping a newer `alerts[].action` fails every `CalendarEvent/get`.

This is a judgement call rather than an outright bug (the RFC values are
closed today), so it is pinned as-is, with the divergence made explicit,
in `tests.rs::wire_enums_without_a_catch_all`. If the answer is "add
`#[serde(other)] Unknown` everywhere", `AlertTrigger` is the model.

Related, and slightly worse: `DataType::Other` is a deserialize-only
catch-all that nonetheless **serialises**, as the literal `"Other"`.
Anything that decodes a server's type name and echoes it back - the
`WebSocketPushEnable.dataTypes` union built in `sync/push.rs`, a
`PushSubscription.types` round-trip - will ask the server to subscribe to
a data type called `Other`. Pinned by
`tests.rs::data_type_wire::other_serialises_as_a_literal_that_is_not_a_jmap_type`.

---

## G3 (GAP) - a malformed capability object silently disables the feature

**Where:** `crates/jmap/src/core/session.rs:84-132` (`try_cap!`). Out of
my edit scope; reported because the failure is invisible.

`try_cap!` falls back to `Capabilities::Other(value)` on **any** parse
failure. `WebSocketCapabilities` has no `#[serde(default)]` and both of
its fields are required, so a server that advertises

```json
"urn:ietf:params:jmap:websocket": {"url": "wss://..."}
```

(no `supportsPush`) produces a session where `websocket_capabilities()`
is `None`. `sync/capabilities.rs` reads that as "no push", the account
opens with `push: None`, and nothing anywhere reports why. The same
shape applies to `BlobCapabilities` (has defaults, safe),
`SieveCapabilities` (has defaults, safe) and `CoreCapabilities` (has
defaults - which is why a `{}` core capability decodes to all-zero limits
rather than falling to `Other`).

**Proposed fix.** `#[serde(default)]` on `WebSocketCapabilities` (absent
`supportsPush` == false is the natural RFC 8887 reading), and/or make the
`Other` fallback observable.

Pinned by `tests.rs::session_capability_fallbacks`.

---

## G4 (GAP) - `EmailPatch`'s wholesale setters do not clear their own dotted paths

**Where:** `crates/jmap/src/email/set.rs:206-236`.

`mailbox_id(id, set)` and `keyword(kw, set)` correctly null out
`self.mailbox_ids` / `self.keywords` so a path and its parent property
never co-occur (RFC 8620 §5.3 forbids it). The reverse is not true:
`mailbox_ids([..])` and `keywords([..])` do not clear the paths a
previous `mailbox_id` / `keyword` installed, so

```rust
patch.keyword("$flagged", true);
patch.keywords(["$seen"]);
```

emits both `"keywords"` and `"keywords/$flagged"`. No live call site
mixes the two forms today. Pinned by
`tests.rs::email_set_patch_shapes::a_wholesale_setter_does_not_clear_previously_set_paths`.

---

## S1 (SMELL) - `CalendarEventPatch` / `ContactCardPatch` reuse the Create setters verbatim

**Where:** `crates/jmap/src/calendar_event/set.rs` (`ce_setters!` applied
to both `CalendarEventCreate` and `CalendarEventPatch`),
`crates/jmap/src/contact_card/set.rs` (`cc_setters!`, same).

`calendar_id(id, false)` writes a **nested** object
`{"calendarIds": {"cal-1": null}}`. On a create that is at worst odd. On
a `/set update` it is a wholesale replacement of `calendarIds` with a map
containing a null, not the `"calendarIds/cal-1": null` dotted path
RFC 8620 §5.3 asks for - so it also drops every calendar membership the
caller did not name, which is exactly the class of bug the
`EmailPatch::submitted_to_sent` doc comment was written to prevent.

The existing test `tests.rs::patch_object_null_semantics` pins the Create
side, and per the standing bug-hunt rule an existing passing test wins,
so I have **not** touched it - I have only added the Patch-side
observation as
`tests.rs::calendar_event_patch_nesting::patch_calendar_id_nests_instead_of_using_a_dotted_path`.
The type-state split exists precisely so the two shapes can differ;
applying one macro to both throws it away. Whoever wires
`calendar_ops.rs` membership edits should split the macro first.

---

## S2 (SMELL) - `Client::with_transport` produces a client that cannot refresh its session

**Where:** `crates/jmap/src/client.rs:277-304`.

`with_transport` sets `session_url: String::new()`. `refresh_session()`
on such a client issues `GET ""`. Harmless today (only the reqwest path
constructs a session URL, and nothing calls `refresh_session`), but the
constructor is the documented custom-transport entry point and it hands
back a half-functional object. Either take the session URL as a
parameter or make `refresh_session` return `Error::InvalidUrl` when it is
empty.

Adjacent: `ClientBuilder::connect` builds `format!("{url}/.well-known/jmap")`
with no trailing-slash normalisation, so a configured base URL ending in
`/` yields `//.well-known/jmap`.

---

## S3 (SMELL) - `send_ws` / `enable_push_ws` / `disable_push_ws` send an empty frame on encode failure

**Where:** `crates/jmap/src/client_ws.rs:250-257, 277-283, 295-300`.

```rust
Message::text(serde_json::to_string(&frame).unwrap_or_default())
```

`unwrap_or_default()` turns an encode failure into an **empty text
frame** rather than an error. The payloads involved cannot currently fail
to serialise (`method_calls` is already a `Value`, `using` is
`&'static str`s, the push frames are two fields), so this is not a live
bug - but the crate's own `Error::RequestEncode` variant exists for
exactly this and the doc comment on it says outbound encode sites must
use it explicitly rather than relying on `?`. These three sites do
neither.

---

## S4 (SMELL) - the SSE parser's `data` accumulation is uncapped

**Where:** `crates/jmap/src/event_source/parser.rs:87, 134` (the
`MAX_EVENT_SIZE` guards) vs `:108-113` (the `data` join).

The 1 MiB guard bounds a single `field`/`value` pair. `self.result.data`
accumulates across every `data:` line of one event with no bound at all,
so a stream of 1 MiB-minus-epsilon `data:` lines with no blank line grows
the buffer without limit. Also, the guard errors without clearing the
offending field, so the parser cannot resynchronise (see B7). Both pinned
in `event_source/parser.rs::tests`.

---

## S5 (SMELL) - the SSE parser's `id` field concatenates instead of replacing

**Where:** `crates/jmap/src/event_source/parser.rs:104-106`.

`self.result.id.extend_from_slice(&self.value)`. The SSE spec says the
`id` field *sets* the last-event-id buffer, so a second `id:` line in one
event replaces the first. Here `"id: 1\nid: 2\n\n"` yields id `"12"` -
which is then what `Last-Event-ID` resumption would send back. Pinned by
`event_source/parser.rs::tests::repeated_id_fields_concatenate_instead_of_replacing`.

Two smaller deviations in the same state machine: `Init` silently ignores
a leading space (SSE treats it as the first character of a field name),
and a field name is capped but a comment line is not.

---

## S6 (SMELL) - blob download URL percent-encoding leaves `&` and `=` alone

**Where:** `crates/jmap/src/blob/download.rs:12-23` (`PATH_SEGMENT`),
`upload.rs:37-48` (same set).

The set encodes `/ ? # % { } < > " ` ` and space, but not `&` or `=`.
RFC 8620's download URL template routinely puts `{name}` and `{type}` in
a **query** position (the session fixtures in this repo use
`.../{name}?accept={type}`), and `name` comes from an attachment
filename, i.e. it is attacker-controlled. A filename containing `&`
appends parameters to the download request. `?` is encoded so a new query
string cannot be started, which caps the impact at "inject extra params
into an existing query" - but the set should just include `&` and `=`.

---

## N1..N6 (NITs)

- **N1** `Address.parameters` (`email_submission/mod.rs:127`) has no
  `skip_serializing_if`, so every envelope address serialises
  `"parameters": null`. Legal per RFC 8621 §7.1, just noise on every
  submission. Pinned in `email_submission_wire::envelope_address_parameters`.
- **N2** `Thread` (`thread/mod.rs:14-18`) requires both `id` and
  `emailIds`; a `Thread/get` with a partial `properties` projection fails
  the decode. Nothing does that today. Pinned in
  `misc_mail_object_decode::thread_requires_both_properties`.
- **N3** `principal::Property::ShareWith = 14` - `Principal` has no
  `shareWith` property in RFC 9670 (it has `accounts`); the variant looks
  copy-pasted from `Mailbox::Property`.
- **N4** `contact_card::query::Filter::Nickname` serialises as
  `"nickname"`. I believe RFC 9610 §2.3.1 spells the filter condition
  `nickName` (matching the JSContact `nicknames` property), but I could
  not verify the RFC text offline. Worth one grep of the spec - it is a
  one-character fix if I am right and a silently-ignored filter if I am
  not. Deliberately **not** pinned by a test, since I would be pinning a
  guess.
- **N5** `URLPart::parse` accepts `"{{a}"` (a second `{` while already in
  a parameter is silently swallowed). Malformed input that decodes
  anyway.
- **N6** `Display for Header` (`email/mod.rs:652-659`) forwards the
  formatter to `self.name.fmt(f)`, so a `{:>20}` on a `Header` pads the
  name rather than the whole token. Cosmetic; `Header` is only ever
  `to_string()`d.

---

## Cross-cutting observation: the skip-predicate family is the single defect

B1, B2, B3, B8 and B9 are all one design problem:
`skip_if_empty_str` / `_list` / `_map` / `skip_if_zero_date` /
`skip_if_empty_id` all encode "`None` means send `null`", and rely on a
hand-written `SetCreate::new` to install a `Some(<empty>)` sentinel so the
*default* is still skipped. `#[derive(Default)]` on the Patch shapes
cannot install a sentinel, so every Patch type that uses one of those
predicates leaks a property removal, and every Patch type that uses
`Option::is_none` instead loses the ability to clear.

`Field<T>` is already in the crate, already documented as "use instead of
`Option<Option<T>>`", and `Calendar` / `AddressBook` / `Quota` already
use it correctly for exactly these properties. Migrating the remaining
Patch shapes to `Field<T>` fixes five findings with one mechanical edit
and deletes the `null_property` workaround in `sync/pim.rs::vacation_set`.
`tests.rs::patch_defaults` is the regression net for that edit: it asserts
the correct shapes stay `{}` and lists the leaking ones explicitly, so
flipping each one over is a visible, reviewable diff.

---

## Tests landed

All in files this pass owns. Every test pins behaviour **as it exists
today**; the ones documenting behaviour I believe is wrong say so in a
comment directly above them ("BUG, documented rather than endorsed" /
"Documented, not endorsed").

`crates/jmap/src/tests.rs` (new modules, appended):

- `method_name_and_capability_table` - `M::NAME` and `M::Cap::URI` for
  every method struct in the crate (~60 methods). Nothing else covered
  these; a typo in either is a runtime `unknownMethod` /
  `unknownCapability`, never a compile error. Also pins the
  non-obvious placements: Identity under `submission`, ShareNotification
  under `principals`, Blob/copy under `core`, `*/parse` under the
  `:parse` sub-capabilities.
- `email_header_property_grammar` - the `header:<name>[:<form>][:all]`
  grammar both directions, all seven forms, the malformed cases, and
  `Property` serde including `Other`.
- `email_object_decode` - `Email` decode, the header-form aliases, the
  flattened header map, and B5.
- `email_query_wire` - every `Email/query` filter condition's wire name,
  the `header` two-element form, UTCDate formatting, comparator
  flattening (incl. `hasKeyword`'s extra field), `collapseThreads`,
  filter-operator nesting.
- `email_set_patch_shapes` - dotted-path null/true semantics, the
  path-clears-wholesale rule, G4, the raw/null escape hatches.
- `mailbox_wire` - role wire names + case folding, B1, B3, B4, B6, the
  create sentinels, create-id references, `Mailbox` decode incl. rights
  defaulting.
- `email_submission_wire` - envelope/parameter shapes incl. the RFC 4865
  `holduntil` form, `#c0` create-id references on both onSuccess
  arguments, `undoStatus` patch, delivery-status decode.
- `settings_object_wire` - B2, B8, the `null_property` workaround,
  vacation decode, Sieve activation references, `SieveScript/validate`
  error decode.
- `patch_defaults` - the cross-cutting table (correct patches / leaking
  patches / leaking creates).
- `set_error_vocabulary` - all 25 known `SetErrorType` codes both
  directions plus the `Other(code)` gate-5 invariant and `Display`.
- `wire_enums_without_a_catch_all` - G2.
- `data_type_wire` - Display/Serialize agreement for every `DataType`,
  `MDN` casing, and the `Other` serialisation hazard.
- `session_capability_fallbacks` - G3 plus the `Other` passthrough.
- `url_template_parsing` - `URLPart::parse` happy paths and all four
  rejection cases, plus the blob parameter set.
- `blob_management_wire` - RFC 9404 `Blob/upload` create shape and
  `DataSource` concatenation, `Blob/get`'s named-vs-dynamic field split,
  `Blob/lookup` round trip.
- `quota_field_three_state`, `address_book_wire`, `calendar_wire` - the
  `Field<T>` three-state contract where it IS implemented correctly, plus
  RFC-shape decodes (`mayRSVP` casing, rights defaulting).
- `calendar_event_patch_nesting` - S1, `set_property` dotted paths,
  Get/Set argument flattening.
- `misc_mail_object_decode` - N2, `SearchSnippet/get` request shape,
  `Email/import` `iN` create-id keying.
- `push_subscription_wire` - the non-account-scoped `accountId` omission.
- `principal_acl_vocabulary` - the RFC 8621 `shareWith` property names,
  and B4's `Display`/`Serialize` divergence across all ten variants.

`crates/jmap/src/event_source/parser.rs` (appended to the existing
`mod tests`): S5, B7's parser half, S4's two halves.

---

## Not done, and why

- **A transport-level test double.** The highest-value thing in scope and
  blocked on G1 (no `[dev-dependencies]`, so no async test can compile in
  this crate). This is the one item I would put at the top of the
  follow-up list: `Client::with_transport` + a stub `HttpTransport` would
  pin the request envelope (`using` construction and de-duplication,
  `methodCalls` tuple encoding, `accountId` injection via
  `JmapMethod::set_account_id`, `CallHandle` -> `Response::get` matching,
  method-error routing, `send_methods` tuple extraction) and none of that
  has a single test today.
- **`blob/download.rs` URL construction.** The templating + percent-encoding
  logic (S6) is inside an `async fn` that immediately calls the transport,
  so it is untestable without either G1 or extracting a pure
  `build_download_url(&[URLPart], &BlobRef) -> String`. That extraction is
  a refactor, and the brief splits tests from fixes, so I left it. It is a
  five-line change and would make S6 verifiable.
- **`client_ws.rs` frame handling beyond the subprotocol check.** The
  `WebSocketMessage_` decode is partly covered by the existing
  `deserializes_single_type_state_change_frame`; the close/error/binary
  arms are inside the `async_stream::stream!` and need G1.
- **`principal/availability.rs`, `principal/query.rs`,
  `share_notification/query.rs`, `sieve/query.rs`,
  `calendar_event_notification/query.rs`, `quota/query.rs`.** Read for
  bugs (none found beyond G2's `NotificationType`), not covered by new
  tests - they are thin filter/comparator enums structurally identical to
  the ones now pinned in `email_query_wire` and `tests.rs`'s existing
  `query_filter_serialization`, and I judged a second copy of the same
  table lower value than the findings above.
- **N4 (`nickName` vs `nickname`).** Needs the RFC 9610 text, which I do
  not have offline. Flagged, not pinned - pinning a guess is worse than
  leaving it open.
- **The `Email::size()` / `has_attachment()` / `EmailBodyPart::size()`
  sentinel collapse.** Already tracked as an open decision in
  `plans/jmap/DEFERRED.md` and `plans/jmap/API.md`; not re-litigated here.
