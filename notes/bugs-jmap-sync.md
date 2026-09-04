# Bug hunt: bifrost-jmap sync (Account impl)

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/jmap/src/sync/` - cursor envelope, inventory/changes/hydration, push,
mutation pipeline, recovery taxonomy.

## Confident defects

### 1. `calendar_ops.rs` fails an entire page on one unrepresentable event, and never reconciles ids - the `Page` loss-lane contract is unimplemented for calendars

`get_events` (`crates/jmap/src/sync/calendar_ops.rs`, ~line 286) maps every
returned event through `event_from_jmap` and collects into `Result<Vec<_>>`.
`event_from_jmap` deliberately errors on modified recurrence overrides,
multiple/excluded recurrence rules, unknown participant roles or statuses (per
the documented "fail rather than drop" policy). Consequence: a single exotic
event anywhere in the queried window makes the whole `events_in_range` /
`event_search` call fail as `Unsupported` - permanently, since the event
doesn't go away. `events_in_range` and `search` return `failed_ids:
Vec::new()` unconditionally. Contrast `contacts.rs::reconcile_cards`, which was
explicitly built so "the consumer preserves the row instead of reading absence
as a deletion," and `reference/sync.md`'s Page-lane definition ("resources the
provider fetched but could not materialize" ride `failed_ids`). Calendars are
missing both halves: no per-item degradation for conversion failures, and no
submitted-id reconciliation at all (an id the server answers in neither `list`
nor `notFound` silently vanishes from the page - the exact shape
`reconcile_cards` and `reconcile_hydration` exist to prevent). This is the most
concrete contract mismatch in the scope; the fix is the same reconciliation
structure contacts already have, with conversion failures routed to
`failed_ids` instead of aborting.

### 2. No forward-progress guard in the `*/changes` loops - a misbehaving server produces an unbounded request loop

`changes.rs::email_changes` / `mailbox_changes` loop while `hasMoreChanges`,
re-calling with `since_state = new_state`. Nothing checks that `newState`
actually moved. A server that answers `hasMoreChanges: true` with `newState ==
sinceState` (or an oscillating pair) drives an infinite loop of wire requests,
each emitting a checkpoint-bearing batch onto the engine at full speed. The
inventory walk has the analogous hole with a weaker trigger: `queryState` is
pinned, but a server that keeps echoing the same trailing ids under a stable
`queryState` never yields the empty page and loops forever. The crate elsewhere
treats exactly this class as `Protocol(ContractViolation)` - and
`error.rs::terminated_contract_violation` is documented as kept precisely for
"the next stream that meets one." A `newState == since_state && hasMoreChanges`
check terminating through it closes the hole cheaply. Latent (needs a broken
server), but the cost is unbounded and invisible.

### 3. Dead routing helper: `JmapAccount::mail_for_object_id` has zero callers

`account.rs` line 157. Masked by the crate-root `#![allow(dead_code)]` that
`error.rs` explicitly calls out as "the wrong default HERE" for boundary code.
Its sibling `foreign_account_id_for_object` (line 302) is production-dead
too - referenced only by tests. Not harmful, but a dead routing function at
exactly the boundary where a primary-fallback regression would live is the kind
of hole the module's own `#![warn(dead_code)]` discipline (applied in error.rs
only) exists to catch. Either delete them or extend the opt-back-in to
account.rs.

### 4. `send_message` resolves Drafts and Sent with two separate full `Mailbox/get` round trips per send

`pim.rs::send_message` calls `role_mailbox(Drafts)` then `role_mailbox(Sent)` -
each is an uncached `fetch_mailboxes` listing every mailbox with rights.
`draft_send` already has the batched `role_mailboxes(&[Sent, Drafts])` helper
for exactly this reason ("Batching matters because ... `fetch_mailboxes` is an
uncached round trip"); `send_message` simply doesn't use it. Two wasted
full-list fetches on the hottest write path. Confident, trivial fix.

### 5. `identities_list` fabricates `is_default: idx == 0`

JMAP has no default-identity concept (the crate itself refuses
`identity_update(is_default)` as Unsupported), and `Identity/get` order is
server-arbitrary. Reporting the first row as the default hands the consumer a
fabricated fact it may persist and act on. Should be `false` (or the shared
type's "unknown" representation if it has one).

## Suspected / lower confidence

### 6. `push_subscribe` with zero mappable scopes returns the wrong error kind

`push.rs::subscribe` errors with `Unsupported(PushSubscribe)` when `data_types`
maps to nothing. The types contract explicitly models the all-rejected case
("the handle is absent when no scope was accepted"), and per-scope failures
already ride the failed lane in the mixed case. The error's *kind* is the real
problem: `Unsupported(PushSubscribe)` claims the account has no push at all,
when the account advertised `PushCapability::InProcess` and only these scopes
are unmappable. An engine or consumer keying off that kind could wrongly
downgrade push wholesale. Legal per the letter of the batch contract (`Err` =
nothing subscribed), but a misleading classification.

### 7. `contact_update` address-book move can silently leave the contact in two books

`contacts.rs::update`: when `patch.address_book_id` is set, it reads the
current card to learn the old book. If `get_cards` returns no card (the
unanswered-id lane - a transient the module itself documents as "not a
deletion"), `current_address_book` is `None` and the patch only *adds* the new
book membership without clearing the old. Narrow, transient-triggered, silent.
Failing the update when the read didn't materialize the card would be the
honest behavior.

### 8. `scope_lifecycle` spurious rename and lost create

Emits a spurious `Renamed { old_name: "" }` for an updated mailbox absent from
the names map, and drops a `Created` event when the mailbox vanishes between
`Mailbox/changes` and the follow-up `Mailbox/get` while still committing the
new state (the create is lost forever to the lifecycle stream). Both benign
under the current engine (account-wide cursor shapes create no per-folder
cursor), but worth knowing they're load-bearing on that engine policy.

### 9. `move_thread` / `delete_thread` re-resolve the thread between the two legs

Each `patch_mailbox_membership` leg runs its own `Thread/get`, so a message
delivered to the thread between the add-to-target and the remove-from-source
legs is removed from the source without ever having been added to the target -
it just loses the source membership. The two-leg non-atomicity itself is the
documented cross-provider shape, but the double-resolve widens the window;
resolving once and reusing the id list for both legs would shrink it to a
single `Email/set` pair over a fixed set.

### 10. Calendar `search` computes totals before its client-side filter

`calendar_ops.rs` ~line 262: `estimated_total` and `next_cursor` are computed
before the client-side `calendar_id` filter, so totals overcount and a page can
come back empty with a live cursor. Same class as the (deliberate, documented)
post-filter in `events_in_range`; fine mechanically, but the total is wrong
when a calendar filter is supplied. Similarly `pim::search` silently drops
result emails lacking `threadId` with no `failed_ids` entry.

### 11. Mail search never covers shares

`search`/`search_messages` run only against the primary account; foreign
accounts, which sync and hydrate fully, are invisible to search. Consistent
with implementation, but it isn't in `reference/jmap.md`'s Known limitations
list - a consumer reading the qualified-id story would reasonably expect shared
mail to be searchable. Doc gap at minimum, product gap at most.

### 12. `filters_list` downloads script blobs serially

N+2 round trips for N scripts; the only other concurrency-capable spot in the
crate (foreign probes) bounds by `maxConcurrentRequests`, and the same
treatment would apply here.

## Checked and found sound

The load-bearing machinery holds up well against its documentation: the cursor
envelope (v1/v2 refusal split, mis-keyed-row vs schema-drift classification),
state-cache CAS discipline, foreign object/folder namespace qualification
(inventory, changes, hydration, blob, thread doors all agree,
unregistered-foreign ids stay literal everywhere), `reconcile_hydration`'s
closed per-item accounting, the mutation pipeline's per-owner batching and
starvation bound, the push reader's bounded setup awaits / keepalive /
backoff-reset-on-traffic logic, `close()` teardown bounds, the open-time
foreign probe concurrency clamp, and the error-translation table all match
`reference/jmap.md` claim for claim. `contact_search`'s pref reading (`pref ==
1` only) versus RFC 9553 pref semantics (1-100 ranking) is the only spec nit
there.
