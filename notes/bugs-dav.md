# bifrost-caldav / bifrost-carddav bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/caldav/` and `crates/carddav/`,
hunted together because they share a DAV surface. Read-only review; no tests were run. Findings are
unverified work material.

The hunter confirmed the `status_line` unification (commit 77df77d) landed cleanly on both sides:
both `parse.rs` files import from `bifrost_net` and no local copy survives.

## How to read this document (triage pass 2026-08-23)

This is an unverified hunt ledger, not a work queue. Its findings are mixed in kind and were
written under one heading level with no marking, which is exactly the shape that let an earlier
loop launder an aesthetic judgment into a mandate to delete published API. Every finding below now
carries a category on its own line directly under its heading (or inline, for bullets):

- **C1 live defect** - the code produces a wrong answer, loses data, hangs, or has a security hole
  today. Observable by a user. Work these.
- **C2 latent defect** - correct today, but an unhandled case will silently misbehave when it
  arrives. A real bug with a fuse on it. Work these.
- **C3 refactor opinion** - duplication, file length, cost, awkward abstraction. Nothing
  misbehaves. Backlog, not a bug.
- **C4 product decision** - whether an API should exist, whether a surface should be reshaped.
  Not an engineering question. The repository owner decides; the loop must never act on one
  unilaterally.
- **STALE** - no longer reproduces against the current tree. Kept in full, with the reason.

A second marker, **PUBLISHED SURFACE**, is orthogonal to the category. It means the finding's
proposed remedy would remove, rename, or reshape a published item or a published behavior. Those
need the owner's sign-off before anyone touches them, regardless of category, and regardless of how
confident the argument reads.

Categories were checked against the tree on 2026-08-23; where the check changed the picture, the
marker line says so. No finding text was altered, compressed, or removed.

The published-surface findings in this document, collected: the recurrence-override EventId
contract, the single-collection cursor scope, the CardDAV phantom address book, the
silently-ignored CalDAV calendar move, and the `bifrost-dav` collapse proposal.

Accepted residual from round 1, on the record: the credential-origin allowlist makes a request to
an origin outside the trusted set fail locally rather than silently going out unauthenticated. A
consumer whose server names resource hrefs on a *third* origin - one that is neither the configured
base nor a discovered home - now gets a hard local error where it previously got a credential leak.
That is the intended trade. It is a behavior change, not an API change: no published item was
removed, renamed, or reshaped.

Highest severity, ahead of its position in the document: **the recurrence-override EventId finding
destroys an entire recurring series on an instance delete, verified against the current tree (no
`#` guard exists anywhere in either crate).** It is worse than a document ordered by discovery
makes it look.

## Multistatus hrefs are resolved against the configured base URL, not against the request URI

**C2 latent defect. Found 2026-08-23 by the round-1 fix-and-commit stage, not by the original
hunt.** Every `resolve_href` / `resolve_hrefs` call site in both crates passes `self.base_url` as
the base: `list_calendars_for_operation`, `list_events_listing`, the multiget and sync-collection
readers, and the CardDAV equivalents. RFC 4918 makes a multistatus `href` relative to the *request
URI*, not to whatever root the account happens to be configured with. The two coincide for the
common deployment, where the base URL is the DAV root and servers emit absolute-path hrefs, which
is why this has never bitten.

It acquires a fuse in this round. The credential-origin allowlist now deliberately supports a
calendar or address book home on a *different origin* than the configured base - that is the
legitimate deployment the allowlist was written not to break. A PROPFIND against such a home whose
response carries relative hrefs will resolve them against the base origin, producing collection and
resource URLs on the wrong host. Those URLs are on the base origin, so the allowlist passes them,
and they simply 404 (or, worse, hit an unrelated resource of the same path). Reported as
"not found" rather than as a resolution bug.

Fix: resolve against the request URI at each decode boundary rather than against `base_url`. Left
open deliberately - it changes the value of native ids at twelve call sites across two crates, and
landing that unreviewed at the end of a round is exactly the shape the loop keeps paying for. It
wants its own round, or at minimum its own review.

## Recurrence-override EventIds are unusable as resource ids; event_delete on one instance destroys the whole series

**C1 live defect. PUBLISHED SURFACE.** Verified 2026-08-23: `ical.rs` still mints
`EventId(format!("{uri}#{recurrence_id}"))` and no `#` guard exists in `account.rs` or `client.rs`
in either crate. Data loss on `event_delete`, wrong-series write on `event_update`. Published-surface
because either remedy changes the documented contract of `EventId` and of four published `Account`
methods: rejecting fragment ids makes calls that succeed today fail, and making them real changes
what those methods do. The owner picks which.

`crates/caldav/src/ical.rs`, `events_from_ical`: an override VEVENT gets
`EventId(format!("{uri}#{recurrence_id}"))`. `crates/caldav/src/account.rs` then feeds that id
straight into `client.resolve_url(&event.0)` for `event_get`, `event_update`, `event_delete`, and
`event_rsvp`. `resolve_url` returns an absolute href verbatim, so the string with the `#` fragment
goes to `reqwest`, which does not put the fragment on the wire. Every one of those calls therefore
hits the master resource:

- `event_get(override_id)` returns the master (via `event_from_ical`, which takes the first VEVENT),
  not the instance the consumer asked for.
- `event_update(override_id, patch)` splices the master VEVENT
  (`replace_first_vevent_properties`) and PUTs it back: the user edits an instance, the series
  changes.
- `event_delete(override_id)` DELETEs the whole `.ics`: one instance deleted, entire recurring
  series gone.

There is no `#` guard anywhere in the crate. The reference documents the qualified id as collision
avoidance for consumer indexes but never says the id is read-only, and nothing enforces it. This is
the worst thing in this scope. Fix options: reject fragment-bearing ids in the mutation paths with a
classified error, or make them real (resolve to the resource, locate the VEVENT by RECURRENCE-ID,
and splice/remove that component; a `THISANDFUTURE`-free instance edit is tractable, an instance
delete means emitting `EXDATE` on the master).

## Cursor sync only ever covers one collection

**C1 live defect. PUBLISHED SURFACE.** Verified 2026-08-23: `discover_cursor_scopes` still yields a
single `CursorScope::Type(ObjectType::CalendarEvent)` and all three lanes read
`default_calendar_url`. Events in every non-first calendar never sync. The finding offers two
remedies and they are different kinds of thing: per-collection `CursorScope` reshapes the published
cursor model and the stored envelope (owner's call), while documenting the limitation is a doc fix
the loop may do. Do not read the second option as permission to close this.

`CalDavAccount::establish_initial_cursor` / `inventory_stream` / `changes_stream` all use
`self.default_calendar_url` (the first collection returned by discovery); `CardDavAccount` does the
same with `default_addressbook_url`. `discover_cursor_scopes` returns a single
`CursorScope::Type(ObjectType::CalendarEvent)` / `Type(Contact)`. So an account with three calendars
enumerates all three in `calendars_list` but syncs only the first: events in the other two never
appear in inventory or changes, and never get an update or a delete. Neither reference names this as
a limitation; `reference/caldav.md` describes the cursor as if it were the account's whole event
surface. Either the scope needs to be per-collection (`CursorScope` per calendar href, which is the
honest model), or the limitation needs to be stated loudly.

## CardDAV fabricates a phantom address book that CalDAV deliberately stopped fabricating

**C2 latent defect. PUBLISHED SURFACE.** Verified 2026-08-23: `address_books_list` still pushes the
synthetic book when the home enumerates none. The defect is real (a consumer cannot distinguish an
empty backend, and the phantom's queries 404 against a spec-correct server) and CalDAV has already
ruled the same shape a bug, but the remedy removes a value a published method returns today, so a
consumer that relies on always getting at least one book breaks. Owner signs off on the removal;
the CalDAV precedent is the argument, not the authority.

`CardDavAccount::address_books_list` pushes a synthetic `AddressBook` pointing at the home when the
home enumerates zero addressbook collections. `reference/caldav.md` spends a paragraph explaining
why the CalDAV equivalent was removed: a consumer cannot distinguish a genuinely empty backend (and
so cannot reap stale collections), and the phantom's queries 404 against a spec-correct server.
CardDAV has exactly the phantom CalDAV calls a bug. `reference/carddav.md` does not mention it either
way.

Related, in both crates: when discovery finds zero collections, `default_*_url` falls back to
`client.resolve_url(&home)`, so the cursor, inventory, and changes lanes all target the home
collection, which is the same phantom by another name.

## CardDAV re-lists the whole address book home on every poll

**C3 refactor opinion (cost).** Verified 2026-08-23: `contact_snapshot` still takes `home: &str`
unconditionally and always calls `list_addressbooks_for_operation`, where CalDAV's takes
`home: Option<&str>`. Nothing misbehaves; a changed-ctag poll costs one extra round trip. Worth
doing, not a bug.

`CardDavAccount::contact_snapshot` always calls `list_addressbooks_for_operation(home, ...)`
(depth-1 PROPFIND over the home) purely to recover the ctag of one collection, then does the depth-1
contact listing. CalDAV fixed exactly this: `event_snapshot` takes `home: Option<&str>` and the poll
path passes `None` to use the cheap depth-0 `collection_sync_token`. CardDAV already has the depth-0
helper (`collection_ctag`) and even calls it in the short-circuit, then throws the answer away and
refetches it the expensive way. A changed-ctag poll costs three requests where two suffice; an
unchanged one is fine. `reference/caldav.md` states the depth-0 refinement; `reference/carddav.md`
implies parity it does not have.

## CalDAV silently ignores a calendar move; CardDAV rejects one

**C1 live defect. PUBLISHED SURFACE.** Verified 2026-08-23: `event_update` uses `patch.calendar_id`
only to pick the fetch URL and then PUTs to `resolve_url(&event.0)`, returning `Ok(())`. A requested
move silently does not happen, which is the worst of the three possible answers. Published-surface
because both remedies change what a published method does with an input it accepts today: refuse
(CardDAV's answer) or implement `MOVE`. Refusing is the smaller change and matches the sibling
crate; the owner still picks.

`CalDavAccount::event_update` uses `patch.calendar_id` only to compute the calendar URL for the
fetch, then PUTs to `client.resolve_url(&event.0)`, the original location. A caller asking to move
an event between calendars gets `Ok(())` and no move. `CardDavAccount::contact_update` handles the
same case explicitly with a `local_error` ("cannot move contacts between address books"). CalDAV
should do the same, or implement `MOVE`.

## RSVP is a non-atomic two-phase write with no compensation

**C2 latent defect.** Verified 2026-08-23: `event_rsvp` still posts the iTIP reply to the outbox and
only then PUTs, with a bare `?` on the PUT, so nothing tells the consumer the reply already went
out. The remedy is additive (populate `TransmissionState` on the second leg's error), touches no
published shape, and is cheap.

`event_rsvp` POSTs the iTIP `METHOD:REPLY` to the schedule outbox first, then PUTs the
locally-rewritten resource. If the PUT fails (412 from `If-Match`, 503, token expiry), the organizer
has already been told the user accepted while the user's own copy still says otherwise, and the
returned error gives the consumer no way to know the reply went out. At minimum the error from the
second leg should carry that the reply was already transmitted; the `TransmissionState` machinery in
the error model exists for exactly this distinction and is not used here.

## Discovery failures permanently disable RSVP for the account's lifetime

**C2 latent defect.** Verified 2026-08-23: `open` still swallows both discovery probes with
`.ok().flatten()` and bakes the result into an immutable capability. A network blip at open silently
and permanently reports the server as non-scheduling. The trailing paragraph about three to six
PROPFINDs on open is a separate **C3** cost observation.

`CalDavAccount::open` calls `discover_calendar_user_email().await.ok().flatten()` and
`discover_schedule_outbox_url().await.ok().flatten()`. A transient 503 or an expired token during
open makes `scheduling_available` false, `caldav_capabilities(false)` bakes
`pim_methods.event_rsvp = false` into an immutable field, and nothing re-probes: the account reports
"this server does not do scheduling" until reopened. A hard failure or a retry would both be better
than silently degrading a capability on a network blip.

Also in `open`: `discover_calendar_user_email`, `discover_schedule_outbox_url`, and
`discover_calendar_home` each independently call `discover_principal` (which itself may retry
against `.well-known`). That is three to six PROPFINDs on open where one principal lookup plus one
multi-prop PROPFIND would do; the three properties can be requested in a single `<D:prop>`.

## The structural finding: these are one crate wearing two hats

**C4 product decision. PUBLISHED SURFACE. The loop must not act on this.** This is the single
highest-risk entry in the document and it is the exact shape that produced the two restored
deletions: a real observation (the duplication is genuine)
attached to a remedy that collapses two published pre-1.0 crates into one. Whether `bifrost-caldav`
and `bifrost-carddav` should become projections over a `bifrost-dav` is the repository owner's call
about the shipped surface, not an engineering conclusion the duplication count can settle. Note also
that the argument's premise ("pre-1.0 with both crates crate-private below a factory, the blast
radius is small") is a claim about this workspace; both crates are published and their consumers are
outside it by definition.

Beyond `dav-F5`'s tracked transport seam, the duplication is far larger than the TODO records, and
it is not just `client.rs`:

- `client.rs`: `DavTransport` + `DavResponse` + `ReqwestDavTransport`, `dav_redirect_policy`,
  `auth_headers`, `escape_xml`,
  `normalize_http_etag`, `prepare_if_match`, `propfind_raw`, `report_raw`,
  `send_body_request`/`send_status_request`/`send_raw_request`, `MultigetFetch`, `worse_recovery`,
  `recovery_rank`, `multiget_failure`, and the ~120-line `status_error` if-ladder (identical but for
  `ResourceKind::Calendar` vs `Contact` and `Protocol::CalDav` vs `CardDav`), plus
  `local_error`/`parse_error`/`transport_error`/`unsupported_error`.
- `parse.rs`: the whole `ResponseParts` propstat state machine, `commit_propstat`, `resolve_href`,
  `local_name`, `push_text`, `trimmed`, `normalize_etag`, `MultigetOutcome` + `classify`,
  `as_failed_multiget_resource`, `as_missing_multiget_data`.
- `account.rs`, not tracked at all: the cursor codec
  (`write_string`/`write_option_string`/`write_u32`/`read_string`/`read_option_string`/`read_u32`/`cursor_error`,
  and the magic+version envelope), `diff_*_snapshots` + `push_destroyed_unless_failed` +
  `object_change` + `inventory_entry_from_snapshot` (structurally identical), `put_condition`,
  `append_path`, `same_url`/`same_collection_url`, `one_outcome_per_id`,
  `unsupported_future`/`unsupported_stream`, `contains`, and the ~400 lines of `Unsupported` trait
  stubs each crate carries for the other's domain.

That is on the order of 1500 duplicated lines. The dav-F5 commit message is right about the
mechanism and understates the scope: a comment was holding two copies in step, and it was not
holding. Two copies have already drifted (the discovery order; the phantom collection;
the depth-0 poll) and the drift is invisible because nothing compares them.

The hunter's recommendation is stronger than "extract helpers": collapse to a single `bifrost-dav`
crate parameterized over the collection/resource kind, with CalDAV and CardDAV as thin projection
layers (`ical.rs` / `vcard.rs`) plus their prop constants and query bodies. The 207 parser, the
propstat state machine, the snapshot/diff/cursor machinery, the multiget chunking with `degraded`,
and `status_error` are all genuinely protocol-neutral WebDAV; the only CalDAV/CardDAV-specific parts
are the property names, the query XML, and the body projection. Pre-1.0 with both crates
crate-private below a factory, the blast radius is small and the payoff is that the phantom
collection, the depth-0 poll, and the resource-identification fixes land once.

Two dependent structural notes:

- **[C3, with a C2 inside it]** The transport-bypass note is a refactor proposal, but one fact
  inside it is a defect on its own: `set_priority` and `set_bandwidth_cap` are published `Account`
  methods that silently do nothing in both crates (confirmed 2026-08-23, both are empty bodies), so
  a composed IMAP account with a `BandwidthMeter` does not meter or cap its DAV legs while
  advertising that it does. That mismatch is fixable or documentable without the unification, and
  `reference/caldav.md` not mentioning it at all is a doc bug.

- The `DavTransport` seam exists solely because `bifrost-net`'s dispatcher is crate-private. The cost
  is that all DAV traffic bypasses bifrost-net entirely: no retry, no rate limiting, no bandwidth
  metering, no observability. `set_priority` and `set_bandwidth_cap` are no-ops in both crates, so an
  IMAP account composed `with_caldav`/`with_carddav` and a `BandwidthMeter` silently does not meter
  or cap its DAV legs. `reference/carddav.md` admits this in one line; `reference/caldav.md` does not
  mention it. If the unification happens, this is the moment to move onto `AccountNet` rather than
  keeping two hand-rolled `reqwest::Client`s with a 30s blanket timeout.
- **[C3]** The IMAP composition seam itself is fine (`classify_dav_open` degrades correctly into
  `skipped_scopes`), but `open_carddav` and `open_caldav` run sequentially, each paying the
  multi-round-trip discovery above. Joining them is free.

## Smaller things

Each bullet carries its category inline. None of these touches a published surface.

- **[C3]** `MULTIGET_BATCH_SIZE = 50` and `CONTACT_PAGE_SIZE` chunking are fine, but `event_search`'s
  empty-query branch lists and hydrates every resource in the collection before applying
  `request.limit`; `events_in_range` likewise truncates to `limit` only after full hydration and
  projection. CardDAV's `contact_search` reruns the entire remote search and rehydrates everything
  for every page (documented as intentional in the reference, and it does make `failed_ids` per-page
  honest, but it is O(collection) per page).
- **[C2]** `event_in_range` uses closed-interval overlap (`event_start <= range_end && event_end >= range_start`)
  where CalDAV `time-range` is half-open. As a defensive guard it only over-includes, so it is not a
  correctness bug, but all-day events (whose DTEND is exclusive per the crate's own contract) will
  match a window starting exactly at their end.
- **[C3]** The finding calls it harmless itself; the ask is observability, not a fix. `changes_from_cursor` (CalDAV) keeps the previous sync token when `report.sync_token` is `None`.
  RFC 6578 requires the server to return one; a server that omits it makes every subsequent poll
  replay the same window. Harmless because the diff absorbs it, but it hides a server bug forever
  rather than surfacing it.
