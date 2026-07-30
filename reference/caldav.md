# bifrost-caldav reference

Current Stage 4 standalone CalDAV account implementation.

## Public surface

- `CalDavCredentials` - Basic or bearer credentials. `bearer(token)`
  wraps a raw string; `bearer_source(Arc<dyn TokenSource>)` takes a
  shared rotation source. The bearer token is read via `current().await`
  per DAV request (in `auth_headers`), so a token rotated mid-sync is
  honored on the next request without reopen. `Clone` only, hand-written
  `Debug` redacting the source; no `PartialEq`/`Eq`.
- `CalDavConfig` - base URL plus credentials (`Debug`/`Clone`, no
  `PartialEq`/`Eq`).
- `CalDavAccountFactory` - implements `AccountFactory`.

`CalDavAccountFactory::open(account_id)` discovers the CalDAV calendar
home, caches the default calendar URL, and returns an `Arc<dyn Account>`
inside an `OpenedAccount` whose skip lane is always empty
(single-principal surface).
The raw DAV client, XML parser, and iCalendar projection stay
crate-private; consumers use only the factory and the shared `Account`
calendar primitives.

## Module layout

- `lib.rs` - public config / credentials / factory.
- `account.rs` - crate-private calendar-only `Account` impl.
- `client.rs` - crate-private reqwest CalDAV client: discovery,
  `PROPFIND`, `REPORT`, `GET`, `PUT`, and `DELETE`.
- `parse.rs` - XML response parsers for calendar discovery, event
  listing, multiget hydration, and nested href properties. Calendar
  collection metadata is staged per `propstat` and committed only for
  successful 2xx propstat statuses. `parse_propfind_events` returns a
  `CalDavEventListing`: committed `entries` plus `failed_hrefs` (`.ics`
  resources whose only propstat failed within the 207), so the snapshot
  diff preserves a transiently-failed resource instead of destroying it.
  Event listing and multiget parsers use element-stack parent checks so
  nested same-name properties do not overwrite response-level hrefs or
  propstat status. Every text-bearing parser accepts both XML text and
  CDATA. Scheduling address-set extraction returns every nested href;
  single-valued discovery properties use the first.
- `ical.rs` - iCalendar projection between DAV resources and
  `bifrost-types` calendar events. Parsing-in uses `caldata`'s streaming
  `ContentLineParser` (RFC 5545 unfolding that strips exactly one fold WSP,
  and quoted-parameter splitting that tolerates `:`/`;`/`,` inside quoted
  values). Values are stored raw by caldata; text fields (summary,
  description, location, CN) are unescaped at read time via a single
  left-to-right scan. `event_from_ical` projects the master (first VEVENT)
  for direct get/update; `events_from_ical` projects *every* VEVENT (master
  plus each recurrence override / CANCEL), carrying RECURRENCE-ID and STATUS
  through, and is used by the range/search listing paths (override instances
  take a recurrence-qualified `EventId` but keep the resource native id).
  VALARM sub-components project into `CalendarEvent.reminders` (relative
  DURATION or absolute DATE-TIME triggers). All-day ends follow the
  exclusive `EventTime` contract: an iCalendar all-day DTEND is neither
  decremented on read nor incremented on write (it matches bifrost-google's
  verbatim exclusive end).
  Tokenizing rather than using caldata's typed builder means strict
  singleton enforcement never hard-fails ingest: a duplicate DTSTART is
  resolved with a precedence picker (VALUE=DATE > TZID > UTC > floating)
  rather than rejected. A genuinely malformed body (unterminated quoted
  parameter, missing name/value, invalid UTF-8) returns an error;
  `event_from_ical` is fallible and the listing/search paths route a
  single bad resource into `Page::failed_ids`, while
  `event_get`/`event_update` surface a local error. A TZID-bearing local time
  projects as a bare wall-clock value
  (no false `Z`) with the zone in `timezone`; Microsoft/Windows zone names
  (e.g. `W. Europe Standard Time`) are mapped to IANA via caldata's
  proprietary-TZID table. A non-ASCII DATE or DATE-TIME grammar value is
  preserved verbatim rather than sliced at fixed byte offsets. `DTSTART`
  plus `DURATION` projects an end when `DTEND` is absent; an explicit end
  patch removes DURATION before emitting DTEND. A resource with no VEVENT
  (a VTODO or VJOURNAL sharing the collection) projects to a
  `event_get`/`event_update` error and to *no* events in the listing lanes,
  never to a fabricated empty event.
  Serialization-out (create/patch/RSVP) stays
  hand-rolled and verbatim-preserving: patches splice on *physical* lines,
  folding only newly emitted lines, so long preserved/unmodeled values
  round-trip byte for byte. Replacement matching is case-insensitive and
  limited to depth-zero VEVENT properties, so VALARM properties are
  preserved; new event properties are inserted before the first nested
  component. A body that offers no splice point (no `BEGIN:VEVENT`, or a
  first VEVENT that never closes) is an error rather than an unchanged
  write-back that would look like a successful edit. Parameter values use
  RFC 6868 caret encoding in both directions; caldata hands back raw
  parameters, so the decode of free-text parameters (CN, TZID) is this
  crate's. RFC 6868 defines no backslash escape, so a backslash is written
  and read verbatim and CN and TZID both round-trip it. The one legacy
  tolerance is `normalize_exchange_cn_param`: an unquoted `CN=Doe\, John`
  carrying a genuine `\,` / `\;` separator escape (which caldata would
  otherwise split at the comma) is resolved and re-encoded conformantly
  before tokenization. The CN read path is a pure RFC 6868 decode, which is
  what keeps a literal backslash from being eaten. The cost is that a
  display name that genuinely contains `\,` is read as Exchange's escaping;
  the two are indistinguishable on the wire. VTIMEZONE
  generation emits a single STANDARD
  block carrying the real UTC offset for the event's instant (the TZID is
  parsed to a `chrono_tz::Tz` after Windows/Exchange-alias folding, and the
  offset is resolved from the DTSTART wall-clock with the same
  ambiguous-picks-earlier / gap-walks-forward `LocalResult` discipline as
  ratatoskr's resolver), not the old `+0000` stub. A single block is
  approximate for a recurring event crossing a DST boundary (off by the DST
  delta on the far side) but strictly correct for the master instant. An
  unknown / unparseable zone omits the offset sub-block rather than asserting
  a misleading `+0000`.
- `capabilities.rs` - calendar-only `AccountCapabilities`.

## Account behavior

Supported calendar primitives:

- `calendars_list` - `PROPFIND` depth 1 on the discovered calendar
  home, filtering `resourcetype` entries that contain `calendar`. A home
  that is itself a calendar collection is returned by that same depth-1
  parse (its own response carries `<calendar/>`), so a home enumerating
  zero calendar collections yields an EMPTY list rather than a fabricated
  placeholder calendar. This lets a consumer distinguish a genuinely empty
  backend (and reap stale calendars) from a real single calendar, and
  avoids a phantom home-calendar whose `events_in_range` REPORT a
  spec-correct server 404s.
- `events_in_range` - `calendar-query` `REPORT` with a CalDAV
  `time-range` filter and calendar-data hydration, followed by local
  overlap filtering as a defensive guard. The local guard is
  recurrence-aware: a recurring master whose own interval sits outside the
  window is retained when its RRULE can still yield an in-window occurrence
  (dropped only when it starts after the window, or an RRULE `UNTIL` ends it
  before the window). Per-resource failures are not swallowed - the failed
  hrefs surface on `Page::failed_ids` so a consumer can tell a transient
  failure apart from a real remote deletion. Two kinds land there and they
  are equivalent to the consumer: a resource the server refused inside the
  207 (non-2xx propstat, or 2xx with no `calendar-data`), and one that
  fetched 200 but would not tokenize. `parse_multiget_report` returns
  `CalDavMultigetReport { events, failed, missing_data }` and reserves `Err`
  for a malformed document, so a single bad propstat can no longer abort the
  whole pull. `event_search` reports the same way, deduped across its four
  per-property REPORTs.

  Ids appear in exactly one lane. `one_outcome_per_id` drops from the failure
  lane anything that materialized in some leg, because the per-property
  REPORTs can disagree about the same resource and a consumer that saw it in
  both would count it twice and treat a displayable event as lost.

  Multiget is chunked and search runs one REPORT per property, so each REPORT
  is classified independently. A leg that fails wholly after other legs
  returned events keeps those events, and the worst recovery class
  encountered rides `MultigetFetch::degraded` into `Page::skipped_scopes` as
  an `ErrorScope::Calendar` entry - `failed_ids` carries ids with no
  classification, so folding a 401 into it would keep the data and destroy
  the reauthorize signal. A refusal with nothing usable anywhere is still an
  `Err`.

  Properties are collected propstat-scoped and promoted to the response
  only by `commit_propstat`, and only from a 2xx propstat. That is what
  makes a multi-propstat response order-independent: `calendar-data`
  inside a refused block is never adopted, and a trailing non-2xx block
  for an unrelated property never retracts data a successful block
  supplied.

  The body is classified, not merely parsed. Per RFC 4918 s13 a 207 may
  describe success, partial success, or complete failure, so
  `CalDavMultigetReport::classify` returns `CompleteFailure` when no
  resource yielded usable data and at least one resource carries an actual
  non-404/410 DAV failure (a resource deleted between listing and multiget
  is the benign per-resource case). A 2xx response that omits
  `calendar-data` remains an absent-data `failed_ids` outcome but is not a
  server failure. `multiget_failure` routes a complete failure through
  `status_error`, so an all-401 body reauthorizes and an all-503 body
  retries instead of returning an empty page that a consumer would
  record as a completed walk - which would drop those resources
  permanently.
- `event_get` - direct `GET` of the event resource.
- `event_create` - creates a VEVENT resource with a UUID-backed
  `.ics` path using `PUT`, including STATUS from shared lifecycle status,
  TRANSP from shared availability, CLASS from shared visibility when
  present, ORGANIZER from the shared organizer field, and VTIMEZONE
  components for TZID-bearing start/end times. Each generated VTIMEZONE
  carries the real UTC offset for the event's instant in a single STANDARD
  block (resolved via `chrono-tz`), not full timezone transition-rule
  definitions; an unknown zone emits the bare VTIMEZONE with no offset block.
- `event_update` - fetches the current event, applies the shared
  `EventPatch`, and writes the replacement resource with `If-Match`
  when a strong etag was present. Weak ETags retain their `W/` marker for
  snapshot comparison but deliberately make the PUT unconditional because
  If-Match requires strong comparison. When the current resource carried raw
  iCalendar data, updates rewrite each VEVENT property the patch carries -
  summary, description, location, start/end, status, transparency
  (TRANSP), class (CLASS), recurrence, and attendees - while preserving
  untouched properties such as organizer. A `visibility` patch that
  resolves to `Default` strips any existing CLASS rather than writing one,
  matching the create path. Non-VEVENT components such as
  VTIMEZONE are preserved on raw-backed updates. Multi-VEVENT recurrence
  override components are preserved for scalar patches, but recurrence
  replacement is rejected for resources with override VEVENTs because the
  shared recurrence model cannot rewrite those instances losslessly.
- `event_delete` - deletes the DAV resource.
- `event_rsvp` - uses an email-like Basic username, or a mailto address
  discovered from the principal's `calendar-user-address-set`, to rewrite
  the matching attendee's participation status. When the principal
  exposes `schedule-outbox-URL`, RSVP first posts an iTIP `METHOD:REPLY`
  to that outbox - with the RFC 6638 `Originator` (the replying user's
  calendar address) and `Recipient` (the organizer's address) headers,
  each normalized to a `mailto:` URI when bare - and then applies the same
  raw-preserving replacement path to the local resource. Accounts without
  an identifiable attendee, organizer, or schedule outbox return
  `Unsupported`. The advertised `pim_methods.event_rsvp` flag is
  discovery-derived, not assumed: `scheduling_available` requires BOTH a
  `CALDAV:schedule-outbox-URL` on the current-user-principal AND a
  calendar-user-address for this user (from
  `CALDAV:calendar-user-address-set`, or configured explicitly). A plain
  RFC 4791 store that advertises neither reports `event_rsvp = false`, so
  a consumer's capability gate rejects the call up front instead of the
  iTIP POST failing on the wire. The address-set is treated as a set:
  discovery examines every href and uses the first case-insensitive
  `mailto:` URI.
- `event_search` / `event_autocomplete` - non-empty searches issue
  CalDAV text-match `calendar-query` `REPORT`s over VEVENT summary,
  description, location, and attendee, then keep local filtering as a
  defensive guard. Empty search lists the collection to preserve
  match-all behavior.

Cursor support is calendar-event only. `discover_cursor_scopes` returns
`CursorScope::Type(ObjectType::CalendarEvent)`. `establish_initial_cursor`
builds a hybrid cursor from the calendar URL, the collection
`sync-token` when present, and a sorted href/etag snapshot.
`changes_stream` uses WebDAV `sync-collection` when the cursor carries a
sync token, issuing the REPORT with `Depth: 0` as required by RFC 6578,
and applies returned href/etag/status entries to the snapshot
(deleting only on explicit per-entry `404`/`410`), and emits
created/updated/destroyed event changes. Calendars without a sync token
fall back to polling snapshot diffs. The PROPFIND-snapshot diff (not the
sync-token path) is hardened against destroy-everything failure modes: an
empty multistatus against a populated prior snapshot suppresses the
mass-delete, and any href in `current.failed_hrefs` is preserved rather
than destroyed. `inventory_stream` emits event inventory entries with ETag
fingerprints for the same cursor scope.
An expired token reported as 403 with `DAV:valid-sync-token`, or as 410,
becomes scoped `SyncState(CursorInvalid)`, which directs the engine to
restart the calendar-event cursor. Cursor entry counts are payload-bounded
before allocation. Range, search, inventory, changes, and their failed-id
lanes all use resolved absolute resource URLs as native ids.

All mail, contact, filter, blob, push, and settings methods return
`AccountErrorKind::Unsupported` stamped with `Protocol::CalDav`.
