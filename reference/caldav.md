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
home, caches the default calendar URL, and returns an `Arc<dyn Account>`.
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
  propstat status.
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
  `event_from_ical` is fallible and the listing/search paths degrade a
  single bad resource to a skip, while `event_get`/`event_update` surface a
  local error. A TZID-bearing local time projects as a bare wall-clock value
  (no false `Z`) with the zone in `timezone`; Microsoft/Windows zone names
  (e.g. `W. Europe Standard Time`) are mapped to IANA via caldata's
  proprietary-TZID table. Serialization-out (create/patch/RSVP) stays
  hand-rolled and verbatim-preserving: patches splice on *physical* lines,
  folding only newly emitted lines, so long preserved/unmodeled values
  round-trip byte for byte. VTIMEZONE generation emits a single STANDARD
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
  before the window). Per-resource parse failures are not swallowed - the
  failed `.ics` hrefs surface on `Page::failed_ids` so a consumer can tell a
  transient failure apart from a real remote deletion.
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
  when an etag was present. When the current resource carried raw
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
  `Unsupported`.
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
sync token, applies returned href/etag/status entries to the snapshot
(deleting only on explicit per-entry `404`/`410`), and emits
created/updated/destroyed event changes. Calendars without a sync token
fall back to polling snapshot diffs. The PROPFIND-snapshot diff (not the
sync-token path) is hardened against destroy-everything failure modes: an
empty multistatus against a populated prior snapshot suppresses the
mass-delete, and any href in `current.failed_hrefs` is preserved rather
than destroyed. `inventory_stream` emits event inventory entries with ETag
fingerprints for the same cursor scope.

All mail, contact, filter, blob, push, and settings methods return
`AccountErrorKind::Unsupported` stamped with `Protocol::CalDav`.
