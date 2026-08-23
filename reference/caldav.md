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

`CalDavAccountFactory::open(account_id)` discovers the current principal once,
then reads the calendar home, scheduling address set, and schedule outbox in
one multi-property principal PROPFIND. Discovery errors fail the open rather
than being mistaken for absent scheduling support. The factory caches the
default calendar URL and returns an `Arc<dyn Account>`
inside an `OpenedAccount` whose skip lane is always empty
(single-principal surface).
The raw DAV client, XML parser, and iCalendar projection stay
crate-private; consumers use only the factory and the shared `Account`
calendar primitives.

## Module layout

- `lib.rs` - public config / credentials / factory.
- `account.rs` - crate-private calendar-only `Account` impl.
- `client.rs` - crate-private CalDAV client: discovery, `PROPFIND`,
  `REPORT`, `GET`, `PUT`, and `DELETE`. A local `DavTransport` seam keeps
  reqwest dispatch in production while scripted request transcripts exercise
  DAV flows without a listener. `bifrost-net`'s dispatcher is crate-private,
  and DAV still owns Basic auth and its redirect policy. Every request path
  except `sync_events` classifies a non-2xx status before the body is
  parsed, so an error page can never decode as an authoritative empty
  report; `sync_events` alone reads the raw response, because it must see
  403 `valid-sync-token` and 410 as cursor invalidation rather than failure.
  Credential-bearing requests are limited to the configured base origin plus
  calendar-home and scheduling origins delegated by an authenticated principal
  on that origin. Resource hrefs, consumer-provided native ids, and a
  cross-origin principal href cannot extend that internal origin set. A
  delegated origin may never weaken the transport guarantee the configured base
  URL established: when the base URL is `https`, a discovered `http` home is
  refused admission, so discovery cannot become a downgrade channel for the
  account credential. A cross-origin `https` home is admitted, because a
  principal and a calendar home on different hosts of one service is a real
  deployment shape. Discovery stages these origins without changing trust;
  they are admitted only after the complete authenticated discovery succeeds,
  before the account is shared or a home request starts. Redirects split into
  two paths. Same-origin hops (exact scheme, host, effective port) are
  followed inside reqwest, which preserves `Authorization` under exactly that
  condition. Cross-origin hops are never followed inside reqwest - it strips
  `Authorization` on any origin change and a redirect policy cannot restore
  it - so the policy stops them and `send_raw_request` re-dispatches the hop
  manually with fresh credentials, gated by the same admitted-origin set the
  credential gate reads. A `Location` naming an unadmitted origin fails
  locally without a request going out; a 303 is not followed. Both the
  reqwest chain and the manual hops are bounded by bifrost-net's hop cap.
- `parse.rs` - XML response parsers for calendar discovery, event
  listing, multiget hydration, and nested href properties. Calendar
  collection metadata and href-valued discovery properties are staged per
  `propstat` and committed only for successful 2xx statuses or a missing
  status, which RFC 4918 requires but the parser tolerates as success.
  `parse_propfind_events` returns a
  `CalDavEventListing`: committed non-collection `entries` plus
  `failed_hrefs` (non-collection resources whose only propstat failed within
  the 207), so the snapshot
  diff preserves a transiently-failed resource instead of destroying it.
  Resource identification does not depend on an `.ics` suffix or a returned
  content type. The depth-1 listing requests `resourcetype` and excludes
  collections; `sync-collection` accepts every returned member href because
  that REPORT supplies neither content type nor a naming convention. The
  `collection` marker obeys the same commit-on-success rule as every other
  property: seen inside a `propstat` it is staged and promoted only if that
  block's status was 2xx, so a server echoing the requested prop skeleton
  (`<collection/>` included) back inside a 404 propstat cannot discard an
  event whose own properties came back 200.
  Propstat-scoped values live in one `PropStat` staging struct cleared with
  `mem::take` at commit. An absent status remains success, while a present but
  unparseable status remains an explicit non-success.
  Event listing and multiget parsers use element-stack parent checks so
  nested same-name properties do not overwrite response-level hrefs or
  propstat status. Every text-bearing parser accepts both XML text and
  CDATA. Scheduling address-set extraction returns every nested href;
  single-valued discovery properties use the first. Response hrefs are
  rebased against the URI of the request that produced the multistatus,
  yielding absolute native URLs at the decode boundary before the account
  layer can consume success or failure lanes. The base is the EFFECTIVE
  request URI: the DAV transport carries the post-redirect URL back on
  every response, because the redirect policy follows admitted-origin hops and
  RFC 4918 resolves against the URI that actually served the body.
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

  **A recurrence-qualified `EventId` is READ-ONLY, and the account enforces
  that.** `EventId("{uri}#{recurrence_id}")` exists so a consumer index can tell
  the occurrences of a series apart; it is not addressable.
  `client.resolve_url` returns an absolute href verbatim and a URL fragment is
  never sent on the wire, so every such id resolves to the master resource.
  `event_get`, `event_update`, `event_delete` and `event_rsvp` therefore refuse
  an id containing `#` via `reject_recurrence_instance_id`, before any I/O, as
  `Request(Malformed)` -> `ClientBug` (no retry or reopen heals a caller passing
  a non-handle). Unguarded, `event_get` returned the master instead of the
  instance asked for, `event_update` spliced and PUT the master so editing one
  occurrence rewrote the series, `event_rsvp` answered for the series, and
  `event_delete` DELETEd the whole `.ics` - **deleting one occurrence destroyed
  every occurrence**.
  `recurrence_instance_ids_are_refused_before_reaching_the_wire` pins all four
  against an empty transport script, so a removed guard starves the script
  rather than failing quietly.

  Real per-occurrence writes would mean resolving the resource, locating the
  VEVENT by RECURRENCE-ID, and splicing or removing that component (an
  occurrence delete emitting `EXDATE` on the master, or `STATUS:CANCELLED` on
  the override - they differ in what attendees see). This is deliberately not
  scheduled. CalDAV is the only calendar crate with the problem, because it is
  the only one whose provider exposes no per-occurrence resource: Graph syncs
  through `calendarView`, whose occurrences carry genuine Graph ids; JMAP keeps
  overrides inside the master object and returns `Unsupported` for one it cannot
  represent; Google's `{calendar}::{event}` ids wrap the provider's own instance
  ids. None can inherit this shape, so fixing it here buys them nothing.

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
  (a VTODO or VJOURNAL sharing the collection) maps to
  `NotFound(Calendar)` for `event_get`/`event_update` and to *no* events in
  the listing lanes, never to a fabricated empty event.
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
  resolved against the jiff tzdb after Windows/Exchange-alias folding, and
  the offset is resolved from the DTSTART wall-clock with the same
  fold-picks-earlier / gap-takes-post-gap-offset discipline as
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
  overlap filtering as a defensive guard. Invalid time bounds fail locally
  before a REPORT is sent, and the encoder preserves legal one-sided ranges.
  Query REPORTs use `Depth: 1`; `calendar-multiget` REPORTs enumerate their
  hrefs in the body and use `Depth: 0`.
  The local guard uses half-open overlap, matching CalDAV time-range and the
  exclusive all-day end contract. It is recurrence-aware: a recurring master
  whose own interval sits outside the
  window is retained when its RRULE can still yield an in-window occurrence
  (dropped only when it starts after the window, or an RRULE `UNTIL` ends it
  before the window). COUNT-bounded recurrences trust the server's
  expansion: fully defending against a hostile server would require
  recurrence expansion that does not exist in this crate. Per-resource failures are not swallowed - the failed
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
  `Err`. The degraded lane exists only where a call spans several REPORTs
  (`event_search`'s per-property legs and the chunked multiget hydration
  behind its empty-query branch); `events_in_range` itself is a single
  `calendar-query` REPORT, so a wholly-failed body there stays an `Err` and
  its pages never carry a skipped scope.

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
- `event_get` - direct `GET` of the event resource. Calendar id and calendar
  provenance are derived from the resource URL rather than stamped with the
  account's default calendar.
- `event_create` - creates a VEVENT resource with a UUID-backed
  `.ics` path using `PUT`, including STATUS from shared lifecycle status,
  TRANSP from shared availability, CLASS from shared visibility when
  present, ORGANIZER from the shared organizer field, and VTIMEZONE
  components for TZID-bearing start/end times. Each generated VTIMEZONE
  carries the real UTC offset for the event's instant in a single STANDARD
  block (resolved via jiff's bundled tzdb), not full timezone transition-rule
  definitions; an unknown zone emits the bare VTIMEZONE with no offset block.
- `event_update` - fetches the current event, applies the shared
  `EventPatch`, and writes the replacement resource with `If-Match`
  when a strong etag was present. Weak ETags retain their `W/` marker for
  snapshot comparison but deliberately make the PUT unconditional because
  If-Match requires strong comparison (RFC 7232), so a weak validator has
  no conforming conditional form. Against a server that only ever emits
  weak ETags this means `event_update` has no lost-update protection at
  all; no better option exists inside HTTP, and a consumer that needs the
  guarantee needs an application-level revision check. When the current resource carried raw
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
  The outbox POST and local-resource PUT remain two server operations. Every
  failure after the POST succeeds - the PUT itself and the local patch and
  iCalendar encoding steps between them - is wrapped as a
  `Protocol(PartialResponse)` carrying an acknowledged `Attempt` plus the
  original cause chain. This routes the non-idempotent RSVP to reconciliation
  and tells the consumer that the organizer may already have received it. The
  flow is still not atomic; the error class is what communicates that.
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
The CalDAV cursor payload is version 2. Version 2 records the request-relative
native-id namespace; version 1 cursors are rejected so a namespace correction
cannot surface as a delete plus create during snapshot diffing. The rejection is
classified `SyncState(SchemaIncompatible)`, deriving to
`Engine(SchemaIncompatible)` rather than a scope restart: only that directive
also deletes the backfill checkpoint, and without the re-walk the objects
already backfilled would keep their pre-correction id spelling.
An expired token reported as 403 with `DAV:valid-sync-token`, or as 410,
becomes scoped `SyncState(CursorInvalid)`, which directs the engine to
restart the calendar-event cursor. Cursor entry counts are payload-bounded
before allocation. Range, search, inventory, changes, and their failed-id
lanes all use resolved absolute resource URLs as native ids. Snapshot-poll
fallback refreshes a collection token with a depth-0 `sync-token` PROPFIND,
rather than repeating the calendar-home depth-1 listing.
When a successful `sync-collection` response omits its required replacement
sync token, the account emits a warning and retains the previous token. This
keeps replay-and-deduplicate behavior while exposing the server violation.

Known cursor limitation: VTODO / VJOURNAL resources sharing the collection
still occupy the event cursor. Both change lanes key on the PROPFIND href
listing, which does not carry the component type, so a task resource is
emitted as a created/updated event change whose hydration yields no events
(the `.ics`-without-VEVENT rule above). Filtering it out needs either a
component-type PROPFIND or a first-fetch classification cache (tracked as
caldav-F1).

All mail, contact, filter, blob, push, and settings methods return
`AccountErrorKind::Unsupported` stamped with `Protocol::CalDav`.
`set_priority` and `set_bandwidth_cap` are no-ops because this crate's local
reqwest transport has no `AccountNet` or metered transport attachment. This
also means DAV legs composed into an IMAP account are not included in that
account's priority scheduling, bandwidth measurements, or bandwidth cap.

## This crate and bifrost-carddav are near-duplicates, and drift is the defect

Roughly 1500 lines are hand-mirrored between the two: the `DavTransport` seam
and `ReqwestDavTransport`, `dav_redirect_policy`, `auth_headers`, `escape_xml`,
etag normalization and `prepare_if_match`, the raw request helpers, the
~120-line `status_error` ladder (identical but for `ResourceKind` and
`Protocol`), the whole `ResponseParts` propstat state machine, href resolution,
multiget classification, the cursor codec, the snapshot diff, `put_condition`,
URL comparison, and the `Unsupported` stubs each crate carries for the other's
domain.

Nothing compares the two copies, so divergence is silent. Five separate defects
in one hardening arc were exactly that: `escape_xml` quoting, the immediate
collection-marker promotion, the eleven hand-maintained CalDAV `propstat_*`
twins, the phantom-collection asymmetry, and `as_fetched_vcard` missing the
`is_collection` guard its CalDAV twin already had (which surfaced an echoed
collection as a phantom card).

**Any fix to shared-shape code in one crate must be checked against the other.**
Where the asymmetry is real - CardDAV has no `sync-collection` path and
discovers one property - it is design, not oversight. Collapsing the two into a
shared `bifrost-dav` has been proposed and is a repository-owner decision about
the published surface, not an engineering conclusion the duplication count can
settle; it is tracked in `notes/todo.md`.
