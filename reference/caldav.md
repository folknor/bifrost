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
Discovery tries `/.well-known/caldav` first - built from the ORIGIN of the
configured base URL via `bifrost_net::url::well_known_url`, never by appending
the suffix to a configured path - and falls back to the configured
base URL only when that initial principal lookup is not found or its successful
body names no principal. A failure after the principal is identified is not a
root-discovery fallback trigger.

## Module layout

- `lib.rs` - public config / credentials / factory.
- `account.rs` - crate-private calendar-only `Account` impl.
- `client.rs` - crate-private CalDAV client: discovery, `PROPFIND`,
  `REPORT`, `GET`, `PUT`, and `DELETE`. Wire traffic rides `bifrost-net`
  through `bifrost-dav-core`'s `DavDispatch`, so DAV legs share the retry
  budget, per-host rate limiting, bandwidth metering and observability with
  every other HTTP protocol crate; scripted transcripts exercise DAV flows at
  the wire, below all of it, without a listener. DAV still mints its own
  credentials and walks its own redirects, for the reasons in "The shared
  layer" below. Every request path
  except `sync_events` classifies a non-2xx status before the body is
  parsed, so an error page can never decode as an authoritative empty
  report; `sync_events` alone reads the raw response, because it must see
  403 `valid-sync-token` and 410 as cursor invalidation rather than failure.
  A response body exceeding the buffered ceiling is classified as
  `Protocol(PartialResponse)` with `Attempt(Acknowledged)`, so a completed
  non-idempotent mutation reconciles instead of replaying blindly.
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
  content type. Depth-1 listing, calendar-query, text-query, and multiget
  requests include `resourcetype` and exclude collection self-responses;
  `sync-collection` accepts every returned member href because
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
  A CalDAV `CalendarId` IS the resolved collection href used by the account;
  `Calendar.id`, `Calendar.native_id`, event `calendar_id`, request routing,
  and `ErrorScope::Calendar` all carry that same URL identity.
- `events_in_range` - `calendar-query` `REPORT` with a CalDAV
  `time-range` filter and calendar-data hydration, followed by local
  overlap filtering as a defensive guard. Invalid time bounds fail locally
  before a REPORT is sent, and the encoder preserves legal one-sided ranges.
  Query REPORTs use `Depth: 1`; `calendar-multiget` REPORTs enumerate their
  hrefs in the body and use `Depth: 0`.
  Range and search results use local offset cursors: when `limit` truncates
  the materialized set, `next_cursor` names the next offset. The offset is
  local and every continuation re-runs the remote REPORT, so `event_page`
  SORTS by the recurrence-qualified `EventId` before slicing. DAV guarantees
  no ordering on a multistatus; slicing raw response order let an unchanged
  result set come back permuted between pages, skipping the events the
  permutation moved behind the offset and serving twice the ones it moved
  past. A `limit` of zero is an exhausted page - no items and no
  continuation - rather than an empty page that names its own offset again
  and loops a cursor-following consumer forever.
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
  is classified independently. Every leg goes through `accumulate_leg`, which
  is the single funnel each leg result passes through: all four ways a leg can
  fail - transport, a non-2xx status, a body that will not parse, and a 207
  describing complete failure - are folded into `degraded` there, and the
  function returns nothing, so a leg added later has no unrouted path
  available to it. A malformed body is account-authored data, classified and
  survived rather than asserted on. A leg that fails wholly after other legs
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
  when a strong etag was present.

  **A cross-calendar move is PERFORMED.** A `calendar_id` differing from the
  event's own collection (derived by `event_calendar_url`) relocates the
  resource; a patch that RESTATES the event's current calendar is not a move and
  takes the ordinary GET-plus-PUT path. The assertion here has been inverted
  twice - the request originally returned `Ok(())` having moved nothing, was
  then refused outright as better than a silent drop, and is now carried out -
  so `event_update_moves_across_calendars_and_updates_in_place_otherwise` pins
  both halves.

  WebDAV `MOVE` is the atomic form and is tried first, with the resource's own
  file name at the destination and `Overwrite: F`, so a collision refuses rather
  than destroying a stranger's resource. `Destination` is credential-gated
  against the same admitted-origin set as the source, so a consumer-supplied
  `CalendarId` cannot steer a write anywhere the gate would refuse. Only 405 and
  501 mean "no MOVE support"; 412 (destination occupied) and 502 (destination
  refused) stay real errors.

  A server without MOVE falls back to PUT-to-new then DELETE-from-old. That
  order is the recoverable one: a failed copy leaves the event exactly where it
  was, while a failed delete leaves it readable in two places. The delete-leg
  failure is wrapped `Protocol(PartialResponse)` with an acknowledged `Attempt`
  (via `partial_sequence_error`, which `event_rsvp` now shares), so a consumer
  can tell "not moved" from "copied but not cleaned up" and reconciles instead
  of replaying a write that already landed.

  A move-only patch is ONE request beyond the fetch - no content changed, so no
  write is issued and no partial-failure verdict can attach to a leg that was
  never needed. Whether content changed is decided by zeroing `calendar_id` on
  the patch and comparing against `EventPatch::default()`, not by comparing
  serialized bytes: the writers re-emit, so a byte comparison reads a move-only
  patch as a content change. Deriving it from the patch also means a field added
  to `EventPatch` is covered automatically.

  **The event id changes, and `event_update` cannot report it.** A CalDAV
  `EventId` IS the resource URL, and the trait method returns `()`. The consumer
  learns the new id through sync, as a destroy plus a create. This matches
  `bifrost-google`, whose `events.move` inside `event_update` has the same
  property for the same reason; reporting it would reshape a published
  signature. Weak ETags retain their `W/` marker for
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

Cursor support is calendar-event only. `discover_cursor_scopes` returns one
`CursorScope::Folder(FolderId(collection_href))` per discovered calendar, and
NOTHING when the calendar home holds no collections - the collection walk
already returns the home itself when the home is a calendar, so an empty walk
is an empty backend and fabricating a home scope would point every cursor and
inventory request at a 404.
`establish_initial_cursor`
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
than destroyed. The checkpoint preserves the prior entries and etags for
both no-observation shapes while retaining a refreshed collection token.
This deliberately means a genuine delete-all is not reported on that poll or a
later poll: the empty success is indistinguishable from the transient empty
response the guard suppresses.
`inventory_stream` emits event inventory entries with ETag fingerprints for
the same cursor scope and reports full coverage of that one collection. Legacy
`CursorScope::Type(CalendarEvent)` calls remain accepted for compatibility,
target the default calendar, and report only a `caldav` provider region rather
than falsely claiming whole-type coverage.
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

## Sync scope is per calendar collection

All three sync lanes resolve their collection from the folder scope returned by
discovery. An account with three calendars therefore has three independent
cursors, inventories, and change streams. The default calendar URL remains the
fallback for PIM calls that omit a calendar and for legacy type-scoped cursor
calls; it no longer limits discovered sync coverage. Standalone and
IMAP-composed opens consequently have no collection-limit skip entries.

The default is `Option<String>`, and it is `None` when the calendar home
enumerated no collections. It is deliberately NOT the calendar home in that
case: `list_calendars` already returns the home when the home is genuinely a
calendar collection, so an empty walk means an empty backend, and addressing the
home would send every collection-less call to a resource a spec-correct server
404s - reporting a local routing failure as a remote `NotFound`, and
contradicting the empty-home contract `calendars_list` is pinned to. A call that
names no calendar against such an account fails locally with
`Request(Malformed)` -> `ClientBug` before any I/O.

Only the doors taking an `Option<CalendarId>` reach the default at all -
`event_search`, `event_create` and the legacy type-scoped cursor calls.
`event_get`, `event_update` and `event_rsvp` derive the collection from the
resource's own URL via `bifrost_net::url::parent_collection_url`, which answers
for every id that resolves absolute, so their fallback to the default is
unreachable in practice and kept only as a total match arm.

`open` delegates to `open_with_client` so the whole discovery-to-account path
can be driven against a scripted transport; `open` itself builds its client from
a `CalDavConfig` and so cannot take one. Both halves are pinned - the selection
by `an_empty_home_leaves_no_default_calendar`, and the opened account by
`an_empty_discovery_opens_an_account_with_no_default_calendar`, which is the one
that bites if the home fallback is reintroduced at the call site.

Multi-leg calendar multiget and text-search REPORTs are dispatched
concurrently, bounded to `MULTIGET_LEG_CONCURRENCY` in-flight legs. The bound
lives at this call site because the multiget chunk count is input-sized and
`bifrost-net` carries no concurrency governor; dispatch is ordered, so the
merged report and the surviving degraded error do not depend on completion
order. Offset continuations still re-run search so each page reflects a
fresh server observation and carries that observation's failure lanes.

## The shared layer: bifrost-dav-core

The protocol-neutral half of this crate lives in `bifrost-dav-core`, a PRIVATE
shared crate on the `bifrost-sasl` precedent. Nothing published moved:
`CalDavConfig`, `CalDavCredentials`, `CalDavAccountFactory` and the `Account`
impl are exactly what they were, and consumers see no change.

What moved: `DavRequest` / `DavResponse` / `DavBody`, `settle_body`,
`url_origin`, `origin_is_secure`, the etag helpers (`response_etag`,
`normalize_http_etag`, `prepare_if_match`) and `PutCondition`, the whole
status-to-`AccountError` ladder with its five constructors, and
`worse_recovery` / `recovery_rank`.

Then the whole request dispatcher, as `DavDispatch`: the `AccountNet` handle,
the credential store, the admitted-origin set, the credential gate
(`auth_headers` / `is_trusted_url` / `admit_discovered_urls`), `resolve_url`,
the redirect walk in `send_raw_request`, and the generic verbs `propfind_raw`, `report_raw`, `report_raw_response`,
`delete_resource` and `move_resource`. `CalDavClient` is now a newtype over it
plus the CalDAV-specific bodies and parsers. This was the security-sensitive
half: the credential gate, the HTTPS-downgrade refusal and the redirect walk
were all duplicated, and a divergence there is a credential leak. Ablating
`is_trusted_url` in the shared crate fails three tests in each crate.

`CalDavCredentials` stays published and unchanged; `to_shared` projects it onto
the dispatcher's `DavCredentials`, cloning the `Arc` so a bearer token is still
read live from the shared source at every request.

Finally the XML decoding primitives, in `bifrost_dav_core::xml`: `local_name`,
`normalize_etag`, `resolve_href`, `push_text`, `trimmed`, plus `escape_xml` and
`append_path`. All were byte-identical.

**Where the extraction stops, and why.** The parsers ABOVE those primitives are
NOT shared and are not candidates for it. `PropStat`, `ResponseParts` and
`parse_multiget_report` differ in real content, not in spelling: they carry
different property sets and different entry types (`calendar-data` and
`CalDavEventEntry` against `address-data` and `CardDavContactEntry`), and CalDAV
additionally has the whole `sync-collection` lane that CardDAV has no analogue
for. Unifying them would mean parameterizing the parser over the resource kind,
which is a redesign of both crates rather than a move - the shape option A was
proposed as, and a different decision from the one taken here. The same is true
of the cursor codecs and the snapshot diffs in each `account.rs`: they are
similar in outline and different in substance.

So the rule from the next section still applies to everything that stayed
behind. What changed is that the code most likely to drift, and most damaging
when it does, no longer can.

Also NOT moved, and for the same reason as `not_found_error`:
`bifrost-carddav` has its own `send_status_request` that returns the response
ETag, where CalDAV's discards it. CardDAV's `put_vcard` and `delete_vcard`
report the new validator to their callers and CalDAV's equivalents do not, so
collapsing the two would have dropped an etag on every CardDAV write.

The two dialects differed in exactly three values - the `ResourceKind` a 404 and
a 403 name, the `Protocol` stamp, and the `field` label on a local argument
error - so those became a `DavProtocol` parameter, and this crate binds it once
as `const DAV: DavProtocol = DavProtocol::CalDav`. That binding is load-bearing
for the crate's entire error surface, so
`every_error_this_crate_mints_is_stamped_caldav` pins it; ablating the constant
before that test existed failed exactly one unrelated assertion.

What deliberately did NOT move: `not_found_error` differs between the crates.
CardDAV's attaches an `ErrorScope` for the contact, CalDAV's
`missing_event_error` carries the id in the cause instead. That is a real
behavioural difference, not drift, and flattening it would have changed what
consumers receive - so both stayed local.

## This crate and bifrost-carddav are near-duplicates, and drift is the defect

The transport, credential and error halves are now shared through
`bifrost-dav-core` and can no longer drift. What remains hand-mirrored between
the two crates is the layer above it: the whole `ResponseParts` propstat state
machine, href resolution, multiget classification, the cursor codec, the
snapshot diff, `put_condition`, URL comparison, and the `Unsupported` stubs each
crate carries for the other's domain.

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
