# Bug hunt: DAV family (dav-core, caldav, carddav)

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/dav-core/`, `crates/caldav/`, `crates/carddav/`. All three crates and
their reference docs read end to end; findings verified against the quoted code
paths and cross-checked against `reference/caldav.md`, `reference/carddav.md`,
`reference/error-model.md`.

## High-confidence defects

### 1. CalDAV cursor invalidation names the wrong scope; the engine will restart a cursor that isn't the invalid one

`crates/caldav/src/client.rs`, `cursor_invalid_error` (~line 621): a 403
`valid-sync-token` / 410 on `sync_events` is classified
`SyncState(CursorInvalid)` with
`ErrorScope::Cursor(CursorScope::Type(ObjectType::CalendarEvent))` -
hardcoded, always. But since the multi-collection redesign, live cursors are
`CursorScope::Folder(collection_href)`, one per calendar. Per
`reference/error-model.md`, `CursorInvalid` derives to `Engine(RestartScope)`
and the engine routes the restart by the `ErrorScope::Cursor` payload (the
builder even rejects a scope-less one for exactly this reason). So a stale
sync token on `/cal/work/` tells the engine to restart the legacy type-scoped
cursor - which in the current design doesn't exist as a live scope - while the
actually-invalid folder cursor is never restarted and fails identically on
every subsequent poll: a permanent sync livelock for that calendar.
`sync_events` doesn't know the scope, but `changes_stream` in `account.rs` has
`cursor.scope` in hand and pushes the error through unre-scoped. Fix shape:
thread the scope into `cursor_invalid_error` (or re-scope in
`changes_stream`). The client test at line ~2055 pins the wrong behavior, so
it will need updating. CardDAV has no invalidation path (ctag), so no twin
exists to drift against - which is presumably why this survived.
Test obligation: the client test at ~2055 currently pins the *wrong* scope;
flip it to pin the folder scope, and revert-and-confirm that it fails against
the pre-fix code.

### 2. RFC 6578 truncated sync-collection responses are misread as creations, and truncation semantics are unhandled

`crates/caldav/src/parse.rs` `parse_sync_collection_report` +
`crates/caldav/src/account.rs` `apply_sync_report`. RFC 6578 s3.6: a server
may truncate a sync-collection result, marking it with a `<D:response>` for
the *collection URI itself* carrying `<D:status>HTTP/1.1 507 Insufficient
Storage</D:status>`, plus a sync-token representing only partial progress; the
client must issue another sync REPORT to drain the rest. Here: (a)
`SyncResponseParts` has no collection awareness (the REPORT requests only
`getetag`, so there's no `resourcetype` to key on, but the href equals the
request collection and could be compared), and (b) `apply_sync_report` treats
every entry whose status is not 404/410 as an upsert. Consequences: the 507
marker entry is inserted into the snapshot as an `EventSnapshotEntry` for the
collection URL and a phantom `Created` change is emitted for an "event" that
is the collection itself; and the remaining changes beyond the truncation
point are never fetched this poll - they are only recovered later by etag
drift, if ever, because the *new partial* token is checkpointed as if
complete. The same non-404/410 blanket-upsert also turns any per-member
failure status (e.g. a 403 or 507 on one member) into a Created/Updated with
`etag: None`. iCloud and large Fastmail/Cyrus collections do truncate in
practice. Fix shape: drop the response whose href equals the collection URL;
treat any entry with a non-2xx/404/410 status as a preserved-not-upserted
resource; and (ideally) loop the REPORT until the response arrives
untruncated.

### 3. `ical_time_from_event_time` mislabels a UTC-instant-with-TZID as a wall clock, shifting the event by the zone offset

`crates/caldav/src/ical.rs` (~line 776): when `time.value` parses as a
`Timestamp` (e.g. `2026-06-02T12:00:00Z`) *and* `time.timezone` is
`Some("Europe/Oslo")`, the code tries `time.value.parse::<civil::DateTime>()`,
which **fails** on the trailing `Z`, and falls back to
`Offset::UTC.to_datetime(instant)` - then emits that UTC wall clock under
`DTSTART;TZID=Europe/Oslo:`. 12:00 UTC becomes 12:00 Oslo: a 1-2 hour shift on
the wire. The internal read path never produces this combination (TZID values
project bare), so round trips are safe; but `EventCreate`/`EventPatch` come
from consumers, and a consumer supplying an instant plus a display zone - a
completely natural pairing given `EventTime`'s shape - gets a silently shifted
event. The correct fallback is to convert the instant *into* the named zone's
wall clock (jiff `TimeZone::get` + `to_datetime`), not into UTC. Same latent
shape feeds `push_vtimezones`' anchor via `event_naive_local`, which for a `Z`
value strips the `Z` and reads the UTC wall clock as the zone's local time, so
the VTIMEZONE offset can also be resolved at the wrong local instant across a
DST boundary.

### 4. CalDAV depth-1 event PROPFIND commits `getetag` from *failed* propstats - drift from both its own multiget parser and the CardDAV twin

`crates/caldav/src/parse.rs` `parse_propfind_events` (~line 345):
`(Some("prop"), "getetag") => current.etag = normalize_etag(&text)` writes the
**committed** field directly, bypassing the propstat staging/commit rule that
the same file's `parse_multiget_report` applies (`current.staged.etag`) and
that CardDAV's `parse_propfind_contacts` applies (`current.staged.etag`, line
249). A server echoing a stale etag value inside a non-2xx propstat (the
echoed-prop-skeleton shape both references document defending against) poisons
the snapshot etag, which then feeds the snapshot diff and inventory
fingerprints - a wrong `Updated`/suppressed-update signal. `content_type` has
the same direct write (harmless today, it's unused for identification). This
is precisely the "drift is the defect" class both reference docs warn about,
in the eleventh hand-mirrored propstat machine.

### 5. Listing-lane asymmetry: a response with *no propstats at all* is an entry in CalDAV and silently vanishes in CardDAV

CalDAV `as_event_entry`: rejects only when `saw_failed_propstat &&
!has_success_propstat`, so an href with zero propstats commits as an entry.
CardDAV `as_contact_entry`: requires `has_success_propstat`, so the same
response yields nothing - and `as_failed_contact_href` also rejects it
(`!saw_failed_propstat`), so it lands in **neither** lane. In CardDAV, a
server emitting a bare `<response><href/></response>` for an existing resource
makes it disappear from the snapshot, and `diff_contact_snapshots` then emits
a `Destroyed` for a resource that exists (the failed-href preservation guard
can't help, because the href never reaches `failed_hrefs`). Undocumented
divergence; one of the two behaviors is wrong, and the CardDAV one destroys
data.

## Contract / spec findings, confident

### 6. Well-known discovery only falls back on 404; common real deployments fail the open

Both crates' `should_fallback_discovery` accepts only
`AccountErrorKind::NotFound(...)`. A deployment whose origin root answers the
`/.well-known/caldav|carddav` PROPFIND with 405 (Method Not Allowed - typical
for a static site or proxy in front of a DAV path), 400, or 500 fails `open`
outright even though the configured base URL works. Worse: a well-known that
*redirects to an origin discovery has not admitted* (the canonical RFC 6764
use - e.g. provider host redirecting to `caldav.provider.com`) dies as a local
`Request(Malformed)` in the redirect walk, which is not NotFound, so no
fallback - the open fails. The comment above the code explicitly defends
failing on 401/403, which is right; but 405/redirect-refused are "this isn't a
discovery endpoint" answers just as 404 is. Since a cross-origin well-known
target genuinely can't be admitted before discovery, the pragmatic fix is to
treat local redirect refusal and 405 on the well-known *probe only* as
fallback triggers (or to admit the well-known redirect target for
discovery-only, credential-less probing).

### 8. `contact_get` addresses the resource via `addressbook-multiget` against the derived *parent collection* URL, unlike CalDAV's direct GET

`fetch_contact_resource` REPORTs the parent collection with the (possibly
absolute) contact URL as body href. Two exposures: (a) some servers reject
absolute-URI hrefs in multiget bodies or reject a REPORT on a resource-derived
URL that isn't actually the collection the server thinks it is (nested
collections, principals with split namespaces); (b) `Depth: 0` multiget
against a *derived* parent that happens not to be a collection is a 404/501
where a plain GET of the resource itself would have worked. CalDAV's
`get_event` proves the simpler shape exists. The etag does come back via the
multiget prop, so the divergence has a reason, but a GET returns the ETag
header too - this looks like an accident of history rather than a necessity.
Low-moderate severity, real-server dependent.

### 9. CardDAV address books always advertise `can_create/update/delete = true`; CalDAV derives writability from `current-user-privilege-set`

`map_addressbook` hardcodes the three flags and `PROPFIND_ADDRESSBOOKS`
doesn't even request the privilege set, while CalDAV's `PROPFIND_CALENDARS` +
`mark_privilege_seen`/`mark_write_seen` do the real derivation. A read-only
shared address book advertises writable, and the consumer's capability gate
passes a PUT that will 403. This is the same defect class as the removed
phantom-book `can_create_contacts: true` the CardDAV reference narrates at
length - measured divergence number nine or ten between the twins.

## Lower-confidence / latent

### 10. `DavRequest::header` / `dispatch_once` header copy drop invalid header values (residual)

The credential half of this finding is fixed (`auth_headers` now errors
locally on header-invalid bytes). Residual: `DavRequest::header` and the
`dispatch_once` header copy (`value.to_str().ok()`) still silently drop
non-ASCII / invalid header values; those inputs are crate-internal today, so
lower priority.

### 11. Non-VALARM nested components leak their properties into the VEVENT

`parse_vevents` treats any nested `BEGIN:`/`END:` other than VALARM as
ordinary properties, so a vendor sub-component (X-components exist in the
wild) contributes its DTSTART/SUMMARY/etc. to the master's prop list;
`pick_datetime`'s specificity ladder can then prefer the sub-component's
DTSTART over the master's. The code comment claims "harmless - nothing reads
it," which is not quite true. Cheap fix: track nesting depth and skip
everything inside an unknown component.

### 14. `TITLE` without `ORG` is dropped on read, and patching `organizations` strips a standalone TITLE line

`parse_vcard` only attaches TITLE to a pending org; `should_replace_property`
removes all `ORG|TITLE` lines when organizations are patched. A card with
`TITLE:` but no `ORG:` loses the title on an organizations patch even though
the model never saw it.

### 15. Duration-derived ends under a TZID use civil (DST-blind) addition

`event_end_from_duration` strptime branch - a `DTSTART;TZID=...` +
`DURATION:PT10H` crossing a DST transition projects an end an hour off from
the RFC 5545 nominal-duration rule. Small and defensible either way; noted
because the crate is otherwise scrupulous about fold/gap discipline.

### 16. `same_url`/`same_collection_url` are byte comparisons after slash-trim

Percent-encoding or case differences in host between a consumer-restated
`calendar_id` and the derived parent URL read as a *relocation* and trigger a
MOVE to what is actually the same collection (`Overwrite: F` then refuses with
412, surfacing a spurious conflict). `url_origin`-style normalization before
comparison would close it.

## Fix grouping

Findings 4 and 5 are both propstat-state-machine discipline defects (13, the
third of the group, is closed as an accepted, commented edge). Work them as a
single change (or as part of the `ResponseParts<P>` redesign, if the owner
rules for it below) - do not fix them as separate patches.

## Structural observations (pre-1.0, rewrite-friendly posture)

- **The propstat state machine is the drift engine, and findings 4, 5 and 13
  all live in it.** The references defend not parameterizing the parser as "a
  redesign, not a move" - but the ledger now shows the redesign paying for
  itself: five historical defects plus three found here, all in the ~1500
  hand-mirrored lines. A generic `ResponseParts<P: PropSet>` in dav-core (each
  crate supplying only its staged-property enum and entry constructors) would
  erase the entire class. The reference marks the collapse as a
  repository-owner decision; the hunter registers that the defect count keeps
  voting for it.
- The **cursor codecs, snapshot diffs, offset-page slicers and
  `one_outcome_per_id`** are likewise near-identical twins differing only in
  field names; same argument, lower stakes.
- `event_page` (CalDAV) materializes and sorts the *entire* result set on
  every page fetch, and empty-query `contact_search` re-hydrates the whole
  address book per page. Documented and honest, but for a large collection
  every pagination step is O(collection) in wire traffic. Since the listing is
  already etag-bearing, a page cursor carrying the sorted href watermark
  (`last_href`) instead of an integer offset would keep the stability property
  while allowing the multiget hydration to fetch only the page - no protocol
  obstacle prevents it.
