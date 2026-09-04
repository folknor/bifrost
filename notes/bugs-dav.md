# Bug hunt: DAV family (dav-core, caldav, carddav)

Hunt date: 2026-09-04. Hunter: Claude (Fable 5), read-only. Scope:
`crates/dav-core/`, `crates/caldav/`, `crates/carddav/`. All three crates and
their reference docs read end to end; findings verified against the quoted code
paths and cross-checked against `reference/caldav.md`, `reference/carddav.md`,
`reference/error-model.md`.

## High-confidence defects

(Finding 1 - cursor invalidation hardcoding the legacy type-wide error scope -
is fixed: `cursor_invalid_error` now takes the calendar URL and scopes the
error `CursorScope::Folder(collection url)`, so the engine restarts the
actually-invalid per-calendar cursor. Test flipped to pin the folder scope and
revert-and-confirmed; `reference/caldav.md` updated.)

(Finding 2 - truncated sync-collection responses misread as creations, with the
partial token checkpointed as complete - is fixed:
`CalDavSyncReport::take_collection_response` drops the response whose href is
the collection itself (comparing through the new normalizing
`bifrost_dav_core::same_dav_url`) and records its `507` as
`CalDavSyncReport::truncated`; `apply_sync_report` now preserves rather than
upserts any member whose status is neither 2xx nor 404/410; and
`changes_from_cursor` drains a truncated result by re-issuing the REPORT with
each returned token, bounded by `SYNC_TRUNCATION_ROUNDS` with a
same-token forward-progress guard. Pinned by
`a_refused_sync_member_is_preserved_rather_than_upserted` and
`a_truncated_sync_report_is_drained_before_the_cursor_advances`, both
revert-and-confirmed; `reference/caldav.md` updated.)

(Finding 3 - a UTC instant under a TZID emitted as the UTC wall clock,
shifting the event by the zone offset - is fixed: `instant_zone_wall` projects
the instant into the named zone (UTC fallback only for unknown zones) in both
`ical_time_from_event_time` and `event_naive_local`, so the DTSTART/DTEND
values and the VTIMEZONE anchor agree. The two serializer tests that pinned
the shifted output now pin the zone-local rendering.)

(Findings 4 and 5 - the depth-1 listing committing `getetag` from failed
propstats, and the zero-propstat response reading as an entry in CalDAV but
vanishing from both lanes in CardDAV - are fixed as one change:
`parse_propfind_events` now stages `getetag` and `getcontenttype` like every
other property (`PropStat` grew a `content_type` slot, committed on 2xx only),
and `as_contact_entry` adopts the CalDAV rule - only a response whose ONLY
propstat failed is withheld, so a bare `<response><href/></response>` commits as
an etag-less entry instead of being destroyed by the diff. Pinned by
`propfind_events_ignores_an_etag_inside_a_failed_propstat` and
`a_propstat_less_response_is_an_entry_rather_than_a_vanished_contact`, both
revert-and-confirmed; both reference docs updated. The `ResponseParts<P>`
redesign the fix grouping mentions was NOT done - it remains the owner's call.)

## Contract / spec findings, confident

(Finding 6 - well-known discovery falling back only on 404 - is fixed in both
crates: `should_fallback_discovery`, which is consulted for the well-known PROBE
only, now also accepts a 405 (static site or proxy on the origin root) and a
locally-refused redirect (`Request(Malformed)` from the redirect walk - RFC
6764's canonical cross-origin well-known, which the credential gate cannot admit
before discovery authenticates anything). 401 and 403 still fail the open, as
the original comment argued. Pinned by
`discovery_falls_back_on_not_found_405_and_a_refused_redirect` in each crate,
revert-and-confirmed in CalDAV; both reference docs updated.)

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

(Finding 9 - CardDAV address books always advertising writable - is fixed:
`PROPFIND_ADDRESSBOOKS` now requests `current-user-privilege-set`, the collection
parser stages and commits `privilege`/`write` markers exactly as the CalDAV twin
does, `AddressBookCollection` carries `can_edit: Option<bool>`, and
`map_addressbook` derives all three flags from it (unknown still means writable).
Pinned by `addressbook_collections_derive_writability_from_the_privilege_set`,
`an_unanswered_privilege_set_leaves_writability_unknown` and
`a_read_only_address_book_is_not_advertised_as_writable`, revert-and-confirmed;
`reference/carddav.md` updated.)

## Lower-confidence / latent

### 10. `DavRequest::header` / `dispatch_once` header copy drop invalid header values (residual)

The credential half of this finding is fixed (`auth_headers` now errors
locally on header-invalid bytes). Residual: `DavRequest::header` and the
`dispatch_once` header copy (`value.to_str().ok()`) still silently drop
non-ASCII / invalid header values; those inputs are crate-internal today, so
lower priority.

(Finding 11 - non-VALARM nested components leaking their properties into the
VEVENT - is fixed: `parse_vevents` tracks nesting depth and skips every line
inside an unmodeled component, VALARMs nested in it included. Pinned by
`an_unknown_nested_component_does_not_leak_into_the_event`; the ablation
reproduced the real defect - the sub-component's TZID DTSTART won over the
event's UTC one - and `reference/caldav.md` is updated.)

(Finding 14 - a standalone `TITLE` dropped on read and then destroyed by an
`organizations` patch - is fixed: `parse_vcard` maps a TITLE with no pending ORG
to an organization with an empty name, and `append_organizations` omits the ORG
line for such an entry rather than inventing a bare `ORG:`. Pinned by
`a_standalone_title_survives_read_and_write`, revert-and-confirmed;
`reference/carddav.md` updated.)

(Finding 15 - duration-derived ends under a TZID using DST-blind civil
addition - is fixed: `event_end_from_duration` routes an EXACT (time-only)
duration through the named zone via the new `exact_zone_end`, keeping civil
arithmetic for the nominal DAY/WEEK parts RFC 5545 s3.3.6 defines as nominal,
and for unknown zones. Pinned by
`a_duration_end_under_a_tzid_respects_the_dst_transition`, which covers both
halves and was revert-and-confirmed; `reference/caldav.md` updated.)

(Finding 16 - `same_url`/`same_collection_url` as byte comparisons after a
slash trim - is fixed: both delegate to the new
`bifrost_dav_core::same_dav_url`, which compares scheme, case-folded host,
`port_or_known_default` and percent-decoded path SEGMENTS (so an encoded `%2F`
stays distinct from a real separator) and falls back to the old comparison for
anything that will not parse. A restated collection id therefore no longer reads
as a relocation and no longer issues a MOVE onto the collection the resource is
already in. Pinned by the two dav-core unit tests plus
`a_restated_calendar_url_is_not_a_relocation` and
`a_restated_address_book_url_is_not_a_relocation`; the dav-core pair was
revert-and-confirmed.)

## Fix grouping

Findings 4 and 5 were propstat-state-machine discipline defects and were worked
as a single change (13, the third of the group, is closed as an accepted,
commented edge). The `ResponseParts<P>` redesign below was NOT taken; it stays
the repository owner's decision.

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
