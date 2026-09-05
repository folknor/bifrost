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
redesign the fix grouping mentions landed later the same day under ruling 6;
both rules are now pinned once more at the dav-core level.)

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

(Finding 8 - `contact_get` fetching through `addressbook-multiget` against a
derived parent collection - is fixed: the new `CardDavClient::get_vcard` is a
plain GET of the resource, reading the validator from the `ETag` header through
the same `normalize_http_etag` the multiget `getetag` went through, and
`fetch_contact_resource` (used by both `contact_get` and `contact_update`) calls
it, still mapping a 404 to `NotFound(Contact)` scoped to the id. No documented
reason to keep the multiget was found. Pinned by
`contact_get_addresses_the_resource_with_a_plain_get`, which asserts method and
URL against the transcript and was revert-and-confirmed; the two move tests
moved from a scripted REPORT to a scripted GET. `reference/carddav.md` updated.)

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

(Finding 10 - `DavRequest::header` and the `dispatch_once` header copy dropping
invalid header values - is fixed: `header` cannot return a `Result` without
rewriting every builder chain, so a rejected name or value is RECORDED on the
request (`invalid_header`) and `dispatch_once` refuses it locally with
`Request(Malformed)` before any I/O; the header copy into the `bifrost-net`
builder refuses a non-ASCII `HeaderValue` the same way instead of skipping it.
Pinned by `an_invalid_header_value_is_recorded_rather_than_dropped` in dav-core
and by `a_header_the_record_could_not_carry_fails_before_the_wire` /
`a_non_ascii_header_value_fails_before_the_wire` in bifrost-carddav, which
script an empty transport so a silent drop panics on the exhausted script; all
three revert-and-confirmed. `reference/caldav.md` (the shared-layer section)
updated.)

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
commented edge, and that comment now lives once on
`ResponseParts::failed_resource` in dav-core). The `ResponseParts<P>` redesign
below was subsequently taken, as ruling 6.

## Structural observations (pre-1.0, rewrite-friendly posture)

- (**The propstat state machine is the drift engine** - DONE 2026-09-04 under
  ruling 6. `bifrost_dav_core::ResponseParts<P: PropSet>` now owns the machine
  and every listing lane in both crates runs through `parse_multistatus`; the
  cursor codecs, snapshot diffs and offset-page slicers followed into
  `bifrost_dav_core::snapshot`. The move found one further twin difference on
  top of findings 4, 5 and 13: CardDAV accepted `<addressbook/>` as a
  resourcetype marker anywhere in the prop bag, where CalDAV required it inside
  `<resourcetype>`, so an `<addressbook/>` nested in `<D:owner>` or a server
  extension minted a phantom address book. Closed to the CalDAV behaviour and
  pinned by `addressbook_element_outside_resourcetype_does_not_mark_a_collection`,
  revert-confirmed. `one_outcome_per_id` was NOT collapsed: the two versions
  filter against different things - CalDAV against a materialized href set,
  CardDAV against the `native_id` of parsed cards - and share only their shape.
  The cold review of the collapse caught one hole the move OPENED: the shared
  machine reported a sync member's status as the first readable propstat code
  when no response-level status was present, so a `404` propstat for a
  property the server lacks, beside the `200` propstat carrying the etag, read
  as a removed member and `apply_sync_report` destroyed a live event. The old
  CalDAV parser kept the last propstat's code, which was wrong the same way
  under the opposite ordering. `member_status_code` now lets a response-level
  status win, treats any successful propstat as member success, and reports
  a failed code only for an all-failed response; pinned by
  `a_failed_property_propstat_beside_a_successful_one_is_not_a_member_failure`,
  revert-confirmed.)
- (**The offset page cursor makes every page O(collection)** - DONE 2026-09-06
  under the owner's ruling. `event_page` (CalDAV) materialized and sorted the
  entire result set per page, and empty-query `contact_search` re-hydrated the
  whole address book per page. The integer offset is replaced outright - there
  were no consumers, so no compatibility lane - by a sorted-href WATERMARK
  cursor carrying the last key served. `bifrost_dav_core::snapshot` now owns
  `decode_watermark_cursor` / `encode_watermark_cursor` /
  `slice_after_watermark`, plus `page_after_watermark` for the lanes whose
  REPORT already answers with the object bodies (`events_in_range`, text
  `event_search`, text `contact_search`). The two list-then-hydrate lanes -
  CalDAV's match-all `event_search`, through the new `listed_event_page`, and
  CardDAV's `hydrated_contacts_page`, which both `contacts_list` and
  empty-query `contact_search` now share - page the depth-1 LISTING and
  multiget only that page's hrefs, which is the traffic this was about. Three
  paged lanes shared the slicer, not two: `contacts_list` already sliced the
  listing, by offset, so it moved as well. An empty cursor payload is refused
  rather than read as a restart from item zero, and a watermark past every
  href is an empty final page that spends no multiget.
  Pinned by `a_match_all_event_page_multigets_only_the_page_hrefs` and
  `a_continued_event_page_multigets_only_what_follows_the_watermark` (CalDAV
  transcripts), `an_empty_query_contact_page_multigets_only_the_page_hrefs`,
  `a_contact_inserted_before_the_watermark_does_not_displace_the_next_page`
  and `a_watermark_past_every_href_is_an_empty_final_page` (CardDAV
  transcripts), plus the codec round-trips and the insert property at each
  level; all revert-and-confirmed, the offset tests moved rather than dropped
  (test count +11, none lost). Both reference docs updated. One incidental
  removal: `CardDavAccount::hydrated_contacts`, a private helper whose only
  caller was the empty-query search lane it no longer has.)
- (**The FILTERING lanes were still O(collection) per page** - DONE 2026-09-06,
  the dav-B8 residual, closed. `events_in_range`, text `event_search` and text
  `contact_search` hydrated the whole result set before the watermark slice
  because they needed bodies to decide membership. The filter now runs on the
  SERVER and the query asks for `getetag` only: the REPORT names candidate
  hrefs, those are sorted and sliced at the watermark, and the multiget carries
  the page. `bifrost_dav_core::query` owns the shared half (`FilteredHrefs`,
  `HrefQuery`, `sorted_candidate_hrefs`), and `filter_unsupported` joins the
  error ladder.

  Three decisions worth keeping:

  - **The server filter is a PREFILTER; the local match is the authority over
    the page.** `event_matches` / `contact_matches` read projected fields and
    fold case with Rust's full Unicode rules, which `i;unicode-casemap` over
    raw properties does not reproduce. Wide on the server, narrow locally - the
    reverse direction drops matches with no symptom. The cost is that a page
    can be short or empty while still carrying a cursor.
  - **The cursor keys on the resource href, never the event id.** The
    recurrence-qualified `EventId` survives on the ITEMS (a time-range filter
    matches recurring masters, and the local overlap guard still picks the
    in-window instances), but the filtered lane slices before hydration and
    only knows hrefs. Keying on the event id would make a lane and its degrade
    disagree about what was already served, so a server that started refusing
    the filter mid-walk would re-serve or skip the override instances of the
    boundary resource. Consequences: the page size counts RESOURCES, and
    `estimated_total` is the candidate count rather than an item count.
  - **A refused filter degrades, a refused caller does not.** 400, 501, or a
    403 naming a filter precondition degrade to the depth-1 listing (which
    still hydrates only the page); a bare 403 stays `NoPermission` and a 401
    stays a reauthorize signal. One leg of a multi-property search reporting
    the filter unsupported degrades the whole lane, because answering out of
    the properties a server happened to accept narrows the search silently.

  Pinned by `a_range_page_filters_on_the_server_and_multigets_only_the_page`,
  `a_range_page_degrades_to_the_listing_when_the_filter_is_refused`,
  `a_text_search_page_filters_on_the_server_and_multigets_only_the_page`,
  `a_text_search_degrades_when_one_query_leg_refuses_the_filter` (both crates),
  `the_local_match_is_the_authority_over_a_generous_server_filter`,
  `a_failure_first_seen_on_a_later_page_is_still_reported` (moved from a helper
  assertion to a two-page transcript),
  `a_refused_filter_degrades_where_a_refused_credential_does_not`,
  `a_refused_filter_is_told_apart_from_a_refused_caller`, plus the query-body
  and dedup tests; ablated mechanically - the dedup, the 400 arm, the 403
  precondition arm, the local-match filter and the page slice were each
  reverted and confirmed failing. The unit tests that pinned the materialized
  slicers moved down to the href level rather than being dropped.

  CLOSED, the accepted loss this round recorded: the candidate 207 is parsed by
  the LISTING parser, and that parser's failure lane now carries a
  `FailedResource` - href plus the member status from
  `ResponseParts::failed_member` - rather than a bare href. `CalDavEventListing`
  and `CardDavContactListing` therefore run
  `bifrost_dav_core::classify_207`, the one RFC 4918 s13 ladder both crates'
  multiget reports were already reading (their hand-mirrored `classify` bodies
  and `*FailedResource` / `MultigetOutcome` types collapsed into it), so an
  all-refused query 207 is the classified `Err` `events_in_range` used to raise
  through `CalDavMultigetReport::classify` instead of an empty page. The
  depth-1 listing and the snapshot poll go through the same `listing_failure`
  funnel and inherit it; a listing with any committed entry never reaches the
  funnel, so a partially failed 207 still serves its page with the refused
  members on `failed_ids`. Pinned on both sides by
  `an_all_refused_query_207_classifies_rather_than_serving_an_empty_page` and
  `a_partly_refused_query_207_still_serves_the_page`, and in dav-core by
  `a_failed_member_carries_the_status_that_classifies_it` and
  `an_all_failed_207_classifies_where_a_mixed_one_stays_usable`; each ablated
  and confirmed failing.

  CLOSED too: `bifrost_dav_core::page_after_watermark` is deleted. It had no
  caller once every lane paged before hydrating, and dav-core is the private
  shared crate (the `bifrost-sasl` precedent), not a published surface. Its two
  tests were retargeted onto `slice_after_watermark`, which is where both rules
  they pinned actually live - a zero page size is an exhausted page rather than
  one re-emitting its own watermark, and a watermark past every key is an empty
  final page. Neither rule was dropped and no other assertion was lost: the
  `failed_ids` / `skipped_scopes` pass-through the deleted function documented
  was never asserted by them, and it is pinned where it now happens, in the
  lanes' own `Page` construction.)
