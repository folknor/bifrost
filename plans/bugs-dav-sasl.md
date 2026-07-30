# Bug hunt: caldav, carddav, sasl - resolved + follow-ups

Date: 2026-07-29

Scope: `crates/caldav/src/**`, `crates/carddav/src/**`,
`crates/sasl/src/**`.

The eight reported DAV bugs were confirmed and fixed. The related tests now
pin the corrected behavior. SASL required no production changes; the audit
added coverage for its existing error paths and RFC 7677 SHA-256 vector.

## Resolved

- CalDAV date and date-time projection now treats non-ASCII grammar values as
  verbatim malformed values instead of slicing them at unsafe UTF-8 byte
  offsets.
- CardDAV multiget and query parsing now returns successes and per-resource
  failures, classifies wholly failed 207 bodies, feeds partial failures into
  `Page::failed_ids`, and maps an absent single-resource result to
  `NotFound(Contact)`.
- CalDAV scheduling discovery examines every href in
  `calendar-user-address-set`, so a non-mailto first entry cannot hide a later
  mailto address.
- CalDAV patch splicing only replaces depth-zero VEVENT properties, preserves
  same-named VALARM properties, compares property names case-insensitively, and
  inserts replacement event properties before the first nested component.
- A stale CalDAV sync token now maps to scoped
  `SyncState(CursorInvalid)`, which derives `Engine(RestartScope)` for the
  calendar-event cursor. The sync REPORT also uses the RFC 6578 required
  `Depth: 0`.
- CalDAV projects `DTSTART` plus `DURATION` into an end time. An explicit end
  patch replaces `DURATION` when it emits `DTEND`, so the mutually exclusive
  properties cannot be written together.
- Both DAV crates preserve the weakness marker on weak ETags and omit
  `If-Match` for them. Strong ETags continue to use conditional PUT.
- Every DAV XML parser now accepts CDATA for text-bearing href, ETag, token,
  display-name, calendar-data, and address-data values.

## Related audit fixes

- URI scheme matching for calendar addresses is fully case-insensitive.
- Calendar collection detection only accepts `calendar` within
  `resourcetype`.
- iCalendar and vCard parameter serialization uses RFC 6868 caret encoding,
  and both crates now *decode* it too. caldata's `get_param` returns the raw
  parameter, so without the decode a `CN` written by us - or by any of the
  servers that caret-encode - read back with `^'` inside the display name.
  vCard decodes in its own hand-rolled parameter parser.
- A calendar resource without a VEVENT no longer projects an empty event
  that updates as a silent no-op. `event_from_ical` (get/update) errors;
  `events_from_ical` (range, search) yields no events, because a VTODO or
  VJOURNAL sharing the collection is neither an event nor a failure and a
  retry cannot change that. The previous fabricated event passed
  `event_in_range`'s empty-start guard and reached the consumer, which then
  could not open it.
- CalDAV patch splicing reports "no spliceable VEVENT" when the body offers
  no splice point at all, instead of returning it verbatim - which PUT the
  pre-patch resource back and reported success.
- CardDAV multiget hrefs are rebased onto the resolved absolute native-id
  namespace in the hydration, search, page, and `failed_ids` lanes, matching
  the snapshot lane and the CalDAV fix. A path-only href gave the same
  contact two ids across lanes.
- Corrupt DAV cursor entry counts are checked against the remaining payload
  before allocating.
- CalDAV range and search results use the same resolved absolute native-id
  namespace as inventory and changes, including `failed_ids`.
- CalDAV search projection failures now enter `failed_ids` instead of being
  silently dropped.
- CardDAV unsupported streams emit `Terminated` followed by `Done(None)`,
  matching CalDAV.
- CardDAV contact search sorts by native id before offset paging, making the
  order stable when the remote result set is unchanged.
- vCard preference ranking is per property group: the lowest valid ordinal
  present within EMAIL, TEL, or ADR maps to the shared primary flag, so a
  card whose only preferred email is `PREF=2` still has a primary. vCard 3's
  `TYPE=PREF` continues to mark primary on its own.

## Round 2: the correctness gaps

The five gaps filed as "worth a later pass" are closed.

- **Chunked multiget keeps its partial result *and* its recovery class.**
  Each REPORT - one per multiget chunk, one per searched property - is
  classified where it happens. A leg that failed wholly no longer aborts the
  call once other legs returned data; instead the worst recovery class
  encountered survives in `MultigetFetch::degraded` and the account layer
  publishes it as a `Page::skipped_scopes` entry (`ErrorScope::Calendar` /
  `ErrorScope::ContactCollection`). The distinction matters: `failed_ids` is
  a bare list of ids with no classification, so folding a 401 into it would
  have kept the cards while destroying the "reauthorize" signal. A refusal
  with nothing usable anywhere still rides the `Err` arm unchanged.
- **A 2xx response without `address-data` / `calendar-data` is an absent-data
  per-resource outcome**, not a synthetic 500. It enters `failed_ids` through
  the new `missing_data` lane and cannot make a body classify as a complete
  failure. `as_failed_multiget_resource` now requires an actual non-2xx
  status.
- **`is_primary` ranks within the property group** (above).
- **`escape_param` is RFC 6868 conformant again.** The interim fix doubled
  backslashes, which RFC 6868 does not define - other peers would have read
  the doubling literally, and TZID (which shares the encoder but not the
  tolerant read path) could not round-trip at all. Reads no longer run
  `unescape_text` over every CN; the Exchange tolerance is confined to
  `normalize_exchange_cn_param`, which repairs only an unquoted CN carrying a
  genuine `\,` / `\;` separator escape and re-encodes it conformantly. CN and
  TZID both round-trip a literal backslash now.
- **`contact_search` reports the failures each page actually observed.**
  Every page reruns the remote search, so the failure set is re-observed per
  page; suppressing it after page one silently discarded any resource that
  first started failing on page two. `Page::failed_ids` is documented
  per-page ("resources the provider fetched for this page"), and nothing in
  bifrost-sync accumulates the lane, so per-page reporting is the honest
  reading. The residual cost is recorded under Remaining follow-ups.
- **One outcome per id.** Text search runs a REPORT per property, so the same
  resource could be reported as both materialized and failed when the REPORTs
  disagreed. `one_outcome_per_id` now drops any id from the failure lane that
  materialized in some leg: the data arrived, so success is the true outcome.
  Both crates.

## Remaining follow-ups

Accepted trade-offs and residual risk:

- **Weak ETags disable conflict detection, not just `If-Match`.** RFC 7232
  requires strong comparison for `If-Match`, so a weak validator has no
  conforming conditional form and the PUT goes out unconditional. Against a
  server that only ever emits weak ETags, `event_update` / `contact_update`
  have no lost-update protection at all - a silent overwrite, where the old
  (invalid) behavior at least got a deterministic 412. No better option
  exists inside HTTP; a consumer that needs the guarantee needs an
  application-level revision check.
- **VTODO / VJOURNAL resources still occupy the event cursor.** The snapshot
  and changes lanes key on the PROPFIND href listing, which does not carry
  the component type, so a task resource in a shared calendar collection is
  still emitted as a created/updated event change. Hydration then yields
  nothing. Filtering needs either a component-type PROPFIND or a first-fetch
  classification cache.
- **A literal backslash in a CN survives, an Exchange-style escaped one is
  normalized.** RFC 6868 defines no escape for a backslash, so the only
  conformant encoding is the character itself - which means a display name
  that genuinely reads `Doe\, John` (backslash included) is indistinguishable
  on the wire from Exchange's escaping of `Doe, John`, and this crate resolves
  the ambiguity in Exchange's favour. That is the spec's limit, not a bug we
  can encode our way out of.
- **`contact_search` names a persistently failing resource once per page.**
  Each page reruns the search, so a resource failing throughout appears in
  every page's `failed_ids`. A consumer accumulating across pages must treat
  the lane as a set. The alternative - carrying the already-reported ids in
  the page cursor - makes the cursor grow with the failure set, which is
  worse for the case that matters (a wholly failing address book).
- `event_in_range` trusts the server for COUNT-bounded recurrence expansion.
  Fully defending against a hostile server would require recurrence expansion
  that does not exist in this crate.

Smells and nits:

- **`sync_events` duplicates `report_raw` + `send_body_request`** (~25 lines)
  only to send `Depth: 0` and inspect the status before parsing. A
  `report_raw_with_depth`, or a raw variant returning status plus body, would
  keep one place where REPORT auth headers and transport errors are built.
- **`status != StatusCode::MULTI_STATUS` is redundant** next to
  `status.is_success()` - 207 is 2xx. Present in the new inline sync path and
  in the pre-existing `send_body_request` it was copied from.
- **`resolve_url` is applied at every use site** rather than once at the
  parse boundary. Resolving where responses are decoded would make a
  mismatched id namespace structurally impossible; the drift it allows is
  exactly what cost CardDAV a fix in this round.
- `event_end_from_duration` re-projects DTSTART through
  `event_time_from_property`, which `project_event` has already called for
  `start`. Harmless duplicate work, once per DURATION-bearing event.
- `IcalParseError` now also carries "resource contains no VEVENT", which is
  not a tokenizer failure. `event_get` on a VTODO href therefore reports a
  projection error where `NotFound` or `Unsupported` would describe it
  better.
- CalDAV snapshot fallback still refreshes the collection sync token by
  repeating the depth-1 calendar listing. A depth-0 sync-token PROPFIND on the
  selected collection would be cheaper.
- Both DAV clients still drive reqwest directly. Hermetic request transcript
  tests require a transport seam around `send_body_request` and
  `send_status_request`; real listeners remain outside this repository's test
  policy.

The broader structural follow-ups for ADR and ORG slots and multi-value
RDATE/EXDATE modeling remain in
`plans/dav-parsing-robustness-2026-06-17.md`.

## Verification

`brokkr check` passes workspace clippy and all hermetic tests: 4,432 passed,
4 ignored.
