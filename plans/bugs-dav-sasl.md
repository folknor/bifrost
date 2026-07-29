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
- vCard `PREF=1` alone maps to the shared primary flag; lower preference
  ordinals do not.

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
- `event_in_range` trusts the server for COUNT-bounded recurrence expansion.
  Fully defending against a hostile server would require recurrence expansion
  that does not exist in this crate.

Correctness gaps worth a later pass:

- **Per-chunk complete-failure classification discards earlier successes.**
  `fetch_vcards` / `fetch_events` classify each `MULTIGET_BATCH_SIZE` chunk
  independently and return `Err` on the first wholly failed chunk, throwing
  away the cards already collected from the chunks that succeeded. A walk
  that hydrates 200 resources and then meets a 401 chunk reports only the
  error. Accumulating into the report and classifying once at the end (or
  degrading a late chunk failure into `failed_ids`) keeps the partial page.
- **A 207 with no `address-data` / `calendar-data` anywhere becomes a
  synthetic 500.** `as_failed_multiget_resource` counts any non-collection
  response lacking the data property as failed even when its propstat was
  200; if every response in a chunk looks like that, `classify` returns
  `CompleteFailure { status: None }` and `multiget_failure` invents
  `INTERNAL_SERVER_ERROR`. Both crates share the shape. Requiring an actual
  non-2xx status before calling a response failed would be truer to the body.
- **`is_primary` requires literally `PREF=1`.** A card whose only preferred
  email carries `PREF=2` now has no primary at all. RFC 6350's model is
  "lowest ordinal wins", which needs sibling context the per-parameter check
  does not have; doing it properly means ranking within each property group.
- **`unescape_text` still runs on `CN` after the RFC 6868 decode.** It is
  kept for Exchange-style `CN=Doe\, John`, but it is not the inverse of
  `escape_param`, so a display name containing a literal backslash does not
  round-trip: the write emits it verbatim and the read eats it.
- **`contact_search` repeats the whole `failed_ids` list on every page.**
  `page.failed_ids` is assigned after the offset slice, so a consumer paging
  through N pages sees the same failed ids N times.

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
