# Bug hunt: caldav, carddav, sasl - 2026-07-29

Scope: `crates/caldav/src/**`, `crates/carddav/src/**`, `crates/sasl/src/**`.
Read against RFC 5802/7677/5929/2195 (sasl), RFC 4918/4791/6352/6578/6638
(DAV), RFC 5545/6350 (bodies), plus `plans/dav-parsing-robustness-2026-06-17.md`
and `plans/bug-hunt-2026-06-17.md` to avoid re-reporting settled ground.

Tests were landed separately (list at the bottom). Bugs below are reported
with proposed fixes but NOT fixed; three landed tests explicitly document
behavior I believe is wrong and say so in their comments
(`non_ascii_datetime_value_panics_in_projection`,
`duration_based_end_is_not_modeled`,
`multiget_complete_failure_is_indistinguishable_from_empty`).

## Bugs

### B1. caldav: remote-controlled panic on non-ASCII date-time values

`crates/caldav/src/ical.rs`, `format_ical_time` and `ical_offset_suffix`.

`format_ical_time` slices the raw property value at fixed byte offsets:
`&value[0..4]`, `&value[4..6]`, ..., `&value[9..11]`, and (for the date arm)
`&value[0..4]`/`[4..6]`/`[6..8]`; `ical_offset_suffix` slices `&value[15..20]`.
The only guard is `value.len() >= 15` (or `== 8`), which is a BYTE length.
Any value containing multi-byte UTF-8 such that one of those offsets is not a
char boundary panics with "byte index N is not a char boundary".

Path to failure: server returns a `.ics` whose `DTSTART`/`DTEND` (or a VALARM
`TRIGGER` on the absolute path, via `trigger_from_property`) carries a
non-ASCII value of >= 15 bytes, e.g. `DTSTART:` + five EURO SIGNs (15 bytes).
`event_from_ical` / `events_from_ical` panic instead of returning
`IcalParseError`, so the per-resource degrade contract (skip + failed_hrefs)
is bypassed and the whole `events_in_range` / `event_get` / search future
unwinds. Every other malformed-body shape in this crate degrades to a
per-resource error; this one kills the pull.

Proposed fix: at the top of `format_ical_time`, bail to the verbatim
`value.to_string()` fallback unless `value.is_ascii()` (the iCalendar
date/date-time grammar is ASCII-only, so this loses nothing), or check
`is_char_boundary` before each slice. Same guard in `ical_offset_suffix`.

Landed test: `non_ascii_datetime_value_panics_in_projection`
(`#[should_panic]`, flagged as documenting the bug; flip it to a
non-panicking assertion with the fix).

### B2. carddav: multiget failures silently vanish; all-failed 207 is an empty success

`crates/carddav/src/parse.rs` `parse_multiget_report`,
`crates/carddav/src/client.rs` `fetch_vcards` / `query_vcards_text`,
`crates/carddav/src/account.rs` `hydrated_contacts_page` /
`fetch_contact_resource`.

This is exactly the defect commit 50335e0 fixed on the CalDAV side, still
present on the CardDAV side (and the drift the open F5 follow-up in
`plans/bug-hunt-2026-06-17.md` predicted). `parse_multiget_report` returns
only `Vec<CardDavFetchedVCard>`; a response whose propstat is non-2xx (or
2xx with no `address-data`) is dropped on the floor:

- A single refused resource inside the 207 disappears from
  `contacts_list`'s page without landing in `Page::failed_ids`
  (`partition_hydrated_vcards` only catches resources that WERE returned
  but do not parse). The consumer cannot tell it from a deletion.
- A wholly refused body (all-401, all-503) parses to an empty Vec that is
  indistinguishable from a legitimately empty result. RFC 4918 s13 says a
  207 can describe complete failure; handing it back as an empty page lets
  a consumer record the walk as complete and drop the contacts. An all-401
  body should reauthorize, an all-503 should retry.
- Downstream mis-classification: `fetch_contact_resource` maps "no card
  came back from the single-resource multiget" to
  `unsupported_error(operation)`, so a `contact_get` of a deleted contact
  (404 propstat inside the 207) surfaces as `Unsupported(ContactGet)`
  instead of `NotFound(Contact)`.

Proposed fix: mirror the CalDAV shape - a `CardDavMultigetReport { cards,
failed: Vec<{href, status}> }` with propstat-scoped commit (the
`ResponseParts` machinery is already there), a `classify()` that treats
all-failed-with-a-non-404/410 as complete failure routed through
`status_error`, failed hrefs fed into `Page::failed_ids`, and a 404-aware
error in `fetch_contact_resource`.

Landed test: `multiget_complete_failure_is_indistinguishable_from_empty`
(flagged as documenting the gap).

### B3. caldav: only the FIRST href of calendar-user-address-set is examined

`crates/caldav/src/client.rs` `discover_email_from_root` +
`crates/caldav/src/parse.rs` `extract_href_property`.

`extract_href_property` returns on the first `<href>` it sees inside the
property. `CALDAV:calendar-user-address-set` routinely carries several
entries - a principal URL and/or an `https:` form first, the `mailto:` form
later (sabre/dav, iCloud, Fastmail all do this). When the first entry is
not `mailto:`, `mailto_email` returns None, discovery yields `Ok(None)`,
`scheduling_available` goes false, and the account advertises
`event_rsvp = false` even though the server advertised a perfectly good
calendar address. The user loses RSVP for no reason, silently.

Proposed fix: add an `extract_all_href_properties(xml, name) -> Vec<String>`
(same walk, push instead of return) and `find_map(mailto_email)` over it.
`discover_schedule_outbox_from_root` is fine with first-href (the outbox is
a single collection), but the address-set is a set by name and by RFC 6638.

### B4. caldav: patch splice deletes same-named properties inside VALARMs

`crates/caldav/src/ical.rs` `replace_first_vevent_properties`.

The splice skips every logical line in the first VEVENT whose name is in
`replace_names`, with no tracking of nested `BEGIN:VALARM`/`END:VALARM`
blocks. VALARM legally carries `DESCRIPTION` (mandatory for DISPLAY and
EMAIL alarms), `SUMMARY` (mandatory for EMAIL alarms), and `ATTENDEE`
(mandatory for EMAIL alarms) - all names the patch path replaces.

Path to failure: event has a DISPLAY alarm
(`ACTION:DISPLAY`/`TRIGGER:-PT15M`/`DESCRIPTION:Ring`). Consumer patches the
event description. The alarm's `DESCRIPTION:Ring` line is removed from
inside the VALARM, the replacement `DESCRIPTION:` is emitted at event level
before `END:VEVENT`. The written resource has a VALARM that violates RFC
5545 (DISPLAY alarm without DESCRIPTION); strict servers 400/403 the PUT,
lenient ones store a corrupted alarm. Patching title or attendees corrupts
EMAIL alarms the same way.

Proposed fix: while iterating groups inside the first VEVENT, track a
nested-component depth (any `BEGIN:` other than the event's own) and emit
those groups verbatim without name-matching. While there, consider
inserting the replacement lines before the first nested `BEGIN:` rather
than before `END:VEVENT` - the RFC 5545 VEVENT ABNF is `eventprop *alarmc`,
so properties after an alarm block are technically malformed too.

### B5. caldav: stale sync-token response never maps to a cursor-reset class

`crates/caldav/src/client.rs` `status_error` /
`crates/caldav/src/account.rs` `changes_from_cursor`.

RFC 6578 s3.2: a server that no longer recognizes the client's sync token
responds 403 with the `DAV:valid-sync-token` precondition (some
implementations use 410). `status_error` maps 403 to
`Authorization(PermissionDenied)` and 410 to a generic server error.
Neither is a SyncState class, so when a calendar's sync token expires (all
real servers truncate history), every `changes_stream` poll terminates with
what looks like a permissions problem, and the engine has no signal to
re-establish the cursor. The account wedges permanently on a routine,
expected server condition.

Proposed fix: in `sync_events`, on a 403/410, sniff the response body for
`valid-sync-token` (or unconditionally on this REPORT, since a 403 on
sync-collection with a token is overwhelmingly the stale-token case) and
build a SyncState-class error (the same lane `cursor_error` uses carries
`RecoveryClass` semantics the engine reacts to), so the consumer falls back
to `establish_initial_cursor` + snapshot diff.

### B6. caldav+carddav: DTSTART+DURATION events project with an empty end

`crates/caldav/src/ical.rs` `project_event`.

RFC 5545 allows `DTSTART` + `DURATION` instead of `DTEND`, and real
producers (Outlook exports, some Google exports, invitation iMIP payloads)
emit it. `project_event` reads only `DTEND`; a DURATION event gets
`end.value == ""`. Consequences: the consumer renders a zero-length/end-less
event; `event_in_range`'s local guard falls back to comparing start only;
and worst, `patch_to_ical` on such an event with `patch.end` set writes a
`DTEND` line while the original `DURATION` line is preserved verbatim -
producing a VEVENT with BOTH, which RFC 5545 forbids and servers may
reject or resolve unpredictably.

Proposed fix: when `DTEND` is absent and `DURATION` present, compute the
end from start + parsed duration on projection (or at minimum carry the
DURATION through so the patch path replaces it: add `DURATION` to
`replace_names` whenever `DTEND` is emitted).

Landed test: `duration_based_end_is_not_modeled` (flagged as documenting
the gap; update the expected end when fixed).

### B7. Weak ETags are mangled into invalid If-Match values (both DAV crates)

`crates/caldav/src/parse.rs` / `crates/carddav/src/parse.rs`
`normalize_etag`, `crates/caldav/src/client.rs` `response_etag` +
`prepare_if_match` (same pair in carddav's client).

`trim_matches('"')` on a weak etag `W/"abc"` strips only the trailing quote,
yielding `W/"abc` (unbalanced, embedded quote). `prepare_if_match` sees it
does not start with `"` and wraps it: `"W/"abc"` - a garbage If-Match value.
Any server that emits weak validators (proxies and servers doing on-the-fly
content-coding do) makes every conditional PUT fail 412, i.e. updates and
RSVP-writeback permanently error with ConcurrencyConflict.

Proposed fix in `normalize_etag`/`response_etag`: strip an optional `W/`
prefix before trimming quotes, and (since RFC 7232 forbids weak comparison
for If-Match) have `put_condition` fall back to `PutCondition::None` when
the stored validator was weak. Snapshot diffing is unaffected either way
(both sides normalize identically).

### B8. XML parsers disagree on CDATA (dropped values)

`crates/caldav/src/parse.rs`: `parse_calendar_collections` and
`parse_multiget_report` handle `Event::CData`; `parse_propfind_events`,
`parse_sync_collection_report` and `extract_href_property` do not.
`crates/carddav/src/parse.rs`: NO parser handles CData.

A server wrapping an href, etag, or `address-data`/`calendar-data` payload
in `<![CDATA[...]]>` (legal XML, and calendar-data-in-CDATA does occur in
the wild since iCalendar bodies are full of XML-hostile characters) parses
to a missing value: the entry is silently dropped from listings, or a
hydrated body comes back empty and the resource is mis-routed to
failed/skip. The asymmetry within one file shows the handling was intended
everywhere. Proposed fix: add the same `Event::CData` arm to all seven
parsers (it is three lines each).

## Smells / observations (not filed as bugs)

- `mailto()` (ical.rs) and `mailto_email()` (both clients) match only
  `mailto:` and `MAILTO:` exactly; URI schemes are case-insensitive, so a
  `Mailto:` attendee/organizer is dropped. One `to_ascii_lowercase` on the
  prefix fixes it.
- `parse_calendar_collections` sets `is_calendar` for a `calendar` element
  ANYWHERE inside the response - unlike the privilege check right next to
  it, there is no `stack.iter().any(|i| i == "resourcetype")` guard. A
  foreign property embedding a `<calendar/>` element would misclassify a
  non-calendar collection.
- `replace_first_vevent_properties` compares property names
  case-sensitively (`ical_line_name` result vs uppercase `replace_names`).
  RFC 5545 names are case-insensitive; a server emitting `summary:` keeps
  the old line next to the new `SUMMARY:` after a patch (duplicate
  singleton property).
- caldav `escape_param` escapes `"` as `\"` inside a quoted parameter
  value; iCalendar has no such escape (RFC 6868 defines `^'`), so a CN
  containing a double quote serializes an invalid line. Same helper in
  vcard.rs.
- `event_from_ical` on a resource with zero VEVENTs (a VTODO-only body)
  projects an empty event; a subsequent `event_update` finds no VEVENT to
  splice into, silently discards every replacement line, PUTs the
  unchanged body back, and returns Ok. Silent no-op update.
- Cursor decode (`decode_cursor_snapshot`, both crates) does
  `Vec::with_capacity(count)` with a count read from the (locally stored,
  but corruptible) cursor before any payload-length sanity check; a
  corrupted count of u32::MAX attempts a multi-GB allocation. Capping
  capacity by `input.len() / 5` is free.
- Id-namespace inconsistency in caldav: inventory/changes ObjectIds are
  `resolve_url` absolute URLs, but `events_in_range` / `event_search` ids
  and `failed_ids` are the raw hrefs out of the 207 (usually
  path-absolute). A consumer joining the two surfaces by id must
  normalize; worth either documenting or resolving uniformly.
- carddav `unsupported_stream` emits only `Terminated` where caldav's also
  emits `Done(None)`. Cosmetic divergence, but consumers iterating to
  `Done` behave differently across the two crates.
- `event_snapshot` re-runs the depth-1 calendars PROPFIND on every poll
  solely to refresh the sync token; a depth-0 PROPFIND for `sync-token`
  on the one collection would be cheaper (mirror of carddav's
  `collection_ctag`).
- vCard `is_primary` treats ANY `PREF=n` as preferred (RFC 6350 PREF is an
  ordinal 1..100); with several PREF-carrying emails all become primary.
  Acknowledged in a code comment; noting for the model owner.
- carddav search paging: offset cursor over results assembled from four
  per-property REPORTs whose order is server-dependent; pages can skip or
  duplicate entries across calls. Inherent to offset paging, but the
  dedup-then-slice order makes it worse than it needs to be.
- SASL came out clean: RFC 5802 + (now) RFC 7677 vectors pin the proof
  math, server-signature comparison and `Secret::eq` are constant-time,
  the iteration ceiling and nonce-extension checks hold, channel binding
  refuses to guess on EdDSA/unknown OIDs, and OAuth payloads strip the
  `\x01` frame delimiter. The only gaps were untested error paths and the
  missing SHA-256 known-answer vector; both are now covered.

## Tests landed (pin current behavior)

- `crates/sasl/src/scram.rs`:
  `scram_sha256_client_final_matches_rfc_7677_vector` (RFC 7677 s3
  known-answer, both directions),
  `escape_username_escapes_equals_before_comma` (escape-order pin),
  `scram_rejects_malformed_server_first_messages` (missing r/s/i, `m=`,
  i=0, bad base64, non-numeric/negative i),
  `scram_rejects_server_nonce_with_different_prefix`,
  `decode_continuation_decodes_text_and_rejects_garbage`.
- `crates/sasl/src/cram.rs`: `cram_md5_rejects_invalid_base64_challenge`.
- `crates/sasl/src/secret.rs`: new test module - constant-time eq semantics
  incl. length mismatch, Debug redaction, conversion surface.
- `crates/caldav/src/parse.rs`:
  `multiget_response_level_status_reports_failed_resource` (no-propstat
  404 response shape), `classify_carries_none_status_for_a_statusless_systemic_failure`.
- `crates/caldav/src/ical.rs`:
  `non_ascii_datetime_value_panics_in_projection` (**documents bug B1**,
  `#[should_panic]`), `duration_based_end_is_not_modeled` (**documents gap
  B6**), `unescape_text_handles_adjacent_backslashes_in_a_single_pass`,
  `escape_then_unescape_round_trips_text`.
- `crates/caldav/src/account.rs`:
  `rrule_until_parses_date_form_and_case_insensitive_key`.
- `crates/carddav/src/parse.rs`:
  `multiget_complete_failure_is_indistinguishable_from_empty` (**documents
  gap B2**).
- `crates/carddav/src/vcard.rs`: `bare_params_collect_multiple_types`,
  `detect_version_reads_version_line_and_defaults_to_v4`,
  `photo_value_uri_accepts_non_http_uri`,
  `unescape_text_handles_adjacent_backslashes_in_a_single_pass`,
  `escape_then_unescape_round_trips_text`.

## Not gotten to, and why

- No in-memory duplex / stub-transport tests: both DAV crates drive
  `reqwest` directly and expose no transport trait, so a byte-level
  transcript test would need a real listener, which the hermeticity rule
  excludes. If a seam is ever wanted here, the `send_body_request` /
  `send_status_request` pair is the natural trait boundary.
- Could not compile or run anything (orchestrator validates); test code was
  written against the in-file API shapes and existing test idioms.
- The IMAP composition of CalDAV/CardDAV and the SASL consumers in
  imap/smtp are other agents' files and were not read beyond what
  reference docs describe; the B5 recovery-class proposal should be
  sanity-checked against `reference/error-model.md`'s RecoveryClass
  derivation by whoever applies it.
- RRULE COUNT-bounded series in `event_in_range` are trusted to the
  server's REPORT (documented in code); verifying that guard against a
  hostile server that returns non-matching resources would need expansion
  logic that does not exist in-crate. Left as-is deliberately.
