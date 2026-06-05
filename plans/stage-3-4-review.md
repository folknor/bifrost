# Stage 3 (contacts) + Stage 4 (calendar) review - open findings

Second review pass over the uncommitted Stage 3/4 work (six agents:
caldav, carddav, jmap, google, graph, types/imap/smtp). The first
pass's claimed fixes were all verified present; this document now
contains only **current open findings** and the **accepted fidelity
limits** that remain deliberate. Resolved items are removed entirely.

Labels: **bug** (silent loss or guaranteed interop failure - fix
before commit), **gap** (silent intent loss, smaller blast radius),
**smell**, **nit**.

---

## Highest priority: SMTP/utf7 test damage (not Stage 3/4 work)

A gremlin-stripping pass deleted emoji from test inputs in
`crates/smtp/` and rewrote expectations to match, silently disabling
the behavior those tests pinned. Restore the test behavior from HEAD,
using `\u{...}` escapes where the gremlin check forbids literal
emoji (the pattern `crates/imap/src/codec/utf7_tests.rs` already
uses).

- **bug** `crates/smtp/src/message/header/mod.rs:655-664` -
  `format_slice_on_char_boundary_bug` is a named regression test for
  a UTF-8 char-boundary panic in RFC 2047 word splitting. Its input
  is now `String::new()`; the panic path is no longer guarded.
- **bug** `crates/smtp/src/message/body.rs:489-500` -
  `quoted_printable_encode_line_wrap` lost the 4-byte char that
  forced the 76-char `=\r\n` soft break; the QP line-wrap path is no
  longer exercised.
- **bug** `crates/smtp/src/message/header/mod.rs:601-678` - the RFC
  2047 encoded-word + folding assertions had their non-ASCII inputs
  stripped; several rewritten expectations look wrong (e.g. line 676
  `=?utf-8?b?AC4=?=` followed by a bare space before CRLF).
- **bug** `crates/smtp/src/message/header/mod.rs:401,427` -
  `non_ascii_headername` / `const_non_ascii_headername` now feed
  empty strings, which happen to also error; the non-ASCII rejection
  path is untested.
- **bug** `crates/smtp/src/transport/smtp/util.rs:31` - xtext
  escaping table lost its multi-codepoint (ZWJ/variation-selector)
  case.
- **nit** `crates/imap/src/codec/utf7_tests.rs:68` - dangling `// `
  comment where the same pass stripped an emoji; the test itself is
  intact (uses `\u{1F4E7}`).

---

## Bugs

1. **caldav** `crates/caldav/src/ical.rs:126-195` - `event_update`
   silently drops `availability` (TRANSP) and `visibility` (CLASS)
   on raw-backed events (the normal path; `event_from_ical` always
   sets `raw_ical`). `patch_to_ical`'s replacement set has no branch
   for either field, so the patch returns `Ok(())` and writes the
   unchanged resource. The non-raw fallback (`create_to_ical`) does
   emit them, so behavior is inconsistent. `reference/caldav.md`
   claims updates "replace only modeled VEVENT properties present in
   the patch" - overstated.
2. **caldav** `crates/caldav/src/client.rs:352-365` -
   `post_schedule_reply` omits the `Originator` and `Recipient` HTTP
   headers that outbox-based CalDAV scheduling requires for iTIP
   routing. Servers implementing the outbox model (the exact ones
   `schedule-outbox-URL` discovery targets) reject the POST, so
   `event_rsvp` fails precisely where it claims to work.
3. **jmap** `crates/jmap/src/sync/contacts.rs:357,406-411` - photo
   writes use `{"kind": "uri", "uri": url}`; JSContact (RFC 9610)
   requires `kind: "photo"`. The read path filters on
   `kind == "photo"`, so self-written photos read back as
   `photo_url: None`, and the payload is non-conformant. The
   round-trip test masks this by hand-crafting correct JSON.
4. **jmap** `crates/jmap/src/sync/calendar_ops.rs:375-379`,
   `crates/jmap/src/sync/contacts.rs:306-310` - create objects omit
   the mandatory top-level `@type` (`"Event"` / `"Card"`).
   Conformant servers reject the create. Not catchable by the unit
   suite (no live server). Nested objects (Participant,
   RecurrenceRule, Name, EmailAddress, Address) likewise lack
   `@type`; the top-level one is the load-bearing fix.
5. **graph** `crates/graph/src/account/contacts.rs:391-489` -
   contact field *clears* are silently dropped on update.
   `merge_patch` collapses `Some(None)` to `None`, then
   `GraphContactPatch`'s `skip_serializing_if = "Option::is_none"`
   omits the field from the PATCH body, so the server keeps the old
   value. The event patch path correctly emits JSON `null` for
   clears; contacts must do the same. No test covers the clear case.
6. **graph** `crates/graph/src/account/calendar.rs:296` -
   `search_with_graph_api` hardcodes `DEFAULT_CALENDAR_ID` into
   every hit's composite `EventId`. Graph Search spans the whole
   mailbox, so results from non-default calendars get an id whose
   calendar segment is wrong; subsequent `event_get`/`update`/
   `delete` build `/calendars/calendar/events/{id}` and 404.

---

## Gaps

- **jmap** `calendar_ops.rs:381-453` - `jmap_patch_from_event_patch`
  never reads `patch.end`. End-only patches are silently lost;
  start-only patches keep the stale `duration` (JSCalendar models
  end as start + duration). The create path computes duration
  correctly; the patch path is asymmetric. Either recompute duration
  from start+end or document end-patching as unsupported.
- **graph** `calendar.rs:761-836` - `relativeMonthly` /
  `relativeYearly` RRULEs with neither BYMONTHDAY nor BYDAY build a
  Graph pattern with `daysOfWeek = None`, which the server rejects
  with 400 - violating the reject-before-payload-construction
  contract. Missing `days_of_week.is_none()` guard on the relative
  arms.
- **graph** `calendar.rs:434-529` - `EventCreate.status` /
  `EventPatch.status` are never written. Creating with
  `status: Cancelled` silently produces a confirmed event. Read maps
  `isCancelled`; there is no write path.
- **google** `calendar.rs:488` - recurrence cannot be cleared on
  update: an empty `EventRecurrence` yields `recurrence_lines() ==
  None`, the key is omitted from the PATCH, and Google keeps the
  existing RRULE. The clear is silently dropped.
- **google** `calendar.rs:468-500,631-642` - an `is_all_day`-only
  patch is a silent no-op: `event_patch_has_non_move_fields` triggers
  a PATCH, but `google_event_from_patch` only emits date/dateTime
  shape when start/end are also present.
- **google** calendar mutations ignore etags: `update`, `delete`,
  and `rsvp` issue blind PATCH/DELETE with no `If-Match` even though
  `GoogleEvent.etag` is read. Consistent with the advertised
  `MutationConcurrency::None`, but a real lost-update window
  (`rsvp`'s read-modify-write of the full attendee array can clobber
  a concurrent attendee edit) and undocumented in
  `reference/google.md`.
- **caldav** `ical.rs:74-75` - TRANSP and CLASS are never parsed on
  read; `availability` is hardcoded `Unknown` and `visibility`
  `Default`. A created event round-trips to Unknown/Default even
  though the create path wrote the fields.
- **imap** `crates/imap/src/account/scopes.rs:5-16`, `mod.rs:861-874`
  - composed CardDAV/CalDAV sub-accounts are primitive-only:
  `discover_cursor_scopes` never emits their contact/calendar cursor
  scopes and `folder_from_scope` returns `Unsupported` for non-Folder
  scopes, so contact/calendar *sync* dead-ends in the IMAP account.
  `reference/imap.md` is honest about this; decide whether
  sync-integrated composition is in scope or document it as the
  contract.
- **types** `crates/types/src/contact.rs:124-135` - `ContactCreate`
  has `photo_url` but no inline `photo` (unlike `ContactCard` and
  `ContactPatch`), so inline-photo contacts require create-then-
  update. Likely intentional (CardDAV/People treat photo upload as a
  separate step) but undocumented. Add the field or document the
  two-step requirement.

---

## Smells

- **caldav** `ical.rs:395-461` - TZID datetimes are read as
  wall-clock digits with a `Z` appended, so `EventTime.value`
  (documented RFC 3339 UTC) holds local time mislabelled as UTC -
  off by the zone offset for any consumer that trusts it. A
  consequence of the VTIMEZONE-stub limit, but the docs frame it
  only as "fixed-offset stubs", not as mislabelled instants.
- **graph** `calendar.rs:426` - `recurrence_id` is populated from
  `seriesMasterId` (the master series id), but the shared model
  documents it as RECURRENCE-ID semantics (an overridden occurrence).
  Consumers following the docstring will misinterpret it.
- **graph** `contacts.rs:367,394-397` - `ContactEmail.kind` maps
  to/from Graph `emailAddress.name`, which is a display name, not a
  type label. Round-trip is consistent (no loss) but semantically
  conflated.
- **imap** `factory.rs`, `capabilities.rs:88-103` - capability flags
  key on `sub.is_some()`, never consulting the sub-account's
  `pim_methods`. Correct today only because the DAV crates hardcode
  all flags `true`; if either ever conditionally disables a method,
  IMAP over-advertises.
- **imap** `factory.rs` - `open_carddav(...).await?` /
  `open_caldav(...).await?` mean a transient DAV outage fails the
  whole IMAP account open, taking mail sync down with it. Currently
  a product decision made by accident; decide fail-hard vs fail-soft
  deliberately.
- **jmap** `calendar_ops.rs:618-619,651-652` - RFC 5545 `UNTIL=...Z`
  is copied verbatim into JSCalendar `until`, which is a
  LocalDateTime (no `Z`). Internal round-trips are lossless but
  outbound payloads are non-conformant; normalize or reject like the
  other unsupported RRULE parts.
- **carddav** `account.rs:402-417` - the ctag is encoded into the
  cursor envelope but never compared, so `changes_stream` always
  does the full PROPFIND + diff; the natural "collection unchanged"
  short-circuit is absent. The cursor pays to carry state it never
  reads.
- **carddav** `parse.rs:309-328` - `ResponseParts` carries fields
  unused by each of its three parser call sites; the shared
  staging/commit logic touches fields irrelevant per site. Mild
  maintenance tax.

---

## Nits

- **caldav** `account.rs:37-41` - `discover_calendar_user_email` and
  `discover_schedule_outbox_url` run unconditionally on every
  `open` (4+ PROPFIND round-trips, errors swallowed via
  `.ok().flatten()`), even for accounts that never RSVP.
- **caldav** `ical.rs:729-745` - fold continuation lines can reach
  76 octets (the leading space is not counted toward the 75-octet
  budget). Within RFC 5545 tolerance; the folding test only checks
  the first-line invariant.
- **carddav** `account.rs:146-159` - multiget failures (failed
  propstat for a requested href) silently drop the contact from list
  pages; no error, no Destroyed.
- **carddav** `account.rs:125-131` - an empty multiget result stamps
  the error `Unsupported(operation)` instead of
  `NotFound(ResourceKind::Contact)`; the HTTP 404 path maps NotFound
  correctly, so the taxonomy is inconsistent for the same condition.
- **google** `calendar.rs:231-259` - cross-calendar search can
  overshoot `limit`: the `.max(1)` on `remaining` plus the
  post-fetch length check lets the page exceed the requested cap at
  calendar boundaries.
- **google** `calendar.rs:846` - `GoogleEvent.recurring_event_id` is
  deserialized but never read (`recurrence_id` is sourced from
  `original_start_time`); only a test fixture references it.
- **google** `contacts.rs:187-205` - `update_contact_photo_url`
  appends `?personFields=...` to the URL while the field mask
  belongs in the request body; redundant at best, response is
  discarded anyway.
- **graph** `capabilities.rs` - the capability test asserts all
  eight contact `pim_methods` flags but none of the nine new
  calendar flags.
- **graph** `calendar.rs:71` - `events_in_range` allows `$top=0`
  (`limit.unwrap_or(250).min(250)` has no lower clamp); the search
  paths clamp to at least 1.
- **jmap** `calendar_ops.rs:769-770` and `jmap_event_status` /
  `jmap_availability` - explicit `Unknown => return None` arm
  immediately followed by an identical `_` wildcard; dead-on-first
  noise.
- **imap** `mod.rs` - new delegation code uses imported
  `AccountOperation::` while 8 pre-existing sites stay fully
  qualified `bifrost_types::AccountOperation::`. Cosmetic.
- **types** `crates/types/src/error/scope.rs:212` - `EventRsvp` is
  classified non-idempotent; setting a fixed RSVP status is
  semantically idempotent. Consistent with the patch-setter
  convention, so a convention call, not a defect.
- **types** `crates/types/src/account.rs:77` - the `Account` trait
  is not `#[non_exhaustive]` despite `plans/unification.md` stating
  it is. Predates Stage 3/4; practical impact low (the attribute on
  traits only restricts external impls).

---

## Accepted fidelity limits (deliberate, documented, not findings)

1. **Recurrence fidelity** - JMAP and Graph expose only common
   recurrence shapes through the shared `EventRecurrence` model;
   unsupported outbound RRULE parts are rejected, not silently
   dropped. CalDAV projects only the first VEVENT (no modified
   override instances), preserves override VEVENTs on raw-backed
   scalar updates, and rejects recurrence replacement when overrides
   are present.
2. **Timezone fidelity** - CalDAV emits and preserves VTIMEZONE
   components but synthesizes only fixed-offset stubs, not full
   transition rules. Graph maps an expanded common IANA-to-Windows
   set and rejects unknown IANA zones instead of silently writing
   UTC; not a complete database.
3. **Provider search limits** - Graph calendar-scoped /
   shared-mailbox / empty / cursor-resume event search and general
   contact substring search are local over list pages; the provider
   APIs document no equivalent server-side filters. Graph Search API
   is used only for unscoped, non-empty default-mailbox event
   searches. Google People and Gmail-side search limits per
   `reference/google.md`.
4. **Trait shape limits** - CardDAV captures fresh PUT ETags but
   `contact_update` returns `Result<(), AccountError>` and cannot
   surface them without changing the shared trait.
5. **Capability semantics** - `PimMethodSupport` advertises dispatch
   support ("implemented, not always-Unsupported"), not lossless
   provider modeling, native server-side search, or coverage of
   every optional provider field. Fidelity gaps live in
   `reference/*.md`, not in the booleans.
6. **CardDAV change detection is polling** - hybrid getctag +
   href/etag snapshot diff, not WebDAV `sync-collection`.
7. **Google organizer is server-derived** - create payloads carrying
   `organizer` are rejected as unsupported on Google and Graph
   rather than silently ignored.
8. **JSCalendar availability** - `freeBusyStatus` has only standard
   `free`/`busy` values, so Tentative/OutOfOffice cannot be
   faithfully represented through the standard field on JMAP.
