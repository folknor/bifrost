# bifrost-jmap bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/jmap/` including
`crates/jmap/src/sync/`. Findings are unverified work material.

Tree was clean at hunt time (`brokkr check -p bifrost-jmap`: 489 tests pass, zero
clippy/gremlins), so everything below is behavior the suite does not cover.

## Coverage gap audit, round 3

Round 3 audited both directions of the mapping in
`crates/jmap/src/sync/calendar_ops.rs` and
`crates/jmap/src/sync/contacts.rs`. The named RRULE/`UNTIL`, all-day end, and
RSVP/participation claims were checked against RFC 5545, RFC 8984, RFC 9553,
the shared bifrost types, and the actual payload builders and readers.

The all-day exclusive-end claim was correct. An inbound `P1D` event maps from
start date D to shared end D+1, and outbound start D / end D+1 maps to `P1D`.
The existing absolute-value tests exercise each direction independently.

The `UNTIL` claim was false and the mapper was wrong. It copied RFC 5545 basic
DATE or DATE-TIME text directly into JSCalendar's extended `LocalDateTime`, and
rejected UTC UNTIL even though the event timezone is available at the create
boundary. The reader copied extended JSCalendar text back into an invalid
RFC 5545 RRULE. The mapper now converts syntax in both directions, resolves a
zoned event's UTC UNTIL into its JSCalendar wall clock on write, converts that
wall clock back to UTC on read, preserves floating sense, pairs all-day starts
with DATE UNTIL, and preserves the inclusive bound. Tests pin the actual wire
values, including a Europe/Oslo daylight-offset conversion, rather than merely
round-tripping the same bug. The durable reference was corrected from
"UTC is rejected" to the implemented and standards-conforming conversion.

The recurrence reader also accepted answers it could not faithfully project.
It selected the first of multiple recurrence rules, ignored excluded rules,
and omitted unknown rule properties while returning a plausible partial
RRULE. These cases now return `Unsupported`. Outbound unsupported RRULE parts
remain rejected before a Set request is built.

The all-day audit did not find an off-by-one error. The shared boundary remains
start-inclusive/end-exclusive and the JSCalendar duration is the exact day
span for the DATE-valued shared events this account surface creates.

The local range overlap check treated both interval ends as inclusive, so an
event ending exactly at the requested start or starting exactly at the
requested exclusive end survived hydration as a false positive. The predicate
now uses half-open interval overlap, with absolute boundary tests.

The participation audit found that attendee writes omitted JSCalendar
`expectReply`, losing iCalendar RSVP intent. Shared attendees now write
`expectReply: true`. The represented `ROLE` and `CUTYPE` cases continue to map
through JSCalendar roles. Unknown shared roles or participation statuses are
rejected before Set construction, and unknown enabled inbound JSCalendar roles
or `participationStatus` values return `Unsupported` instead of being labeled
`Unknown` while the enclosing event reports success. RSVP participant
selection and dotted-path patching were verified; the authenticated-email,
single-attendee fallback, and ambiguity behavior matches the durable
reference.

The contact audit found two symmetric wire-shape defects that the old tests
could not catch. Postal addresses used obsolete flat `street`, `locality`,
`region`, `postcode`, and `country` members on an Address. RFC 9553 represents
these as `AddressComponent` objects in `components`. Writes now emit the
standards shape and reads consume it; tests assert the literal component JSON.
An unknown address-component kind now makes hydration `Unsupported`, because
the shared `ContactAddress` cannot preserve it. Job titles were likewise
written and read as a nonexistent `Organization.title`; they now use separate
RFC 9553 `Title` objects linked by `organizationId`.

A cold review of that fix pass found three defects it introduced or left, all
fixed in the same round:

1. Threading the start and all-day flag into `UNTIL` conversion fixed the
   conversion but broke the update path. A patch that changes only the RRULE
   carries neither field, and the conversion defaulted to a timed, floating
   event, so floating, UTC, and all-day `UNTIL` values were all rejected on a
   normal recurrence-only update. `update` now reads the current event for
   exactly the fields the patch leaves unset. The same read fixes a latent
   defect it uncovered: a start/end patch that did not restate `is_all_day`
   defaulted it to false and wrote a timed start over an all-day event.
   Omitting `is_all_day` now means unchanged.
2. The fix pass wrote into the durable reference that modified recurrence
   overrides return `Unsupported`, but the code still fell through and
   discarded them, returning the master event as though the modified
   occurrence did not exist. A pre-existing test pinned that silent discard as
   correct behavior. The code now rejects, and the test was rewritten.
3. `subdistrict`, `district`, and `separator` passed the new supported-kind
   check and were then neither mapped to a shared scalar nor folded into
   `street`, so the new rejection policy still permitted lossy hydration. The
   accepted set and the projected set are now one pair of constants, so they
   cannot drift again.

A fourth defect surfaced from a test written during that fix, not from either
review. JSCalendar has no DATE type: RFC 8984 `start` is always a
`LocalDateTime`. The mapper copied the shared bare date straight onto the wire
(`"start": "2026-06-02"`), which no conforming server accepts, and the read
path truncated all-day values the same way, so every round trip was green and
the wire value was wrong. This is precisely the symmetric-bug shape, and it
survived the fix pass's own all-day audit, which concluded the direction was
correct. Both directions were corrected and pinned with absolute wire values.

Cross-crate: nothing here required another crate to ship anything, so no
`TODO.md` entry was added. The conversion failures propagate through the
existing account-operation error boundary, so a page or get cannot present a
lossy object as a successful hydration.

### What this document does not cover

This is the last round for this document, so the remaining gaps are recorded
rather than left implied.

- The audit was static, against the RFCs and the in-crate types. No JMAP
  server was involved, per the project's testing rules, so "a conforming
  server accepts this" remains a reading of the specification and not an
  observation. The all-day DATE defect above is exactly the class of error
  that a live round trip catches and an in-process round trip does not.
- `event_from_jmap` now fails a whole hydration when any single event carries
  an unrepresentable recurrence, participant role, or participation status.
  That is the correct direction against silent lossiness, but it means one
  odd event can fail the page that contains it rather than being reported
  individually. Whether the engine wants a per-item lane here is a
  `BatchOutcome` shaping question for `bifrost-sync`, not a JMAP bug, and it
  is not answered here.
- The recurrence mapping still covers only `FREQ`, `INTERVAL`, `COUNT`,
  `UNTIL`, `BYDAY`, `BYMONTH`, and `BYMONTHDAY`. Everything else now rejects
  loudly instead of producing a partial rule, but the mapping was not widened
  and `reference/jmap/DEFERRED.md` remains the place that tracks that.
- `contacts.rs` outside postal addresses and titles (emails, phones, notes,
  media, name) was read but not driven to the same reject-unknown-vocabulary
  standard; those readers still skip values they cannot parse.
