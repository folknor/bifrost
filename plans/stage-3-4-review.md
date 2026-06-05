# Stage 3 (contacts) + Stage 4 (calendar) review - open findings

Second review pass over the Stage 3/4 work (six agents: caldav,
carddav, jmap, google, graph, types/imap/smtp), followed by a fix
wave. This document contains only **current open findings** and the
**accepted fidelity limits** that remain deliberate. Fixed items are
removed entirely; the fix wave closed all six bugs, the SMTP/utf7
test damage, and the tractable gaps (see git history for the list).

Labels: **gap** (silent intent loss or design decision pending),
**smell**, **nit**.

---

## Gaps

- **imap** `crates/imap/src/account/scopes.rs:5-16`, `mod.rs`
  (`folder_from_scope`) - composed CardDAV/CalDAV sub-accounts are
  primitive-only: `discover_cursor_scopes` never emits their
  contact/calendar cursor scopes and `folder_from_scope` returns
  `Unsupported` for non-Folder scopes, so contact/calendar *sync*
  dead-ends in the IMAP account. `reference/imap.md` is honest about
  this. Decide whether sync-integrated composition is in scope or
  document primitive-only delegation as the contract.
- **types** `crates/types/src/contact.rs` - `ContactCreate` has
  `photo_url` but no inline `photo` (unlike `ContactCard` and
  `ContactPatch`), so inline-photo contacts require create-then-
  update. Likely intentional (CardDAV/People treat photo upload as a
  separate step) but undocumented. Add the field or document the
  two-step requirement.

---

## Smells

- **caldav** `ical.rs` (time parsing/formatting) - TZID datetimes
  are read as wall-clock digits with a `Z` appended, so
  `EventTime.value` (documented RFC 3339 UTC) holds local time
  mislabelled as UTC - off by the zone offset for any consumer that
  trusts it. A consequence of the VTIMEZONE-stub limit, but the docs
  frame it only as "fixed-offset stubs", not as mislabelled
  instants. Reconfirmed during the fix wave.
- **graph** `calendar.rs` (`event_from_graph`) - `recurrence_id` is
  populated from `seriesMasterId` (the master series id), but the
  shared model documents it as RECURRENCE-ID semantics (an
  overridden occurrence). Consumers following the docstring will
  misinterpret it. (Google resolved the same question correctly:
  `originalStartTime`, with the master-id field removed.)
- **graph** `contacts.rs` - `ContactEmail.kind` maps to/from Graph
  `emailAddress.name`, which is a display name, not a type label.
  Round-trip is consistent (no loss) but semantically conflated.
- **imap** `factory.rs`, `capabilities.rs` - capability flags key on
  `sub.is_some()`, never consulting the sub-account's `pim_methods`.
  Correct today only because the DAV crates hardcode all flags
  `true`; if either ever conditionally disables a method, IMAP
  over-advertises.
- **imap** `factory.rs` - `open_carddav(...).await?` /
  `open_caldav(...).await?` mean a transient DAV outage fails the
  whole IMAP account open, taking mail sync down with it. Currently
  a product decision made by accident; decide fail-hard vs fail-soft
  deliberately.
- **carddav** `account.rs` - the ctag is encoded into the cursor
  envelope but never compared, so `changes_stream` always does the
  full PROPFIND + diff; the natural "collection unchanged"
  short-circuit is absent. The cursor pays to carry state it never
  reads.
- **carddav** `parse.rs` - `ResponseParts` carries fields unused by
  each of its three parser call sites; the shared staging/commit
  logic touches fields irrelevant per site. Mild maintenance tax.

---

## Nits

- **caldav** `account.rs` - `discover_calendar_user_email` and
  `discover_schedule_outbox_url` run unconditionally on every
  `open` (4+ PROPFIND round-trips, errors swallowed), even for
  accounts that never RSVP. Lazy discovery was evaluated during the
  fix wave and skipped: it needs interior mutability on
  `CalDavAccount` and touches the open happy path.
- **caldav** `account.rs` (`event_rsvp`) - the organizer-email guard
  added in the fix wave is unreachable on the success path because
  `rsvp_reply_ical` already errors when the organizer is absent.
  Redundant defense, harmless.
- **carddav** `account.rs` - multiget failures (failed propstat for
  a requested href) silently drop the contact from list pages; no
  error, no Destroyed.
- **carddav** `account.rs` - an empty multiget result stamps the
  error `Unsupported(operation)` instead of
  `NotFound(ResourceKind::Contact)`; the HTTP 404 path maps NotFound
  correctly, so the taxonomy is inconsistent for the same condition.
- **types** `crates/types/src/error/scope.rs` - `EventRsvp` is
  classified non-idempotent; setting a fixed RSVP status is
  semantically idempotent. Consistent with the patch-setter
  convention, so a convention call, not a defect.
- **types** `crates/types/src/account.rs` - the `Account` trait is
  not `#[non_exhaustive]` despite `plans/unification.md` stating it
  is. Predates Stage 3/4; practical impact low (the attribute on
  traits only restricts external impls).
- **imap** `mod.rs` - new delegation code uses imported
  `AccountOperation::` while 8 pre-existing sites stay fully
  qualified `bifrost_types::AccountOperation::`. Cosmetic.
- **jmap** `calendar_ops.rs` - `jmap_visibility` and `rsvp_value`
  retain the explicit-arm-then-identical-wildcard shape that was
  cleaned out of the other three mappers; left because the explicit
  arms read as intentional documentation of those mappings.

---

## Accepted fidelity limits (deliberate, documented, not findings)

1. **Recurrence fidelity** - JMAP and Graph expose only common
   recurrence shapes through the shared `EventRecurrence` model;
   unsupported outbound RRULE parts are rejected, not silently
   dropped (including, post-fix: UTC `UNTIL=...Z` on JMAP, relative
   monthly/yearly without BYDAY on Graph). CalDAV projects only the
   first VEVENT (no modified override instances), preserves override
   VEVENTs on raw-backed scalar updates, and rejects recurrence
   replacement when overrides are present.
2. **Timezone fidelity** - CalDAV emits and preserves VTIMEZONE
   components but synthesizes only fixed-offset stubs, not full
   transition rules. Graph rejects unknown IANA zones instead of
   silently writing UTC; not a complete database.
3. **Provider search limits** - Graph calendar-scoped /
   shared-mailbox / empty / cursor-resume event search and general
   contact substring search are local over list pages; Graph Search
   API is used only for unscoped, non-empty default-mailbox event
   searches, and its hits carry mailbox-scope (`$mailbox`) event ids
   resolved via `/me/events/{id}`. Google People and Gmail-side
   search limits per `reference/google.md`.
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
7. **Server-derived fields are rejected, not dropped** - create
   payloads carrying `organizer` are rejected on Google and Graph;
   event `status` other than Confirmed is rejected on Graph
   (`isCancelled` is server-derived); People `photo_url` patches are
   rejected (read-only; inline `photo` is the writable path).
8. **JSCalendar availability** - `freeBusyStatus` has only standard
   `free`/`busy` values, so Tentative/OutOfOffice cannot be
   faithfully represented through the standard field on JMAP.
9. **Patches that cannot be expressed losslessly are rejected** -
   JMAP single-bound start/end time patches (duration cannot be
   recomputed without the other bound); Google `is_all_day` flips
   without both start and end.
10. **Google calendar mutations are blind writes** - no `If-Match`
    despite etags being read; `event_rsvp`'s read-modify-write of
    the attendee array can clobber concurrent edits. Consistent with
    `MutationConcurrency::None` and now documented in
    `reference/google.md`; wiring If-Match is a future
    concurrency-model decision (target `rsvp` first).
