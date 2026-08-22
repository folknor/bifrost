# bifrost-jmap bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/jmap/` including
`crates/jmap/src/sync/`. Findings are unverified work material.

Tree was clean at hunt time (`brokkr check -p bifrost-jmap`: 489 tests pass, zero
clippy/gremlins), so everything below is behavior the suite does not cover.

## Design observations

- `email_inventory`, `foreign_email_inventory`, and `email_inventory_page` are three
  near-identical query/get/advance loops (~110 lines each) differing only in the filter, the
  window bound, and whether errors route through `shared_scope_error`. All three repeat the same
  `i32::try_from` / `checked_add` overflow ceremony verbatim. One parameterized loop taking
  `(filter, owner, window)` would remove ~200 lines and the risk of the three drifting, which
  they already have: only the foreign loop applies `qualify_foreign_ids`, only the page loop
  stops on a short window.

## Coverage gap in this hunt

`calendar_ops.rs` and `contacts.rs` (~3k lines of JSCalendar/JSContact mapping) were not audited
in depth. The RRULE/`UNTIL`, all-day exclusive-end, and RSVP claims in the reference are
unverified and would be worth a dedicated pass.
