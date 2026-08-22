# bifrost-jmap bug hunt

Hunter: Claude Opus, single pass, 2026-08-05. Scope: `crates/jmap/` including
`crates/jmap/src/sync/`. Findings are unverified work material.

Tree was clean at hunt time (`brokkr check -p bifrost-jmap`: 489 tests pass, zero
clippy/gremlins), so everything below is behavior the suite does not cover.

## Coverage gap in this hunt

`calendar_ops.rs` and `contacts.rs` (~3k lines of JSCalendar/JSContact mapping) were not audited
in depth. The RRULE/`UNTIL`, all-day exclusive-end, and RSVP claims in the reference are
unverified and would be worth a dedicated pass.
