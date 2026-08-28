# Cross-cutting hunt findings

Current findings that belong to no single crate, plus durable lessons from the
cross-cutting verification pass. Per-crate defects live in their `bugs-*.md`
documents.

## Open structural residual

`bifrost-sync` has one accepted re-divergence risk, recorded as F3 in
`bugs-sync.md`. `BackfillRunner` and `InventoryFusion` share the safety-critical
barrier and resume state through `InventoryWalk`, but checkpoint minting and
terminal `Done` handling remain separate implementations because their protocol
shapes differ. No current loss path was found, so this is not a live code defect.

## Durable lessons

### Shared guards need a closed set of entry points

Graph paging, sync durable writes, and net request deadlines each failed when a
shared guard existed but callers could bypass it. The pattern later recurred in
Google: Gmail inventory, Gmail history, and People contact-group listing were
unbounded while the calendar listing was bounded. Google and Graph now keep a
one-for-one enumeration of every guarded paging walk in their reference docs.

### Mechanically prove that a regression test bites

This hunt found seven tests that passed against the defect they were intended to
pin. The later four add distinct traps to the standing examples:

- a Google lifecycle test failed only through a cache-staleness setup side effect;
- a Google mutation assertion of `is_none()` pinned the defect instead of the fix;
- a sync sibling-scope test counted drive attempts and missed that recovery never ran;
- a paused-time cancellation test passed against a bare sleep because
  `start_paused` auto-advanced until the stream eventually ended.

Ablate the production change, confirm the intended test runs and fails, and read
the failure message to verify that the intended cause produced the failure. For
paused-time cancellation, assert elapsed time and request count, and for mutation
cancellation assert the item outcomes rather than mere stream termination.

## Process note

Several hunters ran builds before returning their report even though the hunt
phase did not ask for a baseline. No hunter prompt template exists in this
repository, so there is no local template to correct here.
