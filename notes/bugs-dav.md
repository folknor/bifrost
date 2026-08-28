# bifrost-caldav and bifrost-carddav: hunt findings

Scope: both DAV account crates in full - discovery/home/collection walks,
sync-token and ctag cursor handling, REPORT and PROPFIND parsing, etag
preconditions, mutation paths, and the CalDAV-into-IMAP composition seam.

Hunter note: read `reference/caldav.md`, `reference/carddav.md`,
`notes/dav-parsing-robustness-2026-06-17.md`, and all of
`account.rs`/`client.rs`/`parse.rs` in both crates; skimmed `ical.rs`/`vcard.rs`
only via the reference, since the note says that path was reworked.

## 2. `inventory_stream` claims `CoverageDomain::full(Type(CalendarEvent))` while walking one collection

**Both crates. High confidence.**

Both crates document, loudly, that sync covers only `collections.first()`. Both then
wrap the inventory stream in `lift_complete_walk(CoverageDomain::full(scope))` where
`scope` is `CursorScope::Type(CalendarEvent)` / `Type(Contact)` - the whole
account's object type. The in-code comment justifies the claim only against
*failure* ("terminates wholesale on any failure"), which is the wrong axis: the walk
never fails, and still never looked at calendars 2..n.

Per `reference/sync.md`, coverage is what discharges debt, and "a wrong `true`
discharges debt nothing re-read (silent loss with a proof record attached)". This is
a wrong `true` emitted on every inventory run of every multi-collection DAV account.
The `SkippedScope` lane at open does not repair it - that is a different channel, and
it is emitted once at open, not per walk.

Minimum honest fix without reshaping the cursor model: report
`CoverageCoordinate::ProviderRegion { namespace: "caldav", region: default_calendar_url }`
instead of `Full`. The real fix is finding 7.

## 7. Structural: one `CursorScope` per account is the root cause of 2, and of the whole `unsynced_*_urls` apparatus

Both crates carry ~40 lines of doc comment, a `Vec<String>` field, an
`open_skipped_scopes` method, a bespoke `unsupported_scope_error` constructor, and a
forwarding path through `bifrost-imap::classify_dav_open` - all of it machinery for
*reporting* that the crate does not do its job. The honest shape
(`CursorScope::Folder(href)` per collection, three lanes keyed on the scope's own
href) deletes all of that, deletes the `default_*_url` field, makes
`CoverageDomain::full(scope)` a true statement, and makes the PIM-vs-sync asymmetry
disappear. Pre-1.0 with envelope bumps on the table, this is the change worth
spending the re-sync on, and everything in 2 is a workaround for not having made it.

## Lesser findings

8. **`event_rsvp` does a GET before checking whether RSVP is possible at all.**
   `fetch_event_from_url` runs first; the `rsvp_email`/`schedule_outbox_url` `else`
   branches that return `Unsupported` come after. Both are account fields known at
   open. Move the guards above the fetch. (CalDAV, low.)

10. **`MULTIGET_BATCH_SIZE` chunk loop is serial**, and CardDAV's text search runs 8
    REPORTs serially per search, re-run on every page of `contact_search`. A 3-page
    search is 24 round trips over the same result set. The per-page re-run is
    documented as deliberate (so `failed_ids` is per-page news) but the cost is not
    acknowledged; caching the search result behind the offset cursor would give both
    properties.

## Out of scope, flagged

- **A GENUINE empty-out is still never reported, and now cannot be.** Finding 1 is
  closed for the transient case: the empty-207 poll checkpoints the prior entries
  and etags, so surviving resources are neither stranded nor replayed as `Created`.
  What the guard cannot do is tell a transient empty 207 from a user who really
  deleted every event/contact - the wire looks identical - so the mass delete is
  suppressed in both cases and a real empty-out is never emitted as `Destroyed`, on
  this poll or any later one. CardDAV compounds it: the refreshed ctag is
  checkpointed too, so the next poll ctag-short-circuits and does not re-observe.
  This is the deliberate trade the guard was written to make (silently deleting a
  populated collection is far worse), but no document should claim the empty-out
  reconciles later. Distinguishing the two needs a signal the depth-1 PROPFIND does
  not carry - a second confirming observation, or a collection-level count.
- `CardDavClient::list_contacts` and `list_contacts_for_operation` are now
  `#[allow(dead_code)]`: every caller moved to `list_contacts_listing` so the
  listing's failure lane cannot be dropped again (finding 4). **Round 1 refused to
  delete them on the no-removal rule, and that refusal was an error of scope: both
  are `pub(crate)`, so they are not published API and the rule does not reach
  them.** They are crate-internal dead code held alive by an `allow`, which is the
  smell the rule exists to avoid being confused with. Round 3 should either delete
  them outright or re-point them at `list_contacts_listing`'s failure-carrying
  shape; what it must not do is leave the `allow` standing. Note the no-removal
  rule is about the *published* surface - applying it to crate-private items
  launders ordinary cleanup into a prohibition.
- The `notes/dav-parsing-robustness-2026-06-17.md` claim that bifrost's DAV XML
  parsers are "stronger than ratatoskr's" still holds against what the hunter read -
  the 2xx commit gating and element-stack checks are genuinely solid. Findings
  11/12/14 are the remaining soft spots in that layer, and they are all small.
