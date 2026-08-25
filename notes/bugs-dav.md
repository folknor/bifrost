# bifrost-caldav and bifrost-carddav: hunt findings

Scope: both DAV account crates in full - discovery/home/collection walks,
sync-token and ctag cursor handling, REPORT and PROPFIND parsing, etag
preconditions, mutation paths, and the CalDAV-into-IMAP composition seam.

Hunter note: read `reference/caldav.md`, `reference/carddav.md`,
`notes/dav-parsing-robustness-2026-06-17.md`, and all of
`account.rs`/`client.rs`/`parse.rs` in both crates; skimmed `ical.rs`/`vcard.rs`
only via the reference, since the note says that path was reworked.

## 1. The empty-multistatus guard suppresses the deletes but *adopts the empty snapshot as the checkpoint*

**Both crates. High confidence, real data-stranding.**

`diff_event_snapshots` / `diff_contact_snapshots` return `Vec::new()` when
`current.entries.is_empty() && !previous.entries.is_empty()` - the mass-delete
suppression. But the caller does not roll back:

```rust
let (current, changes) = changes_from_cursor(&client, &previous).await?;   // current has 0 entries
let checkpoint = cursor_from_snapshot(cursor.scope, &current);             // <- persists the EMPTY snapshot
```

(`caldav/src/account.rs` `changes_stream`; `carddav/src/account.rs`
`changes_stream`, line ~572.)

Two consequences, both bad:

- A **genuine** empty-out (user deleted every event/contact, or the collection was
  replaced) is never reported as `Destroyed` - not on this poll, and not on any later
  poll, because the next `previous` is already empty. The consumer's rows are
  stranded permanently. The comment claims "a genuine empty-out reconciles on the
  next non-empty poll", which is false: after the empty snapshot is checkpointed the
  diff is empty-vs-populated, which yields only `Created`.
- A **transient** empty 207 causes every surviving resource to be re-emitted as
  `Created` on the next poll - a full replay of the collection, which is the exact
  cost the guard was written to avoid.

CardDAV compounds this: on the empty poll the *new* ctag is also written into the
checkpoint, so the next poll ctag-short-circuits and does not even re-observe.

The fix that keeps the guard: when the guard fires, checkpoint `previous`
(optionally with the refreshed sync-token/ctag) rather than `current`, and treat the
poll as "no observation" end-to-end, not only in the change lane. The same shape
applies to `failed_hrefs`: a preserved-failed href is dropped from
`current.entries`, so its etag is lost and it re-emits as `Created` next poll; it
should be carried forward from `previous` into the snapshot that gets encoded.

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

## 3. A whole-leg HTTP failure in a chunked multiget still throws away every prior chunk

**Both crates. High confidence.**

`fetch_events` / `fetch_vcards` / `query_events_text` / `query_vcards_text` use
`self.report_raw(...).await?`. `report_raw` classifies non-2xx into `Err`. So the
entire `MultigetFetch::degraded` machinery - the whole point of which is "a leg that
fails wholly after other legs returned events keeps those events" - only fires for
**207 bodies that failed internally**. A 401, 503, or 429 on chunk 3 of 40 discards
chunks 1 and 2 and returns `Err`, exactly the outcome the design says it avoids. Both
`reference/caldav.md` and `reference/carddav.md` state the stronger property, so the
docs are currently wrong about the code.

Fix: classify the status inside the loop through `status_error` and route it into
`worse_recovery(degraded, ...)` like the 207 path, letting `MultigetFetch::settle`
decide.

## 4. `contacts_list` and empty `contact_search` silently drop the listing's failed hrefs

**CardDAV only. High confidence - textbook drift.**

`hydrated_contacts_page` and `hydrated_contacts` call `client.list_contacts(...)`,
which is `list_contacts_listing(...).entries` - `failed_hrefs` is discarded on the
floor. CalDAV's `event_search` empty-query branch does exactly the opposite and
explicitly says why:

```rust
fetched.report.failed.extend(listing.failed_hrefs.into_iter().map(...));   // caldav
```

So a contact the server refused inside the depth-1 207 is invisible to the consumer:
not in `items`, not in `failed_ids`, indistinguishable from a remote deletion. The
`list_contacts` / `list_contacts_listing` split is the mechanism that made this easy
to get wrong - `list_contacts` exists only to throw the failure lane away, and should
be deleted so callers have to handle it.

## 5. CardDAV's `contact_addressbook_url` is the bug CalDAV already fixed

**High confidence, narrow blast radius.**

```rust
fn contact_addressbook_url(client: &CardDavClient, contact: &ContactId) -> Option<String> {
    let trimmed = client.resolve_url(&contact.0);
    let trimmed = trimmed.trim_end_matches('/');
    trimmed.rfind('/').map(|index| trimmed[..=index].to_string())...
}
```

CalDAV's `event_calendar_url` carries a nine-line comment explaining precisely why
this is wrong (a slash in the query or fragment) and does a real `Url` parse instead.
CardDAV never got the fix. For `https://dav/ab/c.vcf?next=/x` it yields
`https://dav/ab/c.vcf?next=/`, which then travels as the `AddressBookId` stamped on
the returned card and as the left side of the `same_collection_url` move check - so a
legitimate restated `address_book_id` is refused as a cross-book move. This is the
fifth instance of the exact drift class both reference docs warn about.

## 6. CalDAV/CardDAV discovery fallback logic has drifted in shape and in trigger

**Medium confidence on impact.**

- CardDAV tries `.well-known/carddav` **first**, falls back to base; CalDAV tries base
  **first**, falls back to `.well-known/caldav`. Nothing explains why they differ.
- CardDAV falls back both on a not-found error *and* when a 200 body names no
  principal. CalDAV's `should_fallback_discovery` only matches `NotFound(Calendar)` -
  a CalDAV server that answers `.well-known`-less base URLs with a 200 containing no
  `current-user-principal` fails the open outright rather than retrying well-known.
- `should_fallback_discovery` keying on `NotFound(ResourceKind::Calendar)` is fragile
  in a second way: `status_error` maps *every* 404 to `NotFound(Calendar)`, so the
  fallback trigger is "any 404 anywhere in the two-request discovery", including a 404
  on the principal PROPFIND.

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

9. **`read_capped_body` misclassifies a completed mutation.** Exceeding the ceiling
   returns a `transport_error` with `Attempt(InFlight)`. For a `PUT`/`DELETE`/outbox
   `POST`, the server already acted and the response is what overflowed - `InFlight`
   tells the consumer it is safe to retry when it may not be. Both crates.

10. **`MULTIGET_BATCH_SIZE` chunk loop is serial**, and CardDAV's text search runs 8
    REPORTs serially per search, re-run on every page of `contact_search`. A 3-page
    search is 24 round trips over the same result set. The per-page re-run is
    documented as deliberate (so `failed_ids` is per-page news) but the cost is not
    acknowledged; caching the search result behind the offset cursor would give both
    properties.

11. **`is_collection` is effectively dead in the multiget path.** `mark_collection`
    requires `resourcetype` on the element stack, but neither the multiget nor the
    calendar-query/addressbook-query prop request asks for `resourcetype` - only the
    depth-1 PROPFIND does. So the guard in `as_fetched_event` /
    `as_failed_multiget_resource` / `as_missing_multiget_data` fires only if a server
    volunteers the property. Any collection self-response in a query 207 lands in
    `missing_data` -> `failed_ids`. Cheap fix: add `<D:resourcetype/>` to the
    multiget/query `<D:prop>`.

12. **`as_sync_entry` reads `self.staged.status`**, which only survives because
    `parse_sync_collection_report` is the one parser that never calls
    `begin_propstat`/`commit_propstat`. Adding propstat handling there - an obvious
    future edit - silently blanks per-entry statuses, and a blanked 404 becomes an
    `Updated` instead of a `Destroyed`. Land-mine, not a live bug.

13. **`send_raw_request` hop cap is off by one** (`hops += 1` before `if hops >=
    max_hops`), so it permits one fewer manual hop than `max_hops`. Both crates.

14. **`extract_href_properties`'s status detection is not parent-scoped** (`name ==
    "status" && stack.iter().any(|t| t == "propstat")`), unlike every other parser in
    the file which uses the element-stack parent check. A nested `<status>` inside a
    property value would be read as the propstat status.

15. **`events_in_range` truncates to `range.limit` with `next_cursor: None`.** Events
    past the limit are dropped with no continuation handle and no signal that
    truncation happened. Same in `event_search`.

## Out of scope, flagged

- `reference/caldav.md` and `reference/carddav.md` both assert the property in finding
  3 ("A leg that fails wholly after other legs returned events keeps those events") as
  current behavior. Those paragraphs need correcting whichever way 3 is resolved.
- The `notes/dav-parsing-robustness-2026-06-17.md` claim that bifrost's DAV XML
  parsers are "stronger than ratatoskr's" still holds against what the hunter read -
  the 2xx commit gating and element-stack checks are genuinely solid. Findings
  11/12/14 are the remaining soft spots in that layer, and they are all small.
