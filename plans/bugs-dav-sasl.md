# Bug hunt: CalDAV, CardDAV, SASL - decision records

Date: 2026-07-29

Scope: `crates/caldav/src/**`, `crates/carddav/src/**`,
`crates/sasl/src/**`.

## Accepted trade-offs

- **Weak ETags disable conflict detection, not just `If-Match`.** RFC 7232
  requires strong comparison for `If-Match`, so a weak validator has no
  conforming conditional form and the PUT goes out unconditional. Against a
  server that only ever emits weak ETags, `event_update` / `contact_update`
  have no lost-update protection at all. No better option exists inside HTTP;
  a consumer that needs the guarantee needs an application-level revision
  check.
- **VTODO / VJOURNAL resources still occupy the event cursor.** The snapshot
  and changes lanes key on the PROPFIND href listing, which does not carry the
  component type, so a task resource in a shared calendar collection is still
  emitted as a created/updated event change. Hydration yields no events.
  Filtering needs either a component-type PROPFIND or a first-fetch
  classification cache.
- **A literal backslash in a CN survives, an Exchange-style escaped one is
  normalized.** RFC 6868 defines no escape for a backslash, so a display name
  that genuinely contains `Doe\, John` is indistinguishable on the wire from
  Exchange's escaping of `Doe, John`; this crate resolves the ambiguity in
  Exchange's favour.
- **`contact_search` names a persistently failing resource once per page.**
  Every page reruns the search, so a resource failing throughout appears in
  every page's `failed_ids`. A consumer accumulating across pages must treat
  the lane as a set. Carrying already-reported ids in the cursor would make it
  grow with the failure set.
- **The DAV transport seam is local to each crate, not `bifrost-net`.**
  `bifrost-net` keeps its dispatcher crate-private, and both DAV clients
  still own Basic auth and their own redirect policy, so a shared seam would
  have to grow those first. The duplicated `DavTransport` / `DavResponse`
  pair in `caldav` and `carddav` is the accepted cost of testing DAV flows
  in-process until these clients move onto `AccountNet`.
- `event_in_range` trusts the server for COUNT-bounded recurrence expansion.
  Fully defending against a hostile server would require recurrence expansion
  that does not exist in this crate.

## Related structural follow-ups

The broader structural follow-ups for ADR and ORG slots and multi-value
RDATE/EXDATE modeling remain in
`plans/dav-parsing-robustness-2026-06-17.md`.
