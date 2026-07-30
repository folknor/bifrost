# bifrost-carddav reference

Current Stage 3 W2 standalone CardDAV account implementation.

## Public surface

- `CardDavCredentials` - Basic or bearer credentials. `bearer(token)`
  wraps a raw string; `bearer_source(Arc<dyn TokenSource>)` takes a
  shared rotation source. The bearer token is read via `current().await`
  per DAV request (in `auth_headers`), so a token rotated mid-sync is
  honored on the next request without reopen. `Clone` only, hand-written
  `Debug` redacting the source; no `PartialEq`/`Eq`.
- `CardDavConfig` - base URL plus credentials (`Debug`/`Clone`, no
  `PartialEq`/`Eq`).
- `CardDavAccountFactory` - implements `AccountFactory`.

`CardDavAccountFactory::open(account_id)` discovers the CardDAV
addressbook home, caches the default address book URL, and returns an
`Arc<dyn Account>` inside an `OpenedAccount` whose skip lane is always
empty (single-principal surface). The raw DAV client and parser modules stay
crate-private; consumers use only the factory and the shared `Account`
contact primitives.

## Module layout

- `lib.rs` - public config / credentials / factory.
- `account.rs` - crate-private contact-only `Account` impl.
- `client.rs` - crate-private reqwest CardDAV client: discovery,
  `PROPFIND`, `REPORT`, `PUT`, and `DELETE`.
- `parse.rs` - XML response parsers for addressbook discovery,
  contact listing, multiget hydration, depth-0 `getctag`, and nested href
  properties. Addressbook/listing/multiget properties are staged per
  `propstat` and committed only for successful 2xx propstat statuses.
  `parse_propfind_contacts` returns a `CardDavContactListing`: committed
  `entries` plus `failed_hrefs` (vcard resources whose only propstat
  failed within the 207), so the snapshot diff can preserve a
  transiently-failed resource instead of destroying it. Response parsers
  use element-stack parent checks so nested same-name properties do not
  overwrite response-level hrefs. Every text-bearing parser accepts both
  XML text and CDATA.
- `vcard.rs` - small vCard projection between DAV resources and
  `bifrost-types` contact cards. **Parse-in** uses caldata's `LineReader`
  for RFC 6350 line unfolding (deletes exactly one leading WSP, not the
  whole run), then a faithful quoted-parameter splitter (`split_content_line`)
  that handles parameter values containing `:`/`;`/`,` and keeps every TYPE
  value (caldata's public `get_param` exposes only the first). vCard 3.0
  bare-param shorthand (`EMAIL;WORK:`) is read as a bare TYPE value.
  Projection is fallible: a malformed body (unterminated quoted parameter,
  missing value, invalid UTF-8 in a folded run) returns a `VCardParseError`.
  Bulk listing/search route a single bad resource into `Page::failed_ids`;
  single-resource get/update surface it as a local error.
  **Serialize/patch** keeps the hand-rolled verbatim-preserving splice:
  preserved (unmodeled) lines are re-emitted byte for byte on their physical
  line groups - no unfold/refold - and only freshly emitted lines are folded
  (the 75-octet budget reserves the continuation space). Version is detected
  from the card's VERSION line so emitted TYPE/PREF and PHOTO forms match
  (4.0 `PREF=1` + `PHOTO:data:image/<t>;base64,...`; 3.0 `TYPE=PREF` +
  `PHOTO;ENCODING=b;TYPE=...`). vCard group prefixes (`item1.EMAIL` /
  `item1.X-ABLabel`) are carried onto rewritten EMAIL/TEL/ADR lines so an
  Apple-Contacts label stays bound. ADR po-box/extended components are
  preserved as leading entries of the ordered `street` vector rather than
  zeroed; ORG `;`-structure is preserved (the shared model lacks dedicated
  po-box/extended and ORG-component slots - a types change, out of scope
  here, would make these fully structural). The create path emits a minimal
  `N` (mandatory in 3.0). A present-but-empty value is still
  indistinguishable from absent (the shared limitation calcard also has).
  Parameter values use RFC 6868 caret encoding in both directions, so a
  quote or newline in a parameter survives a write/read round trip. For
  vCard 4 preference ordinals, only `PREF=1` maps to the shared primary
  flag.
- `capabilities.rs` - contact-only `AccountCapabilities`.

## Account behavior

Supported contact primitives:

- `address_books_list` - `PROPFIND` depth 1 on the discovered
  addressbook home, filtering `resourcetype` entries that contain
  `addressbook`.
- `contacts_list` - `PROPFIND` depth 1 for vCard resources, local
  offset-cursor slicing of hrefs, then batched `addressbook-multiget`
  `REPORT` hydration for only the requested page. A hydrated vCard that
  will not parse is recorded (by native uri) in `Page::failed_ids` via
  the pure `partition_hydrated_vcards` helper rather than silently
  dropped, so a consumer can tell a transient per-resource hydration
  failure apart from a real remote deletion and preserve the row. Books
  and cards carry `ContactCorpus::Main`; CardDAV has no auto-collected
  corpus. Multiget returns a `CardDavMultigetReport` with successful cards
  and per-resource failures. A wholly failed 207 with any non-404/410
  failure is routed through normal status classification rather than
  returned as an empty page; partial failures feed `Page::failed_ids`.
- `contact_get` - single-resource multiget using the contact id as the
  DAV href. A missing resource maps to `NotFound(Contact)`.
- `contact_create` - creates a vCard 4.0 resource with a UUID-backed
  `.vcf` path using `PUT`.
- `contact_update` - fetches the current vCard, applies the shared
  `ContactPatch`, preserves unmodeled vCard lines for unrelated field
  changes, and writes the replacement vCard with `If-Match` when a strong
  etag was present. Weak ETags retain their `W/` marker for snapshot
  comparison but make the PUT unconditional because If-Match requires
  strong comparison. Inline `ContactPatch.photo` replaces or clears vCard
  PHOTO data. Changing `address_book_id` is rejected; CardDAV moves are
  not implemented.
- `contact_delete` - deletes the DAV resource.
- `contact_search` / `contact_autocomplete` - non-empty searches issue
  CardDAV `addressbook-query` text-match `REPORT`s over common vCard
  fields, including ADR postal addresses, then keep local filtering and
  offset-cursor paging as a
  defensive guard. Results are sorted by native id before slicing so page
  order is stable while the remote result set is unchanged. Empty search
  hydrates the addressbook to preserve
  match-all behavior. This is the *personal* corpus only;
  `directory_search` (org directory / GAL) returns
  `Unsupported(DirectorySearch)` with the capability flag `false`. An RFC
  6352 directory-gateway leg is a named follow-up (see `TODO.md`): the
  port evidence has no CardDAV directory impl, and gateway discovery is a
  substantial provider-specific unknown.

CardDAV maps ADR postal addresses through the shared `ContactAddress`
model and inline PHOTO data through shared `ContactPhoto`, accepting both
the vCard 3.0 `PHOTO;ENCODING=b` form and the vCard 4.0 `PHOTO:data:` URI
form on read and emitting the form matching the card's version on write.
CardDAV preserves inline PHOTO data on unrelated updates.

Cursor support is contact-only. `discover_cursor_scopes` returns
`CursorScope::Type(ObjectType::Contact)`. `establish_initial_cursor`
builds a hybrid cursor from the address book URL, the collection
`getctag` when present, and a sorted href/etag snapshot. `changes_stream`
first runs a **ctag short-circuit**: when the prior cursor carries a ctag
and a cheap depth-0 `getctag` PROPFIND (`client.collection_ctag`) shows it
unchanged, it emits an empty batch and carries the cursor forward, skipping
the full depth-1 PROPFIND + diff. Otherwise it polls the current snapshot
and diffs hrefs/etags. Full WebDAV `sync-collection` parity with CalDAV is
a named follow-up. The PROPFIND-snapshot diff is hardened against
destroy-everything failure modes: an empty multistatus against a populated
prior snapshot suppresses the mass-delete (treated as "no observation"),
and any href in `current.failed_hrefs` is preserved rather than destroyed.
`inventory_stream` emits contact inventory entries with ETag fingerprints
for the same contact scope.
Cursor entry counts are checked against the remaining payload before
allocation. Multiget response hrefs are rebased onto the same resolved
absolute native-id namespace the snapshot, inventory, and changes lanes
use, `failed_ids` included, so a path-only href from the server cannot
give one contact two ids.

All mail, filter, blob, push, calendar, and settings methods return
`AccountErrorKind::Unsupported` stamped with `Protocol::CardDav`.
`push_stream` and `scope_lifecycle_stream` are empty streams.
`set_priority` and `set_bandwidth_cap` are currently no-ops; this
crate has no shared metered transport attachment.

## Related providers

The IMAP account composes `bifrost-carddav` when configured with
`ImapAccountConfig::with_carddav(CardDavConfig)`. JMAP, Google
People, and Graph expose their own native contact primitives through
their Account impls. Calendar DAV support is separate in
`bifrost-caldav`; this crate remains contact-only.
