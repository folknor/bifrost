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
`Arc<dyn Account>`. The raw DAV client and parser modules stay
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
  overwrite response-level hrefs.
- `vcard.rs` - small vCard projection between DAV resources and
  `bifrost-types` contact cards.
- `capabilities.rs` - contact-only `AccountCapabilities`.

## Account behavior

Supported contact primitives:

- `address_books_list` - `PROPFIND` depth 1 on the discovered
  addressbook home, filtering `resourcetype` entries that contain
  `addressbook`.
- `contacts_list` - `PROPFIND` depth 1 for vCard resources, local
  offset-cursor slicing of hrefs, then batched `addressbook-multiget`
  `REPORT` hydration for only the requested page.
- `contact_get` - single-resource multiget using the contact id as the
  DAV href.
- `contact_create` - creates a vCard 4.0 resource with a UUID-backed
  `.vcf` path using `PUT`.
- `contact_update` - fetches the current vCard, applies the shared
  `ContactPatch`, preserves unmodeled vCard lines for unrelated field
  changes, and writes the replacement vCard with `If-Match` when an etag
  was present. Inline `ContactPatch.photo` replaces or clears vCard
  PHOTO data. Changing `address_book_id` is rejected; CardDAV moves are
  not implemented.
- `contact_delete` - deletes the DAV resource.
- `contact_search` / `contact_autocomplete` - non-empty searches issue
  CardDAV `addressbook-query` text-match `REPORT`s over common vCard
  fields, including ADR postal addresses, then keep local filtering and
  offset-cursor paging as a
  defensive guard. Empty search hydrates the addressbook to preserve
  match-all behavior.

CardDAV maps ADR postal addresses through the shared `ContactAddress`
model and inline vCard 3 `PHOTO;ENCODING=b` data through shared
`ContactPhoto`. CardDAV preserves inline PHOTO data on unrelated updates.

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
