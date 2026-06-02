# bifrost-carddav reference

Current Stage 3 W1 skeleton for the CardDAV protocol crate.

## Public surface

- `CardDavCredentials` - Basic or bearer credentials.
- `CardDavConfig` - base URL plus credentials.
- `CardDavAccountFactory` - implements `AccountFactory`.

The factory is compile-only staging in W1. `open(account_id)` returns
`AccountErrorKind::Unsupported(AccountOperation::Discover)` stamped
with `Protocol::CardDav` until W2 ports the ratatoskr CardDAV client
and installs a real `Account` implementation.

## Planned W2 shape

The implementation will be sourced from
`plans/ratatoskr-import/crates/core/src/carddav/`:

- DAV discovery through `.well-known/carddav`, `current-user-principal`,
  and `addressbook-home-set`.
- Address book listing and CTag discovery.
- vCard listing through `PROPFIND`.
- vCard hydration through `addressbook-multiget`.
- Contact create/update/delete through CardDAV resource writes.

The raw CardDAV client stays crate-private. Consumers use
`CardDavAccountFactory` directly for standalone DAV accounts, or the
IMAP account composes it internally when IMAP credentials are paired
with CardDAV settings.
