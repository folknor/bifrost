# Changelog

## bifrost-jmap 0.1.0 - unreleased

Initial release under the `bifrost-jmap` name.

This crate descends from [`jmap-client`](https://github.com/stalwartlabs/jmap-client), which is unmaintained upstream. Substantial portions have been rewritten since the fork point - most notably:

- **Trait-based method dispatch** replaces the central `Method` / `Arguments` / `MethodResponse` enums.
- **Transport abstraction** - `Client<T: HttpTransport>` allows custom transports (testing, WASM, etc.).
- **Calendars** (draft-ietf-jmap-calendars-26), **Contacts** (RFC 9610), **Blob Management** (RFC 9404), **Quotas** (RFC 9425), and **Sharing** (RFC 9670) added.
- **Structured errors** - `Error::Internal(String)` removed; every variant is matchable.
- **`Field<T>`** three-state nullable replaces `Option<Option<T>>`.

For pre-fork history, see <https://github.com/stalwartlabs/jmap-client/blob/main/CHANGELOG.md>.
