# bifrost-smtp upstream triage

Current tracker for SMTP after the lettre review, upstream issue scan, and
bifrost-specific cleanup.

## Current Shape

- Native TLS is unconditional.
- SMTP and LMTP transports are unconditional.
- Message building is unconditional.
- Connection pooling is unconditional.
- Async SMTP is Tokio-only behind the `tokio` feature.
- Removed async-std, rustls, boring-tls, web/wasm, sendmail transport, file
  transport, benches, docs, and testdata inherited from lettre.
- Remaining crate features are `tokio`, `dkim`, `serde`, and `tracing`.

## Completed Work

- Added explicit password and OAuth2 credential constructors.
- Added OAUTHBEARER support and preferred it over XOAUTH2 when available.
- Stored password and OAuth token material in zeroizing memory.
- Refreshed EHLO state after successful AUTH.
- Tightened timeout behavior across sync and Tokio connection setup.
- Made abort close the connection without attempting QUIT on a broken stream.
- Hardened message builder defaults and raw Date handling.
- Added LMTP support, including Unix socket handling.
- Fixed SMTP review blockers around command injection, duplicate parameters,
  DSN recipient parameters, List-Unsubscribe-Post, and FUTURERELEASE parsing.
- Applied the wider SMTP review cleanup for extension validation, transport
  behavior, native TLS handling, and feature surface reduction.
- Simplified JMAP and SMTP around mandatory native TLS.

## Latest Cleanup

- Removed the optional `pool` feature.
- Kept `PoolConfig` public and made every SMTP/LMTP transport use the pool.
- Added `tokio/sync` to the `tokio` feature because async pooling uses Tokio
  synchronization primitives.
- Deleted no-pool connection cleanup paths and pool-only cfg attributes.
- Made SMTP reply errors first-class by carrying the full `Response`, split
  timeout into its own error kind, and renamed ambiguous response/client
  buckets to parse and invalid-input buckets. Internal invariant failures now
  have their own bucket.

## Open Work

- Revisit auth internals after ratatoskr's account/auth API settles. `rsasl`
  remains a candidate, but should not shape the public bifrost API.
- Refresh SMTP examples and reference docs after the public API settles.
- Keep `dkim`, `serde`, `tracing`, and `tokio` unless a concrete consumer cost
  shows up.

## Deferred

- Rustls, boring-tls, web/wasm, async-std, sendmail transport, and file
  transport remain intentionally out of scope.
