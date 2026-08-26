# bifrost-net: hunt findings

Scope: `crates/net/` - shared HTTP transport: retry policy, rate limiting and the
governor, bandwidth caps, account registration/deregistration (`AccountNet`,
`DetachOnDrop`), observability, and the `test_support` scripted-transport seam.

Hunter note: read the crate end to end (`request.rs`, `net.rs`, `rate.rs`,
`bandwidth.rs`, `auth.rs`, `redirect.rs`, `test_support.rs`, `retry.rs`, plus the
account-error mapping table) against `reference/net.md`, and confirmed both
cross-scope claims by grep.

**All findings in this document are closed.** Round 3 was the final round. What
remains below is the disclosed residual: one specific call-site family that
genuinely cannot use the accounting seam, with the reason. Everything durable
from this arc has reached `reference/net.md`, `reference/google.md`,
`reference/graph.md` and `reference/jmap.md`.

## Disclosed residual: the Graph EWS arm is not byte-counted

`bifrost-graph`'s per-batch accounting seam is a `ByteTally` on `GraphClient`,
recorded at that client's two wire funnels. `EwsClient` does not go through
either: it composes `bifrost_net::AccountNet` directly, so no `GraphClient`-level
accumulator can observe its traffic. Three sites are affected and each says so at
the call site:

- `graph/src/account/public_folder.rs` inventory - reports `bytes_in: 0`.
- `graph/src/account/public_folder.rs` changes - reports `bytes_in: 0`.
- `graph/src/account/get.rs` `fetch_batch` - a chunk mixing public-folder ids
  with REST ids reports its REST half only.

This under-reports, never over-reports, which is the safe direction: a consumer
budgeting on these numbers sees less traffic than occurred rather than being told
traffic happened that did not. Closing it means giving `EwsClient` its own
accounting seam, which is a change to that client's shape rather than an
adoption of the existing one, and was out of scope for this arc.

Deliberately NOT residuals, because they are correct: three call sites report
`bytes_in: 0` for batches that perform no request at all - Gmail's constant
`discover_cursor_scopes`, JMAP's session-derived `cursor_scopes`, and the two
locally-rejected mutation lanes in Graph and JMAP. Each carries a comment saying
why the zero is the true answer.

## Closed in this arc

Rounds 1 and 2 landed as dd4a802 and 9c87f84, closing N-1, N-2, N-4 through N-9,
N-12 and two mid-arc additions. Round 3 closed the rest:

- **N-3a** - OAuth issuer traffic is neither metered nor capped. Closed as an
  explicit, documented exception rather than a defect: `TokenSource` is shared by
  HTTP, IMAP and SMTP and owns arbitrary provider exchange machinery, so
  bifrost-net cannot count its wire bytes without replacing that abstraction with
  an HTTP-specific request model.
- **N-10** - no per-request or per-batch byte accounting seam. The transport seam
  landed first (`Response::bytes_in`/`bytes_out`, `StreamingResponse`'s cloneable
  `RequestByteCounter`, one counter created before the retry and redirect loop so
  error drains, 401 recovery, redirects and retries all contribute). Round 3
  finished consumer adoption: a batch-scoped `ByteTally` in each of
  bifrost-google, bifrost-graph and bifrost-jmap, taken by each engine stream and
  cleared at each emitted batch. Every network-backed producer in
  `google/src/account/{scopes,inventory,changes,mutation}.rs`,
  `graph/src/account/{mutate,scopes,inventory,changes,get}.rs` and
  `jmap/src/sync/{mutation,discover,changes,inventory,hydrate}.rs` now reports
  real numbers, except the EWS sites disclosed above. Deliberately not a delta
  across the cumulative account meter: concurrency makes that race.
- **N-11** - the governor could not represent per-tenant or per-user quotas.
  Buckets are now keyed by `(host, quota_scope)`, and - closing the cold
  reviewer's P1 - SELECTION carries the same two components, via
  `RequestBuilder::quota_scope` with the account's declaration as the default.
  Keying registration by the pair while selecting by host alone left a
  registered, refcounted, permanently unreachable bucket whenever an account
  declared two scopes on one host.
- **Cold reviewer P2** - the redirect `PassThrough` arm discarded the response
  body. It now hands the body up through the same `into_byte_stream` the
  redirects-disabled arm uses. The round-2 assertion that guarded the discard was
  removed with the discard: it asserted on server-supplied data, and the
  substantive answer is to stop dropping the bytes rather than to prove the drop
  was permitted. This also removed a divergence the fix created in
  `jmap/src/sync/factory.rs`, whose scripted double blanked 3xx bodies to mirror
  the old arm.
- **ScriptedDispatch** - an unpolled dispatch future consumed a script step. It
  now defers `build()`, the request snapshot and the step pop into the async
  block, matching reqwest, which does nothing until polled.
