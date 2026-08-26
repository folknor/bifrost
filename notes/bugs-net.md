# bifrost-net: hunt findings

Scope: `crates/net/` - shared HTTP transport: retry policy, rate limiting and the
governor, bandwidth caps, account registration/deregistration (`AccountNet`,
`DetachOnDrop`), observability, and the `test_support` scripted-transport seam.

Hunter note: read the crate end to end (`request.rs`, `net.rs`, `rate.rs`,
`bandwidth.rs`, `auth.rs`, `redirect.rs`, `test_support.rs`, `retry.rs`, plus the
account-error mapping table) against `reference/net.md`, and confirmed both
cross-scope claims by grep.

**All findings and residuals in this document are closed.** Everything durable
from this arc has reached `reference/net.md`, `reference/google.md`,
`reference/graph.md` and `reference/jmap.md`.

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
  established consumer adoption with a batch-scoped `ByteTally` in each of
  bifrost-google, bifrost-graph and bifrost-jmap, taken by each engine stream and
  cleared at each emitted batch. Round 4 closed the two remaining holes. First,
  the disclosed residual: Graph's buffered EWS funnel now enrolls in that same
  tally, so public-folder inventory, public-folder changes and a hydration chunk
  mixing EWS and REST ids all report real numbers instead of zero or a REST-only
  half. Second, the general form of the round-4 cold reviewer's P2: every
  consumer funnel recorded on the SUCCESS path only, while bifrost-net drains,
  meters and throttles the bodies of non-2xx responses, exhausted retries and
  repeated 401s before converting them to errors - and the Graph hydration and
  mutation lanes and the Gmail mutation lane all turn such an error into per-item
  failures while STILL emitting a batch. Those batches reported zero for traffic
  that happened. `RequestBuilder::count_bytes_into` takes a caller-owned
  `RequestByteCounter` and makes it the request's counter, which is the only way
  to read the figure back after an `Err`; both Graph funnels, the EWS funnel and
  Gmail's `send_recorded` now record from it. Every network-backed producer in
  `google/src/account/{scopes,inventory,changes,mutation}.rs`,
  `graph/src/account/{mutate,scopes,inventory,changes,get,public_folder}.rs` and
  `jmap/src/sync/{mutation,discover,changes,inventory,hydrate}.rs` now reports
  real numbers on both paths. Deliberately not a delta across the cumulative
  account meter: concurrency makes that race.

  Two deliberate exclusions, both of which report NOTHING rather than a wrong
  number. The long-lived EWS `GetStreamingEvents` funnel does not feed a batch
  tally: its counter is only as complete as the caller's draining, push traffic
  is not an engine batch, and no consumer reaches public-folder data through the
  streaming path - it carries notifications, and the data reads that follow go
  through the buffered funnel. And bifrost-jmap needed no error-path change: it
  was audited call site by call site, and every JMAP path that emits a batch
  after an error emits it after a method-level error inside a successful HTTP
  exchange, whose bytes are already recorded; a transport error there terminates
  the stream without emitting a batch.
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
