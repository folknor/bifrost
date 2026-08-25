# bifrost-net: hunt findings

Scope: `crates/net/` - shared HTTP transport: retry policy, rate limiting and the
governor, bandwidth caps, account registration/deregistration (`AccountNet`,
`DetachOnDrop`), observability, and the `test_support` scripted-transport seam.

Hunter note: read the crate end to end (`request.rs`, `net.rs`, `rate.rs`,
`bandwidth.rs`, `auth.rs`, `redirect.rs`, `test_support.rs`, `retry.rs`, plus the
account-error mapping table) against `reference/net.md`, and confirmed both
cross-scope claims by grep.

## N-3a. OAuth token-endpoint traffic is still unmetered (residual of N-3)

**Open. High confidence, low severity.** N-3's response-body half is fixed: every
body the transport reads - buffered and streaming successes, terminal statuses,
pre-retry drains, rejected-token 401 bodies, followed redirects - now meters its
chunks and pays the per-account bandwidth cap. The rest of N-3 is not fixed and is
recorded here so its removal from this document does not read as closure. The
refresh traffic `OAuthRefresher` drives never touches this pipeline: it goes out
through the caller's own `TokenSource`, so `observed_bps` cannot see it and the
per-account cap cannot throttle it. Closing it means either a metered HTTP client
handed to token sources or a `MeterSink`-shaped hook they call, both of which change
the `TokenSource` contract. Outbound request metering also remains `body.len()` with
headers excluded, which is documented as deliberate in `reference/net.md` rather than
a defect.

## N-10. There is no per-request or per-batch byte accounting seam - only per-account cumulative counters. This is why the consumers hard-code `bytes_in: 0`

**Answers cross-scope question 1. High confidence.** `bifrost-net` does know real
inbound byte counts, but `AccountMeter::bytes_in()` / `bytes_out()` are
process-lifetime cumulative totals for the whole account, and `observed_bps()` is a
10-second sliding rate. Neither can produce "bytes this batch consumed". A consumer
*could* sample before and after, but that is wrong the moment two requests run
concurrently on one account - which is the normal case for both Google and Graph. So
the consumers' `bytes_in: 0` is not laziness; the seam they need does not exist. Note
also that a naive delta would be wrong anyway because of N-3.

What should exist: `Response` and `StreamingResponse` should carry the bytes actually
read for that request (the buffered path already knows `accum.len()`; the streaming
path needs a shared counter handed back alongside the stream, since the count is only
final once the caller drains it), and outbound should carry the real dispatched size.
That makes `bytes_in` a value the consumer reads off the response it already holds,
with no cross-request race. The consumers that *do* report real numbers today -
`jmap/src/sync/blob.rs`, `graph/src/account/blob.rs`, `google/src/account/blobs.rs` -
all do it by measuring the decoded blob themselves, which is why blobs are the only
protocol lane with honest accounting.

## N-11. The rate governor is keyed on host alone, which is the wrong key for tenant-scoped quotas

**Medium confidence, medium severity.** Graph's throttling is per-tenant, not
per-`graph.microsoft.com`. Under `Net::shared_default` every Graph account in the
process shares one bucket for that host, so ten tenants get one tenant's worth of
quota, and "first registration wins" means whichever account attached first sets the
number for all of them. Gmail is the same shape (per-project *and* per-user quotas).
The bucket key should be a caller-supplied scope - `(host, quota_scope)` where the
consumer supplies the tenant/project/user discriminator - with the current host-only
behaviour as the degenerate case where the scope is empty.

## Cross-scope answers

**Question 1 - is there a seam consumers should be using?** Not today. See N-10: the
meter is per-account and cumulative, so `bytes_in: 0` in
`google/src/account/{scopes,inventory,changes,mutation}.rs`,
`graph/src/account/{mutate,scopes,inventory,get}.rs`, and
`jmap/src/sync/{mutation,discover,changes,inventory,hydrate}.rs` is a missing
transport feature, not a consumer omission. Fixing it belongs in `bifrost-net` first.

**Question 2 - the scripted-transport seam claim is false.** `reference/google.md:751`
says the crate has "no scripted-transport seam"; `bifrost_net::test_support` is used
by `crates/google/src/account/{mod,push,blobs,calendar,inventory,cloud,changes}.rs`
and `crates/google/src/client.rs`. The seam offers: `ScriptedDispatch` (answers
`Canned` outcomes in FIFO order, records every `RequestSnapshot` - method, resolved
URL post-redirect, headers as sent including injected `Authorization`/`traceparent`,
body, and the timeout that reached the wire); six `Canned` shapes covering a buffered
response, a chunk-framed stream, a stream that fails mid-body, a stream that stalls
mid-body, a typed transport error, and a dispatch that never completes; and the
`scripted_net` / `scripted_account` builders. It is staged *below* retry, redirect,
rate limiting, metering and token refresh, so a scripted test drives the production
pipeline. The doc line should be deleted.

One wart in the seam worth noting: `ScriptedDispatch::send` pops the script step
synchronously, before the returned future is polled, so a dispatch future that is
constructed and dropped without being awaited still consumes a step. Nothing hits this
today, but it makes cancellation tests read oddly.

## Out of scope, noticed in passing

- The `PassThrough` redirect arm returning `stream::empty()` means a 305/306 body is
  unreadable and unmetered, which is fine for 304 and for Drive's 308 and would be a
  trap for any future passthrough shape. The code comment already says so; it deserves
  to be an assertion rather than a comment.
- `reference/net.md` duplicates the same "bounds the issuer call RATE, not failing
  requests" paragraph twice in a row (lines 363-371).
