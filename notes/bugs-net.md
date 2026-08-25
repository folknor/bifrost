# bifrost-net: hunt findings

Scope: `crates/net/` - shared HTTP transport: retry policy, rate limiting and the
governor, bandwidth caps, account registration/deregistration (`AccountNet`,
`DetachOnDrop`), observability, and the `test_support` scripted-transport seam.

Hunter note: read the crate end to end (`request.rs`, `net.rs`, `rate.rs`,
`bandwidth.rs`, `auth.rs`, `redirect.rs`, `test_support.rs`, `retry.rs`, plus the
account-error mapping table) against `reference/net.md`, and confirmed both
cross-scope claims by grep.

## N-1. There is no deadline over the whole request. `timeout` and `connect_timeout` are per attempt, and a redirect resets the attempt counter to zero

**High confidence, high severity.** `request.rs` applies `connect_timeout` around
each dispatch and hands `RequestBuilder::timeout` to reqwest per attempt.
`max_attempts` multiplies both, backoff sleeps add on top, and the
`RedirectAction::Follow` arm sets `attempt = 0` and `auth_retries = 0`. Worst case
for a default policy with `max_hops = 10`: 10 hops x (3 network attempts + 1 forced
401 retry) = 40 wire dispatches, each entitled to the full "total request deadline",
plus up to 40 backoff sleeps capped at 60s each. `reference/net.md` calls
`RequestBuilder::timeout` "the explicit total request deadline"; it is nothing of the
sort. Google and Graph set no `request_timeout` at all, so nothing rescues this. The
right shape is a single deadline computed once at the top of `send_streaming_inner`
(`Instant::now() + total`), checked before every attempt and every sleep, with the
per-attempt reqwest timeout derived as `min(remaining, per_attempt)`.

## N-2. `connect_timeout` expiry is unconditionally classified `InFlight`, which silently disables retry for every POST

**High confidence, medium-high severity.** `Err(_) => Err(Error::Timeout {
transmission_state: InFlight })`. But this timer covers *dispatch until response
headers*, which includes DNS and TCP/TLS connect - the case where nothing was
transmitted. `transport_error_is_replayable` then refuses to replay a non-idempotent
request, so a Gmail send that never got a socket is surfaced as
`Reconcile(TransportDropAfterSend)` instead of being retried. Compounding it:
`NetConfig` never sets reqwest's own `connect_timeout`, so reqwest's `is_connect() &&
is_timeout()` -> `Unsent` classification (which `send_error_to_error` handles
correctly) can only fire on connect errors that are not timeouts. The fix is to bound
connect separately - set `ClientBuilder::connect_timeout` and let reqwest classify it
- rather than wrapping the whole dispatch in one timer that has to guess.

## N-3. Inbound bandwidth accounting is blind on every non-2xx path

**High confidence, medium severity.** Metering is attached only by `wrap_metered`,
which runs in `send` / `send_streaming` / `download_stream` - i.e. only on the body
that reaches the caller. Every other body read bypasses the meter entirely:
`read_capped_response_body` on terminal 4xx, on `final_response_from_response` for
`AuthLost` / `RetryBudgetExhausted` / `RateLimited`, the `drop(response)` on the 5xx
retry path, the dropped 3xx bodies on redirect hops, and the
`futures::stream::empty()` substituted for a `PassThrough` body. An account being
throttled hard, or walking a long redirect chain, reads real bytes off the wire that
`observed_bps` never sees. Outbound is worse in a different way: `body.len()` only,
headers excluded by design - and the OAuth token endpoint traffic driven by
`OAuthRefresher` is not metered at all, because it goes through the caller's own
`TokenSource`, not this pipeline.

## N-4. A mass-401 produces one issuer round-trip per in-flight request, and each one throws away a valid token

**High confidence, medium-high severity.** The 401 branch calls
`token_source().refresh()` -> `force_refresh` -> `token_inner(force = true)`
unconditionally. `force` sets `fallback: None` and does not short-circuit on a
`Fresh` token, so it never asks "has the token I actually used already been
replaced?". With N concurrent requests on one account hitting 401 simultaneously
(revocation blip, clock skew, a scope error on one endpoint), each one that arrives
after the previous refresh landed starts *another* single-flight refresh against the
IdP. Worse, because `force` discards the fallback, one transient failure at the token
endpoint during that cascade drops the state to `Backoff` and hard-fails the entire
account locally for up to 60s - while a perfectly valid, unexpired token was in hand
a moment earlier. The structural fix is a token generation counter: the request
captures the generation of the token it sent, and `force_refresh(generation)`
refreshes only if the cached generation still matches; otherwise it returns the
already-refreshed token. This also makes "forced refreshes never use the fallback"
safe, because the only forced refresh left is one where the token really was the
failing one.

## N-5. `refund` is not generation-scoped, so a refund can credit a different account's bucket

**High confidence, low severity.** `RateDebit` carries only `(host, cost)`.
`RateLimitGovernor::refund` looks the host up by name and credits whatever bucket is
there now. Every other ticket-handling path in `rate.rs` was carefully made
generation-aware precisely because detach/reopen churn recycles host names -
`Ticket`, the `is_front` filter, `WaiterGuard::drop` all check `generation`. `refund`
is the one hole left. The consequence is bounded (tokens clamp at `burst`) but it is a
real quota inflation for the replacement bucket, and it is inconsistent with the rest
of the module's own invariant. Give `RateDebit` the generation and have `refund`
filter on it.

## N-6. `register` validates `quota_per_second` but not `burst`, and `burst = 0` makes every request to that host fail permanently

**High confidence, low-medium severity.** A new-host registration with a positive
quota and `burst: 0` installs a bucket with `burst = 0.0`. Every `acquire` with `cost
>= 1` then short-circuits to `Error::CostExceedsBurst` forever - a hard,
non-retryable failure on every request to that host, from a config value nothing
rejected. Same for `burst < cost_default`, which is silently a permanently-broken
host. Extend the existing rejection arm: reject `burst == 0` and warn when `burst <
cost_default`, both with the same `return false` that already keeps a rejected
registration out of the attachment token's host set.

## N-7. `read_capped_response_body` swallows body-read failures and truncation indistinguishably

**High confidence, low severity.** Both a chunk error and a read timeout `break` out
of the loop and return whatever was accumulated. The `Error::Status` /
`FinalResponse` that results looks identical to a complete short body. Since this is
the evidence `into_account_error` uses to build provider-error causes, a JSON error
document truncated by a mid-body reset is parsed as if complete. A truncation marker
(which `cap_status_body` already has a concept of) should distinguish "capped at 4
KB", "timed out", and "connection failed".

## N-8. `ByteBucket` is constructed from the cap at stream start, but `consume` reads the cap per chunk

**Medium confidence, low severity.** `ByteBucket::new(account.bandwidth_cap())` gives
`tokens = 0.0` when the cap is `None` at stream start. If the engine then sets a cap
mid-stream (the documented reason `cap_now` is re-read per chunk), the bucket begins
empty and the next chunk pays a full refill window it did not earn. Initialise lazily
on first capped chunk instead.

## N-9. The retry loop is a 430-line function with eight mutable loop variables and three interleaved budgets, and that is where the bugs above live

N-1, N-2 and N-4 are all consequences of `attempt`, `auth_retries`, and
`redirect_hops` being ad-hoc counters mutated in-place across three different
`continue` arms of one `'outer` loop, with no object owning the request's overall
lifetime. The shape it should have: an explicit `RequestAttempt` state carrying the
deadline, the hop chain, and the per-hop budgets, with the redirect walk as the outer
loop and the retry loop strictly inner - so "a hop is a fresh logical request" is
expressed by constructing a new inner budget rather than by assigning `attempt = 0` in
the middle of the body, and so a global deadline has one place to live. Worth the
rewrite; not a tidying exercise.

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

## N-12. `AccountSpec::connect_timeout` is misnamed for what it bounds

It is a response-header deadline covering the entire dispatch, not a connect
deadline; reqwest's actual `connect_timeout` is never set. Renaming it
`headers_timeout` (and adding a real connect bound on the client, per N-2) would make
the three-timeout story - connect / headers / inactivity / total - actually say what
it does.

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
