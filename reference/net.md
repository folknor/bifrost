# bifrost-net reference

Current architecture of the shared HTTP transport crate.

Scope: HTTP/2 connection pool, OAuth bearer-token refresh, retry
budget, per-host rate limiting, per-account bandwidth metering,
native-tls, W3C `traceparent` injection. Used by `bifrost-jmap`,
`bifrost-gmail`, `bifrost-graph`. Not used by `bifrost-imap` or
`bifrost-smtp` (those carry their own TCP/TLS stacks); IMAP/SMTP
report bytes-in/out through `MeterSink` for unified bandwidth
accounting.

## Two-tier handle

```rust
let net = Net::new(NetConfig::default())?;             // process-wide
let account = net.attach_account(account_id, spec);     // per-account
```

- `Net` holds the shared `reqwest::Client`, the per-host
  `RateLimitGovernor`, and the `BandwidthMeter`. Cheap-cloneable
  (one `Arc` bump); shared across every account so multi-account
  quota coordination on Gmail / Graph hosts works out of the box.
- `AccountNet` is account-scoped: token source, default
  `RetryPolicy`, `AtomicU8` priority, `AtomicU64` bandwidth cap.
  `Clone` is one `Arc` bump; protocol crates clone per spawned task.

`Net::detach_account` is symmetric: it forgets the bandwidth
counters and decrements the governor's per-host attach count for
every host the matching `attach_account` registered. Host buckets
drop when the attach count reaches zero, so five Gmail accounts
sharing one host bucket survive any one detach but the bucket is
reclaimed when the last account detaches.

## Request flow

```rust
let response = account.post(url)
    .header("X-Custom", "value")
    .json(&body)
    .cost(5)                       // rate-limit quota debit
    .retry(RetryPolicy::default())
    .timeout(Duration::from_secs(30))
    .send()
    .await?;
```

`RequestBuilder` consumes `self` on every setter and drives the
retry loop in `send` / `send_streaming`. Each retry attempt is a
fresh wire request: governor debit + outbound metering + body send
+ inbound metering. Buffered (`send`) and streaming
(`send_streaming`) share `send_streaming_inner`; the buffered path
drains the body through the same metering reader.

### Retry decision (RetryPolicy)

- 2xx -> return. 3xx is passed through unchanged (reqwest follows
  redirects up to 10 hops by default; a surfacing 3xx means the
  caller disabled the policy, hit the limit, or got a terminal 3xx
  like 304). Conditional-request callers rely on the headers being
  exposed.
- 4xx not in `policy.statuses`, not 401 -> terminal
  `Error::Status`.
- 401 -> force refresh once; second 401 -> `Error::AuthLost`. The
  401-recovery budget is **separate** from the network retry
  budget: a 401 retry no longer burns one of the `max_attempts`
  network attempts.
- 5xx or in `policy.statuses` -> retry with backoff. Honor
  `Retry-After` (delta-seconds + RFC 9110 HTTP-date via
  `httpdate`), capped by `policy.honor_retry_after_cap` (single
  source of truth). Without `Retry-After`: exponential backoff
  with **proportional** jitter (0..capped), capped at
  `policy.max_backoff`. Jitter is taken from an `Instant`-based
  process-start delta, not `SystemTime::now`, so a clock jump
  cannot influence the wait.
- 429 -> same retry path; `Retry-After` honored.
- Network / timeout / decode errors -> retry if
  `policy.network_errors && attempt < max_attempts`.
- Past `max_attempts` -> `Error::RetryBudgetExhausted` (or
  `Error::RateLimited` on 429). The exhausted-attempt branch parses
  the *final* response's `Retry-After` and pushes it onto
  `retry_after_history` before surfacing, so consumers see the
  server's last hint.

Token-source failures returned from `current()` are passed through
to the caller unchanged: `Error::AuthLost` for true auth failures,
`Error::RefreshFailed { source }` for transient
`Network`/`Timeout`/etc. failures (preserving the inner classification
for retry-vs-give-up decisions).

`policy.statuses` is the additive retry set on top of unconditional
5xx; explicitly documented to avoid double-counting.

The rate-limit slot is **refunded on every retried failure**, not
just 429-with-Retry-After, so 5xx retries don't burn quota for work
the server didn't perform.

## OAuth single-flight (`OAuthRefresher`)

State machine: `Empty` -> `Refreshing` -> `Fresh`. The first
`token()` call from `Empty` transitions to `Refreshing` and drives
the network refresh itself; concurrent callers register a
`oneshot::Sender` in `Refreshing.waiters` and await. On success the
driver transitions to `Fresh { token, refreshed_at }`. On failure
the driver fans `Arc<Error>` clones to every waiter (because
`Error` is not `Clone`).

`Error::RefreshFailed { source: Arc<Error> }` preserves
transient-vs-permanent: `Network`/`Timeout` survive as themselves;
true auth failures (token-endpoint 401/403) map to `AuthLost`.

`AccessToken` wraps `Zeroizing<String>`; `Debug` redacts the bytes
and surfaces only length + expiry.

### Proactive-refresh window

- Tokens with an issuer-supplied `expires_at`: refresh 60 s before
  the deadline.
- Tokens without `expires_at` (opaque, no TTL hint): refresh when
  the cached token is older than the refresher's `max_age` (default
  55 min via `DEFAULT_TOKEN_MAX_AGE`, plumbed through
  `NetConfig::token_max_age` and `OAuthRefresher::with_max_age`).
  The previous behaviour treated such tokens as fresh indefinitely
  and waited for a server 401; the max-age fallback closes that
  latency leak.

**Do not nest `OAuthRefresher`s.** Wrapping one refresher around
another causes the outer's `force_refresh` to bypass the inner's
single-flight. Compose against the raw `TokenSource` instead.

## Rate-limit governor (`RateLimitGovernor`)

Per-host token bucket behind `Arc<Mutex<HashMap<String,
HostBucket>>>`. `acquire(host, cost)` returns a `Send + 'static`
future that captures `Arc::clone(&self.buckets)`, refills tokens
based on elapsed time, debits `cost` if available, otherwise awaits
the bucket's `Notify` (with a 250 ms poll cap so refunds don't
deadlock). `refund(host, cost)` wakes one waiter.

`cost > burst` short-circuits to `Error::CostExceedsBurst` instead
of hanging: the bucket can never fill that high. Configuration bug,
not a runtime condition.

Unregistered hosts are unmetered (no-op `acquire`).

### Cost defaults

Each `RateLimit::cost_default` is stored on the host bucket at
registration. `RequestBuilder::send_streaming_inner` consults it
via `RateLimitGovernor::cost_default_for(host)` when the builder
did not call `.cost(n)` explicitly; the precedence is
`RequestBuilder::cost` > host `cost_default` > 1.

### Duplicate registrations

The first registration of a host wins. A subsequent
`register(RateLimit { ... })` for the same host with a different
`quota_per_second`, `burst`, or `cost_default` is **not** silently
dropped: the governor logs a `tracing::warn!` and keeps the
existing bucket. Per-host attach counts are incremented on every
`register` so `detach_account` can decrement symmetrically.

## Bandwidth meter

`BandwidthMeter` holds one `AccountCounters` per account; each
counter wraps a `RateWindow` (10 one-second buckets, ring buffer).
`AccountMeter::observed_bps()` returns total bytes over the
trailing 10 seconds.

Outbound metering is wired in `send_streaming_inner` (records
`body.len()` per attempt). Inbound metering is per-chunk on the
response body reader, both buffered and streaming.

`MeterSink` trait lets IMAP/SMTP (which bypass this crate's
transport) feed `record_bytes_in` / `record_bytes_out` into the
same meter when the engine wires them.

### Bandwidth cap

Per-account cap stored on `AccountNet` in `AtomicU64` with
`u64::MAX` sentinel for `None`. Throttles via `ByteBucket` on the
response-body reader. Chunks larger than the per-second cap are
admitted after `(chunk_size / cap)` seconds of sleep (the cap is a
smoothing throttle, not a hard ceiling on chunk size).

`set_bandwidth_cap(Some(0))` is **not** a sentinel for unlimited
(that is `None`'s job). It is normalised to `Some(1)` with a
`tracing::warn!` so the caller's misconfiguration is visible
without parking the stream forever.

## Range fetches (`download_stream`)

When a `Range` was requested, the response **MUST** be
`206 Partial Content` with a matching `Content-Range`; `200 OK`
returns `Error::RangeNotHonored` (the server collapsed the range,
so the bytes don't match the request). Bare requests accept
`200 OK` as before.

### Open-ended `Content-Range` validation

For an open-ended request (`bytes=N-`), the response's
`Content-Range` is validated against the tail of the resource:

- `bytes N-M/T` with known total: `M == T - 1` (the response must
  cover bytes N through the final byte of the resource).
- `bytes N-M/*` with unknown total: any `M >= N` accepted (we
  cannot verify without the total).

The previous open-ended check accepted any `M >= N` even with a
known total, which silently admitted truncated bodies.

### Zero-length and overflowing ranges

- `ByteRange { length: Some(0), .. }` short-circuits in
  `download_stream` to an empty `ByteStream` without touching the
  network or the meter. The encoder also rejects `Some(0)` as
  `Error::RangeNotHonored { message: "zero-length range" }` so
  hand-rolled callers see the error path.
- `start + length` overflow is rejected up front by `encode_range`
  with `Error::RangeNotHonored { message: "range overflow: ..." }`
  rather than silently emitting `bytes=N-u64::MAX`.

## TLS

`native-tls` only. No `rustls` dep anywhere. `NetConfig` accepts a
`Vec<native_tls::Certificate>` for additional root certs and a
`dangerous_accept_invalid_certs` flag for self-signed fixtures.

`Net::new` returns `Result<Net, Error::NetSetup>` on bad TLS
config; consumer-supplied data is a runtime condition, not a
programmer error.

## traceparent

W3C `traceparent` injected on every outbound request. Span id is
read from `tracing::Span::current()` when available; trace id is
freshly minted via `uuid::Uuid::new_v4` per request as a stopgap.
Full trace propagation (reading trace_id from the current
`tracing-opentelemetry` context) is a follow-up.

## Error model

`Error` is `#[non_exhaustive]` and `thiserror`-derived. Notable
variants:

- `Network { message, source }` - transport-level failures (DNS,
  TCP reset, TLS).
- `Timeout` - per-request deadline.
- `Status { code, body, headers }` - terminal non-retryable HTTP
  status. `body` is capped at 4 KB via `cap_status_body` with a
  truncation marker.
- `RetryBudgetExhausted { last_status, retry_after_history }`.
- `RateLimited { retry_after }` - 429 past the retry budget.
- `AuthLost` - irrecoverable token failure (refresh-token revoked,
  repeated 401).
- `RefreshFailed { source: Arc<Error> }` - transient refresh
  failure preserving the original.
- `CostExceedsBurst { cost, burst }` - rate-limit configuration
  bug.
- `EncodeBody { message, source }` - `RequestBuilder::json`
  serialization failure; surfaced lazily on the next `send`.
- `RangeNotHonored { message }` - server returned non-206 or
  mismatched `Content-Range` to a Range request.
- `NetSetup { message, source }` - `Net::new` construction
  failure.

`Status`/`Response` carry `reqwest::StatusCode` and `HeaderMap`
straight through. Documented coupling cost; acceptable for v1.

## File map

```
crates/net/src/
  lib.rs          // re-exports; AccountId/Priority/ByteRange from
                  // bifrost-types
  config.rs       // NetConfig
  net.rs          // Net + AccountNet; attach/detach; download_stream
  request.rs      // RequestBuilder + Response/StreamingResponse;
                  // retry loop
  auth.rs         // TokenSource + OAuthRefresher state machine
  retry.rs        // RetryPolicy
  rate.rs         // RateLimitGovernor + HostBucket
  bandwidth.rs    // BandwidthMeter + AccountMeter + MeterSink
  error.rs        // Error + cap_status_body
  trace.rs        // traceparent injection (current: uuid trace id)
```
