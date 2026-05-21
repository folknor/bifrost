# bifrost-net

Shared transport for the HTTP-based protocol crates and the sync
engine. Owns HTTP/2 pooling, OAuth refresh, retry, rate limiting,
bandwidth accounting, TLS, and tracing context propagation. Sits
below `bifrost-jmap`, `bifrost-gmail`, `bifrost-graph`, and any
HTTP surface in `bifrost-sync` (account discovery, ad-hoc blob
downloads, one-shot sends). Does not own IMAP or SMTP transport.

This document specifies the crate before it is written. The three
HTTP-based protocol crates each ship a near-duplicate
`execute_with_retry` + `reqwest::Client` + access-token `RwLock`
pair today (`crates/jmap/src/core/transport.rs`,
`crates/gmail/src/client.rs`, `crates/graph/src/client.rs`).
Consolidating those into one well-tested crate is the immediate
payoff; the longer payoff is a single surface where the sync engine
reads bandwidth, controls concurrency, and feeds `traceparent` into
every wire request.

## Crate purpose and boundaries

bifrost-net owns:

- The `reqwest::Client` (one process-wide, per-host configuration
  via reqwest's own host config).
- OAuth bearer-token refresh with single-flight coordination.
- Retry budget honoring `Retry-After` on 429 / 503, exponential
  backoff for 5xx, and typed budget-exhaustion error.
- Per-host token-bucket rate limiter declared per protocol.
- Bandwidth counters readable by `Control::bandwidth_observed`.
- TLS configuration (native-tls; see TLS section).
- W3C `traceparent` header injection on every request.
- Connection-pool tuning (idle eviction, per-host concurrency,
  HTTP/2 multiplexing).

bifrost-net does not own:

- JSON serialization, protocol-specific request shapes, or
  protocol error parsing. These live in the protocol crates.
- Capability detection, cursor types, or sync orchestration.
- IMAP and SMTP transport. IMAP is raw TCP + native-tls with its
  own driver model (`crates/imap/src/connection/`); SMTP is raw
  TCP + native-tls with its own pool (`crates/smtp/src/transport
  /smtp/client/`). Neither uses HTTP, neither benefits from a
  shared `reqwest::Client`, and forcing them through bifrost-net
  would either need a TCP escape hatch (defeats the abstraction)
  or rewrite the driver and codec around hyper (defeats the
  point of bifrost-net being a thin layer). They keep their
  current transports.

The split is HTTP vs. raw-TCP, not protocol-by-protocol. If a
future protocol crate carries an HTTP push channel and a TCP
control channel (EWS streaming is HTTP long-poll; LMTP-over-HTTP
does not exist), the HTTP half goes through bifrost-net and the
TCP half does not.

What bifrost-net shares with IMAP/SMTP, despite not owning their
transport, is a small set of cross-cutting types: a
`RecoveryClass`-shaped error surface (see
`plans/account-trait.md` -> Recovery vocabulary), the
`tracing` span conventions (see `plans/sync-engine.md` ->
Observability), and the bandwidth meter the engine reads through
`Control`. IMAP and SMTP report bytes-in/bytes-out into the same
meter when the engine asks them to.

## HTTP/2 connection pool

Single shared `reqwest::Client` per process. Reqwest's internal
pool is per-host already; sharing the client across protocol
crates means one connection pool covers JMAP, Gmail, and Graph
even when the same account drives multiple protocols (uncommon
today, plausible later for IMAP + Gmail labels coexistence).

Underlying library: `reqwest` on top of `hyper`. Do not drop to
`hyper` directly. The existing crates use reqwest, the migration
cost of going to raw hyper is real, and reqwest's HTTP/2
multiplexing, connection pooling, and timeout knobs already cover
what bifrost-net needs. The pieces reqwest does not cover (single-
flight OAuth refresh, typed retry budget, rate-limit governor,
traceparent injection) are middleware-shaped and compose around
the `reqwest::Client` rather than under it.

Pool configuration, with defaults pinned per protocol via
`NetConfig::for_protocol`:

- `pool_idle_timeout`: 90 seconds. Matches reqwest default;
  long enough that an idle period between sync passes does not
  cost a re-handshake.
- `pool_max_idle_per_host`: 8. The existing Graph client caps
  in-flight requests at 3 (`CONCURRENCY_LIMIT` in
  `crates/graph/src/client.rs`); 8 idle slots cover that ceiling
  with headroom for parallel range fetches on blob download.
- `http2_prior_knowledge`: false. Negotiate via ALPN. Google,
  Microsoft, and Fastmail all serve HTTP/2 over ALPN.
- `http2_keep_alive_interval`: 30 seconds.
- `http2_keep_alive_timeout`: 10 seconds.
- `tcp_keepalive`: 60 seconds.
- `connect_timeout`: 10 seconds.
- `timeout`: not set at the client level. Per-request timeouts
  belong with the protocol crate's command surface (JMAP
  `Email/changes` has different latency expectations than a
  150 MB attachment download).

Per-host concurrency caps are enforced by the rate-limit governor
(see Rate-limit governor), not by reqwest's pool. The pool cap is
an upper bound on TCP connections; the governor is the actual
in-flight ceiling. Layering keeps the pool simple and lets the
governor adapt to per-account quota tier without churning the
connection pool.

## OAuth refresh under load

The hazard: N in-flight requests share one access token, all
discover it is expired roughly simultaneously, and each triggers
an independent refresh. The N refreshes either consume the
refresh-token rate limit, race to install N different new tokens
into the shared store, or both.

Single-flight refresh, modeled on the `tokio::sync::Mutex` +
shared state pattern (and on what `async-stream` style helpers
like `singleflight` provide in other Rust HTTP stacks):

```rust
pub trait TokenSource: Send + Sync + 'static {
    fn current(&self) -> AccountFuture<Result<AccessToken, Error>>;
    fn refresh(&self) -> AccountFuture<Result<AccessToken, Error>>;
}

pub struct OAuthRefresher {
    source: Arc<dyn TokenSource>,
    state: Arc<Mutex<RefreshState>>,
}

enum RefreshState {
    Fresh { token: AccessToken, refreshed_at: Instant },
    Refreshing { waiters: Vec<oneshot::Sender<Result<AccessToken, Error>>> },
}
```

Flow:

1. Caller asks `OAuthRefresher::token()` for the current token.
2. If `Fresh` and not within the proactive-refresh window
   (default: token TTL minus 60 seconds), return immediately.
3. Otherwise transition `Fresh -> Refreshing { waiters: [] }`
   atomically, register a oneshot in `waiters`, drop the lock,
   and spawn one refresh task. The task calls `source.refresh()`,
   reacquires the lock, transitions back to `Fresh`, and fans
   the result out to all waiters.
4. Concurrent callers seeing `Refreshing` register their oneshot
   in `waiters` and await. No second refresh.

The state machine is the same shape `crates/gmail/src/client.rs`
and `crates/graph/src/client.rs` would have written if they had
addressed the problem; today they read the token through an
`RwLock<String>` with no refresh trigger - the consumer pushes a
new token in through `set_access_token`. bifrost-net inverts the
flow: the protocol crate hands a `TokenSource` to bifrost-net at
client construction, and bifrost-net pulls when it needs one.

Proactive refresh window default: 60 seconds before expiry. If the
token does not carry a TTL (some opaque-token providers), the
refresher refreshes only on 401 response. The 401 path is the
fallback for opaque tokens and for clock-skew cases where the
server expires the token earlier than the client expects: the
request layer sees 401, asks the refresher to force a refresh,
retries the request once with the new token. Force-refresh is
itself single-flighted on the same lock.

`AccessToken` is a `Zeroizing<String>` plus expiry. `Debug` redacts.

## Retry budget with `Retry-After` honor

Single retry policy applied to every request:

```rust
pub struct RetryPolicy {
    pub max_attempts: u32,         // default 3
    pub initial_backoff: Duration, // default 1s
    pub max_backoff: Duration,     // default 60s
    pub honor_retry_after_cap: Duration,  // default 60s
    pub retryable: RetryableSet,
}

pub struct RetryableSet {
    pub statuses: &'static [u16],  // default [429, 500, 502, 503, 504]
    pub network_errors: bool,      // default true: connect/reset/timeout
}
```

Decision per attempt:

- 2xx / 3xx: return.
- 4xx not in `statuses`: return error, no retry.
- 5xx and `statuses`: retry with backoff.
- 429: retry with `Retry-After` honored. `Retry-After: <seconds>`
  is honored up to `honor_retry_after_cap` (servers occasionally
  return absurd values during partial outage; the cap prevents a
  multi-hour stall on a 30-second outage). `Retry-After: <date>`
  is parsed via `httpdate`.
- Network error and `network_errors`: exponential backoff with
  decorrelated jitter (`backoff = min(max_backoff, random_between
  (initial_backoff, backoff * 3))`). Decorrelated jitter avoids
  thundering-herd retries from one shared outage.
- Past `max_attempts`: surface as
  `Error::RetryBudgetExhausted { last_status, retry_after_history }`.

The Graph plan in `plans/graph/streaming.md` calls this out
explicitly: "the transport layer (bifrost-net) honors the header
internally with a bounded retry budget. Past the budget, surface
as a typed error rather than looping forever. The existing client
already encounters this; move the policy from the per-call site
into the transport."

This consolidates three near-identical retry loops:
`execute_with_retry` in Gmail at `crates/gmail/src/client.rs:152`
retries 429 only, 3 attempts, no jitter; `execute_with_retry` in
Graph at `crates/graph/src/client.rs:299` retries 429/500/502/503/
504, 3 attempts, no jitter; JMAP has none today
(`crates/jmap/src/core/transport.rs` is a trait surface with no
retry built in). All three converge on bifrost-net's policy after
migration.

The retry budget reports to the protocol crate as a typed error.
The protocol crate maps it to its own `RecoveryClass::Retry { after }`
or `RecoveryClass::OperatorOverrideRequired { reason }` depending
on cause (network vs. server outage vs. quota exhaustion).

## Rate-limit governor

Per-host token-bucket. The protocol crate declares its bucket at
client construction; bifrost-net enforces it on every request.

```rust
pub struct RateLimit {
    pub host: String,                // "gmail.googleapis.com"
    pub quota_per_second: f64,       // 250.0 for Gmail
    pub cost_default: u32,           // 1 by default
    pub burst: u32,                  // bucket capacity
}

pub trait RequestCost {
    /// Cost in quota units. Default 1.
    fn cost(&self) -> u32 { 1 }
}
```

Gmail's quota system is non-uniform (per `plans/gmail/streaming.md`:
"per-user rate limit is 250 quota units/sec; a `messages.get` costs
5 units"). The governor takes a per-request cost so `messages.get`
debits 5 from the bucket while `messages.list` debits 1.

```rust
impl Net {
    pub async fn request<Cost: RequestCost>(
        &self,
        req: PreparedRequest,
        cost: Cost,
    ) -> Result<Response, Error>;
}
```

Backpressure: when the bucket is empty, the request awaits a
refill. The governor's `acquire` is a `Notify`-based wait, not a
fixed sleep, so a refund (server returned a 429 with a long
`Retry-After`, the governor refunds the unused cost) wakes
waiters immediately.

Per-protocol defaults:

- Gmail: 250 units/sec, burst 50, default cost 1, callers pass
  cost explicitly for `messages.get` (5), `messages.attachments
  .get` (5), `messages.batchModify` (50 per batch).
- Graph: per-tenant; defaults to 3 in-flight via the existing
  semaphore. Translated to: `quota_per_second = 10`,
  `burst = 3`. The 429 path is the load-shedding signal; the
  governor only sets a starting ceiling.
- JMAP: server-specific. Default to no rate limit; let 429
  drive the retry path. Add per-deployment overrides via
  `NetConfig`.

Per-account multiplier: when the engine's `Control::priority`
is `Background` or `Bulk`, bifrost-net divides the bucket size
by 4 and 8 respectively. `Foreground` requests pull from a
reserved 25% slice of the bucket so a backfill cannot starve a
user-visible "open this email now."

The governor is per-host because per-account would not deduplicate
across tenants on Graph (one tenant's policy applies to all its
mailboxes). Multi-account quota sharing falls out naturally:
five Gmail accounts on one process share one 250-units/sec
bucket per host until Google's quota system actually treats them
separately (it does not at the API-key level for personal
accounts; it does at the OAuth-app level for some workspace
configurations - rate limits are per project, not per user).
Per-account refinement can layer on top later if production
shows per-user rate limits dominate; the per-host floor is the
correct shape for v1.

## Bandwidth meter

The engine's `Control::bandwidth_observed() -> u64` (defined in
`plans/sync-engine.md` -> Three layers) reads bytes/second
observed at the transport layer. Where the measurement comes from:

```rust
pub struct BandwidthMeter {
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    window: ArcSwap<RateWindow>,  // sliding 10-second window
}

pub struct RateWindow {
    samples: [u64; 10],  // 10 x 1s buckets
    head: usize,
    last_tick: Instant,
}
```

bifrost-net wraps the response body in a counter that increments
`bytes_in` on every chunk. Request bodies count `bytes_out` at
send time. The sliding window updates on read; `observed_bps()`
returns the sum over the last 10 seconds divided by the elapsed
window (so a one-shot burst does not pin the reading at the
burst peak forever).

The engine reads the meter per-account by tagging requests with
an `AccountId` and partitioning samples by tag. The meter
internally holds one `RateWindow` per registered account; the
top-level meter is the sum across accounts:

```rust
impl Net {
    pub fn meter_for(&self, account: AccountId) -> AccountMeter;
}

impl AccountMeter {
    pub fn observed_bps(&self) -> u64;
    pub fn bytes_in(&self) -> u64;
    pub fn bytes_out(&self) -> u64;
}
```

`Control::bandwidth_observed` resolves to `AccountMeter::
observed_bps()` for the account the `Control` handle belongs to.

`Control::bandwidth_cap(Some(bps))` sets a per-account cap.
bifrost-net implements the cap via an additional token bucket
sized in bytes (refilled at `bps` per second). The response-body
reader awaits the bucket before consuming the next chunk. This is
not the rate-limit governor (which counts requests); it is a
distinct byte-rate throttle. Both buckets can apply to a single
request.

IMAP and SMTP feed the same `AccountMeter` through a small
`MeterSink` interface. They are not HTTP, but the engine needs
one bandwidth reading per account regardless of transport. The
meter lives in bifrost-net because the engine already depends on
bifrost-net for the HTTP path; making IMAP and SMTP poke a
bifrost-net handle adds nothing to their dependency footprint
(they already depend on `tokio` and `bytes`, which is what the
meter needs).

## TLS

`native-tls` workspace-wide. One TLS stack, one trust store, one
set of CVE concerns. Reasoning:

- `reference/imap.md` calls out "Tokio + native-tls only" and
  `reference/smtp.md` notes "the TLS backend matrix has been
  removed. Native-tls only." The HTTP-based protocols (JMAP,
  Gmail, Graph) use reqwest's default backend, which is
  native-tls. Nothing in the workspace currently links rustls.
- The platform trust store is where users expect CA overrides
  to live on macOS and Windows, and where system administrators
  expect to deploy private CAs on Linux. The argument the IMAP
  side relies on (servers in the wild ship more self-signed and
  misconfigured certs than HTTP servers) applies with less weight
  to the HTTP layer, but the user-expectation argument applies
  uniformly.
- Reqwest on native-tls supports HTTP/2 ALPN, modern cipher
  suites, and connection pooling - the features bifrost-net
  needs from the TLS layer. There is no HTTP/2 capability lost
  by staying on native-tls.

So: every TLS handshake in every bifrost binary goes through
native-tls. No rustls dependency. `bifrost-net`'s reqwest pulls
the `native-tls` feature; IMAP and SMTP keep their existing
`tokio-native-tls` stacks.

`NetConfig` exposes a `dangerous_accept_invalid_certs` flag for
test fixtures, defaulting to false. There is no production
override for this; consumers who need a private CA inject it via
`NetConfig::with_root_cert`, which takes a native-tls
`Certificate`.

## Tracing context propagation

W3C `traceparent` header injection on every outbound HTTP request.
The header format is fixed:

```
traceparent: 00-<trace-id-32-hex>-<span-id-16-hex>-<flags-2-hex>
```

bifrost-net hooks into the request pipeline at the final pre-send
stage:

1. Reads the current `tracing` span from `tracing::Span::current()`.
2. Extracts trace-id and span-id from a `tracing-opentelemetry`
   layer's stored `OpenTelemetrySpanExt` data, or generates new
   ids if no layer is installed (the request is still
   propagating context to the upstream).
3. Encodes as `traceparent` and inserts into the request headers.

The dependency is `tracing-opentelemetry` for the extraction
helper; bifrost-net does not require any specific OpenTelemetry
exporter to be wired (the user picks one, or none).

`tracestate` is also propagated when present, per the W3C spec.

This is what `plans/sync-engine.md` -> Observability commits to:
"Trace propagation. W3C `traceparent` header for HTTP-based
protocols (JMAP, Gmail, Graph). bifrost-net injects and propagates.
IMAP has no trace header; the span lives entirely inside the
process." bifrost-net owns the HTTP side; SMTP has no header
convention and is also process-internal.

Span naming: bifrost-net opens one child span per request with
the name `bifrost.net.request`, attributes `http.method`,
`http.url` (path only; query string redacted because it carries
JMAP method bodies in raw-HTTP debug modes), `http.status_code`,
`bifrost.net.attempt` (retry attempt number), and
`bifrost.net.account_id`. The protocol crate's own span (e.g.
`bifrost.sync.changes` from the engine) is the parent.

## Public API surface

The minimum stable surface protocol crates build against:

```rust
// One per process. Cheap to clone (internal Arc).
#[derive(Clone)]
pub struct Net {
    inner: Arc<NetInner>,
}

pub struct NetConfig {
    pub pool_idle_timeout: Duration,
    pub pool_max_idle_per_host: usize,
    pub http2_keep_alive_interval: Duration,
    pub http2_keep_alive_timeout: Duration,
    pub tcp_keepalive: Duration,
    pub connect_timeout: Duration,
    pub user_agent: String,
    pub root_certs: Vec<native_tls::Certificate>,
    pub dangerous_accept_invalid_certs: bool,
}

impl Net {
    pub fn new(config: NetConfig) -> Self;
    pub fn builder() -> NetBuilder;

    // Register an account-scoped client. The host(s) the account
    // talks to are declared up front so the rate-limit governor
    // and bandwidth meter can pre-allocate.
    pub fn attach_account(
        &self,
        id: AccountId,
        spec: AccountSpec,
    ) -> AccountNet;

    pub fn detach_account(&self, id: AccountId);
}

pub struct AccountSpec {
    pub hosts: Vec<RateLimit>,
    pub token_source: Arc<dyn TokenSource>,
    pub default_retry: RetryPolicy,
}

#[derive(Clone)]
pub struct AccountNet {
    net: Net,
    account: AccountId,
}

impl AccountNet {
    pub async fn get(&self, url: &str) -> RequestBuilder;
    pub async fn post(&self, url: &str) -> RequestBuilder;
    pub async fn put(&self, url: &str) -> RequestBuilder;
    pub async fn patch(&self, url: &str) -> RequestBuilder;
    pub async fn delete(&self, url: &str) -> RequestBuilder;

    // Streaming download. The body is a futures Stream<Item =
    // Result<Bytes, Error>>; the bandwidth meter counts as the
    // consumer drives the stream.
    pub async fn download_stream(
        &self,
        url: &str,
        range: Option<ByteRange>,
    ) -> Result<ByteStream, Error>;

    pub fn meter(&self) -> AccountMeter;
    pub fn set_bandwidth_cap(&self, bps: Option<u64>);
    pub fn set_priority(&self, p: Priority);
}

pub struct RequestBuilder { /* hides reqwest::RequestBuilder */ }

impl RequestBuilder {
    pub fn header(self, k: &str, v: &str) -> Self;
    pub fn json<B: Serialize>(self, body: &B) -> Self;
    pub fn body(self, body: Bytes) -> Self;
    pub fn cost(self, cost: u32) -> Self;
    pub fn retry(self, policy: RetryPolicy) -> Self;
    pub fn timeout(self, timeout: Duration) -> Self;

    pub async fn send(self) -> Result<Response, Error>;
    pub async fn send_streaming(self) -> Result<StreamingResponse, Error>;
}

pub struct Response {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

pub struct StreamingResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: ByteStream,  // futures::Stream<Item = Result<Bytes, Error>>
}

#[non_exhaustive]
#[derive(Debug)]
pub enum Error {
    Network { source: hyper::Error },
    Timeout,
    Tls { message: String },
    Status { code: StatusCode, body: Bytes, headers: HeaderMap },
    RetryBudgetExhausted {
        last_status: Option<StatusCode>,
        retry_after_history: Vec<Duration>,
    },
    AuthLost,                              // 401 after refresh retry
    RateLimited { retry_after: Duration }, // 429 after budget
    Cancelled,
}
```

Notes:

- `AccountNet` is what protocol crates pass around. It carries
  the account identity for metering, the token source for OAuth,
  and the rate-limit + retry policies. Cheap to clone.
- The `RequestBuilder` deliberately hides `reqwest::RequestBuilder`.
  Protocol crates do not reach for reqwest types; if reqwest gets
  swapped later (unlikely - the migration cost has to clear a
  high bar), the call sites do not break.
- `ByteStream` is `Pin<Box<dyn Stream<Item = Result<Bytes, Error>>
  + Send>>`. The same erased shape as `AccountStream<T>` in
  `plans/account-trait.md` -> The trait surface, so blob download
  paths compose naturally.
- `Error` is `#[non_exhaustive]` per workspace convention. The
  protocol crate maps each variant to its own typed error. The
  engine sees the protocol crate's `RecoveryClass`, never
  `bifrost_net::Error` directly.
- `TokenSource` is a trait, not a struct, so OAuth flows
  (refresh-token, device-flow, service-account JWT) all
  implement the same shape. Consumers that already manage their
  own token lifecycle (some IMAP-XOAUTH2 callers) provide a
  trivial implementation that returns the token they already
  hold.

What is intentionally not in the surface:

- No `set_access_token` setter. Token lifecycle is owned by
  `TokenSource`. The existing Gmail and Graph crates expose
  `set_access_token` because the consumer pushes refreshed
  tokens in; under bifrost-net, the consumer implements
  `TokenSource::refresh` instead and bifrost-net pulls.
- No raw `reqwest::Client` accessor. The existing Gmail and
  Graph crates expose `http_client(&self) -> &reqwest::Client`
  for callers who reach around the abstraction. bifrost-net
  does not. Adding it after the fact is easier than removing
  it; leave it out.

## Migration plan

Three crates migrate; one new crate appears. No protocol-level
behavior changes; the migration is mechanical except for the
token-source inversion.

### New: `crates/net/` -> `bifrost-net`

Skeleton:

```
crates/net/src/
├── auth.rs         - TokenSource trait, OAuthRefresher,
│                     single-flight state machine, AccessToken
├── client.rs       - Net, NetBuilder, NetConfig, AccountNet,
│                     RequestBuilder, internal reqwest::Client
├── error.rs        - Error, with #[non_exhaustive], From impls
│                     for hyper/reqwest internal types
├── meter.rs        - BandwidthMeter, AccountMeter, RateWindow
├── pool.rs         - reqwest::Client configuration helpers
├── rate.rs         - RateLimit, RateLimitGovernor (token bucket),
│                     Priority -> bucket multipliers
├── retry.rs        - RetryPolicy, RetryableSet, decision logic,
│                     httpdate parsing for Retry-After
├── stream.rs       - ByteStream type alias, StreamingResponse,
│                     metered wrapper over reqwest::bytes_stream
├── trace.rs        - traceparent injection, OpenTelemetry
│                     extraction, span construction
└── lib.rs          - re-exports, AccountId, ByteRange,
                      Priority (re-exported from sync-engine
                      contract where defined)
```

Workspace `Cargo.toml` gains:

```toml
[workspace.dependencies]
bifrost-net = { path = "crates/net" }
```

Hoisted dependencies move from per-crate `Cargo.toml`s:

- `reqwest` (with `default-features = false, features =
  ["native-tls", "stream", "json"]`).
- `native-tls`, `httpdate`.
- `tracing-opentelemetry`, `opentelemetry`.

### `crates/jmap/`

What moves out:

- `crates/jmap/src/core/transport.rs` -> `HttpTransport` and
  `SseTransport` traits become bifrost-net consumers, not
  trait surfaces. The JMAP `Client::with_transport` extension
  point stays (some consumers genuinely want a mock transport
  for tests), but its default implementation now wraps
  `AccountNet` instead of holding a raw `reqwest::Client`.
- The default `ReqwestTransport` implementation becomes a
  thin shim over `AccountNet::post` / `get` / `download_stream`.

What stays:

- `JmapMethod` dispatch and `define_*_method!` machinery.
- `Capability` trait, session resource, request/response
  builder. All the JSON-shaped bits.
- `TransportError` stays but is now produced by mapping
  `bifrost_net::Error`. JMAP keeps its own typed transport
  error so `ProblemDetails` parsing has somewhere to land.

The mock-transport seam stays useful for unit tests that exercise
JMAP request/response without standing up an HTTP server.

### `crates/gmail/`

What moves out:

- `crates/gmail/src/client.rs:11-258`: `MAX_RETRY_ATTEMPTS`,
  `INITIAL_BACKOFF_MS`, `execute_with_retry`, `retry_delay`,
  `parse_*_response` -> all gone. Replaced by
  `AccountNet::post(...).cost(5).send()`.
- `RwLock<String>` access token store -> gone. Replaced by
  the consumer registering a `TokenSource` on account attach.
- `reqwest::Client` field -> gone. Replaced by `AccountNet`.

What stays:

- `GmailClient` as a public type, holding `AccountNet`.
- Path-vs-absolute URL handling (`api_url`).
- Service name string for error context.
- All the higher-level method-shaped wrappers in
  `crates/gmail/src/api.rs`, `blob.rs`, `parse.rs`, etc.

Net change per call site: `self.inner.http.post(url)...send().await`
becomes `self.net.post(&url).await.cost(cost).send().await`.

Cost annotations are added per-call: `messages.get` and
`messages.attachments.get` pass `.cost(5)`; `messages.list` and
`labels.list` pass nothing (default 1); `messages.batchModify`
passes `.cost(50)`.

### `crates/graph/`

What moves out:

- `crates/graph/src/client.rs:13-326`: `MAX_RETRY_ATTEMPTS`,
  `INITIAL_BACKOFF_MS`, `CONCURRENCY_LIMIT`, the `Semaphore`,
  `execute_with_retry`, `is_retryable`, `retry_delay`,
  `parse_*_response`, `check_response_status` -> all gone.
  Concurrency is the rate-limit governor; retry is the retry
  policy; the manual semaphore around `execute_once` is dead.
- `RwLock<String>` access token, same as Gmail.

What stays:

- `GraphClient` as a public type, holding `AccountNet`.
- `for_shared_mailbox` and the `mailbox_id` selector. The
  account identity flows through the `AccountId` on the
  underlying `AccountNet`; the shared-mailbox view is a
  thin clone with `mailbox_id` set.
- `FolderMap` caching, `category_lock`, EWS-specific bits in
  `crates/graph/src/ews/`. None of that is transport.
- `put_bytes_range` for OneDrive upload sessions: the wire
  format (Content-Range, Content-Length headers) stays in
  the Graph crate; only the request execution flows through
  `AccountNet`.

### `crates/imap/` and `crates/smtp/`

Do not migrate transport. The TLS handshake, socket lifecycle,
and connection pool stay as they are.

What does change:

- Both crates start reporting bytes-in/bytes-out into the
  bandwidth meter when bifrost-sync is wiring them up. Today
  there is no engine to read the meter; the wiring lands
  alongside the engine. Until then, the meter is dead code
  the protocol crate ignores.
- Both crates start opening a `bifrost.net.account` span
  wrapper at the public-method boundary so the W3C
  `traceparent` lineage is consistent across protocols, even
  though no `traceparent` is sent over the wire. The span
  attributes include `bifrost.net.protocol = "imap" | "smtp"`
  so dashboards can filter.

### Phased rollout

1. **Phase 1.** Land `bifrost-net` with `Net`, `NetConfig`,
   `AccountNet`, `RequestBuilder`, `TokenSource`, `OAuthRefresher`,
   `RetryPolicy`, `RateLimitGovernor`, `BandwidthMeter`, the
   native-tls config, and the traceparent injection. No protocol
   crate consumes it yet. Unit tests for the retry decision
   function, the single-flight refresher, and the token-bucket
   acquire/refund cycle. No live-server tests (per workspace
   policy in `AGENTS.md`).
2. **Phase 2.** Migrate `bifrost-gmail` to consume `AccountNet`.
   Gmail is the cleanest migration: one call shape, one host,
   no shared-mailbox indirection, no SSE/WebSocket. Validates
   the surface end-to-end.
3. **Phase 3.** Migrate `bifrost-graph`. Brings in the
   shared-mailbox-as-clone pattern, the batch endpoint, and
   the EWS HTTP long-poll path. The semaphore-based concurrency
   cap goes away in favor of the governor.
4. **Phase 4.** Migrate `bifrost-jmap`. The mock-transport
   indirection in `crates/jmap/src/core/transport.rs` needs a
   small refactor so the default impl wraps `AccountNet`; the
   `HttpTransport` trait stays for test injection.
5. **Phase 5.** IMAP and SMTP feed the bandwidth meter; both
   crates take a `Net` handle at construction (optional, for
   the engine path; absent for stand-alone usage).

Each phase is one commit's worth of code plus tests. Inter-
phase boundaries are the natural commit points and the natural
agent-file-ownership boundaries: phase 1 owns `crates/net/`,
phase 2 owns `crates/gmail/src/client.rs` plus call sites in
`crates/gmail/src/api.rs` and friends, and so on. No agent ever
touches files owned by another agent in the same orchestration
pass.
