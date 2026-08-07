# bifrost-net reference

Current architecture of the shared HTTP transport crate.

Scope: HTTP/2 connection pool, OAuth bearer-token refresh, retry
budget, per-host rate limiting, per-account bandwidth metering,
native-tls, W3C `traceparent` injection, URL component-encoding
helpers shared by the HTTP protocol crates. Used by `bifrost-jmap`,
`bifrost-google`, `bifrost-graph`. Not used by `bifrost-imap` or
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
- `Net::shared_default()` returns a process-wide `OnceLock<Net>`
  built from `NetConfig::default()`. The default registration target
  for protocol crates whose ergonomic constructors need a `Net`
  without forcing the caller to thread one through; multiple clients
  in the same process share the connection pool, governor, and
  bandwidth meter. Applications that need a custom `NetConfig` still
  call `Net::new` and hand the result to the protocol clients
  explicitly.

Every `attach_account` receives a monotone registration token carried
by its `AccountNet`. `AccountNet::detach()` removes that exact
registration, decrements the meter's per-id attach count, and
decrements the governor for only the hosts that attachment
successfully registered. Release is also automatic: `AccountNetInner`
has a `Drop` that runs the same registration-keyed teardown when the
last clone of a handle goes. Google and Graph call `detach()`
explicitly, the JMAP reqwest transport never did, and
`NetInner::account_hosts` plus the meter map therefore grew by one
entry per JMAP account open for the process lifetime; making the
release automatic closes that for every caller rather than relying on
each to remember, and leaves `detach()` as the way to release early.

Keying on the exact token is what makes this safe when two live handles
share an `AccountId`: a stale Drop cannot detach the replacement.
`detach_registration` returns before touching the meter or the governor
once its token is absent, so a Drop after an explicit `detach()`, or
after a cross-id `retag()` moved the token, is a complete no-op. A
SAME-id `retag` returns the receiver itself rather than minting a
second `AccountNetInner` over one token - two owners of one
registration is ambiguous, and whichever dropped first would release
what the other still depends on.
`Net::detach_account(id)` remains the ID-only compatibility entry point
and removes one registration. Host buckets and meter counters are
reclaimed only after their final matching detach.

`AccountNet::retag(new_id)` re-mints the handle under a different
engine `AccountId`. Used by protocol-crate factories whose
`open(account_id)` needs to honor an engine-minted id when the
consumer built the `AccountNet` ahead of time via
`*Client::with_account_net` and the parent `Net` is no longer
reachable through the client. The exact token's per-account host entry
moves under `new_id`, and the meter transfers one attachment count from
the old id to the new one (it does not separately register the new id,
so a retag never inflates the total attach count). The new handle
inherits token
source, retry policy, priority, and bandwidth cap from the old.
Governor refcounts are per-host and not touched. The old handle keeps
working for in-flight clones; its old-id teardown is a no-op because
the token moved.

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

### Deadlines and the buffered ceiling

Three bounds, at deliberately different layers:

- `NetConfig::connect_timeout` (10s) covers reaching the server.
- `NetConfig::read_timeout` (30s) is an inactivity deadline between
  response body chunks, installed on the shared client so it applies to
  every request, buffered and streaming. Without it a request that
  connects and then stalls mid-body produces no error at all: the retry
  loop never fires, and the sync scope blocks indefinitely. Only JMAP
  set a per-request timeout, so every Gmail and Graph call previously
  had no deadline of any kind. An inactivity bound rather than a total
  one, so it cannot fail a legitimately slow large download.
- `NetConfig::default_request_timeout` (120s) is a TOTAL deadline
  `RequestBuilder::send` supplies when the caller set no explicit
  `.timeout()`. Buffered-only: `send` is the JSON-API path where a
  whole-request ceiling is right, while `send_streaming` carries blob
  downloads that legitimately run for minutes and would fail on size
  rather than on health. Streaming is bounded by `read_timeout` alone.

`NetConfig::max_buffered_response` (`DEFAULT_MAX_BUFFERED_RESPONSE`,
64 MiB) caps what `send` will accumulate, failing with
`Error::ResponseTooLarge` before the allocation that would exceed it
and dropping the stream. Every JSON API call in google / graph / jmap
takes this path, so without a ceiling a provider outage page, a
mis-routed blob URL, or a hostile response OOMs the process; the error
path already had a 4 KB cap in `read_capped_response_body` and the
success path had nothing. `None` disables the check.
`send_streaming` is uncapped by design - the caller owns backpressure
there. The CalDAV and CardDAV clients, which run their own transport,
apply the same constant in their `read_capped_body` rather than
`reqwest::Response::text()`.

The loop sends through a crate-private `Dispatch` seam. Production
dispatch delegates to reqwest. Tests install a scripted dispatcher
that accepts real reqwest request builders and returns in-process
responses or typed transport errors. This covers retry,
authentication, redirect, range, metering, and cancellation behavior
without sockets or listeners.

### The `test-support` feature

The scripted dispatcher is published to downstream crates under the
`test-support` feature, as `bifrost_net::test_support`: `Canned`
(a `Response` / `Error` / `Pending` wire outcome), `ScriptedDispatch`
(answers a script in order, records every `RequestSnapshot`), the
`canned` / `canned_with_headers` constructors, and the `scripted_net`
/ `scripted_account` builders. Consumers enable it as a
dev-dependency.

The `Dispatch` trait itself stays crate-private, deliberately. Its
signature is in terms of `reqwest::RequestBuilder` and
`reqwest::Response`, and keeping reqwest out of the public API is why
the `RequestBuilder` wrapper exists at all. What is published is the
double, not the trait.

Why it exists: account crates cannot construct a `Response`
(`#[non_exhaustive]`, no public constructor), so each one grew a
private double *above* `AccountNet` - which meant each re-derived
this crate's status contract, and a mis-derivation is invisible until
it hides a live defect. Scripting at the wire boundary instead puts
the production retry budget, `Retry-After` honor, rate-limit permit,
redirect walk, and bandwidth meter between the script and the
assertion, which is exactly the layer no consumer-side seam can
reach.

Two contract facts a consumer scripting statuses must know, both
pinned in `tests/test_support_seam.rs`:

- The retry loop turns every 4xx and 5xx into `Err` before a response
  surfaces. Only a 2xx and a passed-through 3xx reach a caller as
  `Ok(Response)`. A branch that matches a 4xx off an `Ok` is dead on
  the production path.
- An exhausted script panics rather than falling through to the
  network, so an under-scripted test fails loudly instead of dialing
  a real socket.

`AccountNet` exposes `get`, `post`, `put`, `patch`, `delete` - one
constructor per HTTP method routing into the same `RequestBuilder`.
Builder setters of note beyond the example: `body(Bytes)` for raw
payloads (used when `json()` is not the right encoding),
`idempotent(bool)` to override the method-derived replay-safety
default (see "Replay safety" below), and
`without_bearer_auth()` for the pre-authenticated-URL and
Basic-auth flows (Gmail upload URLs, JMAP Basic), which still want
the shared retry / rate-limit / metering pipeline. The token-source
short-circuit at `request.rs:375-390` skips `Authorization: Bearer`
injection when `bearer_auth` is false; caller-provided headers go
through unchanged.

`Response` and `StreamingResponse` are `#[non_exhaustive]` structs
with `status()` and `headers()` accessors; `StreamingResponse.body`
is the `ByteStream` returned by `wrap_metered`, so every chunk
feeds the per-account meter and the bandwidth-cap throttle.

### Retry decision (RetryPolicy)

- 2xx -> return. 3xx is passed through unchanged (reqwest follows
  redirects up to 10 hops by default; a surfacing 3xx means the
  caller disabled the policy, hit the limit, or got a terminal 3xx
  like 304). Conditional-request callers rely on the headers being
  exposed.
- 4xx not in `policy.statuses`, not 401 -> terminal
  `Error::Status`.
- 401 -> force refresh once; second 401 -> `Error::AuthLost` with
  `transmission_state: Some(Acknowledged)` and preserved 401 response
  evidence. The 401-recovery budget is **separate** from the network
  retry budget: a 401 retry no longer burns one of the `max_attempts`
  network attempts.
- 5xx or in `policy.statuses` -> retry with backoff. Honor
  `Retry-After` (delta-seconds + RFC 9110 HTTP-date via
  `httpdate`), capped by `policy.honor_retry_after_cap` (single
  source of truth). Without `Retry-After`: exponential backoff
  with **proportional** jitter (0..capped), capped at
  `policy.max_backoff`. Jitter entropy comes from the workspace
  UUID RNG (`Uuid::new_v4`), not `SystemTime::now`, so a clock jump
  cannot influence the wait.
- 429 -> same retry path; `Retry-After` honored.
- Network / timeout / decode errors -> retry if
  `policy.network_errors && attempt < max_attempts` **and the failure
  is replayable**. See "Replay safety" below.
- Past `max_attempts` -> `Error::RetryBudgetExhausted` (or
  `Error::RateLimited` on 429). The exhausted-attempt branch parses
  the *final* response's `Retry-After` and pushes it onto
  `retry_after_history` before surfacing, so consumers see the
  server's last hint.

### Replay safety

The transport-failure retry branch consults idempotency;
`transport_error_is_replayable` allows a retry when EITHER nothing was
transmitted (`TransmissionState::Unsent`) OR replaying cannot change
server state. Past `Unsent` on a non-idempotent request the side effect
may already have landed, and `send_error_to_error` classifies every
non-connect, non-timeout reqwest failure as `Network { InFlight }` -
exactly that evidence. Retrying regardless is how one Gmail
`/messages/send` becomes two delivered messages and one JMAP
`Email/set` create becomes two drafts, and it happened three times
before `into_account_error` ever ran, so the `Reconcile` classification
only ever described the final attempt.

Idempotency defaults to `reqwest::Method::is_idempotent()`: safe
methods plus PUT and DELETE are replayable, POST / PATCH / extension
methods are not. `RequestBuilder::idempotent(bool)` overrides it. The
override exists for two real shapes: a read carried over POST (JMAP's
single `/jmap/api` endpoint, when the payload's method calls are all
`*/get` or `*/query`) opts back in, and a GET a provider treats as an
action opts out. The JMAP transport does not set it today - its `send`
sees only method, URL, and opaque body bytes - so JMAP POSTs are
conservatively non-replayable at this layer.

This is not a net loss of resilience. The refused replay surfaces to
`into_account_error`, which holds the caller's real `AccountOperation`
and routes `InFlight` + idempotent to `Retry(SameRequest)` and
`InFlight` + non-idempotent to
`Reconcile(TransportDropAfterSend, [CheckTarget])`. The retry decision
moves up to the layer that knows what the request meant, instead of
being taken three times by the layer that only knows it is holding
bytes.

Status-driven retry (5xx, 429, `policy.statuses`) is deliberately NOT
gated on idempotency. A complete response is `Acknowledged` evidence
that the server is reporting it did not do the work, and
`recovery::derive`'s `transient_retry_or_reconcile` already treats
`Acknowledged` as replayable for any operation. Gating it here would
contradict the shared error model and turn every 503 on a POST into an
immediate failure.

Token-source failures returned from `current()` are passed through
to the caller unchanged: `Error::AuthLost` for true auth failures,
`Error::RefreshFailed { retry_after, source }` for transient
`Network`/`Timeout`/etc. failures (preserving the inner classification
for retry-vs-give-up decisions).

`policy.statuses` is the additive retry set on top of unconditional
5xx; explicitly documented to avoid double-counting.

The rate-limit slot is **refunded on every retried failure**, not
just 429-with-Retry-After, so 5xx retries don't burn quota for work
the server didn't perform.

Each acquired slot is held by an armed guard until a response arrives.
Dropping or cancelling the request future before server
acknowledgement refunds the slot automatically. Response-backed retry
and redirect paths retain their explicit refund rules.

## OAuth single-flight (`OAuthRefresher`)

State machine: `Empty` -> `Refreshing` -> `Fresh`. The first
`token()` call from `Empty` transitions to `Refreshing` and drives
the network refresh itself; concurrent callers register a
`oneshot::Sender` in `Refreshing.waiters` and await. On success the
driver transitions to `Fresh { token, refreshed_at }`. On failure
the driver fans `Arc<Error>` clones to every waiter (because
`Error` is not `Clone`).

`Error::RefreshFailed { retry_after, source: Arc<Error> }` preserves
transient-vs-permanent: `Network`/`Timeout` survive as themselves;
true auth failures (token-endpoint 401/403) map to `AuthLost`.
OAuth 429/503 `Retry-After` hints are projected to the wrapper's
absolute deadline.

A waiter whose driver drops the sender without answering - task
cancelled, panicked, or the runtime shutting down - also gets
`RefreshFailed`, with `Error::Cancelled` as the preserved source. That
event says nothing about the credential, so classifying it as
`AuthLost` (which `auth_lost` maps unconditionally to
`Authentication(ReauthorizationRequired)` and thence to the terminal
`RecoveryClass::AuthLost`) told the engine a shutdown race meant the
user must re-authorize. `RefreshTransient` into
`Retry(AfterAuthRefresh)` is what a lost driver warrants.

When a proactive refresh displaces a cached token with a known expiry,
`Refreshing` retains that token as a fallback. A transient refresh
failure restores and returns it if it is still valid, and defers the
next proactive attempt for 30 seconds so an endpoint outage does not
cause one refresh call per request. Token expiry overrides that delay.
Forced refreshes after a target 401 and terminal token-endpoint
authentication failures never use the fallback.

`AccessToken` wraps `Zeroizing<String>`; `Debug` redacts the bytes
and surfaces only length + expiry.

### Proactive-refresh window

- Tokens with an issuer-supplied `expires_at`: refresh 60 s before
  the deadline.
- Tokens without `expires_at` (opaque, no TTL hint): refresh when
  the cached token is older than the refresher's `max_age`. Default
  is `DEFAULT_TOKEN_MAX_AGE` (55 min, leaving a 5 min margin under
  the typical 60 min issuer TTL). Callers configure the value at
  construction via `NetConfig::token_max_age`;
  `Net::attach_account` reads it and wires it into the per-account
  refresher with `OAuthRefresher::with_max_age`, so production
  callers do not touch the builder directly. The previous behaviour
  treated opaque tokens as fresh indefinitely and waited for a
  server 401; the max-age fallback closes that latency leak.

**Do not nest `OAuthRefresher`s.** Wrapping one refresher around
another causes the outer's `force_refresh` to bypass the inner's
single-flight. Compose against the raw `TokenSource` instead.

### `StaticTokenSource`

In-memory `TokenSource` for callers that hand bifrost already-minted
access tokens and rotate them out-of-band (e.g. an external auth
service pushes a fresh token periodically). Backed by
`Arc<RwLock<AccessToken>>`; `set(AccessToken)` replaces the cached
token under the write lock, `token()` snapshots the current value,
and the trait's `refresh()` is the same as `current()` because there
is no refresh material inside bifrost-net for this shape. The
refresher above wraps a `StaticTokenSource` just like any other
provider; rotation just means the next `current()` reads the
swapped-in token.

## Rate-limit governor (`RateLimitGovernor`)

Per-host token bucket behind `Arc<Mutex<HashMap<String,
HostBucket>>>`. `acquire(host, cost)` returns a `Send + 'static`
future that captures `Arc::clone(&self.buckets)`, refills tokens
based on elapsed time, debits `cost` if available, otherwise awaits
the bucket's `Notify` (with a 250 ms poll cap so refunds don't
deadlock). Refill timestamps use `tokio::time::Instant`, so paused-time
tests advance deterministically. `refund(host, cost)` wakes one waiter.

`cost > burst` short-circuits to `Error::CostExceedsBurst` instead
of hanging: the bucket can never fill that high. Configuration bug,
not a runtime condition.

Unregistered hosts are unmetered (no-op `acquire`).

For a previously unregistered host, registration rejects non-finite,
zero, and negative `quota_per_second` values with a warning and
returns `false`. `Net` records only successful registrations in the
attachment token's host set, so a rejected account cannot later
unregister another account's valid bucket. A duplicate declaration,
even an invalid one, joins the first valid bucket, increments its
attach count, and returns `true`. Invalid quota configuration
therefore cannot panic, spin, park forever, or unbalance teardown.

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

`AccountNet` caches its `AccountMeter` at attach or retag time, avoiding
an account-id allocation and meter-map lookup on each request attempt.
Meter registrations are counted per `AccountId`; one detach cannot
remove the live entry of a concurrent reopen. After the final detach,
an already-cached handle retains a frozen counter snapshot while new
lookups return zero.

`MeterSink` trait lets IMAP/SMTP (which bypass this crate's
transport) feed `record_bytes_in` / `record_bytes_out` into the
same meter when the engine wires them. `MeterSinkHandle` is the
account-scoped adapter raw-socket transports compose around a
`MeterSink`: it owns the `AccountId` so the connection-level call
site does not have to thread the id alongside every byte count.
`MeterSinkHandle::from_meter` wraps the process-wide
`BandwidthMeter` directly; the trait-object form
`MeterSinkHandle::new` accepts any `Arc<dyn MeterSink>` for test
doubles. Both the direct HTTP meter and `MeterSink` are lookup-only:
recording against an unknown or detached account is a no-op and never
re-registers it. Wiring against IMAP / SMTP lands in S1-W2.

### Bandwidth cap

Per-account cap stored on `AccountNet` in `AtomicU64` with
`u64::MAX` sentinel for `None`. Throttles via `ByteBucket` on the
response-body reader. Chunks larger than the per-second cap are
admitted after `(chunk_size / cap)` seconds of sleep (the cap is a
smoothing throttle, not a hard ceiling on chunk size). `ByteBucket`
also uses `tokio::time::Instant`, matching its Tokio sleep clock and
allowing deterministic virtual-time tests.

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

For a closed request whose end exceeds the resource length, a legal
shortened response is accepted when its end is exactly the known
resource tail. A shorter window before the resource tail remains a
`ContentRangeMismatch`.

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
  `Error::RangeNotHonored { kind: LocalInvalid, message:
  "zero-length range" }` so hand-rolled callers see the error path.
- `start + length` overflow is rejected up front by `encode_range`
  with `Error::RangeNotHonored { kind: LocalInvalid, message:
  "range overflow: ..." }` rather than silently emitting
  `bytes=N-u64::MAX`.

## TLS

`native-tls` only. No `rustls` dep anywhere. `NetConfig` accepts a
`Vec<native_tls::Certificate>` for additional root certs and a
`dangerous_accept_invalid_certs` flag for self-signed fixtures.
`NetConfig::with_root_cert(cert)` is a builder helper that pushes
one `native_tls::Certificate` onto `root_certs` and returns `self`
so callers can chain.

`NetConfig::follow_redirects` is a `FollowRedirects` enum with two
variants: `Disabled` and `Enabled(RedirectPolicy)`. The default is
`Enabled` with an empty trusted-host allowlist and a ten-hop maximum.
Regardless of the variant, `Net::new` always installs
`reqwest::redirect::Policy::none()` on the underlying HTTP client -
`bifrost-net` owns the redirect loop unconditionally so RFC 7231
§6.4 method rewriting, the trusted-host allowlist, and
`Authorization`-stripping on cross-host hops happen exactly once
and the same way for every HTTP protocol crate.

### Redirect loop (`redirect.rs`)

Inside `send_streaming_inner` the retry loop classifies every 3xx
response through `classify_redirect`:

- 304 / 305 / 306: `PassThrough`. Status + headers surface to the
  caller; conditional-request flows (`If-None-Match` -> 304,
  Graph's 304 on `$delta`) keep working.
- 301 / 302: rewrite to GET and drop the body when the prior method
  is not safe (POST / PUT / DELETE / PATCH); preserve method + body
  for GET / HEAD.
- 303 See Other: rewrite to GET and drop the body unconditionally.
- 307 / 308: preserve method and body.
- Any followed-redirect status whose `Location` header is **absent**:
  `PassThrough`. A redirect no one can follow is handed back as a
  terminal status (status + headers) the same way a 304 is. This is
  load-bearing for Google Drive resumable uploads: a `308 Resume
  Incomplete` carries a `Range` header and no `Location`, and the
  cloud chunk loop reads that status + `Range` itself. A
  present-but-malformed `Location` (invalid encoding / unresolvable)
  stays a hard `MalformedRedirect`; only the missing header passes
  through.

`same_origin` - the `keep_auth` / cross-host decision - compares all
three RFC 6454 origin components: scheme, host (case-insensitively, per
RFC 3986 §3.2.2), and `port_or_known_default`. Scheme was previously
uncompared on the reasoning that the pipeline only issues `https`;
nothing enforced that, and an `https` -> `http` downgrade was
classified cross-origin only because the default ports happen to differ
(443 vs 80), so a downgrade spelled `http://h:443/` would have carried
the bearer onto a cleartext hop.

`RedirectPolicy::trusted_hosts` is an allowlist for cross-host hops:
empty means every host is acceptable; populated means only matching
hosts are admitted (case-insensitive host comparison per RFC 3986
§3.2.2) and `Error::RedirectRejected` is returned otherwise.
`Authorization` headers are stripped on every cross-host hop
regardless of allowlist membership, covering both the
token-source-derived bearer the pipeline injects in `build_reqwest`
and any caller-set `Authorization` header sitting in the request's
`HeaderMap` (JMAP Basic-auth, custom HMAC schemes, etc.). The strip
runs in the redirect loop after `classify_redirect` flags
`keep_auth: false`, so a credential never travels to a host the
original request did not target. `max_hops` (default 10) caps the
chain; the pipeline uses a widened counter so even `max_hops = 255`
terminates on hop 256 with `Error::RedirectLoop`. Each redirect hop
resets the retry counter to 0 - hops are fresh logical requests, not
retries.

`RedirectPolicy::reqwest_policy()` is the bare-client entry point for
callers that build their own `reqwest::Client` instead of routing
through the pipeline (the CalDAV / CardDAV clients). It returns a
`reqwest::redirect::Policy::custom` whose follow / stop / error
decision is driven by the same `max_hops` (default 10) and the same
case-insensitive `allows_host` allowlist check the pipeline's
`classify_redirect` uses, so the redirect-hardening rule lives in
exactly one place. A reqwest `Policy` can only decide follow / stop /
error - it cannot rewrite methods or strip headers; cross-origin
`Authorization` stripping is reqwest's own default and applies
regardless, and the method-rewriting / explicit auth-strip in the
pipeline are not part of this bare-client path. A hop outside the
allowlist is stopped (the 3xx surfaces as a terminal status);
exceeding `max_hops` errors. The DAV crates seed a single-host
allowlist (their configured base host) and call this; they previously
duplicated the hop-cap + allowlist logic locally (`dav_redirect_policy`
+ a `DAV_MAX_REDIRECTS = 5` const) - both are deleted and the DAV hop
cap now follows bifrost-net's 10.

`FollowRedirects::Disabled` skips the loop entirely; 3xx surfaces
to the caller exactly as it did before the loop landed. The
`NetConfig::follow_redirects(policy)` and
`NetConfig::with_redirect_policy(RedirectPolicy)` builder helpers
let callers configure the policy without naming the outer enum.

`Net::new` returns `Error::InvalidRequest` for bad TLS / client
configuration data: corrupt native-tls root cert DER, reqwest
rejection of the re-encoded DER, or client-builder failure. These
route to the account error model as `Request(Malformed)`, not as a
retryable transport setup failure.

## traceparent

W3C `traceparent` injected on every outbound request. Span id is
read from `tracing::Span::current()` when available; trace id is
freshly minted via `uuid::Uuid::new_v4` per request as a stopgap.
Full trace propagation (reading trace_id from the current
`tracing-opentelemetry` context) is a follow-up.

## Error model

`Error` is `#[non_exhaustive]` and `thiserror`-derived. Notable
variants:

- `Network { message, transmission_state, source }` - transport-level
  failures (DNS, TCP reset, generic transport). `transmission_state`
  is `Unsent`, `InFlight`, or `Acknowledged`; acknowledged network
  body failures convert to `Protocol(PartialResponse)`, never
  `Transport(_)`.
- `Timeout { transmission_state }` - per-request deadline, with the
  same transmission-state evidence.
- `Tls { message, transmission_state }` - TLS handshake or certificate
  validation failure. Handshake failures are `Unsent`; synthetic
  acknowledged TLS values are defensively converted as partial
  responses.
- `Status { code, body, headers }` - terminal non-retryable HTTP
  status. `body` is capped at 4 KB via `cap_status_body` with a
  truncation marker.
- `RetryBudgetExhausted { final_response, retry_after_history }` -
  retry budget exhausted. `final_response` preserves the last status,
  headers, and capped body when a response was received.
- `RateLimited { retry_after, final_response }` - 429 past the retry
  budget with preserved response evidence.
- `AuthLost { transmission_state, final_response }` - irrecoverable
  token failure. Local token-source loss has no final response;
  repeated target 401 after forced refresh preserves the 401 status,
  headers, and capped body with `transmission_state:
  Some(Acknowledged)`. Token-endpoint 401/403 during refresh also
  preserves its response evidence, but leaves `transmission_state`
  unset because it is not evidence for the target request.
- `RefreshFailed { retry_after, source: Arc<Error> }` - transient
  refresh failure preserving the original. OAuth `Retry-After`
  deadlines are stored on the wrapper for the account-error
  conversion.
- `ResponseTooLarge { limit }` - a buffered body exceeded
  `NetConfig::max_buffered_response`. Maps to
  `Protocol(ContractViolation)`, not a transport class: the server
  answered, and the same answer comes back on a retry.
- `Cancelled` - request cancelled before completion. Produced by
  `wait_for_refresh` as the preserved `RefreshFailed` source when the
  single-flight refresh driver drops its sender without answering.
- `CostExceedsBurst { cost, burst }` - rate-limit configuration
  bug.
- `EncodeBody { message, source }` - `RequestBuilder::json`
  serialization failure; surfaced lazily on the next `send`.
- `InvalidHeader { message, source }` - `RequestBuilder::header`
  rejected the header name or value; deferred to `send` the same
  way `EncodeBody` is, so the fluent setter stays chainable.
- `InvalidRequest { field, detail }` - pre-wire local request or
  client configuration failure.
- `RangeNotHonored { kind, message }` - local invalid range input or
  a response that did not honor the requested range.
- `RedirectRejected { message }` - 3xx target host fell outside
  the configured `RedirectPolicy::trusted_hosts` allowlist.
- `MalformedRedirect { kind, message }` - acknowledged 3xx response
  with a present-but-broken `Location` (non-UTF-8 or unresolvable). A
  *missing* `Location` is not an error: it passes through (see the
  redirect loop above).
- `RedirectLoop { hops }` - redirect chain exceeded
  `RedirectPolicy::max_hops`.

`Status`/`Response` carry `reqwest::StatusCode` and `HeaderMap`
straight through. Documented coupling cost; acceptable for v1.

`account_error.rs` exposes:

```rust
pub struct NetErrorContext {
    pub provider: Option<Provider>,
    pub protocol: Protocol,
    pub operation: AccountOperation,
    pub scope: Option<ErrorScope>,
}

pub fn into_account_error(error: Error, ctx: NetErrorContext) -> AccountError;
```

`operation` is required so in-flight non-idempotent failures cannot
fall through the central recovery mapper as retry-safe. The
conversion attaches `Cause::Attempt` when target-attempt evidence
exists, preserves request / trace ids and capped body text from
`Status`, `RateLimited`, and status-backed `RetryBudgetExhausted`,
and leaves provider JSON interpretation to JMAP, Gmail, and Graph.

## Status-line helpers

`status_line::status_line_code(&str) -> Option<u16>` and
`status_line_is_success(&str) -> bool` parse an HTTP status line
(`HTTP-version SP status-code SP reason-phrase`, RFC 9112). They live
here rather than in a DAV crate because that grammar is HTTP's, not
WebDAV's; DAV merely carries status lines as element text.

Both DAV clients need them: RFC 4918 puts an HTTP status line inside
each `<D:propstat>`'s `<D:status>`, so a 207 Multi-Status body is only
meaningful once each is read. They were previously spelled out once per
crate, and the drift that produced is the reason for consolidating (see
dav-F5).

Two behaviours worth not regressing:

- The code is the first whitespace-delimited token that parses as a
  number **in RFC 9110's `100..=599` range**, NOT the token at position
  1. Servers do emit the protocol-less form (`200 OK`) inside
  `<D:status>`, where a positional read takes `OK` as the code and
  classifies a good propstat as failed. `HTTP/1.1` contains no bare
  numeric token, so the version cannot be mistaken for the code. The
  range constraint is what stops a server's prose (`Error 42 occurred`,
  a `<D:status>` a proxy filled with free text) from parsing as status
  42; an out-of-range number is not a status, so the line lands in the
  unreadable bucket that `status_line_is_success` already fails closed
  on.
- A line with no parseable code is NOT success. An unreadable status is
  not evidence the property was returned, so treating it as success
  commits a value the server may have refused. This is the one place
  the two DAV crates had silently diverged: carddav failed closed here,
  caldav mapped unparseable to `None`, which its
  `propstat_success.unwrap_or(true)` then read as success. An ABSENT
  status remains success (RFC 4918 s14.22 requires one, so a server
  omitting it is describing a success) - only the present-but-unreadable
  case changed.

## URL helpers

`url::encode_path_component(value)` and
`url::encode_query_value(value)` share the RFC 3986 component escape
set: unreserved characters survive and every other ASCII character
plus all controls is escaped. The path form additionally
double-escapes a complete `.` or `..` component so the WHATWG parser
cannot resolve it as navigation while parsing the assembled URL. The
query form leaves those values literal because dots have no structural
meaning there. The HTTP protocol crates name the grammar at every call
site, preventing path hardening from corrupting search and filter
values.

## File map

```
crates/net/src/
  lib.rs          // re-exports; AccountFuture/AccountId/Priority/
                  // ByteRange from bifrost-types
  config.rs       // NetConfig + with_root_cert + follow_redirects
  net.rs          // Net + AccountNet; shared_default; attach/detach;
                  // download_stream
  request.rs      // RequestBuilder + Response/StreamingResponse;
                  // Dispatch seam, retry loop, redirect walk
  redirect.rs     // FollowRedirects + RedirectPolicy + classify_redirect
                  // (RFC 7231 §6.4 method rewriting, trusted-host
                  //  allowlist, Authorization stripping)
  account_error.rs // context-aware conversion to bifrost-types AccountError
  auth.rs         // TokenSource + OAuthRefresher state machine +
                  // StaticTokenSource
  retry.rs        // RetryPolicy
  rate.rs         // RateLimitGovernor + HostBucket
  bandwidth.rs    // BandwidthMeter + AccountMeter + MeterSink +
                  // MeterSinkHandle
  error.rs        // Error + cap_status_body
  trace.rs        // traceparent injection (current: uuid trace id)
  url.rs          // distinct path-component and query-value encoders
```
