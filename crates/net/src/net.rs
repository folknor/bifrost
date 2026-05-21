//! The top-level `Net` handle and the per-account `AccountNet` view.
//!
//! Protocol crates hold an `AccountNet` and never see the underlying
//! `reqwest::Client`. `Net` itself is process-wide; one instance is
//! shared across every account so connection pooling, rate limiting,
//! and bandwidth metering can coordinate across accounts that share a
//! host.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use reqwest::header::RANGE;

use crate::auth::TokenSource;
use crate::bandwidth::{AccountMeter, BandwidthMeter};
use crate::config::NetConfig;
use crate::error::Error;
use crate::rate::{RateLimit, RateLimitGovernor};
use crate::request::{ByteStream, InternalStreaming, RequestBuilder, send_streaming_inner};
use crate::retry::RetryPolicy;
use crate::{AccountId, ByteRange, Priority};

/// Process-wide HTTP transport. Holds the shared reqwest client, the
/// per-host rate-limit governor, and the bandwidth meter. Cheap to
/// clone via the inner `Arc`.
#[derive(Clone)]
pub struct Net {
    inner: Arc<NetInner>,
}

/// Backing state for `Net`. Kept private so the Phase 2
/// implementation can rearrange without touching call sites.
pub(crate) struct NetInner {
    /// Caller-supplied configuration. Snapshotted at construction;
    /// runtime mutations go through the per-account handles.
    pub(crate) config: NetConfig,
    /// Shared reqwest client built from `config` at `Net::new`.
    pub(crate) client: reqwest::Client,
    /// Per-host rate-limit governor. Shared across every account so
    /// multi-account quota sharing on Gmail / Graph works out of the
    /// box.
    pub(crate) governor: RateLimitGovernor,
    /// Bandwidth meter, partitioned per account.
    pub(crate) meter: BandwidthMeter,
}

impl Net {
    /// Construct a `Net` from a `NetConfig`. Builds the shared
    /// `reqwest::Client` with the requested pool, HTTP/2 keepalive,
    /// TCP keepalive, connect-timeout, user-agent, and trust-store
    /// settings. Native-tls only.
    ///
    /// # Errors
    /// Returns `Error::NetSetup` if `native_tls` rejects a supplied
    /// root certificate, if `reqwest::Certificate::from_der` rejects
    /// the DER re-encoding, or if `reqwest::ClientBuilder::build`
    /// itself fails. `NetConfig` is consumer-supplied (the engine reads
    /// the trust store from disk), so these are runtime conditions.
    // `Error` is a wide enum (boxed `dyn Error`, `Bytes`, `HeaderMap`)
    // and a one-shot constructor returning it trips
    // `result_large_err`. Boxing the `Err` here would force the rest
    // of the public surface to box too, which is the wrong tradeoff
    // for a process-wide constructor that runs once at startup.
    #[allow(clippy::result_large_err)]
    pub fn new(config: NetConfig) -> Result<Self, Error> {
        let mut builder = reqwest::ClientBuilder::new()
            .pool_idle_timeout(config.pool_idle_timeout)
            .pool_max_idle_per_host(config.pool_max_idle_per_host)
            .connect_timeout(config.connect_timeout)
            .http2_keep_alive_interval(config.http2_keep_alive_interval)
            .http2_keep_alive_timeout(config.http2_keep_alive_timeout)
            .tcp_keepalive(Some(config.tcp_keepalive))
            .user_agent(&config.user_agent)
            .danger_accept_invalid_certs(config.dangerous_accept_invalid_certs);

        for cert in &config.root_certs {
            // `native_tls::Certificate` -> `reqwest::Certificate` via
            // DER round-trip; both backends share the same DER format
            // so this is lossless under normal conditions. A failure
            // here means the caller handed us a corrupt cert.
            let der = cert.to_der().map_err(|e| Error::NetSetup {
                message: format!("native_tls certificate failed to encode as DER: {e}"),
                source: Some(Box::new(e)),
            })?;
            let reqwest_cert =
                reqwest::Certificate::from_der(&der).map_err(|e| Error::NetSetup {
                    message: format!("reqwest rejected DER certificate: {e}"),
                    source: Some(Box::new(e)),
                })?;
            builder = builder.add_root_certificate(reqwest_cert);
        }

        let client = builder.build().map_err(|e| Error::NetSetup {
            message: format!("reqwest client build failed: {e}"),
            source: Some(Box::new(e)),
        })?;

        Ok(Self {
            inner: Arc::new(NetInner {
                config,
                client,
                governor: RateLimitGovernor::new(),
                meter: BandwidthMeter::new(),
            }),
        })
    }

    /// Register an account with the transport. Returns an
    /// `AccountNet` carrying the per-account token source, rate
    /// limits, and default retry policy.
    pub fn attach_account(&self, id: AccountId, spec: AccountSpec) -> AccountNet {
        // Register the meter so the per-account counters exist before
        // any request lands. Rate-limit registration walks the host
        // list the caller supplied.
        self.inner.meter.register_account(id.clone());
        for limit in &spec.hosts {
            self.inner.governor.register(limit.clone());
        }
        AccountNet {
            inner: Arc::new(AccountNetInner {
                net: self.clone(),
                account: id,
                token_source: spec.token_source,
                default_retry: spec.default_retry,
                priority: AtomicU8::new(Priority::Foreground as u8),
                bandwidth_cap: AtomicU64::new(BANDWIDTH_CAP_NONE),
            }),
        }
    }

    /// Drop the per-account state.
    ///
    /// Idempotent for the bandwidth meter. **Asymmetric** with
    /// `attach_account` for the rate-limit governor: governor buckets
    /// are keyed by host string and shared across every account on
    /// that host (e.g. five Gmail accounts all share the
    /// `gmail.googleapis.com` bucket). Naively unregistering the host
    /// would yank the bucket out from under other accounts, so the
    /// skeleton leaks host buckets for the life of the process.
    /// Phase 2 may refcount per-host registrations and drop on zero;
    /// for now the asymmetry is by design.
    pub fn detach_account(&self, id: &AccountId) {
        self.inner.meter.forget_account(id);
    }

    /// Process-wide bandwidth meter handle. Per-account readings go
    /// through `AccountNet::meter`.
    #[must_use]
    pub fn meter(&self) -> &BandwidthMeter {
        &self.inner.meter
    }

    /// Per-host rate-limit governor.
    #[must_use]
    pub fn governor(&self) -> &RateLimitGovernor {
        &self.inner.governor
    }

    /// Configuration snapshot.
    #[must_use]
    pub fn config(&self) -> &NetConfig {
        &self.inner.config
    }

    /// Crate-internal accessor for the underlying reqwest client.
    /// Used by `RequestBuilder::send` to dispatch the actual HTTP
    /// request; intentionally not exposed publicly so callers cannot
    /// bypass retry, rate-limit, and metering.
    pub(crate) fn client(&self) -> &reqwest::Client {
        &self.inner.client
    }
}

/// Sentinel encoding `None` on the `bandwidth_cap: AtomicU64` field.
/// `u64::MAX` is unreachable in practice (it implies ~18 EB/s) and
/// distinguishable from any real cap.
pub(crate) const BANDWIDTH_CAP_NONE: u64 = u64::MAX;

/// Account-scoped view onto `Net`. Carries the account identity used
/// for metering, the token source for OAuth, and the rate-limit plus
/// retry defaults the protocol crate supplied at registration.
///
/// `Clone` is one `Arc` refcount bump. Protocol crates typically clone
/// this once per spawned task so each stream owns its own handle.
#[derive(Clone)]
pub struct AccountNet {
    pub(crate) inner: Arc<AccountNetInner>,
}

pub(crate) struct AccountNetInner {
    /// Shared underlying transport.
    pub(crate) net: Net,
    /// Account identity for metering and tracing.
    pub(crate) account: AccountId,
    /// Provider of OAuth bearer tokens for this account.
    pub(crate) token_source: Arc<dyn TokenSource>,
    /// Default retry policy applied to every request unless the
    /// caller overrides via `RequestBuilder::retry`.
    pub(crate) default_retry: RetryPolicy,
    /// Engine-controlled priority hint. Atomic so the hot per-request
    /// read does not take a lock. Stored as the `Priority` enum's
    /// discriminant byte; `priority()` converts back.
    pub(crate) priority: AtomicU8,
    /// Engine-controlled bandwidth cap in bytes per second.
    /// `BANDWIDTH_CAP_NONE` (sentinel `u64::MAX`) means unlimited.
    pub(crate) bandwidth_cap: AtomicU64,
}

impl AccountNet {
    /// Start a `GET` request. Not async because the builder itself is
    /// pure construction; the network round-trip happens in
    /// `RequestBuilder::send`.
    #[must_use]
    pub fn get(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(self.clone(), reqwest::Method::GET, url)
    }

    /// Start a `POST` request.
    #[must_use]
    pub fn post(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(self.clone(), reqwest::Method::POST, url)
    }

    /// Start a `PUT` request.
    #[must_use]
    pub fn put(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(self.clone(), reqwest::Method::PUT, url)
    }

    /// Start a `PATCH` request.
    #[must_use]
    pub fn patch(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(self.clone(), reqwest::Method::PATCH, url)
    }

    /// Start a `DELETE` request.
    #[must_use]
    pub fn delete(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(self.clone(), reqwest::Method::DELETE, url)
    }

    /// Stream a download. Issues a GET with optional `Range` header,
    /// applies retry + rate-limit + bandwidth metering, and wraps the
    /// response body in a `ByteStream` that increments the meter on
    /// every chunk.
    ///
    /// When a `Range` was requested, the response must be `206 Partial
    /// Content` with a `Content-Range` that matches the requested
    /// window. A `200 OK` to a ranged request means the server
    /// collapsed the range and would return the full resource; that
    /// would silently corrupt any caller that asked for a specific
    /// byte window, so we reject it with `Error::RangeNotHonored`
    /// before yielding any chunk. When no `Range` was requested we
    /// accept `200 OK` as before.
    pub async fn download_stream(
        &self,
        url: &str,
        range: Option<ByteRange>,
    ) -> Result<ByteStream, Error> {
        // Build the request: GET with optional Range header, going
        // through the full retry/rate-limit/traceparent stack.
        let want_range = range.map(encode_range);
        let mut builder = self.get(url);
        if let Some(ref r) = want_range {
            builder = builder.header(RANGE.as_str(), r);
        }

        let InternalStreaming {
            status,
            headers,
            body,
            account: _,
        } = send_streaming_inner(builder).await?;

        // Ranged-download safety: if the caller asked for a specific
        // window we must see a `206 Partial Content` with a matching
        // `Content-Range`. Anything else means the server gave us
        // bytes that do not match the requested offset; assembling
        // them into the caller's blob would corrupt it silently.
        if let Some(want) = want_range.as_deref() {
            if status != reqwest::StatusCode::PARTIAL_CONTENT {
                return Err(Error::RangeNotHonored {
                    message: format!(
                        "expected 206 Partial Content for Range request {want}, got {status}"
                    ),
                });
            }
            let Some(got_hv) = headers.get(reqwest::header::CONTENT_RANGE) else {
                return Err(Error::RangeNotHonored {
                    message: format!(
                        "206 response for Range request {want} omitted Content-Range header"
                    ),
                });
            };
            let got = got_hv.to_str().unwrap_or("");
            if !content_range_matches(want, got) {
                return Err(Error::RangeNotHonored {
                    message: format!("content-range mismatch: requested {want}, got {got}"),
                });
            }
        }

        // Wrap the body in a metering + bandwidth-cap adapter.
        let metered = wrap_metered(body, self.clone());
        Ok(metered)
    }

    /// Per-account meter handle.
    #[must_use]
    pub fn meter(&self) -> AccountMeter {
        self.inner
            .net
            .inner
            .meter
            .account(self.inner.account.clone())
    }

    /// Set a per-account bandwidth cap in bytes per second. `None`
    /// disables the cap. Single atomic store; safe to call from any
    /// task without taking a lock.
    pub fn set_bandwidth_cap(&self, bps: Option<u64>) {
        let raw = bps.unwrap_or(BANDWIDTH_CAP_NONE);
        self.inner.bandwidth_cap.store(raw, Ordering::Relaxed);
    }

    /// Current bandwidth cap if any. `None` means unlimited.
    #[must_use]
    pub fn bandwidth_cap(&self) -> Option<u64> {
        let raw = self.inner.bandwidth_cap.load(Ordering::Relaxed);
        if raw == BANDWIDTH_CAP_NONE {
            None
        } else {
            Some(raw)
        }
    }

    /// Set the engine-controlled priority hint. Single atomic store.
    pub fn set_priority(&self, p: Priority) {
        self.inner.priority.store(p as u8, Ordering::Relaxed);
    }

    /// Current priority hint.
    #[must_use]
    pub fn priority(&self) -> Priority {
        match self.inner.priority.load(Ordering::Relaxed) {
            x if x == Priority::Foreground as u8 => Priority::Foreground,
            x if x == Priority::Normal as u8 => Priority::Normal,
            x if x == Priority::Background as u8 => Priority::Background,
            x if x == Priority::Bulk as u8 => Priority::Bulk,
            // Unreachable in practice: only set_priority writes to
            // this atomic and the enum is non-exhaustive only at the
            // public API boundary, not on the wire.
            _ => Priority::Normal,
        }
    }

    /// The account this handle is scoped to.
    #[must_use]
    pub fn account(&self) -> &AccountId {
        &self.inner.account
    }

    /// Default retry policy applied to requests on this account.
    #[must_use]
    pub fn default_retry(&self) -> &RetryPolicy {
        &self.inner.default_retry
    }

    /// Underlying token source. Exposed so the OAuth refresher in
    /// `auth.rs` can share the trait object across requests.
    #[must_use]
    pub fn token_source(&self) -> &Arc<dyn TokenSource> {
        &self.inner.token_source
    }

    /// Underlying shared `Net` handle. Crate-internal: used by the
    /// request pipeline to reach the governor and the reqwest client.
    pub(crate) fn net(&self) -> &Net {
        &self.inner.net
    }
}

/// Caller-supplied spec describing how `Net::attach_account` should
/// register an account: which hosts have rate limits, where bearer
/// tokens come from, and what retry budget to apply by default.
pub struct AccountSpec {
    /// Per-host rate-limit declarations. Empty means "no governor
    /// enforcement for this account", which is the default for JMAP.
    pub hosts: Vec<RateLimit>,
    /// OAuth token provider.
    pub token_source: Arc<dyn TokenSource>,
    /// Default retry policy.
    pub default_retry: RetryPolicy,
}

/// Encode a `ByteRange` as an HTTP `Range` header value.
///
/// `length = None` means "from start to the end of the resource",
/// which is the RFC 9110 `bytes=N-` open-ended form. `length = Some(n)`
/// means `bytes=start-(start+n-1)` inclusive. A zero-length range is
/// degenerate; we emit `bytes=start-start` (a one-byte range), which
/// is the closest valid encoding and avoids the negative-length
/// `bytes=start-(start-1)` form that RFC 9110 would reject. Callers
/// should not request a zero-length range in the first place.
pub(crate) fn encode_range(range: ByteRange) -> String {
    match range.length {
        None => format!("bytes={}-", range.start),
        Some(0) => format!("bytes={}-{}", range.start, range.start),
        Some(n) => {
            let end = range.start.saturating_add(n).saturating_sub(1);
            format!("bytes={}-{}", range.start, end)
        }
    }
}

/// Compare a `Range` request header against the corresponding
/// `Content-Range` response header. Server format is
/// `bytes <first>-<last>/<total>` or `bytes <first>-<last>/*`.
/// Returns `true` if `first` matches the request's first byte and
/// `last` either matches the request's last byte (closed range) or
/// is the resource's penultimate byte (open-ended `bytes=N-`).
fn content_range_matches(req_range: &str, resp_range: &str) -> bool {
    // Parse `bytes=START-END?` from the request side.
    let Some(req_rest) = req_range.strip_prefix("bytes=") else {
        return false;
    };
    let (req_start, req_end) = match req_rest.split_once('-') {
        Some((s, e)) => (s.trim(), e.trim()),
        None => return false,
    };
    let Ok(req_start_n) = req_start.parse::<u64>() else {
        return false;
    };
    let req_end_n: Option<u64> = if req_end.is_empty() {
        None
    } else {
        req_end.parse().ok()
    };

    // Parse `bytes START-END/TOTAL` from the response side.
    let Some(resp_rest) = resp_range.strip_prefix("bytes ") else {
        return false;
    };
    let Some((range_part, _total_part)) = resp_rest.split_once('/') else {
        return false;
    };
    let Some((resp_start, resp_end)) = range_part.split_once('-') else {
        return false;
    };
    let Ok(resp_start_n) = resp_start.trim().parse::<u64>() else {
        return false;
    };
    let Ok(resp_end_n) = resp_end.trim().parse::<u64>() else {
        return false;
    };

    if resp_start_n != req_start_n {
        return false;
    }
    match req_end_n {
        Some(end) => resp_end_n == end,
        // Open-ended request: any end the server picks is acceptable
        // as long as it's >= start.
        None => resp_end_n >= req_start_n,
    }
}

/// Wrap a `ByteStream` with per-chunk metering and an optional
/// bandwidth-cap throttle. The throttle is a byte-counting token
/// bucket refilled at `AccountNet::bandwidth_cap` bytes per second;
/// when the cap is `None`, the wrapper is meter-only. The cap is
/// re-read on every chunk so the engine can hot-swap it mid-stream.
pub(crate) fn wrap_metered(body: ByteStream, account: AccountNet) -> ByteStream {
    let meter = account.meter();
    // Bucket carries its own state across chunks. Constructed full so
    // the first chunk of a download flows immediately, then subsequent
    // chunks pay the cap.
    let bucket = ByteBucket::new(account.bandwidth_cap());

    let stream = body.then(move |chunk| {
        let meter = meter.clone();
        let bucket = bucket.clone();
        let cap_now = account.bandwidth_cap();
        async move {
            let chunk = chunk?;
            let n = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
            meter.record_bytes_in(n);
            if cap_now.is_some() {
                bucket.consume(n, cap_now).await;
            }
            Ok(chunk)
        }
    });
    Box::pin(stream)
}

/// Byte-counting token bucket used by the per-account bandwidth cap.
/// Distinct from the `RateLimitGovernor` which counts request units;
/// the cap counts response bytes.
#[derive(Clone)]
pub(crate) struct ByteBucket {
    state: Arc<std::sync::Mutex<ByteBucketState>>,
}

struct ByteBucketState {
    /// Tokens currently available, in bytes.
    tokens: f64,
    /// Last refill instant.
    last_refill: std::time::Instant,
}

impl ByteBucket {
    /// Construct a full bucket. Initial tokens equal the configured
    /// cap so the first chunk of a brand-new download flows without
    /// delay; subsequent chunks pay the bandwidth-per-second cost.
    fn new(cap: Option<u64>) -> Self {
        let initial = cap.map_or(0.0, |c| c as f64);
        Self {
            state: Arc::new(std::sync::Mutex::new(ByteBucketState {
                tokens: initial,
                last_refill: std::time::Instant::now(),
            })),
        }
    }

    /// Block until `n` bytes can be debited. `cap` is read at call
    /// time so the engine can hot-swap the bandwidth cap mid-stream.
    /// A cap of `Some(0)` is treated as no-cap to avoid divide-by-zero
    /// on the deficit calculation.
    ///
    /// When a single chunk is larger than the per-second cap, the
    /// bucket can never accumulate enough tokens for the steady-state
    /// path (`tokens` is clamped at `cap` on every refill). Looping
    /// would hang the stream. We instead sleep for the proportional
    /// duration the chunk represents at the configured rate
    /// (`chunk_size / cap` seconds) and let the chunk through; the
    /// bucket then resets to the empty state. The cap is a smoothing
    /// throttle, not a hard per-chunk ceiling.
    async fn consume(&self, n: u64, cap: Option<u64>) {
        let Some(cap) = cap else { return };
        if cap == 0 {
            return;
        }
        let cap_f = cap as f64;
        let want = n as f64;
        // Oversized-chunk path: pay the proportional throttle and
        // continue. We zero the bucket so the next chunk pays from
        // scratch rather than benefiting from a stale token count.
        if n > cap {
            let secs = (want / cap_f).min(60.0);
            tokio::time::sleep(Duration::from_secs_f64(secs)).await;
            let mut state = self.state.lock().expect("byte-bucket lock poisoned");
            state.tokens = 0.0;
            state.last_refill = std::time::Instant::now();
            return;
        }
        loop {
            let wait = {
                let mut state = self.state.lock().expect("byte-bucket lock poisoned");
                let now = std::time::Instant::now();
                let elapsed = now.duration_since(state.last_refill).as_secs_f64();
                state.tokens = (state.tokens + elapsed * cap_f).min(cap_f);
                state.last_refill = now;
                if state.tokens >= want {
                    state.tokens -= want;
                    return;
                }
                let deficit = want - state.tokens;
                // Cap the sleep duration so a slow refill cannot
                // park a single chunk indefinitely. The loop will
                // re-evaluate on wakeup.
                let secs = (deficit / cap_f).min(60.0);
                Duration::from_secs_f64(secs)
            };
            tokio::time::sleep(wait).await;
        }
    }
}

/// Public re-export helper so `request.rs` can build a response body
/// adapter without pulling the `Net` types in.
pub(crate) fn into_byte_stream(response: reqwest::Response) -> ByteStream {
    use futures::TryStreamExt;
    let stream = response.bytes_stream().map_err(|e| Error::Network {
        message: format!("response body chunk error: {e}"),
        source: Some(Box::new(e)),
    });
    Box::pin(stream)
}
