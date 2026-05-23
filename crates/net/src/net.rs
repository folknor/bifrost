//! The top-level `Net` handle and the per-account `AccountNet` view.
//!
//! Protocol crates hold an `AccountNet` and never see the underlying
//! `reqwest::Client`. `Net` itself is process-wide; one instance is
//! shared across every account so connection pooling, rate limiting,
//! and bandwidth metering can coordinate across accounts that share a
//! host.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use reqwest::header::RANGE;

use crate::auth::{OAuthRefresher, TokenSource};
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
    /// Hosts each registered account asked the governor to track.
    /// Indexed by `AccountId` and populated at `attach_account`;
    /// `detach_account` decrements the governor's per-host attach
    /// count using this list so unused buckets drop to zero and the
    /// map does not grow without bound across attach/detach cycles.
    pub(crate) account_hosts: Mutex<HashMap<AccountId, Vec<String>>>,
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

        // bifrost-net owns the redirect loop unconditionally. Reqwest's
        // default policy follows 3xx but without RFC 7231 method
        // rewriting or a trusted-host allowlist; our loop in
        // `redirect.rs` plus `request.rs::send_streaming_inner` enforces
        // both. Installing `Policy::none()` here is the only handoff
        // point - everything else is bifrost-net code.
        builder = builder.redirect(reqwest::redirect::Policy::none());

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
                account_hosts: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Shared default transport for simple constructors. Applications
    /// with explicit transport ownership can still call `Net::new`
    /// and pass account handles into protocol clients directly.
    #[must_use]
    pub fn shared_default() -> Self {
        static DEFAULT: OnceLock<Net> = OnceLock::new();
        DEFAULT
            .get_or_init(|| Net::new(NetConfig::default()).expect("default NetConfig should build"))
            .clone()
    }

    /// Register an account with the transport. Returns an
    /// `AccountNet` carrying the per-account token source, rate
    /// limits, and default retry policy.
    pub fn attach_account(&self, id: AccountId, spec: AccountSpec) -> AccountNet {
        let previous_hosts = {
            let mut map = self
                .inner
                .account_hosts
                .lock()
                .expect("net account_hosts lock poisoned");
            map.remove(&id).unwrap_or_default()
        };
        if !previous_hosts.is_empty() {
            tracing::warn!(
                target: "bifrost_net::rate",
                account = ?id,
                "attach_account called for an already-attached account; replacing previous host registrations",
            );
        }
        for host in previous_hosts {
            self.inner.governor.unregister(&host);
        }

        // Register the meter so the per-account counters exist before
        // any request lands. Rate-limit registration walks the host
        // list the caller supplied.
        self.inner.meter.register_account(id.clone());
        let mut registered_hosts = Vec::with_capacity(spec.hosts.len());
        for limit in &spec.hosts {
            registered_hosts.push(limit.host.clone());
            self.inner.governor.register(limit.clone());
        }
        // Remember which hosts this account registered so
        // `detach_account` can unregister symmetrically. Multiple
        // attach calls for the same account would overwrite an
        // earlier entry; callers should not attach twice without
        // detaching first.
        {
            let mut map = self
                .inner
                .account_hosts
                .lock()
                .expect("net account_hosts lock poisoned");
            map.insert(id.clone(), registered_hosts);
        }
        AccountNet {
            inner: Arc::new(AccountNetInner {
                net: self.clone(),
                account: id,
                token_source: Arc::new(
                    OAuthRefresher::new(spec.token_source)
                        .with_max_age(self.inner.config.token_max_age),
                ),
                default_retry: spec.default_retry,
                priority: AtomicU8::new(Priority::Foreground as u8),
                bandwidth_cap: AtomicU64::new(BANDWIDTH_CAP_NONE),
            }),
        }
    }

    /// Drop the per-account state.
    ///
    /// Symmetric with `attach_account` for both the bandwidth meter
    /// and the rate-limit governor. Each `attach_account` increments
    /// the governor's per-host attach count for every host the
    /// caller registered; `detach_account` decrements those same
    /// counts, dropping the host bucket only when no other account
    /// still depends on it. Five Gmail accounts sharing the
    /// `gmail.googleapis.com` host all bump the same count to 5;
    /// detaching one drops the count to 4 and the bucket survives.
    /// Detaching the last account drops the count to 0 and the
    /// bucket is reclaimed.
    pub fn detach_account(&self, id: &AccountId) {
        self.inner.meter.forget_account(id);
        let hosts = {
            let mut map = self
                .inner
                .account_hosts
                .lock()
                .expect("net account_hosts lock poisoned");
            map.remove(id).unwrap_or_default()
        };
        for host in hosts {
            self.inner.governor.unregister(&host);
        }
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
    ///
    /// A `range` with `length = Some(0)` is degenerate: the caller
    /// asked for zero bytes. Rather than emit `bytes=N-N` (one byte)
    /// or `bytes=N-(N-1)` (RFC-invalid), we short-circuit before
    /// touching the network and return an empty `ByteStream`. The
    /// bandwidth meter and rate-limit governor are not touched for a
    /// zero-byte read.
    pub async fn download_stream(
        &self,
        url: &str,
        range: Option<ByteRange>,
    ) -> Result<ByteStream, Error> {
        // Short-circuit zero-length: no network round-trip, empty
        // stream. Avoids the degenerate `bytes=N-N` encoding (one
        // byte) and the RFC-9110-invalid `bytes=N-(N-1)` form.
        if matches!(
            range,
            Some(ByteRange {
                length: Some(0),
                ..
            })
        ) {
            let empty = futures::stream::empty::<Result<bytes::Bytes, Error>>();
            return Ok(Box::pin(empty));
        }
        // Build the request: GET with optional Range header, going
        // through the full retry/rate-limit/traceparent stack.
        let want_range = match range {
            Some(r) => Some(encode_range(r)?),
            None => None,
        };
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

    /// Set a per-account bandwidth cap in bytes per second.
    ///
    /// `None` disables the cap. `Some(0)` is **not** a sentinel for
    /// "unlimited" (that's `None`'s job) and not a sentinel for
    /// "block everything" (the throttle is a smoothing layer; we
    /// never park a stream forever on a caller mistake). It would
    /// otherwise collide with the previous behaviour where the
    /// `ByteBucket::consume` early-return treated 0 as no-cap, which
    /// silently undid the cap. We normalise to `Some(1)` (one byte
    /// per second, a trickle) and emit a `tracing::warn!` so a
    /// caller misconfiguration is visible without crashing the
    /// stream.
    ///
    /// Single atomic store after the normalisation; safe to call
    /// from any task without taking a lock.
    pub fn set_bandwidth_cap(&self, bps: Option<u64>) {
        let raw = match bps {
            None => BANDWIDTH_CAP_NONE,
            Some(0) => {
                tracing::warn!(
                    target: "bifrost_net::bandwidth",
                    account = ?self.inner.account,
                    "set_bandwidth_cap(Some(0)) clamped to 1 B/s; use None for unlimited",
                );
                1
            }
            Some(n) => n,
        };
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

    /// Re-mint this `AccountNet` under a new engine `AccountId`.
    ///
    /// The returned handle shares the same parent `Net`, token
    /// source, default retry policy, priority hint, and bandwidth
    /// cap. Used by protocol-crate factories that need to honor
    /// `AccountFactory::open(account_id)` when the caller built the
    /// `AccountNet` via `Net::attach_account` ahead of time
    /// (`*Client::with_account_net`) and the engine-minted id arrives
    /// only at open time. Idempotent: `retag(self.account())` is a
    /// cheap rebuild that re-registers the same id on the meter.
    ///
    /// Bookkeeping:
    ///
    /// - The bandwidth meter registers the new id so per-account
    ///   counters exist before any request lands.
    /// - The per-account host registrations in `Net` move from the
    ///   old id to the new id so `Net::detach_account(new_id)`
    ///   unregisters host buckets symmetrically when the engine
    ///   eventually drops the account. Governor refcounts are
    ///   per-host and unchanged by the rename.
    /// - The old `AccountNet` clone the caller holds keeps working
    ///   for in-flight requests but no longer owns the host
    ///   registrations; dropping it and calling `detach_account` on
    ///   the old id is a no-op afterwards. The new handle is the
    ///   one engine code should propagate forward.
    #[must_use]
    pub fn retag(&self, new_id: AccountId) -> AccountNet {
        let net_inner = &self.inner.net.inner;
        net_inner.meter.register_account(new_id.clone());
        if new_id != self.inner.account {
            let mut map = net_inner
                .account_hosts
                .lock()
                .expect("net account_hosts lock poisoned");
            if let Some(hosts) = map.remove(&self.inner.account) {
                map.insert(new_id.clone(), hosts);
            }
        }
        AccountNet {
            inner: Arc::new(AccountNetInner {
                net: self.inner.net.clone(),
                account: new_id,
                token_source: Arc::clone(&self.inner.token_source),
                default_retry: self.inner.default_retry.clone(),
                priority: AtomicU8::new(self.inner.priority.load(Ordering::Relaxed)),
                bandwidth_cap: AtomicU64::new(self.inner.bandwidth_cap.load(Ordering::Relaxed)),
            }),
        }
    }
}

/// Caller-supplied spec describing how `Net::attach_account` should
/// register an account: which hosts have rate limits, where bearer
/// tokens come from, and what retry budget to apply by default.
pub struct AccountSpec {
    /// Per-host rate-limit declarations. Empty means "no governor
    /// enforcement for this account", which is the default for JMAP.
    pub hosts: Vec<RateLimit>,
    /// Raw OAuth token provider. `Net::attach_account` wraps this in
    /// an `OAuthRefresher` using `NetConfig::token_max_age`, so callers
    /// should pass the provider itself rather than pre-wrapping it in
    /// another refresher.
    pub token_source: Arc<dyn TokenSource>,
    /// Default retry policy.
    pub default_retry: RetryPolicy,
}

/// Encode a `ByteRange` as an HTTP `Range` header value.
///
/// `length = None` means "from start to the end of the resource",
/// which is the RFC 9110 `bytes=N-` open-ended form. `length = Some(n)`
/// means `bytes=start-(start+n-1)` inclusive.
///
/// A `length = Some(0)` request is rejected upstream by
/// `AccountNet::download_stream`; the encoder treats `Some(0)` as a
/// programmer error and returns `Error::RangeNotHonored` (the closest
/// existing variant; no separate `RangeInvalid` because callers map
/// both to the same recovery class).
///
/// `start + length` is checked for `u64` overflow. Previously the
/// saturating math would silently emit `bytes=start-u64::MAX`, which
/// servers either reject or fulfil with the full tail of the resource
/// (silent semantics drift). We surface
/// `Error::RangeNotHonored { message: "range overflow" }` instead so
/// callers see the configuration bug immediately.
// `Error` is intentionally wide (Bytes + HeaderMap), so a Result-wrapping
// constructor trips `result_large_err`. The alternative (boxing) would
// force a `Box<Error>` through every caller's error chain; ranges are an
// uncommon path and the wide Err is a workspace-consistent tradeoff.
#[allow(clippy::result_large_err)]
pub(crate) fn encode_range(range: ByteRange) -> Result<String, Error> {
    match range.length {
        None => Ok(format!("bytes={}-", range.start)),
        Some(0) => Err(Error::RangeNotHonored {
            message: "zero-length range".to_owned(),
        }),
        Some(n) => {
            let Some(end_plus_one) = range.start.checked_add(n) else {
                return Err(Error::RangeNotHonored {
                    message: format!(
                        "range overflow: start {} + length {n} exceeds u64::MAX",
                        range.start,
                    ),
                });
            };
            // `end_plus_one >= 1` because `n > 0` here.
            let end = end_plus_one - 1;
            Ok(format!("bytes={}-{}", range.start, end))
        }
    }
}

/// Compare a `Range` request header against the corresponding
/// `Content-Range` response header. Server format is
/// `bytes <first>-<last>/<total>` or `bytes <first>-<last>/*`.
/// Returns `true` if:
///
/// - Closed request (`bytes=N-M`): the response's first/last match
///   the request's first/last exactly.
/// - Open-ended request (`bytes=N-`): the response's first matches
///   the request's first AND, when the total is known
///   (`/<total>`), the response's last equals `total - 1` (i.e.
///   covers bytes N through the end of the resource). When the
///   server returns `/*` (total unknown) we accept any last >= N
///   because we cannot verify the tail.
///
/// The previous open-ended check accepted any response end >= start,
/// which permitted truncated bodies (e.g. server returns `bytes
/// 0-9/100` to `bytes=0-` and the caller assembled a 10-byte slice
/// believing it had the full resource).
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
    let Some((range_part, total_part)) = resp_rest.split_once('/') else {
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

    if resp_start_n != req_start_n || resp_end_n < resp_start_n {
        return false;
    }
    let total_trim = total_part.trim();
    let total_known: Option<u64> = if total_trim == "*" {
        None
    } else {
        match total_trim.parse::<u64>() {
            Ok(n) => Some(n),
            Err(_) => return false,
        }
    };
    if let Some(total) = total_known
        && resp_end_n >= total
    {
        return false;
    }
    match req_end_n {
        Some(end) => resp_end_n == end,
        // Open-ended request: must cover the full tail of the
        // resource when the total is known. With total unknown
        // (`*`), we cannot verify and accept any end >= start.
        None => match total_known {
            Some(total) => total > 0 && resp_end_n == total - 1,
            None => resp_end_n >= req_start_n,
        },
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
    /// A cap of `Some(0)` cannot occur here because
    /// `AccountNet::set_bandwidth_cap` normalises `Some(0)` to
    /// `Some(1)` to avoid sentinel collision with `None` semantics;
    /// the early return guards against a hand-rolled call site that
    /// stores 0 anyway.
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
        // continue. We zero the bucket and reset `last_refill` to
        // `now`. Side effect: the *next* normal-sized chunk starts
        // from an empty bucket and has to wait one refill cycle even
        // if real time has moved on - it will not benefit from the
        // elapsed-since-last-refill credit. This is intentional: an
        // oversized chunk already paid the proportional throttle, so
        // crediting the elapsed time again would double-spend the
        // throttle window. Callers that stream a steady mix of large
        // and small chunks may see the small chunks throttled a hair
        // more than the average rate suggests; the cap is a smoothing
        // throttle, not a precision shaper.
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

#[cfg(test)]
mod tests {
    use super::content_range_matches;
    use super::{AccountId, AccountSpec, Net};
    use crate::StaticTokenSource;
    use crate::config::NetConfig;
    use crate::rate::RateLimit;
    use crate::retry::RetryPolicy;
    use std::sync::Arc;

    #[test]
    fn content_range_rejects_inverted_response_range() {
        assert!(!content_range_matches("bytes=100-", "bytes 100-99/100"));
    }

    #[test]
    fn content_range_rejects_end_at_or_after_total() {
        assert!(!content_range_matches("bytes=0-99", "bytes 0-99/99"));
        assert!(!content_range_matches("bytes=0-", "bytes 0-100/100"));
    }

    #[test]
    fn content_range_accepts_valid_open_ended_tail() {
        assert!(content_range_matches("bytes=10-", "bytes 10-99/100"));
    }

    fn build_net() -> Net {
        Net::new(NetConfig::default()).expect("default NetConfig should build")
    }

    fn build_spec(host: &str) -> AccountSpec {
        AccountSpec {
            hosts: vec![RateLimit {
                host: host.to_string(),
                quota_per_second: 1.0,
                cost_default: 1,
                burst: 1,
            }],
            token_source: Arc::new(StaticTokenSource::new("test-token", None)),
            default_retry: RetryPolicy::default(),
        }
    }

    fn account_hosts_snapshot(net: &Net, id: &AccountId) -> Option<Vec<String>> {
        let map = net
            .inner
            .account_hosts
            .lock()
            .expect("net account_hosts lock poisoned");
        map.get(id).cloned()
    }

    #[test]
    fn retag_moves_host_registrations_and_registers_meter() {
        let net = build_net();
        let old = AccountId("old".to_string());
        let new = AccountId("new".to_string());
        let original = net.attach_account(old.clone(), build_spec("retag.example"));
        assert_eq!(original.account(), &old);
        let hosts_before = account_hosts_snapshot(&net, &old).expect("old id has hosts");
        assert_eq!(hosts_before, vec!["retag.example".to_string()]);

        let retagged = original.retag(new.clone());

        assert_eq!(retagged.account(), &new);
        assert!(
            account_hosts_snapshot(&net, &old).is_none(),
            "old id should no longer hold host registrations after retag",
        );
        let hosts_after = account_hosts_snapshot(&net, &new).expect("new id has hosts");
        assert_eq!(
            hosts_after, hosts_before,
            "retag moves the same host list under the new id",
        );
        // Meter has counters under the new id: `account` builds an
        // AccountMeter that reads the registered AccountCounters, so a
        // never-recorded counter returns 0 bps without panicking.
        assert_eq!(net.meter().account(new).observed_bps(), 0);
    }

    #[test]
    fn retag_with_same_id_is_idempotent() {
        let net = build_net();
        let id = AccountId("stable".to_string());
        let original = net.attach_account(id.clone(), build_spec("retag.example"));
        let _retagged = original.retag(id.clone());
        let hosts = account_hosts_snapshot(&net, &id).expect("id still has hosts");
        assert_eq!(
            hosts,
            vec!["retag.example".to_string()],
            "retag with same id must not drop the host registration",
        );
    }
}
