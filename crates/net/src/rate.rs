//! Per-host token-bucket rate limiter.
//!
//! Each protocol declares a `RateLimit` per host at client
//! construction. The governor enforces it on every request. `Notify`
//! is used rather than fixed sleeps so a 429 with a long
//! `Retry-After` can refund the unused cost and wake other waiters
//! immediately.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Instant;

use tokio::sync::Notify;

/// A per-host quota declaration. Gmail's 250 units/sec, Graph's
/// per-tenant 10 units/sec, etc. Protocol crates hand a list of
/// these to `Net::attach_account`.
#[derive(Debug, Clone)]
pub struct RateLimit {
    /// Host the limit applies to. Matched against `Url::host_str` for
    /// outbound requests.
    pub host: String,
    /// Refill rate in units per second.
    pub quota_per_second: f64,
    /// Default per-request cost. Callers may override via
    /// `RequestBuilder::cost`.
    pub cost_default: u32,
    /// Burst capacity. Acts as the maximum number of tokens the
    /// bucket can hold.
    pub burst: u32,
}

/// Trait that lets request bodies declare a non-default cost. The
/// `RequestBuilder::cost` fluent setter is the v1 path; this trait is
/// reserved for typed-body integrations layered on later.
pub trait RequestCost {
    /// Cost in quota units. The default of 1 matches Gmail's cheapest
    /// endpoints and Graph's per-request floor.
    fn cost(&self) -> u32 {
        1
    }
}

/// Per-host token-bucket governor. Holds one `HostBucket` per
/// registered host and looks them up by `&str` on every call.
pub struct RateLimitGovernor {
    /// Per-host bucket state. Behind a `Mutex` because the steady
    /// state path takes the lock only to debit tokens or wait, which
    /// is not held across `.await`. The `Notify` lives outside the
    /// `Mutex` so wake calls do not contend.
    buckets: Mutex<HashMap<String, HostBucket>>,
}

/// Internal bucket state for one host. Public only inside the crate
/// so the governor and the bandwidth meter can share refill math
/// later. Fields are written at registration but not read until
/// Phase 2 drives the bucket math.
#[allow(dead_code)]
pub(crate) struct HostBucket {
    /// Current token count. Floating-point so partial refills are
    /// representable.
    pub(crate) tokens: f64,
    /// Burst capacity copied from `RateLimit::burst` at registration.
    pub(crate) burst: f64,
    /// Refill rate copied from `RateLimit::quota_per_second`.
    pub(crate) refill_rate: f64,
    /// Wall-clock instant of the last refill calculation.
    pub(crate) last_refill: Instant,
    /// Notify handle used by `acquire` waiters and the refund path.
    pub(crate) notify: std::sync::Arc<Notify>,
}

impl RateLimitGovernor {
    /// Construct an empty governor. Hosts are registered via
    /// `register`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Register a host with the governor. Idempotent: a second
    /// registration on the same host with the same shape is a no-op;
    /// callers should not rely on this for runtime quota tuning.
    pub fn register(&self, limit: RateLimit) {
        let mut map = self.buckets.lock().expect("rate-governor lock poisoned");
        map.entry(limit.host.clone()).or_insert_with(|| HostBucket {
            tokens: f64::from(limit.burst),
            burst: f64::from(limit.burst),
            refill_rate: limit.quota_per_second,
            last_refill: Instant::now(),
            notify: std::sync::Arc::new(Notify::new()),
        });
    }

    /// Await enough tokens to debit `cost` from the host's bucket.
    /// Returns once the debit succeeds. The returned future is
    /// `Send + 'static` so it composes with the protocol crates'
    /// own erased futures.
    ///
    /// Stub: the v1 skeleton does not yet drive the bucket math.
    /// Phase 2 fills this in.
    pub fn acquire(
        &self,
        _host: &str,
        _cost: u32,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>> {
        Box::pin(async { unimplemented!("RateLimitGovernor::acquire is filled in by Phase 2") })
    }

    /// Refund `cost` tokens to the host's bucket, e.g. when the
    /// server returned 429 with a `Retry-After` and the request did
    /// not actually consume the slot. Wakes one waiter.
    pub fn refund(&self, _host: &str, _cost: u32) {
        unimplemented!("RateLimitGovernor::refund is filled in by Phase 2")
    }
}

impl Default for RateLimitGovernor {
    fn default() -> Self {
        Self::new()
    }
}
