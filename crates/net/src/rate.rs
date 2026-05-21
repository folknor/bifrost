//! Per-host token-bucket rate limiter.
//!
//! Each protocol declares a `RateLimit` per host at client
//! construction. The governor enforces it on every request. `Notify`
//! is used rather than fixed sleeps so a 429 with a long
//! `Retry-After` can refund the unused cost and wake other waiters
//! immediately.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::error::Error;

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
    /// Per-host bucket state. Behind `Arc<Mutex<_>>` rather than
    /// plain `Mutex<_>` because `acquire(...)` returns a
    /// `Pin<Box<dyn Future + Send + 'static>>` that must capture
    /// the bucket map by ownership (it cannot borrow from `&self`,
    /// which would require a non-`'static` lifetime). The wait/notify
    /// loop inside the future re-acquires the lock after each park;
    /// the `Arc` clone is what makes that legal. Steady-state path
    /// takes the lock only to debit tokens, which is not held across
    /// `.await`. The `Notify` lives outside the `Mutex` so wake
    /// calls do not contend.
    buckets: Arc<Mutex<HashMap<String, HostBucket>>>,
}

/// Internal bucket state for one host. Public only inside the crate
/// so the governor and the bandwidth meter can share refill math
/// later.
pub(crate) struct HostBucket {
    /// Current token count. Floating-point so partial refills are
    /// representable.
    pub(crate) tokens: f64,
    /// Burst capacity copied from `RateLimit::burst` at registration.
    pub(crate) burst: f64,
    /// Burst capacity as the original `u32`. Kept alongside `burst`
    /// so the cost-exceeds-burst error path can surface the exact
    /// integer the caller registered, without an `f64 -> u32` cast.
    pub(crate) burst_max: u32,
    /// Refill rate copied from `RateLimit::quota_per_second`.
    pub(crate) refill_rate: f64,
    /// Wall-clock instant of the last refill calculation.
    pub(crate) last_refill: Instant,
    /// Notify handle used by `acquire` waiters and the refund path.
    pub(crate) notify: Arc<Notify>,
}

impl RateLimitGovernor {
    /// Construct an empty governor. Hosts are registered via
    /// `register`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            buckets: Arc::new(Mutex::new(HashMap::new())),
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
            burst_max: limit.burst,
            refill_rate: limit.quota_per_second,
            last_refill: Instant::now(),
            notify: Arc::new(Notify::new()),
        });
    }

    /// Await enough tokens to debit `cost` from the host's bucket.
    /// Returns once the debit succeeds.
    ///
    /// The returned future is `Send + 'static` so it composes with
    /// the protocol crates' own erased futures. If the host is not
    /// registered the future returns immediately - hosts with no
    /// declared quota are unmetered.
    ///
    /// # Errors
    /// Returns `Error::CostExceedsBurst` if `cost` is larger than the
    /// host's burst capacity. The bucket can never accumulate that
    /// many tokens (the refill clamp caps at `burst`), so the future
    /// would otherwise spin forever. This is a caller configuration
    /// bug rather than a transient condition; we surface it instead
    /// of hanging.
    pub fn acquire(
        &self,
        host: &str,
        cost: u32,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'static>> {
        let buckets = Arc::clone(&self.buckets);
        let host = host.to_owned();
        let cost_f = f64::from(cost);
        Box::pin(async move {
            // Up-front burst check. We re-read the bucket configuration
            // under the lock so a config change between registration
            // and acquisition is visible. The detection is done before
            // the wait loop so a misconfigured cost cannot park
            // anything.
            {
                let map = buckets.lock().expect("rate-governor lock poisoned");
                if let Some(bucket) = map.get(&host)
                    && cost_f > bucket.burst
                {
                    // `burst` originated as `RateLimit::burst: u32`
                    // and is preserved exactly in `bucket.burst_max`,
                    // so we round-trip through the integer field to
                    // avoid `f64 -> u32` truncation lints.
                    return Err(Error::CostExceedsBurst {
                        cost,
                        burst: bucket.burst_max,
                    });
                }
            }
            loop {
                // Snapshot the notify handle and wait duration inside
                // the lock; the actual await happens outside.
                let (notify, wait_for) = {
                    let mut map = buckets.lock().expect("rate-governor lock poisoned");
                    let Some(bucket) = map.get_mut(&host) else {
                        // Host has no declared quota; no-op.
                        return Ok(());
                    };
                    let now = Instant::now();
                    let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
                    bucket.tokens =
                        (bucket.tokens + elapsed * bucket.refill_rate).min(bucket.burst);
                    bucket.last_refill = now;
                    if bucket.tokens >= cost_f {
                        bucket.tokens -= cost_f;
                        return Ok(());
                    }
                    // Not enough tokens. Compute the minimum wait until
                    // the bucket can satisfy the request, capped at
                    // 250 ms so the loop also picks up refunds via
                    // `Notify` wakes promptly even if our refill math
                    // is off. A zero refill rate would compute to
                    // infinity; the 250 ms cap collapses that to a
                    // bounded poll interval.
                    let deficit = cost_f - bucket.tokens;
                    let wait_secs = if bucket.refill_rate > 0.0 {
                        deficit / bucket.refill_rate
                    } else {
                        f64::INFINITY
                    };
                    let wait_capped = wait_secs.clamp(0.001, 0.25);
                    let dur = Duration::from_secs_f64(wait_capped);
                    (Arc::clone(&bucket.notify), dur)
                };
                // Either the notify fires (refund / quota-tier change)
                // or the timer elapses (we recompute the refill).
                tokio::select! {
                    _ = notify.notified() => {}
                    _ = tokio::time::sleep(wait_for) => {}
                }
            }
        })
    }

    /// Refund `cost` tokens to the host's bucket, e.g. when the
    /// server returned 429 with a `Retry-After` and the request did
    /// not actually consume the slot. Wakes one waiter.
    pub fn refund(&self, host: &str, cost: u32) {
        let mut map = self.buckets.lock().expect("rate-governor lock poisoned");
        let Some(bucket) = map.get_mut(host) else {
            return;
        };
        bucket.tokens = (bucket.tokens + f64::from(cost)).min(bucket.burst);
        bucket.notify.notify_one();
    }
}

impl Default for RateLimitGovernor {
    fn default() -> Self {
        Self::new()
    }
}
