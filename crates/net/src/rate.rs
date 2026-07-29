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
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

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
#[derive(Clone)]
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
    /// Per-host default cost copied from `RateLimit::cost_default` at
    /// registration. `RequestBuilder::send_streaming_inner` looks this
    /// up when the builder did not call `.cost()` so each host can
    /// have a sensible default (Gmail's cheap reads are 5; Graph's
    /// query endpoints are 10) without forcing every call site to
    /// remember.
    pub(crate) cost_default: u32,
    /// Wall-clock instant of the last refill calculation.
    pub(crate) last_refill: Instant,
    /// Notify handle used by `acquire` waiters and the refund path.
    pub(crate) notify: Arc<Notify>,
    /// Per-host attach count. Each `register` increments; each
    /// `unregister` decrements; the bucket is dropped when the count
    /// reaches zero. Lets `Net::detach_account` shed unused host
    /// buckets symmetrically without yanking the bucket out from
    /// under accounts that still depend on it.
    pub(crate) attach_count: u32,
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

    /// Register a host with the governor.
    ///
    /// Duplicate-host policy: the first registration wins. A second
    /// registration that disagrees on `quota_per_second`, `burst`, or
    /// `cost_default` is **not** silently dropped: we emit a
    /// `tracing::warn!` so the caller sees the conflict, but leave the
    /// installed bucket in place. The alternative (most-restrictive
    /// merge) would let any consumer poison shared host quotas via a
    /// misconfiguration; sticking with the first registration keeps
    /// the rule deterministic.
    ///
    /// Returns true when this registration joined a bucket and must
    /// later be balanced by `unregister`. Returns false when a new
    /// host declaration was rejected as invalid.
    pub fn register(&self, limit: RateLimit) -> bool {
        let mut map = self.buckets.lock().expect("rate-governor lock poisoned");
        match map.get_mut(&limit.host) {
            Some(existing) => {
                #[allow(clippy::float_cmp)]
                let conflict = existing.refill_rate != limit.quota_per_second
                    || existing.burst_max != limit.burst
                    || existing.cost_default != limit.cost_default;
                if conflict {
                    tracing::warn!(
                        target: "bifrost_net::rate",
                        host = %limit.host,
                        existing_quota_per_second = existing.refill_rate,
                        existing_burst = existing.burst_max,
                        existing_cost_default = existing.cost_default,
                        new_quota_per_second = limit.quota_per_second,
                        new_burst = limit.burst,
                        new_cost_default = limit.cost_default,
                        "duplicate RateLimit registration with different quota; keeping first registration",
                    );
                }
                existing.attach_count = existing.attach_count.saturating_add(1);
                true
            }
            None => {
                if !limit.quota_per_second.is_finite() || limit.quota_per_second <= 0.0 {
                    tracing::warn!(
                        target: "bifrost_net::rate",
                        host = %limit.host,
                        quota_per_second = limit.quota_per_second,
                        "ignoring RateLimit registration with a non-finite or non-positive quota",
                    );
                    return false;
                }
                map.insert(
                    limit.host.clone(),
                    HostBucket {
                        tokens: f64::from(limit.burst),
                        burst: f64::from(limit.burst),
                        burst_max: limit.burst,
                        refill_rate: limit.quota_per_second,
                        cost_default: limit.cost_default,
                        last_refill: Instant::now(),
                        notify: Arc::new(Notify::new()),
                        attach_count: 1,
                    },
                );
                true
            }
        }
    }

    /// Decrement the attach count for a host; drop the bucket when
    /// the count reaches zero. Called from `Net::detach_account` to
    /// keep the governor's map from growing without bound across
    /// account attach/detach cycles.
    pub fn unregister(&self, host: &str) {
        let mut map = self.buckets.lock().expect("rate-governor lock poisoned");
        let drop_it = match map.get_mut(host) {
            Some(bucket) => {
                bucket.attach_count = bucket.attach_count.saturating_sub(1);
                bucket.attach_count == 0
            }
            None => false,
        };
        if drop_it {
            map.remove(host);
        }
    }

    /// Look up the host's registered `cost_default`. Returns `None`
    /// if the host has no registration. Used by
    /// `RequestBuilder::send_streaming_inner` when the builder did
    /// not set a per-request cost via `.cost(n)`.
    #[must_use]
    pub fn cost_default_for(&self, host: &str) -> Option<u32> {
        let map = self.buckets.lock().expect("rate-governor lock poisoned");
        map.get(host).map(|b| b.cost_default)
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
                    // is off. Registration rejects non-finite and
                    // non-positive refill rates, so this calculation
                    // is always finite and positive.
                    let deficit = cost_f - bucket.tokens;
                    let wait_secs = deficit / bucket.refill_rate;
                    let wait_capped = wait_secs.clamp(0.005, 0.25);
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
    /// not actually consume the slot.
    ///
    /// **Thundering-herd note:** waiters are tokio `Notify`
    /// listeners. We call `notify_one()` so a single refund wakes
    /// only one waiter, which is the right semantics for token-bucket
    /// fairness: refunding `cost = 1` should not wake N waiters who
    /// each consume the same slot. However, a burst of refunds (e.g.
    /// a chain of 503s from a host that just came back up) will wake
    /// one waiter per refund in quick succession, and they will all
    /// re-enter `acquire` and race to debit the bucket. This is
    /// acceptable: refilled tokens are still scarce on the
    /// just-recovered host, so racing acquirers self-throttle. If a
    /// caller adds higher-volume refund paths (currently only the
    /// retry loop refunds), reconsider.
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
