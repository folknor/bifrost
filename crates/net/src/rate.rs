//! Per-host token-bucket rate limiter.
//!
//! Each protocol declares a `RateLimit` per host at client
//! construction. The governor enforces it on every request. `Notify`
//! is used rather than fixed sleeps so a 429 with a long
//! `Retry-After` can refund the unused cost and wake other waiters
//! immediately. Admission is FIFO: each `acquire` takes a ticket in
//! its host's queue and only the head may debit, so a waiter cannot
//! starve behind a stream of later arrivals.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// FIFO admission queue. Only its front entry may debit tokens.
    waiters: VecDeque<RateWaiter>,
    next_waiter_id: u64,
    /// Identity of this bucket *instance*, distinct from the host name.
    /// A host can be unregistered and registered again while a woken
    /// waiter is still on its way back to the lock; without this the
    /// waiter would find a bucket under its host, fail to recognise it
    /// as a different queue, and park on its own `Notify` forever. It
    /// also stops a cancelled waiter's `Drop` from evicting a
    /// same-numbered ticket belonging to the replacement bucket, since
    /// `next_waiter_id` restarts at zero for every new instance.
    generation: u64,
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
                        waiters: VecDeque::new(),
                        next_waiter_id: 0,
                        generation: NEXT_BUCKET_GENERATION.fetch_add(1, Ordering::Relaxed),
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
        // Waking the queue is what makes detach safe: a parked waiter
        // whose bucket just vanished must complete as unmetered rather
        // than sit on a `Notify` nothing will ever fire again.
        if drop_it && let Some(bucket) = map.remove(host) {
            for waiter in bucket.waiters {
                waiter.notify.notify_one();
            }
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
            let (ticket, waiter_notify) = {
                let mut map = buckets.lock().expect("rate-governor lock poisoned");
                let Some(bucket) = map.get_mut(&host) else {
                    return Ok(());
                };
                let ticket = Ticket {
                    generation: bucket.generation,
                    id: bucket.next_waiter_id,
                };
                bucket.next_waiter_id = bucket.next_waiter_id.wrapping_add(1);
                let notify = Arc::new(Notify::new());
                bucket.waiters.push_back(RateWaiter {
                    id: ticket.id,
                    notify: Arc::clone(&notify),
                });
                (ticket, notify)
            };
            let mut guard = WaiterGuard {
                buckets: Arc::clone(&buckets),
                host: host.clone(),
                ticket,
                armed: true,
            };
            loop {
                let is_front = {
                    let map = buckets.lock().expect("rate-governor lock poisoned");
                    // Three ways this ticket can have stopped being
                    // metered, all of which must complete rather than
                    // park: the host was unregistered outright, the
                    // host was unregistered and registered again by
                    // another account (a different bucket instance,
                    // which never held this ticket), or the ticket was
                    // otherwise drained from the queue we joined.
                    // Completing unmetered matches the documented
                    // detach behaviour; parking on a `Notify` nothing
                    // holds any more would strand the request forever.
                    let Some(bucket) = map.get(&host).filter(|bucket| {
                        bucket.generation == ticket.generation
                            && bucket.waiters.iter().any(|waiter| waiter.id == ticket.id)
                    }) else {
                        guard.armed = false;
                        return Ok(());
                    };
                    bucket.waiters.front().map(|waiter| waiter.id) == Some(ticket.id)
                };
                if !is_front {
                    waiter_notify.notified().await;
                    continue;
                }
                // Snapshot the wait duration inside the lock; the
                // actual await happens outside.
                let wait_for = {
                    let mut map = buckets.lock().expect("rate-governor lock poisoned");
                    let Some(bucket) = map
                        .get_mut(&host)
                        .filter(|bucket| bucket.generation == ticket.generation)
                    else {
                        // Host has no declared quota, or the bucket we
                        // queued on was replaced; either way this
                        // request is now unmetered.
                        guard.armed = false;
                        return Ok(());
                    };
                    let now = Instant::now();
                    let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
                    bucket.tokens =
                        (bucket.tokens + elapsed * bucket.refill_rate).min(bucket.burst);
                    bucket.last_refill = now;
                    if bucket.tokens >= cost_f {
                        bucket.tokens -= cost_f;
                        bucket.waiters.pop_front();
                        if let Some(next) = bucket.waiters.front() {
                            next.notify.notify_one();
                        }
                        guard.armed = false;
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
                    Duration::from_secs_f64(wait_capped)
                };
                // Either a refund wakes the FIFO head or its refill
                // timer elapses.
                tokio::select! {
                    _ = waiter_notify.notified() => {}
                    _ = tokio::time::sleep(wait_for) => {}
                }
            }
        })
    }

    /// Refund `cost` tokens to the host's bucket, e.g. when the
    /// server returned 429 with a `Retry-After` and the request did
    /// not actually consume the slot.
    ///
    /// **No thundering herd:** the wake goes to the head of the
    /// admission queue and nobody else. Refunded tokens are offered to
    /// exactly the waiter entitled to them, so a burst of refunds (a
    /// chain of 503s from a host that just came back up) cannot wake N
    /// waiters to race for one slot. A refund that arrives with the
    /// queue empty simply raises the token count for the next arrival.
    pub fn refund(&self, host: &str, cost: u32) {
        let mut map = self.buckets.lock().expect("rate-governor lock poisoned");
        let Some(bucket) = map.get_mut(host) else {
            return;
        };
        bucket.tokens = (bucket.tokens + f64::from(cost)).min(bucket.burst);
        if let Some(waiter) = bucket.waiters.front() {
            waiter.notify.notify_one();
        }
    }
}

/// Monotonic source of `HostBucket::generation`. Process-wide because
/// bucket instances are compared only against tickets minted from the
/// same instance; uniqueness across governors costs nothing and removes
/// any question about governors that share a host name.
static NEXT_BUCKET_GENERATION: AtomicU64 = AtomicU64::new(0);

struct RateWaiter {
    id: u64,
    notify: Arc<Notify>,
}

/// A waiter's place in one specific bucket instance. The `id` alone is
/// ambiguous across an unregister/register cycle, since `next_waiter_id`
/// restarts at zero with the new bucket.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Ticket {
    generation: u64,
    id: u64,
}

struct WaiterGuard {
    buckets: Arc<Mutex<HashMap<String, HostBucket>>>,
    host: String,
    ticket: Ticket,
    armed: bool,
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut map = self.buckets.lock().expect("rate-governor lock poisoned");
        // Only ever mutate the bucket instance this ticket was minted
        // against. A cancelled waiter whose host was re-registered
        // meanwhile would otherwise evict an unrelated waiter that
        // happens to hold the same recycled id, and hand its wake to
        // the wrong task.
        let Some(bucket) = map
            .get_mut(&self.host)
            .filter(|bucket| bucket.generation == self.ticket.generation)
        else {
            return;
        };
        let was_front = bucket.waiters.front().map(|waiter| waiter.id) == Some(self.ticket.id);
        bucket.waiters.retain(|waiter| waiter.id != self.ticket.id);
        if was_front && let Some(next) = bucket.waiters.front() {
            next.notify.notify_one();
        }
    }
}

impl Default for RateLimitGovernor {
    fn default() -> Self {
        Self::new()
    }
}
