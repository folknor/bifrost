//! Unit tests for `RateLimitGovernor`.
//!
//! T3: token-bucket math - debit drains, refill recovers,
//! `refund` wakes a parked waiter.
//!
//! T4: cost-exceeds-burst short-circuits to
//! `Error::CostExceedsBurst` with the exact integers the caller
//! registered.

use std::time::Duration;

use bifrost_net::error::Error;
use bifrost_net::rate::{RateLimit, RateLimitGovernor};

/// T3: register a host at 10 units/sec, burst 5; debit 5 immediately
/// (drains the bucket), debit 1 (must wait); refund 1 (must wake the
/// waiter). Asserts the timing intent rather than exact wall time -
/// we measure against the tokio test runtime so refunds are
/// deterministic.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn token_bucket_drains_refills_and_refunds_wake_waiter() {
    let governor = std::sync::Arc::new(RateLimitGovernor::new());
    governor.register(RateLimit {
        host: "rate.test".to_owned(),
        quota_per_second: 10.0,
        cost_default: 1,
        burst: 5,
    });

    // Drain: 5 single-token acquires should each return immediately.
    for _ in 0..5 {
        governor
            .acquire("rate.test", 1)
            .await
            .expect("draining acquire");
    }

    // Now a sixth acquire must wait. Spawn it, then refund 1 to wake
    // the waiter. Without the refund the acquire would have to wait
    // ~100ms for natural refill (10/sec means 100ms per token).
    let governor_clone = std::sync::Arc::clone(&governor);
    let waiter = tokio::spawn(async move { governor_clone.acquire("rate.test", 1).await });

    // Yield so the waiter has time to park on `Notify`.
    tokio::task::yield_now().await;

    // Refund: explicit token return, should wake the waiter.
    governor.refund("rate.test", 1);

    // The waiter should complete promptly. We give it generous
    // virtual time so the test does not race on busy CI.
    let result = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("waiter did not complete after refund")
        .expect("waiter panicked");
    assert!(
        result.is_ok(),
        "refund-driven acquire should succeed, got {result:?}",
    );
}

/// T4: cost > burst returns `Error::CostExceedsBurst` immediately,
/// without parking. The integers in the variant are the caller's
/// original `u32` values, not the floating-point bucket size.
#[tokio::test(flavor = "current_thread")]
async fn acquire_cost_exceeds_burst_short_circuits() {
    let governor = RateLimitGovernor::new();
    governor.register(RateLimit {
        host: "burst.test".to_owned(),
        quota_per_second: 1.0,
        cost_default: 1,
        burst: 3,
    });

    let res = governor.acquire("burst.test", 10).await;
    match res {
        Err(Error::CostExceedsBurst { cost, burst }) => {
            assert_eq!(cost, 10);
            assert_eq!(burst, 3);
        }
        other => panic!("expected CostExceedsBurst, got {other:?}"),
    }
}

/// Sanity check: `cost_default_for` returns the registered default
/// and `None` for unregistered hosts. Covers the N7 surface used by
/// `RequestBuilder::send_streaming_inner`.
#[tokio::test]
async fn cost_default_for_returns_registered_default() {
    let governor = RateLimitGovernor::new();
    governor.register(RateLimit {
        host: "default.test".to_owned(),
        quota_per_second: 1.0,
        cost_default: 7,
        burst: 10,
    });
    assert_eq!(governor.cost_default_for("default.test"), Some(7));
    assert_eq!(governor.cost_default_for("unknown.test"), None);
}
