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

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn quota_scopes_on_one_host_have_independent_buckets_and_generations() {
    let governor = RateLimitGovernor::new();
    governor.register(RateLimit::new("api.test", 0.001, 1, 1).with_quota_scope("tenant-a"));
    governor.register(RateLimit::new("api.test", 0.001, 1, 1).with_quota_scope("tenant-b"));
    let generation_a = governor
        .acquire_generation_scoped("api.test", "tenant-a", 1)
        .await
        .unwrap()
        .unwrap();
    governor
        .acquire_generation_scoped("api.test", "tenant-b", 1)
        .await
        .unwrap()
        .unwrap();

    governor.refund_scoped("api.test", "tenant-b", 1, generation_a);
    let mut blocked = Box::pin(governor.acquire_generation_scoped("api.test", "tenant-b", 1));
    assert!(
        tokio::time::timeout(Duration::ZERO, &mut blocked)
            .await
            .is_err(),
        "a generation from another scope refunded tenant-b"
    );
}

/// Cancelling the head must hand the front to its successor, and the
/// successor's own precedence must survive the handoff. The costs are
/// deliberately uneven: with a queue, the expensive B blocks the cheap C
/// until B is funded, so an out-of-order admission is observable. With
/// waiters merely racing for tokens, C takes the first refunded unit and
/// the assertion fires. Uniform costs made this test pass against a
/// governor with no queue at all.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_cancelled_head_hands_the_front_over_without_letting_the_tail_overtake() {
    let governor = std::sync::Arc::new(RateLimitGovernor::new());
    governor.register(RateLimit::new("fifo.test", 0.001, 1, 3));
    let generation = governor
        .acquire_generation("fifo.test", 3)
        .await
        .unwrap()
        .unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut tasks = Vec::new();
    for (id, cost) in [(0, 1), (1, 3), (2, 1)] {
        let governor = std::sync::Arc::clone(&governor);
        let tx = tx.clone();
        tasks.push(tokio::spawn(async move {
            governor.acquire("fifo.test", cost).await.unwrap();
            tx.send(id).unwrap();
        }));
        tokio::task::yield_now().await;
    }

    // Cancel the head. B (cost 3) inherits the front; C (cost 1) must
    // not be admitted by the refunds that are funding B.
    tasks.remove(0).abort();
    tokio::task::yield_now().await;

    for _ in 0..2 {
        governor.refund("fifo.test", 1, generation);
        tokio::task::yield_now().await;
        assert!(
            rx.try_recv().is_err(),
            "a partially funded head must not let its successor overtake"
        );
    }
    governor.refund("fifo.test", 1, generation);
    assert_eq!(
        rx.recv().await,
        Some(1),
        "the cancelled head handed the front to B"
    );
    governor.refund("fifo.test", 1, generation);
    assert_eq!(rx.recv().await, Some(2));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_cheaper_later_waiter_cannot_overtake_the_fifo_head() {
    let governor = std::sync::Arc::new(RateLimitGovernor::new());
    governor.register(RateLimit::new("cost-fifo.test", 0.001, 1, 2));
    let generation = governor
        .acquire_generation("cost-fifo.test", 2)
        .await
        .unwrap()
        .unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    for (id, cost) in [(0, 2), (1, 1)] {
        let governor = std::sync::Arc::clone(&governor);
        let tx = tx.clone();
        tokio::spawn(async move {
            governor.acquire("cost-fifo.test", cost).await.unwrap();
            tx.send(id).unwrap();
        });
        tokio::task::yield_now().await;
    }

    governor.refund("cost-fifo.test", 1, generation);
    tokio::task::yield_now().await;
    assert!(rx.try_recv().is_err(), "the one-unit follower overtook");
    governor.refund("cost-fifo.test", 1, generation);
    assert_eq!(rx.recv().await, Some(0));
    governor.refund("cost-fifo.test", 1, generation);
    assert_eq!(rx.recv().await, Some(1));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn unregister_releases_every_queued_waiter() {
    let governor = std::sync::Arc::new(RateLimitGovernor::new());
    governor.register(RateLimit::new("detach.test", 0.001, 1, 1));
    governor.acquire("detach.test", 1).await.unwrap();

    let mut tasks = Vec::new();
    for _ in 0..2 {
        let governor = std::sync::Arc::clone(&governor);
        tasks.push(tokio::spawn(async move {
            governor.acquire("detach.test", 1).await
        }));
        tokio::task::yield_now().await;
    }

    governor.unregister("detach.test");
    for task in tasks {
        task.await.unwrap().unwrap();
    }
}

/// The final `unregister` wakes queued waiters, but a woken waiter does
/// not resume instantly. If another account registers the SAME host in
/// that window, the waiter finds a bucket under its host again. Matching
/// only on the host name it concluded it was still queued and parked on
/// a `Notify` that the new bucket has no handle to - stranded forever.
/// Bucket generation is what distinguishes "still my queue" from "a
/// different queue that reused my host name".
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_waiter_woken_by_unregister_is_not_recaptured_by_a_re_registered_host() {
    let governor = std::sync::Arc::new(RateLimitGovernor::new());
    let limit = || RateLimit::new("churn.test", 0.001, 1, 1);
    governor.register(limit());
    governor.acquire("churn.test", 1).await.unwrap();

    let queued = {
        let governor = std::sync::Arc::clone(&governor);
        tokio::spawn(async move { governor.acquire("churn.test", 1).await })
    };
    tokio::task::yield_now().await;

    // Detach wakes the waiter; the replacement lands before it is
    // scheduled again. The new bucket is drained, so a waiter that
    // mistakes it for its own queue can never reach the front.
    governor.unregister("churn.test");
    governor.register(limit());
    governor.acquire("churn.test", 1).await.unwrap();

    tokio::time::timeout(Duration::from_secs(30), queued)
        .await
        .expect("a waiter released by unregister must not be recaptured by the new bucket")
        .unwrap()
        .unwrap();
}

/// T3: register a host at 10 units/sec, burst 5; debit 5 immediately
/// (drains the bucket), debit 1 (must wait); refund 1 (must wake the
/// waiter). Asserts the timing intent rather than exact wall time -
/// we measure against the tokio test runtime so refunds are
/// deterministic.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn token_bucket_drains_refills_and_refunds_wake_waiter() {
    let governor = std::sync::Arc::new(RateLimitGovernor::new());
    governor.register(RateLimit::new("rate.test", 10.0, 1, 5));

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
    let generation = governor
        .acquire_generation("rate.test", 0)
        .await
        .unwrap()
        .unwrap();
    governor.refund("rate.test", 1, generation);

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

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn token_bucket_refills_on_tokio_virtual_time() {
    let governor = RateLimitGovernor::new();
    governor.register(limit("virtual.test", 1.0, 1));
    governor
        .acquire("virtual.test", 1)
        .await
        .expect("initial token");
    let waiting = governor.acquire("virtual.test", 1);
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::ZERO, &mut waiting)
            .await
            .is_err()
    );

    tokio::time::advance(Duration::from_secs(1)).await;
    waiting.await.expect("virtual second refills the bucket");
}

/// T4: cost > burst returns `Error::CostExceedsBurst` immediately,
/// without parking. The integers in the variant are the caller's
/// original `u32` values, not the floating-point bucket size.
#[tokio::test(flavor = "current_thread")]
async fn acquire_cost_exceeds_burst_short_circuits() {
    let governor = RateLimitGovernor::new();
    governor.register(RateLimit::new("burst.test", 1.0, 1, 3));

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
    governor.register(RateLimit::new("default.test", 1.0, 7, 10));
    assert_eq!(governor.cost_default_for("default.test"), Some(7));
    assert_eq!(governor.cost_default_for("unknown.test"), None);
}

fn limit(host: &str, quota_per_second: f64, burst: u32) -> RateLimit {
    RateLimit::new(host, quota_per_second, 1, burst)
}

/// A host nobody registered is unmetered: `acquire` returns
/// immediately whatever the cost. JMAP relies on this - it registers no
/// rate limits at all.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn unregistered_hosts_are_unmetered() {
    let governor = RateLimitGovernor::new();
    governor
        .acquire("nobody.test", 10_000)
        .await
        .expect("an unregistered host never blocks");
    assert_eq!(governor.cost_default_for("nobody.test"), None);
}

/// `cost > burst` errors, but `cost == burst` is legal - a request that
/// exactly drains a full bucket must not be misread as a configuration
/// bug.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn cost_exactly_equal_to_burst_is_admitted() {
    let governor = RateLimitGovernor::new();
    governor.register(limit("edge.test", 10.0, 4));
    governor
        .acquire("edge.test", 4)
        .await
        .expect("draining the whole bucket in one debit is legal");
    let err = governor
        .acquire("edge.test", 5)
        .await
        .expect_err("one unit past burst is a configuration bug");
    assert!(matches!(err, Error::CostExceedsBurst { cost: 5, burst: 4 }));
}

/// A zero-cost request bypasses the bucket entirely: `tokens >= 0.0`
/// holds even on a drained bucket. Callers that want a request metered
/// must not pass `.cost(0)`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn zero_cost_requests_bypass_the_bucket() {
    let governor = RateLimitGovernor::new();
    governor.register(limit("zero.test", 1.0, 1));
    governor.acquire("zero.test", 1).await.expect("drain");
    tokio::time::timeout(Duration::from_millis(50), governor.acquire("zero.test", 0))
        .await
        .expect("a zero-cost acquire must not park on an empty bucket")
        .expect("zero cost is never over burst");
}

/// `refund` clamps at `burst`, so an over-refund (or a refund racing a
/// natural refill) cannot inflate the bucket beyond its configured
/// ceiling.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn refund_clamps_at_burst() {
    let governor = RateLimitGovernor::new();
    governor.register(limit("clamp.test", 1.0, 1));
    let generation = governor
        .acquire_generation("clamp.test", 1)
        .await
        .expect("drain")
        .unwrap();

    governor.refund("clamp.test", 1_000, generation);
    governor
        .acquire("clamp.test", 1)
        .await
        .expect("the refund restored the single token");

    let blocked =
        tokio::time::timeout(Duration::from_millis(50), governor.acquire("clamp.test", 1)).await;
    assert!(
        blocked.is_err(),
        "a 1000-unit refund into a burst-1 bucket must leave exactly one token",
    );
}

/// Per-host attach counts are what let five Gmail accounts share one
/// host bucket. The bucket survives every `unregister` but the last.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn attach_counts_keep_a_shared_bucket_alive() {
    let governor = RateLimitGovernor::new();
    governor.register(limit("shared.test", 5.0, 5));
    governor.register(limit("shared.test", 5.0, 5));
    governor.register(limit("shared.test", 5.0, 5));

    governor.unregister("shared.test");
    governor.unregister("shared.test");
    assert_eq!(
        governor.cost_default_for("shared.test"),
        Some(1),
        "two of three accounts detached; the bucket must survive"
    );

    governor.unregister("shared.test");
    assert_eq!(
        governor.cost_default_for("shared.test"),
        None,
        "the last detach reclaims the bucket"
    );

    // Over-unregistering an already-dropped host is a no-op, not a
    // panic or an underflow.
    governor.unregister("shared.test");
    assert_eq!(governor.cost_default_for("shared.test"), None);
}

/// First registration wins. A second registration disagreeing on quota
/// warns and is ignored, so one misconfigured consumer cannot poison a
/// host bucket other accounts share.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn duplicate_registration_keeps_the_first_configuration() {
    let governor = RateLimitGovernor::new();
    governor.register(RateLimit::new("dup.test", 250.0, 5, 250));
    governor.register(RateLimit::new("dup.test", 1.0, 99, 1));

    assert_eq!(governor.cost_default_for("dup.test"), Some(5));
    governor
        .acquire("dup.test", 250)
        .await
        .expect("the first registration's burst of 250 is still in force");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn invalid_quotas_are_ignored_and_leave_the_host_unmetered() {
    let governor = RateLimitGovernor::new();
    for (host, quota) in [
        ("zero.test", 0.0),
        ("negative.test", -1.0),
        ("nan.test", f64::NAN),
        ("infinite.test", f64::INFINITY),
    ] {
        governor.register(limit(host, quota, 1));
        assert_eq!(governor.cost_default_for(host), None);
        governor
            .acquire(host, u32::MAX)
            .await
            .expect("a host with an invalid registration is unmetered");
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn unusable_bursts_are_rejected_and_leave_the_host_unmetered() {
    let governor = RateLimitGovernor::new();
    for limit in [
        RateLimit::new("zero-burst.test", 1.0, 1, 0),
        RateLimit::new("default-over-burst.test", 1.0, 2, 1),
    ] {
        let host = limit.host.clone();
        assert!(!governor.register(limit));
        assert_eq!(governor.cost_default_for(&host), None);
        governor
            .acquire(&host, u32::MAX)
            .await
            .expect("a rejected declaration leaves the host unmetered");
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn invalid_duplicate_still_balances_the_existing_attach_count() {
    let governor = RateLimitGovernor::new();
    governor.register(limit("shared.test", 10.0, 10));
    governor.register(limit("shared.test", f64::NAN, 1));

    governor.unregister("shared.test");
    assert_eq!(
        governor.cost_default_for("shared.test"),
        Some(1),
        "the first detach must leave the first account's bucket registered"
    );
    governor.unregister("shared.test");
    assert_eq!(governor.cost_default_for("shared.test"), None);
}
