//! Unit tests for `OAuthRefresher` single-flight behaviour.
//!
//! T1: N concurrent `token()` calls produce exactly one underlying
//! `refresh()` invocation, even with N tasks racing.
//!
//! T2: when the underlying `refresh()` returns an error, every
//! waiter receives the SAME `Arc<Error>` (verified via `Arc::ptr_eq`
//! on the inner `RefreshFailed::source`).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bifrost_net::auth::{AccessToken, OAuthRefresher, TokenSource};
use bifrost_net::error::Error;
use bifrost_types::AccountFuture;
use bifrost_types::TransmissionState;

/// `TokenSource` that counts `refresh()` invocations and sleeps for
/// a small fake duration to widen the race window. `current()` falls
/// through to `refresh()` to keep the test focused on the
/// single-flight path - the refresher's outer state machine drives
/// `refresh()` either way.
struct CountingSource {
    refresh_calls: AtomicUsize,
    delay: Duration,
    fail: bool,
}

impl CountingSource {
    fn new(delay: Duration, fail: bool) -> Arc<Self> {
        Arc::new(Self {
            refresh_calls: AtomicUsize::new(0),
            delay,
            fail,
        })
    }
}

impl TokenSource for CountingSource {
    fn current(&self) -> AccountFuture<Result<AccessToken, Error>> {
        // Test source: no cache, every `current` drives a refresh.
        // Real sources cache; the refresher then short-circuits.
        self.refresh()
    }

    fn refresh(&self) -> AccountFuture<Result<AccessToken, Error>> {
        let delay = self.delay;
        let fail = self.fail;
        // Bump the counter on entry so the count reflects "refresh
        // started" rather than "refresh completed". Single-flight
        // means only one task should ever observe this increment per
        // refresh.
        self.refresh_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            if fail {
                // Use `Network` to exercise the non-AuthLost
                // classification path so `arc_err_to_error` wraps it
                // in `RefreshFailed` and we can compare `Arc::ptr_eq`
                // on the wrapped source across waiters.
                Err(Error::Network {
                    message: "fake transient failure".to_owned(),
                    transmission_state: TransmissionState::Unsent,
                    source: None,
                })
            } else {
                Ok(AccessToken::new(format!("tok-{fail}"), None))
            }
        })
    }
}

/// T1: eight concurrent `token()` calls collapse to one `refresh()`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_flight_collapses_concurrent_refreshes() {
    let source = CountingSource::new(Duration::from_millis(50), false);
    let refresher = Arc::new(OAuthRefresher::new(
        Arc::clone(&source) as Arc<dyn TokenSource>
    ));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let r = Arc::clone(&refresher);
        handles.push(tokio::spawn(async move { r.token().await }));
    }
    for h in handles {
        let result = h.await.expect("task panicked");
        assert!(result.is_ok(), "single-flight token() should succeed");
    }
    let calls = source.refresh_calls.load(Ordering::SeqCst);
    assert_eq!(
        calls, 1,
        "expected exactly one underlying refresh, got {calls}"
    );
}

/// T2: on failure, every waiter receives the SAME shared `Arc<Error>`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_flight_failure_shares_one_arc_across_waiters() {
    let source = CountingSource::new(Duration::from_millis(50), true);
    let refresher = Arc::new(OAuthRefresher::new(source as Arc<dyn TokenSource>));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let r = Arc::clone(&refresher);
        handles.push(tokio::spawn(async move { r.token().await }));
    }

    // Collect the `Arc<Error>` from each `RefreshFailed`. They should
    // all point at the same underlying allocation.
    let mut shared_arcs: Vec<Arc<Error>> = Vec::new();
    for h in handles {
        let res = h.await.expect("task panicked");
        match res {
            Err(Error::RefreshFailed { source, .. }) => shared_arcs.push(source),
            Err(other) => panic!("expected RefreshFailed, got {other:?}"),
            Ok(_) => panic!("expected failure"),
        }
    }
    let first = shared_arcs.first().expect("at least one waiter");
    for (idx, other) in shared_arcs.iter().enumerate().skip(1) {
        assert!(
            Arc::ptr_eq(first, other),
            "waiter {idx} got a different Arc<Error> than waiter 0",
        );
    }
}
