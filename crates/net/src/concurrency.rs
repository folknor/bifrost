//! Bound on simultaneously in-flight requests.
//!
//! Nothing else in the transport bounds concurrency: the rate-limit governor
//! meters request RATE, and a fan-out call site in a protocol crate caps only
//! its own `buffer_unordered` width, with no view of what else the same account
//! has in flight. Those per-site caps stay; this is the shared bound they
//! cannot provide. A server-advertised limit - JMAP's
//! `maxConcurrentRequests` is the standing case - is a property of the
//! account's whole traffic, so it needs one shared count.
//!
//! [`ConcurrencyLimit`] is that count. It is a cloneable handle, so the scope is
//! the caller's: one handle per account bounds an account, one handle shared
//! across accounts bounds a process, and a handle attached only to some
//! requests bounds only those.

use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A shareable bound on how many requests may be in flight at once.
///
/// A request gated by a limit holds one slot from admission until its
/// response body ends: across its retries, its redirect hops and the body
/// drain. Holding the slot through a retry sleep is deliberate - it can only
/// keep the count BELOW the server's view of open requests, never above it.
/// A streaming response holds its slot until the body stream ends, fails or
/// is dropped, so a caller that keeps a streamed body open while it awaits
/// another request on the same limit can deadlock once the limit is reached.
///
/// Admission is FIFO (tokio's semaphore is fair) and the wait is bounded by
/// the request's total deadline, expiring as `Timeout { Unsent }`.
///
/// [`Self::set_limit`] resizes the bound while requests are in flight.
/// Growing admits waiters at once. Shrinking never revokes a slot already
/// held: the excess is retired as in-flight requests finish, so the count
/// converges to the new bound rather than cutting work short.
#[derive(Clone)]
pub struct ConcurrencyLimit {
    inner: Arc<Inner>,
}

struct Inner {
    semaphore: Arc<Semaphore>,
    state: Mutex<State>,
}

struct State {
    limit: usize,
    /// Slots owed by a shrink that found too few idle permits to forget.
    /// Each finishing request pays one instead of returning its permit.
    debt: usize,
}

impl ConcurrencyLimit {
    /// A bound of `limit` concurrent requests. Zero would admit nothing and
    /// park every gated request until its deadline, so it is raised to one
    /// with a warning; a limit above tokio's permit ceiling is lowered to it.
    #[must_use]
    pub fn new(limit: usize) -> Self {
        let limit = normalize(limit);
        Self {
            inner: Arc::new(Inner {
                semaphore: Arc::new(Semaphore::new(limit)),
                state: Mutex::new(State { limit, debt: 0 }),
            }),
        }
    }

    /// The current bound.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.state().limit
    }

    /// Requests currently holding a slot.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        let state = self.state();
        // Every permit is either idle in the semaphore or held, and the held
        // count exceeds the bound by exactly the unpaid debt.
        (state.limit + state.debt).saturating_sub(self.inner.semaphore.available_permits())
    }

    /// Resize the bound. See the type docs for how a shrink treats slots
    /// already held.
    pub fn set_limit(&self, limit: usize) {
        let limit = normalize(limit);
        let mut state = self.state();
        if limit > state.limit {
            let grow = limit - state.limit;
            let paid = grow.min(state.debt);
            state.debt -= paid;
            self.inner.semaphore.add_permits(grow - paid);
        } else if limit < state.limit {
            let shrink = state.limit - limit;
            let forgotten = self.inner.semaphore.forget_permits(shrink);
            state.debt += shrink - forgotten;
        }
        state.limit = limit;
    }

    /// Wait for a slot. Cancel-safe: dropping the future gives up its place
    /// in the queue and holds nothing.
    pub(crate) async fn acquire(&self) -> ConcurrencyPermit {
        loop {
            let permit = Arc::clone(&self.inner.semaphore)
                .acquire_owned()
                .await
                .expect("the semaphore is owned here and never closed");
            // Debt is paid here as well as on release. A permit tokio granted
            // to a queued waiter that was then cancelled before it was polled
            // goes back to the semaphore without passing through
            // `ConcurrencyPermit::drop`, so release alone could let a shrink's
            // excess be handed out again. Any permit reaching an acquirer
            // while debt is owed is retired instead.
            let mut state = self.state();
            if state.debt > 0 {
                state.debt -= 1;
                permit.forget();
                continue;
            }
            drop(state);
            return ConcurrencyPermit {
                permit: Some(permit),
                limit: self.clone(),
            };
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl std::fmt::Debug for ConcurrencyLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrencyLimit")
            .field("limit", &self.limit())
            .field("in_flight", &self.in_flight())
            .finish()
    }
}

fn normalize(limit: usize) -> usize {
    if limit == 0 {
        tracing::warn!(
            target: "bifrost_net::concurrency",
            "concurrency limit of 0 raised to 1; a zero bound would admit nothing"
        );
        return 1;
    }
    limit.min(Semaphore::MAX_PERMITS)
}

/// One held slot. Releasing it pays down any shrink debt first, so a resized
/// limit converges without revoking work in flight.
pub(crate) struct ConcurrencyPermit {
    permit: Option<OwnedSemaphorePermit>,
    limit: ConcurrencyLimit,
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        let mut state = self.limit.state();
        if state.debt > 0 {
            state.debt -= 1;
            permit.forget();
        } else {
            // Returned under the state lock, so a concurrent `set_limit`
            // cannot observe the permit as neither held nor idle.
            drop(permit);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Poll once and report whether the future is still waiting. A queued
    /// semaphore waiter keeps its place; the later `.await` re-polls it with
    /// a real waker.
    fn pending<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> bool {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        future.poll(&mut cx).is_pending()
    }

    /// The bound holds: with every slot taken, the next request waits until
    /// one is released, and then it is admitted.
    #[tokio::test]
    async fn a_full_limit_admits_the_next_request_only_after_a_release() {
        let limit = ConcurrencyLimit::new(2);
        let first = limit.acquire().await;
        let _second = limit.acquire().await;
        assert_eq!(limit.in_flight(), 2);

        let mut third = Box::pin(limit.acquire());
        assert!(
            pending(third.as_mut()),
            "a third request must wait while two hold the slots"
        );
        drop(first);
        let _third = third.await;
        assert_eq!(limit.in_flight(), 2);
    }

    /// Admission is first come, first served. The waiters are distinguishable
    /// by arrival only, so an implementation that let a later waiter overtake
    /// an earlier one would admit the wrong one here.
    #[tokio::test]
    async fn waiters_are_admitted_in_arrival_order() {
        let limit = ConcurrencyLimit::new(1);
        let held = limit.acquire().await;
        let mut early = Box::pin(limit.acquire());
        let mut late = Box::pin(limit.acquire());
        assert!(pending(early.as_mut()));
        assert!(pending(late.as_mut()));

        drop(held);
        assert!(
            pending(late.as_mut()),
            "the later waiter must not overtake the earlier one"
        );
        let early = early.await;
        drop(early);
        let _late = late.await;
    }

    /// A shrink below the in-flight count revokes nothing: the held slots
    /// stay held, no new request is admitted until enough have finished, and
    /// the count then settles at the new bound.
    #[tokio::test]
    async fn a_shrink_retires_excess_slots_as_requests_finish() {
        let limit = ConcurrencyLimit::new(3);
        let a = limit.acquire().await;
        let b = limit.acquire().await;
        let c = limit.acquire().await;
        limit.set_limit(1);
        assert_eq!(limit.limit(), 1);
        assert_eq!(limit.in_flight(), 3, "a shrink revokes no held slot");

        drop(a);
        drop(b);
        assert_eq!(limit.in_flight(), 1);
        let mut next = Box::pin(limit.acquire());
        assert!(
            pending(next.as_mut()),
            "two releases only paid the shrink; the bound of one is still taken"
        );
        drop(c);
        let _next = next.await;
        assert_eq!(limit.in_flight(), 1);
    }

    /// A grow while a shrink is still owed pays the debt before adding
    /// slots, so the bound is the latest one asked for, not their sum.
    #[tokio::test]
    async fn a_grow_pays_an_outstanding_shrink_before_adding_slots() {
        let limit = ConcurrencyLimit::new(2);
        let a = limit.acquire().await;
        let b = limit.acquire().await;
        limit.set_limit(1);
        limit.set_limit(2);
        let mut next = Box::pin(limit.acquire());
        assert!(
            pending(next.as_mut()),
            "back at a bound of two with two held, nothing is free"
        );
        drop(a);
        let _next = next.await;
        drop(b);
        assert_eq!(limit.in_flight(), 1);
        assert_eq!(limit.limit(), 2);
    }

    /// A permit can return to the semaphore without passing through a
    /// `ConcurrencyPermit`: tokio grants a freed permit to the queued waiter,
    /// and if that waiter is cancelled before it is polled the permit goes
    /// back to the semaphore directly. Debt owed by a shrink must still
    /// absorb it, or a new request is admitted over the new bound.
    #[tokio::test]
    async fn a_cancelled_granted_waiter_cannot_launder_a_shrink() {
        let limit = ConcurrencyLimit::new(2);
        let held = limit.acquire().await;
        let freed = limit.acquire().await;
        let mut waiter = Box::pin(limit.acquire());
        assert!(pending(waiter.as_mut()), "both slots are taken");

        // The freed permit is granted to the queued waiter, unpolled.
        drop(freed);
        limit.set_limit(1);
        // Cancelled after the grant and before any poll: the permit returns
        // to the semaphore without touching the debt.
        drop(waiter);

        let mut intruder = Box::pin(limit.acquire());
        assert!(
            pending(intruder.as_mut()),
            "one request already holds the bound of one"
        );
        drop(held);
        let _admitted = intruder.await;
        assert_eq!(limit.in_flight(), 1);
    }

    #[test]
    fn a_zero_limit_is_raised_to_one() {
        let limit = ConcurrencyLimit::new(0);
        assert_eq!(limit.limit(), 1);
        limit.set_limit(0);
        assert_eq!(limit.limit(), 1);
    }
}

/// The limit wired through the real request pipeline, against the scripted
/// wire double.
#[cfg(test)]
mod pipeline_tests {
    use std::time::Duration;

    use bytes::Bytes;
    use futures::StreamExt;
    use reqwest::StatusCode;
    use reqwest::header::HeaderMap;

    use super::ConcurrencyLimit;
    use crate::config::NetConfig;
    use crate::error::Error;
    use crate::net::{AccountNet, AccountSpec};
    use crate::test_support::{Canned, ScriptedDispatch, canned, scripted_net};
    use crate::{AccountId, TransmissionState};

    fn account(
        script: &std::sync::Arc<ScriptedDispatch>,
        limit: Option<ConcurrencyLimit>,
    ) -> AccountNet {
        let mut spec = AccountSpec::new(None);
        spec.concurrency_limit = limit;
        scripted_net(script, NetConfig::default())
            .attach_account(AccountId("concurrency".to_owned()), spec)
    }

    fn pending<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> bool {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        future.poll(&mut cx).is_pending()
    }

    /// An account-level limit bounds a fan-out wider than itself. The
    /// dispatcher yields before answering, so without the limit all six legs
    /// would be on the wire together; the peak must be the limit exactly,
    /// which also shows the limit admits concurrency rather than serializing.
    #[tokio::test]
    async fn an_account_limit_bounds_a_wider_fan_out() {
        let script = ScriptedDispatch::yielding((0..6).map(|_| canned(StatusCode::OK, b"ok")));
        let account = account(&script, Some(ConcurrencyLimit::new(2)));
        let legs = (0..6).map(|n| {
            account
                .get(&format!("https://limit.test/{n}"))
                .without_bearer_auth()
                .send()
        });
        for result in futures::future::join_all(legs).await {
            result.expect("every gated leg completes");
        }
        assert_eq!(script.peak_in_flight(), 2);
    }

    /// A streamed body holds its slot until it ends: a second request on the
    /// same limit waits while the first body is unread, and is admitted once
    /// the body has been drained.
    #[tokio::test]
    async fn a_streaming_body_holds_its_slot_until_it_ends() {
        let script = ScriptedDispatch::new([
            Canned::Stream {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                chunks: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            },
            canned(StatusCode::OK, b"second"),
        ]);
        let account = account(&script, None);
        let limit = ConcurrencyLimit::new(1);
        let mut first = account
            .get("https://limit.test/stream")
            .without_bearer_auth()
            .concurrency_limit(limit.clone())
            .send_streaming()
            .await
            .expect("the stream opens");
        assert_eq!(limit.in_flight(), 1);

        let mut second = Box::pin(
            account
                .get("https://limit.test/second")
                .without_bearer_auth()
                .concurrency_limit(limit.clone())
                .send(),
        );
        assert!(
            pending(second.as_mut()),
            "the second request must wait while the first body is open"
        );
        while let Some(chunk) = first.body.next().await {
            chunk.expect("chunk");
        }
        // `first` is still alive here: the second request is admitted because
        // the drained body released its slot at its end, not on drop. The
        // release hands the slot straight to the queued waiter.
        let response = second.await.expect("admitted once the body ended");
        assert_eq!(response.body, Bytes::from_static(b"second"));
        drop(first);
        assert_eq!(limit.in_flight(), 0);
    }

    /// The slot wait is bounded by the request's deadline, and an expiry is
    /// `Unsent`: nothing reached the wire, so any method may be replayed.
    #[tokio::test(start_paused = true)]
    async fn a_slot_wait_expires_as_an_unsent_timeout() {
        let script = ScriptedDispatch::new([Canned::StreamThenStall {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            chunks: vec![Bytes::from_static(b"held")],
        }]);
        let limit = ConcurrencyLimit::new(1);
        let account = account(&script, Some(limit.clone()));
        let _held = account
            .get("https://limit.test/held")
            .without_bearer_auth()
            .send_streaming()
            .await
            .expect("the first request holds the only slot");

        let Err(error) = account
            .post("https://limit.test/waits")
            .without_bearer_auth()
            .timeout(Duration::from_millis(50))
            .send()
            .await
        else {
            panic!("the wait must expire");
        };
        assert!(
            matches!(
                error,
                Error::Timeout {
                    transmission_state: TransmissionState::Unsent
                }
            ),
            "expected an unsent timeout, got {error:?}"
        );
        assert_eq!(
            script.requests().len(),
            1,
            "the waiter never reached the wire"
        );
    }
}
