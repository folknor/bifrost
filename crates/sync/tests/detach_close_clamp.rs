//! `detach` bounds `Account::close()`.
//!
//! Every other step of `detach_inner` is clamped to `EngineConfig::detach_timeout`:
//! the worker awaits, the teardown discard drain, and both of its writer sends. The
//! close was not, so a provider whose connection teardown never returned hung
//! `detach` itself: the one call a consumer has for getting rid of an account,
//! and the one it makes on the way out of the process.
//!
//! Both tests run current-thread under `start_paused`, so the elapsed readings
//! below are virtual and exact rather than wall-clock guesses; there are no real
//! sleeps here.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bifrost_sync::{CheckpointStore, EngineConfig, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::{AccountFactory, AccountId, CursorScope};

async fn attach(account_id: &AccountId, stub: Arc<common::StubAccount>) -> SyncEngine {
    let engine = SyncEngine::builder()
        .config(EngineConfig::default())
        .checkpoints(Arc::new(InMemoryCheckpointStore::default()) as Arc<dyn CheckpointStore>)
        .build()
        .expect("engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(common::StubFactory::queue(vec![stub]));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");
    engine
}

/// A `close()` that never answers costs `detach` its timeout, and no more.
///
/// The gate is never notified, so the provider's close is hung for good. Without
/// the clamp `detach` never returns and the outer timeout below is what fires -
/// which is the ablation: remove the `tokio::time::timeout` around `close()` in
/// `detach_inner` and this test fails on "the detach is clamped, not hung".
///
/// The elapsed assertion is what separates this from the refusal case in the
/// next test: a clamped hang spends the whole budget, a refusal spends none of
/// it. Both end with the same warn-and-drop treatment, so the outcome alone
/// cannot tell them apart.
#[tokio::test(start_paused = true)]
async fn a_close_that_never_returns_does_not_hang_detach() {
    let account_id = AccountId("detach-close-hangs".to_owned());
    let mut stub = common::StubAccount::new(vec![CursorScope::Account]);
    stub.close_gate = Some(Arc::new(tokio::sync::Notify::new()));
    let stub = Arc::new(stub);
    let closed = Arc::clone(&stub.closed);
    let engine = attach(&account_id, Arc::clone(&stub)).await;

    let started = tokio::time::Instant::now();
    tokio::time::timeout(
        EngineConfig::default().detach_timeout * 10,
        engine.detach(&account_id),
    )
    .await
    .expect("the detach is clamped, not hung")
    .expect("a close that hangs is not reported to the caller, exactly as a close that fails");
    let elapsed = started.elapsed();

    assert_eq!(
        closed.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the close was reached; it simply never answered"
    );
    assert_eq!(
        stub.close_returned
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "and it never answered: the future was dropped at the deadline"
    );
    assert!(
        elapsed >= EngineConfig::default().detach_timeout,
        "a hung close must be given its whole budget before the handle is abandoned; \
         waited {elapsed:?}"
    );
    // The handle is dropped rather than retained, and nothing can reach it: the
    // slot left `engine.accounts` at the top of the teardown.
    assert!(
        engine.account_changes_stream(&account_id).is_err(),
        "the account is detached whatever its close did"
    );
}

/// A `close()` that REFUSES is absorbed with no wait at all.
///
/// The counterpart reading. The two take different paths to the same outcome -
/// warn, drop the handle, report success to the caller - and this is what pins
/// that the clamp did not turn every close into a timeout: a provider that
/// answers promptly, even with an error, spends none of the budget.
#[tokio::test(start_paused = true)]
async fn a_close_that_fails_is_absorbed_without_spending_the_budget() {
    let account_id = AccountId("detach-close-fails".to_owned());
    let mut stub = common::StubAccount::new(vec![CursorScope::Account]);
    stub.close_fails = true;
    let stub = Arc::new(stub);
    let closed = Arc::clone(&stub.closed);
    let engine = attach(&account_id, Arc::clone(&stub)).await;

    let started = tokio::time::Instant::now();
    engine
        .detach(&account_id)
        .await
        .expect("a failed close is logged, not returned");
    let elapsed = started.elapsed();

    assert_eq!(closed.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        stub.close_returned
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the refusal is an answer: the future ran to completion"
    );
    assert!(
        elapsed < EngineConfig::default().detach_timeout,
        "a close that answers immediately must not be made to wait; waited {elapsed:?}"
    );
    assert!(engine.account_changes_stream(&account_id).is_err());
}

/// The clamp is a FRESH budget, not the remainder of the worker deadline.
///
/// A straggler worker that burns the whole `detach_timeout` must not take the
/// close budget with it: a perfectly healthy close needing one round trip would
/// then be reported as hung and its connection abandoned on a path where nothing
/// was wrong. The straggler here is the deferred-inventory worker, parked in a
/// provider inventory stream that never ends - one of the workers `detach`
/// actually joins, and one whose stream poll has no shutdown arm, so it is
/// aborted only at the deadline. The close, which answers only after a delay of
/// its own, still completes.
///
/// A parked CHANGES stream does NOT work for this, which was established by
/// measurement: the multiplexer owns its per-scope poll tasks and aborts them
/// itself, so they are not among the workers `detach` waits on, and a first
/// version of this test staged that way detached in 11ms. The elapsed assertion
/// below is what caught that, and is why it stays.
///
/// Ablation: switch the clamp to `timeout_at(deadline, ...)` on the shared
/// worker deadline and the close is cut off before it answers - `closed` still
/// reads 1, since that counter is bumped on entry, and `close_returned`, which
/// is bumped only past the gate, stays at 0.
#[tokio::test(start_paused = true)]
async fn a_slow_close_after_a_straggler_worker_still_completes() {
    let account_id = AccountId("detach-close-after-straggler".to_owned());
    let mut stub = common::StubAccount::new(vec![
        CursorScope::Account,
        CursorScope::Type(bifrost_types::ObjectType::Contact),
    ]);
    stub.establishment = |scope| match scope {
        CursorScope::Type(_) => bifrost_types::CursorEstablishment::EstablishViaInventory,
        other => {
            bifrost_types::CursorEstablishment::Ready(common::cursor_for(other, b"stub-ready"))
        }
    };
    // Never notified: the deferred-inventory worker cannot exit, so the worker
    // deadline is spent in full and the close begins with none of it left.
    stub.inventory_stall = Some(Arc::new(tokio::sync::Notify::new()));
    let gate = Arc::new(tokio::sync::Notify::new());
    stub.close_gate = Some(Arc::clone(&gate));
    let stub = Arc::new(stub);
    let closed = Arc::clone(&stub.closed);
    let engine = attach(&account_id, Arc::clone(&stub)).await;
    // The deferred worker waits for a real subscriber before it walks anything,
    // so the straggler does not exist until somebody subscribes.
    let _subscriber = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    tokio::time::timeout(Duration::from_secs(120), async {
        while stub
            .inventory_calls
            .load(std::sync::atomic::Ordering::SeqCst)
            == 0
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the deferred inventory worker enters its walk");

    // Release the close a beat after it is entered, so it is genuinely slow and
    // genuinely finite. `notify_waiters` only reaches a waiter already parked,
    // which the poll below establishes.
    let releasing = tokio::spawn({
        let closed = Arc::clone(&closed);
        async move {
            while closed.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            gate.notify_waiters();
        }
    });

    let started = tokio::time::Instant::now();
    tokio::time::timeout(
        EngineConfig::default().detach_timeout * 10,
        engine.detach(&account_id),
    )
    .await
    .expect("the detach ends")
    .expect("detach succeeds");
    let elapsed = started.elapsed();
    releasing.await.expect("the releaser does not panic");
    assert!(
        elapsed >= EngineConfig::default().detach_timeout,
        "the staging requires a worker that really is a straggler: the parked \
         inventory worker must have been aborted at the worker deadline, so the close \
         begins with none of that deadline left. waited {elapsed:?}"
    );

    assert_eq!(
        closed.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the close is reached even though a worker had to be aborted at the deadline"
    );
    assert_eq!(
        stub.close_returned
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "and it RAN TO COMPLETION: the straggler worker spent the worker deadline, \
         not the close's budget"
    );
}
