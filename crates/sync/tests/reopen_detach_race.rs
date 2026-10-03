//! `SyncEngine::reattach` is public and consumer-driven, and `detach`
//! deliberately does not wait for consumer-driven activity. Nothing else
//! excludes the two, so the replacement open has to observe the teardown
//! itself.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bifrost_types::{
    Account, AccountError, AccountFactory, AccountFuture, AccountId, CursorScope, FolderId,
    OpenedAccount,
};

use common::StubAccount;

/// Serves the first account immediately and parks the second open until the
/// test releases it, so a detach can land squarely inside the replacement open.
struct GatedFactory {
    first: Arc<StubAccount>,
    replacement: Arc<StubAccount>,
    opens: std::sync::atomic::AtomicUsize,
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl AccountFactory for GatedFactory {
    fn open(&self, _account: AccountId) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        let nth = self.opens.fetch_add(1, Ordering::SeqCst);
        if nth == 0 {
            let account: Arc<dyn Account> = Arc::<StubAccount>::clone(&self.first);
            return Box::pin(async move { Ok(OpenedAccount::complete(account)) });
        }
        let account: Arc<dyn Account> = Arc::<StubAccount>::clone(&self.replacement);
        let started = Arc::clone(&self.started);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            started.notify_one();
            release.notified().await;
            Ok(OpenedAccount::complete(account))
        })
    }
}

/// A replacement opened while the slot is being torn down must be CLOSED, not
/// swapped into a slot nothing owns any more.
///
/// The window: `reopen` registers its activity just before `detach` flips the
/// boundary to `Stop`, so the open proceeds while detach removes the slot,
/// awaits the workers, and closes the old handle. With the same scope topology
/// on both sides, the reattach establishes no cursor and recreates no
/// subscription - the two paths that would touch the now-dead writer channel
/// and abort - so it used to run to completion, swap the replacement into the
/// orphaned slot, and close the ALREADY-closed previous handle. The
/// replacement then had no owner and was never closed.
///
/// Detach now waits for the reopen lock the parked open holds, so it runs as
/// a task: the test lets it cancel the slot and block on the lock, and only
/// then releases the open, which must see the cancellation.
#[tokio::test(start_paused = true)]
async fn a_reattach_racing_detach_closes_its_replacement_instead_of_leaking_it() {
    let scopes = vec![CursorScope::Folder(FolderId("inbox".into()))];
    let first = Arc::new(StubAccount::new(scopes.clone()));
    let replacement = Arc::new(StubAccount::new(scopes));
    let factory = Arc::new(GatedFactory {
        first: Arc::clone(&first),
        replacement: Arc::clone(&replacement),
        opens: std::sync::atomic::AtomicUsize::new(0),
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    });

    let engine = Arc::new(bifrost_sync::SyncEngine::builder().build().expect("engine"));
    let account_id = AccountId("reopen-detach-race".into());
    engine
        .attach(
            account_id.clone(),
            Arc::clone(&factory) as Arc<dyn AccountFactory>,
        )
        .await
        .expect("attach");

    let reopening = {
        let engine = Arc::clone(&engine);
        let account_id = account_id.clone();
        tokio::spawn(async move { engine.reattach(&account_id).await })
    };

    // The replacement open is in flight and holds the reopen guard.
    factory.started.notified().await;

    let mut detaching = {
        let engine = Arc::clone(&engine);
        let account_id = account_id.clone();
        tokio::spawn(async move { engine.detach(&account_id).await })
    };
    // Detach cancels the slot first thing, then parks on the reopen lock the
    // open holds. Under paused time this bound elapses only once every task
    // is idle, so expiring it means detach really is waiting.
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut detaching)
            .await
            .is_err(),
        "detach waits for the in-flight reattach rather than racing it"
    );
    factory.release.notify_one();

    let outcome = reopening.await.expect("reopen task");
    detaching.await.expect("detach task").expect("detach");
    assert!(
        outcome.is_err(),
        "a reopen whose slot was detached under it must not report success"
    );
    assert_eq!(
        replacement.closed.load(Ordering::SeqCst),
        1,
        "the replacement connection must be closed rather than left owner-less"
    );
    assert_eq!(
        first.closed.load(Ordering::SeqCst),
        1,
        "detach still closes the handle it knew about, exactly once"
    );
}

/// Serves the first account, then the replacement, both immediately.
struct SequenceFactory {
    first: Arc<StubAccount>,
    replacement: Arc<StubAccount>,
    opens: std::sync::atomic::AtomicUsize,
}

impl AccountFactory for SequenceFactory {
    fn open(&self, _account: AccountId) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        let account: Arc<dyn Account> = if self.opens.fetch_add(1, Ordering::SeqCst) == 0 {
            Arc::<StubAccount>::clone(&self.first)
        } else {
            Arc::<StubAccount>::clone(&self.replacement)
        };
        Box::pin(async move { Ok(OpenedAccount::complete(account)) })
    }
}

/// The later half of the same window: the replacement is already open and
/// past the post-open shutdown check, parked inside the swap phase (its
/// membership read) when detach arrives.
///
/// Detach used to load `current` and close it without waiting, closing the
/// OLD account while the reattach went on to swap the replacement in and close
/// the old account a second time - leaving the replacement owned by nothing
/// and never closed. Detach now waits for the reopen lock the reattach holds
/// through its swap and close, then closes whatever is current: each account
/// is closed exactly once.
#[tokio::test(start_paused = true)]
async fn a_detach_landing_inside_a_reattach_swap_closes_each_account_once() {
    let scopes = vec![CursorScope::Folder(FolderId("inbox".into()))];
    let first = Arc::new(StubAccount::new(scopes.clone()));
    let replacement = Arc::new(StubAccount::new(scopes));
    let gate = Arc::new(tokio::sync::Notify::new());
    let (probe, parked, _destroyed) = common::StallProbe::install();
    *replacement
        .memberships_stall
        .lock()
        .expect("memberships stall lock") = Some((Arc::clone(&gate), Some(probe)));
    let factory = Arc::new(SequenceFactory {
        first: Arc::clone(&first),
        replacement: Arc::clone(&replacement),
        opens: std::sync::atomic::AtomicUsize::new(0),
    });

    let engine = Arc::new(bifrost_sync::SyncEngine::builder().build().expect("engine"));
    let account_id = AccountId("detach-inside-swap".into());
    engine
        .attach(
            account_id.clone(),
            Arc::clone(&factory) as Arc<dyn AccountFactory>,
        )
        .await
        .expect("attach");

    let reopening = {
        let engine = Arc::clone(&engine);
        let account_id = account_id.clone();
        tokio::spawn(async move { engine.reattach(&account_id).await })
    };
    // The replacement is open and parked in the swap phase, holding the
    // reopen lock.
    parked
        .await
        .expect("the replacement's membership read parks");

    let detaching = {
        let engine = Arc::clone(&engine);
        let account_id = account_id.clone();
        tokio::spawn(async move { engine.detach(&account_id).await })
    };
    // Long enough, in virtual time, for detach to be into its reopen-lock
    // wait. The parked reattach holds a clone of the writer's sender, so the
    // writer phase cannot drain and runs out its full `detach_timeout`; the
    // lock wait then runs to at least two. A detach that did not wait would
    // have closed the old account once the writer phase expired; one that
    // waits has closed nothing and is still running. (Confirmed by ablation:
    // with a zero lock wait this assertion fails.)
    let detach_timeout = bifrost_sync::EngineConfig::default().detach_timeout;
    tokio::time::sleep(detach_timeout + detach_timeout / 2).await;
    assert_eq!(
        first.closed.load(Ordering::SeqCst),
        0,
        "detach must wait for the in-flight swap instead of closing around it"
    );
    assert!(
        !detaching.is_finished(),
        "detach is waiting on the reopen lock"
    );
    gate.notify_one();

    let _ = reopening.await.expect("reopen task");
    detaching.await.expect("detach task").expect("detach");
    assert_eq!(
        first.closed.load(Ordering::SeqCst),
        1,
        "the replaced account is closed once, by the reattach"
    );
    assert_eq!(
        replacement.closed.load(Ordering::SeqCst),
        1,
        "the replacement is closed once, by the detach that waited for it"
    );
}
