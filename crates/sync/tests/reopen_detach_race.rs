//! `SyncEngine::reopen` is public and consumer-driven, and `detach`
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
#[tokio::test]
async fn a_reopen_racing_detach_closes_its_replacement_instead_of_leaking_it() {
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
        tokio::spawn(async move { engine.reopen(&account_id).await })
    };

    // The replacement open is in flight and holds the reopen guard.
    factory.started.notified().await;

    engine.detach(&account_id).await.expect("detach");
    factory.release.notify_one();

    let outcome = reopening.await.expect("reopen task");
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
