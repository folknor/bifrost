//! A reopen must keep the consumer's push subscriptions alive across a failed
//! old-side teardown.
//!
//! Reopen recreates every registered subscription on the replacement and then
//! tears the old handles down on the old account. When one of those teardowns
//! fails (a Graph 503 on the DELETE, a JMAP socket that died), the attempt
//! aborts and the record is flagged `teardown_unconfirmed` so the next attempt
//! does not abort on it again. That flag used to double as "orphan, never
//! recreate", so the next attempt tore the old handle down (or carried it) and
//! recreated nothing: the subscription the consumer still wanted vanished with
//! no error anywhere. Whether the old handle's teardown is confirmed and
//! whether the consumer still wants the coverage are two separate facts, and
//! these tests pin both halves.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bifrost_sync::{Error, SyncEngine};
use bifrost_types::{
    AccountFactory, AccountId, CursorScope, FolderId, PushCapability, SubscriptionHandle,
};

use common::{PushLog, StubAccount, StubFactory};

fn inbox() -> Vec<CursorScope> {
    vec![CursorScope::Folder(FolderId("inbox".into()))]
}

/// A push-capable account named `label` whose first `failures` teardowns fail.
fn pushing(label: &str, log: &Arc<PushLog>, failures: usize) -> Arc<StubAccount> {
    let mut caps = common::caps();
    caps.push = PushCapability::WebhookOrEwsStream;
    Arc::new(StubAccount {
        caps,
        push_label: label.to_owned(),
        push_log: Arc::clone(log),
        push_unsubscribe_failures: AtomicUsize::new(failures),
        ..StubAccount::new(inbox())
    })
}

fn call(label: &str, handle: &str) -> (String, SubscriptionHandle) {
    (label.to_owned(), SubscriptionHandle(handle.to_owned()))
}

fn subscribed_on(label: &str) -> (String, Vec<CursorScope>) {
    (label.to_owned(), inbox())
}

async fn attached(id: &AccountId, accounts: Vec<Arc<StubAccount>>) -> SyncEngine {
    let engine = SyncEngine::builder().build().expect("engine");
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(accounts));
    engine.attach(id.clone(), factory).await.expect("attach");
    engine
        .subscribe_push(id, &inbox())
        .await
        .expect("consumer subscribes");
    engine
}

/// The old connection is dead, so every teardown on it fails. The first
/// reopen aborts on that; the retry must commit AND leave the consumer
/// subscribed on the replacement, with the old handle still tracked as an
/// orphan for a later teardown.
///
/// Against the pre-fix code the retry never calls `push_subscribe` on
/// `replacement`, so the `subscribed` assertion fails, and the final
/// `unsubscribe_push` finds only the orphaned old handle.
#[tokio::test]
async fn a_failed_old_side_teardown_does_not_cost_the_consumer_its_subscription() {
    let id = AccountId("reopen-dead-old-connection".into());
    let log = Arc::new(PushLog::default());
    let old = pushing("old", &log, usize::MAX);
    let aborted = pushing("aborted", &log, 0);
    let replacement = pushing("replacement", &log, 0);
    let engine = attached(
        &id,
        vec![
            Arc::clone(&old),
            Arc::clone(&aborted),
            Arc::clone(&replacement),
        ],
    )
    .await;

    assert!(
        matches!(engine.reattach(&id).await, Err(Error::Account(_))),
        "a first failure of a live handle's old-side teardown still aborts the swap"
    );
    engine
        .reattach(&id)
        .await
        .expect("the retry proceeds past the already-unconfirmed teardown");

    assert_eq!(
        log.subscribed(),
        vec![
            subscribed_on("old"),
            subscribed_on("aborted"),
            subscribed_on("replacement"),
        ],
        "the consumer's subscription must be recreated on the replacement that \
         was actually installed"
    );
    assert_eq!(old.closed.load(Ordering::SeqCst), 1, "old account retired");
    assert_eq!(
        aborted.closed.load(Ordering::SeqCst),
        1,
        "aborted replacement closed"
    );
    assert_eq!(
        replacement.closed.load(Ordering::SeqCst),
        0,
        "the replacement is the live account"
    );

    engine
        .unsubscribe_push(&id)
        .await
        .expect("both records tear down against the live account");
    assert_eq!(
        log.unsubscribed(),
        vec![
            // Attempt 1: the old handle refuses, the replacement's copy unwinds.
            call("old", "old"),
            call("aborted", "aborted"),
            // Attempt 2: the old handle refuses again and is carried.
            call("old", "old"),
            // The consumer's teardown: the recreated subscription, then the
            // carried orphan.
            call("replacement", "replacement"),
            call("replacement", "old"),
        ],
        "the registry must hold the recreated subscription AND the orphan"
    );

    engine.detach(&id).await.expect("detach");
}

/// A transient failure - the first DELETE fails, the retry's succeeds. The
/// old handle is then genuinely gone, and the replacement must hold the
/// consumer's subscription.
///
/// Against the pre-fix code this was the worse half: the retry's teardown
/// succeeded, so nothing was even carried, and the registry ended up empty.
#[tokio::test]
async fn a_transient_old_side_teardown_failure_still_recreates_on_the_retry() {
    let id = AccountId("reopen-transient-teardown".into());
    let log = Arc::new(PushLog::default());
    let engine = attached(
        &id,
        vec![
            pushing("old", &log, 1),
            pushing("aborted", &log, 0),
            pushing("replacement", &log, 0),
        ],
    )
    .await;

    assert!(matches!(engine.reattach(&id).await, Err(Error::Account(_))));
    engine.reattach(&id).await.expect("retry commits");

    assert_eq!(
        log.subscribed(),
        vec![
            subscribed_on("old"),
            subscribed_on("aborted"),
            subscribed_on("replacement"),
        ],
    );
    engine.unsubscribe_push(&id).await.expect("teardown");
    assert_eq!(
        log.unsubscribed(),
        vec![
            call("old", "old"),
            call("aborted", "aborted"),
            call("old", "old"),
            call("replacement", "replacement"),
        ],
        "exactly the recreated subscription is registered, nothing else"
    );

    engine.detach(&id).await.expect("detach");
}

/// Where the hole would move to: the old handle carried past a committed
/// reopen must be carried as an ORPHAN, because its desire now lives in the
/// record recreated on the replacement. Carrying it still marked as wanted
/// would make every later reopen subscribe once per carried copy.
///
/// Bites twice: the pre-fix code subscribes on `later` zero times, and a fix
/// that recreated wanted records without demoting the carried one subscribes
/// on it twice.
#[tokio::test]
async fn the_carried_old_handle_is_not_recreated_by_a_later_reopen() {
    let id = AccountId("reopen-carried-orphan".into());
    let log = Arc::new(PushLog::default());
    let engine = attached(
        &id,
        vec![
            pushing("old", &log, usize::MAX),
            pushing("aborted", &log, 0),
            pushing("replacement", &log, 0),
            pushing("later", &log, 0),
        ],
    )
    .await;

    assert!(matches!(engine.reattach(&id).await, Err(Error::Account(_))));
    engine.reattach(&id).await.expect("second attempt commits");
    engine.reattach(&id).await.expect("a later reopen commits");

    assert_eq!(
        log.subscribed(),
        vec![
            subscribed_on("old"),
            subscribed_on("aborted"),
            subscribed_on("replacement"),
            subscribed_on("later"),
        ],
        "one wanted subscription, recreated exactly once per installed account"
    );
    assert_eq!(
        log.unsubscribed(),
        vec![
            call("old", "old"),
            call("aborted", "aborted"),
            call("old", "old"),
            // The later reopen retires the replacement's subscription and
            // retries the orphan, which now succeeds.
            call("replacement", "replacement"),
            call("replacement", "old"),
        ],
    );

    engine.detach(&id).await.expect("detach");
}

/// The other half of the distinction, pinned so a fix cannot overcorrect: a
/// subscription the CONSUMER asked to tear down, whose teardown failed, is an
/// orphan. A reopen retries its teardown and must not bring it back.
///
/// This passes against the pre-fix code too; it guards against a fix that
/// simply stopped filtering unconfirmed records.
#[tokio::test]
async fn a_subscription_the_consumer_tore_down_is_not_resurrected_by_reopen() {
    let id = AccountId("reopen-consumer-orphan".into());
    let log = Arc::new(PushLog::default());
    let engine = attached(
        &id,
        vec![pushing("old", &log, 1), pushing("replacement", &log, 0)],
    )
    .await;

    assert!(
        matches!(engine.unsubscribe_push(&id).await, Err(Error::Account(_))),
        "the consumer's teardown fails once"
    );
    engine
        .reattach(&id)
        .await
        .expect("reopen retries the orphan and commits");

    assert_eq!(
        log.subscribed(),
        vec![subscribed_on("old")],
        "a torn-down subscription must not be recreated on the replacement"
    );
    assert_eq!(
        log.unsubscribed(),
        vec![call("old", "old"), call("old", "old")],
        "the reopen retried the orphan's teardown on the old account"
    );

    engine
        .unsubscribe_push(&id)
        .await
        .expect("nothing left to tear down");
    assert_eq!(log.unsubscribed().len(), 2, "the registry is empty");

    engine.detach(&id).await.expect("detach");
}
