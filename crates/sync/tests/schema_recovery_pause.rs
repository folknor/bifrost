//! A pause landing around account-wide schema recovery, or a lifecycle scope's
//! first establishment, must not lose scopes.
//!
//! Schema recovery deletes EVERY scope's cursor and then re-establishes each one.
//! The establishment refuses a paused account and the multiplexer only polls
//! scopes still in the cursor registry, so a pause landing inside the recovery
//! stranded every scope it had deleted. `scope_repair_pause` holds the same
//! contract for a single scope's restart; these cover the two paths that shared
//! the hole:
//!
//! - schema recovery declines on a paused account before deleting anything, and
//!   once started holds its activity registration through the last
//!   re-establishment;
//! - a `ScopeLifecycle::Created` scope has NO cursor, so a repair declined under
//!   a pause leaves nothing for the poll to re-raise the directive from. It is
//!   parked and re-offered once the account runs.
//!
//! All run under a paused clock, so backoff and poll cadence are virtual.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bifrost_sync::{CheckpointStore, InMemoryCheckpointStore, SyncEngine};
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountFactory, AccountId,
    AccountOperation, Cause, Change, ChangeCursor, Control, CursorScope, FolderId, MembershipScope,
    ObjectType, ScopeLifecycle, ScopeLifecycleEvent, StateCause, SyncEvent, SyncStateErrorKind,
};

use common::{StubAccount, StubFactory};

const EMAIL: CursorScope = CursorScope::Type(ObjectType::Email);
const EVENTS: CursorScope = CursorScope::Type(ObjectType::CalendarEvent);

/// Upper bound on a wait for a poll-driven event. The clock is paused, so this
/// is virtual time; it has to outlast the default 60s poll cadence.
const BOUND: Duration = Duration::from_secs(300);

/// Longer than the default poll cadence, so a poll drive is certain to have run.
const POLL_CADENCE_PASSED: Duration = Duration::from_secs(200);

/// Derives `Engine(SchemaIncompatible)`.
fn schema_incompatible() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
        Cause::State(StateCause::SchemaIncompatible),
    )
    .operation(AccountOperation::SyncChanges)
    .try_build()
    .expect("valid account error classification")
}

/// An account with two scopes whose changes stream terminates with a schema
/// error once per unit stored in the returned counter, and is quiet otherwise.
fn failing_on_demand() -> (StubAccount, Arc<AtomicUsize>) {
    let fire = Arc::new(AtomicUsize::new(0));
    let hook_fire = Arc::clone(&fire);
    let mut account = StubAccount::new(vec![EMAIL, EVENTS]);
    account.changes_hook = Some(Arc::new(move |_cursor: &ChangeCursor| {
        let fires = hook_fire
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if fires {
            vec![SyncEvent::Terminated(schema_incompatible())]
        } else {
            Vec::<SyncEvent<Change>>::new()
        }
    }));
    (account, fire)
}

async fn durable_cursor_exists(
    store: &InMemoryCheckpointStore,
    id: &AccountId,
    scope: &CursorScope,
) -> bool {
    store
        .get_change_cursor(id, scope)
        .await
        .expect("cursor lookup")
        .is_some()
}

fn established(account: &StubAccount) -> Vec<CursorScope> {
    account
        .established
        .lock()
        .expect("established lock")
        .clone()
}

fn engine_over(store: &Arc<InMemoryCheckpointStore>) -> Arc<SyncEngine> {
    Arc::new(
        SyncEngine::builder()
            .checkpoints(Arc::clone(store) as Arc<dyn CheckpointStore>)
            .build()
            .expect("default engine config is valid"),
    )
}

/// Schema recovery reaches a paused account.
///
/// Staged with the reopen lock, as in `scope_repair_pause`: a parked
/// `subscribe_push` holds it, so the directive the failing scope raises queues
/// on the lock. The account is then paused and only then is the lock released.
/// Recovery must decline without deleting ANY cursor, and after resume the
/// still-registered failing cursor must raise the directive again, which then
/// re-establishes both scopes.
///
/// Ablation: without the activity gate at the top of `handle_schema_incompatible`
/// both scopes' durable cursors are deleted, the establishment is refused as
/// `Paused`, and neither is established again.
#[tokio::test(start_paused = true)]
async fn schema_recovery_reaching_a_paused_account_deletes_nothing_and_runs_after_resume() {
    let id = AccountId("schema-pause-declined".to_owned());
    let (account, fire) = failing_on_demand();
    let park = Arc::new(tokio::sync::Notify::new());
    *account
        .push_subscribe_park
        .lock()
        .expect("subscribe park lock") = Some((0, Arc::clone(&park)));
    let push_log = Arc::clone(&account.push_log);
    let account = Arc::new(account);
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store);
    let control = engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");
    for scope in [EMAIL, EVENTS] {
        assert!(
            durable_cursor_exists(&store, &id, &scope).await,
            "attach must have persisted {scope:?}, or survival proves nothing"
        );
    }
    assert_eq!(established(&account).len(), 2);

    // Hold the reopen lock inside a parked push subscription.
    let subscribing = {
        let engine = Arc::clone(&engine);
        let id = id.clone();
        tokio::spawn(async move { engine.subscribe_push(&id, &[EMAIL]).await })
    };
    tokio::time::timeout(BOUND, async {
        while push_log.subscribed().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the subscription never reached the account");

    // A scope's drive raises the directive; the dispatch queues on the lock.
    fire.store(1, Ordering::SeqCst);
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        fire.load(Ordering::SeqCst),
        0,
        "the failure must have fired"
    );

    control.pause().await.expect("an idle account pauses");
    park.notify_one();
    subscribing
        .await
        .expect("subscribe task")
        .expect("subscribe succeeds once released");

    // The dispatch now runs against a paused account.
    tokio::time::sleep(Duration::from_secs(30)).await;
    for scope in [EMAIL, EVENTS] {
        assert!(
            durable_cursor_exists(&store, &id, &scope).await,
            "schema recovery on a paused account deleted {scope:?}'s durable cursor"
        );
    }
    assert_eq!(
        established(&account).len(),
        2,
        "nothing may be re-established while the account is paused"
    );

    // After resume a still-registered failing cursor re-raises the directive.
    fire.store(1, Ordering::SeqCst);
    control.resume();
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        established(&account).len(),
        4,
        "after resume the poll must raise the directive again and recover both scopes"
    );
    for scope in [EMAIL, EVENTS] {
        assert!(durable_cursor_exists(&store, &id, &scope).await);
    }

    engine.detach(&id).await.expect("detach");
}

/// A pause lands while schema recovery is between two establishment attempts.
///
/// Every cursor is already deleted and the first attempt failed, so recovery
/// sleeps its backoff. The pause must wait for the recovery (which holds one
/// registration for all of it), and the retry must not be refused by the
/// boundary the pause flipped.
///
/// Ablation, hold: without the registration the pause reports quiescence
/// mid-recovery. Ablation, admitted control: re-establishing on the plain control
/// makes the retry (and the sibling scope) fail with `Paused`, so the cursors
/// deleted up front are never re-established.
#[tokio::test(start_paused = true)]
async fn a_pause_landing_mid_schema_recovery_waits_for_it_instead_of_stranding_scopes() {
    let id = AccountId("schema-pause-mid".to_owned());
    let (account, fire) = failing_on_demand();
    let account = Arc::new(account);
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store);
    let control = engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");
    assert_eq!(established(&account).len(), 2);

    // The first establishment attempt of the recovery fails; the retry works.
    account.establish_failures.store(1, Ordering::SeqCst);
    fire.store(1, Ordering::SeqCst);
    tokio::time::timeout(BOUND, account.establish_failed.notified())
        .await
        .expect("recovery never attempted its establishment");

    // Recovery is now in its backoff, holding its activity registration.
    let mut pause = Box::pin(control.pause());
    tokio::select! {
        biased;
        result = &mut pause => panic!("pause reported quiescence mid-recovery: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    pause
        .await
        .expect("the pause completes once the recovery has finished");

    assert_eq!(
        established(&account).len(),
        5,
        "attach's two, the failed attempt, its retry, and the sibling scope, none refused"
    );
    for scope in [EMAIL, EVENTS] {
        assert!(
            durable_cursor_exists(&store, &id, &scope).await,
            "the pause stranded {scope:?}: its cursor was deleted and never re-established"
        );
    }

    control.resume();
    engine.detach(&id).await.expect("detach");
}

fn new_folder() -> FolderId {
    FolderId("created".to_owned())
}

/// A lifecycle account: one folder scope, and a lifecycle stream the test drives.
fn lifecycle_account() -> (
    Arc<StubAccount>,
    tokio::sync::mpsc::Sender<ScopeLifecycleEvent>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let mut account = StubAccount::new(vec![CursorScope::Folder(FolderId("inbox".to_owned()))]);
    account.lifecycle_rx = std::sync::Mutex::new(Some(rx));
    (Arc::new(account), tx)
}

async fn create_folder(tx: &tokio::sync::mpsc::Sender<ScopeLifecycleEvent>) {
    tx.send(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(
        MembershipScope::Folder(new_folder()),
    )))
    .await
    .expect("the lifecycle stream accepts the event");
}

fn created_scope() -> CursorScope {
    CursorScope::Folder(new_folder())
}

fn times_established(account: &StubAccount, scope: &CursorScope) -> usize {
    established(account)
        .iter()
        .filter(|established| *established == scope)
        .count()
}

/// A `Created` scope whose establishment is asked for while the account is
/// paused is established once the account resumes.
///
/// The repair declines under the pause, and the scope has no cursor for the poll
/// to re-raise it from, so without a deferral it is lost for good. Two `Created`
/// events land during the pause: the scope is parked once, so it is established
/// exactly once (a second parked repair would restart the cursor the first one
/// had just made).
///
/// Ablation: dropping the parked re-offer (the listener discarding the declined
/// scope) leaves the scope never established, durably or otherwise. Ablation,
/// dedupe: parking twice establishes it twice.
#[tokio::test(start_paused = true)]
async fn a_created_scope_declined_under_a_pause_is_established_after_resume() {
    let id = AccountId("created-pause".to_owned());
    let (account, lifecycle) = lifecycle_account();
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store);
    let control = engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");

    control.pause().await.expect("an idle account pauses");
    create_folder(&lifecycle).await;
    create_folder(&lifecycle).await;
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(
        times_established(&account, &created_scope()),
        0,
        "nothing may be established while the account is paused"
    );

    control.resume();
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        times_established(&account, &created_scope()),
        1,
        "the declined scope must be re-offered after resume, exactly once"
    );
    assert!(
        durable_cursor_exists(&store, &id, &created_scope()).await,
        "the established scope's cursor must be durable"
    );

    engine.detach(&id).await.expect("detach");
}

async fn delete_folder(tx: &tokio::sync::mpsc::Sender<ScopeLifecycleEvent>) {
    tx.send(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Deleted(
        MembershipScope::Folder(new_folder()),
    )))
    .await
    .expect("the lifecycle stream accepts the event");
}

/// A parked scope does not outlive its folder. Created and then deleted while
/// the account is paused, it must not be established after resume: the folder
/// is gone, and establishing it would end in the retry-budget terminal event.
///
/// The deletion reaches nothing through the registry - a parked scope has no
/// cursor - so it has to be routed to the parked set explicitly.
///
/// Ablation: dropping the unpark (or routing only registered scopes to
/// `ScopeDeleted`) establishes the deleted folder after resume.
#[tokio::test(start_paused = true)]
async fn a_parked_scope_whose_folder_is_deleted_during_the_pause_is_not_established() {
    let id = AccountId("created-deleted-pause".to_owned());
    let (account, lifecycle) = lifecycle_account();
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store);
    let control = engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");

    control.pause().await.expect("an idle account pauses");
    create_folder(&lifecycle).await;
    delete_folder(&lifecycle).await;
    tokio::time::sleep(Duration::from_secs(30)).await;

    control.resume();
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        times_established(&account, &created_scope()),
        0,
        "a folder deleted during the pause must not be established after resume"
    );

    engine.detach(&id).await.expect("detach");
}

/// Deleted and created AGAIN during one pause, the folder exists after resume
/// and is established exactly once: the second `Created` parks it afresh, and
/// only one of the two parked tasks finds it still parked.
#[tokio::test(start_paused = true)]
async fn a_folder_deleted_and_recreated_during_a_pause_is_established_once() {
    let id = AccountId("created-deleted-created-pause".to_owned());
    let (account, lifecycle) = lifecycle_account();
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store);
    let control = engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");

    control.pause().await.expect("an idle account pauses");
    create_folder(&lifecycle).await;
    delete_folder(&lifecycle).await;
    create_folder(&lifecycle).await;
    tokio::time::sleep(Duration::from_secs(30)).await;

    control.resume();
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        times_established(&account, &created_scope()),
        1,
        "the recreated folder is established once after resume"
    );

    engine.detach(&id).await.expect("detach");
}

/// On a RUNNING account, a folder deleted while its `Created` repair is still
/// queued on the reopen listener is established by that repair, and the
/// deletion that follows on the same channel must retire the cursor, not only
/// purge its rows. Purging alone left a live cursor for a deleted folder, its
/// durable rows deleted out from under it.
///
/// Staged with the reopen lock: a parked `subscribe_push` holds it, so the
/// `Created` repair queues behind it and the `Deleted` queues behind that.
///
/// Ablation: dropping the arm's cursor retirement leaves the deleted folder's
/// cursor polling after the deletion was handled.
#[tokio::test(start_paused = true)]
async fn a_folder_deleted_while_its_repair_is_queued_leaves_no_live_cursor() {
    let id = AccountId("created-deleted-queued".to_owned());
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let mut account = StubAccount::new(vec![CursorScope::Folder(FolderId("inbox".to_owned()))]);
    account.lifecycle_rx = std::sync::Mutex::new(Some(rx));
    let polls = Arc::new(AtomicUsize::new(0));
    let hook_polls = Arc::clone(&polls);
    account.changes_hook = Some(Arc::new(move |cursor: &ChangeCursor| {
        if cursor.scope == created_scope() {
            hook_polls.fetch_add(1, Ordering::SeqCst);
        }
        Vec::<SyncEvent<Change>>::new()
    }));
    let park = Arc::new(tokio::sync::Notify::new());
    *account
        .push_subscribe_park
        .lock()
        .expect("subscribe park lock") = Some((0, Arc::clone(&park)));
    let push_log = Arc::clone(&account.push_log);
    let account = Arc::new(account);
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store);
    engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");

    let subscribing = {
        let engine = Arc::clone(&engine);
        let id = id.clone();
        let inbox = CursorScope::Folder(FolderId("inbox".to_owned()));
        tokio::spawn(async move { engine.subscribe_push(&id, &[inbox]).await })
    };
    tokio::time::timeout(BOUND, async {
        while push_log.subscribed().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the subscription never reached the account");

    create_folder(&tx).await;
    delete_folder(&tx).await;
    tokio::time::sleep(Duration::from_secs(30)).await;
    park.notify_one();
    subscribing
        .await
        .expect("subscribe task")
        .expect("subscribe succeeds once released");

    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        times_established(&account, &created_scope()),
        1,
        "the queued repair established the folder, or the race was never staged"
    );
    let settled = polls.load(Ordering::SeqCst);
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        polls.load(Ordering::SeqCst),
        settled,
        "a deleted folder's cursor must not keep polling"
    );
    assert!(
        !durable_cursor_exists(&store, &id, &created_scope()).await,
        "the deleted folder's durable cursor is purged"
    );

    engine.detach(&id).await.expect("detach");
}

/// A parked scope does not outlive the account: detach while it waits ends the
/// wait and nothing is established.
#[tokio::test(start_paused = true)]
async fn a_parked_scope_is_dropped_by_detach() {
    let id = AccountId("created-detach".to_owned());
    let (account, lifecycle) = lifecycle_account();
    let factory: Arc<dyn AccountFactory> = Arc::new(StubFactory::queue(vec![Arc::clone(&account)]));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = engine_over(&store);
    let control = engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");

    control.pause().await.expect("an idle account pauses");
    create_folder(&lifecycle).await;
    tokio::time::sleep(Duration::from_secs(30)).await;

    engine.detach(&id).await.expect("detach");
    tokio::time::sleep(POLL_CADENCE_PASSED).await;
    assert_eq!(
        times_established(&account, &created_scope()),
        0,
        "a detached account must not establish a parked scope"
    );
}
