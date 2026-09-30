//! A strategy downgrade actually restarts the account.
//!
//! `EngineDirective::DowngradeStrategy` is derived from a
//! `SyncState(StrategyFailure)` error and handled by restarting the account,
//! then, when the error names a cursor scope, re-establishing that scope. The
//! recovery dispatch takes the slot's `reopen_lock` for scope-local repairs,
//! and `open_replacement` takes the same non-reentrant mutex for the open and
//! the swap. Holding it across the dispatch of a directive that restarts the
//! account therefore hung recovery forever on the first downgrade, with no
//! error and nothing logged.
//!
//! The staging is built so that only a COMPLETED restart satisfies the
//! assertions. The first account terminates its scope's change stream with the
//! downgrade error exactly once; the replacement account never fails. A hung
//! recovery leaves the factory unopened a second time, the first account
//! unclosed, and the replacement never asked to establish anything, so every
//! observation below is false under the hang. Each wait is bounded by a
//! timeout well under the watchdog, so the hang shows up as a failed assertion
//! rather than a stuck suite.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bifrost_sync::SyncEngine;
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountFactory, AccountFuture, AccountId,
    AccountOperation, Cause, Change, ChangeCursor, CursorScope, ErrorScope, ObjectType, StateCause,
    StrategyDowngrade, SyncEvent, SyncStateErrorKind,
};

use common::{StubAccount, StubFactory};

/// Upper bound on any wait. Far above what a healthy restart needs, far below
/// the 20s test watchdog.
const BOUND: Duration = Duration::from_secs(8);

const SCOPE: CursorScope = CursorScope::Type(ObjectType::Email);

fn downgrade_error(scope: ErrorScope) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::StrategyFailure),
        Cause::State(StateCause::StrategyFailure {
            downgrade: StrategyDowngrade::CondstoreToBasic,
        }),
    )
    .operation(AccountOperation::SyncChanges)
    .scope(scope)
    .try_build()
    .expect("valid account error classification")
}

/// Counts `open` calls, delegating to a queue of stub accounts.
struct CountingFactory {
    inner: StubFactory,
    opens: Arc<AtomicUsize>,
}

impl AccountFactory for CountingFactory {
    fn open(
        &self,
        account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.inner.open(account_id)
    }
}

/// An account whose scope terminates once with `error`, then goes quiet.
fn failing_once(error: AccountError) -> Arc<StubAccount> {
    let fired = Arc::new(AtomicUsize::new(0));
    let mut account = StubAccount::new(vec![SCOPE]);
    account.changes_hook = Some(Arc::new(move |_cursor: &ChangeCursor| {
        if fired.fetch_add(1, Ordering::SeqCst) == 0 {
            vec![SyncEvent::Terminated(error.clone())]
        } else {
            Vec::<SyncEvent<Change>>::new()
        }
    }));
    Arc::new(account)
}

/// Poll `condition` every few milliseconds until it holds or `BOUND` passes.
/// Returns whether it held.
async fn holds_within_bound(condition: impl Fn() -> bool) -> bool {
    tokio::time::timeout(BOUND, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

struct Staged {
    engine: SyncEngine,
    id: AccountId,
    first: Arc<StubAccount>,
    replacement: Arc<StubAccount>,
    opens: Arc<AtomicUsize>,
}

async fn attach_with_downgrade(label: &str, error: AccountError) -> Staged {
    let id = AccountId(label.to_owned());
    let first = failing_once(error);
    let replacement = Arc::new(StubAccount::new(vec![SCOPE]));
    let opens = Arc::new(AtomicUsize::new(0));
    let factory: Arc<dyn AccountFactory> = Arc::new(CountingFactory {
        inner: StubFactory::queue(vec![Arc::clone(&first), Arc::clone(&replacement)]),
        opens: Arc::clone(&opens),
    });
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    engine
        .attach(id.clone(), factory)
        .await
        .expect("attach succeeds");
    Staged {
        engine,
        id,
        first,
        replacement,
        opens,
    }
}

/// The error names a cursor scope, so recovery restarts the account AND
/// re-establishes that scope on the replacement.
#[tokio::test]
async fn a_scoped_strategy_downgrade_restarts_the_account_and_the_scope() {
    let staged = attach_with_downgrade(
        "downgrade-scoped",
        downgrade_error(ErrorScope::Cursor(SCOPE)),
    )
    .await;

    // Attach itself opened once and established the scope once on the first
    // account, so neither of those counts is evidence of recovery.
    let replacement = Arc::clone(&staged.replacement);
    let reestablished = holds_within_bound(|| {
        !replacement
            .established
            .lock()
            .expect("established lock")
            .is_empty()
    })
    .await;
    assert!(
        reestablished,
        "the scope was never re-established on the replacement account: recovery \
         for the strategy downgrade did not complete (a hang inside the account \
         restart looks exactly like this)"
    );
    assert_eq!(
        *staged.replacement.established.lock().expect("lock"),
        vec![SCOPE],
        "the replacement must establish the downgraded scope exactly once"
    );
    assert_eq!(
        staged.opens.load(Ordering::SeqCst),
        2,
        "the factory must have opened exactly one replacement"
    );
    let first = Arc::clone(&staged.first);
    assert!(
        holds_within_bound(|| first.closed.load(Ordering::SeqCst) == 1).await,
        "the superseded account was never closed"
    );

    tokio::time::timeout(BOUND, staged.engine.detach(&staged.id))
        .await
        .expect("detach after recovery must not hang on a leaked reopen lock")
        .expect("detach succeeds");
    assert_eq!(staged.replacement.closed.load(Ordering::SeqCst), 1);
}

/// The error names no cursor scope, so only the account restart runs. The
/// trailing scope restart is skipped, which also means nothing after the
/// restart could mask a hang inside it.
#[tokio::test]
async fn an_account_scoped_strategy_downgrade_restarts_the_account() {
    let staged =
        attach_with_downgrade("downgrade-account", downgrade_error(ErrorScope::Account)).await;

    let opens = Arc::clone(&staged.opens);
    let first = Arc::clone(&staged.first);
    assert!(
        holds_within_bound(
            || opens.load(Ordering::SeqCst) == 2 && first.closed.load(Ordering::SeqCst) == 1
        )
        .await,
        "no replacement was opened and swapped in: recovery for the strategy \
         downgrade did not complete (opens={}, first closed={})",
        staged.opens.load(Ordering::SeqCst),
        staged.first.closed.load(Ordering::SeqCst)
    );

    tokio::time::timeout(BOUND, staged.engine.detach(&staged.id))
        .await
        .expect("detach after recovery must not hang")
        .expect("detach succeeds");
    assert_eq!(staged.replacement.closed.load(Ordering::SeqCst), 1);
}
