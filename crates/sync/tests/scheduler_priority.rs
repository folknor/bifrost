//! Scheduler priority + starvation guard tests.
//!
//! - Foreground preempts Background.
//! - Strict priority within {Foreground, Normal} is preserved.
//! - Background and Bulk both starve under sustained Foreground load
//!   until the starvation floor fires; then one lower-lane pull lands.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_sync::SchedulerConfig;
use bifrost_sync::scheduler::lanes::{WorkItem, WorkKind};
use bifrost_sync::scheduler::{BudgetGate, ConcurrencyBudget, Scheduler};
use bifrost_types::{AccountId, Priority};

fn make_scheduler(floor: u32) -> Scheduler {
    let cfg = SchedulerConfig {
        starvation_floor: floor,
    };
    Scheduler::new(cfg, BudgetGate::new(ConcurrencyBudget::default()))
}

fn submit(
    scheduler: &Scheduler,
    account: &str,
    priority: Priority,
    marker: Arc<AtomicU64>,
    tag: u64,
) {
    scheduler.submit(WorkItem {
        account: AccountId(account.into()),
        priority,
        kind: WorkKind::Sync,
        run: Box::new(move || {
            marker.store(tag, Ordering::Relaxed);
        }),
    });
}

#[tokio::test]
async fn foreground_preempts_background() {
    let scheduler = make_scheduler(64);
    let marker = Arc::new(AtomicU64::new(0));
    submit(
        &scheduler,
        "a",
        Priority::Background,
        Arc::clone(&marker),
        10,
    );
    submit(
        &scheduler,
        "a",
        Priority::Foreground,
        Arc::clone(&marker),
        20,
    );

    let first = scheduler.pull_next().await;
    assert_eq!(first.priority, Priority::Foreground);
    let second = scheduler.pull_next().await;
    assert_eq!(second.priority, Priority::Background);
}

#[tokio::test]
async fn strict_priority_walks_lanes_in_order() {
    let scheduler = make_scheduler(64);
    let marker = Arc::new(AtomicU64::new(0));
    submit(&scheduler, "a", Priority::Bulk, Arc::clone(&marker), 1);
    submit(
        &scheduler,
        "a",
        Priority::Background,
        Arc::clone(&marker),
        2,
    );
    submit(&scheduler, "a", Priority::Normal, Arc::clone(&marker), 3);
    submit(
        &scheduler,
        "a",
        Priority::Foreground,
        Arc::clone(&marker),
        4,
    );

    let mut order = Vec::new();
    for _ in 0..4 {
        let item = scheduler.pull_next().await;
        order.push(item.priority);
    }

    assert_eq!(
        order,
        vec![
            Priority::Foreground,
            Priority::Normal,
            Priority::Background,
            Priority::Bulk,
        ]
    );
}

#[tokio::test]
async fn starvation_floor_diverts_to_lower_lane_after_n_pulls() {
    let floor: u32 = 4;
    let scheduler = make_scheduler(floor);
    let marker = Arc::new(AtomicU64::new(0));

    // Submit enough Foreground items to comfortably exceed the floor,
    // plus a single Background item we expect to surface mid-stream.
    for i in 0..(floor as u64 + 5) {
        submit(
            &scheduler,
            "a",
            Priority::Foreground,
            Arc::clone(&marker),
            i,
        );
    }
    submit(
        &scheduler,
        "a",
        Priority::Background,
        Arc::clone(&marker),
        9999,
    );

    let mut saw_background_at: Option<u32> = None;
    for step in 0..(floor + 6) {
        let item = scheduler.pull_next().await;
        if item.priority == Priority::Background && saw_background_at.is_none() {
            saw_background_at = Some(step);
        }
    }

    let pos = saw_background_at.expect("background must surface");
    // The starvation floor fires AT the floor count (0-indexed pull
    // number == floor), so position is exactly `floor`.
    assert_eq!(
        pos, floor,
        "background should land at position {floor} (zero-indexed), got {pos}",
    );
}

#[tokio::test]
async fn pull_wakes_when_work_is_submitted() {
    let scheduler = make_scheduler(64);
    let waiter = tokio::spawn({
        let scheduler = scheduler.clone();
        async move { scheduler.pull_next().await }
    });
    tokio::task::yield_now().await;
    let marker = Arc::new(AtomicU64::new(0));
    submit(&scheduler, "a", Priority::Normal, marker, 1);
    let item = waiter.await.expect("pull task");
    assert_eq!(item.account, AccountId("a".into()));
}

#[test]
fn pull_stays_synchronous_and_answers_the_empty_question() {
    // The non-blocking emptiness check is a published capability: a
    // consumer must be able to ask "is there work right now?" without
    // committing to a wait. `pull_next` is the waiting form.
    let scheduler = make_scheduler(64);
    let marker = Arc::new(AtomicU64::new(0));
    assert!(scheduler.pull().is_none(), "empty scheduler yields None");
    submit(&scheduler, "a", Priority::Normal, Arc::clone(&marker), 7);
    let item = scheduler.pull().expect("submitted item is available");
    (item.run)();
    assert_eq!(marker.load(Ordering::Relaxed), 7);
    assert!(scheduler.try_pull().is_none(), "drained scheduler is empty");
}

#[tokio::test]
async fn account_isolation_does_not_reorder_lanes() {
    // Two accounts, same lane. FIFO within the lane.
    let scheduler = make_scheduler(64);
    let marker = Arc::new(AtomicU64::new(0));
    submit(&scheduler, "a", Priority::Normal, Arc::clone(&marker), 1);
    submit(&scheduler, "b", Priority::Normal, Arc::clone(&marker), 2);

    let first = scheduler.pull_next().await;
    let second = scheduler.pull_next().await;
    assert_eq!(first.account, AccountId("a".into()));
    assert_eq!(second.account, AccountId("b".into()));
}
