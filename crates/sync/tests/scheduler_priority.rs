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

    let first = scheduler.pull().await.expect("first item");
    assert_eq!(first.priority, Priority::Foreground);
    let second = scheduler.pull().await.expect("second item");
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
    while let Some(item) = scheduler.pull().await {
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
    let mut step: u32 = 0;
    while let Some(item) = scheduler.pull().await {
        if item.priority == Priority::Background && saw_background_at.is_none() {
            saw_background_at = Some(step);
        }
        step += 1;
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
async fn empty_scheduler_pulls_none() {
    let scheduler = make_scheduler(64);
    assert!(scheduler.pull().await.is_none());
}

#[tokio::test]
async fn account_isolation_does_not_reorder_lanes() {
    // Two accounts, same lane. FIFO within the lane.
    let scheduler = make_scheduler(64);
    let marker = Arc::new(AtomicU64::new(0));
    submit(&scheduler, "a", Priority::Normal, Arc::clone(&marker), 1);
    submit(&scheduler, "b", Priority::Normal, Arc::clone(&marker), 2);

    let first = scheduler.pull().await.expect("first");
    let second = scheduler.pull().await.expect("second");
    assert_eq!(first.account, AccountId("a".into()));
    assert_eq!(second.account, AccountId("b".into()));
}
