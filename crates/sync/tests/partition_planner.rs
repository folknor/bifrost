//! Backfill window planner tests, through the published planners.
//!
//! Covers the three window shapes plus deterministic boundary chaining
//! for the time-windowed default.

use bifrost_sync::backfill::{default_time_boundaries, page_windows, time_windows, uid_windows};
use bifrost_types::{InventoryPartition, PageWindow, TimeWindow, UidWindow};
use jiff::Timestamp;
use jiff::civil::date;

fn fixed_now() -> Timestamp {
    date(2026, 5, 21)
        .at(12, 0, 0, 0)
        .in_tz("UTC")
        .unwrap()
        .timestamp()
}

fn time(partition: &InventoryPartition) -> TimeWindow {
    match partition {
        InventoryPartition::Time(window) => *window,
        other => panic!("expected a time partition, got {other:?}"),
    }
}

#[test]
fn default_time_partitions_chain_back_to_back() {
    let plan = time_windows(&default_time_boundaries(), fixed_now());
    let n_boundaries = default_time_boundaries().len();
    // n_boundaries explicit partitions + 1 open-ended tail.
    assert_eq!(plan.len(), n_boundaries + 1);
    for window in plan.windows(2) {
        assert_eq!(
            time(&window[0]).from(),
            time(&window[1]).to(),
            "partitions must chain back-to-back"
        );
    }
}

#[test]
fn uid_range_plan_is_newest_first_and_complete() {
    let plan = uid_windows(1000, 4500, 1);
    // 4500 / 1000 = 4.5 -> 5 partitions, newest first
    assert_eq!(plan.len(), 5);
    assert_eq!(
        plan.first(),
        Some(&InventoryPartition::Uid(
            UidWindow::new(1, 3501, 4501).unwrap()
        ))
    );
    assert_eq!(
        plan.last(),
        Some(&InventoryPartition::Uid(UidWindow::new(1, 1, 501).unwrap()))
    );
}

#[test]
fn page_count_plan_walks_in_order() {
    let page = |from, to| InventoryPartition::Page(PageWindow::new(from, to).unwrap());
    assert_eq!(
        page_windows(100, 350),
        vec![page(0, 100), page(100, 200), page(200, 300), page(300, 350)]
    );
}

/// Server clock skew is applied by shifting the planning instant: every
/// bounded edge moves by exactly the skew, and the open tail stays open.
#[test]
fn a_shifted_now_shifts_every_bounded_edge() {
    let skew = jiff::SignedDuration::from_hours(1); // server one hour ahead
    let no_skew = time_windows(&default_time_boundaries(), fixed_now());
    let with_skew = time_windows(
        &default_time_boundaries(),
        fixed_now().checked_add(skew).unwrap(),
    );
    assert_eq!(no_skew.len(), with_skew.len());
    for (lhs, rhs) in no_skew.iter().zip(with_skew.iter()) {
        let (lhs, rhs) = (time(lhs), time(rhs));
        assert_eq!(rhs.to().unwrap() - lhs.to().unwrap(), 3600);
        match (lhs.from(), rhs.from()) {
            (Some(f0), Some(f1)) => assert_eq!(f1 - f0, 3600),
            (None, None) => {}
            other => panic!("open edges must stay open: {other:?}"),
        }
    }
}

#[test]
fn small_total_uid_yields_one_partition() {
    assert_eq!(
        uid_windows(5000, 12, 1),
        vec![InventoryPartition::Uid(UidWindow::new(1, 1, 13).unwrap())]
    );
}

#[test]
fn empty_total_yields_no_partitions() {
    assert!(uid_windows(100, 0, 1).is_empty());
}
