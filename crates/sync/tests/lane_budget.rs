//! Lane-shedding and concurrency-budget math tests.
//!
//! `scheduler_priority.rs` pins lane ordering and the starvation
//! floor; these pin the two remaining scheduler-adjacent pieces:
//! bounded-lane overflow (DropOldest shedding + the shed counter) and
//! the `ConcurrencyBudget` permit arithmetic / validation the engine
//! builder relies on.

use bifrost_sync::ConcurrencyBudget;
use bifrost_sync::scheduler::lanes::{LaneQueue, WorkItem, WorkKind};
use bifrost_types::{AccountId, Priority};

fn item(tag: &str) -> WorkItem {
    WorkItem {
        account: AccountId(tag.into()),
        priority: Priority::Normal,
        kind: WorkKind::Sync,
        run: Box::new(|| {}),
    }
}

#[test]
fn lane_overflow_sheds_the_oldest_item() {
    let lane = LaneQueue::with_capacity(Priority::Normal, 2);
    lane.push(item("first"));
    lane.push(item("second"));
    assert_eq!(lane.shed_count(), 0);

    // Third push overflows: default policy DropOldest evicts "first".
    lane.push(item("third"));
    assert_eq!(lane.shed_count(), 1);

    let a = lane.try_pop().expect("one item");
    let b = lane.try_pop().expect("two items");
    assert!(lane.try_pop().is_none());
    assert_eq!(a.account, AccountId("second".into()));
    assert_eq!(b.account, AccountId("third".into()));
}

#[test]
fn lane_capacity_floor_is_one() {
    // with_capacity(_, 0) clamps to 1 rather than a zero-capacity
    // queue that would shed everything.
    let lane = LaneQueue::with_capacity(Priority::Bulk, 0);
    lane.push(item("only"));
    assert_eq!(lane.shed_count(), 0);
    assert!(lane.try_pop().is_some());
}

#[test]
fn snapshot_reports_depth() {
    let lane = LaneQueue::with_capacity(Priority::Foreground, 8);
    lane.push(item("a"));
    lane.push(item("b"));
    let snap = lane.snapshot();
    assert_eq!(snap.depth, 2);
    assert_eq!(snap.priority, Priority::Foreground);
}

#[test]
fn default_budget_reserves_a_quarter_for_mutations() {
    let budget = ConcurrencyBudget::default();
    assert_eq!(budget.per_account, 8);
    // 8 * 1/4 = 2, rounded up from an exact division.
    assert_eq!(budget.mutation_permits(), 2);
    assert_eq!(budget.sync_permits(), 6);
    budget.validate().expect("default budget is valid");
}

#[test]
fn mutation_permits_round_up_and_never_drop_below_one() {
    // ceil(5 * 1/4) = 2.
    let budget = ConcurrencyBudget {
        per_account: 5,
        global: 64,
        mutation_share_num: 1,
        mutation_share_den: 4,
    };
    assert_eq!(budget.mutation_permits(), 2);
    assert_eq!(budget.sync_permits(), 3);

    // Tiny account pool still computes the split, but validation
    // rejects it because no sync permit remains.
    let budget = ConcurrencyBudget {
        per_account: 1,
        global: 64,
        mutation_share_num: 1,
        mutation_share_den: 100,
    };
    assert_eq!(budget.mutation_permits(), 1);
    assert_eq!(budget.sync_permits(), 0);
    assert!(budget.validate().is_err());
}

#[test]
fn mutation_share_numerator_clamps_to_denominator() {
    // num > den would over-allocate; the accessor clamps to 100%.
    let budget = ConcurrencyBudget {
        per_account: 8,
        global: 64,
        mutation_share_num: 9,
        mutation_share_den: 4,
    };
    assert_eq!(budget.mutation_permits(), 8);
    assert_eq!(budget.sync_permits(), 0);
    assert!(budget.validate().is_err());
}

#[test]
fn validate_rejects_every_zero_field() {
    let base = ConcurrencyBudget::default();
    let cases = [
        ConcurrencyBudget {
            per_account: 0,
            ..base
        },
        ConcurrencyBudget {
            per_account: 1,
            ..base
        },
        ConcurrencyBudget { global: 0, ..base },
        ConcurrencyBudget {
            mutation_share_num: 0,
            ..base
        },
        ConcurrencyBudget {
            mutation_share_den: 0,
            ..base
        },
    ];
    for (idx, budget) in cases.iter().enumerate() {
        assert!(budget.validate().is_err(), "case {idx} must be rejected");
    }
}
