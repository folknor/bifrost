//! Four-lane scheduler.
//!
//! The scheduler is a *gate*, not an executor. Tokio's runtime executes
//! futures; the scheduler decides which `Account` operation gets to
//! acquire concurrency tokens, in what order. Lanes are
//! `Foreground` > `Normal` > `Background` > `Bulk` with a starvation
//! floor: after N consecutive higher-lane pulls, the puller takes one
//! from a lower lane regardless.
//!
//! Concurrency caps are enforced by `budget::BudgetGate` (a layered
//! `tokio::sync::Semaphore` pair: per-account and global).
//!
//! ## Scope status (v1)
//!
//! The scheduler and budget gate are CURRENTLY NOT WIRED into the
//! engine's production work paths. Multiplexer, backfill, and
//! mutation tasks acquire from `Account::*_stream` directly. The
//! scheduler exists as infrastructure for a follow-up pass that
//! threads every protocol call through `Scheduler::submit` and a
//! `BudgetGate::acquire`. Until then the public re-exports of
//! `Scheduler` / `BudgetGate` are intentionally absent from
//! `bifrost-sync`'s lib.rs.

pub mod budget;
pub mod lanes;

pub use budget::{BudgetGate, BudgetPermit, ConcurrencyBudget};
pub use lanes::{LaneQueue, LaneSnapshot, WorkItem, WorkKind};

use std::sync::{Arc, Mutex};

use bifrost_types::Priority;

use crate::types::SchedulerConfig;

/// Public scheduler handle.
///
/// Holds one `LaneQueue` per priority plus the starvation counter and
/// the budget gate. `submit` and `pull` are the only public methods;
/// workers call `pull().await` in a loop and run the popped `WorkItem`s.
#[derive(Debug, Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerInner>,
}

#[derive(Debug)]
struct SchedulerInner {
    foreground: LaneQueue,
    normal: LaneQueue,
    background: LaneQueue,
    bulk: LaneQueue,
    starvation_floor: u32,
    state: Mutex<SchedulerState>,
    budget: BudgetGate,
}

#[derive(Debug, Default)]
struct SchedulerState {
    /// Count of consecutive Foreground/Normal items pulled. Reset
    /// whenever a Background or Bulk item is pulled. When the count
    /// hits `starvation_floor` the next `pull` is forced to take from
    /// the lower lanes.
    higher_consecutive: u32,
}

impl Scheduler {
    #[must_use]
    pub fn new(cfg: SchedulerConfig, budget: BudgetGate) -> Self {
        Self::with_lane_capacity(cfg, budget, LaneQueue::DEFAULT_CAPACITY)
    }

    /// Construct with an explicit per-lane capacity. See `H4` in the
    /// engine notes for why lane shedding matters under push storms.
    #[must_use]
    pub fn with_lane_capacity(
        cfg: SchedulerConfig,
        budget: BudgetGate,
        lane_capacity: usize,
    ) -> Self {
        Self {
            inner: Arc::new(SchedulerInner {
                foreground: LaneQueue::with_capacity(Priority::Foreground, lane_capacity),
                normal: LaneQueue::with_capacity(Priority::Normal, lane_capacity),
                background: LaneQueue::with_capacity(Priority::Background, lane_capacity),
                bulk: LaneQueue::with_capacity(Priority::Bulk, lane_capacity),
                starvation_floor: cfg.starvation_floor,
                state: Mutex::new(SchedulerState::default()),
                budget,
            }),
        }
    }

    pub fn submit(&self, item: WorkItem) {
        self.lane(item.priority).push(item);
    }

    /// Strict-priority pull with starvation floor. Returns `None` if
    /// every lane is empty.
    ///
    /// This is non-blocking and synchronous; the worker decides
    /// whether to back off or `yield_now` when the scheduler is
    /// empty. Workers normally pair this with a notification source
    /// so they wake on `submit`.
    pub fn pull(&self) -> Option<WorkItem> {
        let mut state = self.inner.state.lock().expect("poisoned");
        // If the starvation floor has been hit, take from the lowest
        // non-empty lane and reset.
        if state.higher_consecutive >= self.inner.starvation_floor {
            if let Some(item) = self.inner.bulk.try_pop() {
                state.higher_consecutive = 0;
                return Some(item);
            }
            if let Some(item) = self.inner.background.try_pop() {
                state.higher_consecutive = 0;
                return Some(item);
            }
            // Both lower lanes empty; fall through to normal priority
            // order. Reset so the floor doesn't fire on the very next
            // pull and starve Foreground.
            state.higher_consecutive = 0;
        }

        if let Some(item) = self.inner.foreground.try_pop() {
            state.higher_consecutive = state.higher_consecutive.saturating_add(1);
            return Some(item);
        }
        if let Some(item) = self.inner.normal.try_pop() {
            state.higher_consecutive = state.higher_consecutive.saturating_add(1);
            return Some(item);
        }
        if let Some(item) = self.inner.background.try_pop() {
            state.higher_consecutive = 0;
            return Some(item);
        }
        if let Some(item) = self.inner.bulk.try_pop() {
            state.higher_consecutive = 0;
            return Some(item);
        }
        None
    }

    /// Snapshot lane depths for observability.
    #[must_use]
    pub fn snapshot(&self) -> [LaneSnapshot; 4] {
        [
            self.inner.foreground.snapshot(),
            self.inner.normal.snapshot(),
            self.inner.background.snapshot(),
            self.inner.bulk.snapshot(),
        ]
    }

    #[must_use]
    pub fn budget(&self) -> BudgetGate {
        self.inner.budget.clone()
    }

    fn lane(&self, priority: Priority) -> &LaneQueue {
        match priority {
            Priority::Foreground => &self.inner.foreground,
            Priority::Normal => &self.inner.normal,
            Priority::Background => &self.inner.background,
            Priority::Bulk => &self.inner.bulk,
            // `Priority` is `#[non_exhaustive]`; any future variant
            // falls through to the normal lane until the scheduler is
            // updated to handle it explicitly.
            _ => &self.inner.normal,
        }
    }
}
