//! Per-lane FIFO queue primitive.
//!
//! Each lane is an independent unbounded `Mutex<VecDeque>`. The
//! scheduler's strict-priority `pull` walks the four lanes in priority
//! order, with the starvation floor diverting one pull to the lower
//! lanes every N pulls.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{AccountId, Priority};

/// Work item submitted to the scheduler. Kept deliberately small;
/// the actual work lives behind the boxed thunk.
pub struct WorkItem {
    pub account: AccountId,
    pub priority: Priority,
    pub kind: WorkKind,
    pub run: Box<dyn FnOnce() + Send + 'static>,
}

impl std::fmt::Debug for WorkItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkItem")
            .field("account", &self.account)
            .field("priority", &self.priority)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// Coarse work-kind tag used by `BudgetGate` to pick between the
/// mutation and sync sub-pools.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkKind {
    Sync,
    Mutation,
}

/// Shed policy when a lane is full. `DropOldest` is the v1 default
/// (matching the original behavior); `DropNewest` rejects the
/// incoming submission. Both increment the shed counter for
/// observability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LaneShedPolicy {
    DropOldest,
    DropNewest,
}

impl Default for LaneShedPolicy {
    fn default() -> Self {
        Self::DropOldest
    }
}

/// Per-lane FIFO queue.
#[derive(Debug)]
pub struct LaneQueue {
    priority: Priority,
    capacity: usize,
    queue: Mutex<VecDeque<WorkItem>>,
    shed_count: AtomicU64,
    shed_policy: LaneShedPolicy,
}

impl LaneQueue {
    /// Default cap if no `EngineConfig::lane_capacity` is supplied.
    pub const DEFAULT_CAPACITY: usize = 1024;

    #[must_use]
    pub fn new(priority: Priority) -> Self {
        Self::with_capacity(priority, Self::DEFAULT_CAPACITY)
    }

    #[must_use]
    pub fn with_capacity(priority: Priority, capacity: usize) -> Self {
        Self {
            priority,
            capacity: capacity.max(1),
            queue: Mutex::new(VecDeque::new()),
            shed_count: AtomicU64::new(0),
            shed_policy: LaneShedPolicy::default(),
        }
    }

    /// Push at the tail. If the queue is at capacity, the oldest item
    /// is shed and the shed counter is incremented. Returning silently
    /// matches the contract for non-coalescing schedulers: the engine
    /// surfaces shed counters through observability, not back to the
    /// submitter.
    pub fn push(&self, item: WorkItem) {
        let mut g = self.queue.lock().expect("poisoned");
        if g.len() >= self.capacity {
            // Shed oldest. We log a Warning at the scheduler facade so
            // a single congested lane is visible without spamming on
            // every push.
            let _ = g.pop_front();
            self.shed_count.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                target: "bifrost.sync.scheduler",
                priority = ?self.priority,
                capacity = self.capacity,
                "lane full; shedding oldest work item"
            );
        }
        g.push_back(item);
    }

    pub fn try_pop(&self) -> Option<WorkItem> {
        let mut g = self.queue.lock().expect("poisoned");
        g.pop_front()
    }

    #[must_use]
    pub fn shed_count(&self) -> u64 {
        self.shed_count.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn snapshot(&self) -> LaneSnapshot {
        let g = self.queue.lock().expect("poisoned");
        LaneSnapshot {
            priority: self.priority,
            depth: g.len(),
        }
    }
}

/// Snapshot of a single lane's depth for observability.
#[derive(Debug, Clone, Copy)]
pub struct LaneSnapshot {
    pub priority: Priority,
    pub depth: usize,
}
