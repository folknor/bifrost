//! Backfill orchestrator.
//!
//! Backfill is the cold-start hydration pass. The partitioner plans
//! newest-first partitions (so a user sees recent mail before deep
//! history), the runner drives each partition, and the checkpointer
//! persists at every partition boundary.
//!
//! `BackfillPolicy` is configurable per account; the default is
//! `TimeWindowed` with exponentially-widening windows per
//! `bifrost-sync.md` -> Default partition policy.

pub mod checkpoint;
pub mod partitioner;
pub mod runner;

use std::sync::Arc;

use bifrost_types::CursorScope;
use tokio_util::sync::CancellationToken;

pub use checkpoint::BackfillCheckpointWriter;
pub use partitioner::{BackfillPolicy, BackfillStrategy, PartitionPlan, default_time_boundaries};
pub use runner::{BackfillRunner, LiveSupersedes};

/// Engine-side handle stashed in the per-account slot.
#[derive(Debug)]
pub struct BackfillHandle {
    pub cancel: CancellationToken,
    pub live_supersedes: Arc<LiveSupersedes>,
}

/// Backfill registry: tracks which scopes are mid-backfill, which have
/// completed, and the latest checkpoint per scope so resumption picks
/// up inside the partition that contains the checkpoint's progress
/// marker.
#[derive(Debug, Default)]
pub struct BackfillRegistry {
    inner: dashmap::DashMap<CursorScope, BackfillState>,
}

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum BackfillState {
    Pending,
    Running,
    Completed,
}

impl BackfillRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mark(&self, scope: CursorScope, state: BackfillState) {
        self.inner.insert(scope, state);
    }

    #[must_use]
    pub fn snapshot(&self, scope: &CursorScope) -> Option<BackfillState> {
        self.inner.get(scope).map(|r| *r.value())
    }
}
