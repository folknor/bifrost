//! Backfill partition runner.
//!
//! Walks a `PartitionPlan` newest-first, running each partition's
//! inventory pass through `Account::inventory_stream`. Interleaves
//! cleanly with the live `changes_stream` via the `LiveSupersedes`
//! set: when the live stream reports `Created(id)` for an item the
//! runner has not yet seen, the id is recorded and the runner skips
//! the corresponding inventory entry when it eventually walks past
//! it.

use std::collections::HashSet;
use std::sync::Mutex;

use bifrost_types::{Account, InventoryEntry, ObjectId, SyncEvent};
use futures::stream::StreamExt;

use crate::error::Error;

/// Side channel from the multiplexer to the backfill runner. Live
/// changes can pre-empt inventory entries.
#[derive(Debug, Default)]
pub struct LiveSupersedes {
    inner: Mutex<HashSet<ObjectId>>,
}

impl LiveSupersedes {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark an id as superseded by the live stream. Subsequent
    /// backfill passes will skip it.
    pub fn add(&self, id: ObjectId) {
        let mut g = self.inner.lock().expect("poisoned");
        g.insert(id);
    }

    /// True if the id has been superseded. Removes the entry on hit
    /// so the set does not grow unboundedly.
    #[must_use]
    pub fn take(&self, id: &ObjectId) -> bool {
        let mut g = self.inner.lock().expect("poisoned");
        g.remove(id)
    }

    /// Snapshot for tests.
    #[must_use]
    pub fn len(&self) -> usize {
        let g = self.inner.lock().expect("poisoned");
        g.len()
    }

    /// Convenience: true iff `len() == 0`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One partition's runner.
pub struct BackfillRunner;

impl BackfillRunner {
    /// Walk one inventory pass to completion. Skips ids in the
    /// `LiveSupersedes` set. Returns the count of entries kept.
    ///
    /// The caller is responsible for handing the runner a fresh
    /// inventory stream parameterized to the partition (the protocol
    /// crate's `inventory_stream(scope)` covers the scope wholesale;
    /// partition slicing is the engine's affair).
    pub async fn run_partition(
        account: &dyn Account,
        scope: bifrost_types::CursorScope,
        live: &LiveSupersedes,
    ) -> Result<u64, Error> {
        let mut stream = account.inventory_stream(scope);
        let mut kept: u64 = 0;
        while let Some(event) = stream.next().await {
            if let SyncEvent::Batch(batch) = event {
                kept = kept.saturating_add(count_kept(&batch.items, live));
            }
        }
        Ok(kept)
    }
}

fn count_kept(items: &[InventoryEntry], live: &LiveSupersedes) -> u64 {
    let mut kept = 0u64;
    for entry in items {
        if !live.take(&entry.id) {
            kept = kept.saturating_add(1);
        }
    }
    kept
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::ObjectId;

    #[test]
    fn supersedes_records_and_consumes() {
        let live = LiveSupersedes::new();
        let id = ObjectId("abc".into());
        live.add(id.clone());
        assert_eq!(live.len(), 1);
        assert!(live.take(&id));
        assert!(live.is_empty());
        assert!(!live.take(&id));
    }
}
