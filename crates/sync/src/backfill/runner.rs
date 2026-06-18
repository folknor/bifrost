//! Backfill partition runner.
//!
//! Walks `inventory_partition_stream(scope)` and forwards each `Batch`
//! as a `MultiplexerEvent` carrying `Change::ObjectChange::Created`
//! items plus a `BackfillCheckpoint` checkpoint. The runner uses
//! `LiveSupersedes` to skip ids the live `changes_stream` has already
//! announced.
//!
//! ## Checkpoint-durability contract
//!
//! Durable persistence is consumer-ack-deferred, mirroring
//! `drive_changes_stream`. The runner broadcasts each page and its
//! `BackfillCheckpoint` but does NOT write to `CheckpointStore`. The
//! consumer atomically persists the page items in their own store, then
//! calls `SyncEngine::ack_checkpoint` with the `Checkpoint::Backfill`;
//! the per-account ack writer routes that to
//! `CheckpointStore::put_backfill` and fires `record_checkpoint` to wake
//! pause / checkpoint waiters on a durable boundary.
//!
//! This closes the at-least-once gap an eager engine-side write would
//! open: a crash between broadcast and the consumer's write re-walks the
//! partition on restart (the orchestrator always restarts a scope from
//! its first partition), so no inventory page is lost. An eager write
//! would let the store record progress the consumer never durably
//! persisted, permanently losing that page's objects on cold start.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use bifrost_types::{
    Account, BackfillCheckpoint, BackfillProgress, Batch, Change, Checkpoint, CursorScope,
    InventoryEntry, InventoryPartition, ObjectChange, ObjectChangeKind, ObjectId, SyncEvent,
};
use futures::stream::StreamExt;
use tokio::sync::broadcast;

use crate::error::Error;
use crate::multiplexer::MultiplexerEvent;

use super::partitioner::partition_key;

/// Default cap on `LiveSupersedes`. Beyond this many entries, the
/// oldest insertions are evicted via a FIFO ring so the set does not
/// grow unboundedly in long-running sessions.
pub const LIVE_SUPERSEDES_DEFAULT_CAP: usize = 100_000;

/// Side channel from the multiplexer to the backfill runner. Live
/// `Created` changes pre-empt inventory entries so cold-start
/// hydration does not double-emit objects the live stream has already
/// shown.
///
/// The set is capped (default `LIVE_SUPERSEDES_DEFAULT_CAP`); on
/// overflow the oldest insertion is dropped. A 100k cap is the
/// engine's deliberate trade: enough to absorb a burst of live
/// `Created` events during cold-start without retaining state forever
/// for accounts that never need it.
#[derive(Debug)]
pub struct LiveSupersedes {
    inner: Mutex<LiveSupersedesInner>,
    cap: usize,
}

#[derive(Debug, Default)]
struct LiveSupersedesInner {
    /// Insertion order for ring eviction. Each id appears here exactly
    /// once for each call to `add` that did not collide; on overflow
    /// the front element is dropped from both `order` and `present`.
    order: VecDeque<ObjectId>,
    present: std::collections::HashSet<ObjectId>,
}

impl Default for LiveSupersedes {
    fn default() -> Self {
        Self::new()
    }
}

impl LiveSupersedes {
    /// Construct with the default cap.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(LIVE_SUPERSEDES_DEFAULT_CAP)
    }

    /// Construct with a specific cap. Useful for tests that want to
    /// observe ring-eviction behavior cheaply.
    #[must_use]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: Mutex::new(LiveSupersedesInner::default()),
            cap: cap.max(1),
        }
    }

    /// Mark an id as superseded by the live stream. Subsequent
    /// backfill passes will skip it. On overflow the oldest entry is
    /// dropped from the ring.
    pub fn add(&self, id: ObjectId) {
        let mut g = self.inner.lock().expect("poisoned");
        if g.present.contains(&id) {
            return;
        }
        if g.order.len() >= self.cap
            && let Some(old) = g.order.pop_front()
        {
            g.present.remove(&old);
        }
        g.present.insert(id.clone());
        g.order.push_back(id);
    }

    /// True if the id has been superseded. Removes the entry on hit
    /// so the set drains as backfill walks past entries it can skip.
    #[must_use]
    pub fn take(&self, id: &ObjectId) -> bool {
        let mut g = self.inner.lock().expect("poisoned");
        if g.present.remove(id) {
            // Best-effort removal from the order ring; absence here
            // is fine - eviction will skip stale entries.
            g.order.retain(|x| x != id);
            true
        } else {
            false
        }
    }

    /// Snapshot for tests.
    #[must_use]
    pub fn len(&self) -> usize {
        let g = self.inner.lock().expect("poisoned");
        g.present.len()
    }

    /// Convenience: true iff `len() == 0`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One partition's runner.
pub struct BackfillRunner;

/// Outcome for one partition pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackfillPartitionOutcome {
    /// Inventory entries observed before the live-supersedes filter.
    pub seen: u64,
    /// Inventory entries forwarded after the live-supersedes filter.
    pub kept: u64,
}

impl BackfillRunner {
    /// Walk one inventory pass to completion. Forwards each `Batch`
    /// onto the per-account `changes_tx` broadcast as
    /// `Change::ObjectChange::Created` (skipping ids in
    /// `LiveSupersedes`), each page carrying a `BackfillCheckpoint`.
    /// Does NOT write to `CheckpointStore`: durable advance is
    /// consumer-ack-driven (see module docs). Returns the total count of
    /// entries kept after the supersedes filter.
    pub async fn run_partition(
        account: &dyn Account,
        scope: CursorScope,
        partition: InventoryPartition,
        live: &LiveSupersedes,
        changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
        envelope_version: u32,
    ) -> Result<BackfillPartitionOutcome, Error> {
        let partition_key = partition_key(&partition);
        let mut stream = account.inventory_partition_stream(scope.clone(), partition);
        let mut seen_total: u64 = 0;
        let mut kept_total: u64 = 0;
        while let Some(event) = stream.next().await {
            match event {
                SyncEvent::Batch(batch) => {
                    let seen = u64::try_from(batch.items.len()).unwrap_or(u64::MAX);
                    seen_total = seen_total.saturating_add(seen);
                    let kept = filter_supersedes(&batch.items, live);
                    let kept_count = u64::try_from(kept.len()).unwrap_or(u64::MAX);
                    kept_total = kept_total.saturating_add(kept_count);

                    // Forward the page as a synthetic Batch carrying a
                    // BackfillCheckpoint. Durable persistence awaits the
                    // consumer ack (see module docs); the runner never
                    // writes to CheckpointStore.
                    if let Some(tx) = &changes_tx {
                        let bf = BackfillCheckpoint {
                            scope: scope.clone(),
                            partition: partition_key.clone(),
                            progress_marker: None,
                            progress: BackfillProgress {
                                items_done: kept_total,
                                items_estimated: None,
                            },
                            envelope_version,
                        };
                        let changes: Vec<Change> = kept
                            .into_iter()
                            .map(|entry| {
                                Change::ObjectChange(ObjectChange {
                                    id: entry.id.clone(),
                                    kind: ObjectChangeKind::Created,
                                })
                            })
                            .collect();
                        let synthetic = Batch {
                            items: changes,
                            page_boundary: batch.page_boundary,
                            server_latency: batch.server_latency,
                            bytes_in: batch.bytes_in,
                            checkpoint: Some(Checkpoint::Backfill(bf.clone())),
                        };
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Batch(synthetic)),
                            checkpoint: Some(Checkpoint::Backfill(bf)),
                        };
                        let _ = tx.send(me);
                    }
                }
                SyncEvent::Done(_) => break,
                SyncEvent::Terminated(err) => {
                    if let Some(tx) = &changes_tx {
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Terminated(err.clone())),
                            checkpoint: None,
                        };
                        let _ = tx.send(me);
                    }
                    return Err(Error::Account(err));
                }
                SyncEvent::Progress(_) | SyncEvent::Warning(_) => {}
                _ => {}
            }
        }
        Ok(BackfillPartitionOutcome {
            seen: seen_total,
            kept: kept_total,
        })
    }
}

fn filter_supersedes<'a>(
    items: &'a [InventoryEntry],
    live: &LiveSupersedes,
) -> Vec<&'a InventoryEntry> {
    items.iter().filter(|entry| !live.take(&entry.id)).collect()
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

    #[test]
    fn supersedes_caps_at_capacity() {
        let live = LiveSupersedes::with_capacity(3);
        live.add(ObjectId("a".into()));
        live.add(ObjectId("b".into()));
        live.add(ObjectId("c".into()));
        live.add(ObjectId("d".into()));
        assert_eq!(live.len(), 3);
        // "a" was evicted; "b","c","d" remain.
        assert!(!live.take(&ObjectId("a".into())));
        assert!(live.take(&ObjectId("b".into())));
        assert!(live.take(&ObjectId("c".into())));
        assert!(live.take(&ObjectId("d".into())));
    }
}
