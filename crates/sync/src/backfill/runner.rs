//! Backfill partition runner.
//!
//! Walks `inventory_partition_stream(scope)` and forwards each `Batch`
//! as a `MultiplexerEvent` carrying `Change::ObjectChange::Created`
//! items plus a `BackfillCheckpoint` checkpoint.
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

use std::sync::Arc;

use bifrost_types::{
    Account, BackfillCheckpoint, BackfillProgress, Batch, Change, Checkpoint, CursorScope,
    InventoryPartition, ObjectChange, ObjectChangeKind, SyncEvent,
};
use futures::stream::StreamExt;
use tokio::sync::broadcast;

use crate::control::SyncControl;
use crate::error::Error;
use crate::multiplexer::MultiplexerEvent;

use super::partitioner::partition_key;

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
    /// `Change::ObjectChange::Created`, each page carrying a
    /// `BackfillCheckpoint`.
    /// Does NOT write to `CheckpointStore`: durable advance is
    /// consumer-ack-driven (see module docs). Checkpoint progress counts
    /// entries observed before filtering because it describes the
    /// inventory position, while the returned outcome reports both the
    /// observed and forwarded totals.
    pub async fn run_partition(
        account: &dyn Account,
        scope: CursorScope,
        partition: InventoryPartition,
        changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
        envelope_version: u32,
        control: Option<&SyncControl>,
    ) -> Result<BackfillPartitionOutcome, Error> {
        let _activity = match control {
            Some(control) => Some(control.begin_activity().ok_or(Error::Paused)?),
            None => None,
        };
        let partition_key = partition_key(&partition);
        let mut stream = account.inventory_partition_stream(scope.clone(), partition);
        let mut seen_total: u64 = 0;
        let mut kept_total: u64 = 0;
        while let Some(event) = stream.next().await {
            match event {
                SyncEvent::Batch(batch) => {
                    let seen = u64::try_from(batch.items.len()).unwrap_or(u64::MAX);
                    seen_total = seen_total.saturating_add(seen);
                    let kept = &batch.items;
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
                                items_done: seen_total,
                                items_estimated: None,
                            },
                            envelope_version,
                        };
                        let changes: Vec<Change> = kept
                            .iter()
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
                            checkpoint: Some(Checkpoint::Backfill(bf.clone())),
                        };
                        // Register before publishing so a fast consumer
                        // ack cannot land before the entry exists and
                        // leave it outstanding forever.
                        let expected = Checkpoint::Backfill(bf);
                        if let Some(control) = control {
                            control.expect_checkpoint(expected.clone());
                        }
                        let delivered = tx.send(me).unwrap_or(0);
                        if !crate::multiplexer::delivered_to_real_subscriber(delivered)
                            && let Some(control) = control
                        {
                            control.retire_checkpoint(&expected);
                        }
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
