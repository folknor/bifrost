//! Inventory-fusion path.
//!
//! For `EstablishViaInventory` scopes (Graph all scopes,
//! IMAP-Basic / IMAP-CONDSTORE-only folders) the inventory walk IS the
//! cursor establishment. The multiplexer consumes the inventory stream
//! and registers the cursor exactly once, on the inventory's terminal
//! `Done` event. Mid-stream `Batch` checkpoints (if any) advance the
//! inventory progress marker only; the change cursor is established in
//! memory when the inventory's terminal `Done` arrives. Durable cursor
//! persistence still waits for the consumer ack of the terminal
//! checkpoint.
//!
//! Inventory data: every `Batch` is forwarded to the per-account
//! broadcast as a `ScopeChange::Added` membership signal before the
//! cursor is persisted, so cold-start inventory (Graph all scopes,
//! IMAP-Basic / CONDSTORE-only folders) does not vanish.

use std::sync::Arc;

use bifrost_types::{
    Account, Change, Checkpoint, CursorScope, InventoryEntry, ObjectChange, ObjectChangeKind,
    PageBoundary, RecoveryClass, SyncEvent,
};
use futures::stream::StreamExt;
use tokio::sync::broadcast;

use crate::cursor::CursorRegistry;
use crate::cursor::store::DynCheckpointStore;
use crate::error::Error;

use super::MultiplexerEvent;

/// Outcome of an inventory fusion pass.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum FusionOutcome {
    /// Inventory completed and the in-memory cursor was established.
    Established,
    /// Inventory completed without yielding a cursor; the engine emits
    /// a Warning.
    NoCursor,
    /// Inventory ended in `Fatal`.
    Fatal(RecoveryClass),
}

pub struct InventoryFusion {
    pub account_id: bifrost_types::AccountId,
    pub cursors: Arc<CursorRegistry>,
    pub store: Arc<DynCheckpointStore>,
}

impl InventoryFusion {
    /// Drive an `inventory_stream(scope)` to completion. On the
    /// terminal `Done`, register the cursor carried in
    /// its checkpoint. Mid-stream batches are ignored for the purpose
    /// of cursor establishment, but see `run_with_broadcast` for the
    /// path that forwards inventory data to subscribers.
    pub async fn run(
        &self,
        account: &dyn Account,
        scope: CursorScope,
    ) -> Result<FusionOutcome, Error> {
        self.run_with_broadcast(account, scope, None).await
    }

    /// Same contract as `run`, but each `Batch` of inventory entries
    /// is fanned out to `changes_tx` as `Change::ObjectChange::Created`
    /// events BEFORE the terminal cursor is acknowledged. This is the
    /// only path inventory data flows to consumers for
    /// `EstablishViaInventory` scopes - dropping it would lose every
    /// Graph object and every IMAP-Basic / CONDSTORE-only folder's
    /// cold-start contents.
    pub async fn run_with_broadcast(
        &self,
        account: &dyn Account,
        scope: CursorScope,
        changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
    ) -> Result<FusionOutcome, Error> {
        let mut stream = account.inventory_stream(scope.clone());
        while let Some(event) = stream.next().await {
            match event {
                SyncEvent::Done(checkpoint) => {
                    if let (Some(tx), Some(cp)) = (&changes_tx, checkpoint.clone()) {
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Done(Some(cp.clone()))),
                            checkpoint: Some(cp),
                        };
                        let _ = tx.send(me);
                    }
                    return self.finalize(scope, checkpoint).await;
                }
                SyncEvent::Fatal(f) => {
                    let recovery = f.recovery.clone();
                    if let Some(tx) = &changes_tx {
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Fatal(f)),
                            checkpoint: None,
                        };
                        let _ = tx.send(me);
                    }
                    return Ok(FusionOutcome::Fatal(recovery));
                }
                SyncEvent::Batch(batch) => {
                    if let Some(tx) = &changes_tx {
                        Self::forward_inventory_batch(
                            tx,
                            &scope,
                            &batch.items,
                            batch.page_boundary,
                            batch.server_latency,
                            batch.bytes_in,
                        );
                    }
                }
                SyncEvent::Progress(_) | SyncEvent::Warning(_) => {}
                _ => {}
            }
        }
        Ok(FusionOutcome::NoCursor)
    }

    fn forward_inventory_batch(
        tx: &broadcast::Sender<MultiplexerEvent>,
        scope: &CursorScope,
        items: &[InventoryEntry],
        page_boundary: PageBoundary,
        server_latency: std::time::Duration,
        bytes_in: u64,
    ) {
        // Inventory entries describe object existence; surface them as
        // `Created` changes so consumers handle inventory the same way
        // they handle a freshly-discovered object on the live stream.
        // Note checkpoint is intentionally None: cursor establishment
        // is signaled only at the inventory's terminal Done.
        let changes: Vec<Change> = items
            .iter()
            .map(|entry| {
                Change::ObjectChange(ObjectChange {
                    id: entry.id.clone(),
                    kind: ObjectChangeKind::Created,
                })
            })
            .collect();
        let synthetic = bifrost_types::Batch {
            items: changes,
            page_boundary,
            server_latency,
            bytes_in,
            checkpoint: None,
        };
        let me = MultiplexerEvent {
            scope: scope.clone(),
            event: Arc::new(SyncEvent::Batch(synthetic)),
            checkpoint: None,
        };
        let _ = tx.send(me);
    }

    async fn finalize(
        &self,
        expected_scope: CursorScope,
        checkpoint: Option<Checkpoint>,
    ) -> Result<FusionOutcome, Error> {
        let Some(Checkpoint::Change(cursor)) = checkpoint else {
            return Ok(FusionOutcome::NoCursor);
        };
        if cursor.scope != expected_scope {
            return Err(Error::Other(format!(
                "inventory Done returned cursor for the wrong scope: expected {:?}, got {:?}",
                expected_scope, cursor.scope,
            )));
        }
        self.cursors.put(cursor);
        Ok(FusionOutcome::Established)
    }

    /// Convenience: count entries observed in an inventory stream.
    /// Used by tests to assert backfill partition math.
    #[allow(dead_code)]
    pub async fn count_entries(
        &self,
        account: &dyn Account,
        scope: CursorScope,
    ) -> Result<u64, Error> {
        let mut stream = account.inventory_stream(scope);
        let mut total: u64 = 0;
        while let Some(event) = stream.next().await {
            if let SyncEvent::Batch(b) = event {
                total = total.saturating_add(count(&b.items));
            }
        }
        Ok(total)
    }
}

fn count(items: &[InventoryEntry]) -> u64 {
    u64::try_from(items.len()).unwrap_or(u64::MAX)
}
