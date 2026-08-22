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
//! broadcast as an `ObjectChange::Created` signal before the cursor
//! is persisted, so cold-start inventory (Graph all scopes,
//! IMAP-Basic / CONDSTORE-only folders) does not vanish.

use std::sync::Arc;

use bifrost_types::{
    Account, AccountError, Change, Checkpoint, CursorScope, InventoryEntry, ObjectChange,
    ObjectChangeKind, SyncEvent,
};
use futures::stream::StreamExt;
use tokio::sync::broadcast;

use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
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
    /// Inventory terminated with an account error. Carries the full
    /// `AccountError` so the caller dispatches through
    /// `error.recovery()`.
    Terminated(AccountError),
}

pub struct InventoryFusion {
    pub account_id: bifrost_types::AccountId,
    pub cursors: Arc<CursorRegistry>,
    pub control: Option<SyncControl>,
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
        let stream = account.inventory_stream(scope.clone());
        self.run_stream(scope, stream, changes_tx).await
    }

    /// Resume a provider-owned inventory page cursor. The account returns
    /// `None` for a cursor it does not own, so a stale or cross-provider row
    /// cannot be accidentally reinterpreted as a fresh inventory walk.
    pub async fn run_resume_with_broadcast(
        &self,
        account: &dyn Account,
        cursor: bifrost_types::ChangeCursor,
        changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
    ) -> Result<FusionOutcome, Error> {
        let scope = cursor.scope.clone();
        let Some(stream) = account.inventory_resume_stream(cursor) else {
            return Ok(FusionOutcome::NoCursor);
        };
        self.run_stream(scope, stream, changes_tx).await
    }

    async fn run_stream(
        &self,
        scope: CursorScope,
        mut stream: bifrost_types::AccountStream<SyncEvent<InventoryEntry>>,
        changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
    ) -> Result<FusionOutcome, Error> {
        let _activity = match &self.control {
            Some(control) => Some(control.begin_activity().ok_or(Error::Paused)?),
            None => None,
        };
        while let Some(event) = stream.next().await {
            match event {
                SyncEvent::Done(checkpoint) => {
                    if let (Some(tx), Some(cp)) = (&changes_tx, checkpoint.clone()) {
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Done(Some(cp.clone()))),
                            checkpoint: Some(cp.clone()),
                        };
                        // Register before publishing so a fast consumer
                        // ack cannot land before the entry exists and
                        // leave it outstanding forever.
                        if let Some(control) = &self.control {
                            control.expect_checkpoint(cp.clone());
                        }
                        let delivered = tx.send(me).unwrap_or(0);
                        if delivered <= 1
                            && let Some(control) = &self.control
                        {
                            control.retire_checkpoint(&cp);
                        }
                    }
                    return self.finalize(scope, checkpoint).await;
                }
                SyncEvent::Terminated(err) => {
                    if let Some(tx) = &changes_tx {
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Terminated(err.clone())),
                            checkpoint: None,
                        };
                        let _ = tx.send(me);
                    }
                    return Ok(FusionOutcome::Terminated(err));
                }
                SyncEvent::Batch(batch) => {
                    if let Some(tx) = &changes_tx {
                        self.forward_inventory_batch(tx, &scope, &batch);
                    }
                }
                SyncEvent::Progress(_) | SyncEvent::Warning(_) => {}
                _ => {}
            }
        }
        Ok(FusionOutcome::NoCursor)
    }

    fn forward_inventory_batch(
        &self,
        tx: &broadcast::Sender<MultiplexerEvent>,
        scope: &CursorScope,
        batch: &bifrost_types::Batch<InventoryEntry>,
    ) {
        // Inventory entries describe object existence; surface them as
        // `Created` changes so consumers handle inventory the same way
        // they handle a freshly-discovered object on the live stream.
        let changes: Vec<Change> = batch
            .items
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
            page_boundary: batch.page_boundary,
            server_latency: batch.server_latency,
            bytes_in: batch.bytes_in,
            checkpoint: batch.checkpoint.clone(),
        };
        let me = MultiplexerEvent {
            scope: scope.clone(),
            event: Arc::new(SyncEvent::Batch(synthetic)),
            checkpoint: batch.checkpoint.clone(),
        };
        if let (Some(control), Some(checkpoint)) = (&self.control, batch.checkpoint.as_ref()) {
            control.expect_checkpoint(checkpoint.clone());
        }
        let delivered = tx.send(me).unwrap_or(0);
        if delivered <= 1
            && let (Some(control), Some(checkpoint)) = (&self.control, batch.checkpoint.as_ref())
        {
            control.retire_checkpoint(checkpoint);
        }
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
}
