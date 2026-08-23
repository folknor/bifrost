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
    Account, AccountError, Change, Checkpoint, CursorScope, ObjectChange, ObjectChangeKind,
    SyncEvent,
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
    /// Where this walk registers what it proved, so the durable writer can put
    /// coverage and cursor into one atomic record when the consumer
    /// acknowledges. `None` in contexts with no writer behind them.
    pub coverage: Option<Arc<crate::cursor::PendingCoverage>>,
    /// Durable writer, for barrier incidents. A barrier advances no cursor, so
    /// it has no checkpoint to ride and cannot reach the store through the
    /// acknowledgement path like everything else does.
    pub writer_tx: Option<tokio::sync::mpsc::Sender<super::WriterRequest>>,
    /// Engine-issued generation for this walk. Orders proof events even when
    /// two walks produce identical cursor bytes.
    pub generation: u64,
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
        mut stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent>,
        changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
    ) -> Result<FusionOutcome, Error> {
        let _activity = match &self.control {
            Some(control) => Some(control.begin_activity().ok_or(Error::Paused)?),
            None => None,
        };
        // The last checkpoint this walk published. If a later page turns out to
        // be a barrier, this is the position a future walk resumes from - it
        // certifies a prefix ending before the barrier region begins.
        let mut last_accepted: Option<Checkpoint> = None;
        while let Some(event) = stream.next().await {
            match event {
                bifrost_types::InventoryEvent::Done(completion) => {
                    if completion.coverage.has_barrier() {
                        // The walk ended on ground the cursor may not cross.
                        // Establishing here would advance past a region nothing
                        // can ever replay.
                        self.record_barriers(&scope, &completion.coverage, None)
                            .await;
                        Self::warn_degraded(&changes_tx, &scope, &completion.coverage);
                        return Ok(FusionOutcome::NoCursor);
                    }
                    let degraded = !completion.coverage.is_complete();
                    if degraded {
                        Self::warn_degraded(&changes_tx, &scope, &completion.coverage);
                    }
                    let checkpoint = completion.checkpoint;
                    if let (Some(tx), Some(cp)) = (&changes_tx, checkpoint.clone()) {
                        // Register the claim BEFORE publishing: a fast consumer
                        // can acknowledge between the send and a later
                        // registration, and the writer would then find no claim
                        // for a checkpoint that carried one.
                        let publication = self.publish_claim(&completion.coverage);
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Done(Some(cp.clone()))),
                            checkpoint: Some(cp.clone()),
                            publication,
                        };
                        // Register before publishing so a fast consumer
                        // ack cannot land before the entry exists and
                        // leave it outstanding forever.
                        if let Some(control) = &self.control {
                            control.expect_checkpoint(cp.clone());
                        }
                        let delivered = tx.send(me).unwrap_or(0);
                        if !super::delivered_to_real_subscriber(delivered) {
                            // Nothing can ever acknowledge it, so the claim
                            // would sit in the registry for the life of the
                            // attachment.
                            self.retire_claim(publication);
                            if let Some(control) = &self.control {
                                control.retire_checkpoint(&cp);
                            }
                        }
                    }
                    return self.finalize(scope, checkpoint).await;
                }
                bifrost_types::InventoryEvent::Terminated(err) => {
                    if let Some(tx) = &changes_tx {
                        let me = MultiplexerEvent {
                            scope: scope.clone(),
                            event: Arc::new(SyncEvent::Terminated(err.clone())),
                            checkpoint: None,
                            publication: None,
                        };
                        let _ = tx.send(me);
                    }
                    return Ok(FusionOutcome::Terminated(err));
                }
                bifrost_types::InventoryEvent::Batch(batch) => {
                    // A barrier TAINTS THE WALK. Refusing only this one
                    // checkpoint is not enough: the next page's checkpoint, or
                    // the terminal delta link, would simply leap over the same
                    // region. So the batch's items are still delivered - they
                    // were really seen - but the checkpoint is stripped and the
                    // walk stops here. Checkpoints already accepted earlier in
                    // this walk stand: each of them certifies a prefix that
                    // ends before this region begins.
                    if batch.coverage.has_barrier() {
                        self.record_barriers(&scope, &batch.coverage, last_accepted.clone())
                            .await;
                        if let Some(tx) = &changes_tx {
                            let mut stripped = batch;
                            stripped.checkpoint = None;
                            self.forward_inventory_batch(tx, &scope, &stripped);
                            Self::warn_degraded(&changes_tx, &scope, &stripped.coverage);
                        }
                        return Ok(FusionOutcome::NoCursor);
                    }
                    if batch.checkpoint.is_some() {
                        last_accepted = batch.checkpoint.clone();
                    }
                    if let Some(tx) = &changes_tx {
                        self.forward_inventory_batch(tx, &scope, &batch);
                    }
                }
                bifrost_types::InventoryEvent::Progress(_)
                | bifrost_types::InventoryEvent::Warning(_) => {}
                _ => {}
            }
        }
        Ok(FusionOutcome::NoCursor)
    }

    /// Register what this publication will make durable, and get its identity.
    ///
    /// `None` when there is no writer behind this fusion, in which case nothing
    /// can be acknowledged and nothing needs an identity.
    fn publish_claim(
        &self,
        coverage: &bifrost_types::InventoryCoverageReport,
    ) -> Option<crate::cursor::PublicationId> {
        self.coverage.as_ref().map(|pending| {
            pending.publish(crate::cursor::CoverageClaim::new(
                coverage.clone(),
                self.generation,
            ))
        })
    }

    fn retire_claim(&self, publication: Option<crate::cursor::PublicationId>) {
        if let (Some(pending), Some(id)) = (&self.coverage, publication) {
            pending.retire(id);
        }
    }

    /// Persist the blocked-progress incidents in `coverage`.
    ///
    /// A barrier is not debt behind an advanced cursor - no checkpoint was
    /// accepted past it - so it cannot ride the acknowledgement path. Writing
    /// nothing at all would be worse than it sounds: every restart would forget
    /// the scope keeps hitting the same wall, and an operator would have no
    /// durable object to waive.
    async fn record_barriers(
        &self,
        scope: &CursorScope,
        coverage: &bifrost_types::InventoryCoverageReport,
        resume_from: Option<Checkpoint>,
    ) {
        let Some(writer) = &self.writer_tx else {
            return;
        };
        for obligation in coverage.obligations() {
            let bifrost_types::InventoryObligation::Region {
                key,
                failure_label,
                error,
                recovery,
            } = obligation
            else {
                continue;
            };
            if !recovery.is_barrier() {
                continue;
            }
            let incident = crate::cursor::BarrierIncident {
                key: key.clone(),
                domain: coverage.domain.clone(),
                generation: self.generation,
                failure_label: failure_label.clone(),
                evidence: error.clone(),
                policy: crate::cursor::PolicyStatus::Retrying { attempts: 0 },
                resume_from: resume_from.clone(),
            };
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            if writer
                .send(super::WriterRequest::RecordBarrier {
                    incident,
                    done: done_tx,
                })
                .await
                .is_err()
            {
                return;
            }
            if let Ok(Err(error)) = done_rx.await {
                tracing::error!(
                    target: "bifrost.sync.inventory",
                    account = ?self.account_id,
                    scope = ?scope,
                    error = %error,
                    "failed to persist inventory barrier incident"
                );
            }
        }
    }

    /// Surface degraded coverage to the consumer.
    ///
    /// Durable debt that nothing reports is only half a fix: the scope is live
    /// and converging, but objects are known-missing and only an operator can
    /// decide whether to repair, wait, or accept the gap.
    fn warn_degraded(
        changes_tx: &Option<broadcast::Sender<MultiplexerEvent>>,
        scope: &CursorScope,
        coverage: &bifrost_types::InventoryCoverageReport,
    ) {
        if coverage.is_complete() {
            return;
        }
        let obligations = coverage.obligations();
        let Some(tx) = changes_tx else { return };
        let barriers = obligations
            .iter()
            .filter(|obligation| obligation.is_barrier())
            .count();
        let warning = bifrost_types::Warning::user_safe(
            bifrost_types::WarningKind::OperatorAttentionNeeded,
            if barriers > 0 {
                format!(
                    "inventory stopped at {barriers} region(s) it cannot represent and cannot \
                     replay; the scope cannot advance past them until an operator waives them"
                )
            } else {
                format!(
                    "inventory completed with {} object(s) or region(s) it could not represent; \
                     the scope is live but incomplete",
                    obligations.len()
                )
            },
        );
        let _ = tx.send(MultiplexerEvent {
            scope: scope.clone(),
            event: Arc::new(SyncEvent::Warning(warning)),
            checkpoint: None,
            publication: None,
        });
    }

    fn forward_inventory_batch(
        &self,
        tx: &broadcast::Sender<MultiplexerEvent>,
        scope: &CursorScope,
        batch: &bifrost_types::InventoryBatch,
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
        // A checkpoint-bearing batch declares the coverage it advances across,
        // not just the terminal completion: Graph checkpoints per page, so
        // waiting for `Done` would let a page checkpoint become durable across
        // a gap it never declared.
        let publication = batch
            .checkpoint
            .as_ref()
            .and_then(|_| self.publish_claim(&batch.coverage));
        let me = MultiplexerEvent {
            scope: scope.clone(),
            event: Arc::new(SyncEvent::Batch(synthetic)),
            checkpoint: batch.checkpoint.clone(),
            publication,
        };
        if let (Some(control), Some(checkpoint)) = (&self.control, batch.checkpoint.as_ref()) {
            control.expect_checkpoint(checkpoint.clone());
        }
        let delivered = tx.send(me).unwrap_or(0);
        if !super::delivered_to_real_subscriber(delivered) {
            self.retire_claim(publication);
            if let (Some(control), Some(checkpoint)) = (&self.control, batch.checkpoint.as_ref()) {
                control.retire_checkpoint(checkpoint);
            }
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
