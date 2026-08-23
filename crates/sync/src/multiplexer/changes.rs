//! `changes_stream` driver.
//!
//! `drive_changes_stream` consumes an `Account::changes_stream(cursor)`
//! result and translates each `SyncEvent<Change>` into a
//! `MultiplexerEvent` tagged with the originating scope. The driver
//! broadcasts the event (with its checkpoint, if any) to the per-account
//! broadcast channel and updates the in-memory cursor registry so the
//! next poll iteration uses the advanced cursor.
//!
//! ## Cursor-durability contract
//!
//! The driver does NOT persist the cursor to `CheckpointStore`. The
//! consumer is responsible for calling
//! `SyncEngine::ack_checkpoint(account, checkpoint)` after they have
//! durably persisted the batch items in their own store. Only then does
//! the engine persist the cursor.
//!
//! This shape preserves the trait contract that `Batch` carries
//! `(items, checkpoint)` for atomic consumer persistence. If the engine
//! persists the cursor before the consumer acknowledges, a crash
//! between broadcast and the consumer's write loses items. The ack
//! channel closes the gap.
//!
//! The in-memory `CursorRegistry` IS advanced on broadcast so polling
//! makes forward progress within a single session; only the durable
//! `CheckpointStore` write awaits the ack. On restart the engine reads
//! the last-acked cursor from the store and re-runs `changes_stream`
//! from there - any items the consumer never durably persisted come
//! across again.

use std::sync::Arc;

use bifrost_types::{
    Account, AccountError, AccountId, Change, ChangeCursor, Checkpoint, CursorScope, SyncEvent,
};
use futures::stream::StreamExt;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::cancel::{BoundaryRequest, BoundaryView};
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::error::Error;

use super::MultiplexerEvent;

/// Per-batch driver outcome. Workers consult this between batches to
/// decide whether to keep pulling.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ChangesEvent {
    /// Driver advanced past a `Batch` with a cursor checkpoint.
    Advanced,
    /// Driver saw a `Done` and exited cleanly.
    Done,
    /// Driver was asked to stop (boundary request `Stop`).
    Stopped,
    /// Driver was asked to pause; checkpoint flushed and the driver
    /// parked the stream at a clean boundary.
    Paused,
    /// Driver saw a terminating stream event and aborted. Carries the
    /// full `AccountError` so the engine dispatches via the derived
    /// `RecoveryClass` (Retry / Reconcile / Engine directive /
    /// terminal). The name reflects that non-terminal recoveries can
    /// also terminate a stream and resume after dispatch.
    Terminated(AccountError),
}

/// Consume a `changes_stream` to completion or to the next boundary
/// request, broadcasting each event onto the per-account channel.
///
/// The in-memory cursor registry is updated when a checkpoint arrives;
/// the durable `CheckpointStore` write happens through
/// `SyncEngine::ack_checkpoint`, not here. See module docs for the
/// rationale.
///
/// This driver deliberately does NOT feed `LiveSupersedes`. Having
/// broadcast a change is not evidence the consumer received it, so it
/// is not a sound basis for suppressing the object's inventory copy;
/// see the `LiveSupersedes` type docs for the full argument.
#[allow(clippy::too_many_arguments)]
pub async fn drive_changes_stream(
    account: &dyn Account,
    scope: CursorScope,
    cursor: ChangeCursor,
    cursors: Arc<CursorRegistry>,
    _account_id: AccountId,
    changes_tx: broadcast::Sender<MultiplexerEvent>,
    boundary: BoundaryView,
    control: Option<SyncControl>,
    _ack_tx: Option<mpsc::Sender<WriterRequest>>,
    registry_generation: Option<u64>,
) -> Result<ChangesEvent, Error> {
    let _activity = match &control {
        Some(control) => match control.begin_activity() {
            Some(activity) => Some(activity),
            None => return Ok(ChangesEvent::Paused),
        },
        None => None,
    };
    let mut stream = account.changes_stream(cursor);
    while let Some(event) = stream.next().await {
        match boundary.peek() {
            BoundaryRequest::Stop => return Ok(ChangesEvent::Stopped),
            BoundaryRequest::Pause => {}
            BoundaryRequest::CheckpointNow | BoundaryRequest::Run => {}
        }
        let checkpoint = checkpoint_for(&event).cloned();
        let is_done = matches!(&event, SyncEvent::Done(_));
        // Capture the full `AccountError` so the engine has the
        // derived recovery, scope, operation, provider, protocol, and
        // diagnostics on hand.
        let terminated_error = if let SyncEvent::Terminated(err) = &event {
            Some(err.clone())
        } else {
            None
        };
        let me = MultiplexerEvent {
            scope: scope.clone(),
            event: Arc::new(event),
            checkpoint: checkpoint.clone(),
            publication: None,
        };
        // Broadcast (item + checkpoint) together. Subscribers persist
        // `(items, checkpoint)` atomically in their own store, then
        // call `SyncEngine::ack_checkpoint` to durably advance the
        // engine's cursor.
        // Register the outstanding checkpoint BEFORE publishing it.
        // A consumer that receives, persists and acks between the send
        // and the registration would otherwise leave behind an entry
        // no ack can ever match, permanently wedging boundary waiters.
        let publish = || {
            let expected = match (&control, &checkpoint) {
                (Some(control), Some(checkpoint)) => {
                    control.expect_checkpoint(checkpoint.clone());
                    Some(checkpoint.clone())
                }
                _ => None,
            };
            (changes_tx.send(me.clone()).unwrap_or(0), expected)
        };
        let change_cursor = match &checkpoint {
            Some(Checkpoint::Change(cursor)) => Some(cursor.clone()),
            _ => None,
        };
        let published = match registry_generation {
            Some(generation) => cursors.publish_if_generation(change_cursor, generation, publish),
            None => {
                let result = publish();
                if let Some(cursor) = change_cursor {
                    cursors.put(cursor);
                }
                Some(result)
            }
        };
        let Some((delivered, expected)) = published else {
            return Ok(ChangesEvent::Done);
        };
        if !super::delivered_to_real_subscriber(delivered)
            && let (Some(control), Some(expected)) = (&control, &expected)
        {
            // Only the slot's sentinel receiver saw this batch, so no
            // consumer ack is coming for it.
            control.retire_checkpoint(expected);
        }
        if let Some(outcome) = post_publish_boundary(boundary.peek(), checkpoint.is_some()) {
            return Ok(outcome);
        }
        if is_done {
            return Ok(ChangesEvent::Done);
        }
        if let Some(err) = terminated_error {
            return Ok(ChangesEvent::Terminated(err));
        }
    }
    Ok(ChangesEvent::Done)
}

fn post_publish_boundary(request: BoundaryRequest, has_checkpoint: bool) -> Option<ChangesEvent> {
    match request {
        // Pause is a lifecycle boundary, not a request for a durable
        // cursor. A checkpoint-free batch is still a completed stream
        // item and must release the activity guard here, otherwise a
        // valid checkpoint-free stream can keep the account non-quiescent
        // forever.
        BoundaryRequest::Pause => Some(ChangesEvent::Paused),
        BoundaryRequest::Stop => Some(ChangesEvent::Stopped),
        BoundaryRequest::CheckpointNow if has_checkpoint => Some(ChangesEvent::Done),
        BoundaryRequest::CheckpointNow | BoundaryRequest::Run => None,
    }
}

/// Ack request: scope + checkpoint the engine should durably persist.
/// Consumers send these through `SyncEngine::ack_checkpoint` after
/// their own item store commits the matching batch.
pub struct AckRequest {
    pub scope: CursorScope,
    pub checkpoint: Checkpoint,
    /// Which PUBLICATION of `checkpoint` this acknowledges.
    ///
    /// `Checkpoint` equality cannot serve: a backfill page whose content was
    /// entirely unrepresentable increments no item count and produces a
    /// byte-identical checkpoint to its predecessor, and inventory fusion
    /// publishes its final checkpoint twice. Without the identity, one
    /// publication's coverage claim gets applied to another's checkpoint.
    ///
    /// `None` only for engine-internal acks that carry no coverage claim.
    pub publication: Option<crate::cursor::PublicationId>,
    /// True for legacy engine-generated acks. Current production code
    /// sends only consumer acks (`false`), but the field remains for
    /// trace readability if old tests or callers construct requests
    /// directly inside the crate.
    pub auto: bool,
    /// Completion channel for consumer-driven acks. `ack_checkpoint`
    /// waits on this so it returns only after the checkpoint store
    /// has accepted or rejected the write.
    pub complete: Option<oneshot::Sender<Result<(), Error>>>,
}

impl std::fmt::Debug for AckRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AckRequest")
            .field("scope", &self.scope)
            .field("checkpoint", &self.checkpoint)
            .field("publication", &self.publication)
            .field("auto", &self.auto)
            .field("complete", &self.complete.is_some())
            .finish()
    }
}

/// Everything that may mutate durable checkpoint state for one account.
///
/// There is exactly ONE writer per account and every durable mutation goes
/// through this channel, because ordering between them is load-bearing.
/// Reattach used to write and delete cursors directly against the
/// `CheckpointStore` while the ack writer was concurrently persisting
/// acknowledged checkpoints for the same scopes, which is a race: a
/// replacement's inventory pass broadcasts checkpoint-bearing batches BEFORE
/// the cutover (`run_establish` hands `changes_tx` to `InventoryFusion`), a
/// consumer acknowledges one, and an aborted reattach then deleted the row
/// that acknowledgement had just committed.
///
/// Serializing is necessary but NOT sufficient on its own: a FIFO queue would
/// faithfully order the acknowledged write ahead of an unconditional delete and
/// then destroy it. The writer therefore tracks which scopes are still
/// provisional - see `ReattachInsert`.
pub enum WriterRequest {
    /// Persist an acknowledged checkpoint. Also DISCHARGES any provisional
    /// mark on its scope: an acknowledgement is real committed consumer
    /// progress, and a later reattach abort must not delete it.
    Ack(AckRequest),
    /// Persist a cursor the reattach freshly created, and mark its scope
    /// provisional until the reattach commits or aborts.
    ///
    /// Provisional means "this row exists only because a replacement that has
    /// not cut over put it there". Only such rows may be rolled back, which is
    /// what lets the abort path compensate by plain deletion without ever
    /// touching preexisting durable state.
    ReattachInsert {
        cursor: ChangeCursor,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// The reattach aborted: delete the rows it inserted that are STILL
    /// provisional. A scope whose checkpoint was acknowledged in the meantime
    /// is no longer provisional and survives.
    ReattachAbort { done: oneshot::Sender<()> },
    /// The reattach cut over: its rows are now ordinary durable state.
    ReattachCommit,
    /// Record a walk that stopped at a region the cursor may not cross.
    ///
    /// Has no checkpoint of its own by definition - nothing advanced - so it
    /// cannot ride the acknowledgement path like every other durable mutation.
    /// It still goes through this writer, because it shares the ledger with
    /// everything that does.
    RecordBarrier {
        incident: crate::cursor::BarrierIncident,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// Record obligations from a report that has no acknowledgeable checkpoint
    /// of its own - a backfill partition's terminal summary, say.
    ///
    /// Debt only, never proof: the report may describe entries the consumer
    /// never persisted, so it may not discharge anything. Over-reporting debt
    /// costs a scope that stays degraded until something re-reads it;
    /// under-reporting it costs objects nobody ever sees again.
    RecordDebt {
        report: bifrost_types::InventoryCoverageReport,
        generation: u64,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// Apply the outcomes of one repair pass.
    ///
    /// `publication` is the repair batch's acknowledgement identity, present
    /// only when ids were actually published to a live subscriber. A
    /// `Recovered` resolution discharges only once that publication is
    /// acknowledged: the account having re-read the object proves the
    /// representation healed, but until the consumer durably accepts the
    /// existence notification it still does not know the object is there.
    ApplyRepair {
        resolutions: Vec<crate::repair::RepairResolution>,
        publication: Option<crate::cursor::PublicationId>,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// Operator action on one obligation or barrier occurrence.
    ///
    /// `Waive` is the only path to accepted loss, and it exists only here:
    /// nothing automatic may reach it, because a retry budget expiring is
    /// evidence that retrying is not working, not a decision about what loss is
    /// acceptable.
    OperatorDecision {
        key: bifrost_types::ObligationKey,
        decision: OperatorDecision,
        done: oneshot::Sender<Result<bool, Error>>,
    },
}

/// What an operator decided about an obligation.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum OperatorDecision {
    /// Accept the loss. Changes POLICY only: the record goes on saying that
    /// coverage was never proved, because it never was.
    Waive { by: String, at_unix_seconds: i64 },
    /// Stop automatic retries without accepting the loss. Stays visible, stays
    /// blocking, stays manually retryable.
    Block,
}

impl From<AckRequest> for WriterRequest {
    fn from(request: AckRequest) -> Self {
        Self::Ack(request)
    }
}

impl std::fmt::Debug for WriterRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ack(request) => f.debug_tuple("Ack").field(request).finish(),
            Self::ReattachInsert { cursor, .. } => f
                .debug_struct("ReattachInsert")
                .field("scope", &cursor.scope)
                .finish(),
            Self::ReattachAbort { .. } => f.write_str("ReattachAbort"),
            Self::ReattachCommit => f.write_str("ReattachCommit"),
            Self::ApplyRepair { resolutions, .. } => f
                .debug_struct("ApplyRepair")
                .field("resolutions", &resolutions.len())
                .finish(),
            Self::RecordDebt { generation, .. } => f
                .debug_struct("RecordDebt")
                .field("generation", generation)
                .finish(),
            Self::RecordBarrier { incident, .. } => f
                .debug_struct("RecordBarrier")
                .field("key", &incident.key)
                .finish(),
            Self::OperatorDecision { key, decision, .. } => f
                .debug_struct("OperatorDecision")
                .field("key", key)
                .field("decision", decision)
                .finish(),
        }
    }
}

fn checkpoint_for(event: &SyncEvent<Change>) -> Option<&Checkpoint> {
    match event {
        SyncEvent::Batch(b) => b.checkpoint.as_ref(),
        SyncEvent::Done(c) => c.as_ref(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{ChangesEvent, post_publish_boundary};
    use crate::cancel::BoundaryRequest;

    /// The full boundary truth table for a published item. Pause and
    /// Stop are lifecycle boundaries and must be honoured whether or
    /// not the item carried a checkpoint - a checkpoint-free stream
    /// that only checked at checkpoint boundaries never parks, and the
    /// account never reaches quiescence. `CheckpointNow` is the one
    /// request that genuinely needs a durable cursor to satisfy, so it
    /// alone keeps running when the item produced none.
    #[test]
    fn lifecycle_boundaries_are_honoured_with_or_without_a_checkpoint() {
        for has_checkpoint in [true, false] {
            assert!(
                matches!(
                    post_publish_boundary(BoundaryRequest::Pause, has_checkpoint),
                    Some(ChangesEvent::Paused)
                ),
                "pause must park (has_checkpoint={has_checkpoint})"
            );
            assert!(
                matches!(
                    post_publish_boundary(BoundaryRequest::Stop, has_checkpoint),
                    Some(ChangesEvent::Stopped)
                ),
                "stop must stop (has_checkpoint={has_checkpoint})"
            );
            assert!(
                post_publish_boundary(BoundaryRequest::Run, has_checkpoint).is_none(),
                "run must keep going (has_checkpoint={has_checkpoint})"
            );
        }
        assert!(matches!(
            post_publish_boundary(BoundaryRequest::CheckpointNow, true),
            Some(ChangesEvent::Done)
        ));
        assert!(
            post_publish_boundary(BoundaryRequest::CheckpointNow, false).is_none(),
            "a checkpoint request is not satisfied by a checkpoint-free item"
        );
    }
}
