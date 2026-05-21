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

use bifrost_types::{Account, AccountId, Change, ChangeCursor, Checkpoint, CursorScope, SyncEvent};
use futures::stream::StreamExt;
use tokio::sync::{broadcast, mpsc};

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
    /// Driver saw a `Fatal` and aborted the stream. Carries the
    /// `RecoveryClass` so the engine dispatches the right recovery
    /// (Retry / RestartScope / RestartAccount / surface-to-consumer).
    Fatal(bifrost_types::RecoveryClass),
}

/// Consume a `changes_stream` to completion or to the next boundary
/// request, broadcasting each event onto the per-account channel.
///
/// The in-memory cursor registry is updated when a checkpoint arrives;
/// the durable `CheckpointStore` write happens through
/// `SyncEngine::ack_checkpoint`, not here. See module docs for the
/// rationale.
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
    ack_tx: Option<mpsc::Sender<AckRequest>>,
) -> Result<ChangesEvent, Error> {
    let mut stream = account.changes_stream(cursor);
    while let Some(event) = stream.next().await {
        match boundary.peek() {
            BoundaryRequest::Stop => return Ok(ChangesEvent::Stopped),
            BoundaryRequest::Pause => return Ok(ChangesEvent::Paused),
            BoundaryRequest::CheckpointNow | BoundaryRequest::Run => {}
        }
        let checkpoint = checkpoint_for(&event).cloned();
        let is_done = matches!(&event, SyncEvent::Done(_));
        let fatal_recovery = if let SyncEvent::Fatal(f) = &event {
            Some(f.recovery.clone())
        } else {
            None
        };
        let me = MultiplexerEvent {
            scope: scope.clone(),
            event: Arc::new(event),
            checkpoint: checkpoint.clone(),
        };
        // Broadcast (item + checkpoint) together. Subscribers persist
        // `(items, checkpoint)` atomically in their own store, then
        // call `SyncEngine::ack_checkpoint` to durably advance the
        // engine's cursor.
        let _ = changes_tx.send(me.clone());
        if let Some(Checkpoint::Change(c)) = &checkpoint {
            // Advance the in-memory registry so the next poll
            // iteration starts from the freshly-yielded cursor. The
            // durable write awaits the consumer ack.
            cursors.put(c.clone());
        }
        if let Some(cp) = checkpoint.clone()
            && let Some(ctrl) = &control
        {
            // Notify pause / checkpoint_now waiters that a checkpoint
            // has just been emitted (whether or not it has been
            // acknowledged yet; the generation counter resolves the
            // race - see SyncControl).
            ctrl.record_checkpoint(cp).await;
        }
        // If the consumer ack channel is wired and they have not yet
        // opted into per-batch acks, propagate the checkpoint as a
        // synthetic "self-ack" so v1 consumers that do not yet call
        // `ack_checkpoint` still see durable cursors. This keeps the
        // door open for the strict ack contract while preserving
        // single-process correctness when consumers ignore the API.
        if let (Some(tx), Some(cp)) = (&ack_tx, checkpoint.clone()) {
            let _ = tx
                .send(AckRequest {
                    scope: scope.clone(),
                    checkpoint: cp,
                    auto: true,
                })
                .await;
        }
        if is_done {
            return Ok(ChangesEvent::Done);
        }
        if let Some(rec) = fatal_recovery {
            return Ok(ChangesEvent::Fatal(rec));
        }
    }
    Ok(ChangesEvent::Done)
}

/// Ack request: scope + checkpoint the engine should durably persist.
/// `auto` is true for engine-emitted "self-acks" issued so v1
/// consumers that never call `SyncEngine::ack_checkpoint` still see
/// durable cursors (single-process correctness).
#[derive(Debug, Clone)]
pub struct AckRequest {
    pub scope: CursorScope,
    pub checkpoint: Checkpoint,
    /// True when the engine issued the ack itself (default v1 mode);
    /// false when it came from `SyncEngine::ack_checkpoint`.
    pub auto: bool,
}

fn checkpoint_for(event: &SyncEvent<Change>) -> Option<&Checkpoint> {
    match event {
        SyncEvent::Batch(b) => b.checkpoint.as_ref(),
        SyncEvent::Done(c) => c.as_ref(),
        _ => None,
    }
}
