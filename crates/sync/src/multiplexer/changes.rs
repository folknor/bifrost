//! `changes_stream` driver.
//!
//! `drive_changes_stream` consumes a `Account::changes_stream(cursor)`
//! result and translates each `SyncEvent<Change>` into a
//! `MultiplexerEvent` tagged with the originating scope. The cursor
//! registry is advanced at every `Batch` whose `checkpoint` is `Some`.
//!
//! Ordering invariant: each event is broadcast to subscribers BEFORE
//! the cursor checkpoint is persisted. If the process dies between
//! the broadcast and the checkpoint write, the next resume re-reads
//! the events the consumer already saw - that is safe. The opposite
//! ordering would skip events the consumer never observed.

use std::sync::Arc;

use bifrost_types::{Account, AccountId, Change, ChangeCursor, Checkpoint, CursorScope, SyncEvent};
use futures::stream::StreamExt;
use tokio::sync::broadcast;

use crate::cancel::{BoundaryRequest, BoundaryView};
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::cursor::store::DynCheckpointStore;
use crate::error::Error;

use super::MultiplexerEvent;

/// Per-batch driver outcome. Workers consult this between batches to
/// decide whether to keep pulling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangesEvent {
    /// Driver advanced past a `Batch` with a cursor checkpoint.
    Advanced,
    /// Driver saw a `Done` and exited cleanly.
    Done,
    /// Driver was asked to stop (boundary request `Stop`).
    Stopped,
    /// Driver saw a `Fatal` and aborted the stream.
    Fatal,
}

/// Consume a `changes_stream` to completion or to the next boundary
/// request, persisting checkpoints atomically as each `Batch` with
/// `checkpoint: Some(_)` lands.
///
/// Atomicity here matters: the engine guarantees the consumer sees a
/// batch only after its checkpoint has been persisted to the store, so
/// the "items + checkpoint in one transaction" invariant holds even
/// across crashes.
#[allow(clippy::too_many_arguments)]
pub async fn drive_changes_stream(
    account: &dyn Account,
    scope: CursorScope,
    cursor: ChangeCursor,
    cursors: Arc<CursorRegistry>,
    store: Arc<DynCheckpointStore>,
    account_id: AccountId,
    changes_tx: broadcast::Sender<MultiplexerEvent>,
    boundary: BoundaryView,
    control: Option<SyncControl>,
) -> Result<ChangesEvent, Error> {
    let mut stream = account.changes_stream(cursor);
    while let Some(event) = stream.next().await {
        match boundary.peek() {
            BoundaryRequest::Stop => return Ok(ChangesEvent::Stopped),
            BoundaryRequest::Pause | BoundaryRequest::CheckpointNow | BoundaryRequest::Run => {}
        }
        let checkpoint = checkpoint_for(&event).cloned();
        let is_done = matches!(&event, SyncEvent::Done(_));
        let is_fatal = matches!(&event, SyncEvent::Fatal(_));
        let me = MultiplexerEvent {
            scope: scope.clone(),
            event: Arc::new(event),
            checkpoint: checkpoint.clone(),
        };
        // Order matters: broadcast the batch first, then persist the
        // cursor. The cursor write is the engine's commitment that the
        // data has been delivered to subscribers; if we wrote the
        // cursor first and then crashed before broadcasting, resume
        // would skip data the consumer never saw.
        let _ = changes_tx.send(me.clone());
        if let Some(Checkpoint::Change(c)) = &checkpoint {
            store.put_change_cursor(&account_id, c.clone()).await?;
            cursors.put(c.clone());
        }
        if let Some(cp) = checkpoint
            && let Some(ctrl) = &control
        {
            // Notify pause / checkpoint_now waiters that a checkpoint
            // has just been persisted.
            ctrl.record_checkpoint(cp).await;
        }
        if is_done {
            return Ok(ChangesEvent::Done);
        }
        if is_fatal {
            return Ok(ChangesEvent::Fatal);
        }
    }
    Ok(ChangesEvent::Done)
}

fn checkpoint_for(event: &SyncEvent<Change>) -> Option<&Checkpoint> {
    match event {
        SyncEvent::Batch(b) => b.checkpoint.as_ref(),
        SyncEvent::Done(c) => c.as_ref(),
        _ => None,
    }
}
