//! Backfill checkpoint writer.
//!
//! The runner emits a `BackfillCheckpoint` at every partition boundary and a
//! finer-grained partial checkpoint at each `Batch` boundary inside a
//! partition. This module wraps the `CheckpointStore` calls so the runner
//! doesn't need to know the persistence shape.

use std::sync::Arc;

use bifrost_types::{AccountId, BackfillCheckpoint, Checkpoint};

use crate::cursor::store::{CheckpointTransition, DynCheckpointStore};
use crate::error::Error;

/// Backfill checkpoint writer.
pub struct BackfillCheckpointWriter {
    pub account_id: AccountId,
    pub store: Arc<DynCheckpointStore>,
}

impl BackfillCheckpointWriter {
    /// Persist `checkpoint`, PRESERVING the account's debt ledger.
    ///
    /// It used to write a record asserting complete coverage, which was a
    /// convenience constructor quietly manufacturing a proof: a write that
    /// carries no coverage report means "leave the ledger unchanged", never
    /// "everything is accounted for". The distinction is the whole point of the
    /// ledger, because a spurious `Complete` discharges debt nothing re-read.
    pub async fn persist(&self, checkpoint: BackfillCheckpoint) -> Result<(), Error> {
        let ledger = self.store.get_ledger(&self.account_id).await?;
        self.store
            .apply_transition(
                &self.account_id,
                CheckpointTransition {
                    checkpoint: Checkpoint::Backfill(checkpoint),
                    ledger,
                },
            )
            .await
    }
}
