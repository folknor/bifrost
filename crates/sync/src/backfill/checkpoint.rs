//! Backfill checkpoint writer.
//!
//! The runner emits a `BackfillCheckpoint` at every partition boundary
//! and a finer-grained partial checkpoint at each `Batch` boundary
//! inside a partition. This module wraps the `CheckpointStore` calls
//! so the runner doesn't need to know the persistence shape.

use std::sync::Arc;

use bifrost_types::{AccountId, BackfillCheckpoint};

use crate::cursor::store::{BackfillCheckpointRecord, DynCheckpointStore};
use crate::error::Error;

/// Backfill checkpoint writer.
pub struct BackfillCheckpointWriter {
    pub account_id: AccountId,
    pub store: Arc<DynCheckpointStore>,
}

impl BackfillCheckpointWriter {
    pub async fn persist(&self, checkpoint: BackfillCheckpoint) -> Result<(), Error> {
        self.store
            .put_backfill(
                &self.account_id,
                BackfillCheckpointRecord::complete(checkpoint),
            )
            .await
    }
}
