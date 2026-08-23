//! `CheckpointStore` trait and in-memory reference implementation.
//!
//! The engine's only persistence contract. Consumers wire their own
//! backing store; the engine never assumes sled / sqlite / rocksdb /
//! anything specific. The in-memory backend is for tests and for
//! consumers happy to lose state on process restart.
//!
//! Two distinct kinds of stored object:
//! - `ChangeCursor` per `(account, scope)`.
//! - `BackfillCheckpoint` per `(account, scope, partition)`.
//!
//! Backfill partitions are addressed by their opaque `Partition`
//! bytes so resume can pick up inside a partition rather than at the
//! start.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bifrost_types::{
    AccountId, BackfillCheckpoint, ChangeCursor, CursorScope, InventoryCoverage, Partition,
};

use crate::error::Error;

/// One durable change-cursor row: the cursor AND what the enumeration behind it
/// proved.
///
/// Cursor and coverage are ONE logical record, and every implementation must
/// persist them all-or-nothing. Partial persistence is a contract violation,
/// not a degraded mode. A backend may store them across several tables
/// internally - the representation is backend-owned, the atomicity is not.
///
/// They cannot be two store operations. No ordering of two independent writes
/// is crash-safe: cursor-first recreates the silent loss the coverage record
/// exists to prevent, and coverage-first can leave debt whose cursor never
/// commits. That is why this is an aggregate rather than a separate
/// "obligations lane" - two successful calls are not equivalent to one atomic
/// one, however carefully they are sequenced.
#[derive(Debug, Clone)]
pub struct ChangeCheckpointRecord {
    pub cursor: ChangeCursor,
    pub coverage: InventoryCoverage,
}

impl ChangeCheckpointRecord {
    /// A record whose enumeration left nothing unaccounted for.
    #[must_use]
    pub fn complete(cursor: ChangeCursor) -> Self {
        Self {
            cursor,
            coverage: InventoryCoverage::Complete,
        }
    }
}

/// One durable backfill row, with the same atomicity contract as
/// [`ChangeCheckpointRecord`].
///
/// Backfill needs coverage for the same reason the change cursor does, and it
/// is not a lesser case: `BackfillRunner` emits a checkpoint on EVERY batch and
/// counts only the entries a page materialized, so a dropped object is not even
/// counted, and the orchestrator can then write a completion sentinel that
/// permanently skips the scope. Every provider reaches this path, including
/// ones whose live cursor was established separately.
#[derive(Debug, Clone)]
pub struct BackfillCheckpointRecord {
    pub checkpoint: BackfillCheckpoint,
    pub coverage: InventoryCoverage,
}

impl BackfillCheckpointRecord {
    #[must_use]
    pub fn complete(checkpoint: BackfillCheckpoint) -> Self {
        Self {
            checkpoint,
            coverage: InventoryCoverage::Complete,
        }
    }
}

/// Persistence contract.
///
/// All methods are async because real persistence backends are
/// IO-bound. The in-memory impl returns ready futures.
pub trait CheckpointStore: Send + Sync {
    /// Persist or replace the change-cursor record for `(account, scope)`.
    ///
    /// The whole record lands or none of it does. See
    /// [`ChangeCheckpointRecord`].
    fn put_change_record<'a>(
        &'a self,
        account: &'a AccountId,
        record: ChangeCheckpointRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;

    /// Read the latest change-cursor record for `(account, scope)` if any.
    fn get_change_record<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ChangeCheckpointRecord>, Error>> + Send + 'a>>;

    /// Persist or replace a backfill record for `(account, scope, partition)`.
    fn put_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        record: BackfillCheckpointRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;

    /// Read the latest backfill checkpoint for `(account, scope)`.
    /// Returns the one with the greatest `items_done` if the store has
    /// more than one partition's worth of state. Ties must be stable:
    /// a page partition with the greatest parsed upper bound wins,
    /// followed by lexicographically-greatest opaque partition bytes.
    ///
    /// The default `InMemoryCheckpointStore` implementation scans
    /// every backfill entry per call (O(n) on total partitions per
    /// account). Production backends backing onto a real store
    /// should maintain a per-`(account, scope)` "latest" index for
    /// constant-time reads; the trait does not require it because
    /// the engine calls `get_backfill` infrequently (resume path,
    /// observability), not on the hot path.
    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<Option<BackfillCheckpointRecord>, Error>> + Send + 'a>>;

    /// Convenience: persist a cursor whose enumeration was complete.
    ///
    /// DO NOT OVERRIDE. This exists so callers that have nothing to say about
    /// coverage do not have to spell out `ChangeCheckpointRecord::complete`;
    /// the atomic unit is still [`Self::put_change_record`]. An implementation
    /// that overrides this instead of the record method would silently stop
    /// persisting coverage.
    fn put_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        cursor: ChangeCursor,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        self.put_change_record(account, ChangeCheckpointRecord::complete(cursor))
    }

    /// Convenience: read just the cursor, discarding coverage.
    ///
    /// DO NOT OVERRIDE, and prefer [`Self::get_change_record`] anywhere the
    /// answer feeds a decision about whether progress may be accepted -
    /// discarding coverage is exactly how an unresolved obligation gets
    /// forgotten.
    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>> {
        Box::pin(async move {
            Ok(self
                .get_change_record(account, scope)
                .await?
                .map(|record| record.cursor))
        })
    }

    /// Drop the change cursor for `(account, scope)`. Used by the
    /// engine's `EngineDirective::RestartScope` recovery path so the
    /// next attach / poll re-establishes via inventory. This is
    /// required because a no-op delete silently preserves the stale
    /// durable cursor and makes restart-scope recovery ineffective.
    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;

    /// Drop every backfill checkpoint for `(account, scope)`, the
    /// durable completion marker included. Used ONLY by the engine's
    /// `SchemaIncompatible` recovery: a schema bump means the ids the
    /// consumer holds were minted under an encoding the protocol has
    /// disowned, and the completion marker is what makes the next
    /// attach skip the inventory re-walk that re-mints them - so a
    /// no-op here silently pins the consumer to the old ids forever.
    /// Routine `RestartScope` recovery deliberately does NOT call
    /// this: re-walking a completed backfill after every cursor
    /// invalidation would re-hydrate the whole scope for no schema
    /// reason.
    fn delete_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;
}

/// Engine-side erased handle type. The slot holds `Arc<dyn
/// CheckpointStore>` cloned across tasks.
pub type DynCheckpointStore = dyn CheckpointStore;

/// In-memory reference implementation. Keyed by
/// `(AccountId, CursorScope)` for change cursors and by
/// `(AccountId, CursorScope, Partition)` for backfill checkpoints.
#[derive(Debug, Default)]
pub struct InMemoryCheckpointStore {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    change: HashMap<(AccountId, CursorScope), ChangeCheckpointRecord>,
    backfill: HashMap<(AccountId, CursorScope, Partition), BackfillCheckpointRecord>,
}

impl InMemoryCheckpointStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Wrap in an `Arc` for the engine. Convenience over
    /// `Arc::new(InMemoryCheckpointStore::new())`.
    #[must_use]
    pub fn arc() -> Arc<dyn CheckpointStore> {
        Arc::new(Self::new())
    }
}

impl CheckpointStore for InMemoryCheckpointStore {
    fn put_change_record<'a>(
        &'a self,
        account: &'a AccountId,
        record: ChangeCheckpointRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        let account = account.clone();
        Box::pin(async move {
            let mut guard = self.inner.lock().expect("poisoned");
            // One map entry, so cursor and coverage cannot land apart. A
            // backend spreading them over tables owes the same guarantee
            // through a transaction.
            guard
                .change
                .insert((account, record.cursor.scope.clone()), record);
            Ok(())
        })
    }

    fn get_change_record<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ChangeCheckpointRecord>, Error>> + Send + 'a>>
    {
        let account = account.clone();
        let scope = scope.clone();
        Box::pin(async move {
            let guard = self.inner.lock().expect("poisoned");
            Ok(guard.change.get(&(account, scope)).cloned())
        })
    }

    fn put_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        record: BackfillCheckpointRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        let account = account.clone();
        Box::pin(async move {
            let mut guard = self.inner.lock().expect("poisoned");
            let key = (
                account,
                record.checkpoint.scope.clone(),
                record.checkpoint.partition.clone(),
            );
            guard.backfill.insert(key, record);
            Ok(())
        })
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<Option<BackfillCheckpointRecord>, Error>> + Send + 'a>>
    {
        let account = account.clone();
        let scope = scope.clone();
        Box::pin(async move {
            let guard = self.inner.lock().expect("poisoned");
            let mut latest: Option<BackfillCheckpointRecord> = None;
            for ((aid, s, _p), ck) in &guard.backfill {
                if aid == &account && s == &scope {
                    let beats_current = match &latest {
                        None => true,
                        Some(existing) => {
                            backfill_checkpoint_is_later(&ck.checkpoint, &existing.checkpoint)
                        }
                    };
                    if beats_current {
                        latest = Some(ck.clone());
                    }
                }
            }
            Ok(latest)
        })
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        let account = account.clone();
        let scope = scope.clone();
        Box::pin(async move {
            let mut guard = self.inner.lock().expect("poisoned");
            guard.change.remove(&(account, scope));
            Ok(())
        })
    }

    fn delete_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        let account = account.clone();
        let scope = scope.clone();
        Box::pin(async move {
            let mut guard = self.inner.lock().expect("poisoned");
            guard
                .backfill
                .retain(|(aid, s, _), _| !(aid == &account && s == &scope));
            Ok(())
        })
    }
}

fn backfill_checkpoint_is_later(
    candidate: &BackfillCheckpoint,
    current: &BackfillCheckpoint,
) -> bool {
    match candidate
        .progress
        .items_done
        .cmp(&current.progress.items_done)
    {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => {
            let candidate_page =
                crate::backfill::partitioner::parse_page_partition(&candidate.partition);
            let current_page =
                crate::backfill::partitioner::parse_page_partition(&current.partition);
            match (candidate_page, current_page) {
                (Some((_, candidate_to)), Some((_, current_to))) if candidate_to != current_to => {
                    candidate_to > current_to
                }
                _ => candidate.partition.0 > current.partition.0,
            }
        }
    }
}
