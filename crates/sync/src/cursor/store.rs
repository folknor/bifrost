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

use bifrost_types::{AccountId, BackfillCheckpoint, ChangeCursor, CursorScope, Partition};

use crate::error::Error;

/// Persistence contract.
///
/// All five methods are async because real persistence backends are
/// IO-bound. The in-memory impl returns ready futures.
pub trait CheckpointStore: Send + Sync {
    /// Persist or replace the change cursor for `(account, scope)`.
    fn put_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        cursor: ChangeCursor,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;

    /// Read the latest change cursor for `(account, scope)` if any.
    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>>;

    /// Persist or replace a backfill checkpoint for
    /// `(account, scope, partition)`.
    fn put_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        checkpoint: BackfillCheckpoint,
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
    ) -> Pin<Box<dyn Future<Output = Result<Option<BackfillCheckpoint>, Error>> + Send + 'a>>;

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
    change: HashMap<(AccountId, CursorScope), ChangeCursor>,
    backfill: HashMap<(AccountId, CursorScope, Partition), BackfillCheckpoint>,
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
    fn put_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        cursor: ChangeCursor,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        let account = account.clone();
        Box::pin(async move {
            let mut guard = self.inner.lock().expect("poisoned");
            guard.change.insert((account, cursor.scope.clone()), cursor);
            Ok(())
        })
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>> {
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
        checkpoint: BackfillCheckpoint,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        let account = account.clone();
        Box::pin(async move {
            let mut guard = self.inner.lock().expect("poisoned");
            let key = (
                account,
                checkpoint.scope.clone(),
                checkpoint.partition.clone(),
            );
            guard.backfill.insert(key, checkpoint);
            Ok(())
        })
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn Future<Output = Result<Option<BackfillCheckpoint>, Error>> + Send + 'a>> {
        let account = account.clone();
        let scope = scope.clone();
        Box::pin(async move {
            let guard = self.inner.lock().expect("poisoned");
            let mut latest: Option<BackfillCheckpoint> = None;
            for ((aid, s, _p), ck) in &guard.backfill {
                if aid == &account && s == &scope {
                    let beats_current = match &latest {
                        None => true,
                        Some(existing) => backfill_checkpoint_is_later(ck, existing),
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
