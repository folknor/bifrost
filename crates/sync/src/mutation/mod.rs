//! Mutation pipeline.
//!
//! The engine wraps each `Account::bulk_*` call with:
//! - `IdempotencyKey` vending (run_id + monotonic sequence + protocol salt),
//! - per-id accumulation through `ItemOutcome<MutationSuccess>` lanes
//!   (applied / skipped / failed_terminal / pending_retry /
//!   pending_readback / blocked_by_engine),
//! - retry on `RecoveryClass::Retry`, sleeping per `RetryAdvice` and
//!   re-submitting unresolved ids with the same key,
//! - the read-back guard, which re-fetches each pending-readback id via
//!   `account.get_stream(_, Projection::FlagsOnly)` and reconciles
//!   apparent failures against actual server state.

pub mod idempotency;
pub mod readback;

use tokio_util::sync::CancellationToken;

pub use idempotency::{IdempotencyVendor, MutationCampaignId};
pub use readback::{ReadbackOutcome, run_readback_guard};

/// Engine-side handle stashed in the per-account slot for the mutation
/// pipeline.
#[derive(Debug)]
pub struct MutationHandle {
    pub cancel: CancellationToken,
}

/// Aggregate counters for a mutation campaign.
#[derive(Debug, Default, Clone, Copy)]
pub struct MutationCounters {
    pub applied: u64,
    pub skipped: u64,
    pub failed_terminal: u64,
    pub pending_retry: u64,
}

impl MutationCounters {
    pub fn record_applied(&mut self) {
        self.applied = self.applied.saturating_add(1);
    }
    pub fn record_skipped(&mut self) {
        self.skipped = self.skipped.saturating_add(1);
    }
    pub fn record_failed(&mut self) {
        self.failed_terminal = self.failed_terminal.saturating_add(1);
    }
    pub fn record_pending(&mut self) {
        self.pending_retry = self.pending_retry.saturating_add(1);
    }
}

/// Cross-account fanout partitioner.
///
/// Single-pass and bounded: partitions a stream of `(AccountId, T)`
/// pairs into per-account sub-channels of `T`. The caller pre-builds
/// the receivers map (one `mpsc::Sender<T>` per attached account) and
/// hands it to `partition_by_account`; items addressed to accounts
/// not in the map are dropped silently (the consumer-facing contract
/// is "you addressed an unattached account, that is your bug, not
/// the engine's").
///
/// Returns a `JoinHandle` the caller can `await` on to know when the
/// input stream has terminated.
pub mod fanout {
    use std::collections::HashMap;

    use bifrost_types::AccountId;
    use futures::stream::{Stream, StreamExt};
    use tokio::sync::mpsc;

    /// Drive `input` onto pre-built per-account receivers. Returns a
    /// `JoinHandle<()>` that completes when `input` ends.
    pub fn partition_by_account<S, T>(
        mut input: S,
        receivers: HashMap<AccountId, mpsc::Sender<T>>,
    ) -> tokio::task::JoinHandle<()>
    where
        S: Stream<Item = (AccountId, T)> + Unpin + Send + 'static,
        T: Send + 'static,
    {
        tokio::spawn(async move {
            while let Some((account, item)) = input.next().await {
                if let Some(tx) = receivers.get(&account) {
                    let _ = tx.send(item).await;
                }
            }
        })
    }
}
