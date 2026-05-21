//! Push reconciler + `InvalidationSink` implementation.
//!
//! Two push paths converge on one per-account `mpsc::Sender<WatchEvent>`:
//! - In-process: the multiplexer forwards `Account::push_stream` items.
//! - Out-of-process: the consumer holds an `Arc<dyn InvalidationSink>`
//!   (from `SyncEngine::invalidation_sink`) and calls `push` from its
//!   Pub/Sub or webhook receiver.
//!
//! The reconciler reads the merged stream and runs `changes_stream`
//! for each affected scope via `drive_to_completion`. Output flows to
//! the same broadcast as the multiplexer's, so consumers do not see
//! "push-derived" vs "poll-derived" changes - just one unified
//! `Change` stream.

pub mod reconciler;
pub mod subscription;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{
    AccountId, HintPayload, InvalidationHint, InvalidationSink, PushSource, WatchEvent,
};
use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub use reconciler::{Reconciler, scopes_for_hint};
pub use subscription::SubscriptionRegistry;

/// Engine-side handle stashed in the per-account slot for the push
/// reconciler task.
#[derive(Debug)]
pub struct PushHandle {
    pub cancel: CancellationToken,
    pub sender: mpsc::Sender<WatchEvent>,
}

/// Engine-side `InvalidationSink` implementation.
///
/// `senders` is the registry of per-account mpsc senders. `attach`
/// inserts; `detach` removes. The drop counter is exposed as
/// `bifrost_sync_push_dropped_total`.
#[derive(Debug)]
pub struct InvalidationSinkInner {
    senders: DashMap<AccountId, mpsc::Sender<WatchEvent>>,
    drop_counter: AtomicU64,
}

impl InvalidationSinkInner {
    #[must_use]
    pub fn new() -> Self {
        Self {
            senders: DashMap::new(),
            drop_counter: AtomicU64::new(0),
        }
    }

    pub fn register(&self, account: AccountId, sender: mpsc::Sender<WatchEvent>) {
        self.senders.insert(account, sender);
    }

    pub fn unregister(&self, account: &AccountId) {
        self.senders.remove(account);
    }

    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.drop_counter.load(Ordering::Relaxed)
    }

    /// Engine-internal helper: enumerate scopes affected by a hint
    /// using the cursor registry's membership index. Mirrors
    /// `scopes_for_hint` but exposed here so out-of-process callers
    /// can compute the same answer for observability without the
    /// reconciler in scope.
    #[allow(dead_code)]
    pub(crate) fn affected_scopes(
        &self,
        registry: &crate::cursor::CursorRegistry,
        hint: &HintPayload,
    ) -> Vec<bifrost_types::CursorScope> {
        scopes_for_hint(registry, hint)
    }
}

impl Default for InvalidationSinkInner {
    fn default() -> Self {
        Self::new()
    }
}

impl InvalidationSink for InvalidationSinkInner {
    fn push(&self, account: AccountId, event: WatchEvent) {
        let Some(tx) = self.senders.get(&account) else {
            return;
        };
        match tx.try_send(event.clone()) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(rejected)) => {
                // Channel full: try to land a coalesced
                // `HintPayload::Unknown` so the reconciler still
                // performs a full reconcile. We spawn a short-lived
                // sender task that uses `send().await` with a tight
                // deadline so a transiently-full channel doesn't
                // drop the wakeup entirely. Increment the drop counter
                // for observability either way.
                let unknown = WatchEvent::Invalidated {
                    hint: InvalidationHint {
                        source: PushSource::Coalesced,
                        payload: HintPayload::Unknown,
                    },
                };
                let _ = rejected;
                let sender = tx.clone();
                self.drop_counter.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let deadline = tokio::time::Duration::from_millis(100);
                    let _ = tokio::time::timeout(deadline, sender.send(unknown)).await;
                });
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Account detached during in-flight push.
            }
        }
    }
}

/// Convenience: wrap in an `Arc`.
#[must_use]
pub fn shared_sink() -> Arc<InvalidationSinkInner> {
    Arc::new(InvalidationSinkInner::new())
}
