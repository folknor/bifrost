//! `Control` handle implementation.
//!
//! `bifrost_types::Control` is the consumer-facing trait. `SyncControl`
//! is the engine's concrete implementor: holds the boundary sender,
//! the priority watch sender, the bandwidth meters, and the
//! checkpoint store handle.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{AccountId, Checkpoint, Control, Error as TypesError, Priority};
use tokio::sync::{Notify, watch};

use crate::cancel::{Boundary, BoundaryRequest};

/// Concrete `Control` implementation.
///
/// Cloneable so the engine can hand out additional handles (e.g. one
/// per `account_changes_stream` subscription, sharing the same lifecycle).
#[derive(Clone)]
pub struct SyncControl {
    inner: Arc<SyncControlInner>,
}

struct SyncControlInner {
    account: AccountId,
    boundary: Boundary,
    boundary_recipient: Arc<Notify>,
    last_checkpoint: tokio::sync::Mutex<Option<Checkpoint>>,
    priority: watch::Sender<Priority>,
    bandwidth_cap: tokio::sync::Mutex<Option<u64>>,
    bandwidth_observed: AtomicU64,
}

impl SyncControl {
    #[must_use]
    pub fn new(account: AccountId, boundary: Boundary, priority: watch::Sender<Priority>) -> Self {
        Self {
            inner: Arc::new(SyncControlInner {
                account,
                boundary,
                boundary_recipient: Arc::new(Notify::new()),
                last_checkpoint: tokio::sync::Mutex::new(None),
                priority,
                bandwidth_cap: tokio::sync::Mutex::new(None),
                bandwidth_observed: AtomicU64::new(0),
            }),
        }
    }

    /// Engine-side hook: workers report the last persisted checkpoint
    /// so `pause` / `checkpoint_now` can return it.
    pub async fn record_checkpoint(&self, checkpoint: Checkpoint) {
        let mut guard = self.inner.last_checkpoint.lock().await;
        *guard = Some(checkpoint);
        self.inner.boundary_recipient.notify_waiters();
    }

    /// Engine-side hook: bandwidth meter feeds observed throughput.
    pub fn observe_bandwidth(&self, bps: u64) {
        self.inner.bandwidth_observed.store(bps, Ordering::Relaxed);
    }

    /// Read the configured cap (used by `bifrost-net` if the engine
    /// chooses to forward it).
    pub async fn bandwidth_cap_snapshot(&self) -> Option<u64> {
        let guard = self.inner.bandwidth_cap.lock().await;
        *guard
    }

    /// Account id this control governs. Exposed for tracing spans.
    #[must_use]
    pub fn account(&self) -> &AccountId {
        &self.inner.account
    }

    async fn wait_for_checkpoint(&self) -> Result<Checkpoint, TypesError> {
        // The boundary worker calls record_checkpoint after persisting.
        // We park on the recipient Notify until that happens.
        loop {
            let notified = self.inner.boundary_recipient.notified();
            {
                let guard = self.inner.last_checkpoint.lock().await;
                if let Some(c) = guard.as_ref() {
                    return Ok(c.clone());
                }
            }
            notified.await;
        }
    }
}

impl Control for SyncControl {
    fn pause(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Checkpoint, TypesError>> + Send + '_>,
    > {
        Box::pin(async move {
            self.inner.boundary.set(BoundaryRequest::Pause);
            self.wait_for_checkpoint().await
        })
    }

    fn checkpoint_now(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Checkpoint, TypesError>> + Send + '_>,
    > {
        Box::pin(async move {
            self.inner.boundary.set(BoundaryRequest::CheckpointNow);
            self.wait_for_checkpoint().await
        })
    }

    fn resume(&self) {
        self.inner.boundary.set(BoundaryRequest::Run);
    }

    fn priority(&self, p: Priority) {
        let _ = self.inner.priority.send(p);
    }

    fn bandwidth_cap(&self, bps: Option<u64>) {
        // Lock here is fine: bandwidth_cap is a control-plane knob,
        // not on the hot path; the lock is uncontested.
        if let Ok(mut g) = self.inner.bandwidth_cap.try_lock() {
            *g = bps;
        }
    }

    fn bandwidth_observed(&self) -> u64 {
        self.inner.bandwidth_observed.load(Ordering::Relaxed)
    }
}

impl std::fmt::Debug for SyncControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncControl")
            .field("account", &self.inner.account)
            .finish_non_exhaustive()
    }
}
