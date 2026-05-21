//! `Control` handle implementation.
//!
//! `bifrost_types::Control` is the consumer-facing trait. `SyncControl`
//! is the engine's concrete implementor: holds the boundary sender,
//! the priority watch sender, the bandwidth meters, and the
//! per-generation checkpoint signal.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{AccountId, Checkpoint, Control, Error as TypesError, Priority};
use tokio::sync::watch;

use crate::cancel::{Boundary, BoundaryRequest};

/// Concrete `Control` implementation.
///
/// Cloneable so the engine can hand out additional handles (e.g. one
/// per `account_changes_stream` subscription, sharing the same lifecycle).
#[derive(Clone)]
pub struct SyncControl {
    inner: Arc<SyncControlInner>,
}

/// Latest checkpoint snapshot carried by the `watch` channel. The
/// generation counter increments each time `pause` or
/// `checkpoint_now` is invoked, so `wait_for_checkpoint` returns the
/// FIRST checkpoint observed at or after the request's generation -
/// never a stale pre-request value.
#[derive(Debug, Clone)]
struct CheckpointSnapshot {
    /// Generation at which this checkpoint was recorded.
    generation: u64,
    /// The checkpoint itself, if any has been recorded yet.
    checkpoint: Option<Checkpoint>,
}

struct SyncControlInner {
    account: AccountId,
    boundary: Boundary,
    /// Watch channel carrying the latest persisted checkpoint plus a
    /// generation counter. The generation increments on every
    /// boundary-flip request (pause / checkpoint_now) so waiters
    /// distinguish their pre-request snapshot from the post-flip
    /// checkpoint they actually want.
    checkpoint_tx: watch::Sender<CheckpointSnapshot>,
    /// Monotonic generation counter; bumped by `pause` /
    /// `checkpoint_now` BEFORE flipping the boundary so the waiter
    /// reads the new generation before parking.
    generation: AtomicU64,
    priority: watch::Sender<Priority>,
    bandwidth_cap: AtomicU64,
    bandwidth_observed: AtomicU64,
}

/// Sentinel value used for `bandwidth_cap` to mean "no cap".
const BANDWIDTH_CAP_NONE: u64 = u64::MAX;

impl SyncControl {
    #[must_use]
    pub fn new(account: AccountId, boundary: Boundary, priority: watch::Sender<Priority>) -> Self {
        let (checkpoint_tx, _rx) = watch::channel(CheckpointSnapshot {
            generation: 0,
            checkpoint: None,
        });
        Self {
            inner: Arc::new(SyncControlInner {
                account,
                boundary,
                checkpoint_tx,
                generation: AtomicU64::new(0),
                priority,
                bandwidth_cap: AtomicU64::new(BANDWIDTH_CAP_NONE),
                bandwidth_observed: AtomicU64::new(0),
            }),
        }
    }

    /// Engine-side hook: workers report the last persisted checkpoint
    /// so `pause` / `checkpoint_now` can return it. Each call advances
    /// the watch channel with the current generation; waiters parked
    /// on `wait_for_checkpoint` see exactly the checkpoint produced
    /// at or after their own generation.
    pub async fn record_checkpoint(&self, checkpoint: Checkpoint) {
        let generation = self.inner.generation.load(Ordering::SeqCst);
        let snapshot = CheckpointSnapshot {
            generation,
            checkpoint: Some(checkpoint),
        };
        // `send` errors only if every receiver has dropped; the
        // sender side holds the canonical value so that is fine.
        let _ = self.inner.checkpoint_tx.send(snapshot);
    }

    /// Engine-side hook: bandwidth meter feeds observed throughput.
    pub fn observe_bandwidth(&self, bps: u64) {
        self.inner.bandwidth_observed.store(bps, Ordering::Relaxed);
    }

    /// Read the configured cap (used by `bifrost-net` if the engine
    /// chooses to forward it). `None` when no cap is set.
    #[must_use]
    pub fn bandwidth_cap_snapshot(&self) -> Option<u64> {
        let raw = self.inner.bandwidth_cap.load(Ordering::Relaxed);
        if raw == BANDWIDTH_CAP_NONE {
            None
        } else {
            Some(raw)
        }
    }

    /// Account id this control governs. Exposed for tracing spans.
    #[must_use]
    pub fn account(&self) -> &AccountId {
        &self.inner.account
    }

    /// Block until a checkpoint at or after the given generation
    /// arrives. The caller bumps `generation` itself before flipping
    /// the boundary, so the wait observes only post-request
    /// checkpoints.
    async fn wait_for_checkpoint_at_or_after(
        &self,
        generation: u64,
    ) -> Result<Checkpoint, TypesError> {
        // Subscribe to a fresh receiver. The current value is the
        // last recorded snapshot; if it already matches the
        // generation we return immediately.
        let mut rx = self.inner.checkpoint_tx.subscribe();
        loop {
            {
                let snap = rx.borrow();
                if snap.generation >= generation
                    && let Some(c) = snap.checkpoint.clone()
                {
                    return Ok(c);
                }
            }
            if rx.changed().await.is_err() {
                return Err(TypesError::Other(
                    "control: checkpoint watch channel closed".into(),
                ));
            }
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
            // Bump the generation BEFORE flipping the boundary so
            // `record_checkpoint` calls that race with us land in the
            // new generation, not the old one.
            let gen_id = self.inner.generation.fetch_add(1, Ordering::SeqCst) + 1;
            self.inner.boundary.set(BoundaryRequest::Pause);
            self.wait_for_checkpoint_at_or_after(gen_id).await
        })
    }

    fn checkpoint_now(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Checkpoint, TypesError>> + Send + '_>,
    > {
        Box::pin(async move {
            let gen_id = self.inner.generation.fetch_add(1, Ordering::SeqCst) + 1;
            self.inner.boundary.set(BoundaryRequest::CheckpointNow);
            let cp = self.wait_for_checkpoint_at_or_after(gen_id).await?;
            // `CheckpointNow` returns the boundary to `Run` after the
            // flush; the worker observes the next batch's boundary
            // peek and continues without waiting.
            self.inner.boundary.set(BoundaryRequest::Run);
            Ok(cp)
        })
    }

    fn resume(&self) {
        self.inner.boundary.set(BoundaryRequest::Run);
    }

    fn priority(&self, p: Priority) {
        let _ = self.inner.priority.send(p);
    }

    fn bandwidth_cap(&self, bps: Option<u64>) {
        let stored = bps.unwrap_or(BANDWIDTH_CAP_NONE);
        self.inner.bandwidth_cap.store(stored, Ordering::Relaxed);
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
