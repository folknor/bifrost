//! `Control` handle implementation.
//!
//! `bifrost_types::Control` is the consumer-facing trait. `SyncControl`
//! is the engine's concrete implementor: holds the boundary sender,
//! the priority watch sender, the bandwidth meters, and the
//! per-generation checkpoint signal.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountId, Cause, Checkpoint, Control,
    CursorScope, Priority, RequestCause, RequestErrorKind,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

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
/// `checkpoint_now` is invoked. A waiter returns only after a
/// quiescence observation in its own generation, carrying the latest
/// durable checkpoint snapshot when one exists.
#[derive(Debug, Clone)]
struct CheckpointSnapshot {
    /// Generation at which this checkpoint was recorded.
    generation: u64,
    /// The checkpoint itself, if any has been recorded yet.
    checkpoint: Option<Checkpoint>,
    /// No engine stream operation is active and every broadcast
    /// checkpoint has been consumer-acked.
    quiescent: bool,
}

struct SyncControlInner {
    account: AccountId,
    boundary: Boundary,
    /// Watch channel carrying the latest persisted checkpoint and the
    /// generation in which quiescence was observed.
    checkpoint_tx: watch::Sender<CheckpointSnapshot>,
    /// Monotonic generation counter; bumped by `pause` /
    /// `checkpoint_now` BEFORE flipping the boundary so the waiter
    /// reads the new generation before parking.
    generation: AtomicU64,
    active: AtomicU64,
    /// Broadcast checkpoints awaiting a consumer ack, at most one per
    /// `pending_key` (lane + scope). Every entry leaves through either
    /// `record_checkpoint` (acked and durable), `retire_checkpoint`
    /// (ack processed but not durable, or never delivered), or
    /// supersession by a newer broadcast on the same key - so an
    /// unacked batch can never wedge boundary waiters indefinitely.
    pending_checkpoints: Mutex<Vec<Checkpoint>>,
    priority: watch::Sender<Priority>,
    bandwidth_cap: watch::Sender<Option<u64>>,
    bandwidth_observed: AtomicU64,
}

impl SyncControl {
    #[must_use]
    pub fn new(
        account: AccountId,
        boundary: Boundary,
        priority: watch::Sender<Priority>,
        bandwidth_cap: watch::Sender<Option<u64>>,
    ) -> Self {
        let (checkpoint_tx, _rx) = watch::channel(CheckpointSnapshot {
            generation: 0,
            checkpoint: None,
            quiescent: true,
        });
        Self {
            inner: Arc::new(SyncControlInner {
                account,
                boundary,
                checkpoint_tx,
                generation: AtomicU64::new(0),
                active: AtomicU64::new(0),
                pending_checkpoints: Mutex::new(Vec::new()),
                priority,
                bandwidth_cap,
                bandwidth_observed: AtomicU64::new(0),
            }),
        }
    }

    /// Engine-side hook called after the checkpoint store accepts a
    /// consumer ack. Removes the matching outstanding broadcast and
    /// refreshes the latest durable snapshot.
    ///
    /// Removal is by exact identity, not by `pending_key`: acking an
    /// older checkpoint says nothing about a newer outstanding one on
    /// the same scope, and claiming otherwise would report a safe
    /// boundary the consumer has not actually reached. Supersession in
    /// `expect_checkpoint` is what keeps the set bounded.
    pub async fn record_checkpoint(&self, checkpoint: Checkpoint) {
        {
            let mut pending = match self.inner.pending_checkpoints.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some(index) = pending.iter().position(|expected| expected == &checkpoint) {
                pending.remove(index);
            }
        }
        let generation = self.inner.generation.load(Ordering::SeqCst);
        let snapshot = CheckpointSnapshot {
            generation,
            checkpoint: Some(checkpoint),
            quiescent: self.is_quiescent(),
        };
        self.inner.checkpoint_tx.send_replace(snapshot);
    }

    /// Register a protocol operation that may broadcast work. The
    /// increment-then-boundary-check closes the pause race: either the
    /// pause observes this activity, or this method observes the
    /// pause and drops the guard before any work starts.
    #[must_use]
    pub(crate) fn begin_activity(&self) -> Option<SyncActivityGuard> {
        self.inner.active.fetch_add(1, Ordering::SeqCst);
        if matches!(self.inner.boundary.snapshot(), BoundaryRequest::Run) {
            Some(SyncActivityGuard {
                control: self.clone(),
            })
        } else {
            self.end_activity();
            None
        }
    }

    /// Mark a broadcast checkpoint as outstanding BEFORE publishing
    /// its batch. Quiescence cannot satisfy a boundary waiter until a
    /// matching consumer ack reaches `record_checkpoint`, or the
    /// broadcast is retired as never-acked.
    ///
    /// A newer broadcast on the same `pending_key` supersedes the
    /// older one, so the set is bounded by the account's scope count
    /// rather than by how many batches a consumer left unacked. A
    /// consumer that persists several batches and acks only the last
    /// checkpoint therefore still reaches a safe boundary.
    pub(crate) fn expect_checkpoint(&self, checkpoint: Checkpoint) {
        let mut pending = match self.inner.pending_checkpoints.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(key) = pending_key(&checkpoint) {
            pending.retain(|existing| pending_key(existing).as_ref() != Some(&key));
        }
        // Backstop for a future `Checkpoint` variant with no key: the
        // set must never grow without bound, and a consumer with this
        // many distinct outstanding broadcasts is already broken.
        if pending.len() >= PENDING_CHECKPOINT_CAP {
            let dropped = pending.remove(0);
            tracing::warn!(
                target: "bifrost.sync.control",
                account = ?self.inner.account,
                cap = PENDING_CHECKPOINT_CAP,
                dropped = ?dropped,
                "pending checkpoint set at capacity; dropping the oldest outstanding broadcast"
            );
        }
        pending.push(checkpoint);
    }

    /// Engine-side hook for a broadcast that will never produce a
    /// durable checkpoint: the checkpoint store rejected the ack, or
    /// the batch reached no real subscriber so no ack is coming.
    ///
    /// The batch is no longer in flight either way, so it must stop
    /// gating boundary waiters - leaving it pending would wedge every
    /// later `pause` / `checkpoint_now` on this account for the rest
    /// of the process. The durable snapshot is deliberately NOT
    /// advanced, because nothing new became durable; a consumer whose
    /// ack failed learns that from `ack_checkpoint`'s own `Result`.
    pub(crate) fn retire_checkpoint(&self, checkpoint: &Checkpoint) {
        {
            let mut pending = match self.inner.pending_checkpoints.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some(index) = pending.iter().position(|expected| expected == checkpoint) {
                pending.remove(index);
            }
        }
        let generation = self.inner.generation.load(Ordering::SeqCst);
        self.publish_quiescence(generation);
    }

    /// Engine-side hook: bandwidth meter feeds observed throughput.
    pub fn observe_bandwidth(&self, bps: u64) {
        self.inner.bandwidth_observed.store(bps, Ordering::Relaxed);
    }

    /// Read the configured cap. `None` when no cap is set.
    #[must_use]
    pub fn bandwidth_cap_snapshot(&self) -> Option<u64> {
        *self.inner.bandwidth_cap.borrow()
    }

    /// Latest requested priority.
    #[must_use]
    pub fn priority_snapshot(&self) -> Priority {
        *self.inner.priority.borrow()
    }

    /// Account id this control governs. Exposed for tracing spans.
    #[must_use]
    pub fn account(&self) -> &AccountId {
        &self.inner.account
    }

    pub(crate) async fn wait_until_running(&self, shutdown: &CancellationToken) -> bool {
        let mut boundary = self.inner.boundary.subscribe();
        loop {
            match boundary.peek() {
                BoundaryRequest::Run => return true,
                BoundaryRequest::Stop => return false,
                BoundaryRequest::Pause | BoundaryRequest::CheckpointNow => {}
            }
            tokio::select! {
                () = shutdown.cancelled() => return false,
                changed = boundary.changed() => {
                    if changed.is_none() {
                        return false;
                    }
                }
            }
        }
    }

    fn is_quiescent(&self) -> bool {
        if self.inner.active.load(Ordering::SeqCst) != 0 {
            return false;
        }
        let pending = match self.inner.pending_checkpoints.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        pending.is_empty()
    }

    fn publish_quiescence(&self, generation: u64) {
        if !self.is_quiescent() {
            return;
        }
        let checkpoint = self.inner.checkpoint_tx.borrow().checkpoint.clone();
        self.inner.checkpoint_tx.send_replace(CheckpointSnapshot {
            generation,
            checkpoint,
            quiescent: true,
        });
    }

    fn end_activity(&self) {
        let previous = self.inner.active.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(previous > 0, "sync activity counter underflow");
        if previous == 1 {
            let generation = self.inner.generation.load(Ordering::SeqCst);
            self.publish_quiescence(generation);
        }
    }

    /// Block until the account is quiescent at or after the request
    /// generation. The latest durable checkpoint may be absent when
    /// the account has never produced traffic.
    async fn wait_for_checkpoint_at_or_after(
        &self,
        generation: u64,
    ) -> Result<Option<Checkpoint>, AccountError> {
        // Subscribe to a fresh receiver. The current value is the
        // last recorded snapshot; if it already matches the
        // generation we return immediately.
        let mut rx = self.inner.checkpoint_tx.subscribe();
        loop {
            {
                let snap = rx.borrow();
                if snap.generation >= generation && snap.quiescent {
                    return Ok(snap.checkpoint.clone());
                }
            }
            if rx.changed().await.is_err() {
                return Err(AccountErrorBuilder::new(
                    AccountErrorKind::Request(RequestErrorKind::Malformed),
                    Cause::Request(RequestCause::Malformed {
                        detail: bifrost_types::DiagnosticText::support_only(
                            "control: checkpoint watch channel closed",
                        ),
                    }),
                )
                .try_build()
                .expect("valid account error classification"));
            }
        }
    }
}

/// Hard ceiling on outstanding broadcast checkpoints. Reachable only
/// through a `Checkpoint` variant `pending_key` does not recognise;
/// keyed variants are already bounded by the account's scope count.
const PENDING_CHECKPOINT_CAP: usize = 1024;

/// Supersession identity of an outstanding broadcast: the lane (change
/// cursor vs backfill) plus the scope it advances. Broadcasts within a
/// lane+scope are produced by a single sequential task, so the newest
/// one subsumes its predecessors - if the consumer acks it, everything
/// before it on that scope is durable too.
///
/// `None` for a variant this revision does not know, which falls back
/// to plain accumulation under `PENDING_CHECKPOINT_CAP`.
fn pending_key(checkpoint: &Checkpoint) -> Option<(u8, CursorScope)> {
    match checkpoint {
        Checkpoint::Change(cursor) => Some((0, cursor.scope.clone())),
        Checkpoint::Backfill(backfill) => Some((1, backfill.scope.clone())),
        _ => None,
    }
}

pub(crate) struct SyncActivityGuard {
    control: SyncControl,
}

impl Drop for SyncActivityGuard {
    fn drop(&mut self) {
        self.control.end_activity();
    }
}

impl Control for SyncControl {
    fn pause(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<Checkpoint>, AccountError>> + Send + '_>,
    > {
        Box::pin(async move {
            // Bump the generation BEFORE flipping the boundary so
            // `record_checkpoint` calls that race with us land in the
            // new generation, not the old one.
            let gen_id = self.inner.generation.fetch_add(1, Ordering::SeqCst) + 1;
            self.inner
                .boundary
                .set_unless_stopped(BoundaryRequest::Pause);
            self.publish_quiescence(gen_id);
            self.wait_for_checkpoint_at_or_after(gen_id).await
        })
    }

    fn checkpoint_now(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<Checkpoint>, AccountError>> + Send + '_>,
    > {
        Box::pin(async move {
            let gen_id = self.inner.generation.fetch_add(1, Ordering::SeqCst) + 1;
            // Read-and-install is one atomic step, so a `Pause` landing
            // between the two is never displaced by this request.
            let previous = self.inner.boundary.request_checkpoint();
            self.publish_quiescence(gen_id);
            let cp = self.wait_for_checkpoint_at_or_after(gen_id).await?;
            // Restore only if no concurrent control or engine action
            // changed the boundary while this request was waiting.
            if let Some(previous) = previous {
                self.inner
                    .boundary
                    .restore_if_current(BoundaryRequest::CheckpointNow, previous);
            }
            Ok(cp)
        })
    }

    fn resume(&self) {
        self.inner.boundary.set_unless_stopped(BoundaryRequest::Run);
    }

    fn priority(&self, p: Priority) {
        self.inner.priority.send_replace(p);
    }

    fn bandwidth_cap(&self, bps: Option<u64>) {
        self.inner.bandwidth_cap.send_replace(bps);
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

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{ChangeCursor, CursorScope, OpaqueChangeState, ProtocolKind};

    fn control() -> SyncControl {
        let (boundary, _view) = Boundary::new();
        let (priority, _priority_view) = watch::channel(Priority::Normal);
        let (bandwidth, _bandwidth_view) = watch::channel(None);
        SyncControl::new(
            AccountId("control-unit".into()),
            boundary,
            priority,
            bandwidth,
        )
    }

    fn checkpoint(state: &[u8]) -> Checkpoint {
        Checkpoint::Change(ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Imap,
                envelope_version: 1,
                bytes: state.to_vec(),
            },
            advanced_through: None,
            envelope_version: 1,
        })
    }

    #[tokio::test]
    async fn pause_waits_for_active_work_to_reach_a_boundary() {
        let control = control();
        let activity = control.begin_activity().expect("running activity");
        let waiter_control = control.clone();
        let waiter = tokio::spawn(async move { waiter_control.pause().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        drop(activity);
        assert_eq!(waiter.await.expect("waiter task").expect("pause"), None);
    }

    #[tokio::test]
    async fn pause_waits_for_the_consumer_ack_of_a_broadcast_checkpoint() {
        let control = control();
        let activity = control.begin_activity().expect("running activity");
        let expected = checkpoint(b"pending");
        control.expect_checkpoint(expected.clone());
        let waiter_control = control.clone();
        let waiter = tokio::spawn(async move { waiter_control.pause().await });
        tokio::task::yield_now().await;

        drop(activity);
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "quiescent workers are not safe while a consumer ack is outstanding"
        );

        control.record_checkpoint(expected.clone()).await;
        assert_eq!(
            waiter.await.expect("waiter task").expect("pause"),
            Some(expected)
        );
    }

    fn backfill_checkpoint(scope: CursorScope, partition: &[u8]) -> Checkpoint {
        Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
            scope,
            partition: bifrost_types::Partition(partition.to_vec()),
            progress_marker: None,
            progress: bifrost_types::BackfillProgress::default(),
            envelope_version: 1,
        })
    }

    fn pending_len(control: &SyncControl) -> usize {
        control
            .inner
            .pending_checkpoints
            .lock()
            .expect("pending lock")
            .len()
    }

    /// A consumer that persists several batches and acks only the last
    /// checkpoint must still reach a safe boundary. Without
    /// supersession every skipped ack would strand an entry that no
    /// later ack can match, wedging this account's waiters forever.
    #[tokio::test]
    async fn acking_only_the_latest_checkpoint_still_reaches_a_boundary() {
        let control = control();
        for state in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
            control.expect_checkpoint(checkpoint(state));
        }
        assert_eq!(
            pending_len(&control),
            1,
            "a newer broadcast on the same scope supersedes its predecessors"
        );

        control.record_checkpoint(checkpoint(b"three")).await;
        assert_eq!(
            control.pause().await.expect("pause"),
            Some(checkpoint(b"three"))
        );
    }

    /// Supersession is per lane and per scope: concurrent work on
    /// distinct scopes must each be tracked, or a pause would report a
    /// safe boundary while another scope's batch is still unacked.
    #[test]
    fn supersession_is_keyed_by_lane_and_scope() {
        let control = control();
        let email = CursorScope::Type(bifrost_types::ObjectType::Email);
        control.expect_checkpoint(checkpoint(b"account-change"));
        control.expect_checkpoint(backfill_checkpoint(CursorScope::Account, b"page:0:500"));
        control.expect_checkpoint(backfill_checkpoint(email.clone(), b"page:0:500"));
        assert_eq!(pending_len(&control), 3);

        // Same lane, same scope, later window: replaces, does not add.
        control.expect_checkpoint(backfill_checkpoint(email, b"page:500:1000"));
        assert_eq!(pending_len(&control), 3);
    }

    /// A checkpoint-store write failure leaves nothing durable, but the
    /// batch is no longer in flight. Retiring it keeps the boundary
    /// primitive usable; the durable snapshot must NOT advance.
    #[tokio::test]
    async fn a_failed_ack_retires_its_broadcast_without_advancing_the_snapshot() {
        let control = control();
        control.record_checkpoint(checkpoint(b"durable")).await;
        let failed = checkpoint(b"never-persisted");
        control.expect_checkpoint(failed.clone());

        control.retire_checkpoint(&failed);

        assert_eq!(pending_len(&control), 0);
        assert_eq!(
            control.pause().await.expect("pause"),
            Some(checkpoint(b"durable")),
            "a failed ack must not be reported as the durable checkpoint"
        );
    }

    /// A batch that reached only the slot's sentinel receiver gets its
    /// registration retracted, so an account with no live consumer
    /// still pauses.
    #[tokio::test]
    async fn a_retracted_registration_does_not_gate_the_boundary() {
        let control = control();
        let undelivered = checkpoint(b"no-subscriber");
        control.expect_checkpoint(undelivered.clone());
        control.retire_checkpoint(&undelivered);

        assert_eq!(control.pause().await.expect("pause"), None);
    }

    #[tokio::test]
    async fn checkpoint_waiter_does_not_restore_over_a_concurrent_pause() {
        let control = control();
        let activity = control.begin_activity().expect("running activity");
        let waiter_control = control.clone();
        let waiter = tokio::spawn(async move { waiter_control.checkpoint_now().await });
        tokio::task::yield_now().await;
        assert_eq!(
            control.inner.boundary.snapshot(),
            BoundaryRequest::CheckpointNow
        );

        control.inner.boundary.set(BoundaryRequest::Pause);
        drop(activity);

        assert_eq!(
            waiter.await.expect("waiter task").expect("checkpoint"),
            None
        );
        assert_eq!(control.inner.boundary.snapshot(), BoundaryRequest::Pause);
    }
}
