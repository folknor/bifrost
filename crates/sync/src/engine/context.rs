//! The per-slot worker context.
//!
//! Every long-running worker `attach` spawns needs some subset of the
//! same eighteen handles, and each one used to take them as positional
//! arguments - the backfill orchestrator carried sixteen, the deferred
//! inventory worker twenty, and the reopen listener rebuilt a
//! seventeen-field [`RecoveryContext`] literal by hand at each of its
//! call sites. Bundling them once at the end of `attach` means a worker
//! takes the context plus only the handles that are genuinely its own,
//! and a new slot-wide handle is added in one place rather than threaded
//! through four signatures.
//!
//! Cheap to clone by construction: every field is an `Arc`, a channel
//! sender, or a handle that is itself `Arc`-backed.

use super::*;

/// Slot-wide handles shared by the workers `attach` spawns.
#[derive(Clone)]
pub(super) struct SlotContext {
    pub factory: Arc<dyn AccountFactory>,
    /// The currently-open protocol handle. Workers load through the
    /// `ArcSwap` on every hot-path use so a reopen is observed
    /// immediately.
    pub current: Arc<ArcSwap<Arc<dyn Account>>>,
    pub cursors: Arc<CursorRegistry>,
    pub changes_tx: broadcast::Sender<MultiplexerEvent>,
    pub account_id: AccountId,
    pub control: SyncControl,
    pub account_control_tx: broadcast::Sender<AccountControl>,
    pub throttles: Arc<std::sync::Mutex<crate::recovery::ThrottleBucket>>,
    pub boundary_tx: watch::Sender<crate::cancel::BoundaryRequest>,
    pub capabilities: Arc<std::sync::RwLock<AccountCapabilities>>,
    pub subscriptions: Arc<SubscriptionRegistry>,
    pub account_generation_tx: watch::Sender<u64>,
    pub reopen_lock: Arc<AsyncMutex<()>>,
    /// Slot-lifetime open-skip lane; a successful reattach replaces it
    /// with the replacement open's `OpenedAccount::skipped_scopes`.
    pub open_skips: Arc<std::sync::Mutex<Vec<SkippedScope>>>,
    /// Per-slot child of the engine's root cancellation token.
    pub shutdown: CancellationToken,
    /// Sender onto the account's single durable writer.
    pub writer_tx: mpsc::Sender<WriterRequest>,
    pub coverage: Arc<PendingCoverage>,
    /// Fired when a real consumer subscribes to `changes_tx`, so a
    /// cold-start worker can park instead of broadcasting into a channel
    /// with no reader.
    pub subscriber_notify: Arc<Notify>,
    pub scheduler: Scheduler,
}

impl SlotContext {
    /// A handle onto the account's single durable writer.
    ///
    /// Minted per use rather than stored, because `WriterHandle` is a
    /// thin wrapper over the sender and the recovery paths want one
    /// borrowed for the duration of a single dispatch.
    pub(super) fn writer(&self) -> WriterHandle {
        WriterHandle::new(self.writer_tx.clone())
    }

    /// Borrow this context as the recovery dispatch's argument bundle.
    ///
    /// The writer is passed separately because `RecoveryContext` borrows
    /// it, so it has to outlive the dispatch on the caller's stack.
    pub(super) fn recovery<'a>(&'a self, writer: &'a WriterHandle) -> RecoveryContext<'a> {
        RecoveryContext {
            factory: &self.factory,
            current: &self.current,
            cursors: &self.cursors,
            changes_tx: &self.changes_tx,
            account_id: &self.account_id,
            control: &self.control,
            account_control_tx: &self.account_control_tx,
            throttles: &self.throttles,
            boundary_tx: &self.boundary_tx,
            capabilities: &self.capabilities,
            subscriptions: &self.subscriptions,
            account_generation_tx: &self.account_generation_tx,
            reopen_lock: &self.reopen_lock,
            open_skips: &self.open_skips,
            shutdown: &self.shutdown,
            writer,
            coverage: &self.coverage,
        }
    }
}
