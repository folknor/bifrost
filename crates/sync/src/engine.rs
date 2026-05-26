//! `SyncEngine` lifecycle.
//!
//! Holds an `Arc<dyn AccountFactory>` per attached account, drives one
//! multiplexer / backfill / push reconciler / mutation pipeline per
//! slot, and exposes the engine's public surface: `attach`, `detach`,
//! `account_changes_stream`, `bulk_*` campaign entry points,
//! `invalidation_sink`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFactory, AccountId, AccountStream,
    ChangeCursor, Checkpoint, CursorEstablishment, CursorScope, DiagnosticText, EngineDirective,
    ErrorScope, InvalidationSink, InventoryPartition, InventoryPartitioning, ItemOutcome,
    MembershipScope, MutationSuccess, Priority, RecoveryClass, RetryAdvice, SubscriptionHandle,
    SyncEvent, WatchEvent,
};
use dashmap::DashMap;
use futures::stream::StreamExt;
use tokio::sync::{Mutex as AsyncMutex, broadcast, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::backfill::{
    BackfillHandle, BackfillPolicy, BackfillRegistry, BackfillRunner, BackfillState,
    BackfillStrategy, LiveSupersedes,
};
use crate::cancel::{Boundary, BoundaryRequest};
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::cursor::store::{DynCheckpointStore, InMemoryCheckpointStore};
use crate::error::Error;
use crate::multiplexer::{
    AckRequest, Multiplexer, MultiplexerEvent, MultiplexerHandle, ReopenRequest,
};
use crate::mutation::MutationHandle;
use crate::push::{InvalidationSinkInner, PushHandle, SubscriptionRegistry};
use crate::scheduler::{BudgetGate, ConcurrencyBudget, Scheduler};
use crate::types::{AccountSlot, BackfillConfig, EngineConfig, WorkerTask};

/// Top-level engine.
pub struct SyncEngine {
    config: EngineConfig,
    checkpoints: Arc<DynCheckpointStore>,
    accounts: DashMap<AccountId, Arc<AccountSlot>>,
    backfill_registry: Arc<BackfillRegistry>,
    sink: Arc<InvalidationSinkInner>,
    subscriptions: Arc<SubscriptionRegistry>,
    scheduler: Scheduler,
    root_cancel: CancellationToken,
    /// Optional bandwidth meter wired through `bifrost-net`. When
    /// present, `attach` spawns a periodic bandwidth-feed task per
    /// account so `Control::bandwidth_observed` returns a real reading.
    bandwidth_meter: Option<Arc<bifrost_net::BandwidthMeter>>,
    /// Per-account ack senders; consumers call `ack_checkpoint` to
    /// durably persist a cursor for a specific scope.
    ack_senders: DashMap<AccountId, mpsc::Sender<AckRequest>>,
    /// Per-account in-flight attach guard. Prevents two concurrent
    /// `attach` calls for the same account from spawning duplicate
    /// workers between the existence check and the final insert.
    attaching: Arc<AsyncMutex<std::collections::HashSet<AccountId>>>,
}

impl std::fmt::Debug for SyncEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncEngine")
            .field("attached_accounts", &self.accounts.len())
            .finish_non_exhaustive()
    }
}

/// Builder for `SyncEngine`. Lets the consumer pre-configure the
/// concurrency budget, checkpoint store, and tuning knobs before any
/// account is attached.
pub struct SyncEngineBuilder {
    config: EngineConfig,
    checkpoints: Option<Arc<DynCheckpointStore>>,
    bandwidth_meter: Option<Arc<bifrost_net::BandwidthMeter>>,
}

impl SyncEngineBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: EngineConfig::default(),
            checkpoints: None,
            bandwidth_meter: None,
        }
    }

    #[must_use]
    pub fn budget(mut self, budget: ConcurrencyBudget) -> Self {
        self.config.budget = budget;
        self
    }

    #[must_use]
    pub fn config(mut self, config: EngineConfig) -> Self {
        self.config = config;
        self
    }

    #[must_use]
    pub fn checkpoints(mut self, store: Arc<DynCheckpointStore>) -> Self {
        self.checkpoints = Some(store);
        self
    }

    /// Wire a `bifrost-net` `BandwidthMeter` so the engine can feed
    /// `Control::bandwidth_observed`. When this is unset,
    /// `bandwidth_observed` returns 0 for every account.
    #[must_use]
    pub fn with_bandwidth_meter(mut self, meter: Arc<bifrost_net::BandwidthMeter>) -> Self {
        self.bandwidth_meter = Some(meter);
        self
    }

    pub fn build(self) -> Result<SyncEngine, Error> {
        self.config.budget.validate()?;
        let checkpoints = self
            .checkpoints
            .unwrap_or_else(|| Arc::new(InMemoryCheckpointStore::new()));
        let budget_gate = BudgetGate::new(self.config.budget);
        let scheduler = Scheduler::with_lane_capacity(
            self.config.scheduler,
            budget_gate,
            self.config.lane_capacity,
        );
        Ok(SyncEngine {
            config: self.config,
            checkpoints,
            accounts: DashMap::new(),
            backfill_registry: Arc::new(BackfillRegistry::new()),
            sink: Arc::new(InvalidationSinkInner::new()),
            subscriptions: Arc::new(SubscriptionRegistry::new()),
            scheduler,
            root_cancel: CancellationToken::new(),
            bandwidth_meter: self.bandwidth_meter,
            ack_senders: DashMap::new(),
            attaching: Arc::new(AsyncMutex::new(std::collections::HashSet::new())),
        })
    }
}

impl Default for SyncEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

enum InitialScope {
    Ready,
    DeferredInventory(CursorScope),
}

impl SyncEngine {
    #[must_use]
    pub fn builder() -> SyncEngineBuilder {
        SyncEngineBuilder::new()
    }

    /// Attach an account to the engine.
    ///
    /// Flow per `bifrost-sync.md` -> attach:
    /// 1. Call `factory.open(account_id)` to obtain the first
    ///    `Arc<dyn Account>`.
    /// 2. Read `capabilities()` and stash on the slot.
    /// 3. For each scope from `discover_cursor_scopes()`, call
    ///    `establish_initial_cursor(scope)` and react accordingly.
    /// 4. Spawn multiplexer, backfill orchestrator, push reconciler.
    /// 5. Hand back a `SyncControl`.
    pub async fn attach(
        &self,
        account_id: AccountId,
        factory: Arc<dyn AccountFactory>,
    ) -> Result<SyncControl, Error> {
        // Take a per-account in-flight guard to close the duplicate-
        // attach race (existence check + factory.open + spawn workers
        // is a multi-await window between the early bail and the
        // final insert).
        {
            let mut guard = self.attaching.lock().await;
            if guard.contains(&account_id) || self.accounts.contains_key(&account_id) {
                return Err(Error::AccountAlreadyAttached(account_id));
            }
            guard.insert(account_id.clone());
        }
        let result = self.attach_inner(account_id.clone(), factory).await;
        // Always release the in-flight guard, success or failure.
        {
            let mut guard = self.attaching.lock().await;
            guard.remove(&account_id);
        }
        result
    }

    async fn attach_inner(
        &self,
        account_id: AccountId,
        factory: Arc<dyn AccountFactory>,
    ) -> Result<SyncControl, Error> {
        let opened = factory
            .open(account_id.clone())
            .await
            .map_err(Error::OpenFailed)?;
        let capabilities: AccountCapabilities = opened.capabilities().clone();

        let cursors = Arc::new(CursorRegistry::new());

        // Per-account broadcast for the unified Change stream. Keep a
        // sentinel receiver on the slot so the channel never closes
        // when subscribers come and go.
        let (changes_tx, sentinel_rx) =
            broadcast::channel::<MultiplexerEvent>(self.config.multiplexer.changes_capacity);

        // Drive cursor establishment per scope. We do this before
        // spawning long-running tasks because multiplexer + backfill
        // need the registry pre-populated for `Ready` scopes. For
        // `EstablishViaInventory` scopes the inventory walk IS the
        // cursor establishment AND surfaces inventory items - we
        // defer those walks until the slot can be subscribed to, so
        // consumers observe cold-start data the same way they observe
        // live changes.
        let scopes = self.discover_scopes(opened.as_ref()).await?;
        let mut deferred_inventory_scopes = Vec::new();
        for scope in scopes.clone() {
            match self
                .establish_one(&account_id, opened.as_ref(), scope, Arc::clone(&cursors))
                .await?
            {
                InitialScope::Ready => {}
                InitialScope::DeferredInventory(scope) => deferred_inventory_scopes.push(scope),
            }
        }

        // Drive membership discovery to populate the push-reconciler's
        // side-index. Bounded stream; one walk per attach.
        self.discover_and_link_memberships(opened.as_ref(), Arc::clone(&cursors))
            .await?;

        // Wire boundary + priority watches.
        let (boundary, boundary_view) = Boundary::new();
        let (priority_tx, priority_rx) = watch::channel(Priority::Normal);
        let (bandwidth_cap_tx, bandwidth_cap_rx) = watch::channel(None);

        // Per-account watch-event sender / receiver. The reconciler
        // owns the receiver; the multiplexer (in-process forwarder)
        // and the engine's `InvalidationSink` both feed the sender.
        let (watch_tx, watch_rx) =
            mpsc::channel::<WatchEvent>(self.config.multiplexer.watch_capacity);
        self.sink.register(account_id.clone(), watch_tx.clone());

        // Per-account ack channel: consumers push
        // (scope, checkpoint) here after committing the matching
        // batch; a dedicated writer task persists them to
        // `CheckpointStore`.
        let (ack_tx, ack_rx) = mpsc::channel::<AckRequest>(256);
        self.ack_senders.insert(account_id.clone(), ack_tx.clone());

        // Register per-account budget semaphores.
        self.scheduler.budget().register(account_id.clone());
        if let Some(meter) = &self.bandwidth_meter {
            meter.register_account(account_id.clone());
        }

        // Shutdown token tree: per-slot child of engine root.
        let shutdown = self.root_cancel.child_token();

        // ArcSwap holds the current `Arc<dyn Account>` so spawned
        // workers see post-reopen handles immediately.
        let current: Arc<ArcSwap<Arc<dyn Account>>> = Arc::new(ArcSwap::from(Arc::new(opened)));

        // Control handle shared between the SyncControl returned to
        // the consumer and the engine's spawned workers (so workers
        // can call `record_checkpoint`).
        let control = SyncControl::new(
            account_id.clone(),
            boundary.clone(),
            priority_tx.clone(),
            bandwidth_cap_tx.clone(),
        );

        // Reopen channel: the multiplexer's per-scope tasks raise
        // requests when a stream ends with a recoverable Fatal,
        // carrying the full `RecoveryClass`.
        let (reopen_tx, mut reopen_rx) = mpsc::channel::<ReopenRequest>(16);

        let mut workers: Vec<WorkerTask> = Vec::new();

        // Helper to track both the JoinHandle and a clone of its
        // AbortHandle so detach can fire `abort()` on timeout.
        let mut spawn = |fut: tokio::task::JoinHandle<()>| {
            workers.push(WorkerTask {
                abort: fut.abort_handle(),
                join: fut,
            });
        };

        // Ack writer: durably persists cursors as they are acked. Lives
        // as long as the slot does.
        let ack_writer_store = Arc::clone(&self.checkpoints);
        let ack_writer_aid = account_id.clone();
        let ack_writer_control = control.clone();
        spawn(tokio::spawn(ack_writer(
            ack_writer_aid,
            ack_writer_store,
            ack_writer_control,
            ack_rx,
        )));

        // Control applier: forwards priority and bandwidth-cap
        // changes to the currently-open protocol handle. Reopen also
        // reapplies the snapshots to the replacement handle.
        {
            let control_account = Arc::clone(&current);
            let control_shutdown = shutdown.clone();
            let mut priority_view = priority_rx.clone();
            let mut bandwidth_view = bandwidth_cap_rx.clone();
            spawn(tokio::spawn(async move {
                {
                    let account = control_account.load_full();
                    account
                        .as_ref()
                        .as_ref()
                        .set_priority(*priority_view.borrow());
                    account
                        .as_ref()
                        .as_ref()
                        .set_bandwidth_cap(*bandwidth_view.borrow());
                }
                loop {
                    tokio::select! {
                        () = control_shutdown.cancelled() => return,
                        changed = priority_view.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            let priority = *priority_view.borrow();
                            let account = control_account.load_full();
                            account.as_ref().as_ref().set_priority(priority);
                        }
                        changed = bandwidth_view.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            let cap = *bandwidth_view.borrow();
                            let account = control_account.load_full();
                            account.as_ref().as_ref().set_bandwidth_cap(cap);
                        }
                    }
                }
            }));
        }

        // Spawn push reconciler.
        let reconciler = crate::push::Reconciler {
            account_id: account_id.clone(),
            account: Arc::clone(&current),
            cursors: Arc::clone(&cursors),
            changes_tx: changes_tx.clone(),
            boundary: boundary_view.clone(),
            shutdown: shutdown.clone(),
            control: control.clone(),
            ack_tx: Some(ack_tx.clone()),
            reopen_tx: reopen_tx.clone(),
        };
        spawn(tokio::spawn(reconciler.run(watch_rx)));

        // Push forwarder: drain `Account::push_stream` into the
        // per-account mpsc. In-process accounts carry invalidations
        // and health here; out-of-process accounts may carry only
        // subscription-health transitions.
        if capabilities.push != bifrost_types::PushCapability::None {
            let acc = Arc::clone(&current);
            let aid = account_id.clone();
            let tx = watch_tx.clone();
            let sd = shutdown.clone();
            spawn(tokio::spawn(async move {
                loop {
                    if sd.is_cancelled() {
                        return;
                    }
                    let acc_arc = acc.load_full();
                    let mut stream = acc_arc.push_stream();
                    loop {
                        tokio::select! {
                            () = sd.cancelled() => return,
                            next = stream.next() => {
                                let Some(event) = next else { break; };
                                match tx.try_send(event) {
                                    Ok(()) => {}
                                    Err(mpsc::error::TrySendError::Full(rejected)) => {
                                        tracing::trace!(
                                            target: "bifrost.sync.changes",
                                            account = ?aid,
                                            "in-process push: queue full, coalesced"
                                        );
                                        let unknown = crate::push::coalesced_event(rejected);
                                        tokio::select! {
                                            () = sd.cancelled() => return,
                                            _ = tokio::time::timeout(
                                                Duration::from_millis(100),
                                                tx.send(unknown),
                                            ) => {}
                                        }
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => return,
                                }
                            }
                        }
                    }
                    // push_stream ended; loop reloads the (possibly
                    // reopened) handle and restarts. Sleep a tick so
                    // a tight reopen loop does not hot-spin.
                    tokio::select! {
                        () = sd.cancelled() => return,
                        () = tokio::time::sleep(Duration::from_millis(50)) => {}
                    }
                }
            }));
        }

        // Spawn the multiplexer.
        let mux = Multiplexer {
            account_id: account_id.clone(),
            account: Arc::clone(&current),
            cursors: Arc::clone(&cursors),
            config: self.config.multiplexer,
            boundary: boundary_view.clone(),
            changes_tx: changes_tx.clone(),
            watch_tx: watch_tx.clone(),
            control: control.clone(),
            shutdown: shutdown.clone(),
            reopen_tx: reopen_tx.clone(),
            ack_tx: Some(ack_tx.clone()),
            poll: Default::default(),
            scope_tokens: Arc::new(std::sync::Mutex::new(HashMap::new())),
        };
        spawn(tokio::spawn(mux.run()));

        // Spawn the backfill orchestrator. It walks the registered
        // scopes and runs one `BackfillRunner::run_partition` per
        // scope under a default policy. Items + checkpoints flow onto
        // the same per-account broadcast.
        let live_supersedes = Arc::new(LiveSupersedes::new());
        let backfill_registry_handle = Arc::clone(&self.backfill_registry);
        let bf_account = Arc::clone(&current);
        let bf_cursors = Arc::clone(&cursors);
        let bf_live = Arc::clone(&live_supersedes);
        let bf_shutdown = shutdown.clone();
        let bf_aid = account_id.clone();
        let bf_store = Arc::clone(&self.checkpoints);
        let bf_changes = changes_tx.clone();
        let bf_config = self.config.backfill;
        let bf_control = control.clone();
        spawn(tokio::spawn(async move {
            run_backfill_orchestrator(
                bf_account,
                bf_cursors,
                bf_live,
                backfill_registry_handle,
                bf_shutdown,
                bf_aid,
                bf_store,
                Some(bf_changes),
                bf_control,
                bf_config,
            )
            .await;
        }));

        // Deferred inventory establishment must happen after the slot
        // can be subscribed to. The worker waits for a real subscriber
        // before broadcasting cold-start inventory batches, so those
        // batches do not vanish during attach.
        if !deferred_inventory_scopes.is_empty() {
            let inventory_factory = Arc::clone(&factory);
            let inventory_account = Arc::clone(&current);
            let inventory_cursors = Arc::clone(&cursors);
            let inventory_store = Arc::clone(&self.checkpoints);
            let inventory_changes = changes_tx.clone();
            let inventory_shutdown = shutdown.clone();
            let inventory_aid = account_id.clone();
            let inventory_control = control.clone();
            spawn(tokio::spawn(async move {
                run_deferred_inventory_establishment(
                    inventory_factory,
                    inventory_account,
                    inventory_cursors,
                    inventory_store,
                    inventory_changes,
                    inventory_shutdown,
                    inventory_aid,
                    inventory_control,
                    deferred_inventory_scopes,
                )
                .await;
            }));
        }

        // Spawn the reopen listener.
        let reopen_factory = Arc::clone(&factory);
        let reopen_current = Arc::clone(&current);
        let reopen_cursors = Arc::clone(&cursors);
        let reopen_changes = changes_tx.clone();
        let reopen_aid = account_id.clone();
        let reopen_shutdown = shutdown.clone();
        let reopen_store = Arc::clone(&self.checkpoints);
        let reopen_control = control.clone();
        spawn(tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = reopen_shutdown.cancelled() => return,
                    req = reopen_rx.recv() => {
                        let Some(req) = req else { return; };
                        match req {
                            ReopenRequest::Recovery { scope, error } => {
                                handle_account_error(
                                    &reopen_factory,
                                    &reopen_current,
                                    &reopen_cursors,
                                    &reopen_store,
                                    &reopen_changes,
                                    &reopen_aid,
                                    &reopen_control,
                                    scope,
                                    error,
                                )
                                .await;
                            }
                        }
                    }
                }
            }
        }));

        // Bandwidth feed: optional periodic task that polls
        // `BandwidthMeter::account(id).observed_bps()` into the
        // control's atomic.
        if let Some(meter) = &self.bandwidth_meter {
            let meter_handle = Arc::clone(meter);
            let bw_aid = account_id.clone();
            let bw_control = control.clone();
            let bw_shutdown = shutdown.clone();
            spawn(tokio::spawn(async move {
                let view = meter_handle.account(bw_aid);
                loop {
                    tokio::select! {
                        () = bw_shutdown.cancelled() => return,
                        () = tokio::time::sleep(Duration::from_secs(1)) => {
                            let bps = view.observed_bps();
                            bw_control.observe_bandwidth(bps);
                        }
                    }
                }
            }));
        }

        let multiplexer = MultiplexerHandle {
            cancel: shutdown.child_token(),
            changes_tx: changes_tx.clone(),
            watch_tx: watch_tx.clone(),
        };
        let backfill = BackfillHandle {
            cancel: shutdown.child_token(),
            live_supersedes,
        };
        let push = PushHandle {
            cancel: shutdown.child_token(),
            sender: watch_tx,
        };
        let mutation = MutationHandle {
            cancel: shutdown.child_token(),
        };

        let bandwidth_meter = self.bandwidth_meter.clone();
        let slot = Arc::new(AccountSlot {
            factory,
            current,
            capabilities,
            multiplexer,
            backfill,
            push,
            mutation,
            cursors: Arc::clone(&cursors),
            checkpoints: Arc::clone(&self.checkpoints),
            priority_tx: priority_tx.clone(),
            priority_rx,
            bandwidth_cap_rx,
            boundary_tx: boundary.sender(),
            shutdown: shutdown.clone(),
            control: control.clone(),
            _sentinel_rx: sentinel_rx,
            workers: std::sync::Mutex::new(workers),
            bandwidth_meter,
        });

        self.accounts.insert(account_id.clone(), slot);

        Ok(control)
    }

    /// Durably persist a checkpoint for the given account and scope.
    /// Consumers call this AFTER they have written the corresponding
    /// items into their own store. The engine acks (`auto = false`) so
    /// the ack writer can distinguish consumer-driven acks from the
    /// engine's own auto-ack path.
    pub async fn ack_checkpoint(
        &self,
        account_id: &AccountId,
        scope: CursorScope,
        checkpoint: Checkpoint,
    ) -> Result<(), Error> {
        let tx = self
            .ack_senders
            .get(account_id)
            .map(|r| r.value().clone())
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let (complete_tx, complete_rx) = oneshot::channel();
        tx.send(AckRequest {
            scope,
            checkpoint,
            auto: false,
            complete: Some(complete_tx),
        })
        .await
        .map_err(|e| Error::Other(format!("ack channel closed: {e}")))?;
        complete_rx
            .await
            .map_err(|e| Error::Other(format!("ack writer dropped before persisting: {e}")))?
    }

    /// Explicitly shutdown the engine. Awaits all attached accounts'
    /// workers up to `EngineConfig::detach_timeout`. Strongly preferred
    /// over relying on `Drop`, which can only fire a best-effort
    /// cancel.
    pub async fn shutdown(self) -> Result<(), Error> {
        let ids: Vec<AccountId> = self.accounts.iter().map(|r| r.key().clone()).collect();
        for id in ids {
            // Best-effort: any detach error is logged but does not
            // prevent shutting the remaining accounts down.
            if let Err(e) = self.detach(&id).await {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?id,
                    error = %e,
                    "detach during shutdown failed"
                );
            }
        }
        self.root_cancel.cancel();
        Ok(())
    }

    /// Detach an account. Trips the shutdown token, awaits the in-flight
    /// `Batch`-with-checkpoint via the safe-boundary primitive, calls
    /// `account.close().await`, and removes the slot. Does NOT destroy
    /// server-side push subscriptions; call `unsubscribe_push` first
    /// for that.
    pub async fn detach(&self, account_id: &AccountId) -> Result<(), Error> {
        let Some((_, slot)) = self.accounts.remove(account_id) else {
            return Err(Error::AccountNotAttached(account_id.clone()));
        };
        // Drop the public ack sender so no new consumer acks enter
        // during teardown. Worker-held clones remain alive long enough
        // to flush their final checkpoint to the ack writer.
        self.ack_senders.remove(account_id);
        // Ask running workers to checkpoint cleanly, then stop.
        let _ = slot.boundary_tx.send(BoundaryRequest::Stop);
        // Trip shutdown before awaiting workers. Some account-level
        // workers park on the shutdown token rather than the boundary
        // watch, so waiting first would always run to detach_timeout.
        slot.shutdown.cancel();

        // Await spawned workers up to the configured timeout. Each
        // stored worker owns both its join and abort handles so a
        // timeout cannot detach a task and let it run forever.
        let timeout = self.config.detach_timeout;
        let mut drained: Vec<WorkerTask> = {
            let mut workers = slot.workers.lock().expect("worker list lock poisoned");
            workers.drain(..).collect()
        };
        // The ack writer is spawned first. Wait for stream workers
        // before the writer so final worker-held ack sender clones can
        // close naturally and the writer can drain everything it
        // received.
        let ack_worker = if drained.is_empty() {
            None
        } else {
            Some(drained.remove(0))
        };
        let deadline = tokio::time::Instant::now() + timeout;
        for worker in drained {
            await_worker_until(deadline, worker).await;
        }
        if let Some(worker) = ack_worker {
            await_worker_until(deadline, worker).await;
        }

        let current = slot.current.load_full();
        if let Err(e) = current.close().await {
            tracing::warn!(target: "bifrost.sync.changes", error=?e, "account close failed");
        }
        self.sink.unregister(account_id);
        self.scheduler.budget().forget(account_id);
        if let Some(meter) = &self.bandwidth_meter {
            meter.forget_account(account_id);
        }
        Ok(())
    }

    /// Hot-swap the account handle (capability change, transport reset).
    /// Spawned workers pick up the new handle on their next iteration
    /// because they load through the slot's `ArcSwap`.
    pub async fn reopen(&self, account_id: &AccountId) -> Result<(), Error> {
        let slot = self
            .accounts
            .get(account_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        // `reopen` is the post-attach swap path. Per the engine error
        // policy, failures in this path use `Account` (not `OpenFailed`)
        // so callers know the account was running and the engine is
        // reporting an in-flight failure.
        let next = slot
            .factory
            .open(account_id.clone())
            .await
            .map_err(Error::Account)?;
        next.as_ref().set_priority(slot.control.priority_snapshot());
        next.as_ref()
            .set_bandwidth_cap(slot.control.bandwidth_cap_snapshot());
        slot.current.store(Arc::new(next));
        Ok(())
    }

    /// Subscribe to the per-account unified change stream. Each
    /// subscriber gets its own broadcast receiver; missed events
    /// (slow consumer) are dropped per `tokio::sync::broadcast`'s
    /// lagging semantics.
    pub fn account_changes_stream(
        &self,
        account_id: &AccountId,
    ) -> Result<broadcast::Receiver<MultiplexerEvent>, Error> {
        let slot = self
            .accounts
            .get(account_id)
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        Ok(slot.multiplexer.changes_tx.subscribe())
    }

    /// Hand the consumer a clone of the engine's `InvalidationSink`
    /// so out-of-process push receivers (Gmail Pub/Sub listener,
    /// Graph webhook server) can feed events directly.
    #[must_use]
    pub fn invalidation_sink(&self) -> Arc<dyn InvalidationSink> {
        Arc::<InvalidationSinkInner>::clone(&self.sink)
    }

    /// Currently-known accounts. Useful for tests / observability.
    #[must_use]
    pub fn attached_account_ids(&self) -> Vec<AccountId> {
        self.accounts.iter().map(|r| r.key().clone()).collect()
    }

    /// Server-side subscription teardown.
    pub async fn unsubscribe_push(&self, account_id: &AccountId) -> Result<(), Error> {
        let slot = self
            .accounts
            .get(account_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let handles = self.subscriptions.take(account_id);
        let account = slot.current.load_full();
        for handle in handles {
            if let Err(e) = account.push_unsubscribe(handle).await {
                tracing::warn!(target: "bifrost.sync.reconcile", error=?e, "push_unsubscribe failed");
            }
        }
        Ok(())
    }

    /// Engine-side push subscription request. The engine stashes the
    /// returned handle in its registry so a later `unsubscribe_push`
    /// can find it.
    pub async fn subscribe_push(
        &self,
        account_id: &AccountId,
        scopes: &[CursorScope],
    ) -> Result<SubscriptionHandle, Error> {
        let slot = self
            .accounts
            .get(account_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let account = slot.current.load_full();
        let handle = account.push_subscribe(scopes).await?;
        self.subscriptions
            .record(account_id.clone(), handle.clone());
        Ok(handle)
    }

    /// Drive a single-account bulk-flag campaign against an attached
    /// account, applying the read-back guard to the retry candidates.
    ///
    /// Returns the per-batch counters aggregated across the run.
    ///
    /// Mutation accounting consumes `ItemOutcome<MutationSuccess>` and
    /// dispatches retries from `AccountError::recovery()`:
    /// - `Retry::SameRequest` / `AfterAuthRefresh` queue for retry.
    /// - `Retry::AfterStateRefresh` and `Reconcile` queue for read-back.
    /// - `Engine(_)` blocks the campaign and signals the engine.
    /// - terminal recovery lands in `failed_terminal`.
    /// `ItemOutcome::Uncertain` is always queued for read-back.
    pub async fn bulk_set_flags(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::ObjectId>,
        op: bifrost_types::FlagOp,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        let slot = self
            .accounts
            .get(account_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let max_retries = self.config.mutation_max_retries;

        let key = vendor.next(protocol);

        let mut outcomes: HashMap<bifrost_types::ObjectId, MutationBucket> = HashMap::new();
        let mut retry_ids: Vec<bifrost_types::ObjectId> = Vec::new();
        let mut readback_ids: Vec<bifrost_types::ObjectId> = Vec::new();
        let mut remaining: Vec<bifrost_types::ObjectId> = targets;
        let mut attempt: u32 = 0;
        let mut retry_advice: Option<RetryAdvice> = None;

        loop {
            if let Some(advice) = retry_advice.take() {
                let delay = crate::recovery::retry_delay(
                    &advice,
                    std::time::SystemTime::now(),
                    Duration::from_secs(1),
                );
                tokio::time::sleep(delay).await;
            }

            let account = slot.current.load_full();
            let target_stream: AccountStream<bifrost_types::ObjectId> =
                Box::pin(futures::stream::iter(remaining.clone()));
            let mut stream = account.bulk_set_flags(target_stream, op.clone(), key.clone());
            retry_ids.clear();
            readback_ids.clear();
            let mut stream_termination_advice: Option<RetryAdvice> = None;

            while let Some(event) = stream.next().await {
                match event {
                    bifrost_types::SyncEvent::Batch(batch) => {
                        for item in batch.items {
                            classify_item_outcome(
                                item,
                                &mut outcomes,
                                &mut retry_ids,
                                &mut readback_ids,
                            );
                        }
                    }
                    bifrost_types::SyncEvent::Terminated(err) => {
                        // Stream-level terminating event: dispatch via
                        // `error.recovery()`. Retry / Reconcile let
                        // the campaign continue; Engine and terminal
                        // bail.
                        let recovery = err.recovery().clone();
                        match recovery {
                            RecoveryClass::Retry(advice) => {
                                stream_termination_advice = Some(advice);
                                break;
                            }
                            RecoveryClass::Reconcile(_) => {
                                // Funnel every still-unresolved item
                                // into the read-back queue.
                                for id in &remaining {
                                    if !matches!(
                                        outcomes.get(id),
                                        Some(
                                            MutationBucket::Applied
                                                | MutationBucket::Skipped
                                                | MutationBucket::FailedTerminal
                                        )
                                    ) {
                                        readback_ids.push(id.clone());
                                    }
                                }
                                break;
                            }
                            RecoveryClass::Engine(_) => {
                                return Err(Error::Account(err));
                            }
                            _ => return Err(Error::Account(err)),
                        }
                    }
                    bifrost_types::SyncEvent::Done(_) => break,
                    bifrost_types::SyncEvent::Progress(_)
                    | bifrost_types::SyncEvent::Warning(_) => {}
                    _ => {}
                }
            }

            attempt = attempt.saturating_add(1);
            let retry_set: std::collections::HashSet<_> = retry_ids.iter().cloned().collect();
            let mut next_remaining: Vec<bifrost_types::ObjectId> = remaining
                .iter()
                .filter(|id| retry_set.contains(*id))
                .cloned()
                .collect();
            if attempt < max_retries && !next_remaining.is_empty() {
                retry_advice = stream_termination_advice;
                std::mem::swap(&mut remaining, &mut next_remaining);
                continue;
            }
            // No more attempts. Anything still in `retry_ids` becomes
            // a pending read-back candidate so the guard can decide
            // applied vs failed_terminal.
            for id in &retry_set {
                readback_ids.push(id.clone());
                outcomes.insert(id.clone(), MutationBucket::PendingRetry);
            }
            break;
        }

        let mut totals = counters_from_outcomes(&outcomes);
        if !readback_ids.is_empty() {
            let account = slot.current.load_full();
            let outcome =
                crate::mutation::run_readback_guard(account.as_ref().as_ref(), readback_ids, &op)
                    .await?;
            totals.pending_retry = totals
                .pending_retry
                .saturating_sub(outcome.skipped)
                .saturating_sub(outcome.still_failed);
            totals.skipped = totals.skipped.saturating_add(outcome.skipped);
            totals.failed_terminal = totals.failed_terminal.saturating_add(outcome.still_failed);
        }
        Ok(totals)
    }

    /// Scheduler handle for advanced consumers (tests, instrumentation).
    #[must_use]
    pub fn scheduler(&self) -> Scheduler {
        self.scheduler.clone()
    }

    /// Backfill registry handle.
    #[must_use]
    pub fn backfill_registry(&self) -> Arc<BackfillRegistry> {
        Arc::clone(&self.backfill_registry)
    }

    // ---------- internal helpers ----------

    async fn discover_scopes(&self, account: &dyn Account) -> Result<Vec<CursorScope>, Error> {
        let mut out = Vec::new();
        let mut stream = account.discover_cursor_scopes();
        while let Some(event) = stream.next().await {
            match event {
                SyncEvent::Batch(batch) => out.extend(batch.items),
                SyncEvent::Done(_) => break,
                SyncEvent::Terminated(err) => {
                    return Err(Error::Account(err));
                }
                SyncEvent::Progress(_) | SyncEvent::Warning(_) => {}
                // Future `SyncEvent` variants are ignored here; the
                // discovery loop only consumes the four documented
                // cases.
                _ => {}
            }
        }
        Ok(out)
    }

    async fn establish_one(
        &self,
        account_id: &AccountId,
        account: &dyn Account,
        scope: CursorScope,
        cursors: Arc<CursorRegistry>,
    ) -> Result<InitialScope, Error> {
        // Check the store first - resume path.
        if let Some(existing) = self
            .checkpoints
            .get_change_cursor(account_id, &scope)
            .await?
        {
            cursors.put(existing);
            return Ok(InitialScope::Ready);
        }
        match account
            .establish_initial_cursor(scope.clone())
            .await
            .map_err(Error::Account)?
        {
            CursorEstablishment::Ready(cursor) => {
                self.persist_cursor(account_id, cursor, cursors).await?;
                Ok(InitialScope::Ready)
            }
            CursorEstablishment::EstablishViaInventory => {
                Ok(InitialScope::DeferredInventory(scope))
            }
            // `CursorEstablishment` is `#[non_exhaustive]`; treat any
            // future variant as "cannot establish" so the engine fails
            // closed rather than silently continuing.
            _ => Err(Error::EstablishCursorFailed(
                "unknown CursorEstablishment variant".into(),
            )),
        }
    }

    async fn persist_cursor(
        &self,
        account_id: &AccountId,
        cursor: ChangeCursor,
        cursors: Arc<CursorRegistry>,
    ) -> Result<(), Error> {
        self.checkpoints
            .put_change_cursor(account_id, cursor.clone())
            .await?;
        cursors.put(cursor);
        Ok(())
    }

    /// Drive the bounded `discover_memberships` stream to completion
    /// and link each `MembershipScope` to a covering `CursorScope` in
    /// the registry. This populates the side-index the push reconciler
    /// consults on `HintPayload::SpecificMembership`.
    ///
    /// Cost is one bounded enumeration per attach; the engine accepts
    /// that cost to make hint-targeted reconciles actually scope-bound.
    async fn discover_and_link_memberships(
        &self,
        account: &dyn Account,
        cursors: Arc<CursorRegistry>,
    ) -> Result<(), Error> {
        link_discovered_memberships(account, &cursors).await
    }
}

/// True if the given cursor scope covers the given membership scope.
///
/// The mapping is engine policy: account-wide cursors cover every
/// membership; folder-typed cursors cover only the matching folder;
/// query cursors cover only the matching query.
fn scope_covers_membership(scope: &CursorScope, membership: &MembershipScope) -> bool {
    use bifrost_types::FolderId;
    match (scope, membership) {
        // Account-wide cursors cover every membership.
        (CursorScope::Account, _) | (CursorScope::Type(_), _) => true,
        // Folder-typed cursors cover the matching folder only.
        (CursorScope::Folder(folder), MembershipScope::Folder(m)) => folder == m,
        (CursorScope::FolderType { folder, .. }, MembershipScope::Folder(m)) => folder == m,
        // Folder-typed cursors over an arbitrary folder match mailbox
        // memberships that share the same id (Graph maps folders 1:1
        // to mailboxes; we treat the FolderId / MailboxId String the
        // same in the side index).
        (CursorScope::Folder(FolderId(folder_id)), MembershipScope::Mailbox(mbx)) => {
            folder_id == &mbx.0
        }
        (
            CursorScope::FolderType {
                folder: FolderId(folder_id),
                ..
            },
            MembershipScope::Mailbox(mbx),
        ) => folder_id == &mbx.0,
        // Query cursors cover the same query membership.
        (CursorScope::Query(q), MembershipScope::Query(mq)) => q == mq,
        _ => false,
    }
}

async fn await_worker_until(deadline: tokio::time::Instant, worker: WorkerTask) {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        worker.abort.abort();
        return;
    }
    match tokio::time::timeout(remaining, worker.join).await {
        Ok(Ok(())) => {}
        Ok(Err(join)) => {
            if !join.is_cancelled() {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    error = ?join,
                    "worker panic during detach"
                );
            }
        }
        Err(_) => {
            worker.abort.abort();
            tracing::warn!(
                target: "bifrost.sync.changes",
                "worker exceeded detach timeout; aborted"
            );
        }
    }
}

/// Backfill orchestrator. Walks the registered cursor scopes and runs
/// one partition pass per scope via `BackfillRunner::run_partition`.
///
/// The runner uses the slot's shared `LiveSupersedes` set so live
/// `Created` events from the multiplexer skip over inventory entries
/// the user has already seen.
#[allow(clippy::too_many_arguments)]
async fn run_backfill_orchestrator(
    account: Arc<ArcSwap<Arc<dyn Account>>>,
    cursors: Arc<CursorRegistry>,
    live: Arc<LiveSupersedes>,
    registry: Arc<BackfillRegistry>,
    shutdown: CancellationToken,
    account_id: AccountId,
    store: Arc<DynCheckpointStore>,
    changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
    control: SyncControl,
    config: BackfillConfig,
) {
    let scopes = cursors.all_scopes();
    for scope in scopes {
        if shutdown.is_cancelled() {
            return;
        }
        registry.mark(scope.clone(), BackfillState::Running);
        let acc_arc = account.load_full();
        let acc: &dyn Account = acc_arc.as_ref().as_ref();
        match backfill_plan_for(acc, &scope, config) {
            BackfillPlan::Fixed(partitions) => {
                let mut completed = true;
                for partition in partitions {
                    if shutdown.is_cancelled() {
                        return;
                    }
                    if let Err(err) = BackfillRunner::run_partition(
                        acc,
                        scope.clone(),
                        partition,
                        live.as_ref(),
                        &account_id,
                        Arc::clone(&store),
                        changes_tx.clone(),
                        Some(control.clone()),
                        crate::cursor::ENGINE_VERSION,
                    )
                    .await
                    {
                        tracing::warn!(
                            target: "bifrost.sync.backfill",
                            scope = ?scope,
                            error = %err,
                            "backfill partition failed; leaving scope Pending"
                        );
                        completed = false;
                        break;
                    }
                }
                registry.mark(
                    scope.clone(),
                    if completed {
                        BackfillState::Completed
                    } else {
                        BackfillState::Pending
                    },
                );
            }
            BackfillPlan::OpenPages { chunk } => {
                let mut completed = true;
                let mut from = 0_u32;
                loop {
                    if shutdown.is_cancelled() {
                        return;
                    }
                    let to = from.saturating_add(chunk);
                    let partition = InventoryPartition::Page { from, to };
                    match BackfillRunner::run_partition(
                        acc,
                        scope.clone(),
                        partition,
                        live.as_ref(),
                        &account_id,
                        Arc::clone(&store),
                        changes_tx.clone(),
                        Some(control.clone()),
                        crate::cursor::ENGINE_VERSION,
                    )
                    .await
                    {
                        Ok(outcome) => {
                            if outcome.seen < u64::from(chunk) {
                                break;
                            }
                            from = to;
                        }
                        Err(err) => {
                            tracing::warn!(
                                target: "bifrost.sync.backfill",
                                scope = ?scope,
                                error = %err,
                                "backfill page partition failed; leaving scope Pending"
                            );
                            completed = false;
                            break;
                        }
                    }
                }
                registry.mark(
                    scope.clone(),
                    if completed {
                        BackfillState::Completed
                    } else {
                        BackfillState::Pending
                    },
                );
            }
        }
    }
}

enum BackfillPlan {
    Fixed(Vec<InventoryPartition>),
    OpenPages { chunk: u32 },
}

fn backfill_plan_for(
    account: &dyn Account,
    scope: &CursorScope,
    config: BackfillConfig,
) -> BackfillPlan {
    match account.inventory_partitioning(scope) {
        InventoryPartitioning::Full => BackfillPlan::Fixed(vec![InventoryPartition::Full]),
        InventoryPartitioning::TimeWindowed => {
            let policy = BackfillPolicy::default();
            let plan = crate::backfill::partitioner::plan(&policy, chrono::Utc::now(), 0);
            BackfillPlan::Fixed(
                plan.partitions
                    .iter()
                    .map(crate::backfill::partitioner::inventory_partition_for)
                    .collect(),
            )
        }
        InventoryPartitioning::UidRange {
            max_uid: Some(max_uid),
        } => {
            let policy = BackfillPolicy {
                strategy: BackfillStrategy::UidRange {
                    chunk_size: config.uid_range_chunk,
                },
                clock_skew: std::time::Duration::ZERO,
            };
            let plan = crate::backfill::partitioner::plan(&policy, chrono::Utc::now(), max_uid);
            BackfillPlan::Fixed(
                plan.partitions
                    .iter()
                    .map(crate::backfill::partitioner::inventory_partition_for)
                    .collect(),
            )
        }
        InventoryPartitioning::UidRange { max_uid: None } => {
            tracing::warn!(
                target: "bifrost.sync.backfill",
                scope = ?scope,
                "uid-range partitioning requested without max_uid; using full inventory pass"
            );
            BackfillPlan::Fixed(vec![InventoryPartition::Full])
        }
        InventoryPartitioning::PageCount {
            total: Some(total),
            page_size,
        } => {
            let policy = BackfillPolicy {
                strategy: BackfillStrategy::PageCount {
                    items_per_partition: page_size.unwrap_or(config.page_count_chunk).max(1),
                },
                clock_skew: std::time::Duration::ZERO,
            };
            let plan = crate::backfill::partitioner::plan(&policy, chrono::Utc::now(), total);
            BackfillPlan::Fixed(
                plan.partitions
                    .iter()
                    .map(crate::backfill::partitioner::inventory_partition_for)
                    .collect(),
            )
        }
        InventoryPartitioning::PageCount {
            total: None,
            page_size,
        } => BackfillPlan::OpenPages {
            chunk: page_size.unwrap_or(config.page_count_chunk).max(1),
        },
        _ => BackfillPlan::Fixed(vec![InventoryPartition::Full]),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_deferred_inventory_establishment(
    factory: Arc<dyn AccountFactory>,
    account: Arc<ArcSwap<Arc<dyn Account>>>,
    cursors: Arc<CursorRegistry>,
    store: Arc<DynCheckpointStore>,
    changes_tx: broadcast::Sender<MultiplexerEvent>,
    shutdown: CancellationToken,
    account_id: AccountId,
    control: crate::control::SyncControl,
    scopes: Vec<CursorScope>,
) {
    if !wait_for_real_subscriber(&changes_tx, &shutdown).await {
        return;
    }
    for scope in scopes {
        if shutdown.is_cancelled() {
            return;
        }
        let acc_arc = account.load_full();
        let acc: &dyn Account = acc_arc.as_ref().as_ref();
        let fusion = crate::multiplexer::InventoryFusion {
            account_id: account_id.clone(),
            cursors: Arc::clone(&cursors),
            store: Arc::clone(&store),
        };
        match fusion
            .run_with_broadcast(acc, scope.clone(), Some(changes_tx.clone()))
            .await
        {
            Ok(crate::multiplexer::FusionOutcome::Established) => {
                if let Err(err) = link_discovered_memberships(acc, &cursors).await {
                    tracing::warn!(
                        target: "bifrost.sync.changes",
                        account = ?account_id,
                        scope = ?scope,
                        error = %err,
                        "deferred inventory: membership refresh failed"
                    );
                }
            }
            Ok(crate::multiplexer::FusionOutcome::NoCursor) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?account_id,
                    scope = ?scope,
                    "deferred inventory completed without a cursor"
                );
            }
            Ok(crate::multiplexer::FusionOutcome::Terminated(error)) => {
                // The fusion already broadcast the terminating event.
                // Route the error through `handle_account_error` so
                // engine directives (RestartScope, RestartAccount,
                // SchemaIncompatible, etc.) are dispatched, and
                // terminal errors emit structured telemetry via the
                // same path as every other recovery.
                handle_account_error(
                    &factory,
                    &account,
                    &cursors,
                    &store,
                    &changes_tx,
                    &account_id,
                    &control,
                    Some(scope.clone()),
                    error,
                )
                .await;
            }
            Err(err) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?account_id,
                    scope = ?scope,
                    error = %err,
                    "deferred inventory failed"
                );
            }
        }
    }
}

async fn wait_for_real_subscriber(
    changes_tx: &broadcast::Sender<MultiplexerEvent>,
    shutdown: &CancellationToken,
) -> bool {
    loop {
        // One receiver is the slot's sentinel. A count above one
        // means at least one consumer has called account_changes_stream.
        if changes_tx.receiver_count() > 1 {
            return true;
        }
        tokio::select! {
            () = shutdown.cancelled() => return false,
            () = tokio::time::sleep(Duration::from_millis(25)) => {}
        }
    }
}

async fn link_discovered_memberships(
    account: &dyn Account,
    cursors: &CursorRegistry,
) -> Result<(), Error> {
    let mut stream = account.discover_memberships();
    while let Some(event) = stream.next().await {
        match event {
            SyncEvent::Batch(batch) => {
                let known_scopes = cursors.all_scopes();
                for membership in batch.items {
                    for scope in &known_scopes {
                        if scope_covers_membership(scope, &membership) {
                            cursors.link_membership(membership.clone(), scope.clone());
                        }
                    }
                }
            }
            SyncEvent::Done(_) => break,
            SyncEvent::Terminated(err) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    kind = ?err.kind(),
                    message_key = err.message_key(),
                    "discover_memberships terminated; continuing without index"
                );
                break;
            }
            SyncEvent::Progress(_) | SyncEvent::Warning(_) => {}
            _ => {}
        }
    }
    Ok(())
}

/// Ack writer task. One per attached account. Receives `AckRequest`
/// messages on `rx` and durably persists the carried checkpoint via
/// the `CheckpointStore`. Notifies the control's checkpoint watch so
/// `pause()` / `checkpoint_now()` waiters wake on a real persisted
/// boundary. Exits when the channel closes (slot detach).
async fn ack_writer(
    account_id: AccountId,
    store: Arc<DynCheckpointStore>,
    control: SyncControl,
    mut rx: mpsc::Receiver<AckRequest>,
) {
    while let Some(req) = rx.recv().await {
        let result = persist_ack_request(&account_id, Arc::clone(&store), &req).await;
        match result {
            Ok(()) => {
                // Notify pause / checkpoint_now waiters AFTER the
                // durable write lands - the contract is that the
                // returned checkpoint has been persisted.
                control.record_checkpoint(req.checkpoint).await;
                if let Some(done) = req.complete {
                    let _ = done.send(Ok(()));
                }
            }
            Err(err) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?account_id,
                    scope = ?req.scope,
                    error = %err,
                    auto = req.auto,
                    "ack: checkpoint persist failed"
                );
                if let Some(done) = req.complete {
                    let _ = done.send(Err(err));
                }
            }
        }
    }
}

async fn persist_ack_request(
    account_id: &AccountId,
    store: Arc<DynCheckpointStore>,
    req: &AckRequest,
) -> Result<(), Error> {
    match &req.checkpoint {
        Checkpoint::Change(c) => store.put_change_cursor(account_id, c.clone()).await,
        Checkpoint::Backfill(b) => store.put_backfill(account_id, b.clone()).await,
        _ => Err(Error::CheckpointStore(
            "unknown checkpoint variant in ack".into(),
        )),
    }
}

/// Dispatch an `AccountError` to the engine's recovery machinery.
///
/// Reads `error.recovery()` and routes Retry / Reconcile / Engine /
/// terminal verdicts to the appropriate engine path. The `scope`
/// argument is the worker's convenience-suggested scope: scope-bound
/// directives use the directive's own scope when present and fall
/// back to this `scope`; account-wide directives ignore it.
#[allow(clippy::too_many_arguments)]
async fn handle_account_error(
    factory: &Arc<dyn AccountFactory>,
    current: &Arc<ArcSwap<Arc<dyn Account>>>,
    cursors: &Arc<CursorRegistry>,
    store: &Arc<DynCheckpointStore>,
    changes_tx: &broadcast::Sender<MultiplexerEvent>,
    account_id: &AccountId,
    control: &SyncControl,
    scope: Option<CursorScope>,
    error: AccountError,
) {
    let recovery = error.recovery().clone();
    match recovery {
        RecoveryClass::Retry(advice) => {
            // Sleep per advice; the per-scope poll loop and the push
            // reconciler already handle the retry sleep themselves, so
            // the reopen listener seeing a Retry verdict here means a
            // worker chose to delegate. Honor the advice.
            handle_retry(&advice).await;
        }
        RecoveryClass::Reconcile(_advice) => {
            // The poll loop / push reconciler do the inline reconcile;
            // if we reach this branch via the reopen listener we trust
            // their next pass to probe. Log for telemetry only.
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?account_id,
                scope = ?scope,
                kind = ?error.kind(),
                message_key = error.message_key(),
                "reconcile recovery reached reopen listener"
            );
        }
        RecoveryClass::Engine(directive) => {
            handle_engine_directive(
                factory, current, cursors, store, changes_tx, account_id, control, scope,
                directive, error,
            )
            .await;
        }
        // Every terminal recovery: broadcast already carried the
        // terminating event; the engine has no automated next step.
        // `Fatal::try_from(error)` would succeed; we keep the original
        // around in the log for support telemetry.
        _ => {
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?account_id,
                scope = ?scope,
                kind = ?error.kind(),
                message_key = error.message_key(),
                recovery = ?error.recovery(),
                "terminal recovery; engine takes no automated action"
            );
        }
    }
}

async fn handle_retry(advice: &RetryAdvice) {
    let delay = crate::recovery::retry_delay(
        advice,
        std::time::SystemTime::now(),
        std::time::Duration::from_secs(1),
    );
    tokio::time::sleep(delay).await;
}

#[allow(clippy::too_many_arguments)]
async fn handle_engine_directive(
    factory: &Arc<dyn AccountFactory>,
    current: &Arc<ArcSwap<Arc<dyn Account>>>,
    cursors: &Arc<CursorRegistry>,
    store: &Arc<DynCheckpointStore>,
    changes_tx: &broadcast::Sender<MultiplexerEvent>,
    account_id: &AccountId,
    control: &SyncControl,
    fallback_scope: Option<CursorScope>,
    directive: EngineDirective,
    error: AccountError,
) {
    match directive {
        EngineDirective::RestartScope(directive_scope) => {
            restart_scope(
                current,
                cursors,
                store,
                changes_tx,
                account_id,
                directive_scope,
            )
            .await;
        }
        EngineDirective::DowngradeCapabilityForScope(directive_scope) => {
            // Same cursor-deletion + re-establishment path as
            // RestartScope. Emit a warning that names the scope so
            // telemetry can pivot on capability downgrades.
            broadcast_warning(
                changes_tx,
                Some(directive_scope.clone()),
                bifrost_types::Warning::user_safe(
                    bifrost_types::WarningKind::Other,
                    format!("scope capability downgraded: {directive_scope:?}"),
                )
                .with_protocol_detail(DiagnosticText::support_only(format!("{directive_scope:?}"))),
            );
            restart_scope(
                current,
                cursors,
                store,
                changes_tx,
                account_id,
                directive_scope,
            )
            .await;
        }
        EngineDirective::RestartAccount => {
            restart_account(factory, current, account_id, control).await;
        }
        EngineDirective::CapabilityChanged { delta } => {
            // Reopen so the protocol can refresh its snapshot, then
            // emit a warning so telemetry pivots on the delta. The
            // engine's `AccountSlot.capabilities` is an attach-time
            // snapshot - production workers do not consult it as the
            // source of truth, so leaving it stale is intentional.
            restart_account(factory, current, account_id, control).await;
            broadcast_warning(
                changes_tx,
                fallback_scope.clone(),
                bifrost_types::Warning::user_safe(
                    bifrost_types::WarningKind::Other,
                    "account capabilities changed",
                )
                .with_protocol_detail(DiagnosticText::support_only(format!("{delta:?}"))),
            );
        }
        EngineDirective::DowngradeStrategy(downgrade) => {
            // No sync-owned strategy table. Emit a warning carrying
            // the downgrade payload in both the human-summary
            // `message` and the support-only `protocol_detail` so
            // neither audience loses the evidence. Then reopen so the
            // protocol crate picks the lower strategy on the next
            // `establish_initial_cursor`.
            broadcast_warning(
                changes_tx,
                fallback_scope.clone(),
                bifrost_types::Warning::user_safe(
                    bifrost_types::WarningKind::StrategyDowngraded,
                    format!("downgraded sync strategy: {downgrade:?}"),
                )
                .with_protocol_detail(DiagnosticText::support_only(format!("{downgrade:?}"))),
            );
            restart_account(factory, current, account_id, control).await;
            // If the originating error was scoped to a cursor, also
            // re-establish that scope so the downgrade takes effect
            // immediately rather than at the next poll.
            if let Some(ErrorScope::Cursor(scoped)) = error.scope() {
                restart_scope(
                    current,
                    cursors,
                    store,
                    changes_tx,
                    account_id,
                    scoped.clone(),
                )
                .await;
            }
        }
        EngineDirective::SchemaIncompatible => {
            // Stop trusting durable cursor envelopes. Clear every
            // in-memory cursor and delete every durable change cursor
            // we know about, then re-establish each from the current
            // account handle.
            let scopes: Vec<CursorScope> = cursors.all_scopes();
            for s in &scopes {
                cursors.delete(s);
                if let Err(err) = store.delete_change_cursor(account_id, s).await {
                    tracing::warn!(
                        target: "bifrost.sync.changes",
                        account = ?account_id,
                        scope = ?s,
                        error = %err,
                        "SchemaIncompatible: delete_change_cursor failed"
                    );
                }
            }
            let acc_arc = current.load_full();
            let acc: &dyn Account = acc_arc.as_ref().as_ref();
            for s in scopes {
                if let Err(err) = run_establish(
                    account_id,
                    acc,
                    s.clone(),
                    Arc::clone(cursors),
                    Arc::clone(store),
                    changes_tx.clone(),
                )
                .await
                {
                    tracing::warn!(
                        target: "bifrost.sync.changes",
                        account = ?account_id,
                        scope = ?s,
                        error = %err,
                        "SchemaIncompatible: re-establishment failed"
                    );
                }
            }
        }
        EngineDirective::OperatorOverrideRequired { reason } => {
            // No automatic reopen. Surface the original account error
            // through the change stream so the consumer can pause /
            // alert. The driver already pushed the terminating event;
            // we add an `OperatorAttentionNeeded` warning carrying the
            // reason for telemetry.
            broadcast_warning(
                changes_tx,
                fallback_scope.clone(),
                bifrost_types::Warning::user_safe(
                    bifrost_types::WarningKind::OperatorAttentionNeeded,
                    reason.clone(),
                )
                .with_protocol_detail(DiagnosticText::support_only(reason)),
            );
        }
        _ => {
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?account_id,
                directive = ?directive,
                "unknown engine directive; no automated action"
            );
        }
    }
}

async fn restart_scope(
    current: &Arc<ArcSwap<Arc<dyn Account>>>,
    cursors: &Arc<CursorRegistry>,
    store: &Arc<DynCheckpointStore>,
    changes_tx: &broadcast::Sender<MultiplexerEvent>,
    account_id: &AccountId,
    scope: CursorScope,
) {
    cursors.delete(&scope);
    if let Err(err) = store.delete_change_cursor(account_id, &scope).await {
        tracing::warn!(
            target: "bifrost.sync.changes",
            account = ?account_id,
            scope = ?scope,
            error = %err,
            "RestartScope: delete_change_cursor failed"
        );
    }
    let acc_arc = current.load_full();
    let acc: &dyn Account = acc_arc.as_ref().as_ref();
    match run_establish(
        account_id,
        acc,
        scope.clone(),
        Arc::clone(cursors),
        Arc::clone(store),
        changes_tx.clone(),
    )
    .await
    {
        Ok(()) => {
            if let Err(err) = link_discovered_memberships(acc, cursors).await {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?account_id,
                    scope = ?scope,
                    error = %err,
                    "RestartScope: membership refresh failed"
                );
            }
        }
        Err(err) => {
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?account_id,
                scope = ?scope,
                error = %err,
                "RestartScope: re-establishment failed"
            );
        }
    }
}

async fn restart_account(
    factory: &Arc<dyn AccountFactory>,
    current: &Arc<ArcSwap<Arc<dyn Account>>>,
    account_id: &AccountId,
    control: &SyncControl,
) {
    match factory.open(account_id.clone()).await {
        Ok(next) => {
            next.as_ref().set_priority(control.priority_snapshot());
            next.as_ref()
                .set_bandwidth_cap(control.bandwidth_cap_snapshot());
            current.store(Arc::new(next));
        }
        Err(err) => tracing::warn!(
            target: "bifrost.sync.changes",
            account = ?account_id,
            error = %err,
            "RestartAccount: factory.open failed"
        ),
    }
}

fn broadcast_warning(
    changes_tx: &broadcast::Sender<MultiplexerEvent>,
    scope: Option<CursorScope>,
    warning: bifrost_types::Warning,
) {
    let me = MultiplexerEvent {
        scope: scope.unwrap_or(CursorScope::Account),
        event: Arc::new(SyncEvent::Warning(warning)),
        checkpoint: None,
    };
    let _ = changes_tx.send(me);
}

/// Re-establish a single scope. Mirrors `SyncEngine::establish_one`
/// but lives at file scope so the reopen listener task can call it
/// without owning a reference to the engine.
async fn run_establish(
    account_id: &AccountId,
    account: &dyn Account,
    scope: CursorScope,
    cursors: Arc<CursorRegistry>,
    store: Arc<DynCheckpointStore>,
    changes_tx: broadcast::Sender<MultiplexerEvent>,
) -> Result<(), Error> {
    if let Some(existing) = store.get_change_cursor(account_id, &scope).await? {
        cursors.put(existing);
        return Ok(());
    }
    match account
        .establish_initial_cursor(scope.clone())
        .await
        .map_err(Error::Account)?
    {
        CursorEstablishment::Ready(cursor) => {
            store.put_change_cursor(account_id, cursor.clone()).await?;
            cursors.put(cursor);
            Ok(())
        }
        CursorEstablishment::EstablishViaInventory => {
            let fusion = crate::multiplexer::InventoryFusion {
                account_id: account_id.clone(),
                cursors: Arc::clone(&cursors),
                store: Arc::clone(&store),
            };
            match fusion
                .run_with_broadcast(account, scope, Some(changes_tx))
                .await?
            {
                crate::multiplexer::FusionOutcome::Established
                | crate::multiplexer::FusionOutcome::NoCursor => Ok(()),
                crate::multiplexer::FusionOutcome::Terminated(error) => {
                    Err(Error::EstablishCursorTerminated(error))
                }
            }
        }
        _ => Err(Error::EstablishCursorFailed(
            "unknown CursorEstablishment variant".into(),
        )),
    }
}

impl Drop for SyncEngine {
    fn drop(&mut self) {
        // Best-effort cancel only. Drop CANNOT await spawned workers
        // because the destructor is sync and we cannot reach the
        // tokio runtime from here. Workers hold `Arc` clones of
        // engine-internal state and may run for a few more seconds
        // before they observe the cancellation - that is the price of
        // forgoing `shutdown`.
        //
        // The contract documented on `SyncEngine::shutdown` is:
        //
        //     Call `engine.shutdown().await` before dropping the
        //     engine. Otherwise workers may keep running briefly,
        //     and any unacknowledged cursor advances are lost.
        self.root_cancel.cancel();
    }
}

/// Per-id mutation bookkeeping bucket. Reflects the campaign's final
/// resolution for each id; `PendingRetry` items get the read-back
/// guard before counters are reported back to the consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationBucket {
    Applied,
    Skipped,
    FailedTerminal,
    PendingRetry,
    PendingReadback,
    BlockedByEngine,
}

/// Classify one `ItemOutcome<MutationSuccess>` and update the
/// per-id outcome map, retry queue, and read-back queue.
///
/// Dispatch rules (per
/// `plans/error-model-sync.md::Mutation pipeline`):
/// - `Succeeded(Applied)` -> `Applied`.
/// - `Succeeded(Skipped)` -> `Skipped`.
/// - `Failed { error }` dispatches via `error.recovery()`:
///   - `Retry::SameRequest` / `AfterAuthRefresh` -> retry queue.
///   - `Retry::AfterStateRefresh` and `Reconcile(_)` -> read-back.
///   - `Engine(_)` -> `BlockedByEngine`.
///   - terminal -> `FailedTerminal`.
/// - `Uncertain { error }` always -> read-back, even if the carried
///   recovery says retryable; the uncertainty lane exists precisely
///   to avoid blindly replaying writes whose first attempt may have
///   landed.
fn classify_item_outcome(
    item: ItemOutcome<MutationSuccess>,
    outcomes: &mut HashMap<bifrost_types::ObjectId, MutationBucket>,
    retry_ids: &mut Vec<bifrost_types::ObjectId>,
    readback_ids: &mut Vec<bifrost_types::ObjectId>,
) {
    match item {
        ItemOutcome::Succeeded(success) => {
            let id = bifrost_types::ObjectId(success.item.0);
            let bucket = match success.output {
                MutationSuccess::Applied => MutationBucket::Applied,
                MutationSuccess::Skipped => MutationBucket::Skipped,
                _ => MutationBucket::Applied,
            };
            outcomes.insert(id, bucket);
        }
        ItemOutcome::Failed(failure) => {
            let id = bifrost_types::ObjectId(failure.item.0);
            match failure.error.recovery() {
                RecoveryClass::Retry(advice) => match advice.disposition {
                    bifrost_types::RetryDisposition::AfterStateRefresh => {
                        readback_ids.push(id.clone());
                        outcomes.insert(id, MutationBucket::PendingReadback);
                    }
                    bifrost_types::RetryDisposition::SameRequest
                    | bifrost_types::RetryDisposition::AfterAuthRefresh => {
                        retry_ids.push(id.clone());
                        outcomes.insert(id, MutationBucket::PendingRetry);
                    }
                    _ => {
                        readback_ids.push(id.clone());
                        outcomes.insert(id, MutationBucket::PendingReadback);
                    }
                },
                RecoveryClass::Reconcile(_) => {
                    readback_ids.push(id.clone());
                    outcomes.insert(id, MutationBucket::PendingReadback);
                }
                RecoveryClass::Engine(_) => {
                    outcomes.insert(id, MutationBucket::BlockedByEngine);
                }
                _ => {
                    outcomes.insert(id, MutationBucket::FailedTerminal);
                }
            }
        }
        ItemOutcome::Uncertain(uncertain) => {
            let id = bifrost_types::ObjectId(uncertain.item.0);
            readback_ids.push(id.clone());
            outcomes.insert(id, MutationBucket::PendingReadback);
        }
    }
}

fn counters_from_outcomes(
    outcomes: &HashMap<bifrost_types::ObjectId, MutationBucket>,
) -> crate::mutation::MutationCounters {
    let mut counters = crate::mutation::MutationCounters::default();
    for bucket in outcomes.values() {
        match bucket {
            MutationBucket::Applied => counters.record_applied(),
            MutationBucket::Skipped => counters.record_skipped(),
            MutationBucket::FailedTerminal | MutationBucket::BlockedByEngine => {
                counters.record_failed();
            }
            MutationBucket::PendingRetry | MutationBucket::PendingReadback => {
                counters.record_pending();
            }
        }
    }
    counters
}

#[cfg(test)]
mod tests {
    use super::scope_covers_membership;
    use bifrost_types::{CursorScope, FolderId, MembershipScope, ObjectType};

    #[test]
    fn folder_type_scope_covers_matching_mailbox_membership() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".into()),
            ty: ObjectType::Email,
        };
        let membership = MembershipScope::Mailbox(bifrost_types::MailboxId("inbox".into()));
        assert!(scope_covers_membership(&scope, &membership));
    }

    #[test]
    fn folder_type_scope_rejects_other_mailbox_membership() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".into()),
            ty: ObjectType::Email,
        };
        let membership = MembershipScope::Mailbox(bifrost_types::MailboxId("archive".into()));
        assert!(!scope_covers_membership(&scope, &membership));
    }
}
