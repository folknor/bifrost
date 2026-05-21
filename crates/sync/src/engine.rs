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
    Account, AccountCapabilities, AccountFactory, AccountId, AccountStream, ChangeCursor,
    Checkpoint, CursorEstablishment, CursorScope, InvalidationSink, MembershipScope, Priority,
    SubscriptionHandle, SyncEvent, WatchEvent,
};
use dashmap::DashMap;
use futures::stream::StreamExt;
use tokio::sync::{Mutex as AsyncMutex, broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::backfill::{
    BackfillHandle, BackfillRegistry, BackfillRunner, BackfillState, LiveSupersedes,
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
use crate::types::{AccountSlot, EngineConfig, WorkerTask};

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

impl SyncEngine {
    #[must_use]
    pub fn builder() -> SyncEngineBuilder {
        SyncEngineBuilder::new()
    }

    /// Attach an account to the engine.
    ///
    /// Flow per `bifrost-sync.md` -> attach:
    /// 1. Call `factory.open()` to obtain the first `Arc<dyn Account>`.
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
        let opened = factory.open().await.map_err(Error::OpenFailed)?;
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
        // forward those onto `changes_tx` so consumers observe
        // cold-start data the same way they observe live changes.
        let scopes = self.discover_scopes(opened.as_ref()).await?;
        for scope in scopes.clone() {
            self.establish_one(
                &account_id,
                opened.as_ref(),
                scope,
                Arc::clone(&cursors),
                Some(changes_tx.clone()),
            )
            .await?;
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
        let ack_writer_shutdown = shutdown.clone();
        spawn(tokio::spawn(ack_writer(
            ack_writer_aid,
            ack_writer_store,
            ack_writer_control,
            ack_rx,
            ack_writer_shutdown,
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
        };
        spawn(tokio::spawn(reconciler.run(watch_rx)));

        // In-process push forwarder: drain `Account::push_stream` into
        // the per-account mpsc. Reload `current.load_full()` inside
        // the loop so reopens take effect.
        if capabilities.push_in_process() {
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
            )
            .await;
        }));

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
                            ReopenRequest::Recovery { scope, recovery } => {
                                handle_recovery(
                                    &reopen_factory,
                                    &reopen_current,
                                    &reopen_cursors,
                                    &reopen_store,
                                    &reopen_changes,
                                    &reopen_aid,
                                    &reopen_control,
                                    scope,
                                    recovery,
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
        tx.send(AckRequest {
            scope,
            checkpoint,
            auto: false,
        })
        .await
        .map_err(|e| Error::Other(format!("ack channel closed: {e}")))
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
        slot.shutdown.cancel();

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
        let next = slot.factory.open().await.map_err(Error::OpenFailed)?;
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

        // Reuse the same idempotency key across retry attempts so the
        // protocol can dedup; see `IdempotencyVendor` for the
        // run_id/sequence layout. Each campaign retry comes from a
        // single `vendor.next(protocol)` call.
        let key = vendor.next(protocol);

        // Per-id final outcome map. Updates as attempts make progress;
        // the final aggregate counters are computed once at the end,
        // so a retry that flips a previous-attempt Failed -> Applied
        // does not double-count.
        let mut outcomes: HashMap<bifrost_types::ObjectId, MutationBucket> = HashMap::new();
        let mut retry_ids: Vec<bifrost_types::ObjectId> = Vec::new();
        let mut remaining: Vec<bifrost_types::ObjectId> = targets;
        let mut attempt: u32 = 0;
        let mut retry_after: Option<Duration> = None;

        loop {
            // Sleep before retry if the previous attempt requested it
            // via `RecoveryClass::Retry`.
            if let Some(delay) = retry_after.take() {
                tokio::time::sleep(delay).await;
            }

            let account = slot.current.load_full();
            let target_stream: AccountStream<bifrost_types::ObjectId> =
                Box::pin(futures::stream::iter(remaining.clone()));
            let mut stream = account.bulk_set_flags(target_stream, op.clone(), key.clone());
            let mut fatal_retry: Option<Duration> = None;
            retry_ids.clear();

            while let Some(event) = stream.next().await {
                match event {
                    bifrost_types::SyncEvent::Batch(batch) => {
                        for result in batch.items {
                            let bucket = classify_mutation_outcome(&result.outcome);
                            if bucket == MutationBucket::PendingRetry {
                                retry_ids.push(result.id.clone());
                            }
                            outcomes.insert(result.id, bucket);
                        }
                    }
                    bifrost_types::SyncEvent::Fatal(f) => {
                        if let bifrost_types::RecoveryClass::Retry { after } = &f.recovery {
                            fatal_retry = Some(*after);
                            break;
                        }
                        return Err(Error::Other(format!("bulk_set_flags fatal: {}", f.message)));
                    }
                    bifrost_types::SyncEvent::Done(_) => break,
                    bifrost_types::SyncEvent::Progress(_)
                    | bifrost_types::SyncEvent::Warning(_) => {}
                    _ => {}
                }
            }

            attempt = attempt.saturating_add(1);
            if let Some(after) = fatal_retry
                && attempt < max_retries
            {
                retry_after = Some(after);
                let retry_set: std::collections::HashSet<_> = retry_ids.iter().cloned().collect();
                remaining.retain(|id| {
                    retry_set.contains(id)
                        || !matches!(
                            outcomes.get(id),
                            Some(
                                MutationBucket::Applied
                                    | MutationBucket::Skipped
                                    | MutationBucket::FailedTerminal
                            )
                        )
                });
                if remaining.is_empty() {
                    break;
                }
                continue;
            }

            for id in &remaining {
                outcomes
                    .entry(id.clone())
                    .or_insert(MutationBucket::PendingRetry);
            }
            break;
        }
        let mut totals = counters_from_outcomes(&outcomes);
        let pending_ids: Vec<_> = outcomes
            .iter()
            .filter_map(|(id, bucket)| {
                if *bucket == MutationBucket::PendingRetry {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        if !pending_ids.is_empty() {
            let account = slot.current.load_full();
            let outcome =
                crate::mutation::run_readback_guard(account.as_ref().as_ref(), pending_ids, &op)
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
                SyncEvent::Fatal(f) => {
                    return Err(Error::EstablishCursorFailed(format!(
                        "discover_cursor_scopes fatal: {}",
                        f.message
                    )));
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
        changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
    ) -> Result<(), Error> {
        // Check the store first - resume path.
        if let Some(existing) = self
            .checkpoints
            .get_change_cursor(account_id, &scope)
            .await?
        {
            cursors.put(existing);
            return Ok(());
        }
        match account
            .establish_initial_cursor(scope.clone())
            .await
            .map_err(|e| Error::EstablishCursorFailed(format!("{e}")))?
        {
            CursorEstablishment::Ready(cursor) => {
                self.persist_cursor(account_id, cursor, cursors).await
            }
            CursorEstablishment::EstablishViaInventory => {
                // Drive the inventory fusion through the per-account
                // broadcast so the inventory items DO reach
                // subscribers; the cursor establishes only on a
                // successful terminal Done.
                let fusion = crate::multiplexer::InventoryFusion {
                    account_id: account_id.clone(),
                    cursors: Arc::clone(&cursors),
                    store: Arc::clone(&self.checkpoints),
                };
                match fusion
                    .run_with_broadcast(account, scope, changes_tx)
                    .await?
                {
                    crate::multiplexer::FusionOutcome::Established
                    | crate::multiplexer::FusionOutcome::NoCursor => Ok(()),
                    crate::multiplexer::FusionOutcome::Fatal(recovery) => {
                        Err(Error::EstablishCursorFatal {
                            message: "inventory fusion fatal".into(),
                            recovery,
                        })
                    }
                }
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
        let mut stream = account.discover_memberships();
        let known_scopes = cursors.all_scopes();
        while let Some(event) = stream.next().await {
            match event {
                SyncEvent::Batch(batch) => {
                    for membership in batch.items {
                        for scope in &known_scopes {
                            if scope_covers_membership(scope, &membership) {
                                cursors.link_membership(membership.clone(), scope.clone());
                            }
                        }
                    }
                }
                SyncEvent::Done(_) => break,
                SyncEvent::Fatal(f) => {
                    tracing::warn!(
                        target: "bifrost.sync.changes",
                        message = %f.message,
                        "discover_memberships fatal; continuing without index"
                    );
                    break;
                }
                SyncEvent::Progress(_) | SyncEvent::Warning(_) => {}
                _ => {}
            }
        }
        Ok(())
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
) {
    let scopes = cursors.all_scopes();
    for scope in scopes {
        if shutdown.is_cancelled() {
            return;
        }
        registry.mark(scope.clone(), BackfillState::Running);
        let acc_arc = account.load_full();
        let acc: &dyn Account = acc_arc.as_ref().as_ref();
        // Use a single-partition policy for v1; partition planning
        // beyond one inventory pass is tracked as a follow-up.
        let partition = bifrost_types::Partition(Vec::new());
        match BackfillRunner::run_partition(
            acc,
            scope.clone(),
            partition,
            live.as_ref(),
            &account_id,
            Arc::clone(&store),
            changes_tx.clone(),
            crate::cursor::ENGINE_VERSION,
        )
        .await
        {
            Ok(_kept) => {
                registry.mark(scope.clone(), BackfillState::Completed);
            }
            Err(err) => {
                tracing::warn!(
                    target: "bifrost.sync.backfill",
                    scope = ?scope,
                    error = %err,
                    "backfill partition failed; leaving scope Pending"
                );
                registry.mark(scope.clone(), BackfillState::Pending);
            }
        }
    }
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
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            req = rx.recv() => {
                let Some(req) = req else { return; };
                match &req.checkpoint {
                    Checkpoint::Change(c) => {
                        if let Err(err) = store
                            .put_change_cursor(&account_id, c.clone())
                            .await
                        {
                            tracing::warn!(
                                target: "bifrost.sync.changes",
                                account = ?account_id,
                                scope = ?req.scope,
                                error = %err,
                                auto = req.auto,
                                "ack: persist_change_cursor failed"
                            );
                            continue;
                        }
                    }
                    Checkpoint::Backfill(b) => {
                        if let Err(err) = store
                            .put_backfill(&account_id, b.clone())
                            .await
                        {
                            tracing::warn!(
                                target: "bifrost.sync.backfill",
                                account = ?account_id,
                                scope = ?req.scope,
                                error = %err,
                                "ack: put_backfill failed"
                            );
                            continue;
                        }
                    }
                    _ => {
                        // Unknown future Checkpoint variant; ignore.
                        continue;
                    }
                }
                // Notify pause / checkpoint_now waiters AFTER the
                // durable write lands - the contract is that the
                // returned checkpoint has been persisted.
                control.record_checkpoint(req.checkpoint).await;
            }
        }
    }
}

/// Dispatch a recovery class. Threads through the engine's reopen +
/// re-establishment machinery.
#[allow(clippy::too_many_arguments)]
async fn handle_recovery(
    factory: &Arc<dyn AccountFactory>,
    current: &Arc<ArcSwap<Arc<dyn Account>>>,
    cursors: &Arc<CursorRegistry>,
    store: &Arc<DynCheckpointStore>,
    changes_tx: &broadcast::Sender<MultiplexerEvent>,
    account_id: &AccountId,
    control: &SyncControl,
    scope: CursorScope,
    recovery: bifrost_types::RecoveryClass,
) {
    use bifrost_types::RecoveryClass;
    match recovery {
        RecoveryClass::Retry { after } => {
            // Sleep; the per-scope poll task will re-enter on its own
            // cadence regardless. We do not need to do anything else.
            tokio::time::sleep(after).await;
        }
        RecoveryClass::RestartScope(_) | RecoveryClass::DowngradeCapabilityForScope(_) => {
            // Drop the scope's cursor and the durable cursor, then
            // re-establish via inventory. The next poll iteration will
            // see `cursors.snapshot(&scope).is_none()` and exit; we
            // re-establish here so the broadcast path picks up
            // immediately on the engine-level reopen sweep.
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
            if let Err(err) = run_establish(
                account_id,
                acc,
                scope.clone(),
                Arc::clone(cursors),
                Arc::clone(store),
                changes_tx.clone(),
            )
            .await
            {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?account_id,
                    scope = ?scope,
                    error = %err,
                    "RestartScope: re-establishment failed"
                );
            }
        }
        RecoveryClass::RestartAccount | RecoveryClass::CapabilityChanged { .. } => {
            match factory.open().await {
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
        RecoveryClass::AuthLost
        | RecoveryClass::SchemaIncompatible
        | RecoveryClass::OperatorOverrideRequired { .. }
        | RecoveryClass::Fatal
        | RecoveryClass::DowngradeStrategy(_) => {
            // Engine has no automated recovery; surface via broadcast
            // so consumers observe the terminal Fatal that was emitted
            // by the driver. The driver already pushed the Fatal onto
            // changes_tx; nothing further to do here.
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?account_id,
                scope = ?scope,
                "recovery requires consumer action; not auto-handling"
            );
        }
        // Unknown future variant: same surface-only handling.
        _ => {
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?account_id,
                scope = ?scope,
                "unknown RecoveryClass variant; surface-only"
            );
        }
    }
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
        .map_err(|e| Error::EstablishCursorFailed(format!("{e}")))?
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
                crate::multiplexer::FusionOutcome::Fatal(recovery) => {
                    Err(Error::EstablishCursorFatal {
                        message: "inventory fusion fatal during recovery".into(),
                        recovery,
                    })
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

/// Classify a `MutationOutcome` into the counter bucket the engine
/// reports back to the consumer.
///
/// `Failed(Error)` is split: terminal failures (auth lost,
/// unsupported, cursor/schema mismatch) bypass the read-back guard
/// and land in `failed_terminal`. Everything else goes through the
/// retry/read-back path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationBucket {
    Applied,
    Skipped,
    FailedTerminal,
    PendingRetry,
}

fn classify_mutation_outcome(outcome: &bifrost_types::MutationOutcome) -> MutationBucket {
    match outcome {
        bifrost_types::MutationOutcome::Applied => MutationBucket::Applied,
        bifrost_types::MutationOutcome::Skipped => MutationBucket::Skipped,
        bifrost_types::MutationOutcome::Failed(err) => {
            if is_terminal_mutation_error(err) {
                MutationBucket::FailedTerminal
            } else {
                MutationBucket::PendingRetry
            }
        }
        // `MutationOutcome` is `#[non_exhaustive]`; conservatively
        // surface unknown future variants as terminal failures.
        _ => MutationBucket::FailedTerminal,
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
            MutationBucket::FailedTerminal => counters.record_failed(),
            MutationBucket::PendingRetry => counters.record_pending(),
        }
    }
    counters
}

fn is_terminal_mutation_error(err: &bifrost_types::Error) -> bool {
    matches!(
        err,
        bifrost_types::Error::Auth(_)
            | bifrost_types::Error::Unsupported
            | bifrost_types::Error::MissingCoreCapability
            | bifrost_types::Error::CursorProtocolMismatch
            | bifrost_types::Error::CursorEnvelopeUnknown
            | bifrost_types::Error::SchemaIncompatible
    )
}
