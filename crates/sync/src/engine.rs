//! `SyncEngine` lifecycle.
//!
//! Holds an `Arc<dyn AccountFactory>` per attached account, drives one
//! multiplexer / backfill / push reconciler / mutation pipeline per
//! slot, and exposes the engine's public surface: `attach`, `detach`,
//! `account_changes_stream`, `bulk_*` campaign entry points, the
//! read-only hydration passthrough (`get_stream`, `message_hydrate`,
//! `open_blob`, `open_raw_rfc822`, ...), `invalidation_sink`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bifrost_types::{
    Account, AccountCapabilities, AccountControl, AccountError, AccountFactory, AccountFuture,
    AccountId, AccountStream, BackfillCheckpoint, BackfillProgress, Batch, ChangeCursor,
    Checkpoint, CursorEstablishment, CursorScope, DiagnosticText, EngineDirective, ErrorScope,
    InvalidationSink, InventoryPartition, InventoryPartitioning, ItemOutcome, MembershipScope,
    MutationSuccess, PageBoundary, PauseReason, Priority, ReconcileAction, ReconcileAdvice,
    RecoveryClass, RetryAdvice, SubscriptionHandle, SyncEvent, WatchEvent,
};
use dashmap::DashMap;
use futures::stream::StreamExt;
use tokio::sync::{Mutex as AsyncMutex, Notify, broadcast, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::backfill::{
    BackfillPolicy, BackfillRegistry, BackfillRunner, BackfillState, BackfillStrategy,
    LiveSupersedes,
};
use crate::cancel::{Boundary, BoundaryRequest};
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::cursor::store::{DynCheckpointStore, InMemoryCheckpointStore};
use crate::error::Error;
use crate::multiplexer::{
    AckRequest, Multiplexer, MultiplexerEvent, MultiplexerHandle, ReopenRequest,
};
use crate::push::{InvalidationSinkInner, RegisteredSubscription, SubscriptionRegistry};
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

/// Whether an `establish_initial_cursor` failure is contained to the scope
/// that produced it, rather than a reason to fail the whole account attach.
///
/// The signal is the account layer's own derived `RecoveryClass`: a protocol
/// crate that classifies a failure as `DisableScope(_)` has already said the
/// scope is independently quarantinable (a revoked shared mailbox, an
/// unreadable public folder). The running path honors that through
/// `disable_scope`; this is the same rule applied at attach, where the loop
/// previously propagated every error and let one dead share take the primary
/// mailbox down with it.
///
/// Deliberately narrow: anything else (auth loss, transport failure, a
/// schema-incompatible cursor) still fails the attach, because those are not
/// scope-local facts.
///
/// Pure so the containment rule is unit-pinnable without an engine.
fn scope_local_establish_failure(error: &AccountError) -> bool {
    matches!(
        error.recovery(),
        RecoveryClass::Engine(EngineDirective::DisableScope(_))
    )
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
        let cleanup = Arc::clone(&opened);
        let result = self.attach_opened(account_id, factory, opened).await;
        if result.is_err()
            && let Err(error) = cleanup.close().await
        {
            tracing::warn!(
                target: "bifrost.sync.attach",
                error = ?error,
                "account close failed while unwinding attach"
            );
        }
        result
    }

    async fn attach_opened(
        &self,
        account_id: AccountId,
        factory: Arc<dyn AccountFactory>,
        opened: Arc<dyn Account>,
    ) -> Result<SyncControl, Error> {
        let capabilities = Arc::new(std::sync::RwLock::new(opened.capabilities().clone()));

        let cursors = Arc::new(CursorRegistry::new());

        // Per-account broadcast for the unified Change stream. Keep a
        // sentinel receiver on the slot so the channel never closes
        // when subscribers come and go.
        let (changes_tx, sentinel_rx) =
            broadcast::channel::<MultiplexerEvent>(self.config.multiplexer.changes_capacity);

        // Per-account control broadcast. The engine publishes
        // `AccountControl::Pause(reason)` on this channel when it
        // auto-pauses the account (operator override, retry budget
        // exhausted). Consumers subscribe via
        // `SyncEngine::account_control_stream`.
        let (account_control_tx, account_control_sentinel) =
            broadcast::channel::<AccountControl>(16);

        // Notify fired when a real subscriber arrives so deferred
        // inventory workers can park without hot-polling.
        let subscriber_notify = Arc::new(Notify::new());

        // Per-account throttle bucket. Shared via Mutex because the
        // engine's recovery paths cross task boundaries.
        let throttles = Arc::new(std::sync::Mutex::new(crate::recovery::ThrottleBucket::new()));

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
            let established = self
                .establish_one(
                    &account_id,
                    opened.as_ref(),
                    scope.clone(),
                    Arc::clone(&cursors),
                )
                .await;
            match established {
                Ok(InitialScope::Ready) => {}
                Ok(InitialScope::DeferredInventory(scope)) => {
                    deferred_inventory_scopes.push(scope);
                }
                // A scope-local failure must stay scope-local. The running
                // path already quarantines one revoked shared/public scope
                // without touching its siblings (`disable_scope`); attach did
                // not, so a single unreachable shared mailbox or public folder
                // failed the whole account and the consumer got no sync at
                // all - primary mail included. Drop the scope and continue;
                // the next reopen re-runs discovery and picks it back up if
                // access returned.
                Err(Error::Account(error)) if scope_local_establish_failure(&error) => {
                    tracing::warn!(
                        target: "bifrost.sync.attach",
                        account = ?account_id,
                        scope = ?scope,
                        error = %error,
                        "scope-local establishment failure; skipping scope, account continues"
                    );
                }
                Err(other) => return Err(other),
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
        let (account_generation_tx, account_generation_rx) = watch::channel(0_u64);
        let reopen_lock = Arc::new(AsyncMutex::new(()));

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
        // requests when a stream ends with an `EngineDirective`-class
        // recovery, carrying the originating `AccountError`.
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
        {
            let acc = Arc::clone(&current);
            let aid = account_id.clone();
            let tx = watch_tx.clone();
            let sd = shutdown.clone();
            let mut generation = account_generation_rx.clone();
            spawn(tokio::spawn(async move {
                let mut reconnect_delay = Duration::from_millis(50);
                loop {
                    if sd.is_cancelled() {
                        return;
                    }
                    let acc_arc = acc.load_full();
                    if acc_arc.capabilities().push == bifrost_types::PushCapability::None {
                        tokio::select! {
                            () = sd.cancelled() => return,
                            changed = generation.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                            }
                            () = tokio::time::sleep(Duration::from_secs(1)) => {}
                        }
                        continue;
                    }
                    let mut stream = acc_arc.push_stream();
                    let mut received = false;
                    let mut reopened = false;
                    loop {
                        tokio::select! {
                            () = sd.cancelled() => return,
                            changed = generation.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                                reopened = true;
                                break;
                            }
                            next = stream.next() => {
                                let Some(event) = next else { break; };
                                if !matches!(&event, WatchEvent::Terminated(_)) {
                                    received = true;
                                }
                                match tx.try_send(event) {
                                    Ok(()) => {}
                                    Err(mpsc::error::TrySendError::Full(rejected)) => {
                                        tracing::trace!(
                                            target: "bifrost.sync.changes",
                                            account = ?aid,
                                            "in-process push: queue full, coalesced"
                                        );
                                        let lossless =
                                            crate::push::requires_lossless_delivery(&rejected);
                                        let delivery = if lossless {
                                            rejected
                                        } else {
                                            crate::push::coalesced_event(rejected)
                                        };
                                        if lossless {
                                            tokio::select! {
                                                () = sd.cancelled() => return,
                                                _ = tx.send(delivery) => {}
                                            }
                                        } else {
                                            tokio::select! {
                                                () = sd.cancelled() => return,
                                                _ = tokio::time::timeout(
                                                    Duration::from_millis(100),
                                                    tx.send(delivery),
                                                ) => {}
                                            }
                                        }
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => return,
                                }
                            }
                        }
                    }
                    if reopened {
                        reconnect_delay = Duration::from_millis(50);
                        continue;
                    }
                    if received {
                        reconnect_delay = Duration::from_millis(50);
                    }
                    // push_stream ended; loop reloads the (possibly
                    // reopened) handle and restarts. Exponential
                    // backoff prevents an empty stream implementation
                    // from reconstructing itself 20 times per second.
                    tokio::select! {
                        () = sd.cancelled() => return,
                        () = tokio::time::sleep(reconnect_delay) => {}
                    }
                    reconnect_delay = reconnect_delay
                        .saturating_mul(2)
                        .min(Duration::from_secs(30));
                }
            }));
        }

        // Spawn the multiplexer.
        let mux = Multiplexer {
            account_id: account_id.clone(),
            account: Arc::clone(&current),
            account_generation: account_generation_rx,
            cursors: Arc::clone(&cursors),
            config: self.config.multiplexer,
            boundary: boundary_view.clone(),
            changes_tx: changes_tx.clone(),
            control: control.clone(),
            shutdown: shutdown.clone(),
            reopen_tx: reopen_tx.clone(),
            ack_tx: Some(ack_tx.clone()),
            scope_tokens: Arc::new(std::sync::Mutex::new(HashMap::new())),
        };
        spawn(tokio::spawn(mux.run()));

        // Spawn the backfill orchestrator. It walks the registered
        // scopes and runs one `BackfillRunner::run_partition` per
        // scope under a default policy. Items + checkpoints flow onto
        // the same per-account broadcast.
        // The de-dup set the partition runner filters against. Nothing
        // populates it - see the `LiveSupersedes` type docs for why
        // broadcasting a live change is not evidence the consumer got
        // it, and therefore not a sound basis for suppressing that
        // object's inventory copy.
        let live_supersedes = Arc::new(LiveSupersedes::new());
        let backfill_registry_handle = Arc::clone(&self.backfill_registry);
        let bf_account = Arc::clone(&current);
        let bf_account_id = account_id.clone();
        let bf_cursors = Arc::clone(&cursors);
        let bf_live = Arc::clone(&live_supersedes);
        let bf_store = Arc::clone(&self.checkpoints);
        let bf_shutdown = shutdown.clone();
        let bf_changes = changes_tx.clone();
        let bf_subscriber_notify = Arc::clone(&subscriber_notify);
        let bf_config = self.config.backfill;
        let bf_control = control.clone();
        spawn(tokio::spawn(async move {
            run_backfill_orchestrator(
                bf_account,
                bf_account_id,
                bf_cursors,
                bf_live,
                bf_store,
                backfill_registry_handle,
                bf_shutdown,
                Some(bf_changes),
                bf_subscriber_notify,
                bf_config,
                bf_control,
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
            let inventory_account_control_tx = account_control_tx.clone();
            let inventory_throttles = Arc::clone(&throttles);
            let inventory_notify = Arc::clone(&subscriber_notify);
            let inventory_boundary_tx = boundary.sender();
            let inventory_capabilities = Arc::clone(&capabilities);
            let inventory_subscriptions = Arc::clone(&self.subscriptions);
            let inventory_account_generation_tx = account_generation_tx.clone();
            let inventory_reopen_lock = Arc::clone(&reopen_lock);
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
                    inventory_account_control_tx,
                    inventory_throttles,
                    inventory_notify,
                    inventory_boundary_tx,
                    inventory_capabilities,
                    inventory_subscriptions,
                    inventory_account_generation_tx,
                    inventory_reopen_lock,
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
        let reopen_account_control_tx = account_control_tx.clone();
        let reopen_throttles = Arc::clone(&throttles);
        let reopen_boundary_tx = boundary.sender();
        let reopen_capabilities = Arc::clone(&capabilities);
        let reopen_subscriptions = Arc::clone(&self.subscriptions);
        let reopen_account_generation_tx = account_generation_tx.clone();
        let reopen_serial = Arc::clone(&reopen_lock);
        spawn(tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = reopen_shutdown.cancelled() => return,
                    req = reopen_rx.recv() => {
                        let Some(req) = req else { return; };
                        match req {
                            ReopenRequest::Recovery { scope, error } => {
                                let ctx = RecoveryContext {
                                    factory: &reopen_factory,
                                    current: &reopen_current,
                                    cursors: &reopen_cursors,
                                    store: &reopen_store,
                                    changes_tx: &reopen_changes,
                                    account_id: &reopen_aid,
                                    control: &reopen_control,
                                    account_control_tx: &reopen_account_control_tx,
                                    throttles: &reopen_throttles,
                                    boundary_tx: &reopen_boundary_tx,
                                    capabilities: &reopen_capabilities,
                                    subscriptions: &reopen_subscriptions,
                                    account_generation_tx: &reopen_account_generation_tx,
                                    reopen_lock: &reopen_serial,
                                };
                                handle_account_error(&ctx, scope, error).await;
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
        };
        let slot = Arc::new(AccountSlot {
            factory,
            current,
            account_generation_tx,
            reopen_lock,
            capabilities,
            multiplexer,
            cursors: Arc::clone(&cursors),
            checkpoints: Arc::clone(&self.checkpoints),
            boundary_tx: boundary.sender(),
            shutdown: shutdown.clone(),
            control: control.clone(),
            _sentinel_rx: sentinel_rx,
            workers: std::sync::Mutex::new(workers),
            account_control_tx,
            _account_control_sentinel: account_control_sentinel,
            subscriber_notify,
            reopen_tx: reopen_tx.clone(),
            throttles: Arc::clone(&throttles),
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

    /// Detach an account. Publishes `Stop`, cancels the slot, drains or
    /// aborts workers within `detach_timeout`, closes the live account,
    /// and removes engine registrations. It does not wait for a
    /// consumer-acked safe boundary and does not destroy server-side
    /// push subscriptions; call `unsubscribe_push` first for that.
    pub async fn detach(&self, account_id: &AccountId) -> Result<(), Error> {
        let Some((_, slot)) = self.accounts.remove(account_id) else {
            return Err(Error::AccountNotAttached(account_id.clone()));
        };
        // Drop the public ack sender so no new consumer acks enter
        // during teardown. Worker-held clones remain alive long enough
        // to flush their final checkpoint to the ack writer.
        self.ack_senders.remove(account_id);
        // Ask running workers to checkpoint cleanly, then stop.
        slot.boundary_tx.send_replace(BoundaryRequest::Stop);
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
        self.backfill_registry.forget_account(account_id);
        if let Some(meter) = &self.bandwidth_meter {
            meter.forget_account(account_id);
        }
        Ok(())
    }

    /// Reopen and fully reattach an account slot. Scope and membership
    /// discovery, cursor topology, push subscriptions, capabilities,
    /// and the live protocol handle all refresh before the old handle
    /// is closed.
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
        let ctx = RecoveryContext {
            factory: &slot.factory,
            current: &slot.current,
            cursors: &slot.cursors,
            store: &slot.checkpoints,
            changes_tx: &slot.multiplexer.changes_tx,
            account_id,
            control: &slot.control,
            account_control_tx: &slot.account_control_tx,
            throttles: &slot.throttles,
            boundary_tx: &slot.boundary_tx,
            capabilities: &slot.capabilities,
            subscriptions: &self.subscriptions,
            account_generation_tx: &slot.account_generation_tx,
            reopen_lock: &slot.reopen_lock,
        };
        let _reopen_guard = slot.reopen_lock.lock().await;
        reattach_account(&ctx, next).await
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
        let rx = slot.multiplexer.changes_tx.subscribe();
        // Wake any deferred-inventory workers parked on the Notify so
        // they observe the new subscriber without hot-polling.
        slot.subscriber_notify.notify_waiters();
        Ok(rx)
    }

    /// Subscribe to the per-account control stream. Engine publishes
    /// `AccountControl::Pause(reason)` on this channel when it
    /// auto-pauses the account (operator override, retry budget
    /// exhausted). Consumers respond by acting on the bounded
    /// `PauseReason` and, once resolved, calling
    /// [`SyncEngine::resume_account`].
    pub fn account_control_stream(
        &self,
        account_id: &AccountId,
    ) -> Result<broadcast::Receiver<AccountControl>, Error> {
        let slot = self
            .accounts
            .get(account_id)
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        Ok(slot.account_control_tx.subscribe())
    }

    /// Consumer-driven resume. Flips the boundary back to `Run` and
    /// publishes `AccountControl::Resume` so other subscribers observe
    /// the transition. Idempotent.
    pub fn resume_account(&self, account_id: &AccountId) -> Result<(), Error> {
        let slot = self
            .accounts
            .get(account_id)
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        slot.boundary_tx
            .send_replace(crate::cancel::BoundaryRequest::Run);
        let _ = slot.account_control_tx.send(AccountControl::Resume);
        Ok(())
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
            .record(account_id.clone(), handle.clone(), scopes.to_vec());
        Ok(handle)
    }

    /// Drive a single-account bulk-flag campaign against an attached
    /// account, applying the read-back guard to the retry candidates.
    ///
    /// Returns the per-batch counters aggregated across the run.
    ///
    /// Mutation accounting consumes `ItemOutcome<MutationSuccess>` and
    /// dispatches per-item failures via [`crate::recovery::plan_recovery`]:
    ///
    /// - `Retry { SameRequest | AfterAuthRefresh }` -> retry queue.
    /// - `Retry { AfterStateRefresh }` and `Reconcile { CheckTarget }`
    ///   -> read-back queue.
    /// - `Reconcile { DedupeByClientId }` -> dedupe counter increments
    ///   plus a `Warning::OperatorAttentionNeeded` (the dedupe itself
    ///   lives at the consumer because the client-id space is theirs).
    ///   (sync-D4)
    /// - `Engine(_)` -> blocked-by-engine counter increments AND the
    ///   directive is forwarded through `ReopenRequest::Recovery` so
    ///   the engine's recovery dispatch handles the restart / downgrade
    ///   / schema clear. The campaign returns with a partial outcome
    ///   accounting for work seen so far. (sync-D5)
    /// - terminal recovery -> `failed_terminal`.
    ///
    /// `ItemOutcome::Uncertain` is always queued for read-back.
    pub async fn bulk_set_flags(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::ObjectId>,
        op: bifrost_types::FlagOp,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        use crate::recovery::{RecoveryPlan, plan_recovery};
        let slot = self
            .accounts
            .get(account_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let max_retries = self.config.mutation_max_retries;

        let key = vendor.next(protocol);

        let mut outcomes: HashMap<bifrost_types::ObjectId, MutationBucket> = HashMap::new();
        let mut retry_ids: Vec<bifrost_types::ObjectId> = Vec::new();
        let mut dedupe_count: u64 = 0;
        let mut remaining: Vec<bifrost_types::ObjectId> = targets;
        let mut attempt: u32 = 0;
        let mut retry_advice: Option<RetryAdvice> = None;
        let mut blocked_by_engine: bool = false;

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
            // Only the retry queue is per-attempt. Read-back membership
            // is derived from `outcomes` after the campaign ends, so an
            // id parked in the read-back lane survives an attempt that
            // resubmits its siblings.
            retry_ids.clear();
            let mut stream_termination_advice: Option<RetryAdvice> = None;

            while let Some(event) = stream.next().await {
                match event {
                    bifrost_types::SyncEvent::Batch(batch) => {
                        for item in batch.items {
                            if let Some((scope, error)) = classify_item_outcome(
                                item,
                                &mut outcomes,
                                &mut retry_ids,
                                &mut dedupe_count,
                            ) {
                                let _ = slot
                                    .reopen_tx
                                    .send(ReopenRequest::Recovery { scope, error })
                                    .await;
                                blocked_by_engine = true;
                            }
                        }
                    }
                    bifrost_types::SyncEvent::Terminated(err) => {
                        // Plan the recovery on the stream-level
                        // terminating error and dispatch. Retry /
                        // Reconcile let the campaign continue; Engine
                        // routes through the reopen channel so the
                        // engine performs the directive (restart,
                        // downgrade, schema clear, etc.) and the
                        // campaign returns the partial outcome. Clone
                        // so the engine arm can forward the original
                        // error through the reopen channel without
                        // losing it to `plan_recovery`'s consume.
                        let original = err.clone();
                        match plan_recovery(err) {
                            RecoveryPlan::Retry(advice) => {
                                queue_unresolved_for_retry(&remaining, &outcomes, &mut retry_ids);
                                stream_termination_advice = Some(advice);
                                break;
                            }
                            RecoveryPlan::Reconcile(advice) => {
                                let mut wants_dedupe = false;
                                for action in &advice.guidance.actions {
                                    match action {
                                        ReconcileAction::CheckTarget => {}
                                        ReconcileAction::DedupeByClientId => wants_dedupe = true,
                                        _ => {}
                                    }
                                }
                                for id in &remaining {
                                    if !matches!(
                                        outcomes.get(id),
                                        Some(
                                            MutationBucket::Applied
                                                | MutationBucket::Skipped
                                                | MutationBucket::FailedTerminal
                                        )
                                    ) {
                                        // PendingReadback both queues the id
                                        // for the guard and makes
                                        // `counters_from_outcomes` count it
                                        // alongside other read-back-queued
                                        // items. Without this insert the
                                        // counter rebalance would underflow
                                        // when read-back resolves these ids
                                        // to skipped/still_failed.
                                        outcomes
                                            .insert(id.clone(), MutationBucket::PendingReadback);
                                    }
                                }
                                if wants_dedupe {
                                    dedupe_count = dedupe_count.saturating_add(1);
                                    let warning = bifrost_types::Warning::user_safe(
                                        bifrost_types::WarningKind::OperatorAttentionNeeded,
                                        "reconcile requested dedupe-by-client-id; consumer must dedupe",
                                    );
                                    let me = MultiplexerEvent {
                                        scope: CursorScope::Account,
                                        event: Arc::new(SyncEvent::Warning(warning)),
                                        checkpoint: None,
                                    };
                                    let _ = slot.multiplexer.changes_tx.send(me);
                                }
                                break;
                            }
                            RecoveryPlan::Engine(directive) => {
                                let directive_scope =
                                    crate::recovery::directive_target_scope(&directive);
                                let _ = slot
                                    .reopen_tx
                                    .send(ReopenRequest::Recovery {
                                        scope: directive_scope,
                                        error: original,
                                    })
                                    .await;
                                blocked_by_engine = true;
                                // Every still-unresolved id is blocked
                                // by the engine directive; record so
                                // the partial outcome surfaces them.
                                for id in &remaining {
                                    if !matches!(
                                        outcomes.get(id),
                                        Some(
                                            MutationBucket::Applied
                                                | MutationBucket::Skipped
                                                | MutationBucket::FailedTerminal
                                        )
                                    ) {
                                        outcomes
                                            .insert(id.clone(), MutationBucket::BlockedByEngine);
                                    }
                                }
                                break;
                            }
                            RecoveryPlan::Terminal(fatal) => {
                                return Err(Error::Account(fatal.into_inner()));
                            }
                        }
                    }
                    bifrost_types::SyncEvent::Done(_) => break,
                    bifrost_types::SyncEvent::Progress(_)
                    | bifrost_types::SyncEvent::Warning(_) => {}
                    _ => {}
                }
            }

            if blocked_by_engine {
                break;
            }

            attempt = attempt.saturating_add(1);
            let retry_set: HashSet<_> = retry_ids.iter().cloned().collect();
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
            // No more attempts. Anything still in `retry_ids` stays
            // pending so the read-back guard can decide applied vs
            // failed_terminal.
            for id in &retry_set {
                outcomes.insert(id.clone(), MutationBucket::PendingRetry);
            }
            break;
        }

        let mut totals = counters_from_outcomes(&outcomes);
        for _ in 0..dedupe_count {
            totals.record_dedupe_by_client_id();
        }
        let readback_ids = unresolved_readback_ids(&outcomes);
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

    /// Drive a single-account bulk-move campaign against an attached
    /// account, routing every target into `destination`.
    ///
    /// Shares the idempotency / retry / recovery loop with
    /// [`Self::bulk_set_flags`] (see [`Self::run_bulk_pipeline`]); the
    /// only differences are the wire op (`Account::bulk_move_from`) and the
    /// read-back guard, which reconciles against container membership
    /// rather than a flag set. Like `bulk_set_flags`, this is the
    /// volume path where the read-back guard and idempotency key matter,
    /// so it is NOT a one-shot direct call.
    pub async fn bulk_move(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::ObjectId>,
        destination: MembershipScope,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        self.bulk_move_from(account_id, targets, destination, None, vendor, protocol)
            .await
    }

    /// [`Self::bulk_move`] with the source container the targets are
    /// leaving.
    ///
    /// Identical pipeline; the source rides through to
    /// [`bifrost_types::Account::bulk_move_from`], which is what lets a
    /// Gmail campaign express the detach in the same `batchModify` as
    /// the attach instead of one `remove_from_container` per message.
    /// `None` is exactly [`Self::bulk_move`].
    ///
    /// The read-back guard is unchanged: it reconciles membership of
    /// `destination`, which is the property that says the move landed.
    /// It does not separately re-verify absence from `source`.
    pub async fn bulk_move_from(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::ObjectId>,
        destination: MembershipScope,
        source: Option<MembershipScope>,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        self.run_bulk_pipeline(
            account_id,
            targets,
            BulkPipelineOp::Move {
                destination,
                source,
            },
            vendor,
            protocol,
        )
        .await
    }

    /// Drive a single-account bulk-destroy campaign against an attached
    /// account.
    ///
    /// Shares the idempotency / retry / recovery loop with
    /// [`Self::bulk_set_flags`] (see [`Self::run_bulk_pipeline`]); the
    /// wire op is `Account::bulk_destroy` and the read-back guard
    /// reconciles against object *absence* (a destroyed id no longer
    /// hydrates). NOT a one-shot direct call, for the same reason as
    /// `bulk_set_flags`.
    pub async fn bulk_destroy(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::ObjectId>,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        self.run_bulk_pipeline(
            account_id,
            targets,
            BulkPipelineOp::Destroy,
            vendor,
            protocol,
        )
        .await
    }

    /// Shared bulk-mutation pipeline backing [`Self::bulk_move`] and
    /// [`Self::bulk_destroy`].
    ///
    /// Mirrors [`Self::bulk_set_flags`] exactly - idempotency-key
    /// vending, the per-id `classify_item_outcome` accumulation, the
    /// retry loop, the stream-terminated recovery dispatch
    /// (Retry / Reconcile / Engine / Terminal), and the final read-back
    /// guard - with two op-specific seams: the wire submit call and the
    /// matching read-back guard. `bulk_set_flags` deliberately keeps its
    /// own copy of the loop (its read-back is `FlagOp`-shaped); this
    /// helper is the move/destroy counterpart whose read-back is
    /// membership/absence-shaped.
    async fn run_bulk_pipeline(
        &self,
        account_id: &AccountId,
        targets: Vec<bifrost_types::ObjectId>,
        op: BulkPipelineOp,
        vendor: &crate::mutation::IdempotencyVendor,
        protocol: bifrost_types::ProtocolKind,
    ) -> Result<crate::mutation::MutationCounters, Error> {
        use crate::recovery::{RecoveryPlan, plan_recovery};
        let slot = self
            .accounts
            .get(account_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let max_retries = self.config.mutation_max_retries;

        let key = vendor.next(protocol);

        let mut outcomes: HashMap<bifrost_types::ObjectId, MutationBucket> = HashMap::new();
        let mut retry_ids: Vec<bifrost_types::ObjectId> = Vec::new();
        let mut dedupe_count: u64 = 0;
        let mut remaining: Vec<bifrost_types::ObjectId> = targets;
        let mut attempt: u32 = 0;
        let mut retry_advice: Option<RetryAdvice> = None;
        let mut blocked_by_engine: bool = false;

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
            let mut stream = match &op {
                BulkPipelineOp::Move {
                    destination,
                    source,
                } => account.bulk_move_from(
                    target_stream,
                    destination.clone(),
                    source.clone(),
                    key.clone(),
                ),
                BulkPipelineOp::Destroy => account.bulk_destroy(target_stream, key.clone()),
            };
            // Only the retry queue is per-attempt. Read-back membership
            // is derived from `outcomes` after the campaign ends, so an
            // id parked in the read-back lane survives an attempt that
            // resubmits its siblings.
            retry_ids.clear();
            let mut stream_termination_advice: Option<RetryAdvice> = None;

            while let Some(event) = stream.next().await {
                match event {
                    bifrost_types::SyncEvent::Batch(batch) => {
                        for item in batch.items {
                            if let Some((scope, error)) = classify_item_outcome(
                                item,
                                &mut outcomes,
                                &mut retry_ids,
                                &mut dedupe_count,
                            ) {
                                let _ = slot
                                    .reopen_tx
                                    .send(ReopenRequest::Recovery { scope, error })
                                    .await;
                                blocked_by_engine = true;
                            }
                        }
                    }
                    bifrost_types::SyncEvent::Terminated(err) => {
                        let original = err.clone();
                        match plan_recovery(err) {
                            RecoveryPlan::Retry(advice) => {
                                queue_unresolved_for_retry(&remaining, &outcomes, &mut retry_ids);
                                stream_termination_advice = Some(advice);
                                break;
                            }
                            RecoveryPlan::Reconcile(advice) => {
                                let mut wants_dedupe = false;
                                for action in &advice.guidance.actions {
                                    match action {
                                        ReconcileAction::CheckTarget => {}
                                        ReconcileAction::DedupeByClientId => wants_dedupe = true,
                                        _ => {}
                                    }
                                }
                                for id in &remaining {
                                    if !matches!(
                                        outcomes.get(id),
                                        Some(
                                            MutationBucket::Applied
                                                | MutationBucket::Skipped
                                                | MutationBucket::FailedTerminal
                                        )
                                    ) {
                                        outcomes
                                            .insert(id.clone(), MutationBucket::PendingReadback);
                                    }
                                }
                                if wants_dedupe {
                                    dedupe_count = dedupe_count.saturating_add(1);
                                    let warning = bifrost_types::Warning::user_safe(
                                        bifrost_types::WarningKind::OperatorAttentionNeeded,
                                        "reconcile requested dedupe-by-client-id; consumer must dedupe",
                                    );
                                    let me = MultiplexerEvent {
                                        scope: CursorScope::Account,
                                        event: Arc::new(SyncEvent::Warning(warning)),
                                        checkpoint: None,
                                    };
                                    let _ = slot.multiplexer.changes_tx.send(me);
                                }
                                break;
                            }
                            RecoveryPlan::Engine(directive) => {
                                let directive_scope =
                                    crate::recovery::directive_target_scope(&directive);
                                let _ = slot
                                    .reopen_tx
                                    .send(ReopenRequest::Recovery {
                                        scope: directive_scope,
                                        error: original,
                                    })
                                    .await;
                                blocked_by_engine = true;
                                for id in &remaining {
                                    if !matches!(
                                        outcomes.get(id),
                                        Some(
                                            MutationBucket::Applied
                                                | MutationBucket::Skipped
                                                | MutationBucket::FailedTerminal
                                        )
                                    ) {
                                        outcomes
                                            .insert(id.clone(), MutationBucket::BlockedByEngine);
                                    }
                                }
                                break;
                            }
                            RecoveryPlan::Terminal(fatal) => {
                                return Err(Error::Account(fatal.into_inner()));
                            }
                        }
                    }
                    bifrost_types::SyncEvent::Done(_) => break,
                    bifrost_types::SyncEvent::Progress(_)
                    | bifrost_types::SyncEvent::Warning(_) => {}
                    _ => {}
                }
            }

            if blocked_by_engine {
                break;
            }

            attempt = attempt.saturating_add(1);
            let retry_set: HashSet<_> = retry_ids.iter().cloned().collect();
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
            for id in &retry_set {
                outcomes.insert(id.clone(), MutationBucket::PendingRetry);
            }
            break;
        }

        let mut totals = counters_from_outcomes(&outcomes);
        for _ in 0..dedupe_count {
            totals.record_dedupe_by_client_id();
        }
        let readback_ids = unresolved_readback_ids(&outcomes);
        if !readback_ids.is_empty() {
            let account = slot.current.load_full();
            let outcome = match &op {
                BulkPipelineOp::Move { destination, .. } => {
                    crate::mutation::run_move_readback_guard(
                        account.as_ref().as_ref(),
                        readback_ids,
                        destination,
                    )
                    .await?
                }
                BulkPipelineOp::Destroy => {
                    crate::mutation::run_destroy_readback_guard(
                        account.as_ref().as_ref(),
                        readback_ids,
                    )
                    .await?
                }
            };
            totals.pending_retry = totals
                .pending_retry
                .saturating_sub(outcome.skipped)
                .saturating_sub(outcome.still_failed);
            totals.skipped = totals.skipped.saturating_add(outcome.skipped);
            totals.failed_terminal = totals.failed_terminal.saturating_add(outcome.still_failed);
        }
        Ok(totals)
    }

    // ---------- hydration passthrough ----------
    //
    // The change and inventory streams the engine broadcasts are
    // projection-only: a `Change` carries `{ id, kind }` and an
    // inventory entry carries a fingerprint, never message content. A
    // consumer that turns those signals into real rows therefore has to
    // fetch full content out-of-band. The `Account` handle that can do
    // so lives behind the slot's `ArcSwap` and is otherwise private, so
    // the engine exposes this read-only passthrough cluster as the
    // consumer's single door to hydration.
    //
    // Two deliberate properties:
    //
    // - Every method resolves the handle through `live_account`, i.e.
    //   `ArcSwap::load_full`, so a hydrate issued after a reopen runs
    //   against the freshly-installed connection, never a stale snapshot
    //   the consumer cached. This is the same discipline the spawned
    //   workers follow on their hot paths.
    // - Only the *read* surface is forwarded. Mutations stay funnelled
    //   through `bulk_set_flags` (and its siblings) so the idempotency /
    //   read-back / recovery pipeline remains the one chokepoint for
    //   writes; cursor and push driving stay engine-owned. Handing out a
    //   raw `Arc<dyn Account>` would leak both, so we do not.
    //
    // The forwarded methods return `'static` streams / futures that
    // capture their own internal `Arc` clones, so they outlive the
    // short-lived handle resolved per call.

    /// Resolve the live `Account` handle for an attached account.
    ///
    /// Loads through the slot's `ArcSwap` so the caller sees the handle
    /// installed by the most recent reopen. Errors with
    /// `AccountNotAttached` when no slot exists for `account_id`.
    fn live_account(&self, account_id: &AccountId) -> Result<Arc<Arc<dyn Account>>, Error> {
        let slot = self
            .accounts
            .get(account_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        Ok(slot.current.load_full())
    }

    /// Hydrate a stream of known ids at a chosen projection.
    ///
    /// Forwards to [`Account::get_stream`]. The input ids are streamed
    /// so a long fetch pass backpressures cleanly; per-item results flow
    /// through `ItemOutcome<HydratedObject>` (`Succeeded` / `Failed` /
    /// `Uncertain`) on the returned stream. This is the primary entry
    /// for turning a broadcast `Change` into real content.
    pub fn get_stream(
        &self,
        account_id: &AccountId,
        ids: AccountStream<bifrost_types::ObjectId>,
        projection: bifrost_types::Projection,
    ) -> Result<AccountStream<SyncEvent<ItemOutcome<bifrost_types::HydratedObject>>>, Error> {
        Ok(self.live_account(account_id)?.get_stream(ids, projection))
    }

    /// Hydrate a single message at a specific projection level.
    ///
    /// Forwards to [`Account::message_hydrate`]. Prefer this over a
    /// one-id `get_stream` when the consumer wants a parsed `Message`
    /// rather than the raw projection envelope. Protocol errors surface
    /// as `Error::Account`; an unattached account yields
    /// `AccountNotAttached`.
    pub async fn message_hydrate(
        &self,
        account_id: &AccountId,
        message: bifrost_types::ObjectId,
        projection: bifrost_types::HydrationProjection,
    ) -> Result<bifrost_types::Message, Error> {
        Ok(self
            .live_account(account_id)?
            .message_hydrate(message, projection)
            .await?)
    }

    /// Hydrate every message in a thread.
    ///
    /// Forwards to [`Account::thread_hydrate`]. The protocol crate picks
    /// the threading primitive for its backend (JMAP `Thread/get`, IMAP
    /// `THREAD REFERENCES`, Gmail `threads.get`, Graph conversation API).
    pub async fn thread_hydrate(
        &self,
        account_id: &AccountId,
        thread: bifrost_types::ThreadId,
    ) -> Result<bifrost_types::ThreadHydration, Error> {
        Ok(self
            .live_account(account_id)?
            .thread_hydrate(thread)
            .await?)
    }

    /// Open a message's assembled RFC822 octets for streaming download.
    ///
    /// Forwards to [`Account::open_raw_rfc822`]. Yields the verbatim
    /// server-assembled MIME bytes (never re-encoded), gated by
    /// `capabilities().pim_methods.open_raw_rfc822`; an account whose
    /// flag is false terminates the stream with `Unsupported`.
    pub fn open_raw_rfc822(
        &self,
        account_id: &AccountId,
        message: bifrost_types::ObjectId,
    ) -> Result<AccountStream<SyncEvent<bytes::Bytes>>, Error> {
        Ok(self.live_account(account_id)?.open_raw_rfc822(message))
    }

    /// Open a blob for streaming download.
    ///
    /// Forwards to [`Account::open_blob`]. Used to pull attachment or
    /// inline-part bytes referenced by a hydrated object's `blobs`.
    pub fn open_blob(
        &self,
        account_id: &AccountId,
        handle: bifrost_types::BlobHandle,
    ) -> Result<AccountStream<SyncEvent<bytes::Bytes>>, Error> {
        Ok(self.live_account(account_id)?.open_blob(handle))
    }

    /// Open a byte range of a blob for streaming download.
    ///
    /// Forwards to [`Account::open_blob_range`]. Errors with
    /// `Unsupported(OpenBlobRange)` on the stream where the blob's
    /// capability flag is false.
    pub fn open_blob_range(
        &self,
        account_id: &AccountId,
        handle: bifrost_types::BlobHandle,
        range: bifrost_types::ByteRange,
    ) -> Result<AccountStream<SyncEvent<bytes::Bytes>>, Error> {
        Ok(self
            .live_account(account_id)?
            .open_blob_range(handle, range))
    }

    /// Host an over-limit attachment in the account's cloud drive and
    /// return a shareable link, in one call.
    ///
    /// Forwards to [`Account::host_attachment`]. Gated by
    /// `capabilities().pim_methods.host_attachment`; an account whose
    /// flag is false resolves the future with
    /// `Unsupported(HostAttachment)`. The synchronous `live_account`
    /// resolution surfaces `AccountNotAttached` here, while the returned
    /// `AccountFuture` surfaces the trait's `AccountError` when awaited.
    pub fn host_attachment(
        &self,
        account_id: &AccountId,
        bytes: bytes::Bytes,
        meta: bifrost_types::CloudUploadMeta,
    ) -> Result<AccountFuture<Result<bifrost_types::HostedAttachment, AccountError>>, Error> {
        Ok(self.live_account(account_id)?.host_attachment(bytes, meta))
    }

    // ---------- mutation passthrough (direct) ----------
    //
    // The write-side companion to the read-only hydration cluster above:
    // the single-object conveniences, membership primitives, and
    // container CRUD a consumer needs to drive object-level mutations
    // against the live attached connection without holding the
    // engine-private `Arc<dyn Account>`. Like the read cluster, every
    // method resolves through `live_account` (so a mutation issued after
    // a reopen runs against the freshly-installed connection, never a
    // stale snapshot the consumer cached) and forwards to the matching
    // `Account` method 1:1, inventing no new semantics.
    //
    // These are DIRECT - one wire op each - and deliberately bypass the
    // idempotency / read-back / recovery pipeline. That pipeline guards
    // the *volume* mutations (`bulk_set_flags`, `bulk_move`,
    // `bulk_destroy`), where a partial-apply replay across a retry would
    // corrupt state and the read-back guard earns its keep. A
    // single-object convenience carries no batch idempotency key and is
    // cheap to reissue, so routing it through the pipeline would buy
    // nothing. An unattached account yields `Error::AccountNotAttached`
    // up front; the forwarded `Account` future is `'static` and captures
    // its own `Arc` clones, so it outlives the short-lived handle
    // resolved per call.

    /// Toggle the starred / flagged bit on `target`. Forwards to
    /// [`Account::set_starred`].
    pub async fn set_starred(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        starred: bool,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .set_starred(target, starred)
            .await?)
    }

    /// Set or clear `target`'s read state. Forwards to
    /// [`Account::set_read`].
    pub async fn set_read(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        is_read: bool,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .set_read(target, is_read)
            .await?)
    }

    /// Apply `label` to `target`. Forwards to [`Account::apply_label`],
    /// which dispatches by `label.provenance` to the right primitive.
    pub async fn apply_label(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        label: bifrost_types::Label,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .apply_label(target, label)
            .await?)
    }

    /// Remove `label` from `target`. Forwards to
    /// [`Account::remove_label`].
    pub async fn remove_label(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        label: bifrost_types::Label,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .remove_label(target, label)
            .await?)
    }

    /// Mark `message` as replied. Forwards to [`Account::mark_replied`].
    pub async fn mark_replied(
        &self,
        account_id: &AccountId,
        message: bifrost_types::ObjectId,
    ) -> Result<(), Error> {
        Ok(self.live_account(account_id)?.mark_replied(message).await?)
    }

    /// Mark `message` as forwarded. Forwards to
    /// [`Account::mark_forwarded`].
    pub async fn mark_forwarded(
        &self,
        account_id: &AccountId,
        message: bifrost_types::ObjectId,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .mark_forwarded(message)
            .await?)
    }

    /// Persist that an MDN (read receipt) was dispatched for `message`.
    /// Forwards to [`Account::mark_mdn_sent`].
    pub async fn mark_mdn_sent(
        &self,
        account_id: &AccountId,
        message: bifrost_types::ObjectId,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .mark_mdn_sent(message)
            .await?)
    }

    /// Move a thread between containers. Forwards to
    /// [`Account::move_thread`]; the protocol crate composes the
    /// add-then-remove pair against its own `Arc`-shaped handle.
    pub async fn move_thread(
        &self,
        account_id: &AccountId,
        thread: bifrost_types::ThreadId,
        target: bifrost_types::ContainerId,
        source: Option<bifrost_types::ContainerId>,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .move_thread(thread, target, source)
            .await?)
    }

    /// Move a thread to Trash, or delete-permanently if already in Trash.
    /// Forwards to [`Account::delete_thread`].
    pub async fn delete_thread(
        &self,
        account_id: &AccountId,
        thread: bifrost_types::ThreadId,
        current: Option<bifrost_types::ContainerId>,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .delete_thread(thread, current)
            .await?)
    }

    /// Add `target` to `container`. Forwards to
    /// [`Account::add_to_container`].
    pub async fn add_to_container(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        container: bifrost_types::ContainerId,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .add_to_container(target, container)
            .await?)
    }

    /// Remove `target` from `container`. Forwards to
    /// [`Account::remove_from_container`]. Providers without a symmetric
    /// remove (Graph) surface `Unsupported(RemoveFromContainer)`.
    pub async fn remove_from_container(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        container: bifrost_types::ContainerId,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .remove_from_container(target, container)
            .await?)
    }

    /// Set or clear `target`'s read state. The membership-primitive
    /// spelling; forwards to [`Account::set_is_read`].
    pub async fn set_is_read(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        is_read: bool,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .set_is_read(target, is_read)
            .await?)
    }

    /// Set or clear a single IMAP / JMAP keyword on `target`. Forwards to
    /// [`Account::set_keyword`].
    pub async fn set_keyword(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        keyword: String,
        value: bool,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .set_keyword(target, keyword, value)
            .await?)
    }

    /// Set or clear membership in a Gmail-style label. Forwards to
    /// [`Account::set_label_membership`].
    pub async fn set_label_membership(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        label: bifrost_types::ContainerId,
        value: bool,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .set_label_membership(target, label, value)
            .await?)
    }

    /// Set or clear a Graph category on `target`. Forwards to
    /// [`Account::set_category`].
    pub async fn set_category(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        category: String,
        value: bool,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .set_category(target, category, value)
            .await?)
    }

    /// Set `target`'s importance to exactly `level`. Forwards to
    /// [`Account::set_importance`].
    pub async fn set_importance(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        level: bifrost_types::Importance,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .set_importance(target, level)
            .await?)
    }

    /// Enumerate an account's containers (folders, labels, mailboxes).
    /// Forwards 1:1 to [`Account::containers_list`].
    ///
    /// The read companion to the container-mutation cluster below: like
    /// every passthrough it resolves through `live_account` so a list
    /// issued after a `RestartAccount` reopen runs against the freshly
    /// installed connection, and yields `Error::AccountNotAttached` up
    /// front when no live slot exists. Read-only, so it does not pass
    /// through the idempotency / read-back pipeline.
    pub async fn containers_list(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<bifrost_types::Container>, Error> {
        Ok(self.live_account(account_id)?.containers_list().await?)
    }

    /// Forwards to the account's provider category-definition surface.
    pub async fn category_definitions_list(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<bifrost_types::CategoryDefinition>, Error> {
        Ok(self
            .live_account(account_id)?
            .category_definitions_list()
            .await?)
    }

    /// Forwards to the account's provider reaction-read surface.
    pub async fn message_reactions(
        &self,
        account_id: &AccountId,
        ids: &[bifrost_types::ObjectId],
    ) -> Result<bifrost_types::BatchOutcome<bifrost_types::MessageReactionState>, Error> {
        Ok(self
            .live_account(account_id)?
            .message_reactions(ids)
            .await?)
    }

    /// Create a new container of `kind` named `name` under `parent`.
    /// Forwards to [`Account::container_create`]; returns the
    /// engine-facing id. `style` carries an optional initial color
    /// (Gmail labels only; ignored by folder-shaped protocols).
    pub async fn container_create(
        &self,
        account_id: &AccountId,
        kind: bifrost_types::ContainerKind,
        name: String,
        parent: Option<bifrost_types::ContainerId>,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> Result<bifrost_types::ContainerId, Error> {
        Ok(self
            .live_account(account_id)?
            .container_create(kind, name, parent, style)
            .await?)
    }

    /// Rename a container. Forwards to [`Account::container_rename`].
    /// `style`, when `Some`, also recolors the container (Gmail labels
    /// only; ignored by folder-shaped protocols).
    pub async fn container_rename(
        &self,
        account_id: &AccountId,
        container: bifrost_types::ContainerId,
        name: String,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .container_rename(container, name, style)
            .await?)
    }

    /// Move a container under a new parent. Forwards to
    /// [`Account::container_move`].
    pub async fn container_move(
        &self,
        account_id: &AccountId,
        container: bifrost_types::ContainerId,
        new_parent: Option<bifrost_types::ContainerId>,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .container_move(container, new_parent)
            .await?)
    }

    /// Delete a container. Forwards to [`Account::container_delete`].
    pub async fn container_delete(
        &self,
        account_id: &AccountId,
        container: bifrost_types::ContainerId,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .container_delete(container)
            .await?)
    }

    // ---------- compose passthrough (direct) ----------
    //
    // The send / draft / scheduled-send companion to the mutation
    // passthrough cluster above. Same discipline: every method resolves
    // through `live_account` (so a compose issued after a reopen runs
    // against the freshly-installed connection), forwards 1:1 to the
    // matching `Account` method, invents no new semantics, and bails
    // `AccountNotAttached` up front. The forwarded `Account` future is
    // `'static` and captures its own `Arc` clones, so it outlives the
    // short-lived handle resolved per call. Capability gating
    // (`scheduled_send`, `send_as`) stays in the protocol crate exactly
    // as the direct call would; a consumer reads `account_capabilities`
    // below to decide whether to dispatch before paying the round trip.

    /// Send an RFC 5322 message. Forwards to [`Account::send_message`].
    pub async fn send_message(
        &self,
        account_id: &AccountId,
        request: bifrost_types::SendRequest,
    ) -> Result<bifrost_types::ObjectId, Error> {
        Ok(self.live_account(account_id)?.send_message(request).await?)
    }

    /// Send pre-assembled RFC 5322 / RFC 8098 octets verbatim. Forwards to
    /// [`Account::send_raw_message`]; the MDN submission lane.
    pub async fn send_raw_message(
        &self,
        account_id: &AccountId,
        raw: bytes::Bytes,
        save_to_sent: Option<bool>,
    ) -> Result<bifrost_types::ObjectId, Error> {
        Ok(self
            .live_account(account_id)?
            .send_raw_message(raw, save_to_sent)
            .await?)
    }

    /// Create a new draft. Forwards to [`Account::draft_create`].
    pub async fn draft_create(
        &self,
        account_id: &AccountId,
        patch: bifrost_types::DraftPatch,
    ) -> Result<bifrost_types::DraftHandle, Error> {
        Ok(self.live_account(account_id)?.draft_create(patch).await?)
    }

    /// Update an existing draft. Forwards to [`Account::draft_update`].
    pub async fn draft_update(
        &self,
        account_id: &AccountId,
        draft: bifrost_types::DraftHandle,
        patch: bifrost_types::DraftPatch,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .draft_update(draft, patch)
            .await?)
    }

    /// Discard (delete) a draft without sending. Forwards to
    /// [`Account::draft_discard`].
    pub async fn draft_discard(
        &self,
        account_id: &AccountId,
        draft: bifrost_types::DraftHandle,
    ) -> Result<(), Error> {
        Ok(self.live_account(account_id)?.draft_discard(draft).await?)
    }

    /// Convert a draft into a sent message. Forwards to
    /// [`Account::draft_send`].
    pub async fn draft_send(
        &self,
        account_id: &AccountId,
        draft: bifrost_types::DraftHandle,
    ) -> Result<bifrost_types::ObjectId, Error> {
        Ok(self.live_account(account_id)?.draft_send(draft).await?)
    }

    /// Cancel a previously scheduled send. Forwards to
    /// [`Account::cancel_scheduled_send`]; gated by
    /// `capabilities().pim_methods.scheduled_send` at the protocol layer.
    pub async fn cancel_scheduled_send(
        &self,
        account_id: &AccountId,
        handle: bifrost_types::ObjectId,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .cancel_scheduled_send(handle)
            .await?)
    }

    /// Reschedule a previously scheduled send to a new instant. Forwards
    /// to [`Account::reschedule_send`]; returns the (possibly new)
    /// submission id.
    pub async fn reschedule_send(
        &self,
        account_id: &AccountId,
        handle: bifrost_types::ObjectId,
        scheduled: std::time::SystemTime,
    ) -> Result<bifrost_types::ObjectId, Error> {
        Ok(self
            .live_account(account_id)?
            .reschedule_send(handle, scheduled)
            .await?)
    }

    // ---------- contact passthrough (direct) ----------
    //
    // The contact companion to the container / compose passthrough
    // clusters above. Same discipline: every method resolves through
    // `live_account` (so a call issued after a `RestartAccount` reopen
    // runs against the freshly-installed connection, never a stale
    // snapshot the consumer cached), forwards 1:1 to the matching
    // `Account` method, invents no new semantics, and yields
    // `Error::AccountNotAttached` up front when no live slot exists. The
    // forwarded `Account` future is `'static` and captures its own `Arc`
    // clones, so it outlives the short-lived handle resolved per call.
    // The address-book / contact reads are read-only, and the single
    // contact mutations are direct one-wire-op conveniences, so - like the
    // container / compose clusters - they deliberately bypass the
    // idempotency / read-back / recovery pipeline that guards the volume
    // mutations. Capability gating (`pim_methods.directory_search`, etc.)
    // stays in the protocol crate; a consumer reads `account_capabilities`
    // to decide whether to dispatch before paying the round trip.

    /// List an account's address books / contact folders. Forwards 1:1 to
    /// [`Account::address_books_list`].
    pub async fn address_books_list(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<bifrost_types::AddressBook>, Error> {
        Ok(self.live_account(account_id)?.address_books_list().await?)
    }

    /// List contacts, optionally scoped to one address book, resuming from
    /// a prior `Page::next_cursor`. Forwards to [`Account::contacts_list`].
    pub async fn contacts_list(
        &self,
        account_id: &AccountId,
        address_book: Option<bifrost_types::AddressBookId>,
        page_cursor: Option<Vec<u8>>,
    ) -> Result<bifrost_types::Page<bifrost_types::ContactCard>, Error> {
        Ok(self
            .live_account(account_id)?
            .contacts_list(address_book, page_cursor)
            .await?)
    }

    /// Fetch one contact card by engine-facing id. Forwards to
    /// [`Account::contact_get`].
    pub async fn contact_get(
        &self,
        account_id: &AccountId,
        contact: bifrost_types::ContactId,
    ) -> Result<bifrost_types::ContactCard, Error> {
        Ok(self.live_account(account_id)?.contact_get(contact).await?)
    }

    /// Create one contact card. Forwards to [`Account::contact_create`].
    pub async fn contact_create(
        &self,
        account_id: &AccountId,
        contact: bifrost_types::ContactCreate,
    ) -> Result<bifrost_types::ContactId, Error> {
        Ok(self
            .live_account(account_id)?
            .contact_create(contact)
            .await?)
    }

    /// Partially update one contact card. Forwards to
    /// [`Account::contact_update`].
    pub async fn contact_update(
        &self,
        account_id: &AccountId,
        contact: bifrost_types::ContactId,
        patch: bifrost_types::ContactPatch,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .contact_update(contact, patch)
            .await?)
    }

    /// Delete one contact card. Forwards to [`Account::contact_delete`].
    pub async fn contact_delete(
        &self,
        account_id: &AccountId,
        contact: bifrost_types::ContactId,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .contact_delete(contact)
            .await?)
    }

    /// Search the organization directory (Global Address List). Forwards
    /// to [`Account::directory_search`]; gated by
    /// `capabilities().pim_methods.directory_search` at the protocol layer.
    /// The argument order mirrors the trait: `(query, limit, page_cursor)`.
    pub async fn directory_search(
        &self,
        account_id: &AccountId,
        query: String,
        limit: Option<u32>,
        page_cursor: Option<Vec<u8>>,
    ) -> Result<bifrost_types::Page<bifrost_types::DirectoryCard>, Error> {
        Ok(self
            .live_account(account_id)?
            .directory_search(query, limit, page_cursor)
            .await?)
    }

    /// List the mail-enabled organization-directory groups the account's
    /// mailbox belongs to. Forwards 1:1 to
    /// [`Account::directory_groups_list`]; gated by
    /// `capabilities().pim_methods.directory_groups_list` at the protocol
    /// layer.
    pub async fn directory_groups_list(
        &self,
        account_id: &AccountId,
        page_cursor: Option<Vec<u8>>,
    ) -> Result<bifrost_types::Page<bifrost_types::DirectoryGroup>, Error> {
        Ok(self
            .live_account(account_id)?
            .directory_groups_list(page_cursor)
            .await?)
    }

    /// Expand one directory group to its user members (transitive,
    /// provider-side). Forwards 1:1 to
    /// [`Account::directory_group_expand`]; gated by
    /// `capabilities().pim_methods.directory_group_expand` at the
    /// protocol layer.
    pub async fn directory_group_expand(
        &self,
        account_id: &AccountId,
        group: bifrost_types::DirectoryGroupId,
        page_cursor: Option<Vec<u8>>,
    ) -> Result<bifrost_types::Page<bifrost_types::DirectoryGroupMember>, Error> {
        Ok(self
            .live_account(account_id)?
            .directory_group_expand(group, page_cursor)
            .await?)
    }

    /// List an account's server-side filter rules or scripts. Forwards
    /// 1:1 to [`Account::filters_list`]; the supported model is
    /// advertised through `capabilities().filter_rule_shape` and
    /// per-method support through `capabilities().pim_methods`.
    pub async fn filters_list(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<bifrost_types::ServerFilter>, Error> {
        Ok(self.live_account(account_id)?.filters_list().await?)
    }

    /// Create one server-side filter rule or script. Forwards to
    /// [`Account::filter_create`].
    pub async fn filter_create(
        &self,
        account_id: &AccountId,
        filter: bifrost_types::ServerFilterCreate,
    ) -> Result<bifrost_types::ServerFilterId, Error> {
        Ok(self.live_account(account_id)?.filter_create(filter).await?)
    }

    /// Partially update one server-side filter rule or script. Forwards
    /// to [`Account::filter_update`].
    pub async fn filter_update(
        &self,
        account_id: &AccountId,
        filter: bifrost_types::ServerFilterId,
        patch: bifrost_types::ServerFilterPatch,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .filter_update(filter, patch)
            .await?)
    }

    /// Delete one server-side filter rule or script. Forwards to
    /// [`Account::filter_delete`].
    pub async fn filter_delete(
        &self,
        account_id: &AccountId,
        filter: bifrost_types::ServerFilterId,
    ) -> Result<(), Error> {
        Ok(self.live_account(account_id)?.filter_delete(filter).await?)
    }

    /// Validate a server-side filter payload without storing it. Forwards
    /// to [`Account::filter_validate`].
    pub async fn filter_validate(
        &self,
        account_id: &AccountId,
        filter: bifrost_types::ServerFilterCreate,
    ) -> Result<bifrost_types::FilterValidation, Error> {
        Ok(self
            .live_account(account_id)?
            .filter_validate(filter)
            .await?)
    }

    /// List the account's sending identities. Forwards 1:1 to
    /// [`Account::identities_list`]; per-method support is advertised
    /// through `capabilities().pim_methods`.
    pub async fn identities_list(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<bifrost_types::Identity>, Error> {
        Ok(self.live_account(account_id)?.identities_list().await?)
    }

    /// Partially update one sending identity. Forwards to
    /// [`Account::identity_update`].
    pub async fn identity_update(
        &self,
        account_id: &AccountId,
        identity: bifrost_types::IdentityId,
        patch: bifrost_types::IdentityPatch,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .identity_update(identity, patch)
            .await?)
    }

    /// Read the vacation responder config, when supported. Forwards to
    /// [`Account::vacation_get`].
    pub async fn vacation_get(
        &self,
        account_id: &AccountId,
    ) -> Result<Option<bifrost_types::VacationConfig>, Error> {
        Ok(self.live_account(account_id)?.vacation_get().await?)
    }

    /// Replace the vacation responder config. Forwards to
    /// [`Account::vacation_set`].
    pub async fn vacation_set(
        &self,
        account_id: &AccountId,
        config: bifrost_types::VacationConfig,
    ) -> Result<(), Error> {
        Ok(self.live_account(account_id)?.vacation_set(config).await?)
    }

    /// Read the storage quota readout, when supported. Forwards to
    /// [`Account::quota_get`].
    pub async fn quota_get(
        &self,
        account_id: &AccountId,
    ) -> Result<Option<bifrost_types::QuotaInfo>, Error> {
        Ok(self.live_account(account_id)?.quota_get().await?)
    }

    /// Read the attached account's capabilities snapshot, as stashed at
    /// attach time.
    ///
    /// Non-async: the engine clones `capabilities()` onto the slot during
    /// `attach`, so this is an in-memory read, not a wire call. Lets a
    /// consumer branch on `pim_methods.{scheduled_send, send_as}` (and the
    /// rest) declaratively before dispatching a compose op, rather than
    /// firing the call and translating an `Unsupported` back into a
    /// disabled affordance. Errors with `AccountNotAttached` when no slot
    /// exists for `account_id`.
    pub fn account_capabilities(
        &self,
        account_id: &AccountId,
    ) -> Result<AccountCapabilities, Error> {
        let slot = self
            .accounts
            .get(account_id)
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        slot.capabilities
            .read()
            .map(|capabilities| capabilities.clone())
            .map_err(|_| Error::Other("capabilities lock poisoned".into()))
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
        discover_scopes_from(account).await
    }

    async fn establish_one(
        &self,
        account_id: &AccountId,
        account: &dyn Account,
        scope: CursorScope,
        cursors: Arc<CursorRegistry>,
    ) -> Result<InitialScope, Error> {
        // Check the store first - resume path.
        //
        // A schema-incompatible envelope is healed HERE rather than
        // handed to the recovery translator. The running-account path
        // (`run_establish`) can return a typed `AccountError` because a
        // reopen listener is alive to receive
        // `Engine(SchemaIncompatible)` and run the schema-clear loop;
        // at attach that listener does not exist yet - it is spawned
        // further down `attach_inner`. Returning an error here instead
        // would fail the whole attach with nothing left running to
        // clear the undecodable row, so the account would refuse to
        // attach on every subsequent start with no recovery path.
        // Dropping the row and re-establishing this scope is exactly
        // what `handle_schema_incompatible` would have done, scoped to
        // the one cursor that cannot be read. (sync-D10)
        match self.checkpoints.get_change_cursor(account_id, &scope).await {
            Ok(Some(existing)) => {
                cursors.put(existing);
                return Ok(InitialScope::Ready);
            }
            Ok(None) => {}
            Err(Error::SchemaIncompatible) => {
                tracing::warn!(
                    target: "bifrost.sync.attach",
                    account = ?account_id,
                    scope = ?scope,
                    "schema-incompatible cursor envelope; clearing the row and re-establishing the scope"
                );
                if let Err(err) = self
                    .checkpoints
                    .delete_change_cursor(account_id, &scope)
                    .await
                {
                    // Non-fatal: re-establishment overwrites the row on
                    // success anyway. Log and keep going rather than
                    // failing the attach on a store hiccup.
                    tracing::warn!(
                        target: "bifrost.sync.attach",
                        account = ?account_id,
                        scope = ?scope,
                        error = %err,
                        "delete_change_cursor failed while clearing an unreadable envelope"
                    );
                }
            }
            Err(other) => return Err(other),
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

async fn discover_scopes_from(account: &dyn Account) -> Result<Vec<CursorScope>, Error> {
    let mut out = Vec::new();
    let mut stream = account.discover_cursor_scopes();
    while let Some(event) = stream.next().await {
        match event {
            SyncEvent::Batch(batch) => out.extend(batch.items),
            SyncEvent::Done(_) => break,
            SyncEvent::Terminated(err) => return Err(Error::Account(err)),
            SyncEvent::Progress(_) | SyncEvent::Warning(_) => {}
            _ => {}
        }
    }
    Ok(out)
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
///
/// Resume: the in-memory `BackfillRegistry` is wiped on detach, so the
/// only durable record of backfill progress is the consumer-acked
/// `BackfillCheckpoint` in the `CheckpointStore`. Before walking an
/// open-ended page scope the orchestrator reads that checkpoint back via
/// `get_backfill` and either skips a scope whose final short page is
/// already persisted (the steady-state delta case - it must not re-walk
/// at all) or resumes after the furthest durably-checkpointed full page
/// instead of re-paginating from page 0 on every re-attach.
#[allow(clippy::too_many_arguments)]
async fn run_backfill_orchestrator(
    account: Arc<ArcSwap<Arc<dyn Account>>>,
    account_id: AccountId,
    cursors: Arc<CursorRegistry>,
    live: Arc<LiveSupersedes>,
    store: Arc<DynCheckpointStore>,
    registry: Arc<BackfillRegistry>,
    shutdown: CancellationToken,
    changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
    subscriber_notify: Arc<Notify>,
    config: BackfillConfig,
    control: SyncControl,
) {
    // Cold-start backfill pages broadcast onto the per-account channel
    // during `attach`, but a consumer can only call
    // `account_changes_stream` after `attach` inserts the slot into
    // `self.accounts`. A `tokio::broadcast` receiver that subscribes
    // late starts at the ring's tail and never sees values sent before
    // it joined (the slot's sentinel receiver keeps `receiver_count()`
    // at 1, so the send "succeeds" yet lands in front of no real
    // reader). For a `Ready`-cursor account whose entire cold start
    // rides backfill - Gmail `CursorScope::Account`, JMAP
    // `CursorScope::Type(Email)` - that silently drops the initial
    // inventory page, so the consumer ingests zero objects. Park until a
    // real subscriber arrives, exactly as
    // `run_deferred_inventory_establishment` does for the fusion path,
    // so the first page is observed rather than raced away. (sync-N3)
    if let Some(tx) = &changes_tx
        && !wait_for_real_subscriber(tx, &subscriber_notify, &shutdown).await
    {
        return;
    }
    // Snapshot only Ready scopes. EstablishViaInventory scopes are
    // owned by the concurrent fusion worker, which broadcasts their
    // cold-start inventory while minting the cursor. Adding them to
    // this already-running plan after fusion would double-walk and
    // double-publish the same scope.
    let scopes = cursors.all_scopes();
    for scope in scopes {
        if shutdown.is_cancelled() {
            return;
        }
        let acc_arc = account.load_full();
        let acc: &dyn Account = acc_arc.as_ref().as_ref();
        match backfill_plan_for(acc, &scope, config) {
            BackfillPlan::Fixed(partitions) => {
                // Skip a scope whose backfill already completed on a prior
                // run. A fixed plan has no positional "resume from here"
                // (its partitions are a known, finite set, not an open page
                // walk), so the durable signal is binary: the completion
                // marker is present (skip the whole plan) or it is not
                // (walk every partition; re-emitting acked pages is
                // idempotent, so a crash mid-plan simply re-walks).
                match store.get_backfill(&account_id, &scope).await {
                    Ok(opt) => {
                        if backfill_complete_recorded(opt.as_ref()) {
                            registry.mark(
                                account_id.clone(),
                                scope.clone(),
                                BackfillState::Completed,
                            );
                            continue;
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "bifrost.sync.backfill",
                            scope = ?scope,
                            error = %err,
                            "backfill resume read failed; re-walking all partitions"
                        );
                    }
                }
                registry.mark(account_id.clone(), scope.clone(), BackfillState::Running);
                let mut completed = true;
                let mut total_seen = 0_u64;
                for partition in partitions {
                    if shutdown.is_cancelled() {
                        return;
                    }
                    let Some(result) = run_backfill_partition_at_boundary(
                        &account,
                        scope.clone(),
                        partition,
                        &live,
                        changes_tx.clone(),
                        &control,
                        &shutdown,
                    )
                    .await
                    else {
                        return;
                    };
                    match result {
                        Ok(outcome) => {
                            total_seen = total_seen.saturating_add(outcome.seen);
                        }
                        Err(err) => {
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
                }
                // Persist a durable completion marker through the same
                // consumer-ack path the page batches use. It is ordered
                // behind every page, so a crash before its ack re-walks
                // instead of recording a false completion.
                if completed
                    && !emit_backfill_complete(
                        changes_tx.as_ref(),
                        &scope,
                        total_seen,
                        &control,
                        &shutdown,
                    )
                    .await
                {
                    return;
                }
                registry.mark(
                    account_id.clone(),
                    scope.clone(),
                    if completed {
                        BackfillState::Completed
                    } else {
                        BackfillState::Pending
                    },
                );
            }
            BackfillPlan::OpenPages { chunk } => {
                // Resume from durable progress instead of page 0. The
                // persisted checkpoint is consumer-acked, so anything it
                // covers is safe to skip; we never skip a window the consumer
                // has not durably persisted.
                let mut from = 0_u32;
                match store.get_backfill(&account_id, &scope).await {
                    Ok(opt) => match open_pages_resume(opt.as_ref()) {
                        OpenPagesResume::Skip => {
                            registry.mark(
                                account_id.clone(),
                                scope.clone(),
                                BackfillState::Completed,
                            );
                            continue;
                        }
                        OpenPagesResume::ResumeFrom(position) => from = position,
                    },
                    Err(err) => {
                        // A read failure is not authoritative; fall back to a
                        // full walk rather than risk skipping unpersisted
                        // pages.
                        tracing::warn!(
                            target: "bifrost.sync.backfill",
                            scope = ?scope,
                            error = %err,
                            "backfill resume read failed; re-walking from page 0"
                        );
                    }
                }
                registry.mark(account_id.clone(), scope.clone(), BackfillState::Running);
                let mut completed = true;
                let mut total_seen = 0_u64;
                loop {
                    if shutdown.is_cancelled() {
                        return;
                    }
                    let to = from.saturating_add(chunk);
                    let partition = InventoryPartition::Page { from, to };
                    let Some(result) = run_backfill_partition_at_boundary(
                        &account,
                        scope.clone(),
                        partition,
                        &live,
                        changes_tx.clone(),
                        &control,
                        &shutdown,
                    )
                    .await
                    else {
                        return;
                    };
                    match result {
                        Ok(outcome) => {
                            // Terminate only on a genuinely empty page, never
                            // on a merely short one. A partition stream whose
                            // server caps a page below the requested `chunk`
                            // (e.g. a JMAP Email/query cap below the window
                            // width) returns fewer entries than asked for;
                            // treating that as exhaustion silently drops every
                            // later page. The Page partition stream fills its
                            // window by paging internally past any such cap, so
                            // an empty window is the unambiguous end-of-inventory
                            // boundary - a short window means the final partial
                            // page, and the next pass returns empty.
                            if outcome.seen == 0 {
                                break;
                            }
                            total_seen = total_seen.saturating_add(outcome.seen);
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
                if completed
                    && !emit_backfill_complete(
                        changes_tx.as_ref(),
                        &scope,
                        total_seen,
                        &control,
                        &shutdown,
                    )
                    .await
                {
                    return;
                }
                registry.mark(
                    account_id.clone(),
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

#[allow(clippy::too_many_arguments)]
async fn run_backfill_partition_at_boundary(
    account: &Arc<ArcSwap<Arc<dyn Account>>>,
    scope: CursorScope,
    partition: InventoryPartition,
    live: &Arc<LiveSupersedes>,
    changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
    control: &SyncControl,
    shutdown: &CancellationToken,
) -> Option<Result<crate::backfill::BackfillPartitionOutcome, Error>> {
    loop {
        if !control.wait_until_running(shutdown).await {
            return None;
        }
        let current = account.load_full();
        let result = BackfillRunner::run_partition(
            current.as_ref().as_ref(),
            scope.clone(),
            partition.clone(),
            live.as_ref(),
            changes_tx.clone(),
            crate::cursor::ENGINE_VERSION,
            Some(control),
        )
        .await;
        if matches!(result, Err(Error::Paused)) {
            continue;
        }
        return Some(result);
    }
}

/// Resume decision for an open-ended page scope, derived purely from the
/// durably-persisted (consumer-acked) backfill checkpoint.
#[derive(Debug, PartialEq, Eq)]
enum OpenPagesResume {
    /// Inventory was exhausted on a prior run; do not walk at all.
    Skip,
    /// Begin (or resume) the page walk at this position.
    ResumeFrom(u32),
}

/// True if the persisted checkpoint is the durable completion marker: a
/// prior run walked the whole scope to exhaustion and the consumer acked
/// it. The single skip-on-complete signal shared by fixed and open-ended
/// plans; `get_backfill` only ever returns consumer-acked checkpoints, so
/// a marker means every page the consumer needed was durably persisted.
fn backfill_complete_recorded(checkpoint: Option<&BackfillCheckpoint>) -> bool {
    checkpoint
        .is_some_and(|ck| crate::backfill::partitioner::is_completion_partition(&ck.partition))
}

/// Map the persisted backfill checkpoint to a resume decision.
///
/// - The completion sentinel means a prior run reached exhaustion and the
///   consumer acked it: skip entirely.
/// - A short final `page:F:T` (`items_done < T - F`) means inventory ran
///   out inside that window even though no completion marker landed (e.g.
///   the consumer never acked it): still exhausted, so skip.
/// - A full `page:F:T` means there may be more after it: resume at `T`.
/// - No checkpoint, or an unrecognised partition kind, starts fresh at 0.
///
/// Resume never skips a window the consumer has not durably persisted,
/// because `get_backfill` only ever returns consumer-acked checkpoints.
fn open_pages_resume(checkpoint: Option<&BackfillCheckpoint>) -> OpenPagesResume {
    if backfill_complete_recorded(checkpoint) {
        return OpenPagesResume::Skip;
    }
    let Some(checkpoint) = checkpoint else {
        return OpenPagesResume::ResumeFrom(0);
    };
    if let Some((from, to)) =
        crate::backfill::partitioner::parse_page_partition(&checkpoint.partition)
    {
        let width = u64::from(to.saturating_sub(from));
        if checkpoint.progress.items_done < width {
            return OpenPagesResume::Skip;
        }
        return OpenPagesResume::ResumeFrom(to);
    }
    OpenPagesResume::ResumeFrom(0)
}

/// Broadcast a durable backfill-completion marker for a scope. The marker
/// is a synthetic empty `Batch` carrying a `BackfillCheckpoint` on the
/// `completion_partition` sentinel key; it flows through the consumer-ack
/// path exactly like a page batch, so the store only records completion
/// after the consumer has durably persisted every page. A crash before the
/// marker is acked therefore re-walks rather than recording a false "done".
///
/// `items_done` is set one past `total_seen` so the marker strictly wins
/// `get_backfill`'s "latest by items_done" query regardless of how a store
/// breaks ties: every per-partition checkpoint records its observed count
/// (`<= total_seen`), so `total_seen + 1` is guaranteed larger and the
/// marker is the row returned on re-attach. The honest total rides in
/// `items_estimated`.
async fn emit_backfill_complete(
    changes_tx: Option<&broadcast::Sender<MultiplexerEvent>>,
    scope: &CursorScope,
    total_seen: u64,
    control: &SyncControl,
    shutdown: &CancellationToken,
) -> bool {
    let Some(tx) = changes_tx else {
        return true;
    };
    let _activity = loop {
        if !control.wait_until_running(shutdown).await {
            return false;
        }
        if let Some(activity) = control.begin_activity() {
            break activity;
        }
    };
    let marker = BackfillCheckpoint {
        scope: scope.clone(),
        partition: crate::backfill::partitioner::completion_partition(),
        progress_marker: None,
        progress: BackfillProgress {
            items_done: total_seen.saturating_add(1),
            items_estimated: Some(total_seen),
        },
        envelope_version: crate::cursor::ENGINE_VERSION,
    };
    let batch: Batch<bifrost_types::Change> = Batch {
        items: Vec::new(),
        page_boundary: PageBoundary::Final,
        server_latency: Duration::ZERO,
        bytes_in: 0,
        checkpoint: Some(Checkpoint::Backfill(marker.clone())),
    };
    let event = MultiplexerEvent {
        scope: scope.clone(),
        event: Arc::new(SyncEvent::Batch(batch)),
        checkpoint: Some(Checkpoint::Backfill(marker)),
    };
    // Register before publishing so a fast consumer ack cannot land
    // before the entry exists and leave it outstanding forever.
    let expected = event.checkpoint.clone();
    if let Some(expected) = &expected {
        control.expect_checkpoint(expected.clone());
    }
    let delivered = tx.send(event).unwrap_or(0);
    if delivered <= 1
        && let Some(expected) = &expected
    {
        control.retire_checkpoint(expected);
    }
    true
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
    account_control_tx: broadcast::Sender<AccountControl>,
    throttles: Arc<std::sync::Mutex<crate::recovery::ThrottleBucket>>,
    subscriber_notify: Arc<Notify>,
    boundary_tx: watch::Sender<crate::cancel::BoundaryRequest>,
    capabilities: Arc<std::sync::RwLock<AccountCapabilities>>,
    subscriptions: Arc<SubscriptionRegistry>,
    account_generation_tx: watch::Sender<u64>,
    reopen_lock: Arc<AsyncMutex<()>>,
    scopes: Vec<CursorScope>,
) {
    if !wait_for_real_subscriber(&changes_tx, &subscriber_notify, &shutdown).await {
        return;
    }
    for scope in scopes {
        if shutdown.is_cancelled() {
            return;
        }
        let outcome = loop {
            if !control.wait_until_running(&shutdown).await {
                return;
            }
            let acc_arc = account.load_full();
            let acc: &dyn Account = acc_arc.as_ref().as_ref();
            let fusion = crate::multiplexer::InventoryFusion {
                account_id: account_id.clone(),
                cursors: Arc::clone(&cursors),
                control: Some(control.clone()),
            };
            let result = fusion
                .run_with_broadcast(acc, scope.clone(), Some(changes_tx.clone()))
                .await;
            if matches!(result, Err(Error::Paused)) {
                continue;
            }
            break result;
        };
        match outcome {
            Ok(crate::multiplexer::FusionOutcome::Established) => {
                let current = account.load_full();
                if let Err(err) =
                    link_discovered_memberships(current.as_ref().as_ref(), &cursors).await
                {
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
                let ctx = RecoveryContext {
                    factory: &factory,
                    current: &account,
                    cursors: &cursors,
                    store: &store,
                    changes_tx: &changes_tx,
                    account_id: &account_id,
                    control: &control,
                    account_control_tx: &account_control_tx,
                    throttles: &throttles,
                    boundary_tx: &boundary_tx,
                    capabilities: &capabilities,
                    subscriptions: &subscriptions,
                    account_generation_tx: &account_generation_tx,
                    reopen_lock: &reopen_lock,
                };
                handle_account_error(&ctx, Some(scope.clone()), error).await;
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

/// Park until a real subscriber arrives on `changes_tx`.
///
/// The slot keeps a sentinel receiver alive so `receiver_count()`
/// stays at 1 until a consumer calls `account_changes_stream`. We use
/// the slot's `subscriber_notify` (fired by
/// `SyncEngine::account_changes_stream`) so this waits without
/// hot-polling. (sync-N3)
async fn wait_for_real_subscriber(
    changes_tx: &broadcast::Sender<MultiplexerEvent>,
    subscriber_notify: &Notify,
    shutdown: &CancellationToken,
) -> bool {
    loop {
        if changes_tx.receiver_count() > 1 {
            return true;
        }
        tokio::select! {
            () = shutdown.cancelled() => return false,
            () = subscriber_notify.notified() => {
                // Loop and re-check; the notification might have been
                // spurious or a subscriber may have left between the
                // notify and our check.
            }
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
                // Surface the structured error to the caller instead
                // of returning `Ok(())` with an empty membership index.
                // A silent return here left the push reconciler routing
                // hints to "every registered scope" because the index
                // was never populated. Callers log the error and may
                // route via `reopen_tx` for terminal / engine-action
                // classes; auth-lost etc. then escalates rather than
                // silently degrading routing for the session lifetime.
                return Err(Error::Account(err));
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
                // The batch is no longer in flight - the consumer took
                // delivery and the engine has done all it can - so it
                // must stop gating boundary waiters even though it
                // never became durable. Leaving it outstanding would
                // wedge every later pause / checkpoint_now on this
                // account. The consumer learns about the store failure
                // from `ack_checkpoint`'s own `Result`, below.
                control.retire_checkpoint(&req.checkpoint);
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

/// Shared context bundle used by every recovery-dispatch path. Folds
/// the per-account state most paths need into a single argument so
/// `handle_account_error`, the engine-directive arm, and the reopen
/// loop don't carry seven-positional argument lists.
pub(crate) struct RecoveryContext<'a> {
    pub factory: &'a Arc<dyn AccountFactory>,
    pub current: &'a Arc<ArcSwap<Arc<dyn Account>>>,
    pub cursors: &'a Arc<CursorRegistry>,
    pub store: &'a Arc<DynCheckpointStore>,
    pub changes_tx: &'a broadcast::Sender<MultiplexerEvent>,
    pub account_id: &'a AccountId,
    pub control: &'a SyncControl,
    pub account_control_tx: &'a broadcast::Sender<AccountControl>,
    pub throttles: &'a Arc<std::sync::Mutex<crate::recovery::ThrottleBucket>>,
    pub boundary_tx: &'a watch::Sender<crate::cancel::BoundaryRequest>,
    pub capabilities: &'a Arc<std::sync::RwLock<AccountCapabilities>>,
    pub subscriptions: &'a Arc<SubscriptionRegistry>,
    pub account_generation_tx: &'a watch::Sender<u64>,
    pub reopen_lock: &'a Arc<AsyncMutex<()>>,
}

/// Dispatch an `AccountError` to the engine's recovery machinery.
///
/// Routes through `plan_recovery` so the closed `RecoveryPlan` enum
/// drives the dispatch. The `scope` argument is the worker's
/// convenience-suggested scope: scope-bound directives use the
/// directive's own scope when present; account-wide directives ignore
/// it.
pub(crate) async fn handle_account_error(
    ctx: &RecoveryContext<'_>,
    scope: Option<CursorScope>,
    error: AccountError,
) {
    use crate::recovery::{RecoveryPlan, plan_recovery};
    // Clone so we can log structured fields after dispatch consumes
    // the value.
    let plan = plan_recovery(error.clone());
    match plan {
        RecoveryPlan::Retry(advice) => {
            // The reopen listener is a serialization point for real
            // engine directives. Record any shared throttle deadline,
            // but never sleep here: no failed operation is retried
            // after such a sleep, and blocking this task would hold
            // RestartScope / RestartAccount requests behind an inert
            // provider Retry-After delay.
            apply_throttle(ctx, &advice);
            tracing::debug!(
                target: "bifrost.sync.recovery",
                account = ?ctx.account_id,
                scope = ?scope,
                retry = ?advice,
                "retry recovery reached reopen listener; caller owns the retry"
            );
        }
        RecoveryPlan::Reconcile(advice) => {
            // The poll loop / push reconciler perform the actual probe;
            // if we reach this branch via the reopen listener their
            // next pass owns the reconcile. Surface the actions for
            // telemetry so dropped guidance is visible.
            log_reconcile_advice(ctx, scope.as_ref(), &error, &advice);
        }
        RecoveryPlan::Engine(directive) => {
            let _reopen_guard = ctx.reopen_lock.lock().await;
            handle_engine_directive(ctx, scope, directive, error).await;
        }
        RecoveryPlan::Terminal(fatal) => {
            // Broadcast already carried the terminating event. Emit a
            // `TelemetryView` structured log so dashboards pivot on
            // stable fields rather than `?debug` text.
            let err = fatal.as_ref();
            let view = err.telemetry_fields();
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?ctx.account_id,
                scope = ?scope,
                kind = view.kind_discriminant,
                message_key = view.message_key,
                recovery = view.recovery_discriminant,
                provider = ?view.provider,
                protocol = ?view.protocol,
                status = ?view.status,
                operation = ?view.operation,
                "terminal recovery; engine takes no automated action"
            );
        }
    }
}

/// Record any provider-documented throttle scope on the engine's
/// shared `ThrottleBucket`. `CurrentOperation` is a per-call hint and
/// never enters the bucket (the originating caller owns any inline
/// delay).
fn apply_throttle(ctx: &RecoveryContext<'_>, advice: &RetryAdvice) {
    let Some(scope) = advice.throttle_scope else {
        return;
    };
    let Some(hint) = advice.retry_hint else {
        return;
    };
    let now = std::time::SystemTime::now();
    let Some(key) = crate::recovery::throttle_key_for(scope, ctx.account_id, None, None, None)
    else {
        // CurrentOperation, or unresolvable identity; nothing to bucket.
        return;
    };
    let wait_until = hint.not_before(now);
    if let Ok(mut bucket) = ctx.throttles.lock() {
        bucket.record(key, wait_until);
    }
}

fn log_reconcile_advice(
    ctx: &RecoveryContext<'_>,
    scope: Option<&CursorScope>,
    error: &AccountError,
    advice: &ReconcileAdvice,
) {
    let actions: Vec<&'static str> = advice
        .guidance
        .actions
        .iter()
        .map(|a| match a {
            ReconcileAction::CheckTarget => "check-target",
            ReconcileAction::DedupeByClientId => "dedupe-by-client-id",
            // `ReconcileAction` is `#[non_exhaustive]`; new variants
            // are surfaced as a stable tag so dashboards don't lose
            // signal silently.
            _ => "unknown",
        })
        .collect();
    tracing::warn!(
        target: "bifrost.sync.changes",
        account = ?ctx.account_id,
        scope = ?scope,
        kind = ?error.kind(),
        message_key = error.message_key(),
        reason = ?advice.reason,
        actions = ?actions,
        "reconcile recovery reached reopen listener"
    );
}

async fn handle_engine_directive(
    ctx: &RecoveryContext<'_>,
    fallback_scope: Option<CursorScope>,
    directive: EngineDirective,
    error: AccountError,
) {
    // Match exhaustively over the current `EngineDirective` variants.
    // `EngineDirective` is `#[non_exhaustive]`; a new variant must
    // fail to compile here rather than silently route through a
    // catch-all to a generic warning. (sync-N1 / sync-D8.)
    match directive {
        EngineDirective::RestartScope(directive_scope) => {
            // Pass the directive's own scope to broadcast_warning so
            // the multiplexer event's `scope` matches the affected
            // scope - not the worker-suggested fallback. (sync-N4)
            restart_scope(ctx, directive_scope).await;
        }
        EngineDirective::DowngradeCapabilityForScope(directive_scope) => {
            broadcast_warning(
                ctx.changes_tx,
                Some(directive_scope.clone()),
                bifrost_types::Warning::user_safe(
                    bifrost_types::WarningKind::Other,
                    format!("scope capability downgraded: {directive_scope:?}"),
                )
                .with_protocol_detail(DiagnosticText::support_only(format!("{directive_scope:?}"))),
            );
            restart_scope(ctx, directive_scope).await;
        }
        EngineDirective::RestartAccount => {
            restart_account(ctx).await;
        }
        EngineDirective::DowngradeStrategy(downgrade) => {
            broadcast_warning(
                ctx.changes_tx,
                fallback_scope.clone(),
                bifrost_types::Warning::user_safe(
                    bifrost_types::WarningKind::StrategyDowngraded,
                    format!("downgraded sync strategy: {downgrade:?}"),
                )
                .with_protocol_detail(DiagnosticText::support_only(format!("{downgrade:?}"))),
            );
            restart_account(ctx).await;
            // If the originating error was scoped to a cursor, also
            // re-establish that scope so the downgrade takes effect
            // immediately rather than at the next poll.
            if let Some(ErrorScope::Cursor(scoped)) = error.scope() {
                restart_scope(ctx, scoped.clone()).await;
            }
        }
        EngineDirective::SchemaIncompatible => {
            handle_schema_incompatible(ctx).await;
        }
        EngineDirective::OperatorOverrideRequired { reason } => {
            // Auto-pause the account: the engine no longer drives work
            // for it until the consumer flips `AccountControl::Resume`.
            // The reason is bounded (`PauseReason::OperatorOverrideRequired`);
            // free-form reason text rides through the warning only.
            // (sync-D9)
            broadcast_warning(
                ctx.changes_tx,
                fallback_scope.clone(),
                bifrost_types::Warning::user_safe(
                    bifrost_types::WarningKind::OperatorAttentionNeeded,
                    reason.clone(),
                )
                .with_protocol_detail(DiagnosticText::support_only(reason)),
            );
            engine_pause(ctx, PauseReason::OperatorOverrideRequired);
        }
        EngineDirective::DisableScope(directive_scope) => {
            disable_scope(ctx, directive_scope).await;
        }
        // EngineDirective is #[non_exhaustive] from bifrost-types; new
        // variants land here unhandled and require explicit dispatch
        // before they ship. The fallback warns rather than silently
        // routing.
        other => {
            tracing::warn!(
                ?other,
                "unhandled engine directive; defaulting to no-op until dispatch is wired"
            );
        }
    }
}

async fn handle_schema_incompatible(ctx: &RecoveryContext<'_>) {
    // Stop trusting durable cursor envelopes. Clear every in-memory
    // cursor and delete every durable change cursor we know about,
    // then re-establish each from the current account handle. Failure
    // to re-establish a single scope escalates per-scope (sync-D7):
    // the account keeps running for the scopes that succeed.
    let scopes: Vec<CursorScope> = ctx.cursors.all_scopes();
    for s in &scopes {
        ctx.cursors.delete(s);
        if let Err(err) = ctx.store.delete_change_cursor(ctx.account_id, s).await {
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?ctx.account_id,
                scope = ?s,
                error = %err,
                "SchemaIncompatible: delete_change_cursor failed"
            );
        }
    }
    for s in scopes {
        re_establish_scope_with_backoff(ctx, s).await;
    }
}

/// Reopen-failure budget. The engine attempts three reopens; after the
/// third consecutive failure the account is paused with
/// `PauseReason::RetryBudgetExhausted` and the last error is emitted
/// verbatim as `SyncEvent::Terminated`. (sync-D6)
const REOPEN_RETRY_BUDGET: u32 = 3;

const REOPEN_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const REOPEN_BACKOFF_CAP: Duration = Duration::from_secs(5 * 60);

/// Restart a single scope with exponential backoff and a retry budget.
/// After three consecutive re-establishment failures the scope is
/// abandoned, a `SyncEvent::Terminated(last_error)` is broadcast for
/// that scope, and the operator is alerted via
/// `Warning::OperatorAttentionNeeded`. The other scopes keep running.
/// (sync-D6, sync-D7)
async fn restart_scope(ctx: &RecoveryContext<'_>, scope: CursorScope) {
    ctx.cursors.delete(&scope);
    if let Err(err) = ctx.store.delete_change_cursor(ctx.account_id, &scope).await {
        tracing::warn!(
            target: "bifrost.sync.changes",
            account = ?ctx.account_id,
            scope = ?scope,
            error = %err,
            "RestartScope: delete_change_cursor failed"
        );
    }
    re_establish_scope_with_backoff(ctx, scope).await;
}

/// Quarantine a single scope: delete its in-memory and durable cursor and
/// drop it from the membership index (`CursorRegistry::delete` does both),
/// then broadcast a scoped operator warning. The per-scope poll loop
/// self-terminates on its next iteration when `cursors.snapshot(scope)`
/// returns `None` (the self-drain check in the multiplexer). Unlike
/// `restart_scope`, there is NO re-establishment, NO pause, and NO
/// account-wide escalation: a revoked shared folder must stay gone until
/// the next full account reopen re-runs discovery (where MYRIGHTS filters
/// it out if still revoked, or it re-appears if access was restored).
/// Siblings are untouched.
async fn disable_scope(ctx: &RecoveryContext<'_>, scope: CursorScope) {
    ctx.cursors.delete(&scope);
    if let Err(err) = ctx.store.delete_change_cursor(ctx.account_id, &scope).await {
        tracing::warn!(
            target: "bifrost.sync.changes",
            account = ?ctx.account_id,
            scope = ?scope,
            error = %err,
            "DisableScope: delete_change_cursor failed"
        );
    }
    broadcast_warning(
        ctx.changes_tx,
        Some(scope.clone()),
        bifrost_types::Warning::user_safe(
            bifrost_types::WarningKind::OperatorAttentionNeeded,
            format!("shared folder access revoked; scope disabled: {scope:?}"),
        )
        .with_protocol_detail(DiagnosticText::support_only(format!("{scope:?}"))),
    );
}

async fn re_establish_scope_with_backoff(ctx: &RecoveryContext<'_>, scope: CursorScope) {
    let mut delay = REOPEN_BACKOFF_INITIAL;
    let mut last_account_error: Option<AccountError> = None;
    for attempt in 0..REOPEN_RETRY_BUDGET {
        if attempt > 0 {
            let sleep_for = jittered(delay);
            tokio::time::sleep(sleep_for).await;
            delay = (delay.saturating_mul(2)).min(REOPEN_BACKOFF_CAP);
        }
        let acc_arc = ctx.current.load_full();
        let acc: &dyn Account = acc_arc.as_ref().as_ref();
        match run_establish(
            ctx.account_id,
            acc,
            scope.clone(),
            Arc::clone(ctx.cursors),
            Arc::clone(ctx.store),
            ctx.changes_tx.clone(),
            Some(ctx.control),
        )
        .await
        {
            Ok(()) => {
                if let Err(err) = link_discovered_memberships(acc, ctx.cursors).await {
                    tracing::warn!(
                        target: "bifrost.sync.changes",
                        account = ?ctx.account_id,
                        scope = ?scope,
                        error = %err,
                        "scope re-establishment: membership refresh failed"
                    );
                }
                return;
            }
            Err(Error::Paused) => return,
            Err(Error::Account(err) | Error::EstablishCursorTerminated(err)) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?ctx.account_id,
                    scope = ?scope,
                    attempt,
                    kind = ?err.kind(),
                    message_key = err.message_key(),
                    "scope re-establishment failed"
                );
                last_account_error = Some(err);
            }
            Err(err) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?ctx.account_id,
                    scope = ?scope,
                    attempt,
                    error = %err,
                    "scope re-establishment failed (engine error)"
                );
                // Engine-level errors (EstablishCursorFailed,
                // CheckpointStore, ...) carry no AccountError of their
                // own. Synthesize one so a budget exhausted entirely on
                // engine-level failures still broadcasts
                // SyncEvent::Terminated, per the documented contract,
                // rather than emitting only the operator warning.
                last_account_error = Some(crate::recovery::establish_failure_error(
                    scope.clone(),
                    bifrost_types::AccountOperation::EstablishCursor,
                ));
            }
        }
    }
    // Budget exhausted for this scope. Emit the last AccountError
    // verbatim and warn that operator attention is needed. The account
    // continues running for sibling scopes; only this scope stays
    // unestablished.
    if let Some(err) = last_account_error {
        broadcast_terminated(ctx.changes_tx, scope.clone(), err);
    }
    broadcast_warning(
        ctx.changes_tx,
        Some(scope.clone()),
        bifrost_types::Warning::user_safe(
            bifrost_types::WarningKind::OperatorAttentionNeeded,
            "scope re-establishment retry budget exhausted",
        )
        .with_protocol_detail(DiagnosticText::support_only(format!(
            "scope: {scope:?}; attempts: {REOPEN_RETRY_BUDGET}"
        ))),
    );
}

/// Restart the whole account with exponential backoff and a retry
/// budget. After three consecutive `factory.open` failures the account
/// is paused with `PauseReason::RetryBudgetExhausted` and the last
/// `AccountError` is broadcast as `SyncEvent::Terminated`. (sync-D6,
/// sync-D7)
async fn reattach_account(ctx: &RecoveryContext<'_>, next: Arc<dyn Account>) -> Result<(), Error> {
    next.set_priority(ctx.control.priority_snapshot());
    next.set_bandwidth_cap(ctx.control.bandwidth_cap_snapshot());
    let _activity = ctx.control.begin_activity().ok_or(Error::Paused)?;

    let result = async {
        let discovered = discover_scopes_from(next.as_ref()).await?;
        let staged = Arc::new(CursorRegistry::new());

        for scope in &discovered {
            if let Some(existing) = ctx.cursors.snapshot(scope) {
                staged.put(existing);
                continue;
            }
            match run_establish(
                ctx.account_id,
                next.as_ref(),
                scope.clone(),
                Arc::clone(&staged),
                Arc::clone(ctx.store),
                ctx.changes_tx.clone(),
                None,
            )
            .await
            {
                Ok(()) => {}
                Err(Error::Account(error)) if scope_local_establish_failure(&error) => {
                    tracing::warn!(
                        target: "bifrost.sync.reopen",
                        account = ?ctx.account_id,
                        scope = ?scope,
                        error = %error,
                        "scope-local establishment failure during reopen; skipping scope"
                    );
                }
                Err(error) => return Err(error),
            }
        }

        link_discovered_memberships(next.as_ref(), &staged).await?;

        // Refresh existing cursor snapshots at the last possible
        // moment so an in-flight old-handle poll cannot be lost merely
        // because discovery and subscription setup took time.
        for scope in &discovered {
            if let Some(latest) = ctx.cursors.snapshot(scope) {
                staged.put(latest);
            }
        }

        let discovered_set: HashSet<_> = discovered.iter().cloned().collect();
        for vanished in ctx
            .cursors
            .all_scopes()
            .into_iter()
            .filter(|scope| !discovered_set.contains(scope))
        {
            ctx.store
                .delete_change_cursor(ctx.account_id, &vanished)
                .await?;
        }

        let previous_subscriptions = ctx.subscriptions.snapshot(ctx.account_id);
        let mut replacement_subscriptions = Vec::with_capacity(previous_subscriptions.len());
        for record in previous_subscriptions
            .iter()
            .filter(|_| next.capabilities().push != bifrost_types::PushCapability::None)
        {
            let mut scopes: Vec<CursorScope> = record
                .scopes
                .iter()
                .filter(|scope| discovered_set.contains(*scope))
                .cloned()
                .collect();
            if scopes.is_empty() {
                scopes.clone_from(&discovered);
            }
            if scopes.is_empty() {
                continue;
            }
            match next.push_subscribe(&scopes).await {
                Ok(handle) => {
                    replacement_subscriptions.push(RegisteredSubscription { handle, scopes });
                }
                Err(error) => {
                    for replacement in replacement_subscriptions.drain(..) {
                        if let Err(cleanup) = next.push_unsubscribe(replacement.handle).await {
                            tracing::warn!(
                                target: "bifrost.sync.reopen",
                                account = ?ctx.account_id,
                                error = %cleanup,
                                "replacement push cleanup failed while unwinding reopen"
                            );
                        }
                    }
                    return Err(Error::Account(error));
                }
            }
        }

        let previous = ctx.current.swap(Arc::new(Arc::clone(&next)));
        ctx.cursors.replace_from(&staged);
        {
            let mut capabilities = match ctx.capabilities.write() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            *capabilities = next.capabilities().clone();
        }
        ctx.subscriptions
            .replace(ctx.account_id.clone(), replacement_subscriptions);
        ctx.account_generation_tx
            .send_modify(|generation| *generation = generation.saturating_add(1));

        for record in previous_subscriptions {
            if let Err(error) = previous.push_unsubscribe(record.handle).await {
                tracing::warn!(
                    target: "bifrost.sync.reopen",
                    account = ?ctx.account_id,
                    error = %error,
                    "old-handle push unsubscribe failed during reopen"
                );
            }
        }
        if let Err(error) = previous.close().await {
            tracing::warn!(
                target: "bifrost.sync.reopen",
                account = ?ctx.account_id,
                error = %error,
                "old account close failed during reopen"
            );
        }
        Ok(())
    }
    .await;

    if result.is_err()
        && let Err(error) = next.close().await
    {
        tracing::warn!(
            target: "bifrost.sync.reopen",
            account = ?ctx.account_id,
            error = %error,
            "replacement account close failed while unwinding reopen"
        );
    }
    result
}

async fn restart_account(ctx: &RecoveryContext<'_>) {
    let mut delay = REOPEN_BACKOFF_INITIAL;
    let mut last_error: Option<AccountError> = None;
    for attempt in 0..REOPEN_RETRY_BUDGET {
        if attempt > 0 {
            let sleep_for = jittered(delay);
            tokio::time::sleep(sleep_for).await;
            delay = (delay.saturating_mul(2)).min(REOPEN_BACKOFF_CAP);
        }
        match ctx.factory.open(ctx.account_id.clone()).await {
            Ok(next) => match reattach_account(ctx, next).await {
                Ok(()) => return,
                Err(Error::Paused) => return,
                Err(Error::Account(error) | Error::EstablishCursorTerminated(error)) => {
                    tracing::warn!(
                        target: "bifrost.sync.changes",
                        account = ?ctx.account_id,
                        attempt,
                        kind = ?error.kind(),
                        message_key = error.message_key(),
                        "RestartAccount: replacement attach failed"
                    );
                    last_error = Some(error);
                }
                Err(error) => {
                    tracing::warn!(
                        target: "bifrost.sync.changes",
                        account = ?ctx.account_id,
                        attempt,
                        error = %error,
                        "RestartAccount: replacement attach failed"
                    );
                    last_error = Some(crate::recovery::establish_failure_error(
                        CursorScope::Account,
                        bifrost_types::AccountOperation::EstablishCursor,
                    ));
                }
            },
            Err(err) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?ctx.account_id,
                    attempt,
                    kind = ?err.kind(),
                    message_key = err.message_key(),
                    "RestartAccount: factory.open failed"
                );
                last_error = Some(err);
            }
        }
    }
    // Account-wide budget exhausted: emit Terminated + pause the
    // account. All scopes pause as a consequence of the engine no
    // longer driving work for this account.
    if let Some(err) = last_error {
        broadcast_terminated(ctx.changes_tx, CursorScope::Account, err);
    }
    engine_pause(ctx, PauseReason::RetryBudgetExhausted);
}

/// Engine-driven pause: publish `AccountControl::Pause(reason)` on the
/// account's control broadcast and trip the boundary so per-scope
/// workers park. Consumers flip `AccountControl::Resume` (engine-side
/// helper on `SyncEngine`) to unpause.
fn engine_pause(ctx: &RecoveryContext<'_>, reason: PauseReason) {
    let _ = ctx.account_control_tx.send(AccountControl::Pause(reason));
    // Flip the boundary to Pause. Per-scope polls / push reconciler
    // park on `boundary.peek() == Pause`, so this halts all engine-
    // driven work for the account until a consumer calls
    // `SyncEngine::resume_account` (or sends `Resume` through
    // `Control::resume`).
    ctx.boundary_tx
        .send_replace(crate::cancel::BoundaryRequest::Pause);
}

fn jittered(base: Duration) -> Duration {
    // ±20% jitter to break thundering-herd reopen lockstep. Entropy
    // comes from the workspace UUID RNG (the same source bifrost-net's
    // backoff uses) rather than `SystemTime::now()`: a wall-clock jump
    // or low-resolution clock must not influence the jitter, and a
    // clock running backwards would otherwise collapse the spread.
    let entropy = i64::try_from(uuid::Uuid::new_v4().as_u128() % 401).unwrap_or(0);
    let percent_offset = entropy - 200; // [-200, +200] basis points
    let base_ms = i64::try_from(base.as_millis()).unwrap_or(i64::MAX);
    let delta_ms = (base_ms * percent_offset) / 1_000;
    let total = u64::try_from((base_ms + delta_ms).max(0)).unwrap_or(0);
    Duration::from_millis(total)
}

fn broadcast_terminated(
    changes_tx: &broadcast::Sender<MultiplexerEvent>,
    scope: CursorScope,
    error: AccountError,
) {
    let me = MultiplexerEvent {
        scope,
        event: Arc::new(SyncEvent::Terminated(error)),
        checkpoint: None,
    };
    let _ = changes_tx.send(me);
}

/// Broadcast a `Warning` on the per-account changes channel. The
/// `scope` argument is the directive's target scope when available;
/// the caller passes `None` only for warnings that are genuinely
/// account-wide. The fallback is `CursorScope::Account` because the
/// `MultiplexerEvent::scope` field is `CursorScope`, not
/// `Option<CursorScope>`. (sync-N4)
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
    control: Option<&SyncControl>,
) -> Result<(), Error> {
    let _activity = match control {
        Some(control) => Some(control.begin_activity().ok_or(Error::Paused)?),
        None => None,
    };
    match store.get_change_cursor(account_id, &scope).await {
        Ok(Some(existing)) => {
            cursors.put(existing);
            return Ok(());
        }
        Ok(None) => {}
        // Unlike `establish_one`, this path runs with a live reopen
        // listener, so an undecodable envelope is reported as a typed
        // error whose derived `RecoveryClass` is
        // `Engine(SchemaIncompatible)` and the listener runs the
        // account-wide schema-clear loop.
        Err(Error::SchemaIncompatible) => {
            return Err(Error::Account(crate::recovery::cursor_decode_failure(
                bifrost_types::AccountOperation::EstablishCursor,
            )));
        }
        Err(other) => return Err(other),
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
                control: control.cloned(),
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

/// Which bulk mutation a [`SyncEngine::run_bulk_pipeline`] run drives.
///
/// Selects the two op-specific seams in the shared pipeline: the wire
/// submit call (`Account::bulk_move` vs `Account::bulk_destroy`) and the
/// matching read-back guard (membership vs absence). Everything else in
/// the idempotency / retry / recovery loop is identical to
/// `bulk_set_flags`.
enum BulkPipelineOp {
    /// Route every target into `destination` (and, when the consumer
    /// knows it, out of `source`) via `Account::bulk_move_from`.
    Move {
        destination: MembershipScope,
        source: Option<MembershipScope>,
    },
    /// Destroy every target via `Account::bulk_destroy`.
    Destroy,
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

/// Add every retry-eligible target to the retry queue after a
/// retryable stream termination.
///
/// A stream may terminate before it emits an item outcome for every
/// submitted target. Those unseen ids are still part of the campaign
/// and must be retried alongside explicit per-item retry failures;
/// without this sweep they are neither resubmitted, read back, nor
/// counted, and the campaign reports success for work that never
/// happened.
///
/// Deliberately narrower than the `Reconcile` / `Engine` termination
/// sweeps, which claim every id that is not already resolved. An id
/// sitting in `PendingReadback` is owned by the read-back guard -
/// that lane exists precisely so a write whose first attempt may have
/// landed is verified rather than replayed - and a `BlockedByEngine`
/// id is waiting on an engine directive. Neither is ours to resubmit,
/// so only ids the stream never resolved (`None`) or explicitly asked
/// us to retry (`PendingRetry`) are queued.
fn queue_unresolved_for_retry(
    remaining: &[bifrost_types::ObjectId],
    outcomes: &HashMap<bifrost_types::ObjectId, MutationBucket>,
    retry_ids: &mut Vec<bifrost_types::ObjectId>,
) {
    // Set-based dedupe: `remaining` and `retry_ids` both scale with the
    // campaign, so a linear scan per candidate is quadratic on the
    // exact path (a large bulk that trips a rate limit) where it hurts.
    let mut queued: HashSet<bifrost_types::ObjectId> = retry_ids.iter().cloned().collect();
    for id in remaining {
        let eligible = matches!(outcomes.get(id), None | Some(MutationBucket::PendingRetry));
        if eligible && queued.insert(id.clone()) {
            retry_ids.push(id.clone());
        }
    }
}

/// Every id whose campaign outcome is still unresolved once the retry
/// loop ends, in no particular order.
///
/// The read-back queue is derived from `outcomes` rather than
/// accumulated during the loop: the map is the single source of truth
/// for per-id state, it already survives across attempts, and deriving
/// it means an id parked in the read-back lane cannot be lost when a
/// later attempt resubmits its siblings.
fn unresolved_readback_ids(
    outcomes: &HashMap<bifrost_types::ObjectId, MutationBucket>,
) -> Vec<bifrost_types::ObjectId> {
    let mut ids = Vec::new();
    for (id, bucket) in outcomes {
        if matches!(
            bucket,
            MutationBucket::PendingRetry | MutationBucket::PendingReadback
        ) {
            ids.push(id.clone());
        }
    }
    ids
}

/// Classify one `ItemOutcome<MutationSuccess>` and update the
/// per-id outcome map and retry queue.
///
/// Read-back membership is not a separate queue: `PendingReadback`
/// (and any `PendingRetry` still unresolved when the loop ends) IS the
/// read-back lane, collected by `unresolved_readback_ids`.
///
/// Dispatch rules:
/// - `Succeeded(Applied)` -> `Applied`.
/// - `Succeeded(Skipped)` -> `Skipped`.
/// - `Failed { error }` dispatches via `error.recovery()`:
///   - `Retry::SameRequest` / `AfterAuthRefresh` -> retry queue.
///   - `Retry::AfterStateRefresh` and `Reconcile(_)` -> read-back.
///   - `Engine(_)` -> `BlockedByEngine` and returns the original
///     structured error plus directive target for the reopen listener.
///   - terminal -> `FailedTerminal`.
/// - `Uncertain { error }` always -> read-back, even if the carried
///   recovery says retryable; the uncertainty lane exists precisely
///   to avoid blindly replaying writes whose first attempt may have
///   landed.
fn classify_item_outcome(
    item: ItemOutcome<MutationSuccess>,
    outcomes: &mut HashMap<bifrost_types::ObjectId, MutationBucket>,
    retry_ids: &mut Vec<bifrost_types::ObjectId>,
    dedupe_count: &mut u64,
) -> Option<(Option<CursorScope>, AccountError)> {
    use crate::recovery::{RecoveryPlan, plan_recovery};
    match item {
        ItemOutcome::Succeeded(success) => {
            let id = bifrost_types::ObjectId(success.item.0);
            let bucket = match success.output {
                MutationSuccess::Applied => MutationBucket::Applied,
                MutationSuccess::Skipped => MutationBucket::Skipped,
                _ => MutationBucket::Applied,
            };
            outcomes.insert(id, bucket);
            None
        }
        ItemOutcome::Failed(failure) => {
            let id = bifrost_types::ObjectId(failure.item.0);
            let original = failure.error.clone();
            match plan_recovery(failure.error) {
                RecoveryPlan::Retry(advice) => match advice.disposition {
                    bifrost_types::RetryDisposition::AfterStateRefresh => {
                        outcomes.insert(id, MutationBucket::PendingReadback);
                        None
                    }
                    bifrost_types::RetryDisposition::SameRequest
                    | bifrost_types::RetryDisposition::AfterAuthRefresh => {
                        retry_ids.push(id.clone());
                        outcomes.insert(id, MutationBucket::PendingRetry);
                        None
                    }
                    // RetryDisposition is #[non_exhaustive]; new variants
                    // default to read-back (the safe path) and require
                    // explicit handling here when added.
                    _ => {
                        outcomes.insert(id, MutationBucket::PendingReadback);
                        None
                    }
                },
                RecoveryPlan::Reconcile(advice) => {
                    for action in &advice.guidance.actions {
                        if matches!(action, ReconcileAction::DedupeByClientId) {
                            *dedupe_count = dedupe_count.saturating_add(1);
                        }
                    }
                    // Both reconcile shapes land in the same lane.
                    // `CheckTarget` asks for exactly what the read-back
                    // guard performs; without it the consumer must
                    // dedupe before any retry, so the engine cannot
                    // resolve the item automatically either. Either way
                    // pending-readback is the honest state and surfaces
                    // the id through the counters.
                    outcomes.insert(id, MutationBucket::PendingReadback);
                    None
                }
                RecoveryPlan::Engine(directive) => {
                    outcomes.insert(id, MutationBucket::BlockedByEngine);
                    Some((
                        crate::recovery::directive_target_scope(&directive),
                        original,
                    ))
                }
                RecoveryPlan::Terminal(_) => {
                    outcomes.insert(id, MutationBucket::FailedTerminal);
                    None
                }
            }
        }
        ItemOutcome::Uncertain(uncertain) => {
            let id = bifrost_types::ObjectId(uncertain.item.0);
            outcomes.insert(id, MutationBucket::PendingReadback);
            None
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
            MutationBucket::FailedTerminal => counters.record_failed(),
            MutationBucket::BlockedByEngine => counters.record_blocked_by_engine(),
            MutationBucket::PendingRetry | MutationBucket::PendingReadback => {
                counters.record_pending();
            }
        }
    }
    counters
}

#[cfg(test)]
mod tests {
    use super::{
        MutationBucket, classify_item_outcome, queue_unresolved_for_retry, scope_covers_membership,
        unresolved_readback_ids,
    };
    use bifrost_types::{CursorScope, FolderId, MembershipScope, ObjectType};

    #[test]
    fn jittered_stays_within_plus_minus_20_percent() {
        use super::jittered;
        use std::time::Duration;
        // Entropy comes from a fresh UUID per call; sample enough draws
        // that the ±20% envelope is exercised without depending on the
        // clock. The math must keep every draw inside [0.8x, 1.2x].
        let base = Duration::from_millis(1_000);
        let lo = Duration::from_millis(800);
        let hi = Duration::from_millis(1_200);
        for _ in 0..10_000 {
            let j = jittered(base);
            assert!(j >= lo && j <= hi, "jitter {j:?} outside ±20% of {base:?}");
        }
    }

    #[test]
    fn jittered_zero_base_is_zero() {
        use super::jittered;
        use std::time::Duration;
        assert_eq!(jittered(Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn retryable_stream_termination_requeues_every_unresolved_target() {
        let applied = bifrost_types::ObjectId("applied".into());
        let explicit_retry = bifrost_types::ObjectId("explicit-retry".into());
        let unseen = bifrost_types::ObjectId("unseen".into());
        let remaining = vec![applied.clone(), explicit_retry.clone(), unseen.clone()];
        let outcomes = std::collections::HashMap::from([
            (applied, MutationBucket::Applied),
            (explicit_retry.clone(), MutationBucket::PendingRetry),
        ]);
        let mut retry_ids = vec![explicit_retry.clone()];

        queue_unresolved_for_retry(&remaining, &outcomes, &mut retry_ids);

        assert_eq!(retry_ids, vec![explicit_retry, unseen]);
    }

    /// The retry sweep must not raid the other two lanes. An id the
    /// protocol reported `Uncertain` (or a reconcile that wants its
    /// target checked) belongs to the read-back guard - resubmitting it
    /// is exactly the blind replay that lane exists to prevent - and an
    /// engine-blocked id is waiting on a directive.
    #[test]
    fn retry_sweep_leaves_readback_and_engine_blocked_ids_alone() {
        let uncertain = bifrost_types::ObjectId("uncertain".into());
        let blocked = bifrost_types::ObjectId("blocked".into());
        let unseen = bifrost_types::ObjectId("unseen".into());
        let remaining = vec![uncertain.clone(), blocked.clone(), unseen.clone()];
        let outcomes = std::collections::HashMap::from([
            (uncertain, MutationBucket::PendingReadback),
            (blocked, MutationBucket::BlockedByEngine),
        ]);
        let mut retry_ids = Vec::new();

        queue_unresolved_for_retry(&remaining, &outcomes, &mut retry_ids);

        assert_eq!(retry_ids, vec![unseen]);
    }

    #[test]
    fn per_item_engine_failure_returns_the_directive_for_forwarding() {
        use bifrost_types::{
            AccountOperation, BatchFailure, BatchItemId, ItemOutcome, MutationSuccess,
        };
        let scope = CursorScope::Folder(FolderId("INBOX".into()));
        let error =
            crate::recovery::restart_scope_error(scope.clone(), AccountOperation::UpdateFlags);
        let item: ItemOutcome<MutationSuccess> =
            ItemOutcome::Failed(BatchFailure::new(BatchItemId("message".into()), error));
        let mut outcomes = std::collections::HashMap::new();
        let mut retry = Vec::new();
        let mut dedupe = 0;

        let (target, forwarded) =
            classify_item_outcome(item, &mut outcomes, &mut retry, &mut dedupe)
                .expect("engine recovery must be forwarded");

        assert_eq!(target, Some(scope));
        assert!(forwarded.recovery().requires_engine_action());
        assert_eq!(
            outcomes.get(&bifrost_types::ObjectId("message".into())),
            Some(&MutationBucket::BlockedByEngine)
        );
    }

    /// Both pending lanes reach the guard, and nothing else does.
    #[test]
    fn readback_queue_is_every_still_pending_id() {
        let outcomes = std::collections::HashMap::from([
            (
                bifrost_types::ObjectId("applied".into()),
                MutationBucket::Applied,
            ),
            (
                bifrost_types::ObjectId("skipped".into()),
                MutationBucket::Skipped,
            ),
            (
                bifrost_types::ObjectId("failed".into()),
                MutationBucket::FailedTerminal,
            ),
            (
                bifrost_types::ObjectId("blocked".into()),
                MutationBucket::BlockedByEngine,
            ),
            (
                bifrost_types::ObjectId("retry".into()),
                MutationBucket::PendingRetry,
            ),
            (
                bifrost_types::ObjectId("readback".into()),
                MutationBucket::PendingReadback,
            ),
        ]);

        let mut ids: Vec<String> = unresolved_readback_ids(&outcomes)
            .into_iter()
            .map(|id| id.0)
            .collect();
        ids.sort();

        assert_eq!(ids, vec!["readback".to_string(), "retry".to_string()]);
    }

    /// Attach must contain a scope-local establishment failure instead of
    /// failing the whole account. One revoked shared mailbox or unreadable
    /// public folder previously took the primary mailbox down with it,
    /// because the establish loop propagated every error with `?`.
    #[test]
    fn scope_revoked_establishment_failure_is_contained_to_its_scope() {
        use super::scope_local_establish_failure;
        use bifrost_types::{
            AccessCause, AccessErrorKind, AccountErrorBuilder, AccountErrorKind, AccountOperation,
            Cause, ErrorScope, Protocol, StateCause, SyncStateErrorKind,
        };

        let revoked = AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked),
            Cause::State(StateCause::ScopeRevoked),
        )
        .protocol(Protocol::Imap)
        .operation(AccountOperation::EstablishCursor)
        .scope(ErrorScope::Cursor(CursorScope::Folder(FolderId(
            "Shared/alice/INBOX".into(),
        ))))
        .try_build()
        .expect("valid account error classification");
        assert!(scope_local_establish_failure(&revoked));

        // An account-wide fact still fails the attach: containment is
        // deliberately narrow, keyed on the protocol crate's own
        // `DisableScope` classification and nothing else.
        let denied = AccountErrorBuilder::new(
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied { resource: None }),
        )
        .protocol(Protocol::Imap)
        .operation(AccountOperation::EstablishCursor)
        .try_build()
        .expect("valid account error classification");
        assert!(!scope_local_establish_failure(&denied));
    }

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

    /// `disable_scope` quarantines a single shared-folder scope by
    /// deleting its cursor and dropping its membership index edges. The
    /// poll loop self-drains on the next iteration once
    /// `cursors.snapshot(scope)` returns `None`. This pins the registry
    /// mechanism the quarantine relies on: a shared `Folder` scope tagged
    /// with its owning `Mailbox` membership is fully removed by
    /// `CursorRegistry::delete` (cursor + membership), and an untargeted
    /// sibling scope is untouched.
    #[test]
    fn disable_scope_deletes_cursor_and_drops_membership() {
        use crate::cursor::CursorRegistry;
        use bifrost_types::{ChangeCursor, MailboxId, OpaqueChangeState, ProtocolKind};

        let cursors = CursorRegistry::new();
        let shared = CursorScope::Folder(FolderId("Shared/alice/INBOX".into()));
        let sibling = CursorScope::Folder(FolderId("INBOX".into()));

        let mk = |scope: &CursorScope| ChangeCursor {
            scope: scope.clone(),
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Imap,
                envelope_version: 1,
                bytes: Vec::new(),
            },
            advanced_through: None,
            envelope_version: 1,
        };
        cursors.put(mk(&shared));
        cursors.put(mk(&sibling));
        cursors.link_membership(
            MembershipScope::Mailbox(MailboxId("alice".into())),
            shared.clone(),
        );
        cursors.link_membership(
            MembershipScope::Folder(FolderId("INBOX".into())),
            sibling.clone(),
        );

        // The mechanism inside `disable_scope`: a single registry delete
        // drops both the cursor and the membership edges for the scope.
        cursors.delete(&shared);

        assert!(cursors.snapshot(&shared).is_none());
        assert!(
            cursors
                .scopes_for_membership(&MembershipScope::Mailbox(MailboxId("alice".into())))
                .is_empty()
        );
        // Sibling scope untouched.
        assert!(cursors.snapshot(&sibling).is_some());
        assert_eq!(
            cursors.scopes_for_membership(&MembershipScope::Folder(FolderId("INBOX".into()))),
            vec![sibling]
        );
    }

    /// Build a backfill checkpoint on a given partition key + progress for
    /// the open-pages resume tests.
    #[cfg(test)]
    fn checkpoint_on(
        partition: bifrost_types::Partition,
        items_done: u64,
    ) -> bifrost_types::BackfillCheckpoint {
        bifrost_types::BackfillCheckpoint {
            scope: CursorScope::Type(ObjectType::Email),
            partition,
            progress_marker: None,
            progress: bifrost_types::BackfillProgress {
                items_done,
                items_estimated: None,
            },
            envelope_version: 1,
        }
    }

    #[test]
    fn backfill_complete_recorded_detects_sentinel() {
        use super::backfill_complete_recorded;
        assert!(!backfill_complete_recorded(None));

        let marker = checkpoint_on(crate::backfill::partitioner::completion_partition(), 99);
        assert!(backfill_complete_recorded(Some(&marker)));

        // A real partition checkpoint (e.g. a fully-walked Full plan) is
        // not the completion signal.
        let full = checkpoint_on(
            crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Full),
            99,
        );
        assert!(!backfill_complete_recorded(Some(&full)));
    }

    #[test]
    fn open_pages_resume_no_checkpoint_starts_fresh() {
        use super::{OpenPagesResume, open_pages_resume};
        assert_eq!(open_pages_resume(None), OpenPagesResume::ResumeFrom(0));
    }

    #[test]
    fn open_pages_resume_completion_marker_skips() {
        use super::{OpenPagesResume, open_pages_resume};
        let ck = checkpoint_on(crate::backfill::partitioner::completion_partition(), 1234);
        assert_eq!(open_pages_resume(Some(&ck)), OpenPagesResume::Skip);
    }

    #[test]
    fn open_pages_resume_short_final_page_skips() {
        use super::{OpenPagesResume, open_pages_resume};
        // A 256-of-500 page means inventory ran out inside the window.
        let key =
            crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Page {
                from: 0,
                to: 500,
            });
        let ck = checkpoint_on(key, 256);
        assert_eq!(open_pages_resume(Some(&ck)), OpenPagesResume::Skip);
    }

    #[test]
    fn open_pages_resume_full_page_resumes_after_it() {
        use super::{OpenPagesResume, open_pages_resume};
        let key =
            crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Page {
                from: 500,
                to: 1000,
            });
        let ck = checkpoint_on(key, 500);
        assert_eq!(
            open_pages_resume(Some(&ck)),
            OpenPagesResume::ResumeFrom(1000)
        );
    }

    #[test]
    fn open_pages_resume_unrecognised_partition_starts_fresh() {
        use super::{OpenPagesResume, open_pages_resume};
        let key =
            crate::backfill::partitioner::partition_key(&bifrost_types::InventoryPartition::Full);
        let ck = checkpoint_on(key, 7);
        assert_eq!(open_pages_resume(Some(&ck)), OpenPagesResume::ResumeFrom(0));
    }
}
