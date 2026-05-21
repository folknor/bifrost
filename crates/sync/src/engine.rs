//! `SyncEngine` lifecycle.
//!
//! Holds an `Arc<dyn AccountFactory>` per attached account, drives one
//! multiplexer / backfill / push reconciler / mutation pipeline per
//! slot, and exposes the engine's public surface: `attach`, `detach`,
//! `account_changes_stream`, `bulk_*` campaign entry points,
//! `invalidation_sink`.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bifrost_types::{
    Account, AccountCapabilities, AccountFactory, AccountId, AccountStream, ChangeCursor,
    CursorEstablishment, CursorScope, InvalidationSink, MembershipScope, Priority,
    SubscriptionHandle, SyncEvent, WatchEvent,
};
use dashmap::DashMap;
use futures::stream::StreamExt;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::backfill::{
    BackfillHandle, BackfillRegistry, BackfillRunner, BackfillState, LiveSupersedes,
};
use crate::cancel::{Boundary, BoundaryRequest};
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::cursor::store::{DynCheckpointStore, InMemoryCheckpointStore};
use crate::error::Error;
use crate::multiplexer::{Multiplexer, MultiplexerEvent, MultiplexerHandle, ReopenRequest};
use crate::mutation::MutationHandle;
use crate::push::{InvalidationSinkInner, PushHandle, SubscriptionRegistry};
use crate::scheduler::{BudgetGate, ConcurrencyBudget, Scheduler};
use crate::types::{AccountSlot, EngineConfig};

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
}

impl SyncEngineBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: EngineConfig::default(),
            checkpoints: None,
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

    #[must_use]
    pub fn build(self) -> SyncEngine {
        let checkpoints = self
            .checkpoints
            .unwrap_or_else(|| Arc::new(InMemoryCheckpointStore::new()));
        let budget_gate = BudgetGate::new(self.config.budget);
        let scheduler = Scheduler::with_lane_capacity(
            self.config.scheduler,
            budget_gate,
            self.config.lane_capacity,
        );
        SyncEngine {
            config: self.config,
            checkpoints,
            accounts: DashMap::new(),
            backfill_registry: Arc::new(BackfillRegistry::new()),
            sink: Arc::new(InvalidationSinkInner::new()),
            subscriptions: Arc::new(SubscriptionRegistry::new()),
            scheduler,
            root_cancel: CancellationToken::new(),
        }
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
        if self.accounts.contains_key(&account_id) {
            return Err(Error::AccountAlreadyAttached(account_id));
        }

        let opened = factory.open().await.map_err(Error::OpenFailed)?;
        let capabilities: AccountCapabilities = opened.capabilities().clone();

        let cursors = Arc::new(CursorRegistry::new());

        // Drive cursor establishment per scope. We do this before
        // spawning tasks because multiplexer + backfill need the
        // registry pre-populated for `Ready` scopes.
        let scopes = self.discover_scopes(opened.as_ref()).await?;
        for scope in scopes.clone() {
            self.establish_one(&account_id, opened.as_ref(), scope, Arc::clone(&cursors))
                .await?;
        }

        // Drive membership discovery to populate the push-reconciler's
        // side-index (H6). Bounded stream; one walk per attach.
        self.discover_and_link_memberships(opened.as_ref(), Arc::clone(&cursors))
            .await?;

        // Wire boundary + priority watches.
        let (boundary, boundary_view) = Boundary::new();
        let (priority_tx, _priority_rx) = watch::channel(Priority::Normal);

        // Per-account broadcast for the unified Change stream. Keep a
        // sentinel receiver on the slot so the channel never closes
        // when subscribers come and go.
        let (changes_tx, sentinel_rx) =
            broadcast::channel::<MultiplexerEvent>(self.config.multiplexer.changes_capacity);

        // Per-account watch-event sender / receiver. The reconciler
        // owns the receiver; the multiplexer (in-process forwarder)
        // and the engine's `InvalidationSink` both feed the sender.
        let (watch_tx, watch_rx) =
            mpsc::channel::<WatchEvent>(self.config.multiplexer.watch_capacity);
        self.sink.register(account_id.clone(), watch_tx.clone());

        // Register per-account budget semaphores.
        self.scheduler.budget().register(account_id.clone());

        // Shutdown token tree: per-slot child of engine root.
        let shutdown = self.root_cancel.child_token();

        // ArcSwap holds the current `Arc<dyn Account>` so spawned
        // workers see post-reopen handles immediately.
        let current: Arc<ArcSwap<Arc<dyn Account>>> = Arc::new(ArcSwap::from(Arc::new(opened)));

        // Control handle shared between the SyncControl returned to
        // the consumer and the engine's spawned workers (so workers
        // can call `record_checkpoint`).
        let control = SyncControl::new(account_id.clone(), boundary.clone(), priority_tx.clone());

        // Reopen channel: the multiplexer's per-scope tasks raise
        // requests when a stream ends with a RestartScope /
        // RestartAccount / CapabilityChanged recovery class.
        let (reopen_tx, mut reopen_rx) = mpsc::channel::<ReopenRequest>(16);

        let mut workers: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        // Spawn push reconciler.
        let reconciler = crate::push::Reconciler {
            account_id: account_id.clone(),
            account: Arc::clone(&current),
            cursors: Arc::clone(&cursors),
            store: Arc::clone(&self.checkpoints),
            changes_tx: changes_tx.clone(),
            boundary: boundary_view.clone(),
            shutdown: shutdown.clone(),
            control: control.clone(),
        };
        workers.push(tokio::spawn(reconciler.run(watch_rx)));

        // In-process push forwarder: drain `Account::push_stream` into
        // the per-account mpsc. Out-of-process push goes through the
        // `InvalidationSink` directly.
        if capabilities.push_in_process() {
            let acc = Arc::clone(&current);
            let aid = account_id.clone();
            let tx = watch_tx.clone();
            let sd = shutdown.clone();
            workers.push(tokio::spawn(async move {
                let acc_arc = acc.load_full();
                let mut stream = acc_arc.push_stream();
                loop {
                    tokio::select! {
                        () = sd.cancelled() => return,
                        next = stream.next() => {
                            let Some(event) = next else { return; };
                            // Bounded: if the queue is full the
                            // reconciler is already busy; drop.
                            if tx.try_send(event).is_err() {
                                tracing::trace!(
                                    target: "bifrost.sync.changes",
                                    account = ?aid,
                                    "in-process push: queue full, coalesced"
                                );
                            }
                        }
                    }
                }
            }));
        }

        // Spawn the multiplexer.
        let mux = Multiplexer {
            account_id: account_id.clone(),
            account: Arc::clone(&current),
            cursors: Arc::clone(&cursors),
            store: Arc::clone(&self.checkpoints),
            config: self.config.multiplexer,
            boundary: boundary_view.clone(),
            changes_tx: changes_tx.clone(),
            watch_tx: watch_tx.clone(),
            control: control.clone(),
            shutdown: shutdown.clone(),
            reopen_tx: reopen_tx.clone(),
            poll: Default::default(),
        };
        workers.push(tokio::spawn(mux.run()));

        // Spawn the backfill orchestrator. It walks the registered
        // scopes and runs one `BackfillRunner::run_partition` per
        // scope under a default policy. The runner uses the slot's
        // shared `LiveSupersedes` so live `Created` events skip
        // backfilled duplicates.
        let live_supersedes = Arc::new(LiveSupersedes::new());
        let backfill_registry_handle = Arc::clone(&self.backfill_registry);
        let bf_account = Arc::clone(&current);
        let bf_cursors = Arc::clone(&cursors);
        let bf_live = Arc::clone(&live_supersedes);
        let bf_shutdown = shutdown.clone();
        workers.push(tokio::spawn(async move {
            run_backfill_orchestrator(
                bf_account,
                bf_cursors,
                bf_live,
                backfill_registry_handle,
                bf_shutdown,
            )
            .await;
        }));

        // Spawn the reopen listener.
        let reopen_factory = Arc::clone(&factory);
        let reopen_current = Arc::clone(&current);
        let reopen_cursors = Arc::clone(&cursors);
        let reopen_store = Arc::clone(&self.checkpoints);
        let reopen_aid = account_id.clone();
        let reopen_shutdown = shutdown.clone();
        workers.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = reopen_shutdown.cancelled() => return,
                    req = reopen_rx.recv() => {
                        let Some(req) = req else { return; };
                        match req {
                            ReopenRequest::Account => {
                                match reopen_factory.open().await {
                                    Ok(next) => reopen_current.store(Arc::new(next)),
                                    Err(err) => tracing::warn!(
                                        target: "bifrost.sync.changes",
                                        account = ?reopen_aid,
                                        error = %err,
                                        "reopen failed"
                                    ),
                                }
                            }
                            ReopenRequest::Scope(scope) => {
                                // Drop the cursor so the next poll
                                // cycle picks up via inventory.
                                reopen_cursors.put(
                                    ChangeCursor {
                                        scope: scope.clone(),
                                        server_state: bifrost_types::OpaqueChangeState {
                                            protocol: bifrost_types::ProtocolKind::Jmap,
                                            envelope_version: 1,
                                            bytes: Vec::new(),
                                        },
                                        advanced_through: None,
                                        envelope_version: 1,
                                    },
                                );
                                let _ = reopen_store
                                    .put_change_cursor(
                                        &reopen_aid,
                                        reopen_cursors
                                            .snapshot(&scope)
                                            .expect("just-inserted cursor"),
                                    )
                                    .await;
                            }
                        }
                    }
                }
            }
        }));

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
            boundary_tx: boundary.sender(),
            shutdown: shutdown.clone(),
            control: control.clone(),
            _sentinel_rx: sentinel_rx,
            workers: tokio::sync::Mutex::new(workers),
        });

        self.accounts.insert(account_id.clone(), slot);
        drop(boundary_view);

        Ok(control)
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
        // Ask running workers to checkpoint cleanly, then stop.
        let _ = slot.boundary_tx.send(BoundaryRequest::Stop);
        slot.shutdown.cancel();

        // Await spawned workers up to the configured timeout. Any
        // task that hasn't exited gets aborted to ensure detach is
        // bounded.
        let timeout = self.config.detach_timeout;
        let mut workers = slot.workers.lock().await;
        let drained: Vec<_> = workers.drain(..).collect();
        drop(workers);
        let deadline = tokio::time::Instant::now() + timeout;
        for handle in drained {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                handle.abort();
                continue;
            }
            match tokio::time::timeout(remaining, handle).await {
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
                    tracing::warn!(
                        target: "bifrost.sync.changes",
                        "worker exceeded detach timeout; aborted"
                    );
                }
            }
        }

        let current = slot.current.load_full();
        if let Err(e) = current.close().await {
            tracing::warn!(target: "bifrost.sync.changes", error=?e, "account close failed");
        }
        self.sink.unregister(account_id);
        self.scheduler.budget().forget(account_id);
        Ok(())
    }

    /// Hot-swap the account handle (capability change, transport reset).
    /// Spawned workers pick up the new handle on their next iteration
    /// because they load through the slot's `ArcSwap`.
    pub async fn reopen(&self, account_id: &AccountId) -> Result<(), Error> {
        let slot = self
            .accounts
            .get(account_id)
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let next = slot.factory.open().await.map_err(Error::OpenFailed)?;
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
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let max_retries = self.config.mutation_max_retries;

        // Reuse the same idempotency key across retry attempts so the
        // protocol can dedup; see `IdempotencyVendor` for the
        // run_id/sequence layout. Each campaign retry comes from a
        // single `vendor.next(protocol)` call.
        let key = vendor.next(protocol);

        let mut counters = crate::mutation::MutationCounters::default();
        let mut retry_ids: Vec<bifrost_types::ObjectId> = Vec::new();
        let remaining: Vec<bifrost_types::ObjectId> = targets;
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

            while let Some(event) = stream.next().await {
                match event {
                    bifrost_types::SyncEvent::Batch(batch) => {
                        for result in batch.items {
                            classify_mutation_outcome(
                                &result.id,
                                &result.outcome,
                                &mut counters,
                                &mut retry_ids,
                            );
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
                retry_ids.clear();
                // remaining stays the same for the retry.
                continue;
            }

            // Run the read-back guard against retry candidates.
            if !retry_ids.is_empty() {
                let account = slot.current.load_full();
                let outcome = crate::mutation::run_readback_guard(
                    account.as_ref().as_ref(),
                    retry_ids.clone(),
                    &op,
                )
                .await?;
                counters.pending_retry = counters
                    .pending_retry
                    .saturating_sub(outcome.skipped)
                    .saturating_sub(outcome.still_failed);
                counters.skipped = counters.skipped.saturating_add(outcome.skipped);
                counters.failed_terminal = counters
                    .failed_terminal
                    .saturating_add(outcome.still_failed);
            }
            return Ok(counters);
        }
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
                // Spawn the inventory fusion. We do this synchronously
                // inside attach because the engine needs the cursor
                // before changes_stream can run; if inventory is
                // expensive (Graph all scopes), this awaits the full
                // initial sync. That matches the
                // `EstablishViaInventory` contract: the inventory
                // walk IS the cursor establishment.
                let fusion = crate::multiplexer::InventoryFusion {
                    account_id: account_id.clone(),
                    cursors: Arc::clone(&cursors),
                    store: Arc::clone(&self.checkpoints),
                };
                match fusion.run(account, scope).await? {
                    crate::multiplexer::FusionOutcome::Established
                    | crate::multiplexer::FusionOutcome::NoCursor => Ok(()),
                    crate::multiplexer::FusionOutcome::Fatal => Err(Error::EstablishCursorFailed(
                        "inventory fusion fatal".into(),
                    )),
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

/// Backfill orchestrator. Walks the registered cursor scopes and runs
/// one partition pass per scope via `BackfillRunner::run_partition`.
///
/// The runner uses the slot's shared `LiveSupersedes` set so live
/// `Created` events from the multiplexer skip over inventory entries
/// the user has already seen.
async fn run_backfill_orchestrator(
    account: Arc<ArcSwap<Arc<dyn Account>>>,
    cursors: Arc<CursorRegistry>,
    live: Arc<LiveSupersedes>,
    registry: Arc<BackfillRegistry>,
    shutdown: CancellationToken,
) {
    let scopes = cursors.all_scopes();
    for scope in scopes {
        if shutdown.is_cancelled() {
            return;
        }
        registry.mark(scope.clone(), BackfillState::Running);
        let acc_arc = account.load_full();
        let acc: &dyn Account = acc_arc.as_ref().as_ref();
        match BackfillRunner::run_partition(acc, scope.clone(), live.as_ref()).await {
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

impl Drop for SyncEngine {
    fn drop(&mut self) {
        // Tear down every attached slot. Workers exit at their next
        // boundary; no checkpoint loss because slot.boundary_tx was
        // flipped to Stop on detach (the consumer's detach path),
        // and any slot still around at Drop never explicitly
        // detached - the safe behavior there is best-effort drain.
        //
        // We cannot `await` here without entering the runtime; the
        // root cancel is the strongest signal we can fire from Drop.
        // Consumers that need bounded teardown call `detach` for
        // every account explicitly first.
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
fn classify_mutation_outcome(
    id: &bifrost_types::ObjectId,
    outcome: &bifrost_types::MutationOutcome,
    counters: &mut crate::mutation::MutationCounters,
    retry_ids: &mut Vec<bifrost_types::ObjectId>,
) {
    match outcome {
        bifrost_types::MutationOutcome::Applied => counters.record_applied(),
        bifrost_types::MutationOutcome::Skipped => counters.record_skipped(),
        bifrost_types::MutationOutcome::Failed(err) => {
            if is_terminal_mutation_error(err) {
                counters.record_failed();
            } else {
                counters.record_pending();
                retry_ids.push(id.clone());
            }
        }
        // `MutationOutcome` is `#[non_exhaustive]`; conservatively
        // surface unknown future variants as terminal failures.
        _ => counters.record_failed(),
    }
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
