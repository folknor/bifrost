//! `SyncEngine` lifecycle.
//!
//! Holds an `Arc<dyn AccountFactory>` per attached account, drives one
//! multiplexer / backfill / push reconciler / mutation pipeline per
//! slot, and exposes the engine's public surface: `attach`, `detach`,
//! `account_changes_stream`, `bulk_*` campaign entry points, the
//! read-only hydration passthrough (`get_stream`, `message_hydrate`,
//! `open_blob`, `open_raw_rfc822`, ...), `invalidation_sink`.
//!
//! `SyncEngine` itself lives here, along with the builder, the slot
//! lifecycle (`detach`, `reopen`, `shutdown`), the consumer-facing ack
//! and debt surface, and `Drop`. Its remaining methods are split by
//! concern across sibling modules, each of which adds its own
//! `impl SyncEngine` block:
//!
//! - `attach` - scope discovery, cursor establishment, the worker spawn
//!   block, and the deferred-inventory worker.
//! - `backfill` - the backfill orchestrator: rescan, resume planning,
//!   and the per-partition walk.
//! - `ack` - the account's single durable writer and `WriterHandle`.
//! - `reattach` - recovery dispatch: reattach, restart, re-establish,
//!   and the engine-directive arms.
//! - `bulk` - the bulk mutation pipeline and its campaign entry points.
//! - `passthrough` - the read-only hydration and PIM forwarders.
//! - `context` - `SlotContext`, the handle bundle the spawned workers
//!   share.
//! - `lane` - the bounded backfill lane: producer-side flow control so a
//!   cold start cannot outrun a consumer into the broadcast ring.
//!
//! The split is by concern only: nothing on the published surface moved,
//! and every path that resolved through `crate::engine` before still
//! does.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bifrost_types::{
    Account, AccountCapabilities, AccountControl, AccountError, AccountFactory, AccountFuture,
    AccountId, AccountStream, BackfillCheckpoint, BackfillProgress, Batch, ChangeCursor,
    Checkpoint, CursorEstablishment, CursorScope, DiagnosticText, EngineDirective, ErrorScope,
    InvalidationSink, InventoryPartition, InventoryPartitioning, ItemOutcome, MembershipScope,
    MutationSuccess, OpenedAccount, PageBoundary, PauseReason, Priority, ReconcileAction,
    ReconcileAdvice, RecoveryClass, RetryAdvice, SkippedScope, SyncEvent, WatchEvent,
};
use dashmap::DashMap;
use futures::stream::StreamExt;
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard, broadcast, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::backfill::{
    BackfillPolicy, BackfillRegistry, BackfillRunner, BackfillState, BackfillStrategy,
    LiveSupersedes,
};
use crate::cancel::{Boundary, BoundaryRequest};
use crate::control::{SyncActivityGuard, SyncControl};
use crate::cursor::CursorRegistry;
use crate::cursor::store::{CheckpointTransition, DynCheckpointStore, InMemoryCheckpointStore};
use crate::cursor::{ClaimLookup, PendingCoverage};
use crate::error::Error;
use crate::multiplexer::changes::OperatorDecision;
use crate::multiplexer::{
    AckRequest, ChangeDelivery, Multiplexer, MultiplexerEvent, MultiplexerHandle, ReopenRequest,
    WriterRequest,
};
use crate::push::{InvalidationSinkInner, RegisteredSubscription, SubscriptionRegistry};
use crate::scheduler::{BudgetGate, ConcurrencyBudget, Scheduler};
use crate::types::{AccountSlot, BackfillConfig, EngineConfig, WorkerTask};

mod ack;
mod attach;
mod backfill;
mod bulk;
mod context;
pub mod lane;
mod passthrough;
mod reattach;
#[cfg(test)]
mod tests;

// Re-imported at the parent so every sibling module reaches them through
// its own `use super::*`, exactly as they reached each other while this
// was one file.
use ack::{WriterHandle, ack_writer, await_worker_until, take_ack_writer};
use attach::{discover_scopes_from, link_discovered_memberships, wait_for_real_subscriber};
use backfill::{BackfillWiring, run_backfill_orchestrator};
use context::SlotContext;
use lane::LaneGate;
use reattach::{
    RecoveryContext, ReplacementOpen, accepted_push_scopes, handle_account_error, log_open_skips,
    open_replacement, reattach_account,
};

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
    ack_senders: DashMap<AccountId, mpsc::Sender<WriterRequest>>,
    /// Per-account in-flight lifecycle guard, held for the whole of
    /// `attach` AND the whole of `detach`.
    ///
    /// Attach needs it because the existence check, `factory.open`, and
    /// the final slot insert span several awaits, so two concurrent
    /// attaches would otherwise both spawn workers. Detach needs it for
    /// the mirror-image reason: it removes the slot first but does its
    /// registry cleanup last, after awaiting workers (up to
    /// `detach_timeout`) and `Account::close()`. An attach landing in
    /// that window sees no slot and no in-flight entry, installs fresh
    /// registrations, and then the still-running detach unregisters the
    /// NEW incarnation from the invalidation sink, the budget gate, the
    /// backfill registry, the throttle memberships, and the bandwidth
    /// meter. The result is an account that looks attached but silently
    /// drops every out-of-process push. Serializing the two is what
    /// makes the teardown's identity assumption true.
    lifecycle_inflight: Arc<AsyncMutex<std::collections::HashSet<AccountId>>>,
    /// Engine-wide throttle bucket, shared by every attached account
    /// so `Tenant` / `Provider` deadlines recorded by one account can
    /// pause its siblings. Recovery paths record; the per-scope poll
    /// loop and the push reconciler consult it before driving work.
    throttles: Arc<std::sync::Mutex<crate::recovery::ThrottleBucket>>,
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
            lifecycle_inflight: Arc::new(AsyncMutex::new(std::collections::HashSet::new())),
            throttles: Arc::new(std::sync::Mutex::new(crate::recovery::ThrottleBucket::new())),
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
    DeferredInventory(DeferredInventory),
}

enum DeferredInventory {
    Start(CursorScope),
    Resume(ChangeCursor),
}

impl DeferredInventory {
    fn scope(&self) -> &CursorScope {
        match self {
            Self::Start(scope) => scope,
            Self::Resume(cursor) => &cursor.scope,
        }
    }
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

    /// Durably persist a checkpoint for the given account and scope.
    /// Consumers call this AFTER they have written the corresponding
    /// items into their own store. The engine acks (`auto = false`) so
    /// the ack writer can distinguish consumer-driven acks from the
    /// engine's own auto-ack path.
    ///
    /// `publication` is the `MultiplexerEvent::publication` that arrived with
    /// this checkpoint. It cannot be inferred from the checkpoint value,
    /// because two distinct publications can carry equal checkpoints while
    /// proving different coverage - so passing the wrong one, or none, would
    /// apply another publication's coverage claim to this checkpoint. An
    /// unrecognized publication is rejected rather than assumed complete.
    pub async fn ack_checkpoint(
        &self,
        account_id: &AccountId,
        scope: CursorScope,
        checkpoint: Checkpoint,
        publication: Option<crate::cursor::PublicationId>,
    ) -> Result<(), Error> {
        let tx = self
            .ack_senders
            .get(account_id)
            .map(|r| r.value().clone())
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let (complete_tx, complete_rx) = oneshot::channel();
        tx.send(WriterRequest::Ack(AckRequest {
            scope,
            checkpoint,
            publication,
            auto: false,
            complete: Some(complete_tx),
        }))
        .await
        .map_err(|e| Error::Other(format!("ack channel closed: {e}")))?;
        complete_rx
            .await
            .map_err(|e| Error::Other(format!("ack writer dropped before persisting: {e}")))?
    }

    /// Build the published backfill checkpoint helper on this account's single
    /// durable writer. The helper cannot race the acknowledgement ledger.
    pub fn backfill_checkpoint_writer(
        &self,
        account_id: &AccountId,
    ) -> Result<crate::backfill::BackfillCheckpointWriter, Error> {
        let tx = self
            .ack_senders
            .get(account_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        Ok(crate::backfill::BackfillCheckpointWriter::new(
            account_id.clone(),
            crate::backfill::BackfillCheckpointTarget::attached(tx),
        ))
    }

    /// Acknowledge an event publication that intentionally has no checkpoint.
    /// Repair notifications use this path so coverage debt is discharged only
    /// after the consumer has durably accepted the recovered object ids.
    pub async fn ack_publication(
        &self,
        account_id: &AccountId,
        publication: crate::cursor::PublicationId,
    ) -> Result<(), Error> {
        let tx = self
            .ack_senders
            .get(account_id)
            .map(|r| r.value().clone())
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let (done, wait) = oneshot::channel();
        tx.send(WriterRequest::AcknowledgePublication {
            publication: publication.clone(),
            done,
        })
        .await
        .map_err(|e| Error::Other(format!("ack channel closed: {e}")))?;
        wait.await
            .map_err(|e| Error::Other(format!("ack writer dropped before acknowledging: {e}")))?
    }

    /// Everything this account still owes: obligations no walk has resolved and
    /// regions its cursor cannot cross.
    ///
    /// The enumeration surface. `get_backfill` cannot serve it - that returns
    /// one selected row, so it could never list what a scope owes across all
    /// its partitions - and durable debt nothing can list is only half a fix.
    pub async fn debt(&self, account_id: &AccountId) -> Result<crate::cursor::DebtLedger, Error> {
        self.checkpoints.get_ledger(account_id).await
    }

    /// Run one repair pass over this account's open coverage debt.
    ///
    /// Asks the account to re-read what an enumeration could not represent,
    /// publishes recovered ids as ordinary `Created` signals, and discharges
    /// only what the consumer durably acknowledges. Returns how many
    /// obligations reached a terminal resolution.
    ///
    /// Skips anything waived, operator-blocked, or lacking a repair
    /// descriptor. That last case is every barrier, by construction: no
    /// checkpoint advanced past a barrier, so it is blocked progress rather
    /// than debt, and its only terminal state is [`Self::waive_obligation`].
    ///
    /// Deliberately caller-driven rather than scheduled. Repair is remote work
    /// against an account that may be throttled, paused, or degraded, and the
    /// consumer is better placed than the engine to decide when to spend that
    /// budget.
    pub async fn repair_debt(
        &self,
        account_id: &AccountId,
        max_requests: usize,
    ) -> Result<usize, Error> {
        let slot = self
            .accounts
            .get(account_id)
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let account = slot.current.load_full();
        let changes_tx = slot.multiplexer.changes_tx.clone();
        let coverage = Arc::clone(&slot.coverage);
        let writer_tx = self
            .ack_senders
            .get(account_id)
            .map(|r| r.value().clone())
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        drop(slot);

        let ledger = self.checkpoints.get_ledger(account_id).await?;
        crate::repair::run_repair_pass(
            account.as_ref().as_ref(),
            account_id,
            &ledger,
            Some(&changes_tx),
            &writer_tx,
            &coverage,
            max_requests,
        )
        .await
    }

    /// Accept the loss at `key`, permanently and on the record.
    ///
    /// The ONLY route to abandonment, and it is deliberately reachable only
    /// from an operator. A retry budget running out is evidence that automatic
    /// work is not helping, which is `OperatorBlocked` - it is not a decision
    /// about what loss is acceptable, and the account layer classifies
    /// evidence rather than choosing the user's policy.
    ///
    /// Changes POLICY only. The obligation stays `Unresolved` forever, because
    /// nothing was ever proved about it; what changes is that it stops blocking
    /// automatic backfill completion. An audit can therefore always tell proved
    /// coverage from loss somebody agreed to live with.
    ///
    /// Targets ONE occurrence, by the key from [`Self::debt`]. Waiving a
    /// failure CLASS - "this scope has id-less values" - would authorize every
    /// future instance sight unseen, which is a far larger decision than
    /// accepting a gap you can see.
    ///
    /// Returns whether anything matched `key`.
    pub async fn waive_obligation(
        &self,
        account_id: &AccountId,
        key: bifrost_types::ObligationKey,
        operator: String,
    ) -> Result<bool, Error> {
        self.operator_decision(
            account_id,
            key,
            OperatorDecision::Waive {
                by: operator,
                at_unix_seconds: jiff::Timestamp::now().as_second(),
            },
        )
        .await
    }

    /// Stop retrying `key` automatically WITHOUT accepting the loss.
    ///
    /// Stays visible, stays blocking, stays manually retryable. The honest
    /// state for "this is not working and someone needs to look at it".
    pub async fn block_obligation(
        &self,
        account_id: &AccountId,
        key: bifrost_types::ObligationKey,
    ) -> Result<bool, Error> {
        self.operator_decision(account_id, key, OperatorDecision::Block)
            .await
    }

    async fn operator_decision(
        &self,
        account_id: &AccountId,
        key: bifrost_types::ObligationKey,
        decision: OperatorDecision,
    ) -> Result<bool, Error> {
        // Through the account's single writer like every other durable
        // mutation: an operator decision races acknowledged checkpoints for the
        // same ledger, and serializing them is what makes a compare-and-swap on
        // the store unnecessary.
        let tx = self
            .ack_senders
            .get(account_id)
            .map(|r| r.value().clone())
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let (done, wait) = oneshot::channel();
        tx.send(WriterRequest::OperatorDecision {
            key,
            decision,
            done,
        })
        .await
        .map_err(|e| Error::Other(format!("writer channel closed: {e}")))?;
        wait.await
            .map_err(|e| Error::Other(format!("writer dropped before deciding: {e}")))?
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
    /// push subscriptions; call [`Self::unsubscribe_push`] first for that.
    ///
    /// Detach is the LAST point at which that call is possible: it drops
    /// this incarnation's push registry records, and afterwards
    /// `unsubscribe_push` rejects with `AccountNotAttached`. Records still
    /// present at detach are logged as stranded rather than torn down,
    /// because the contract places the teardown decision with the
    /// consumer. Push delivers to a consumer-owned endpoint, so an app
    /// that wants events to queue while it is down is a legitimate
    /// pattern that unconditional teardown would break silently.
    pub async fn detach(&self, account_id: &AccountId) -> Result<(), Error> {
        // Claim the lifecycle guard and remove the slot under the same
        // lock, so an `attach` cannot slip into the teardown window
        // below and have its brand-new registrations unregistered by
        // this call's tail. A concurrent attach or detach already owns
        // the transition: for an in-flight attach the slot is not
        // installed yet, and for an in-flight detach it is already
        // gone, so `AccountNotAttached` is the honest answer either way.
        {
            let mut guard = self.lifecycle_inflight.lock().await;
            if guard.contains(account_id) || !self.accounts.contains_key(account_id) {
                return Err(Error::AccountNotAttached(account_id.clone()));
            }
            guard.insert(account_id.clone());
        }
        let result = self.detach_inner(account_id).await;
        {
            let mut guard = self.lifecycle_inflight.lock().await;
            guard.remove(account_id);
        }
        result
    }

    /// Teardown body. Runs under the lifecycle guard taken by
    /// [`Self::detach`]; every registry cleanup at the tail assumes no
    /// other incarnation of this id can exist while it runs.
    async fn detach_inner(&self, account_id: &AccountId) -> Result<(), Error> {
        let Some((_, slot)) = self.accounts.remove(account_id) else {
            return Err(Error::AccountNotAttached(account_id.clone()));
        };
        // Drop the public ack sender so no new consumer acks enter
        // during teardown. Worker-held clones remain alive long enough
        // to flush their final checkpoint to the ack writer.
        //
        // The removed sender is KEPT here rather than discarded: teardown still
        // has one durable request to make (the backfill discards below), and
        // looking the key up again after removing it is a lookup that can only
        // ever answer `None`. It is dropped before the writer is awaited, so it
        // cannot hold the writer's channel open past its own drain.
        let teardown_writer = self.ack_senders.remove(account_id).map(|(_, tx)| tx);
        // Ask running workers to checkpoint cleanly, then stop.
        slot.boundary_tx.send_replace(BoundaryRequest::Stop);
        // Trip shutdown before awaiting workers. Some account-level
        // workers park on the shutdown token rather than the boundary
        // watch, so waiting first would always run to detach_timeout.
        slot.shutdown.cancel();
        // And pulse the capacity wake, so a producer parked on the backfill
        // bound leaves through its shutdown arm at once. The cancel alone is
        // enough - the wait selects on the token - but a cold start parked at the
        // bound behind a consumer that stopped acknowledging is exactly the case
        // that would otherwise be awaited for the whole `detach_timeout` and then
        // aborted mid-partition, so it is worth being explicit about.
        slot.coverage.wake_capacity();

        // Await spawned workers up to the configured timeout. Each
        // stored worker owns both its join and abort handles so a
        // timeout cannot detach a task and let it run forever.
        let timeout = self.config.detach_timeout;
        let mut drained: Vec<WorkerTask> = {
            let mut workers = slot.workers.lock().expect("worker list lock poisoned");
            workers.drain(..).collect()
        };
        // Wait for stream workers before the writer so final worker-held ack
        // sender clones can close naturally and the writer can drain
        // everything it received. Identify the writer structurally: spawn
        // order is not part of the teardown contract.
        let ack_worker = take_ack_writer(&mut drained);
        let deadline = tokio::time::Instant::now() + timeout;
        for worker in drained {
            await_worker_until(deadline, worker).await;
        }
        // Every loss recorded during this attachment asked for that scope's
        // backfill rows to be dropped, and the walk drains those requests at its
        // own end - but the orchestrator returns on shutdown from INSIDE its
        // partition loop, so a detach between a loss and the end of the walk
        // never reaches that drain. The rows left behind point past the hole and
        // carry no marker above them: harmless for a `Fixed` plan, which re-walks
        // everything, and a permanent hole for `OpenPages`, which resumes from
        // them. This is the last moment the writer is still alive, so it is where
        // the outstanding requests are settled.
        if let Some(writer) = teardown_writer {
            for scope in slot.coverage.take_backfill_discards() {
                let (done, recv) = oneshot::channel();
                if writer
                    .send(WriterRequest::DiscardBackfillProgress {
                        scope: scope.clone(),
                        done,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
                if let Ok(Err(error)) = recv.await {
                    tracing::warn!(
                        target: "bifrost.sync.backfill",
                        account = ?account_id,
                        scope = ?scope,
                        error = %error,
                        "could not drop the backfill rows of a walk that lost pages before \
                         detaching; a later attach may resume past them"
                    );
                }
            }
        }
        if let Some(worker) = ack_worker {
            await_worker_until(deadline, worker).await;
        }

        let current = slot.current.load_full();
        if let Err(e) = current.close().await {
            tracing::warn!(target: "bifrost.sync.changes", error=?e, "account close failed");
        }
        // Drop the push registry records for this incarnation.
        //
        // Nothing can reach them after this point - `unsubscribe_push`
        // rejects with `AccountNotAttached` and reopen only runs on an
        // attached slot - so leaving them behind does not preserve a
        // teardown opportunity; it only means a later attach of the same
        // `AccountId` inherits handles minted by a dead connection and
        // hands them to the provider as if they were live. A handle whose
        // connection is gone is not retryable, so dropping is strictly
        // better than carrying.
        //
        // This does NOT delete the server-side subscriptions: per
        // `reference/types.md` that stays the consumer's job via
        // `SyncEngine::unsubscribe_push` BEFORE detach. Records still
        // present here mean that call never happened, so the provider will
        // hold live subscriptions until it expires them on its own
        // schedule (24h for Graph). That is a consumer bug the contract
        // cannot prevent, so it is reported rather than silently absorbed.
        let stranded = self.subscriptions.take(account_id);
        if !stranded.is_empty() {
            tracing::warn!(
                target: "bifrost.sync.push",
                account = ?account_id,
                count = stranded.len(),
                "detached with live push subscriptions still registered; call \
                 SyncEngine::unsubscribe_push before detach or the provider \
                 keeps delivering until they expire",
            );
        }
        self.sink.unregister(account_id);
        self.scheduler.budget().forget(account_id);
        self.backfill_registry.forget_account(account_id);
        if let Ok(mut throttles) = self.throttles.lock() {
            throttles.forget_account(account_id);
        }
        if let Some(meter) = &self.bandwidth_meter {
            meter.forget_account(account_id);
        }
        Ok(())
    }

    /// Reopen and fully reattach an account slot. Scope and membership
    /// discovery, cursor topology, push subscriptions, capabilities,
    /// and the live protocol handle all refresh before the old handle
    /// is closed.
    ///
    /// A paused account is quiescent by contract, so this call waits for
    /// `resume_account` (or `Control::resume`) before opening a replacement.
    /// Shutdown while waiting returns `Error::ShuttingDown`.
    ///
    /// This is also the entry a consumer drives to pick up shares granted
    /// after the last open, paired with
    /// `capabilities().reopen_discovers_foreign_namespaces`. The CADENCE is
    /// consumer policy and the engine will never schedule speculative
    /// reopens; see `reference/sync.md`, the `reopen` paragraph, for the
    /// ruling and the alternatives that were rejected.
    pub async fn reopen(&self, account_id: &AccountId) -> Result<(), Error> {
        let slot = self
            .accounts
            .get(account_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let writer_tx = self
            .ack_senders
            .get(account_id)
            .map(|r| r.value().clone())
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let writer = WriterHandle::new(writer_tx);
        let ctx = RecoveryContext {
            factory: &slot.factory,
            current: &slot.current,
            cursors: &slot.cursors,
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
            open_skips: &slot.open_skips,
            shutdown: &slot.shutdown,
            writer: &writer,
            coverage: &slot.coverage,
        };
        loop {
            if !slot.control.wait_until_running(&slot.shutdown).await {
                return Err(Error::ShuttingDown);
            }
            match open_replacement(&ctx).await {
                Ok((reopen_guard, activity, next)) => {
                    let result = reattach_account(&ctx, activity, next).await;
                    drop(reopen_guard);
                    return result;
                }
                // A pause won the race between the wait above and the activity
                // registration, and nothing was opened. Keep this public
                // request queued until the account runs again.
                Err(ReplacementOpen::Paused) => continue,
                // The slot is going away under us. Report it as what it is
                // rather than looping into another open.
                Err(ReplacementOpen::Detached) => {
                    return Err(Error::AccountNotAttached(account_id.clone()));
                }
                // `reopen` is the post-attach swap path. Per the engine error
                // policy, failures in this path use `Account` (not
                // `OpenFailed`) so callers know the account was running and
                // the engine is reporting an in-flight failure.
                Err(ReplacementOpen::Failed(error)) => return Err(Error::Account(error)),
            }
        }
    }

    /// The `OpenedAccount::skipped_scopes` lane from the account's most
    /// recent successful open (initial attach or reopen swap): parts of
    /// the account surface - typically foreign / shared namespaces -
    /// the protocol crate discovered but could not bring up, each with
    /// its classified `AccountError`. Empty when the whole discovered
    /// surface is live. A skip whose error is retryable heals on a
    /// later `reopen`; a terminal one (revoked grant) will keep
    /// reappearing until the grant returns or goes away.
    pub fn open_skipped_scopes(&self, account_id: &AccountId) -> Result<Vec<SkippedScope>, Error> {
        let slot = self
            .accounts
            .get(account_id)
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        let skips = slot
            .open_skips
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(skips.clone())
    }

    /// Subscribe to the per-account unified change stream. Each
    /// subscriber gets its own broadcast receiver; missed events
    /// (slow consumer) are dropped per `tokio::sync::broadcast`'s
    /// lagging semantics.
    pub fn account_changes_stream(
        &self,
        account_id: &AccountId,
    ) -> Result<crate::multiplexer::ChangesReceiver, Error> {
        let slot = self
            .accounts
            .get(account_id)
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        // Subscribing and numbering happen together inside the delivery gate. A
        // `tokio::broadcast` receiver joins at the ring's TAIL, so it can never
        // take delivery of - and therefore never acknowledge - anything broadcast
        // before this moment; the number is what lets the engine say so later,
        // and it is exact only because no backfill send can interleave between
        // the subscribe and the numbering.
        let receiver = slot
            .delivery
            .subscribe(Some(slot.control.clone()), Some(Arc::clone(&slot.coverage)));
        // Wake any deferred-inventory workers parked on the Notify so
        // they observe the new subscriber without hot-polling.
        slot.subscriber_notify.notify_waiters();
        Ok(receiver)
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
        // Serialize against a concurrent reopen. Reattach snapshots the
        // registry, tears old handles down, and installs a replacement set;
        // a take/restore interleaved with that window would either resurrect
        // records the consumer just tore down or race the handle swap.
        let _reopen_guard = slot.reopen_lock.lock().await;
        let records = self.subscriptions.take(account_id);
        let account = slot.current.load_full();
        let mut failed = Vec::new();
        let mut first_error = None;
        for record in records {
            if let Err(error) = account.push_unsubscribe(record.handle.clone()).await {
                tracing::warn!(target: "bifrost.sync.reconcile", error=?error, "push_unsubscribe failed; retaining handle for retry");
                if first_error.is_none() {
                    first_error = Some(error);
                }
                // The server side may still be live. Keep the record, but flag
                // it so a reopen carries it for retry instead of recreating it
                // against the replacement connection.
                failed.push(RegisteredSubscription {
                    teardown_unconfirmed: true,
                    ..record
                });
            }
        }
        self.subscriptions.restore(account_id.clone(), failed);
        match first_error {
            Some(error) => Err(Error::Account(error)),
            None => Ok(()),
        }
    }

    /// Engine-side push subscription request. The engine stashes the
    /// returned handle in its registry so a later `unsubscribe_push`
    /// can find it.
    pub async fn subscribe_push(
        &self,
        account_id: &AccountId,
        scopes: &[CursorScope],
    ) -> Result<bifrost_types::PushSubscription, Error> {
        let slot = self
            .accounts
            .get(account_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        // Serialize against a concurrent reopen. Reattach replaces the
        // registry rows wholesale from a snapshot; a registration landing
        // between that snapshot and the swap would be silently erased while
        // its server-side subscription - created on a handle about to be
        // closed - kept delivering with nothing left able to tear it down.
        let _reopen_guard = slot.reopen_lock.lock().await;
        let account = slot.current.load_full();
        let result = account.push_subscribe(scopes).await?;
        let covered = accepted_push_scopes(&result);
        for failure in result.outcomes.failed() {
            tracing::warn!(
                target: "bifrost.sync.reconcile",
                error = ?failure.error,
                "push scope rejected; retaining polling coverage"
            );
        }
        if let Some(handle) = &result.handle {
            self.subscriptions
                .record(account_id.clone(), handle.clone(), covered);
        }
        Ok(result)
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
