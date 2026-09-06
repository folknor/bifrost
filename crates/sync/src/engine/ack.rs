//! The account's single durable ack writer and its handle.

use super::*;

/// Remove the account's single ack-writer worker from a drained worker list.
///
/// Detach must wait on the stream workers FIRST: every one of them holds a
/// clone of the writer's `WriterRequest` sender, and the writer only sees its
/// channel close (and only then drains and persists what it already received)
/// once the last clone is gone. Waiting on the writer first therefore hits the
/// detach timeout and aborts a writer with unpersisted work.
///
/// The writer is identified by ROLE, not by position. It happens to be
/// spawned first, and the predecessor of this function read `drained[0]` on
/// that basis - a coupling between spawn order and teardown order that
/// nothing announced and that any future reordering of the spawn block would
/// have silently broken, mistaking a stream worker for the writer and then
/// waiting on the real writer in the wrong phase.
pub(super) fn take_ack_writer(workers: &mut Vec<WorkerTask>) -> Option<WorkerTask> {
    workers
        .iter()
        .position(|worker| worker.role == crate::types::WorkerRole::AckWriter)
        .map(|position| workers.remove(position))
}

pub(super) async fn await_worker_until(deadline: tokio::time::Instant, worker: WorkerTask) {
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

/// Ack writer task. One per attached account. Receives `AckRequest`
/// messages on `rx` and durably persists the carried checkpoint via
/// the `CheckpointStore`. Notifies the control's checkpoint watch so
/// `pause()` / `checkpoint_now()` waiters wake on a real persisted
/// boundary. Exits when the channel closes (slot detach).
pub(super) async fn ack_writer(
    account_id: AccountId,
    store: Arc<DynCheckpointStore>,
    control: SyncControl,
    coverage: Arc<PendingCoverage>,
    mut rx: mpsc::Receiver<WriterRequest>,
) {
    // Scopes whose durable row exists ONLY because an uncommitted reattach put
    // it there. An abort may delete exactly these and nothing else.
    //
    // This is the fence that makes serialization sufficient. A plain FIFO queue
    // would order an acknowledged write ahead of the abort's unconditional
    // delete and then faithfully destroy it; tracking which rows are still
    // provisional turns the abort into a conditional delete, implemented here
    // rather than pushed onto every `CheckpointStore` backend as a
    // compare-and-swap it could get subtly wrong.
    let mut provisional: HashSet<CursorScope> = HashSet::new();
    let mut pending_repairs: HashMap<
        crate::cursor::PublicationId,
        Vec<crate::repair::RepairResolution>,
    > = HashMap::new();
    let mut acknowledged_publications: HashSet<crate::cursor::PublicationId> = HashSet::new();
    // The authoritative debt ledger for this account. Held here rather than
    // re-read per write because this task is the only thing that may mutate it,
    // which is what makes generation-aware conditional transitions possible
    // without a compare-and-swap on every `CheckpointStore` backend.
    let mut ledger = match store.get_ledger(&account_id).await {
        Ok(ledger) => ledger,
        Err(error) => {
            tracing::error!(
                target: "bifrost.sync.changes",
                account = ?account_id,
                error = %error,
                "failed to load debt ledger; starting empty, which can only UNDER-report debt"
            );
            crate::cursor::DebtLedger::default()
        }
    };
    while let Some(req) = rx.recv().await {
        let req = match req {
            WriterRequest::Ack(req) => {
                // An acknowledgement is real committed consumer progress, so
                // the row is no longer merely provisional even if a reattach
                // inserted it. A replacement's inventory pass broadcasts
                // checkpoint-bearing batches BEFORE cutover, so this genuinely
                // happens: without the discharge, an abort would delete a
                // cursor the consumer had already persisted against.
                provisional.remove(&req.scope);
                req
            }
            WriterRequest::ReattachInsert { cursor, done } => {
                let scope = cursor.scope.clone();
                // Carries NO coverage report, which means "leave the ledger
                // unchanged" - emphatically not "coverage is complete". A
                // reattach writes a cursor it obtained from establishment; it
                // proves nothing about what any enumeration covered, and
                // manufacturing a completeness claim here would discharge debt
                // that nothing re-read.
                let result = store
                    .apply_transition(
                        &account_id,
                        CheckpointTransition {
                            checkpoint: Checkpoint::Change(cursor),
                            ledger: ledger.clone(),
                        },
                    )
                    .await;
                if result.is_ok() {
                    provisional.insert(scope);
                }
                let _ = done.send(result);
                continue;
            }
            WriterRequest::RecordBarrier { incident, done } => {
                ledger.record_barrier(incident);
                // No checkpoint advanced, so there is nothing to write
                // alongside it. The ledger still has to reach disk, or the
                // barrier is forgotten on restart and no operator can act on
                // it. Persisted against the scope's existing durable position.
                let result = persist_ledger_only(&account_id, &store, &ledger).await;
                let _ = done.send(result);
                continue;
            }
            WriterRequest::CrossWaivedBarriers {
                report,
                generation,
                done,
            } => {
                let mut crossed = ledger.clone();
                if !crossed.cross_waived_barriers(
                    &report,
                    generation,
                    jiff::Timestamp::now().as_second(),
                ) {
                    let _ = done.send(Ok(false));
                    continue;
                }
                let result = persist_ledger_only(&account_id, &store, &crossed).await;
                if result.is_ok() {
                    ledger = crossed;
                }
                let _ = done.send(result.map(|()| true));
                continue;
            }
            WriterRequest::ScopeBarrierBlocked { scope, done } => {
                let _ = done.send(ledger.scope_has_blocked_barrier(&scope));
                continue;
            }
            WriterRequest::ApplyRepair {
                resolutions,
                publication,
                done,
            } => {
                // A repair discharges only once the CONSUMER has acknowledged
                // the batch of recovered ids. The acknowledgement can arrive
                // either side of this request, so the resolutions park here
                // until it does.
                let acknowledged = match publication {
                    Some(publication) => {
                        if acknowledged_publications.remove(&publication) {
                            true
                        } else {
                            park_pending_repair(
                                &account_id,
                                &mut pending_repairs,
                                publication,
                                resolutions,
                            );
                            let _ = done.send(Ok(()));
                            continue;
                        }
                    }
                    // Never published at all: nothing recovered may discharge.
                    None => false,
                };
                let result = apply_repair_resolutions(
                    &account_id,
                    &store,
                    &mut ledger,
                    resolutions,
                    acknowledged,
                )
                .await;
                let _ = done.send(result);
                continue;
            }
            WriterRequest::AcknowledgePublication { publication, done } => {
                // The REPAIR lane specifically: a checkpoint publication's id
                // resolves to `Unknown` here rather than being consumed, so a
                // consumer cannot discharge a checkpoint through this path and
                // leave the later `ack_checkpoint` short-circuiting as already
                // persisted over a store write that never happened.
                let result = match coverage.claim_repair(publication.clone()) {
                    ClaimLookup::Apply(_) => {
                        if let Some(resolutions) = pending_repairs.remove(&publication) {
                            let outcome = apply_repair_resolutions(
                                &account_id,
                                &store,
                                &mut ledger,
                                resolutions,
                                true,
                            )
                            .await;
                            if outcome.is_ok() {
                                coverage.settle_repair(publication.clone());
                            }
                            outcome
                        } else {
                            acknowledged_publications.insert(publication.clone());
                            coverage.settle_repair(publication);
                            Ok(())
                        }
                    }
                    // Idempotent: this publication was already applied.
                    ClaimLookup::AlreadyPersisted => Ok(()),
                    ClaimLookup::Unknown => Err(Error::CheckpointStore(
                        "acknowledgement names an unknown publication".into(),
                    )),
                };
                let _ = done.send(result);
                continue;
            }
            WriterRequest::RecordDebt {
                report,
                generation,
                done,
            } => {
                ledger.ingest_debt_only(&report, generation, jiff::Timestamp::now().as_second());
                let result = persist_ledger_only(&account_id, &store, &ledger).await;
                let _ = done.send(result);
                continue;
            }
            WriterRequest::OperatorDecision {
                key,
                decision,
                done,
            } => {
                let changed = match decision {
                    OperatorDecision::Waive {
                        by,
                        at_unix_seconds,
                    } => ledger.waive(&key, by, at_unix_seconds),
                    OperatorDecision::Block => ledger.block(&key),
                };
                let result = if changed {
                    persist_ledger_only(&account_id, &store, &ledger)
                        .await
                        .map(|()| true)
                } else {
                    Ok(false)
                };
                let _ = done.send(result);
                continue;
            }
            WriterRequest::ReattachAbort { done } => {
                for scope in provisional.drain() {
                    if let Err(error) = store.delete_change_cursor(&account_id, &scope).await {
                        tracing::error!(
                            target: "bifrost.sync.reopen",
                            account = ?account_id,
                            scope = ?scope,
                            error = %error,
                            "failed to roll back replacement cursor after aborted reattach"
                        );
                    }
                }
                let _ = done.send(());
                continue;
            }
            WriterRequest::ReattachCommit => {
                provisional.clear();
                continue;
            }
            WriterRequest::GetChangeCursor { scope, done } => {
                let result = store.get_change_cursor(&account_id, &scope).await;
                let _ = done.send(result);
                continue;
            }
            WriterRequest::PersistEstablished { cursor, done } => {
                let result = store
                    .apply_transition(
                        &account_id,
                        CheckpointTransition {
                            checkpoint: Checkpoint::Change(cursor),
                            ledger: ledger.clone(),
                        },
                    )
                    .await;
                let _ = done.send(result);
                continue;
            }
            WriterRequest::PersistBackfill { checkpoint, done } => {
                let result = store
                    .apply_transition(
                        &account_id,
                        CheckpointTransition {
                            checkpoint: Checkpoint::Backfill(checkpoint),
                            ledger: ledger.clone(),
                        },
                    )
                    .await;
                let _ = done.send(result);
                continue;
            }
            WriterRequest::DiscardBackfillProgress { scope, done } => {
                // The change cursor stays and nothing re-establishes: this is not
                // a reset, the scope is healthy and its walk simply cannot be
                // resumed from where it stopped.
                //
                // But deleting the rows is not enough on its own, because the
                // publications of the discarded attempt are still outstanding. A
                // replacement consumer holding a later page can let this delete
                // finish and acknowledge afterwards, and the writer would honour
                // it and RECREATE exactly the resume position past the hole that
                // the discard existed to remove. So the discarded attempt is
                // fenced by the same machinery a reset uses: `invalidate_scope`
                // retires the scope's publications, raises the acknowledgement
                // fence past every id minted before now, and hands back their
                // debt - which must be ingested here or the obligations vanish
                // with the publications carrying them.
                //
                // Two passes for the reason the reset has two: the delete below
                // is an await, and a still-running walk can register a further
                // publication inside it.
                // Unbounded: the orchestrator sends this BEFORE its retry mints
                // anything, so there is nothing newer to protect.
                let discarded =
                    discard_backfill(&account_id, &store, &coverage, &mut ledger, &scope, None)
                        .await;
                let _ = done.send(discarded);
                continue;
            }
            WriterRequest::ResetScope {
                scope,
                delete_backfill,
                done,
            } => {
                provisional.remove(&scope);
                // Nothing here touches backfill capacity explicitly, and that is
                // the fix rather than an omission. `invalidate_scope` removes the
                // scope's boundary registrations, and those registrations ARE the
                // capacity, so both passes free it atomically with the retirement
                // - including a page the still-running walk published inside the
                // store awaits below, and including pages a later publication had
                // already superseded. The previous shape kept a separate permit
                // map and needed a reset counter, a per-pass id list, and a rule
                // about registrations racing retirements; each of those was a
                // seam, and each seam was a defect.
                // Retiring the scope's publications and extracting their debt
                // is ONE ledger operation, and it happens BEFORE the first
                // await. Snapshotting the debt first and invalidating after the
                // store writes left an await window in which a still-running
                // scope could register a further publication: invalidation
                // would then retire it, but its debt was never in the
                // persisted snapshot, so the obligation vanished.
                let mut debt = coverage.invalidate_scope(&scope);
                let now = jiff::Timestamp::now().as_second();
                for report in &debt.reports {
                    ledger.ingest_debt_only(report, debt.generation, now);
                }
                let result = async {
                    persist_ledger_only(&account_id, &store, &ledger).await?;
                    if delete_backfill {
                        store.delete_backfill(&account_id, &scope).await?;
                    }
                    store.delete_change_cursor(&account_id, &scope).await?;
                    Ok(())
                }
                .await;
                // Second pass, closing the window the deletes just opened.
                // Anything the still-draining scope published while they were
                // in flight is retired here, its debt carried, and the fence
                // moved past it - so a late acknowledgement cannot re-create
                // the cursor row this reset deleted. Ids minted after this
                // point (the re-establish) sit above the fence.
                debt = coverage.invalidate_scope(&scope);
                if !debt.reports.is_empty() {
                    let now = jiff::Timestamp::now().as_second();
                    for report in &debt.reports {
                        ledger.ingest_debt_only(report, debt.generation, now);
                    }
                    if let Err(error) = persist_ledger_only(&account_id, &store, &ledger).await {
                        tracing::warn!(
                            target: "bifrost.sync.changes",
                            account = ?account_id,
                            scope = ?scope,
                            error = %error,
                            "scope reset could not persist debt published during its deletes"
                        );
                    }
                }
                let _ = done.send(result);
                continue;
            }
        };
        // No lane bookkeeping here, deliberately. Backfill capacity IS the
        // publication's boundary registration, so each arm below frees it by
        // doing what it already does to the ledger: `record_publication`
        // acknowledges, `retire_publication` retires. An unconditional release
        // beside them was a defect rather than a shortcut - it fired on the
        // REJECTED arm too, where the consumer's token named a different lane
        // from its checkpoint, and released a live sibling publication's
        // capacity on the strength of an acknowledgement the ledger had just
        // refused.
        let result = persist_ack_request(&account_id, &store, &coverage, &mut ledger, &req).await;
        match result {
            Ok(AckPersistOutcome::Withheld) => {
                // The completion sentinel was deliberately NOT written: the
                // scope still carries open debt, so only the ledger landed.
                // Nothing durable was created for this publication, so the
                // watermark must not move (a retried ack has to report
                // `Unknown` and be re-evaluated, never `AlreadyPersisted`) and
                // the boundary must not be announced durable - the store holds
                // no such row and the next attach re-walks. The publication is
                // retired instead, exactly as a failed write retires it, so
                // boundary waiters stop being gated. The consumer's ack itself
                // succeeded: its batch was empty and was honoured.
                if let Some(publication) = req.publication {
                    control.retire_publication(publication);
                }
                if let Some(done) = req.complete {
                    let _ = done.send(Ok(()));
                }
            }
            Ok(AckPersistOutcome::Durable) => {
                // Notify pause / checkpoint_now waiters AFTER the
                // durable write lands - the contract is that the
                // returned checkpoint has been persisted. The
                // watermark moves here for the same reason: a retried
                // acknowledgement may only report "already persisted"
                // for a write that actually landed.
                if let Some(publication) = req.publication.clone() {
                    coverage.settle_checkpoint(publication, &req.checkpoint);
                }
                control
                    .record_publication(req.publication, req.checkpoint)
                    .await;
                if let Some(done) = req.complete {
                    let _ = done.send(Ok(()));
                }
            }
            Err(AckFailure::Store(err)) => {
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
                // from `ack_checkpoint`'s own `Result`, below. The
                // watermark deliberately does NOT move: a retry of
                // this acknowledgement must report `Unknown` and fail
                // again, never "already persisted" for a write that
                // never landed.
                if let Some(publication) = req.publication {
                    control.retire_publication(publication);
                }
                if let Some(done) = req.complete {
                    let _ = done.send(Err(err));
                }
            }
            Err(AckFailure::Rejected(err)) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?account_id,
                    scope = ?req.scope,
                    error = %err,
                    auto = req.auto,
                    "ack: acknowledgement refused before any durable write"
                );
                // NOTHING is retired here, and the distinction from a store
                // failure is the point. A store failure is evidence the consumer
                // took delivery and answered; a REJECTION is evidence the
                // acknowledgement did not name what it claimed to - a token from
                // one lane presented with another lane's checkpoint, or an
                // undecodable envelope. Acting on it would retire, and so free
                // the capacity of, a publication that is still perfectly live and
                // still awaiting its own acknowledgement.
                if let Some(done) = req.complete {
                    let _ = done.send(Err(err));
                }
            }
        }
    }
}

/// The only handle recovery code receives for durable state. It can request
/// reads and mutations, but cannot reach the consumer's store implementation.
#[derive(Clone)]
pub(crate) struct WriterHandle {
    tx: mpsc::Sender<WriterRequest>,
}

impl WriterHandle {
    pub(super) fn new(tx: mpsc::Sender<WriterRequest>) -> Self {
        Self { tx }
    }

    pub(super) async fn get_change_cursor(
        &self,
        scope: CursorScope,
    ) -> Result<Option<ChangeCursor>, Error> {
        let (done, recv) = oneshot::channel();
        self.tx
            .send(WriterRequest::GetChangeCursor { scope, done })
            .await
            .map_err(|error| Error::Other(format!("writer channel closed: {error}")))?;
        recv.await
            .map_err(|error| Error::Other(format!("writer dropped before reading: {error}")))?
    }

    pub(super) async fn persist_established(&self, cursor: ChangeCursor) -> Result<(), Error> {
        let (done, recv) = oneshot::channel();
        self.tx
            .send(WriterRequest::PersistEstablished { cursor, done })
            .await
            .map_err(|error| Error::Other(format!("writer channel closed: {error}")))?;
        recv.await
            .map_err(|error| Error::Other(format!("writer dropped before persisting: {error}")))?
    }

    /// Routine cursor invalidation: drop the change cursor so the next
    /// establish re-runs, and PRESERVE backfill state.
    ///
    /// The completion marker is what stops the next attach from re-walking and
    /// re-hydrating the scope's entire history. Deleting it on every cursor
    /// invalidation - which is what `RestartScope` is - would make an ordinary
    /// recovery cost a full historical inventory pass for no schema reason.
    /// The naming exists so that choice is stated at the call site instead of
    /// riding on a bare boolean.
    pub(super) async fn reset_scope_for_restart(&self, scope: CursorScope) -> Result<(), Error> {
        self.reset_scope(scope, false).await
    }

    /// A scope being taken out of service. Same durable footprint as a restart;
    /// nothing re-establishes afterwards.
    pub(super) async fn reset_scope_for_disable(&self, scope: CursorScope) -> Result<(), Error> {
        self.reset_scope(scope, false).await
    }

    /// A provider-deleted scope (folder gone): drop the backfill rows too,
    /// completion marker included. Unlike a disable, the folder can come
    /// back under the same id, and a surviving completion marker would make
    /// the recreated incarnation skip its entire cold-start walk - the
    /// consumer plausibly purged its data on `Deleted`, so that is data
    /// invisibility, not a mere leak.
    pub(super) async fn reset_scope_for_deletion(&self, scope: CursorScope) -> Result<(), Error> {
        self.reset_scope(scope, true).await
    }

    /// Schema recovery: drop the backfill rows too, completion marker included.
    /// The re-walk is the point - it re-mints ids under the new encoding, and
    /// the marker would make the next attach skip it.
    pub(super) async fn reset_scope_for_schema_recovery(
        &self,
        scope: CursorScope,
    ) -> Result<(), Error> {
        self.reset_scope(scope, true).await
    }

    async fn reset_scope(&self, scope: CursorScope, delete_backfill: bool) -> Result<(), Error> {
        let (done, recv) = oneshot::channel();
        self.tx
            .send(WriterRequest::ResetScope {
                scope,
                delete_backfill,
                done,
            })
            .await
            .map_err(|error| Error::Other(format!("writer channel closed: {error}")))?;
        recv.await
            .map_err(|error| Error::Other(format!("writer dropped before reset: {error}")))?
    }

    pub(super) fn sender(&self) -> mpsc::Sender<WriterRequest> {
        self.tx.clone()
    }

    pub(super) async fn reattach_insert(&self, cursor: ChangeCursor) -> Result<(), Error> {
        let (done, recv) = oneshot::channel();
        self.tx
            .send(WriterRequest::ReattachInsert { cursor, done })
            .await
            .map_err(|error| Error::Other(format!("writer channel closed: {error}")))?;
        recv.await
            .map_err(|error| Error::Other(format!("writer dropped before persisting: {error}")))?
    }

    pub(super) async fn reattach_abort(&self) {
        let (done, recv) = oneshot::channel();
        if self
            .tx
            .send(WriterRequest::ReattachAbort { done })
            .await
            .is_ok()
        {
            let _ = recv.await;
        }
    }

    pub(super) async fn reattach_commit(&self) -> Result<(), Error> {
        self.tx
            .send(WriterRequest::ReattachCommit)
            .await
            .map_err(|error| Error::Other(format!("writer channel closed: {error}")))
    }
}

/// Drop a scope's durable backfill rows AND fence the attempt that produced
/// them, as one writer-ordered operation.
///
/// Called both for the orchestrator's explicit discard and from the ack path
/// when a completion marker turns out to belong to a walk that lost pages: in
/// both cases every publication of that attempt is void, and leaving them
/// acknowledgeable is what lets a late ack rebuild the resume position the
/// discard just removed.
async fn discard_backfill(
    account_id: &AccountId,
    store: &Arc<DynCheckpointStore>,
    coverage: &PendingCoverage,
    ledger: &mut crate::cursor::DebtLedger,
    scope: &CursorScope,
    through: Option<&crate::cursor::PublicationId>,
) -> Result<(), Error> {
    // The BACKFILL half only. Fencing the whole scope refused a replacement
    // consumer's acknowledgement of live batches it had really received, with an
    // `Unknown` no retry could ever satisfy and no instruction to reconcile -
    // after which the live cursor advances past changes nothing replayed. The
    // discard is about this scope's backfill rows and about nothing else.
    // The ceiling bounds the ledger retirement; it cannot bound the DELETE.
    // `CheckpointStore::delete_backfill` takes every row of the scope and knows
    // nothing of publication ids, so a delayed acknowledgement of an old walk's
    // marker - refused, correctly, as `WalkNotWhole` - would delete the rows a
    // LATER attempt earned, up to and including a durable completion its consumer
    // had already acknowledged. When a newer attempt exists, the fence is the
    // whole of what this discard may safely do: the old attempt stops being
    // acknowledgeable, the request stays outstanding (a ceilinged discard never
    // takes it), and the newer attempt's own repair - the rescan's reopen and its
    // unbounded pre-retry discard - is what removes rows if they need removing.
    let rows_belong_to_a_newer_attempt =
        through.is_some_and(|ceiling| coverage.backfill_minted_after(scope, ceiling));
    let mut debt = coverage.invalidate_scope_backfill(scope, through);
    let now = jiff::Timestamp::now().as_second();
    for report in &debt.reports {
        ledger.ingest_debt_only(report, debt.generation, now);
    }
    let result = async {
        persist_ledger_only(account_id, store, ledger).await?;
        if rows_belong_to_a_newer_attempt {
            tracing::warn!(
                target: "bifrost.sync.backfill",
                account = ?account_id,
                scope = ?scope,
                "a later backfill attempt has published on this scope; fencing the refused \
                 attempt without deleting rows that are not its own"
            );
            return Ok(());
        }
        store.delete_backfill(account_id, scope).await
    }
    .await;
    // Second pass, closing the window the delete just opened.
    debt = coverage.invalidate_scope_backfill(scope, through);
    if !debt.reports.is_empty() {
        let now = jiff::Timestamp::now().as_second();
        for report in &debt.reports {
            ledger.ingest_debt_only(report, debt.generation, now);
        }
        if let Err(error) = persist_ledger_only(account_id, store, ledger).await {
            tracing::warn!(
                target: "bifrost.sync.backfill",
                account = ?account_id,
                scope = ?scope,
                error = %error,
                "could not persist debt published while a backfill discard was in flight"
            );
        }
    }
    // The request is settled only by a delete that SUCCEEDED, and only by an
    // UNBOUNDED discard.
    //
    // Success, because a store error leaves the rows exactly where they were: a
    // request consumed by a delete that did not happen is a repair nobody will
    // attempt again, and the teardown drain finds nothing to retry. Unbounded,
    // because a ceilinged discard retires only the attempt at or below the
    // refused marker while its delete takes every row - so a delayed W1 marker
    // acknowledgement arriving during W2 would clear W2's request while leaving
    // W2's own loss unrepaired.
    if through.is_none() && result.is_ok() {
        coverage.take_backfill_discard(scope);
    }
    result
}

/// Write the ledger with no checkpoint advance.
async fn persist_ledger_only(
    account_id: &AccountId,
    store: &Arc<DynCheckpointStore>,
    ledger: &crate::cursor::DebtLedger,
) -> Result<(), Error> {
    store.put_ledger(account_id, ledger.clone()).await
}

/// Ceiling on repair passes parked waiting for a consumer acknowledgement.
///
/// A consumer that never acknowledges repair batches would otherwise
/// accumulate one entry per pass for the life of the attachment. Evicting the
/// oldest costs nothing durable: its obligations were never discharged, so they
/// are still owed and a later pass retries them.
const PARKED_REPAIR_CAP: usize = 64;

fn park_pending_repair(
    account_id: &AccountId,
    parked: &mut HashMap<crate::cursor::PublicationId, Vec<crate::repair::RepairResolution>>,
    publication: crate::cursor::PublicationId,
    resolutions: Vec<crate::repair::RepairResolution>,
) {
    if parked.len() >= PARKED_REPAIR_CAP
        && let Some(oldest) = parked.keys().min().cloned()
    {
        parked.remove(&oldest);
        tracing::warn!(
            target: "bifrost.sync.repair",
            account = ?account_id,
            cap = PARKED_REPAIR_CAP,
            "repair resolutions awaiting acknowledgement at capacity; the oldest pass stays owed"
        );
    }
    parked.insert(publication, resolutions);
}

/// Fold one repair pass's outcomes into the ledger.
///
/// The ordering that matters: a `Recovered` resolution discharges ONLY when the
/// consumer has acknowledged the publication carrying its id. Discharging
/// before that recreates the original silent loss - the engine would forget the
/// obligation while the consumer never learned the object exists. If the
/// publication was never acknowledged (no subscriber, lag, cancellation) the
/// obligation simply stays open and a later pass tries again; a crash between
/// publish and acknowledgement costs a duplicate recovery, which is the same
/// at-least-once asymmetry ordinary checkpoint delivery already has.
async fn apply_repair_resolutions(
    account_id: &AccountId,
    store: &Arc<DynCheckpointStore>,
    ledger: &mut crate::cursor::DebtLedger,
    resolutions: Vec<crate::repair::RepairResolution>,
    published_and_acknowledged: bool,
) -> Result<(), Error> {
    let now = jiff::Timestamp::now().as_second();

    for resolution in resolutions {
        match resolution {
            crate::repair::RepairResolution::Recovered {
                key,
                attempt,
                generation,
            } => {
                if !published_and_acknowledged {
                    // Recovered but never delivered. Costs an attempt and stays
                    // owed, which is the conservative direction.
                    ledger.record_attempt(&key, crate::repair::DEFAULT_REPAIR_BUDGET);
                    continue;
                }
                if !ledger.discharge_repaired(
                    &key,
                    generation,
                    crate::cursor::DischargeEvidence::RepairedAndPublished { attempt },
                ) {
                    tracing::debug!(
                        target: "bifrost.sync.repair",
                        account = ?account_id,
                        "repair result refused: the obligation moved on since it was read"
                    );
                }
            }
            crate::repair::RepairResolution::Irrelevant {
                key,
                detail,
                generation,
            } => {
                // Nothing was published and nothing needs to be: absence from
                // an old inventory snapshot is not a deletion to apply against
                // current consumer state.
                ledger.discharge_repaired(
                    &key,
                    generation,
                    crate::cursor::DischargeEvidence::ProvedIrrelevant { detail },
                );
            }
            crate::repair::RepairResolution::Replaced {
                key,
                proof,
                generation,
            } => {
                apply_replacement(ledger, &key, &proof, generation, now);
            }
            crate::repair::RepairResolution::Deferred { key } => {
                ledger.record_attempt(&key, crate::repair::DEFAULT_REPAIR_BUDGET);
            }
        }
    }

    persist_ledger_only(account_id, store, ledger).await
}

/// Apply a region proof, which either discharges the region or splits it.
fn apply_replacement(
    ledger: &mut crate::cursor::DebtLedger,
    key: &bifrost_types::ObligationKey,
    proof: &bifrost_types::RegionRepairProof,
    generation: u64,
    now: i64,
) {
    match proof {
        // The account consumed the exact region its own durable token named and
        // accounted for every result. Identity is what is checked here, not set
        // inclusion - the lattice cannot do algebra over an opaque token.
        bifrost_types::RegionRepairProof::ExactReplay => {
            ledger.discharge_repaired(
                key,
                generation,
                crate::cursor::DischargeEvidence::ProvedIrrelevant {
                    detail: "exact replay of the named region accounted for every result".into(),
                },
            );
        }
        // Expressible in the lattice, so the engine checks it rather than
        // taking the account's word.
        bifrost_types::RegionRepairProof::CoveredBy { domain } => {
            let covers = ledger
                .entry(key)
                .is_some_and(|entry| domain.covers(&entry.domain));
            if covers {
                ledger.discharge_repaired(
                    key,
                    generation,
                    crate::cursor::DischargeEvidence::CoveringWalk {
                        domain: domain.clone(),
                    },
                );
            }
        }
        bifrost_types::RegionRepairProof::Partitioned {
            proved, residual, ..
        } => {
            match ledger.replace_obligation(key, proved, residual, generation, now) {
                Ok(crate::cursor::ReplacementProgress::Stalled) => {
                    // Reshaped without improving anything. Charged against the
                    // lineage so an account cannot loop forever by rotating
                    // keys and tokens over the same unresolved extent.
                    ledger.record_attempt(key, crate::repair::DEFAULT_REPAIR_BUDGET);
                }
                Ok(crate::cursor::ReplacementProgress::Progressed) => {}
                Err(refusal) => {
                    tracing::warn!(
                        target: "bifrost.sync.repair",
                        refusal = ?refusal,
                        "refusing a region replacement"
                    );
                    ledger.record_attempt(key, crate::repair::DEFAULT_REPAIR_BUDGET);
                }
            }
        }
        _ => {}
    }
}

/// Persist one acknowledged checkpoint together with everything it proves.
///
/// Three things happen here and they are one indivisible decision:
///
/// 1. The publication's coverage claim is resolved and folded into the ledger.
///    A claim is applied when the CONSUMER acknowledges, never when the account
///    emitted it - an unacknowledged report may describe entries the consumer
///    never persisted, so it cannot be allowed to prove anything.
/// 2. If the checkpoint is a backfill completion sentinel, eligibility is
///    evaluated against the ledger AS IT NOW STANDS. The runner's earlier
///    `complete` flag is not authoritative: a sibling partition's debt may have
///    been accepted since, and writing the sentinel over it makes the next
///    attach skip a scope with open obligations.
/// 3. The checkpoint and the resulting ledger land in ONE store operation.
///
/// The outcome distinguishes "the checkpoint is durable" from "only the ledger
/// was written because the completion sentinel was withheld". The caller must
/// not settle the watermark or announce a durable boundary for the latter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckPersistOutcome {
    /// The acknowledged checkpoint itself is in the store.
    Durable,
    /// The checkpoint was deliberately NOT written: a completion sentinel the
    /// ledger refused, or a backfill row acknowledged from a prior attachment's
    /// publication. Only the ledger was persisted, so nothing durable exists for
    /// this checkpoint.
    Withheld,
}

/// Why an acknowledgement produced nothing durable.
///
/// Two cases that look alike and must not be treated alike. `Store` means the
/// consumer's acknowledgement was valid and the write failed, so the batch is
/// no longer in flight and its registration must be retired. `Rejected` means
/// the acknowledgement never named a publication this ledger would honour - a
/// mismatched lane, an undecodable envelope - so it is evidence about the
/// CALLER, not about delivery, and retiring anything on the strength of it
/// would free a live publication's boundary and its backfill capacity.
#[derive(Debug)]
enum AckFailure {
    Rejected(Error),
    Store(Error),
}

/// Why this completion sentinel must not become durable, if it must not.
///
/// Two independent questions, and the second is the one a point-in-time check
/// cannot answer. The ledger knows whether the scope carries open debt. It does
/// NOT know whether every page of the walk that produced this marker reached a
/// consumer: a page that was CLEAN and simply never persisted owes nothing, so
/// the debt check waves it through. The marker therefore carries the walk's
/// undelivered watermark from the moment it was published, and it is compared
/// again here - the same `Withheld` answer the emission gives, one layer later,
/// because with spare capacity the marker is published before the loss happens.
/// The reader that departs and strands a page is not required to do so before
/// the marker goes out; it only has to do so before the replacement acknowledges
/// it, and the retirement warning arrives BEHIND the marker in the same ring.
///
/// A marker whose registration is already gone reports no watermark, and that is
/// refused too: an abandoned or evicted marker is precisely the case where
/// delivery cannot be vouched for. So is an acknowledgement carrying no
/// publication at all, which skips the reading, the reset fence and the claim
/// lookup alike.
fn completion_refusal(
    coverage: &PendingCoverage,
    ledger: &crate::cursor::DebtLedger,
    checkpoint: &bifrost_types::BackfillCheckpoint,
    publication: Option<&crate::cursor::PublicationId>,
) -> Option<CompletionRefusal> {
    // Evaluated FIRST and independently of the debt, because the two answers
    // call for different repairs and the stronger one must not be masked. When
    // both hold, an early `OpenDebt` return withholds the marker and leaves the
    // walk's positional rows - which point past the page nobody received -
    // sitting in the store, so a detach before the in-memory retry hands a fresh
    // attach a resume position beyond the hole. Debt is a property of the scope;
    // this is a property of the walk.
    // The watermark rides the RECEIPT, so "absent" does not mean "the
    // registration is gone": a marker whose store write failed once, or whose
    // entry a later publication superseded, still carries its reading and is
    // still judged here. What is left over is an acknowledgement replayed across
    // a detach, and that one is `Unvouchable` - withheld, with nothing touched.
    // A completion sentinel acknowledged with NO publication at all is refused
    // too, and for the same reason: with no id there is no reading, no reset
    // fence and no claim lookup, so every check that stands between an
    // acknowledgement and a durable completion is skipped and the marker becomes
    // durable on the caller's say-so alone. Nothing in the engine acknowledges a
    // completion marker without its publication - the live driver refuses a
    // backfill checkpoint outright, and the orchestrator always publishes one -
    // so this is reachable only from a consumer that dropped the id, which is the
    // case that must not settle a scope for good.
    let Some(publication) = publication else {
        return Some(CompletionRefusal::Unvouchable);
    };
    match coverage.walk_watermark(publication) {
        Some(recorded) if recorded != coverage.undelivered_watermark(&checkpoint.scope) => {
            return Some(CompletionRefusal::WalkNotWhole);
        }
        None => return Some(CompletionRefusal::Unvouchable),
        Some(_) => {}
    }
    if !ledger.completion_permitted(&checkpoint.scope) {
        return Some(CompletionRefusal::OpenDebt);
    }
    None
}

/// Three reasons, kept apart because they call for different repairs.
///
/// `OpenDebt` is about the scope's obligations and says nothing about the walk:
/// the rows it produced are still a valid resume position, and a later pass may
/// earn the marker once the debt is discharged.
///
/// `WalkNotWhole` says the walk itself is void - its pages did not all reach a
/// consumer - so the rows it left are a resume position PAST a hole and must go,
/// together with the outstanding publications of the attempt that wrote them.
///
/// `Unvouchable` says this ledger cannot answer the question at all: the marker
/// was minted by a PRIOR attachment, so the reading its receipt carries belongs
/// to a per-scope counter that no longer exists - or it arrived with no
/// publication, in which case there is no reading, no fence and no claim to check
/// it against. Neither writing it nor
/// discarding on it is defensible. Writing it lets a cross-attachment replay land
/// a completion nothing can vouch for - the interleaving is ordinary: a consumer
/// holds an unacknowledged page, a replacement receives the marker, the detach
/// beats the replacement's acknowledgement, and on reattach that acknowledgement
/// flushes through the receipt fallback before the orchestrator has even read
/// `get_backfill`, so the fresh walk answers `Skip` and the stranded page is
/// never re-offered. Discarding on it is just as wrong in the other direction: it
/// would delete a marker an earlier attachment legitimately earned. So the answer
/// is to WITHHOLD and touch nothing. A marker that is already durable keeps its
/// row; one that never was has to be earned again by the attachment that can
/// actually judge it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionRefusal {
    OpenDebt,
    WalkNotWhole,
    Unvouchable,
}

impl CompletionRefusal {
    fn reason(self) -> &'static str {
        match self {
            Self::OpenDebt => "the scope has open, unwaived debt",
            Self::WalkNotWhole => {
                "the walk that produced this marker did not reach a consumer with every page"
            }
            Self::Unvouchable => {
                "this ledger cannot judge the walk behind the marker: no publication, or one \
                 minted by a prior attachment"
            }
        }
    }
}

async fn persist_ack_request(
    account_id: &AccountId,
    store: &Arc<DynCheckpointStore>,
    coverage: &PendingCoverage,
    ledger: &mut crate::cursor::DebtLedger,
    req: &AckRequest,
) -> Result<AckPersistOutcome, AckFailure> {
    if let Checkpoint::Change(cursor) = &req.checkpoint {
        cursor
            .validate_envelope()
            .map_err(|_| AckFailure::Rejected(Error::SchemaIncompatible))?;
    }
    // A BACKFILL acknowledgement whose publication this ledger did not mint is
    // withheld, and checked before the claim lookup so nothing of a prior
    // attachment's evidence is ingested on the way past.
    //
    // The receipt travels with the id the consumer persisted, so an
    // acknowledgement can replay across a detach - re-delivery rather than loss
    // for a change cursor, which simply re-reads from an older position. A
    // backfill row is not a position to re-read from but the position the next
    // WALK starts at: replaying one that a discard removed hands the fresh walk a
    // place to begin beyond a hole, after which an `OpenPages` plan takes its
    // empty end probe, lands a marker, and never offers the stranded window
    // again. The window is ordinary, not exotic: the orchestrator parks on the
    // subscriber gate at attach, which is exactly when a returning consumer
    // subscribes and flushes what it never got to acknowledge - before
    // `get_backfill` has been read.
    //
    // Answered `Ok` with nothing durable, as `Unvouchable` is: the consumer did
    // receive that batch, and the engine has nothing to reproach it with.
    // Only when the receipt really names THIS batch's lane. A publication from
    // another lane presented with a backfill checkpoint is evidence about the
    // caller, not a replay, and keeps its `Rejected` answer below.
    if let Checkpoint::Backfill(acked) = &req.checkpoint
        && req.publication.as_ref().is_some_and(|id| {
            !coverage.minted_here(id)
                && matches!(
                    id.1.checkpoint.as_ref(),
                    Some(Checkpoint::Backfill(saved))
                        if saved.scope == acked.scope && saved.partition == acked.partition
                )
        })
    {
        tracing::warn!(
            target: "bifrost.sync.backfill",
            account = ?account_id,
            scope = ?req.scope,
            "withholding a backfill row acknowledged from a prior attachment's \
             publication; the next walk must not resume from a position this \
             attachment cannot vouch for"
        );
        persist_ledger_only(account_id, store, ledger)
            .await
            .map_err(AckFailure::Store)?;
        return Ok(AckPersistOutcome::Withheld);
    }
    match req
        .publication
        .clone()
        .map(|id| coverage.claim_checkpoint(id, &req.checkpoint))
    {
        Some(ClaimLookup::Apply(claim)) => {
            let now = jiff::Timestamp::now().as_second();
            for report in &claim.reports {
                ledger.ingest(report, claim.generation, now);
            }
        }
        // Already persisted by an earlier acknowledgement of this exact
        // publication. Re-applying would double-ingest; reporting failure would
        // make a successful ack non-idempotent.
        Some(ClaimLookup::AlreadyPersisted) => return Ok(AckPersistOutcome::Durable),
        Some(ClaimLookup::Unknown) => {
            // Emphatically NOT treated as complete coverage. An unknown
            // publication is a stale or buggy caller, and inventing a
            // completeness claim for it is the lying record this whole
            // mechanism exists to prevent.
            return Err(AckFailure::Rejected(Error::CheckpointStore(
                "acknowledgement names an unknown publication".into(),
            )));
        }
        // An engine-internal ack with no coverage claim: leave the ledger
        // exactly as it is.
        None => {}
    }

    if let Checkpoint::Backfill(b) = &req.checkpoint
        && crate::backfill::partitioner::is_completion_partition(&b.partition)
        && let Some(reason) = completion_refusal(coverage, ledger, b, req.publication.as_ref())
    {
        tracing::warn!(
            target: "bifrost.sync.backfill",
            account = ?account_id,
            scope = ?b.scope,
            reason = reason.reason(),
            "withholding backfill completion sentinel"
        );
        // Not an error for the consumer - its batch was empty and its
        // acknowledgement was honoured. The sentinel simply does not become
        // durable, so the next attach re-walks instead of skipping the scope.
        if reason == CompletionRefusal::WalkNotWhole {
            // The rows this walk left behind are a resume position PAST a hole,
            // so they go, and the attempt that wrote them is fenced with them.
            // Withholding the marker alone would leave an `OpenPages` re-attach
            // resuming beyond the pages nobody received - the marker is the only
            // artefact the withholding touched, and it was never the one that
            // carried the position.
            // Bounded by the refused marker's own id. The marker is the last
            // thing its walk published, so everything of that attempt sits at or
            // below it - and a retry that has already started publishing sits
            // above it and is left alone.
            discard_backfill(
                account_id,
                store,
                coverage,
                ledger,
                &b.scope,
                req.publication.as_ref(),
            )
            .await
            .map_err(AckFailure::Store)?;
            return Ok(AckPersistOutcome::Withheld);
        }
        persist_ledger_only(account_id, store, ledger)
            .await
            .map_err(AckFailure::Store)?;
        return Ok(AckPersistOutcome::Withheld);
    }

    store
        .apply_transition(
            account_id,
            CheckpointTransition {
                checkpoint: req.checkpoint.clone(),
                ledger: ledger.clone(),
            },
        )
        .await
        .map_err(AckFailure::Store)?;
    Ok(AckPersistOutcome::Durable)
}
