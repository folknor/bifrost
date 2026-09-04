//! The backfill orchestrator: scope rescan, resume planning, and the
//! per-partition walk.

use super::*;

/// The backfill-specific half of the orchestrator's wiring; everything
/// slot-wide rides in the [`SlotContext`].
pub(super) struct BackfillWiring {
    pub live: Arc<LiveSupersedes>,
    pub store: Arc<DynCheckpointStore>,
    pub registry: Arc<BackfillRegistry>,
    pub config: BackfillConfig,
    /// Scope incarnations whose cold-start inventory the fusion worker
    /// owns; the orchestrator must not double-publish them.
    pub fusion_owned_scopes: HashSet<CursorScope>,
    /// Where inventory pages are published. `None` runs the walk with no
    /// broadcast at all, which is why this is not simply the context's
    /// `changes_tx`.
    pub changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
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
/// `BackfillCheckpoint` in the `CheckpointStore`. Before walking a scope
/// the orchestrator reads that checkpoint back via `get_backfill` and
/// hands it to `BackfillPlan::resume`, which either skips a scope a prior
/// run finished (the steady-state delta case - it must not re-walk at
/// all) or resumes after the furthest durably-checkpointed full page
/// instead of re-paginating from page 0 on every re-attach.
pub(super) async fn run_backfill_orchestrator(ctx: SlotContext, wiring: BackfillWiring) {
    let SlotContext {
        current: account,
        account_id,
        cursors,
        shutdown,
        subscriber_notify,
        control,
        throttles,
        coverage,
        writer_tx,
        scheduler,
        ..
    } = ctx;
    let BackfillWiring {
        live,
        store,
        registry,
        config,
        fusion_owned_scopes,
        changes_tx,
    } = wiring;
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
    // Fusion-owned scopes are excluded by identity, not by whether the
    // fusion worker happened to install their cursor before this scan.
    // Keep scanning for new scope incarnations for the lifetime of the
    // attach so lifecycle creation and explicit re-establishment receive
    // their own cold-start pass.
    let mut scan = BackfillScan::default();
    loop {
        let scopes = scan.select(
            cursors.all_scope_incarnations(),
            &fusion_owned_scopes,
            tokio::time::Instant::now(),
        );
        for (scope, incarnation) in scopes {
            if shutdown.is_cancelled() {
                return;
            }
            let incarnation_key = (scope.clone(), incarnation);
            let barrier_admission = tokio::select! {
                () = shutdown.cancelled() => return,
                permit = scheduler.admit(
                    account_id.clone(),
                    control.priority_snapshot(),
                    crate::scheduler::WorkKind::Sync,
                ) => match permit {
                    Ok(permit) => permit,
                    Err(error) => {
                        tracing::warn!(target: "bifrost.sync.scheduler", %error, "backfill admission failed");
                        scan.record_attempt(incarnation_key, false);
                        continue;
                    }
                },
            };
            if scope_barrier_blocked(&writer_tx, scope.clone()).await {
                tracing::debug!(
                    target: "bifrost.sync.backfill",
                    account = ?account_id,
                    scope = ?scope,
                    "backfill parked at operator-blocked barrier"
                );
                // Record the park as an attempt so the incarnation rejoins
                // the exponential rescan delay. Skipping this leaves the
                // scope's `failed` deadline in the past, so the 1s rescan
                // tick re-asks the single account writer about the same
                // blocked scope every second for as long as the operator
                // leaves the block in place - and the writer is the task
                // that also owns every durable mutation. `select` already
                // withholds a scope until its delay elapses, so reusing it
                // here parks the query on the same 5s-to-5min ramp the walk
                // itself would have used.
                scan.record_attempt(incarnation_key, false);
                continue;
            }
            drop(barrier_admission);
            let acc_arc = account.load_full();
            let acc: &dyn Account = acc_arc.as_ref().as_ref();
            // Both plan shapes drive the same `ScopeWalkDriver`, so the
            // only thing that differs between them is how the durable
            // checkpoint is turned into a starting driver: a fixed plan
            // has no positional "resume from here" (its partitions are a
            // known finite set), so its durable signal is binary, while an
            // open-ended page walk resumes at the furthest acked window.
            // `BackfillPlan::resume` is where that difference lives; the
            // walk below is shared, which is what keeps the two shapes
            // from drifting apart on barrier handling, completion-marker
            // withholding, or registry bookkeeping.
            let plan = backfill_plan_for(acc, &scope, config);
            let labels = plan.labels();
            let resume = match store.get_backfill(&account_id, &scope).await {
                Ok(stored) => plan.resume(stored.as_ref()),
                Err(err) => {
                    // A read failure is not authoritative; fall back to a
                    // full walk rather than risk skipping unpersisted
                    // pages.
                    tracing::warn!(
                        target: "bifrost.sync.backfill",
                        scope = ?scope,
                        error = %err,
                        "{}", labels.resume_read_failed
                    );
                    plan.walk_from_scratch()
                }
            };
            // Skip a scope whose backfill already reached a durable
            // conclusion on a prior run. `get_backfill` only ever returns
            // consumer-acked checkpoints, so this never skips a window the
            // consumer has not durably persisted.
            let ScopeResume::Walk(mut driver) = resume else {
                registry.mark(account_id.clone(), scope.clone(), BackfillState::Completed);
                scan.settled(incarnation_key);
                continue;
            };
            registry.mark(account_id.clone(), scope.clone(), BackfillState::Running);
            // The driver owns the sequence: a barrier stops the SCOPE, not
            // merely the partition that hit it, so a stopped walk simply
            // hands out no further partition. There is no flag here to
            // forget to check.
            //
            // For the page walk that also means terminating only on a
            // genuinely empty page, never on a merely short one. A
            // partition stream whose server caps a page below the
            // requested `chunk` (e.g. a JMAP Email/query cap below the
            // window width) returns fewer entries than asked for; treating
            // that as exhaustion silently drops every later page. So the
            // Page partition stream owes us a stronger guarantee than "it
            // filled the window": it must yield zero entries ONLY when the
            // scope has no more results past `from`. A window whose ids all
            // vanished between listing and hydration is NOT
            // end-of-inventory, and a stream that stopped there would
            // truncate the backfill; implementations are required to keep
            // walking past the window until they produce an entry or the
            // listing runs dry. Given that, `seen == 0` is unambiguous
            // here, and the driver applies it.
            while let Some(partition) = driver.next_partition() {
                if shutdown.is_cancelled() {
                    return;
                }
                let Some(result) = run_backfill_partition_at_boundary(
                    &account,
                    &account_id,
                    scope.clone(),
                    partition,
                    &live,
                    changes_tx.clone(),
                    &control,
                    &shutdown,
                    &throttles,
                    &coverage,
                    &writer_tx,
                    &scheduler,
                )
                .await
                else {
                    return;
                };
                match result {
                    Ok(outcome) => {
                        let complete = outcome.complete;
                        let step = driver.fold(&outcome);
                        if !complete {
                            // An unresolved obligation means the
                            // enumeration space was not exhausted, so the
                            // completion marker must be withheld.
                            tracing::warn!(
                                target: "bifrost.sync.backfill",
                                account = ?account_id,
                                scope = ?scope,
                                "{}", labels.unresolved_coverage
                            );
                        }
                        if step == crate::backfill::ScopeWalkStep::StopScopeWalk {
                            tracing::warn!(
                                target: "bifrost.sync.backfill",
                                account = ?account_id,
                                scope = ?scope,
                                "{}", labels.barrier_stop
                            );
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "bifrost.sync.backfill",
                            scope = ?scope,
                            error = %err,
                            "{}", labels.partition_failed
                        );
                        driver.fail();
                    }
                }
            }
            let completed = driver.completed();
            // Persist a durable completion marker through the same
            // consumer-ack path the page batches use. It is ordered behind
            // every page, so a crash before its ack re-walks instead of
            // recording a false completion.
            if completed
                && !emit_backfill_complete(
                    changes_tx.as_ref(),
                    &scope,
                    driver.total_seen(),
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
            scan.record_attempt(incarnation_key, completed);
        }
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

async fn scope_barrier_blocked(writer: &mpsc::Sender<WriterRequest>, scope: CursorScope) -> bool {
    let (done, recv) = oneshot::channel();
    if writer
        .send(WriterRequest::ScopeBarrierBlocked { scope, done })
        .await
        .is_err()
    {
        return false;
    }
    recv.await.unwrap_or(false)
}

/// First retry delay for a scope incarnation whose backfill did not
/// complete, and the ceiling the delay doubles towards.
pub(super) const BACKFILL_RETRY_INITIAL: Duration = Duration::from_secs(5);
pub(super) const BACKFILL_RETRY_CAP: Duration = Duration::from_secs(300);

/// Which scope incarnations the orchestrator's rescan still owes a
/// cold-start pass.
///
/// The distinction that matters here is settled versus attempted. An
/// incarnation leaves the rescan permanently only when its backfill
/// reached a durable conclusion: the plan ran to completion, or the
/// checkpoint store already carries a completion marker. A partition
/// failure or a checkpoint-store read failure is transient by
/// construction - the scope is deliberately left `Pending` so it can be
/// retried - so filtering it out for the rest of the attachment would
/// mean one flaky request costs the scope its entire backfill until the
/// account is reattached. Failed incarnations stay eligible and come
/// back on an exponential delay, which is what keeps a permanently
/// failing scope from re-walking on every rescan tick.
#[derive(Default)]
pub(super) struct BackfillScan {
    /// Incarnations that reached a durable conclusion. Never re-walked.
    settled: HashSet<(CursorScope, u64)>,
    /// Fusion-owned scopes whose cold-start incarnation the fusion
    /// worker already published. Only the first sighting is fusion's;
    /// a later incarnation of the same scope is a genuine
    /// re-establishment and gets its own pass.
    fusion_skipped: HashSet<CursorScope>,
    /// Failure count plus earliest next attempt, per incarnation.
    failed: HashMap<(CursorScope, u64), (u32, tokio::time::Instant)>,
}

impl BackfillScan {
    pub(super) fn select(
        &mut self,
        available: Vec<(CursorScope, u64)>,
        fusion_owned: &HashSet<CursorScope>,
        now: tokio::time::Instant,
    ) -> Vec<(CursorScope, u64)> {
        let mut pending = Vec::new();
        for (scope, incarnation) in available {
            let key = (scope, incarnation);
            if self.settled.contains(&key) {
                continue;
            }
            if fusion_owned.contains(&key.0)
                && !self.failed.contains_key(&key)
                && self.fusion_skipped.insert(key.0.clone())
            {
                // Fusion owns this incarnation's cold-start inventory;
                // walking it here would double-publish the same scope.
                self.settled.insert(key);
                continue;
            }
            if self.failed.get(&key).is_some_and(|(_, at)| *at > now) {
                continue;
            }
            pending.push(key);
        }
        pending
    }

    /// The incarnation reached a durable conclusion with no walk of our
    /// own (completion marker already present, or fusion owns it).
    pub(super) fn settled(&mut self, key: (CursorScope, u64)) {
        self.failed.remove(&key);
        self.settled.insert(key);
    }

    /// Record the outcome of a walk we actually ran.
    pub(super) fn record_attempt(&mut self, key: (CursorScope, u64), completed: bool) {
        if completed {
            self.settled(key);
            return;
        }
        let failures = self.failed.get(&key).map_or(0, |(count, _)| *count) + 1;
        let delay = BACKFILL_RETRY_INITIAL
            .saturating_mul(1_u32 << failures.min(6).saturating_sub(1))
            .min(BACKFILL_RETRY_CAP);
        self.failed
            .insert(key, (failures, tokio::time::Instant::now() + delay));
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_backfill_partition_at_boundary(
    account: &Arc<ArcSwap<Arc<dyn Account>>>,
    account_id: &AccountId,
    scope: CursorScope,
    partition: InventoryPartition,
    live: &Arc<LiveSupersedes>,
    changes_tx: Option<broadcast::Sender<MultiplexerEvent>>,
    control: &SyncControl,
    shutdown: &CancellationToken,
    throttles: &std::sync::Mutex<crate::recovery::ThrottleBucket>,
    coverage: &Arc<PendingCoverage>,
    writer_tx: &mpsc::Sender<WriterRequest>,
    scheduler: &Scheduler,
) -> Option<Result<crate::backfill::BackfillPartitionOutcome, Error>> {
    // One generation per partition pass, so a re-walk's proof is ordered after
    // the debt an earlier pass raised.
    let generation = coverage.next_generation();
    loop {
        if !control.wait_until_running(shutdown).await {
            return None;
        }
        // Honor any account-wide throttle deadline before walking the
        // partition: cold-start hydration is the heaviest request lane
        // the engine drives, so barreling through a provider Retry-After
        // that paused the polls would defeat the pause. Re-checked after
        // waking, and boundary-checked again, since a pause or a longer
        // deadline can land mid-sleep.
        if let Some(wait) = crate::recovery::account_throttle_wait(
            throttles,
            account_id,
            std::time::SystemTime::now(),
        ) {
            tracing::debug!(
                target: "bifrost.sync.backfill",
                account = ?account_id,
                scope = ?scope,
                wait_secs = wait.as_secs(),
                "backfill partition deferred by shared throttle deadline"
            );
            tokio::select! {
                () = shutdown.cancelled() => return None,
                () = tokio::time::sleep(wait) => {}
            }
            continue;
        }
        let admission = tokio::select! {
            () = shutdown.cancelled() => return None,
            permit = scheduler.admit(
                account_id.clone(),
                control.priority_snapshot(),
                crate::scheduler::WorkKind::Sync,
            ) => match permit {
                Ok(permit) => permit,
                Err(error) => return Some(Err(error)),
            },
        };
        let current = account.load_full();
        let result = BackfillRunner::run_partition(
            current.as_ref().as_ref(),
            scope.clone(),
            partition.clone(),
            live.as_ref(),
            changes_tx.clone(),
            crate::cursor::ENGINE_VERSION,
            Some(control),
            Some(coverage),
            Some(writer_tx),
            generation,
        )
        .await;
        drop(admission);
        if matches!(result, Err(Error::Paused)) {
            continue;
        }
        return Some(result);
    }
}

/// Resume decision for an open-ended page scope, derived purely from the
/// durably-persisted (consumer-acked) backfill checkpoint.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum OpenPagesResume {
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
pub(super) fn backfill_complete_recorded(checkpoint: Option<&BackfillCheckpoint>) -> bool {
    checkpoint
        .is_some_and(|ck| crate::backfill::partitioner::is_completion_partition(&ck.partition))
}

/// Map the persisted backfill checkpoint to a resume decision.
///
/// - The completion sentinel means a prior run reached exhaustion and the
///   consumer acked it: skip entirely.
/// - Any other `page:F:T` resumes at `T`. A SHORT page (`items_done <
///   T - F`) is deliberately NOT read as exhaustion: the partition
///   contract is that zero entries means end-of-inventory, but a
///   partition may legitimately emit fewer entries than its window width
///   while the scope still has results - ids that vanished between
///   listing and hydration, or objects dropped for arriving without an
///   id. Only the completion marker proves exhaustion; a short page
///   without one costs a single empty probe query on re-attach, whereas
///   skipping on it would silently drop every message past the window.
/// - No checkpoint, or an unrecognised partition kind, starts fresh at 0.
///
/// Resume never skips a window the consumer has not durably persisted,
/// because `get_backfill` only ever returns consumer-acked checkpoints.
pub(super) fn open_pages_resume(checkpoint: Option<&BackfillCheckpoint>) -> OpenPagesResume {
    if backfill_complete_recorded(checkpoint) {
        return OpenPagesResume::Skip;
    }
    let Some(checkpoint) = checkpoint else {
        return OpenPagesResume::ResumeFrom(0);
    };
    if let Some((_, to)) = crate::backfill::partitioner::parse_page_partition(&checkpoint.partition)
    {
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
    let expected = Checkpoint::Backfill(marker);
    // Register before publishing so a fast consumer ack cannot land
    // before the entry exists and leave it outstanding forever.
    let publication = control.publish_checkpoint_without_report(expected.clone(), 0);
    let event = MultiplexerEvent {
        scope: scope.clone(),
        event: Arc::new(SyncEvent::Batch(batch)),
        checkpoint: Some(expected),
        publication: Some(publication.clone()),
    };
    let delivered = tx.send(event).unwrap_or(0);
    if !crate::multiplexer::delivered_to_real_subscriber(delivered) {
        control.retire_publication(publication);
    }
    true
}

pub(super) enum BackfillPlan {
    Fixed(Vec<InventoryPartition>),
    OpenPages { chunk: u32 },
}

/// What the orchestrator does with a scope incarnation once its durable
/// checkpoint has been read back.
///
/// The two plan shapes differ ONLY here. Both drive the same
/// `ScopeWalkDriver` afterwards, so folding their resume decisions into
/// one enum is what lets the walk itself be written once - barrier
/// handling, completion-marker withholding and registry bookkeeping
/// included.
pub(super) enum ScopeResume {
    /// Durable evidence that a prior run finished this scope. Do not walk.
    Skip,
    /// Walk, starting from wherever the plan says to start.
    Walk(crate::backfill::ScopeWalkDriver),
}

/// Log wording for a walk, so collapsing the two plan arms into one loop
/// does not collapse their operator-facing messages into one another.
struct WalkLabels {
    resume_read_failed: &'static str,
    unresolved_coverage: &'static str,
    barrier_stop: &'static str,
    partition_failed: &'static str,
}

const FIXED_LABELS: WalkLabels = WalkLabels {
    resume_read_failed: "backfill resume read failed; re-walking all partitions",
    unresolved_coverage: "backfill partition completed with unresolved coverage; withholding the \
                          completion marker",
    barrier_stop: "backfill stopped at a barrier; refusing to walk any further partition of this \
                   scope",
    partition_failed: "backfill partition failed; leaving scope Pending",
};

const OPEN_PAGES_LABELS: WalkLabels = WalkLabels {
    resume_read_failed: "backfill resume read failed; re-walking from page 0",
    unresolved_coverage: "backfill page completed with unresolved coverage; withholding the \
                          completion marker",
    barrier_stop: "backfill stopped at a barrier; refusing to walk any further page window of \
                   this scope",
    partition_failed: "backfill page partition failed; leaving scope Pending",
};

impl BackfillPlan {
    fn labels(&self) -> &'static WalkLabels {
        match self {
            BackfillPlan::Fixed(_) => &FIXED_LABELS,
            BackfillPlan::OpenPages { .. } => &OPEN_PAGES_LABELS,
        }
    }

    /// Turn the durably-persisted (consumer-acked) checkpoint into a
    /// resume decision.
    ///
    /// A fixed plan's partitions are a known finite set with no positional
    /// "resume from here", so its durable signal is binary: the completion
    /// marker is present (skip the whole plan) or it is not (walk every
    /// partition; re-emitting acked pages is idempotent, so a crash
    /// mid-plan simply re-walks). An open-ended page walk resumes after the
    /// furthest durably-checkpointed window instead of re-paginating from
    /// page 0 on every re-attach.
    pub(super) fn resume(self, stored: Option<&BackfillCheckpoint>) -> ScopeResume {
        match self {
            BackfillPlan::Fixed(partitions) => {
                if backfill_complete_recorded(stored) {
                    ScopeResume::Skip
                } else {
                    ScopeResume::Walk(crate::backfill::ScopeWalkDriver::fixed(partitions))
                }
            }
            BackfillPlan::OpenPages { chunk } => match open_pages_resume(stored) {
                OpenPagesResume::Skip => ScopeResume::Skip,
                OpenPagesResume::ResumeFrom(from) => {
                    ScopeResume::Walk(crate::backfill::ScopeWalkDriver::open_pages(from, chunk))
                }
            },
        }
    }

    /// The resume decision to use when the checkpoint READ failed, as
    /// opposed to came back empty. Deliberately the same answer as an
    /// absent checkpoint: a store error proves nothing about coverage, and
    /// skipping on it would silently drop everything a prior run had not
    /// finished.
    pub(super) fn walk_from_scratch(self) -> ScopeResume {
        self.resume(None)
    }
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
            let plan = crate::backfill::partitioner::plan(&policy, jiff::Timestamp::now(), 0);
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
            let plan = crate::backfill::partitioner::plan(&policy, jiff::Timestamp::now(), max_uid);
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
            let plan = crate::backfill::partitioner::plan(&policy, jiff::Timestamp::now(), total);
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
