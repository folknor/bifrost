//! Recovery dispatch: reattach, restart, re-establish, and the
//! engine-directive arms.

use super::*;

/// Shared context bundle used by every recovery-dispatch path. Folds
/// the per-account state most paths need into a single argument so
/// `handle_account_error`, the engine-directive arm, and the reopen
/// loop don't carry seven-positional argument lists.
pub(crate) struct RecoveryContext<'a> {
    pub factory: &'a Arc<dyn AccountFactory>,
    pub current: &'a Arc<ArcSwap<Arc<dyn Account>>>,
    pub cursors: &'a Arc<CursorRegistry>,
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
    /// Slot-lifetime open-skip lane; a successful reattach replaces it
    /// with the replacement open's `OpenedAccount::skipped_scopes`.
    pub open_skips: &'a Arc<std::sync::Mutex<Vec<SkippedScope>>>,
    /// Cancels a queued recovery during detach.
    pub shutdown: &'a CancellationToken,
    /// The account's single durable writer.
    ///
    /// Reattach persists and rolls back replacement cursors THROUGH this
    /// channel rather than touching `store` directly. Both are ordered against
    /// consumer acknowledgements that way, which matters because a
    /// replacement's inventory pass broadcasts checkpoint-bearing batches
    /// before the cutover: a direct write racing the ack writer let an aborted
    /// reattach delete a cursor a consumer had already acknowledged.
    pub writer: &'a WriterHandle,
    /// Where inventory walks record what they proved, read back by the writer.
    pub coverage: &'a Arc<PendingCoverage>,
    /// The account's change delivery gate. Re-establishment runs inventory
    /// fusion, whose checkpoint-bearing pages have to know whether a NUMBERED
    /// receiver took delivery; the raw `changes_tx` cannot say.
    pub delivery: &'a Arc<ChangeDelivery>,
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
            apply_throttle(ctx, &advice, &error);
            tracing::debug!(
                target: "bifrost.sync.recovery",
                account = ?ctx.account_id,
                scope = ?scope,
                retry = ?advice,
                "retry recovery reached reopen listener; caller owns the retry"
            );
        }
        RecoveryPlan::Reconcile(advice) => {
            crate::recovery::record_reconcile_throttle(
                ctx.throttles,
                ctx.account_id,
                &advice,
                &error,
            );
            // The poll loop / push reconciler perform the actual probe;
            // if we reach this branch via the reopen listener their
            // next pass owns the reconcile. Surface the actions for
            // telemetry so dropped guidance is visible.
            log_reconcile_advice(ctx, scope.as_ref(), &error, &advice);
        }
        RecoveryPlan::Engine(directive) => {
            // Every directive except `RestartAccount` is a bounded,
            // scope-local repair that must not interleave with a
            // connection swap, so it takes the serialization lock here.
            // RestartScope and SchemaIncompatible deliberately retain the
            // guard across their retry backoff. Releasing it during a sleep
            // would let a reopen replace the account and registry topology
            // halfway through the delete-then-establish transaction, or let
            // a second recovery establish the same scope concurrently. The
            // queueing cost is bounded by the three-attempt budget and is the
            // price of keeping recovery transitions serial.
            //
            // `RestartAccount` must NOT take it here. It waits for the
            // boundary to read `Run` before each attempt, and that wait is
            // unbounded: a consumer can pause and hold the account idle
            // indefinitely. Holding the lock across it made
            // `unsubscribe_push` - which needs the same lock - hang for
            // the entire pause, so push teardown was unavailable exactly
            // when a consumer was trying to detach cleanly. The lock it
            // does need is taken inside `open_replacement`, spanning only
            // the open and the swap. Reintroducing an acquisition here
            // deadlocks against that one rather than silently regressing.
            let _reopen_guard = match directive {
                EngineDirective::RestartAccount => None,
                _ => Some(ctx.reopen_lock.lock().await),
            };
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
/// shared `ThrottleBucket`, resolving the key from the identities the
/// classified error carries (`ErrorScope::Mailbox`, provider).
/// `CurrentOperation` is a per-call hint and never enters the bucket
/// (the originating caller owns any inline delay).
fn apply_throttle(ctx: &RecoveryContext<'_>, advice: &RetryAdvice, error: &AccountError) {
    crate::recovery::record_throttle(ctx.throttles, ctx.account_id, advice, error);
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
    // Every current directive has an explicit dispatch arm. The required
    // non-exhaustive fallback makes a future variant visible in logs until a
    // human gives it an intentional engine action.
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
            // Route the warning to the scope the downgrade is actually
            // about: the originating error's cursor scope when it names
            // one (the same signal the immediate re-establish below
            // keys on), else the worker's suggestion, else account.
            let warning_scope = match error.scope() {
                Some(ErrorScope::Cursor(scoped)) => Some(scoped.clone()),
                _ => fallback_scope.clone(),
            };
            broadcast_warning(
                ctx.changes_tx,
                warning_scope,
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
            // Deliberately account-scoped, not the worker's fallback:
            // the directive pauses the WHOLE account, and a consumer
            // routing the warning to one folder's scope would misfile
            // an account-wide condition.
            broadcast_warning(
                ctx.changes_tx,
                None,
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
    // cursor, delete every durable change cursor we know about, AND
    // delete every backfill checkpoint - the completion marker
    // included. The ids the consumer holds were minted under an
    // encoding the protocol has disowned (that is what a schema bump
    // asserts), and re-minting them requires the inventory re-walk
    // the completion marker would otherwise skip at the next attach.
    // For protocols whose establishment returns a `Ready` cursor from
    // live state (JMAP), that next-attach re-walk IS the migration;
    // this session's re-established cursor only covers changes from
    // now on. Then re-establish each scope from the current account
    // handle. Failure to re-establish a single scope escalates
    // per-scope (sync-D7): the account keeps running for the scopes
    // that succeed.
    let scopes: Vec<CursorScope> = ctx.cursors.all_scopes();
    for s in &scopes {
        ctx.cursors.delete(s);
        if let Err(err) = ctx.writer.reset_scope_for_schema_recovery(s.clone()).await {
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?ctx.account_id,
                scope = ?s,
                error = %err,
                "SchemaIncompatible: durable scope reset failed"
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

/// Wait out a recovery backoff, unless the account is being torn down.
///
/// Returns `false` when the shutdown token tripped, which every caller reads as
/// "stop, do not attempt again". An un-selected sleep here is what guaranteed
/// that a detach landing during recovery burned toward `detach_timeout` and hit
/// the abort path instead of draining the worker cleanly.
pub(super) async fn sleep_unless_shutdown(delay: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        () = shutdown.cancelled() => false,
        () = tokio::time::sleep(delay) => true,
    }
}

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
    if let Err(err) = ctx.writer.reset_scope_for_restart(scope.clone()).await {
        tracing::warn!(
            target: "bifrost.sync.changes",
            account = ?ctx.account_id,
            scope = ?scope,
            error = %err,
            "RestartScope: durable scope reset failed"
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
    if let Err(err) = ctx.writer.reset_scope_for_disable(scope.clone()).await {
        tracing::warn!(
            target: "bifrost.sync.changes",
            account = ?ctx.account_id,
            scope = ?scope,
            error = %err,
            "DisableScope: durable scope reset failed"
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
            if !sleep_unless_shutdown(sleep_for, ctx.shutdown).await {
                return;
            }
            delay = (delay.saturating_mul(2)).min(REOPEN_BACKOFF_CAP);
        }
        let acc_arc = ctx.current.load_full();
        let acc: &dyn Account = acc_arc.as_ref().as_ref();
        match run_establish(
            ctx.account_id,
            acc,
            scope.clone(),
            Arc::clone(ctx.cursors),
            ctx.writer,
            Arc::clone(ctx.delivery),
            Some(ctx.control),
            Arc::clone(ctx.coverage),
            true,
        )
        .await
        {
            Ok(_) => {
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
                // Dispatch the classified error through `plan_recovery`
                // instead of blind-retrying every class three times: a
                // scope-quarantine directive names its own action, and
                // a terminal class cannot be repaired by another
                // attempt - breaking out lands in the exhaustion tail
                // below, which broadcasts `Terminated` plus the
                // operator warning exactly as a spent budget would.
                use crate::recovery::{RecoveryPlan, plan_recovery};
                match plan_recovery(err.clone()) {
                    RecoveryPlan::Engine(EngineDirective::DisableScope(quarantined)) => {
                        disable_scope(ctx, quarantined).await;
                        return;
                    }
                    RecoveryPlan::Terminal(_) => {
                        last_account_error = Some(err);
                        break;
                    }
                    _ => {
                        last_account_error = Some(err);
                    }
                }
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
/// Tear down the subscriptions already created on a replacement connection
/// that is about to be discarded.
///
/// Any handle whose delete did not succeed is registered against the account
/// with `teardown_unconfirmed`, because the replacement is closed immediately
/// after and `Account::close()` does not delete server-side subscriptions.
/// Without that record the engine would forget the handle entirely and
/// recreate exactly the orphaned-webhook leak the retained-handle rule exists
/// to prevent - a provider like Graph keeps delivering to the endpoint until
/// the subscription expires on its own.
async fn unwind_replacement_subscriptions(
    ctx: &RecoveryContext<'_>,
    next: &dyn Account,
    replacements: &mut Vec<RegisteredSubscription>,
) {
    let mut orphaned = Vec::new();
    for replacement in replacements.drain(..) {
        if let Err(cleanup) = next.push_unsubscribe(replacement.handle.clone()).await {
            tracing::warn!(
                target: "bifrost.sync.reopen",
                account = ?ctx.account_id,
                error = %cleanup,
                "replacement push cleanup failed while unwinding reopen; retaining handle for retry"
            );
            orphaned.push(RegisteredSubscription {
                teardown_unconfirmed: true,
                ..replacement
            });
        }
    }
    ctx.subscriptions.restore(ctx.account_id.clone(), orphaned);
}

/// Delete the durable cursor rows an aborted reattach created.
///
/// This is deliberately the ONLY durable compensation the reopen path has,
/// and it only ever deletes rows whose scope had no stored cursor before
/// this reattach began. Preexisting rows are never deleted, snapshotted, or
/// restored here: the old account's ack writer keeps persisting checkpoints
/// concurrently (nothing serializes it against a reopen), so any
/// snapshot-and-restore of a preexisting row could overwrite a newer
/// consumer-acknowledged checkpoint. Deleting only rows this reattach itself
/// created cannot destroy preexisting data - the worst outcome of a failed
/// delete is a leaked cursor for a topology that was never installed.
/// Compensate an aborted reattach by deleting the cursor rows it inserted.
///
/// The set of rows is NOT passed in. The account's writer tracks which scopes
/// are still provisional - inserted by this reattach and not since claimed by a
/// consumer acknowledgement - and deletes exactly those. Passing a list
/// computed here would reintroduce the bug: this task cannot see whether an ack
/// landed for one of those scopes in the meantime, and a replacement inventory
/// pass broadcasts acknowledgeable checkpoints before the cutover, so the list
/// goes stale the moment a consumer acts on one.
///
/// Serializing the delete behind the writer is necessary but not sufficient on
/// its own; a plain FIFO queue would order the acknowledged write first and
/// then faithfully destroy it. The provisional set is what makes this a
/// conditional delete.
async fn rollback_reattach_inserts(ctx: &RecoveryContext<'_>) {
    ctx.writer.reattach_abort().await;
}

/// Promote this reattach's provisional rows to ordinary durable state.
///
/// Called after the cutover commits, past the last point an abort can occur.
/// Without it the scopes stay marked provisional and the NEXT reattach's abort
/// would delete cursors belonging to a reattach that succeeded.
async fn commit_reattach_inserts(ctx: &RecoveryContext<'_>) {
    if ctx.writer.reattach_commit().await.is_err() {
        tracing::error!(
            target: "bifrost.sync.reopen",
            account = ?ctx.account_id,
            "writer channel closed before the reattach could be committed"
        );
    }
}

/// Why no replacement connection was opened.
pub(super) enum ReplacementOpen {
    /// The boundary left `Run` before activity could be registered. Nothing
    /// was opened, so there is nothing to close.
    Paused,
    /// The slot is being torn down: its shutdown token was cancelled either
    /// before the open started or while it was in flight. A replacement opened
    /// inside that window has no owner - `detach` has already closed the handle
    /// it knew about and removed the slot - so `open_replacement` closes the
    /// replacement itself and reports this rather than swapping it into an
    /// orphaned slot, where nothing would ever close it.
    Detached,
    Failed(AccountError),
}

/// Open a replacement connection with the account's activity registration
/// already held.
///
/// The guard must span the open itself, not just the reattach that follows
/// it. `pause` reports quiescence as soon as the active count reaches zero,
/// so registering after `factory.open()` returns would let a pause observe an
/// idle account while a replacement connection is being established in the
/// background - the opposite of what the quiescence contract promises. The
/// guard is returned so the caller can hand it to `reattach_account` and keep
/// the whole reopen inside one activity registration.
///
/// This is also the ONLY place the reopen serialization lock is taken for
/// an open/swap, and the guard leaves only inside the returned tuple. That
/// is deliberate structure, not style: callers all wait for the boundary to
/// read `Run` before they get here, and a caller that took the lock itself
/// would hold it across that unbounded wait - which is exactly how
/// `unsubscribe_push` came to hang for the whole duration of a pause. With
/// acquisition owned here, holding the lock across a pause wait is not
/// something a caller can express.
pub(super) async fn open_replacement(
    ctx: &RecoveryContext<'_>,
) -> Result<(OwnedMutexGuard<()>, SyncActivityGuard, OpenedAccount), ReplacementOpen> {
    let activity = ctx
        .control
        .begin_activity()
        .ok_or(ReplacementOpen::Paused)?;
    let reopen_guard = Arc::clone(ctx.reopen_lock).lock_owned().await;
    // Detach does not wait for consumer-driven activity, so nothing excludes a
    // `SyncEngine::reopen` that registered its activity just before detach
    // flipped the boundary to `Stop`. The slot's shutdown token is the one
    // thing that observes the teardown from here, so it is consulted on both
    // sides of the open: before, to avoid spending a connection at all, and
    // after, because detach can remove the slot and close the old handle while
    // the open is in flight. A reattach that then completes without touching
    // the closed writer channel would swap the replacement into an orphaned
    // slot and close the already-closed previous handle, leaking the
    // replacement connection for the life of the process.
    if ctx.shutdown.is_cancelled() {
        return Err(ReplacementOpen::Detached);
    }
    match ctx.factory.open(ctx.account_id.clone()).await {
        Ok(next) => {
            if ctx.shutdown.is_cancelled() {
                // Close what we opened. Best-effort, exactly like every other
                // abort path here: the alternative is an owner-less connection.
                let _ = next.account.close().await;
                return Err(ReplacementOpen::Detached);
            }
            Ok((reopen_guard, activity, next))
        }
        // Dropping `activity` here is the point: the failed open registered
        // no lasting work, so quiescence must not stay blocked on it.
        Err(error) => Err(ReplacementOpen::Failed(error)),
    }
}

/// Swap in a replacement connection. `activity` is the registration taken by
/// `open_replacement` before the connection was opened; holding it here keeps
/// the open and the swap inside one uninterrupted non-quiescent window.
pub(super) async fn reattach_account(
    ctx: &RecoveryContext<'_>,
    activity: SyncActivityGuard,
    next: OpenedAccount,
) -> Result<(), Error> {
    let _activity = activity;
    let OpenedAccount {
        account: next,
        skipped_scopes: next_skips,
    } = next;
    next.set_priority(ctx.control.priority_snapshot());
    next.set_bandwidth_cap(ctx.control.bandwidth_cap_snapshot());

    let result = async {
        let discovered = discover_scopes_from(next.as_ref()).await?;
        let staged = Arc::new(CursorRegistry::new());
        let mut newly_established = Vec::new();

        for scope in &discovered {
            if let Some(existing) = ctx.cursors.snapshot(scope) {
                staged.put(existing);
                continue;
            }
            // A scope absent from the live registry may still have a durable
            // cursor - a prior session persisted it, this session's account
            // never discovered it, and the replacement discovers it again.
            // `run_establish` resumes from that row without writing, and
            // reports the origin itself so a resumed row can never be
            // counted as created here - an aborted swap would otherwise
            // delete a legitimately persisted cursor. The origin comes from
            // run_establish's own single store read, not a separate
            // pre-check: a pre-check both races that read and, if it
            // swallowed a store error as "no row", would misclassify a
            // preexisting row as freshly created.
            match run_establish(
                ctx.account_id,
                next.as_ref(),
                scope.clone(),
                Arc::clone(&staged),
                ctx.writer,
                Arc::clone(ctx.delivery),
                None,
                Arc::clone(ctx.coverage),
                false,
            )
            .await
            {
                Ok(EstablishOrigin::ResumedStored) => {}
                Ok(EstablishOrigin::CreatedFresh) => {
                    if let Some(cursor) = staged.snapshot(scope) {
                        newly_established.push(cursor);
                    }
                }
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

        let discovered_set: HashSet<_> = discovered.iter().cloned().collect();
        let vanished: Vec<_> = ctx
            .cursors
            .all_scopes()
            .into_iter()
            .filter(|scope| !discovered_set.contains(scope))
            .collect();

        let previous_subscriptions = ctx.subscriptions.snapshot(ctx.account_id);
        let mut replacement_subscriptions = Vec::with_capacity(previous_subscriptions.len());
        for record in previous_subscriptions.iter().filter(|record| {
            // An unconfirmed record is an orphan being carried for retry, not
            // a live subscription the consumer asked for; recreating it on the
            // replacement would double-subscribe the account.
            !record.teardown_unconfirmed
                && next.capabilities().push != bifrost_types::PushCapability::None
        }) {
            let scopes: Vec<CursorScope> = record
                .scopes
                .iter()
                .filter(|scope| discovered_set.contains(*scope))
                .cloned()
                .collect();
            if scopes.is_empty() {
                // The requested topology vanished. Do not silently widen a
                // scoped subscription to every discovered scope; its old
                // handle is explicitly torn down below, and the consumer can
                // opt in again against the replacement topology.
                continue;
            }
            match next.push_subscribe(&scopes).await {
                Ok(result) => {
                    let covered = accepted_push_scopes(&result);
                    if let Some(handle) = result.handle {
                        replacement_subscriptions.push(RegisteredSubscription {
                            handle,
                            scopes: covered,
                            teardown_unconfirmed: false,
                        });
                    }
                }
                Err(error) => {
                    unwind_replacement_subscriptions(
                        ctx,
                        next.as_ref(),
                        &mut replacement_subscriptions,
                    )
                    .await;
                    return Err(Error::Account(error));
                }
            }
        }

        // Persist the freshly created cursors while the old account and its
        // push subscriptions are still completely intact. Only rows whose
        // scope had no stored cursor before this reattach are written here,
        // so the abort path below can compensate by plain deletion without
        // ever touching preexisting durable state. Vanished-scope rows are
        // deliberately NOT deleted yet: the old account's ack writer may
        // still be persisting checkpoints for them, and a delete now would
        // need a snapshot-and-restore on abort that races that writer.
        // Their deletion happens after the cutover, where an abort can no
        // longer occur.
        // Through the account's single writer, NOT `ctx.store` directly. A
        // direct write races the ack writer, which is concurrently persisting
        // acknowledged checkpoints - including ones this very reattach caused,
        // because `run_establish` hands `changes_tx` to `InventoryFusion` and a
        // replacement inventory pass broadcasts checkpoint-bearing batches
        // before the cutover. The writer also marks these scopes provisional so
        // an abort deletes only rows no acknowledgement has claimed.
        let durable_result = async {
            for cursor in &newly_established {
                ctx.writer.reattach_insert(cursor.clone()).await?;
            }
            Ok::<(), Error>(())
        }
        .await;
        if let Err(error) = durable_result {
            rollback_reattach_inserts(ctx).await;
            unwind_replacement_subscriptions(ctx, next.as_ref(), &mut replacement_subscriptions)
                .await;
            return Err(error);
        }

        // Accounts such as Graph retain server-side subscription ids after a
        // failed delete so the same handle can retry. Tear old handles down
        // before swapping and closing their account: otherwise a vanished
        // record would lose the only retry path and leak a server subscription.
        let previous = ctx.current.load_full();
        let mut carried_unconfirmed = Vec::new();
        for record in &previous_subscriptions {
            let teardown = previous.push_unsubscribe(record.handle.clone()).await;
            match teardown {
                Ok(()) => {}
                Err(error) if record.teardown_unconfirmed => {
                    // Already an orphan. Its handle may belong to a connection
                    // that is long gone, so a repeated failure must not block
                    // the swap; keep carrying it so the next reopen or an
                    // `unsubscribe_push` call can try again.
                    tracing::warn!(
                        target: "bifrost.sync.reopen",
                        account = ?ctx.account_id,
                        error = %error,
                        "carrying push subscription whose teardown is still unconfirmed"
                    );
                    carried_unconfirmed.push(record.clone());
                }
                Err(error) => {
                    // The replacement's own subscriptions must not be stranded
                    // by this unwind: `Account::close()` deliberately does not
                    // delete server-side subscriptions, so any handle whose
                    // teardown did not succeed stays in the registry - on
                    // whichever side it was created - and is retried later.
                    unwind_replacement_subscriptions(
                        ctx,
                        next.as_ref(),
                        &mut replacement_subscriptions,
                    )
                    .await;
                    ctx.subscriptions
                        .mark_unconfirmed(ctx.account_id, &record.handle);
                    rollback_reattach_inserts(ctx).await;
                    return Err(Error::Account(error));
                }
            }
        }
        replacement_subscriptions.extend(carried_unconfirmed);

        // Take the final old-handle snapshot immediately before cutover.
        // replace_from advances the registry generation atomically with the
        // topology swap, so an old stream that finishes later cannot write a
        // cursor minted by the retired connection into this new topology.
        let previous = ctx.current.swap(Arc::new(Arc::clone(&next)));
        ctx.cursors.replace_topology_preserving_cursors(&staged);
        {
            let mut capabilities = match ctx.capabilities.write() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            *capabilities = next.capabilities().clone();
        }
        ctx.subscriptions
            .replace(ctx.account_id.clone(), replacement_subscriptions);
        // The replacement open's skip lane supersedes the previous
        // one: a healed namespace disappears from it, a still-degraded
        // one reappears with a fresh classification.
        log_open_skips(ctx.account_id, &next_skips, "reopen");
        {
            let mut skips = ctx
                .open_skips
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *skips = next_skips;
        }
        ctx.account_generation_tx
            .send_modify(|generation| *generation = generation.saturating_add(1));

        // Past the last abort point, so the rows this reattach inserted are now
        // ordinary durable state. Leaving them marked provisional would let a
        // LATER reattach's abort delete cursors belonging to this one, which
        // succeeded. Deliberately after the generation bump: this is a channel
        // send, and the lifecycle reader is waiting on that watch to resubscribe
        // to the replacement handle - no await belongs between the topology swap
        // and the bump that publishes it.
        commit_reattach_inserts(ctx).await;

        // Delete vanished-scope rows only now that the cutover is committed.
        // Nothing after this point can abort the swap, so no compensation is
        // needed; a failed (or raced) delete merely leaks a stale row for a
        // scope no longer in the topology, and a later rediscovery of that
        // scope resumes from it, which is valid. A retired stream's late ack
        // can likewise re-persist such a row after this delete - the same
        // benign leak, never data loss.
        for scope in &vanished {
            if let Err(error) = ctx.writer.reset_scope_for_disable(scope.clone()).await {
                tracing::warn!(
                    target: "bifrost.sync.reopen",
                    account = ?ctx.account_id,
                    scope = ?scope,
                    error = %error,
                    "failed to delete vanished-scope cursor after reopen cutover"
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

pub(super) fn accepted_push_scopes(result: &bifrost_types::PushSubscription) -> Vec<CursorScope> {
    result
        .outcomes
        .succeeded()
        .iter()
        .map(|success| success.output.clone())
        .collect()
}

async fn restart_account(ctx: &RecoveryContext<'_>) {
    let mut delay = REOPEN_BACKOFF_INITIAL;
    let mut last_error: Option<AccountError> = None;
    let mut attempt = 0;
    while attempt < REOPEN_RETRY_BUDGET {
        // A consumer pause is a quiescence boundary. Keep this recovery
        // request queued until resume instead of opening a replacement while
        // the account has promised to be idle.
        if !ctx.control.wait_until_running(ctx.shutdown).await {
            return;
        }
        if attempt > 0 {
            let sleep_for = jittered(delay);
            if !sleep_unless_shutdown(sleep_for, ctx.shutdown).await {
                return;
            }
            delay = (delay.saturating_mul(2)).min(REOPEN_BACKOFF_CAP);
        }
        match open_replacement(ctx).await {
            // A pause won the race between the wait above and the activity
            // registration, and nothing was opened. Loop back through the
            // boundary wait instead of spending an attempt on it.
            Err(ReplacementOpen::Paused) => continue,
            // The slot is being torn down. Nothing to restart, and the
            // replacement (if one was opened at all) has already been closed.
            Err(ReplacementOpen::Detached) => return,
            Ok((_reopen_guard, activity, next)) => {
                match reattach_account(ctx, activity, next).await {
                    Ok(()) => return,
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
                        attempt = attempt.saturating_add(1);
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
                        attempt = attempt.saturating_add(1);
                    }
                }
            }
            Err(ReplacementOpen::Failed(err)) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?ctx.account_id,
                    attempt,
                    kind = ?err.kind(),
                    message_key = err.message_key(),
                    "RestartAccount: factory.open failed"
                );
                last_error = Some(err);
                attempt = attempt.saturating_add(1);
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

/// Emit one structured `warn!` per open-time skip. The durable record
/// is the slot's `open_skips` lane (read via
/// `SyncEngine::open_skipped_scopes`); the log line exists so a
/// degraded shared namespace is visible in telemetry even when the
/// consumer never queries the lane.
pub(super) fn log_open_skips(account_id: &AccountId, skips: &[SkippedScope], phase: &'static str) {
    for skip in skips {
        tracing::warn!(
            target: "bifrost.sync.attach",
            account = ?account_id,
            scope = ?skip.scope,
            kind = ?skip.error.kind(),
            message_key = skip.error.message_key(),
            phase,
            "open skipped a degraded scope; primary surface attached without it"
        );
    }
}

pub(super) fn jittered(base: Duration) -> Duration {
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
        publication: None,
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
        publication: None,
    };
    let _ = changes_tx.send(me);
}

/// How `run_establish` obtained a scope's cursor. Reattach uses this to
/// decide which durable rows an aborted swap may delete: only a cursor
/// this establishment created fresh corresponds to a row the reattach
/// itself will write, so only those may be compensated by deletion. The
/// distinction is reported from inside `run_establish` - off the single
/// store read it already performs - rather than probed by the caller
/// beforehand, because a separate pre-check races that read (and a
/// pre-check that swallowed a store error once misclassified a
/// preexisting row as freshly created, which an abort then deleted).
#[derive(Clone, Copy, PartialEq, Eq)]
enum EstablishOrigin {
    /// A durable row already existed and was resumed from (directly, or
    /// by finishing the inventory it recorded). The stored row is
    /// preexisting data that no reattach abort may touch.
    ResumedStored,
    /// No durable row existed; whatever cursor the registry now holds
    /// for the scope was created fresh by this establishment.
    CreatedFresh,
}

/// Re-establish a single scope. Mirrors `SyncEngine::establish_one`
/// but lives at file scope so the reopen listener task can call it
/// without owning a reference to the engine.
#[allow(clippy::too_many_arguments)]
async fn run_establish(
    account_id: &AccountId,
    account: &dyn Account,
    scope: CursorScope,
    cursors: Arc<CursorRegistry>,
    writer: &WriterHandle,
    delivery: Arc<crate::multiplexer::ChangeDelivery>,
    control: Option<&SyncControl>,
    coverage: Arc<PendingCoverage>,
    persist_ready: bool,
) -> Result<EstablishOrigin, Error> {
    let _activity = match control {
        Some(control) => Some(control.begin_activity().ok_or(Error::Paused)?),
        None => None,
    };
    match writer.get_change_cursor(scope.clone()).await {
        Ok(Some(existing)) if existing.validate_envelope().is_ok() => {
            // A stored cursor may be a mid-inventory page position rather
            // than a live changes cursor. Putting one into the registry
            // would hand it straight to `changes_stream`, which has no
            // delta link to walk. Finish the inventory instead - the same
            // decision `establish_initial_cursor` makes at open.
            if account.is_inventory_cursor(&existing) {
                let fusion = crate::multiplexer::InventoryFusion {
                    account_id: account_id.clone(),
                    cursors: Arc::clone(&cursors),
                    control: control.cloned(),
                    coverage: Some(Arc::clone(&coverage)),
                    writer_tx: Some(writer.sender()),
                    generation: coverage.next_generation(),
                };
                return match fusion
                    .run_resume_with_broadcast(account, existing, Some(delivery))
                    .await?
                {
                    crate::multiplexer::FusionOutcome::Established
                    | crate::multiplexer::FusionOutcome::NoCursor => {
                        Ok(EstablishOrigin::ResumedStored)
                    }
                    crate::multiplexer::FusionOutcome::Terminated(error) => {
                        Err(Error::EstablishCursorTerminated(error))
                    }
                };
            }
            cursors.put(existing);
            return Ok(EstablishOrigin::ResumedStored);
        }
        Ok(None) => {}
        // Unlike `establish_one`, this path runs with a live reopen
        // listener, so an undecodable envelope is reported as a typed
        // error whose derived `RecoveryClass` is
        // `Engine(SchemaIncompatible)` and the listener runs the
        // account-wide schema-clear loop.
        Ok(Some(_)) | Err(Error::SchemaIncompatible) => {
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
            cursor.validate_envelope().map_err(|_| {
                Error::Account(crate::recovery::cursor_decode_failure(
                    bifrost_types::AccountOperation::EstablishCursor,
                ))
            })?;
            if persist_ready {
                // No coverage report: preserve the ledger rather than
                // asserting completeness a cursor establishment never proved.
                writer.persist_established(cursor.clone()).await?;
            }
            cursors.put(cursor);
            Ok(EstablishOrigin::CreatedFresh)
        }
        CursorEstablishment::EstablishViaInventory => {
            let fusion = crate::multiplexer::InventoryFusion {
                account_id: account_id.clone(),
                cursors: Arc::clone(&cursors),
                control: control.cloned(),
                coverage: Some(Arc::clone(&coverage)),
                writer_tx: Some(writer.sender()),
                generation: coverage.next_generation(),
            };
            match fusion
                .run_with_broadcast(account, scope, Some(delivery))
                .await?
            {
                crate::multiplexer::FusionOutcome::Established
                | crate::multiplexer::FusionOutcome::NoCursor => Ok(EstablishOrigin::CreatedFresh),
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
