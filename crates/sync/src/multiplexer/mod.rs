//! Per-account multiplexer.
//!
//! One task per account drives:
//! - `changes_stream(cursor)` for each known scope,
//! - the adaptive NOOP/STATUS poll cadence per scope,
//! - `scope_lifecycle_stream()` for scope creation / rename / deletion,
//! - inventory fusion for `EstablishViaInventory` scopes (the
//!   inventory's terminal `Done` carries the cursor, which the
//!   multiplexer registers exactly once).
//!
//! Output is a unified `SyncEvent<Change>` stream on the account's
//! broadcast channel.

pub mod changes;
pub mod fusion;
pub mod poll;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwap;
use bifrost_types::{
    Account, AccountError, AccountId, AccountOperation, ChangeCursor, Checkpoint, CursorScope,
    MembershipScope, ScopeLifecycle, SyncEvent,
};
use futures::stream::StreamExt;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::cancel::BoundaryView;
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::error::Error;
use crate::types::MultiplexerConfig;

pub use changes::{
    AckRequest, ChangesEvent, OperatorDecision, WriterRequest, drive_changes_stream,
};
pub use fusion::{FusionOutcome, InventoryFusion};
pub use poll::{AdaptiveCadence, PollSchedule};

/// Handle the engine stashes in the per-account slot for the
/// multiplexer task. Cancellation is rooted in the slot's shutdown
/// token; explicit shutdown shuts the task down between two
/// `boundary.peek()` calls.
#[derive(Debug)]
pub struct MultiplexerHandle {
    pub cancel: CancellationToken,
    pub changes_tx: broadcast::Sender<MultiplexerEvent>,
}

/// Unified output of the multiplexer: either a synced `Change` batch
/// (with optional checkpoint) or a per-scope warning/fatal.
///
/// Wrapped in an `Arc` so subscribers receive cheap clones from the
/// broadcast.
#[derive(Debug, Clone)]
pub struct MultiplexerEvent {
    pub scope: CursorScope,
    pub event: Arc<SyncEvent<bifrost_types::Change>>,
    pub checkpoint: Option<Checkpoint>,
    /// Engine-issued identity for THIS publication of `checkpoint`, to be
    /// passed back to `Engine::ack_checkpoint`.
    ///
    /// `Some` for every acknowledgeable publication. Checkpoint-bearing
    /// events return it through `ack_checkpoint`; repair events carry no
    /// checkpoint and return it through `ack_publication`. It exists because
    /// `Checkpoint` equality is value equality and cannot identify a
    /// publication: a backfill page whose content was entirely unrepresentable
    /// increments no item count and produces a checkpoint byte-identical to its
    /// predecessor, and inventory fusion publishes its final checkpoint twice.
    /// Acknowledging by value would then apply one publication's coverage claim
    /// to a different publication's checkpoint.
    pub publication: Option<crate::cursor::PublicationId>,
}

impl MultiplexerEvent {
    /// An event carrying no checkpoint, and so nothing to acknowledge.
    #[must_use]
    pub fn unacked(scope: CursorScope, event: Arc<SyncEvent<bifrost_types::Change>>) -> Self {
        Self {
            scope,
            event,
            checkpoint: None,
            publication: None,
        }
    }
}

/// Consumer-facing receiver that turns broadcast ring overflow into an
/// observable event. The overwritten batches cannot be replayed in-session,
/// but loss is never presented as an ordinary successful receive.
///
/// The receiver also owns the recovery half of a lag. Batches destroyed by
/// ring overflow had their checkpoints registered as expected before the
/// send, and only a consumer ack retires such a registration - so a lag
/// that merely warned would leave `pause` and `checkpoint_now` waiting for
/// acks the consumer can no longer produce. Surfacing lag without doing
/// this would convert in-session data loss into a permanent hang. The
/// receiver therefore abandons the account's outstanding registrations at
/// the moment it observes the gap, and reports the count in the warning.
pub struct ChangesReceiver {
    inner: broadcast::Receiver<MultiplexerEvent>,
    control: Option<crate::control::SyncControl>,
}

/// Every account keeps this many internal receivers alive solely to keep the
/// broadcast channel open. Publication sites use this helper instead of
/// duplicating knowledge of that channel topology.
const SENTINEL_RECEIVERS: usize = 1;

pub(crate) fn delivered_to_real_subscriber(delivered: usize) -> bool {
    delivered > SENTINEL_RECEIVERS
}

pub(crate) fn has_real_subscriber(tx: &broadcast::Sender<MultiplexerEvent>) -> bool {
    tx.receiver_count() > SENTINEL_RECEIVERS
}

impl ChangesReceiver {
    pub(crate) fn new(
        inner: broadcast::Receiver<MultiplexerEvent>,
        control: Option<crate::control::SyncControl>,
    ) -> Self {
        Self { inner, control }
    }

    pub async fn recv(&mut self) -> Result<MultiplexerEvent, broadcast::error::RecvError> {
        match self.inner.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Lagged(skipped)) => Ok(self.on_lag(skipped)),
            Err(error) => Err(error),
        }
    }

    pub fn try_recv(&mut self) -> Result<MultiplexerEvent, broadcast::error::TryRecvError> {
        match self.inner.try_recv() {
            Ok(event) => Ok(event),
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => Ok(self.on_lag(skipped)),
            Err(error) => Err(error),
        }
    }

    fn on_lag(&self, skipped: u64) -> MultiplexerEvent {
        let abandoned = self
            .control
            .as_ref()
            .map_or(0, crate::control::SyncControl::abandon_pending_checkpoints);
        lag_warning(skipped, abandoned)
    }
}

fn lag_warning(skipped: u64, abandoned: usize) -> MultiplexerEvent {
    let warning = bifrost_types::Warning::user_safe(
        bifrost_types::WarningKind::OperatorAttentionNeeded,
        format!(
            "change stream lagged and lost {skipped} batches; reconcile from the last acknowledged checkpoint"
        ),
    )
    .with_next_action(bifrost_types::DiagnosticText::user_safe(format!(
        "{abandoned} outstanding checkpoint registrations were abandoned because their batches \
         can no longer be delivered; re-read from the last durable checkpoint. Boundary waits \
         (pause / checkpoint_now) stay usable, but the checkpoint they report may predate the \
         batches that were lost."
    )));
    MultiplexerEvent {
        scope: CursorScope::Account,
        event: Arc::new(SyncEvent::Warning(warning)),
        checkpoint: None,
        publication: None,
    }
}

/// Reopen request raised by per-scope drivers when a stream ends with
/// an account-side failure. The engine listens on the receiver and
/// dispatches according to the carried `AccountError`'s recovery.
///
/// `scope` is `Option<CursorScope>`: scope-bound directives
/// (`Engine(RestartScope)`, `Engine(DowngradeCapabilityForScope)`,
/// `Engine(DowngradeStrategy)` with `ErrorScope::Cursor`) carry
/// `Some(scope)`. Account-wide directives (`RestartAccount`,
/// `SchemaIncompatible`, `OperatorOverrideRequired`)
/// carry `None`. Workers must not paper over an account-wide directive
/// with the worker's own scope - passing `Some(arbitrary_scope)` would
/// mask the directive's account-wide intent.
///
/// `#[non_exhaustive]` is deliberate and stays: the enum is `pub` and
/// re-exported from `lib.rs`, so new request kinds must not break
/// downstream matches. Adjudicated during the error-model close-out -
/// do not re-raise.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ReopenRequest {
    /// Account error observed at the scope's stream terminus.
    Recovery {
        scope: Option<CursorScope>,
        error: AccountError,
    },
}

/// Upper bound on the lifecycle reader's wait for a replacement connection.
///
/// Only a successful account reattach bumps the account generation. A reopen
/// that exhausts its retry budget, or that is queued behind a pause, produces
/// no generation change at all, so an unbounded wait would park the reader for
/// the process lifetime and silently stop folder/label lifecycle observation.
const LIFECYCLE_REOPEN_WAIT: Duration = Duration::from_secs(30);

/// Lifecycle-stream termination policy.
///
/// A terminal account error cannot be repaired by reconnecting the same dead
/// stream. An `Engine(RestartAccount)` directive replaces the connection this
/// reader is holding, so it hands the error over once and waits (bounded) for
/// the generation change rather than hammering a dead handle. Every other
/// directive - `RestartScope`, `DisableScope`, `SchemaIncompatible`,
/// `OperatorOverrideRequired`, the downgrades - is handled without swapping
/// the account, so no generation change is ever coming and the reader must
/// reconnect on its own backoff after handing the error over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleTermination {
    /// Forward once, then wait (bounded) for a replacement connection.
    AwaitReopen,
    /// Forward once, then reconnect this reader on backoff.
    ForwardAndReconnect,
    /// Reconnect on backoff; the engine has nothing to decide.
    Reconnect,
    /// Report once and stop this reader.
    Terminate,
}

fn lifecycle_termination(error: &AccountError) -> LifecycleTermination {
    use crate::recovery::{RecoveryPlan, plan_recovery};
    use bifrost_types::EngineDirective;

    match plan_recovery(error.clone()) {
        RecoveryPlan::Engine(EngineDirective::RestartAccount) => LifecycleTermination::AwaitReopen,
        RecoveryPlan::Engine(_) => LifecycleTermination::ForwardAndReconnect,
        RecoveryPlan::Terminal(_) => LifecycleTermination::Terminate,
        RecoveryPlan::Retry(_) | RecoveryPlan::Reconcile(_) => LifecycleTermination::Reconnect,
    }
}

/// A per-scope poll task's cancellation token, tagged with the
/// identity of the task that owns it.
///
/// The generation is what makes cleanup safe. A task removing its
/// entry by key alone can evict a SUCCESSOR's token: task A exits
/// naturally while its scope is momentarily absent from the registry
/// (mid-`restart_scope`), the 1s scan in `Multiplexer::run` sees a
/// re-established cursor with no live token and spawns task B, and
/// A's cleanup then deletes B's entry. The next scan sees no token and
/// spawns C, leaving B and C polling the same scope forever - doubled
/// wire traffic, doubled broadcasts, and two concurrent producers on
/// one lane+scope, which is exactly what `SyncControl`'s checkpoint
/// supersession assumes cannot happen.
#[derive(Debug, Clone)]
pub struct ScopeToken {
    generation: u64,
    token: CancellationToken,
}

impl ScopeToken {
    fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    fn cancel(&self) {
        self.token.cancel();
    }
}

/// Per-scope poll tokens, keyed by the scope the task polls.
pub type ScopeTokens = Arc<StdMutex<HashMap<CursorScope, ScopeToken>>>;

/// Monotonic source of `ScopeToken::generation`. Process-wide rather
/// than per-multiplexer: the value only has to be unique, and threading
/// another `Arc` through the poll-spawn argument list buys nothing.
static SCOPE_TOKEN_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Multiplexer task state. Constructed in `engine::attach` and run on
/// a `tokio::spawn`.
pub struct Multiplexer {
    pub account_id: AccountId,
    pub account: Arc<ArcSwap<Arc<dyn Account>>>,
    pub account_generation: watch::Receiver<u64>,
    pub cursors: Arc<CursorRegistry>,
    pub config: MultiplexerConfig,
    pub boundary: BoundaryView,
    pub changes_tx: broadcast::Sender<MultiplexerEvent>,
    pub control: SyncControl,
    pub shutdown: CancellationToken,
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
    pub ack_tx: Option<mpsc::Sender<WriterRequest>>,
    /// Per-scope cancellation tokens keyed by membership-id so
    /// `ScopeLifecycle::Deleted` can stop the matching poll task.
    pub scope_tokens: ScopeTokens,
    /// Engine-wide throttle bucket. Poll tasks consult it before each
    /// drive so a tenant- or account-wide `Retry-After` recorded by one
    /// scope (or a sibling account) pauses the others; the Retry arm
    /// records deadlines it observes.
    pub throttles: Arc<StdMutex<crate::recovery::ThrottleBucket>>,
    pub scheduler: crate::scheduler::Scheduler,
}

impl Multiplexer {
    /// Apply the adaptive cadence rule to a scope: halve on change,
    /// double after five consecutive no-change ticks, clamped to
    /// `[poll_min, poll_max]`. Pure function; tested below.
    #[must_use]
    pub fn updated_cadence(
        cur: AdaptiveCadence,
        seen_change: bool,
        min: Duration,
        max: Duration,
    ) -> AdaptiveCadence {
        if seen_change {
            AdaptiveCadence {
                interval: (cur.interval / 2).max(min),
                no_change_streak: 0,
            }
        } else {
            let new_streak = cur.no_change_streak.saturating_add(1);
            let interval = if new_streak >= 5 {
                cur.interval.saturating_mul(2).min(max)
            } else {
                cur.interval
            };
            AdaptiveCadence {
                interval,
                no_change_streak: if new_streak >= 5 { 0 } else { new_streak },
            }
        }
    }

    /// Bookkeeping helper: register a cursor in the registry. Idempotent.
    pub fn register_cursor(&self, cursor: ChangeCursor) {
        self.cursors.put(cursor);
    }

    /// Run the multiplexer loop until the shutdown token is cancelled.
    ///
    /// Spawns per-scope poll tasks (each driving
    /// `changes_stream(cursor)` with adaptive cadence) and a single
    /// `scope_lifecycle_stream` task. New scopes added at runtime get
    /// their own poll task via `ScopeLifecycle::Created`; deleted
    /// scopes have their per-scope cancellation token tripped and the
    /// cursor entry dropped from the registry.
    pub async fn run(self) {
        let Self {
            account_id,
            account,
            account_generation,
            cursors,
            config,
            boundary,
            changes_tx,
            control,
            shutdown,
            reopen_tx,
            ack_tx,
            scope_tokens,
            throttles,
            scheduler,
        } = self;

        // Snapshot the known scopes from the registry; the engine has
        // populated it via `establish_one` before spawning us.
        spawn_missing_scope_polls(
            account_id.clone(),
            Arc::clone(&account),
            Arc::clone(&cursors),
            config,
            boundary.clone(),
            changes_tx.clone(),
            control.clone(),
            shutdown.clone(),
            reopen_tx.clone(),
            ack_tx.clone(),
            Arc::clone(&scope_tokens),
            Arc::clone(&throttles),
            scheduler.clone(),
        );

        // Drive scope_lifecycle in the background. `Created` asks the
        // engine to establish a cursor; the scan loop below notices it
        // and starts polling. `Deleted` cancels and drops the matching
        // scope; `Renamed` is delete+create on a fresh CursorScope id.
        let lifecycle_account = Arc::clone(&account);
        let lifecycle_shutdown = shutdown.clone();
        let lifecycle_cursors = Arc::clone(&cursors);
        let lifecycle_tokens = Arc::clone(&scope_tokens);
        let lifecycle_reopen = reopen_tx.clone();
        let lifecycle_changes = changes_tx.clone();
        let mut lifecycle_generation = account_generation;
        let lifecycle_handle = tokio::spawn(async move {
            let mut reconnect_delay = Duration::from_millis(50);
            loop {
                let acc = lifecycle_account.load_full();
                let mut stream = acc.scope_lifecycle_stream();
                let mut received = false;
                let mut reopened = false;
                loop {
                    tokio::select! {
                            () = lifecycle_shutdown.cancelled() => return,
                            changed = lifecycle_generation.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                                reopened = true;
                                break;
                            }
                            next = stream.next() => {
                            let Some(ev) = next else { break; };
                            if matches!(
                                &ev,
                                bifrost_types::ScopeLifecycleEvent::Lifecycle(_)
                            ) {
                                received = true;
                            }
                            let lifecycle = match ev {
                                bifrost_types::ScopeLifecycleEvent::Lifecycle(lc) => lc,
                                bifrost_types::ScopeLifecycleEvent::Terminated(err) => {
                                    let policy = lifecycle_termination(&err);
                                    match policy {
                                        LifecycleTermination::AwaitReopen
                                        | LifecycleTermination::ForwardAndReconnect => {
                                            // Hand the classified error to the
                                            // engine exactly once. The reopen
                                            // path owns the three-strike
                                            // budget, so one dead lifecycle
                                            // stream cannot enqueue unbounded
                                            // account reopens.
                                            if lifecycle_reopen
                                                .send(ReopenRequest::Recovery {
                                                    scope: None,
                                                    error: err,
                                                })
                                                .await
                                                .is_err()
                                            {
                                                return;
                                            }
                                            if policy == LifecycleTermination::AwaitReopen {
                                                // A replacement connection can
                                                // revive this stream, so prefer
                                                // waiting for it - but bound the
                                                // wait, because a failed or
                                                // pause-queued reopen never
                                                // bumps the generation and this
                                                // reader must not park forever.
                                                tokio::select! {
                                                    () = lifecycle_shutdown.cancelled() => return,
                                                    changed = lifecycle_generation.changed() => {
                                                        if changed.is_err() {
                                                            return;
                                                        }
                                                        reopened = true;
                                                    }
                                                    () = tokio::time::sleep(LIFECYCLE_REOPEN_WAIT) => {}
                                                }
                                            }
                                            break;
                                        }
                                        LifecycleTermination::Terminate => {
                                            // Auth/policy/provider terminal
                                            // errors cannot be revived by an
                                            // account reopen. Surface the
                                            // classified failure once and end
                                            // this lifecycle reader.
                                            let _ = lifecycle_changes.send(MultiplexerEvent {
                                                scope: CursorScope::Account,
                                                event: Arc::new(SyncEvent::Terminated(err)),
                                                checkpoint: None,
                                                publication: None,
                                            });
                                            return;
                                        }
                                        LifecycleTermination::Reconnect => break,
                                    }
                                }
                                _ => continue,
                            };
                            match lifecycle {
                                ScopeLifecycle::Created(membership) => {
                                    for scope in
                                        membership_to_cursor_scopes(&lifecycle_cursors, &membership)
                                    {
                                        // Track the new scope. The cursor
                                        // registry has no entry yet; the
                                        // poll task will see snapshot=None
                                        // and exit cleanly unless the
                                        // engine establishes a cursor for
                                        // it first. We raise a Recovery
                                        // request so the engine drives the
                                        // initial cursor establishment.
                                        // Synthesize a cursor-invalid
                                        // `AccountError` for this scope so
                                        // the engine's `handle_account_error`
                                        // dispatch derives
                                        // `Engine(RestartScope(scope))`
                                        // through the same path it uses for
                                        // every other recovery.
                                        let error = crate::recovery::restart_scope_error(
                                            scope.clone(),
                                            AccountOperation::SyncChanges,
                                        );
                                        let _ = lifecycle_reopen
                                            .send(ReopenRequest::Recovery {
                                                scope: Some(scope.clone()),
                                                error,
                                            })
                                            .await;
                                    }
                                }
                                ScopeLifecycle::Deleted(membership) => {
                                    let scopes = lifecycle_cursors.scopes_for_membership(&membership);
                                    for scope in scopes {
                                        cancel_scope_token(&lifecycle_tokens, &scope);
                                        lifecycle_cursors.delete(&scope);
                                    }
                                }
                                ScopeLifecycle::Renamed { old, new, .. } => {
                                    // Treat as delete + create: cancel the
                                    // old scope's poll task, drop the
                                    // cursor, then trigger a fresh
                                    // establishment for the new id.
                                    let old_scopes =
                                        lifecycle_cursors.scopes_for_membership(&old);
                                    for scope in old_scopes {
                                        cancel_scope_token(&lifecycle_tokens, &scope);
                                        lifecycle_cursors.delete(&scope);
                                    }
                                    for scope in membership_to_cursor_scopes(&lifecycle_cursors, &new) {
                                        // Synthesize a cursor-invalid
                                        // `AccountError` for this scope so
                                        // the engine's `handle_account_error`
                                        // dispatch derives
                                        // `Engine(RestartScope(scope))`
                                        // through the same path it uses for
                                        // every other recovery.
                                        let error = crate::recovery::restart_scope_error(
                                            scope.clone(),
                                            AccountOperation::SyncChanges,
                                        );
                                        let _ = lifecycle_reopen
                                            .send(ReopenRequest::Recovery {
                                                scope: Some(scope.clone()),
                                                error,
                                            })
                                            .await;
                                    }
                                }
                                _ => {}
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
                tokio::select! {
                    () = lifecycle_shutdown.cancelled() => return,
                    () = tokio::time::sleep(reconnect_delay) => {}
                }
                reconnect_delay = reconnect_delay
                    .saturating_mul(2)
                    .min(Duration::from_secs(30));
            }
        });

        // Park until shutdown while periodically noticing cursors
        // established by deferred inventory or lifecycle recovery.
        let mut scan = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                _ = scan.tick() => {
                    spawn_missing_scope_polls(
                        account_id.clone(),
                        Arc::clone(&account),
                        Arc::clone(&cursors),
                        config,
                        boundary.clone(),
                        changes_tx.clone(),
                        control.clone(),
                        shutdown.clone(),
                        reopen_tx.clone(),
                        ack_tx.clone(),
                        Arc::clone(&scope_tokens),
                        Arc::clone(&throttles),
                        scheduler.clone(),
                    );
                }
            }
        }

        // Cancel every tracked scope token so per-scope polls exit
        // cleanly. Tokens are dropped as part of the map clear.
        let drained: Vec<ScopeToken> = {
            let mut g = scope_tokens.lock().expect("poisoned");
            g.drain().map(|(_, t)| t).collect()
        };
        for token in drained {
            token.cancel();
        }
        lifecycle_handle.abort();
    }

    /// Apply an inventory-fusion `Done`: only the terminal `Done` of
    /// an `EstablishViaInventory` scope carries the cursor that should
    /// be registered. Mid-inventory `Batch` checkpoints are ignored.
    pub fn fuse_inventory_done(
        &self,
        scope: &CursorScope,
        done: Option<Checkpoint>,
    ) -> Result<(), Error> {
        match done {
            Some(Checkpoint::Change(c)) if &c.scope == scope => {
                // Last account-authored seam before the registry. A wrong
                // outer version is a bad cursor, not a bug in this engine, so
                // it classifies as schema recovery rather than reaching the
                // registry's unreachable-by-construction debug assertion.
                c.validate_envelope().map_err(|_| {
                    Error::Account(crate::recovery::cursor_decode_failure(
                        bifrost_types::AccountOperation::SyncInventory,
                    ))
                })?;
                self.cursors.put(c);
                Ok(())
            }
            Some(Checkpoint::Change(_)) => Err(Error::Other(
                "inventory Done carried a Change checkpoint for the wrong scope".into(),
            )),
            Some(Checkpoint::Backfill(_)) => Err(Error::Other(
                "inventory Done carried a Backfill checkpoint; expected Change".into(),
            )),
            // `Checkpoint` is `#[non_exhaustive]`; reject unknown
            // variants so the engine fails closed.
            Some(_) => Err(Error::Other(
                "inventory Done carried an unknown Checkpoint variant".into(),
            )),
            None => Err(Error::Other(
                "inventory Done carried no checkpoint for EstablishViaInventory scope".into(),
            )),
        }
    }
}

/// Spawn one polling task for a single scope and track its
/// cancellation token in `scope_tokens` so lifecycle events can stop
/// it later.
#[allow(clippy::too_many_arguments)]
fn spawn_and_track_scope_poll(
    account_id: AccountId,
    account: Arc<ArcSwap<Arc<dyn Account>>>,
    cursors: Arc<CursorRegistry>,
    config: MultiplexerConfig,
    boundary: BoundaryView,
    changes_tx: broadcast::Sender<MultiplexerEvent>,
    control: SyncControl,
    shutdown: CancellationToken,
    reopen_tx: mpsc::Sender<ReopenRequest>,
    ack_tx: Option<mpsc::Sender<WriterRequest>>,
    scope_tokens: ScopeTokens,
    throttles: Arc<StdMutex<crate::recovery::ThrottleBucket>>,
    scheduler: crate::scheduler::Scheduler,
    scope: CursorScope,
) {
    let scope_cancel = shutdown.child_token();
    let generation = SCOPE_TOKEN_GENERATION.fetch_add(1, Ordering::Relaxed);
    scope_tokens.lock().expect("poisoned").insert(
        scope.clone(),
        ScopeToken {
            generation,
            token: scope_cancel.clone(),
        },
    );
    let cleanup_tokens = Arc::clone(&scope_tokens);
    let cleanup_scope = scope.clone();
    tokio::spawn(async move {
        spawn_scope_poll_inner(
            account_id,
            account,
            cursors,
            config,
            boundary,
            changes_tx,
            control,
            shutdown,
            scope_cancel,
            reopen_tx,
            ack_tx,
            throttles,
            scheduler,
            scope,
        )
        .await;
        retire_scope_token(&cleanup_tokens, &cleanup_scope, generation);
    });
}

/// Drop an exiting poll task's registration, by identity.
///
/// Removing by key alone is what produces the duplicate-poll
/// interleaving described on [`ScopeToken`]: the entry under this scope
/// may already belong to a successor spawned by the 1s scan while this
/// task was on its way out, and evicting it leaves that successor
/// untracked, so the next scan spawns a second live poll for the same
/// scope.
fn retire_scope_token(tokens: &ScopeTokens, scope: &CursorScope, generation: u64) {
    let mut g = tokens.lock().expect("poisoned");
    if matches!(g.get(scope), Some(entry) if entry.generation == generation) {
        g.remove(scope);
    }
}

fn cancel_scope_token(tokens: &ScopeTokens, scope: &CursorScope) {
    let current = tokens.lock().expect("poisoned").get(scope).cloned();
    if let Some(entry) = current {
        entry.token.cancel();
        retire_scope_token(tokens, scope, entry.generation);
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_missing_scope_polls(
    account_id: AccountId,
    account: Arc<ArcSwap<Arc<dyn Account>>>,
    cursors: Arc<CursorRegistry>,
    config: MultiplexerConfig,
    boundary: BoundaryView,
    changes_tx: broadcast::Sender<MultiplexerEvent>,
    control: SyncControl,
    shutdown: CancellationToken,
    reopen_tx: mpsc::Sender<ReopenRequest>,
    ack_tx: Option<mpsc::Sender<WriterRequest>>,
    scope_tokens: ScopeTokens,
    throttles: Arc<StdMutex<crate::recovery::ThrottleBucket>>,
    scheduler: crate::scheduler::Scheduler,
) {
    for scope in cursors.all_scopes() {
        let should_spawn = {
            let mut g = scope_tokens.lock().expect("poisoned");
            match g.get(&scope) {
                Some(token) if !token.is_cancelled() => false,
                Some(_) => {
                    g.remove(&scope);
                    true
                }
                None => true,
            }
        };
        if should_spawn {
            spawn_and_track_scope_poll(
                account_id.clone(),
                Arc::clone(&account),
                Arc::clone(&cursors),
                config,
                boundary.clone(),
                changes_tx.clone(),
                control.clone(),
                shutdown.clone(),
                reopen_tx.clone(),
                ack_tx.clone(),
                Arc::clone(&scope_tokens),
                Arc::clone(&throttles),
                scheduler.clone(),
                scope,
            );
        }
    }
}

/// Per-scope poll loop. Drives `changes_stream(cursor)` and sleeps
/// for the scope's adaptive cadence between passes. Stream-end
/// recovery actions are propagated to the engine via `ReopenRequest`
/// with the originating `AccountError`.
#[allow(clippy::too_many_arguments)]
async fn spawn_scope_poll_inner(
    account_id: AccountId,
    account: Arc<ArcSwap<Arc<dyn Account>>>,
    cursors: Arc<CursorRegistry>,
    config: MultiplexerConfig,
    mut boundary: BoundaryView,
    changes_tx: broadcast::Sender<MultiplexerEvent>,
    control: SyncControl,
    shutdown: CancellationToken,
    scope_cancel: CancellationToken,
    reopen_tx: mpsc::Sender<ReopenRequest>,
    ack_tx: Option<mpsc::Sender<WriterRequest>>,
    throttles: Arc<StdMutex<crate::recovery::ThrottleBucket>>,
    scheduler: crate::scheduler::Scheduler,
    scope: CursorScope,
) {
    let mut cadence = AdaptiveCadence {
        interval: config.poll_initial,
        no_change_streak: 0,
    };
    loop {
        if shutdown.is_cancelled() || scope_cancel.is_cancelled() {
            return;
        }
        // If the boundary is asking us to pause, park here until it
        // changes back to Run (or Stop / Shutdown trips).
        if matches!(boundary.peek(), crate::cancel::BoundaryRequest::Pause) {
            // Park until the request changes.
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = scope_cancel.cancelled() => return,
                    next = boundary.changed() => {
                        match next {
                            None => return,
                            Some(crate::cancel::BoundaryRequest::Pause) => continue,
                            Some(_) => break,
                        }
                    }
                }
            }
        }
        // Honor any account-wide throttle deadline (recorded by a
        // sibling scope's Retry-After, or by a sibling account via a
        // shared tenant/provider key) before driving the wire.
        // Re-checked after waking: a longer deadline can land while
        // this scope sleeps off the first one.
        while let Some(wait) =
            crate::recovery::account_throttle_wait(&throttles, &account_id, SystemTime::now())
        {
            tracing::debug!(
                target: "bifrost.sync.changes",
                account = ?account_id,
                scope = ?scope,
                wait_secs = wait.as_secs(),
                "poll deferred by shared throttle deadline"
            );
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = scope_cancel.cancelled() => return,
                () = tokio::time::sleep(wait) => {}
            }
        }
        let _admission = tokio::select! {
            () = shutdown.cancelled() => return,
            () = scope_cancel.cancelled() => return,
            permit = scheduler.admit(
                account_id.clone(),
                control.priority_snapshot(),
                crate::scheduler::WorkKind::Sync,
            ) => match permit {
                Ok(permit) => permit,
                Err(error) => {
                    // A refused admission is transient (a full lane, or a
                    // scheduler shutting down). Exiting here would retire
                    // the scope's poll loop permanently for a condition
                    // that clears on its own, so back off one cadence
                    // step and re-enter instead.
                    tracing::warn!(
                        target: "bifrost.sync.scheduler",
                        scope = ?scope,
                        %error,
                        "poll admission refused; retrying at the next cadence"
                    );
                    tokio::select! {
                        () = shutdown.cancelled() => return,
                        () = scope_cancel.cancelled() => return,
                        () = tokio::time::sleep(cadence.interval) => {}
                    }
                    continue;
                }
            },
        };
        let driven = cursors
            .with_drive(&scope, |cursor, registry_generation| {
                let pre_advance_state = cursor.server_state.bytes.clone();
                let acc_arc = account.load_full();
                let drive_cursors = Arc::clone(&cursors);
                let advance_cursors = Arc::clone(&cursors);
                let advance_scope = scope.clone();
                let scope = scope.clone();
                let account_id = account_id.clone();
                let changes_tx = changes_tx.clone();
                let boundary = boundary.clone();
                let control = control.clone();
                let ack_tx = ack_tx.clone();
                async move {
                    let acc: &dyn Account = acc_arc.as_ref().as_ref();
                    let outcome = drive_changes_stream(
                        acc,
                        scope,
                        cursor,
                        drive_cursors,
                        account_id,
                        changes_tx,
                        boundary,
                        Some(control),
                        ack_tx,
                        Some(registry_generation),
                    )
                    .await;
                    // Read the post-drive cursor while the lease is still
                    // held. The lease no longer covers the tail of the poll
                    // iteration, so a push reconcile can advance this scope
                    // the instant we return; comparing out there would credit
                    // this drive with the reconciler's progress and hold the
                    // cadence at `poll_min` on a scope this loop never moved.
                    let advanced = advance_cursors
                        .snapshot(&advance_scope)
                        .is_some_and(|c| c.server_state.bytes != pre_advance_state);
                    (advanced, outcome)
                }
            })
            .await;
        drop(_admission);
        let Some((advanced, outcome)) = driven else {
            // Scope was removed from the registry; exit cleanly.
            return;
        };
        let recovered = handle_drive_outcome(
            &scope,
            outcome,
            advanced,
            &reopen_tx,
            &account_id,
            &throttles,
        )
        .await;

        cadence = Multiplexer::updated_cadence(
            cadence,
            recovered.advanced,
            config.poll_min,
            config.poll_max,
        );
        if recovered.exit {
            return;
        }
        if matches!(
            boundary.peek(),
            crate::cancel::BoundaryRequest::CheckpointNow
        ) {
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = scope_cancel.cancelled() => return,
                    next = boundary.changed() => {
                        match next {
                            None | Some(crate::cancel::BoundaryRequest::Stop) => return,
                            Some(crate::cancel::BoundaryRequest::CheckpointNow) => continue,
                            Some(crate::cancel::BoundaryRequest::Pause) => break,
                            Some(crate::cancel::BoundaryRequest::Run) => break,
                        }
                    }
                }
            }
            if matches!(boundary.peek(), crate::cancel::BoundaryRequest::Pause) {
                continue;
            }
        }
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = scope_cancel.cancelled() => return,
            () = tokio::time::sleep(cadence.interval) => {}
        }
    }
}

struct DriveRecovery {
    advanced: bool,
    exit: bool,
}

async fn handle_drive_outcome(
    scope: &CursorScope,
    outcome: Result<ChangesEvent, Error>,
    // `advanced`: whether this drive moved the scope's cursor, measured by
    // the caller while it still held the drive lease. Deliberately not
    // re-read here - the lease is already released by the time this runs, so
    // a concurrent push reconcile could be credited to this drive.
    advanced: bool,
    reopen_tx: &mpsc::Sender<ReopenRequest>,
    account_id: &AccountId,
    throttles: &StdMutex<crate::recovery::ThrottleBucket>,
) -> DriveRecovery {
    // Account errors already carry the complete recovery verdict. Normalize
    // them onto the same path as an account-authored Terminated event instead
    // of letting the generic engine-error arm silently choose "retry".
    let outcome = match outcome {
        Err(Error::Account(error)) => Ok(ChangesEvent::Terminated(error)),
        other => other,
    };
    match outcome {
        Ok(ChangesEvent::Advanced | ChangesEvent::Done) => DriveRecovery {
            advanced,
            exit: false,
        },
        Ok(ChangesEvent::Stopped) => DriveRecovery {
            advanced: false,
            exit: true,
        },
        // Pause is a temporary park; the poll loop's boundary check picks it
        // up at the top of the next iteration.
        Ok(ChangesEvent::Paused) => DriveRecovery {
            advanced,
            exit: false,
        },
        Ok(ChangesEvent::Terminated(error)) => {
            use crate::recovery::{
                RecoveryPlan, directive_target_scope, plan_recovery, retry_delay,
            };
            // Dispatch via `plan_recovery` so the closed `RecoveryPlan`
            // drives the branch. Retry and reconcile keep the poll
            // loop alive; engine directives hand off to the reopen
            // listener carrying the original account error; terminal
            // errors stop the loop (the broadcast already carried the
            // terminating event). The account error needs to ride
            // along on the engine directive arm; we clone it before
            // planning.
            let original = error.clone();
            match plan_recovery(error) {
                RecoveryPlan::Retry(advice) => {
                    // Share any provider-documented throttle deadline
                    // with sibling scopes (and, via tenant/provider
                    // keys, sibling accounts) before sleeping it off
                    // locally.
                    crate::recovery::record_throttle(throttles, account_id, &advice, &original);
                    let delay = retry_delay(
                        &advice,
                        std::time::SystemTime::now(),
                        std::time::Duration::from_secs(1),
                    );
                    tokio::time::sleep(delay).await;
                    DriveRecovery {
                        advanced: false,
                        exit: false,
                    }
                }
                RecoveryPlan::Reconcile(advice) => {
                    crate::recovery::record_reconcile_throttle(
                        throttles, account_id, &advice, &original,
                    );
                    // A read stream's reconcile collapses to "rerun
                    // this scope soon". Sleep briefly so we do not
                    // hot-spin, then re-enter the poll loop. The
                    // mutation-side `Reconcile` actions (CheckTarget /
                    // DedupeByClientId) drive through the mutation
                    // pipeline; the change-stream side does not have a
                    // per-item lane to dedupe.
                    let delay = crate::recovery::reconcile_delay(
                        &advice,
                        std::time::SystemTime::now(),
                        std::time::Duration::from_secs(1),
                    );
                    tokio::time::sleep(delay).await;
                    DriveRecovery {
                        advanced: false,
                        exit: false,
                    }
                }
                RecoveryPlan::Engine(directive) => {
                    let directive_scope = directive_target_scope(&directive);
                    let _ = reopen_tx
                        .send(ReopenRequest::Recovery {
                            scope: directive_scope,
                            error: original,
                        })
                        .await;
                    DriveRecovery {
                        advanced: false,
                        exit: false,
                    }
                }
                RecoveryPlan::Terminal(fatal) => {
                    let err = fatal.as_ref();
                    let view = err.telemetry_fields();
                    tracing::warn!(
                        target: "bifrost.sync.changes",
                        account = ?account_id,
                        scope = ?scope,
                        kind = view.kind_discriminant,
                        message_key = view.message_key,
                        recovery = view.recovery_discriminant,
                        operation = ?view.operation,
                        "changes stream terminated with terminal recovery"
                    );
                    DriveRecovery {
                        advanced: false,
                        exit: true,
                    }
                }
            }
        }
        Err(err) => {
            tracing::warn!(
                target: "bifrost.sync.changes",
                account = ?account_id,
                scope = ?scope,
                error = %err,
                "drive_changes_stream errored"
            );
            DriveRecovery {
                advanced: false,
                exit: false,
            }
        }
    }
}

/// Map a `MembershipScope` from `ScopeLifecycle` to newly-required
/// cursor scopes using the account's registered cursor topology.
///
/// Account-wide and type-wide cursor models already cover a newly
/// created membership, so they produce no scope. Per-folder models
/// preserve their existing shape, including every registered
/// `FolderType` object type. An empty or unrecognized topology also
/// produces no scope rather than inventing one the protocol never
/// advertised.
#[must_use]
pub fn membership_to_cursor_scopes(
    cursors: &CursorRegistry,
    membership: &MembershipScope,
) -> Vec<CursorScope> {
    use bifrost_types::FolderId;
    let registered = cursors.all_scopes();
    if registered
        .iter()
        .any(|scope| matches!(scope, CursorScope::Account | CursorScope::Type(_)))
    {
        return Vec::new();
    }
    match membership {
        MembershipScope::Folder(folder) => folder_cursor_shapes(&registered, folder.clone()),
        MembershipScope::Mailbox(mailbox) => {
            folder_cursor_shapes(&registered, FolderId(mailbox.0.clone()))
        }
        MembershipScope::Query(q)
            if registered
                .iter()
                .any(|scope| matches!(scope, CursorScope::Query(_))) =>
        {
            vec![CursorScope::Query(q.clone())]
        }
        _ => Vec::new(),
    }
}

fn folder_cursor_shapes(
    registered: &[CursorScope],
    folder: bifrost_types::FolderId,
) -> Vec<CursorScope> {
    let mut scopes = Vec::new();
    if registered
        .iter()
        .any(|scope| matches!(scope, CursorScope::Folder(_)))
    {
        scopes.push(CursorScope::Folder(folder.clone()));
    }
    for scope in registered {
        if let CursorScope::FolderType { ty, .. } = scope {
            let candidate = CursorScope::FolderType {
                folder: folder.clone(),
                ty: *ty,
            };
            if !scopes.contains(&candidate) {
                scopes.push(candidate);
            }
        }
    }
    scopes
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{
        AccountErrorBuilder, AccountErrorKind, AccountOperation, AuthCause, AuthErrorKind, Cause,
        StateCause, SyncStateErrorKind,
    };

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn scope_tokens() -> ScopeTokens {
        Arc::new(StdMutex::new(HashMap::new()))
    }

    fn warning_event(message: &str) -> MultiplexerEvent {
        MultiplexerEvent {
            scope: CursorScope::Account,
            event: Arc::new(SyncEvent::Warning(bifrost_types::Warning::user_safe(
                bifrost_types::WarningKind::Other,
                message,
            ))),
            checkpoint: None,
            publication: None,
        }
    }

    fn lag_test_control() -> crate::control::SyncControl {
        let (boundary, _view) = crate::cancel::Boundary::new();
        let (priority, _priority_view) =
            tokio::sync::watch::channel(bifrost_types::Priority::Normal);
        let (bandwidth, _bandwidth_view) = tokio::sync::watch::channel(None);
        crate::control::SyncControl::new(
            bifrost_types::AccountId("lag".into()),
            boundary,
            priority,
            bandwidth,
        )
    }

    fn account_change_checkpoint(state: &[u8]) -> Checkpoint {
        Checkpoint::Change(bifrost_types::ChangeCursor {
            scope: CursorScope::Account,
            server_state: bifrost_types::OpaqueChangeState {
                protocol: bifrost_types::ProtocolKind::Imap,
                envelope_version: 1,
                bytes: state.to_vec(),
            },
            advanced_through: None,
            envelope_version: 1,
        })
    }

    #[tokio::test]
    async fn account_error_from_driver_routes_through_recovery_plan() {
        let scope = CursorScope::Account;
        let cursors = CursorRegistry::new();
        cursors.put(match account_change_checkpoint(b"unchanged") {
            Checkpoint::Change(cursor) => cursor,
            _ => unreachable!(),
        });
        let (reopen_tx, mut reopen_rx) = mpsc::channel(1);
        let error = crate::recovery::cursor_decode_failure(AccountOperation::SyncChanges);
        let recovery = handle_drive_outcome(
            &scope,
            Err(Error::Account(error.clone())),
            false,
            &reopen_tx,
            &AccountId("routing".into()),
            &StdMutex::new(crate::recovery::ThrottleBucket::default()),
        )
        .await;

        assert!(!recovery.exit);
        let request = reopen_rx.try_recv().expect("engine directive forwarded");
        assert!(matches!(
            request,
            ReopenRequest::Recovery { error: routed, .. }
                if routed.kind() == error.kind()
        ));
    }

    /// A lag destroys batches whose checkpoints are already registered
    /// as expected. If the receiver only warned, those registrations
    /// would gate `pause` / `checkpoint_now` forever: surfacing the loss
    /// would have converted it into a permanent hang.
    /// `try_recv` shares the lag path, so it must abandon too - a
    /// polling consumer that never calls `recv` would otherwise wedge
    /// the boundary exactly as the awaiting one used to.
    #[tokio::test]
    async fn try_recv_takes_the_same_lag_recovery_path() {
        let (tx, sentinel) = broadcast::channel(1);
        let control = lag_test_control();
        let mut receiver = ChangesReceiver::new(tx.subscribe(), Some(control.clone()));
        control.expect_checkpoint(account_change_checkpoint(b"lost"));
        tx.send(warning_event("overwritten"))
            .expect("receivers live");
        tx.send(warning_event("retained")).expect("receivers live");

        let lag = receiver.try_recv().expect("lag becomes an event");
        assert!(matches!(
            lag.event.as_ref(),
            SyncEvent::Warning(warning)
                if warning.kind == bifrost_types::WarningKind::OperatorAttentionNeeded
        ));
        assert!(
            control.abandon_pending_checkpoints() == 0,
            "try_recv's lag must already have abandoned the registration"
        );
        drop(sentinel);
    }

    #[tokio::test(start_paused = true)]
    async fn broadcast_lag_releases_boundary_waiters_that_can_never_be_acked() {
        use bifrost_types::Control as _;

        let (tx, sentinel) = broadcast::channel(1);
        let control = lag_test_control();
        let mut receiver = ChangesReceiver::new(tx.subscribe(), Some(control.clone()));
        control.expect_checkpoint(account_change_checkpoint(b"lost"));
        tx.send(warning_event("overwritten"))
            .expect("receivers live");
        tx.send(warning_event("retained")).expect("receivers live");

        let pause = tokio::spawn({
            let control = control.clone();
            async move { control.pause().await }
        });
        tokio::task::yield_now().await;
        assert!(
            !pause.is_finished(),
            "the outstanding registration must gate the boundary before the lag is observed"
        );

        let _lag = receiver.recv().await.expect("lag becomes an event");
        // Bounded so the regression reports as a failure rather than a
        // hung test: the defect this pins IS an unbounded wait.
        let released = tokio::time::timeout(std::time::Duration::from_secs(5), pause)
            .await
            .expect("a lost batch's registration must stop gating the boundary");
        assert_eq!(
            released.expect("pause task").expect("pause"),
            bifrost_types::DurableCheckpointSet::default()
        );
        drop(sentinel);
    }

    #[tokio::test]
    async fn changes_receiver_surfaces_broadcast_lag_as_operator_warning() {
        let (tx, sentinel) = broadcast::channel(1);
        let mut receiver = ChangesReceiver::new(tx.subscribe(), None);
        tx.send(warning_event("overwritten"))
            .expect("receivers live");
        tx.send(warning_event("retained")).expect("receivers live");

        let lag = receiver.recv().await.expect("lag becomes an event");
        assert!(matches!(
            lag.event.as_ref(),
            SyncEvent::Warning(warning)
                if warning.kind == bifrost_types::WarningKind::OperatorAttentionNeeded
        ));
        let retained = receiver
            .recv()
            .await
            .expect("retained event follows warning");
        assert!(matches!(
            retained.event.as_ref(),
            SyncEvent::Warning(warning)
                if warning.kind == bifrost_types::WarningKind::Other
        ));
        drop(sentinel);
    }

    fn track(tokens: &ScopeTokens, scope: &CursorScope) -> u64 {
        let generation = SCOPE_TOKEN_GENERATION.fetch_add(1, Ordering::Relaxed);
        tokens.lock().expect("poisoned").insert(
            scope.clone(),
            ScopeToken {
                generation,
                token: CancellationToken::new(),
            },
        );
        generation
    }

    /// The ordinary case: a task that is still the registered owner
    /// clears its own entry, so the 1s scan can respawn the scope.
    #[test]
    fn an_exiting_poll_task_retires_its_own_token() {
        let tokens = scope_tokens();
        let scope = CursorScope::Account;
        let generation = track(&tokens, &scope);

        retire_scope_token(&tokens, &scope, generation);

        assert!(tokens.lock().expect("poisoned").is_empty());
    }

    /// The interleaving that produced duplicate poll tasks: task A
    /// exits while its scope is momentarily absent from the cursor
    /// registry, the scan spawns B under the same key, and A's cleanup
    /// then lands. Removing by key would evict B, leaving it untracked
    /// and the next scan free to spawn a third live poll for one
    /// scope - two concurrent producers on a lane+scope the checkpoint
    /// supersession rule assumes has exactly one.
    #[test]
    fn an_exiting_poll_task_does_not_retire_its_successors_token() {
        let tokens = scope_tokens();
        let scope = CursorScope::Account;
        let departing = track(&tokens, &scope);
        let successor = track(&tokens, &scope);
        assert_ne!(departing, successor);

        retire_scope_token(&tokens, &scope, departing);

        let guard = tokens.lock().expect("poisoned");
        let entry = guard.get(&scope).expect("the successor stays tracked");
        assert_eq!(entry.generation, successor);
    }

    /// A cancelled entry left behind by a `ScopeLifecycle::Deleted` or
    /// a `restart_scope` is still the successor's to own; the departing
    /// task must not remove it on the strength of the cancel flag
    /// alone.
    #[test]
    fn retirement_ignores_the_cancellation_flag() {
        let tokens = scope_tokens();
        let scope = CursorScope::Folder(bifrost_types::FolderId("Inbox".into()));
        let departing = track(&tokens, &scope);
        let successor = track(&tokens, &scope);
        tokens
            .lock()
            .expect("poisoned")
            .get(&scope)
            .expect("successor")
            .cancel();

        retire_scope_token(&tokens, &scope, departing);

        let guard = tokens.lock().expect("poisoned");
        assert_eq!(
            guard
                .get(&scope)
                .expect("successor still tracked")
                .generation,
            successor
        );
    }

    #[test]
    fn cadence_halves_on_change() {
        let cur = AdaptiveCadence {
            interval: ms(800),
            no_change_streak: 3,
        };
        let next = Multiplexer::updated_cadence(cur, true, ms(100), ms(60_000));
        assert_eq!(next.interval, ms(400));
        assert_eq!(next.no_change_streak, 0);
    }

    #[test]
    fn cadence_doubles_after_five_idle() {
        let mut c = AdaptiveCadence {
            interval: ms(500),
            no_change_streak: 0,
        };
        for _ in 0..4 {
            c = Multiplexer::updated_cadence(c, false, ms(100), ms(60_000));
            assert_eq!(c.interval, ms(500));
        }
        c = Multiplexer::updated_cadence(c, false, ms(100), ms(60_000));
        assert_eq!(c.interval, ms(1000));
        assert_eq!(c.no_change_streak, 0);
    }

    #[test]
    fn cadence_respects_floor_and_ceiling() {
        let c = AdaptiveCadence {
            interval: ms(150),
            no_change_streak: 0,
        };
        let next = Multiplexer::updated_cadence(c, true, ms(200), ms(60_000));
        assert_eq!(next.interval, ms(200));
        let c = AdaptiveCadence {
            interval: ms(30_000),
            no_change_streak: 4,
        };
        let next = Multiplexer::updated_cadence(c, false, ms(100), ms(30_000));
        assert_eq!(next.interval, ms(30_000));
    }

    /// A lifecycle delete cancels the scope's poll task AND retires its
    /// registry entry. Cancelling without retiring leaves a cancelled
    /// token behind, and `spawn_missing_scope_polls` then sees an entry
    /// for the scope and never respawns it if the scope comes back.
    #[test]
    fn cancelling_a_scope_token_also_retires_its_entry() {
        let tokens: ScopeTokens = Arc::new(StdMutex::new(HashMap::new()));
        let scope = CursorScope::Account;
        let token = CancellationToken::new();
        tokens.lock().expect("poisoned").insert(
            scope.clone(),
            ScopeToken {
                generation: 7,
                token: token.clone(),
            },
        );

        cancel_scope_token(&tokens, &scope);

        assert!(token.is_cancelled(), "the poll task must be cancelled");
        assert!(
            !tokens.lock().expect("poisoned").contains_key(&scope),
            "a cancelled scope must leave no entry blocking a later respawn"
        );
    }

    /// Retirement is generation-checked, so a late retirement from a
    /// previous incarnation cannot delete a freshly spawned task's entry.
    #[test]
    fn a_stale_retirement_leaves_the_current_entry_alone() {
        let tokens: ScopeTokens = Arc::new(StdMutex::new(HashMap::new()));
        let scope = CursorScope::Account;
        tokens.lock().expect("poisoned").insert(
            scope.clone(),
            ScopeToken {
                generation: 9,
                token: CancellationToken::new(),
            },
        );

        retire_scope_token(&tokens, &scope, 8);

        assert!(
            tokens.lock().expect("poisoned").contains_key(&scope),
            "an older generation must not retire the current entry"
        );
    }

    #[test]
    fn lifecycle_terminal_errors_do_not_reconnect_or_reopen() {
        let error = AccountErrorBuilder::new(
            AccountErrorKind::Authentication(AuthErrorKind::Expired),
            Cause::Auth(AuthCause::Expired),
        )
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid terminal authentication error");

        assert_eq!(
            lifecycle_termination(&error),
            LifecycleTermination::Terminate
        );
    }

    /// A `RestartAccount` directive swaps the connection this reader holds,
    /// so waiting for the generation change is the right park - bounded, so a
    /// reopen that never succeeds cannot silence lifecycle observation.
    #[test]
    fn lifecycle_account_reopen_errors_wait_for_the_generation_change() {
        let error = AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
        )
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid account-reopen recovery error");

        assert_eq!(
            lifecycle_termination(&error),
            LifecycleTermination::AwaitReopen
        );
    }

    /// Scope-level and account-wide-but-not-reopen directives never bump the
    /// account generation, so parking on it would strand this reader for the
    /// process lifetime. Hand the error over once, then reconnect on backoff.
    #[test]
    fn lifecycle_non_reopen_directives_reconnect_instead_of_parking() {
        let schema = AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
            Cause::State(StateCause::SchemaIncompatible),
        )
        .operation(AccountOperation::SyncChanges)
        .try_build()
        .expect("valid schema recovery error");
        assert_eq!(
            lifecycle_termination(&schema),
            LifecycleTermination::ForwardAndReconnect
        );

        let revoked = crate::recovery::restart_scope_error(
            CursorScope::Folder(bifrost_types::FolderId("Shared".into())),
            AccountOperation::SyncChanges,
        );
        assert_eq!(
            lifecycle_termination(&revoked),
            LifecycleTermination::ForwardAndReconnect
        );
    }
}
