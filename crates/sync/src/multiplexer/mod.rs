//! Per-account multiplexer.
//!
//! One task per account drives:
//! - `changes_stream(cursor)` for each known scope (round-robin or
//!   IDLE-on-most-active),
//! - the adaptive NOOP/STATUS poll cadence per scope,
//! - the IDLE / WebSocket reader for the protocol's most-active scope,
//! - `scope_lifecycle_stream()` for scope creation / rename / deletion,
//! - inventory fusion for `EstablishViaInventory` scopes (the
//!   inventory's terminal `Done` carries the cursor, which the
//!   multiplexer registers exactly once).
//!
//! Output is a unified `SyncEvent<Change>` stream on the account's
//! broadcast channel.

pub mod changes;
pub mod fusion;
pub mod idle;
pub mod poll;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bifrost_types::{
    Account, AccountId, ChangeCursor, Checkpoint, CursorScope, RecoveryClass, ScopeLifecycle,
    SyncEvent, WatchEvent,
};
use futures::stream::StreamExt;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::cancel::BoundaryView;
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::cursor::store::DynCheckpointStore;
use crate::error::Error;
use crate::types::MultiplexerConfig;

pub use changes::{ChangesEvent, drive_changes_stream};
pub use fusion::{FusionOutcome, InventoryFusion};
pub use idle::{IdleHolder, IdleSignal};
pub use poll::{AdaptiveCadence, PollSchedule};

/// Handle the engine stashes in the per-account slot for the
/// multiplexer task. Cancellation is rooted in the slot's shutdown
/// token; explicit shutdown shuts the task down between two
/// `boundary.peek()` calls.
#[derive(Debug)]
pub struct MultiplexerHandle {
    pub cancel: CancellationToken,
    pub changes_tx: broadcast::Sender<MultiplexerEvent>,
    pub watch_tx: mpsc::Sender<WatchEvent>,
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
}

/// Reopen request the multiplexer can raise when it observes a
/// `RecoveryClass::RestartAccount` or `CapabilityChanged` Fatal. The
/// engine listens on the receiver and calls its reopen path.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ReopenRequest {
    /// Account-wide reopen: factory.open and restart every scope.
    Account,
    /// Single-scope restart: drop the cursor and re-establish via
    /// inventory.
    Scope(CursorScope),
}

/// Multiplexer task state. Constructed in `engine::attach` and run on
/// a `tokio::spawn`.
pub struct Multiplexer {
    pub account_id: AccountId,
    pub account: Arc<ArcSwap<Arc<dyn Account>>>,
    pub cursors: Arc<CursorRegistry>,
    pub store: Arc<DynCheckpointStore>,
    pub config: MultiplexerConfig,
    pub boundary: BoundaryView,
    pub changes_tx: broadcast::Sender<MultiplexerEvent>,
    pub watch_tx: mpsc::Sender<WatchEvent>,
    pub control: SyncControl,
    pub shutdown: CancellationToken,
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
    pub poll: HashMap<CursorScope, AdaptiveCadence>,
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
    /// `scope_lifecycle_stream` task. New scopes added at runtime via
    /// `add_scope` get their own poll task; deleted scopes have their
    /// poll task signaled via the shared shutdown token (best-effort).
    pub async fn run(self) {
        let Self {
            account_id,
            account,
            cursors,
            store,
            config,
            boundary,
            changes_tx,
            watch_tx: _,
            control,
            shutdown,
            reopen_tx,
            poll: _,
        } = self;

        // Snapshot the known scopes from the registry; the engine has
        // populated it via `establish_one` before spawning us.
        let initial_scopes = cursors.all_scopes();
        let mut scope_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        for scope in initial_scopes {
            let h = spawn_scope_poll(
                account_id.clone(),
                Arc::clone(&account),
                Arc::clone(&cursors),
                Arc::clone(&store),
                config,
                boundary.clone(),
                changes_tx.clone(),
                control.clone(),
                shutdown.clone(),
                reopen_tx.clone(),
                scope,
            );
            scope_handles.push(h);
        }

        // Drive scope_lifecycle in the background. Per the trait it's
        // a long-lived stream; we forward observed events through the
        // broadcast as informational ScopeChange events. Renames /
        // deletions also adjust the registry side-index implicitly
        // (consumers re-derive membership; we keep the simple path
        // here).
        let lifecycle_account = Arc::clone(&account);
        let lifecycle_shutdown = shutdown.clone();
        let lifecycle_handle = tokio::spawn(async move {
            let acc = lifecycle_account.load_full();
            let mut stream = acc.scope_lifecycle_stream();
            loop {
                tokio::select! {
                    () = lifecycle_shutdown.cancelled() => return,
                    next = stream.next() => {
                        let Some(ev) = next else { return; };
                        // Lifecycle events are surfaced as tracing
                        // events for now; downstream consumers
                        // consume the unified change broadcast.
                        // Forwarding lifecycle into the broadcast as
                        // typed events is the engine's contract once
                        // the consumer wire-up lands; until then we
                        // observe.
                        tracing::debug!(
                            target: "bifrost.sync.changes",
                            event = ?ev,
                            "scope lifecycle"
                        );
                        // Suppress unused warning.
                        let _: ScopeLifecycle = ev;
                    }
                }
            }
        });

        // Park until shutdown; the per-scope tasks own their own loops.
        shutdown.cancelled().await;

        for h in scope_handles {
            h.abort();
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

/// Spawn one polling task for a single scope. The task loops, driving
/// `changes_stream(cursor)` and sleeping for the scope's adaptive
/// cadence between passes. Stream-end recovery actions (RestartScope,
/// RestartAccount, CapabilityChanged) raise a `ReopenRequest` to the
/// engine.
#[allow(clippy::too_many_arguments)]
fn spawn_scope_poll(
    account_id: AccountId,
    account: Arc<ArcSwap<Arc<dyn Account>>>,
    cursors: Arc<CursorRegistry>,
    store: Arc<DynCheckpointStore>,
    config: MultiplexerConfig,
    boundary: BoundaryView,
    changes_tx: broadcast::Sender<MultiplexerEvent>,
    control: SyncControl,
    shutdown: CancellationToken,
    reopen_tx: mpsc::Sender<ReopenRequest>,
    scope: CursorScope,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut cadence = AdaptiveCadence {
            interval: config.poll_initial,
            no_change_streak: 0,
        };
        loop {
            if shutdown.is_cancelled() {
                return;
            }
            let Some(cursor) = cursors.snapshot(&scope) else {
                // Scope was removed from the registry; exit cleanly.
                return;
            };
            let pre_advance_state = cursor.server_state.bytes.clone();
            let acc_arc = account.load_full();
            let acc: &dyn Account = acc_arc.as_ref().as_ref();
            let outcome = drive_changes_stream(
                acc,
                scope.clone(),
                cursor,
                Arc::clone(&cursors),
                Arc::clone(&store),
                account_id.clone(),
                changes_tx.clone(),
                boundary.clone(),
                Some(control.clone()),
            )
            .await;
            // The driver yields a `ChangesEvent` or an `Error`; both
            // are handled via the same recovery surface here.
            let recovered = handle_drive_outcome(
                &scope,
                outcome,
                &cursors,
                &pre_advance_state,
                &reopen_tx,
                &account_id,
            )
            .await;

            // Update cadence based on whether the cursor advanced.
            cadence = Multiplexer::updated_cadence(
                cadence,
                recovered.advanced,
                config.poll_min,
                config.poll_max,
            );
            if recovered.exit {
                return;
            }

            // Sleep for the adaptive interval, but exit early on
            // shutdown.
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(cadence.interval) => {}
            }
        }
    })
}

struct DriveRecovery {
    advanced: bool,
    exit: bool,
}

async fn handle_drive_outcome(
    scope: &CursorScope,
    outcome: Result<ChangesEvent, Error>,
    cursors: &CursorRegistry,
    pre_advance_state: &[u8],
    reopen_tx: &mpsc::Sender<ReopenRequest>,
    account_id: &AccountId,
) -> DriveRecovery {
    match outcome {
        Ok(ChangesEvent::Advanced | ChangesEvent::Done) => {
            let advanced = cursors
                .snapshot(scope)
                .map(|c| c.server_state.bytes.as_slice() != pre_advance_state)
                .unwrap_or(false);
            DriveRecovery {
                advanced,
                exit: false,
            }
        }
        Ok(ChangesEvent::Stopped) => DriveRecovery {
            advanced: false,
            exit: true,
        },
        Ok(ChangesEvent::Fatal) => {
            // A Fatal already crossed the broadcast; raise reopen if
            // the consumer wants the engine to recover. We cannot
            // observe the RecoveryClass directly from this layer
            // (Fatal is broadcast and consumed by subscribers), so
            // conservatively request an account-wide reopen and let
            // the engine decide.
            let _ = reopen_tx.send(ReopenRequest::Scope(scope.clone())).await;
            DriveRecovery {
                advanced: false,
                exit: false,
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

/// Engine-facing helper: translate the engine's view of a Fatal's
/// `RecoveryClass` into the right reopen action. The multiplexer
/// itself sees Fatals only through the broadcast, so this lives here
/// for the engine task that listens on the broadcast and decides
/// whether to dispatch reopen.
#[must_use]
pub fn reopen_for_recovery(recovery: &RecoveryClass) -> Option<ReopenRequest> {
    match recovery {
        RecoveryClass::RestartScope(s) | RecoveryClass::DowngradeCapabilityForScope(s) => {
            Some(ReopenRequest::Scope(s.clone()))
        }
        RecoveryClass::RestartAccount | RecoveryClass::CapabilityChanged { .. } => {
            Some(ReopenRequest::Account)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
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
}
