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
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use arc_swap::ArcSwap;
use bifrost_types::{
    Account, AccountId, ChangeCursor, Checkpoint, CursorScope, MembershipScope, RecoveryClass,
    ScopeLifecycle, SyncEvent, WatchEvent,
};
use futures::stream::StreamExt;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::cancel::BoundaryView;
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::error::Error;
use crate::types::MultiplexerConfig;

pub use changes::{AckRequest, ChangesEvent, drive_changes_stream};
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

/// Reopen request raised by per-scope drivers when a stream ends with
/// a recoverable Fatal. The engine listens on the receiver and
/// dispatches the recovery action carried in `RecoveryClass`.
///
/// The full `RecoveryClass` is preserved so the engine can:
/// - `Retry { after }`: sleep then re-poll the same scope.
/// - `RestartScope` / `DowngradeCapabilityForScope`: drop the scope's
///   cursor and re-establish via inventory.
/// - `RestartAccount` / `CapabilityChanged`: `factory.open()` + reseed
///   the slot's `ArcSwap`.
/// - `AuthLost` / `Fatal` / `OperatorOverrideRequired`: surface; do
///   not auto-reopen.
/// - `DowngradeStrategy` / `SchemaIncompatible`: surface (engine has
///   no automated path; the protocol crate is expected to apply the
///   downgrade on the next `establish_initial_cursor`).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ReopenRequest {
    /// Recovery class observed at the scope's stream terminus.
    Recovery {
        scope: CursorScope,
        recovery: RecoveryClass,
    },
}

/// Multiplexer task state. Constructed in `engine::attach` and run on
/// a `tokio::spawn`.
pub struct Multiplexer {
    pub account_id: AccountId,
    pub account: Arc<ArcSwap<Arc<dyn Account>>>,
    pub cursors: Arc<CursorRegistry>,
    pub config: MultiplexerConfig,
    pub boundary: BoundaryView,
    pub changes_tx: broadcast::Sender<MultiplexerEvent>,
    pub watch_tx: mpsc::Sender<WatchEvent>,
    pub control: SyncControl,
    pub shutdown: CancellationToken,
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
    pub ack_tx: Option<mpsc::Sender<AckRequest>>,
    pub poll: HashMap<CursorScope, AdaptiveCadence>,
    /// Per-scope cancellation tokens keyed by membership-id so
    /// `ScopeLifecycle::Deleted` can stop the matching poll task.
    pub scope_tokens: Arc<StdMutex<HashMap<CursorScope, CancellationToken>>>,
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
            cursors,
            config,
            boundary,
            changes_tx,
            watch_tx: _,
            control,
            shutdown,
            reopen_tx,
            ack_tx,
            poll: _,
            scope_tokens,
        } = self;

        // Snapshot the known scopes from the registry; the engine has
        // populated it via `establish_one` before spawning us.
        let initial_scopes = cursors.all_scopes();

        for scope in initial_scopes {
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
                scope,
            );
        }

        // Drive scope_lifecycle in the background. `Created` spawns a
        // new poll task; `Deleted` cancels and drops the matching
        // scope; `Renamed` is delete+create on a fresh CursorScope id.
        let lifecycle_account = Arc::clone(&account);
        let lifecycle_shutdown = shutdown.clone();
        let lifecycle_cursors = Arc::clone(&cursors);
        let lifecycle_tokens = Arc::clone(&scope_tokens);
        let lifecycle_reopen = reopen_tx.clone();
        let lifecycle_handle = tokio::spawn(async move {
            let acc = lifecycle_account.load_full();
            let mut stream = acc.scope_lifecycle_stream();
            loop {
                tokio::select! {
                    () = lifecycle_shutdown.cancelled() => return,
                    next = stream.next() => {
                        let Some(ev) = next else { return; };
                        match ev {
                            ScopeLifecycle::Created(membership) => {
                                if let Some(scope) = membership_to_cursor_scope(&membership) {
                                    // Track the new scope. The cursor
                                    // registry has no entry yet; the
                                    // poll task will see snapshot=None
                                    // and exit cleanly unless the
                                    // engine establishes a cursor for
                                    // it first. We raise a Recovery
                                    // request so the engine drives the
                                    // initial cursor establishment.
                                    let _ = lifecycle_reopen
                                        .send(ReopenRequest::Recovery {
                                            scope: scope.clone(),
                                            recovery: RecoveryClass::RestartScope(scope.clone()),
                                        })
                                        .await;
                                }
                            }
                            ScopeLifecycle::Deleted(membership) => {
                                let scopes = lifecycle_cursors.scopes_for_membership(&membership);
                                for scope in scopes {
                                    if let Some(token) =
                                        lifecycle_tokens.lock().expect("poisoned").remove(&scope)
                                    {
                                        token.cancel();
                                    }
                                    lifecycle_cursors.delete(&scope);
                                }
                            }
                            ScopeLifecycle::Renamed { old, new } => {
                                // Treat as delete + create: cancel the
                                // old scope's poll task, drop the
                                // cursor, then trigger a fresh
                                // establishment for the new id.
                                let old_scopes =
                                    lifecycle_cursors.scopes_for_membership(&old);
                                for scope in old_scopes {
                                    if let Some(token) =
                                        lifecycle_tokens.lock().expect("poisoned").remove(&scope)
                                    {
                                        token.cancel();
                                    }
                                    lifecycle_cursors.delete(&scope);
                                }
                                if let Some(scope) = membership_to_cursor_scope(&new) {
                                    let _ = lifecycle_reopen
                                        .send(ReopenRequest::Recovery {
                                            scope: scope.clone(),
                                            recovery: RecoveryClass::RestartScope(scope.clone()),
                                        })
                                        .await;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        // Park until shutdown; the per-scope tasks own their own loops.
        shutdown.cancelled().await;

        // Cancel every tracked scope token so per-scope polls exit
        // cleanly. Tokens are dropped as part of the map clear.
        let drained: Vec<CancellationToken> = {
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
    ack_tx: Option<mpsc::Sender<AckRequest>>,
    scope_tokens: Arc<StdMutex<HashMap<CursorScope, CancellationToken>>>,
    scope: CursorScope,
) {
    let scope_cancel = shutdown.child_token();
    scope_tokens
        .lock()
        .expect("poisoned")
        .insert(scope.clone(), scope_cancel.clone());
    tokio::spawn(spawn_scope_poll_inner(
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
        scope,
    ));
}

/// Per-scope poll loop. Drives `changes_stream(cursor)` and sleeps
/// for the scope's adaptive cadence between passes. Stream-end
/// recovery actions are propagated to the engine via `ReopenRequest`
/// with the full `RecoveryClass`.
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
    ack_tx: Option<mpsc::Sender<AckRequest>>,
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
            account_id.clone(),
            changes_tx.clone(),
            boundary.clone(),
            Some(control.clone()),
            ack_tx.clone(),
        )
        .await;
        let recovered = handle_drive_outcome(
            &scope,
            outcome,
            &cursors,
            &pre_advance_state,
            &reopen_tx,
            &account_id,
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
        Ok(ChangesEvent::Paused) => {
            // Pause is a temporary park; the poll loop's boundary
            // check picks it up at the top of the next iteration.
            let advanced = cursors
                .snapshot(scope)
                .map(|c| c.server_state.bytes.as_slice() != pre_advance_state)
                .unwrap_or(false);
            DriveRecovery {
                advanced,
                exit: false,
            }
        }
        Ok(ChangesEvent::Fatal(recovery)) => {
            // Send the full RecoveryClass to the engine so it can
            // dispatch the right action: Retry / RestartScope /
            // RestartAccount / AuthLost / etc.
            let _ = reopen_tx
                .send(ReopenRequest::Recovery {
                    scope: scope.clone(),
                    recovery,
                })
                .await;
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

/// Map a `MembershipScope` from `ScopeLifecycle` to the
/// `CursorScope` shape the engine indexes on. The mapping mirrors
/// `scope_covers_membership` in the engine: folder memberships map
/// to per-folder cursor scopes; labels/queries map to their natural
/// cursor scope shapes. Returns `None` when the membership has no
/// obvious cursor-scope counterpart (e.g. Gmail labels under an
/// account-wide cursor model).
#[must_use]
pub fn membership_to_cursor_scope(membership: &MembershipScope) -> Option<CursorScope> {
    use bifrost_types::FolderId;
    match membership {
        MembershipScope::Folder(folder) => Some(CursorScope::Folder(folder.clone())),
        MembershipScope::Mailbox(mailbox) => Some(CursorScope::Folder(FolderId(mailbox.0.clone()))),
        MembershipScope::Query(q) => Some(CursorScope::Query(q.clone())),
        // Gmail labels are typically covered by the account-wide
        // cursor; no per-label cursor exists.
        MembershipScope::Label(_) => None,
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
