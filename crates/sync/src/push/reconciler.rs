//! Push reconciler task.
//!
//! One task per account. Reads `WatchEvent`s off the per-account mpsc
//! and, on `Invalidated`, runs `changes_stream` against the current
//! cursor for each affected scope. Output flows to the multiplexer's
//! broadcast so consumers see a single unified `Change` stream.

use std::sync::Arc;

use bifrost_types::{Account, AccountId, CursorScope, HintPayload, InvalidationHint, WatchEvent};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use arc_swap::ArcSwap;

use crate::cancel::BoundaryView;
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::error::{Error, Warning};
use crate::multiplexer::{
    AckRequest, ChangesEvent, MultiplexerEvent, ReopenRequest, drive_changes_stream,
};

pub struct Reconciler {
    pub account_id: AccountId,
    pub account: Arc<ArcSwap<Arc<dyn Account>>>,
    pub cursors: Arc<CursorRegistry>,
    pub changes_tx: broadcast::Sender<MultiplexerEvent>,
    pub boundary: BoundaryView,
    pub shutdown: CancellationToken,
    pub control: SyncControl,
    pub ack_tx: Option<mpsc::Sender<AckRequest>>,
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
}

impl Reconciler {
    /// Run the reconciler loop. Returns when the channel closes or
    /// the shutdown token trips.
    pub async fn run(mut self, mut rx: mpsc::Receiver<WatchEvent>) {
        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => return,
                maybe_event = rx.recv() => {
                    let Some(event) = maybe_event else { return; };
                    self.handle(event).await;
                }
            }
        }
    }

    async fn handle(&mut self, event: WatchEvent) {
        match event {
            WatchEvent::Invalidated { hint } => {
                if let Err(err) = self.reconcile(&hint).await {
                    tracing::warn!(target: "bifrost.sync.reconcile", error=%err, "reconcile failed");
                }
            }
            WatchEvent::Disconnected => {
                let _ = self.changes_tx.send(self.warning_event(
                    "push transport disconnected",
                    bifrost_types::WarningKind::Other,
                ));
            }
            WatchEvent::Reconnected => {
                // Treat reconnect as a full reconcile across every
                // registered cursor scope.
                let hint = InvalidationHint {
                    source: bifrost_types::PushSource::JmapStateChange,
                    payload: HintPayload::Unknown,
                };
                if let Err(err) = self.reconcile(&hint).await {
                    tracing::warn!(target: "bifrost.sync.reconcile", error=%err, "post-reconnect reconcile failed");
                }
            }
            WatchEvent::Terminated(error) => {
                // Push stream cannot be reconnected without engine
                // intervention. Plan recovery and route engine
                // directives through the reopen channel so the engine
                // restarts the subscription (or escalates per its
                // budget). Terminal classes broadcast a Terminated
                // event so consumers observe the structured error.
                use crate::recovery::{RecoveryPlan, directive_target_scope, plan_recovery};
                let original = error.clone();
                match plan_recovery(error) {
                    RecoveryPlan::Engine(directive) => {
                        let scope = directive_target_scope(&directive);
                        let _ = self
                            .reopen_tx
                            .send(ReopenRequest::Recovery {
                                scope,
                                error: original,
                            })
                            .await;
                    }
                    RecoveryPlan::Retry(_) | RecoveryPlan::Reconcile(_) => {
                        // Push streams cannot retry inline; the
                        // forwarder reconnects on the next iteration.
                        // Surface a warning so dashboards observe the
                        // hiccup.
                        let warning = bifrost_types::Warning::user_safe(
                            bifrost_types::WarningKind::Other,
                            "push stream terminated with retryable recovery",
                        );
                        let scope = self
                            .cursors
                            .all_scopes()
                            .into_iter()
                            .next()
                            .unwrap_or(CursorScope::Account);
                        let me = MultiplexerEvent {
                            scope,
                            event: Arc::new(bifrost_types::SyncEvent::Warning(warning)),
                            checkpoint: None,
                        };
                        let _ = self.changes_tx.send(me);
                    }
                    RecoveryPlan::Terminal(fatal) => {
                        let scope = self
                            .cursors
                            .all_scopes()
                            .into_iter()
                            .next()
                            .unwrap_or(CursorScope::Account);
                        let me = MultiplexerEvent {
                            scope,
                            event: Arc::new(bifrost_types::SyncEvent::Terminated(
                                fatal.into_inner(),
                            )),
                            checkpoint: None,
                        };
                        let _ = self.changes_tx.send(me);
                    }
                }
            }
            // `WatchEvent` is `#[non_exhaustive]`; an unknown variant
            // is treated as a no-op wakeup.
            _ => {}
        }
    }

    async fn reconcile(&self, hint: &InvalidationHint) -> Result<(), Error> {
        let scopes = scopes_for_hint(&self.cursors, &hint.payload);
        for scope in scopes {
            let Some(cursor) = self.cursors.snapshot(&scope) else {
                continue;
            };
            // Load the current account handle each iteration so a
            // mid-reconcile reopen surfaces to the next scope.
            let account_swap = self.account.load_full();
            let account: &dyn Account = account_swap.as_ref().as_ref();
            let outcome = drive_changes_stream(
                account,
                scope.clone(),
                cursor,
                Arc::clone(&self.cursors),
                self.account_id.clone(),
                self.changes_tx.clone(),
                self.boundary.clone(),
                Some(self.control.clone()),
                self.ack_tx.clone(),
            )
            .await?;
            match outcome {
                ChangesEvent::Advanced
                | ChangesEvent::Done
                | ChangesEvent::Stopped
                | ChangesEvent::Paused => {}
                ChangesEvent::Terminated(error) => {
                    use crate::recovery::{
                        RecoveryPlan, directive_target_scope, plan_recovery, retry_delay,
                    };
                    let original = error.clone();
                    match plan_recovery(error) {
                        RecoveryPlan::Retry(advice) => {
                            let delay = retry_delay(
                                &advice,
                                std::time::SystemTime::now(),
                                std::time::Duration::from_secs(1),
                            );
                            tokio::time::sleep(delay).await;
                        }
                        RecoveryPlan::Reconcile(_) => {
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        }
                        RecoveryPlan::Engine(directive) => {
                            let directive_scope = directive_target_scope(&directive);
                            let _ = self
                                .reopen_tx
                                .send(ReopenRequest::Recovery {
                                    scope: directive_scope,
                                    error: original,
                                })
                                .await;
                            return Ok(());
                        }
                        RecoveryPlan::Terminal(_) => {
                            return Ok(());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn warning_event(&self, message: &str, kind: bifrost_types::WarningKind) -> MultiplexerEvent {
        // We need an arbitrary CursorScope for the event; pick the
        // first registered one or fall back to Account.
        let scope = self
            .cursors
            .all_scopes()
            .into_iter()
            .next()
            .unwrap_or(CursorScope::Account);
        let warning = Warning::user_safe(kind, message);
        MultiplexerEvent {
            scope,
            event: Arc::new(bifrost_types::SyncEvent::Warning(warning)),
            checkpoint: None,
        }
    }
}

/// Enumerate the cursor scopes a hint covers.
///
/// - `SpecificCursorScope(s)` -> `vec![s]`
/// - `SpecificMembership(m)` -> registry's membership index
/// - `Unknown` -> every registered cursor scope.
#[must_use]
pub fn scopes_for_hint(registry: &CursorRegistry, hint: &HintPayload) -> Vec<CursorScope> {
    match hint {
        HintPayload::SpecificCursorScope(s) => vec![s.clone()],
        HintPayload::SpecificMembership(m) => registry.scopes_for_membership(m),
        HintPayload::Unknown => registry.all_scopes(),
        // `HintPayload` is `#[non_exhaustive]`; fall back to a full
        // reconcile under uncertainty - that is the contract for
        // `Unknown` and the safest default for any new variant.
        _ => registry.all_scopes(),
    }
}
