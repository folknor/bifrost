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
    ChangesEvent, MultiplexerEvent, ReopenRequest, WriterRequest, drive_changes_stream,
};

pub struct Reconciler {
    pub account_id: AccountId,
    pub account: Arc<ArcSwap<Arc<dyn Account>>>,
    pub cursors: Arc<CursorRegistry>,
    pub changes_tx: broadcast::Sender<MultiplexerEvent>,
    pub boundary: BoundaryView,
    pub shutdown: CancellationToken,
    pub control: SyncControl,
    pub ack_tx: Option<mpsc::Sender<WriterRequest>>,
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
    /// Engine-wide throttle bucket. The reconciler honors account-wide
    /// deadlines before driving a hinted scope and records deadlines
    /// its own Retry outcomes carry.
    pub throttles: Arc<std::sync::Mutex<crate::recovery::ThrottleBucket>>,
}

impl Reconciler {
    /// Run the reconciler loop. Returns when the channel closes or
    /// the shutdown token trips. Honors the account boundary: when
    /// the engine flips to `Pause` (e.g. `OperatorOverrideRequired`
    /// or `RetryBudgetExhausted`), pushes park alongside polls until
    /// the consumer flips to `Resume`. Buffered `WatchEvent`s remain
    /// in the channel and drain on resume.
    pub async fn run(mut self, mut rx: mpsc::Receiver<WatchEvent>) {
        loop {
            // Park while the account is paused. The boundary watch
            // notifies on every state change; we wake when it flips
            // back to `Run`.
            while matches!(self.boundary.peek(), crate::cancel::BoundaryRequest::Pause) {
                tokio::select! {
                    () = self.shutdown.cancelled() => return,
                    changed = self.boundary.changed() => {
                        match changed {
                            Some(crate::cancel::BoundaryRequest::Run) => break,
                            // Pause / CheckpointNow / Stop all keep
                            // the loop parked. Stop is observed via
                            // the shutdown token; CheckpointNow is a
                            // poll-only signal that the reconciler
                            // ignores.
                            Some(_) => continue,
                            None => return,
                        }
                    }
                }
            }
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
                    source: bifrost_types::PushSource::Coalesced,
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
                    RecoveryPlan::Retry(advice) => {
                        // Push streams cannot retry inline; the
                        // forwarder reconnects on the next iteration.
                        // But a retryable termination can still carry a
                        // provider-documented throttle deadline (the
                        // stream died on a 429), and dropping it here
                        // would hide it from polls and sibling
                        // accounts. Record, then surface the warning.
                        crate::recovery::record_throttle(
                            &self.throttles,
                            &self.account_id,
                            &advice,
                            &original,
                        );
                        let warning = bifrost_types::Warning::user_safe(
                            bifrost_types::WarningKind::Other,
                            "push stream terminated with retryable recovery",
                        );
                        let me = MultiplexerEvent {
                            scope: CursorScope::Account,
                            event: Arc::new(bifrost_types::SyncEvent::Warning(warning)),
                            checkpoint: None,
                            publication: None,
                        };
                        let _ = self.changes_tx.send(me);
                    }
                    RecoveryPlan::Reconcile(advice) => {
                        crate::recovery::record_reconcile_throttle(
                            &self.throttles,
                            &self.account_id,
                            &advice,
                            &original,
                        );
                        // Surface a warning so dashboards observe the
                        // hiccup; the forwarder reconnects on its next
                        // iteration.
                        let warning = bifrost_types::Warning::user_safe(
                            bifrost_types::WarningKind::Other,
                            "push stream terminated with retryable recovery",
                        );
                        let me = MultiplexerEvent {
                            scope: CursorScope::Account,
                            event: Arc::new(bifrost_types::SyncEvent::Warning(warning)),
                            checkpoint: None,
                            publication: None,
                        };
                        let _ = self.changes_tx.send(me);
                    }
                    RecoveryPlan::Terminal(fatal) => {
                        let me = MultiplexerEvent {
                            scope: CursorScope::Account,
                            event: Arc::new(bifrost_types::SyncEvent::Terminated(
                                fatal.into_inner(),
                            )),
                            checkpoint: None,
                            publication: None,
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
            // Honor any account-wide throttle deadline (a sibling
            // scope's Retry-After, or a shared tenant/provider key)
            // before driving the wire for this hint. Re-checked after
            // waking: a longer deadline can land mid-sleep.
            while let Some(wait) = crate::recovery::account_throttle_wait(
                &self.throttles,
                &self.account_id,
                std::time::SystemTime::now(),
            ) {
                tracing::debug!(
                    target: "bifrost.sync.reconcile",
                    account = ?self.account_id,
                    scope = ?scope,
                    wait_secs = wait.as_secs(),
                    "reconcile deferred by shared throttle deadline"
                );
                tokio::select! {
                    () = self.shutdown.cancelled() => return Ok(()),
                    () = tokio::time::sleep(wait) => {}
                }
            }
            let driven = self
                .cursors
                .with_drive(&scope, |cursor, registry_generation| {
                    let account_swap = self.account.load_full();
                    let cursors = Arc::clone(&self.cursors);
                    let account_id = self.account_id.clone();
                    let changes_tx = self.changes_tx.clone();
                    let boundary = self.boundary.clone();
                    let control = self.control.clone();
                    let ack_tx = self.ack_tx.clone();
                    let scope = scope.clone();
                    async move {
                        let account: &dyn Account = account_swap.as_ref().as_ref();
                        drive_changes_stream(
                            account,
                            scope,
                            cursor,
                            cursors,
                            account_id,
                            changes_tx,
                            boundary,
                            Some(control),
                            ack_tx,
                            Some(registry_generation),
                        )
                        .await
                    }
                })
                .await;
            let Some(outcome) = driven else {
                continue;
            };
            let outcome = match outcome {
                Ok(outcome) => outcome,
                // An account error already carries a complete recovery
                // verdict - an incompatible cursor envelope derives
                // `Engine(SchemaIncompatible)`, which MUST reset state.
                // Surviving the sibling scopes of a multi-scope hint must
                // not cost the failing scope its recovery: logging this and
                // moving on leaves the invalid cursor installed, so push
                // re-terminates on every hint until some later poll happens
                // to hit the same failure. Normalize onto the same path an
                // account-authored `Terminated` takes, exactly as the poll
                // loop's `handle_drive_outcome` does.
                Err(Error::Account(error)) => ChangesEvent::Terminated(error),
                // Engine-internal errors carry no recovery verdict and name
                // nothing to reset. Log and keep sweeping.
                Err(error) => {
                    tracing::warn!(
                        target: "bifrost.sync.reconcile",
                        account = ?self.account_id,
                        scope = ?scope,
                        error = %error,
                        "hinted scope drive failed; continuing remaining scopes"
                    );
                    continue;
                }
            };
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
                            // Share the throttle deadline with the poll
                            // loop and sibling accounts before sleeping
                            // it off locally.
                            crate::recovery::record_throttle(
                                &self.throttles,
                                &self.account_id,
                                &advice,
                                &original,
                            );
                            let delay = retry_delay(
                                &advice,
                                std::time::SystemTime::now(),
                                std::time::Duration::from_secs(1),
                            );
                            tokio::time::sleep(delay).await;
                        }
                        RecoveryPlan::Reconcile(advice) => {
                            crate::recovery::record_reconcile_throttle(
                                &self.throttles,
                                &self.account_id,
                                &advice,
                                &original,
                            );
                            let delay = crate::recovery::reconcile_delay(
                                &advice,
                                std::time::SystemTime::now(),
                                std::time::Duration::from_secs(1),
                            );
                            tokio::time::sleep(delay).await;
                        }
                        RecoveryPlan::Engine(directive) => {
                            let directive_scope = directive_target_scope(&directive);
                            // `directive_target_scope` IS the blast radius:
                            // `Some(scope)` names a single scope to reset, so
                            // the rest of this hint's scopes are unaffected
                            // and must still be swept; `None` is account-wide
                            // (RestartAccount, SchemaIncompatible,
                            // OperatorOverrideRequired), and driving further
                            // scopes into a reset the engine is about to run
                            // wastes wire calls against cursors it is about
                            // to discard.
                            let account_wide = directive_scope.is_none();
                            let _ = self
                                .reopen_tx
                                .send(ReopenRequest::Recovery {
                                    scope: directive_scope,
                                    error: original,
                                })
                                .await;
                            if account_wide {
                                return Ok(());
                            }
                            continue;
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
        let warning = Warning::user_safe(kind, message);
        MultiplexerEvent {
            scope: CursorScope::Account,
            event: Arc::new(bifrost_types::SyncEvent::Warning(warning)),
            checkpoint: None,
            publication: None,
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
