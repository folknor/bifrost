//! Engine-side recovery dispatch helpers.
//!
//! The protocol crate has already derived a `RecoveryClass` on every
//! `AccountError` it produces; the engine's job is to dispatch that
//! verdict, not to reclassify. These helpers exist for the small
//! amount of plumbing the engine reuses across multiple dispatch
//! sites: computing the effective retry sleep, resolving which
//! `CursorScope` an `EngineDirective` targets, and constructing the
//! single account-error shape the engine itself emits
//! (`restart_scope_error`).

use std::time::{Duration, SystemTime};

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, CursorScope,
    EngineDirective, ErrorScope, RetryAdvice, StateCause, SyncStateErrorKind,
};

/// Resolve the effective sleep an engine path should observe before
/// retrying. `not_before` is treated as an absolute wall-clock floor;
/// `min_delay` is a relative floor. When both are present we sleep
/// long enough to satisfy whichever is longer.
#[must_use]
pub(crate) fn retry_delay(advice: &RetryAdvice, now: SystemTime, fallback: Duration) -> Duration {
    let not_before = advice
        .not_before
        .and_then(|when| when.duration_since(now).ok());
    not_before
        .into_iter()
        .chain(advice.min_delay)
        .max()
        .unwrap_or(fallback)
}

/// True when `directive` targets a single cursor scope. Used by the
/// multiplexer to populate `ReopenRequest::Recovery::scope`.
#[must_use]
pub(crate) fn directive_target_scope(directive: &EngineDirective) -> Option<CursorScope> {
    match directive {
        EngineDirective::RestartScope(scope)
        | EngineDirective::DowngradeCapabilityForScope(scope) => Some(scope.clone()),
        EngineDirective::DowngradeStrategy(_)
        | EngineDirective::RestartAccount
        | EngineDirective::SchemaIncompatible
        | EngineDirective::CapabilityChanged { .. }
        | EngineDirective::OperatorOverrideRequired { .. } => None,
        _ => None,
    }
}

/// Build a sync-state cursor-invalid error scoped to a single cursor.
/// Used by scope lifecycle (`Created`, `Renamed`) and the reopen
/// listener when the engine needs to ask itself for a fresh
/// establishment of one scope. The derived recovery is
/// `Engine(RestartScope(scope))`.
#[must_use]
pub(crate) fn restart_scope_error(scope: CursorScope, operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
        Cause::State(StateCause::CursorInvalid),
    )
    .scope(ErrorScope::Cursor(scope))
    .operation(operation)
    .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{RecoveryClass, RetryDisposition, RetryReason};

    #[test]
    fn retry_delay_uses_later_of_not_before_and_min_delay() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let advice = RetryAdvice::new(
            RetryDisposition::SameRequest,
            Some(now + Duration::from_secs(5)),
            Some(Duration::from_secs(2)),
            RetryReason::Transport,
            None,
        );
        // not_before is +5s from now, min_delay is 2s; choose +5s.
        let delay = retry_delay(&advice, now, Duration::from_secs(1));
        assert_eq!(delay, Duration::from_secs(5));

        let advice2 = RetryAdvice::new(
            RetryDisposition::SameRequest,
            Some(now + Duration::from_secs(1)),
            Some(Duration::from_secs(10)),
            RetryReason::Transport,
            None,
        );
        let delay2 = retry_delay(&advice2, now, Duration::from_secs(1));
        assert_eq!(delay2, Duration::from_secs(10));

        let advice3 = RetryAdvice::new(
            RetryDisposition::SameRequest,
            None,
            None,
            RetryReason::Transport,
            None,
        );
        let delay3 = retry_delay(&advice3, now, Duration::from_secs(3));
        assert_eq!(delay3, Duration::from_secs(3));
    }

    #[test]
    fn restart_scope_error_derives_engine_restart_scope() {
        let scope = CursorScope::Account;
        let err = restart_scope_error(scope.clone(), AccountOperation::SyncChanges);
        assert!(matches!(
            err.recovery(),
            RecoveryClass::Engine(EngineDirective::RestartScope(got)) if *got == scope
        ));
    }

    #[test]
    fn directive_target_scope_picks_scope_bound_directives() {
        let scope = CursorScope::Account;
        assert_eq!(
            directive_target_scope(&EngineDirective::RestartScope(scope.clone())),
            Some(scope.clone())
        );
        assert_eq!(
            directive_target_scope(&EngineDirective::DowngradeCapabilityForScope(scope.clone())),
            Some(scope.clone())
        );
        assert_eq!(
            directive_target_scope(&EngineDirective::RestartAccount),
            None
        );
        assert_eq!(
            directive_target_scope(&EngineDirective::SchemaIncompatible),
            None
        );
    }
}
