//! Engine-side recovery dispatch helpers.
//!
//! The protocol crate has already derived a `RecoveryClass` on every
//! `AccountError` it produces; the engine's job is to dispatch that
//! verdict, not to reclassify. `plan_recovery` is the four-way split
//! the multiplexer, push reconciler, and mutation pipeline all use to
//! decide what happens next.
//!
//! Engine-created `AccountError` constructors (cursor envelope schema
//! mismatch, cursor-invalid for a scope, malformed account checkpoint)
//! also live here so the per-call sites do not redo the kind/cause
//! plumbing.

use std::time::{Duration, SystemTime};

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, CursorScope,
    DiagnosticText, EngineDirective, ErrorScope, Fatal, Protocol, ProtocolErrorKind,
    ReconcileAdvice, RecoveryClass, RetryAdvice, StateCause, SyncStateErrorKind,
};

/// Engine-facing dispatch verdict for an `AccountError`. The four
/// variants exactly mirror `RecoveryClass`'s four-way split
/// (`is_retryable`, `requires_reconciliation`, `requires_engine_action`,
/// `is_terminal`); collapsing the nine terminal variants into
/// `SurfaceTerminal` is the only intentional information loss.
#[derive(Clone, Debug)]
pub(crate) enum RecoveryPlan {
    Retry(RetryAdvice),
    Reconcile(ReconcileAdvice),
    Engine(EngineDirective),
    SurfaceTerminal(Fatal),
}

/// Dispatch an `AccountError` to the engine plan.
///
/// The wildcard arm matches precisely the terminal `RecoveryClass`
/// variants, and `Fatal::try_from(AccountError)` succeeds on exactly
/// those, so the `.expect` cannot fire under the current types-crate
/// contract. The `plan_recovery_terminal_round_trip` test below pins
/// this property against silent drift.
#[must_use]
pub(crate) fn plan_recovery(err: AccountError) -> RecoveryPlan {
    match err.recovery() {
        RecoveryClass::Retry(advice) => RecoveryPlan::Retry(advice.clone()),
        RecoveryClass::Reconcile(advice) => RecoveryPlan::Reconcile(advice.clone()),
        RecoveryClass::Engine(directive) => RecoveryPlan::Engine(directive.clone()),
        _ => RecoveryPlan::SurfaceTerminal(
            Fatal::try_from(err).expect("terminal recovery must convert to Fatal"),
        ),
    }
}

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

// ---------- engine-created AccountError constructors ----------

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

/// Build a sync-state schema-incompatible error. The derived recovery
/// is `Engine(SchemaIncompatible)`; the engine clears every durable
/// cursor and re-establishes via inventory.
#[must_use]
pub(crate) fn schema_incompatible_error(operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
        Cause::State(StateCause::SchemaIncompatible),
    )
    .operation(operation)
    .build()
}

/// Build a protocol contract-violation error stamped with the upstream
/// protocol that produced the malformed checkpoint. Used when the
/// engine reads back a checkpoint shape it cannot interpret (wrong
/// scope, wrong checkpoint kind, unknown checkpoint variant).
#[must_use]
pub(crate) fn malformed_checkpoint_error(
    protocol: Protocol,
    operation: AccountOperation,
    detail: impl Into<String>,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
        Cause::Wire(bifrost_types::WireCause::MalformedResponse {
            protocol,
            detail: Some(DiagnosticText::support_only(detail.into())),
        }),
    )
    .protocol(protocol)
    .operation(operation)
    .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{
        AccessCause, AccessErrorKind, AttemptCause, AuthCause, AuthErrorKind, RequestCause,
        RequestErrorKind, ResourceKind, RetryDisposition, RetryReason, ServerCause,
        ServerErrorKind, ThrottleScope, TransmissionState, TransportCause, TransportErrorKind,
        TransportKind,
    };

    fn transport_retry_error() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(TransportKind::Network, None)),
        )
        .operation(AccountOperation::SyncChanges)
        .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
        .build()
    }

    #[test]
    fn plan_recovery_retry_preserves_advice() {
        let err = AccountErrorBuilder::new(
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited {
                retry_after: Some(Duration::from_secs(5)),
            }),
        )
        .operation(AccountOperation::Search)
        .throttle_scope(ThrottleScope::Tenant)
        .build();

        match plan_recovery(err) {
            RecoveryPlan::Retry(advice) => {
                assert_eq!(advice.disposition, RetryDisposition::SameRequest);
                assert_eq!(advice.reason, RetryReason::RateLimited);
                assert_eq!(advice.throttle_scope, Some(ThrottleScope::Tenant));
                assert_eq!(advice.min_delay, Some(Duration::from_secs(5)));
            }
            other => panic!("expected Retry, got {other:?}"),
        }
    }

    #[test]
    fn plan_recovery_reconcile_preserves_actions() {
        // Non-idempotent op + InFlight transport drop -> Reconcile.
        let err = AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(TransportKind::Network, None)),
        )
        .operation(AccountOperation::Send)
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::InFlight,
        )))
        .build();

        match plan_recovery(err) {
            RecoveryPlan::Reconcile(advice) => {
                assert!(
                    advice
                        .guidance
                        .actions
                        .contains(&bifrost_types::ReconcileAction::CheckTarget)
                );
            }
            other => panic!("expected Reconcile, got {other:?}"),
        }
    }

    #[test]
    fn plan_recovery_engine_restart_scope() {
        let scope = CursorScope::Account;
        let err = AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        )
        .scope(ErrorScope::Cursor(scope.clone()))
        .operation(AccountOperation::SyncChanges)
        .build();

        match plan_recovery(err) {
            RecoveryPlan::Engine(EngineDirective::RestartScope(got)) => {
                assert_eq!(got, scope);
            }
            other => panic!("expected Engine(RestartScope), got {other:?}"),
        }
    }

    /// Critical test: every terminal `RecoveryClass` variant must
    /// round-trip through `plan_recovery` to `SurfaceTerminal`. This
    /// pins the `.expect("terminal recovery must convert to Fatal")`
    /// against silent drift in `Fatal::TryFrom<AccountError>`.
    #[test]
    fn plan_recovery_terminal_round_trip() {
        // AuthLost
        let auth_lost = AccountErrorBuilder::new(
            AccountErrorKind::Authentication(AuthErrorKind::Expired),
            Cause::Auth(AuthCause::Expired),
        )
        .build();
        assert!(matches!(
            plan_recovery(auth_lost),
            RecoveryPlan::SurfaceTerminal(_)
        ));

        // NeedsAdminConsent
        let needs_consent = AccountErrorBuilder::new(
            AccountErrorKind::Authorization(AccessErrorKind::AdminConsentRequired),
            Cause::Access(AccessCause::AdminConsentRequired {
                needed: "mail.read",
            }),
        )
        .build();
        assert!(matches!(
            plan_recovery(needs_consent),
            RecoveryPlan::SurfaceTerminal(_)
        ));

        // NeedsPolicyChange
        let needs_policy = AccountErrorBuilder::new(
            AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked),
            Cause::Access(AccessCause::PolicyBlocked),
        )
        .build();
        assert!(matches!(
            plan_recovery(needs_policy),
            RecoveryPlan::SurfaceTerminal(_)
        ));

        // NoPermission
        let no_perm = AccountErrorBuilder::new(
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: Some(ResourceKind::Mailbox),
            }),
        )
        .build();
        assert!(matches!(
            plan_recovery(no_perm),
            RecoveryPlan::SurfaceTerminal(_)
        ));

        // Unsupported
        let unsupported = AccountErrorBuilder::new(
            AccountErrorKind::Unsupported(AccountOperation::Send),
            Cause::Request(RequestCause::Unsupported {
                operation: AccountOperation::Send,
            }),
        )
        .build();
        assert!(matches!(
            plan_recovery(unsupported),
            RecoveryPlan::SurfaceTerminal(_)
        ));

        // ClientBug
        let client_bug = AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("bad request"),
            }),
        )
        .build();
        assert!(matches!(
            plan_recovery(client_bug),
            RecoveryPlan::SurfaceTerminal(_)
        ));

        // ProviderContractViolation
        let contract = AccountErrorBuilder::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(bifrost_types::WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: None,
            }),
        )
        .build();
        assert!(matches!(
            plan_recovery(contract),
            RecoveryPlan::SurfaceTerminal(_)
        ));

        // ProviderRefused (4xx via Server::Error with Some(451))
        let refused = AccountErrorBuilder::new(
            AccountErrorKind::Server(ServerErrorKind::Error { status: Some(451) }),
            Cause::Server(ServerCause::Error { status: Some(451) }),
        )
        .operation(AccountOperation::SyncChanges)
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::Acknowledged,
        )))
        .build();
        assert!(matches!(
            plan_recovery(refused),
            RecoveryPlan::SurfaceTerminal(_)
        ));

        // UnknownPermanent (Protocol::Unknown)
        let unknown_perm = AccountErrorBuilder::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(bifrost_types::WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: None,
            }),
        )
        .operation(AccountOperation::SyncChanges)
        .build();
        assert!(matches!(
            plan_recovery(unknown_perm),
            RecoveryPlan::SurfaceTerminal(_)
        ));
    }

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
    fn schema_incompatible_error_derives_engine_schema_incompatible() {
        let err = schema_incompatible_error(AccountOperation::SyncChanges);
        assert!(matches!(
            err.recovery(),
            RecoveryClass::Engine(EngineDirective::SchemaIncompatible)
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

    /// Sanity-check that the test transport error rebuilds the expected
    /// recovery (Retry::SameRequest). Used as the seed for several
    /// dispatch tests above.
    #[test]
    fn transport_retry_error_derives_retry() {
        let err = transport_retry_error();
        assert!(err.recovery().is_retryable());
    }
}
