use std::time::{Duration, SystemTime};

use serde::Serialize;

use crate::capabilities::CapabilityDelta;
use crate::cursor::CursorScope;

use super::account_error::AccountError;
use super::cause::{
    AccessCause, AuthCause, Cause, CauseChain, RequestCause, ServerCause, StateCause,
    TransmissionState,
};
use super::kind::{
    AccessErrorKind, AccountErrorKind, AuthErrorKind, MailboxUnavailableKind, ProtocolErrorKind,
    RequestErrorKind, ResourceKind, ServerErrorKind, SyncStateErrorKind,
};
use super::scope::{AccountOperation, ErrorScope};

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RecoveryClass {
    Retry(RetryAdvice),
    Reconcile(ReconcileAdvice),
    Engine(EngineDirective),
    AuthLost,
    NeedsAdminConsent { needed: &'static str },
    NeedsPolicyChange,
    NoPermission { resource: Option<ResourceKind> },
    Unsupported(AccountOperation),
    ClientBug,
    ProviderContractViolation,
    ProviderRefused,
    UnknownPermanent,
}

impl RecoveryClass {
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retry(_))
    }

    #[must_use]
    pub fn requires_reconciliation(&self) -> bool {
        matches!(self, Self::Reconcile(_))
    }

    #[must_use]
    pub fn requires_engine_action(&self) -> bool {
        matches!(self, Self::Engine(_))
    }

    #[must_use]
    pub fn is_terminal(&self) -> bool {
        !self.is_retryable() && !self.requires_reconciliation() && !self.requires_engine_action()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EngineDirective {
    RestartScope(CursorScope),
    RestartAccount,
    DowngradeStrategy(StrategyDowngrade),
    DowngradeCapabilityForScope(CursorScope),
    SchemaIncompatible,
    CapabilityChanged { delta: CapabilityDelta },
    OperatorOverrideRequired { reason: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RetryAdvice {
    pub disposition: RetryDisposition,
    pub not_before: Option<SystemTime>,
    pub min_delay: Option<Duration>,
    pub reason: RetryReason,
    pub throttle_scope: Option<ThrottleScope>,
}

impl RetryAdvice {
    #[must_use]
    pub fn new(
        disposition: RetryDisposition,
        not_before: Option<SystemTime>,
        min_delay: Option<Duration>,
        reason: RetryReason,
        throttle_scope: Option<ThrottleScope>,
    ) -> Self {
        Self {
            disposition,
            not_before,
            min_delay,
            reason,
            throttle_scope,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum RetryDisposition {
    SameRequest,
    AfterStateRefresh,
    AfterAuthRefresh,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ReconcileAdvice {
    pub reason: ReconcileReason,
    pub guidance: ReconcileGuidance,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum ReconcileReason {
    TransportDropAfterSend,
    PartialCompletionSignal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconcileGuidance {
    pub actions: Vec<ReconcileAction>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum ReconcileAction {
    CheckTarget,
    DedupeByClientId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum RetryReason {
    Transport,
    ServerUnavailable,
    RateLimited,
    QuotaExhausted,
    ConcurrencyConflict,
    RefreshTransient,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum ThrottleScope {
    Request,
    Mailbox,
    Account,
    Tenant,
    Provider,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RemediationAction {
    RefreshToken,
    Reauthorize,
    RequestAdminConsent { needed: &'static str },
    UpdateTenantPolicy,
    CheckMailboxLicense,
    RetryLater { not_before: Option<SystemTime> },
    RestartAccount,
    RestartScope(CursorScope),
    FixClientRequest,
    ContactProviderSupport,
}

#[derive(Debug, Clone)]
pub struct Fatal(pub AccountError);

impl TryFrom<AccountError> for Fatal {
    type Error = AccountError;

    fn try_from(err: AccountError) -> Result<Self, Self::Error> {
        if err.recovery().is_terminal() {
            Ok(Self(err))
        } else {
            Err(err)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StrategyDowngrade {
    QResyncToCondstore,
    CondstoreToBasic,
}

pub(crate) fn derive(
    kind: &AccountErrorKind,
    scope: Option<&ErrorScope>,
    operation: Option<AccountOperation>,
    chain: &CauseChain,
    retry_not_before: Option<SystemTime>,
    throttle_scope: Option<ThrottleScope>,
    idempotency_override: Option<bool>,
) -> RecoveryClass {
    let tx_state = transmission_state(chain).unwrap_or(TransmissionState::Unsent);
    let idempotent = idempotency_override
        .unwrap_or_else(|| operation.is_none_or(AccountOperation::is_idempotent));

    match kind {
        AccountErrorKind::Transport(_) => {
            assert!(
                tx_state != TransmissionState::Acknowledged,
                "transport failures cannot have acknowledged transmission state"
            );
            transient_retry_or_reconcile(
                tx_state,
                idempotent,
                RetryReason::Transport,
                retry_not_before,
                None,
                None,
            )
        }
        AccountErrorKind::Authentication(kind) => derive_auth(*kind, retry_not_before),
        AccountErrorKind::Authorization(kind) => derive_access(*kind, chain),
        AccountErrorKind::Server(kind) => derive_server(
            *kind,
            tx_state,
            idempotent,
            chain,
            retry_not_before,
            throttle_scope,
        ),
        AccountErrorKind::SyncState(kind) => derive_sync_state(*kind, scope, chain),
        AccountErrorKind::ConcurrencyConflict => RecoveryClass::Retry(RetryAdvice {
            disposition: RetryDisposition::AfterStateRefresh,
            not_before: retry_not_before,
            min_delay: None,
            reason: RetryReason::ConcurrencyConflict,
            throttle_scope: None,
        }),
        AccountErrorKind::Request(
            RequestErrorKind::Malformed | RequestErrorKind::BatchInputInvalid,
        ) => RecoveryClass::ClientBug,
        AccountErrorKind::NotFound(_) => RecoveryClass::ProviderRefused,
        AccountErrorKind::Unsupported(op) => RecoveryClass::Unsupported(*op),
        AccountErrorKind::Protocol(kind) => derive_protocol(*kind, idempotent),
    }
}

pub(crate) fn suggest(
    kind: &AccountErrorKind,
    recovery: &RecoveryClass,
    scope: Option<&ErrorScope>,
    chain: &CauseChain,
) -> Option<RemediationAction> {
    match kind {
        AccountErrorKind::Authentication(AuthErrorKind::Expired) => {
            Some(RemediationAction::RefreshToken)
        }
        AccountErrorKind::Authentication(
            AuthErrorKind::Revoked | AuthErrorKind::ReauthorizationRequired,
        ) => Some(RemediationAction::Reauthorize),
        AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient) => None,
        AccountErrorKind::Authorization(AccessErrorKind::AdminConsentRequired) => {
            Some(RemediationAction::RequestAdminConsent {
                needed: access_needed(chain).unwrap_or("admin-consent"),
            })
        }
        AccountErrorKind::Authorization(
            AccessErrorKind::ConditionalAccessBlocked
            | AccessErrorKind::PolicyBlocked
            | AccessErrorKind::InsufficientScope,
        ) => Some(RemediationAction::UpdateTenantPolicy),
        AccountErrorKind::Authorization(AccessErrorKind::MailboxNotLicensed) => {
            Some(RemediationAction::CheckMailboxLicense)
        }
        AccountErrorKind::Authorization(
            AccessErrorKind::PermissionDenied
            | AccessErrorKind::AccountDisabled
            | AccessErrorKind::MailboxUnavailable {
                kind: MailboxUnavailableKind::Permanent,
            },
        ) => Some(RemediationAction::ContactProviderSupport),
        AccountErrorKind::Authorization(AccessErrorKind::MailboxUnavailable {
            kind: MailboxUnavailableKind::Transient,
        }) => retry_later(recovery),
        AccountErrorKind::Server(
            ServerErrorKind::RateLimited | ServerErrorKind::QuotaExhausted,
        ) => retry_later(recovery),
        AccountErrorKind::Server(ServerErrorKind::Unavailable | ServerErrorKind::Error { .. }) => {
            None
        }
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible) => {
            Some(RemediationAction::RestartAccount)
        }
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid) => cursor_scope(scope)
            .map(RemediationAction::RestartScope)
            .or(Some(RemediationAction::RestartAccount)),
        AccountErrorKind::Request(_) => Some(RemediationAction::FixClientRequest),
        AccountErrorKind::Protocol(
            ProtocolErrorKind::ParseFailed
            | ProtocolErrorKind::MissingField
            | ProtocolErrorKind::ContractViolation
            | ProtocolErrorKind::Unknown,
        )
        | AccountErrorKind::NotFound(_) => Some(RemediationAction::ContactProviderSupport),
        AccountErrorKind::Transport(_)
        | AccountErrorKind::SyncState(
            SyncStateErrorKind::StrategyFailure
            | SyncStateErrorKind::ScopeCapabilityLost
            | SyncStateErrorKind::CapabilityChanged
            | SyncStateErrorKind::OperatorOverrideNeeded,
        )
        | AccountErrorKind::ConcurrencyConflict
        | AccountErrorKind::Unsupported(_)
        | AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse) => None,
    }
}

pub(crate) fn kind_matches_cause(kind: &AccountErrorKind, cause: &Cause) -> bool {
    match (kind, cause) {
        (AccountErrorKind::Transport(kind), Cause::Transport(cause)) => *kind == cause.error_kind(),
        (AccountErrorKind::Authentication(kind), Cause::Auth(cause)) => {
            auth_kind_matches_cause(*kind, cause)
        }
        (AccountErrorKind::Authorization(kind), Cause::Access(cause)) => {
            access_kind_matches_cause(*kind, cause)
        }
        (AccountErrorKind::Server(kind), Cause::Server(cause)) => {
            server_kind_matches_cause(*kind, cause)
        }
        (AccountErrorKind::SyncState(kind), Cause::State(cause)) => {
            sync_kind_matches_cause(*kind, cause)
        }
        (AccountErrorKind::Protocol(_), Cause::Wire(_)) => true,
        (AccountErrorKind::ConcurrencyConflict, Cause::State(StateCause::ConcurrencyConflict)) => {
            true
        }
        (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed { .. } | RequestCause::InvalidArgument { .. }),
        )
        | (
            AccountErrorKind::Request(RequestErrorKind::BatchInputInvalid),
            Cause::Request(RequestCause::BatchInputInvalid { .. }),
        ) => true,
        (AccountErrorKind::NotFound(kind), Cause::Request(RequestCause::NotFound { what, .. })) => {
            kind == what
        }
        (
            AccountErrorKind::Unsupported(op),
            Cause::Request(RequestCause::Unsupported { operation }),
        ) => op == operation,
        _ => false,
    }
}

fn auth_kind_matches_cause(kind: AuthErrorKind, cause: &AuthCause) -> bool {
    matches!(
        (kind, cause),
        (AuthErrorKind::Expired, AuthCause::Expired)
            | (AuthErrorKind::RefreshTransient, AuthCause::RefreshTransient)
            | (AuthErrorKind::Revoked, AuthCause::Revoked)
            | (
                AuthErrorKind::ReauthorizationRequired,
                AuthCause::ReauthorizationRequired
            )
    )
}

fn access_kind_matches_cause(kind: AccessErrorKind, cause: &AccessCause) -> bool {
    matches!(
        (kind, cause),
        (
            AccessErrorKind::AdminConsentRequired,
            AccessCause::AdminConsentRequired { .. }
        ) | (
            AccessErrorKind::ConditionalAccessBlocked,
            AccessCause::ConditionalAccessBlocked
        ) | (AccessErrorKind::PolicyBlocked, AccessCause::PolicyBlocked)
            | (
                AccessErrorKind::InsufficientScope,
                AccessCause::InsufficientScope { .. }
            )
            | (
                AccessErrorKind::PermissionDenied,
                AccessCause::PermissionDenied { .. }
            )
            | (
                AccessErrorKind::AccountDisabled,
                AccessCause::AccountDisabled
            )
            | (
                AccessErrorKind::MailboxUnavailable { .. },
                AccessCause::MailboxUnavailable { .. }
            )
            | (
                AccessErrorKind::MailboxNotLicensed,
                AccessCause::MailboxNotLicensed
            )
    )
}

fn server_kind_matches_cause(kind: ServerErrorKind, cause: &ServerCause) -> bool {
    match (kind, cause) {
        (ServerErrorKind::Unavailable, ServerCause::Unavailable { .. })
        | (ServerErrorKind::RateLimited, ServerCause::RateLimited { .. })
        | (ServerErrorKind::QuotaExhausted, ServerCause::QuotaExhausted { .. }) => true,
        (
            ServerErrorKind::Error { status },
            ServerCause::Error {
                status: cause_status,
            },
        ) => match (status, cause_status) {
            (Some(kind_status), Some(cause_status)) => kind_status == *cause_status,
            (None, _) | (_, None) => true,
        },
        _ => false,
    }
}

fn sync_kind_matches_cause(kind: SyncStateErrorKind, cause: &StateCause) -> bool {
    matches!(
        (kind, cause),
        (SyncStateErrorKind::CursorInvalid, StateCause::CursorInvalid)
            | (
                SyncStateErrorKind::StrategyFailure,
                StateCause::StrategyFailure { .. }
            )
            | (
                SyncStateErrorKind::ScopeCapabilityLost,
                StateCause::ScopeCapabilityLost
            )
            | (
                SyncStateErrorKind::SchemaIncompatible,
                StateCause::SchemaIncompatible
            )
            | (
                SyncStateErrorKind::CapabilityChanged,
                StateCause::CapabilityChanged { .. }
            )
            | (
                SyncStateErrorKind::OperatorOverrideNeeded,
                StateCause::OperatorOverrideNeeded { .. }
            )
    )
}

fn derive_auth(kind: AuthErrorKind, retry_not_before: Option<SystemTime>) -> RecoveryClass {
    match kind {
        AuthErrorKind::RefreshTransient => RecoveryClass::Retry(RetryAdvice {
            disposition: RetryDisposition::AfterAuthRefresh,
            not_before: retry_not_before,
            min_delay: None,
            reason: RetryReason::RefreshTransient,
            throttle_scope: None,
        }),
        AuthErrorKind::Expired
        | AuthErrorKind::Revoked
        | AuthErrorKind::ReauthorizationRequired => RecoveryClass::AuthLost,
    }
}

fn derive_access(kind: AccessErrorKind, chain: &CauseChain) -> RecoveryClass {
    match kind {
        AccessErrorKind::AdminConsentRequired => RecoveryClass::NeedsAdminConsent {
            needed: access_needed(chain).unwrap_or("admin-consent"),
        },
        AccessErrorKind::ConditionalAccessBlocked
        | AccessErrorKind::PolicyBlocked
        | AccessErrorKind::InsufficientScope
        | AccessErrorKind::MailboxNotLicensed => RecoveryClass::NeedsPolicyChange,
        AccessErrorKind::PermissionDenied => RecoveryClass::NoPermission {
            resource: access_resource(chain),
        },
        AccessErrorKind::AccountDisabled => RecoveryClass::ProviderRefused,
        AccessErrorKind::MailboxUnavailable {
            kind: MailboxUnavailableKind::Transient,
        } => RecoveryClass::Retry(RetryAdvice {
            disposition: RetryDisposition::SameRequest,
            not_before: None,
            min_delay: None,
            reason: RetryReason::ServerUnavailable,
            throttle_scope: None,
        }),
        AccessErrorKind::MailboxUnavailable {
            kind: MailboxUnavailableKind::Permanent,
        } => RecoveryClass::ProviderRefused,
    }
}

fn derive_server(
    kind: ServerErrorKind,
    tx_state: TransmissionState,
    idempotent: bool,
    chain: &CauseChain,
    retry_not_before: Option<SystemTime>,
    throttle_scope: Option<ThrottleScope>,
) -> RecoveryClass {
    let retry_after = server_retry_after(chain);
    match kind {
        ServerErrorKind::Unavailable => transient_retry_or_reconcile(
            tx_state,
            idempotent,
            RetryReason::ServerUnavailable,
            retry_not_before,
            retry_after,
            None,
        ),
        ServerErrorKind::RateLimited => transient_retry_or_reconcile(
            tx_state,
            idempotent,
            RetryReason::RateLimited,
            retry_not_before,
            retry_after,
            throttle_scope,
        ),
        ServerErrorKind::QuotaExhausted => transient_retry_or_reconcile(
            tx_state,
            idempotent,
            RetryReason::QuotaExhausted,
            retry_not_before,
            retry_after,
            throttle_scope,
        ),
        ServerErrorKind::Error { status } => {
            let status = status.or_else(|| server_status(chain));
            if matches!(status, Some(500..=599)) {
                transient_retry_or_reconcile(
                    tx_state,
                    idempotent,
                    RetryReason::ServerUnavailable,
                    retry_not_before,
                    retry_after,
                    None,
                )
            } else {
                RecoveryClass::ProviderRefused
            }
        }
    }
}

fn derive_sync_state(
    kind: SyncStateErrorKind,
    scope: Option<&ErrorScope>,
    chain: &CauseChain,
) -> RecoveryClass {
    match kind {
        SyncStateErrorKind::CursorInvalid => cursor_scope(scope)
            .map(EngineDirective::RestartScope)
            .map(RecoveryClass::Engine)
            .unwrap_or(RecoveryClass::Engine(EngineDirective::RestartAccount)),
        SyncStateErrorKind::StrategyFailure => RecoveryClass::Engine(
            EngineDirective::DowngradeStrategy(strategy_downgrade(chain)),
        ),
        SyncStateErrorKind::ScopeCapabilityLost => cursor_scope(scope)
            .map(EngineDirective::DowngradeCapabilityForScope)
            .map(RecoveryClass::Engine)
            .unwrap_or(RecoveryClass::Engine(EngineDirective::RestartAccount)),
        SyncStateErrorKind::SchemaIncompatible => {
            RecoveryClass::Engine(EngineDirective::SchemaIncompatible)
        }
        SyncStateErrorKind::CapabilityChanged => {
            RecoveryClass::Engine(EngineDirective::CapabilityChanged {
                delta: capability_delta(chain).unwrap_or_default(),
            })
        }
        SyncStateErrorKind::OperatorOverrideNeeded => {
            RecoveryClass::Engine(EngineDirective::OperatorOverrideRequired {
                reason: operator_reason(chain)
                    .unwrap_or("operator override required")
                    .to_string(),
            })
        }
    }
}

fn derive_protocol(kind: ProtocolErrorKind, idempotent: bool) -> RecoveryClass {
    match kind {
        ProtocolErrorKind::ParseFailed
        | ProtocolErrorKind::MissingField
        | ProtocolErrorKind::ContractViolation => RecoveryClass::ProviderContractViolation,
        ProtocolErrorKind::PartialResponse if idempotent => RecoveryClass::Retry(RetryAdvice {
            disposition: RetryDisposition::SameRequest,
            not_before: None,
            min_delay: None,
            reason: RetryReason::Transport,
            throttle_scope: None,
        }),
        ProtocolErrorKind::PartialResponse => RecoveryClass::Reconcile(ReconcileAdvice {
            reason: ReconcileReason::PartialCompletionSignal,
            guidance: ReconcileGuidance {
                actions: vec![
                    ReconcileAction::CheckTarget,
                    ReconcileAction::DedupeByClientId,
                ],
            },
        }),
        ProtocolErrorKind::Unknown => RecoveryClass::UnknownPermanent,
    }
}

fn transient_retry_or_reconcile(
    tx_state: TransmissionState,
    idempotent: bool,
    reason: RetryReason,
    not_before: Option<SystemTime>,
    min_delay: Option<Duration>,
    throttle_scope: Option<ThrottleScope>,
) -> RecoveryClass {
    match tx_state {
        TransmissionState::InFlight if !idempotent => RecoveryClass::Reconcile(ReconcileAdvice {
            reason: ReconcileReason::TransportDropAfterSend,
            guidance: ReconcileGuidance {
                actions: vec![ReconcileAction::CheckTarget],
            },
        }),
        TransmissionState::Unsent
        | TransmissionState::InFlight
        | TransmissionState::Acknowledged => RecoveryClass::Retry(RetryAdvice {
            disposition: RetryDisposition::SameRequest,
            not_before,
            min_delay,
            reason,
            throttle_scope,
        }),
    }
}

fn transmission_state(chain: &CauseChain) -> Option<TransmissionState> {
    chain.iter().find_map(Cause::transmission_state)
}

fn access_needed(chain: &CauseChain) -> Option<&'static str> {
    chain.iter().find_map(|cause| match cause {
        Cause::Access(AccessCause::InsufficientScope { needed })
        | Cause::Access(AccessCause::AdminConsentRequired { needed }) => Some(*needed),
        _ => None,
    })
}

fn access_resource(chain: &CauseChain) -> Option<ResourceKind> {
    chain.iter().find_map(|cause| match cause {
        Cause::Access(AccessCause::PermissionDenied { resource }) => *resource,
        _ => None,
    })
}

fn server_retry_after(chain: &CauseChain) -> Option<Duration> {
    chain.iter().find_map(|cause| match cause {
        Cause::Server(cause) => cause.retry_after(),
        _ => None,
    })
}

fn server_status(chain: &CauseChain) -> Option<u16> {
    chain.iter().find_map(|cause| match cause {
        Cause::Server(ServerCause::Error { status }) => *status,
        _ => None,
    })
}

fn cursor_scope(scope: Option<&ErrorScope>) -> Option<CursorScope> {
    match scope {
        Some(ErrorScope::Cursor(scope)) => Some(scope.clone()),
        _ => None,
    }
}

fn strategy_downgrade(chain: &CauseChain) -> StrategyDowngrade {
    chain
        .iter()
        .find_map(|cause| match cause {
            Cause::State(StateCause::StrategyFailure { downgrade }) => Some(*downgrade),
            _ => None,
        })
        .unwrap_or(StrategyDowngrade::QResyncToCondstore)
}

fn capability_delta(chain: &CauseChain) -> Option<CapabilityDelta> {
    chain.iter().find_map(|cause| match cause {
        Cause::State(StateCause::CapabilityChanged { delta }) => Some(delta.clone()),
        _ => None,
    })
}

fn operator_reason(chain: &CauseChain) -> Option<&str> {
    chain.iter().find_map(|cause| match cause {
        Cause::State(StateCause::OperatorOverrideNeeded { reason }) => Some(reason.as_str()),
        _ => None,
    })
}

fn retry_later(recovery: &RecoveryClass) -> Option<RemediationAction> {
    match recovery {
        RecoveryClass::Retry(advice) => Some(RemediationAction::RetryLater {
            not_before: advice.not_before,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{AttemptCause, TransportCause, TransportKind};

    fn chain(cause: Cause) -> CauseChain {
        CauseChain::new(vec![cause])
    }

    fn transport_chain(state: TransmissionState) -> CauseChain {
        CauseChain::new(vec![
            Cause::Transport(TransportCause {
                kind: TransportKind::Network,
                message: None,
            }),
            Cause::Attempt(AttemptCause {
                transmission_state: state,
            }),
        ])
    }

    #[test]
    fn transport_unsent_retries_same_request() {
        let recovery = derive(
            &AccountErrorKind::Transport(super::super::kind::TransportErrorKind::Network),
            None,
            Some(AccountOperation::Send),
            &transport_chain(TransmissionState::Unsent),
            None,
            None,
            None,
        );

        assert!(matches!(
            recovery,
            RecoveryClass::Retry(RetryAdvice {
                disposition: RetryDisposition::SameRequest,
                reason: RetryReason::Transport,
                ..
            })
        ));
    }

    #[test]
    fn non_idempotent_inflight_transport_reconciles() {
        let recovery = derive(
            &AccountErrorKind::Transport(super::super::kind::TransportErrorKind::Network),
            None,
            Some(AccountOperation::Send),
            &transport_chain(TransmissionState::InFlight),
            None,
            None,
            None,
        );

        assert!(matches!(
            recovery,
            RecoveryClass::Reconcile(ReconcileAdvice {
                reason: ReconcileReason::TransportDropAfterSend,
                ..
            })
        ));
    }

    #[test]
    fn idempotent_inflight_transport_retries() {
        let recovery = derive(
            &AccountErrorKind::Transport(super::super::kind::TransportErrorKind::Network),
            None,
            Some(AccountOperation::UpdateFlags),
            &transport_chain(TransmissionState::InFlight),
            None,
            None,
            None,
        );

        assert!(recovery.is_retryable());
    }

    #[test]
    #[should_panic(expected = "transport failures cannot have acknowledged transmission state")]
    fn acknowledged_transport_failure_is_rejected() {
        let _ = derive(
            &AccountErrorKind::Transport(super::super::kind::TransportErrorKind::Network),
            None,
            Some(AccountOperation::UpdateFlags),
            &transport_chain(TransmissionState::Acknowledged),
            None,
            None,
            None,
        );
    }

    #[test]
    fn refresh_transient_retries_after_auth_refresh() {
        let recovery = derive(
            &AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient),
            None,
            Some(AccountOperation::SyncChanges),
            &chain(Cause::Auth(AuthCause::RefreshTransient)),
            None,
            None,
            None,
        );

        assert!(matches!(
            recovery,
            RecoveryClass::Retry(RetryAdvice {
                disposition: RetryDisposition::AfterAuthRefresh,
                reason: RetryReason::RefreshTransient,
                ..
            })
        ));
    }

    #[test]
    fn admin_consent_is_terminal_with_needed_scope() {
        let recovery = derive(
            &AccountErrorKind::Authorization(AccessErrorKind::AdminConsentRequired),
            None,
            Some(AccountOperation::SyncChanges),
            &chain(Cause::Access(AccessCause::AdminConsentRequired {
                needed: "mail.read",
            })),
            None,
            None,
            None,
        );

        assert_eq!(
            recovery,
            RecoveryClass::NeedsAdminConsent {
                needed: "mail.read"
            }
        );
    }

    #[test]
    fn concurrency_conflict_retries_after_state_refresh() {
        let recovery = derive(
            &AccountErrorKind::ConcurrencyConflict,
            None,
            Some(AccountOperation::UpdateFlags),
            &chain(Cause::State(StateCause::ConcurrencyConflict)),
            None,
            None,
            None,
        );

        assert!(matches!(
            recovery,
            RecoveryClass::Retry(RetryAdvice {
                disposition: RetryDisposition::AfterStateRefresh,
                reason: RetryReason::ConcurrencyConflict,
                ..
            })
        ));
    }

    #[test]
    fn request_errors_are_client_bugs() {
        let recovery = derive(
            &AccountErrorKind::Request(RequestErrorKind::Malformed),
            None,
            None,
            &chain(Cause::Request(RequestCause::Malformed {
                detail: crate::error::DiagnosticText::support_only("bad request"),
            })),
            None,
            None,
            None,
        );

        assert_eq!(recovery, RecoveryClass::ClientBug);
    }

    #[test]
    fn protocol_partial_non_idempotent_reconciles() {
        let recovery = derive(
            &AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
            None,
            Some(AccountOperation::Send),
            &chain(Cause::Wire(crate::error::WireCause::Jmap(
                crate::error::JmapMethod::ServerPartialFail,
            ))),
            None,
            None,
            None,
        );

        assert!(recovery.requires_reconciliation());
    }

    #[test]
    fn rate_limit_carries_retry_deadline_and_throttle_scope() {
        let when = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let recovery = derive(
            &AccountErrorKind::Server(ServerErrorKind::RateLimited),
            None,
            Some(AccountOperation::Search),
            &chain(Cause::Server(ServerCause::RateLimited {
                retry_after: Some(Duration::from_secs(5)),
            })),
            Some(when),
            Some(ThrottleScope::Tenant),
            None,
        );

        assert_eq!(
            recovery,
            RecoveryClass::Retry(RetryAdvice {
                disposition: RetryDisposition::SameRequest,
                not_before: Some(when),
                min_delay: Some(Duration::from_secs(5)),
                reason: RetryReason::RateLimited,
                throttle_scope: Some(ThrottleScope::Tenant),
            })
        );
    }

    #[test]
    fn quota_inflight_non_idempotent_reconciles() {
        let recovery = derive(
            &AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
            None,
            Some(AccountOperation::Send),
            &CauseChain::new(vec![
                Cause::Server(ServerCause::QuotaExhausted { retry_after: None }),
                Cause::Attempt(AttemptCause {
                    transmission_state: TransmissionState::InFlight,
                }),
            ]),
            None,
            Some(ThrottleScope::Account),
            None,
        );

        assert!(matches!(
            recovery,
            RecoveryClass::Reconcile(ReconcileAdvice {
                reason: ReconcileReason::TransportDropAfterSend,
                ..
            })
        ));
    }

    #[test]
    fn server_5xx_retries_and_4xx_refuses() {
        let retry = derive(
            &AccountErrorKind::Server(ServerErrorKind::Error { status: Some(503) }),
            None,
            Some(AccountOperation::SyncChanges),
            &chain(Cause::Server(ServerCause::Error { status: Some(503) })),
            None,
            None,
            None,
        );
        let refused = derive(
            &AccountErrorKind::Server(ServerErrorKind::Error { status: Some(451) }),
            None,
            Some(AccountOperation::SyncChanges),
            &chain(Cause::Server(ServerCause::Error { status: Some(451) })),
            None,
            None,
            None,
        );

        assert!(retry.is_retryable());
        assert_eq!(refused, RecoveryClass::ProviderRefused);
    }

    #[test]
    fn cursor_invalid_uses_scope_when_available() {
        let scope = ErrorScope::Cursor(crate::CursorScope::Account);
        let scoped = derive(
            &AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Some(&scope),
            Some(AccountOperation::SyncChanges),
            &chain(Cause::State(StateCause::CursorInvalid)),
            None,
            None,
            None,
        );
        let account = derive(
            &AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            None,
            Some(AccountOperation::SyncChanges),
            &chain(Cause::State(StateCause::CursorInvalid)),
            None,
            None,
            None,
        );

        assert!(matches!(
            scoped,
            RecoveryClass::Engine(EngineDirective::RestartScope(crate::CursorScope::Account))
        ));
        assert_eq!(
            account,
            RecoveryClass::Engine(EngineDirective::RestartAccount)
        );
    }

    #[test]
    fn engine_directives_preserve_state_payloads() {
        let downgrade = derive(
            &AccountErrorKind::SyncState(SyncStateErrorKind::StrategyFailure),
            None,
            Some(AccountOperation::SyncChanges),
            &chain(Cause::State(StateCause::StrategyFailure {
                downgrade: StrategyDowngrade::CondstoreToBasic,
            })),
            None,
            None,
            None,
        );
        let operator = derive(
            &AccountErrorKind::SyncState(SyncStateErrorKind::OperatorOverrideNeeded),
            None,
            Some(AccountOperation::SyncChanges),
            &chain(Cause::State(StateCause::OperatorOverrideNeeded {
                reason: "qresync repeatedly failed".to_string(),
            })),
            None,
            None,
            None,
        );

        assert_eq!(
            downgrade,
            RecoveryClass::Engine(EngineDirective::DowngradeStrategy(
                StrategyDowngrade::CondstoreToBasic
            ))
        );
        assert_eq!(
            operator,
            RecoveryClass::Engine(EngineDirective::OperatorOverrideRequired {
                reason: "qresync repeatedly failed".to_string(),
            })
        );
    }

    #[test]
    fn mailbox_unavailable_transient_retries_and_permanent_refuses() {
        let transient = derive(
            &AccountErrorKind::Authorization(AccessErrorKind::MailboxUnavailable {
                kind: MailboxUnavailableKind::Transient,
            }),
            None,
            Some(AccountOperation::SyncChanges),
            &chain(Cause::Access(AccessCause::MailboxUnavailable {
                kind: MailboxUnavailableKind::Transient,
            })),
            None,
            None,
            None,
        );
        let permanent = derive(
            &AccountErrorKind::Authorization(AccessErrorKind::MailboxUnavailable {
                kind: MailboxUnavailableKind::Permanent,
            }),
            None,
            Some(AccountOperation::SyncChanges),
            &chain(Cause::Access(AccessCause::MailboxUnavailable {
                kind: MailboxUnavailableKind::Permanent,
            })),
            None,
            None,
            None,
        );

        assert!(transient.is_retryable());
        assert_eq!(permanent, RecoveryClass::ProviderRefused);
    }
}
