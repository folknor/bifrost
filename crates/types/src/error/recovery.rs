use std::time::{Duration, SystemTime};

use serde::Serialize;

use crate::cursor::CursorScope;
use crate::ids::{AccountId, MailboxId};

use super::account_error::AccountError;
use super::cause::{
    AccessCause, AuthCause, Cause, CauseChain, RequestCause, ServerCause, StateCause,
    TransmissionState,
};
use super::kind::{
    AccessErrorKind, AccountErrorKind, AuthErrorKind, MailboxUnavailableKind, ProtocolErrorKind,
    RequestErrorKind, ResourceKind, ServerErrorKind, SyncStateErrorKind,
};
use super::scope::{AccountOperation, ErrorScope, Provider};

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
        match self {
            Self::Retry(_) | Self::Reconcile(_) | Self::Engine(_) => false,
            Self::AuthLost
            | Self::NeedsAdminConsent { .. }
            | Self::NeedsPolicyChange
            | Self::NoPermission { .. }
            | Self::Unsupported(_)
            | Self::ClientBug
            | Self::ProviderContractViolation
            | Self::ProviderRefused
            | Self::UnknownPermanent => true,
        }
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
    OperatorOverrideRequired {
        reason: String,
    },
    /// Permanently disable a single cursor scope without escalating to
    /// the account: an admin revoked access to one shared/other-user
    /// folder mid-sync. The engine deletes the scope's in-memory and
    /// durable cursor, drops it from the membership index, and broadcasts
    /// a scoped `Warning::OperatorAttentionNeeded`. Siblings keep syncing;
    /// the account is NOT paused and auth is NOT treated as lost.
    DisableScope(CursorScope),
}

/// Provider-supplied hint for when a retry may be attempted. The hint
/// is the value; callers compute either an absolute wall-clock or a
/// duration via the accessors below. Storing both pre-computed fields
/// invites drift (the spec used to carry `not_before: SystemTime` and
/// `min_delay: Duration` separately and that produced the side-channel
/// bug the convergence rewrite eliminated).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RetryHint {
    /// Wait at least this long. Suitable for parsed `Retry-After` in
    /// seconds and for `min_delay` semantics.
    After(Duration),
    /// Do not retry before this wall-clock time. Suitable for parsed
    /// HTTP-date `Retry-After` values.
    At(SystemTime),
}

impl RetryHint {
    /// Resolve the hint to an absolute wall-clock deadline given the
    /// caller's notion of "now".
    #[must_use]
    pub fn not_before(&self, now: SystemTime) -> SystemTime {
        match self {
            Self::After(duration) => now + *duration,
            Self::At(when) => *when,
        }
    }

    /// Resolve the hint to a duration to wait given the caller's notion
    /// of "now". Returns `Duration::ZERO` if `now` is already past the
    /// `At(_)` deadline.
    #[must_use]
    pub fn min_delay(&self, now: SystemTime) -> Duration {
        match self {
            Self::After(duration) => *duration,
            Self::At(when) => when.duration_since(now).unwrap_or(Duration::ZERO),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RetryAdvice {
    pub disposition: RetryDisposition,
    pub retry_hint: Option<RetryHint>,
    pub reason: RetryReason,
    pub throttle_scope: Option<ThrottleScope>,
}

impl RetryAdvice {
    #[must_use]
    pub fn new(
        disposition: RetryDisposition,
        retry_hint: Option<RetryHint>,
        reason: RetryReason,
        throttle_scope: Option<ThrottleScope>,
    ) -> Self {
        Self {
            disposition,
            retry_hint,
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

/// The sealing here is deliberate and has been the state of this file since
/// the original error-model commit, in both directions: the ADVICE structs
/// (`ReconcileAdvice`, [`RetryAdvice`]) carry `#[non_exhaustive]`, while the
/// nested [`ReconcileGuidance`] is a plain public struct with a public field
/// and so is genuinely constructible downstream. An explicit seal on the
/// advice plus a real constructor path for the guidance is the intended
/// posture, not an accident - a finding asserting the reverse arrangement
/// rested on an inverted reading of this code and has been filed twice. Read
/// the derives before re-filing it a third time.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ReconcileAdvice {
    pub reason: ReconcileReason,
    pub guidance: ReconcileGuidance,
    pub retry_hint: Option<RetryHint>,
    pub throttle_scope: Option<ThrottleScope>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum ReconcileReason {
    TransportDropAfterSend,
    PartialCompletionSignal,
    ThrottledMidFlight,
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

/// Provider-documented scope at which a throttle applies. `Mailbox`,
/// `Account`, `Tenant`, and `Provider` map onto sharable
/// [`ThrottleKey`] entries the engine consults across work items.
/// `CurrentOperation` is a per-call hint: the caller delays the single
/// work item inline without entering any shared bucket.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum ThrottleScope {
    CurrentOperation,
    Mailbox,
    Account,
    Tenant,
    Provider,
}

/// Engine bucket key for throttle holds that survive across requests.
/// Constructed by the engine from a `ThrottleScope` plus the relevant
/// identity. `Tenant` and `Provider` cross account boundaries by
/// design - a tenant throttle pauses every account on that tenant.
/// `ThrottleScope::CurrentOperation` never enters a `ThrottleKey`;
/// it is a per-call hint, not a bucket.
///
/// Tenant identity is currently a free-form provider-supplied string;
/// the engine treats it as opaque.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ThrottleKey {
    Mailbox {
        account: AccountId,
        mailbox: MailboxId,
    },
    Account(AccountId),
    Tenant(String),
    Provider(Provider),
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RemediationAction {
    RefreshToken,
    Reauthorize,
    RequestAdminConsent { needed: &'static str },
    UpdateTenantPolicy,
    CheckMailboxLicense,
    RetryLater { retry_hint: Option<RetryHint> },
    FixClientRequest,
    ContactProviderSupport,
}

/// Newtype carrying a terminal-class [`AccountError`]. Constructed only
/// through [`Fatal::try_from`], which rejects any `AccountError` whose
/// `RecoveryClass` is not terminal. Engine boundaries that specifically
/// need "the engine has nothing more to try" (operator-notification
/// queues, permanent-failure dashboards) consume this type so the
/// type system enforces the precondition.
#[derive(Debug, Clone)]
pub struct Fatal(AccountError);

impl Fatal {
    /// Consume the wrapper and return the carried `AccountError`.
    #[must_use]
    pub fn into_inner(self) -> AccountError {
        self.0
    }
}

impl AsRef<AccountError> for Fatal {
    fn as_ref(&self) -> &AccountError {
        &self.0
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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
    throttle_scope: Option<ThrottleScope>,
    idempotency_override: Option<bool>,
) -> RecoveryClass {
    let tx_state = transmission_state(chain).unwrap_or(TransmissionState::Unsent);
    let idempotent = idempotency_override
        .unwrap_or_else(|| operation.is_none_or(AccountOperation::is_idempotent));

    match kind {
        AccountErrorKind::Transport(_) => {
            debug_assert!(
                tx_state != TransmissionState::Acknowledged,
                "transport failures cannot have acknowledged transmission state"
            );
            // Defensive fallback in release builds: a misbehaving
            // producer pushing `Transport + Acknowledged` is treated as
            // `InFlight` (the most conservative classification) rather
            // than crashing the process. `try_build` rejects the same
            // shape at construction so this branch is only reached when
            // a builder somehow bypasses validation.
            //
            // Accepted, with its cost stated so it is not re-filed as a
            // smell: `Transport + Acknowledged` is rejected TWICE. The
            // first rejection is `try_build`, which returns
            // `TransportAcknowledged` before ever calling this function,
            // and since `derive` runs only from inside `try_build` after
            // that branch has already returned `Err`, the check here is
            // dead in normal flow - reachable only by calling the
            // `pub(crate) derive` directly, which the tests do. It is kept
            // as belt-and-suspenders on a classification that must never
            // silently say "acknowledged" about a transport failure.
            //
            // Be aware of what the belt actually does, because it is a real
            // behaviour split rather than a pure assertion: in debug builds
            // the `debug_assert!` PANICS, while in release the same input
            // is silently demoted to `InFlight`. A future producer that
            // manages to mint this shape therefore fails loudly in tests
            // and quietly in production. That asymmetry is deliberate -
            // crashing a consumer's process over a producer bug is worse
            // than conservative misclassification - but anything changing
            // this branch should change both halves together.
            let effective_tx_state = if tx_state == TransmissionState::Acknowledged {
                TransmissionState::InFlight
            } else {
                tx_state
            };
            transient_retry_or_reconcile(
                effective_tx_state,
                idempotent,
                RetryReason::Transport,
                None,
                None,
            )
        }
        AccountErrorKind::Authentication(kind) => derive_auth(*kind, chain),
        AccountErrorKind::Authorization(kind) => derive_access(*kind, chain),
        AccountErrorKind::Server(kind) => {
            derive_server(*kind, tx_state, idempotent, chain, throttle_scope)
        }
        AccountErrorKind::SyncState(kind) => derive_sync_state(*kind, scope, chain),
        AccountErrorKind::ConcurrencyConflict => RecoveryClass::Retry(RetryAdvice {
            disposition: RetryDisposition::AfterStateRefresh,
            retry_hint: None,
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
    _scope: Option<&ErrorScope>,
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
        AccountErrorKind::SyncState(
            SyncStateErrorKind::SchemaIncompatible | SyncStateErrorKind::CursorInvalid,
        ) => None,
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
            | SyncStateErrorKind::OperatorOverrideNeeded
            | SyncStateErrorKind::ScopeRevoked,
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
            Cause::Request(RequestCause::BatchInputInvalid { .. } | RequestCause::BatchInputEmpty),
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
            | (SyncStateErrorKind::ScopeRevoked, StateCause::ScopeRevoked)
    )
}

fn derive_auth(kind: AuthErrorKind, chain: &CauseChain) -> RecoveryClass {
    match kind {
        AuthErrorKind::RefreshTransient => RecoveryClass::Retry(RetryAdvice {
            disposition: RetryDisposition::AfterAuthRefresh,
            retry_hint: server_retry_hint(chain),
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
            retry_hint: None,
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
    throttle_scope: Option<ThrottleScope>,
) -> RecoveryClass {
    let retry_hint = server_retry_hint(chain);
    match kind {
        ServerErrorKind::Unavailable => transient_retry_or_reconcile(
            tx_state,
            idempotent,
            RetryReason::ServerUnavailable,
            retry_hint,
            None,
        ),
        ServerErrorKind::RateLimited => transient_retry_or_reconcile(
            tx_state,
            idempotent,
            RetryReason::RateLimited,
            retry_hint,
            throttle_scope,
        ),
        ServerErrorKind::QuotaExhausted => transient_retry_or_reconcile(
            tx_state,
            idempotent,
            RetryReason::QuotaExhausted,
            retry_hint,
            throttle_scope,
        ),
        ServerErrorKind::Error { status } => {
            let status = status.or_else(|| server_status(chain));
            match status {
                Some(500..=599) => transient_retry_or_reconcile(
                    tx_state,
                    idempotent,
                    RetryReason::ServerUnavailable,
                    retry_hint,
                    None,
                ),
                // No numeric status (IMAP NO/BAD without response code,
                // SMTP transport shutdown without final reply) combined
                // with an in-flight attempt: idempotent ops retry,
                // non-idempotent ops reconcile. Per the convergence
                // recovery table.
                None if tx_state == TransmissionState::InFlight => transient_retry_or_reconcile(
                    tx_state,
                    idempotent,
                    RetryReason::ServerUnavailable,
                    retry_hint,
                    None,
                ),
                _ => RecoveryClass::ProviderRefused,
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
        // CursorInvalid without a cursor scope is rejected at build
        // time (see `AccountErrorBuildError::CursorInvalidWithoutScope`),
        // so this branch can safely require the scope. The fallback
        // remains for the central recovery table's deterministic
        // behavior; in practice `try_build` prevents it.
        SyncStateErrorKind::CursorInvalid => match cursor_scope(scope) {
            Some(scope) => RecoveryClass::Engine(EngineDirective::RestartScope(scope)),
            None => RecoveryClass::Engine(EngineDirective::RestartAccount),
        },
        SyncStateErrorKind::StrategyFailure => RecoveryClass::Engine(
            EngineDirective::DowngradeStrategy(strategy_downgrade(chain)),
        ),
        SyncStateErrorKind::ScopeCapabilityLost => match cursor_scope(scope) {
            Some(scope) => {
                RecoveryClass::Engine(EngineDirective::DowngradeCapabilityForScope(scope))
            }
            None => RecoveryClass::Engine(EngineDirective::RestartAccount),
        },
        // A revoked single shared folder quarantines that scope without
        // escalating account-wide. `ScopeRevoked` requires a cursor scope
        // to be meaningful; rather than add a `try_build` invariant, the
        // scope-less case falls back to a full reopen (matching the
        // `ScopeCapabilityLost` shape above). The IMAP producer always
        // threads the scope, so the fallback is unreachable in practice.
        SyncStateErrorKind::ScopeRevoked => match cursor_scope(scope) {
            Some(scope) => RecoveryClass::Engine(EngineDirective::DisableScope(scope)),
            None => RecoveryClass::Engine(EngineDirective::RestartAccount),
        },
        SyncStateErrorKind::SchemaIncompatible => {
            RecoveryClass::Engine(EngineDirective::SchemaIncompatible)
        }
        // Capability shifts no longer have a dedicated `EngineDirective`
        // variant. `StateCause::CapabilityChanged { delta }` retains the
        // (optional) delta payload for forensic exports; recovery maps
        // the shift to a full account reopen so discovery re-runs.
        SyncStateErrorKind::CapabilityChanged => {
            RecoveryClass::Engine(EngineDirective::RestartAccount)
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

fn strategy_downgrade(chain: &CauseChain) -> StrategyDowngrade {
    chain
        .iter()
        .find_map(|cause| match cause {
            Cause::State(StateCause::StrategyFailure { downgrade }) => Some(*downgrade),
            _ => None,
        })
        .unwrap_or(StrategyDowngrade::QResyncToCondstore)
}

fn operator_reason(chain: &CauseChain) -> Option<&str> {
    chain.iter().find_map(|cause| match cause {
        Cause::State(StateCause::OperatorOverrideNeeded { reason }) => Some(reason.as_str()),
        _ => None,
    })
}

fn derive_protocol(kind: ProtocolErrorKind, idempotent: bool) -> RecoveryClass {
    match kind {
        ProtocolErrorKind::ParseFailed
        | ProtocolErrorKind::MissingField
        | ProtocolErrorKind::ContractViolation => RecoveryClass::ProviderContractViolation,
        ProtocolErrorKind::PartialResponse if idempotent => RecoveryClass::Retry(RetryAdvice {
            disposition: RetryDisposition::SameRequest,
            retry_hint: None,
            reason: RetryReason::Transport,
            throttle_scope: None,
        }),
        // Partial-response signal on a non-idempotent op: the server
        // told us it committed something we cannot prove. Reconcile
        // by probing the target and deduping by client id; do not
        // blindly retry.
        ProtocolErrorKind::PartialResponse => RecoveryClass::Reconcile(ReconcileAdvice {
            reason: ReconcileReason::PartialCompletionSignal,
            guidance: ReconcileGuidance {
                actions: vec![
                    ReconcileAction::CheckTarget,
                    ReconcileAction::DedupeByClientId,
                ],
            },
            retry_hint: None,
            throttle_scope: None,
        }),
        ProtocolErrorKind::Unknown => RecoveryClass::UnknownPermanent,
    }
}

fn transient_retry_or_reconcile(
    tx_state: TransmissionState,
    idempotent: bool,
    reason: RetryReason,
    retry_hint: Option<RetryHint>,
    throttle_scope: Option<ThrottleScope>,
) -> RecoveryClass {
    match tx_state {
        // Only `InFlight` (transport dropped after the request went out, no
        // response) on a non-idempotent op reconciles: the side effect may
        // have landed but we have no acknowledgement, so a blind same-request
        // retry could double-apply. `Acknowledged` is different: the server
        // returned a complete response, so an ack-then-transient-fail is a
        // commit-rejection (nothing committed), safe to retry the same
        // request. This matches reference/error-model.md: `Unsent`/
        // `Acknowledged`, or `InFlight`+idempotent -> `Retry(SameRequest)`;
        // `InFlight`+non-idempotent -> `Reconcile`.
        TransmissionState::InFlight if !idempotent => RecoveryClass::Reconcile(ReconcileAdvice {
            reason: match reason {
                RetryReason::RateLimited | RetryReason::QuotaExhausted => {
                    ReconcileReason::ThrottledMidFlight
                }
                _ => ReconcileReason::TransportDropAfterSend,
            },
            guidance: ReconcileGuidance {
                actions: vec![ReconcileAction::CheckTarget],
            },
            retry_hint,
            throttle_scope,
        }),
        TransmissionState::Unsent
        | TransmissionState::InFlight
        | TransmissionState::Acknowledged => RecoveryClass::Retry(RetryAdvice {
            disposition: RetryDisposition::SameRequest,
            retry_hint,
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

fn server_status(chain: &CauseChain) -> Option<u16> {
    chain.iter().find_map(|cause| match cause {
        Cause::Server(ServerCause::Error { status }) => *status,
        _ => None,
    })
}

pub(crate) fn cursor_scope(scope: Option<&ErrorScope>) -> Option<CursorScope> {
    match scope {
        Some(ErrorScope::Cursor(scope)) => Some(scope.clone()),
        _ => None,
    }
}

pub(crate) fn server_retry_hint(chain: &CauseChain) -> Option<RetryHint> {
    chain.iter().find_map(|cause| match cause {
        Cause::Server(cause) => cause.retry_hint(),
        _ => None,
    })
}

fn retry_later(recovery: &RecoveryClass) -> Option<RemediationAction> {
    match recovery {
        RecoveryClass::Retry(advice) => Some(RemediationAction::RetryLater {
            retry_hint: advice.retry_hint,
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
        // SyncChanges is a read; an in-flight transport drop is safely
        // re-driven. (UpdateFlags used to stand in for "idempotent" here,
        // but flag writes are now correctly in the non-idempotent set.)
        let recovery = derive(
            &AccountErrorKind::Transport(super::super::kind::TransportErrorKind::Network),
            None,
            Some(AccountOperation::SyncChanges),
            &transport_chain(TransmissionState::InFlight),
            None,
            None,
        );

        assert!(recovery.is_retryable());
    }

    /// `Transport + Acknowledged` is a producer-bug shape. In debug
    /// builds, `derive` panics via `debug_assert!`. In release builds,
    /// it falls back to treating the attempt as `InFlight` (the most
    /// conservative defensible classify) so the process degrades rather
    /// than crashes. The same shape is rejected at construction time by
    /// `AccountErrorBuilder::try_build`.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "transport failures cannot have acknowledged transmission state")]
    fn acknowledged_transport_failure_panics_in_debug() {
        let _ = derive(
            &AccountErrorKind::Transport(super::super::kind::TransportErrorKind::Network),
            None,
            Some(AccountOperation::UpdateFlags),
            &transport_chain(TransmissionState::Acknowledged),
            None,
            None,
        );
    }

    #[test]
    #[cfg(not(debug_assertions))]
    fn acknowledged_transport_failure_falls_back_to_inflight_in_release() {
        let recovery = derive(
            &AccountErrorKind::Transport(super::super::kind::TransportErrorKind::Network),
            None,
            Some(AccountOperation::Send),
            &transport_chain(TransmissionState::Acknowledged),
            None,
            None,
        );

        // Non-idempotent + InFlight defensive fallback -> Reconcile.
        assert!(recovery.requires_reconciliation());
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
        );

        assert!(recovery.requires_reconciliation());
    }

    #[test]
    fn rate_limit_carries_retry_hint_and_throttle_scope() {
        let hint = RetryHint::After(Duration::from_secs(5));
        let recovery = derive(
            &AccountErrorKind::Server(ServerErrorKind::RateLimited),
            None,
            Some(AccountOperation::Search),
            &chain(Cause::Server(ServerCause::RateLimited {
                retry_hint: Some(hint),
            })),
            Some(ThrottleScope::Tenant),
            None,
        );

        assert_eq!(
            recovery,
            RecoveryClass::Retry(RetryAdvice {
                disposition: RetryDisposition::SameRequest,
                retry_hint: Some(hint),
                reason: RetryReason::RateLimited,
                throttle_scope: Some(ThrottleScope::Tenant),
            })
        );
    }

    #[test]
    fn quota_inflight_non_idempotent_reconciles() {
        let hint = RetryHint::After(Duration::from_secs(45));
        let recovery = derive(
            &AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
            None,
            Some(AccountOperation::Send),
            &CauseChain::new(vec![
                Cause::Server(ServerCause::QuotaExhausted {
                    retry_hint: Some(hint),
                }),
                Cause::Attempt(AttemptCause {
                    transmission_state: TransmissionState::InFlight,
                }),
            ]),
            Some(ThrottleScope::Account),
            None,
        );

        assert!(matches!(
            recovery,
            RecoveryClass::Reconcile(ReconcileAdvice {
                reason: ReconcileReason::ThrottledMidFlight,
                retry_hint: Some(actual_hint),
                throttle_scope: Some(ThrottleScope::Account),
                ..
            })
            if actual_hint == hint
        ));
    }

    #[test]
    fn quota_acknowledged_non_idempotent_retries_same_request() {
        // Per reference/error-model.md, a `Server(QuotaExhausted)` carrying an
        // `Attempt(Acknowledged)` retries the same request even for a
        // non-idempotent send: an ack-then-transient-fail is a commit
        // rejection (the server returned a complete response, nothing
        // committed), so it is safe to re-send. Only `InFlight` (no
        // acknowledgement) + non-idempotent reconciles.
        let recovery = derive(
            &AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
            None,
            Some(AccountOperation::Send),
            &CauseChain::new(vec![
                Cause::Server(ServerCause::QuotaExhausted { retry_hint: None }),
                Cause::Attempt(AttemptCause {
                    transmission_state: TransmissionState::Acknowledged,
                }),
            ]),
            Some(ThrottleScope::Account),
            None,
        );

        assert!(matches!(
            recovery,
            RecoveryClass::Retry(RetryAdvice {
                disposition: RetryDisposition::SameRequest,
                ..
            })
        ));
    }

    #[test]
    fn inflight_non_idempotent_send_reconciles() {
        // The reconcile case the helper does cover: a transport drop with no
        // acknowledgement (`InFlight`) on a non-idempotent send may have
        // landed, so probe the target instead of blind-retrying.
        let recovery = derive(
            &AccountErrorKind::Server(ServerErrorKind::Unavailable),
            None,
            Some(AccountOperation::Send),
            &CauseChain::new(vec![
                Cause::Server(ServerCause::Unavailable { retry_hint: None }),
                Cause::Attempt(AttemptCause {
                    transmission_state: TransmissionState::InFlight,
                }),
            ]),
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
    fn rate_limited_acknowledged_idempotent_still_retries() {
        // An idempotent read that the server acknowledged then rate-limited
        // is still safely retryable.
        let recovery = derive(
            &AccountErrorKind::Server(ServerErrorKind::RateLimited),
            None,
            Some(AccountOperation::SyncChanges),
            &CauseChain::new(vec![
                Cause::Server(ServerCause::RateLimited { retry_hint: None }),
                Cause::Attempt(AttemptCause {
                    transmission_state: TransmissionState::Acknowledged,
                }),
            ]),
            None,
            None,
        );

        assert!(recovery.is_retryable());
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
        );
        let refused = derive(
            &AccountErrorKind::Server(ServerErrorKind::Error { status: Some(451) }),
            None,
            Some(AccountOperation::SyncChanges),
            &chain(Cause::Server(ServerCause::Error { status: Some(451) })),
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
        );
        let account = derive(
            &AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            None,
            Some(AccountOperation::SyncChanges),
            &chain(Cause::State(StateCause::CursorInvalid)),
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

    /// Every `Reconcile` the mapping can produce carries `CheckTarget`.
    ///
    /// `bifrost-sync`'s mutation loop queues `PendingReadback` for every
    /// unresolved id on ANY reconcile advice, consulting the action list
    /// only to decide whether to ALSO warn about dedupe. That is correct
    /// exactly as long as this holds: the read-back guard IS the target
    /// probe, so a reconcile that did not ask for `CheckTarget` would be
    /// getting a probe it never requested.
    ///
    /// Producers cannot set `RecoveryClass` directly - `try_build` always
    /// routes through `derive` - so pinning both arms here pins the whole
    /// producible space. A new `Reconcile` arm that omits `CheckTarget`
    /// breaks this test rather than silently changing engine behaviour.
    #[test]
    fn every_producible_reconcile_requests_check_target() {
        // Arm 1: transport drop after send, non-idempotent op.
        let transport_drop = derive(
            &AccountErrorKind::Transport(super::super::kind::TransportErrorKind::Network),
            None,
            Some(AccountOperation::Send),
            &transport_chain(TransmissionState::InFlight),
            None,
            None,
        );
        // Arm 2: partial-response signal, non-idempotent op. The only arm
        // that also asks for dedupe-by-client-id.
        let partial = derive(
            &AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
            None,
            Some(AccountOperation::Send),
            &chain(Cause::Wire(crate::error::WireCause::Jmap(
                crate::error::JmapMethod::ServerPartialFail,
            ))),
            None,
            None,
        );

        for recovery in [&transport_drop, &partial] {
            let RecoveryClass::Reconcile(advice) = recovery else {
                panic!("expected Reconcile, got {recovery:?}");
            };
            assert!(
                advice
                    .guidance
                    .actions
                    .contains(&ReconcileAction::CheckTarget),
                "reconcile advice must request CheckTarget: {advice:?}"
            );
        }

        // The dedupe action is reachable, and only alongside CheckTarget -
        // so the engine's warn-and-still-queue behaviour is right, not an
        // over-reach.
        let RecoveryClass::Reconcile(advice) = &partial else {
            panic!("expected Reconcile");
        };
        assert!(
            advice
                .guidance
                .actions
                .contains(&ReconcileAction::DedupeByClientId),
            "the partial-response arm is what makes DedupeByClientId reachable"
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
        );

        assert!(transient.is_retryable());
        assert_eq!(permanent, RecoveryClass::ProviderRefused);
    }

    /// Catalog every `RecoveryClass` variant exactly once. Used by the
    /// helper-exclusivity and `Fatal::try_from` round-trip tests below
    /// so adding a new `RecoveryClass` variant fails to compile here
    /// rather than silently slipping past the invariants.
    fn every_recovery_variant() -> Vec<RecoveryClass> {
        vec![
            RecoveryClass::Retry(RetryAdvice {
                disposition: RetryDisposition::SameRequest,
                retry_hint: None,
                reason: RetryReason::Transport,
                throttle_scope: None,
            }),
            RecoveryClass::Reconcile(ReconcileAdvice {
                reason: ReconcileReason::TransportDropAfterSend,
                guidance: ReconcileGuidance {
                    actions: vec![ReconcileAction::CheckTarget],
                },
                retry_hint: None,
                throttle_scope: None,
            }),
            RecoveryClass::Engine(EngineDirective::RestartAccount),
            RecoveryClass::AuthLost,
            RecoveryClass::NeedsAdminConsent { needed: "scope.x" },
            RecoveryClass::NeedsPolicyChange,
            RecoveryClass::NoPermission { resource: None },
            RecoveryClass::Unsupported(AccountOperation::Send),
            RecoveryClass::ClientBug,
            RecoveryClass::ProviderContractViolation,
            RecoveryClass::ProviderRefused,
            RecoveryClass::UnknownPermanent,
        ]
    }

    /// types-D11: the four `RecoveryClass` helpers are mutually
    /// exclusive and exhaustive over every variant.
    #[test]
    fn recovery_helpers_are_mutually_exclusive_and_exhaustive() {
        for variant in every_recovery_variant() {
            let flags = [
                variant.is_retryable(),
                variant.requires_reconciliation(),
                variant.requires_engine_action(),
                variant.is_terminal(),
            ];
            let true_count = flags.iter().filter(|f| **f).count();
            assert_eq!(
                true_count, 1,
                "RecoveryClass {variant:?} should match exactly one helper, matched {true_count}"
            );
        }
    }

    /// types-D12: build a representative `AccountError` per
    /// `RecoveryClass` shape and verify `Fatal::try_from` succeeds iff
    /// the variant is terminal. We construct each error through the
    /// builder using a kind/cause pair that produces the target
    /// recovery class.
    #[test]
    fn fatal_try_from_round_trips_terminal_only() {
        use crate::CursorScope;
        use crate::error::{AccountErrorBuilder, AccountErrorKind, RequestErrorKind};

        let terminal_cases: Vec<(&str, AccountError)> = vec![
            (
                "AuthLost",
                build(
                    AccountErrorKind::Authentication(AuthErrorKind::Expired),
                    Cause::Auth(AuthCause::Expired),
                ),
            ),
            (
                "NeedsAdminConsent",
                build(
                    AccountErrorKind::Authorization(AccessErrorKind::AdminConsentRequired),
                    Cause::Access(AccessCause::AdminConsentRequired { needed: "x" }),
                ),
            ),
            (
                "NeedsPolicyChange",
                build(
                    AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked),
                    Cause::Access(AccessCause::PolicyBlocked),
                ),
            ),
            (
                "NoPermission",
                build(
                    AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
                    Cause::Access(AccessCause::PermissionDenied { resource: None }),
                ),
            ),
            (
                "Unsupported",
                AccountErrorBuilder::new(
                    AccountErrorKind::Unsupported(AccountOperation::Send),
                    Cause::Request(RequestCause::Unsupported {
                        operation: AccountOperation::Send,
                    }),
                )
                .try_build()
                .expect("valid"),
            ),
            (
                "ClientBug",
                AccountErrorBuilder::new(
                    AccountErrorKind::Request(RequestErrorKind::Malformed),
                    Cause::Request(RequestCause::Malformed {
                        detail: crate::error::DiagnosticText::support_only("x"),
                    }),
                )
                .try_build()
                .expect("valid"),
            ),
            (
                "ProviderContractViolation",
                build(
                    AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
                    Cause::Wire(crate::error::WireCause::Jmap(
                        crate::error::JmapMethod::NotJson,
                    )),
                ),
            ),
            (
                "ProviderRefused",
                build(
                    AccountErrorKind::Server(ServerErrorKind::Error { status: Some(451) }),
                    Cause::Server(ServerCause::Error { status: Some(451) }),
                ),
            ),
            (
                "UnknownPermanent",
                build(
                    AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
                    Cause::Wire(crate::error::WireCause::Jmap(
                        crate::error::JmapMethod::Unknown { code: "x".into() },
                    )),
                ),
            ),
        ];
        let non_terminal_cases: Vec<(&str, AccountError)> = vec![
            (
                "Retry",
                AccountErrorBuilder::new(
                    AccountErrorKind::Transport(super::super::kind::TransportErrorKind::Network),
                    Cause::Transport(TransportCause {
                        kind: TransportKind::Network,
                        message: None,
                    }),
                )
                .push_cause(Cause::Attempt(AttemptCause {
                    transmission_state: TransmissionState::Unsent,
                }))
                .try_build()
                .expect("valid"),
            ),
            (
                "Reconcile",
                AccountErrorBuilder::new(
                    AccountErrorKind::Transport(super::super::kind::TransportErrorKind::Network),
                    Cause::Transport(TransportCause {
                        kind: TransportKind::Network,
                        message: None,
                    }),
                )
                .operation(AccountOperation::Send)
                .push_cause(Cause::Attempt(AttemptCause {
                    transmission_state: TransmissionState::InFlight,
                }))
                .try_build()
                .expect("valid"),
            ),
            (
                "Engine",
                build(
                    AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
                    Cause::State(StateCause::SchemaIncompatible),
                ),
            ),
        ];

        for (name, error) in terminal_cases {
            assert!(error.recovery().is_terminal(), "{name} should be terminal");
            assert!(
                Fatal::try_from(error).is_ok(),
                "{name} should convert to Fatal"
            );
        }
        for (name, error) in non_terminal_cases {
            assert!(
                !error.recovery().is_terminal(),
                "{name} should be non-terminal"
            );
            assert!(
                Fatal::try_from(error).is_err(),
                "{name} should not convert to Fatal"
            );
        }

        // Silence unused: helper for cursor-scoped engine variants.
        let _scoped = build_with_scope(
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
            ErrorScope::Cursor(CursorScope::Account),
        );
    }

    #[test]
    fn scope_revoked_derives_disable_scope() {
        let scope = ErrorScope::Cursor(CursorScope::Folder(crate::FolderId("Shared/alice".into())));
        let recovery = derive(
            &AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked),
            Some(&scope),
            Some(AccountOperation::SyncChanges),
            &chain(Cause::State(StateCause::ScopeRevoked)),
            None,
            None,
        );

        assert_eq!(
            recovery,
            RecoveryClass::Engine(EngineDirective::DisableScope(CursorScope::Folder(
                crate::FolderId("Shared/alice".into())
            )))
        );
    }

    #[test]
    fn scope_revoked_without_scope_falls_back_to_restart_account() {
        let recovery = derive(
            &AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked),
            None,
            Some(AccountOperation::SyncChanges),
            &chain(Cause::State(StateCause::ScopeRevoked)),
            None,
            None,
        );

        assert_eq!(
            recovery,
            RecoveryClass::Engine(EngineDirective::RestartAccount)
        );
    }

    #[test]
    fn disable_scope_is_engine_action_not_terminal() {
        let recovery = RecoveryClass::Engine(EngineDirective::DisableScope(CursorScope::Folder(
            crate::FolderId("Shared/alice".into()),
        )));
        assert!(recovery.requires_engine_action());
        assert!(!recovery.is_terminal());
    }

    fn build(kind: AccountErrorKind, primary_cause: Cause) -> AccountError {
        crate::error::AccountErrorBuilder::new(kind, primary_cause)
            .try_build()
            .expect("valid account error classification")
    }

    fn build_with_scope(
        kind: AccountErrorKind,
        primary_cause: Cause,
        scope: ErrorScope,
    ) -> AccountError {
        crate::error::AccountErrorBuilder::new(kind, primary_cause)
            .scope(scope)
            .try_build()
            .expect("valid account error classification")
    }
}
