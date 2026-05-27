use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use super::cause::CauseChain;
use super::diagnostic::{
    DetailVisibility, DiagnosticInfo, SupportExportConsented, SupportExportInternal,
    SupportExportMinimal, TelemetryView, account_kind_discriminant,
};
use super::kind::AccountErrorKind;
use super::recovery::{
    ReconcileAction, ReconcileReason, RecoveryClass, RemediationAction, RetryDisposition,
    RetryReason, ThrottleScope,
};
use super::scope::{AccountOperation, ErrorScope, Protocol, Provider};

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct AccountError {
    inner: Arc<AccountErrorInner>,
}

#[derive(Clone, Debug)]
struct AccountErrorInner {
    kind: AccountErrorKind,
    recovery: RecoveryClass,
    remediation: Option<RemediationAction>,
    scope: Option<ErrorScope>,
    operation: Option<AccountOperation>,
    provider: Option<Provider>,
    protocol: Option<Protocol>,
    diagnostics: DiagnosticInfo,
    chain: CauseChain,
    message_key: &'static str,
}

impl AccountError {
    pub(crate) fn from_parts(parts: AccountErrorParts) -> Self {
        Self {
            inner: Arc::new(AccountErrorInner {
                kind: parts.kind,
                recovery: parts.recovery,
                remediation: parts.remediation,
                scope: parts.scope,
                operation: parts.operation,
                provider: parts.provider,
                protocol: parts.protocol,
                diagnostics: parts.diagnostics,
                chain: parts.chain,
                message_key: parts.message_key,
            }),
        }
    }

    #[must_use]
    pub fn kind(&self) -> &AccountErrorKind {
        &self.inner.kind
    }

    #[must_use]
    pub fn recovery(&self) -> &RecoveryClass {
        &self.inner.recovery
    }

    #[must_use]
    pub fn suggested_remediation(&self) -> Option<&RemediationAction> {
        self.inner.remediation.as_ref()
    }

    #[must_use]
    pub fn scope(&self) -> Option<&ErrorScope> {
        self.inner.scope.as_ref()
    }

    #[must_use]
    pub fn operation(&self) -> Option<AccountOperation> {
        self.inner.operation
    }

    #[must_use]
    pub fn provider(&self) -> Option<Provider> {
        self.inner.provider
    }

    #[must_use]
    pub fn protocol(&self) -> Option<Protocol> {
        self.inner.protocol
    }

    #[must_use]
    pub fn message_key(&self) -> &'static str {
        self.inner.message_key
    }

    pub fn user_safe_text(&self) -> impl Iterator<Item = &str> {
        self.inner
            .diagnostics
            .text
            .iter()
            .filter(|text| text.visibility == DetailVisibility::UserSafe)
            .map(|text| text.value.as_str())
    }

    #[must_use]
    pub fn telemetry_fields(&self) -> TelemetryView<'_> {
        TelemetryView::from_account_error(self)
    }

    #[must_use]
    pub fn support_minimal(&self) -> SupportExportMinimal<'_> {
        self.telemetry_fields()
    }

    #[must_use]
    pub fn support_consented(&self) -> SupportExportConsented<'_> {
        let mut user_safe_text = Vec::new();
        let mut support_text = Vec::new();
        for text in &self.inner.diagnostics.text {
            match text.visibility {
                DetailVisibility::UserSafe => user_safe_text.push(text.value.as_str()),
                DetailVisibility::SupportOnly => support_text.push(text.value.as_str()),
            }
        }

        SupportExportConsented {
            telemetry: self.telemetry_fields(),
            user_safe_text,
            support_text,
            scope: self.inner.scope.as_ref(),
        }
    }

    #[must_use]
    pub fn support_internal(&self) -> SupportExportInternal<'_> {
        SupportExportInternal {
            consented: self.support_consented(),
            chain: self
                .inner
                .chain
                .iter()
                .map(super::cause::Cause::summary)
                .collect(),
        }
    }

    #[must_use]
    pub fn chain(&self) -> &CauseChain {
        &self.inner.chain
    }

    /// Consume this error and return a builder pre-populated with its
    /// fields and chain.
    ///
    /// `into_builder` is for *decoration*, not reclassification: the
    /// returned builder carries the original `kind` and primary
    /// `Cause`, and exposes no kind-changing path. `push_cause` only
    /// appends secondary evidence to the chain. If a caller needs a
    /// different primary `kind` or a different outermost `Cause`, it
    /// must construct a fresh builder via `AccountErrorBuilder::new`.
    ///
    /// Derived fields (`recovery`, `remediation`, `message_key`) are
    /// recomputed on `try_build()`. Builder-only overrides
    /// (`idempotency_override`, `throttle_scope`) are not preserved by
    /// the round-trip and must be reapplied if the caller needs them.
    /// Retry hints set on a `ServerCause` survive structurally through
    /// the chain.
    #[must_use]
    pub fn into_builder(self) -> super::builder::AccountErrorBuilder {
        let inner = Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone());
        let mut causes = inner.chain.into_vec();
        let primary_cause = causes.remove(0);
        super::builder::AccountErrorBuilder::from_rebuild(super::builder::RebuildParts {
            kind: inner.kind,
            primary_cause,
            chain_extras: causes,
            scope: inner.scope,
            operation: inner.operation,
            provider: inner.provider,
            protocol: inner.protocol,
            diagnostics: inner.diagnostics,
        })
    }
}

impl fmt::Display for AccountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({:?})", self.inner.message_key, self.inner.kind)
    }
}

impl StdError for AccountError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.inner.chain.outermost())
    }
}

pub(crate) struct AccountErrorParts {
    pub kind: AccountErrorKind,
    pub recovery: RecoveryClass,
    pub remediation: Option<RemediationAction>,
    pub scope: Option<ErrorScope>,
    pub operation: Option<AccountOperation>,
    pub provider: Option<Provider>,
    pub protocol: Option<Protocol>,
    pub diagnostics: DiagnosticInfo,
    pub chain: CauseChain,
    pub message_key: &'static str,
}

impl<'a> TelemetryView<'a> {
    pub(crate) fn from_account_error(error: &'a AccountError) -> Self {
        const EMPTY_ACTIONS: &[ReconcileAction] = &[];

        let (retry_disposition, retry_reason, throttle_scope, reconcile_reason, reconcile_actions) =
            recovery_fields(error.recovery());

        Self {
            kind_discriminant: account_kind_discriminant(error.kind()),
            message_key: error.message_key(),
            recovery_discriminant: recovery_discriminant(error.recovery()),
            provider: error.provider(),
            protocol: error.protocol(),
            status: error.inner.diagnostics.status,
            native_code: error.inner.diagnostics.native_code.as_deref(),
            request_id: error.inner.diagnostics.request_id.as_deref(),
            trace_id: error.inner.diagnostics.trace_id.as_deref(),
            retry_disposition,
            retry_reason,
            throttle_scope,
            reconcile_reason,
            reconcile_actions: reconcile_actions.unwrap_or(EMPTY_ACTIONS),
            transmission_state: error.chain().iter().find_map(|cause| {
                if let super::cause::Cause::Attempt(cause) = cause {
                    Some(cause.transmission_state)
                } else {
                    None
                }
            }),
            operation: error.operation(),
        }
    }
}

type RecoveryFields<'a> = (
    Option<RetryDisposition>,
    Option<RetryReason>,
    Option<ThrottleScope>,
    Option<ReconcileReason>,
    Option<&'a [ReconcileAction]>,
);

fn recovery_fields(recovery: &RecoveryClass) -> RecoveryFields<'_> {
    match recovery {
        RecoveryClass::Retry(advice) => (
            Some(advice.disposition),
            Some(advice.reason),
            advice.throttle_scope,
            None,
            None,
        ),
        RecoveryClass::Reconcile(advice) => (
            None,
            None,
            None,
            Some(advice.reason),
            Some(advice.guidance.actions.as_slice()),
        ),
        RecoveryClass::Engine(_)
        | RecoveryClass::AuthLost
        | RecoveryClass::NeedsAdminConsent { .. }
        | RecoveryClass::NeedsPolicyChange
        | RecoveryClass::NoPermission { .. }
        | RecoveryClass::Unsupported(_)
        | RecoveryClass::ClientBug
        | RecoveryClass::ProviderContractViolation
        | RecoveryClass::ProviderRefused
        | RecoveryClass::UnknownPermanent => (None, None, None, None, None),
    }
}

fn recovery_discriminant(recovery: &RecoveryClass) -> &'static str {
    match recovery {
        RecoveryClass::Retry(_) => "retry",
        RecoveryClass::Reconcile(_) => "reconcile",
        RecoveryClass::Engine(_) => "engine",
        RecoveryClass::AuthLost => "auth_lost",
        RecoveryClass::NeedsAdminConsent { .. } => "needs_admin_consent",
        RecoveryClass::NeedsPolicyChange => "needs_policy_change",
        RecoveryClass::NoPermission { .. } => "no_permission",
        RecoveryClass::Unsupported(_) => "unsupported",
        RecoveryClass::ClientBug => "client_bug",
        RecoveryClass::ProviderContractViolation => "provider_contract_violation",
        RecoveryClass::ProviderRefused => "provider_refused",
        RecoveryClass::UnknownPermanent => "unknown_permanent",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{
        AccountErrorBuilder, AccountErrorKind, Cause, DiagnosticText, RequestCause,
        RequestErrorKind,
    };

    fn request_error() -> AccountError {
        AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("raw provider body"),
            }),
        )
        .text(DiagnosticText::user_safe("Check the request."))
        .text(DiagnosticText::support_only("raw provider body"))
        .try_build()
        .expect("valid account error classification")
    }

    #[test]
    fn user_safe_text_filters_support_only_text() {
        let error = request_error();
        let text = error.user_safe_text().collect::<Vec<_>>();

        assert_eq!(text, ["Check the request."]);
    }

    #[test]
    fn telemetry_has_no_free_form_text() {
        let error = request_error();
        let telemetry = error.telemetry_fields();

        assert_eq!(telemetry.message_key, "request.malformed");
        assert_eq!(telemetry.kind_discriminant, "request");
    }

    #[test]
    fn support_exports_have_expected_consent_tiers() {
        let error = request_error();
        let consented = error.support_consented();
        let internal = error.support_internal();

        assert_eq!(consented.user_safe_text, ["Check the request."]);
        assert_eq!(consented.support_text, ["raw provider body"]);
        assert_eq!(internal.chain.len(), 1);
    }

    #[test]
    fn into_builder_round_trips_preserves_observable_fields() {
        let original = request_error();
        let kind_before = original.kind().clone();
        let message_key_before = original.message_key();
        let chain_len_before = original.chain().iter().count();

        let rebuilt = original
            .into_builder()
            .try_build()
            .expect("valid account error classification");

        assert_eq!(rebuilt.kind(), &kind_before);
        assert_eq!(rebuilt.message_key(), message_key_before);
        assert_eq!(rebuilt.chain().iter().count(), chain_len_before);
        let text = rebuilt.user_safe_text().collect::<Vec<_>>();
        assert_eq!(text, ["Check the request."]);
    }

    #[test]
    fn into_builder_push_cause_appends_secondary_evidence() {
        use crate::error::{AttemptCause, Cause, TransmissionState};

        let original = request_error();
        let chain_len_before = original.chain().iter().count();

        let rebuilt = original
            .into_builder()
            .push_cause(Cause::Attempt(AttemptCause {
                transmission_state: TransmissionState::Unsent,
            }))
            .try_build()
            .expect("valid account error classification");

        assert_eq!(rebuilt.chain().iter().count(), chain_len_before + 1);
        let last_cause_is_attempt = matches!(
            rebuilt.chain().iter().last(),
            Some(Cause::Attempt(AttemptCause {
                transmission_state: TransmissionState::Unsent,
            }))
        );
        assert!(last_cause_is_attempt);
    }
}
