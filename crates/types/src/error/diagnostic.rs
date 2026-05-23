use serde::Serialize;

use super::cause::{CauseSummary, TransmissionState};
use super::kind::AccountErrorKind;
use super::recovery::{
    ReconcileAction, ReconcileReason, RetryDisposition, RetryReason, ThrottleScope,
};
use super::scope::{AccountOperation, ErrorScope, Protocol, Provider};

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct DiagnosticInfo {
    pub request_id: Option<String>,
    pub trace_id: Option<String>,
    pub status: Option<u16>,
    pub native_code: Option<String>,
    pub text: Vec<DiagnosticText>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DiagnosticText {
    pub value: String,
    pub visibility: DetailVisibility,
}

impl DiagnosticText {
    #[must_use]
    pub fn user_safe(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            visibility: DetailVisibility::UserSafe,
        }
    }

    #[must_use]
    pub fn support_only(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            visibility: DetailVisibility::SupportOnly,
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.value.as_str()
    }

    #[must_use]
    pub fn is_user_safe(&self) -> bool {
        self.visibility == DetailVisibility::UserSafe
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum DetailVisibility {
    UserSafe,
    SupportOnly,
}

#[derive(Clone, Debug, Serialize)]
pub struct TelemetryView<'a> {
    pub kind_discriminant: &'static str,
    pub message_key: &'static str,
    pub recovery_discriminant: &'static str,
    pub provider: Option<Provider>,
    pub protocol: Option<Protocol>,
    pub status: Option<u16>,
    pub native_code: Option<&'a str>,
    pub request_id: Option<&'a str>,
    pub trace_id: Option<&'a str>,
    pub retry_disposition: Option<RetryDisposition>,
    pub retry_reason: Option<RetryReason>,
    pub throttle_scope: Option<ThrottleScope>,
    pub reconcile_reason: Option<ReconcileReason>,
    pub reconcile_actions: &'a [ReconcileAction],
    pub transmission_state: Option<TransmissionState>,
    pub operation: Option<AccountOperation>,
}

pub type SupportExportMinimal<'a> = TelemetryView<'a>;

#[derive(Clone, Debug, Serialize)]
pub struct SupportExportConsented<'a> {
    pub telemetry: TelemetryView<'a>,
    pub user_safe_text: Vec<&'a str>,
    pub support_text: Vec<&'a str>,
    pub scope: Option<&'a ErrorScope>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SupportExportInternal<'a> {
    pub consented: SupportExportConsented<'a>,
    pub chain: Vec<CauseSummary<'a>>,
}

pub(crate) fn account_kind_discriminant(kind: &AccountErrorKind) -> &'static str {
    match kind {
        AccountErrorKind::Transport(_) => "transport",
        AccountErrorKind::Authentication(_) => "authentication",
        AccountErrorKind::Authorization(_) => "authorization",
        AccountErrorKind::Server(_) => "server",
        AccountErrorKind::SyncState(_) => "sync_state",
        AccountErrorKind::ConcurrencyConflict => "concurrency_conflict",
        AccountErrorKind::Request(_) => "request",
        AccountErrorKind::NotFound(_) => "not_found",
        AccountErrorKind::Unsupported(_) => "unsupported",
        AccountErrorKind::Protocol(_) => "protocol",
    }
}
