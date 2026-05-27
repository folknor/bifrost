use std::fmt;

use super::account_error::{AccountError, AccountErrorParts};
use super::cause::{Cause, CauseChain};
use super::diagnostic::{DiagnosticInfo, DiagnosticText};
use super::kind::{AccountErrorKind, ServerErrorKind, SyncStateErrorKind};
use super::message_key;
use super::recovery::{self, ThrottleScope};
use super::scope::{AccountOperation, ErrorScope, Protocol, Provider};

/// Producer-bug invariants that `AccountErrorBuilder::try_build`
/// detects at construction time. These never escape into
/// `Result<T, AccountError>`; protocol-crate translation boundaries
/// call `.expect("valid account error classification")` because an
/// invalid combination is a library bug, not recoverable runtime
/// state.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AccountErrorBuildError {
    /// The chain was empty. Every `AccountError` must carry at least
    /// one `Cause` (the primary).
    EmptyChain,
    /// The outermost `Cause` does not match the declared
    /// `AccountErrorKind`. Producers must classify both halves
    /// consistently.
    KindCauseMismatch {
        kind: AccountErrorKind,
        primary_cause: Cause,
    },
    /// `Transport(_)` kind combined with an `Attempt(Acknowledged)`
    /// cause. Transport failures by definition mean no complete
    /// server response was received; an acknowledged transmission
    /// would belong on a `Server(_)` or `Protocol(_)` kind.
    TransportAcknowledged,
    /// `SyncState(CursorInvalid)` without an `ErrorScope::Cursor`.
    /// Producers must thread the cursor scope from the call site;
    /// the engine cannot route a scope-less cursor restart.
    CursorInvalidWithoutScope,
}

impl fmt::Display for AccountErrorBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyChain => write!(f, "AccountError chain is empty"),
            Self::KindCauseMismatch {
                kind,
                primary_cause,
            } => write!(
                f,
                "AccountErrorKind {kind:?} does not match outermost cause {primary_cause:?}",
            ),
            Self::TransportAcknowledged => write!(
                f,
                "transport failures cannot have acknowledged transmission state",
            ),
            Self::CursorInvalidWithoutScope => {
                write!(f, "SyncState(CursorInvalid) requires an ErrorScope::Cursor",)
            }
        }
    }
}

impl std::error::Error for AccountErrorBuildError {}

pub(crate) struct RebuildParts {
    pub kind: AccountErrorKind,
    pub primary_cause: Cause,
    pub chain_extras: Vec<Cause>,
    pub scope: Option<ErrorScope>,
    pub operation: Option<AccountOperation>,
    pub provider: Option<Provider>,
    pub protocol: Option<Protocol>,
    pub diagnostics: DiagnosticInfo,
}

#[derive(Clone, Debug)]
pub struct AccountErrorBuilder {
    kind: AccountErrorKind,
    primary_cause: Cause,
    chain_extras: Vec<Cause>,
    scope: Option<ErrorScope>,
    operation: Option<AccountOperation>,
    provider: Option<Provider>,
    protocol: Option<Protocol>,
    diagnostics: DiagnosticInfo,
    idempotency_override: Option<bool>,
    throttle_scope: Option<ThrottleScope>,
}

impl AccountErrorBuilder {
    #[must_use]
    pub fn new(kind: AccountErrorKind, primary_cause: Cause) -> Self {
        Self {
            kind,
            primary_cause,
            chain_extras: Vec::new(),
            scope: None,
            operation: None,
            provider: None,
            protocol: None,
            diagnostics: DiagnosticInfo::default(),
            idempotency_override: None,
            throttle_scope: None,
        }
    }

    /// Constructor used by `AccountError::into_builder`. Pre-populates
    /// the chain (split into primary + extras), diagnostics, and the
    /// top-level fields from an existing built error so the caller can
    /// `push_cause`, then `try_build()` to obtain a new `AccountError`
    /// with derived fields recomputed.
    ///
    /// The `idempotency_override` and `throttle_scope` builder
    /// overrides are reset on round-trip; the derived `recovery` will
    /// be recomputed from the chain. Callers that want to preserve
    /// those overrides must reapply them after `into_builder`. A
    /// retry hint that was set on a `ServerCause` is preserved
    /// structurally through the chain - no separate side-channel.
    #[must_use]
    pub(crate) fn from_rebuild(parts: RebuildParts) -> Self {
        Self {
            kind: parts.kind,
            primary_cause: parts.primary_cause,
            chain_extras: parts.chain_extras,
            scope: parts.scope,
            operation: parts.operation,
            provider: parts.provider,
            protocol: parts.protocol,
            diagnostics: parts.diagnostics,
            idempotency_override: None,
            throttle_scope: None,
        }
    }

    #[must_use]
    pub fn operation(mut self, op: AccountOperation) -> Self {
        self.operation = Some(op);
        self
    }

    #[must_use]
    pub fn scope(mut self, scope: ErrorScope) -> Self {
        self.scope = Some(scope);
        self
    }

    #[must_use]
    pub fn provider(mut self, provider: Provider) -> Self {
        self.provider = Some(provider);
        self
    }

    #[must_use]
    pub fn protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = Some(protocol);
        self
    }

    #[must_use]
    pub fn push_cause(mut self, cause: Cause) -> Self {
        self.chain_extras.push(cause);
        self
    }

    #[must_use]
    pub fn request_id(mut self, id: impl Into<String>) -> Self {
        self.diagnostics.request_id = Some(id.into());
        self
    }

    #[must_use]
    pub fn trace_id(mut self, id: impl Into<String>) -> Self {
        self.diagnostics.trace_id = Some(id.into());
        self
    }

    /// Server-returned numeric status. `None` is permitted and represents
    /// "server responded with an error this protocol does not carry a
    /// numeric status for" (IMAP `NO`/`BAD` without a response code).
    /// Builders must not synthesize sentinel values like `0`; pass `None`
    /// instead.
    #[must_use]
    pub fn status(mut self, status: Option<u16>) -> Self {
        self.diagnostics.status = status;
        self
    }

    #[must_use]
    pub fn native_code(mut self, code: impl Into<String>) -> Self {
        self.diagnostics.native_code = Some(code.into());
        self
    }

    #[must_use]
    pub fn text(mut self, text: DiagnosticText) -> Self {
        self.diagnostics.text.push(text);
        self
    }

    /// Override the default idempotency for this operation. Reserved
    /// for exceptional cases where a normally non-idempotent op is
    /// made safe by a request-side guard (e.g. an `If-Match` etag) or
    /// vice versa. `AccountOperation::is_idempotent()` is the
    /// authoritative source for the common case; callers that find
    /// themselves reaching for this method should first check whether
    /// their operation is mis-classified.
    #[must_use]
    pub fn idempotency_override(mut self, idempotent: bool) -> Self {
        self.idempotency_override = Some(idempotent);
        self
    }

    /// Provider-documented throttle scope for rate-limit / quota
    /// failures. Maps onto sharable [`ThrottleKey`](super::recovery::ThrottleKey)
    /// entries the engine consults across work items. Use
    /// `ThrottleScope::CurrentOperation` for per-call hints that
    /// should not enter a shared bucket.
    #[must_use]
    pub fn throttle_scope(mut self, scope: ThrottleScope) -> Self {
        self.throttle_scope = Some(scope);
        self
    }

    /// Construct the `AccountError`. Returns `Err` only on producer-bug
    /// invariants (see [`AccountErrorBuildError`]); protocol-crate
    /// translation boundaries call `.expect("valid account error
    /// classification")` because an invalid combination is a library
    /// bug, not recoverable runtime state.
    pub fn try_build(self) -> Result<AccountError, AccountErrorBuildError> {
        if self.throttle_scope.is_some()
            && !matches!(
                &self.kind,
                AccountErrorKind::Server(
                    ServerErrorKind::RateLimited | ServerErrorKind::QuotaExhausted
                )
            )
        {
            // throttle_scope is meaningful only on rate-limit/quota
            // failures; producers attaching it elsewhere is a bug.
            // Caught here so the error surfaces at construction.
            return Err(AccountErrorBuildError::KindCauseMismatch {
                kind: self.kind,
                primary_cause: self.primary_cause,
            });
        }

        if !recovery::kind_matches_cause(&self.kind, &self.primary_cause) {
            return Err(AccountErrorBuildError::KindCauseMismatch {
                kind: self.kind,
                primary_cause: self.primary_cause,
            });
        }

        // CursorInvalid without an ErrorScope::Cursor cannot be routed
        // by the engine. The convergence rewrite removed the silent
        // fallback to `RestartAccount`; producers must thread the
        // cursor scope from the call site.
        if matches!(
            self.kind,
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ) && recovery::cursor_scope(self.scope.as_ref()).is_none()
        {
            return Err(AccountErrorBuildError::CursorInvalidWithoutScope);
        }

        let mut causes = Vec::with_capacity(1 + self.chain_extras.len());
        causes.push(self.primary_cause);
        causes.extend(self.chain_extras);
        // CauseChain::new asserts non-empty; the vec above always has
        // the primary cause so the assertion never trips here.
        let chain = CauseChain::new(causes);

        // Transport(_) cannot pair with an Acknowledged Attempt. The
        // wire-level evidence contradicts the kind.
        if matches!(self.kind, AccountErrorKind::Transport(_))
            && chain.iter().any(|cause| {
                matches!(
                    cause,
                    Cause::Attempt(attempt)
                    if attempt.transmission_state
                        == super::cause::TransmissionState::Acknowledged
                )
            })
        {
            return Err(AccountErrorBuildError::TransportAcknowledged);
        }

        let recovery = recovery::derive(
            &self.kind,
            self.scope.as_ref(),
            self.operation,
            &chain,
            self.throttle_scope,
            self.idempotency_override,
        );
        let remediation = recovery::suggest(&self.kind, &recovery, self.scope.as_ref(), &chain);
        let message_key = message_key::derive(&self.kind);

        Ok(AccountError::from_parts(AccountErrorParts {
            kind: self.kind,
            recovery,
            remediation,
            scope: self.scope,
            operation: self.operation,
            provider: self.provider,
            protocol: self.protocol,
            diagnostics: self.diagnostics,
            chain,
            message_key,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{
        AttemptCause, Cause, RequestCause, RequestErrorKind, TransmissionState, TransportCause,
        TransportErrorKind, TransportKind,
    };

    #[test]
    fn build_derives_message_key_and_recovery() {
        let err = AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("bad"),
            }),
        )
        .try_build()
        .expect("valid account error classification");

        assert_eq!(err.message_key(), "request.malformed");
        assert!(err.recovery().is_terminal());
    }

    #[test]
    fn mismatched_kind_and_cause_rejected() {
        let err = AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Transport(TransportCause {
                kind: TransportKind::Network,
                message: None,
            }),
        )
        .try_build()
        .expect_err("kind and cause disagree");

        assert!(matches!(
            err,
            AccountErrorBuildError::KindCauseMismatch { .. }
        ));
    }

    #[test]
    fn throttle_scope_rejected_outside_rate_or_quota() {
        let err = AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause {
                kind: TransportKind::Network,
                message: None,
            }),
        )
        .throttle_scope(ThrottleScope::Account)
        .try_build()
        .expect_err("throttle_scope on transport is invalid");

        assert!(matches!(
            err,
            AccountErrorBuildError::KindCauseMismatch { .. }
        ));
    }

    #[test]
    fn transport_acknowledged_rejected() {
        let err = AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause {
                kind: TransportKind::Network,
                message: None,
            }),
        )
        .push_cause(Cause::Attempt(AttemptCause {
            transmission_state: TransmissionState::Acknowledged,
        }))
        .try_build()
        .expect_err("transport + acknowledged is invalid");

        assert_eq!(err, AccountErrorBuildError::TransportAcknowledged);
    }

    #[test]
    fn cursor_invalid_without_scope_rejected() {
        use super::super::cause::StateCause;
        let err = AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        )
        .try_build()
        .expect_err("CursorInvalid requires a cursor scope");

        assert_eq!(err, AccountErrorBuildError::CursorInvalidWithoutScope);
    }
}
