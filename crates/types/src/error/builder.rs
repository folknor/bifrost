use std::time::SystemTime;

use super::account_error::{AccountError, AccountErrorParts};
use super::cause::{Cause, CauseChain};
use super::diagnostic::{DiagnosticInfo, DiagnosticText};
use super::kind::{AccountErrorKind, ServerErrorKind};
use super::message_key;
use super::recovery::{self, ThrottleScope};
use super::scope::{AccountOperation, ErrorScope, Protocol, Provider};

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
    retry_not_before: Option<SystemTime>,
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
            retry_not_before: None,
            throttle_scope: None,
        }
    }

    /// Constructor used by `AccountError::into_builder`. Pre-populates
    /// the chain (split into primary + extras), diagnostics, and the
    /// top-level fields from an existing built error so the caller can
    /// `push_cause`, then `build()` to obtain a new `AccountError` with
    /// derived fields recomputed.
    ///
    /// The `idempotency_override`, `retry_not_before`, and
    /// `throttle_scope` builder overrides are reset on round-trip; the
    /// derived `recovery` will be recomputed from the chain. Callers
    /// that want to preserve those overrides must reapply them after
    /// `into_builder`.
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
            retry_not_before: None,
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

    #[must_use]
    pub fn idempotency_override(mut self, idempotent: bool) -> Self {
        self.idempotency_override = Some(idempotent);
        self
    }

    #[must_use]
    pub fn retry_not_before(mut self, when: SystemTime) -> Self {
        self.retry_not_before = Some(when);
        self
    }

    #[must_use]
    pub fn throttle_scope(mut self, scope: ThrottleScope) -> Self {
        self.throttle_scope = Some(scope);
        self
    }

    #[must_use]
    pub fn build(self) -> AccountError {
        assert!(
            self.throttle_scope.is_none()
                || matches!(
                    &self.kind,
                    AccountErrorKind::Server(
                        ServerErrorKind::RateLimited | ServerErrorKind::QuotaExhausted
                    )
                ),
            "throttle_scope only applies to rate-limit and quota failures"
        );

        let mut causes = Vec::with_capacity(1 + self.chain_extras.len());
        causes.push(self.primary_cause);
        causes.extend(self.chain_extras);
        let chain = CauseChain::new(causes);

        assert!(
            recovery::kind_matches_cause(&self.kind, chain.outermost()),
            "AccountErrorKind must match the outermost semantic cause"
        );

        let recovery = recovery::derive(
            &self.kind,
            self.scope.as_ref(),
            self.operation,
            &chain,
            self.retry_not_before,
            self.throttle_scope,
            self.idempotency_override,
        );
        let remediation = recovery::suggest(&self.kind, &recovery, self.scope.as_ref(), &chain);
        let message_key = message_key::derive(&self.kind);

        AccountError::from_parts(AccountErrorParts {
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{
        Cause, RequestCause, RequestErrorKind, TransportCause, TransportErrorKind, TransportKind,
    };

    #[test]
    fn build_derives_message_key_and_recovery() {
        let err = AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("bad"),
            }),
        )
        .build();

        assert_eq!(err.message_key(), "request.malformed");
        assert!(err.recovery().is_terminal());
    }

    #[test]
    #[should_panic(expected = "AccountErrorKind must match")]
    fn mismatched_kind_and_cause_panics() {
        let _ = AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Transport(TransportCause {
                kind: TransportKind::Network,
                message: None,
            }),
        )
        .build();
    }

    #[test]
    #[should_panic(expected = "throttle_scope only applies")]
    fn throttle_scope_panics_outside_rate_or_quota() {
        let _ = AccountErrorBuilder::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause {
                kind: TransportKind::Network,
                message: None,
            }),
        )
        .throttle_scope(ThrottleScope::Account)
        .build();
    }
}
