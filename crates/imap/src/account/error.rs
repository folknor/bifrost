//! IMAP -> `AccountError` translation boundary.
//!
//! This module is the only place in `bifrost-imap` that constructs
//! `bifrost_types::AccountError`. Every account-trait call site funnels
//! its crate-private `crate::Error` through `into_account_error` with an
//! `ImapErrorContext` carrying operation, scope, and provider.
//!
//! Why this matters: the central recovery mapping in
//! `bifrost_types::error::recovery` derives `RecoveryClass` from
//! `(kind, scope, operation, primary_cause)`, where the primary cause is
//! often a `Cause::Attempt(AttemptCause { transmission_state })`. IMAP
//! is its own transport (no `bifrost-net` for protocol traffic), so the
//! driver / pipeline owns the wire-level evidence of whether a command's
//! bytes ever crossed the side-effect boundary. That evidence has to
//! survive into the chain or the engine will silently retry
//! non-idempotent operations across `InFlight` transport drops.

use bifrost_types::{
    AccessCause, AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation,
    AttemptCause, AuthCause, AuthErrorKind, Cause, CursorScope, DiagnosticText, ErrorScope,
    ImapResponseCode, Protocol, ProtocolErrorKind, Provider, RequestCause, RequestErrorKind,
    ResourceKind, ServerCause, ServerErrorKind, StateCause, StrategyDowngrade, SyncStateErrorKind,
    ThrottleScope, TransmissionState, TransportCause, TransportErrorKind, TransportKind, WireCause,
};

use crate::Error;
use crate::types::{MailboxName, ResponseCode};

/// Context carried alongside `crate::Error` across the account boundary.
///
/// Constructed at every account-trait call site with the operation under
/// way and the scope identifying which mailbox / message / thread the
/// error pertains to. `transmission_state` is a fallback only - the
/// translation prefers `Error::attempt()` when the driver populated it.
#[derive(Clone, Debug)]
pub(crate) struct ImapErrorContext {
    pub(crate) operation: AccountOperation,
    pub(crate) scope: Option<ErrorScope>,
    pub(crate) provider: Option<Provider>,
    pub(crate) idempotency_override: Option<bool>,
    pub(crate) transmission_state: Option<TransmissionState>,
}

impl ImapErrorContext {
    pub(crate) fn operation(operation: AccountOperation) -> Self {
        Self {
            operation,
            scope: None,
            provider: None,
            idempotency_override: None,
            transmission_state: None,
        }
    }

    #[must_use]
    pub(crate) fn with_scope(mut self, scope: ErrorScope) -> Self {
        self.scope = Some(scope);
        self
    }

    #[must_use]
    pub(crate) fn with_cursor_scope(mut self, scope: CursorScope) -> Self {
        self.scope = Some(ErrorScope::Cursor(scope));
        self
    }

    #[must_use]
    pub(crate) fn with_mailbox(mut self, mailbox: &MailboxName) -> Self {
        self.scope = Some(ErrorScope::Mailbox {
            id: mailbox.as_str().to_owned(),
        });
        self
    }

    /// Like `with_mailbox` but produces an `ErrorScope::Cursor(Folder(_))`
    /// instead of `Mailbox { id }`. Folder-scoped operations (STORE,
    /// FETCH against a folder, mutation paths) use this so that the
    /// `EXPUNGEISSUED` / `CLOSED` / `NotificationOverflow` response
    /// codes - which classify as `SyncState(CursorInvalid)` - build a
    /// structurally valid error. The previous shape paired
    /// `ErrorScope::Mailbox` with `CursorInvalid` and relied on a
    /// translation-boundary auto-promote shim to fix it up; this
    /// helper makes the cursor scoping explicit at the producer.
    #[must_use]
    pub(crate) fn with_folder_scope(mut self, mailbox: &MailboxName) -> Self {
        self.scope = Some(ErrorScope::Cursor(CursorScope::Folder(
            bifrost_types::FolderId(mailbox.as_str().to_owned()),
        )));
        self
    }

    #[must_use]
    pub(crate) fn with_message_id(mut self, id: impl Into<String>) -> Self {
        self.scope = Some(ErrorScope::Message { id: id.into() });
        self
    }

    #[must_use]
    pub(crate) fn with_transmission_state(mut self, state: TransmissionState) -> Self {
        self.transmission_state = Some(state);
        self
    }

    #[must_use]
    pub(crate) fn with_idempotency_override(mut self, idempotent: bool) -> Self {
        self.idempotency_override = Some(idempotent);
        self
    }
}

/// Translate an internal IMAP error into a public `AccountError`.
pub(crate) fn into_account_error(error: Error, ctx: ImapErrorContext) -> AccountError {
    let attempt = error.attempt().or(ctx.transmission_state);
    let translation = classify(&error, &ctx);
    let Translation {
        kind,
        primary_cause,
        wire_code,
        attempt: explicit_attempt,
        throttle_scope,
        diagnostic_text,
        native_code,
        skip_attempt_cause,
    } = translation;

    // CursorInvalid requires a cursor scope or `try_build` rejects
    // (`CursorInvalidWithoutScope`). Folder-scoped producers use
    // `ImapErrorContext::with_folder_scope(&mailbox)` so the scope
    // is `ErrorScope::Cursor(Folder(id))` rather than
    // `ErrorScope::Mailbox { id }`.
    let scope_for_kind = ctx.scope.clone();

    let mut builder = AccountErrorBuilder::new(kind, primary_cause)
        .protocol(Protocol::Imap)
        .operation(ctx.operation);

    if let Some(scope) = scope_for_kind {
        builder = builder.scope(scope);
    }
    if let Some(provider) = ctx.provider {
        builder = builder.provider(provider);
    }
    if let Some(override_) = ctx.idempotency_override {
        builder = builder.idempotency_override(override_);
    }
    if let Some(throttle) = throttle_scope {
        builder = builder.throttle_scope(throttle);
    }
    if let Some(code) = native_code {
        builder = builder.native_code(code);
    }
    if let Some(text) = diagnostic_text {
        builder = builder.text(text);
    }

    // Wire cause (IMAP response code) is pushed before the attempt
    // cause so that support exports walk wire evidence first.
    if let Some(code) = wire_code {
        builder = builder.push_cause(Cause::Wire(WireCause::Imap(code)));
    }

    // Attach attempt cause unless explicitly suppressed (transport
    // failures must never carry `Acknowledged`; the builder asserts).
    let effective_attempt = explicit_attempt.or(attempt);
    if !skip_attempt_cause && let Some(state) = effective_attempt {
        builder = builder.push_cause(Cause::Attempt(AttemptCause::new(state)));
    }

    builder
        .try_build()
        .expect("valid account error classification")
}

/// Build an `AccountError` for a locally detected `Unsupported` PIM
/// operation. The PIM layer used to return `AccountError::Unsupported`;
/// in the new contract every unsupported call is structured.
pub(crate) fn unsupported(operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(operation),
        Cause::Request(RequestCause::Unsupported { operation }),
    )
    .protocol(Protocol::Imap)
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// Build a fatal-stream error for UIDVALIDITY change on a folder cursor.
pub(crate) fn uidvalidity_changed(
    folder: &MailboxName,
    expected: u32,
    actual: u32,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
        Cause::State(StateCause::CursorInvalid),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::SyncChanges)
    .scope(ErrorScope::Cursor(super::folder_scope(folder)))
    .text(DiagnosticText::support_only(format!(
        "IMAP UIDVALIDITY changed for {} from {} to {}",
        folder.as_str(),
        expected,
        actual,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// Build a fatal-stream error for HIGHESTMODSEQ regression on a folder cursor.
pub(crate) fn modseq_reset(
    folder: &MailboxName,
    previous: u64,
    current: Option<u64>,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
        Cause::State(StateCause::CursorInvalid),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::SyncChanges)
    .scope(ErrorScope::Cursor(super::folder_scope(folder)))
    .text(DiagnosticText::support_only(format!(
        "IMAP HIGHESTMODSEQ reset for {} from {} to {:?}",
        folder.as_str(),
        previous,
        current,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// Build a fatal-stream error for a terminal QRESYNC/CONDSTORE strategy
/// downgrade that cannot be served on the current connection.
pub(crate) fn strategy_failure(folder: &MailboxName, downgrade: StrategyDowngrade) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::StrategyFailure),
        Cause::State(StateCause::StrategyFailure { downgrade }),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::SyncChanges)
    .scope(ErrorScope::Cursor(super::folder_scope(folder)))
    .try_build()
    .expect("valid account error classification")
}

/// Build a `SyncState(ScopeRevoked)` error scoped to one shared folder.
/// Derives to `Engine(DisableScope(Folder(id)))` - the engine quarantines
/// just that scope without escalating to account-wide auth loss. The
/// owning mailbox rides as support-only diagnostic text for telemetry.
pub(crate) fn scope_revoked(
    folder: &MailboxName,
    owner: &bifrost_types::MailboxId,
    operation: AccountOperation,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked),
        Cause::State(StateCause::ScopeRevoked),
    )
    .protocol(Protocol::Imap)
    .operation(operation)
    .scope(ErrorScope::Cursor(super::folder_scope(folder)))
    .text(DiagnosticText::support_only(format!(
        "shared folder access revoked for {} (owner {})",
        folder.as_str(),
        owner.0,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// Map a per-folder failure for a shared/other-user folder. When the
/// failure is a permission/access denial (the class that would otherwise
/// derive terminal `NoPermission`) and the folder is shared
/// (`shared_owner.is_some()`), produce a scoped `ScopeRevoked` that
/// quarantines just this folder instead of escalating account-wide. A
/// personal folder (`shared_owner == None`), or any non-permission
/// failure, flows through the normal `into_account_error` mapping - a
/// personal-folder permission loss is a genuine account-level signal.
pub(crate) fn shared_folder_error(
    error: Error,
    folder: &MailboxName,
    shared_owner: Option<&bifrost_types::MailboxId>,
    ctx: ImapErrorContext,
) -> AccountError {
    if let Some(owner) = shared_owner {
        let translation = classify(&error, &ctx);
        if matches!(
            translation.kind,
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied)
        ) {
            return scope_revoked(folder, owner, ctx.operation);
        }
    }
    into_account_error(error, ctx)
}

// ---------------------------------------------------------------------
// Internal classification machinery.
// ---------------------------------------------------------------------

struct Translation {
    kind: AccountErrorKind,
    primary_cause: Cause,
    wire_code: Option<ImapResponseCode>,
    /// Explicit attempt-state override (used by `Bye` during command
    /// response loop, etc.). When `None`, the builder uses the
    /// `Error::attempt()` evidence.
    attempt: Option<TransmissionState>,
    throttle_scope: Option<ThrottleScope>,
    diagnostic_text: Option<DiagnosticText>,
    native_code: Option<String>,
    /// Set when the kind is `Transport(_)` and the effective attempt
    /// would be `Acknowledged` - the builder asserts that combination
    /// is impossible, so we drop the attempt rather than panic.
    skip_attempt_cause: bool,
}

impl Translation {
    fn new(kind: AccountErrorKind, primary_cause: Cause) -> Self {
        Self {
            kind,
            primary_cause,
            wire_code: None,
            attempt: None,
            throttle_scope: None,
            diagnostic_text: None,
            native_code: None,
            skip_attempt_cause: false,
        }
    }
}

fn classify(error: &Error, ctx: &ImapErrorContext) -> Translation {
    match error {
        Error::Io { source, .. } => Translation::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(
                TransportKind::Network,
                Some(DiagnosticText::support_only(source.to_string())),
            )),
        ),
        Error::Timeout { .. } => Translation::new(
            AccountErrorKind::Transport(TransportErrorKind::Timeout),
            Cause::Transport(TransportCause::new(TransportKind::Timeout, None)),
        ),
        Error::Closed { .. } => Translation::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(
                TransportKind::Network,
                Some(DiagnosticText::support_only("IMAP connection closed")),
            )),
        ),
        Error::DriverGone { .. } => Translation::new(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(
                TransportKind::Network,
                Some(DiagnosticText::support_only("IMAP driver task gone")),
            )),
        ),
        Error::DriverPanicked { message, .. } => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only(format!(
                    "IMAP driver panicked: {message}"
                ))),
            }),
        ),
        Error::Auth { text, code } => classify_auth(text, code.as_ref()),
        Error::AuthPolicy(failure) => {
            let mut t = Translation::new(
                AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PolicyBlocked),
                Cause::Access(AccessCause::PolicyBlocked),
            );
            t.diagnostic_text = Some(DiagnosticText::support_only(failure.to_string()));
            t
        }
        Error::StartTlsUnavailable => {
            let mut t = Translation::new(
                AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PolicyBlocked),
                Cause::Access(AccessCause::PolicyBlocked),
            );
            t.diagnostic_text = Some(DiagnosticText::support_only(
                "server does not advertise STARTTLS",
            ));
            t
        }
        Error::No { text, code, .. } => {
            classify_status(text, code.as_ref(), StatusFallback::No, ctx)
        }
        Error::Bad { text, code, .. } => {
            classify_status(text, code.as_ref(), StatusFallback::Bad, ctx)
        }
        Error::Bye { text, code, .. } => {
            classify_status(text, code.as_ref(), StatusFallback::Bye, ctx)
        }
        Error::Protocol(msg) => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only(msg.clone())),
            }),
        ),
        Error::Parse(msg) => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only(msg.clone())),
            }),
        ),
        Error::InvalidInput(msg) => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(msg.clone()),
            }),
        ),
        Error::Internal(msg) => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only(msg.clone())),
            }),
        ),
        Error::MissingCapability(cap) => {
            let operation = ctx.operation;
            let mut t = Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            );
            t.diagnostic_text = Some(DiagnosticText::support_only(cap.clone()));
            t
        }
        Error::AppendLimit { size, limit } => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(format!(
                    "message size {size} exceeds server APPENDLIMIT of {limit}"
                )),
            }),
        ),
        Error::FetchLimit {
            estimated, limit, ..
        } => {
            let mut t = Translation::new(
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::Malformed {
                    detail: DiagnosticText::support_only(format!(
                        "FETCH estimated size {estimated} exceeds caller budget {limit}"
                    )),
                }),
            );
            t.attempt = Some(TransmissionState::Acknowledged);
            t
        }
        Error::SearchResultTruncated { returned, omitted } => {
            let mut t = Translation::new(
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::Malformed {
                    detail: DiagnosticText::support_only(format!(
                        "UID SEARCH result exceeded the local expansion limit after {returned} IDs; \
                         omitted {omitted:?}"
                    )),
                }),
            );
            // The server answered the search. The account boundary must not
            // return a partial Page as a successful result.
            t.attempt = Some(TransmissionState::Acknowledged);
            t
        }
        Error::InvalidAppendDate(msg) => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(msg.clone()),
            }),
        ),
    }
}

fn classify_auth(text: &str, code: Option<&ResponseCode>) -> Translation {
    let (kind, cause) = match code {
        Some(ResponseCode::Expired) => (
            AccountErrorKind::Authentication(AuthErrorKind::Expired),
            Cause::Auth(AuthCause::Expired),
        ),
        Some(ResponseCode::AuthenticationFailed) | None => (
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
        ),
        Some(ResponseCode::AuthorizationFailed) => (
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied { resource: None }),
        ),
        Some(ResponseCode::PrivacyRequired) => (
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PolicyBlocked),
            Cause::Access(AccessCause::PolicyBlocked),
        ),
        _ => (
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
        ),
    };
    let mut t = Translation::new(kind, cause);
    if !text.is_empty() {
        t.diagnostic_text = Some(DiagnosticText::support_only(text.to_owned()));
    }
    if let Some(code) = code {
        t.wire_code = Some(imap_response_code(code));
        t.native_code = Some(response_code_name(code).to_owned());
    }
    // Auth failures are server-acknowledged tagged responses.
    t.attempt = Some(TransmissionState::Acknowledged);
    t
}

enum StatusFallback {
    No,
    Bad,
    Bye,
}

fn classify_status(
    text: &str,
    code: Option<&ResponseCode>,
    fallback: StatusFallback,
    ctx: &ImapErrorContext,
) -> Translation {
    let mut t = if let Some(code) = code {
        classify_response_code(code, ctx).unwrap_or_else(|| fallback_status(text, &fallback))
    } else {
        fallback_status(text, &fallback)
    };
    if let Some(code) = code {
        t.wire_code = Some(imap_response_code(code));
        t.native_code = Some(response_code_name(code).to_owned());
        // For parameterized codes whose payload is interesting, attach
        // as support-only text if not already set.
        if t.diagnostic_text.is_none()
            && let Some(payload) = response_code_payload(code)
        {
            t.diagnostic_text = Some(payload);
        }
    }
    if !text.is_empty() && t.diagnostic_text.is_none() {
        t.diagnostic_text = Some(DiagnosticText::support_only(text.to_owned()));
    }
    t
}

fn fallback_status(_text: &str, fallback: &StatusFallback) -> Translation {
    match fallback {
        StatusFallback::No => Translation::new(
            AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
            Cause::Server(ServerCause::Error { status: None }),
        ),
        StatusFallback::Bad => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("server returned BAD with no response code"),
            }),
        ),
        StatusFallback::Bye => Translation::new(
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
        ),
    }
}

fn classify_response_code(code: &ResponseCode, ctx: &ImapErrorContext) -> Option<Translation> {
    let resource_from_scope = || match ctx.scope.as_ref() {
        Some(ErrorScope::Mailbox { .. }) | Some(ErrorScope::Cursor(CursorScope::Folder(_))) => {
            Some(ResourceKind::Mailbox)
        }
        Some(ErrorScope::Message { .. }) => Some(ResourceKind::Message),
        Some(ErrorScope::Thread { .. }) => Some(ResourceKind::Thread),
        _ => None,
    };
    let id_from_scope = || match ctx.scope.as_ref() {
        Some(ErrorScope::Mailbox { id }) => Some(id.clone()),
        Some(ErrorScope::Message { id }) => Some(id.clone()),
        Some(ErrorScope::Thread { id }) => Some(id.clone()),
        _ => None,
    };
    let mailbox_throttle = || match ctx.scope.as_ref() {
        Some(ErrorScope::Mailbox { .. }) => ThrottleScope::Mailbox,
        _ => ThrottleScope::Account,
    };

    let t = match code {
        // Each arm produces an owned Translation. Some arms below mutate
        // it locally before yielding; the outer binding stays immutable.
        // Auth / policy
        ResponseCode::AuthenticationFailed => Translation::new(
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
        ),
        ResponseCode::Expired => Translation::new(
            AccountErrorKind::Authentication(AuthErrorKind::Expired),
            Cause::Auth(AuthCause::Expired),
        ),
        ResponseCode::AuthorizationFailed | ResponseCode::NoPerm => Translation::new(
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: resource_from_scope(),
            }),
        ),
        ResponseCode::PrivacyRequired | ResponseCode::ContactAdmin => Translation::new(
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PolicyBlocked),
            Cause::Access(AccessCause::PolicyBlocked),
        ),
        ResponseCode::MetadataNoPrivate => Translation::new(
            AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: Some(ResourceKind::Mailbox),
            }),
        ),

        // Transient server
        ResponseCode::Unavailable
        | ResponseCode::InUse
        | ResponseCode::Corruption
        | ResponseCode::TempFail(_) => Translation::new(
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
        ),
        ResponseCode::ServerBug => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only("server returned [SERVERBUG]")),
            }),
        ),

        // Mailbox / cursor state
        ResponseCode::ExpungeIssued | ResponseCode::Closed => Translation::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        ),
        ResponseCode::UidNotSticky => Translation::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::ScopeCapabilityLost),
            Cause::State(StateCause::ScopeCapabilityLost),
        ),
        ResponseCode::NoModSeq => Translation::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::StrategyFailure),
            Cause::State(StateCause::StrategyFailure {
                downgrade: StrategyDowngrade::CondstoreToBasic,
            }),
        ),
        ResponseCode::Modified(_)
        | ResponseCode::AlreadyExists
        | ResponseCode::HasChildren
        | ResponseCode::NoUpdate(_) => Translation::new(
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
        ),
        ResponseCode::NonExistent => {
            let resource = resource_from_scope().unwrap_or(ResourceKind::Mailbox);
            Translation::new(
                AccountErrorKind::NotFound(resource),
                Cause::Request(RequestCause::NotFound {
                    what: resource,
                    id: id_from_scope(),
                }),
            )
        }
        ResponseCode::TryCreate => Translation::new(
            AccountErrorKind::NotFound(ResourceKind::Mailbox),
            Cause::Request(RequestCause::NotFound {
                what: ResourceKind::Mailbox,
                id: id_from_scope(),
            }),
        ),

        // Quota / limits
        ResponseCode::OverQuota => {
            let mut t = Translation::new(
                AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
                Cause::Server(ServerCause::QuotaExhausted { retry_hint: None }),
            );
            t.throttle_scope = Some(ThrottleScope::Account);
            t
        }
        ResponseCode::Limit => {
            let mut t = Translation::new(
                AccountErrorKind::Server(ServerErrorKind::RateLimited),
                Cause::Server(ServerCause::RateLimited { retry_hint: None }),
            );
            t.throttle_scope = Some(mailbox_throttle());
            t
        }
        ResponseCode::MetadataTooMany => {
            let mut t = Translation::new(
                AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
                Cause::Server(ServerCause::QuotaExhausted { retry_hint: None }),
            );
            t.throttle_scope = Some(ThrottleScope::Account);
            t
        }
        ResponseCode::TooBig
        | ResponseCode::MaxConvertMessages(_)
        | ResponseCode::MaxConvertParts(_)
        | ResponseCode::MetadataMaxSize(_) => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(response_code_name(code).to_owned()),
            }),
        ),

        // Unsupported / unknown feature
        ResponseCode::BadCharset(_) => {
            let operation = ctx.operation;
            Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
        }
        ResponseCode::Cannot | ResponseCode::Annotate(_) | ResponseCode::Annotations(_) => {
            let operation = ctx.operation;
            Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
        }
        ResponseCode::BadComparator(_) => Translation::new(
            AccountErrorKind::Unsupported(AccountOperation::Search),
            Cause::Request(RequestCause::Unsupported {
                operation: AccountOperation::Search,
            }),
        ),
        ResponseCode::BadEvent(_) | ResponseCode::UndefinedFilter(_) => Translation::new(
            AccountErrorKind::Unsupported(AccountOperation::PushSubscribe),
            Cause::Request(RequestCause::Unsupported {
                operation: AccountOperation::PushSubscribe,
            }),
        ),
        ResponseCode::UnknownCte => {
            let operation = ctx.operation;
            Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
        }
        ResponseCode::CompressionActive => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only(
                    "server returned [COMPRESSIONACTIVE]",
                )),
            }),
        ),
        ResponseCode::UseAttr => {
            let operation = ctx.operation;
            Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
        }

        // Referral and URL
        ResponseCode::Referral(_) => {
            let operation = ctx.operation;
            Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
        }
        ResponseCode::NewName(_) => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only("[NEWNAME]")),
            }),
        ),
        ResponseCode::UrlMech(_) => {
            let operation = ctx.operation;
            Translation::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
        }
        ResponseCode::BadUrl(_) => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("[BADURL]"),
            }),
        ),

        // Parse and bug
        ResponseCode::Parse => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Imap,
                detail: Some(DiagnosticText::support_only("[PARSE]")),
            }),
        ),
        ResponseCode::ClientBug => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("[CLIENTBUG]"),
            }),
        ),

        // Search / saved-result / notification
        ResponseCode::NotSaved => Translation::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("[NOTSAVED]"),
            }),
        ),
        ResponseCode::NotificationOverflow(_) => Translation::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        ),

        // Other / unknown
        ResponseCode::Other { name, value } => Translation::new(
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Imap(ImapResponseCode::Unknown {
                code: name.clone(),
                value: value
                    .as_ref()
                    .map(|v| DiagnosticText::support_only(v.clone())),
            })),
        ),

        // Informational / state-carrying codes that don't change error semantics.
        // The caller will fall back to status classification.
        ResponseCode::Alert
        | ResponseCode::Capability(_)
        | ResponseCode::PermanentFlags(_)
        | ResponseCode::ReadOnly
        | ResponseCode::ReadWrite
        | ResponseCode::UidNext(_)
        | ResponseCode::UidValidity(_)
        | ResponseCode::Unseen(_)
        | ResponseCode::AppendUid { .. }
        | ResponseCode::CopyUid { .. }
        | ResponseCode::HighestModSeq(_)
        | ResponseCode::MailboxId(_)
        | ResponseCode::MetadataLongEntries(_) => return None,
    };

    Some(t)
}

/// Map crate `ResponseCode` -> `bifrost_types::ImapResponseCode`.
///
/// Parameterized codes lose their payload at the kind level; the payload
/// is preserved separately as `DiagnosticText`. The `Unknown` variant
/// carries the raw name and an optional value.
fn imap_response_code(code: &ResponseCode) -> ImapResponseCode {
    match code {
        ResponseCode::Alert => ImapResponseCode::Alert,
        ResponseCode::BadCharset(_) => ImapResponseCode::BadCharset,
        ResponseCode::Capability(_) => ImapResponseCode::Capability,
        ResponseCode::Parse => ImapResponseCode::Parse,
        ResponseCode::PermanentFlags(_) => ImapResponseCode::PermanentFlags,
        ResponseCode::ReadOnly => ImapResponseCode::ReadOnly,
        ResponseCode::ReadWrite => ImapResponseCode::ReadWrite,
        ResponseCode::TryCreate => ImapResponseCode::TryCreate,
        ResponseCode::UidNext(_) => ImapResponseCode::UidNext,
        ResponseCode::UidValidity(_) => ImapResponseCode::UidValidity,
        ResponseCode::Unseen(_) => ImapResponseCode::Unseen,
        ResponseCode::AppendUid { .. } => ImapResponseCode::AppendUid,
        ResponseCode::CopyUid { .. } => ImapResponseCode::CopyUid,
        ResponseCode::HighestModSeq(_) => ImapResponseCode::HighestModSeq,
        ResponseCode::Modified(_) => ImapResponseCode::Modified,
        ResponseCode::NoModSeq => ImapResponseCode::NoModSeq,
        ResponseCode::Closed => ImapResponseCode::Closed,
        ResponseCode::MailboxId(_) => ImapResponseCode::MailboxId,
        ResponseCode::Unavailable => ImapResponseCode::Unavailable,
        ResponseCode::AuthenticationFailed => ImapResponseCode::AuthenticationFailed,
        ResponseCode::AuthorizationFailed => ImapResponseCode::AuthorizationFailed,
        ResponseCode::Expired => ImapResponseCode::Expired,
        ResponseCode::PrivacyRequired => ImapResponseCode::PrivacyRequired,
        ResponseCode::ContactAdmin => ImapResponseCode::ContactAdmin,
        ResponseCode::NoPerm => ImapResponseCode::NoPerm,
        ResponseCode::InUse => ImapResponseCode::InUse,
        ResponseCode::ExpungeIssued => ImapResponseCode::ExpungeIssued,
        ResponseCode::Corruption => ImapResponseCode::Corruption,
        ResponseCode::ServerBug => ImapResponseCode::ServerBug,
        ResponseCode::ClientBug => ImapResponseCode::ClientBug,
        ResponseCode::Cannot => ImapResponseCode::Cannot,
        ResponseCode::Limit => ImapResponseCode::Limit,
        ResponseCode::OverQuota => ImapResponseCode::OverQuota,
        ResponseCode::AlreadyExists => ImapResponseCode::AlreadyExists,
        ResponseCode::NonExistent => ImapResponseCode::NonExistent,
        ResponseCode::NewName(_) => ImapResponseCode::NewName,
        ResponseCode::Referral(_) => ImapResponseCode::Referral,
        ResponseCode::UrlMech(_) => ImapResponseCode::UrlMech,
        ResponseCode::BadUrl(_) => ImapResponseCode::BadUrl,
        ResponseCode::BadComparator(_) => ImapResponseCode::BadComparator,
        ResponseCode::Annotate(_) => ImapResponseCode::Annotate,
        ResponseCode::Annotations(_) => ImapResponseCode::Annotations,
        ResponseCode::TempFail(_) => ImapResponseCode::TempFail,
        ResponseCode::MaxConvertMessages(_) => ImapResponseCode::MaxConvertMessages,
        ResponseCode::MaxConvertParts(_) => ImapResponseCode::MaxConvertParts,
        ResponseCode::NoUpdate(_) => ImapResponseCode::NoUpdate,
        ResponseCode::NotificationOverflow(_) => ImapResponseCode::NotificationOverflow,
        ResponseCode::BadEvent(_) => ImapResponseCode::BadEvent,
        ResponseCode::UndefinedFilter(_) => ImapResponseCode::UndefinedFilter,
        ResponseCode::UidNotSticky => ImapResponseCode::UidNotSticky,
        ResponseCode::NotSaved => ImapResponseCode::NotSaved,
        ResponseCode::HasChildren => ImapResponseCode::HasChildren,
        ResponseCode::UnknownCte => ImapResponseCode::UnknownCte,
        ResponseCode::TooBig => ImapResponseCode::TooBig,
        ResponseCode::CompressionActive => ImapResponseCode::CompressionActive,
        ResponseCode::UseAttr => ImapResponseCode::UseAttr,
        ResponseCode::MetadataLongEntries(_) => ImapResponseCode::MetadataLongEntries,
        ResponseCode::MetadataMaxSize(_) => ImapResponseCode::MetadataMaxSize,
        ResponseCode::MetadataTooMany => ImapResponseCode::MetadataTooMany,
        ResponseCode::MetadataNoPrivate => ImapResponseCode::MetadataNoPrivate,
        ResponseCode::Other { name, value } => ImapResponseCode::Unknown {
            code: name.clone(),
            value: value
                .as_ref()
                .map(|v| DiagnosticText::support_only(v.clone())),
        },
    }
}

fn response_code_name(code: &ResponseCode) -> &'static str {
    match code {
        ResponseCode::Alert => "ALERT",
        ResponseCode::BadCharset(_) => "BADCHARSET",
        ResponseCode::Capability(_) => "CAPABILITY",
        ResponseCode::Parse => "PARSE",
        ResponseCode::PermanentFlags(_) => "PERMANENTFLAGS",
        ResponseCode::ReadOnly => "READ-ONLY",
        ResponseCode::ReadWrite => "READ-WRITE",
        ResponseCode::TryCreate => "TRYCREATE",
        ResponseCode::UidNext(_) => "UIDNEXT",
        ResponseCode::UidValidity(_) => "UIDVALIDITY",
        ResponseCode::Unseen(_) => "UNSEEN",
        ResponseCode::AppendUid { .. } => "APPENDUID",
        ResponseCode::CopyUid { .. } => "COPYUID",
        ResponseCode::HighestModSeq(_) => "HIGHESTMODSEQ",
        ResponseCode::Modified(_) => "MODIFIED",
        ResponseCode::NoModSeq => "NOMODSEQ",
        ResponseCode::Closed => "CLOSED",
        ResponseCode::MailboxId(_) => "MAILBOXID",
        ResponseCode::Unavailable => "UNAVAILABLE",
        ResponseCode::AuthenticationFailed => "AUTHENTICATIONFAILED",
        ResponseCode::AuthorizationFailed => "AUTHORIZATIONFAILED",
        ResponseCode::Expired => "EXPIRED",
        ResponseCode::PrivacyRequired => "PRIVACYREQUIRED",
        ResponseCode::ContactAdmin => "CONTACTADMIN",
        ResponseCode::NoPerm => "NOPERM",
        ResponseCode::InUse => "INUSE",
        ResponseCode::ExpungeIssued => "EXPUNGEISSUED",
        ResponseCode::Corruption => "CORRUPTION",
        ResponseCode::ServerBug => "SERVERBUG",
        ResponseCode::ClientBug => "CLIENTBUG",
        ResponseCode::Cannot => "CANNOT",
        ResponseCode::Limit => "LIMIT",
        ResponseCode::OverQuota => "OVERQUOTA",
        ResponseCode::AlreadyExists => "ALREADYEXISTS",
        ResponseCode::NonExistent => "NONEXISTENT",
        ResponseCode::NewName(_) => "NEWNAME",
        ResponseCode::Referral(_) => "REFERRAL",
        ResponseCode::UrlMech(_) => "URLMECH",
        ResponseCode::BadUrl(_) => "BADURL",
        ResponseCode::BadComparator(_) => "BADCOMPARATOR",
        ResponseCode::Annotate(_) => "ANNOTATE",
        ResponseCode::Annotations(_) => "ANNOTATIONS",
        ResponseCode::TempFail(_) => "TEMPFAIL",
        ResponseCode::MaxConvertMessages(_) => "MAXCONVERTMESSAGES",
        ResponseCode::MaxConvertParts(_) => "MAXCONVERTPARTS",
        ResponseCode::NoUpdate(_) => "NOUPDATE",
        ResponseCode::NotificationOverflow(_) => "NOTIFICATIONOVERFLOW",
        ResponseCode::BadEvent(_) => "BADEVENT",
        ResponseCode::UndefinedFilter(_) => "UNDEFINED-FILTER",
        ResponseCode::UidNotSticky => "UIDNOTSTICKY",
        ResponseCode::NotSaved => "NOTSAVED",
        ResponseCode::HasChildren => "HASCHILDREN",
        ResponseCode::UnknownCte => "UNKNOWN-CTE",
        ResponseCode::TooBig => "TOOBIG",
        ResponseCode::CompressionActive => "COMPRESSIONACTIVE",
        ResponseCode::UseAttr => "USEATTR",
        ResponseCode::MetadataLongEntries(_) => "METADATA LONGENTRIES",
        ResponseCode::MetadataMaxSize(_) => "METADATA MAXSIZE",
        ResponseCode::MetadataTooMany => "METADATA TOOMANY",
        ResponseCode::MetadataNoPrivate => "METADATA NOPRIVATE",
        ResponseCode::Other { .. } => "OTHER",
    }
}

/// Render parameterized response-code payloads as support-only diagnostic
/// text. Returns `None` when there is no useful payload.
fn response_code_payload(code: &ResponseCode) -> Option<DiagnosticText> {
    let value = match code {
        ResponseCode::BadCharset(items) if !items.is_empty() => {
            format!("BADCHARSET {}", items.join(" "))
        }
        ResponseCode::Modified(ranges) if !ranges.is_empty() => {
            format!("MODIFIED ({} ranges)", ranges.len())
        }
        ResponseCode::TempFail(Some(text))
        | ResponseCode::NoUpdate(Some(text))
        | ResponseCode::Referral(Some(text))
        | ResponseCode::UrlMech(Some(text))
        | ResponseCode::BadUrl(Some(text))
        | ResponseCode::BadComparator(Some(text))
        | ResponseCode::Annotate(Some(text))
        | ResponseCode::Annotations(Some(text))
        | ResponseCode::MaxConvertMessages(Some(text))
        | ResponseCode::MaxConvertParts(Some(text))
        | ResponseCode::NotificationOverflow(Some(text))
        | ResponseCode::BadEvent(Some(text))
        | ResponseCode::UndefinedFilter(Some(text))
        | ResponseCode::NewName(Some(text)) => text.clone(),
        ResponseCode::MetadataMaxSize(n) => format!("METADATA MAXSIZE {n}"),
        ResponseCode::MetadataLongEntries(n) => format!("METADATA LONGENTRIES {n}"),
        _ => return None,
    };
    Some(DiagnosticText::support_only(value))
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
