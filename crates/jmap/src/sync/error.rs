//! JMAP -> AccountError conversion boundary.
//!
//! Every `crate::Error` exits the JMAP crate through
//! [`into_account_error`]. Transport-level failures from the default
//! reqwest transport delegate to `bifrost_net::into_account_error`;
//! parsed JMAP method errors, problem-details responses, set-error
//! responses, and local crate errors all funnel through
//! `AccountErrorBuilder` here with operation, scope, and
//! provider/protocol context attached.
//!
//! No `RecoveryClass` or `Fatal` is constructed in this crate. The
//! central recovery mapping in `bifrost_types::error::recovery` derives
//! recovery from `(kind, scope, operation, primary_cause)` at builder
//! `build()` time.

use std::time::Duration;

use bifrost_net::{NetErrorContext, into_account_error as net_into_account_error};
use bifrost_types::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountOperation, AttemptCause, AuthCause, AuthErrorKind, BatchFailure, BatchItemId,
    BatchSuccess, Cause, CursorScope, DiagnosticText, ErrorScope, ItemOutcome, JmapMethod,
    MutationSuccess, Protocol, ProtocolErrorKind, Provider, RequestCause, RequestErrorKind,
    ResourceKind, RetryHint, ServerCause, ServerErrorKind, StateCause, SyncEvent,
    SyncStateErrorKind, ThrottleScope, TransmissionState, TransportCause, TransportErrorKind,
    TransportKind, WireCause,
};

use crate::core::error::{JMAPError, MethodErrorType, ProblemDetails, ProblemType};
use crate::core::set::{SetError, SetErrorType};

/// Context attached at the JMAP conversion boundary. Mirrors
/// `bifrost_net::NetErrorContext` but with `protocol` fixed to
/// `Protocol::Jmap` and operation non-optional so call sites cannot
/// silently classify a non-idempotent in-flight failure with the
/// idempotent-true default of the central recovery mapper.
#[derive(Clone, Debug)]
pub(crate) struct JmapErrorContext {
    pub(crate) provider: Option<Provider>,
    pub(crate) operation: AccountOperation,
    pub(crate) scope: Option<ErrorScope>,
}

impl JmapErrorContext {
    #[must_use]
    pub(crate) fn new(operation: AccountOperation) -> Self {
        Self {
            provider: None,
            operation,
            scope: None,
        }
    }

    #[must_use]
    pub(crate) fn with_scope(mut self, scope: ErrorScope) -> Self {
        self.scope = Some(scope);
        self
    }

    #[must_use]
    pub(crate) fn cursor(operation: AccountOperation, scope: CursorScope) -> Self {
        Self::new(operation).with_scope(ErrorScope::Cursor(scope))
    }

    #[must_use]
    pub(crate) fn message(operation: AccountOperation, id: impl Into<String>) -> Self {
        Self::new(operation).with_scope(ErrorScope::Message { id: id.into() })
    }

    #[must_use]
    pub(crate) fn mailbox(operation: AccountOperation, id: impl Into<String>) -> Self {
        Self::new(operation).with_scope(ErrorScope::Mailbox { id: id.into() })
    }

    #[must_use]
    pub(crate) fn thread(operation: AccountOperation, id: impl Into<String>) -> Self {
        Self::new(operation).with_scope(ErrorScope::Thread { id: id.into() })
    }
}

/// Convert a JMAP crate error into an opaque `AccountError`. This is
/// the only conversion path from `crate::Error` to the consumer surface.
#[must_use]
pub(crate) fn into_account_error(error: crate::Error, ctx: JmapErrorContext) -> AccountError {
    match error {
        crate::Error::Transport(transport) => convert_transport(transport, ctx),
        crate::Error::Problem { details, transport } => convert_problem(*details, transport, ctx),
        crate::Error::Method(method) => convert_method(method, ctx),
        crate::Error::Set(set) => set_error_to_account_error(set, ctx, None),
        crate::Error::RequestEncode(err) => build(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(err.to_string()),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        crate::Error::ResponseDecode(err) => build(
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(err.to_string())),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        crate::Error::CallNotFound(id) => build(
            AccountErrorKind::Protocol(ProtocolErrorKind::MissingField),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(format!(
                    "call {id} not found in response"
                ))),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        crate::Error::UnexpectedMethodResponse {
            call_id,
            expected,
            actual,
        } => build(
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(format!(
                    "call {call_id} returned {actual}, expected {expected}"
                ))),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        crate::Error::IdNotFound(id) => convert_id_not_found(id, ctx),
        crate::Error::NotParsable(detail) => build(
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(detail)),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        crate::Error::InvalidUrl(detail) => build(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::InvalidArgument {
                field: Some("url"),
                message: Some(DiagnosticText::support_only(detail)),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        // The session lists no primary account for the capability we
        // need. Per `reference/jmap.md` this is an authentication-level
        // signal: the credential no longer resolves to a usable primary
        // account, so it maps to `Authentication(ReauthorizationRequired)
        // -> AuthLost` (terminal, operator must re-authorize) rather than
        // a `CapabilityChanged -> RestartAccount` reopen loop that would
        // spin re-running discovery against the same empty session.
        crate::Error::NoPrimaryAccount { capability } => build(
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
            &ctx,
        )
        .text(DiagnosticText::support_only(format!(
            "session lists no primary account for capability {capability}"
        )))
        .try_build()
        .expect("valid account error classification"),
        #[cfg(feature = "websockets")]
        crate::Error::WebSocketHandshake(err) => websocket_handshake_error(err.to_string(), ctx),
        #[cfg(feature = "websockets")]
        crate::Error::WebSocketRuntime(err) => websocket_runtime_error(err.to_string(), ctx),
        #[cfg(feature = "websockets")]
        crate::Error::WebSocketClosed => {
            websocket_runtime_error("WebSocket connection closed by peer".to_string(), ctx)
        }
        #[cfg(feature = "websockets")]
        crate::Error::WebSocketNotConnected => build(
            AccountErrorKind::Unsupported(ctx.operation),
            Cause::Request(RequestCause::Unsupported {
                operation: ctx.operation,
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification"),
        #[cfg(feature = "websockets")]
        crate::Error::WebSocketSetup(setup) => match setup {
            crate::WebSocketSetupError::Tls(message) => build(
                AccountErrorKind::Transport(TransportErrorKind::Tls),
                Cause::Transport(TransportCause::new(
                    TransportKind::Tls,
                    Some(DiagnosticText::support_only(message)),
                )),
                &ctx,
            )
            .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
            .try_build()
            .expect("valid account error classification"),
            crate::WebSocketSetupError::InvalidHeader(message) => build(
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::InvalidArgument {
                    field: Some("authorization"),
                    message: Some(DiagnosticText::support_only(message)),
                }),
                &ctx,
            )
            .try_build()
            .expect("valid account error classification"),
            crate::WebSocketSetupError::Subprotocol(message) => build(
                AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
                Cause::State(StateCause::CapabilityChanged { delta: None }),
                &ctx,
            )
            .text(DiagnosticText::support_only(message))
            .try_build()
            .expect("valid account error classification"),
        },
    }
}

/// True if `err` is a JMAP `stateMismatch` method-level error. Used by
/// the mutation pipeline to gate an `Email/set` retry after refreshing
/// the cached state. The predicate inspects only `MethodErrorType` -
/// it does not construct recovery advice.
#[must_use]
pub(crate) fn is_state_mismatch(err: &crate::Error) -> bool {
    matches!(
        err,
        crate::Error::Method(method)
            if matches!(method.error_type(), MethodErrorType::StateMismatch)
    )
}

/// Emit `SyncEvent::Terminated(AccountError)` for stream termination.
#[must_use]
pub(crate) fn terminated<T>(error: AccountError) -> SyncEvent<T> {
    SyncEvent::Terminated(error)
}

#[must_use]
pub(crate) fn terminated_from_jmap<T>(err: crate::Error, ctx: JmapErrorContext) -> SyncEvent<T> {
    terminated(into_account_error(err, ctx))
}

/// `Unsupported` for an operation that JMAP does not provide. Lets
/// stream call sites emit a Terminated event without constructing
/// `RecoveryClass` directly.
#[must_use]
pub(crate) fn unsupported_error(
    operation: AccountOperation,
    scope: Option<ErrorScope>,
    message: impl Into<String>,
) -> AccountError {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(operation),
        Cause::Request(RequestCause::Unsupported { operation }),
    )
    .protocol(Protocol::Jmap)
    .operation(operation)
    .text(DiagnosticText::support_only(message));
    if let Some(scope) = scope {
        builder = builder.scope(scope);
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

/// A send-as mailbox id that was not present in the successfully seeded
/// foreign-account routing table. Consumers receive these ids from foreign
/// membership ownership, so an unknown id is a malformed request.
#[must_use]
pub(crate) fn send_as_unknown_account(mailbox: &bifrost_types::MailboxId) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some("send_as.mailbox"),
            message: Some(DiagnosticText::support_only(format!(
                "unknown foreign submission account {}",
                mailbox.0
            ))),
        }),
    )
    .protocol(Protocol::Jmap)
    .operation(AccountOperation::Send)
    .try_build()
    .expect("valid account error classification")
}

/// Convenience for stream call sites that need to emit a
/// `SyncEvent::Terminated(Unsupported(op))`. Callers pass the
/// operation the stream is performing; the helper previously
/// hard-coded `AccountOperation::Discover` for every caller,
/// which mis-classified inventory / hydrate / blob-range streams
/// and produced consumer-visible "discovery unsupported" copy for
/// operations that have nothing to do with discovery.
#[must_use]
pub(crate) fn terminated_unsupported<T>(
    operation: AccountOperation,
    scope: Option<ErrorScope>,
    message: impl Into<String>,
) -> SyncEvent<T> {
    terminated(unsupported_error(operation, scope, message))
}

/// Stream-side variant for protocol-shape failures that are
/// `Protocol(ContractViolation)`, not `Unsupported`. Pagination
/// overflows, response-shape mismatches, and similar producer-side
/// limits that the protocol cannot represent in a request live here.
/// `Discover`-coded `Unsupported` was the wrong consumer-facing
/// classification - the server is not refusing the operation; the
/// library cannot encode the request shape.
#[must_use]
pub(crate) fn terminated_contract_violation<T>(
    operation: AccountOperation,
    scope: Option<ErrorScope>,
    message: impl Into<String>,
) -> SyncEvent<T> {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Jmap,
            detail: Some(DiagnosticText::support_only(message)),
        }),
    )
    .protocol(Protocol::Jmap)
    .operation(operation);
    if let Some(scope) = scope {
        builder = builder.scope(scope);
    }
    terminated(
        builder
            .try_build()
            .expect("valid account error classification"),
    )
}

/// Per-item classification for `Email/set` outcomes. Returns
/// `Succeeded(Skipped)` when the set error is an absorbed notFound
/// (the per-item NotFound absorption policy for bulk mutations); all
/// other set errors map to a `Failed` lane with a full `AccountError`.
#[must_use]
pub(crate) fn classify_set_item(
    set_error: SetError<String>,
    ctx: JmapErrorContext,
    item: BatchItemId,
    item_scope: Option<ErrorScope>,
) -> ItemOutcome<MutationSuccess> {
    let absorb = matches!(
        set_error.error_type(),
        &SetErrorType::NotFound | &SetErrorType::BlobNotFound
    ) && matches!(
        ctx.operation,
        AccountOperation::UpdateFlags
            | AccountOperation::BulkMove
            | AccountOperation::BulkDestroy
            | AccountOperation::SetKeyword
            | AccountOperation::SetIsRead
            | AccountOperation::AddToContainer
            | AccountOperation::RemoveFromContainer
    );
    if absorb {
        ItemOutcome::Succeeded(BatchSuccess::new(item, MutationSuccess::Skipped))
    } else {
        let error = set_error_to_account_error(set_error, ctx, item_scope);
        ItemOutcome::Failed(BatchFailure::new(item, error))
    }
}

/// Convert a JMAP `SetError` to an `AccountError`. When `item_scope`
/// is present, the resource-bearing classifications (NotFound,
/// PermissionDenied) derive their resource from it.
#[must_use]
pub(crate) fn set_error_to_account_error(
    set_error: SetError<String>,
    ctx: JmapErrorContext,
    item_scope: Option<ErrorScope>,
) -> AccountError {
    let scope_for_resource = item_scope.as_ref().or(ctx.scope.as_ref());
    let error_type = set_error.error_type().clone();
    let (kind, primary) = match error_type {
        SetErrorType::Forbidden
        | SetErrorType::ForbiddenFrom
        | SetErrorType::ForbiddenMailFrom
        | SetErrorType::ForbiddenToSend => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: resource_from_scope(scope_for_resource),
            }),
        ),
        SetErrorType::OverQuota => (
            AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
            Cause::Server(ServerCause::QuotaExhausted { retry_hint: None }),
        ),
        SetErrorType::RateLimit => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint: None }),
        ),
        SetErrorType::NotFound | SetErrorType::BlobNotFound => {
            let wire = if matches!(set_error.error_type(), &SetErrorType::BlobNotFound) {
                JmapMethod::BlobNotFound
            } else {
                JmapMethod::NotFound
            };
            if let Some(resource) = resource_from_scope(scope_for_resource) {
                let id = id_from_scope(scope_for_resource);
                (
                    AccountErrorKind::NotFound(resource),
                    Cause::Request(RequestCause::NotFound { what: resource, id }),
                )
            } else {
                // No scope tells us what was missing. Classify as a
                // typed protocol-shape error so the kind/cause pair
                // matches the strict builder invariants. Without the
                // resource scope the previous shape was
                // `Server(Error { status: None }) + Cause::Wire(_)`,
                // which doesn't pass `kind_matches_cause`.
                (
                    AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
                    Cause::Wire(WireCause::Jmap(wire)),
                )
            }
        }
        SetErrorType::AlreadyExists => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
        ),
        SetErrorType::TooLarge
        | SetErrorType::TooManyKeywords
        | SetErrorType::TooManyMailboxes
        | SetErrorType::TooManyRecipients
        | SetErrorType::InvalidPatch
        | SetErrorType::InvalidProperties
        | SetErrorType::InvalidEmail
        | SetErrorType::InvalidRecipients
        | SetErrorType::NoRecipients
        | SetErrorType::InvalidScript => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(set_error.to_string()),
            }),
        ),
        // The following set-error variants are legitimate JMAP wire
        // responses describing protocol-shape state conflicts (id is
        // on the destroy list, singleton already exists, mailbox has
        // children, etc.). They classify under `Protocol(Unknown)`
        // paired with a typed `Cause::Wire(JmapMethod::*)` so the
        // strict `kind_matches_cause` rule holds. Recovery for
        // `Protocol(Unknown)` is `UnknownPermanent`, which is the
        // right consumer-facing outcome: the operation cannot retry
        // and the engine has no automatic remediation.
        SetErrorType::WillDestroy => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::WillDestroy)),
        ),
        SetErrorType::Singleton => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::Singleton)),
        ),
        SetErrorType::ScriptIsActive => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::ScriptIsActive)),
        ),
        SetErrorType::CannotUnsend => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::CannotUnsend)),
        ),
        SetErrorType::MailboxHasChild => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::MailboxHasChild)),
        ),
        SetErrorType::MailboxHasEmail => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::MailboxHasEmail)),
        ),
        SetErrorType::Other(code) => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::Unknown { code })),
        ),
    };

    let mut builder = build_with(&ctx, kind, primary)
        // Set errors are returned in a successful JMAP response, which
        // already crossed the side-effect boundary.
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::Acknowledged,
        )));
    // Push the typed wire cause as a forensic layer for all non-Other
    // variants whose primary cause is not already a WireCause. For
    // WillDestroy/Singleton/etc. the primary IS the WireCause so we
    // skip to avoid duplication; for Forbidden/OverQuota/RateLimit/etc.
    // the primary is an Access/Server cause and we add the wire signal.
    let primary_was_wire = matches!(
        set_error.error_type(),
        SetErrorType::WillDestroy
            | SetErrorType::Singleton
            | SetErrorType::ScriptIsActive
            | SetErrorType::CannotUnsend
            | SetErrorType::MailboxHasChild
            | SetErrorType::MailboxHasEmail
            | SetErrorType::Other(_)
    );
    if !primary_was_wire
        && let Some(wire) = set_error_type_to_jmap_method(set_error.error_type().clone())
    {
        builder = builder.push_cause(Cause::Wire(WireCause::Jmap(wire)));
    }
    if let Some(description) = set_error.description() {
        builder = builder.text(DiagnosticText::support_only(description));
    }
    if matches!(set_error.error_type(), &SetErrorType::RateLimit) {
        builder = builder.throttle_scope(ThrottleScope::Account);
    }
    if let Some(scope) = item_scope {
        builder = builder.scope(scope);
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

/// Map a `SetErrorType` to the corresponding typed `JmapMethod` wire
/// cause variant. Returns `None` only for `SetErrorType::Other` (no
/// stable wire code) and the resource-classified cases handled above
/// whose primary cause is already a `WireCause` (`WillDestroy`,
/// `Singleton`, etc.). Callers that already check `primary_was_wire`
/// will not call this for those variants.
fn set_error_type_to_jmap_method(error_type: SetErrorType) -> Option<JmapMethod> {
    match error_type {
        SetErrorType::Forbidden
        | SetErrorType::ForbiddenFrom
        | SetErrorType::ForbiddenMailFrom
        | SetErrorType::ForbiddenToSend => Some(JmapMethod::Forbidden),
        SetErrorType::OverQuota => Some(JmapMethod::OverQuota),
        SetErrorType::RateLimit => Some(JmapMethod::RateLimit),
        SetErrorType::NotFound => Some(JmapMethod::NotFound),
        SetErrorType::BlobNotFound => Some(JmapMethod::BlobNotFound),
        SetErrorType::AlreadyExists => Some(JmapMethod::AlreadyExists),
        SetErrorType::TooLarge => Some(JmapMethod::TooLarge),
        SetErrorType::TooManyKeywords => Some(JmapMethod::TooManyKeywords),
        SetErrorType::TooManyMailboxes => Some(JmapMethod::TooManyMailboxes),
        SetErrorType::TooManyRecipients => Some(JmapMethod::TooManyRecipients),
        SetErrorType::InvalidPatch => Some(JmapMethod::InvalidPatch),
        SetErrorType::InvalidProperties => Some(JmapMethod::InvalidProperties),
        SetErrorType::InvalidEmail => Some(JmapMethod::InvalidEmail),
        SetErrorType::InvalidRecipients => Some(JmapMethod::InvalidRecipients),
        SetErrorType::InvalidScript => Some(JmapMethod::InvalidScript),
        SetErrorType::NoRecipients => Some(JmapMethod::NoRecipients),
        // WillDestroy/Singleton/ScriptIsActive/CannotUnsend/MailboxHasChild/
        // MailboxHasEmail primary IS the WireCause; callers skip these.
        // Other(code) carries the unknown wire code already as the
        // primary `Cause::Wire(JmapMethod::Unknown { code })`; no
        // forensic duplicate is added.
        SetErrorType::WillDestroy
        | SetErrorType::Singleton
        | SetErrorType::ScriptIsActive
        | SetErrorType::CannotUnsend
        | SetErrorType::MailboxHasChild
        | SetErrorType::MailboxHasEmail
        | SetErrorType::Other(_) => None,
    }
}

/// Build a `SyncState(ScopeRevoked)` error scoped to one foreign
/// (shared/delegate) account mailbox's `Folder` scope. With the scope
/// attached, the central `derive` resolves it to
/// `Engine(DisableScope(scope))` - the engine quarantines just that
/// cursor scope without escalating account-wide. The owning account
/// rides as support-only diagnostic text for telemetry.
#[must_use]
pub(crate) fn jmap_scope_revoked(
    scope: CursorScope,
    owner: &bifrost_types::MailboxId,
    operation: AccountOperation,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked),
        Cause::State(StateCause::ScopeRevoked),
    )
    .protocol(Protocol::Jmap)
    .operation(operation)
    .scope(ErrorScope::Cursor(scope))
    .text(DiagnosticText::support_only(format!(
        "shared account access revoked (owner {})",
        owner.0,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// A `Folder` scope that parses as a foreign (shared/delegate) mailbox
/// but whose account is no longer registered (the share/delegation
/// disappeared from the session since the cursor was seeded). The scope
/// can no longer route to a real account handle, and falling back to the
/// primary account would conflate the foreign mailbox id with a primary
/// one. Classify as `SyncState(ScopeRevoked)` scoped to the cursor so the
/// engine quarantines just this scope (`Engine(DisableScope)`) rather
/// than misrouting it to the primary mailbox or escalating account-wide.
#[must_use]
pub(crate) fn unregistered_foreign_scope(
    scope: CursorScope,
    operation: AccountOperation,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked),
        Cause::State(StateCause::ScopeRevoked),
    )
    .protocol(Protocol::Jmap)
    .operation(operation)
    .scope(ErrorScope::Cursor(scope))
    .text(DiagnosticText::support_only(
        "foreign account for this mailbox scope is no longer registered in the session",
    ))
    .try_build()
    .expect("valid account error classification")
}

/// Map a per-scope failure for a foreign (shared/delegate) account
/// mailbox. When the scope belongs to a shared account (`owner.is_some()`)
/// and the failure classifies as permission-denied (the class that would
/// otherwise derive terminal `NoPermission` - JMAP `forbidden`),
/// quarantine just this foreign scope via `ScopeRevoked` instead of
/// escalating account-wide. A primary scope (`owner == None`), or any
/// non-permission failure, flows through the unchanged `into_account_error`
/// path - a primary-account permission loss is a genuine account-level
/// signal.
#[must_use]
pub(crate) fn shared_scope_error(
    err: crate::Error,
    scope: &CursorScope,
    owner: Option<&bifrost_types::MailboxId>,
    ctx: JmapErrorContext,
) -> AccountError {
    if let Some(owner) = owner {
        let account_error = into_account_error(err, ctx.clone());
        if matches!(
            account_error.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        ) {
            return jmap_scope_revoked(scope.clone(), owner, ctx.operation);
        }
        return account_error;
    }
    into_account_error(err, ctx)
}

// ---- internal helpers --------------------------------------------------

fn build(kind: AccountErrorKind, primary: Cause, ctx: &JmapErrorContext) -> AccountErrorBuilder {
    build_with(ctx, kind, primary)
}

fn build_with(
    ctx: &JmapErrorContext,
    kind: AccountErrorKind,
    primary: Cause,
) -> AccountErrorBuilder {
    let mut builder = AccountErrorBuilder::new(kind, primary)
        .protocol(Protocol::Jmap)
        .operation(ctx.operation);
    if let Some(provider) = ctx.provider {
        builder = builder.provider(provider);
    }
    if let Some(scope) = ctx.scope.clone() {
        builder = builder.scope(scope);
    }
    builder
}

fn resource_from_scope(scope: Option<&ErrorScope>) -> Option<ResourceKind> {
    match scope? {
        ErrorScope::Message { .. } => Some(ResourceKind::Message),
        ErrorScope::Mailbox { .. } => Some(ResourceKind::Mailbox),
        ErrorScope::Thread { .. } => Some(ResourceKind::Thread),
        ErrorScope::Calendar { .. } | ErrorScope::CalendarCollection => {
            Some(ResourceKind::Calendar)
        }
        ErrorScope::Contact { .. } | ErrorScope::ContactCollection => Some(ResourceKind::Contact),
        ErrorScope::Account | ErrorScope::Cursor(_) => None,
        // `ErrorScope` is `#[non_exhaustive]` in `bifrost-types`, so
        // this catch-all is required for the crate to compile after a
        // new variant lands upstream. Any new variant must be handled
        // explicitly above with the appropriate `ResourceKind` mapping
        // (or an explicit `None`) before relying on this fall-through.
        _ => None,
    }
}

fn id_from_scope(scope: Option<&ErrorScope>) -> Option<String> {
    match scope? {
        ErrorScope::Message { id }
        | ErrorScope::Mailbox { id }
        | ErrorScope::Thread { id }
        | ErrorScope::Calendar { id }
        | ErrorScope::Contact { id } => Some(id.clone()),
        ErrorScope::Account
        | ErrorScope::Cursor(_)
        | ErrorScope::CalendarCollection
        | ErrorScope::ContactCollection => None,
        // `ErrorScope` is `#[non_exhaustive]`; see `resource_from_scope`
        // for the explicit-handling discipline new variants must follow.
        _ => None,
    }
}

fn convert_transport(
    transport: crate::core::transport::TransportError,
    ctx: JmapErrorContext,
) -> AccountError {
    if let Some(net) = transport.net {
        net_into_account_error(
            net,
            NetErrorContext {
                provider: ctx.provider,
                protocol: Protocol::Jmap,
                operation: ctx.operation,
                scope: ctx.scope,
            },
        )
    } else {
        // Custom transport without bifrost-net evidence. No attempt
        // cause: we have no wire-level signal about transmission.
        build(
            AccountErrorKind::Transport(TransportErrorKind::Network),
            Cause::Transport(TransportCause::new(
                TransportKind::Network,
                Some(DiagnosticText::support_only(transport.message.clone())),
            )),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification")
    }
}

fn convert_problem(
    details: ProblemDetails,
    transport: Option<crate::core::transport::TransportError>,
    ctx: JmapErrorContext,
) -> AccountError {
    let net_status = transport
        .as_ref()
        .and_then(|t| status_from_net(t.net.as_ref()));
    let status_u32 = details.status().or(net_status);
    let status_u16 = status_u32.and_then(|s| u16::try_from(s).ok());
    let retry_hint = transport
        .as_ref()
        .and_then(|t| retry_after_from_net(t.net.as_ref()))
        .map(RetryHint::After);

    let (kind, primary) = match (details.error(), status_u16) {
        (ProblemType::JMAP(JMAPError::Limit), _) => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint }),
        ),
        (ProblemType::JMAP(JMAPError::UnknownCapability), _) => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
        ),
        // The server claims our request was not JSON or not a valid
        // JMAP request envelope; the client side did send valid JSON
        // (any deserialization issue is caught locally before the
        // request leaves), so the server's claim is a protocol contract
        // violation, not a client bug. Per jmap-F2.
        (ProblemType::JMAP(JMAPError::NotJSON | JMAPError::NotRequest), _) => (
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(details.to_string())),
            }),
        ),
        (ProblemType::Other(_), Some(400 | 422)) => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(details.to_string()),
            }),
        ),
        (ProblemType::Other(_), Some(401)) => (
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
        ),
        (ProblemType::Other(_), Some(403)) => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: resource_from_scope(ctx.scope.as_ref()),
            }),
        ),
        (ProblemType::Other(_), Some(404)) => {
            if let Some(resource) = resource_from_scope(ctx.scope.as_ref()) {
                let id = id_from_scope(ctx.scope.as_ref());
                (
                    AccountErrorKind::NotFound(resource),
                    Cause::Request(RequestCause::NotFound { what: resource, id }),
                )
            } else {
                // Scope-less HTTP 404. The three scope-less not-found
                // shapes in this crate are deliberately distinct by wire
                // source, not unified, because each preserves a different
                // diagnostic signal while all three resolve terminal:
                //   - a bare HTTP 404 (here) is a transport-level
                //     resource refusal -> `Server(Error{404})` ->
                //     `ProviderRefused`;
                //   - a JMAP `notFound` SetError without a resource scope
                //     -> `Protocol(Unknown)` -> `UnknownPermanent`
                //     (`set_error_to_account_error`);
                //   - a local "id absent from response" shape error ->
                //     `Protocol(MissingField)` ->
                //     `ProviderContractViolation` (`convert_id_not_found`).
                // The message keys differ so support exports can tell the
                // three apart; recovery is terminal in every case, so the
                // engine behaves identically.
                (
                    AccountErrorKind::Server(ServerErrorKind::Error { status: Some(404) }),
                    Cause::Server(ServerCause::Error { status: Some(404) }),
                )
            }
        }
        (ProblemType::Other(_), Some(409)) => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
        ),
        (ProblemType::Other(_), Some(410)) if matches!(ctx.scope, Some(ErrorScope::Cursor(_))) => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        ),
        (ProblemType::Other(_), Some(429)) => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint }),
        ),
        (ProblemType::Other(_), Some(status)) if (500..=599).contains(&status) => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint }),
        ),
        (ProblemType::Other(_), Some(status)) => (
            AccountErrorKind::Server(ServerErrorKind::Error {
                status: Some(status),
            }),
            Cause::Server(ServerCause::Error {
                status: Some(status),
            }),
        ),
        (ProblemType::Other(code), None) => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::Unknown { code: code.clone() })),
        ),
    };

    let mut builder = build_with(&ctx, kind, primary)
        .push_cause(Cause::Attempt(AttemptCause::new(
            TransmissionState::Acknowledged,
        )))
        .push_cause(Cause::Wire(wire_for_problem(details.error())));
    if let Some(status) = status_u16 {
        builder = builder.status(Some(status));
    }
    if let Some(request_id) = details.request_id() {
        builder = builder.request_id(request_id.to_string());
    }
    if let Some(title) = details.title() {
        builder = builder.text(DiagnosticText::support_only(title.to_string()));
    }
    if let Some(detail) = details.detail() {
        builder = builder.text(DiagnosticText::support_only(detail.to_string()));
    }
    if let Some(limit) = details.limit() {
        builder = builder.text(DiagnosticText::support_only(format!("limit: {limit}")));
    }
    if matches!(details.error(), ProblemType::JMAP(JMAPError::Limit)) {
        builder = builder.throttle_scope(ThrottleScope::Account);
    }
    if matches!(details.error(), ProblemType::Other(_)) && matches!(status_u16, Some(429)) {
        builder = builder.throttle_scope(ThrottleScope::Account);
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

fn wire_for_problem(error: &ProblemType) -> WireCause {
    match error {
        ProblemType::JMAP(JMAPError::UnknownCapability) => {
            WireCause::Jmap(JmapMethod::UnknownCapability)
        }
        ProblemType::JMAP(JMAPError::NotJSON) => WireCause::Jmap(JmapMethod::NotJson),
        ProblemType::JMAP(JMAPError::NotRequest) => WireCause::Jmap(JmapMethod::NotRequest),
        ProblemType::JMAP(JMAPError::Limit) => WireCause::Jmap(JmapMethod::Limit),
        ProblemType::Other(code) => WireCause::Jmap(JmapMethod::Unknown { code: code.clone() }),
    }
}

fn status_from_net(net: Option<&bifrost_net::Error>) -> Option<u32> {
    match net? {
        bifrost_net::Error::Status { code, .. } => Some(u32::from(code.as_u16())),
        bifrost_net::Error::RateLimited { final_response, .. } => {
            Some(u32::from(final_response.status.as_u16()))
        }
        bifrost_net::Error::RetryBudgetExhausted {
            final_response: Some(final_response),
            ..
        } => Some(u32::from(final_response.status.as_u16())),
        _ => None,
    }
}

fn retry_after_from_net(net: Option<&bifrost_net::Error>) -> Option<Duration> {
    match net? {
        bifrost_net::Error::RateLimited { retry_after, .. } => *retry_after,
        _ => None,
    }
}

fn convert_method(method: crate::core::error::MethodError, ctx: JmapErrorContext) -> AccountError {
    let method_type = method.error_type();
    // A `SyncState(CursorInvalid)` kind requires an `ErrorScope::Cursor`
    // per the post-Phase-5 builder contract. JMAP method-level cursor
    // errors (`cannotCalculateChanges`, `anchorNotFound`,
    // `tooManyChanges`) classify as `CursorInvalid` only when the caller
    // threaded a cursor scope through `JmapErrorContext::cursor(...)`;
    // otherwise the server returned a cursor-specific error for a
    // non-cursored request, which is a contract violation and routes
    // there instead.
    let has_cursor_scope = matches!(ctx.scope, Some(ErrorScope::Cursor(_)));
    let (kind, primary, wire) = match method_type {
        MethodErrorType::ServerUnavailable => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
            JmapMethod::ServerUnavailable,
        ),
        MethodErrorType::ServerFail => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint: None }),
            JmapMethod::ServerFail,
        ),
        MethodErrorType::ServerPartialFail => (
            AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
            Cause::Wire(WireCause::Jmap(JmapMethod::ServerPartialFail)),
            JmapMethod::ServerPartialFail,
        ),
        MethodErrorType::UnknownMethod => (
            AccountErrorKind::Unsupported(ctx.operation),
            Cause::Request(RequestCause::Unsupported {
                operation: ctx.operation,
            }),
            JmapMethod::UnknownMethod,
        ),
        MethodErrorType::InvalidArguments => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("invalidArguments".to_string()),
            }),
            JmapMethod::InvalidArguments,
        ),
        MethodErrorType::InvalidResultReference => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("invalidResultReference".to_string()),
            }),
            JmapMethod::InvalidResultReference,
        ),
        MethodErrorType::Forbidden => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: resource_from_scope(ctx.scope.as_ref()),
            }),
            JmapMethod::Forbidden,
        ),
        MethodErrorType::AccountNotFound => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
            JmapMethod::AccountNotFound,
        ),
        MethodErrorType::FromAccountNotFound => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
            JmapMethod::FromAccountNotFound,
        ),
        MethodErrorType::AccountNotSupportedByMethod => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
            JmapMethod::AccountNotSupportedByMethod,
        ),
        MethodErrorType::FromAccountNotSupportedByMethod => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged { delta: None }),
            JmapMethod::FromAccountNotSupportedByMethod,
        ),
        MethodErrorType::AccountReadOnly => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: resource_from_scope(ctx.scope.as_ref()),
            }),
            JmapMethod::AccountReadOnly,
        ),
        MethodErrorType::RequestTooLarge => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("requestTooLarge".to_string()),
            }),
            JmapMethod::RequestTooLarge,
        ),
        MethodErrorType::CannotCalculateChanges if has_cursor_scope => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
            JmapMethod::CannotCalculateChanges,
        ),
        MethodErrorType::CannotCalculateChanges => (
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::Jmap(JmapMethod::CannotCalculateChanges)),
            JmapMethod::CannotCalculateChanges,
        ),
        MethodErrorType::StateMismatch => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
            JmapMethod::StateMismatch,
        ),
        MethodErrorType::AlreadyExists => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
            JmapMethod::AlreadyExists,
        ),
        MethodErrorType::AnchorNotFound if has_cursor_scope => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
            JmapMethod::AnchorNotFound,
        ),
        MethodErrorType::AnchorNotFound => (
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::Jmap(JmapMethod::AnchorNotFound)),
            JmapMethod::AnchorNotFound,
        ),
        MethodErrorType::UnsupportedSort => (
            AccountErrorKind::Unsupported(ctx.operation),
            Cause::Request(RequestCause::Unsupported {
                operation: ctx.operation,
            }),
            JmapMethod::UnsupportedSort,
        ),
        MethodErrorType::UnsupportedFilter => (
            AccountErrorKind::Unsupported(ctx.operation),
            Cause::Request(RequestCause::Unsupported {
                operation: ctx.operation,
            }),
            JmapMethod::UnsupportedFilter,
        ),
        MethodErrorType::TooManyChanges if has_cursor_scope => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
            JmapMethod::TooManyChanges,
        ),
        MethodErrorType::TooManyChanges => (
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::Jmap(JmapMethod::TooManyChanges)),
            JmapMethod::TooManyChanges,
        ),
        MethodErrorType::Other(code) => {
            let unknown = JmapMethod::Unknown { code: code.clone() };
            (
                AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
                Cause::Wire(WireCause::Jmap(unknown.clone())),
                unknown,
            )
        }
    };

    let mut builder = build_with(&ctx, kind, primary).push_cause(Cause::Attempt(
        AttemptCause::new(TransmissionState::Acknowledged),
    ));
    // Push the wire cause as a forensic-only layer only if it differs
    // from the primary cause already on the chain (ServerPartialFail
    // and Other already use Wire as primary).
    let primary_was_wire = matches!(
        method_type,
        MethodErrorType::ServerPartialFail | MethodErrorType::Other(_)
    );
    if !primary_was_wire {
        builder = builder.push_cause(Cause::Wire(WireCause::Jmap(wire)));
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

/// An id the server listed in a `/get` response's `notFound`: the object
/// is gone, or this account cannot reach it. Terminal for the id, which
/// is the honest answer - re-requesting it will produce the same
/// `notFound`.
#[must_use]
pub(crate) fn get_id_not_found(id: impl Into<String>, ctx: JmapErrorContext) -> AccountError {
    convert_id_not_found(id.into(), ctx)
}

/// An id that a `/get` answered in NEITHER `list` nor `notFound`.
///
/// RFC 8620 s5.1 requires a conforming server to account for every
/// requested id in exactly one of the two, so an unanswered id is a
/// contract breach - but it must not classify as
/// `Protocol(ContractViolation)`, which derives the terminal
/// `ProviderContractViolation` and would drop the id for good. `/get` is
/// idempotent and the omission says nothing about the object, so
/// `Protocol(PartialResponse)` is the right class: the shared recovery
/// mapping retries it and the next `/get` normally answers. Transmission
/// evidence is `Acknowledged` - the method response itself arrived, only
/// this id's answer is missing.
///
/// This is the same shape `bifrost-graph` uses for a `$batch` request
/// that comes back with fewer responses than it submitted.
#[must_use]
pub(crate) fn get_id_unanswered(id: &str, ctx: JmapErrorContext) -> AccountError {
    build(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Jmap,
            detail: Some(DiagnosticText::support_only(format!(
                "requested id {id} appeared in neither `list` nor `notFound`"
            ))),
        }),
        &ctx,
    )
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// An `Email/set` response that accounted for a submitted update or destroy
/// id in neither its success nor its error map. RFC 8620 requires exactly
/// one answer per submitted id, but the omission says nothing about whether
/// the write landed. The method response did arrive, so preserve
/// acknowledged transmission evidence and let non-idempotent work reconcile.
#[must_use]
pub(crate) fn set_id_unanswered(id: &str, ctx: JmapErrorContext) -> AccountError {
    build(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Jmap,
            detail: Some(DiagnosticText::support_only(format!(
                "submitted Email/set id {id} appeared in neither a success nor an error map"
            ))),
        }),
        &ctx,
    )
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .try_build()
    .expect("valid account error classification")
}

fn convert_id_not_found(id: String, ctx: JmapErrorContext) -> AccountError {
    if let Some(resource) = resource_from_scope(ctx.scope.as_ref()) {
        build(
            AccountErrorKind::NotFound(resource),
            Cause::Request(RequestCause::NotFound {
                what: resource,
                id: Some(id),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification")
    } else {
        build(
            AccountErrorKind::Protocol(ProtocolErrorKind::MissingField),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(format!(
                    "id not found in response: {id}"
                ))),
            }),
            &ctx,
        )
        .try_build()
        .expect("valid account error classification")
    }
}

#[cfg(feature = "websockets")]
fn websocket_runtime_error(message: String, ctx: JmapErrorContext) -> AccountError {
    // Per the plan, post-handshake WebSocket errors classify as
    // `Protocol(PartialResponse)` with `Attempt(Acknowledged)`. The
    // pre-handshake `WebSocketSetup(Tls)` arm is handled in the main
    // `match` and routes through `Transport(Tls) + Unsent`. This
    // helper covers stream-level errors and post-handshake closes.
    build(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Jmap,
            detail: Some(DiagnosticText::support_only(message)),
        }),
        &ctx,
    )
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .try_build()
    .expect("valid account error classification")
}

/// Pre-handshake WebSocket failure. Classification is
/// `Transport(Network) + Attempt(Unsent)` - no bytes from the JMAP
/// request itself crossed the side-effect boundary. The blanket
/// `From<tokio_websockets::Error>` impl that previously routed every
/// websocket error through `websocket_runtime_error` mis-classified
/// handshake-time TCP/TLS drops.
#[cfg(feature = "websockets")]
fn websocket_handshake_error(message: String, ctx: JmapErrorContext) -> AccountError {
    build(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause::new(
            TransportKind::Network,
            Some(DiagnosticText::support_only(message)),
        )),
        &ctx,
    )
    .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
    .try_build()
    .expect("valid account error classification")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::error::{MethodError, ProblemDetails};
    use bifrost_types::{
        AccessErrorKind, AccountErrorKind, AccountOperation, ProtocolErrorKind, RecoveryClass,
        RetryDisposition, ServerErrorKind, SyncStateErrorKind, ThrottleScope,
    };

    fn method_error(kind: &str) -> crate::Error {
        let err: MethodError = serde_json::from_str(&format!(r#"{{"type":"{kind}"}}"#)).unwrap();
        crate::Error::Method(err)
    }

    fn problem_with_status(error: ProblemType, status: Option<u32>) -> crate::Error {
        crate::Error::Problem {
            details: Box::new(ProblemDetails::new(error, status, None, None, None, None)),
            transport: None,
        }
    }

    #[test]
    fn host_attachment_unsupported() {
        // JMAP has no cloud-drive hosting; the leg builds an
        // `Unsupported(HostAttachment)` error via the shared helper.
        let err = unsupported_error(
            AccountOperation::HostAttachment,
            None,
            "JMAP has no cloud-drive attachment hosting",
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::HostAttachment)
        );
    }

    #[test]
    fn send_as_unknown_account_is_malformed() {
        let err = send_as_unknown_account(&bifrost_types::MailboxId("foreign-id".to_string()));
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
        assert_eq!(err.operation(), Some(AccountOperation::Send));
        assert!(err.recovery().is_terminal());
    }

    #[test]
    fn state_mismatch_maps_to_concurrency_conflict() {
        let err = into_account_error(
            method_error("stateMismatch"),
            JmapErrorContext::new(AccountOperation::UpdateFlags),
        );
        assert_eq!(err.kind(), &AccountErrorKind::ConcurrencyConflict);
        match err.recovery() {
            RecoveryClass::Retry(advice) => {
                assert_eq!(advice.disposition, RetryDisposition::AfterStateRefresh);
            }
            other => panic!("expected Retry, got {other:?}"),
        }
        // Last-but-one cause carries the wire JmapMethod::StateMismatch.
        let wire_seen = err
            .chain()
            .iter()
            .any(|c| matches!(c, Cause::Wire(WireCause::Jmap(JmapMethod::StateMismatch))));
        assert!(wire_seen, "wire cause missing");
    }

    #[test]
    fn cannot_calculate_changes_restarts_scope() {
        let scope = CursorScope::Type(bifrost_types::ObjectType::Email);
        let err = into_account_error(
            method_error("cannotCalculateChanges"),
            JmapErrorContext::cursor(AccountOperation::SyncChanges, scope.clone()),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        );
        assert!(err.recovery().requires_engine_action());
    }

    #[test]
    fn cannot_calculate_changes_without_scope_is_contract_violation() {
        // Post-Phase-5: `SyncState(CursorInvalid)` requires an
        // `ErrorScope::Cursor`. A `cannotCalculateChanges` arriving on a
        // context that lacks the cursor scope means the server returned
        // a cursor-specific error for a non-cursored request, which is
        // a `Protocol(ContractViolation)`, not a cursor restart.
        let err = into_account_error(
            method_error("cannotCalculateChanges"),
            JmapErrorContext::new(AccountOperation::SyncChanges),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        );
        assert!(err.recovery().is_terminal());
    }

    #[test]
    fn server_partial_fail_reconciles_for_send() {
        let err = into_account_error(
            method_error("serverPartialFail"),
            JmapErrorContext::new(AccountOperation::Send),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
        );
        assert!(err.recovery().requires_reconciliation());
    }

    #[test]
    fn server_partial_fail_retries_for_idempotent_update() {
        let err = into_account_error(
            method_error("serverPartialFail"),
            JmapErrorContext::new(AccountOperation::UpdateFlags),
        );
        assert!(err.recovery().is_retryable());
    }

    #[test]
    fn forbidden_maps_to_no_permission() {
        let err = into_account_error(
            method_error("forbidden"),
            JmapErrorContext::new(AccountOperation::UpdateFlags),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        );
        assert!(err.recovery().is_terminal());
        assert!(matches!(err.recovery(), RecoveryClass::NoPermission { .. }));
    }

    #[test]
    fn shared_account_permission_loss_derives_disable_scope() {
        use bifrost_types::{EngineDirective, MailboxId};
        let scope = CursorScope::Folder(super::super::foreign::encode_foreign("acct-9", "mbx-3"));
        let owner = MailboxId("acct-9".to_string());
        let err = shared_scope_error(
            method_error("forbidden"),
            &scope,
            Some(&owner),
            JmapErrorContext::cursor(AccountOperation::SyncChanges, scope.clone()),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
        );
        assert_eq!(
            *err.recovery(),
            RecoveryClass::Engine(EngineDirective::DisableScope(scope.clone()))
        );
        assert_eq!(err.scope(), Some(&ErrorScope::Cursor(scope)));
    }

    #[test]
    fn primary_account_permission_loss_stays_terminal() {
        // A primary scope passes no owner: the same `forbidden` stays
        // terminal `NoPermission`, not quarantine.
        let scope = CursorScope::Type(bifrost_types::ObjectType::Email);
        let err = shared_scope_error(
            method_error("forbidden"),
            &scope,
            None,
            JmapErrorContext::cursor(AccountOperation::SyncChanges, scope.clone()),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        );
        assert!(err.recovery().is_terminal());
        assert!(matches!(err.recovery(), RecoveryClass::NoPermission { .. }));
    }

    #[test]
    fn unknown_method_maps_to_unsupported_operation() {
        let err = into_account_error(
            method_error("unknownMethod"),
            JmapErrorContext::new(AccountOperation::Search),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Unsupported(AccountOperation::Search)
        );
    }

    #[test]
    fn jmap_limit_problem_maps_rate_limited() {
        let err = into_account_error(
            problem_with_status(ProblemType::JMAP(JMAPError::Limit), Some(429)),
            JmapErrorContext::new(AccountOperation::Send),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Server(ServerErrorKind::RateLimited)
        );
        match err.recovery() {
            RecoveryClass::Retry(advice) => {
                assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
            }
            // Acknowledged + non-idempotent Send is still retryable
            // because the server rejected before commit (the 429 is
            // an acknowledged terminal response, no side effect).
            other => panic!("expected Retry, got {other:?}"),
        }
    }

    #[test]
    fn no_primary_account_maps_auth_lost() {
        // `reference/jmap.md` documents NoPrimaryAccount as
        // `Authentication(ReauthorizationRequired) -> AuthLost`, not a
        // `CapabilityChanged -> RestartAccount` reopen loop.
        let err = into_account_error(
            crate::Error::NoPrimaryAccount {
                capability: "urn:ietf:params:jmap:mail",
            },
            JmapErrorContext::new(AccountOperation::SyncInventory),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
        );
        assert!(matches!(err.recovery(), RecoveryClass::AuthLost));
    }

    #[test]
    fn unregistered_foreign_scope_disables_scope() {
        use bifrost_types::EngineDirective;
        let scope =
            CursorScope::Folder(super::super::foreign::encode_foreign("acct-gone", "mbx-1"));
        let err = unregistered_foreign_scope(scope.clone(), AccountOperation::SyncChanges);
        assert_eq!(
            err.kind(),
            &AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
        );
        assert_eq!(
            *err.recovery(),
            RecoveryClass::Engine(EngineDirective::DisableScope(scope.clone()))
        );
        assert_eq!(err.scope(), Some(&ErrorScope::Cursor(scope)));
    }

    #[test]
    fn problem_status_401_maps_auth_lost() {
        let err = into_account_error(
            problem_with_status(ProblemType::Other("token".into()), Some(401)),
            JmapErrorContext::new(AccountOperation::SyncInventory),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
        );
        assert!(matches!(err.recovery(), RecoveryClass::AuthLost));
    }

    #[test]
    fn problem_not_json_maps_protocol_contract_violation() {
        // jmap-F2: the server's "not JSON" / "not request" complaints
        // describe a protocol contract violation, not a client bug. The
        // local serializer would have caught any client-side malformed
        // JSON before sending; if the server insists otherwise, that is
        // a server-side conformance failure.
        let err = into_account_error(
            problem_with_status(ProblemType::JMAP(JMAPError::NotJSON), Some(400)),
            JmapErrorContext::new(AccountOperation::Send),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
        );
    }

    #[test]
    fn response_decode_maps_protocol_parse_failed() {
        let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let err = into_account_error(
            crate::Error::ResponseDecode(json_err),
            JmapErrorContext::new(AccountOperation::SyncInventory),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
        );
    }

    #[test]
    fn set_not_found_with_message_scope_maps_not_found() {
        let set: SetError<String> = serde_json::from_str(r#"{"type":"notFound"}"#).unwrap();
        let err = set_error_to_account_error(
            set,
            JmapErrorContext::new(AccountOperation::HydrateMessage),
            Some(ErrorScope::Message { id: "msg-1".into() }),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::NotFound(ResourceKind::Message)
        );
    }

    #[test]
    fn set_rate_limit_maps_rate_limited() {
        let set: SetError<String> = serde_json::from_str(r#"{"type":"rateLimit"}"#).unwrap();
        let err = set_error_to_account_error(
            set,
            JmapErrorContext::new(AccountOperation::UpdateFlags),
            None,
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Server(ServerErrorKind::RateLimited)
        );
    }

    #[cfg(feature = "websockets")]
    #[test]
    fn websocket_subprotocol_maps_capability_changed() {
        let err = into_account_error(
            crate::Error::WebSocketSetup(crate::WebSocketSetupError::Subprotocol(
                "server did not accept jmap".to_string(),
            )),
            JmapErrorContext::new(AccountOperation::PushSubscribe),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged)
        );
    }

    #[test]
    fn classify_set_item_absorbs_notfound_for_bulk_destroy() {
        let set: SetError<String> = serde_json::from_str(r#"{"type":"notFound"}"#).unwrap();
        let outcome = classify_set_item(
            set,
            JmapErrorContext::new(AccountOperation::BulkDestroy),
            BatchItemId("msg-1".into()),
            Some(ErrorScope::Message { id: "msg-1".into() }),
        );
        assert!(matches!(
            outcome,
            ItemOutcome::Succeeded(BatchSuccess {
                output: MutationSuccess::Skipped,
                ..
            })
        ));
    }

    #[test]
    fn classify_set_item_fails_for_real_error() {
        let set: SetError<String> = serde_json::from_str(r#"{"type":"rateLimit"}"#).unwrap();
        let outcome = classify_set_item(
            set,
            JmapErrorContext::new(AccountOperation::UpdateFlags),
            BatchItemId("msg-2".into()),
            None,
        );
        match outcome {
            ItemOutcome::Failed(failure) => {
                assert_eq!(failure.item.0, "msg-2");
                assert_eq!(
                    failure.error.kind(),
                    &AccountErrorKind::Server(ServerErrorKind::RateLimited)
                );
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// jmap-N2: `SetErrorType::Other` carries the actual wire code so
    /// `JmapMethod::Unknown { code }` reflects what the server sent,
    /// instead of synthesizing a placeholder `"other"` literal.
    #[test]
    fn set_error_other_preserves_wire_code() {
        let set: SetError<String> = serde_json::from_str(r#"{"type":"someNovelCode"}"#).unwrap();
        let err = set_error_to_account_error(
            set,
            JmapErrorContext::new(AccountOperation::UpdateFlags),
            None,
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::Unknown)
        );
        let mut saw_real_code = false;
        for cause in err.chain().iter() {
            if let Cause::Wire(WireCause::Jmap(JmapMethod::Unknown { code })) = cause {
                saw_real_code = true;
                assert_eq!(code, "someNovelCode");
                assert_ne!(code, "other", "must not synthesize the 'other' placeholder");
            }
        }
        assert!(
            saw_real_code,
            "Unknown wire cause with the real code missing"
        );
    }

    /// jmap-N1: `terminated_unsupported(op, scope, msg)` carries the
    /// caller's operation. The pre-fix helper hard-coded
    /// `AccountOperation::Discover`, mis-classifying every stream that
    /// emitted an unsupported event.
    #[test]
    fn terminated_unsupported_threads_operation() {
        let event = terminated_unsupported::<()>(
            AccountOperation::SyncInventory,
            Some(ErrorScope::Cursor(CursorScope::Type(
                bifrost_types::ObjectType::Thread,
            ))),
            "JMAP thread inventory is derived from Email inventory",
        );
        match event {
            SyncEvent::Terminated(err) => {
                assert_eq!(
                    err.kind(),
                    &AccountErrorKind::Unsupported(AccountOperation::SyncInventory)
                );
                assert_eq!(err.operation(), Some(AccountOperation::SyncInventory));
            }
            other => panic!("expected Terminated, got {other:?}"),
        }
    }

    /// jmap-N1 follow-on: pagination overflows reclassify as
    /// `Protocol(ContractViolation)` because the server response shape
    /// is incompatible with the request; "unsupported" was the wrong
    /// consumer-facing classification.
    #[test]
    fn terminated_contract_violation_classifies_pagination_overflow() {
        let event = terminated_contract_violation::<()>(
            AccountOperation::SyncInventory,
            Some(ErrorScope::Cursor(CursorScope::Type(
                bifrost_types::ObjectType::Email,
            ))),
            "JMAP inventory position overflowed",
        );
        match event {
            SyncEvent::Terminated(err) => {
                assert_eq!(
                    err.kind(),
                    &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
                );
                assert!(err.recovery().is_terminal());
            }
            other => panic!("expected Terminated, got {other:?}"),
        }
    }

    /// jmap-D1: WebSocket pre-handshake failures classify as
    /// `Transport(Network)` with `Attempt(Unsent)`, not
    /// `Protocol(PartialResponse) + Acknowledged`.
    #[cfg(feature = "websockets")]
    #[test]
    fn websocket_handshake_classifies_as_transport_network_unsent() {
        // Construct a `tokio_websockets::Error` indirectly via a known
        // failure path. We can't easily build one directly, so synthesize
        // the classification helper and assert its shape.
        let ctx = JmapErrorContext::new(AccountOperation::PushSubscribe);
        let err = websocket_handshake_error("handshake aborted".to_string(), ctx);
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Transport(TransportErrorKind::Network)
        );
        let mut saw_unsent = false;
        for cause in err.chain().iter() {
            if let Cause::Attempt(attempt) = cause
                && attempt.transmission_state == TransmissionState::Unsent
            {
                saw_unsent = true;
            }
        }
        assert!(saw_unsent, "expected Attempt(Unsent) on the chain");
        match err.recovery() {
            RecoveryClass::Retry(advice) => {
                assert_eq!(advice.disposition, RetryDisposition::SameRequest);
            }
            other => panic!("expected Retry::SameRequest, got {other:?}"),
        }
    }

    /// jmap-D1: WebSocket post-handshake (runtime) failures classify as
    /// `Protocol(PartialResponse)` with `Attempt(Acknowledged)`.
    #[cfg(feature = "websockets")]
    #[test]
    fn websocket_runtime_classifies_as_protocol_partial_response_acknowledged() {
        let ctx = JmapErrorContext::new(AccountOperation::PushSubscribe);
        let err = websocket_runtime_error("stream dropped".to_string(), ctx);
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
        );
        let mut saw_acknowledged = false;
        for cause in err.chain().iter() {
            if let Cause::Attempt(attempt) = cause
                && attempt.transmission_state == TransmissionState::Acknowledged
            {
                saw_acknowledged = true;
            }
        }
        assert!(
            saw_acknowledged,
            "expected Attempt(Acknowledged) on the chain"
        );
    }
}
