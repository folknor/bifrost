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
    BatchSuccess, CapabilityDelta, Cause, CursorScope, DiagnosticText, ErrorScope, ItemOutcome,
    JmapMethod, MutationSuccess, Protocol, ProtocolErrorKind, Provider, RequestCause,
    RequestErrorKind, ResourceKind, ServerCause, ServerErrorKind, StateCause, SyncEvent,
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
        .build(),
        crate::Error::ResponseDecode(err) => build(
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(err.to_string())),
            }),
            &ctx,
        )
        .build(),
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
        .build(),
        crate::Error::IdNotFound(id) => convert_id_not_found(id, ctx),
        crate::Error::NotParsable(detail) => build(
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: Protocol::Jmap,
                detail: Some(DiagnosticText::support_only(detail)),
            }),
            &ctx,
        )
        .build(),
        crate::Error::InvalidUrl(detail) => build(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::InvalidArgument {
                field: Some("url"),
                message: Some(DiagnosticText::support_only(detail)),
            }),
            &ctx,
        )
        .build(),
        crate::Error::NoPrimaryAccount { capability } => build(
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged {
                delta: CapabilityDelta::default(),
            }),
            &ctx,
        )
        .text(DiagnosticText::support_only(format!(
            "session lists no primary account for capability {capability}"
        )))
        .build(),
        #[cfg(feature = "websockets")]
        crate::Error::WebSocket(err) => websocket_runtime_error(err.to_string(), ctx),
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
        .build(),
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
            .build(),
            crate::WebSocketSetupError::InvalidHeader(message) => build(
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::InvalidArgument {
                    field: Some("authorization"),
                    message: Some(DiagnosticText::support_only(message)),
                }),
                &ctx,
            )
            .build(),
            crate::WebSocketSetupError::Subprotocol(message) => build(
                AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
                Cause::State(StateCause::CapabilityChanged {
                    delta: CapabilityDelta::default(),
                }),
                &ctx,
            )
            .text(DiagnosticText::support_only(message))
            .build(),
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
    builder.build()
}

#[must_use]
pub(crate) fn terminated_unsupported<T>(message: impl Into<String>) -> SyncEvent<T> {
    terminated(unsupported_error(AccountOperation::Discover, None, message))
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
            Cause::Server(ServerCause::QuotaExhausted { retry_after: None }),
        ),
        SetErrorType::RateLimit => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_after: None }),
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
                (
                    AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
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
        SetErrorType::WillDestroy => (
            AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
            Cause::Wire(WireCause::Jmap(JmapMethod::WillDestroy)),
        ),
        SetErrorType::Singleton => (
            AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
            Cause::Wire(WireCause::Jmap(JmapMethod::Singleton)),
        ),
        SetErrorType::ScriptIsActive => (
            AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
            Cause::Wire(WireCause::Jmap(JmapMethod::ScriptIsActive)),
        ),
        SetErrorType::CannotUnsend => (
            AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
            Cause::Wire(WireCause::Jmap(JmapMethod::CannotUnsend)),
        ),
        SetErrorType::MailboxHasChild => (
            AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
            Cause::Wire(WireCause::Jmap(JmapMethod::MailboxHasChild)),
        ),
        SetErrorType::MailboxHasEmail => (
            AccountErrorKind::Server(ServerErrorKind::Error { status: None }),
            Cause::Wire(WireCause::Jmap(JmapMethod::MailboxHasEmail)),
        ),
        SetErrorType::Other => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::Unknown {
                code: "other".to_string(),
            })),
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
        &SetErrorType::WillDestroy
            | &SetErrorType::Singleton
            | &SetErrorType::ScriptIsActive
            | &SetErrorType::CannotUnsend
            | &SetErrorType::MailboxHasChild
            | &SetErrorType::MailboxHasEmail
            | &SetErrorType::Other
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
    builder.build()
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
        // Other has no stable code.
        _ => None,
    }
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
        .build()
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
    let retry_after = transport
        .as_ref()
        .and_then(|t| retry_after_from_net(t.net.as_ref()));

    let (kind, primary) = match (details.error(), status_u16) {
        (ProblemType::JMAP(JMAPError::Limit), _) => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_after }),
        ),
        (ProblemType::JMAP(JMAPError::UnknownCapability), _) => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged {
                delta: CapabilityDelta::default(),
            }),
        ),
        (ProblemType::JMAP(JMAPError::NotJSON | JMAPError::NotRequest), _) => (
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(details.to_string()),
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
            Cause::Server(ServerCause::RateLimited { retry_after }),
        ),
        (ProblemType::Other(_), Some(status)) if (500..=599).contains(&status) => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_after }),
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
    builder.build()
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
    let (kind, primary, wire) = match method_type {
        MethodErrorType::ServerUnavailable => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_after: None }),
            JmapMethod::ServerUnavailable,
        ),
        MethodErrorType::ServerFail => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_after: None }),
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
            Cause::State(StateCause::CapabilityChanged {
                delta: CapabilityDelta::default(),
            }),
            JmapMethod::AccountNotFound,
        ),
        MethodErrorType::FromAccountNotFound => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged {
                delta: CapabilityDelta::default(),
            }),
            JmapMethod::FromAccountNotFound,
        ),
        MethodErrorType::AccountNotSupportedByMethod => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged {
                delta: CapabilityDelta::default(),
            }),
            JmapMethod::AccountNotSupportedByMethod,
        ),
        MethodErrorType::FromAccountNotSupportedByMethod => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
            Cause::State(StateCause::CapabilityChanged {
                delta: CapabilityDelta::default(),
            }),
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
        MethodErrorType::CannotCalculateChanges => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
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
        MethodErrorType::AnchorNotFound => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
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
        MethodErrorType::TooManyChanges => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
            JmapMethod::TooManyChanges,
        ),
        MethodErrorType::Other(code) => (
            AccountErrorKind::Protocol(ProtocolErrorKind::Unknown),
            Cause::Wire(WireCause::Jmap(JmapMethod::Unknown { code: code.clone() })),
            JmapMethod::Unknown { code: code.clone() },
        ),
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
    builder.build()
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
        .build()
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
        .build()
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
    .build()
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
    fn cannot_calculate_changes_without_scope_restarts_account() {
        let err = into_account_error(
            method_error("cannotCalculateChanges"),
            JmapErrorContext::new(AccountOperation::SyncChanges),
        );
        assert!(err.recovery().requires_engine_action());
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
    fn problem_not_json_maps_request_malformed() {
        let err = into_account_error(
            problem_with_status(ProblemType::JMAP(JMAPError::NotJSON), Some(400)),
            JmapErrorContext::new(AccountOperation::Send),
        );
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Request(RequestErrorKind::Malformed)
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
}
