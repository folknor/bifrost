//! Graph account error boundary.
//!
//! Every Graph-side failure that leaves a streaming helper or
//! `Account` method passes through `into_account_error`. Transport
//! variants delegate to `bifrost_net::into_account_error` with Graph
//! provider/protocol context. Graph response variants build the
//! `AccountError` directly through `AccountErrorBuilder`, classified
//! by the typed `GraphSignal`. No path in this module looks at
//! `error.message` text for control flow.

use std::time::{Duration, SystemTime};

use bifrost_net::NetErrorContext;
use bifrost_types::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountOperation, AttemptCause, AuthCause, AuthErrorKind, Cause, DiagnosticText, ErrorScope,
    GraphSignal, MailboxUnavailableKind, Protocol, ProtocolErrorKind, Provider, RequestCause,
    ResourceKind, ServerCause, ServerErrorKind, StateCause, SyncStateErrorKind, ThrottleScope,
    TransmissionState, WireCause,
};
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderName, RETRY_AFTER};

use crate::error::{GraphError, GraphInnerError, GraphResponseError};

/// Context every Graph (or EWS-fallback) error needs before the
/// account boundary. `protocol` selects `Protocol::Graph` for REST
/// calls or `Protocol::Ews` for the streaming-notification fallback.
#[derive(Clone, Debug)]
pub(crate) struct GraphErrorContext {
    pub(crate) protocol: Protocol,
    pub(crate) operation: AccountOperation,
    pub(crate) scope: Option<ErrorScope>,
}

impl GraphErrorContext {
    #[must_use]
    pub(crate) fn graph(operation: AccountOperation) -> Self {
        Self {
            protocol: Protocol::Graph,
            operation,
            scope: None,
        }
    }

    #[must_use]
    pub(crate) fn with_scope(mut self, scope: ErrorScope) -> Self {
        self.scope = Some(scope);
        self
    }

    fn to_net_ctx(&self) -> NetErrorContext {
        NetErrorContext {
            provider: Some(Provider::Microsoft),
            protocol: self.protocol,
            operation: self.operation,
            scope: self.scope.clone(),
        }
    }
}

/// Convert a structured `GraphError` into the opaque
/// `bifrost_types::AccountError`. The only entry point any account
/// layer call site should use.
#[must_use]
pub(crate) fn into_account_error(error: GraphError, ctx: GraphErrorContext) -> AccountError {
    match error {
        GraphError::Net(net) => bifrost_net::into_account_error(net, ctx.to_net_ctx()),
        GraphError::Response(response) => response_to_account_error(response, &ctx),
        GraphError::Json { message, body } => json_parse_to_account_error(&message, body, &ctx),
    }
}

fn json_parse_to_account_error(
    message: &str,
    body: Option<bytes::Bytes>,
    ctx: &GraphErrorContext,
) -> AccountError {
    let detail = if message.trim().is_empty() {
        DiagnosticText::support_only("Graph response JSON parse failed".to_string())
    } else {
        DiagnosticText::support_only(format!("Graph response JSON parse failed: {message}"))
    };
    let mut builder = base_builder(
        ctx,
        AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: ctx.protocol,
            detail: Some(detail.clone()),
        }),
    )
    .text(detail);
    if let Some(body) = body
        && let Some(text) = body_diagnostic(&body)
    {
        builder = builder.text(text);
    }
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    finish(builder, ctx)
}

fn response_to_account_error(
    response: GraphResponseError,
    ctx: &GraphErrorContext,
) -> AccountError {
    let GraphResponseError {
        status,
        headers,
        body,
        signal,
        message,
        inner,
    } = response;

    let code = status.as_u16();
    let retry_after = parse_retry_after_header(&headers);

    let (kind, cause, throttle) = classify(ctx, code, &signal, retry_after);
    let apply_retry_deadline = retry_deadline_applies(&kind);

    let primary_cause = wire_or_specific(cause.clone(), &signal, ctx.protocol);

    let mut builder = base_builder(ctx, kind, primary_cause);
    // Attach the GraphSignal as an inner wire cause whenever the
    // primary cause is not itself a wire cause, so support tooling
    // can read the typed provider code regardless of which semantic
    // bucket the kind landed in.
    if !matches!(cause, Cause::Wire(_)) {
        builder = builder.push_cause(Cause::Wire(WireCause::Graph(signal.clone())));
    }
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    builder = response_diagnostics(builder, status, &headers, &body, inner.as_ref());

    if let Some(msg) = message.as_ref().filter(|m| !m.trim().is_empty()) {
        builder = builder.text(DiagnosticText::support_only(msg.clone()));
    }
    if let Some(inner) = inner.as_ref() {
        if let Some(code) = inner.code.as_ref().filter(|c| !c.trim().is_empty()) {
            builder = builder.text(DiagnosticText::support_only(format!(
                "innerError.code: {code}"
            )));
        }
        if let Some(msg) = inner.message.as_ref().filter(|m| !m.trim().is_empty()) {
            builder = builder.text(DiagnosticText::support_only(format!(
                "innerError.message: {msg}"
            )));
        }
        if let Some(date) = inner.date.as_ref().filter(|d| !d.trim().is_empty()) {
            builder = builder.text(DiagnosticText::support_only(format!(
                "innerError.date: {date}"
            )));
        }
    }

    if apply_retry_deadline && let Some(deadline) = retry_deadline(retry_after) {
        builder = builder.retry_not_before(deadline);
    }
    if throttle && let Some(scope) = throttle_scope_for(ctx) {
        builder = builder.throttle_scope(scope);
    }
    finish(builder, ctx)
}

/// Replace a `Cause::Wire` primary with one that carries the actual
/// `GraphSignal`, so the chain's outermost cause stays kind-matched
/// when the kind side is `Protocol(...)`.
fn wire_or_specific(cause: Cause, signal: &GraphSignal, _protocol: Protocol) -> Cause {
    if matches!(
        cause,
        Cause::Wire(WireCause::MalformedResponse { .. } | WireCause::Graph(_))
    ) {
        Cause::Wire(WireCause::Graph(signal.clone()))
    } else {
        cause
    }
}

/// Classification rules. All decisions are made on `GraphSignal` plus
/// HTTP status; no path inspects the `error.message` string.
fn classify(
    ctx: &GraphErrorContext,
    code: u16,
    signal: &GraphSignal,
    retry_after: Option<Duration>,
) -> (AccountErrorKind, Cause, bool) {
    // 1) Typed Graph signals - these win over status-only mapping
    //    because Microsoft sometimes returns the same status for
    //    semantically different conditions (400 InvalidDeltaToken vs.
    //    400 Malformed body).
    match signal {
        GraphSignal::InvalidAuthenticationToken => {
            return (
                AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
                Cause::Auth(AuthCause::ReauthorizationRequired),
                false,
            );
        }
        GraphSignal::AccessDenied | GraphSignal::Forbidden => {
            return (
                AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
                Cause::Access(AccessCause::PermissionDenied {
                    resource: resource_from_scope(ctx.scope.as_ref()),
                }),
                false,
            );
        }
        GraphSignal::AccessRestricted | GraphSignal::ConditionalAccessBlocked => {
            return (
                AccountErrorKind::Authorization(AccessErrorKind::ConditionalAccessBlocked),
                Cause::Access(AccessCause::ConditionalAccessBlocked),
                false,
            );
        }
        GraphSignal::AdminConsentRequired => {
            return (
                AccountErrorKind::Authorization(AccessErrorKind::AdminConsentRequired),
                Cause::Access(AccessCause::AdminConsentRequired {
                    needed: "admin-consent",
                }),
                false,
            );
        }
        GraphSignal::MailboxNotEnabledForRestApi => {
            return (
                AccountErrorKind::Authorization(AccessErrorKind::MailboxNotLicensed),
                Cause::Access(AccessCause::MailboxNotLicensed),
                false,
            );
        }
        GraphSignal::MailboxStoreUnavailable => {
            return (
                AccountErrorKind::Authorization(AccessErrorKind::MailboxUnavailable {
                    kind: MailboxUnavailableKind::Transient,
                }),
                Cause::Access(AccessCause::MailboxUnavailable {
                    kind: MailboxUnavailableKind::Transient,
                }),
                false,
            );
        }
        GraphSignal::ResyncRequired => {
            return (
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                Cause::State(StateCause::CursorInvalid),
                false,
            );
        }
        GraphSignal::TooManyRequests => {
            return (
                AccountErrorKind::Server(ServerErrorKind::RateLimited),
                Cause::Server(ServerCause::RateLimited { retry_after }),
                true,
            );
        }
        GraphSignal::GenericFileError => {
            return (
                AccountErrorKind::Server(ServerErrorKind::Unavailable),
                Cause::Server(ServerCause::Unavailable { retry_after }),
                false,
            );
        }
        GraphSignal::PreconditionFailed => {
            return (
                AccountErrorKind::ConcurrencyConflict,
                Cause::State(StateCause::ConcurrencyConflict),
                false,
            );
        }
        GraphSignal::Gone => {
            // 410 Gone on a cursor scope is the canonical
            // SyncState(CursorInvalid) → Engine(RestartScope) signal.
            if matches!(ctx.scope, Some(ErrorScope::Cursor(_))) {
                return (
                    AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                    Cause::State(StateCause::CursorInvalid),
                    false,
                );
            }
            // 410 outside cursor context is a provider-refused server
            // error; we keep the HTTP status to disambiguate.
            return (
                AccountErrorKind::Server(ServerErrorKind::Error { status: Some(410) }),
                Cause::Server(ServerCause::Error { status: Some(410) }),
                false,
            );
        }
        GraphSignal::NotFound => {
            return match resource_from_scope(ctx.scope.as_ref()) {
                Some(resource) => (
                    AccountErrorKind::NotFound(resource),
                    Cause::Request(RequestCause::NotFound {
                        what: resource,
                        id: id_from_scope(ctx.scope.as_ref()),
                    }),
                    false,
                ),
                None => server_error_tuple(404),
            };
        }
        GraphSignal::InvalidDeltaToken | GraphSignal::SyncStateNotFound => {
            return (
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                Cause::State(StateCause::CursorInvalid),
                false,
            );
        }
        GraphSignal::Unknown { .. } => {}
        _ => {}
    }

    // 2) Status-only fallback when the GraphSignal carries no
    //    actionable code.
    classify_by_status(ctx, code, retry_after)
}

fn classify_by_status(
    ctx: &GraphErrorContext,
    code: u16,
    retry_after: Option<Duration>,
) -> (AccountErrorKind, Cause, bool) {
    match code {
        400 | 422 => (
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(format!("HTTP {code}")),
            }),
            false,
        ),
        401 => (
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
            false,
        ),
        403 => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: resource_from_scope(ctx.scope.as_ref()),
            }),
            false,
        ),
        404 => match resource_from_scope(ctx.scope.as_ref()) {
            Some(resource) => (
                AccountErrorKind::NotFound(resource),
                Cause::Request(RequestCause::NotFound {
                    what: resource,
                    id: id_from_scope(ctx.scope.as_ref()),
                }),
                false,
            ),
            None => server_error_tuple(code),
        },
        409 | 412 => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
            false,
        ),
        410 if matches!(ctx.scope, Some(ErrorScope::Cursor(_))) => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
            false,
        ),
        410 => server_error_tuple(code),
        408 | 502 | 503 | 504 => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_after }),
            false,
        ),
        429 => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_after }),
            true,
        ),
        507 => (
            AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
            Cause::Server(ServerCause::QuotaExhausted { retry_after }),
            true,
        ),
        500..=599 => server_error_tuple(code),
        _ => server_error_tuple(code),
    }
}

fn server_error_tuple(code: u16) -> (AccountErrorKind, Cause, bool) {
    (
        AccountErrorKind::Server(ServerErrorKind::Error { status: Some(code) }),
        Cause::Server(ServerCause::Error { status: Some(code) }),
        false,
    )
}

fn retry_deadline_applies(kind: &AccountErrorKind) -> bool {
    matches!(
        kind,
        AccountErrorKind::Server(
            ServerErrorKind::Unavailable
                | ServerErrorKind::RateLimited
                | ServerErrorKind::QuotaExhausted
                | ServerErrorKind::Error {
                    status: Some(500..=599)
                },
        )
    )
}

fn base_builder(
    ctx: &GraphErrorContext,
    kind: AccountErrorKind,
    cause: Cause,
) -> AccountErrorBuilder {
    AccountErrorBuilder::new(kind, cause)
        .protocol(ctx.protocol)
        .operation(ctx.operation)
        .provider(Provider::Microsoft)
}

fn finish(builder: AccountErrorBuilder, ctx: &GraphErrorContext) -> AccountError {
    let builder = match &ctx.scope {
        Some(scope) => builder.scope(scope.clone()),
        None => builder,
    };
    builder.build()
}

fn push_attempt(
    builder: AccountErrorBuilder,
    transmission_state: TransmissionState,
) -> AccountErrorBuilder {
    builder.push_cause(Cause::Attempt(AttemptCause::new(transmission_state)))
}

fn response_diagnostics(
    mut builder: AccountErrorBuilder,
    status: StatusCode,
    headers: &HeaderMap,
    body: &bytes::Bytes,
    inner: Option<&GraphInnerError>,
) -> AccountErrorBuilder {
    builder = builder.status(Some(status.as_u16()));

    // Prefer header request-id; fall back to innerError.request-id
    // when the headers do not carry one.
    let request_id = first_header(headers, REQUEST_ID_HEADERS)
        .or_else(|| inner.and_then(|i| i.request_id.clone()));
    if let Some(id) = request_id {
        builder = builder.request_id(id);
    }
    let client_id = header_text(headers, "client-request-id")
        .or_else(|| inner.and_then(|i| i.client_request_id.clone()));
    if let Some(trace) = client_id {
        builder = builder.trace_id(trace);
    }

    for name in SUPPORT_ONLY_HEADER_TEXT {
        if let Some(value) = header_text(headers, name) {
            builder = builder.text(DiagnosticText::support_only(format!("{name}: {value}")));
        }
    }
    if let Some(text) = body_diagnostic(body) {
        builder = builder.text(text);
    }
    builder
}

const REQUEST_ID_HEADERS: &[&str] = &["request-id", "x-ms-request-id", "x-request-id"];

const SUPPORT_ONLY_HEADER_TEXT: &[&str] = &["x-ms-ags-diagnostic"];

fn first_header(headers: &HeaderMap, names: &[&str]) -> Option<String> {
    for name in names {
        if let Some(value) = header_text(headers, name) {
            return Some(value);
        }
    }
    None
}

fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    let name = HeaderName::from_bytes(name.as_bytes()).ok()?;
    let value = headers.get(name)?;
    let text = value.to_str().ok()?.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_owned())
    }
}

fn body_diagnostic(body: &bytes::Bytes) -> Option<DiagnosticText> {
    if body.is_empty() {
        return None;
    }
    Some(DiagnosticText::support_only(format!(
        "response body: {}",
        String::from_utf8_lossy(body)
    )))
}

fn parse_retry_after_header(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?;
    let text = value.to_str().ok()?.trim();
    if let Ok(secs) = text.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    None
}

fn retry_deadline(retry_after: Option<Duration>) -> Option<SystemTime> {
    retry_after.and_then(|duration| SystemTime::now().checked_add(duration))
}

fn resource_from_scope(scope: Option<&ErrorScope>) -> Option<ResourceKind> {
    match scope {
        Some(ErrorScope::Message { .. }) => Some(ResourceKind::Message),
        Some(ErrorScope::Mailbox { .. }) => Some(ResourceKind::Mailbox),
        Some(ErrorScope::Thread { .. }) => Some(ResourceKind::Thread),
        Some(ErrorScope::Calendar { .. }) => Some(ResourceKind::Calendar),
        Some(ErrorScope::Contact { .. }) => Some(ResourceKind::Contact),
        Some(
            ErrorScope::Account
            | ErrorScope::Cursor(_)
            | ErrorScope::CalendarCollection
            | ErrorScope::ContactCollection,
        )
        | Some(_)
        | None => None,
    }
}

fn id_from_scope(scope: Option<&ErrorScope>) -> Option<String> {
    match scope {
        Some(
            ErrorScope::Message { id }
            | ErrorScope::Mailbox { id }
            | ErrorScope::Thread { id }
            | ErrorScope::Calendar { id }
            | ErrorScope::Contact { id },
        ) => Some(id.clone()),
        Some(
            ErrorScope::Account
            | ErrorScope::Cursor(_)
            | ErrorScope::CalendarCollection
            | ErrorScope::ContactCollection,
        )
        | Some(_)
        | None => None,
    }
}

fn throttle_scope_for(_ctx: &GraphErrorContext) -> Option<ThrottleScope> {
    // All Microsoft tenants share the per-tenant throttle policy
    // (Graph REST and EWS both meter at the tenant level).
    Some(ThrottleScope::Tenant)
}

/// Build an `AccountError` for an operation this account does not support.
#[must_use]
pub(crate) fn unsupported_account_error(operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(operation),
        Cause::Request(RequestCause::Unsupported { operation }),
    )
    .operation(operation)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .build()
}

/// Translate a `CursorError` into the account-boundary `AccountError`.
///
/// `CursorProtocolMismatch`, `CursorEnvelopeUnknown`, and
/// `SchemaIncompatible` map to `SyncState(SchemaIncompatible)` so the
/// engine can restart the scope with a cleared cursor. `Unsupported`
/// maps to `Unsupported(EstablishCursor)`. `Encode` (serialization
/// failures) maps to `Protocol(ContractViolation)`.
#[must_use]
pub(crate) fn cursor_error_to_account_error(
    error: crate::account::cursor::CursorError,
    ctx: GraphErrorContext,
) -> AccountError {
    use crate::account::cursor::CursorError;
    match error {
        CursorError::ProtocolMismatch
        | CursorError::EnvelopeUnknown
        | CursorError::SchemaIncompatible => base_builder(
            &ctx,
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
            Cause::State(StateCause::SchemaIncompatible),
        )
        .text(DiagnosticText::support_only(error.to_string()))
        .build(),
        CursorError::Unsupported => base_builder(
            &ctx,
            AccountErrorKind::Unsupported(ctx.operation),
            Cause::Request(RequestCause::Unsupported {
                operation: ctx.operation,
            }),
        )
        .build(),
        CursorError::Encode(msg) => base_builder(
            &ctx,
            AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
            Cause::Wire(WireCause::MalformedResponse {
                protocol: ctx.protocol,
                detail: Some(DiagnosticText::support_only(msg)),
            }),
        )
        .build(),
    }
}

/// Translate a single `$batch` response item into an
/// `ItemOutcome<MutationSuccess>`. Centralizes the mutation outcome
/// rules from the plan: 2xx → applied, destroy-404 → skipped, 404 → not
/// found, 409 / 412 → concurrency conflict (never `Skipped`), 429 →
/// rate limited with retry-after + throttle scope, 5xx → unavailable.
#[must_use]
pub(crate) fn mutation_item_outcome(
    status: u16,
    headers: HeaderMap,
    body: bytes::Bytes,
    destroy: bool,
    item_id: bifrost_types::BatchItemId,
    operation: AccountOperation,
    scope: ErrorScope,
) -> bifrost_types::ItemOutcome<bifrost_types::MutationSuccess> {
    use bifrost_types::{BatchFailure, BatchSuccess, ItemOutcome, MutationSuccess};

    if (200..300).contains(&status) {
        return ItemOutcome::Succeeded(BatchSuccess::new(item_id, MutationSuccess::Applied));
    }
    if status == 404 && destroy {
        return ItemOutcome::Succeeded(BatchSuccess::new(item_id, MutationSuccess::Skipped));
    }
    let response = GraphResponseError::from_response(
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        headers,
        body,
    );
    let ctx = GraphErrorContext::graph(operation).with_scope(scope);
    let error = response_to_account_error(response, &ctx);
    ItemOutcome::Failed(BatchFailure::new(item_id, error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{GraphError, GraphResponseError};
    use bifrost_types::{
        AccessErrorKind, AccountErrorKind, AuthErrorKind, CursorScope, EngineDirective,
        ProtocolErrorKind, RecoveryClass, ServerErrorKind, SyncStateErrorKind,
    };
    use bytes::Bytes;
    use reqwest::StatusCode;
    use reqwest::header::{HeaderMap, HeaderValue};

    fn body(json: &str) -> Bytes {
        Bytes::copy_from_slice(json.as_bytes())
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            let name = HeaderName::from_bytes(k.as_bytes()).expect("valid header name");
            let value = HeaderValue::from_str(v).expect("valid header value");
            h.insert(name, value);
        }
        h
    }

    fn classify(
        status: StatusCode,
        json: &str,
        ctx: GraphErrorContext,
        extra_headers: &[(&str, &str)],
    ) -> AccountError {
        let response =
            GraphResponseError::from_response(status, headers(extra_headers), body(json));
        into_account_error(GraphError::Response(response), ctx)
    }

    fn graph_ctx(op: AccountOperation) -> GraphErrorContext {
        GraphErrorContext::graph(op)
    }

    #[test]
    fn invalid_authentication_token_maps_to_reauth() {
        let err = classify(
            StatusCode::UNAUTHORIZED,
            r#"{"error":{"code":"InvalidAuthenticationToken","message":"x"}}"#,
            graph_ctx(AccountOperation::SyncChanges),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
        ));
        assert!(matches!(err.recovery(), RecoveryClass::AuthLost));
        assert_eq!(err.provider(), Some(Provider::Microsoft));
        assert_eq!(err.protocol(), Some(Protocol::Graph));
    }

    #[test]
    fn access_denied_maps_to_permission_denied() {
        let err = classify(
            StatusCode::FORBIDDEN,
            r#"{"error":{"code":"AccessDenied","message":"nope"}}"#,
            graph_ctx(AccountOperation::Hydrate),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        ));
    }

    #[test]
    fn forbidden_maps_to_permission_denied() {
        let err = classify(
            StatusCode::FORBIDDEN,
            r#"{"error":{"code":"Forbidden","message":"nope"}}"#,
            graph_ctx(AccountOperation::Hydrate),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        ));
    }

    #[test]
    fn access_restricted_maps_to_conditional_access() {
        let err = classify(
            StatusCode::FORBIDDEN,
            r#"{"error":{"code":"AccessRestricted"}}"#,
            graph_ctx(AccountOperation::SyncChanges),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::ConditionalAccessBlocked)
        ));
    }

    #[test]
    fn admin_consent_required_maps_to_admin_consent() {
        let err = classify(
            StatusCode::FORBIDDEN,
            r#"{"error":{"code":"AdminConsentRequired"}}"#,
            graph_ctx(AccountOperation::SyncChanges),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::AdminConsentRequired)
        ));
        assert!(matches!(
            err.recovery(),
            RecoveryClass::NeedsAdminConsent { .. }
        ));
    }

    #[test]
    fn mailbox_not_enabled_maps_to_not_licensed() {
        let err = classify(
            StatusCode::FORBIDDEN,
            r#"{"error":{"code":"MailboxNotEnabledForRESTAPI"}}"#,
            graph_ctx(AccountOperation::SyncInventory),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::MailboxNotLicensed)
        ));
    }

    #[test]
    fn mailbox_store_unavailable_is_transient() {
        let err = classify(
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"error":{"code":"MailboxStoreUnavailable"}}"#,
            graph_ctx(AccountOperation::SyncInventory),
            &[],
        );
        match err.kind() {
            AccountErrorKind::Authorization(AccessErrorKind::MailboxUnavailable {
                kind: MailboxUnavailableKind::Transient,
            }) => {}
            other => panic!("expected mailbox unavailable transient, got {other:?}"),
        }
    }

    #[test]
    fn resync_required_maps_to_cursor_invalid_with_engine_restart() {
        let cursor_scope = ErrorScope::Cursor(CursorScope::Account);
        let err = classify(
            StatusCode::BAD_REQUEST,
            r#"{"error":{"code":"ResyncRequired"}}"#,
            graph_ctx(AccountOperation::SyncChanges).with_scope(cursor_scope.clone()),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
        match err.recovery() {
            RecoveryClass::Engine(EngineDirective::RestartScope(_)) => {}
            other => panic!("expected Engine(RestartScope(_)), got {other:?}"),
        }
    }

    #[test]
    fn invalid_delta_token_maps_to_cursor_invalid() {
        let err = classify(
            StatusCode::BAD_REQUEST,
            r#"{"error":{"code":"InvalidDeltaToken"}}"#,
            graph_ctx(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(CursorScope::Account)),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
    }

    #[test]
    fn sync_state_not_found_maps_to_cursor_invalid() {
        // Microsoft ships both SyncStateNotFound and syncStateNotFound; both
        // must classify as CursorInvalid via the typed variant.
        for code in ["SyncStateNotFound", "syncStateNotFound"] {
            let err = classify(
                StatusCode::BAD_REQUEST,
                &format!(r#"{{"error":{{"code":"{code}"}}}}"#),
                graph_ctx(AccountOperation::SyncChanges)
                    .with_scope(ErrorScope::Cursor(CursorScope::Account)),
                &[],
            );
            assert!(
                matches!(
                    err.kind(),
                    AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
                ),
                "code={code} did not classify as CursorInvalid"
            );
        }
    }

    #[test]
    fn too_many_requests_carries_retry_hint_and_throttle_scope() {
        let err = classify(
            StatusCode::TOO_MANY_REQUESTS,
            r#"{"error":{"code":"TooManyRequests"}}"#,
            graph_ctx(AccountOperation::BulkMove),
            &[("retry-after", "30")],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Server(ServerErrorKind::RateLimited)
        ));
        match err.recovery() {
            RecoveryClass::Retry(advice) => {
                assert_eq!(advice.throttle_scope, Some(ThrottleScope::Tenant));
                assert!(advice.not_before.is_some());
            }
            other => panic!("expected Retry, got {other:?}"),
        }
    }

    #[test]
    fn generic_file_error_is_retryable_unavailable() {
        let err = classify(
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":{"code":"GenericFileError"}}"#,
            graph_ctx(AccountOperation::OpenBlob),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Server(ServerErrorKind::Unavailable)
        ));
    }

    #[test]
    fn precondition_failed_maps_to_concurrency_conflict() {
        let err = classify(
            StatusCode::PRECONDITION_FAILED,
            r#"{"error":{"code":"PreconditionFailed"}}"#,
            graph_ctx(AccountOperation::SetIsRead),
            &[],
        );
        assert!(matches!(err.kind(), AccountErrorKind::ConcurrencyConflict));
        // ConcurrencyConflict derives a Retry(AfterStateRefresh) recovery,
        // not a Reconcile remediation. Lock that down here so a future
        // change to the recovery table cannot silently weaken the
        // If-Match guarantee.
        match err.recovery() {
            RecoveryClass::Retry(_) => {}
            other => panic!("expected Retry recovery, got {other:?}"),
        }
    }

    #[test]
    fn http_412_without_signal_still_maps_to_concurrency_conflict() {
        let err = classify(
            StatusCode::PRECONDITION_FAILED,
            "",
            graph_ctx(AccountOperation::BulkMove),
            &[],
        );
        assert!(matches!(err.kind(), AccountErrorKind::ConcurrencyConflict));
    }

    #[test]
    fn http_410_on_delta_maps_to_cursor_invalid() {
        let err = classify(
            StatusCode::GONE,
            "",
            graph_ctx(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(CursorScope::Account)),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
        match err.recovery() {
            RecoveryClass::Engine(EngineDirective::RestartScope(_)) => {}
            other => panic!("expected Engine(RestartScope(_)), got {other:?}"),
        }
    }

    #[test]
    fn http_410_outside_delta_maps_to_provider_server_error() {
        let err = classify(
            StatusCode::GONE,
            "",
            graph_ctx(AccountOperation::OpenBlob),
            &[],
        );
        match err.kind() {
            AccountErrorKind::Server(ServerErrorKind::Error { status: Some(410) }) => {}
            other => panic!("expected Server(Error 410), got {other:?}"),
        }
    }

    #[test]
    fn http_404_with_message_scope_maps_to_notfound_message() {
        let err = classify(
            StatusCode::NOT_FOUND,
            "",
            graph_ctx(AccountOperation::HydrateMessage).with_scope(ErrorScope::Message {
                id: "abc".to_string(),
            }),
            &[],
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::NotFound(ResourceKind::Message)
        ));
    }

    #[test]
    fn graph_request_ids_are_preserved() {
        let err = classify(
            StatusCode::TOO_MANY_REQUESTS,
            r#"{"error":{"code":"TooManyRequests","innerError":{"request-id":"inner-rid","client-request-id":"inner-crid"}}}"#,
            graph_ctx(AccountOperation::SyncChanges),
            &[
                ("request-id", "header-rid"),
                ("client-request-id", "header-crid"),
            ],
        );
        let telemetry = err.telemetry_fields();
        assert_eq!(telemetry.request_id, Some("header-rid"));
        assert_eq!(telemetry.trace_id, Some("header-crid"));
    }

    #[test]
    fn unparseable_body_falls_through_to_status_with_support_text() {
        let err = classify(
            StatusCode::INTERNAL_SERVER_ERROR,
            "definitely not JSON",
            graph_ctx(AccountOperation::SyncInventory),
            &[],
        );
        match err.kind() {
            AccountErrorKind::Server(ServerErrorKind::Error { status: Some(500) }) => {}
            other => panic!("expected Server(Error 500), got {other:?}"),
        }
        let support: Vec<_> = err.support_consented().support_text.into_iter().collect();
        assert!(support.iter().any(|t| t.contains("definitely not JSON")));
    }

    #[test]
    fn net_error_delegates_to_net_conversion() {
        let net_err = bifrost_net::Error::Network {
            message: "connection reset".to_string(),
            transmission_state: TransmissionState::InFlight,
            source: None,
        };
        let err = into_account_error(
            GraphError::Net(net_err),
            graph_ctx(AccountOperation::SyncChanges),
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network)
        ));
        assert_eq!(err.protocol(), Some(Protocol::Graph));
        assert_eq!(err.provider(), Some(Provider::Microsoft));
    }

    #[test]
    fn mutation_2xx_is_applied() {
        let outcome = mutation_item_outcome(
            204,
            HeaderMap::new(),
            Bytes::new(),
            false,
            bifrost_types::BatchItemId("m1".to_string()),
            AccountOperation::UpdateFlags,
            ErrorScope::Message {
                id: "m1".to_string(),
            },
        );
        assert!(matches!(
            outcome,
            bifrost_types::ItemOutcome::Succeeded(ref s) if matches!(s.output, bifrost_types::MutationSuccess::Applied)
        ));
    }

    #[test]
    fn mutation_destroy_404_is_skipped() {
        let outcome = mutation_item_outcome(
            404,
            HeaderMap::new(),
            Bytes::new(),
            true,
            bifrost_types::BatchItemId("m1".to_string()),
            AccountOperation::BulkDestroy,
            ErrorScope::Message {
                id: "m1".to_string(),
            },
        );
        assert!(matches!(
            outcome,
            bifrost_types::ItemOutcome::Succeeded(ref s) if matches!(s.output, bifrost_types::MutationSuccess::Skipped)
        ));
    }

    #[test]
    fn mutation_non_destroy_404_is_failed_notfound() {
        let outcome = mutation_item_outcome(
            404,
            HeaderMap::new(),
            Bytes::new(),
            false,
            bifrost_types::BatchItemId("m1".to_string()),
            AccountOperation::SetIsRead,
            ErrorScope::Message {
                id: "m1".to_string(),
            },
        );
        match outcome {
            bifrost_types::ItemOutcome::Failed(failure) => {
                assert!(matches!(
                    failure.error.kind(),
                    AccountErrorKind::NotFound(ResourceKind::Message)
                ));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn mutation_412_is_failed_concurrency_conflict_not_skipped() {
        let outcome = mutation_item_outcome(
            412,
            HeaderMap::new(),
            Bytes::new(),
            false,
            bifrost_types::BatchItemId("m1".to_string()),
            AccountOperation::UpdateFlags,
            ErrorScope::Message {
                id: "m1".to_string(),
            },
        );
        match outcome {
            bifrost_types::ItemOutcome::Failed(failure) => {
                assert!(matches!(
                    failure.error.kind(),
                    AccountErrorKind::ConcurrencyConflict
                ));
            }
            other => panic!("expected Failed (412 must not be Skipped), got {other:?}"),
        }
    }

    #[test]
    fn mutation_429_carries_retry_after_and_throttle_scope() {
        let outcome = mutation_item_outcome(
            429,
            headers(&[("retry-after", "10")]),
            Bytes::new(),
            false,
            bifrost_types::BatchItemId("m1".to_string()),
            AccountOperation::BulkMove,
            ErrorScope::Message {
                id: "m1".to_string(),
            },
        );
        match outcome {
            bifrost_types::ItemOutcome::Failed(failure) => {
                assert!(matches!(
                    failure.error.kind(),
                    AccountErrorKind::Server(ServerErrorKind::RateLimited)
                ));
                match failure.error.recovery() {
                    RecoveryClass::Retry(advice) => {
                        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Tenant));
                        assert!(advice.not_before.is_some());
                    }
                    other => panic!("expected Retry, got {other:?}"),
                }
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn json_parse_failure_is_protocol_parse_failed() {
        let err = into_account_error(
            GraphError::Json {
                message: "expected `,` or `}`".to_string(),
                body: Some(body("{not-json")),
            },
            graph_ctx(AccountOperation::SyncInventory),
        );
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
        ));
    }
}
