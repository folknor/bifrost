use std::time::{Duration, SystemTime};

use bifrost_types::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountOperation, AttemptCause, AuthCause, AuthErrorKind, Cause, DiagnosticText, ErrorScope,
    Protocol, ProtocolErrorKind, Provider, RequestCause, RequestErrorKind, ResourceKind,
    ServerCause, ServerErrorKind, StateCause, SyncStateErrorKind, ThrottleScope, TransmissionState,
    TransportCause, TransportErrorKind, TransportKind, WireCause,
};
use bytes::Bytes;
use reqwest::{
    StatusCode,
    header::{HeaderMap, HeaderName, RETRY_AFTER},
};

use crate::error::{Error, FinalResponse, RangeFailureKind};
use crate::request::parse_retry_after;

#[derive(Clone, Debug)]
pub struct NetErrorContext {
    pub provider: Option<Provider>,
    pub protocol: Protocol,
    pub operation: AccountOperation,
    pub scope: Option<ErrorScope>,
}

#[must_use]
pub fn into_account_error(error: Error, ctx: NetErrorContext) -> AccountError {
    match error {
        Error::Network {
            message,
            transmission_state,
            source,
        } => transport_or_partial(
            &ctx,
            TransportKind::Network,
            TransportErrorKind::Network,
            message,
            transmission_state,
            source.as_ref().map(|source| source.to_string()),
        ),
        Error::Timeout { transmission_state } => transport_or_partial(
            &ctx,
            TransportKind::Timeout,
            TransportErrorKind::Timeout,
            "request timed out".to_owned(),
            transmission_state,
            None,
        ),
        Error::Tls {
            message,
            transmission_state,
        } => transport_or_partial(
            &ctx,
            TransportKind::Tls,
            TransportErrorKind::Tls,
            message,
            transmission_state,
            None,
        ),
        Error::Status {
            code,
            body,
            headers,
        } => status_error(&ctx, code, &headers, &body, None),
        Error::RetryBudgetExhausted {
            final_response,
            retry_after_history,
        } => match final_response {
            Some(response) => status_error(
                &ctx,
                response.status,
                &response.headers,
                &response.body,
                retry_after_history.last().copied(),
            ),
            None => {
                let detail = retry_history_text(&retry_after_history);
                let mut builder = base_builder(
                    &ctx,
                    AccountErrorKind::Transport(TransportErrorKind::Network),
                    Cause::Transport(TransportCause {
                        kind: TransportKind::Network,
                        message: detail.clone(),
                    }),
                );
                if let Some(text) = detail {
                    builder = builder.text(text);
                }
                finish(push_attempt(builder, TransmissionState::InFlight), &ctx)
            }
        },
        Error::AuthLost {
            transmission_state,
            final_response,
        } => auth_lost(&ctx, transmission_state, final_response.as_ref()),
        Error::RateLimited {
            retry_after,
            final_response,
        } => rate_limited(&ctx, retry_after, &final_response),
        Error::Cancelled => {
            let builder = base_builder(
                &ctx,
                AccountErrorKind::Transport(TransportErrorKind::Network),
                Cause::Transport(TransportCause {
                    kind: TransportKind::Network,
                    message: Some(DiagnosticText::support_only("request cancelled")),
                }),
            )
            .text(DiagnosticText::support_only("request cancelled"));
            finish(push_attempt(builder, TransmissionState::InFlight), &ctx)
        }
        Error::CostExceedsBurst { cost, burst } => invalid_argument(
            &ctx,
            "request_cost",
            format!("request cost {cost} exceeds bucket burst {burst}"),
        ),
        Error::EncodeBody { message, source } => invalid_argument_with_source(
            &ctx,
            "body",
            message,
            source.as_ref().map(|source| source.to_string()),
        ),
        Error::InvalidHeader { message, source } => invalid_argument_with_source(
            &ctx,
            "header",
            message,
            source.as_ref().map(|source| source.to_string()),
        ),
        Error::InvalidRequest { field, detail } => invalid_argument(&ctx, field, detail),
        Error::RefreshFailed {
            retry_after,
            source,
        } => refresh_failed(&ctx, retry_after, source.as_ref()),
        Error::RangeNotHonored { kind, message } => match kind {
            RangeFailureKind::LocalInvalid => invalid_argument(&ctx, "range", message),
            RangeFailureKind::ResponseNotPartial
            | RangeFailureKind::MissingContentRange
            | RangeFailureKind::ContentRangeMismatch => {
                contract_violation(&ctx, message, TransmissionState::Acknowledged)
            }
        },
        Error::NetSetup { message, source } => invalid_argument_with_source(
            &ctx,
            "client_config",
            message,
            source.as_ref().map(|source| source.to_string()),
        ),
        Error::RedirectRejected { message } => {
            let builder = invalid_argument_builder(&ctx, "redirect_policy", message);
            finish(push_attempt(builder, TransmissionState::Acknowledged), &ctx)
        }
        Error::MalformedRedirect { message, .. } => {
            contract_violation(&ctx, message, TransmissionState::Acknowledged)
        }
        Error::RedirectLoop { hops } => contract_violation(
            &ctx,
            format!("redirect chain exceeded configured maximum after {hops} hops"),
            TransmissionState::Acknowledged,
        ),
    }
}

fn transport_or_partial(
    ctx: &NetErrorContext,
    transport_kind: TransportKind,
    error_kind: TransportErrorKind,
    message: String,
    transmission_state: TransmissionState,
    source_message: Option<String>,
) -> AccountError {
    if transmission_state == TransmissionState::Acknowledged {
        let detail = match source_message {
            Some(source) if !source.trim().is_empty() => {
                format!("{message}; source: {source}")
            }
            _ => message,
        };
        return partial_response(ctx, detail, transmission_state);
    }

    let diagnostic = maybe_support_text(message.clone());
    let mut builder = base_builder(
        ctx,
        AccountErrorKind::Transport(error_kind),
        Cause::Transport(TransportCause {
            kind: transport_kind,
            message: diagnostic.clone(),
        }),
    );
    if let Some(text) = diagnostic {
        builder = builder.text(text);
    }
    if let Some(source) = maybe_support_text_from_option(source_message) {
        builder = builder.text(source);
    }
    finish(push_attempt(builder, transmission_state), ctx)
}

fn auth_lost(
    ctx: &NetErrorContext,
    transmission_state: Option<TransmissionState>,
    final_response: Option<&FinalResponse>,
) -> AccountError {
    let mut builder = base_builder(
        ctx,
        AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
        Cause::Auth(AuthCause::ReauthorizationRequired),
    );
    if let Some(response) = final_response {
        builder = response_diagnostics(builder, response.status, &response.headers, &response.body);
    }
    if let Some(state) = transmission_state {
        builder = push_attempt(builder, state);
    }
    finish(builder, ctx)
}

fn refresh_failed(
    ctx: &NetErrorContext,
    retry_after: Option<SystemTime>,
    source: &Error,
) -> AccountError {
    let mut builder = base_builder(
        ctx,
        AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient),
        Cause::Auth(AuthCause::RefreshTransient),
    )
    .text(DiagnosticText::support_only(format!(
        "OAuth refresh failed: {source}"
    )));

    if let Some(cause) = support_cause_from_source(source) {
        builder = builder.push_cause(cause);
    }
    if let Some(deadline) = retry_after {
        builder = builder.retry_not_before(deadline);
    }
    finish(builder, ctx)
}

fn rate_limited(
    ctx: &NetErrorContext,
    retry_after: Option<Duration>,
    final_response: &FinalResponse,
) -> AccountError {
    let retry_after =
        retry_after.or_else(|| parse_retry_after(final_response.headers.get(RETRY_AFTER)));
    let mut builder = base_builder(
        ctx,
        AccountErrorKind::Server(ServerErrorKind::RateLimited),
        Cause::Server(ServerCause::RateLimited { retry_after }),
    );
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    builder = response_diagnostics(
        builder,
        final_response.status,
        &final_response.headers,
        &final_response.body,
    );
    if let Some(deadline) = retry_deadline(retry_after) {
        builder = builder.retry_not_before(deadline);
    }
    if let Some(scope) = throttle_scope(ctx) {
        builder = builder.throttle_scope(scope);
    }
    finish(builder, ctx)
}

fn status_error(
    ctx: &NetErrorContext,
    status: StatusCode,
    headers: &HeaderMap,
    body: &Bytes,
    retry_hint: Option<Duration>,
) -> AccountError {
    let code = status.as_u16();
    let retry_after = retry_hint.or_else(|| parse_retry_after(headers.get(RETRY_AFTER)));
    let (kind, cause, throttle) = status_kind_cause(ctx, code, body, retry_after);
    let apply_retry_deadline = should_apply_retry_deadline(&kind);
    let mut builder = base_builder(ctx, kind, cause);
    builder = push_attempt(builder, TransmissionState::Acknowledged);
    builder = response_diagnostics(builder, status, headers, body);
    if apply_retry_deadline {
        if let Some(deadline) = retry_deadline(retry_after) {
            builder = builder.retry_not_before(deadline);
        }
    }
    if throttle {
        if let Some(scope) = throttle_scope(ctx) {
            builder = builder.throttle_scope(scope);
        }
    }
    finish(builder, ctx)
}

fn status_kind_cause(
    ctx: &NetErrorContext,
    code: u16,
    body: &Bytes,
    retry_after: Option<Duration>,
) -> (AccountErrorKind, Cause, bool) {
    match code {
        400 | 422 => {
            let detail = status_detail(code, body);
            (
                AccountErrorKind::Request(RequestErrorKind::Malformed),
                Cause::Request(RequestCause::Malformed { detail }),
                false,
            )
        }
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
            None => server_error(code),
        },
        409 => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
            false,
        ),
        410 if matches!(ctx.scope.as_ref(), Some(ErrorScope::Cursor(_))) => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
            false,
        ),
        410 => server_error(code),
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
        500..=599 => server_error(code),
        _ => server_error(code),
    }
}

fn should_apply_retry_deadline(kind: &AccountErrorKind) -> bool {
    matches!(
        kind,
        AccountErrorKind::Server(
            ServerErrorKind::Unavailable
                | ServerErrorKind::RateLimited
                | ServerErrorKind::QuotaExhausted
                | ServerErrorKind::Error {
                    status: Some(500..=599),
                }
        )
    )
}

fn server_error(code: u16) -> (AccountErrorKind, Cause, bool) {
    (
        AccountErrorKind::Server(ServerErrorKind::Error { status: Some(code) }),
        Cause::Server(ServerCause::Error { status: code }),
        false,
    )
}

fn invalid_argument(ctx: &NetErrorContext, field: &'static str, detail: String) -> AccountError {
    let builder = invalid_argument_builder(ctx, field, detail);
    finish(builder, ctx)
}

fn invalid_argument_with_source(
    ctx: &NetErrorContext,
    field: &'static str,
    detail: String,
    source: Option<String>,
) -> AccountError {
    let mut builder = invalid_argument_builder(ctx, field, detail);
    if let Some(text) = maybe_support_text_from_option(source) {
        builder = builder.text(text);
    }
    finish(builder, ctx)
}

fn invalid_argument_builder(
    ctx: &NetErrorContext,
    field: &'static str,
    detail: String,
) -> AccountErrorBuilder {
    let message = maybe_support_text(detail);
    let mut builder = base_builder(
        ctx,
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some(field),
            message: message.clone(),
        }),
    );
    if let Some(text) = message {
        builder = builder.text(text);
    }
    builder
}

fn contract_violation(
    ctx: &NetErrorContext,
    detail: String,
    transmission_state: TransmissionState,
) -> AccountError {
    let builder = base_builder(
        ctx,
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
        malformed_response_cause(ctx, detail),
    );
    finish(push_attempt(builder, transmission_state), ctx)
}

fn partial_response(
    ctx: &NetErrorContext,
    detail: String,
    transmission_state: TransmissionState,
) -> AccountError {
    let builder = base_builder(
        ctx,
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        malformed_response_cause(ctx, detail),
    );
    finish(push_attempt(builder, transmission_state), ctx)
}

fn malformed_response_cause(ctx: &NetErrorContext, detail: String) -> Cause {
    Cause::Wire(WireCause::MalformedResponse {
        protocol: ctx.protocol,
        detail: maybe_support_text(detail),
    })
}

fn base_builder(
    ctx: &NetErrorContext,
    kind: AccountErrorKind,
    cause: Cause,
) -> AccountErrorBuilder {
    let builder = AccountErrorBuilder::new(kind, cause)
        .protocol(ctx.protocol)
        .operation(ctx.operation);
    match ctx.provider {
        Some(provider) => builder.provider(provider),
        None => builder,
    }
}

fn finish(builder: AccountErrorBuilder, ctx: &NetErrorContext) -> AccountError {
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
    builder.push_cause(Cause::Attempt(AttemptCause { transmission_state }))
}

fn response_diagnostics(
    mut builder: AccountErrorBuilder,
    status: StatusCode,
    headers: &HeaderMap,
    body: &Bytes,
) -> AccountErrorBuilder {
    builder = builder.status(status.as_u16());
    if let Some(id) = first_header(headers, REQUEST_ID_HEADERS) {
        builder = builder.request_id(id);
    }
    if let Some(trace_id) = trace_id_from_headers(headers) {
        builder = builder.trace_id(trace_id);
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

const REQUEST_ID_HEADERS: &[&str] = &[
    "x-request-id",
    "request-id",
    "x-ms-request-id",
    "x-goog-request-id",
    "x-guploader-uploadid",
];

const SUPPORT_ONLY_HEADER_TEXT: &[&str] = &[
    "traceparent",
    "x-cloud-trace-context",
    "x-ms-ags-diagnostic",
];

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

fn trace_id_from_headers(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = header_text(headers, "traceparent")
        && let Some(trace_id) = traceparent_trace_id(&value)
    {
        return Some(trace_id);
    }
    header_text(headers, "x-cloud-trace-context").and_then(|value| {
        let trace_id = match value.split_once('/') {
            Some((trace_id, _)) => trace_id.trim(),
            None => value.trim(),
        };
        if trace_id.is_empty() {
            None
        } else {
            Some(trace_id.to_owned())
        }
    })
}

fn traceparent_trace_id(value: &str) -> Option<String> {
    let mut parts = value.split('-');
    let version = parts.next()?;
    let trace_id = parts.next()?;
    let parent_id = parts.next()?;
    let flags = parts.next()?;
    if parts.next().is_some()
        || version.len() != 2
        || trace_id.len() != 32
        || parent_id.len() != 16
        || flags.len() != 2
    {
        return None;
    }
    if !trace_id.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    Some(trace_id.to_owned())
}

fn body_diagnostic(body: &Bytes) -> Option<DiagnosticText> {
    if body.is_empty() {
        return None;
    }
    Some(DiagnosticText::support_only(format!(
        "response body: {}",
        String::from_utf8_lossy(body)
    )))
}

fn status_detail(code: u16, _body: &Bytes) -> DiagnosticText {
    DiagnosticText::support_only(format!("HTTP {code} malformed request"))
}

fn maybe_support_text(value: String) -> Option<DiagnosticText> {
    if value.trim().is_empty() {
        None
    } else {
        Some(DiagnosticText::support_only(value))
    }
}

fn maybe_support_text_from_option(value: Option<String>) -> Option<DiagnosticText> {
    value.and_then(maybe_support_text)
}

fn retry_deadline(retry_after: Option<Duration>) -> Option<SystemTime> {
    retry_after.and_then(|duration| SystemTime::now().checked_add(duration))
}

fn retry_history_text(history: &[Duration]) -> Option<DiagnosticText> {
    if history.is_empty() {
        None
    } else {
        Some(DiagnosticText::support_only(format!(
            "retry budget exhausted after retry-after history: {history:?}"
        )))
    }
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
        | None => None,
    }
}

fn throttle_scope(ctx: &NetErrorContext) -> Option<ThrottleScope> {
    if ctx.protocol == Protocol::Graph || ctx.provider == Some(Provider::Microsoft) {
        Some(ThrottleScope::Tenant)
    } else if ctx.protocol == Protocol::Gmail || ctx.provider == Some(Provider::Gmail) {
        Some(ThrottleScope::Account)
    } else if ctx.protocol == Protocol::Jmap && ctx.provider == Some(Provider::Fastmail) {
        Some(ThrottleScope::Account)
    } else {
        None
    }
}

fn support_cause_from_source(error: &Error) -> Option<Cause> {
    match error {
        Error::Network { message, .. } => Some(Cause::Transport(TransportCause {
            kind: TransportKind::Network,
            message: maybe_support_text(message.clone()),
        })),
        Error::Timeout { .. } => Some(Cause::Transport(TransportCause {
            kind: TransportKind::Timeout,
            message: Some(DiagnosticText::support_only("request timed out")),
        })),
        Error::Tls { message, .. } => Some(Cause::Transport(TransportCause {
            kind: TransportKind::Tls,
            message: maybe_support_text(message.clone()),
        })),
        Error::AuthLost { .. } => Some(Cause::Auth(AuthCause::ReauthorizationRequired)),
        Error::RefreshFailed { .. } => Some(Cause::Auth(AuthCause::RefreshTransient)),
        Error::RateLimited {
            retry_after,
            final_response,
        } => Some(Cause::Server(ServerCause::RateLimited {
            retry_after: (*retry_after)
                .or_else(|| parse_retry_after(final_response.headers.get(RETRY_AFTER))),
        })),
        Error::RetryBudgetExhausted {
            final_response: Some(response),
            retry_after_history,
        } => server_cause_from_status(
            response.status.as_u16(),
            response.headers.get(RETRY_AFTER),
            retry_after_history.last().copied(),
        ),
        Error::RetryBudgetExhausted {
            final_response: None,
            ..
        } => Some(Cause::Transport(TransportCause {
            kind: TransportKind::Network,
            message: Some(DiagnosticText::support_only("retry budget exhausted")),
        })),
        Error::Status { code, headers, .. } => {
            server_cause_from_status(code.as_u16(), headers.get(RETRY_AFTER), None)
        }
        Error::Cancelled => Some(Cause::Transport(TransportCause {
            kind: TransportKind::Network,
            message: Some(DiagnosticText::support_only("request cancelled")),
        })),
        Error::CostExceedsBurst { .. }
        | Error::EncodeBody { .. }
        | Error::InvalidHeader { .. }
        | Error::InvalidRequest { .. }
        | Error::RangeNotHonored { .. }
        | Error::NetSetup { .. }
        | Error::RedirectRejected { .. }
        | Error::MalformedRedirect { .. }
        | Error::RedirectLoop { .. } => None,
    }
}

fn server_cause_from_status(
    code: u16,
    retry_after_header: Option<&reqwest::header::HeaderValue>,
    retry_hint: Option<Duration>,
) -> Option<Cause> {
    let retry_after = retry_hint.or_else(|| parse_retry_after(retry_after_header));
    match code {
        408 | 502 | 503 | 504 => Some(Cause::Server(ServerCause::Unavailable { retry_after })),
        429 => Some(Cause::Server(ServerCause::RateLimited { retry_after })),
        507 => Some(Cause::Server(ServerCause::QuotaExhausted { retry_after })),
        500..=599 => Some(Cause::Server(ServerCause::Error { status: code })),
        _ => None,
    }
}
