//! Gmail-side translation boundary from `crate::Error` to the public
//! `AccountError`.
//!
//! The new error model has exactly one public path to `AccountError`:
//! `bifrost_types::AccountErrorBuilder`. This module funnels every
//! Gmail-internal failure through that builder, deriving recovery
//! centrally rather than emitting `RecoveryClass` values directly.
//!
//! Transport-only signals delegate to `bifrost_net::into_account_error`
//! so the net layer's transmission-state evidence and retry-after
//! parsing are not duplicated. Where Gmail has a more precise
//! interpretation (Gmail JSON error reason, history-id staleness,
//! cursor envelope failure), this module builds directly with
//! `AccountErrorBuilder` and attaches `WireCause::Gmail(signal)`.

use std::time::SystemTime;

use bifrost_types::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountOperation, AuthCause, AuthErrorKind, Cause, CursorScope, DiagnosticText, ErrorScope,
    GmailSignal, ItemOutcome, MutationSuccess, ObjectId, Protocol, ProtocolErrorKind, Provider,
    RequestCause, RequestErrorKind, ResourceKind, ServerCause, ServerErrorKind, StateCause,
    SyncStateErrorKind, ThrottleScope, WireCause,
};
use bifrost_types::{BatchFailure, BatchItemId, BatchSuccess};

use crate::error::{
    Error, GmailCursorFailure, GmailErrorEnvelope, GmailLocalError, GmailResponseError,
};

/// Resource hint that drives `NotFound` and `PermissionDenied` payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GmailResource {
    Account,
    Message,
    Thread,
    Label,
    Draft,
    Identity,
    Vacation,
    Blob,
    PubSubWatch,
}

impl GmailResource {
    fn to_resource_kind(self) -> Option<ResourceKind> {
        match self {
            Self::Message => Some(ResourceKind::Message),
            Self::Thread => Some(ResourceKind::Thread),
            Self::Label => Some(ResourceKind::Mailbox),
            // Drafts/identities/vacation/blob/pubsub/account have no
            // direct `ResourceKind` analogue; the central kind enum is
            // mail-centric.
            Self::Draft
            | Self::Identity
            | Self::Vacation
            | Self::Blob
            | Self::PubSubWatch
            | Self::Account => None,
        }
    }
}

/// Per-call context for translating a Gmail error.
///
/// Constructors below should be preferred over building this by hand;
/// they encode the operation/scope/resource defaults for each Gmail
/// entry point.
#[derive(Clone, Debug)]
pub(crate) struct GmailErrorContext {
    pub(crate) operation: AccountOperation,
    pub(crate) scope: Option<ErrorScope>,
    pub(crate) resource: Option<GmailResource>,
    pub(crate) throttle_scope: Option<ThrottleScope>,
    pub(crate) idempotency_override: Option<bool>,
    /// True when the failing request targets `users.history.list`. The
    /// 404/410/`historyNotFound`/`failedPrecondition` mappings need
    /// this signal to route to `SyncState(CursorInvalid)` rather than
    /// `NotFound` or a generic provider refusal.
    pub(crate) history_endpoint: bool,
}

impl GmailErrorContext {
    pub(crate) fn base(operation: AccountOperation) -> Self {
        Self {
            operation,
            scope: None,
            resource: None,
            throttle_scope: Some(ThrottleScope::Account),
            idempotency_override: None,
            history_endpoint: false,
        }
    }

    pub(crate) fn open() -> Self {
        let mut ctx = Self::base(AccountOperation::Discover);
        ctx.resource = Some(GmailResource::Account);
        ctx
    }

    pub(crate) fn inventory() -> Self {
        let mut ctx = Self::base(AccountOperation::SyncInventory);
        ctx.scope = Some(ErrorScope::Cursor(CursorScope::Account));
        ctx.resource = Some(GmailResource::Message);
        ctx
    }

    pub(crate) fn changes() -> Self {
        let mut ctx = Self::base(AccountOperation::SyncChanges);
        ctx.scope = Some(ErrorScope::Cursor(CursorScope::Account));
        ctx.history_endpoint = true;
        ctx
    }

    pub(crate) fn establish_cursor() -> Self {
        let mut ctx = Self::base(AccountOperation::EstablishCursor);
        ctx.scope = Some(ErrorScope::Cursor(CursorScope::Account));
        ctx
    }

    pub(crate) fn hydrate_message(id: impl Into<String>) -> Self {
        let mut ctx = Self::base(AccountOperation::HydrateMessage);
        ctx.scope = Some(ErrorScope::Message { id: id.into() });
        ctx.resource = Some(GmailResource::Message);
        ctx
    }

    pub(crate) fn hydrate_thread(id: impl Into<String>) -> Self {
        let mut ctx = Self::base(AccountOperation::HydrateThread);
        ctx.scope = Some(ErrorScope::Thread { id: id.into() });
        ctx.resource = Some(GmailResource::Thread);
        ctx
    }

    pub(crate) fn mutation(operation: AccountOperation) -> Self {
        let mut ctx = Self::base(operation);
        ctx.resource = Some(GmailResource::Message);
        ctx
    }

    pub(crate) fn push_subscribe() -> Self {
        let mut ctx = Self::base(AccountOperation::PushSubscribe);
        ctx.resource = Some(GmailResource::PubSubWatch);
        ctx
    }

    pub(crate) fn push_unsubscribe() -> Self {
        let mut ctx = Self::base(AccountOperation::PushUnsubscribe);
        ctx.resource = Some(GmailResource::PubSubWatch);
        ctx
    }

    pub(crate) fn open_blob(id: impl Into<String>) -> Self {
        let mut ctx = Self::base(AccountOperation::OpenBlob);
        ctx.resource = Some(GmailResource::Blob);
        // Blob lives below a message in Gmail; use `Message` scope to
        // anchor diagnostics. The blob id itself is opaque to the
        // central error model so it is not surfaced as ErrorScope.
        let _ = id;
        ctx
    }

    pub(crate) fn open_blob_range() -> Self {
        let mut ctx = Self::base(AccountOperation::OpenBlobRange);
        ctx.resource = Some(GmailResource::Blob);
        ctx
    }

    pub(crate) fn send() -> Self {
        let mut ctx = Self::base(AccountOperation::Send);
        ctx.idempotency_override = Some(false);
        ctx
    }

    pub(crate) fn draft(operation: AccountOperation) -> Self {
        let mut ctx = Self::base(operation);
        // Drafts other than DraftSend are write-but-not-side-effectful
        // outside Gmail; only DraftSend needs explicit non-idempotent.
        if matches!(operation, AccountOperation::DraftSend) {
            ctx.idempotency_override = Some(false);
        }
        ctx.resource = Some(GmailResource::Draft);
        ctx
    }

    pub(crate) fn search() -> Self {
        Self::base(AccountOperation::Search)
    }

    pub(crate) fn search_messages() -> Self {
        Self::base(AccountOperation::SearchMessages)
    }

    pub(crate) fn containers_list() -> Self {
        let mut ctx = Self::base(AccountOperation::ContainersList);
        ctx.resource = Some(GmailResource::Label);
        ctx
    }

    pub(crate) fn container(op: AccountOperation) -> Self {
        let mut ctx = Self::base(op);
        ctx.resource = Some(GmailResource::Label);
        ctx
    }

    pub(crate) fn identities_list() -> Self {
        let mut ctx = Self::base(AccountOperation::IdentitiesList);
        ctx.resource = Some(GmailResource::Identity);
        ctx
    }

    pub(crate) fn identity_update() -> Self {
        let mut ctx = Self::base(AccountOperation::IdentityUpdate);
        ctx.resource = Some(GmailResource::Identity);
        ctx
    }

    pub(crate) fn vacation_get() -> Self {
        let mut ctx = Self::base(AccountOperation::VacationGet);
        ctx.resource = Some(GmailResource::Vacation);
        ctx
    }

    pub(crate) fn vacation_set() -> Self {
        let mut ctx = Self::base(AccountOperation::VacationSet);
        ctx.resource = Some(GmailResource::Vacation);
        ctx
    }
}

/// Translate a `crate::Error` into the public `AccountError`. This is
/// the sole boundary; no other public path produces an `AccountError`
/// in this crate.
#[must_use]
pub(crate) fn into_account_error(error: Error, ctx: GmailErrorContext) -> AccountError {
    match error {
        Error::Net(net_error) => translate_net_error(net_error, &ctx),
        Error::Response(resp) => translate_response(*resp, &ctx),
        Error::JsonDecode { source, .. } => {
            parse_failed(&ctx, format!("Gmail JSON decode: {source}"))
        }
        Error::Base64 { encoding, source } => {
            parse_failed(&ctx, format!("Gmail {encoding} decode failed: {source}"))
        }
        Error::Local(local) => translate_local(local, &ctx),
    }
}

/// Translate a per-batch mutation error into `ItemOutcome` lanes.
///
/// Returns `Err(AccountError)` when the failure terminates the stream
/// (retryable transport / auth / server-rate / quota / unknown-pre-
/// commit). Returns `Ok(Vec<ItemOutcome<MutationSuccess>>)` when the
/// failure is per-item (permanent provider refusal of a transmitted
/// batch, 404 for every id, etc.). `AccountError` is cloneable, so the
/// per-id outcomes carry direct clones - no template helper is needed.
pub(crate) fn mutation_error(
    ids: &[ObjectId],
    error: Error,
    ctx: GmailErrorContext,
) -> Result<Vec<ItemOutcome<MutationSuccess>>, AccountError> {
    let account_error = into_account_error(error, ctx);
    if terminates_mutation_stream(&account_error) {
        return Err(account_error);
    }

    // Build per-id outcomes. 404 for a transmitted batch is a
    // `NotFound(Message)` per id; any other permanent provider error
    // also lands as `Failed` per id with the cloned error.
    let outcomes = ids
        .iter()
        .map(|id| {
            ItemOutcome::Failed(BatchFailure::new(
                BatchItemId(id.0.clone()),
                account_error.clone(),
            ))
        })
        .collect();
    Ok(outcomes)
}

/// Build per-id `MutationSuccess::Applied` outcomes for a fully
/// successful batch.
pub(crate) fn applied_outcomes(ids: &[ObjectId]) -> Vec<ItemOutcome<MutationSuccess>> {
    ids.iter()
        .map(|id| {
            ItemOutcome::Succeeded(BatchSuccess::new(
                BatchItemId(id.0.clone()),
                MutationSuccess::Applied,
            ))
        })
        .collect()
}

/// Build per-id `MutationSuccess::Skipped` outcomes (empty patch or
/// unsupported flag set).
pub(crate) fn skipped_outcomes(ids: &[ObjectId]) -> Vec<ItemOutcome<MutationSuccess>> {
    ids.iter()
        .map(|id| {
            ItemOutcome::Succeeded(BatchSuccess::new(
                BatchItemId(id.0.clone()),
                MutationSuccess::Skipped,
            ))
        })
        .collect()
}

/// Detection helper for the TRASH-fallback path. Returns true when
/// the structured Gmail signal is `insufficientPermissions`,
/// `forbidden`, or an HTTP 403 with no parsed Gmail reason - i.e. the
/// token cannot permanently delete and the driver should retry the
/// batch as a TRASH label patch.
pub(crate) fn is_batch_delete_scope_failure(error: &Error) -> bool {
    let Error::Response(resp) = error else {
        return false;
    };
    if resp.status != 403 {
        return false;
    }
    let Some(env) = resp.envelope.as_ref() else {
        return true;
    };
    matches!(
        env.primary_reason(),
        Some("forbidden") | Some("insufficientPermissions") | None
    )
}

/// Build a Gmail-specific `AccountError` for the TRASH fallback path
/// when both primary delete and fallback fail. The fallback error is
/// the outermost interpretation; the primary delete's outermost cause
/// is attached as a secondary cause so the support export carries
/// both signals.
///
/// Uses `AccountError::into_builder()` to preserve all fallback fields
/// and derived values, then pushes the primary's outermost cause as
/// secondary evidence and adds a diagnostic note.
pub(crate) fn merge_delete_fallback_error(
    fallback: AccountError,
    primary: &AccountError,
) -> AccountError {
    let primary_outermost = primary.chain().outermost().clone();
    let primary_kind = primary
        .telemetry_fields()
        .native_code
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{:?}", primary.kind()));
    fallback
        .into_builder()
        .push_cause(primary_outermost)
        .text(DiagnosticText::support_only(format!(
            "primary batchDelete failed before TRASH fallback: {primary_kind}"
        )))
        .build()
}

// ---------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------

fn terminates_mutation_stream(err: &AccountError) -> bool {
    use bifrost_types::RecoveryClass;
    match err.recovery() {
        RecoveryClass::Retry(_)
        | RecoveryClass::Reconcile(_)
        | RecoveryClass::Engine(_)
        | RecoveryClass::AuthLost
        | RecoveryClass::NeedsAdminConsent { .. } => true,
        RecoveryClass::NeedsPolicyChange
        | RecoveryClass::NoPermission { .. }
        | RecoveryClass::Unsupported(_)
        | RecoveryClass::ClientBug
        | RecoveryClass::ProviderContractViolation
        | RecoveryClass::ProviderRefused
        | RecoveryClass::UnknownPermanent => {
            // Permanent terminal: per-id outcomes carry the same error.
            // Only NotFound is a clean per-id lane; everything else
            // also fans out per-id but is still terminal for the
            // engine's retry machinery.
            !matches!(err.kind(), AccountErrorKind::NotFound(_))
                && !matches!(
                    err.kind(),
                    AccountErrorKind::Server(ServerErrorKind::Error { .. })
                )
        }
        _ => true,
    }
}

fn translate_net_error(net_error: bifrost_net::Error, ctx: &GmailErrorContext) -> AccountError {
    // For HTTP status responses the bifrost-net translator does generic
    // HTTP mapping. Gmail JSON reason codes are more precise, so we
    // peek at the body first.
    if let bifrost_net::Error::Status {
        ref code,
        ref body,
        ref headers,
    } = net_error
        && let Some(refined) =
            refine_status_with_gmail_body(code.as_u16(), headers, body.as_ref(), ctx)
    {
        return refined;
    }

    // For rate-limit / retry-budget exhaustion paths where the net
    // error carries a final response, attempt to refine using the
    // Gmail JSON reason. If parsing fails, the net-derived
    // classification stands.
    if let Some(refined) = try_refine_net_with_final_response(&net_error, ctx) {
        return refined;
    }

    let net_ctx = bifrost_net::NetErrorContext {
        provider: Some(Provider::Gmail),
        protocol: Protocol::Gmail,
        operation: ctx.operation,
        scope: ctx.scope.clone(),
    };
    bifrost_net::into_account_error(net_error, net_ctx)
}

fn try_refine_net_with_final_response(
    net_error: &bifrost_net::Error,
    ctx: &GmailErrorContext,
) -> Option<AccountError> {
    let response = match net_error {
        bifrost_net::Error::RateLimited { final_response, .. } => Some(final_response),
        bifrost_net::Error::RetryBudgetExhausted {
            final_response: Some(response),
            ..
        } => Some(response),
        _ => None,
    }?;
    refine_status_with_gmail_body(
        response.status.as_u16(),
        &response.headers,
        response.body.as_ref(),
        ctx,
    )
}

fn refine_status_with_gmail_body(
    status: u16,
    headers: &reqwest::header::HeaderMap,
    body: &[u8],
    ctx: &GmailErrorContext,
) -> Option<AccountError> {
    let env = parse_envelope(body)?;
    let resp_headers = crate::error::GmailResponseHeaders::from_headers(headers);
    let resp = GmailResponseError {
        service: crate::error::GmailService::GmailApi,
        status,
        headers: resp_headers,
        body: bytes::Bytes::copy_from_slice(body),
        envelope: Some(env),
    };
    Some(translate_response(resp, ctx))
}

fn parse_envelope(body: &[u8]) -> Option<GmailErrorEnvelope> {
    #[derive(serde::Deserialize)]
    struct Wrapper {
        error: GmailErrorEnvelope,
    }
    serde_json::from_slice::<Wrapper>(body)
        .ok()
        .map(|w| w.error)
}

fn translate_response(resp: GmailResponseError, ctx: &GmailErrorContext) -> AccountError {
    let status = resp.status;
    let env = resp.envelope.as_ref();
    let reason = env
        .and_then(GmailErrorEnvelope::primary_reason)
        .map(str::to_owned);
    let message = env
        .and_then(GmailErrorEnvelope::primary_message)
        .map(str::to_owned);
    let signal = reason
        .as_deref()
        .map(map_reason_to_signal)
        .unwrap_or_else(|| GmailSignal::Unknown {
            code: status.to_string(),
        });

    let (kind, cause) = classify_response(status, &signal, ctx);

    let mut builder = AccountErrorBuilder::new(kind, cause)
        .provider(Provider::Gmail)
        .protocol(Protocol::Gmail)
        .operation(ctx.operation)
        .status(Some(status));

    if let Some(scope) = ctx.scope.clone() {
        builder = builder.scope(scope);
    }

    // Native code: prefer Gmail reason, then top-level status, then HTTP status.
    let native = reason
        .clone()
        .or_else(|| env.and_then(|e| e.status.clone()))
        .unwrap_or_else(|| status.to_string());
    builder = builder.native_code(native);

    builder = builder.push_cause(Cause::Wire(WireCause::Gmail(signal.clone())));

    if let Some(id) = resp.headers.request_id.as_deref() {
        builder = builder.request_id(id.to_owned());
    }
    if let Some(id) = resp.headers.trace_id.as_deref() {
        builder = builder.trace_id(id.to_owned());
    }

    if let Some(msg) = message {
        builder = builder.text(DiagnosticText::support_only(msg));
    } else if !resp.body.is_empty() {
        builder = builder.text(DiagnosticText::support_only(format!(
            "gmail response body: {}",
            String::from_utf8_lossy(resp.body.as_ref()),
        )));
    }

    if let Some(retry_after) = resp.headers.retry_after
        && let Some(deadline) = SystemTime::now().checked_add(retry_after)
        && is_retry_after_applicable(status, &signal)
    {
        builder = builder.retry_not_before(deadline);
    }

    if let Some(throttle) = throttle_scope_for(status, &signal, ctx) {
        builder = builder.throttle_scope(throttle);
    }

    if let Some(idem) = ctx.idempotency_override {
        builder = builder.idempotency_override(idem);
    }

    builder.build()
}

fn classify_response(
    status: u16,
    signal: &GmailSignal,
    ctx: &GmailErrorContext,
) -> (AccountErrorKind, Cause) {
    // First, dispatch on Gmail reason (most precise).
    match signal {
        GmailSignal::InvalidQuery => {
            return malformed_kind_cause(format!("HTTP {status} invalidQuery"));
        }
        GmailSignal::InvalidCredentials | GmailSignal::AuthError => {
            return (
                AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
                Cause::Auth(AuthCause::ReauthorizationRequired),
            );
        }
        GmailSignal::Forbidden => {
            return (
                AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
                Cause::Access(AccessCause::PermissionDenied {
                    resource: ctx.resource.and_then(GmailResource::to_resource_kind),
                }),
            );
        }
        GmailSignal::QuotaExceeded => {
            return (
                AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
                Cause::Server(ServerCause::QuotaExhausted { retry_after: None }),
            );
        }
        GmailSignal::RateLimitExceeded | GmailSignal::UserRateLimitExceeded => {
            return (
                AccountErrorKind::Server(ServerErrorKind::RateLimited),
                Cause::Server(ServerCause::RateLimited { retry_after: None }),
            );
        }
        GmailSignal::BackendError => {
            return (
                AccountErrorKind::Server(ServerErrorKind::Unavailable),
                Cause::Server(ServerCause::Unavailable { retry_after: None }),
            );
        }
        GmailSignal::FailedPrecondition if ctx.history_endpoint => {
            return (
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                Cause::State(StateCause::CursorInvalid),
            );
        }
        GmailSignal::FailedPrecondition => {
            return malformed_kind_cause("failedPrecondition outside history context".to_owned());
        }
        GmailSignal::HistoryNotFound => {
            return (
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                Cause::State(StateCause::CursorInvalid),
            );
        }
        GmailSignal::NotFound if ctx.history_endpoint => {
            return (
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                Cause::State(StateCause::CursorInvalid),
            );
        }
        GmailSignal::NotFound => return not_found_kind_cause(ctx),
        GmailSignal::PreconditionFailed => {
            return (
                AccountErrorKind::ConcurrencyConflict,
                Cause::State(StateCause::ConcurrencyConflict),
            );
        }
        GmailSignal::PubSubSubscriptionDeleted | GmailSignal::PubSubSubscriptionExpired
            if matches!(ctx.operation, AccountOperation::PushSubscribe) =>
        {
            return (
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                Cause::State(StateCause::CursorInvalid),
            );
        }
        GmailSignal::PubSubSubscriptionDeleted | GmailSignal::PubSubSubscriptionExpired => {
            return (
                AccountErrorKind::Server(ServerErrorKind::Unavailable),
                Cause::Server(ServerCause::Unavailable { retry_after: None }),
            );
        }
        GmailSignal::Unknown { code } => {
            // Specific stable Google reasons mapped explicitly per
            // the Gmail plan; these intentionally use Unknown { code }
            // and rely on `native_code` for downstream filters.
            match code.as_str() {
                "invalidArgument" => {
                    return malformed_kind_cause(format!("HTTP {status} invalidArgument"));
                }
                "insufficientPermissions" => {
                    return (
                        AccountErrorKind::Authorization(AccessErrorKind::InsufficientScope),
                        Cause::Access(AccessCause::InsufficientScope {
                            needed: "gmail.modify",
                        }),
                    );
                }
                "domainPolicy" => {
                    return (
                        AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked),
                        Cause::Access(AccessCause::PolicyBlocked),
                    );
                }
                "dailyLimitExceeded" => {
                    return (
                        AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
                        Cause::Server(ServerCause::QuotaExhausted { retry_after: None }),
                    );
                }
                "conditionNotMet" => {
                    return (
                        AccountErrorKind::ConcurrencyConflict,
                        Cause::State(StateCause::ConcurrencyConflict),
                    );
                }
                "notAuthorizedToAccessThisResource" => {
                    return (
                        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
                        Cause::Access(AccessCause::PermissionDenied {
                            resource: ctx.resource.and_then(GmailResource::to_resource_kind),
                        }),
                    );
                }
                _ => {}
            }
            // Fall through to HTTP status mapping.
        }
        _ => {}
    }

    // HTTP status fallback (used when no recognized Gmail reason
    // matched, e.g. an unknown reason with a stable status code).
    classify_by_status(status, ctx)
}

fn classify_by_status(status: u16, ctx: &GmailErrorContext) -> (AccountErrorKind, Cause) {
    match status {
        400 if ctx.history_endpoint => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        ),
        400 | 422 => malformed_kind_cause(format!("HTTP {status}")),
        401 => (
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
        ),
        403 => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: ctx.resource.and_then(GmailResource::to_resource_kind),
            }),
        ),
        404 if ctx.history_endpoint => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        ),
        404 => not_found_kind_cause(ctx),
        410 if ctx.history_endpoint => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        ),
        412 => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
        ),
        413 => malformed_kind_cause("HTTP 413 payload too large".to_owned()),
        429 => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_after: None }),
        ),
        500..=599 => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_after: None }),
        ),
        other => (
            AccountErrorKind::Server(ServerErrorKind::Error {
                status: Some(other),
            }),
            Cause::Server(ServerCause::Error {
                status: Some(other),
            }),
        ),
    }
}

fn malformed_kind_cause(detail: String) -> (AccountErrorKind, Cause) {
    (
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail),
        }),
    )
}

fn not_found_kind_cause(ctx: &GmailErrorContext) -> (AccountErrorKind, Cause) {
    let what = ctx
        .resource
        .and_then(GmailResource::to_resource_kind)
        .unwrap_or(ResourceKind::Message);
    let id = ctx.scope.as_ref().and_then(|scope| match scope {
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
    });
    (
        AccountErrorKind::NotFound(what),
        Cause::Request(RequestCause::NotFound { what, id }),
    )
}

fn is_retry_after_applicable(status: u16, signal: &GmailSignal) -> bool {
    matches!(status, 429 | 500..=599)
        || matches!(
            signal,
            GmailSignal::RateLimitExceeded
                | GmailSignal::UserRateLimitExceeded
                | GmailSignal::QuotaExceeded
                | GmailSignal::BackendError
        )
}

fn throttle_scope_for(
    status: u16,
    signal: &GmailSignal,
    ctx: &GmailErrorContext,
) -> Option<ThrottleScope> {
    let is_throttle_status = matches!(status, 429)
        || matches!(
            signal,
            GmailSignal::RateLimitExceeded
                | GmailSignal::UserRateLimitExceeded
                | GmailSignal::QuotaExceeded
        )
        || matches!(signal, GmailSignal::Unknown { code } if code == "dailyLimitExceeded");
    if is_throttle_status {
        ctx.throttle_scope.or(Some(ThrottleScope::Account))
    } else {
        None
    }
}

fn parse_failed(ctx: &GmailErrorContext, detail: String) -> AccountError {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Gmail,
            detail: Some(DiagnosticText::support_only(detail.clone())),
        }),
    )
    .provider(Provider::Gmail)
    .protocol(Protocol::Gmail)
    .operation(ctx.operation)
    .text(DiagnosticText::support_only(detail));
    if let Some(scope) = ctx.scope.clone() {
        builder = builder.scope(scope);
    }
    builder.build()
}

fn malformed_request(ctx: &GmailErrorContext, detail: String) -> AccountError {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail.clone()),
        }),
    )
    .provider(Provider::Gmail)
    .protocol(Protocol::Gmail)
    .operation(ctx.operation)
    .text(DiagnosticText::support_only(detail));
    if let Some(scope) = ctx.scope.clone() {
        builder = builder.scope(scope);
    }
    builder.build()
}

fn missing_field(ctx: &GmailErrorContext, field: &'static str, detail: String) -> AccountError {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::MissingField),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Gmail,
            detail: Some(DiagnosticText::support_only(format!(
                "missing field `{field}`: {detail}"
            ))),
        }),
    )
    .provider(Provider::Gmail)
    .protocol(Protocol::Gmail)
    .operation(ctx.operation)
    .text(DiagnosticText::support_only(format!(
        "missing field `{field}`: {detail}"
    )));
    if let Some(scope) = ctx.scope.clone() {
        builder = builder.scope(scope);
    }
    builder.build()
}

fn translate_local(local: GmailLocalError, ctx: &GmailErrorContext) -> AccountError {
    match local {
        GmailLocalError::Unsupported { operation, detail } => {
            let mut builder = AccountErrorBuilder::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
            .provider(Provider::Gmail)
            .protocol(Protocol::Gmail)
            .operation(operation);
            if let Some(scope) = ctx.scope.clone() {
                builder = builder.scope(scope);
            }
            if let Some(text) = detail {
                builder = builder.text(DiagnosticText::support_only(text));
            }
            builder.build()
        }
        GmailLocalError::InvalidRequest { operation, detail } => {
            let mut new_ctx = ctx.clone();
            new_ctx.operation = operation;
            malformed_request(&new_ctx, detail)
        }
        GmailLocalError::InvalidCursor { kind, detail } => {
            let (sync_kind, cause) = match kind {
                GmailCursorFailure::ProtocolMismatch
                | GmailCursorFailure::EnvelopeMismatch
                | GmailCursorFailure::SchemaMismatch => (
                    AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
                    Cause::State(StateCause::SchemaIncompatible),
                ),
                GmailCursorFailure::MalformedPayload => (
                    AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                    Cause::State(StateCause::CursorInvalid),
                ),
            };
            let mut builder = AccountErrorBuilder::new(sync_kind, cause)
                .provider(Provider::Gmail)
                .protocol(Protocol::Gmail)
                .operation(ctx.operation)
                .scope(ErrorScope::Cursor(CursorScope::Account))
                .text(DiagnosticText::support_only(detail));
            builder = builder.native_code(match kind {
                GmailCursorFailure::ProtocolMismatch => "cursor_protocol_mismatch",
                GmailCursorFailure::EnvelopeMismatch => "cursor_envelope_mismatch",
                GmailCursorFailure::SchemaMismatch => "cursor_schema_mismatch",
                GmailCursorFailure::MalformedPayload => "cursor_malformed",
            });
            builder.build()
        }
        GmailLocalError::AccountIdentityMismatch {
            cursor_email,
            profile_email,
        } => AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
            Cause::State(StateCause::SchemaIncompatible),
        )
        .provider(Provider::Gmail)
        .protocol(Protocol::Gmail)
        .operation(ctx.operation)
        .scope(ErrorScope::Cursor(CursorScope::Account))
        .native_code("cursor_account_identity_mismatch")
        .text(DiagnosticText::support_only(format!(
            "cursor account {cursor_email} != open account {profile_email}"
        )))
        .build(),
        GmailLocalError::MissingField { field, detail } => missing_field(ctx, field, detail),
        GmailLocalError::BlobRangeUnsupported { blob_id } => {
            let mut builder = AccountErrorBuilder::new(
                AccountErrorKind::Unsupported(AccountOperation::OpenBlobRange),
                Cause::Request(RequestCause::Unsupported {
                    operation: AccountOperation::OpenBlobRange,
                }),
            )
            .provider(Provider::Gmail)
            .protocol(Protocol::Gmail)
            .operation(AccountOperation::OpenBlobRange)
            .text(DiagnosticText::support_only(format!(
                "gmail blob {blob_id} does not support range reads"
            )));
            if let Some(scope) = ctx.scope.clone() {
                builder = builder.scope(scope);
            }
            builder.build()
        }
        GmailLocalError::Internal { detail } => {
            // Internal failures classify as protocol contract violations
            // so the engine routes them to telemetry rather than retry.
            let mut builder = AccountErrorBuilder::new(
                AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
                Cause::Wire(WireCause::MalformedResponse {
                    protocol: Protocol::Gmail,
                    detail: Some(DiagnosticText::support_only(detail.clone())),
                }),
            )
            .provider(Provider::Gmail)
            .protocol(Protocol::Gmail)
            .operation(ctx.operation)
            .text(DiagnosticText::support_only(detail));
            if let Some(scope) = ctx.scope.clone() {
                builder = builder.scope(scope);
            }
            builder.build()
        }
    }
}

fn map_reason_to_signal(reason: &str) -> GmailSignal {
    match reason {
        "invalidQuery" => GmailSignal::InvalidQuery,
        "failedPrecondition" => GmailSignal::FailedPrecondition,
        "invalidCredentials" => GmailSignal::InvalidCredentials,
        "authError" => GmailSignal::AuthError,
        "quotaExceeded" => GmailSignal::QuotaExceeded,
        "rateLimitExceeded" => GmailSignal::RateLimitExceeded,
        "userRateLimitExceeded" => GmailSignal::UserRateLimitExceeded,
        "forbidden" => GmailSignal::Forbidden,
        "notFound" => GmailSignal::NotFound,
        "preconditionFailed" => GmailSignal::PreconditionFailed,
        "backendError" => GmailSignal::BackendError,
        "historyNotFound" => GmailSignal::HistoryNotFound,
        "pubsubSubscriptionDeleted" => GmailSignal::PubSubSubscriptionDeleted,
        "pubsubSubscriptionExpired" => GmailSignal::PubSubSubscriptionExpired,
        other => GmailSignal::Unknown {
            code: other.to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{
        EngineDirective, RecoveryClass, RetryDisposition, RetryReason, TransportErrorKind,
    };
    use bytes::Bytes;

    fn gmail_response(status: u16, body: &str) -> Error {
        Error::response_from_parts(
            crate::error::GmailService::GmailApi,
            status,
            crate::error::GmailResponseHeaders::default(),
            Bytes::copy_from_slice(body.as_bytes()),
        )
    }

    fn body_with_reason(reason: &str) -> String {
        format!(
            r#"{{"error":{{"code":429,"message":"x","errors":[{{"domain":"usageLimits","reason":"{reason}"}}]}}}}"#
        )
    }

    #[test]
    fn invalid_query_maps_to_malformed_request() {
        let err = gmail_response(400, &body_with_reason("invalidQuery"));
        let acc = into_account_error(err, GmailErrorContext::search());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
        assert!(acc.recovery().is_terminal());
    }

    #[test]
    fn invalid_argument_unknown_signal_is_malformed() {
        let err = gmail_response(400, &body_with_reason("invalidArgument"));
        let acc = into_account_error(err, GmailErrorContext::search());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
        // The builder sets native_code on diagnostics; telemetry exposes it.
        assert_eq!(acc.telemetry_fields().native_code, Some("invalidArgument"));
    }

    #[test]
    fn invalid_credentials_maps_to_reauth() {
        let err = gmail_response(401, &body_with_reason("invalidCredentials"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
        ));
        assert!(matches!(acc.recovery(), RecoveryClass::AuthLost));
    }

    #[test]
    fn auth_error_maps_to_reauth() {
        let err = gmail_response(401, &body_with_reason("authError"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
        ));
    }

    #[test]
    fn forbidden_maps_to_permission_denied() {
        let err = gmail_response(403, &body_with_reason("forbidden"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        ));
    }

    #[test]
    fn insufficient_permissions_maps_to_insufficient_scope() {
        let err = gmail_response(403, &body_with_reason("insufficientPermissions"));
        let acc = into_account_error(
            err,
            GmailErrorContext::mutation(AccountOperation::UpdateFlags),
        );
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::InsufficientScope)
        ));
    }

    #[test]
    fn domain_policy_maps_to_policy_blocked() {
        let err = gmail_response(403, &body_with_reason("domainPolicy"));
        let acc = into_account_error(err, GmailErrorContext::send());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
        ));
    }

    #[test]
    fn quota_exceeded_maps_to_quota_with_throttle() {
        let err = gmail_response(429, &body_with_reason("quotaExceeded"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
        ));
        let RecoveryClass::Retry(advice) = acc.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.reason, RetryReason::QuotaExhausted);
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
    }

    #[test]
    fn rate_limit_exceeded_maps_to_rate_limited() {
        let err = gmail_response(429, &body_with_reason("rateLimitExceeded"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Server(ServerErrorKind::RateLimited)
        ));
        let RecoveryClass::Retry(advice) = acc.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.reason, RetryReason::RateLimited);
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
    }

    #[test]
    fn user_rate_limit_uses_account_throttle() {
        let err = gmail_response(429, &body_with_reason("userRateLimitExceeded"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        let RecoveryClass::Retry(advice) = acc.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
    }

    #[test]
    fn history_failed_precondition_maps_to_cursor_invalid() {
        let err = gmail_response(400, &body_with_reason("failedPrecondition"));
        let acc = into_account_error(err, GmailErrorContext::changes());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
        assert!(matches!(
            acc.recovery(),
            RecoveryClass::Engine(EngineDirective::RestartScope(CursorScope::Account))
        ));
    }

    #[test]
    fn history_404_maps_to_cursor_invalid() {
        let err = gmail_response(404, r#"{"error":{"code":404,"message":"x"}}"#);
        let acc = into_account_error(err, GmailErrorContext::changes());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
    }

    #[test]
    fn history_410_maps_to_cursor_invalid() {
        let err = gmail_response(410, r#"{"error":{"code":410}}"#);
        let acc = into_account_error(err, GmailErrorContext::changes());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
    }

    #[test]
    fn non_history_404_maps_to_not_found() {
        let err = gmail_response(404, r#"{"error":{"code":404}}"#);
        let acc = into_account_error(err, GmailErrorContext::hydrate_message("m1"));
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::NotFound(ResourceKind::Message)
        ));
    }

    #[test]
    fn http_500_maps_to_server_unavailable() {
        let err = gmail_response(500, "internal server error");
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Server(ServerErrorKind::Unavailable)
        ));
        let RecoveryClass::Retry(advice) = acc.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.reason, RetryReason::ServerUnavailable);
    }

    #[test]
    fn http_503_maps_to_server_unavailable() {
        let err = gmail_response(503, "");
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Server(ServerErrorKind::Unavailable)
        ));
    }

    #[test]
    fn unparseable_body_falls_back_by_status() {
        let err = gmail_response(500, "<html>oops</html>");
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Server(ServerErrorKind::Unavailable)
        ));
        // Support-only text retains the raw body.
        let support = acc.support_consented();
        assert!(
            support
                .support_text
                .iter()
                .any(|t| t.contains("html") || t.contains("oops"))
        );
    }

    #[test]
    fn net_transport_delegates_with_gmail_context() {
        let net_err = bifrost_net::Error::Network {
            message: "dns failure".to_owned(),
            transmission_state: bifrost_types::TransmissionState::Unsent,
            source: None,
        };
        let acc = into_account_error(Error::Net(net_err), GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Transport(TransportErrorKind::Network)
        ));
        assert_eq!(acc.provider(), Some(Provider::Gmail));
        assert_eq!(acc.protocol(), Some(Protocol::Gmail));
        let RecoveryClass::Retry(advice) = acc.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.disposition, RetryDisposition::SameRequest);
    }

    #[test]
    fn cursor_protocol_mismatch_maps_to_schema_incompatible() {
        let err = Error::Local(GmailLocalError::InvalidCursor {
            kind: GmailCursorFailure::ProtocolMismatch,
            detail: "expected gmail".to_owned(),
        });
        let acc = into_account_error(err, GmailErrorContext::establish_cursor());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
        assert!(matches!(
            acc.recovery(),
            RecoveryClass::Engine(EngineDirective::SchemaIncompatible)
        ));
    }

    #[test]
    fn cursor_malformed_payload_maps_to_cursor_invalid() {
        let err = Error::Local(GmailLocalError::InvalidCursor {
            kind: GmailCursorFailure::MalformedPayload,
            detail: "truncated".to_owned(),
        });
        let acc = into_account_error(err, GmailErrorContext::establish_cursor());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
        assert!(matches!(
            acc.recovery(),
            RecoveryClass::Engine(EngineDirective::RestartScope(CursorScope::Account))
        ));
    }

    #[test]
    fn cursor_account_identity_mismatch_maps_to_schema_incompatible() {
        let err = Error::Local(GmailLocalError::AccountIdentityMismatch {
            cursor_email: "old@example.com".to_owned(),
            profile_email: "new@example.com".to_owned(),
        });
        let acc = into_account_error(err, GmailErrorContext::changes());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
    }

    #[test]
    fn unsupported_local_maps_to_unsupported() {
        let err = Error::unsupported(AccountOperation::SyncInventory);
        let acc = into_account_error(
            err,
            GmailErrorContext::base(AccountOperation::SyncInventory),
        );
        assert!(matches!(acc.kind(), AccountErrorKind::Unsupported(_)));
    }

    #[test]
    fn mutation_unsupported_flag_yields_skipped() {
        let ids = vec![ObjectId("m1".into()), ObjectId("m2".into())];
        let outcomes = skipped_outcomes(&ids);
        assert_eq!(outcomes.len(), 2);
        assert!(matches!(
            outcomes[0],
            ItemOutcome::Succeeded(BatchSuccess {
                output: MutationSuccess::Skipped,
                ..
            })
        ));
    }

    #[test]
    fn mutation_permanent_404_becomes_failed_per_id() {
        let ids = vec![ObjectId("m1".into()), ObjectId("m2".into())];
        let err = gmail_response(404, &body_with_reason("notFound"));
        let outcomes = mutation_error(
            &ids,
            err,
            GmailErrorContext::mutation(AccountOperation::UpdateFlags),
        )
        .expect("per-item failures");
        assert_eq!(outcomes.len(), 2);
        assert!(matches!(
            outcomes[0],
            ItemOutcome::Failed(BatchFailure { .. })
        ));
    }

    #[test]
    fn mutation_429_terminates_stream() {
        let ids = vec![ObjectId("m1".into())];
        let err = gmail_response(429, &body_with_reason("rateLimitExceeded"));
        let result = mutation_error(
            &ids,
            err,
            GmailErrorContext::mutation(AccountOperation::UpdateFlags),
        );
        assert!(result.is_err(), "rate-limit must terminate stream");
    }

    #[test]
    fn mutation_auth_loss_terminates_stream() {
        let ids = vec![ObjectId("m1".into())];
        let err = gmail_response(401, &body_with_reason("authError"));
        let result = mutation_error(
            &ids,
            err,
            GmailErrorContext::mutation(AccountOperation::UpdateFlags),
        );
        assert!(result.is_err(), "auth loss must terminate stream");
    }

    #[test]
    fn batch_delete_403_with_forbidden_triggers_fallback() {
        let err = gmail_response(403, &body_with_reason("forbidden"));
        assert!(is_batch_delete_scope_failure(&err));
    }

    #[test]
    fn batch_delete_403_with_insufficient_permissions_triggers_fallback() {
        let err = gmail_response(403, &body_with_reason("insufficientPermissions"));
        assert!(is_batch_delete_scope_failure(&err));
    }

    #[test]
    fn batch_delete_500_does_not_trigger_fallback() {
        let err = gmail_response(500, "x");
        assert!(!is_batch_delete_scope_failure(&err));
    }

    #[test]
    fn blob_range_unsupported_maps_to_unsupported_open_blob_range() {
        let err = Error::Local(GmailLocalError::BlobRangeUnsupported {
            blob_id: "b1".to_owned(),
        });
        let acc = into_account_error(err, GmailErrorContext::open_blob_range());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Unsupported(AccountOperation::OpenBlobRange)
        ));
    }

    #[test]
    fn invalid_blob_id_maps_to_malformed_request() {
        let err = Error::invalid_request(AccountOperation::OpenBlob, "bad json");
        let acc = into_account_error(err, GmailErrorContext::open_blob("b1"));
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
    }

    #[test]
    fn gmail_base64_decode_maps_to_protocol_parse_failed() {
        use base64::Engine;
        let decode_err = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode("!!!not-base64!!!")
            .expect_err("invalid base64");
        let err = Error::base64url(decode_err);
        let acc = into_account_error(err, GmailErrorContext::hydrate_message("m1"));
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
        ));
    }

    #[test]
    fn send_uses_non_idempotent_context() {
        let err = gmail_response(500, "internal");
        let acc = into_account_error(err, GmailErrorContext::send());
        // Server(Unavailable) + non-idempotent + Acknowledged should
        // be `Retry(SameRequest)` (commit-rejection semantics).
        let RecoveryClass::Retry(advice) = acc.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.disposition, RetryDisposition::SameRequest);
        assert_eq!(acc.operation(), Some(AccountOperation::Send));
    }

    #[test]
    fn request_id_header_preserved() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::HeaderName::from_static("x-goog-request-id"),
            reqwest::header::HeaderValue::from_static("abc-123"),
        );
        let resp_headers = crate::error::GmailResponseHeaders::from_headers(&headers);
        let resp = GmailResponseError {
            service: crate::error::GmailService::GmailApi,
            status: 500,
            headers: resp_headers,
            body: bytes::Bytes::from_static(b"x"),
            envelope: None,
        };
        let acc = translate_response(resp, &GmailErrorContext::inventory());
        let view = acc.telemetry_fields();
        assert_eq!(view.request_id, Some("abc-123"));
    }

    #[test]
    fn unknown_gmail_reason_preserves_native_code() {
        let err = gmail_response(418, &body_with_reason("teapot"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        // The builder sets native_code from the Gmail reason; telemetry
        // exposes it verbatim so dashboards can filter on unknown
        // reasons until the gap is closed.
        assert_eq!(acc.telemetry_fields().native_code, Some("teapot"));
    }
}
