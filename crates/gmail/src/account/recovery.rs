use std::time::Duration;

use bifrost_types::{CursorScope, Error as AccountError, Fatal, RecoveryClass};
use reqwest::StatusCode;

use crate::error::Error as GmailError;

const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

pub(crate) fn account_error_from_gmail(error: &GmailError) -> AccountError {
    match error {
        GmailError::Auth { service, body, .. } => AccountError::Auth(format!("{service}: {body}")),
        GmailError::Transport { message, .. } => AccountError::Transport(message.clone()),
        GmailError::QuotaExhausted { service, body, .. } => {
            AccountError::Transport(format!("{service} quota exhausted: {body}"))
        }
        GmailError::HttpStatus {
            service,
            status,
            body,
        } => AccountError::Transport(format!("{service} returned HTTP {status}: {body}")),
        GmailError::Json(err) => AccountError::Other(err.to_string()),
        GmailError::Base64 { source, .. } => AccountError::Other(source.to_string()),
        GmailError::MalformedPayload(message) | GmailError::InvalidInput(message) => {
            AccountError::Other(message.clone())
        }
    }
}

pub(crate) fn fatal_for_error(error: GmailError, recovery: RecoveryClass) -> Fatal {
    let message = error.to_string();
    let source = Some(account_error_from_gmail(&error));
    Fatal {
        recovery,
        message,
        source,
    }
}

pub(crate) fn fatal_for_account_error(error: AccountError, recovery: RecoveryClass) -> Fatal {
    Fatal {
        message: error.to_string(),
        recovery,
        source: Some(error),
    }
}

pub(crate) fn classify_history_error(error: &GmailError) -> RecoveryClass {
    match error {
        GmailError::HttpStatus { status, .. }
            if *status == StatusCode::NOT_FOUND || *status == StatusCode::GONE =>
        {
            RecoveryClass::RestartScope(CursorScope::Account)
        }
        _ => classify_general_error(error),
    }
}

pub(crate) fn classify_general_error(error: &GmailError) -> RecoveryClass {
    match error {
        GmailError::Auth { .. } => RecoveryClass::AuthLost,
        GmailError::QuotaExhausted { .. } => RecoveryClass::Retry {
            after: DEFAULT_RETRY_AFTER,
        },
        GmailError::Transport {
            retryable: true, ..
        } => RecoveryClass::Retry {
            after: DEFAULT_RETRY_AFTER,
        },
        GmailError::HttpStatus { status, .. } if status.is_server_error() => RecoveryClass::Retry {
            after: DEFAULT_RETRY_AFTER,
        },
        _ => RecoveryClass::Fatal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http_error(status: StatusCode) -> GmailError {
        GmailError::HttpStatus {
            service: "Gmail API".to_string(),
            status,
            body: "body".to_string(),
        }
    }

    #[test]
    fn stale_history_404_restarts_account_scope() {
        assert!(matches!(
            classify_history_error(&http_error(StatusCode::NOT_FOUND)),
            RecoveryClass::RestartScope(CursorScope::Account)
        ));
    }

    #[test]
    fn stale_history_410_restarts_account_scope() {
        assert!(matches!(
            classify_history_error(&http_error(StatusCode::GONE)),
            RecoveryClass::RestartScope(CursorScope::Account)
        ));
    }

    #[test]
    fn server_error_is_retryable() {
        assert!(matches!(
            classify_general_error(&http_error(StatusCode::BAD_GATEWAY)),
            RecoveryClass::Retry { .. }
        ));
    }

    fn auth_error() -> GmailError {
        GmailError::Auth {
            service: "Gmail API".to_string(),
            status: Some(StatusCode::UNAUTHORIZED),
            body: "invalid token".to_string(),
            refresh_required: true,
        }
    }

    fn quota_error(status: StatusCode) -> GmailError {
        GmailError::QuotaExhausted {
            service: "Gmail API".to_string(),
            status,
            body: "rateLimitExceeded".to_string(),
        }
    }

    #[test]
    fn unauthorized_is_auth_lost() {
        assert!(matches!(
            classify_general_error(&auth_error()),
            RecoveryClass::AuthLost
        ));
    }

    #[test]
    fn unauthorized_on_history_endpoint_is_auth_lost() {
        // 401 on the history endpoint must not collapse onto RestartScope:
        // it stays AuthLost so the engine re-authenticates rather than
        // re-establishing the cursor.
        assert!(matches!(
            classify_history_error(&auth_error()),
            RecoveryClass::AuthLost
        ));
    }

    #[test]
    fn too_many_requests_is_retry() {
        assert!(matches!(
            classify_general_error(&quota_error(StatusCode::TOO_MANY_REQUESTS)),
            RecoveryClass::Retry { .. }
        ));
    }

    #[test]
    fn forbidden_quota_shape_is_retry() {
        // The error constructor maps 403 with a quota-shaped body into
        // QuotaExhausted before classification ever runs.
        let err = GmailError::status(
            "Gmail API",
            StatusCode::FORBIDDEN,
            "user-rate-limit exceeded".to_string(),
        );
        assert!(matches!(err, GmailError::QuotaExhausted { .. }));
        assert!(matches!(
            classify_general_error(&err),
            RecoveryClass::Retry { .. }
        ));
    }

    #[test]
    fn plain_forbidden_is_fatal() {
        // 403 without a quota-shaped body is not retried.
        let err = GmailError::status(
            "Gmail API",
            StatusCode::FORBIDDEN,
            "permission denied".to_string(),
        );
        assert!(matches!(err, GmailError::HttpStatus { .. }));
        assert!(matches!(classify_general_error(&err), RecoveryClass::Fatal));
    }

    #[test]
    fn bad_request_is_fatal() {
        assert!(matches!(
            classify_general_error(&http_error(StatusCode::BAD_REQUEST)),
            RecoveryClass::Fatal
        ));
    }

    #[test]
    fn service_unavailable_is_retry() {
        assert!(matches!(
            classify_general_error(&http_error(StatusCode::SERVICE_UNAVAILABLE)),
            RecoveryClass::Retry { .. }
        ));
    }

    #[test]
    fn malformed_payload_is_fatal() {
        let err = GmailError::MalformedPayload("bad shape".to_string());
        assert!(matches!(classify_general_error(&err), RecoveryClass::Fatal));
    }

    #[test]
    fn history_endpoint_410_restarts_scope_not_account() {
        // Gmail returns 410 Gone for compacted historyIds. The engine
        // must restart the Account scope, not the whole account.
        let recovery = classify_history_error(&http_error(StatusCode::GONE));
        assert!(matches!(
            recovery,
            RecoveryClass::RestartScope(CursorScope::Account)
        ));
    }

    #[test]
    fn account_error_from_auth_preserves_service() {
        let projected = account_error_from_gmail(&auth_error());
        let AccountError::Auth(message) = projected else {
            panic!("expected AccountError::Auth");
        };
        assert!(message.contains("Gmail API"));
    }

    #[test]
    fn account_error_from_quota_projects_to_transport() {
        let projected = account_error_from_gmail(&quota_error(StatusCode::TOO_MANY_REQUESTS));
        assert!(matches!(projected, AccountError::Transport(_)));
    }

    #[test]
    fn account_error_from_http_status_projects_to_transport() {
        let projected = account_error_from_gmail(&http_error(StatusCode::BAD_GATEWAY));
        assert!(matches!(projected, AccountError::Transport(_)));
    }

    #[test]
    fn account_error_from_malformed_projects_to_other() {
        let projected = account_error_from_gmail(&GmailError::MalformedPayload("x".to_string()));
        assert!(matches!(projected, AccountError::Other(_)));
    }

    #[test]
    fn fatal_for_error_preserves_recovery_and_source() {
        let fatal = fatal_for_error(http_error(StatusCode::BAD_GATEWAY), RecoveryClass::Fatal);
        assert!(matches!(fatal.recovery, RecoveryClass::Fatal));
        assert!(matches!(fatal.source, Some(AccountError::Transport(_))));
    }

    #[test]
    fn fatal_for_account_error_carries_source() {
        let fatal = fatal_for_account_error(
            AccountError::SchemaIncompatible,
            RecoveryClass::SchemaIncompatible,
        );
        assert!(matches!(fatal.recovery, RecoveryClass::SchemaIncompatible));
        assert!(matches!(
            fatal.source,
            Some(AccountError::SchemaIncompatible)
        ));
    }
}
