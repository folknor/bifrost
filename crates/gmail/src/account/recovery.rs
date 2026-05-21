use std::time::Duration;

use bifrost_types::{CursorScope, Error as AccountError, Fatal, RecoveryClass};
use reqwest::StatusCode;

use crate::error::Error as GmailError;

const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

pub(crate) fn account_error_from_gmail(error: &GmailError) -> AccountError {
    match error {
        GmailError::Auth { service, body, .. } => AccountError::Auth(format!("{service}: {body}")),
        GmailError::Transport(err) => AccountError::Transport(err.to_string()),
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
        GmailError::Transport(err) if err.is_timeout() || err.is_connect() => {
            RecoveryClass::Retry {
                after: DEFAULT_RETRY_AFTER,
            }
        }
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
}
