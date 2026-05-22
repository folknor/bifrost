use std::time::Duration;

use bifrost_types::{Error, Fatal, RecoveryClass, SyncEvent};

use crate::core::error::{JMAPError, MethodErrorType, ProblemType};

pub(crate) fn fatal_unsupported<T>(message: impl Into<String>) -> SyncEvent<T> {
    fatal_from_account_error(Error::Unsupported, None, message)
}

pub(crate) fn fatal_from_account_error<T>(
    err: Error,
    scope: Option<bifrost_types::CursorScope>,
    message: impl Into<String>,
) -> SyncEvent<T> {
    let recovery = match &err {
        Error::CursorProtocolMismatch | Error::CursorEnvelopeUnknown => {
            RecoveryClass::SchemaIncompatible
        }
        Error::SchemaIncompatible => RecoveryClass::SchemaIncompatible,
        Error::Unsupported => RecoveryClass::Fatal,
        Error::RangeNotSupported | Error::BlobNotByteStream | Error::RangeOutOfBounds { .. } => {
            RecoveryClass::Fatal
        }
        Error::ConcurrencyConflict => RecoveryClass::Retry {
            after: Duration::ZERO,
        },
        Error::Auth(_) => RecoveryClass::AuthLost,
        Error::Transport(_) => RecoveryClass::Retry {
            after: Duration::from_secs(5),
        },
        Error::MissingCoreCapability => RecoveryClass::CapabilityChanged {
            delta: bifrost_types::CapabilityDelta::default(),
        },
        Error::IdleBusy | Error::Other(_) => scope
            .map(RecoveryClass::RestartScope)
            .unwrap_or(RecoveryClass::Fatal),
        _ => RecoveryClass::Fatal,
    };

    SyncEvent::Fatal(Fatal {
        recovery,
        message: message.into(),
        source: Some(err),
    })
}

pub(crate) fn fatal_from_jmap<T>(
    err: crate::Error,
    scope: Option<bifrost_types::CursorScope>,
) -> SyncEvent<T> {
    let recovery = to_recovery(&err, scope);
    let message = err.to_string();
    SyncEvent::Fatal(Fatal {
        recovery,
        message,
        source: Some(to_account_error(err)),
    })
}

pub(crate) fn to_recovery(
    err: &crate::Error,
    scope: Option<bifrost_types::CursorScope>,
) -> RecoveryClass {
    match err {
        crate::Error::Method(method) => match method.error_type() {
            MethodErrorType::CannotCalculateChanges => scope
                .map(RecoveryClass::RestartScope)
                .unwrap_or(RecoveryClass::RestartAccount),
            MethodErrorType::StateMismatch => RecoveryClass::Retry {
                after: Duration::ZERO,
            },
            MethodErrorType::AccountNotFound
            | MethodErrorType::FromAccountNotFound
            | MethodErrorType::AccountNotSupportedByMethod
            | MethodErrorType::FromAccountNotSupportedByMethod
            | MethodErrorType::AccountReadOnly => RecoveryClass::RestartAccount,
            MethodErrorType::ServerUnavailable => RecoveryClass::Retry {
                after: Duration::from_secs(5),
            },
            MethodErrorType::ServerFail | MethodErrorType::ServerPartialFail => {
                RecoveryClass::Retry {
                    after: Duration::from_secs(30),
                }
            }
            MethodErrorType::RequestTooLarge | MethodErrorType::TooManyChanges => {
                RecoveryClass::Retry {
                    after: Duration::from_secs(1),
                }
            }
            MethodErrorType::Forbidden => RecoveryClass::AuthLost,
            MethodErrorType::InvalidArguments
            | MethodErrorType::InvalidResultReference
            | MethodErrorType::UnknownMethod
            | MethodErrorType::UnsupportedSort
            | MethodErrorType::UnsupportedFilter
            | MethodErrorType::AnchorNotFound
            | MethodErrorType::AlreadyExists
            | MethodErrorType::Other => RecoveryClass::Fatal,
        },
        crate::Error::Problem(problem) => match problem.error() {
            ProblemType::JMAP(JMAPError::Limit) => RecoveryClass::Retry {
                after: Duration::from_secs(30),
            },
            ProblemType::JMAP(JMAPError::UnknownCapability) => RecoveryClass::CapabilityChanged {
                delta: bifrost_types::CapabilityDelta::default(),
            },
            ProblemType::JMAP(JMAPError::NotJSON | JMAPError::NotRequest) => RecoveryClass::Fatal,
            ProblemType::Other(_) => match problem.status() {
                Some(401 | 403) => RecoveryClass::AuthLost,
                Some(429) => RecoveryClass::Retry {
                    after: Duration::from_secs(30),
                },
                Some(500..=599) => RecoveryClass::Retry {
                    after: Duration::from_secs(30),
                },
                _ => RecoveryClass::Fatal,
            },
        },
        crate::Error::Transport(_) => RecoveryClass::Retry {
            after: Duration::from_secs(5),
        },
        crate::Error::NoPrimaryAccount { .. } => RecoveryClass::RestartAccount,
        crate::Error::WebSocket(_) | crate::Error::WebSocketNotConnected => RecoveryClass::Retry {
            after: Duration::from_secs(5),
        },
        crate::Error::Parse(_)
        | crate::Error::Set(_)
        | crate::Error::CallNotFound(_)
        | crate::Error::IdNotFound(_)
        | crate::Error::EmptyResponse
        | crate::Error::NotParsable(_)
        | crate::Error::InvalidUrl(_) => RecoveryClass::Fatal,
    }
}

pub(crate) fn to_account_error(err: crate::Error) -> Error {
    match err {
        crate::Error::Method(method)
            if matches!(method.error_type(), MethodErrorType::StateMismatch) =>
        {
            Error::ConcurrencyConflict
        }
        crate::Error::Problem(problem) if matches!(problem.status(), Some(401 | 403)) => {
            Error::Auth(problem.to_string())
        }
        crate::Error::NoPrimaryAccount { capability } => {
            Error::Auth(format!("no primary account for capability {capability}"))
        }
        other => Error::Transport(other.to_string()),
    }
}

pub(crate) fn is_state_mismatch(err: &crate::Error) -> bool {
    matches!(
        err,
        crate::Error::Method(method)
            if matches!(method.error_type(), MethodErrorType::StateMismatch)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::error::{MethodError, ProblemDetails};

    fn method_error(kind: &str) -> crate::Error {
        let err: MethodError = serde_json::from_str(&format!(r#"{{"type":"{kind}"}}"#)).unwrap();
        crate::Error::Method(err)
    }

    #[test]
    fn cannot_calculate_changes_restarts_scope() {
        let scope = bifrost_types::CursorScope::Type(bifrost_types::ObjectType::Email);
        let recovery = to_recovery(&method_error("cannotCalculateChanges"), Some(scope.clone()));

        assert!(matches!(
            recovery,
            RecoveryClass::RestartScope(found) if found == scope
        ));
    }

    #[test]
    fn state_mismatch_is_retry_and_account_error_is_concurrency() {
        let err = method_error("stateMismatch");
        assert!(matches!(
            to_recovery(&err, None),
            RecoveryClass::Retry { after } if after == Duration::ZERO
        ));
        assert!(matches!(to_account_error(err), Error::ConcurrencyConflict));
    }

    #[test]
    fn jmap_limit_problem_is_retry() {
        let problem = ProblemDetails::new(
            ProblemType::JMAP(JMAPError::Limit),
            Some(429),
            None,
            None,
            None,
            None,
        );
        let recovery = to_recovery(&crate::Error::Problem(Box::new(problem)), None);

        assert!(matches!(
            recovery,
            RecoveryClass::Retry { after } if after == Duration::from_secs(30)
        ));
    }
}
