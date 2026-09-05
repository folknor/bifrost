//! The shared DAV status-to-`AccountError` ladder and error constructors.
//!
//! The two protocol crates carried this ladder twice. The copies differed in
//! exactly three tokens - the `ResourceKind` a 404 and a 403 name, the
//! `Protocol` stamp, and the `field` label on a local argument error - so those
//! are the parameter, and everything else is one implementation.

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, AttemptCause, Cause,
    DiagnosticText, Protocol, ProtocolErrorKind, RecoveryClass, RequestCause, RequestErrorKind,
    ResourceKind, ServerCause, ServerErrorKind, StateCause, TransmissionState, TransportCause,
    TransportErrorKind, TransportKind, WireCause,
};
use reqwest::StatusCode;

/// Which DAV dialect an error is being minted for.
///
/// Carries the three values that distinguish the two otherwise identical error
/// surfaces, so a caller cannot mix a CalDAV `Protocol` stamp with a CardDAV
/// `ResourceKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DavProtocol {
    CalDav,
    CardDav,
}

impl DavProtocol {
    #[must_use]
    pub fn protocol(self) -> Protocol {
        match self {
            Self::CalDav => Protocol::CalDav,
            Self::CardDav => Protocol::CardDav,
        }
    }

    /// The resource a bare status refers to when the server names none.
    #[must_use]
    pub fn resource(self) -> ResourceKind {
        match self {
            Self::CalDav => ResourceKind::Calendar,
            Self::CardDav => ResourceKind::Contact,
        }
    }

    /// The protocol's name as it appears in support-only diagnostics.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::CalDav => "CalDAV",
            Self::CardDav => "CardDAV",
        }
    }

    /// The `field` label on a locally-refused request.
    #[must_use]
    pub fn field(self) -> &'static str {
        match self {
            Self::CalDav => "caldav",
            Self::CardDav => "carddav",
        }
    }
}

#[must_use]
pub fn unsupported_error(operation: AccountOperation, protocol: DavProtocol) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(operation),
        Cause::Request(RequestCause::Unsupported { operation }),
    )
    .protocol(protocol.protocol())
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

#[must_use]
pub fn not_found_error(
    operation: AccountOperation,
    id: impl Into<String>,
    protocol: DavProtocol,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::NotFound(protocol.resource()),
        Cause::Request(RequestCause::NotFound {
            what: protocol.resource(),
            id: Some(id.into()),
        }),
    )
    .protocol(protocol.protocol())
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

#[must_use]
pub fn local_error(
    operation: AccountOperation,
    message: impl Into<String>,
    protocol: DavProtocol,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::InvalidArgument {
            field: Some(protocol.field()),
            message: Some(DiagnosticText::support_only(message)),
        }),
    )
    .protocol(protocol.protocol())
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

#[must_use]
pub fn parse_error(
    operation: AccountOperation,
    message: impl Into<String>,
    protocol: DavProtocol,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: protocol.protocol(),
            detail: Some(DiagnosticText::support_only(message)),
        }),
    )
    .protocol(protocol.protocol())
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

#[must_use]
pub fn transport_error(
    operation: AccountOperation,
    message: impl Into<String>,
    protocol: DavProtocol,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(TransportCause::new(
            TransportKind::Network,
            Some(DiagnosticText::support_only(message)),
        )),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::InFlight,
    )))
    .protocol(protocol.protocol())
    .operation(operation)
    .try_build()
    .expect("valid account error classification")
}

/// A response body that exceeded the buffered ceiling.
///
/// Classified `Protocol(PartialResponse)` with an ACKNOWLEDGED attempt: the
/// request reached the server and may have taken effect, so a non-idempotent
/// mutation must reconcile rather than replay blindly.
#[must_use]
pub fn response_read_error(
    operation: AccountOperation,
    message: String,
    protocol: DavProtocol,
) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: protocol.protocol(),
            detail: Some(DiagnosticText::support_only(message)),
        }),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )))
    .protocol(protocol.protocol())
    .operation(operation)
    .try_build()
    .expect("valid acknowledged response-overflow classification")
}

#[must_use]
pub fn status_error(
    operation: AccountOperation,
    status: StatusCode,
    body: String,
    protocol: DavProtocol,
) -> AccountError {
    let resource = protocol.resource();
    let kind = if status == StatusCode::UNAUTHORIZED {
        AccountErrorKind::Authentication(bifrost_types::AuthErrorKind::ReauthorizationRequired)
    } else if status == StatusCode::FORBIDDEN {
        AccountErrorKind::Authorization(bifrost_types::AccessErrorKind::PermissionDenied)
    } else if status == StatusCode::NOT_FOUND {
        AccountErrorKind::NotFound(resource)
    } else if status == StatusCode::CONFLICT
        || status == StatusCode::PRECONDITION_FAILED
        || status == StatusCode::LOCKED
    {
        AccountErrorKind::ConcurrencyConflict
    } else if status == StatusCode::TOO_MANY_REQUESTS {
        AccountErrorKind::Server(ServerErrorKind::RateLimited)
    } else if status == StatusCode::SERVICE_UNAVAILABLE {
        AccountErrorKind::Server(ServerErrorKind::Unavailable)
    } else if status == StatusCode::INSUFFICIENT_STORAGE {
        AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
    } else {
        AccountErrorKind::Server(ServerErrorKind::Error {
            status: Some(status.as_u16()),
        })
    };
    let cause = if status == StatusCode::UNAUTHORIZED {
        Cause::Auth(bifrost_types::AuthCause::ReauthorizationRequired)
    } else if status == StatusCode::FORBIDDEN {
        Cause::Access(bifrost_types::AccessCause::PermissionDenied {
            resource: Some(resource),
        })
    } else if status == StatusCode::NOT_FOUND {
        Cause::Request(RequestCause::NotFound {
            what: resource,
            id: None,
        })
    } else if status == StatusCode::CONFLICT
        || status == StatusCode::PRECONDITION_FAILED
        || status == StatusCode::LOCKED
    {
        Cause::State(StateCause::ConcurrencyConflict)
    } else if status == StatusCode::TOO_MANY_REQUESTS {
        Cause::Server(ServerCause::RateLimited { retry_hint: None })
    } else if status == StatusCode::SERVICE_UNAVAILABLE {
        Cause::Server(ServerCause::Unavailable { retry_hint: None })
    } else if status == StatusCode::INSUFFICIENT_STORAGE {
        Cause::Server(ServerCause::QuotaExhausted { retry_hint: None })
    } else {
        Cause::Server(ServerCause::Error {
            status: Some(status.as_u16()),
        })
    };

    let mut builder = AccountErrorBuilder::new(kind, cause)
        .protocol(protocol.protocol())
        .operation(operation)
        .status(Some(status.as_u16()));
    let body = body.trim();
    if !body.is_empty() {
        builder = builder.text(DiagnosticText::support_only(body.to_string()));
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

/// Keep whichever of two errors a consumer most needs to act on.
///
/// A multi-leg REPORT can fail several ways at once; the surviving error is the
/// one whose recovery class demands the most, so a 401 in one leg is never
/// buried under a retryable 503 in another.
#[must_use]
pub fn worse_recovery(
    current: Option<AccountError>,
    candidate: AccountError,
) -> Option<AccountError> {
    match current {
        Some(existing)
            if recovery_rank(existing.recovery()) >= recovery_rank(candidate.recovery()) =>
        {
            Some(existing)
        }
        _ => Some(candidate),
    }
}

#[must_use]
pub fn recovery_rank(class: &RecoveryClass) -> u8 {
    match class {
        RecoveryClass::AuthLost => 4,
        RecoveryClass::NeedsAdminConsent { .. }
        | RecoveryClass::NeedsPolicyChange
        | RecoveryClass::NoPermission { .. } => 3,
        RecoveryClass::Retry(_) => 0,
        RecoveryClass::Reconcile(_) | RecoveryClass::Engine(_) => 1,
        RecoveryClass::Unsupported(_)
        | RecoveryClass::ClientBug
        | RecoveryClass::ProviderContractViolation
        | RecoveryClass::ProviderRefused
        | RecoveryClass::UnknownPermanent => 2,
        // RecoveryClass is non-exhaustive. An unknown future class must win
        // rather than being silently ranked below a known terminal failure.
        _ => u8::MAX,
    }
}

/// The DAV precondition elements a server names when it refuses to RUN a
/// report's filter, as opposed to refusing the caller.
///
/// Matched case-insensitively against the response body, which is the only
/// place the precondition appears: RFC 4918 s16 carries it inside a
/// `DAV:error` document, and the status alone (403) cannot tell "I do not
/// implement that filter" apart from "you may not read this collection".
const FILTER_PRECONDITIONS: [&str; 4] = [
    "supported-filter",
    "supported-collation",
    "valid-filter",
    "supported-report",
];

/// Does this REPORT rejection mean "I will not run that filter" rather than
/// "no"?
///
/// The filtered listing lanes push their predicate to the server so a page can
/// be sliced before anything is hydrated. A server that will not run the filter
/// must not fail the call: the lane degrades to listing the collection and
/// matching locally, which is what both crates did unconditionally before.
/// Only three answers mean that, and each is a statement about the REPORT
/// rather than about the credential:
///
/// - `400`, the catch-all for a body the server would not process. Servers that
///   implement no query filter at all answer this, with no precondition
///   element to inspect.
/// - `501`, an explicit "not implemented".
/// - `403` naming one of [`FILTER_PRECONDITIONS`]. A bare 403 is deliberately
///   NOT degraded: it is far more often a permission refusal, and swallowing it
///   into a whole-collection walk would replace a classified `NoPermission`
///   with whatever the listing happens to answer.
///
/// `401` is never here. A stale credential must reach the consumer as a
/// reauthorize signal, not as a quietly narrower search.
#[must_use]
pub fn filter_unsupported(status: StatusCode, body: &str) -> bool {
    if status == StatusCode::BAD_REQUEST || status == StatusCode::NOT_IMPLEMENTED {
        return true;
    }
    if status != StatusCode::FORBIDDEN {
        return false;
    }
    let body = body.to_ascii_lowercase();
    FILTER_PRECONDITIONS
        .iter()
        .any(|precondition| body.contains(precondition))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refused_filter_is_told_apart_from_a_refused_caller() {
        assert!(filter_unsupported(StatusCode::BAD_REQUEST, ""));
        assert!(filter_unsupported(StatusCode::NOT_IMPLEMENTED, ""));
        assert!(filter_unsupported(
            StatusCode::FORBIDDEN,
            "<D:error xmlns:D=\"DAV:\"><C:supported-filter/></D:error>"
        ));
        assert!(filter_unsupported(
            StatusCode::FORBIDDEN,
            "<D:error><C:SUPPORTED-COLLATION/></D:error>"
        ));
        // A bare permission refusal keeps its classification.
        assert!(!filter_unsupported(StatusCode::FORBIDDEN, "go away"));
        // A stale credential must stay a reauthorize signal.
        assert!(!filter_unsupported(StatusCode::UNAUTHORIZED, ""));
        assert!(!filter_unsupported(
            StatusCode::SERVICE_UNAVAILABLE,
            "supported-filter"
        ));
    }

    /// The two dialects differ in exactly the three values `DavProtocol`
    /// carries, and in nothing else.
    #[test]
    fn the_dialects_differ_only_in_resource_protocol_and_field() {
        assert_eq!(DavProtocol::CalDav.protocol(), Protocol::CalDav);
        assert_eq!(DavProtocol::CardDav.protocol(), Protocol::CardDav);
        assert_eq!(DavProtocol::CalDav.resource(), ResourceKind::Calendar);
        assert_eq!(DavProtocol::CardDav.resource(), ResourceKind::Contact);
        assert_eq!(DavProtocol::CalDav.field(), "caldav");
        assert_eq!(DavProtocol::CardDav.field(), "carddav");
    }

    /// Migrated from both crates, which each carried it for one dialect. The
    /// loop is the point: the ladder must answer identically for both.
    #[test]
    fn status_error_maps_write_conflicts() {
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            for status in [
                StatusCode::CONFLICT,
                StatusCode::PRECONDITION_FAILED,
                StatusCode::LOCKED,
            ] {
                let error = status_error(
                    AccountOperation::EventUpdate,
                    status,
                    String::new(),
                    protocol,
                );
                assert!(
                    matches!(error.kind(), AccountErrorKind::ConcurrencyConflict),
                    "{protocol:?} {status} must be a write conflict"
                );
            }
        }
    }

    /// Migrated from `bifrost-caldav`, which was the only crate still pinning
    /// the transient and quota statuses after the merge.
    #[test]
    fn status_error_maps_transient_and_quota_statuses() {
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            let kind = |status| {
                status_error(
                    AccountOperation::EventUpdate,
                    status,
                    String::new(),
                    protocol,
                )
            };
            assert!(matches!(
                kind(StatusCode::TOO_MANY_REQUESTS).kind(),
                AccountErrorKind::Server(ServerErrorKind::RateLimited)
            ));
            assert!(matches!(
                kind(StatusCode::SERVICE_UNAVAILABLE).kind(),
                AccountErrorKind::Server(ServerErrorKind::Unavailable)
            ));
            assert!(matches!(
                kind(StatusCode::INSUFFICIENT_STORAGE).kind(),
                AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
            ));
        }
    }

    #[test]
    fn status_error_names_the_dialects_own_resource() {
        let calendar = status_error(
            AccountOperation::EventGet,
            StatusCode::NOT_FOUND,
            String::new(),
            DavProtocol::CalDav,
        );
        assert!(matches!(
            calendar.kind(),
            AccountErrorKind::NotFound(ResourceKind::Calendar)
        ));
        let contact = status_error(
            AccountOperation::ContactGet,
            StatusCode::NOT_FOUND,
            String::new(),
            DavProtocol::CardDav,
        );
        assert!(matches!(
            contact.kind(),
            AccountErrorKind::NotFound(ResourceKind::Contact)
        ));
    }

    /// Migrated from `bifrost-caldav` with the ranking it pins. It was the
    /// stronger of the two copies - it enumerates every nameable variant rather
    /// than sampling three - so it is the one that survived the merge.
    ///
    /// The `_ =>` arm cannot be pinned hermetically: `RecoveryClass` is
    /// `#[non_exhaustive]` and lives in `bifrost-types`, so no test outside it
    /// can name a variant this `match` does not already list. What is pinnable
    /// is that every variant we CAN name ranks strictly below the sentinel,
    /// which is what makes an unknown one win by construction.
    #[test]
    fn recovery_ranks_order_from_retryable_up_to_auth_lost() {
        use bifrost_types::{EngineDirective, RetryAdvice, RetryDisposition, RetryReason};

        let retry = RecoveryClass::Retry(RetryAdvice::new(
            RetryDisposition::SameRequest,
            None,
            RetryReason::Transport,
            None,
        ));
        let engine = RecoveryClass::Engine(EngineDirective::RestartAccount);
        let terminal = [
            RecoveryClass::Unsupported(AccountOperation::EventSearch),
            RecoveryClass::ClientBug,
            RecoveryClass::ProviderContractViolation,
            RecoveryClass::ProviderRefused,
            RecoveryClass::UnknownPermanent,
        ];
        let consent = [
            RecoveryClass::NeedsAdminConsent { needed: "scope" },
            RecoveryClass::NeedsPolicyChange,
            RecoveryClass::NoPermission { resource: None },
        ];

        assert!(recovery_rank(&retry) < recovery_rank(&engine));
        for class in &terminal {
            assert!(
                recovery_rank(&engine) < recovery_rank(class),
                "{class:?} must outrank an engine directive"
            );
            for stronger in &consent {
                assert!(
                    recovery_rank(class) < recovery_rank(stronger),
                    "{stronger:?} must outrank {class:?}"
                );
            }
        }
        for class in &consent {
            assert!(
                recovery_rank(class) < recovery_rank(&RecoveryClass::AuthLost),
                "AuthLost must outrank {class:?}"
            );
            // Every named variant sits below the catch-all sentinel, so an
            // unknown future class escalates rather than being buried.
            assert!(recovery_rank(class) < u8::MAX);
        }
        assert!(recovery_rank(&RecoveryClass::AuthLost) < u8::MAX);
    }

    /// A retryable failure in one leg must never bury a reauthorization in
    /// another, whichever order the legs complete in. Migrated from the two
    /// crates' `the_worst_recovery_class_wins_whatever_the_chunk_order`, and
    /// now run against BOTH dialects rather than one.
    #[test]
    fn the_worst_recovery_class_wins_whatever_the_leg_order() {
        for protocol in [DavProtocol::CalDav, DavProtocol::CardDav] {
            let error = |status| {
                status_error(
                    AccountOperation::EventSearch,
                    status,
                    "refused".to_string(),
                    protocol,
                )
            };
            let auth_first = worse_recovery(
                Some(error(StatusCode::UNAUTHORIZED)),
                error(StatusCode::SERVICE_UNAVAILABLE),
            );
            let transient_first = worse_recovery(
                Some(error(StatusCode::SERVICE_UNAVAILABLE)),
                error(StatusCode::UNAUTHORIZED),
            );

            for surviving in [auth_first, transient_first] {
                assert_eq!(
                    surviving.expect("kept").recovery(),
                    &RecoveryClass::AuthLost,
                    "{protocol:?} must keep the reauthorization signal"
                );
            }
        }
    }
}
