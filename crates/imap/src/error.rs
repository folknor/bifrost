//! Error types for IMAP operations.
//!
//! Distinguishes protocol errors, I/O errors, auth failures, parse errors, and timeouts.
//! Server status responses (OK, NO, BAD, BYE) are defined in RFC 3501 Section 7.1
//! and RFC 9051 Section 7.1.

use std::sync::Arc;

use crate::types::{AuthMechanism, ResponseCode};

/// Error type for IMAP client operations.
// protocol-specific: direct IMAP APIs preserve response codes beyond bifrost_types::Error.
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// Underlying I/O error, including TLS transport errors (RFC 3501 Section 2.1).
    ///
    /// Wrapped in [`Arc`] so that `Error` can implement `Clone`.
    #[error("I/O error: {0}")]
    Io(#[source] Arc<std::io::Error>),

    /// Authentication was rejected by the server (RFC 3501 Section 6.2.2).
    ///
    /// The optional [`ResponseCode`] carries the structured reason code
    /// (e.g., `[AUTHENTICATIONFAILED]`, `[EXPIRED]`, `[PRIVACYREQUIRED]`)
    /// when the server provides one (RFC 5530 Section 3).
    #[error("authentication failed: {text}")]
    Auth {
        /// Human-readable response text.
        text: String,
        /// Structured response code, if present (RFC 5530 Section 3).
        code: Option<ResponseCode>,
    },

    /// Server returned a NO response to a command (RFC 3501 Section 7.1.2).
    ///
    /// The optional [`ResponseCode`] carries the structured reason code
    /// (e.g., `[NOPERM]`, `[OVERQUOTA]`) when the server provides one
    /// (RFC 5530 Section 3).
    #[error("server rejected command: {text}")]
    No {
        /// Human-readable response text.
        text: String,
        /// Structured response code, if present (RFC 5530 Section 3).
        code: Option<ResponseCode>,
    },

    /// Server returned a BAD response  -  client sent something invalid (RFC 3501 Section 7.1.3).
    ///
    /// The optional [`ResponseCode`] carries the structured reason code
    /// when the server provides one (RFC 5530 Section 3).
    #[error("server reported bad command: {text}")]
    Bad {
        /// Human-readable response text.
        text: String,
        /// Structured response code, if present (RFC 5530 Section 3).
        code: Option<ResponseCode>,
    },

    /// Server sent BYE  -  closing connection (RFC 3501 Section 7.1.5).
    ///
    /// BYE responses can include response codes such as `[ALERT]` or
    /// `[UNAVAILABLE]` that carry actionable information for the client
    /// (RFC 3501 Section 7.1.5, RFC 5530 Section 3).
    /// The `[ALERT]` code in particular MUST be presented to the user
    /// (RFC 3501 Section 7.1).
    #[error("server closing connection: {text}")]
    Bye {
        /// Human-readable response text.
        text: String,
        /// Structured response code, if present (RFC 5530 Section 3).
        code: Option<ResponseCode>,
    },

    /// IMAP protocol violation by the server (RFC 3501 Section 7 / RFC 9051 Section 7).
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Failed to parse a server response (RFC 3501 Section 7 / RFC 9051 Section 7).
    #[error("parse error: {0}")]
    Parse(String),

    /// Operation exceeded the caller-supplied timeout.
    ///
    /// This is a client-imposed constraint, not a protocol-level error.
    /// See RFC 3501 Section 5.4 for the server-side autologout timer;
    /// client-side timeouts guard against indefinite blocking on I/O.
    #[error("operation timed out")]
    Timeout,

    /// The TCP connection has been closed (RFC 3501 Section 2.1).
    #[error("connection closed")]
    Closed,

    /// STARTTLS was requested but the server does not advertise it
    /// (RFC 3501 Section 6.2.1, RFC 9051 Section 6.2.1).
    #[error("STARTTLS not supported by server")]
    StartTlsUnavailable,

    /// Authentication policy rejected every mechanism the server offered.
    #[error("authentication policy rejected authentication: {0}")]
    AuthPolicy(AuthPolicyFailure),

    /// A capability required for the requested operation is not advertised
    /// (RFC 3501 Section 6.1.1).
    #[error("missing required capability: {0}")]
    MissingCapability(String),

    /// Message exceeds the server's advertised APPENDLIMIT (RFC 7889 Section 3).
    #[error("message size {size} exceeds server APPENDLIMIT of {limit}")]
    AppendLimit {
        /// Size of the message the caller tried to append (RFC 7889 Section 3).
        size: u64,
        /// Server-advertised maximum in octets (RFC 7889 Section 5).
        limit: u64,
    },

    /// A buffered FETCH exceeded the caller's configured memory budget.
    ///
    /// The driver drains the command to tagged completion before returning
    /// this error so the IMAP stream remains usable. `seq` and `uid` identify
    /// the response that first crossed the budget when the server supplied
    /// those values.
    #[error("estimated FETCH response size {estimated} exceeds caller limit of {limit}")]
    FetchLimit {
        /// Estimated bytes observed while parsing the FETCH responses.
        estimated: usize,
        /// Caller-supplied maximum estimated bytes.
        limit: usize,
        /// Message sequence number of the response that crossed the limit.
        seq: u32,
        /// UID of the response that crossed the limit, if present.
        uid: Option<u32>,
    },

    /// The date-time string supplied to APPEND does not conform to the
    /// `date-time` production in RFC 3501 Section 9.
    ///
    /// ```text
    /// date-time      = DQUOTE date-day-fixed "-" date-month "-" date-year
    ///                  SP time SP zone DQUOTE
    /// date-day-fixed = (SP DIGIT) / 2DIGIT
    /// date-month     = "Jan" / "Feb" / ... / "Dec"
    /// time           = 2DIGIT ":" 2DIGIT ":" 2DIGIT
    /// zone           = ("+" / "-") 4DIGIT
    /// ```
    #[error("invalid APPEND date-time: {0}")]
    InvalidAppendDate(String),

    /// Internal driver error  -  the driver task stub has not been replaced
    /// by its full implementation yet, or an invariant was violated that
    /// indicates a bug in the library.
    #[error("internal error: {0}")]
    Internal(String),

    /// The driver task panicked. The payload is the panic message
    /// extracted from the `JoinError` (best-effort  -  non-string panics
    /// produce a generic description).
    #[error("driver task panicked: {0}")]
    DriverPanicked(String),

    /// The driver task exited (cleanly or via cancellation) and the
    /// command channel is closed, but no panic was observed.
    #[error("driver task gone")]
    DriverGone,
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(Arc::new(e))
    }
}

impl From<crate::types::ValidationError> for Error {
    fn from(e: crate::types::ValidationError) -> Self {
        Self::Protocol(e.to_string())
    }
}

impl From<crate::codec::encode::EncodeError> for Error {
    fn from(e: crate::codec::encode::EncodeError) -> Self {
        match e {
            crate::codec::encode::EncodeError::MissingCapability { cmd, cap } => {
                Self::MissingCapability(format!("{cmd} requires {cap}"))
            }
            crate::codec::encode::EncodeError::Validation(msg) => Self::Protocol(msg),
        }
    }
}

/// Compares two IMAP errors for equality.
///
/// The [`Io`](Error::Io) variant compares by [`std::io::ErrorKind`] only, since
/// `std::io::Error` does not implement `PartialEq`. Two `Io` errors with the
/// same `ErrorKind` are considered equal even if their messages differ.
impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Io(a), Self::Io(b)) => a.kind() == b.kind(),
            (Self::Auth { text: t1, code: c1 }, Self::Auth { text: t2, code: c2 })
            | (Self::No { text: t1, code: c1 }, Self::No { text: t2, code: c2 })
            | (Self::Bad { text: t1, code: c1 }, Self::Bad { text: t2, code: c2 })
            | (Self::Bye { text: t1, code: c1 }, Self::Bye { text: t2, code: c2 }) => {
                t1 == t2 && c1 == c2
            }
            (Self::Protocol(a), Self::Protocol(b))
            | (Self::Parse(a), Self::Parse(b))
            | (Self::MissingCapability(a), Self::MissingCapability(b))
            | (Self::InvalidAppendDate(a), Self::InvalidAppendDate(b))
            | (Self::Internal(a), Self::Internal(b))
            | (Self::DriverPanicked(a), Self::DriverPanicked(b)) => a == b,
            (Self::AuthPolicy(a), Self::AuthPolicy(b)) => a == b,
            (Self::Timeout, Self::Timeout)
            | (Self::Closed, Self::Closed)
            | (Self::StartTlsUnavailable, Self::StartTlsUnavailable)
            | (Self::DriverGone, Self::DriverGone) => true,
            (
                Self::AppendLimit {
                    size: s1,
                    limit: l1,
                },
                Self::AppendLimit {
                    size: s2,
                    limit: l2,
                },
            ) => s1 == s2 && l1 == l2,
            (
                Self::FetchLimit {
                    estimated: e1,
                    limit: l1,
                    seq: s1,
                    uid: u1,
                },
                Self::FetchLimit {
                    estimated: e2,
                    limit: l2,
                    seq: s2,
                    uid: u2,
                },
            ) => e1 == e2 && l1 == l2 && s1 == s2 && u1 == u2,
            _ => false,
        }
    }
}

impl Eq for Error {}

impl Error {
    /// Construct an [`Error::No`] with an optional response code (RFC 5530 Section 3).
    pub(crate) fn no_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::No { text, code }
    }

    /// Construct an [`Error::Bad`] with an optional response code (RFC 5530 Section 3).
    pub(crate) fn bad_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::Bad { text, code }
    }

    /// Construct an [`Error::Auth`] with an optional response code (RFC 5530 Section 3).
    pub(crate) fn auth_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::Auth { text, code }
    }

    /// Construct an [`Error::Bye`] with an optional response code
    /// (RFC 3501 Section 7.1.5, RFC 5530 Section 3).
    pub(crate) fn bye_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::Bye { text, code }
    }

    /// Return the broad policy category for this error.
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::Io(_) => ErrorCategory::Transport,
            Self::Closed | Self::DriverGone | Self::DriverPanicked(_) => ErrorCategory::Connection,
            Self::Auth { .. } | Self::AuthPolicy(_) => ErrorCategory::Authentication,
            Self::No { code, .. } | Self::Bad { code, .. } => {
                ErrorCategory::from_response_code(code.as_ref())
            }
            Self::Bye { code: None, .. } => ErrorCategory::Connection,
            Self::Bye {
                code: Some(code), ..
            } => ErrorCategory::from_response_code(Some(code)),
            Self::Protocol(_) => ErrorCategory::Protocol,
            Self::Parse(_) => ErrorCategory::Parse,
            Self::Timeout => ErrorCategory::Timeout,
            Self::StartTlsUnavailable => ErrorCategory::SecurityPolicy,
            Self::MissingCapability(_) => ErrorCategory::Capability,
            Self::AppendLimit { .. } | Self::FetchLimit { .. } => ErrorCategory::Limit,
            Self::InvalidAppendDate(_) => ErrorCategory::InvalidInput,
            Self::Internal(_) => ErrorCategory::Internal,
        }
    }

    /// Suggested high-level recovery action.
    pub fn recovery(&self) -> Recovery {
        match self.category() {
            ErrorCategory::Connection => Recovery::Reconnect,
            ErrorCategory::Transport => Recovery::Reconnect,
            ErrorCategory::Timeout => Recovery::RetryOrReconnect,
            ErrorCategory::Authentication => Recovery::Reauthenticate,
            ErrorCategory::Capability
            | ErrorCategory::SecurityPolicy
            | ErrorCategory::InvalidInput
            | ErrorCategory::Internal => Recovery::DoNotRetry,
            ErrorCategory::Protocol | ErrorCategory::Parse => Recovery::Reconnect,
            ErrorCategory::Transient => Recovery::RetryAfter,
            ErrorCategory::Referral => Recovery::FollowReferral,
            ErrorCategory::NotificationOverflow => Recovery::RebuildNotificationRegistration,
            ErrorCategory::Authorization | ErrorCategory::Limit | ErrorCategory::ServerRejected => {
                Recovery::DoNotRetry
            }
            ErrorCategory::MailboxState => Recovery::ResyncMailbox,
        }
    }

    /// Response code carried by a server status error, if any.
    ///
    /// The current parser stores the single response code attached to a
    /// status response. If a future parser preserves multiple codes, this
    /// accessor should grow alongside the stored representation.
    pub fn response_code(&self) -> Option<&ResponseCode> {
        match self {
            Self::Auth { code, .. }
            | Self::No { code, .. }
            | Self::Bad { code, .. }
            | Self::Bye { code, .. } => code.as_ref(),
            _ => None,
        }
    }
}

/// Broad error category for consumer policy decisions.
// protocol-specific: direct IMAP callers classify RFC 5530 response codes locally.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    Connection,
    Transport,
    Timeout,
    Authentication,
    Authorization,
    Capability,
    SecurityPolicy,
    Limit,
    MailboxState,
    Transient,
    Referral,
    NotificationOverflow,
    ServerRejected,
    Protocol,
    Parse,
    InvalidInput,
    Internal,
}

/// Structured reason automatic authentication could not select a mechanism.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthPolicyFailure {
    /// Mechanisms or commands the server offered for the supplied credential type.
    pub offered: Vec<String>,
    /// Offered mechanisms rejected by local policy.
    pub rejected: Vec<AuthMechanismRejection>,
}

impl AuthPolicyFailure {
    pub fn new(offered: Vec<String>, rejected: Vec<AuthMechanismRejection>) -> Self {
        Self { offered, rejected }
    }
}

impl std::fmt::Display for AuthPolicyFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.offered.is_empty() {
            f.write_str("server offered no compatible authentication mechanisms")
        } else if self.rejected.is_empty() {
            write!(
                f,
                "no permitted authentication mechanism among offered mechanisms: {}",
                self.offered.join(", ")
            )
        } else {
            write!(
                f,
                "no permitted authentication mechanism among offered mechanisms: {}; rejected: ",
                self.offered.join(", ")
            )?;
            for (idx, rejection) in self.rejected.iter().enumerate() {
                if idx > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{} ({})", rejection.mechanism.name(), rejection.reason)?;
            }
            Ok(())
        }
    }
}

/// Offered authentication mechanism rejected by local policy.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthMechanismRejection {
    /// Mechanism or legacy command rejected by policy.
    pub mechanism: AuthMechanism,
    /// Why the mechanism was rejected.
    pub reason: AuthMechanismRejectionReason,
}

impl AuthMechanismRejection {
    pub const fn new(mechanism: AuthMechanism, reason: AuthMechanismRejectionReason) -> Self {
        Self { mechanism, reason }
    }
}

/// Local policy reason for rejecting an offered authentication mechanism.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMechanismRejectionReason {
    /// Mechanism is disabled by local policy.
    DisabledByPolicy,
    /// Mechanism would expose credentials or bearer tokens without TLS.
    CleartextWithoutTls,
}

impl std::fmt::Display for AuthMechanismRejectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DisabledByPolicy => f.write_str("disabled by policy"),
            Self::CleartextWithoutTls => f.write_str("requires TLS by policy"),
        }
    }
}

impl ErrorCategory {
    fn from_response_code(code: Option<&ResponseCode>) -> Self {
        match code {
            Some(
                ResponseCode::AuthenticationFailed
                | ResponseCode::Expired
                | ResponseCode::PrivacyRequired,
            ) => Self::Authentication,
            Some(ResponseCode::AuthorizationFailed | ResponseCode::NoPerm) => Self::Authorization,
            Some(ResponseCode::ContactAdmin) => Self::Authorization,
            Some(
                ResponseCode::OverQuota
                | ResponseCode::TooBig
                | ResponseCode::Limit
                | ResponseCode::MetadataMaxSize(_),
            ) => Self::Limit,
            Some(
                ResponseCode::ExpungeIssued
                | ResponseCode::UidNotSticky
                | ResponseCode::Closed
                | ResponseCode::NoModSeq,
            ) => Self::MailboxState,
            Some(ResponseCode::AlreadyExists | ResponseCode::NonExistent) => Self::MailboxState,
            Some(
                ResponseCode::Unavailable
                | ResponseCode::InUse
                | ResponseCode::Corruption
                | ResponseCode::TempFail(_),
            ) => Self::Transient,
            Some(ResponseCode::Referral(_)) => Self::Referral,
            Some(ResponseCode::NotificationOverflow(_)) => Self::NotificationOverflow,
            Some(ResponseCode::Parse) => Self::Parse,
            Some(ResponseCode::BadCharset(_)) => Self::Capability,
            Some(
                ResponseCode::TryCreate
                | ResponseCode::NotSaved
                | ResponseCode::MetadataTooMany
                | ResponseCode::MetadataNoPrivate,
            ) => Self::MailboxState,
            Some(ResponseCode::Cannot | ResponseCode::ClientBug | ResponseCode::ServerBug) => {
                Self::Protocol
            }
            _ => Self::ServerRejected,
        }
    }
}

/// Suggested high-level recovery action for an IMAP error.
// protocol-specific: mapped into bifrost_types::RecoveryClass only at the Account boundary.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Recovery {
    /// The same request may be retried, but reconnecting may also be needed.
    RetryOrReconnect,
    /// Drop the connection and establish a new one.
    Reconnect,
    /// Re-authenticate before retrying account operations.
    Reauthenticate,
    /// Resynchronize the selected mailbox.
    ResyncMailbox,
    /// Retry later, optionally honoring backoff information from the server text.
    RetryAfter,
    /// Follow the referral target carried by the response code before retrying.
    FollowReferral,
    /// Rebuild NOTIFY registration state before relying on asynchronous events.
    RebuildNotificationRegistration,
    /// Do not retry automatically.
    DoNotRetry,
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
