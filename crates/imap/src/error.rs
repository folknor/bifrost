//! Internal IMAP error type.
//!
//! `Error` is the crate-private failure representation produced by the
//! driver, parser, encoder, and high-level command surface. It carries
//! enough wire-level evidence (response codes, attempt state) for the
//! account-boundary translation in `account/error.rs` to build a
//! faithful `bifrost_types::AccountError`.
//!
//! Server status responses (OK, NO, BAD, BYE) are defined in RFC 3501
//! Section 7.1 and RFC 9051 Section 7.1.

use std::sync::Arc;

use bifrost_types::TransmissionState;

use crate::types::{AuthMechanism, ResponseCode};

/// Crate-internal wire-level transmission evidence attached to
/// transport-shaped failures.
///
/// The driver populates this when it has direct knowledge of whether a
/// command's bytes ever crossed the side-effect boundary; the account
/// boundary then projects it into `bifrost_types::AttemptCause`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct ImapAttempt {
    pub(crate) transmission_state: TransmissionState,
}

impl ImapAttempt {
    pub(crate) const fn new(transmission_state: TransmissionState) -> Self {
        Self { transmission_state }
    }
}

/// Error type for IMAP client operations.
//
// Several variants carry `attempt: Option<ImapAttempt>` so the account
// boundary can distinguish `Unsent` / `InFlight` / `Acknowledged`
// transmissions when building `bifrost_types::AccountError`. The
// driver fills this in at the point of failure; pre-driver call sites
// (encoding, validation, builder preflight) leave it `None`.
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum Error {
    /// Underlying I/O error, including TLS transport errors (RFC 3501 Section 2.1).
    #[error("I/O error: {source}")]
    Io {
        #[source]
        source: Arc<std::io::Error>,
        attempt: Option<ImapAttempt>,
    },

    /// Authentication was rejected by the server (RFC 3501 Section 6.2.2).
    #[error("authentication failed: {text}")]
    Auth {
        text: String,
        code: Option<ResponseCode>,
    },

    /// Server returned a NO response to a command (RFC 3501 Section 7.1.2).
    #[error("server rejected command: {text}")]
    No {
        text: String,
        code: Option<ResponseCode>,
    },

    /// Server returned a BAD response (RFC 3501 Section 7.1.3).
    #[error("server reported bad command: {text}")]
    Bad {
        text: String,
        code: Option<ResponseCode>,
    },

    /// Server sent BYE (RFC 3501 Section 7.1.5).
    #[error("server closing connection: {text}")]
    Bye {
        text: String,
        code: Option<ResponseCode>,
        attempt: Option<ImapAttempt>,
    },

    /// IMAP protocol violation by the server.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Failed to parse a server response.
    #[error("parse error: {0}")]
    Parse(String),

    /// Local request was rejected before transmission (invalid input,
    /// mailbox name, validator failure). Distinct from `Protocol`,
    /// which means the *server* violated the wire contract.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// Operation exceeded the caller-supplied timeout.
    #[error("operation timed out")]
    Timeout { attempt: Option<ImapAttempt> },

    /// The TCP connection has been closed (RFC 3501 Section 2.1).
    #[error("connection closed")]
    Closed { attempt: Option<ImapAttempt> },

    /// STARTTLS was requested but the server does not advertise it.
    #[error("STARTTLS not supported by server")]
    StartTlsUnavailable,

    /// Authentication policy rejected every mechanism the server offered.
    #[error("authentication policy rejected authentication: {0}")]
    AuthPolicy(AuthPolicyFailure),

    /// A capability required for the requested operation is not advertised.
    #[error("missing required capability: {0}")]
    MissingCapability(String),

    /// Message exceeds the server's advertised APPENDLIMIT (RFC 7889 Section 3).
    #[error("message size {size} exceeds server APPENDLIMIT of {limit}")]
    AppendLimit { size: u64, limit: u64 },

    /// A buffered FETCH exceeded the caller's configured memory budget.
    #[error("estimated FETCH response size {estimated} exceeds caller limit of {limit}")]
    FetchLimit {
        estimated: usize,
        limit: usize,
        seq: u32,
        uid: Option<u32>,
    },

    /// Invalid APPEND date-time (RFC 3501 Section 9 `date-time`).
    #[error("invalid APPEND date-time: {0}")]
    InvalidAppendDate(String),

    /// Internal driver invariant violation.
    #[error("internal error: {0}")]
    Internal(String),

    /// The driver task panicked.
    #[error("driver task panicked: {message}")]
    DriverPanicked {
        message: String,
        attempt: Option<ImapAttempt>,
    },

    /// The driver task exited and the command channel is closed.
    #[error("driver task gone")]
    DriverGone { attempt: Option<ImapAttempt> },
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io {
            source: Arc::new(e),
            attempt: None,
        }
    }
}

impl From<crate::types::ValidationError> for Error {
    fn from(e: crate::types::ValidationError) -> Self {
        Self::InvalidInput(e.to_string())
    }
}

impl From<crate::codec::encode::EncodeError> for Error {
    fn from(e: crate::codec::encode::EncodeError) -> Self {
        match e {
            crate::codec::encode::EncodeError::MissingCapability { cmd, cap } => {
                Self::MissingCapability(format!("{cmd} requires {cap}"))
            }
            crate::codec::encode::EncodeError::Validation(msg) => Self::InvalidInput(msg),
        }
    }
}

/// Equality across variants. `Io` compares by `std::io::ErrorKind` only
/// (the underlying error is not `PartialEq`).
impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Io {
                    source: a,
                    attempt: a_att,
                },
                Self::Io {
                    source: b,
                    attempt: b_att,
                },
            ) => a.kind() == b.kind() && a_att == b_att,
            (Self::Auth { text: t1, code: c1 }, Self::Auth { text: t2, code: c2 })
            | (Self::No { text: t1, code: c1 }, Self::No { text: t2, code: c2 })
            | (Self::Bad { text: t1, code: c1 }, Self::Bad { text: t2, code: c2 }) => {
                t1 == t2 && c1 == c2
            }
            (
                Self::Bye {
                    text: t1,
                    code: c1,
                    attempt: a1,
                },
                Self::Bye {
                    text: t2,
                    code: c2,
                    attempt: a2,
                },
            ) => t1 == t2 && c1 == c2 && a1 == a2,
            (Self::Protocol(a), Self::Protocol(b))
            | (Self::Parse(a), Self::Parse(b))
            | (Self::InvalidInput(a), Self::InvalidInput(b))
            | (Self::MissingCapability(a), Self::MissingCapability(b))
            | (Self::InvalidAppendDate(a), Self::InvalidAppendDate(b))
            | (Self::Internal(a), Self::Internal(b)) => a == b,
            (Self::AuthPolicy(a), Self::AuthPolicy(b)) => a == b,
            (Self::Timeout { attempt: a }, Self::Timeout { attempt: b })
            | (Self::Closed { attempt: a }, Self::Closed { attempt: b })
            | (Self::DriverGone { attempt: a }, Self::DriverGone { attempt: b }) => a == b,
            (Self::StartTlsUnavailable, Self::StartTlsUnavailable) => true,
            (
                Self::DriverPanicked {
                    message: m1,
                    attempt: a1,
                },
                Self::DriverPanicked {
                    message: m2,
                    attempt: a2,
                },
            ) => m1 == m2 && a1 == a2,
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
    /// Construct a transport-flavored I/O error with no attempt-state evidence.
    pub(crate) fn io(source: std::io::Error) -> Self {
        Self::Io {
            source: Arc::new(source),
            attempt: None,
        }
    }

    /// Construct a `Timeout` with no attempt-state evidence.
    pub(crate) const fn timeout() -> Self {
        Self::Timeout { attempt: None }
    }

    /// Construct a `Timeout` with `InFlight` attempt evidence.
    ///
    /// Used by command sites that submit to the driver and then time out
    /// waiting for the response: the command bytes have already been sent
    /// to the server by the time the outer timeout fires.
    pub(crate) fn timeout_inflight() -> Self {
        Self::Timeout {
            attempt: Some(ImapAttempt::new(TransmissionState::InFlight)),
        }
    }

    /// Construct a `Closed` with no attempt-state evidence.
    pub(crate) const fn closed() -> Self {
        Self::Closed { attempt: None }
    }

    /// Construct a `DriverGone` with no attempt-state evidence.
    pub(crate) const fn driver_gone() -> Self {
        Self::DriverGone { attempt: None }
    }

    /// Attach (or override) the attempt evidence on a transport-shaped error.
    ///
    /// No-op for variants that do not carry attempt state (auth status,
    /// local validation, capability gating).
    #[must_use]
    pub(crate) fn with_attempt(self, state: TransmissionState) -> Self {
        let attempt = Some(ImapAttempt::new(state));
        match self {
            Self::Io { source, .. } => Self::Io { source, attempt },
            Self::Timeout { .. } => Self::Timeout { attempt },
            Self::Closed { .. } => Self::Closed { attempt },
            Self::Bye { text, code, .. } => Self::Bye {
                text,
                code,
                attempt,
            },
            Self::DriverGone { .. } => Self::DriverGone { attempt },
            Self::DriverPanicked { message, .. } => Self::DriverPanicked { message, attempt },
            other => other,
        }
    }

    /// Read out attempt evidence, if any.
    pub(crate) fn attempt(&self) -> Option<TransmissionState> {
        match self {
            Self::Io { attempt, .. }
            | Self::Timeout { attempt }
            | Self::Closed { attempt }
            | Self::Bye { attempt, .. }
            | Self::DriverPanicked { attempt, .. }
            | Self::DriverGone { attempt } => attempt.map(|a| a.transmission_state),
            _ => None,
        }
    }

    /// Construct an [`Error::No`] with an optional response code.
    pub(crate) fn no_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::No { text, code }
    }

    /// Construct an [`Error::Bad`] with an optional response code.
    pub(crate) fn bad_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::Bad { text, code }
    }

    /// Construct an [`Error::Auth`] with an optional response code.
    pub(crate) fn auth_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::Auth { text, code }
    }

    /// Construct an [`Error::Bye`] with an optional response code.
    pub(crate) fn bye_with_code(text: String, code: Option<ResponseCode>) -> Self {
        Self::Bye {
            text,
            code,
            attempt: None,
        }
    }

    /// Response code carried by a server status error, if any.
    pub(crate) fn response_code(&self) -> Option<&ResponseCode> {
        match self {
            Self::Auth { code, .. }
            | Self::No { code, .. }
            | Self::Bad { code, .. }
            | Self::Bye { code, .. } => code.as_ref(),
            _ => None,
        }
    }
}

/// Structured reason automatic authentication could not select a mechanism.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthPolicyFailure {
    pub(crate) offered: Vec<String>,
    pub(crate) rejected: Vec<AuthMechanismRejection>,
}

impl AuthPolicyFailure {
    pub(crate) fn new(offered: Vec<String>, rejected: Vec<AuthMechanismRejection>) -> Self {
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
pub(crate) struct AuthMechanismRejection {
    pub(crate) mechanism: AuthMechanism,
    pub(crate) reason: AuthMechanismRejectionReason,
}

impl AuthMechanismRejection {
    pub(crate) const fn new(
        mechanism: AuthMechanism,
        reason: AuthMechanismRejectionReason,
    ) -> Self {
        Self { mechanism, reason }
    }
}

/// Local policy reason for rejecting an offered authentication mechanism.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum AuthMechanismRejectionReason {
    DisabledByPolicy,
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

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
