//! Error and result type for SMTP clients

use std::{error::Error as StdError, fmt, io};

use crate::{
    BoxError,
    transport::smtp::response::{Code, EnhancedStatusCode, Response, Severity},
};

// Inspired by https://github.com/seanmonstar/reqwest/blob/a8566383168c0ef06c21f38cbc9213af6ff6db31/src/error.rs

/// The Errors that may occur when sending an email over SMTP
// pub: SMTP keeps full Response-carrying errors instead of Account recovery errors.
pub struct Error {
    inner: Box<Inner>,
}

impl Clone for Error {
    fn clone(&self) -> Self {
        // BoxError is not Clone. Preserve diagnostic text as a plain string source so
        // batch pipelines that clone an error mid-flight retain the diagnostic message.
        let source: Option<BoxError> = self
            .inner
            .source
            .as_ref()
            .map(|e| -> BoxError { Box::new(StringError(e.to_string())) });
        Self {
            inner: Box::new(Inner {
                kind: self.inner.kind.clone(),
                source,
                attempt: self.inner.attempt,
                phase: self.inner.phase,
            }),
        }
    }
}

struct Inner {
    kind: ErrorKind,
    source: Option<BoxError>,
    attempt: Option<SmtpAttempt>,
    phase: Option<SmtpCommandPhase>,
}

/// Wire-level transmission state for the mail-send side effect.
///
/// `Unsent` means no recipient command has been written.
/// `InFlight` means a write or read failed mid-command and the server's view is
/// ambiguous. `Acknowledged` means the server returned a definitive negative
/// reply for the command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SmtpTransmissionState {
    Unsent,
    InFlight,
    Acknowledged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SmtpAttempt {
    pub(crate) transmission_state: SmtpTransmissionState,
}

/// Coarse command phase at the point an SMTP transport error was constructed.
///
/// Used by the shared-error mapper to refine kind/cause selection (notably `AUTH`
/// vs send-side effects) without leaking the public transport `ErrorKind` enum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SmtpCommandPhase {
    Connect,
    Greeting,
    Hello,
    StartTls,
    Auth,
    MailFrom,
    RcptTo,
    DataCommand,
    DataBody,
    BdatBody,
    LmtpFinalStatus,
    Noop,
    Vrfy,
    Expn,
    Rset,
}

impl Error {
    pub(crate) fn new<E>(kind: ErrorKind, source: Option<E>) -> Error
    where
        E: Into<BoxError>,
    {
        Error {
            inner: Box::new(Inner {
                kind,
                source: source.map(Into::into),
                attempt: None,
                phase: None,
            }),
        }
    }

    fn without_source(kind: ErrorKind) -> Error {
        Error {
            inner: Box::new(Inner {
                kind,
                source: None,
                attempt: None,
                phase: None,
            }),
        }
    }

    /// Attach transmission state to a transport-layer error. Returns a builder-
    /// style updated `Error`. Crate-internal: send pipelines decorate errors at
    /// the point they cross a command boundary so the account-error mapper can
    /// emit the right `Attempt` cause without re-deriving wire context.
    pub(crate) fn with_attempt(mut self, state: SmtpTransmissionState) -> Self {
        self.inner.attempt = Some(SmtpAttempt {
            transmission_state: state,
        });
        self
    }

    /// Attach the command phase the error originated in. Diagnostic only;
    /// classification continues to use `ErrorKind` and the response payload.
    pub(crate) fn with_phase(mut self, phase: SmtpCommandPhase) -> Self {
        self.inner.phase = Some(phase);
        self
    }

    pub(crate) fn attempt(&self) -> Option<SmtpTransmissionState> {
        self.inner.attempt.map(|a| a.transmission_state)
    }

    pub(crate) fn phase(&self) -> Option<SmtpCommandPhase> {
        self.inner.phase
    }

    /// Support-safe diagnostic string for the account-error mapper. AUTH paths
    /// already redact credentials before they reach here; this just exposes the
    /// boxed source or the kind discriminant for support text.
    pub(crate) fn diagnostic_text(&self) -> Option<String> {
        self.inner
            .source
            .as_ref()
            .map(std::string::ToString::to_string)
    }

    /// Returns the classification for this SMTP error.
    pub fn kind(&self) -> &ErrorKind {
        &self.inner.kind
    }

    /// Returns the SMTP reply that caused this error.
    pub fn smtp_response(&self) -> Option<&Response> {
        match &self.inner.kind {
            ErrorKind::Transient(response) | ErrorKind::Permanent(response) => Some(response),
            _ => None,
        }
    }

    /// Returns the SMTP status code for reply errors.
    pub fn status(&self) -> Option<Code> {
        self.smtp_response().map(Response::code)
    }

    /// Returns the first enhanced status code from the SMTP reply text.
    pub fn enhanced_status_code(&self) -> Option<EnhancedStatusCode> {
        self.smtp_response()
            .and_then(Response::enhanced_status_code)
    }

    /// Returns true if the error is a transient or permanent SMTP reply.
    pub fn is_smtp_reply(&self) -> bool {
        self.smtp_response().is_some()
    }

    /// Returns true if the client failed while parsing an SMTP response.
    ///
    /// This does not include SMTP 4xx or 5xx replies. For those, use
    /// [`Error::is_transient`], [`Error::is_permanent`], or
    /// [`Error::smtp_response`].
    pub fn is_parse(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Parse)
    }

    /// Returns true if the error is from invalid caller input or local SMTP state.
    pub fn is_invalid_input(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::InvalidInput)
    }

    /// Returns true if the error is from a violated client invariant.
    pub fn is_internal(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Internal)
    }

    /// Returns true if the error is a client-side policy refusal.
    ///
    /// This includes refusing to transmit credentials over an unencrypted
    /// SMTP connection.
    pub fn is_policy(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Policy)
    }

    /// Returns true if the error is a transient SMTP 4xx reply.
    ///
    /// This is not the inverse of [`Error::is_permanent`]: parse,
    /// invalid-input, connection, network, TLS, timeout, and shutdown errors
    /// are neither transient nor permanent SMTP replies.
    pub fn is_transient(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Transient(_))
    }

    /// Returns true if the error is a permanent SMTP 5xx reply.
    ///
    /// This is not the inverse of [`Error::is_transient`]: parse,
    /// invalid-input, connection, network, TLS, timeout, and shutdown errors
    /// are neither transient nor permanent SMTP replies.
    pub fn is_permanent(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Permanent(_))
    }

    /// Returns true if the error happened while opening or maintaining the SMTP connection.
    pub fn is_connection(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Connection)
    }

    /// Returns true if the error came from the underlying network I/O layer.
    pub fn is_network(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Network)
    }

    /// Returns true if the error is caused by a timeout.
    pub fn is_timeout(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Timeout)
    }

    /// Returns true if the error is from TLS.
    pub fn is_tls(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Tls)
    }

    /// Returns true if the error is because the transport was shut down.
    pub fn is_transport_shutdown(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::TransportShutdown)
    }
}

/// Public classification for [`Error`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
// pub: callers classify SMTP reply, transport, TLS, and policy failures.
pub enum ErrorKind {
    /// Transient SMTP reply, 4xx status.
    Transient(Response),
    /// Permanent SMTP reply, 5xx status.
    Permanent(Response),
    /// Error parsing an SMTP response.
    Parse,
    /// Invalid caller input, unsupported local configuration, or local protocol state.
    InvalidInput,
    /// Internal client invariant failure.
    Internal,
    /// Client-side policy refusal.
    Policy,
    /// Connection error.
    Connection,
    /// Underlying network I/O error.
    Network,
    /// Timeout while connecting, reading, writing, or upgrading TLS.
    Timeout,
    /// TLS error.
    Tls,
    /// Transport shutdown error.
    TransportShutdown,
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("bifrost_smtp::transport::smtp::Error");

        builder.field("kind", &self.inner.kind);

        if let Some(source) = &self.inner.source {
            builder.field("source", source);
        }

        builder.finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner.kind {
            ErrorKind::Parse => f.write_str("SMTP response parse error")?,
            ErrorKind::InvalidInput => f.write_str("invalid SMTP input")?,
            ErrorKind::Internal => f.write_str("internal SMTP error")?,
            ErrorKind::Policy => f.write_str("SMTP policy error")?,
            ErrorKind::Network => f.write_str("SMTP network error")?,
            ErrorKind::Connection => f.write_str("SMTP connection error")?,
            ErrorKind::Timeout => f.write_str("SMTP operation timed out")?,
            ErrorKind::Tls => f.write_str("SMTP TLS error")?,
            ErrorKind::TransportShutdown => f.write_str("SMTP transport has been shut down")?,
            ErrorKind::Transient(response) => {
                write!(f, "transient SMTP reply ({})", response.code())?;
                write_response_message(f, response)?;
            }
            ErrorKind::Permanent(response) => {
                write!(f, "permanent SMTP reply ({})", response.code())?;
                write_response_message(f, response)?;
            }
        }

        if let Some(e) = &self.inner.source {
            write!(f, ": {e}")?;
        }

        Ok(())
    }
}

fn write_response_message(f: &mut fmt::Formatter<'_>, response: &Response) -> fmt::Result {
    let mut message = response.message();
    if let Some(first) = message.next() {
        write!(f, ": {first}")?;
        for line in message {
            write!(f, " / {line}")?;
        }
    }
    Ok(())
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner.source.as_ref().map(|e| {
            let r: &(dyn std::error::Error + 'static) = &**e;
            r
        })
    }
}

pub(crate) fn status(response: Response) -> Error {
    match response.code().severity {
        Severity::TransientNegativeCompletion => {
            Error::without_source(ErrorKind::Transient(response))
        }
        Severity::PermanentNegativeCompletion => {
            Error::without_source(ErrorKind::Permanent(response))
        }
        _ => internal(format!(
            "unexpected positive SMTP reply {}",
            response.code()
        )),
    }
}

/// Helper for `Clone` on `Error`: wraps a diagnostic string as a `BoxError`
/// source so the cloned error retains its human-readable message while discarding
/// the original non-Clone source.
#[derive(Debug)]
pub(crate) struct StringError(pub(crate) String);

impl fmt::Display for StringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl StdError for StringError {}

pub(crate) fn parse<E: Into<BoxError>>(e: E) -> Error {
    Error::new(ErrorKind::Parse, Some(e))
}

pub(crate) fn invalid_input<E: Into<BoxError>>(e: E) -> Error {
    Error::new(ErrorKind::InvalidInput, Some(e))
}

pub(crate) fn internal<E: Into<BoxError>>(e: E) -> Error {
    Error::new(ErrorKind::Internal, Some(e))
}

pub(crate) fn policy<E: Into<BoxError>>(e: E) -> Error {
    Error::new(ErrorKind::Policy, Some(e))
}

fn io_error(default_kind: ErrorKind, error: io::Error) -> Error {
    let kind = match error.kind() {
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => ErrorKind::Timeout,
        _ => default_kind,
    };
    Error::new(kind, Some(error))
}

pub(crate) fn network(e: io::Error) -> Error {
    io_error(ErrorKind::Network, e)
}

pub(crate) fn connection_io(e: io::Error) -> Error {
    io_error(ErrorKind::Connection, e)
}

// Use `connection_io` for connection-layer I/O errors so timeout
// classification is preserved at construction.
pub(crate) fn connection<E: Into<BoxError>>(e: E) -> Error {
    Error::new(ErrorKind::Connection, Some(e))
}

#[cfg(feature = "tokio")]
pub(crate) fn timeout(message: &'static str) -> Error {
    Error::new(
        ErrorKind::Timeout,
        Some(std::io::Error::new(std::io::ErrorKind::TimedOut, message)),
    )
}

pub(crate) fn tls<E: Into<BoxError>>(e: E) -> Error {
    Error::new(ErrorKind::Tls, Some(e))
}

pub(crate) fn transport_shutdown() -> Error {
    Error::without_source(ErrorKind::TransportShutdown)
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::{ErrorKind, connection, connection_io, internal, network, policy, status};
    use crate::transport::smtp::response::{Category, Code, Detail, Response, Severity};

    #[test]
    fn exposes_connection_kind() {
        let error = connection("server closed the connection");

        assert_eq!(error.kind(), &ErrorKind::Connection);
        assert!(error.is_connection());
        assert!(!error.is_network());
        assert!(!error.is_transient());
        assert!(!error.is_permanent());
    }

    #[test]
    fn exposes_network_kind() {
        let error = network(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "socket write failed",
        ));

        assert_eq!(error.kind(), &ErrorKind::Network);
        assert!(error.is_network());
        assert!(!error.is_connection());
    }

    #[test]
    fn exposes_timeout_kind_for_io_timeouts() {
        let network_timeout = network(io::Error::new(io::ErrorKind::TimedOut, "read timed out"));
        let connection_timeout = connection_io(io::Error::new(
            io::ErrorKind::WouldBlock,
            "connect timed out",
        ));

        assert_eq!(network_timeout.kind(), &ErrorKind::Timeout);
        assert!(network_timeout.is_timeout());
        assert_eq!(connection_timeout.kind(), &ErrorKind::Timeout);
        assert!(connection_timeout.is_timeout());
    }

    #[test]
    fn exposes_internal_kind() {
        let error = internal("recipient status invariant failed");

        assert_eq!(error.kind(), &ErrorKind::Internal);
        assert!(error.is_internal());
        assert!(!error.is_invalid_input());
    }

    #[test]
    fn exposes_smtp_reply_kind_and_status() {
        let code = Code::new(
            Severity::TransientNegativeCompletion,
            Category::Connections,
            Detail::Zero,
        );
        let response = Response::new(code, vec!["4.2.0 mailbox busy".to_owned()]);
        let error = status(response.clone());

        assert_eq!(error.kind(), &ErrorKind::Transient(response));
        assert_eq!(error.status(), Some(code));
        assert_eq!(error.enhanced_status_code().unwrap().to_string(), "4.2.0");
        assert!(error.is_transient());
        assert!(!error.is_permanent());
        assert!(error.is_smtp_reply());
    }

    #[test]
    fn exposes_policy_kind() {
        let error = policy("refusing to authenticate over plaintext");

        assert_eq!(error.kind(), &ErrorKind::Policy);
        assert!(error.is_policy());
        assert!(!error.is_invalid_input());
    }
}
