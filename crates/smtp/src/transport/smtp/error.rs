//! Error and result type for SMTP clients

use std::{error::Error as StdError, fmt};

use crate::{
    BoxError,
    transport::smtp::response::{Code, Severity},
};

// Inspired by https://github.com/seanmonstar/reqwest/blob/a8566383168c0ef06c21f38cbc9213af6ff6db31/src/error.rs

/// The Errors that may occur when sending an email over SMTP
pub struct Error {
    inner: Box<Inner>,
}

struct Inner {
    kind: Kind,
    source: Option<BoxError>,
}

impl Error {
    pub(crate) fn new<E>(kind: Kind, source: Option<E>) -> Error
    where
        E: Into<BoxError>,
    {
        Error {
            inner: Box::new(Inner {
                kind,
                source: source.map(Into::into),
            }),
        }
    }

    /// Returns the public kind of this SMTP error.
    pub fn kind(&self) -> ErrorKind {
        self.inner.kind.as_public()
    }

    /// Returns true if the client failed while parsing an SMTP response.
    ///
    /// This does not include SMTP 4xx or 5xx replies. For those, use
    /// [`Error::is_transient`], [`Error::is_permanent`], or [`Error::status`].
    pub fn is_response(&self) -> bool {
        matches!(self.inner.kind, Kind::Response)
    }

    /// Returns true if the error is from client-side validation or protocol state.
    pub fn is_client(&self) -> bool {
        matches!(self.inner.kind, Kind::Client)
    }

    /// Returns true if the error is a transient SMTP 4xx reply.
    ///
    /// This is not the inverse of [`Error::is_permanent`]: parse, client,
    /// connection, network, TLS, and shutdown errors are neither transient nor
    /// permanent SMTP replies.
    pub fn is_transient(&self) -> bool {
        matches!(self.inner.kind, Kind::Transient(_))
    }

    /// Returns true if the error is a permanent SMTP 5xx reply.
    ///
    /// This is not the inverse of [`Error::is_transient`]: parse, client,
    /// connection, network, TLS, and shutdown errors are neither transient nor
    /// permanent SMTP replies.
    pub fn is_permanent(&self) -> bool {
        matches!(self.inner.kind, Kind::Permanent(_))
    }

    /// Returns true if the error happened while opening or maintaining the SMTP connection.
    pub fn is_connection(&self) -> bool {
        matches!(self.inner.kind, Kind::Connection)
    }

    /// Returns true if the error came from the underlying network I/O layer.
    pub fn is_network(&self) -> bool {
        matches!(self.inner.kind, Kind::Network)
    }

    /// Returns true if the error is caused by a timeout
    pub fn is_timeout(&self) -> bool {
        let mut source = self.source();

        while let Some(err) = source {
            if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
                return io_err.kind() == std::io::ErrorKind::TimedOut;
            }

            source = err.source();
        }

        false
    }

    /// Returns true if the error is from TLS
    #[cfg(feature = "native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
    pub fn is_tls(&self) -> bool {
        matches!(self.inner.kind, Kind::Tls)
    }

    /// Returns true if the error is because the transport was shut down
    pub fn is_transport_shutdown(&self) -> bool {
        matches!(self.inner.kind, Kind::TransportShutdown)
    }

    /// Returns the status code, if the error was generated from a response.
    pub fn status(&self) -> Option<Code> {
        match self.inner.kind {
            Kind::Transient(code) | Kind::Permanent(code) => Some(code),
            _ => None,
        }
    }
}

/// Public classification for [`Error`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Transient SMTP error, 4xx reply code.
    Transient(Code),
    /// Permanent SMTP error, 5xx reply code.
    Permanent(Code),
    /// Error parsing an SMTP response.
    Response,
    /// Internal client error.
    Client,
    /// Connection error.
    Connection,
    /// Underlying network I/O error.
    Network,
    /// TLS error.
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
    #[cfg(feature = "native-tls")]
    Tls,
    /// Transport shutdown error.
    TransportShutdown,
}

#[derive(Debug)]
pub(crate) enum Kind {
    /// Transient SMTP error, 4xx reply code
    ///
    /// [RFC 5321, section 4.2.1](https://tools.ietf.org/html/rfc5321#section-4.2.1)
    Transient(Code),
    /// Permanent SMTP error, 5xx reply code
    ///
    /// [RFC 5321, section 4.2.1](https://tools.ietf.org/html/rfc5321#section-4.2.1)
    Permanent(Code),
    /// Error parsing a response
    Response,
    /// Internal client error
    Client,
    /// Connection error
    Connection,
    /// Underlying network i/o error
    Network,
    /// TLS error
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
    #[cfg(feature = "native-tls")]
    Tls,
    /// Transport shutdown error
    TransportShutdown,
}

impl Kind {
    fn as_public(&self) -> ErrorKind {
        match *self {
            Kind::Transient(code) => ErrorKind::Transient(code),
            Kind::Permanent(code) => ErrorKind::Permanent(code),
            Kind::Response => ErrorKind::Response,
            Kind::Client => ErrorKind::Client,
            Kind::Connection => ErrorKind::Connection,
            Kind::Network => ErrorKind::Network,
            #[cfg(feature = "native-tls")]
            Kind::Tls => ErrorKind::Tls,
            Kind::TransportShutdown => ErrorKind::TransportShutdown,
        }
    }
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
            Kind::Response => f.write_str("response error")?,
            Kind::Client => f.write_str("internal client error")?,
            Kind::Network => f.write_str("network error")?,
            Kind::Connection => f.write_str("Connection error")?,
            #[cfg(feature = "native-tls")]
            Kind::Tls => f.write_str("tls error")?,
            Kind::TransportShutdown => f.write_str("transport has been shut down")?,
            Kind::Transient(code) => {
                write!(f, "transient error ({code})")?;
            }
            Kind::Permanent(code) => {
                write!(f, "permanent error ({code})")?;
            }
        }

        if let Some(e) = &self.inner.source {
            write!(f, ": {e}")?;
        }

        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner.source.as_ref().map(|e| {
            let r: &(dyn std::error::Error + 'static) = &**e;
            r
        })
    }
}

pub(crate) fn code(c: Code, s: Option<String>) -> Error {
    match c.severity {
        Severity::TransientNegativeCompletion => Error::new(Kind::Transient(c), s),
        Severity::PermanentNegativeCompletion => Error::new(Kind::Permanent(c), s),
        _ => client("Unknown error code"),
    }
}

pub(crate) fn response<E: Into<BoxError>>(e: E) -> Error {
    Error::new(Kind::Response, Some(e))
}

pub(crate) fn client<E: Into<BoxError>>(e: E) -> Error {
    Error::new(Kind::Client, Some(e))
}

pub(crate) fn network<E: Into<BoxError>>(e: E) -> Error {
    Error::new(Kind::Network, Some(e))
}

pub(crate) fn connection<E: Into<BoxError>>(e: E) -> Error {
    Error::new(Kind::Connection, Some(e))
}

#[cfg(any(feature = "tokio1", feature = "async-std1"))]
pub(crate) fn timeout(message: &'static str) -> Error {
    connection(std::io::Error::new(std::io::ErrorKind::TimedOut, message))
}

#[cfg(feature = "native-tls")]
pub(crate) fn tls<E: Into<BoxError>>(e: E) -> Error {
    Error::new(Kind::Tls, Some(e))
}

pub(crate) fn transport_shutdown() -> Error {
    Error::new::<BoxError>(Kind::TransportShutdown, None)
}

#[cfg(test)]
mod tests {
    use super::{ErrorKind, code, connection, network};
    use crate::transport::smtp::response::{Category, Code, Detail, Severity};

    #[test]
    fn exposes_connection_kind() {
        let error = connection("server closed the connection");

        assert_eq!(error.kind(), ErrorKind::Connection);
        assert!(error.is_connection());
        assert!(!error.is_network());
        assert!(!error.is_transient());
        assert!(!error.is_permanent());
    }

    #[test]
    fn exposes_network_kind() {
        let error = network("socket write failed");

        assert_eq!(error.kind(), ErrorKind::Network);
        assert!(error.is_network());
        assert!(!error.is_connection());
    }

    #[test]
    fn exposes_smtp_reply_kind_and_status() {
        let status = Code::new(
            Severity::TransientNegativeCompletion,
            Category::Connections,
            Detail::Zero,
        );
        let error = code(status, None);

        assert_eq!(error.kind(), ErrorKind::Transient(status));
        assert_eq!(error.status(), Some(status));
        assert!(error.is_transient());
        assert!(!error.is_permanent());
    }
}
