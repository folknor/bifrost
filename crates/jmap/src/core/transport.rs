use bytes::Bytes;

/// Transport-level error. Wraps the underlying HTTP client error
/// without leaking it into the crate's public error model.
///
/// Every value carries its provenance: either the `bifrost-net` error the
/// wire produced, or a local failure this crate raised itself. There is no
/// third, unknown-provenance state. The one transport is `ReqwestTransport`
/// over `bifrost-net`, and tests script the wire beneath it rather than
/// replacing it, so nothing can build an error that says neither.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) struct TransportError {
    pub(crate) message: String,
    /// HTTP response body, if available (for parsing ProblemDetails).
    pub(crate) body: Option<Bytes>,
    pub(crate) cause: TransportCause,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Where a [`TransportError`] came from.
#[derive(Debug)]
pub(crate) enum TransportCause {
    /// The net pipeline's own error, classified through
    /// `bifrost_net::into_account_error_with_throttle_scope`.
    Net(bifrost_net::Error),
    /// This crate failed locally rather than on the wire: an invariant
    /// broken before anything was sent, or a client-side safety limit
    /// tripped while reading a response. Never a network fault to retry.
    Local(LocalFailure),
}

/// What failed locally in a [`TransportError`]. Semantic rather than a flag,
/// so each variant maps to its own `Internal` kind and no classification
/// ever keys on the message text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalFailure {
    /// A state the transport believes impossible by construction.
    InvariantViolated,
    /// A bound this crate imposes on a response the server is free to make
    /// arbitrarily large, where the protocol itself sets no limit (the
    /// EventSource block cap). The server broke no rule, so this is not a
    /// provider fault, and the same stream replayed trips the same bound, so
    /// it is not a network fault to retry either: `Internal(LimitExceeded)`.
    LimitExceeded,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

impl TransportError {
    fn local(
        failure: LocalFailure,
        message: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self {
            message: message.into(),
            body: None,
            cause: TransportCause::Local(failure),
            source,
        }
    }

    /// The net error, when the wire produced this failure.
    pub(crate) fn net(&self) -> Option<&bifrost_net::Error> {
        match &self.cause {
            TransportCause::Net(error) => Some(error),
            TransportCause::Local(_) => None,
        }
    }

    /// The local failure, when this crate raised it itself.
    pub(crate) fn local_failure(&self) -> Option<LocalFailure> {
        match self.cause {
            TransportCause::Local(failure) => Some(failure),
            TransportCause::Net(_) => None,
        }
    }

    /// A received HTTP refusal, with its status retained as evidence.
    pub(crate) fn with_body(status: reqwest::StatusCode, body: impl Into<Bytes>) -> Self {
        Self::from_net(bifrost_net::Error::Status {
            code: status,
            body: body.into(),
            headers: reqwest::header::HeaderMap::new(),
        })
    }

    /// A local state this crate believes impossible by construction, raised
    /// before anything was sent: `Internal(InvariantViolated)`, never a
    /// network fault.
    pub(crate) fn invariant(message: impl Into<String>) -> Self {
        Self::local(LocalFailure::InvariantViolated, message, None)
    }

    /// As [`TransportError::invariant`], keeping the underlying error.
    pub(crate) fn invariant_with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::local(
            LocalFailure::InvariantViolated,
            message,
            Some(Box::new(source)),
        )
    }

    /// A client-side safety limit on a response the protocol leaves
    /// unbounded: `Internal(LimitExceeded)`, never a network fault.
    pub(crate) fn limit_exceeded(message: impl Into<String>) -> Self {
        Self::local(LocalFailure::LimitExceeded, message, None)
    }

    /// Construct a `TransportError` from a `bifrost_net::Error`,
    /// preserving the original net error so the JMAP conversion
    /// boundary can classify net evidence through
    /// `bifrost_net::into_account_error_with_throttle_scope` with its
    /// explicit account throttle scope.
    pub(crate) fn from_net(error: bifrost_net::Error) -> Self {
        let message = match &error {
            bifrost_net::Error::Status { code, .. } => format!("HTTP {code}"),
            other => other.to_string(),
        };
        let body = match &error {
            bifrost_net::Error::Status { body, .. } => Some(body.clone()),
            other => preserved_response_body(other),
        };
        Self {
            message,
            body,
            cause: TransportCause::Net(error),
            source: None,
        }
    }
}

/// The response body a non-`Status` net error still carries.
///
/// `Error::Status` is the only variant bifrost-net produces for a
/// *terminal* HTTP status, and a 429 or 5xx never takes that path: the
/// retry loop consumes those and surfaces `RateLimited` /
/// `RetryBudgetExhausted` with the final attempt's response preserved
/// on the error instead. Those are exactly the statuses a JMAP server
/// answers with an RFC 7807 problem document (RFC 8620 s3.6.1), so the
/// body has to be lifted back out of the evidence or the JMAP boundary
/// never sees the provider's own explanation of the failure.
///
/// `AuthLost` is deliberately not lifted. Its 401 classification and
/// its `transmission_state` evidence are bifrost-net's to interpret,
/// and routing it through the problem-details branch would replace a
/// typed auth failure with a status-derived guess.
fn preserved_response_body(error: &bifrost_net::Error) -> Option<Bytes> {
    match error {
        bifrost_net::Error::UnexpectedResponse { response } => Some(response.body.clone()),
        bifrost_net::Error::RateLimited { final_response, .. } => Some(final_response.body.clone()),
        bifrost_net::Error::RetryBudgetExhausted {
            final_response: Some(final_response),
            ..
        } => Some(final_response.body.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod wire_tests;
