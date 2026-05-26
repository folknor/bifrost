use std::future::Future;

use bytes::Bytes;

/// Transport-level error. Wraps the underlying HTTP client error
/// without leaking it into the crate's public error model.
// types: crate-owned so custom transports can still carry response bodies for ProblemDetails.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) struct TransportError {
    pub(crate) message: String,
    /// HTTP response body, if available (for parsing ProblemDetails).
    pub(crate) body: Option<Bytes>,
    /// Original net error, when the default reqwest-backed transport
    /// produced this `TransportError`. Custom transports leave this
    /// `None`; the conversion boundary falls back to a generic
    /// `Transport(Network)` classification when absent.
    pub(crate) net: Option<bifrost_net::Error>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
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
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            body: None,
            net: None,
            source: None,
        }
    }

    pub(crate) fn with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            message: message.into(),
            body: None,
            net: None,
            source: Some(Box::new(source)),
        }
    }

    pub(crate) fn with_body(message: impl Into<String>, body: impl Into<Bytes>) -> Self {
        Self {
            message: message.into(),
            body: Some(body.into()),
            net: None,
            source: None,
        }
    }

    /// Construct a `TransportError` from a `bifrost_net::Error`,
    /// preserving the original net error so the JMAP conversion
    /// boundary can delegate to `bifrost_net::into_account_error`
    /// for pure transport failures.
    pub(crate) fn from_net(error: bifrost_net::Error) -> Self {
        let message = error.to_string();
        match error {
            bifrost_net::Error::Status {
                code,
                body,
                headers,
            } => Self {
                message: format!("HTTP {code}"),
                body: Some(body.clone()),
                net: Some(bifrost_net::Error::Status {
                    code,
                    body,
                    headers,
                }),
                source: None,
            },
            other => Self {
                message,
                body: None,
                net: Some(other),
                source: None,
            },
        }
    }
}

/// HTTP transport abstraction.
///
/// Implement this trait to use a custom HTTP client. The default
/// implementation uses `reqwest`.
pub(crate) trait HttpTransport: Send + Sync + 'static {
    /// Send a JMAP API request (POST with JSON body).
    fn api_request(
        &self,
        url: &str,
        body: Vec<u8>,
    ) -> impl Future<Output = Result<Bytes, TransportError>> + Send;

    /// Upload a blob (POST with binary body).
    fn upload(
        &self,
        url: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> impl Future<Output = Result<Bytes, TransportError>> + Send;

    /// Download a blob (GET, returns raw bytes).
    fn download(&self, url: &str) -> impl Future<Output = Result<Bytes, TransportError>> + Send;

    /// Fetch the session resource (GET, returns JSON).
    fn get_session(&self, url: &str) -> impl Future<Output = Result<Bytes, TransportError>> + Send;
}

/// Streaming transport for Server-Sent Events (EventSource).
///
/// Implement this to provide EventSource support with a custom HTTP client.
/// The default implementation uses reqwest's byte streaming.
pub(crate) trait SseTransport: Send + Sync + 'static {
    /// The byte stream type returned by the SSE connection.
    type ByteStream: futures::Stream<Item = Result<Vec<u8>, TransportError>> + Send + Unpin;

    /// Open an SSE connection to the given URL.
    ///
    /// If `last_event_id` is provided, the transport should send it as the
    /// `Last-Event-ID` HTTP header, allowing the server to replay missed events.
    fn open_sse(
        &self,
        url: &str,
        last_event_id: Option<&str>,
    ) -> impl Future<Output = Result<Self::ByteStream, TransportError>> + Send;
}
