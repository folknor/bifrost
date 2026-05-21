//! Per-request builder, finished response shapes, and the streaming
//! body adapter.
//!
//! Protocol crates never see a `reqwest::RequestBuilder` directly.
//! Everything routes through this wrapper so the underlying HTTP
//! stack can be swapped without breaking call sites.

use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use reqwest::{StatusCode, header::HeaderMap};
use serde::Serialize;

use crate::error::Error;
use crate::retry::RetryPolicy;

/// Erased byte stream returned by streaming download endpoints. Same
/// shape as `AccountStream<Bytes>` in the engine plan so blob
/// download paths compose naturally.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send + 'static>>;

/// Byte-range selector for partial downloads. Maps onto an HTTP
/// `Range: bytes=start-end` header at the transport level.
#[derive(Debug, Clone, Copy)]
pub struct ByteRange {
    /// Inclusive start offset.
    pub start: u64,
    /// Length in bytes. `None` means "from start to EOF".
    pub length: Option<u64>,
}

/// Fluent request builder. Consumes `self` on every setter so the
/// final `send` call is a single move.
pub struct RequestBuilder {
    /// Method + URL + headers + body, captured opaquely. The v1
    /// skeleton does not construct a backing reqwest request; that
    /// lands in Phase 2.
    inner: RequestBuilderInner,
}

/// Backing state for the builder. Kept private so the Phase 2
/// implementation can replace the representation without touching
/// the call sites.
#[allow(dead_code)]
struct RequestBuilderInner {
    /// HTTP method.
    method: reqwest::Method,
    /// Target URL.
    url: String,
    /// Caller-provided headers. Merged with transport-injected
    /// `Authorization` and `traceparent` at send time.
    headers: HeaderMap,
    /// Body bytes, if any.
    body: Option<Bytes>,
    /// Optional per-request cost override; otherwise the host's
    /// default cost is used.
    cost: Option<u32>,
    /// Optional per-request retry policy override.
    retry: Option<RetryPolicy>,
    /// Optional per-request timeout override.
    timeout: Option<Duration>,
}

impl RequestBuilder {
    /// Construct a builder from a method and URL. The
    /// account-scoped wrappers (`AccountNet::get` etc.) call this.
    pub(crate) fn new(method: reqwest::Method, url: &str) -> Self {
        Self {
            inner: RequestBuilderInner {
                method,
                url: url.to_owned(),
                headers: HeaderMap::new(),
                body: None,
                cost: None,
                retry: None,
                timeout: None,
            },
        }
    }

    /// Set a header. Multiple calls with the same key append.
    #[must_use]
    pub fn header(self, _key: &str, _value: &str) -> Self {
        // Header insertion is delegated to Phase 2; the skeleton
        // accepts and discards so call sites compile.
        self
    }

    /// Set the request body to a JSON-serialized value. Reqwest
    /// serializes with `serde_json` under the hood.
    #[must_use]
    pub fn json<B: Serialize + ?Sized>(self, _body: &B) -> Self {
        // Phase 2 wires this to `reqwest::RequestBuilder::json`.
        self
    }

    /// Set the request body to raw bytes.
    #[must_use]
    pub fn body(mut self, body: Bytes) -> Self {
        self.inner.body = Some(body);
        self
    }

    /// Override the per-request cost for the rate-limit governor.
    #[must_use]
    pub fn cost(mut self, cost: u32) -> Self {
        self.inner.cost = Some(cost);
        self
    }

    /// Override the per-request retry policy.
    #[must_use]
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.inner.retry = Some(policy);
        self
    }

    /// Override the per-request timeout.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.inner.timeout = Some(timeout);
        self
    }

    /// Drive the request to completion with the configured retry
    /// budget, returning the buffered response.
    pub async fn send(self) -> Result<Response, Error> {
        unimplemented!("RequestBuilder::send is filled in by Phase 2")
    }

    /// Drive the request to completion but return the response body
    /// as a `ByteStream` rather than buffering. Used for blob
    /// download endpoints.
    pub async fn send_streaming(self) -> Result<StreamingResponse, Error> {
        unimplemented!("RequestBuilder::send_streaming is filled in by Phase 2")
    }
}

/// Buffered HTTP response.
#[non_exhaustive]
pub struct Response {
    /// HTTP status code of the final attempt.
    pub status: StatusCode,
    /// Response headers as received.
    pub headers: HeaderMap,
    /// Buffered response body.
    pub body: Bytes,
}

/// Streaming HTTP response. The body is a `ByteStream` so the caller
/// can apply backpressure and avoid buffering large attachments.
#[non_exhaustive]
pub struct StreamingResponse {
    /// HTTP status code of the final attempt.
    pub status: StatusCode,
    /// Response headers as received.
    pub headers: HeaderMap,
    /// Response body as an erased byte stream. Increments the
    /// bandwidth meter on every chunk.
    pub body: ByteStream,
}
