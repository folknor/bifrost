use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use reqwest::header;
use reqwest::redirect;
use reqwest::Client as HttpClient;

use crate::core::transport::{HttpTransport, SseTransport, TransportError};
use futures_util::Stream;

/// Default HTTP transport implementation using `reqwest`.
///
/// Reuses a single `reqwest::Client` for connection pooling, DNS cache,
/// and TLS session reuse.
#[allow(dead_code)]
pub struct ReqwestTransport {
    client: HttpClient,
    #[cfg(feature = "websockets")]
    headers: header::HeaderMap,
    #[cfg(feature = "websockets")]
    trusted_hosts: Arc<HashSet<String>>,
}

impl ReqwestTransport {
    pub fn new(
        headers: header::HeaderMap,
        timeout: Duration,
        accept_invalid_certs: bool,
        trusted_hosts: Arc<HashSet<String>>,
    ) -> Result<Self, TransportError> {
        let trusted_hosts_ = Arc::clone(&trusted_hosts);
        let client = HttpClient::builder()
            .redirect(redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() > 5 {
                    attempt.error("Too many redirects.")
                } else if matches!(attempt.url().host_str(), Some(host) if trusted_hosts_.contains(host))
                {
                    attempt.follow()
                } else {
                    let msg = format!(
                        "Aborting redirect to unknown host '{}'.",
                        attempt.url().host_str().unwrap_or("")
                    );
                    attempt.error(msg)
                }
            }))
            .danger_accept_invalid_certs(accept_invalid_certs)
            .timeout(timeout)
            .default_headers(headers.clone())
            .build()
            .map_err(|e| TransportError::with_source("Failed to build HTTP client", e))?;

        Ok(Self {
            client,
            #[cfg(feature = "websockets")]
            headers,
            #[cfg(feature = "websockets")]
            trusted_hosts,
        })
    }

    /// (EventSource, WebSocket) that can't use the HttpTransport trait.
    #[cfg(feature = "websockets")]
    #[allow(dead_code)]
    pub(crate) fn headers(&self) -> &header::HeaderMap {
        &self.headers
    }

    #[cfg(feature = "websockets")]
        #[allow(dead_code)]
    pub(crate) fn trusted_hosts(&self) -> &Arc<HashSet<String>> {
        &self.trusted_hosts
    }

    async fn handle_response(response: reqwest::Response) -> Result<bytes::Bytes, TransportError> {
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|e| TransportError::with_source("Failed to read response body", e))?;

        if status.is_success() {
            Ok(body)
        } else {
            // Return the full body so the caller can parse ProblemDetails
            Err(TransportError::with_body(
                format!("HTTP {status}"),
                body,
            ))
        }
    }
}

impl HttpTransport for ReqwestTransport {
    async fn api_request(
        &self,
        url: &str,
        body: Vec<u8>,
    ) -> Result<bytes::Bytes, TransportError> {
        let response = self
            .client
            .post(url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| TransportError::with_source("API request failed", e))?;
        Self::handle_response(response).await
    }

    async fn upload(
        &self,
        url: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<bytes::Bytes, TransportError> {
        let mut req = self.client.post(url);
        if let Some(ct) = content_type {
            req = req.header(header::CONTENT_TYPE, ct);
        }
        let response = req
            .body(body)
            .send()
            .await
            .map_err(|e| TransportError::with_source("Upload failed", e))?;
        Self::handle_response(response).await
    }

    async fn download(&self, url: &str) -> Result<bytes::Bytes, TransportError> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| TransportError::with_source("Download failed", e))?;
        Self::handle_response(response).await
    }

    async fn get_session(&self, url: &str) -> Result<bytes::Bytes, TransportError> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| TransportError::with_source("Session fetch failed", e))?;
        Self::handle_response(response).await
    }
}

impl SseTransport for ReqwestTransport {
    type ByteStream = ReqwestByteStream;

    async fn open_sse(
        &self,
        url: &str,
        last_event_id: Option<&str>,
    ) -> Result<Self::ByteStream, TransportError> {
        let mut request = self.client.get(url).header(header::ACCEPT, "text/event-stream");
        if let Some(id) = last_event_id {
            request = request.header("Last-Event-ID", id);
        }
        let response = request.send()
            .await
            .map_err(|e| TransportError::with_source("SSE connection failed", e))?;

        if !response.status().is_success() {
            return Err(TransportError::new(format!(
                "SSE: HTTP {}",
                response.status()
            )));
        }

        Ok(ReqwestByteStream {
            inner: Box::pin(response.bytes_stream()),
        })
    }
}

/// Adapter that converts reqwest's `Bytes` stream into `Vec<u8>` chunks.
pub struct ReqwestByteStream {
    inner: std::pin::Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>,
}

impl Stream for ReqwestByteStream {
    type Item = Result<Vec<u8>, TransportError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx).map(|opt| {
            opt.map(|result| {
                result
                    .map(|bytes| bytes.to_vec())
                    .map_err(|e| TransportError::with_source("SSE stream error", e))
            })
        })
    }
}

impl Unpin for ReqwestByteStream {}
