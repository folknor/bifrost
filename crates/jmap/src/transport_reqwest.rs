use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bifrost_net::Response;
use bifrost_net::{
    AccountId, AccountNet, AccountSpec, Net, NetConfig, Priority, RateLimit, RetryPolicy,
};
use bytes::Bytes;
use reqwest::Url;
use reqwest::header;

use crate::client::Authorization;
use crate::core::transport::{HttpTransport, SseTransport, TransportError};
use futures::Stream;

/// Default HTTP transport implementation using `bifrost-net`.
///
/// Routes through the shared HTTP pipeline for retry, metering, and
/// token handling.
pub struct ReqwestTransport {
    net: AccountNet,
    headers: header::HeaderMap,
    authorization: Authorization,
    timeout: Duration,
    trusted_hosts: Arc<HashSet<String>>,
}

impl ReqwestTransport {
    pub(crate) fn new(
        headers: header::HeaderMap,
        authorization: Authorization,
        timeout: Duration,
        accept_invalid_certs: bool,
        trusted_hosts: Arc<HashSet<String>>,
    ) -> Result<Self, TransportError> {
        let config = NetConfig {
            connect_timeout: timeout,
            dangerous_accept_invalid_certs: accept_invalid_certs,
            user_agent: concat!("bifrost-jmap/", env!("CARGO_PKG_VERSION")).to_string(),
            follow_redirects: false,
            ..NetConfig::default()
        };
        let net = Net::new(config)
            .map_err(|e| TransportError::with_source("Failed to build HTTP transport", e))?;
        let token_source: Arc<dyn bifrost_net::TokenSource> =
            Arc::new(authorization.account_token_source());
        let net = net.attach_account(
            AccountId("jmap".to_string()),
            AccountSpec {
                hosts: Vec::<RateLimit>::new(),
                token_source,
                default_retry: RetryPolicy::default(),
            },
        );

        Ok(Self {
            net,
            headers,
            authorization,
            timeout,
            trusted_hosts,
        })
    }

    pub(crate) fn set_priority(&self, priority: Priority) {
        self.net.set_priority(priority);
    }

    pub(crate) fn set_bandwidth_cap(&self, bps: Option<u64>) {
        self.net.set_bandwidth_cap(bps);
    }

    async fn send(
        &self,
        method: &str,
        url: &str,
        body: Option<Bytes>,
        content_type: Option<&str>,
    ) -> Result<Bytes, TransportError> {
        let mut current_url = url.to_string();
        let original_host = host_of(url)?;
        for redirect_count in 0..=5 {
            let include_auth = host_of(&current_url)? == original_host;
            let response = self
                .send_once(
                    method,
                    &current_url,
                    body.clone(),
                    content_type,
                    include_auth,
                )
                .await?;
            if !response.status.is_redirection() {
                return Self::handle_response(response.status, response.body);
            }

            if redirect_count == 5 {
                return Err(TransportError::new("Too many redirects."));
            }

            let location = response
                .headers
                .get(header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| {
                    TransportError::new(format!(
                        "Redirect from {current_url} omitted Location header"
                    ))
                })?;
            let next = redirect_url(&current_url, location)?;
            let next_host = next.host_str().unwrap_or("");
            if !self.trusted_hosts.contains(next_host) {
                return Err(TransportError::new(format!(
                    "Aborting redirect to unknown host '{next_host}'."
                )));
            }
            current_url = next.to_string();
        }

        Err(TransportError::new("Too many redirects."))
    }

    async fn send_once(
        &self,
        method: &str,
        url: &str,
        body: Option<Bytes>,
        content_type: Option<&str>,
        include_auth: bool,
    ) -> Result<Response, TransportError> {
        let mut request = match method {
            "GET" => self.net.get(url),
            "POST" => self.net.post(url),
            _ => {
                return Err(TransportError::new(format!(
                    "unsupported HTTP method: {method}"
                )));
            }
        };
        request = request.timeout(self.timeout);
        for (name, value) in &self.headers {
            let value = value.to_str().map_err(|e| {
                TransportError::with_source(format!("Invalid default header value for {name}"), e)
            })?;
            request = request.header(name.as_str(), value);
        }
        if !include_auth {
            request = request.without_bearer_auth();
        } else if self.authorization.uses_bearer_pipeline() {
            // AccountNet injects the bearer token from its
            // StaticTokenSource.
        } else {
            let value = self.authorization.header_value();
            request = request
                .without_bearer_auth()
                .header(header::AUTHORIZATION.as_str(), &value);
        }
        if let Some(ct) = content_type {
            request = request.header(header::CONTENT_TYPE.as_str(), ct);
        }
        if let Some(body) = body {
            request = request.body(body);
        }
        request.send().await.map_err(transport_error_from_net)
    }

    fn handle_response(
        status: reqwest::StatusCode,
        body: bytes::Bytes,
    ) -> Result<bytes::Bytes, TransportError> {
        if status.is_success() {
            Ok(body)
        } else {
            // Return the full body so the caller can parse ProblemDetails
            Err(TransportError::with_body(format!("HTTP {status}"), body))
        }
    }
}

impl HttpTransport for ReqwestTransport {
    async fn api_request(&self, url: &str, body: Vec<u8>) -> Result<bytes::Bytes, TransportError> {
        self.send(
            "POST",
            url,
            Some(Bytes::from(body)),
            Some("application/json"),
        )
        .await
    }

    async fn upload(
        &self,
        url: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<bytes::Bytes, TransportError> {
        self.send("POST", url, Some(Bytes::from(body)), content_type)
            .await
    }

    async fn download(&self, url: &str) -> Result<bytes::Bytes, TransportError> {
        self.send("GET", url, None, None).await
    }

    async fn get_session(&self, url: &str) -> Result<bytes::Bytes, TransportError> {
        self.send("GET", url, None, None).await
    }
}

impl SseTransport for ReqwestTransport {
    type ByteStream = ReqwestByteStream;

    async fn open_sse(
        &self,
        url: &str,
        last_event_id: Option<&str>,
    ) -> Result<Self::ByteStream, TransportError> {
        let mut current_url = url.to_string();
        let original_host = host_of(url)?;
        for redirect_count in 0..=5 {
            let include_auth = host_of(&current_url)? == original_host;
            let mut request = self
                .net
                .get(&current_url)
                .header(header::ACCEPT.as_str(), "text/event-stream")
                .timeout(self.timeout);
            for (name, value) in &self.headers {
                let value = value.to_str().map_err(|e| {
                    TransportError::with_source(
                        format!("Invalid default header value for {name}"),
                        e,
                    )
                })?;
                request = request.header(name.as_str(), value);
            }
            if !include_auth {
                request = request.without_bearer_auth();
            } else if self.authorization.uses_bearer_pipeline() {
            } else {
                let value = self.authorization.header_value();
                request = request
                    .without_bearer_auth()
                    .header(header::AUTHORIZATION.as_str(), &value);
            }
            if let Some(id) = last_event_id {
                request = request.header("Last-Event-ID", id);
            }
            let response = request
                .send_streaming()
                .await
                .map_err(transport_error_from_net)?;

            if response.status().is_success() {
                return Ok(ReqwestByteStream {
                    inner: response.body,
                });
            }
            if !response.status().is_redirection() {
                return Err(TransportError::new(format!(
                    "SSE: HTTP {}",
                    response.status()
                )));
            }
            if redirect_count == 5 {
                return Err(TransportError::new("Too many redirects."));
            }
            let location = response
                .headers
                .get(header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| {
                    TransportError::new(format!(
                        "Redirect from {current_url} omitted Location header"
                    ))
                })?;
            let next = redirect_url(&current_url, location)?;
            let next_host = next.host_str().unwrap_or("");
            if !self.trusted_hosts.contains(next_host) {
                return Err(TransportError::new(format!(
                    "Aborting redirect to unknown host '{next_host}'."
                )));
            }
            current_url = next.to_string();
        }

        Err(TransportError::new("Too many redirects."))
    }
}

/// Adapter that converts reqwest's `Bytes` stream into `Vec<u8>` chunks.
pub struct ReqwestByteStream {
    inner: bifrost_net::ByteStream,
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
                    .map_err(transport_error_from_net)
            })
        })
    }
}

impl Unpin for ReqwestByteStream {}

fn transport_error_from_net(error: bifrost_net::Error) -> TransportError {
    match error {
        bifrost_net::Error::Status { code, body, .. } => {
            TransportError::with_body(format!("HTTP {code}"), body)
        }
        other => TransportError::with_source("HTTP transport failed", other),
    }
}

fn host_of(url: &str) -> Result<String, TransportError> {
    let url = Url::parse(url).map_err(|error| {
        TransportError::with_source(format!("Invalid URL for redirect policy: {url}"), error)
    })?;
    Ok(url.host_str().unwrap_or("").to_string())
}

fn redirect_url(current_url: &str, location: &str) -> Result<Url, TransportError> {
    let current = Url::parse(current_url).map_err(|error| {
        TransportError::with_source(
            format!("Invalid current URL for redirect: {current_url}"),
            error,
        )
    })?;
    current.join(location).map_err(|error| {
        TransportError::with_source(
            format!("Invalid redirect Location header: {location}"),
            error,
        )
    })
}
