use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bifrost_net::{
    AccountId, AccountNet, AccountSpec, FollowRedirects, Net, NetConfig, Priority, RateLimit,
    RedirectPolicy, RetryPolicy,
};
use bytes::Bytes;
use reqwest::header;

use crate::client::Authorization;
use crate::core::transport::{HttpTransport, SseTransport, TransportError};
use futures::Stream;

/// Default HTTP transport implementation using `bifrost-net`.
///
/// Routes through the shared HTTP pipeline for retry, metering, and
/// token handling.
pub(crate) struct ReqwestTransport {
    net: AccountNet,
    headers: header::HeaderMap,
    authorization: Authorization,
    timeout: Duration,
}

impl ReqwestTransport {
    pub(crate) fn new(
        headers: header::HeaderMap,
        authorization: Authorization,
        account_id: AccountId,
        timeout: Duration,
        accept_invalid_certs: bool,
        trusted_hosts: Arc<HashSet<String>>,
    ) -> Result<Self, TransportError> {
        let redirect_policy = redirect_policy_from_trusted_hosts(&trusted_hosts);
        let config = NetConfig {
            connect_timeout: timeout,
            dangerous_accept_invalid_certs: accept_invalid_certs,
            user_agent: concat!("bifrost-jmap/", env!("CARGO_PKG_VERSION")).to_string(),
            follow_redirects: FollowRedirects::Enabled(redirect_policy),
            ..NetConfig::default()
        };
        let net = Net::new(config)
            .map_err(|e| TransportError::with_source("Failed to build HTTP transport", e))?;
        let token_source = authorization.account_token_source();
        let net = net.attach_account(
            account_id,
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
        let response = self.send_once(method, url, body, content_type).await?;
        Self::handle_response(response.status, response.body)
    }

    async fn send_once(
        &self,
        method: &str,
        url: &str,
        body: Option<Bytes>,
        content_type: Option<&str>,
    ) -> Result<bifrost_net::Response, TransportError> {
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
        if self.authorization.uses_bearer_pipeline() {
            // AccountNet injects the bearer token from the shared
            // token source on every request attempt.
        } else {
            let value = self
                .authorization
                .header_value()
                .await
                .map_err(TransportError::from_net)?;
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
            // Only a passed-through 3xx reaches here: bifrost-net turns
            // every 4xx/5xx into a typed `Error` before `send` returns,
            // so `Ok(response)` with a non-2xx status means a 304 / 305 /
            // 306 or a `Location`-less redirect. On the production stack
            // `body` is always empty - the redirect loop's `PassThrough`
            // arm replaces the body with an empty stream
            // (`crates/net/src/request.rs`) - so the status is the whole
            // evidence; the body is threaded through for shape, not
            // because a problem document can arrive on it.
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
        let mut request = self
            .net
            .get(url)
            .header(header::ACCEPT.as_str(), "text/event-stream")
            .timeout(self.timeout);
        for (name, value) in &self.headers {
            let value = value.to_str().map_err(|e| {
                TransportError::with_source(format!("Invalid default header value for {name}"), e)
            })?;
            request = request.header(name.as_str(), value);
        }
        if self.authorization.uses_bearer_pipeline() {
        } else {
            let value = self
                .authorization
                .header_value()
                .await
                .map_err(TransportError::from_net)?;
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

        // `send_streaming` already turns every 4xx/5xx into a typed
        // bifrost-net error, so the only non-2xx response it can hand
        // us is a passed-through redirect (304/305/306, or a
        // `Location`-less 3xx). Preserve its status and headers through
        // the same typed error boundary instead of discarding them
        // behind a generic network error. The body is knowably empty:
        // `ReqwestTransport` always rides bifrost-net's redirect loop,
        // whose `PassThrough` arm deliberately replaces the body with
        // an empty stream (`crates/net/src/request.rs`), so there is no
        // problem-details document to capture here and this path does
        // not pretend otherwise.
        Err(TransportError::from_net(bifrost_net::Error::Status {
            code: response.status,
            body: Bytes::new(),
            headers: response.headers,
        }))
    }
}

/// Adapter that converts reqwest's `Bytes` stream into `Vec<u8>` chunks.
///
/// Constructed only by the `SseTransport::open_sse` impl, which the
/// Account impl does not currently drive; kept so the trait surface
/// stays complete.
#[allow(dead_code)]
pub(crate) struct ReqwestByteStream {
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
    // Preserve the original net error so the JMAP conversion boundary
    // can delegate to bifrost_net::into_account_error for pure
    // transport-level signals. `TransportError::from_net` also lifts the
    // response body out of whichever variant carries it (`Status`
    // directly, `RateLimited` / `RetryBudgetExhausted` from the
    // preserved final response) so ProblemDetails parsing still works
    // for the statuses the retry loop consumed.
    TransportError::from_net(error)
}

fn redirect_policy_from_trusted_hosts(trusted_hosts: &HashSet<String>) -> RedirectPolicy {
    let mut policy = RedirectPolicy::with_hops(5);
    if trusted_hosts.is_empty() {
        policy = policy.trust_host("__bifrost_jmap_no_cross_host_redirects__");
    } else {
        for host in trusted_hosts {
            policy = policy.trust_host(host);
        }
    }
    policy
}
