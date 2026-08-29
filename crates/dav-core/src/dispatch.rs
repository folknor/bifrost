//! The shared DAV request dispatcher: credentials, the origin trust gate, the
//! manual cross-origin redirect walk, and the generic WebDAV verbs.
//!
//! This is the security-sensitive half of the duplication. Both crates carried
//! `send_raw_request`, `auth_headers`, `admit_discovered_urls`, `is_trusted_url`
//! and `resolve_url` byte for byte - the credential gate, the downgrade refusal
//! and the redirect walk included. A divergence here is a credential leak or a
//! silent downgrade, which is the worst possible place to keep two copies.

use std::sync::Arc;

use base64::Engine as _;
use bifrost_net::TokenSource;
use bifrost_types::{AccountError, AccountOperation};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Method, StatusCode, Url};

use crate::error::{DavProtocol, local_error, status_error, transport_error};
use crate::transport::{
    DAV_CLIENT_TIMEOUT, DavBody, DavResponse, DavTransport, ReqwestDavTransport,
    dav_redirect_policy, origin_is_secure, settle_body, transport_failure, url_origin,
};

/// Credentials as the dispatcher needs them.
///
/// The published `CalDavCredentials` and `CardDavCredentials` stay exactly where
/// they are and convert into this; it exists so one dispatcher can serve both
/// without either public enum moving.
#[derive(Clone)]
pub enum DavCredentials {
    Basic { username: String, password: String },
    Bearer { token_source: Arc<dyn TokenSource> },
}

/// A DAV client's transport half: HTTP client, seam, credentials, and the set
/// of origins those credentials may reach.
#[derive(Clone)]
pub struct DavDispatch {
    http: reqwest::Client,
    transport: Arc<dyn DavTransport>,
    base_url: String,
    credentials: DavCredentials,
    trusted_origins: Vec<String>,
    protocol: DavProtocol,
}

impl DavDispatch {
    pub fn new(
        base_url: &str,
        credentials: DavCredentials,
        protocol: DavProtocol,
    ) -> Result<Self, AccountError> {
        let http = reqwest::Client::builder()
            .redirect(dav_redirect_policy())
            .timeout(DAV_CLIENT_TIMEOUT)
            .build()
            .map_err(|error| {
                local_error(AccountOperation::Discover, error.to_string(), protocol)
            })?;
        Ok(Self::around(
            http,
            Arc::new(ReqwestDavTransport),
            base_url,
            credentials,
            protocol,
        ))
    }

    /// A dispatcher over a scripted transport, for tests.
    ///
    /// Takes credentials rather than defaulting them: the transcript tests
    /// assert on the exact `Authorization` header that reaches each origin, so
    /// what the caller configures has to be what goes out.
    pub fn with_transport(
        base_url: &str,
        transport: Arc<dyn DavTransport>,
        credentials: DavCredentials,
        protocol: DavProtocol,
    ) -> Self {
        Self::around(
            reqwest::Client::new(),
            transport,
            base_url,
            credentials,
            protocol,
        )
    }

    fn around(
        http: reqwest::Client,
        transport: Arc<dyn DavTransport>,
        base_url: &str,
        credentials: DavCredentials,
        protocol: DavProtocol,
    ) -> Self {
        let base_url = base_url.trim_end_matches('/').to_string();
        let trusted_origins = url_origin(&base_url).into_iter().collect();
        Self {
            http,
            transport,
            base_url,
            credentials,
            trusted_origins,
            protocol,
        }
    }

    #[must_use]
    pub fn protocol(&self) -> DavProtocol {
        self.protocol
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Start a request the caller will finish with its own headers and body.
    pub fn request(&self, method: Method, url: &str) -> reqwest::RequestBuilder {
        self.http.request(method, url)
    }

    #[must_use]
    pub fn resolve_url(&self, href: &str) -> String {
        if href.starts_with("http://") || href.starts_with("https://") {
            return href.to_string();
        }
        if let Ok(base) = Url::parse(&self.base_url)
            && let Ok(resolved) = base.join(href)
        {
            return resolved.to_string();
        }
        if self.base_url.ends_with('/') || href.starts_with('/') {
            format!("{}{href}", self.base_url)
        } else {
            format!("{}/{href}", self.base_url)
        }
    }

    /// Admit successfully discovered DAV origins to the credential gate.
    ///
    /// Discovery is server-steered: the collection home (and, for CalDAV, the
    /// scheduling outbox) comes out of the principal's own PROPFIND response,
    /// so a compromised or hostile server picks these origins. Two rules bound
    /// what it can pick. A discovered origin must parse, and it must never
    /// weaken the transport guarantee the configured base URL already
    /// established - an HTTPS-configured account never trusts a plaintext
    /// discovered home, because that would turn discovery into a downgrade
    /// channel for the account credential. A cross-origin HTTPS home is still
    /// admitted; that is a real deployment shape, where the principal and the
    /// home live on different hosts of the same service. Admission happens only
    /// after the complete authenticated discovery result is available, before
    /// the account is shared or a home request can be in flight.
    pub fn admit_discovered_urls(&mut self, urls: impl IntoIterator<Item = String>) {
        for url in urls {
            let Some(origin) = url_origin(&url) else {
                continue;
            };
            if origin_is_secure(&self.base_url) && !origin_is_secure(&url) {
                continue;
            }
            if !self.trusted_origins.contains(&origin) {
                self.trusted_origins.push(origin);
            }
        }
    }

    #[must_use]
    pub fn is_trusted_url(&self, url: &str) -> bool {
        url_origin(url).is_some_and(|origin| self.trusted_origins.contains(&origin))
    }

    /// Build the per-request auth headers.
    ///
    /// The bearer token is read from the shared source on every call, so a
    /// token rotated mid-sync is honored on the next DAV request without
    /// reopening the account.
    pub async fn auth_headers(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<HeaderMap, AccountError> {
        if !self.is_trusted_url(url) {
            return Err(local_error(
                operation,
                format!("refusing to send DAV credentials to untrusted URL: {url}"),
                self.protocol,
            ));
        }
        let mut headers = HeaderMap::new();
        match &self.credentials {
            DavCredentials::Basic { username, password } => {
                let credentials = base64::engine::general_purpose::STANDARD
                    .encode(format!("{username}:{password}"));
                if let Ok(value) = HeaderValue::from_str(&format!("Basic {credentials}")) {
                    headers.insert(AUTHORIZATION, value);
                }
            }
            DavCredentials::Bearer { token_source } => {
                let token = token_source.current().await.map_err(|error| {
                    transport_error(
                        operation,
                        format!("failed to read OAuth access token: {error}"),
                        self.protocol,
                    )
                })?;
                if let Ok(value) = HeaderValue::from_str(&format!("Bearer {}", token.as_str())) {
                    headers.insert(AUTHORIZATION, value);
                }
            }
        }
        Ok(headers)
    }

    /// Send a request, following cross-origin redirects by hand.
    ///
    /// Same-origin hops are followed inside reqwest, which preserves the
    /// `Authorization` header when scheme, host, and effective port are all
    /// unchanged. A cross-origin hop cannot ride that path: reqwest strips
    /// `Authorization` on any origin change and its redirect policy has no way
    /// to restore it, so a followed hop would reach the destination
    /// unauthenticated. Cross-origin 3xx responses are therefore stopped by the
    /// policy and re-dispatched here with fresh `auth_headers` for the new
    /// origin, which refuses locally when that origin was never admitted.
    pub async fn send_raw_request(
        &self,
        request: reqwest::RequestBuilder,
        operation: AccountOperation,
    ) -> Result<DavResponse, AccountError> {
        let max_hops = usize::from(bifrost_net::RedirectPolicy::default().max_hops);
        let mut request = request;
        let mut hops = 0usize;
        loop {
            let replay = request.try_clone();
            let response = self
                .transport
                .send(request)
                .await
                .map_err(|error| transport_failure(operation, error, self.protocol))?;
            let redirect = matches!(
                response.status,
                StatusCode::MOVED_PERMANENTLY
                    | StatusCode::FOUND
                    | StatusCode::TEMPORARY_REDIRECT
                    | StatusCode::PERMANENT_REDIRECT
            );
            let location = response
                .headers
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok());
            let Some(location) = location.filter(|_| redirect) else {
                if !self.is_trusted_url(&response.url) {
                    return Err(local_error(
                        operation,
                        format!(
                            "response arrived from an untrusted DAV origin: {}",
                            response.url
                        ),
                        self.protocol,
                    ));
                }
                return Ok(response);
            };
            let next = Url::parse(&response.url)
                .and_then(|base| base.join(location))
                .map_err(|error| {
                    local_error(
                        operation,
                        format!("unresolvable redirect target: {error}"),
                        self.protocol,
                    )
                })?;
            hops += 1;
            if hops > max_hops {
                return Err(local_error(operation, "too many redirects", self.protocol));
            }
            let Some(replay) = replay else {
                return Err(local_error(
                    operation,
                    "redirected DAV request cannot be replayed",
                    self.protocol,
                ));
            };
            let previous = replay
                .build()
                .map_err(|error| local_error(operation, error.to_string(), self.protocol))?;
            // Fresh credentials for the target origin; refused locally when
            // the origin was never admitted by discovery.
            let auth = self.auth_headers(next.as_str(), operation).await?;
            let mut headers = previous.headers().clone();
            headers.remove(AUTHORIZATION);
            for (name, value) in &auth {
                headers.insert(name, value.clone());
            }
            let mut rebuilt = self
                .http
                .request(previous.method().clone(), next)
                .headers(headers);
            if let Some(body) = previous.body().and_then(reqwest::Body::as_bytes) {
                rebuilt = rebuilt.body(body.to_vec());
            }
            request = rebuilt;
        }
    }

    pub async fn send_body_request(
        &self,
        request: reqwest::RequestBuilder,
        operation: AccountOperation,
    ) -> Result<DavBody, AccountError> {
        let response = self.send_raw_request(request, operation).await?;
        settle_body(response, operation, self.protocol)
    }

    pub async fn send_status_request(
        &self,
        request: reqwest::RequestBuilder,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        let response = self.send_raw_request(request, operation).await?;
        if response.status.is_success() {
            Ok(())
        } else {
            Err(status_error(
                operation,
                response.status,
                response.body,
                self.protocol,
            ))
        }
    }

    pub async fn propfind_raw(
        &self,
        url: &str,
        depth: &str,
        body: &str,
        operation: AccountOperation,
    ) -> Result<DavBody, AccountError> {
        let method = Method::from_bytes(b"PROPFIND")
            .map_err(|error| local_error(operation, error.to_string(), self.protocol))?;
        let request = self
            .http
            .request(method, url)
            .header(CONTENT_TYPE, "application/xml; charset=utf-8")
            .header("Depth", depth)
            .headers(self.auth_headers(url, operation).await?)
            .body(body.to_string());
        self.send_body_request(request, operation).await
    }

    /// A `REPORT` returning the raw response, so a caller that must inspect a
    /// non-2xx status before it becomes an error can do so. CalDAV's
    /// `sync-collection` is the only such caller: it reads 403
    /// `valid-sync-token` and 410 as cursor invalidation rather than failure.
    pub async fn report_raw_response(
        &self,
        url: &str,
        depth: &str,
        body: &str,
        operation: AccountOperation,
    ) -> Result<DavResponse, AccountError> {
        let method = Method::from_bytes(b"REPORT")
            .map_err(|error| local_error(operation, error.to_string(), self.protocol))?;
        let request = self
            .http
            .request(method, url)
            .header(CONTENT_TYPE, "application/xml; charset=utf-8")
            .header("Depth", depth)
            .headers(self.auth_headers(url, operation).await?)
            .body(body.to_string());
        self.send_raw_request(request, operation).await
    }

    pub async fn report_raw(
        &self,
        url: &str,
        depth: &str,
        body: &str,
        operation: AccountOperation,
    ) -> Result<DavBody, AccountError> {
        let response = self
            .report_raw_response(url, depth, body, operation)
            .await?;
        settle_body(response, operation, self.protocol)
    }

    pub async fn delete_resource(
        &self,
        url: &str,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        let request = self
            .request(Method::DELETE, url)
            .headers(self.auth_headers(url, operation).await?);
        self.send_status_request(request, operation).await
    }

    /// WebDAV `MOVE` of one resource into another collection.
    ///
    /// `Overwrite: F` so a name collision at the destination is refused rather
    /// than silently destroying whatever already sits there. `Destination` must
    /// be an absolute URI (RFC 4918 s10.3).
    ///
    /// The destination is gated by the same admitted-origin set as the source:
    /// it is a URL this client asks the server to write to, and a
    /// consumer-supplied collection id must not be able to steer it anywhere
    /// the credential gate would refuse.
    ///
    /// `Ok(false)` means the server does not implement MOVE, so the caller can
    /// fall back to copy-then-delete. Every other non-2xx is a real error - in
    /// particular 412 (the destination is occupied) and 502 (the server refuses
    /// the destination) are failures, not fallback triggers.
    pub async fn move_resource(
        &self,
        from: &str,
        to: &str,
        operation: AccountOperation,
    ) -> Result<bool, AccountError> {
        if !self.is_trusted_url(to) {
            return Err(local_error(
                operation,
                format!("refusing to name an untrusted DAV move destination: {to}"),
                self.protocol,
            ));
        }
        let method = Method::from_bytes(b"MOVE")
            .map_err(|error| local_error(operation, error.to_string(), self.protocol))?;
        let destination = HeaderValue::from_str(to)
            .map_err(|error| local_error(operation, error.to_string(), self.protocol))?;
        let request = self
            .request(method, from)
            .header("Destination", destination)
            .header("Overwrite", "F")
            .headers(self.auth_headers(from, operation).await?);
        let response = self.send_raw_request(request, operation).await?;
        if response.status.is_success() {
            return Ok(true);
        }
        if matches!(
            response.status,
            StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_IMPLEMENTED
        ) {
            return Ok(false);
        }
        Err(status_error(
            operation,
            response.status,
            response.body,
            self.protocol,
        ))
    }
}

impl std::fmt::Debug for DavDispatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DavDispatch")
            .field("base_url", &self.base_url)
            .field("protocol", &self.protocol)
            .finish_non_exhaustive()
    }
}
