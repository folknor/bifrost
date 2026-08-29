//! The shared DAV request dispatcher: credentials, the origin trust gate, the
//! redirect walk, and the generic WebDAV verbs.
//!
//! This is the security-sensitive half of the duplication. Both crates carried
//! `send_raw_request`, `auth_headers`, `admit_discovered_urls`, `is_trusted_url`
//! and `resolve_url` byte for byte - the credential gate, the downgrade refusal
//! and the redirect walk included. A divergence here is a credential leak or a
//! silent downgrade, which is the worst possible place to keep two copies.
//!
//! Wire traffic goes through `bifrost-net`, so DAV legs share the retry budget,
//! per-host rate limiting, bandwidth metering and observability with every other
//! HTTP protocol crate. Two deliberate departures from the net defaults:
//!
//! - Redirects are DISABLED on the account spec and walked here instead. The
//!   net redirect loop strips `Authorization` on any cross-origin hop and cannot
//!   restore it, and its allowlist compares hosts where this gate compares
//!   scheme, host and effective port. Walking here means every hop - same-origin
//!   or not - is re-credentialed for the origin it actually addresses, and an
//!   origin discovery never admitted is refused locally.
//! - Bearer injection is opted out with `without_bearer_auth`. The credential
//!   has to pass the origin gate before it is minted, which is a decision this
//!   crate makes and the transport cannot.

use std::sync::Arc;

use base64::Engine as _;
use bifrost_net::{
    AccountNet, AccountSpec, Error as NetError, FollowRedirects, Net, NetErrorContext, TokenSource,
    into_account_error,
};
use bifrost_types::{AccountError, AccountId, AccountOperation};
use bytes::Bytes;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Method, StatusCode, Url};

use crate::error::{DavProtocol, local_error, response_read_error, status_error, transport_error};
use crate::transport::{
    DAV_CLIENT_TIMEOUT, DavBody, DavRequest, DavResponse, origin_is_secure, settle_body, url_origin,
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

/// The account spec every DAV account attaches under.
///
/// `token_source` stays `None` and every request opts out of bearer injection:
/// the origin gate decides whether a credential may be minted at all, and the
/// transport cannot make that call. Redirects are disabled because the walk
/// below owns them.
fn dav_account_spec() -> AccountSpec {
    let mut spec = AccountSpec::new(None);
    spec.request_timeout = Some(DAV_CLIENT_TIMEOUT);
    spec.follow_redirects = FollowRedirects::Disabled;
    spec
}

/// A DAV client's transport half: the account handle, credentials, and the set
/// of origins those credentials may reach.
#[derive(Clone)]
pub struct DavDispatch {
    net: AccountNet,
    base_url: String,
    credentials: DavCredentials,
    trusted_origins: Vec<String>,
    protocol: DavProtocol,
}

impl DavDispatch {
    /// Attach a DAV account to the process-wide shared transport.
    ///
    /// The shared `Net` is the same one the other HTTP protocol crates use, so a
    /// DAV leg joins the existing per-host rate-limit buckets and connection
    /// pool rather than opening a private client beside them.
    #[must_use]
    pub fn new(
        account_id: AccountId,
        base_url: &str,
        credentials: DavCredentials,
        protocol: DavProtocol,
    ) -> Self {
        let net = Net::shared_default().attach_account(account_id, dav_account_spec());
        Self::around(net, base_url, credentials, protocol)
    }

    /// Build over a caller-supplied `AccountNet`.
    ///
    /// Two callers: an account composed into an IMAP-shaped account, which hands
    /// down the handle carrying that account's bandwidth meter and priority so
    /// the DAV legs are metered and capped with the rest of it; and tests, which
    /// hand down a handle attached to a scripted wire dispatcher.
    #[must_use]
    pub fn with_account_net(
        net: AccountNet,
        base_url: &str,
        credentials: DavCredentials,
        protocol: DavProtocol,
    ) -> Self {
        Self::around(net, base_url, credentials, protocol)
    }

    fn around(
        net: AccountNet,
        base_url: &str,
        credentials: DavCredentials,
        protocol: DavProtocol,
    ) -> Self {
        let base_url = base_url.trim_end_matches('/').to_string();
        let trusted_origins = url_origin(&base_url).into_iter().collect();
        Self {
            net,
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

    /// The account handle, for the priority and bandwidth-cap doors the
    /// `Account` impls forward onto it.
    #[must_use]
    pub fn net(&self) -> &AccountNet {
        &self.net
    }

    /// Start a request the caller will finish with its own headers and body.
    #[must_use]
    pub fn request(&self, method: Method, url: &str) -> DavRequest {
        DavRequest::new(method, url)
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

    /// One wire attempt through `bifrost-net`, with its status-bearing failures
    /// turned back into responses.
    ///
    /// DAV reads statuses as data, not only as failure: `sync-collection` reads
    /// 403 and 410 as cursor invalidation, `MOVE` reads 405 and 501 as "no MOVE
    /// here, fall back to copy-then-delete", and a conditional PUT reads 412.
    /// The net retry loop turns every 4xx and 5xx into `Err` before a response
    /// surfaces, so a terminal `Error::Status` is reconstituted here rather than
    /// classified. Everything else - an exhausted retry budget, a rate limit
    /// past its budget, a transport failure - is left to
    /// `bifrost_net::into_account_error`, which classifies with the retry hints
    /// and transmission evidence the DAV status ladder does not carry.
    ///
    /// One narrowing worth knowing: a reconstituted body is capped at
    /// `bifrost_net::STATUS_BODY_CAP`, where the previous DAV transport buffered
    /// error bodies to the full 64 MiB ceiling. Every status DAV reads as data
    /// carries a short precondition document, so the cap does not bite; a
    /// diagnostic body longer than the cap now arrives truncated.
    async fn dispatch_once(
        &self,
        request: &DavRequest,
        operation: AccountOperation,
    ) -> Result<DavResponse, AccountError> {
        let mut builder = self
            .net
            .request(request.method.clone(), &request.url)
            .without_bearer_auth();
        for (name, value) in &request.headers {
            if let Ok(value) = value.to_str() {
                builder = builder.header(name.as_str(), value);
            }
        }
        if let Some(idempotent) = request.idempotent {
            builder = builder.idempotent(idempotent);
        }
        if let Some(body) = &request.body {
            builder = builder.body(body.clone());
        }
        match builder.send().await {
            Ok(response) => Ok(DavResponse {
                status: response.status,
                headers: response.headers,
                body: decode_body(&response.body),
                url: request.url.clone(),
            }),
            Err(NetError::Status {
                code,
                body,
                headers,
            }) => Ok(DavResponse {
                status: code,
                headers,
                body: decode_body(&body),
                url: request.url.clone(),
            }),
            // A retry budget spent against a status-bearing response, or a 429
            // past its budget, still ends in a response the server sent. The
            // DAV ladder classifies it exactly as it did before this crate had
            // any retry at all - the difference is only that the server got
            // asked more than once first, which is the capability dav-B9 was
            // about. A budget exhausted with no final response (every attempt
            // failed below the status line) has nothing to classify and falls
            // through to the transport mapping.
            Err(
                NetError::RetryBudgetExhausted {
                    final_response: Some(final_response),
                    ..
                }
                | NetError::RateLimited { final_response, .. },
            ) => Ok(DavResponse {
                status: final_response.status,
                headers: final_response.headers,
                body: decode_body(&final_response.body),
                url: request.url.clone(),
            }),
            // A body past the buffered ceiling is acknowledged but unreadable:
            // the request reached the server and may have taken effect, so a
            // non-idempotent mutation must reconcile rather than replay.
            Err(NetError::ResponseTooLarge { limit }) => Err(response_read_error(
                operation,
                format!("DAV response body exceeded the buffered ceiling ({limit} bytes)"),
                self.protocol,
            )),
            Err(error) => Err(into_account_error(
                error,
                NetErrorContext {
                    provider: None,
                    protocol: self.protocol.protocol(),
                    operation,
                    scope: None,
                },
            )),
        }
    }

    /// Send a request, following every redirect hop by hand.
    ///
    /// Redirects are disabled in the transport, so both same-origin and
    /// cross-origin hops arrive here. Each hop re-mints `auth_headers` for the
    /// origin it is about to address, which refuses locally when that origin was
    /// never admitted by discovery. Doing this for same-origin hops too costs
    /// nothing - the credential is the same one - and removes the previous
    /// split, where reqwest followed same-origin hops internally and only
    /// cross-origin hops were walked.
    pub async fn send_raw_request(
        &self,
        request: DavRequest,
        operation: AccountOperation,
    ) -> Result<DavResponse, AccountError> {
        let max_hops = usize::from(bifrost_net::RedirectPolicy::default().max_hops);
        let mut request = request;
        let mut hops = 0usize;
        loop {
            let response = self.dispatch_once(&request, operation).await?;
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
            // Fresh credentials for the target origin; refused locally when
            // the origin was never admitted by discovery.
            let auth = self.auth_headers(next.as_str(), operation).await?;
            let mut headers = request.headers.clone();
            headers.remove(AUTHORIZATION);
            for (name, value) in &auth {
                headers.insert(name, value.clone());
            }
            request = DavRequest {
                method: request.method,
                url: next.to_string(),
                headers,
                body: request.body,
                idempotent: request.idempotent,
            };
        }
    }

    pub async fn send_body_request(
        &self,
        request: DavRequest,
        operation: AccountOperation,
    ) -> Result<DavBody, AccountError> {
        let response = self.send_raw_request(request, operation).await?;
        settle_body(response, operation, self.protocol)
    }

    pub async fn send_status_request(
        &self,
        request: DavRequest,
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
        let request = DavRequest::new(method, url)
            .header(CONTENT_TYPE, "application/xml; charset=utf-8")
            .header("Depth", depth)
            .headers(self.auth_headers(url, operation).await?)
            // A read: replaying after a dropped connection cannot change state.
            .idempotent(true)
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
        let request = DavRequest::new(method, url)
            .header(CONTENT_TYPE, "application/xml; charset=utf-8")
            .header("Depth", depth)
            .headers(self.auth_headers(url, operation).await?)
            .idempotent(true)
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
        let request =
            DavRequest::new(Method::DELETE, url).headers(self.auth_headers(url, operation).await?);
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
        let request = DavRequest::new(method, from)
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

/// Decode a DAV response body.
///
/// RFC 4918 bodies are XML, whose declared default encoding is UTF-8. Lossy
/// rather than strict keeps a malformed byte behaving as it always has - a
/// replacement character, not a failed request.
fn decode_body(body: &Bytes) -> String {
    String::from_utf8_lossy(body).into_owned()
}

impl std::fmt::Debug for DavDispatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DavDispatch")
            .field("base_url", &self.base_url)
            .field("protocol", &self.protocol)
            .finish_non_exhaustive()
    }
}
