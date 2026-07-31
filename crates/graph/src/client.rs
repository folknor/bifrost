#[cfg(test)]
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use bifrost_net::{
    AccountId, AccountNet, AccountSpec, Net, RateLimit, RetryPolicy, StaticTokenSource, TokenSource,
};
use bifrost_types::TransmissionState;
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;

use crate::error::{GraphError, GraphResponseError};

pub(crate) const GRAPH_API_BASE: &str = "https://graph.microsoft.com/v1.0";

/// Production origin for the two non-Graph Microsoft surfaces this crate
/// talks to: Exchange Autodiscover (`/autodiscover/autodiscover.xml`,
/// `/autodiscover/autodiscover.svc`) and EWS (`/EWS/Exchange.asmx`). Both
/// live on `outlook.office365.com`, NOT on the Graph host, so the Graph
/// api-base override alone never redirected them.
pub(crate) const OUTLOOK_BASE: &str = "https://outlook.office365.com";

/// Production Graph host. Used only to decide whether an api-base is the
/// real Graph endpoint or a harness redirect.
const GRAPH_HOST: &str = "graph.microsoft.com";

const CONCURRENCY_LIMIT: usize = 3;

// pub: GraphAccountFactory consumers need a constructible Graph client handle.
#[derive(Clone)]
pub struct GraphClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    net: Option<Net>,
    account_net: RwLock<Option<AccountNet>>,
    api_base: String,
    /// Origin for the Autodiscover + EWS surfaces. Derived from `api_base`
    /// (see [`derive_outlook_base`]) unless a consumer overrode it with
    /// [`GraphClient::with_outlook_base`].
    outlook_base: String,
    rate_limit_host: String,
    token_source: Arc<dyn TokenSource>,
    mailbox_id: Option<String>,
    semaphore: Arc<Semaphore>,
    /// Shared with every client derived from this one
    /// (`for_shared_mailbox`, `with_outlook_base`), the way the semaphore
    /// and the `AccountNet` already are: in production those derivatives
    /// issue their requests down the same transport, so a seam they did not
    /// share would leave every foreign-mailbox path unscriptable and
    /// silently outside the recorded request list.
    #[cfg(test)]
    scripted: Arc<std::sync::Mutex<ScriptedRest>>,
}

/// Graph-owned response shape at the one REST funnel.  Keeping this small
/// adapter local lets tests script Graph responses without reaching into
/// bifrost-net's private dispatch seam.
struct RestResponse {
    status: reqwest::StatusCode,
    headers: reqwest::header::HeaderMap,
    body: Bytes,
}

impl From<bifrost_net::Response> for RestResponse {
    fn from(response: bifrost_net::Response) -> Self {
        Self {
            status: response.status(),
            headers: response.headers,
            body: response.body,
        }
    }
}

/// Request body plus the content type it travels under, at the one wire
/// funnel. JSON callers serialize up front instead of handing
/// `RequestBuilder::json` a `Serialize` value, which keeps the funnel
/// non-generic so the raw-MIME caller (`post_mime`) can share it - and
/// therefore share the test seam - rather than opening a second,
/// unscriptable path to `AccountNet`.
struct WireBody {
    content_type: &'static str,
    bytes: Bytes,
}

/// Content type every JSON call sends, and the one a bodiless call still
/// sends (unchanged from before the funnel merge).
const JSON_CONTENT_TYPE: &str = "application/json";

/// Graph's import-from-MIME content type: the body is the base64 of the
/// RFC 5322 octets, not JSON.
const MIME_CONTENT_TYPE: &str = "text/plain";

impl WireBody {
    fn json<B: Serialize + ?Sized>(body: &B) -> Result<Self, GraphError> {
        // Mirrors `RequestBuilder::json`'s failure shape so a body whose
        // `Serialize` impl fails still surfaces as `EncodeBody`.
        let bytes = serde_json::to_vec(body).map_err(|error| {
            GraphError::Net(bifrost_net::Error::EncodeBody {
                message: format!("serde_json::to_vec failed: {error}"),
                source: Some(Box::new(error)),
            })
        })?;
        Ok(Self {
            content_type: JSON_CONTENT_TYPE,
            bytes: Bytes::from(bytes),
        })
    }

    fn mime(bytes: Bytes) -> Self {
        Self {
            content_type: MIME_CONTENT_TYPE,
            bytes,
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct RestRequest {
    pub(crate) method: String,
    pub(crate) url: String,
    pub(crate) if_match: Option<String>,
    pub(crate) prefer: Option<String>,
    pub(crate) content_type: String,
    /// The JSON body as sent, when the call sent one. `None` for a
    /// bodiless call and for the raw-MIME call, whose bytes land in
    /// `raw_body` instead.
    pub(crate) body: Option<serde_json::Value>,
    /// The verbatim bytes of a non-JSON body (Graph's base64 MIME import).
    pub(crate) raw_body: Option<Bytes>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct ScriptedRestResponse {
    status: reqwest::StatusCode,
    headers: reqwest::header::HeaderMap,
    body: Bytes,
}

#[cfg(test)]
impl ScriptedRestResponse {
    pub(crate) fn json(status: reqwest::StatusCode, body: serde_json::Value) -> Self {
        Self {
            status,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).expect("JSON response serializes")),
        }
    }

    pub(crate) fn empty(status: reqwest::StatusCode) -> Self {
        Self {
            status,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::new(),
        }
    }

    pub(crate) fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.insert(
            reqwest::header::HeaderName::from_bytes(name.as_bytes()).expect("valid test header"),
            reqwest::header::HeaderValue::from_str(value).expect("valid test header value"),
        );
        self
    }

    /// Reproduce what `bifrost-net` actually hands back for this status.
    ///
    /// The retry loop NEVER returns `Ok(Response)` for a 4xx or 5xx: a 4xx
    /// outside the retry set becomes `Error::Status`, a 401 that survives a
    /// forced token refresh becomes `Error::AuthLost`, and the retryable set
    /// (429 plus the 5xx family) becomes `Error::RateLimited` /
    /// `Error::RetryBudgetExhausted` once the budget is gone. Only 2xx and a
    /// passed-through 3xx arrive as a response. A seam that handed a scripted
    /// 500 back as a successful response would put every test built on it on
    /// a code path production cannot reach, and the divergence would show up
    /// as tests passing rather than as a failure.
    ///
    /// The retry set is read off `RetryPolicy::default()` - the policy
    /// `attach_account` installs - rather than restated here, so a change to
    /// the policy cannot leave the seam behind. Backoff, the attempt count,
    /// and the client's own concurrency permit are deliberately not
    /// simulated: they change how long production takes to reach an outcome,
    /// not which outcome it reaches. A 3xx passes through as a response
    /// because that is what the transport does with one it cannot follow.
    fn into_net_outcome(self) -> Result<RestResponse, GraphError> {
        use reqwest::StatusCode;

        let Self {
            status,
            headers,
            body,
        } = self;
        if status.is_success() || status.is_redirection() {
            return Ok(RestResponse {
                status,
                headers,
                body,
            });
        }

        let policy = RetryPolicy::default();
        let retry_after = bifrost_net::parse_retry_after(headers.get(reqwest::header::RETRY_AFTER))
            .map(|hint| hint.min(policy.honor_retry_after_cap));
        let final_response = bifrost_net::FinalResponse {
            status,
            headers: headers.clone(),
            body: bifrost_net::error::cap_status_body(body.clone()),
        };

        let error = if status == StatusCode::UNAUTHORIZED {
            bifrost_net::Error::AuthLost {
                transmission_state: Some(TransmissionState::Acknowledged),
                final_response: Some(final_response),
            }
        } else if status == StatusCode::TOO_MANY_REQUESTS {
            bifrost_net::Error::RateLimited {
                retry_after,
                final_response,
            }
        } else if status.is_server_error() || policy.statuses.contains(&status) {
            bifrost_net::Error::RetryBudgetExhausted {
                final_response: Some(final_response),
                retry_after_history: retry_after.into_iter().collect(),
            }
        } else {
            bifrost_net::Error::Status {
                code: status,
                body: bifrost_net::error::cap_status_body(body),
                headers,
            }
        };
        Err(GraphError::Net(error))
    }
}

#[cfg(test)]
#[derive(Default)]
struct ScriptedRest {
    /// Set by the first `script_rest` call and never cleared. While it is
    /// set the client is a closed system: an unscripted request is a test
    /// bug, not a reason to reach for the network.
    armed: bool,
    responses: VecDeque<ScriptedRestResponse>,
    requests: Vec<RestRequest>,
}

impl GraphClient {
    // pub: ergonomic constructor for the default Microsoft Graph endpoint.
    pub fn new(access_token: impl Into<String>) -> Self {
        Self::with_api_base(GRAPH_API_BASE, access_token)
    }

    // pub: consumers may need sovereign-cloud or test Graph API endpoints before registration.
    pub fn with_api_base(api_base: impl Into<String>, access_token: impl Into<String>) -> Self {
        // Bases are trimmed once in `with_bases_and_source`.
        let token_source: Arc<dyn TokenSource> =
            Arc::new(StaticTokenSource::new(access_token, None));
        Self::with_source(api_base, token_source)
    }

    // pub: source-accepting constructor. ratatoskr hands in a shared
    // `Arc<dyn TokenSource>` (typically an `OAuthRefresher` over its own
    // refresh-token store) so a token it refreshes and persists is read
    // live at every wire authentication without reopening the client.
    pub fn with_source(api_base: impl Into<String>, source: Arc<dyn TokenSource>) -> Self {
        let api_base = trim_base(api_base.into());
        let rate_limit_host = host_from_api_base(&api_base);
        let outlook_base = derive_outlook_base(&api_base);
        Self {
            inner: Arc::new(ClientInner {
                net: Some(Net::shared_default()),
                account_net: RwLock::new(None),
                api_base,
                outlook_base,
                rate_limit_host,
                token_source: source,
                mailbox_id: None,
                semaphore: Arc::new(Semaphore::new(CONCURRENCY_LIMIT)),
                #[cfg(test)]
                scripted: Arc::new(std::sync::Mutex::new(ScriptedRest::default())),
            }),
        }
    }

    // pub: custom Net injection lets consumers opt out of Net::shared_default host buckets.
    pub fn with_account_net(
        net: AccountNet,
        api_base: impl Into<String>,
        token_source: Arc<dyn TokenSource>,
    ) -> Self {
        let api_base = trim_base(api_base.into());
        let outlook_base = derive_outlook_base(&api_base);
        // Derived from the supplied base like every other constructor,
        // not hardcoded: an injected net against a redirected base must
        // meter under its own host bucket, not the production Graph
        // host's.
        let rate_limit_host = host_from_api_base(&api_base);
        Self {
            inner: Arc::new(ClientInner {
                net: None,
                account_net: RwLock::new(Some(net)),
                api_base,
                outlook_base,
                rate_limit_host,
                token_source,
                mailbox_id: None,
                semaphore: Arc::new(Semaphore::new(CONCURRENCY_LIMIT)),
                #[cfg(test)]
                scripted: Arc::new(std::sync::Mutex::new(ScriptedRest::default())),
            }),
        }
    }

    pub(crate) fn attach_account(&self, account_id: AccountId) {
        if let Some(net) = self.inner.net.as_ref() {
            let token_source = Arc::clone(&self.inner.token_source);
            let account_net = net.attach_account(
                account_id,
                AccountSpec {
                    hosts: vec![RateLimit {
                        host: self.inner.rate_limit_host.clone(),
                        quota_per_second: 10.0,
                        cost_default: 1,
                        burst: 10,
                    }],
                    token_source,
                    default_retry: RetryPolicy::default(),
                },
            );
            // Install the replacement first, then tear down whatever
            // registration it displaced. `Net::attach_account` mints a
            // fresh token on every call and no longer unregisters a
            // previous attachment for the same id, so a reopen that
            // dropped its old handle without detaching would leak one
            // meter attachment and one governor attach count per
            // cycle - the host bucket would never be reclaimed.
            // Detaching after the install keeps the shared counts from
            // dipping to zero, so in-flight requests on the old handle
            // keep metering against the same counters.
            let displaced = match self.inner.account_net.write() {
                Ok(mut slot) => slot.replace(account_net),
                Err(_) => None,
            };
            if let Some(displaced) = displaced {
                displaced.detach();
            }
            return;
        }
        // No parent `Net`: the consumer constructed us via
        // `with_account_net`. Retag the existing `AccountNet` so
        // per-account metering and host bookkeeping move under the
        // engine id; in-flight requests on the old handle continue.
        let existing = self
            .inner
            .account_net
            .read()
            .ok()
            .and_then(|slot| slot.clone());
        let Some(existing) = existing else {
            return;
        };
        if existing.account() == &account_id {
            return;
        }
        let retagged = existing.retag(account_id);
        if let Ok(mut slot) = self.inner.account_net.write() {
            *slot = Some(retagged);
        }
    }

    pub(crate) fn account_net(&self) -> Option<AccountNet> {
        self.inner
            .account_net
            .read()
            .ok()
            .and_then(|slot| slot.clone())
    }

    pub(crate) fn api_base(&self) -> &str {
        &self.inner.api_base
    }

    /// Origin for the Autodiscover + EWS surfaces.
    pub(crate) fn outlook_base(&self) -> &str {
        &self.inner.outlook_base
    }

    /// Override the Autodiscover / EWS origin independently of the Graph
    /// api-base. Test seam mirroring `bifrost-google`'s
    /// `with_people_api_base`: those two surfaces live on a different host
    /// from Graph in production, so a harness that only redirects the Graph
    /// base would still send Autodiscover and EWS traffic to
    /// `outlook.office365.com`. Redirecting the Graph api-base to a
    /// non-Graph host already derives this (so the common harness case needs
    /// no extra call); this exists for a sovereign cloud, where the Graph and
    /// Outlook hosts differ but neither is the public one.
    // pub: harness / sovereign-cloud consumers redirect Autodiscover + EWS before registration.
    #[must_use]
    pub fn with_outlook_base(&self, outlook_base: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                net: self.inner.net.clone(),
                account_net: RwLock::new(self.account_net()),
                api_base: self.inner.api_base.clone(),
                outlook_base: trim_base(outlook_base.into()),
                rate_limit_host: self.inner.rate_limit_host.clone(),
                token_source: Arc::clone(&self.inner.token_source),
                mailbox_id: self.inner.mailbox_id.clone(),
                semaphore: Arc::clone(&self.inner.semaphore),
                #[cfg(test)]
                scripted: Arc::clone(&self.inner.scripted),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) async fn access_token(&self) -> String {
        self.inner
            .token_source
            .current()
            .await
            .expect("static token source is infallible")
            .as_str()
            .to_string()
    }

    pub(crate) fn api_path_prefix(&self) -> String {
        match &self.inner.mailbox_id {
            Some(id) => format!("/users/{}", bifrost_net::url::encode_path_component(id)),
            None => "/me".to_string(),
        }
    }

    // pub: shared-mailbox consumers derive a scoped client before building the factory.
    pub fn for_shared_mailbox(&self, mailbox_id: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                net: self.inner.net.clone(),
                account_net: RwLock::new(self.account_net()),
                api_base: self.inner.api_base.clone(),
                outlook_base: self.inner.outlook_base.clone(),
                rate_limit_host: self.inner.rate_limit_host.clone(),
                token_source: Arc::clone(&self.inner.token_source),
                mailbox_id: Some(mailbox_id.into()),
                semaphore: Arc::clone(&self.inner.semaphore),
                #[cfg(test)]
                scripted: Arc::clone(&self.inner.scripted),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_shared_mailbox(&self) -> bool {
        self.inner.mailbox_id.is_some()
    }

    pub(crate) fn uses_default_mailbox(&self) -> bool {
        self.inner.mailbox_id.is_none()
    }

    #[cfg(test)]
    pub(crate) fn mailbox_id(&self) -> Option<&str> {
        self.inner.mailbox_id.as_deref()
    }

    pub(crate) async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, GraphError> {
        let url = self.api_url(path);
        self.request::<T, ()>(&url, "GET", None).await
    }

    pub(crate) async fn get_json_prefer<T: DeserializeOwned>(
        &self,
        path: &str,
        prefer: &str,
    ) -> Result<T, GraphError> {
        let url = self.api_url(path);
        self.request_prefer::<T, ()>(&url, "GET", prefer, None)
            .await
    }

    pub(crate) async fn get_absolute<T: DeserializeOwned>(
        &self,
        url: &str,
    ) -> Result<T, GraphError> {
        self.request::<T, ()>(url, "GET", None).await
    }

    pub(crate) async fn get_absolute_prefer<T: DeserializeOwned>(
        &self,
        url: &str,
        prefer: &str,
    ) -> Result<T, GraphError> {
        self.request_prefer::<T, ()>(url, "GET", prefer, None).await
    }

    pub(crate) async fn post<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, GraphError> {
        let url = self.api_url(path);
        self.request(&url, "POST", Some(body)).await
    }

    pub(crate) async fn post_empty(&self, path: &str) -> Result<(), GraphError> {
        let url = self.api_url(path);
        let response = self.execute(&url, "POST", None::<&()>).await?;
        check_response_status(response)
    }

    pub(crate) async fn post_no_response<B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<(), GraphError> {
        let url = self.api_url(path);
        let response = self.execute(&url, "POST", Some(body)).await?;
        check_response_status(response)
    }

    /// POST a base64-encoded RFC 5322 message to create a draft from raw
    /// MIME (Graph's import-from-MIME path: `Content-Type: text/plain`,
    /// body = the base64 of the MIME octets). Returns the created message
    /// resource. Distinct from `post` because that path hardcodes a JSON
    /// content type and serializes its body; raw MIME needs neither.
    pub(crate) async fn post_mime<T: DeserializeOwned>(
        &self,
        path: &str,
        base64_mime: Bytes,
    ) -> Result<T, GraphError> {
        let url = self.api_url(path);
        let response = self
            .execute_wire(&url, "POST", None, None, Some(WireBody::mime(base64_mime)))
            .await?;
        parse_json_response(response)
    }

    pub(crate) async fn patch<B: Serialize>(&self, path: &str, body: &B) -> Result<(), GraphError> {
        let url = self.api_url(path);
        let response = self.execute(&url, "PATCH", Some(body)).await?;
        check_response_status(response)
    }

    pub(crate) async fn patch_if_match<B: Serialize>(
        &self,
        path: &str,
        etag: &str,
        body: &B,
    ) -> Result<(), GraphError> {
        let url = self.api_url(path);
        let response = self
            .execute_if_match(&url, "PATCH", etag, Some(body))
            .await?;
        check_response_status(response)
    }

    pub(crate) async fn patch_json<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, GraphError> {
        let url = self.api_url(path);
        self.request(&url, "PATCH", Some(body)).await
    }

    pub(crate) async fn delete(&self, path: &str) -> Result<(), GraphError> {
        let url = self.api_url(path);
        let response = self.execute(&url, "DELETE", None::<&()>).await?;
        check_response_status(response)
    }

    pub(crate) async fn delete_if_match(&self, path: &str, etag: &str) -> Result<(), GraphError> {
        let url = self.api_url(path);
        let response = self
            .execute_if_match(&url, "DELETE", etag, None::<&()>)
            .await?;
        check_response_status(response)
    }

    pub(crate) async fn post_batch(
        &self,
        batch: &crate::types::BatchRequest,
    ) -> Result<crate::types::BatchResponse, GraphError> {
        self.post("/$batch", batch).await
    }

    fn api_url(&self, path: &str) -> String {
        build_url(&self.inner.api_base, path)
    }

    async fn request<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &str,
        method: &str,
        body: Option<&B>,
    ) -> Result<T, GraphError> {
        let response = self.execute(url, method, body).await?;
        parse_json_response(response)
    }

    async fn request_prefer<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &str,
        method: &str,
        prefer: &str,
        body: Option<&B>,
    ) -> Result<T, GraphError> {
        let response = self
            .execute_request(url, method, None, Some(prefer), body)
            .await?;
        parse_json_response(response)
    }

    async fn execute<B: Serialize>(
        &self,
        url: &str,
        method: &str,
        body: Option<&B>,
    ) -> Result<RestResponse, GraphError> {
        self.execute_request(url, method, None, None, body).await
    }

    async fn execute_if_match<B: Serialize>(
        &self,
        url: &str,
        method: &str,
        etag: &str,
        body: Option<&B>,
    ) -> Result<RestResponse, GraphError> {
        self.execute_request(url, method, Some(etag), None, body)
            .await
    }

    async fn execute_request<B: Serialize>(
        &self,
        url: &str,
        method: &str,
        if_match: Option<&str>,
        prefer: Option<&str>,
        body: Option<&B>,
    ) -> Result<RestResponse, GraphError> {
        let body = match body {
            Some(body) => Some(WireBody::json(body)?),
            None => None,
        };
        self.execute_wire(url, method, if_match, prefer, body).await
    }

    /// The one place a Graph REST request leaves this crate. Every helper
    /// above funnels through here, raw MIME included, so the scripted seam
    /// covers the whole surface and no path can quietly bypass it.
    async fn execute_wire(
        &self,
        url: &str,
        method: &str,
        if_match: Option<&str>,
        prefer: Option<&str>,
        body: Option<WireBody>,
    ) -> Result<RestResponse, GraphError> {
        #[cfg(test)]
        if let Some(outcome) = self.scripted_wire(method, url, if_match, prefer, body.as_ref()) {
            return outcome;
        }
        let _permit = self.inner.semaphore.acquire().await.map_err(|_| {
            GraphError::Net(bifrost_net::Error::Network {
                message: "Graph request semaphore closed".to_string(),
                transmission_state: TransmissionState::Unsent,
                source: None,
            })
        })?;
        let account_net = self.account_net().ok_or_else(|| {
            GraphError::Net(bifrost_net::Error::Network {
                message: "Graph client is not attached to an account".to_string(),
                transmission_state: TransmissionState::Unsent,
                source: None,
            })
        })?;

        let mut builder = match method {
            "GET" => account_net.get(url),
            "POST" => account_net.post(url),
            "PATCH" => account_net.patch(url),
            "DELETE" => account_net.delete(url),
            // pub(crate) callers route through `get_json` / `post` /
            // `patch` / `delete` / `post_empty` only; any other token
            // is a programmer bug rather than a runtime path.
            other => {
                unreachable!("unsupported HTTP method passed to GraphClient::execute: {other}")
            }
        };

        builder = builder.header(
            "Content-Type",
            body.as_ref()
                .map_or(JSON_CONTENT_TYPE, |body| body.content_type),
        );
        if let Some(etag) = if_match {
            builder = builder.header("If-Match", etag);
        }
        if let Some(prefer) = prefer {
            builder = builder.header("Prefer", prefer);
        }

        if let Some(body) = body {
            builder = builder.body(body.bytes);
        }

        builder
            .send()
            .await
            .map(RestResponse::from)
            .map_err(GraphError::Net)
    }

    /// Answer a request from the installed script, or `None` when this
    /// client was never scripted (production, and the many unit tests that
    /// never issue a request at all).
    ///
    /// An ARMED client that runs out of responses panics rather than
    /// falling through to `AccountNet`: falling through would let an
    /// unexpected extra request reach the network, which breaks
    /// hermeticity outright, and would leave it out of the recorded
    /// requests, so a test asserting "exactly N requests" could not detect
    /// the N+1th. Mirrors the EWS double's exhaustion panic.
    #[cfg(test)]
    fn scripted_wire(
        &self,
        method: &str,
        url: &str,
        if_match: Option<&str>,
        prefer: Option<&str>,
        body: Option<&WireBody>,
    ) -> Option<Result<RestResponse, GraphError>> {
        let mut scripted = self.inner.scripted.lock().expect("REST script lock");
        if !scripted.armed {
            return None;
        }
        let Some(response) = scripted.responses.pop_front() else {
            panic!("Graph REST script exhausted by request: {method} {url}");
        };
        let is_json = body.is_none_or(|body| body.content_type == JSON_CONTENT_TYPE);
        scripted.requests.push(RestRequest {
            method: method.to_string(),
            url: url.to_string(),
            if_match: if_match.map(str::to_string),
            prefer: prefer.map(str::to_string),
            content_type: body
                .map_or(JSON_CONTENT_TYPE, |body| body.content_type)
                .to_string(),
            body: body.filter(|_| is_json).map(|body| {
                serde_json::from_slice(body.bytes.as_ref()).expect("Graph JSON body round-trips")
            }),
            raw_body: body.filter(|_| !is_json).map(|body| body.bytes.clone()),
        });
        Some(response.into_net_outcome())
    }

    #[cfg(test)]
    pub(crate) fn script_rest(&self, responses: impl IntoIterator<Item = ScriptedRestResponse>) {
        let mut scripted = self.inner.scripted.lock().expect("REST script lock");
        scripted.armed = true;
        scripted.responses.extend(responses);
    }

    #[cfg(test)]
    pub(crate) fn take_rest_requests(&self) -> Vec<RestRequest> {
        std::mem::take(
            &mut self
                .inner
                .scripted
                .lock()
                .expect("REST script lock")
                .requests,
        )
    }
}

fn trim_base(base: String) -> String {
    base.trim_end_matches('/').to_string()
}

fn host_from_api_base(api_base: &str) -> String {
    reqwest::Url::parse(api_base)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| GRAPH_HOST.to_string())
}

/// Derive the Autodiscover / EWS origin from the Graph api-base.
///
/// A base on the production Graph host means production Microsoft, so
/// Autodiscover and EWS keep their own production origin
/// (`outlook.office365.com`) - they are a different host from Graph and
/// must not be rewritten to it. Any OTHER host means the api-base was
/// redirected (a harness mock, a sovereign cloud), and the same origin is
/// the only sane target for the sibling surfaces: a harness that redirects
/// Graph but leaves Autodiscover/EWS pointing at the real
/// `outlook.office365.com` cannot exercise the public-folder or EWS-streaming
/// legs at all. An unparseable base falls back to production rather than
/// inventing an origin.
fn derive_outlook_base(api_base: &str) -> String {
    let Ok(url) = reqwest::Url::parse(api_base) else {
        return OUTLOOK_BASE.to_string();
    };
    if url.host_str() == Some(GRAPH_HOST) {
        return OUTLOOK_BASE.to_string();
    }
    match (url.host_str(), url.port()) {
        (Some(host), Some(port)) => format!("{}://{host}:{port}", url.scheme()),
        (Some(host), None) => format!("{}://{host}", url.scheme()),
        (None, _) => OUTLOOK_BASE.to_string(),
    }
}

fn build_url(base: &str, path: &str) -> String {
    if path.starts_with("http://") || path.starts_with("https://") {
        path.to_string()
    } else if path.starts_with('/') {
        format!("{base}{path}")
    } else {
        format!("{base}/{path}")
    }
}

/// Parse a Graph JSON success response. On a non-success status,
/// constructs a `GraphResponseError` from the raw response so the
/// account boundary receives structured evidence, not a formatted
/// string.
///
/// The reachable non-success statuses here are the 3xx `bifrost-net`
/// passes through (304 on a conditional read, and a redirect no one can
/// follow); 4xx and 5xx never arrive as a response at all, because the
/// transport converts them into `bifrost_net::Error` first. Those are
/// re-decoded against Graph's error envelope at the account boundary
/// (`graph_error::into_account_error`), not here.
fn parse_json_response<T: DeserializeOwned>(response: RestResponse) -> Result<T, GraphError> {
    let status = response.status;
    let RestResponse { headers, body, .. } = response;
    if !status.is_success() {
        let err = GraphResponseError::from_response(status, headers, body);
        return Err(GraphError::Response(err));
    }

    serde_json::from_slice(body.as_ref()).map_err(|e| GraphError::Json {
        message: e.to_string(),
        body: if body.is_empty() { None } else { Some(body) },
    })
}

/// Check a Graph response for success status. On failure, constructs
/// a `GraphResponseError` from the raw response.
fn check_response_status(response: RestResponse) -> Result<(), GraphError> {
    let status = response.status;
    if status.is_success() {
        return Ok(());
    }
    let body = Bytes::copy_from_slice(response.body.as_ref());
    let err = GraphResponseError::from_response(status, response.headers, body);
    Err(GraphError::Response(err))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn trims_api_base() {
        let client = GraphClient::with_api_base("https://example.test/v1.0/", "token");
        assert_eq!(client.api_base(), "https://example.test/v1.0");
        assert_eq!(client.access_token().await, "token");
    }

    #[test]
    fn harness_api_base_redirects_autodiscover_and_ews() {
        // A harness pointing the Graph api-base at its mock must get the
        // Autodiscover + EWS surfaces redirected too: in production they
        // live on `outlook.office365.com`, a different host from Graph, so
        // an api-base-only override left them hitting the real service.
        let harness = GraphClient::with_api_base("http://127.0.0.1:8181/v1.0", "token");
        assert_eq!(harness.outlook_base(), "http://127.0.0.1:8181");
        assert_eq!(
            crate::ews::ews_url(harness.outlook_base()),
            "http://127.0.0.1:8181/EWS/Exchange.asmx"
        );
        assert_eq!(
            crate::account::autodiscover_soap_url(harness.outlook_base()),
            "http://127.0.0.1:8181/autodiscover/autodiscover.svc"
        );
        assert_eq!(
            crate::account::autodiscover_xml_url(harness.outlook_base()),
            "http://127.0.0.1:8181/autodiscover/autodiscover.xml"
        );

        // The production Graph base keeps the production Outlook origin -
        // Autodiscover/EWS are NOT on the Graph host.
        let production = GraphClient::new("token");
        assert_eq!(production.outlook_base(), OUTLOOK_BASE);
        assert_eq!(
            crate::ews::ews_url(production.outlook_base()),
            "https://outlook.office365.com/EWS/Exchange.asmx"
        );

        // A shared-mailbox client inherits the override.
        assert_eq!(
            harness
                .for_shared_mailbox("shared@contoso.com")
                .outlook_base(),
            "http://127.0.0.1:8181"
        );

        // An explicit override wins over the derivation.
        assert_eq!(
            production
                .with_outlook_base("https://ews.test/")
                .outlook_base(),
            "https://ews.test"
        );
    }

    #[tokio::test]
    async fn rotated_token_source_is_read() {
        use bifrost_net::AccessToken;

        let source = StaticTokenSource::new("old-token", None);
        let client = GraphClient::with_source(GRAPH_API_BASE, Arc::new(source.clone()));
        assert_eq!(client.access_token().await, "old-token");
        source.set(AccessToken::new("new-token", None));
        assert_eq!(client.access_token().await, "new-token");
    }

    #[test]
    fn api_path_prefix_returns_me_for_primary_mailbox() {
        let client = GraphClient::new("token");
        assert_eq!(client.api_path_prefix(), "/me");
        assert!(!client.is_shared_mailbox());
    }

    #[test]
    fn for_shared_mailbox_creates_scoped_client() {
        let client = GraphClient::new("token");
        let scoped = client.for_shared_mailbox("shared@example.com");
        assert_eq!(scoped.api_path_prefix(), "/users/shared%40example.com");
        assert_eq!(scoped.mailbox_id(), Some("shared@example.com"));
        assert!(scoped.is_shared_mailbox());
    }

    #[test]
    fn non_versioned_api_base_keeps_its_path() {
        let client = GraphClient::with_api_base("http://127.0.0.1:8181/graph", "token");
        assert_eq!(client.api_base(), "http://127.0.0.1:8181/graph");
        // The Outlook origin, by contrast, correctly follows the redirect.
        assert_eq!(client.outlook_base(), "http://127.0.0.1:8181");
    }

    #[test]
    fn build_url_joins_relative_paths_and_passes_absolute_urls_through() {
        // The `@odata.nextLink` / `@odata.deltaLink` walk feeds absolute
        // URLs back in; rewriting them onto the api-base would break
        // pagination.
        assert_eq!(
            build_url("https://x/v1.0", "/me/messages"),
            "https://x/v1.0/me/messages"
        );
        assert_eq!(
            build_url("https://x/v1.0", "me/messages"),
            "https://x/v1.0/me/messages"
        );
        assert_eq!(
            build_url("https://x/v1.0", "https://graph.example/next?$skiptoken=a"),
            "https://graph.example/next?$skiptoken=a"
        );
        assert_eq!(
            build_url("https://x/v1.0", "http://graph.example/next"),
            "http://graph.example/next"
        );
    }

    #[test]
    fn rate_limit_host_tracks_the_api_base_host() {
        // The per-host token bucket must follow a redirected base, or a
        // harness run would meter against `graph.microsoft.com`.
        assert_eq!(
            host_from_api_base("https://graph.microsoft.com/v1.0"),
            GRAPH_HOST
        );
        assert_eq!(
            host_from_api_base("http://127.0.0.1:8181/v1.0"),
            "127.0.0.1"
        );
        // An unparseable base falls back to production rather than panicking.
        assert_eq!(host_from_api_base("not a url"), GRAPH_HOST);
    }

    /// nc-9's shape: `with_account_net` was the one constructor that
    /// hardcoded `rate_limit_host` to the production Graph host, so an
    /// injected net against a redirected base metered under the wrong
    /// bucket. It must derive from the supplied base like the others.
    #[test]
    fn with_account_net_derives_the_rate_limit_host_from_its_base() {
        let donor = GraphClient::new("token");
        donor.attach_account(AccountId("with-account-net-host-probe".to_string()));
        let net = donor.account_net().expect("account net attached");
        let client = GraphClient::with_account_net(
            net,
            "http://127.0.0.1:8181/v1.0",
            Arc::new(StaticTokenSource::new("token", None)),
        );
        assert_eq!(client.inner.rate_limit_host, "127.0.0.1");
    }

    #[test]
    fn outlook_base_keeps_the_scheme_and_port_of_a_redirected_api_base() {
        assert_eq!(
            derive_outlook_base("https://graph.contoso-cloud.test/v1.0"),
            "https://graph.contoso-cloud.test"
        );
        assert_eq!(
            derive_outlook_base("http://127.0.0.1:8181/v1.0"),
            "http://127.0.0.1:8181"
        );
        assert_eq!(derive_outlook_base("nonsense"), OUTLOOK_BASE);
    }

    #[test]
    fn shared_mailbox_prefix_percent_encodes_the_routing_key() {
        // The routing key is an SMTP address; `@` and any `+` tag must not
        // leak into the path unencoded.
        let client = GraphClient::new("token").for_shared_mailbox("a+tag@contoso.com");
        let prefix = client.api_path_prefix();
        assert!(!prefix.contains('@'), "{prefix}");
        assert!(!prefix.contains('+'), "{prefix}");
        assert!(prefix.starts_with("/users/"), "{prefix}");
        // The stored key stays verbatim - it is the map key every routing
        // site (`client_for_owner`, `encode_foreign`) looks up.
        assert_eq!(client.mailbox_id(), Some("a+tag@contoso.com"));
        assert!(!client.uses_default_mailbox());
    }

    #[test]
    fn attach_account_uses_engine_account_id() {
        let client = GraphClient::new("token");
        client.attach_account(AccountId("engine-account".to_string()));
        let account_net = client.account_net().expect("account net attached");
        assert_eq!(
            account_net.account(),
            &AccountId("engine-account".to_string())
        );
    }

    /// Every reopen calls `attach_account` again with the same engine
    /// id. `Net` mints a fresh registration token per attach and no
    /// longer unregisters a previous one, so the displaced handle has
    /// to be detached explicitly or the host bucket leaks an attach
    /// count per cycle and is never reclaimed.
    /// `GraphClient::new` rides the process-wide `Net`, so this keys
    /// on a meter entry for an id no other test touches rather than on
    /// the shared `GRAPH_HOST` bucket.
    #[test]
    fn reattaching_the_same_engine_id_does_not_leak_registrations() {
        let client = GraphClient::new("token");
        let id = AccountId("graph-reattach-leak-probe".to_string());
        let net = client.inner.net.clone().expect("default client owns a Net");

        for _ in 0..3 {
            client.attach_account(id.clone());
        }
        bifrost_net::MeterSink::record_bytes_in(net.meter(), &id, 5);
        assert_eq!(net.meter().account(id.clone()).bytes_in(), 5);

        client.account_net().expect("account net attached").detach();

        assert_eq!(
            net.meter().account(id).bytes_in(),
            0,
            "three attaches must leave exactly one live registration to detach"
        );
    }

    #[tokio::test]
    async fn scripted_rest_records_the_graph_owned_request_and_returns_its_response() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({"displayName":"Ada"}),
        )]);

        let profile = client.get_profile().await.expect("scripted profile");
        assert_eq!(profile.display_name.as_deref(), Some("Ada"));
        let requests = client.take_rest_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "GET");
        assert!(
            requests[0]
                .url
                .ends_with("/me?$select=displayName,mail,userPrincipalName")
        );
        assert_eq!(requests[0].if_match, None);
        assert_eq!(requests[0].prefer, None);
        assert_eq!(requests[0].body, None);

        let response = ScriptedRestResponse::empty(reqwest::StatusCode::TOO_MANY_REQUESTS)
            .with_header("Retry-After", "5");
        assert_eq!(response.headers["retry-after"], "5");
    }

    /// The seam is only worth building if it answers the way the transport
    /// answers. `bifrost-net` returns `Ok(Response)` for 2xx and a
    /// passed-through 3xx ONLY; every other status leaves its retry loop as
    /// a typed `Error`. A seam that returned a scripted 500 as a response
    /// would send every test built on it down a branch production cannot
    /// reach, and nothing would fail to say so.
    #[test]
    fn a_scripted_status_takes_the_shape_bifrost_net_would_have_produced() {
        use reqwest::StatusCode;

        assert!(
            ScriptedRestResponse::empty(StatusCode::OK)
                .into_net_outcome()
                .is_ok()
        );
        // A 304 on a conditional read is passed through as a response.
        assert!(
            ScriptedRestResponse::empty(StatusCode::NOT_MODIFIED)
                .into_net_outcome()
                .is_ok()
        );

        match ScriptedRestResponse::empty(StatusCode::NOT_FOUND).into_net_outcome() {
            Err(GraphError::Net(bifrost_net::Error::Status { code, .. })) => {
                assert_eq!(code, StatusCode::NOT_FOUND);
            }
            _ => panic!("a terminal 4xx is Error::Status"),
        }

        match ScriptedRestResponse::empty(StatusCode::TOO_MANY_REQUESTS)
            .with_header("Retry-After", "5")
            .into_net_outcome()
        {
            Err(GraphError::Net(bifrost_net::Error::RateLimited {
                retry_after,
                final_response,
            })) => {
                assert_eq!(retry_after, Some(std::time::Duration::from_secs(5)));
                assert_eq!(final_response.status, StatusCode::TOO_MANY_REQUESTS);
            }
            _ => panic!("429 past the budget is Error::RateLimited"),
        }

        match ScriptedRestResponse::empty(StatusCode::INTERNAL_SERVER_ERROR).into_net_outcome() {
            Err(GraphError::Net(bifrost_net::Error::RetryBudgetExhausted {
                final_response: Some(final_response),
                ..
            })) => assert_eq!(final_response.status, StatusCode::INTERNAL_SERVER_ERROR),
            _ => panic!("5xx past the budget is Error::RetryBudgetExhausted"),
        }

        match ScriptedRestResponse::empty(StatusCode::UNAUTHORIZED).into_net_outcome() {
            Err(GraphError::Net(bifrost_net::Error::AuthLost {
                transmission_state,
                final_response: Some(_),
            })) => assert_eq!(transmission_state, Some(TransmissionState::Acknowledged)),
            _ => panic!("a 401 surviving the forced refresh is Error::AuthLost"),
        }
    }

    /// An armed script that runs out must fail the test at the request that
    /// exceeded it. Falling through to `AccountNet` would put a real socket
    /// behind an unexpected request and leave it out of the recorded list,
    /// so a test asserting "exactly N requests" could not see the N+1th.
    #[tokio::test]
    #[should_panic(expected = "Graph REST script exhausted")]
    async fn an_exhausted_script_fails_loudly_instead_of_reaching_the_network() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            serde_json::json!({}),
        )]);
        let _ = client.get_profile().await;
        let _ = client.get_profile().await;
    }
}
