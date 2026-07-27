use std::sync::{Arc, RwLock};

use bifrost_net::{
    AccountId, AccountNet, AccountSpec, Net, RateLimit, Response, RetryPolicy, StaticTokenSource,
    TokenSource,
};
use bifrost_types::TransmissionState;
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;

use crate::error::{GraphError, GraphResponseError};

pub(crate) const GRAPH_API_BASE: &str = "https://graph.microsoft.com/v1.0";
pub(crate) const GRAPH_API_BETA: &str = "https://graph.microsoft.com/beta";

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
    api_beta_base: String,
    /// Origin for the Autodiscover + EWS surfaces. Derived from `api_base`
    /// (see [`derive_outlook_base`]) unless a consumer overrode it with
    /// [`GraphClient::with_outlook_base`].
    outlook_base: String,
    rate_limit_host: String,
    token_source: Arc<dyn TokenSource>,
    mailbox_id: Option<String>,
    semaphore: Arc<Semaphore>,
}

impl GraphClient {
    // pub: ergonomic constructor for the default Microsoft Graph endpoint.
    pub fn new(access_token: impl Into<String>) -> Self {
        Self::with_api_bases(GRAPH_API_BASE, GRAPH_API_BETA, access_token)
    }

    // pub: consumers may need sovereign-cloud or test Graph API endpoints before registration.
    pub fn with_api_base(api_base: impl Into<String>, access_token: impl Into<String>) -> Self {
        let api_base = api_base.into();
        let api_beta_base =
            derive_beta_base(&api_base).unwrap_or_else(|| GRAPH_API_BETA.to_string());
        Self::with_api_bases(api_base, api_beta_base, access_token)
    }

    // pub: lets callers configure v1.0 and beta endpoints independently for non-public clouds.
    pub fn with_api_bases(
        api_base: impl Into<String>,
        api_beta_base: impl Into<String>,
        access_token: impl Into<String>,
    ) -> Self {
        // Bases are trimmed once in `with_bases_and_source`.
        let token_source: Arc<dyn TokenSource> =
            Arc::new(StaticTokenSource::new(access_token, None));
        Self::with_bases_and_source(api_base, api_beta_base, token_source)
    }

    // pub: source-accepting constructor. ratatoskr hands in a shared
    // `Arc<dyn TokenSource>` (typically an `OAuthRefresher` over its own
    // refresh-token store) so a token it refreshes and persists is read
    // live at every wire authentication without reopening the client.
    pub fn with_source(
        api_base: impl Into<String>,
        api_beta_base: impl Into<String>,
        source: Arc<dyn TokenSource>,
    ) -> Self {
        Self::with_bases_and_source(api_base, api_beta_base, source)
    }

    fn with_bases_and_source(
        api_base: impl Into<String>,
        api_beta_base: impl Into<String>,
        token_source: Arc<dyn TokenSource>,
    ) -> Self {
        let api_base = trim_base(api_base.into());
        let api_beta_base = trim_base(api_beta_base.into());
        let rate_limit_host = host_from_api_base(&api_base);
        let outlook_base = derive_outlook_base(&api_base);
        Self {
            inner: Arc::new(ClientInner {
                net: Some(Net::shared_default()),
                account_net: RwLock::new(None),
                api_base,
                api_beta_base,
                outlook_base,
                rate_limit_host,
                token_source,
                mailbox_id: None,
                semaphore: Arc::new(Semaphore::new(CONCURRENCY_LIMIT)),
            }),
        }
    }

    // pub: custom Net injection lets consumers opt out of Net::shared_default host buckets.
    pub fn with_account_net(
        net: AccountNet,
        api_base: impl Into<String>,
        api_beta_base: impl Into<String>,
        token_source: Arc<dyn TokenSource>,
    ) -> Self {
        let api_base = trim_base(api_base.into());
        let outlook_base = derive_outlook_base(&api_base);
        Self {
            inner: Arc::new(ClientInner {
                net: None,
                account_net: RwLock::new(Some(net)),
                api_base,
                api_beta_base: trim_base(api_beta_base.into()),
                outlook_base,
                rate_limit_host: GRAPH_HOST.to_string(),
                token_source,
                mailbox_id: None,
                semaphore: Arc::new(Semaphore::new(CONCURRENCY_LIMIT)),
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
            if let Ok(mut slot) = self.inner.account_net.write() {
                *slot = Some(account_net);
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
                api_beta_base: self.inner.api_beta_base.clone(),
                outlook_base: trim_base(outlook_base.into()),
                rate_limit_host: self.inner.rate_limit_host.clone(),
                token_source: Arc::clone(&self.inner.token_source),
                mailbox_id: self.inner.mailbox_id.clone(),
                semaphore: Arc::clone(&self.inner.semaphore),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn api_beta_base(&self) -> &str {
        &self.inner.api_beta_base
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
            Some(id) => format!("/users/{}", bifrost_net::url::encode_component(id)),
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
                api_beta_base: self.inner.api_beta_base.clone(),
                outlook_base: self.inner.outlook_base.clone(),
                rate_limit_host: self.inner.rate_limit_host.clone(),
                token_source: Arc::clone(&self.inner.token_source),
                mailbox_id: Some(mailbox_id.into()),
                semaphore: Arc::clone(&self.inner.semaphore),
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
        let response = account_net
            .post(&url)
            .header("Content-Type", "text/plain")
            .body(base64_mime)
            .send()
            .await
            .map_err(GraphError::Net)?;
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
    ) -> Result<Response, GraphError> {
        self.execute_request(url, method, None, None, body).await
    }

    async fn execute_if_match<B: Serialize>(
        &self,
        url: &str,
        method: &str,
        etag: &str,
        body: Option<&B>,
    ) -> Result<Response, GraphError> {
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
    ) -> Result<Response, GraphError> {
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

        builder = builder.header("Content-Type", "application/json");
        if let Some(etag) = if_match {
            builder = builder.header("If-Match", etag);
        }
        if let Some(prefer) = prefer {
            builder = builder.header("Prefer", prefer);
        }

        if let Some(b) = body {
            builder = builder.json(b);
        }

        builder.send().await.map_err(GraphError::Net)
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

fn derive_beta_base(api_base: &str) -> Option<String> {
    api_base
        .trim_end_matches('/')
        .strip_suffix("/v1.0")
        .map(|prefix| format!("{prefix}/beta"))
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
fn parse_json_response<T: DeserializeOwned>(response: Response) -> Result<T, GraphError> {
    let status = response.status();
    let Response { headers, body, .. } = response;
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
fn check_response_status(response: Response) -> Result<(), GraphError> {
    let status = response.status();
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
    async fn trims_api_bases() {
        let client = GraphClient::with_api_bases(
            "https://example.test/v1.0/",
            "https://example.test/beta/",
            "token",
        );
        assert_eq!(client.api_base(), "https://example.test/v1.0");
        assert_eq!(client.api_beta_base(), "https://example.test/beta");
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

    #[test]
    fn derives_beta_base_from_v1_base() {
        let client = GraphClient::with_api_base("https://example.test/v1.0/", "token");
        assert_eq!(client.api_beta_base(), "https://example.test/beta");
    }

    #[tokio::test]
    async fn rotated_token_source_is_read() {
        use bifrost_net::AccessToken;

        let source = StaticTokenSource::new("old-token", None);
        let client =
            GraphClient::with_source(GRAPH_API_BASE, GRAPH_API_BETA, Arc::new(source.clone()));
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
    fn attach_account_uses_engine_account_id() {
        let client = GraphClient::new("token");
        client.attach_account(AccountId("engine-account".to_string()));
        let account_net = client.account_net().expect("account net attached");
        assert_eq!(
            account_net.account(),
            &AccountId("engine-account".to_string())
        );
    }
}
