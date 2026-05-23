use std::sync::{Arc, RwLock};

use bifrost_net::{
    AccessToken, AccountId, AccountNet, AccountSpec, Net, RateLimit, Response, RetryPolicy,
    StaticTokenSource, TokenSource,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;

pub(crate) const GRAPH_API_BASE: &str = "https://graph.microsoft.com/v1.0";
pub(crate) const GRAPH_API_BETA: &str = "https://graph.microsoft.com/beta";

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
    rate_limit_host: String,
    token_source: StaticTokenSource,
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
        let api_base = trim_base(api_base.into());
        let api_beta_base = trim_base(api_beta_base.into());
        let rate_limit_host = host_from_api_base(&api_base);
        let token_source = StaticTokenSource::new(access_token, None);
        Self {
            inner: Arc::new(ClientInner {
                net: Some(Net::shared_default()),
                account_net: RwLock::new(None),
                api_base,
                api_beta_base,
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
        token_source: StaticTokenSource,
    ) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                net: None,
                account_net: RwLock::new(Some(net)),
                api_base: trim_base(api_base.into()),
                api_beta_base: trim_base(api_beta_base.into()),
                rate_limit_host: "graph.microsoft.com".to_string(),
                token_source,
                mailbox_id: None,
                semaphore: Arc::new(Semaphore::new(CONCURRENCY_LIMIT)),
            }),
        }
    }

    pub(crate) fn attach_account(&self, account_id: AccountId) {
        if let Some(net) = self.inner.net.as_ref() {
            let token_source: Arc<dyn TokenSource> = Arc::new(self.inner.token_source.clone());
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

    #[cfg(test)]
    pub(crate) fn api_beta_base(&self) -> &str {
        &self.inner.api_beta_base
    }

    #[cfg(test)]
    pub(crate) async fn access_token(&self) -> String {
        self.inner.token_source.token().as_str().to_string()
    }

    // pub: token rotation must update the shared source held by open factories and accounts.
    pub async fn set_access_token(&self, access_token: impl Into<String>) {
        self.inner
            .token_source
            .set(AccessToken::new(access_token, None));
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
                rate_limit_host: self.inner.rate_limit_host.clone(),
                token_source: self.inner.token_source.clone(),
                mailbox_id: Some(mailbox_id.into()),
                semaphore: Arc::clone(&self.inner.semaphore),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_shared_mailbox(&self) -> bool {
        self.inner.mailbox_id.is_some()
    }

    #[cfg(test)]
    pub(crate) fn mailbox_id(&self) -> Option<&str> {
        self.inner.mailbox_id.as_deref()
    }

    pub(crate) async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, String> {
        let url = self.api_url(path);
        self.request::<T, ()>(&url, "GET", None).await
    }

    pub(crate) async fn get_absolute<T: DeserializeOwned>(&self, url: &str) -> Result<T, String> {
        self.request::<T, ()>(url, "GET", None).await
    }

    pub(crate) async fn post<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, String> {
        let url = self.api_url(path);
        self.request(&url, "POST", Some(body)).await
    }

    pub(crate) async fn post_empty(&self, path: &str) -> Result<(), String> {
        let url = self.api_url(path);
        let response = self.execute(&url, "POST", None::<&()>).await?;
        check_response_status(response, "Graph API").await
    }

    pub(crate) async fn patch<B: Serialize>(&self, path: &str, body: &B) -> Result<(), String> {
        let url = self.api_url(path);
        let response = self.execute(&url, "PATCH", Some(body)).await?;
        check_response_status(response, "Graph API").await
    }

    pub(crate) async fn delete(&self, path: &str) -> Result<(), String> {
        let url = self.api_url(path);
        let response = self.execute(&url, "DELETE", None::<&()>).await?;
        check_response_status(response, "Graph API").await
    }

    pub(crate) async fn post_batch(
        &self,
        batch: &crate::types::BatchRequest,
    ) -> Result<crate::types::BatchResponse, String> {
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
    ) -> Result<T, String> {
        let response = self.execute(url, method, body).await?;
        parse_json_response(response, "Graph API").await
    }

    async fn execute<B: Serialize>(
        &self,
        url: &str,
        method: &str,
        body: Option<&B>,
    ) -> Result<Response, String> {
        let _permit = self
            .inner
            .semaphore
            .acquire()
            .await
            .map_err(|_| "Graph request semaphore closed".to_string())?;
        let account_net = self
            .account_net()
            .ok_or_else(|| "Graph client is not attached to an account".to_string())?;

        let mut builder = match method {
            "GET" => account_net.get(url),
            "POST" => account_net.post(url),
            "PATCH" => account_net.patch(url),
            "DELETE" => account_net.delete(url),
            _ => return Err(format!("Unsupported HTTP method: {method}")),
        };

        builder = builder.header("Content-Type", "application/json");

        if let Some(b) = body {
            builder = builder.json(b);
        }

        builder
            .send()
            .await
            .map_err(|error| net_error("Graph API", error))
    }
}

fn trim_base(base: String) -> String {
    base.trim_end_matches('/').to_string()
}

fn host_from_api_base(api_base: &str) -> String {
    reqwest::Url::parse(api_base)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| "graph.microsoft.com".to_string())
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

async fn parse_json_response<T: DeserializeOwned>(
    response: Response,
    service: &str,
) -> Result<T, String> {
    let status = response.status();
    if !status.is_success() {
        let body = response_body_string(response);
        return Err(format!("{service} error {status}: {body}"));
    }

    serde_json::from_slice(response.body.as_ref())
        .map_err(|e| format!("{service} JSON parse failed: {e}"))
}

async fn check_response_status(response: Response, service: &str) -> Result<(), String> {
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }

    let body = response_body_string(response);
    Err(format!("{service} error {status}: {body}"))
}

fn response_body_string(response: Response) -> String {
    String::from_utf8_lossy(response.body.as_ref()).into_owned()
}

fn net_error(service: &str, err: bifrost_net::Error) -> String {
    match err {
        bifrost_net::Error::Status { code, body, .. } => {
            format!(
                "{service} error {code}: {}",
                String::from_utf8_lossy(body.as_ref())
            )
        }
        bifrost_net::Error::AuthLost => format!("{service} error 401 Unauthorized: auth lost"),
        bifrost_net::Error::RateLimited { .. } => {
            format!("{service} error 429 Too Many Requests: rate limited")
        }
        bifrost_net::Error::RetryBudgetExhausted {
            last_status: Some(status),
            ..
        } => format!("{service} error {status}: retry budget exhausted"),
        other => format!("{service} request failed: {other}"),
    }
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
    fn derives_beta_base_from_v1_base() {
        let client = GraphClient::with_api_base("https://example.test/v1.0/", "token");
        assert_eq!(client.api_beta_base(), "https://example.test/beta");
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
