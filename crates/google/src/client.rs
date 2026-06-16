use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_net::{
    AccountId, AccountNet, AccountSpec, Net, RateLimit, RequestBuilder, Response, RetryPolicy,
    StaticTokenSource, TokenSource,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{Error, Result};

const GMAIL_API_BASE: &str = "https://www.googleapis.com/gmail/v1/users/me";
const GOOGLE_API_QUOTA_PER_SECOND: f64 = 250.0;
const GOOGLE_API_BURST: u32 = 250;
const PEOPLE_API_QUOTA_PER_SECOND: f64 = 1.5;
const PEOPLE_API_BURST: u32 = 30;

#[derive(Clone)]
pub(crate) struct GmailClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    net: AccountNet,
    parent_net: Net,
    api_base: String,
    token_source: Arc<dyn TokenSource>,
}

impl GmailClient {
    pub(crate) fn new(access_token: impl Into<String>) -> Self {
        Self::with_api_base(GMAIL_API_BASE, access_token)
    }

    fn with_api_base(api_base: impl Into<String>, access_token: impl Into<String>) -> Self {
        let token_source: Arc<dyn TokenSource> =
            Arc::new(StaticTokenSource::new(access_token, None));
        Self::with_api_base_and_source(api_base, token_source)
    }

    // Source-accepting constructor. ratatoskr hands in a shared
    // `Arc<dyn TokenSource>` so a refreshed-and-persisted token is read
    // live at every wire authentication without reopening the client.
    pub(crate) fn with_source(source: Arc<dyn TokenSource>) -> Self {
        Self::with_api_base_and_source(GMAIL_API_BASE, source)
    }

    fn with_api_base_and_source(
        api_base: impl Into<String>,
        token_source: Arc<dyn TokenSource>,
    ) -> Self {
        let parent_net = Net::shared_default();
        let net = default_account_net(
            &parent_net,
            AccountId("gmail-direct".to_string()),
            "www.googleapis.com",
            Arc::clone(&token_source),
        );
        Self {
            inner: Arc::new(ClientInner {
                net,
                parent_net,
                api_base: api_base.into().trim_end_matches('/').to_string(),
                token_source,
            }),
        }
    }

    pub(crate) fn for_account(&self, account_id: AccountId) -> Self {
        let net = default_account_net(
            &self.inner.parent_net,
            account_id,
            "www.googleapis.com",
            Arc::clone(&self.inner.token_source),
        );
        Self {
            inner: Arc::new(ClientInner {
                net,
                parent_net: self.inner.parent_net.clone(),
                api_base: self.inner.api_base.clone(),
                token_source: Arc::clone(&self.inner.token_source),
            }),
        }
    }

    pub(crate) fn account_net(&self) -> &AccountNet {
        &self.inner.net
    }

    pub(crate) fn api_base(&self) -> &str {
        &self.inner.api_base
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

    pub(crate) async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.api_url(path);
        self.request::<T, ()>(&url, "GET", None).await
    }

    pub(crate) async fn post<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let url = self.api_url(path);
        self.request(&url, "POST", Some(body)).await
    }

    pub(crate) async fn put<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let url = self.api_url(path);
        self.request(&url, "PUT", Some(body)).await
    }

    pub(crate) async fn patch<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let url = self.api_url(path);
        self.request(&url, "PATCH", Some(body)).await
    }

    pub(crate) async fn delete(&self, path: &str) -> Result<()> {
        let url = self.api_url(path);
        self.delete_absolute(&url, "Gmail API").await
    }

    async fn delete_absolute(&self, url: &str, service: &str) -> Result<()> {
        let response = self.execute(url, "DELETE", None::<&()>).await?;
        check_response_status(response, service).await
    }

    pub(crate) async fn post_no_content<B: Serialize>(&self, path: &str, body: &B) -> Result<()> {
        let url = self.api_url(path);
        let response = self.execute(&url, "POST", Some(body)).await?;
        check_response_status(response, "Gmail API").await
    }

    fn api_url(&self, path: &str) -> String {
        if path.starts_with("http://") || path.starts_with("https://") {
            path.to_string()
        } else if path.starts_with('/') {
            format!("{}{}", self.inner.api_base, path)
        } else {
            format!("{}/{}", self.inner.api_base, path)
        }
    }

    async fn request<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &str,
        method: &str,
        body: Option<&B>,
    ) -> Result<T> {
        let response = self.execute(url, method, body).await?;
        parse_json_response(response, "Gmail API").await
    }

    async fn execute<B: Serialize>(
        &self,
        url: &str,
        method: &str,
        body: Option<&B>,
    ) -> Result<Response> {
        let mut builder = match method {
            "GET" => self.inner.net.get(url),
            "POST" => self.inner.net.post(url),
            "PUT" => self.inner.net.put(url),
            "PATCH" => self.inner.net.patch(url),
            "DELETE" => self.inner.net.delete(url),
            // gmail-N3: every internal caller routes through the typed
            // `get` / `post` / `put` / `patch` / `delete` wrappers, so
            // this branch is unreachable. Previously we synthesized a
            // `Discover`-flavored placeholder error; replace with the
            // panic so a future caller that adds a new method gets a
            // hard failure instead of misclassified telemetry.
            other => unreachable!("GmailClient::execute called with unsupported method {other}"),
        };

        builder = builder.header("Content-Type", "application/json");

        if let Some(b) = body {
            builder = builder.json(b);
        }

        builder.send().await.map_err(Error::from)
    }

    pub(crate) async fn execute_builder(
        &self,
        builder: RequestBuilder,
        _service: &str,
    ) -> Result<Response> {
        // Net errors are preserved verbatim through `Error::Net(_)` so
        // the account-side translation boundary can inspect transmission
        // state, retry-after, and other forensic evidence. Service
        // string is unused now that we no longer flatten errors here.
        builder.send().await.map_err(Error::from)
    }
}

async fn parse_json_response<T: DeserializeOwned>(response: Response, _service: &str) -> Result<T> {
    let status = response.status();
    if !status.is_success() {
        let headers = crate::error::GmailResponseHeaders::from_headers(response.headers());
        return Err(Error::response_from_parts(
            crate::error::GmailService::GmailApi,
            status.as_u16(),
            headers,
            response.body,
        ));
    }
    serde_json::from_slice(response.body.as_ref()).map_err(|source| Error::JsonDecode {
        service: crate::error::GmailService::GmailApi,
        source,
    })
}

async fn check_response_status(response: Response, _service: &str) -> Result<()> {
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    let headers = crate::error::GmailResponseHeaders::from_headers(response.headers());
    Err(Error::response_from_parts(
        crate::error::GmailService::GmailApi,
        status.as_u16(),
        headers,
        response.body,
    ))
}

fn default_account_net(
    net: &Net,
    account: AccountId,
    host: impl Into<String>,
    token_source: Arc<dyn TokenSource>,
) -> AccountNet {
    net.attach_account(
        uniquify_account_id(account),
        AccountSpec {
            hosts: vec![
                RateLimit {
                    host: host.into(),
                    quota_per_second: GOOGLE_API_QUOTA_PER_SECOND,
                    cost_default: 1,
                    burst: GOOGLE_API_BURST,
                },
                RateLimit {
                    host: "people.googleapis.com".to_string(),
                    quota_per_second: PEOPLE_API_QUOTA_PER_SECOND,
                    cost_default: 1,
                    burst: PEOPLE_API_BURST,
                },
            ],
            token_source,
            default_retry: RetryPolicy::default(),
        },
    )
}

fn uniquify_account_id(account: AccountId) -> AccountId {
    if account.0 == "gmail-direct" {
        static NEXT_DEFAULT_ACCOUNT_ID: AtomicU64 = AtomicU64::new(1);
        let id = NEXT_DEFAULT_ACCOUNT_ID.fetch_add(1, Ordering::Relaxed);
        return AccountId(format!("gmail-direct-{id}"));
    }
    account
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn trims_api_base() {
        let client = GmailClient::with_api_base("https://example.test/base/", "token");
        assert_eq!(client.api_base(), "https://example.test/base");
    }

    #[tokio::test]
    async fn rotated_token_source_is_read() {
        use bifrost_net::AccessToken;

        let source = StaticTokenSource::new("old-token", None);
        let client = GmailClient::with_source(Arc::new(source.clone()));
        assert_eq!(client.access_token().await, "old-token");
        source.set(AccessToken::new("new-token", None));
        assert_eq!(client.access_token().await, "new-token");
    }
}
