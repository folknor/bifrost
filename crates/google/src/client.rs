use std::sync::Arc;

use bifrost_net::{
    AccountId, AccountNet, AccountSpec, Net, RateLimit, RequestBuilder, Response,
    StaticTokenSource, TokenSource,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{Error, Result};

const GMAIL_API_BASE: &str = "https://www.googleapis.com/gmail/v1/users/me";
const PEOPLE_API_BASE: &str = "https://people.googleapis.com/v1";
const GOOGLE_API_QUOTA_PER_SECOND: f64 = 250.0;
const GOOGLE_API_BURST: u32 = 250;
const PEOPLE_API_QUOTA_PER_SECOND: f64 = 1.5;
const PEOPLE_API_BURST: u32 = 30;

#[derive(Clone)]
pub(crate) struct GmailClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    net: Option<AccountNet>,
    parent_net: Net,
    api_base: String,
    // People/contacts API base. A third, independent Google surface:
    // Gmail mail lives on www.googleapis.com and Calendar on
    // www.googleapis.com/calendar, but People contacts + directory live
    // on people.googleapis.com, so this base is threaded separately from
    // `api_base` (the Gmail mail base) rather than reusing it. Defaults to
    // the production People base; a harness redirects it independently.
    people_base: String,
    token_source: Arc<dyn TokenSource>,
}

impl GmailClient {
    #[cfg(test)]
    pub(crate) fn with_account_net(api_base: impl Into<String>, net: AccountNet) -> Self {
        let token_source: Arc<dyn TokenSource> = Arc::new(StaticTokenSource::new("token", None));
        Self {
            inner: Arc::new(ClientInner {
                net: Some(net),
                parent_net: Net::shared_default(),
                api_base: api_base.into().trim_end_matches('/').to_string(),
                people_base: PEOPLE_API_BASE.to_string(),
                token_source,
            }),
        }
    }

    pub(crate) fn new(access_token: impl Into<String>) -> Self {
        Self::with_api_base(GMAIL_API_BASE, access_token)
    }

    // pub(crate): the factory's `from_access_token_with_api_base` test seam
    // builds a bearer-token client against a redirected Gmail base.
    pub(crate) fn with_api_base(
        api_base: impl Into<String>,
        access_token: impl Into<String>,
    ) -> Self {
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

    // pub(crate): the factory's `from_token_source_with_api_base` test seam
    // routes a refresher-backed client at a redirected Gmail base, mirroring
    // bifrost-graph's `with_source`.
    pub(crate) fn with_api_base_and_source(
        api_base: impl Into<String>,
        token_source: Arc<dyn TokenSource>,
    ) -> Self {
        let parent_net = Net::shared_default();
        Self {
            inner: Arc::new(ClientInner {
                net: None,
                parent_net,
                api_base: api_base.into().trim_end_matches('/').to_string(),
                people_base: PEOPLE_API_BASE.to_string(),
                token_source,
            }),
        }
    }

    // pub(crate): the factory's `with_people_api_base` test seam points the
    // People/contacts base at a mock endpoint instead of
    // people.googleapis.com, independently of the Gmail mail base. Returns a
    // fresh client sharing the same net/token source with only the People
    // base swapped.
    pub(crate) fn with_people_base(&self, people_base: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                net: self.inner.net.clone(),
                parent_net: self.inner.parent_net.clone(),
                api_base: self.inner.api_base.clone(),
                people_base: people_base.into().trim_end_matches('/').to_string(),
                token_source: Arc::clone(&self.inner.token_source),
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
                net: Some(net),
                parent_net: self.inner.parent_net.clone(),
                api_base: self.inner.api_base.clone(),
                people_base: self.inner.people_base.clone(),
                token_source: Arc::clone(&self.inner.token_source),
            }),
        }
    }

    pub(crate) fn account_net(&self) -> &AccountNet {
        self.inner
            .net
            .as_ref()
            .expect("GmailClient must be scoped with for_account before issuing requests")
    }

    pub(crate) fn detach_account(&self) {
        if let Some(account_net) = &self.inner.net {
            account_net.detach();
        }
    }

    pub(crate) fn api_base(&self) -> &str {
        &self.inner.api_base
    }

    pub(crate) fn people_base(&self) -> &str {
        &self.inner.people_base
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

    pub(crate) fn api_url(&self, path: &str) -> String {
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

    pub(crate) async fn execute<B: Serialize>(
        &self,
        url: &str,
        method: &str,
        body: Option<&B>,
    ) -> Result<Response> {
        let mut builder = match method {
            "GET" => self.account_net().get(url),
            "POST" => self.account_net().post(url),
            "PUT" => self.account_net().put(url),
            "PATCH" => self.account_net().patch(url),
            "DELETE" => self.account_net().delete(url),
            // Every internal caller routes through the typed
            // `get` / `post` / `put` / `patch` / `delete` wrappers, so
            // this branch is unreachable; the panic gives a future
            // caller that adds a new method a hard failure instead of
            // misclassified telemetry.
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

pub(crate) async fn parse_json_response<T: DeserializeOwned>(
    response: Response,
    _service: &str,
) -> Result<T> {
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
        account,
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
            ..AccountSpec::new(Some(token_source))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn trims_api_base() {
        let client = GmailClient::with_api_base("https://example.test/base/", "token");
        assert_eq!(client.api_base(), "https://example.test/base");
        assert!(
            client.inner.net.is_none(),
            "constructing a factory client must not attach a throwaway account"
        );
    }

    #[tokio::test]
    async fn people_base_defaults_and_overrides_independently() {
        let client = GmailClient::with_api_base("https://example.test/gmail", "token");
        // People base defaults to production, independent of the Gmail base.
        assert_eq!(client.people_base(), PEOPLE_API_BASE);

        // Overriding People leaves the Gmail base untouched and trims the
        // trailing slash like the Gmail base does.
        let redirected = client.with_people_base("https://people.mock.test/v1/");
        assert_eq!(redirected.people_base(), "https://people.mock.test/v1");
        assert_eq!(redirected.api_base(), "https://example.test/gmail");
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

    #[test]
    fn account_scope_is_attached_only_by_for_account() {
        let client = GmailClient::new("token");
        let opened = client.for_account(AccountId("client-test".to_string()));
        assert!(opened.inner.net.is_some());
        opened.detach_account();
    }
}
