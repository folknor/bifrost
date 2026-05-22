use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_net::{
    AccessToken, AccountId, AccountNet, AccountSpec, Net, RateLimit, RequestBuilder, Response,
    RetryPolicy, StaticTokenSource, TokenSource,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{Error, Result};

pub const GMAIL_API_BASE: &str = "https://www.googleapis.com/gmail/v1/users/me";

#[derive(Clone)]
pub struct GmailClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    net: AccountNet,
    api_base: String,
    token_source: StaticTokenSource,
}

impl GmailClient {
    pub fn new(access_token: impl Into<String>) -> Self {
        Self::with_api_base(GMAIL_API_BASE, access_token)
    }

    pub fn with_api_base(api_base: impl Into<String>, access_token: impl Into<String>) -> Self {
        let token_source = StaticTokenSource::new(access_token, None);
        let net = default_account_net("gmail", "www.googleapis.com", token_source.clone());
        Self::with_account_net(net, api_base, token_source)
    }

    pub fn with_account_net(
        net: AccountNet,
        api_base: impl Into<String>,
        token_source: StaticTokenSource,
    ) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                net,
                api_base: api_base.into().trim_end_matches('/').to_string(),
                token_source,
            }),
        }
    }

    pub fn account_net(&self) -> &AccountNet {
        &self.inner.net
    }

    pub fn api_base(&self) -> &str {
        &self.inner.api_base
    }

    pub async fn access_token(&self) -> String {
        self.inner.token_source.token().as_str().to_string()
    }

    pub async fn set_access_token(&self, access_token: impl Into<String>) {
        self.inner
            .token_source
            .set(AccessToken::new(access_token, None));
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.api_url(path);
        self.request::<T, ()>(&url, "GET", None).await
    }

    pub async fn get_absolute<T: DeserializeOwned>(&self, url: &str) -> Result<T> {
        self.request::<T, ()>(url, "GET", None).await
    }

    pub async fn post<T: DeserializeOwned, B: Serialize>(&self, path: &str, body: &B) -> Result<T> {
        let url = self.api_url(path);
        self.request(&url, "POST", Some(body)).await
    }

    pub async fn post_absolute<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T> {
        self.request(url, "POST", Some(body)).await
    }

    pub async fn put<T: DeserializeOwned, B: Serialize>(&self, path: &str, body: &B) -> Result<T> {
        let url = self.api_url(path);
        self.request(&url, "PUT", Some(body)).await
    }

    pub async fn put_absolute<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T> {
        self.request(url, "PUT", Some(body)).await
    }

    pub async fn patch<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let url = self.api_url(path);
        self.request(&url, "PATCH", Some(body)).await
    }

    pub async fn patch_absolute<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T> {
        self.request(url, "PATCH", Some(body)).await
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let url = self.api_url(path);
        self.delete_absolute(&url, "Gmail API").await
    }

    pub async fn delete_absolute(&self, url: &str, service: &str) -> Result<()> {
        let response = self.execute(url, "DELETE", None::<&()>).await?;
        check_response_status(response, service).await
    }

    pub async fn post_no_content<B: Serialize>(&self, path: &str, body: &B) -> Result<()> {
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
            _ => {
                return Err(Error::InvalidInput(format!(
                    "unsupported HTTP method: {method}"
                )));
            }
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
        service: &str,
    ) -> Result<Response> {
        builder
            .send()
            .await
            .map_err(|error| Error::from_net(service, error))
    }
}

async fn parse_json_response<T: DeserializeOwned>(response: Response, service: &str) -> Result<T> {
    let status = response.status();
    let body = response_body_string(response);
    if !status.is_success() {
        return Err(Error::status(service, status, body));
    }

    serde_json::from_str(&body).map_err(Error::from)
}

async fn check_response_status(response: Response, service: &str) -> Result<()> {
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }

    let body = response_body_string(response);
    Err(Error::status(service, status, body))
}

fn response_body_string(response: Response) -> String {
    String::from_utf8_lossy(response.body.as_ref()).into_owned()
}

fn default_account_net(
    account: impl Into<String>,
    host: impl Into<String>,
    token_source: StaticTokenSource,
) -> AccountNet {
    static NEXT_DEFAULT_ACCOUNT_ID: AtomicU64 = AtomicU64::new(1);
    let id = NEXT_DEFAULT_ACCOUNT_ID.fetch_add(1, Ordering::Relaxed);
    let net = Net::shared_default();
    let token_source: Arc<dyn TokenSource> = Arc::new(token_source);
    net.attach_account(
        AccountId(format!("{}-{id}", account.into())),
        AccountSpec {
            hosts: vec![RateLimit {
                host: host.into(),
                quota_per_second: 250.0,
                cost_default: 1,
                burst: 250,
            }],
            token_source,
            default_retry: RetryPolicy::default(),
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
        assert_eq!(client.access_token().await, "token");
    }
}
