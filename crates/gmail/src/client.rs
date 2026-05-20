use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub const GMAIL_API_BASE: &str = "https://www.googleapis.com/gmail/v1/users/me";

const MAX_RETRY_ATTEMPTS: u32 = 3;
const INITIAL_BACKOFF_MS: u64 = 1000;

#[derive(Clone)]
pub struct GmailClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    http: reqwest::Client,
    api_base: String,
    access_token: RwLock<String>,
}

impl GmailClient {
    pub fn new(access_token: impl Into<String>) -> Self {
        Self::with_http_client(reqwest::Client::new(), GMAIL_API_BASE, access_token)
    }

    pub fn with_api_base(api_base: impl Into<String>, access_token: impl Into<String>) -> Self {
        Self::with_http_client(reqwest::Client::new(), api_base, access_token)
    }

    pub fn with_http_client(
        http: reqwest::Client,
        api_base: impl Into<String>,
        access_token: impl Into<String>,
    ) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                http,
                api_base: api_base.into().trim_end_matches('/').to_string(),
                access_token: RwLock::new(access_token.into()),
            }),
        }
    }

    pub fn http_client(&self) -> &reqwest::Client {
        &self.inner.http
    }

    pub fn api_base(&self) -> &str {
        &self.inner.api_base
    }

    pub async fn access_token(&self) -> String {
        self.inner.access_token.read().await.clone()
    }

    pub async fn set_access_token(&self, access_token: impl Into<String>) {
        *self.inner.access_token.write().await = access_token.into();
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, String> {
        let url = self.api_url(path);
        self.request::<T, ()>(&url, "GET", None).await
    }

    pub async fn get_absolute<T: DeserializeOwned>(&self, url: &str) -> Result<T, String> {
        self.request::<T, ()>(url, "GET", None).await
    }

    pub async fn post<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, String> {
        let url = self.api_url(path);
        self.request(&url, "POST", Some(body)).await
    }

    pub async fn post_absolute<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T, String> {
        self.request(url, "POST", Some(body)).await
    }

    pub async fn put<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, String> {
        let url = self.api_url(path);
        self.request(&url, "PUT", Some(body)).await
    }

    pub async fn put_absolute<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T, String> {
        self.request(url, "PUT", Some(body)).await
    }

    pub async fn patch<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, String> {
        let url = self.api_url(path);
        self.request(&url, "PATCH", Some(body)).await
    }

    pub async fn patch_absolute<T: DeserializeOwned, B: Serialize>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T, String> {
        self.request(url, "PATCH", Some(body)).await
    }

    pub async fn delete(&self, path: &str) -> Result<(), String> {
        let url = self.api_url(path);
        self.delete_absolute(&url, "Gmail API").await
    }

    pub async fn delete_absolute(&self, url: &str, service: &str) -> Result<(), String> {
        let access_token = self.access_token().await;
        let response = self
            .execute_with_retry(url, "DELETE", None::<&()>, &access_token)
            .await?;
        check_response_status(response, service).await
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
    ) -> Result<T, String> {
        let access_token = self.access_token().await;
        let response = self
            .execute_with_retry(url, method, body, &access_token)
            .await?;
        parse_json_response(response, "Gmail API").await
    }

    async fn execute_with_retry<B: Serialize>(
        &self,
        url: &str,
        method: &str,
        body: Option<&B>,
        access_token: &str,
    ) -> Result<reqwest::Response, String> {
        let mut last_response = None;

        for attempt in 0..MAX_RETRY_ATTEMPTS {
            let response = self.execute_once(url, method, body, access_token).await?;

            if response.status().as_u16() != 429 {
                return Ok(response);
            }

            last_response = Some(response);
            if attempt == MAX_RETRY_ATTEMPTS - 1 {
                break;
            }

            let delay = retry_delay(last_response.as_ref(), attempt);
            tokio::time::sleep(delay).await;
        }

        last_response.ok_or_else(|| "No response received".to_string())
    }

    async fn execute_once<B: Serialize>(
        &self,
        url: &str,
        method: &str,
        body: Option<&B>,
        access_token: &str,
    ) -> Result<reqwest::Response, String> {
        let mut builder = match method {
            "GET" => self.inner.http.get(url),
            "POST" => self.inner.http.post(url),
            "PUT" => self.inner.http.put(url),
            "PATCH" => self.inner.http.patch(url),
            "DELETE" => self.inner.http.delete(url),
            _ => return Err(format!("Unsupported HTTP method: {method}")),
        };

        builder = builder
            .header("Authorization", format!("Bearer {access_token}"))
            .header("Content-Type", "application/json");

        if let Some(b) = body {
            builder = builder.json(b);
        }

        builder
            .send()
            .await
            .map_err(|e| format!("Gmail API request failed: {e}"))
    }
}

async fn parse_json_response<T: DeserializeOwned>(
    response: reqwest::Response,
    service: &str,
) -> Result<T, String> {
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("{service} error {status}: {body}"));
    }

    response
        .json()
        .await
        .map_err(|e| format!("{service} JSON parse failed: {e}"))
}

async fn check_response_status(response: reqwest::Response, service: &str) -> Result<(), String> {
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }

    let body = response.text().await.unwrap_or_default();
    Err(format!("{service} error {status}: {body}"))
}

fn retry_delay(response: Option<&reqwest::Response>, attempt: u32) -> Duration {
    if let Some(delay) = response
        .and_then(|r| r.headers().get(reqwest::header::RETRY_AFTER))
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
    {
        return Duration::from_secs(delay);
    }

    Duration::from_millis(INITIAL_BACKOFF_MS * u64::from(attempt + 1))
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
