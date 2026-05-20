use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{Mutex, RwLock, Semaphore};

use crate::folder_mapper::FolderMap;

pub const GRAPH_API_BASE: &str = "https://graph.microsoft.com/v1.0";
pub const GRAPH_API_BETA: &str = "https://graph.microsoft.com/beta";

const MAX_RETRY_ATTEMPTS: u32 = 3;
const INITIAL_BACKOFF_MS: u64 = 1000;
const CONCURRENCY_LIMIT: usize = 3;

#[derive(Clone)]
pub struct GraphClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    http: reqwest::Client,
    api_base: String,
    api_beta_base: String,
    access_token: Arc<RwLock<String>>,
    mailbox_id: Option<String>,
    semaphore: Arc<Semaphore>,
    folder_map: RwLock<Option<(FolderMap, Instant)>>,
    category_lock: Mutex<()>,
}

impl GraphClient {
    pub fn new(access_token: impl Into<String>) -> Self {
        Self::with_http_client(
            reqwest::Client::new(),
            GRAPH_API_BASE,
            GRAPH_API_BETA,
            access_token,
        )
    }

    pub fn with_api_base(api_base: impl Into<String>, access_token: impl Into<String>) -> Self {
        let api_base = api_base.into();
        let api_beta_base =
            derive_beta_base(&api_base).unwrap_or_else(|| GRAPH_API_BETA.to_string());
        Self::with_http_client(
            reqwest::Client::new(),
            api_base,
            api_beta_base,
            access_token,
        )
    }

    pub fn with_api_bases(
        api_base: impl Into<String>,
        api_beta_base: impl Into<String>,
        access_token: impl Into<String>,
    ) -> Self {
        Self::with_http_client(
            reqwest::Client::new(),
            api_base,
            api_beta_base,
            access_token,
        )
    }

    pub fn with_http_client(
        http: reqwest::Client,
        api_base: impl Into<String>,
        api_beta_base: impl Into<String>,
        access_token: impl Into<String>,
    ) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                http,
                api_base: trim_base(api_base.into()),
                api_beta_base: trim_base(api_beta_base.into()),
                access_token: Arc::new(RwLock::new(access_token.into())),
                mailbox_id: None,
                semaphore: Arc::new(Semaphore::new(CONCURRENCY_LIMIT)),
                folder_map: RwLock::new(None),
                category_lock: Mutex::new(()),
            }),
        }
    }

    pub fn http_client(&self) -> &reqwest::Client {
        &self.inner.http
    }

    pub fn api_base(&self) -> &str {
        &self.inner.api_base
    }

    pub fn api_beta_base(&self) -> &str {
        &self.inner.api_beta_base
    }

    pub async fn access_token(&self) -> String {
        self.inner.access_token.read().await.clone()
    }

    pub async fn set_access_token(&self, access_token: impl Into<String>) {
        *self.inner.access_token.write().await = access_token.into();
    }

    pub fn api_path_prefix(&self) -> String {
        match &self.inner.mailbox_id {
            Some(id) => format!("/users/{}", urlencoding::encode(id)),
            None => "/me".to_string(),
        }
    }

    pub fn for_shared_mailbox(&self, mailbox_id: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                http: self.inner.http.clone(),
                api_base: self.inner.api_base.clone(),
                api_beta_base: self.inner.api_beta_base.clone(),
                access_token: Arc::clone(&self.inner.access_token),
                mailbox_id: Some(mailbox_id.into()),
                semaphore: Arc::clone(&self.inner.semaphore),
                folder_map: RwLock::new(None),
                category_lock: Mutex::new(()),
            }),
        }
    }

    pub fn is_shared_mailbox(&self) -> bool {
        self.inner.mailbox_id.is_some()
    }

    pub fn mailbox_id(&self) -> Option<&str> {
        self.inner.mailbox_id.as_deref()
    }

    pub async fn folder_map(&self) -> Option<FolderMap> {
        self.inner
            .folder_map
            .read()
            .await
            .as_ref()
            .map(|(map, _)| map.clone())
    }

    pub async fn set_folder_map(&self, map: FolderMap) {
        *self.inner.folder_map.write().await = Some((map, Instant::now()));
    }

    pub async fn folder_map_age(&self) -> Option<Duration> {
        self.inner
            .folder_map
            .read()
            .await
            .as_ref()
            .map(|(_, instant)| instant.elapsed())
    }

    pub async fn clear_folder_map(&self) {
        *self.inner.folder_map.write().await = None;
    }

    pub async fn lock_categories(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.inner.category_lock.lock().await
    }

    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, String> {
        let url = self.api_url(path);
        self.request::<T, ()>(&url, "GET", None).await
    }

    pub async fn get_bytes(&self, path: &str) -> Result<Vec<u8>, String> {
        let url = self.api_url(path);
        let access_token = self.access_token().await;
        let response = self
            .execute_with_retry(&url, "GET", None::<&()>, &access_token)
            .await?;
        parse_bytes_response(response, "Graph API").await
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

    pub async fn post_beta<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, String> {
        let url = self.api_beta_url(path);
        self.request(&url, "POST", Some(body)).await
    }

    pub async fn post_no_content<B: Serialize>(
        &self,
        path: &str,
        body: Option<&B>,
    ) -> Result<(), String> {
        let url = self.api_url(path);
        let access_token = self.access_token().await;
        let response = self
            .execute_with_retry(&url, "POST", body, &access_token)
            .await?;
        check_response_status(response, "Graph API").await
    }

    pub async fn patch<B: Serialize>(&self, path: &str, body: &B) -> Result<(), String> {
        let url = self.api_url(path);
        let access_token = self.access_token().await;
        let response = self
            .execute_with_retry(&url, "PATCH", Some(body), &access_token)
            .await?;
        check_response_status(response, "Graph API").await
    }

    pub async fn delete(&self, path: &str) -> Result<(), String> {
        let url = self.api_url(path);
        let access_token = self.access_token().await;
        let response = self
            .execute_with_retry(&url, "DELETE", None::<&()>, &access_token)
            .await?;
        check_response_status(response, "Graph API").await
    }

    pub async fn put_bytes_range(
        &self,
        url: &str,
        data: &[u8],
        start: usize,
        end: usize,
        total: usize,
    ) -> Result<reqwest::Response, String> {
        let response = self
            .inner
            .http
            .put(url)
            .header("Content-Range", format!("bytes {start}-{end}/{total}"))
            .header("Content-Length", data.len().to_string())
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| format!("Graph upload request failed: {e}"))?;

        if response.status().is_success() {
            Ok(response)
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Err(format!("Graph upload error {status}: {body}"))
        }
    }

    pub async fn post_batch(
        &self,
        batch: &crate::types::BatchRequest,
    ) -> Result<crate::types::BatchResponse, String> {
        self.post("/$batch", batch).await
    }

    fn api_url(&self, path: &str) -> String {
        build_url(&self.inner.api_base, path)
    }

    fn api_beta_url(&self, path: &str) -> String {
        build_url(&self.inner.api_beta_base, path)
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
        parse_json_response(response, "Graph API").await
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

            if !is_retryable(response.status()) {
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
        let _permit = self
            .inner
            .semaphore
            .acquire()
            .await
            .map_err(|_| "Graph request semaphore closed".to_string())?;

        let mut builder = match method {
            "GET" => self.inner.http.get(url),
            "POST" => self.inner.http.post(url),
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
            .map_err(|e| format!("Graph API request failed: {e}"))
    }
}

fn trim_base(base: String) -> String {
    base.trim_end_matches('/').to_string()
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

fn is_retryable(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 500 | 502 | 503 | 504)
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

async fn parse_bytes_response(
    response: reqwest::Response,
    service: &str,
) -> Result<Vec<u8>, String> {
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("{service} error {status}: {body}"));
    }

    response
        .bytes()
        .await
        .map(|bytes| bytes.to_vec())
        .map_err(|e| format!("{service} body read failed: {e}"))
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
}
