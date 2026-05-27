use std::{
    collections::HashSet,
    net::IpAddr,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bifrost_net::{AccessToken, AccountId as NetAccountId, StaticTokenSource};
use reqwest::header;

use crate::{
    blob,
    core::{
        request::{self, Request},
        response,
        session::{Session, URLPart},
        transport::HttpTransport,
    },
    transport_reqwest::ReqwestTransport,
};

const DEFAULT_TIMEOUT_MS: u64 = 10 * 1000;

#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum Credentials {
    Basic(String),
    Bearer(StaticTokenSource),
}

#[derive(Debug, Clone)]
pub(crate) enum Authorization {
    Basic(String),
    Bearer(StaticTokenSource),
}

impl Authorization {
    fn from_credentials(credentials: Credentials) -> Self {
        match credentials {
            Credentials::Basic(value) => Self::Basic(value),
            Credentials::Bearer(token) => Self::Bearer(token),
        }
    }

    pub(crate) fn header_value(&self) -> String {
        match self {
            Self::Basic(value) => format!("Basic {value}"),
            Self::Bearer(source) => format!("Bearer {}", source.token().as_str()),
        }
    }

    pub(crate) fn account_token_source(&self) -> StaticTokenSource {
        match self {
            // AccountNet requires a token source even when JMAP is
            // using Basic auth. This source is intentionally inert;
            // the request builder disables bearer injection for Basic.
            Self::Basic(_) => StaticTokenSource::new("", None),
            Self::Bearer(source) => source.clone(),
        }
    }

    pub(crate) fn uses_bearer_pipeline(&self) -> bool {
        matches!(self, Self::Bearer(_))
    }

    pub(crate) fn set_bearer_token(&self, token: AccessToken) -> bool {
        match self {
            Self::Basic(_) => false,
            Self::Bearer(source) => {
                source.set(token);
                true
            }
        }
    }
}

/// Internal shared state of a [`Client`]. Stored behind an `Arc` so the
/// client itself is cheap to clone and pass around.
pub(crate) struct ClientInner<T: HttpTransport = ReqwestTransport> {
    transport: T,
    session: std::sync::Mutex<Arc<Session>>,
    session_url: String,
    api_url: String,
    session_updated: AtomicBool,

    upload_url: Vec<URLPart<blob::URLParameter>>,
    download_url: Vec<URLPart<blob::URLParameter>>,
    event_source_url: Vec<URLPart<crate::event_source::URLParameter>>,

    default_account_id: crate::core::id::AccountId,
    timeout: Duration,
    #[cfg(feature = "websockets")]
    pub(crate) accept_invalid_certs: bool,

    #[cfg(feature = "websockets")]
    pub(crate) authorization: Authorization,
    #[cfg(feature = "websockets")]
    pub(crate) ws: tokio::sync::Mutex<Option<crate::client_ws::WsStream>>,
}

/// A JMAP client. Cheap to clone - wraps an `Arc<ClientInner>` internally.
///
/// Cloning a `Client` shares the underlying transport, session state, and
/// (when enabled) WebSocket connection. There is no public lifetime
/// parameter, so `Client` can be stored in long-lived structs and moved
/// across tasks freely.
pub(crate) struct Client<T: HttpTransport = ReqwestTransport> {
    inner: Arc<ClientInner<T>>,
}

impl<T: HttpTransport> Clone for Client<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: HttpTransport> Deref for Client<T> {
    type Target = ClientInner<T>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub(crate) struct ClientBuilder {
    credentials: Option<Credentials>,
    net_account_id: NetAccountId,
    trusted_hosts: HashSet<String>,
    forwarded_for: Option<String>,
    accept_invalid_certs: bool,
    timeout: Duration,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    pub(crate) fn new() -> Self {
        Self {
            credentials: None,
            net_account_id: NetAccountId("jmap".to_string()),
            trusted_hosts: HashSet::new(),
            timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
            forwarded_for: None,
            accept_invalid_certs: false,
        }
    }

    pub(crate) fn credentials(mut self, credentials: impl Into<Credentials>) -> Self {
        self.credentials = Some(credentials.into());
        self
    }

    pub(crate) fn net_account_id(mut self, account_id: NetAccountId) -> Self {
        self.net_account_id = account_id;
        self
    }

    pub(crate) fn accept_invalid_certs(mut self, accept_invalid_certs: bool) -> Self {
        self.accept_invalid_certs = accept_invalid_certs;
        self
    }

    pub(crate) fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub(crate) fn follow_redirects(
        mut self,
        trusted_hosts: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.trusted_hosts
            .extend(trusted_hosts.into_iter().map(std::convert::Into::into));
        self
    }

    pub(crate) fn forwarded_for(mut self, ip: IpAddr) -> Self {
        self.forwarded_for = Some(match ip {
            IpAddr::V4(addr) => format!("for={addr}"),
            IpAddr::V6(addr) => format!("for=\"{addr}\""),
        });
        self
    }

    pub(crate) async fn connect(self, url: &str) -> crate::Result<Client> {
        let credentials = self.credentials.ok_or_else(|| {
            crate::core::transport::TransportError::new(
                "Missing credentials - call .credentials() before .connect()",
            )
        })?;
        let authorization = Authorization::from_credentials(credentials);
        let mut headers = header::HeaderMap::new();
        if let Some(forwarded_for) = self.forwarded_for {
            headers.insert(
                header::FORWARDED,
                header::HeaderValue::from_str(&forwarded_for).map_err(|e| {
                    crate::core::transport::TransportError::with_source(
                        "Invalid forwarded-for header",
                        e,
                    )
                })?,
            );
        }

        let trusted_hosts = Arc::new(self.trusted_hosts);

        let transport = ReqwestTransport::new(
            headers.clone(),
            authorization.clone(),
            self.net_account_id,
            self.timeout,
            self.accept_invalid_certs,
            Arc::clone(&trusted_hosts),
        )
        .map_err(crate::Error::from)?;

        let session_url = format!("{url}/.well-known/jmap");
        let session_bytes = transport
            .get_session(&session_url)
            .await
            .map_err(crate::Error::from)?;
        let session: Session = serde_json::from_slice(&session_bytes)?;

        let default_account_id = session
            .primary_accounts()
            .next()
            .map(|a| crate::core::id::AccountId::new(a.1.clone()))
            .unwrap_or_else(|| crate::core::id::AccountId::new(""));

        Ok(Client {
            inner: Arc::new(ClientInner {
                download_url: URLPart::parse(session.download_url())?,
                upload_url: URLPart::parse(session.upload_url())?,
                event_source_url: URLPart::parse(session.event_source_url())?,
                api_url: session.api_url().to_string(),
                session: std::sync::Mutex::new(Arc::new(session)),
                session_url,
                session_updated: true.into(),
                timeout: self.timeout,
                transport,
                default_account_id,
                #[cfg(feature = "websockets")]
                accept_invalid_certs: self.accept_invalid_certs,
                #[cfg(feature = "websockets")]
                authorization,
                #[cfg(feature = "websockets")]
                ws: None.into(),
            }),
        })
    }
}

/// Default client using reqwest. Use `Client::new()` / `ClientBuilder` to construct.
pub(crate) type DefaultClient = Client<ReqwestTransport>;

impl Client {
    /// Create a new client builder (uses reqwest transport by default).
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new() -> ClientBuilder {
        ClientBuilder::new()
    }
}

impl<T: HttpTransport> Client<T> {
    /// Create a client with a custom transport and pre-fetched session.
    pub(crate) fn with_transport(transport: T, session: Session) -> crate::Result<Self> {
        let default_account_id = session
            .primary_accounts()
            .next()
            .map(|a| crate::core::id::AccountId::new(a.1.clone()))
            .unwrap_or_else(|| crate::core::id::AccountId::new(""));

        Ok(Client {
            inner: Arc::new(ClientInner {
                upload_url: URLPart::parse(session.upload_url())?,
                download_url: URLPart::parse(session.download_url())?,
                event_source_url: URLPart::parse(session.event_source_url())?,
                api_url: session.api_url().to_string(),
                session_url: String::new(),
                session: std::sync::Mutex::new(Arc::new(session)),
                session_updated: true.into(),
                timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
                transport,
                default_account_id,
                #[cfg(feature = "websockets")]
                accept_invalid_certs: false,
                #[cfg(feature = "websockets")]
                authorization: Authorization::Basic(String::new()),
                #[cfg(feature = "websockets")]
                ws: None.into(),
            }),
        })
    }

    pub(crate) fn build(&self) -> Request<'_, T> {
        Request::new(self)
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.inner.timeout
    }

    pub(crate) fn session(&self) -> Arc<Session> {
        self.inner
            .session
            .lock()
            .expect("session mutex poisoned")
            .clone()
    }

    pub(crate) fn session_url(&self) -> &str {
        &self.inner.session_url
    }

    pub(crate) fn default_account_id(&self) -> &crate::core::id::AccountId {
        &self.inner.default_account_id
    }

    pub(crate) fn download_url(&self) -> &[URLPart<blob::URLParameter>] {
        &self.inner.download_url
    }

    pub(crate) fn upload_url(&self) -> &[URLPart<blob::URLParameter>] {
        &self.inner.upload_url
    }

    pub(crate) fn event_source_url(&self) -> &[URLPart<crate::event_source::URLParameter>] {
        &self.inner.event_source_url
    }

    /// Send a JMAP request and get a typed Response.
    pub(crate) async fn send_request(
        &self,
        request: &request::Request<'_, T>,
    ) -> crate::Result<response::Response> {
        let body = serde_json::to_vec(request).map_err(crate::Error::RequestEncode)?;
        let bytes = self
            .inner
            .transport
            .api_request(&self.inner.api_url, body)
            .await
            .map_err(crate::Error::from)?;
        let response: response::Response = serde_json::from_slice(&bytes)?;
        {
            let session = self.inner.session.lock().expect("session mutex poisoned");
            if response.session_state() != session.state() {
                self.inner.session_updated.store(false, Ordering::Release);
            }
        }
        Ok(response)
    }

    pub(crate) async fn refresh_session(&self) -> crate::Result<()> {
        let bytes = self
            .inner
            .transport
            .get_session(&self.inner.session_url)
            .await
            .map_err(crate::Error::from)?;
        let session: Session = serde_json::from_slice(&bytes)?;
        {
            *self.inner.session.lock().expect("session mutex poisoned") = Arc::new(session);
            self.inner.session_updated.store(true, Ordering::Release);
        }
        Ok(())
    }

    pub(crate) fn is_session_updated(&self) -> bool {
        self.inner.session_updated.load(Ordering::Acquire)
    }

    /// Access the underlying transport.
    pub(crate) fn transport(&self) -> &T {
        &self.inner.transport
    }

    /// Returns the `Authorization` header value used by this client.
    #[cfg(feature = "websockets")]
    pub(crate) fn authorization(&self) -> String {
        self.inner.authorization.header_value()
    }
}

impl Credentials {
    pub(crate) fn basic(username: &str, password: &str) -> Self {
        use base64::{Engine, engine::general_purpose::STANDARD};
        Credentials::Basic(STANDARD.encode(format!("{username}:{password}")))
    }

    pub(crate) fn bearer(token: impl Into<String>) -> Self {
        Credentials::Bearer(StaticTokenSource::new(token, None))
    }

    pub(crate) fn bearer_source(source: StaticTokenSource) -> Self {
        Credentials::Bearer(source)
    }
}

impl From<(&str, &str)> for Credentials {
    fn from(credentials: (&str, &str)) -> Self {
        Credentials::basic(credentials.0, credentials.1)
    }
}

impl From<(String, String)> for Credentials {
    fn from(credentials: (String, String)) -> Self {
        Credentials::basic(&credentials.0, &credentials.1)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::response::Response;

    #[test]
    fn test_deserialize() {
        let _r: Response = serde_json::from_slice(
            br#"{"sessionState": "123", "methodResponses": [[ "Email/query", {
                "accountId": "A1",
                "queryState": "abcdefg",
                "canCalculateChanges": true,
                "position": 0,
                "total": 101,
                "ids": [ "msg1023", "msg223", "msg110", "msg93", "msg91",
                    "msg38", "msg36", "msg33", "msg11", "msg1" ]
            }, "t0" ],
            [ "Email/get", {
                "accountId": "A1",
                "state": "123456",
                "list": [{
                    "id": "msg1023",
                    "threadId": "trd194"
                }, {
                    "id": "msg223",
                    "threadId": "trd114"
                }
                ],
                "notFound": []
            }, "t1" ],
            [ "Thread/get", {
                "accountId": "A1",
                "state": "123456",
                "list": [{
                    "id": "trd194",
                    "emailIds": [ "msg1020", "msg1021", "msg1023" ]
                }, {
                    "id": "trd114",
                    "emailIds": [ "msg201", "msg223" ]
                }
                ],
                "notFound": []
            }, "t2" ]]}"#,
        )
        .unwrap();
    }
}
