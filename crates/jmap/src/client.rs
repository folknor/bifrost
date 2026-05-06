use std::{
    collections::HashSet,
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

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

#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Credentials {
    Basic(String),
    Bearer(String),
}

pub struct Client<T: HttpTransport = ReqwestTransport> {
    transport: T,
    session: std::sync::Mutex<Arc<Session>>,
    session_url: String,
    api_url: String,
    session_updated: AtomicBool,

    upload_url: Vec<URLPart<blob::URLParameter>>,
    download_url: Vec<URLPart<blob::URLParameter>>,
    event_source_url: Vec<URLPart<crate::event_source::URLParameter>>,

    default_account_id: String,
    timeout: Duration,
    pub(crate) accept_invalid_certs: bool,

    #[cfg(feature = "websockets")]
    pub(crate) authorization: String,
    #[cfg(feature = "websockets")]
    pub(crate) ws: tokio::sync::Mutex<Option<crate::client_ws::WsStream>>,
}

pub struct ClientBuilder {
    credentials: Option<Credentials>,
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
    pub fn new() -> Self {
        Self {
            credentials: None,
            trusted_hosts: HashSet::new(),
            timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
            forwarded_for: None,
            accept_invalid_certs: false,
        }
    }

    pub fn credentials(mut self, credentials: impl Into<Credentials>) -> Self {
        self.credentials = Some(credentials.into());
        self
    }

    pub fn accept_invalid_certs(mut self, accept_invalid_certs: bool) -> Self {
        self.accept_invalid_certs = accept_invalid_certs;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn follow_redirects(
        mut self,
        trusted_hosts: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.trusted_hosts
            .extend(trusted_hosts.into_iter().map(std::convert::Into::into));
        self
    }

    pub fn forwarded_for(mut self, ip: IpAddr) -> Self {
        self.forwarded_for = Some(match ip {
            IpAddr::V4(addr) => format!("for={addr}"),
            IpAddr::V6(addr) => format!("for=\"{addr}\""),
        });
        self
    }

    pub async fn connect(self, url: &str) -> crate::Result<Client> {
        let credentials = self.credentials.ok_or_else(|| {
            crate::core::transport::TransportError::new("Missing credentials — call .credentials() before .connect()")
        })?;
        let authorization = match credentials {
            Credentials::Basic(s) => format!("Basic {s}"),
            Credentials::Bearer(s) => format!("Bearer {s}"),
        };
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::USER_AGENT,
            header::HeaderValue::from_static(concat!(
                "bifrost-jmap/",
                env!("CARGO_PKG_VERSION")
            )),
        );
        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_str(&authorization).map_err(|e| crate::core::transport::TransportError::with_source("Invalid authorization header", e))?,
        );
        if let Some(forwarded_for) = self.forwarded_for {
            headers.insert(
                header::FORWARDED,
                header::HeaderValue::from_str(&forwarded_for).map_err(|e| crate::core::transport::TransportError::with_source("Invalid forwarded-for header", e))?,
            );
        }

        let trusted_hosts = Arc::new(self.trusted_hosts);

        let transport = ReqwestTransport::new(
            headers.clone(),
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
            .map(|a| a.1.clone())
            .unwrap_or_default();

        Ok(Client {
            download_url: URLPart::parse(session.download_url())?,
            upload_url: URLPart::parse(session.upload_url())?,
            event_source_url: URLPart::parse(session.event_source_url())?,
            api_url: session.api_url().to_string(),
            session: std::sync::Mutex::new(Arc::new(session)),
            session_url,
            session_updated: true.into(),
            accept_invalid_certs: self.accept_invalid_certs,
            timeout: self.timeout,
            transport,
            default_account_id,
            #[cfg(feature = "websockets")]
            authorization,
            #[cfg(feature = "websockets")]
            ws: None.into(),
        })
    }
}

/// Default client using reqwest. Use `Client::new()` / `ClientBuilder` to construct.
pub type DefaultClient = Client<ReqwestTransport>;

impl Client {
    /// Create a new client builder (uses reqwest transport by default).
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> ClientBuilder {
        ClientBuilder::new()
    }
}

impl<T: HttpTransport> Client<T> {
    /// Create a client with a custom transport and pre-fetched session.
    pub fn with_transport(transport: T, session: Session) -> crate::Result<Self> {
        let default_account_id = session
            .primary_accounts()
            .next()
            .map(|a| a.1.clone())
            .unwrap_or_default();

        Ok(Client {
            upload_url: URLPart::parse(session.upload_url())?,
            download_url: URLPart::parse(session.download_url())?,
            event_source_url: URLPart::parse(session.event_source_url())?,
            api_url: session.api_url().to_string(),
            session_url: String::new(),
            session: std::sync::Mutex::new(Arc::new(session)),
            session_updated: true.into(),
            accept_invalid_certs: false,
            timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
            transport,
            default_account_id,
            #[cfg(feature = "websockets")]
            authorization: String::new(),
            #[cfg(feature = "websockets")]
            ws: None.into(),
        })
    }

    pub fn build(&self) -> Request<'_, T> {
        Request::new(self)
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn session(&self) -> Arc<Session> {
        self.session.lock().expect("session mutex poisoned").clone()
    }

    pub fn session_url(&self) -> &str {
        &self.session_url
    }

    pub fn default_account_id(&self) -> &str {
        &self.default_account_id
    }

    /// Get the default account ID as a typed `AccountId`.
    pub fn default_account(&self) -> crate::core::id::AccountId {
        crate::core::id::AccountId::new(&self.default_account_id)
    }

    pub(crate) fn download_url(&self) -> &[URLPart<blob::URLParameter>] {
        &self.download_url
    }

    pub(crate) fn upload_url(&self) -> &[URLPart<blob::URLParameter>] {
        &self.upload_url
    }

    pub(crate) fn event_source_url(&self) -> &[URLPart<crate::event_source::URLParameter>] {
        &self.event_source_url
    }

    /// Send a JMAP request and get a typed Response.
    pub async fn send_request(
        &self,
        request: &request::Request<'_, T>,
    ) -> crate::Result<response::Response> {
        let body = serde_json::to_vec(request)?;
        let bytes = self
            .transport
            .api_request(&self.api_url, body)
            .await
            .map_err(crate::Error::from)?;
        let response: response::Response = serde_json::from_slice(&bytes)?;
        {
            let session = self.session.lock().expect("session mutex poisoned");
            if response.session_state() != session.state() {
                self.session_updated.store(false, Ordering::Release);
            }
        }
        Ok(response)
    }

    pub async fn refresh_session(&self) -> crate::Result<()> {
        let bytes = self
            .transport
            .get_session(&self.session_url)
            .await
            .map_err(crate::Error::from)?;
        let session: Session = serde_json::from_slice(&bytes)?;
        {
            *self.session.lock().expect("session mutex poisoned") = Arc::new(session);
            self.session_updated.store(true, Ordering::Release);
        }
        Ok(())
    }

    pub fn is_session_updated(&self) -> bool {
        self.session_updated.load(Ordering::Acquire)
    }

    /// Access the underlying transport.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Returns the `Authorization` header value used by this client.
    #[cfg(feature = "websockets")]
    pub fn authorization(&self) -> &str {
        &self.authorization
    }
}


impl Credentials {
    pub fn basic(username: &str, password: &str) -> Self {
        use base64::{engine::general_purpose::STANDARD, Engine};
        Credentials::Basic(STANDARD.encode(format!("{username}:{password}")))
    }

    pub fn bearer(token: impl Into<String>) -> Self {
        Credentials::Bearer(token.into())
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
