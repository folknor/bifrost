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

use bifrost_net::{AccountId as NetAccountId, StaticTokenSource, TokenSource};
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

#[non_exhaustive]
pub(crate) enum Credentials {
    Basic(String),
    Bearer(Arc<dyn TokenSource>),
}

#[derive(Clone)]
pub(crate) enum Authorization {
    Basic(String),
    Bearer(Arc<dyn TokenSource>),
}

impl Authorization {
    fn from_credentials(credentials: Credentials) -> Self {
        match credentials {
            Credentials::Basic(value) => Self::Basic(value),
            Credentials::Bearer(token) => Self::Bearer(token),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_credentials_for_test(credentials: Credentials) -> Self {
        Self::from_credentials(credentials)
    }

    /// Build the `Authorization` header value. For the bearer case this
    /// reads `source.current()` so a token rotated on the shared source
    /// is presented live; only the WebSocket handshake (which builds its
    /// own header) and the Basic path take this route - the HTTP request
    /// pipeline injects the bearer through `AccountNet` instead.
    pub(crate) async fn header_value(&self) -> Result<String, bifrost_net::Error> {
        match self {
            Self::Basic(value) => Ok(format!("Basic {value}")),
            Self::Bearer(source) => {
                let token = source.current().await?;
                Ok(format!("Bearer {}", token.as_str()))
            }
        }
    }

    pub(crate) fn account_token_source(&self) -> Arc<dyn TokenSource> {
        match self {
            // AccountNet requires a token source even when JMAP is
            // using Basic auth. This source is intentionally inert;
            // the request builder disables bearer injection for Basic.
            Self::Basic(_) => Arc::new(StaticTokenSource::new("", None)),
            Self::Bearer(source) => Arc::clone(source),
        }
    }

    pub(crate) fn uses_bearer_pipeline(&self) -> bool {
        matches!(self, Self::Bearer(_))
    }
}

/// Everything a client derives from a [`Session`], held together so a
/// refresh swaps all of it at once.
///
/// RFC 8620 §2 lets any Session property change between fetches -
/// `apiUrl`, the upload / download / EventSource templates and
/// `primaryAccounts` included. Caching those next to, rather than with,
/// the `Session` let a refreshed session be observable through
/// `session()` while requests kept going to the endpoints and account of
/// the session it replaced. One `Arc<SessionState>` behind one lock makes
/// the swap atomic: a reader either sees the whole old session or the
/// whole new one.
pub(crate) struct SessionState {
    session: Arc<Session>,
    api_url: String,
    upload_url: Vec<URLPart<blob::URLParameter>>,
    download_url: Vec<URLPart<blob::URLParameter>>,
    event_source_url: Vec<URLPart<crate::event_source::URLParameter>>,
    default_account_id: crate::core::id::AccountId,
}

impl SessionState {
    fn derive(session: Session) -> crate::Result<Self> {
        let default_account_id = session
            .default_account_id()
            .map(crate::core::id::AccountId::new)
            .unwrap_or_else(|| crate::core::id::AccountId::new(""));

        Ok(Self {
            api_url: session.api_url().to_string(),
            upload_url: URLPart::parse(session.upload_url())?,
            download_url: URLPart::parse(session.download_url())?,
            event_source_url: URLPart::parse(session.event_source_url())?,
            default_account_id,
            session: Arc::new(session),
        })
    }

    pub(crate) fn session(&self) -> &Arc<Session> {
        &self.session
    }

    pub(crate) fn api_url(&self) -> &str {
        &self.api_url
    }

    pub(crate) fn upload_url(&self) -> &[URLPart<blob::URLParameter>] {
        &self.upload_url
    }

    pub(crate) fn download_url(&self) -> &[URLPart<blob::URLParameter>] {
        &self.download_url
    }

    pub(crate) fn event_source_url(&self) -> &[URLPart<crate::event_source::URLParameter>] {
        &self.event_source_url
    }

    pub(crate) fn default_account_id(&self) -> &crate::core::id::AccountId {
        &self.default_account_id
    }
}

/// Internal shared state of a [`Client`]. Stored behind an `Arc` so the
/// client itself is cheap to clone and pass around.
pub(crate) struct ClientInner<T: HttpTransport = ReqwestTransport> {
    transport: T,
    state: std::sync::Mutex<Arc<SessionState>>,
    session_url: String,
    session_updated: AtomicBool,
    session_changes: tokio::sync::watch::Sender<u64>,

    timeout: Duration,
    #[cfg(feature = "websockets")]
    pub(crate) accept_invalid_certs: bool,

    #[cfg(feature = "websockets")]
    pub(crate) authorization: Authorization,
    #[cfg(feature = "websockets")]
    pub(crate) ws: tokio::sync::Mutex<Option<crate::client_ws::WsStream>>,
    /// RFC 8887 `requestId` counter for WebSocket requests.
    ///
    /// It lives on the CLIENT, not on the `WsStream`, so it survives a
    /// reconnect. Per-connection it restarted at 0, which meant a response
    /// arriving late from a previous connection carried an id the new
    /// connection was about to reuse - the one case where correlation by
    /// id is actively worse than none.
    #[cfg(feature = "websockets")]
    pub(crate) ws_request_id: std::sync::atomic::AtomicU64,
    /// Callers awaiting a WebSocket response, keyed by that `requestId`.
    ///
    /// On the CLIENT for the same reason the counter is: the map has to
    /// outlive any one connection so a reconnect can fail the waiters
    /// the dropped connection will never answer, instead of leaving them
    /// parked forever on a socket that is gone.
    #[cfg(feature = "websockets")]
    pub(crate) ws_pending: Arc<crate::client_ws::PendingRequests>,
}

/// A JMAP client. Cheap to clone - wraps an `Arc<ClientInner>` internally.
///
/// Cloning a `Client` shares the underlying transport, session state, and
/// (when enabled) WebSocket connection. There is no public lifetime
/// parameter, so `Client` can be stored in long-lived structs and moved
/// across tasks freely.
pub(crate) struct Client<T: HttpTransport = ReqwestTransport> {
    inner: Arc<ClientInner<T>>,
    /// Where this handle reports request-local inbound bytes, if
    /// anywhere. `None` on every ordinary client; a sync stream that
    /// needs per-batch accounting takes a metered handle via
    /// `metered()`. Kept OUTSIDE `ClientInner` deliberately: forking
    /// the inner would fork the session-state mutex, and two clients
    /// with independent session state is a correctness bug, not an
    /// accounting detail.
    tally: Option<ByteTally>,
}

/// Inbound payload bytes attributed to one engine batch.
///
/// An engine batch here is one `/jmap/api` POST carrying several
/// method calls, and a paged walk issues one per page, so the bytes
/// are summed on an accumulator owned by one stream rather than
/// sampled off the account-cumulative bandwidth meter - which is
/// shared by every concurrent request on the account and would
/// attribute another scope's traffic to this batch.
///
/// `take` reads and resets, so consecutive batches from one stream
/// partition the bytes rather than each reporting a running total.
#[derive(Clone, Default)]
pub(crate) struct ByteTally(Arc<std::sync::atomic::AtomicU64>);

impl ByteTally {
    fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    /// Bytes recorded since the previous `take`.
    pub(crate) fn take(&self) -> u64 {
        self.0.swap(0, Ordering::Relaxed)
    }
}

impl<T: HttpTransport> Clone for Client<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            tally: self.tally.clone(),
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

        let session_url = well_known_session_url(url);
        let session_bytes = transport
            .get_session(&session_url)
            .await
            .map_err(crate::Error::from)?;
        let session: Session = serde_json::from_slice(&session_bytes)?;

        Ok(Client {
            tally: None,
            inner: Arc::new(ClientInner {
                state: std::sync::Mutex::new(Arc::new(SessionState::derive(session)?)),
                session_url,
                session_updated: true.into(),
                session_changes: tokio::sync::watch::channel(0).0,
                timeout: self.timeout,
                transport,
                #[cfg(feature = "websockets")]
                accept_invalid_certs: self.accept_invalid_certs,
                #[cfg(feature = "websockets")]
                authorization,
                #[cfg(feature = "websockets")]
                ws: None.into(),
                #[cfg(feature = "websockets")]
                ws_request_id: std::sync::atomic::AtomicU64::new(0),
                #[cfg(feature = "websockets")]
                ws_pending: Arc::new(crate::client_ws::PendingRequests::new()),
            }),
        })
    }
}

/// Decision note: the well-known path is appended to WHATEVER the caller
/// gave, deliberately. RFC 8620 puts `/.well-known/jmap` at the origin
/// root, but real deployments mount JMAP under a path prefix and answer
/// the well-known there; appending supports both (pass the bare origin
/// for the RFC shape). The trade-off is that a caller holding the ACTUAL
/// session URL cannot connect to it directly through this door.
fn well_known_session_url(url: &str) -> String {
    format!("{}/.well-known/jmap", url.trim_end_matches('/'))
}

#[cfg(test)]
mod session_state_tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use serde_json::json;

    use super::{Client, Session, well_known_session_url};
    use crate::core::transport::{HttpTransport, TransportError};

    fn session_json(tag: &str, account: &str, state: &str) -> String {
        json!({
            "capabilities": {},
            "accounts": {},
            "primaryAccounts": {"urn:ietf:params:jmap:mail": account},
            "username": "user@example.test",
            "apiUrl": format!("https://{tag}.example.test/api"),
            "downloadUrl": format!("https://{tag}.example.test/dl/{{accountId}}/{{blobId}}/{{name}}/{{type}}"),
            "uploadUrl": format!("https://{tag}.example.test/upload/{{accountId}}"),
            "eventSourceUrl": format!("https://{tag}.example.test/es"),
            "state": state
        })
        .to_string()
    }

    /// RFC 8887 request ids must not repeat across WebSocket
    /// reconnects: a late response from the dropped connection would
    /// otherwise carry an id the new connection is about to reuse, which
    /// is worse than having no correlation at all. The counter therefore
    /// belongs to the CLIENT and not to the per-connection `WsStream`,
    /// which is reconstructed on every `connect_ws`.
    #[cfg(feature = "websockets")]
    #[test]
    fn websocket_request_ids_do_not_restart() {
        let session: Session = serde_json::from_str(&session_json("old", "A1", "session-1"))
            .expect("session fixture parses");
        let client = Client::with_transport(
            RefreshingTransport {
                api_urls: Arc::new(Mutex::new(Vec::new())),
            },
            session,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");

        let ids: Vec<String> = (0..3).map(|_| client.next_ws_request_id()).collect();
        assert_eq!(ids, vec!["0", "1", "2"]);
        // A reconnect replaces the `WsStream`; the counter is not in it,
        // so the sequence continues rather than restarting at 0.
        assert_eq!(client.next_ws_request_id(), "3");
    }

    /// A session advertising no `primaryAccounts` leaves the derived
    /// default account id empty. `build()` used to bake that into every
    /// generic request and the method structs serialized
    /// `"accountId": ""` - a malformed request answered with an opaque
    /// server error, for a fault that is entirely local. It must fail
    /// here instead, naming the capability whose account is missing.
    #[test]
    fn a_session_without_a_primary_account_refuses_to_build_a_request() {
        let session: Session = serde_json::from_value(json!({
            "capabilities": {},
            "accounts": {},
            "primaryAccounts": {},
            "username": "user@example.test",
            "apiUrl": "https://example.test/api",
            "downloadUrl": "https://example.test/dl/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/es",
            "state": "session-1"
        }))
        .expect("session parses");
        let client = Client::with_transport(
            RefreshingTransport {
                api_urls: Arc::new(Mutex::new(Vec::new())),
            },
            session,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        assert_eq!(client.default_account_id().as_str(), "");

        let mut request = client.build();
        let error = request
            .call(crate::mailbox::MailboxGet::new())
            .err()
            .expect("an empty account id must not reach the wire");
        assert!(
            matches!(
                error,
                crate::Error::NoPrimaryAccount {
                    capability: "urn:ietf:params:jmap:mail"
                }
            ),
            "unexpected error: {error:?}"
        );
        // Nothing was appended, so the request cannot serialize the
        // malformed call either.
        let body = serde_json::to_string(&request).expect("request serializes");
        assert!(
            !body.contains("\"accountId\":\"\""),
            "empty account id must not be serialized: {body}"
        );
    }

    fn leading_value<P: crate::core::session::URLParser>(parts: &[super::URLPart<P>]) -> &str {
        match parts.first() {
            Some(super::URLPart::Value(value)) => value,
            _ => panic!("a parsed template starts with a literal"),
        }
    }

    /// Serves a second, entirely different session on refresh, and
    /// records which `apiUrl` requests actually went to.
    struct RefreshingTransport {
        api_urls: Arc<Mutex<Vec<String>>>,
    }

    impl HttpTransport for RefreshingTransport {
        async fn api_request(&self, url: &str, _body: Vec<u8>) -> Result<Bytes, TransportError> {
            self.api_urls
                .lock()
                .expect("api url lock")
                .push(url.to_string());
            Err(TransportError::new("stub transport returns no response"))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not upload"))
        }

        async fn download(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not download"))
        }

        async fn get_session(&self, _url: &str) -> Result<Bytes, TransportError> {
            Ok(Bytes::from(session_json("new", "B2", "session-2")))
        }
    }

    /// RFC 8620 §2 lets any Session property change. A refresh that only
    /// replaced the `Session` left `apiUrl`, the blob / EventSource
    /// templates and the default account frozen at the values of the
    /// session it replaced, so `session()` reported the new server while
    /// every request still went to the old one.
    #[tokio::test]
    async fn refresh_replaces_the_session_and_everything_derived_from_it() {
        let api_urls = Arc::new(Mutex::new(Vec::new()));
        let old: Session = serde_json::from_str(&session_json("old", "A1", "session-1"))
            .expect("session fixture parses");
        let client = Client::with_transport(
            RefreshingTransport {
                api_urls: Arc::clone(&api_urls),
            },
            old,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");

        assert_eq!(client.default_account_id().as_str(), "A1");
        let _ = client.send_request(&client.build()).await;

        client.refresh_session().await.expect("refresh succeeds");

        assert_eq!(client.session().state(), "session-2");
        assert_eq!(client.default_account_id().as_str(), "B2");
        let state = client.session_state();
        assert_eq!(state.api_url(), "https://new.example.test/api");
        assert_eq!(
            leading_value(state.download_url()),
            "https://new.example.test/dl/"
        );
        assert_eq!(
            leading_value(state.upload_url()),
            "https://new.example.test/upload/"
        );
        assert_eq!(
            leading_value(state.event_source_url()),
            "https://new.example.test/es"
        );

        let _ = client.send_request(&client.build()).await;
        let urls = api_urls.lock().expect("api url lock").clone();
        assert_eq!(
            urls,
            vec![
                "https://old.example.test/api".to_string(),
                "https://new.example.test/api".to_string()
            ]
        );
    }

    #[test]
    fn well_known_session_url_has_one_separator() {
        assert_eq!(
            well_known_session_url("https://example.test/jmap"),
            "https://example.test/jmap/.well-known/jmap"
        );
        assert_eq!(
            well_known_session_url("https://example.test/jmap/"),
            "https://example.test/jmap/.well-known/jmap"
        );
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
    pub(crate) fn with_transport(
        transport: T,
        session: Session,
        session_url: impl Into<String>,
    ) -> crate::Result<Self> {
        let session_url = session_url.into();
        if session_url.is_empty() {
            return Err(crate::Error::InvalidUrl(
                "a custom transport client needs a session URL".to_string(),
            ));
        }
        Ok(Client {
            tally: None,
            inner: Arc::new(ClientInner {
                state: std::sync::Mutex::new(Arc::new(SessionState::derive(session)?)),
                session_url,
                session_updated: true.into(),
                session_changes: tokio::sync::watch::channel(0).0,
                timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
                transport,
                #[cfg(feature = "websockets")]
                accept_invalid_certs: false,
                #[cfg(feature = "websockets")]
                authorization: Authorization::Basic(String::new()),
                #[cfg(feature = "websockets")]
                ws: None.into(),
                #[cfg(feature = "websockets")]
                ws_request_id: std::sync::atomic::AtomicU64::new(0),
                #[cfg(feature = "websockets")]
                ws_pending: Arc::new(crate::client_ws::PendingRequests::new()),
            }),
        })
    }

    pub(crate) fn build(&self) -> Request<'_, T> {
        Request::new(self)
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.inner.timeout
    }

    /// The session and everything derived from it, as one consistent
    /// snapshot. Hold the returned `Arc` for the duration of a URL build
    /// so a concurrent [`Client::refresh_session`] cannot splice two
    /// sessions into one request.
    pub(crate) fn session_state(&self) -> Arc<SessionState> {
        self.inner
            .state
            .lock()
            .expect("session mutex poisoned")
            .clone()
    }

    pub(crate) fn session(&self) -> Arc<Session> {
        Arc::clone(self.session_state().session())
    }

    pub(crate) fn session_url(&self) -> &str {
        &self.inner.session_url
    }

    pub(crate) fn default_account_id(&self) -> crate::core::id::AccountId {
        self.session_state().default_account_id().clone()
    }

    /// Send a JMAP request and get a typed Response.
    pub(crate) async fn send_request(
        &self,
        request: &request::Request<'_, T>,
    ) -> crate::Result<response::Response> {
        let body = serde_json::to_vec(request).map_err(crate::Error::RequestEncode)?;
        let state = self.session_state();
        let (bytes, bytes_in) = self
            .inner
            .transport
            .api_request_measured(state.api_url(), body)
            .await
            .map_err(crate::Error::from)?;
        if let Some(tally) = self.tally.as_ref() {
            tally.add(bytes_in);
        }
        let response: response::Response = serde_json::from_slice(&bytes)?;
        self.note_session_state(response.session_state());
        Ok(response)
    }

    /// The next RFC 8887 `requestId` for a WebSocket request. Monotonic
    /// for the life of the CLIENT, so ids are not reused across reconnects.
    #[cfg(feature = "websockets")]
    pub(crate) fn next_ws_request_id(&self) -> String {
        self.inner
            .ws_request_id
            .fetch_add(1, Ordering::Relaxed)
            .to_string()
    }

    /// Compare a server-reported `sessionState` against the one this client
    /// is running on, and publish a divergence.
    ///
    /// Every response carries `sessionState` (RFC 8620 s3.4), and the whole
    /// scope-lifecycle `CapabilityChanged` story rests on noticing when it
    /// moves. Both response doors must therefore run this: the HTTP door
    /// always did, while the WebSocket door rebuilt a `Response` and handed
    /// it up without ever looking, so staleness went undetected on exactly
    /// the connection that stays open longest.
    pub(crate) fn note_session_state(&self, session_state: &str) {
        if session_state != self.session_state().session().state() {
            self.inner.session_updated.store(false, Ordering::Release);
            self.inner.session_changes.send_modify(|generation| {
                *generation = generation.wrapping_add(1);
            });
        }
    }

    /// Re-fetch the session and republish everything derived from it.
    ///
    /// The derived endpoints and default account are replaced together
    /// with the `Session` itself; a partial swap would leave requests
    /// routed by a session no caller can observe.
    ///
    /// Currently unwired: session divergence is handled by the lifecycle path,
    /// which wakes on a watch channel rather than polling. Kept and disclosed
    /// in `reference/jmap.md` rather than deleted - an audit finding it
    /// uncalled has found a recorded state, not dead code.
    pub(crate) async fn refresh_session(&self) -> crate::Result<()> {
        let bytes = self
            .inner
            .transport
            .get_session(&self.inner.session_url)
            .await
            .map_err(crate::Error::from)?;
        let session: Session = serde_json::from_slice(&bytes)?;
        let state = Arc::new(SessionState::derive(session)?);
        *self.inner.state.lock().expect("session mutex poisoned") = state;
        self.inner.session_updated.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) fn is_session_updated(&self) -> bool {
        self.inner.session_updated.load(Ordering::Acquire)
    }

    /// Subscribe to session-state divergence detected at the response boundary.
    pub(crate) fn session_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.session_changes.subscribe()
    }

    /// A handle over the same client - same transport, same session
    /// state - that reports every `/jmap/api` response's inbound bytes
    /// into a fresh accumulator.
    ///
    /// One accumulator per stream, not per client: the returned handle
    /// is the only one recording into it, so concurrent work on other
    /// scopes cannot contaminate the total.
    pub(crate) fn metered(&self) -> (Self, ByteTally) {
        let tally = ByteTally::default();
        (
            Self {
                inner: Arc::clone(&self.inner),
                tally: Some(tally.clone()),
            },
            tally,
        )
    }

    /// Access the underlying transport.
    pub(crate) fn transport(&self) -> &T {
        &self.inner.transport
    }

    /// Returns the `Authorization` header value used by this client.
    /// Async because the bearer case reads the current token from the
    /// shared source so a rotated token is presented on reconnect.
    #[cfg(feature = "websockets")]
    pub(crate) async fn authorization(&self) -> crate::Result<String> {
        self.inner
            .authorization
            .header_value()
            .await
            .map_err(|e| crate::Error::from(crate::core::transport::TransportError::from_net(e)))
    }
}

impl Credentials {
    pub(crate) fn basic(username: &str, password: &str) -> Self {
        use base64::{Engine, engine::general_purpose::STANDARD};
        Credentials::Basic(STANDARD.encode(format!("{username}:{password}")))
    }

    pub(crate) fn bearer(token: impl Into<String>) -> Self {
        Credentials::Bearer(Arc::new(StaticTokenSource::new(token, None)))
    }

    pub(crate) fn bearer_source(source: Arc<dyn TokenSource>) -> Self {
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
