//! Configuration for the shared `Net` HTTP transport.

use std::time::Duration;

use crate::auth::DEFAULT_TOKEN_MAX_AGE;

/// Tunable parameters for the underlying reqwest client. Hidden behind
/// an explicit config struct so future migrations off reqwest can
/// remap fields without breaking the public surface.
#[derive(Clone)]
pub struct NetConfig {
    /// Idle eviction timeout for pooled connections.
    pub pool_idle_timeout: Duration,
    /// Maximum idle connections kept per host.
    pub pool_max_idle_per_host: usize,
    /// HTTP/2 PING frame interval for keepalive on long-lived
    /// connections.
    pub http2_keep_alive_interval: Duration,
    /// HTTP/2 PING ack timeout before the connection is closed.
    pub http2_keep_alive_timeout: Duration,
    /// TCP-level keepalive probe interval.
    pub tcp_keepalive: Duration,
    /// Connection establishment timeout.
    pub connect_timeout: Duration,
    /// User-Agent header set on every outbound request.
    pub user_agent: String,
    /// Extra trusted root certificates injected into the native-tls
    /// trust store. Empty in the default; populated by callers that
    /// pin a private CA.
    pub root_certs: Vec<native_tls::Certificate>,
    /// Test-fixture escape hatch. Production callers must never set
    /// this true.
    pub dangerous_accept_invalid_certs: bool,
    /// Whether the underlying HTTP client follows redirects
    /// automatically. Protocols that need a tighter redirect policy
    /// can disable this and handle 3xx responses themselves.
    pub follow_redirects: bool,
    /// Proactive-refresh max-age for OAuth tokens that lack an
    /// `expires_at` hint. The `OAuthRefresher` falls back to this
    /// when the issuer did not surface a TTL. Defaults to 55 min;
    /// typical OAuth access-token TTLs are 60 min.
    pub token_max_age: Duration,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            pool_idle_timeout: Duration::from_secs(60),
            pool_max_idle_per_host: 8,
            http2_keep_alive_interval: Duration::from_secs(30),
            http2_keep_alive_timeout: Duration::from_secs(10),
            tcp_keepalive: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(10),
            user_agent: format!("bifrost-net/{}", env!("CARGO_PKG_VERSION")),
            root_certs: Vec::new(),
            dangerous_accept_invalid_certs: false,
            follow_redirects: true,
            token_max_age: DEFAULT_TOKEN_MAX_AGE,
        }
    }
}

impl NetConfig {
    /// Construct a `NetConfig` with all defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a native-tls root certificate to the trust store. Returns
    /// `self` for chaining.
    #[must_use]
    pub fn with_root_cert(mut self, cert: native_tls::Certificate) -> Self {
        self.root_certs.push(cert);
        self
    }
}
