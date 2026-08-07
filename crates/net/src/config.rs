//! Configuration for the shared `Net` HTTP transport.

use std::time::Duration;

use crate::auth::DEFAULT_TOKEN_MAX_AGE;
use crate::redirect::{FollowRedirects, RedirectPolicy};

/// Default ceiling on a buffered response body: 64 MiB. Chosen to be
/// far above any JSON envelope the protocol crates exchange (the
/// largest realistic one is a JMAP `Email/get` batch with full bodies)
/// and far below a size that threatens the process.
pub const DEFAULT_MAX_BUFFERED_RESPONSE: usize = 64 * 1024 * 1024;

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
    /// Inactivity deadline between response body chunks, applied to
    /// every request.
    ///
    /// `connect_timeout` only covers reaching the server. A request
    /// that connects and then stalls mid-body produces no error at all,
    /// so the retry loop never fires and the caller waits forever - and
    /// only JMAP was setting a per-request timeout, leaving every Gmail
    /// and Graph call with no deadline of any kind. This is an
    /// inactivity timeout rather than a total one, so it bounds the
    /// stall without capping a legitimately long blob download.
    pub read_timeout: Option<Duration>,
    /// Total deadline applied by `RequestBuilder::send` when the caller
    /// set no explicit `timeout`.
    ///
    /// Buffered-only, deliberately. `send` is the JSON-API path, where
    /// a whole-request ceiling is right; `send_streaming` is the blob
    /// path, where a multi-minute attachment download is normal and a
    /// total deadline would fail it on size rather than on health.
    /// Streaming is bounded by `read_timeout` instead.
    pub default_request_timeout: Option<Duration>,
    /// Ceiling on a buffered response body, enforced by
    /// `RequestBuilder::send`.
    ///
    /// `send` accumulates the whole body into memory, and every JSON
    /// API call in google / graph / jmap takes that path. Without a
    /// ceiling a provider outage page, a mis-routed blob URL, or a
    /// hostile response can OOM the process. The error path already had
    /// a 4 KB cap (`read_capped_response_body`); this is the success
    /// path's. `None` disables the check.
    pub max_buffered_response: Option<usize>,
    /// User-Agent header set on every outbound request.
    pub user_agent: String,
    /// Extra trusted root certificates injected into the native-tls
    /// trust store. Empty in the default; populated by callers that
    /// pin a private CA.
    pub root_certs: Vec<native_tls::Certificate>,
    /// Test-fixture escape hatch. Production callers must never set
    /// this true.
    pub dangerous_accept_invalid_certs: bool,
    /// Method-aware HTTP redirect policy applied by `bifrost-net`'s
    /// own redirect loop. The underlying `reqwest` client's redirect
    /// policy is set to `redirect::Policy::none()` in every config -
    /// `bifrost-net` owns the loop so RFC 7231 §6.4 method
    /// rewriting, the trusted-host allowlist, and `Authorization`-
    /// stripping on cross-host hops happen exactly once and the same
    /// way for every HTTP protocol crate.
    pub follow_redirects: FollowRedirects,
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
            read_timeout: Some(Duration::from_secs(30)),
            default_request_timeout: Some(Duration::from_secs(120)),
            max_buffered_response: Some(DEFAULT_MAX_BUFFERED_RESPONSE),
            user_agent: format!("bifrost-net/{}", env!("CARGO_PKG_VERSION")),
            root_certs: Vec::new(),
            dangerous_accept_invalid_certs: false,
            follow_redirects: FollowRedirects::default_on(),
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

    /// Replace the redirect policy. Builder-style; returns `self`.
    ///
    /// Pass `FollowRedirects::Disabled` to turn redirect-following
    /// off entirely (3xx responses surface as terminal statuses). Pass
    /// `FollowRedirects::Enabled(RedirectPolicy { .. })` to configure
    /// the trusted-host allowlist and max hop count; the default
    /// constructed by `NetConfig::default()` is
    /// `FollowRedirects::default_on()` (no allowlist, ten hops).
    #[must_use]
    pub fn follow_redirects(mut self, policy: FollowRedirects) -> Self {
        self.follow_redirects = policy;
        self
    }

    /// Replace the redirect policy with an `Enabled` variant carrying
    /// the supplied policy. Shorthand for the common case of
    /// configuring the allowlist and hop count without naming the
    /// outer enum.
    #[must_use]
    pub fn with_redirect_policy(mut self, policy: RedirectPolicy) -> Self {
        self.follow_redirects = FollowRedirects::Enabled(policy);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_safe_for_production() {
        let config = NetConfig::default();
        assert!(
            !config.dangerous_accept_invalid_certs,
            "the invalid-cert escape hatch must never default on"
        );
        assert!(config.root_certs.is_empty());
        assert_eq!(config.token_max_age, DEFAULT_TOKEN_MAX_AGE);
        assert!(
            config.user_agent.starts_with("bifrost-net/"),
            "every outbound request identifies the crate, got {}",
            config.user_agent
        );
        assert!(config.connect_timeout > Duration::ZERO);
        assert!(
            config.http2_keep_alive_timeout < config.http2_keep_alive_interval,
            "the PING ack deadline must be shorter than the PING interval"
        );
    }

    /// The default is redirect-following ON with an empty allowlist and
    /// ten hops - the same cap reqwest classically used.
    #[test]
    fn default_redirect_policy_is_enabled_with_ten_hops() {
        let config = NetConfig::new();
        match config.follow_redirects {
            FollowRedirects::Enabled(policy) => {
                assert_eq!(policy.max_hops, 10);
                assert!(
                    policy.trusted_hosts.is_empty(),
                    "an empty allowlist means every host is acceptable"
                );
            }
            FollowRedirects::Disabled => panic!("default must follow redirects"),
        }
    }

    #[test]
    fn builder_helpers_replace_the_redirect_policy() {
        let disabled = NetConfig::new().follow_redirects(FollowRedirects::Disabled);
        assert!(matches!(
            disabled.follow_redirects,
            FollowRedirects::Disabled
        ));

        let scoped = NetConfig::new()
            .with_redirect_policy(RedirectPolicy::with_hops(3).trust_host("Allowed.Example"));
        match scoped.follow_redirects {
            FollowRedirects::Enabled(policy) => {
                assert_eq!(policy.max_hops, 3);
                assert!(
                    policy.allows_host("allowed.example"),
                    "trust_host lowercases so the allowlist is case-insensitive"
                );
                assert!(!policy.allows_host("other.example"));
            }
            FollowRedirects::Disabled => panic!("with_redirect_policy must enable following"),
        }
    }

    #[test]
    fn token_max_age_leaves_a_margin_under_a_typical_one_hour_ttl() {
        assert!(
            DEFAULT_TOKEN_MAX_AGE < Duration::from_secs(60 * 60),
            "the opaque-token max age must expire before a typical issuer TTL"
        );
        assert!(DEFAULT_TOKEN_MAX_AGE >= Duration::from_secs(30 * 60));
    }
}
