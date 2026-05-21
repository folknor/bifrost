//! The top-level `Net` handle and the per-account `AccountNet` view.
//!
//! Protocol crates hold an `AccountNet` and never see the underlying
//! `reqwest::Client`. `Net` itself is process-wide; one instance is
//! shared across every account so connection pooling, rate limiting,
//! and bandwidth metering can coordinate across accounts that share a
//! host.

use std::sync::Arc;
use std::sync::RwLock;

use crate::auth::TokenSource;
use crate::bandwidth::{AccountMeter, BandwidthMeter};
use crate::config::NetConfig;
use crate::error::Error;
use crate::rate::{RateLimit, RateLimitGovernor};
use crate::request::{ByteRange, ByteStream, RequestBuilder};
use crate::retry::RetryPolicy;
use crate::{AccountId, Priority};

/// Process-wide HTTP transport. Holds the shared reqwest client, the
/// per-host rate-limit governor, and the bandwidth meter. Cheap to
/// clone via the inner `Arc`.
#[derive(Clone)]
pub struct Net {
    inner: Arc<NetInner>,
}

/// Backing state for `Net`. Kept private so the Phase 2
/// implementation can rearrange without touching call sites.
#[allow(dead_code)]
struct NetInner {
    /// Caller-supplied configuration. Snapshotted at construction;
    /// runtime mutations go through the per-account handles.
    config: NetConfig,
    /// Shared reqwest client. `None` in the v1 skeleton because the
    /// constructor does not yet wire up the underlying client.
    client: RwLock<Option<reqwest::Client>>,
    /// Per-host rate-limit governor. Shared across every account so
    /// multi-account quota sharing on Gmail / Graph works out of the
    /// box.
    governor: RateLimitGovernor,
    /// Bandwidth meter, partitioned per account.
    meter: BandwidthMeter,
}

impl Net {
    /// Construct a `Net` from a `NetConfig`. The skeleton does not
    /// build the underlying reqwest client; Phase 2 fills that in.
    #[must_use]
    pub fn new(config: NetConfig) -> Self {
        Self {
            inner: Arc::new(NetInner {
                config,
                client: RwLock::new(None),
                governor: RateLimitGovernor::new(),
                meter: BandwidthMeter::new(),
            }),
        }
    }

    /// Register an account with the transport. Returns an
    /// `AccountNet` carrying the per-account token source, rate
    /// limits, and default retry policy.
    pub fn attach_account(&self, id: AccountId, spec: AccountSpec) -> AccountNet {
        // Register the meter so the per-account counters exist before
        // any request lands. Rate-limit registration walks the host
        // list the caller supplied.
        self.inner.meter.register_account(id);
        for limit in &spec.hosts {
            self.inner.governor.register(limit.clone());
        }
        AccountNet {
            net: self.clone(),
            account: id,
            token_source: spec.token_source,
            default_retry: spec.default_retry,
            priority: RwLock::new(Priority::Foreground),
            bandwidth_cap: RwLock::new(None),
        }
    }

    /// Drop the per-account state. Idempotent.
    pub fn detach_account(&self, id: AccountId) {
        self.inner.meter.forget_account(id);
    }

    /// Process-wide bandwidth meter handle. Per-account readings go
    /// through `AccountNet::meter`.
    #[must_use]
    pub fn meter(&self) -> &BandwidthMeter {
        &self.inner.meter
    }

    /// Per-host rate-limit governor.
    #[must_use]
    pub fn governor(&self) -> &RateLimitGovernor {
        &self.inner.governor
    }

    /// Configuration snapshot.
    #[must_use]
    pub fn config(&self) -> &NetConfig {
        &self.inner.config
    }
}

/// Account-scoped view onto `Net`. Carries the account identity used
/// for metering, the token source for OAuth, and the rate-limit plus
/// retry defaults the protocol crate supplied at registration.
pub struct AccountNet {
    /// Shared underlying transport.
    net: Net,
    /// Account identity for metering and tracing.
    account: AccountId,
    /// Provider of OAuth bearer tokens for this account.
    token_source: Arc<dyn TokenSource>,
    /// Default retry policy applied to every request unless the
    /// caller overrides via `RequestBuilder::retry`.
    default_retry: RetryPolicy,
    /// Engine-controlled priority hint. The rate-limit governor
    /// divides the bucket size for `Background` and `Bulk` accounts.
    priority: RwLock<Priority>,
    /// Engine-controlled bandwidth cap in bytes per second. `None`
    /// means unlimited.
    bandwidth_cap: RwLock<Option<u64>>,
}

impl AccountNet {
    /// Start a `GET` request.
    pub async fn get(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(reqwest::Method::GET, url)
    }

    /// Start a `POST` request.
    pub async fn post(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(reqwest::Method::POST, url)
    }

    /// Start a `PUT` request.
    pub async fn put(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(reqwest::Method::PUT, url)
    }

    /// Start a `PATCH` request.
    pub async fn patch(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(reqwest::Method::PATCH, url)
    }

    /// Start a `DELETE` request.
    pub async fn delete(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(reqwest::Method::DELETE, url)
    }

    /// Stream a download. The returned `ByteStream` increments the
    /// bandwidth meter on every chunk.
    pub async fn download_stream(
        &self,
        _url: &str,
        _range: Option<ByteRange>,
    ) -> Result<ByteStream, Error> {
        unimplemented!("AccountNet::download_stream is filled in by Phase 2")
    }

    /// Per-account meter handle.
    #[must_use]
    pub fn meter(&self) -> AccountMeter {
        self.net.inner.meter.account(self.account)
    }

    /// Set a per-account bandwidth cap in bytes per second. `None`
    /// disables the cap.
    pub fn set_bandwidth_cap(&self, bps: Option<u64>) {
        let mut guard = self
            .bandwidth_cap
            .write()
            .expect("AccountNet bandwidth-cap lock poisoned");
        *guard = bps;
    }

    /// Set the engine-controlled priority hint. The rate-limit
    /// governor reads this on every request.
    pub fn set_priority(&self, p: Priority) {
        let mut guard = self
            .priority
            .write()
            .expect("AccountNet priority lock poisoned");
        *guard = p;
    }

    /// The account this handle is scoped to.
    #[must_use]
    pub fn account(&self) -> AccountId {
        self.account
    }

    /// Default retry policy applied to requests on this account.
    #[must_use]
    pub fn default_retry(&self) -> &RetryPolicy {
        &self.default_retry
    }

    /// Underlying token source. Exposed so the OAuth refresher in
    /// `auth.rs` can share the trait object across requests.
    #[must_use]
    pub fn token_source(&self) -> &Arc<dyn TokenSource> {
        &self.token_source
    }
}

/// Caller-supplied spec describing how `Net::attach_account` should
/// register an account: which hosts have rate limits, where bearer
/// tokens come from, and what retry budget to apply by default.
pub struct AccountSpec {
    /// Per-host rate-limit declarations. Empty means "no governor
    /// enforcement for this account", which is the default for JMAP.
    pub hosts: Vec<RateLimit>,
    /// OAuth token provider.
    pub token_source: Arc<dyn TokenSource>,
    /// Default retry policy.
    pub default_retry: RetryPolicy,
}
