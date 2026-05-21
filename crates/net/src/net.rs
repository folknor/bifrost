//! The top-level `Net` handle and the per-account `AccountNet` view.
//!
//! Protocol crates hold an `AccountNet` and never see the underlying
//! `reqwest::Client`. `Net` itself is process-wide; one instance is
//! shared across every account so connection pooling, rate limiting,
//! and bandwidth metering can coordinate across accounts that share a
//! host.

use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use crate::auth::TokenSource;
use crate::bandwidth::{AccountMeter, BandwidthMeter};
use crate::config::NetConfig;
use crate::error::Error;
use crate::rate::{RateLimit, RateLimitGovernor};
use crate::request::{ByteStream, RequestBuilder};
use crate::retry::RetryPolicy;
use crate::{AccountId, ByteRange, Priority};

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
        self.inner.meter.register_account(id.clone());
        for limit in &spec.hosts {
            self.inner.governor.register(limit.clone());
        }
        AccountNet {
            inner: Arc::new(AccountNetInner {
                net: self.clone(),
                account: id,
                token_source: spec.token_source,
                default_retry: spec.default_retry,
                priority: AtomicU8::new(Priority::Foreground as u8),
                bandwidth_cap: AtomicU64::new(BANDWIDTH_CAP_NONE),
            }),
        }
    }

    /// Drop the per-account state.
    ///
    /// Idempotent for the bandwidth meter. **Asymmetric** with
    /// `attach_account` for the rate-limit governor: governor buckets
    /// are keyed by host string and shared across every account on
    /// that host (e.g. five Gmail accounts all share the
    /// `gmail.googleapis.com` bucket). Naively unregistering the host
    /// would yank the bucket out from under other accounts, so the
    /// skeleton leaks host buckets for the life of the process.
    /// Phase 2 may refcount per-host registrations and drop on zero;
    /// for now the asymmetry is by design.
    pub fn detach_account(&self, id: &AccountId) {
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

/// Sentinel encoding `None` on the `bandwidth_cap: AtomicU64` field.
/// `u64::MAX` is unreachable in practice (it implies ~18 EB/s) and
/// distinguishable from any real cap.
const BANDWIDTH_CAP_NONE: u64 = u64::MAX;

/// Account-scoped view onto `Net`. Carries the account identity used
/// for metering, the token source for OAuth, and the rate-limit plus
/// retry defaults the protocol crate supplied at registration.
///
/// `Clone` is one `Arc` refcount bump. Protocol crates typically clone
/// this once per spawned task so each stream owns its own handle.
#[derive(Clone)]
pub struct AccountNet {
    inner: Arc<AccountNetInner>,
}

#[allow(dead_code)]
struct AccountNetInner {
    /// Shared underlying transport.
    net: Net,
    /// Account identity for metering and tracing.
    account: AccountId,
    /// Provider of OAuth bearer tokens for this account.
    token_source: Arc<dyn TokenSource>,
    /// Default retry policy applied to every request unless the
    /// caller overrides via `RequestBuilder::retry`.
    default_retry: RetryPolicy,
    /// Engine-controlled priority hint. Atomic so the hot per-request
    /// read does not take a lock. Stored as the `Priority` enum's
    /// discriminant byte; `priority()` converts back.
    priority: AtomicU8,
    /// Engine-controlled bandwidth cap in bytes per second.
    /// `BANDWIDTH_CAP_NONE` (sentinel `u64::MAX`) means unlimited.
    bandwidth_cap: AtomicU64,
}

impl AccountNet {
    /// Start a `GET` request. Not async because the builder itself is
    /// pure construction; the network round-trip happens in
    /// `RequestBuilder::send`.
    #[must_use]
    pub fn get(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(reqwest::Method::GET, url)
    }

    /// Start a `POST` request.
    #[must_use]
    pub fn post(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(reqwest::Method::POST, url)
    }

    /// Start a `PUT` request.
    #[must_use]
    pub fn put(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(reqwest::Method::PUT, url)
    }

    /// Start a `PATCH` request.
    #[must_use]
    pub fn patch(&self, url: &str) -> RequestBuilder {
        RequestBuilder::new(reqwest::Method::PATCH, url)
    }

    /// Start a `DELETE` request.
    #[must_use]
    pub fn delete(&self, url: &str) -> RequestBuilder {
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
        self.inner
            .net
            .inner
            .meter
            .account(self.inner.account.clone())
    }

    /// Set a per-account bandwidth cap in bytes per second. `None`
    /// disables the cap. Single atomic store; safe to call from any
    /// task without taking a lock.
    pub fn set_bandwidth_cap(&self, bps: Option<u64>) {
        let raw = bps.unwrap_or(BANDWIDTH_CAP_NONE);
        self.inner.bandwidth_cap.store(raw, Ordering::Relaxed);
    }

    /// Current bandwidth cap if any. `None` means unlimited.
    #[must_use]
    pub fn bandwidth_cap(&self) -> Option<u64> {
        let raw = self.inner.bandwidth_cap.load(Ordering::Relaxed);
        if raw == BANDWIDTH_CAP_NONE {
            None
        } else {
            Some(raw)
        }
    }

    /// Set the engine-controlled priority hint. Single atomic store.
    pub fn set_priority(&self, p: Priority) {
        self.inner.priority.store(p as u8, Ordering::Relaxed);
    }

    /// Current priority hint.
    #[must_use]
    pub fn priority(&self) -> Priority {
        match self.inner.priority.load(Ordering::Relaxed) {
            x if x == Priority::Foreground as u8 => Priority::Foreground,
            x if x == Priority::Normal as u8 => Priority::Normal,
            x if x == Priority::Background as u8 => Priority::Background,
            x if x == Priority::Bulk as u8 => Priority::Bulk,
            // Unreachable in practice: only set_priority writes to
            // this atomic and the enum is non-exhaustive only at the
            // public API boundary, not on the wire.
            _ => Priority::Normal,
        }
    }

    /// The account this handle is scoped to.
    #[must_use]
    pub fn account(&self) -> &AccountId {
        &self.inner.account
    }

    /// Default retry policy applied to requests on this account.
    #[must_use]
    pub fn default_retry(&self) -> &RetryPolicy {
        &self.inner.default_retry
    }

    /// Underlying token source. Exposed so the OAuth refresher in
    /// `auth.rs` can share the trait object across requests.
    #[must_use]
    pub fn token_source(&self) -> &Arc<dyn TokenSource> {
        &self.inner.token_source
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
