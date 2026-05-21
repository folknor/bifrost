//! Retry policy for transient HTTP failures.
//!
//! One policy applies to every request unless the caller overrides
//! via `RequestBuilder::retry`. The defaults cover the union of what
//! Gmail and Graph were doing before the consolidation: 429 plus the
//! 5xx family, three attempts, jittered exponential backoff, and a
//! capped honor for `Retry-After`.

use std::time::Duration;

use reqwest::StatusCode;

/// Per-request retry policy. Used by the transport's retry loop to
/// decide whether to retry, how long to wait, and when to surface a
/// `RetryBudgetExhausted` error.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of attempts including the initial one. A value
    /// of 1 disables retry.
    pub max_attempts: u32,
    /// Initial backoff before the second attempt. Subsequent attempts
    /// use decorrelated jitter capped by `max_backoff`.
    pub initial_backoff: Duration,
    /// Hard cap on any single backoff sleep.
    pub max_backoff: Duration,
    /// Hard cap on any single `Retry-After` server hint. Servers
    /// occasionally return absurd values during partial outage; the
    /// cap prevents multi-hour stalls.
    pub honor_retry_after_cap: Duration,
    /// Additional HTTP status codes the policy retries on, on top of
    /// the always-on 5xx family. The retry loop treats `statuses` as
    /// the *additive* set and applies `StatusCode::is_server_error()`
    /// unconditionally: 5xx responses are retried whether or not the
    /// caller listed them here, so callers should populate `statuses`
    /// only with 4xx codes they want retried (typically just 429).
    /// Listing a 5xx code here is harmless (the membership test
    /// short-circuits before `is_server_error()`) but redundant.
    pub statuses: Vec<StatusCode>,
    /// Whether to retry transport-level failures (connect, reset,
    /// idle timeout, TLS handshake).
    pub network_errors: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            honor_retry_after_cap: Duration::from_secs(60),
            statuses: vec![
                StatusCode::TOO_MANY_REQUESTS,
                StatusCode::INTERNAL_SERVER_ERROR,
                StatusCode::BAD_GATEWAY,
                StatusCode::SERVICE_UNAVAILABLE,
                StatusCode::GATEWAY_TIMEOUT,
            ],
            network_errors: true,
        }
    }
}

impl RetryPolicy {
    /// Build a `RetryPolicy` with the defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable retries entirely. Useful for one-shot operations where
    /// the caller wants the raw status surfaced.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            max_attempts: 1,
            ..Self::default()
        }
    }
}
