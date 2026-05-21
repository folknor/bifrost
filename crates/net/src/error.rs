//! Typed errors surfaced by the HTTP transport.
//!
//! Protocol crates map each variant onto their own typed errors and
//! recovery classes. The engine never sees `bifrost_net::Error`
//! directly.
//!
//! `Error::Status` (and `Response` in `request.rs`) carries
//! `reqwest::StatusCode` and `reqwest::header::HeaderMap` straight
//! through. That couples downstream protocol crates to reqwest; a
//! future swap to a different HTTP client would break every
//! pattern-match on status across the four protocol crates.
//! Acceptable for v1 because all four current protocols are on
//! reqwest and `StatusCode` is a thin u16 wrapper. Phase 2 may wrap
//! these in opaque newtypes if the surface fans out further.

use std::time::Duration;

use bytes::Bytes;
use reqwest::{StatusCode, header::HeaderMap};
use thiserror::Error;

/// Errors returned by `Net`, `AccountNet`, and `RequestBuilder`.
///
/// Marked `#[non_exhaustive]` to allow new variants without breaking
/// downstream matches.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum Error {
    /// Connect, reset, DNS, TLS handshake, or other transport-level
    /// failure surfaced by the underlying HTTP stack. The source is
    /// boxed so the public surface does not leak the `hyper` /
    /// `reqwest` error types directly.
    #[error("network error: {message}")]
    Network {
        /// Human-readable description of the underlying failure.
        message: String,
        /// Boxed underlying error. Optional because some call sites
        /// synthesize a `Network` error without a backing source.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    /// Per-request timeout elapsed.
    #[error("request timed out")]
    Timeout,

    /// TLS handshake or certificate validation failure. Kept distinct
    /// from `Network` so consumers can present trust-store-specific
    /// guidance without sniffing error strings.
    #[error("TLS error: {message}")]
    Tls {
        /// Description of the TLS failure.
        message: String,
    },

    /// Server returned a non-success status code outside the retry
    /// set, or returned a status code inside the set but the retry
    /// budget was exhausted before the call escalated to
    /// `RetryBudgetExhausted`.
    #[error("HTTP {code}")]
    Status {
        /// HTTP status code.
        code: StatusCode,
        /// Response body bytes as received.
        body: Bytes,
        /// Response headers as received.
        headers: HeaderMap,
    },

    /// Retry budget exhausted. The final attempt's status code (if
    /// any) and the `Retry-After` history are surfaced so the
    /// protocol crate can map this to `RecoveryClass::Retry { after }`
    /// or `RecoveryClass::OperatorOverrideRequired { reason }` as
    /// appropriate.
    #[error("retry budget exhausted")]
    RetryBudgetExhausted {
        /// Status code of the final attempt, if there was one.
        last_status: Option<StatusCode>,
        /// `Retry-After` durations honored across attempts, in order.
        retry_after_history: Vec<Duration>,
    },

    /// OAuth token refresh failed or the server returned 401 after a
    /// forced refresh and retry. The protocol crate maps this to a
    /// terminal-auth recovery class.
    #[error("auth lost")]
    AuthLost,

    /// Server returned 429 and the retry budget was exhausted.
    #[error("rate limited")]
    RateLimited {
        /// Server-supplied retry hint from `Retry-After`, if present.
        retry_after: Option<Duration>,
    },

    /// Request was cancelled before completion.
    #[error("request cancelled")]
    Cancelled,
}
