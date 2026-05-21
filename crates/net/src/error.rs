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

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use reqwest::{StatusCode, header::HeaderMap};
use thiserror::Error;

/// Maximum body bytes captured in `Error::Status`. The terminal-error
/// path runs to completion regardless of body size, so a multi-megabyte
/// HTML error page would otherwise be moved into the typed error and
/// kept alive until the protocol crate dropped the error. 4 KB is large
/// enough to fit a typical JSON error envelope plus stack hint while
/// keeping the error allocation small.
const STATUS_BODY_CAP: usize = 4096;
/// Marker appended when `Error::Status::body` was truncated. Callers
/// that pattern-match the body for diagnostics can detect truncation
/// without an extra field on the variant.
const STATUS_BODY_TRUNCATED_MARKER: &[u8] = b" ... (truncated)";

/// Truncate a response body to at most `STATUS_BODY_CAP` bytes,
/// appending a visible marker when truncation occurred. Used by the
/// retry loop before constructing `Error::Status` so terminal errors
/// do not retain megabyte-sized response payloads.
#[must_use]
pub fn cap_status_body(body: Bytes) -> Bytes {
    if body.len() <= STATUS_BODY_CAP {
        return body;
    }
    let mut buf = Vec::with_capacity(STATUS_BODY_CAP + STATUS_BODY_TRUNCATED_MARKER.len());
    buf.extend_from_slice(&body[..STATUS_BODY_CAP]);
    buf.extend_from_slice(STATUS_BODY_TRUNCATED_MARKER);
    Bytes::from(buf)
}

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

    /// Caller asked the per-host governor to debit a cost that exceeds
    /// the bucket's burst capacity. The bucket can never fill that high,
    /// so the request would block forever; we surface a typed error
    /// instead. Configuration bug rather than runtime condition.
    #[error("request cost {cost} exceeds bucket burst {burst}")]
    CostExceedsBurst {
        /// Requested debit in quota units.
        cost: u32,
        /// Configured burst capacity for the host.
        burst: u32,
    },

    /// Request body could not be serialized to bytes (e.g. `RequestBuilder::json`
    /// received a value whose `Serialize` impl returned an error). The
    /// error is captured at the builder call site and surfaced on the
    /// subsequent `send` so the fluent API does not have to return a
    /// `Result` on every chained setter.
    #[error("request body encoding failed: {message}")]
    EncodeBody {
        /// Human-readable description of the encoding failure.
        message: String,
        /// Boxed underlying error (typically `serde_json::Error`).
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    /// OAuth refresh attempt failed for a reason other than the refresh
    /// token being rejected. The original error is preserved behind an
    /// `Arc` so multiple waiters on a single-flight refresh can share
    /// it without `Error` having to be `Clone`. Surfaced when the
    /// refresher would otherwise have to flatten a `Network` or
    /// `Timeout` failure into `AuthLost` and lose the transient-vs-
    /// permanent distinction.
    #[error("OAuth refresh failed: {source}")]
    RefreshFailed {
        /// Underlying error from the token source. Shared with any
        /// concurrent waiters that were single-flighted onto the same
        /// refresh attempt.
        source: Arc<Error>,
    },

    /// Caller requested a byte range but the server returned a response
    /// that did not honor it (either `200 OK` collapsed the range, or
    /// `206 Partial Content` with a `Content-Range` that disagrees with
    /// the request). Returned before any body bytes are yielded so the
    /// caller does not assemble a misaligned blob.
    #[error("range not honored: {message}")]
    RangeNotHonored {
        /// Description of the mismatch.
        message: String,
    },

    /// Constructing `Net` failed during TLS or HTTP-client setup. The
    /// `NetConfig` is consumer-supplied so the failure is a runtime
    /// condition rather than a programmer error.
    #[error("Net construction failed: {message}")]
    NetSetup {
        /// Description of the setup failure.
        message: String,
        /// Boxed underlying error from `native_tls` or `reqwest`.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },
}
