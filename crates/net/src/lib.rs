#![forbid(unsafe_code)]
//! Shared HTTP transport for the bifrost JMAP, Gmail, and Graph
//! clients.
//!
//! `bifrost-net` owns the HTTP/2 connection pool, OAuth bearer-token
//! refresh, retry budget with `Retry-After` honor, per-host token
//! bucket rate limiter, per-account bandwidth metering, native-tls
//! configuration, and W3C `traceparent` injection. It does not own
//! protocol-level JSON shapes, IMAP, or SMTP transport.
//!
//! The v1 surface in this crate is a skeleton: every type and method
//! exists with the right signature, and bodies that would require
//! real network logic return `unimplemented!()`. Phase 2 fills in the
//! retry loop, single-flight refresher, token-bucket math, metering,
//! and traceparent injection without changing the public surface.
//!
//! Local stub types `AccountId` and `Priority` live here for now;
//! Phase 2 reconciles them with `bifrost-types`.

pub mod auth;
pub mod bandwidth;
pub mod config;
pub mod error;
pub mod net;
pub mod rate;
pub mod request;
pub mod retry;

pub use auth::{AccessToken, AccountFuture, OAuthRefresher, RefreshState, TokenSource};
pub use bandwidth::{AccountMeter, BandwidthMeter, MeterSink};
pub use config::NetConfig;
pub use error::Error;
pub use net::{AccountNet, AccountSpec, Net};
pub use rate::{RateLimit, RateLimitGovernor, RequestCost};
pub use request::{ByteRange, ByteStream, RequestBuilder, Response, StreamingResponse};
pub use retry::RetryPolicy;

/// Stable account identifier. Local to this crate in the v1
/// skeleton; replaced by `bifrost_types::AccountId` once that crate
/// lands and Phase 2 reconciles the import.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AccountId(pub u64);

/// Engine-controlled per-account priority hint. The rate-limit
/// governor divides bucket size by 4 for `Background` and by 8 for
/// `Bulk` so user-visible foreground traffic is not starved by a
/// backfill.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Priority {
    /// User-visible request that should run as soon as the bucket
    /// allows.
    Foreground,
    /// Background sync work. Bucket is divided by 4.
    Background,
    /// Bulk backfill. Bucket is divided by 8.
    Bulk,
}
