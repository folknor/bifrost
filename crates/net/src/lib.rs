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

pub mod auth;
pub mod bandwidth;
pub mod config;
pub mod error;
pub mod net;
pub mod rate;
pub mod request;
pub mod retry;

pub use auth::{AccessToken, OAuthRefresher, RefreshState, TokenSource};
pub use bandwidth::{AccountMeter, BandwidthMeter, MeterSink};
// Shared identity / priority / byte-range / future-alias types live
// in bifrost-types so the engine, the protocol crates, and this
// crate all speak the same language. Re-exported here for ergonomic
// imports from downstream code.
pub use bifrost_types::{AccountFuture, AccountId, ByteRange, Priority};
pub use config::NetConfig;
pub use error::Error;
pub use net::{AccountNet, AccountSpec, Net};
pub use rate::{RateLimit, RateLimitGovernor, RequestCost};
pub use request::{ByteStream, RequestBuilder, Response, StreamingResponse};
pub use retry::RetryPolicy;
