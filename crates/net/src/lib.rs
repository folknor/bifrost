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
//! The Phase 2 implementation wires the retry loop, single-flight
//! refresher, per-host token bucket, sliding-window bandwidth meter,
//! and traceparent header. The public surface (`Net`, `AccountNet`,
//! `RequestBuilder`, `Response`, `StreamingResponse`, `Error`) is
//! frozen for Phase 2 consumers and matches the Phase 1 skeleton.

pub mod auth;
pub mod bandwidth;
pub mod config;
pub mod error;
pub mod net;
pub mod rate;
pub mod redirect;
pub mod request;
pub mod retry;
pub(crate) mod trace;
pub mod url;

pub use auth::{AccessToken, OAuthRefresher, RefreshState, StaticTokenSource, TokenSource};
pub use bandwidth::{AccountMeter, BandwidthMeter, MeterSink, MeterSinkHandle};
// Shared identity / priority / byte-range / future-alias types live
// in bifrost-types so the engine, the protocol crates, and this
// crate all speak the same language. Re-exported here for ergonomic
// imports from downstream code.
pub use bifrost_types::{AccountFuture, AccountId, ByteRange, Priority};
pub use config::NetConfig;
pub use error::Error;
pub use net::{AccountNet, AccountSpec, Net};
pub use rate::{RateLimit, RateLimitGovernor, RequestCost};
pub use redirect::{FollowRedirects, RedirectAction, RedirectPolicy, RedirectStep};
pub use request::{ByteStream, RequestBuilder, Response, StreamingResponse};
pub use retry::RetryPolicy;
